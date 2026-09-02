package dev.rtp.aggregator;

import dev.rtp.model.ClickHouseHttp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 버려진 레코드를 남긴다.
 *
 * <p>세기만 하고 버리면 "무엇이 왜 버려졌는지" 를 사후에 볼 수 없고, 고친 뒤
 * 재처리할 수도 없다. 카운터는 <b>몇 건인지</b>만 말해 준다 -
 * 특정 종목에 몰렸는지, 소스 형식이 바뀐 것인지, 우리 파서가 틀린 것인지는 답하지 못한다.
 *
 * <h2>왜 큐와 별도 스레드를 두는가</h2>
 * 처음에는 {@code record()} 안에서 곧바로 HTTP 를 호출했다. 한 건마다 한 번씩,
 * 동기로. <b>그 구현은 dead letter 가 많아질수록 파이프라인을 느리게 만든다.</b>
 * 호출자는 Flink 처리 스레드({@code deserialize}, {@code processElement})이고
 * 그 스레드가 최대 30초 타임아웃의 블로킹 요청에 묶인다.
 *
 * <p>자체 실험에서 이미 한 번에 4,255건을 버린 적이 있다. 그 조건이면 처리
 * 스레드가 수천 번의 순차 HTTP 호출에 붙잡힌다. 즉 <b>"버려야 할 것" 때문에
 * 본류가 멈추는</b> 상황이고, {@code failure-policy.md} 가 경계한 바로 그것이다.
 * 예외가 아니라 동기 I/O 형태로 같은 일이 벌어진다.
 *
 * <p>그래서 유한한 큐에 넣고 별도 스레드가 묶어서 적재한다.
 *
 * <h2>큐가 가득 차면 버린다 (그리고 그 사실을 센다)</h2>
 * 큐를 무한히 두면 메모리가 늘어나 결국 TaskManager 가 죽는다.
 * dead letter 를 지키려다 파이프라인을 잃는 것은 본말이 전도된 것이다.
 * 가득 차면 <b>기다리지 않고 버리고</b> {@link #droppedByBackpressure()} 로 센다.
 * 버린 것을 또 버렸다는 사실 자체는 남는다.
 *
 * <p><b>적재 실패를 삼킨다.</b> 다만 조용히 삼키지는 않는다 - 처음 몇 건은
 * 예외 내용까지 로그로 남긴다. 실패 사유를 모르면 "왜 안 쌓였는가" 에 답할 수 없다.
 */
public final class DeadLetter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(DeadLetter.class);

    public static final String TABLE = "rtp.dead_letters";
    public static final String COLUMNS = "stage, reason, symbol, payload, job_run";

    /** 큐 용량. 넘치면 버린다. 메모리를 위해 유한해야 한다. */
    static final int QUEUE_CAPACITY = 10_000;
    /** 한 번에 묶어 보내는 최대 행 수. */
    static final int BATCH_SIZE = 200;
    /** 배치가 안 차도 이만큼 지나면 보낸다. 드문드문 오는 것도 곧 보이게. */
    static final long FLUSH_INTERVAL_MS = 2_000L;

    private final String baseUrl;
    private final String user;
    private final String password;
    private final String jobRun;

    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong written = new AtomicLong();

    private transient BlockingQueue<String> queue;
    private transient Thread writer;
    private transient volatile boolean running;
    private transient int loggedFailures;

    public DeadLetter(String baseUrl, String user, String password, String jobRun) {
        this.baseUrl = baseUrl;
        this.user = user;
        this.password = password;
        this.jobRun = jobRun;
        start();
    }

    private void start() {
        queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        running = true;
        ClickHouseHttp ch = new ClickHouseHttp(baseUrl, user, password);
        writer = new Thread(() -> drainLoop(ch), "dead-letter-writer");
        // 데몬이라 잡이 끝날 때 붙잡지 않는다.
        writer.setDaemon(true);
        writer.start();
    }

    private void drainLoop(ClickHouseHttp ch) {
        List<String> batch = new ArrayList<>(BATCH_SIZE);
        while (running || !queue.isEmpty()) {
            try {
                String first = queue.poll(FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                queue.drainTo(batch, BATCH_SIZE - 1);
                flush(ch, batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
                batch.clear();
            }
        }
    }

    private void flush(ClickHouseHttp ch, List<String> batch) {
        try {
            ch.insertJsonEachRow(TABLE, COLUMNS, batch);
            written.addAndGet(batch.size());
        } catch (Exception e) {
            failed.addAndGet(batch.size());
            // 조용히 삼키면 "왜 dead_letters 가 비어 있는가" 에 답할 수 없다.
            // 처음 몇 건만 남긴다 - 싱크가 계속 막히면 로그가 이것으로 가득 찬다.
            if (loggedFailures++ < 5) {
                log.warn("dead letter 적재 실패 rows={} 원인={}: {}",
                        batch.size(), e.getClass().getSimpleName(), e.getMessage());
            }
        }
    }

    /**
     * 큐에 넣는다. <b>블로킹하지 않는다.</b>
     *
     * @return 큐에 들어갔으면 true. 큐가 가득 차 버렸으면 false.
     *         적재 성공 여부가 아니라는 점에 주의한다 - 적재는 비동기다.
     */
    public boolean record(String stage, String reason, String symbol, String payload) {
        String row = "{\"stage\":" + ClickHouseHttp.quote(stage)
                + ",\"reason\":" + ClickHouseHttp.quote(reason)
                + ",\"symbol\":" + ClickHouseHttp.quote(symbol == null ? "" : symbol)
                // 페이로드가 클 수 있다. 원인 파악에는 앞부분이면 충분하고,
                // 통째로 넣으면 형식이 바뀐 날 테이블이 폭발한다.
                + ",\"payload\":" + ClickHouseHttp.quote(truncate(payload, 2000))
                + ",\"job_run\":" + ClickHouseHttp.quote(jobRun) + "}";

        if (queue.offer(row)) {
            return true;
        }
        long n = dropped.incrementAndGet();
        if (n == 1 || n % 1000 == 0) {
            log.warn("dead letter 큐가 가득 차 버림 누적={}건 (capacity={})", n, QUEUE_CAPACITY);
        }
        return false;
    }

    /** 큐가 가득 차서 기록조차 못 한 건수. */
    public long droppedByBackpressure() {
        return dropped.get();
    }

    /** 적재를 시도했다가 실패한 건수. */
    public long failedWrites() {
        return failed.get();
    }

    /** 실제로 적재된 건수. */
    public long writtenRows() {
        return written.get();
    }

    /** 잡 종료 시 남은 것을 비운다. */
    public void close() {
        running = false;
        Thread w = writer;
        if (w != null) {
            w.interrupt();
        }
    }

    static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "…(truncated)";
    }
}
