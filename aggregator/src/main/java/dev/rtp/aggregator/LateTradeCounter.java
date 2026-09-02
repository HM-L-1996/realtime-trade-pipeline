package dev.rtp.aggregator;

import dev.rtp.model.TradeRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 워터마크를 지나 도착해 버려진 체결을 센다.
 *
 * <p>이 수치가 없으면 "공식 캔들과 거래량이 다르다" 에서 멈춘다.
 * 몇 건이 버려졌는지를 알아야 워터마크 지연 설정과 정확도를 잇는 표
 * ({@code validation.md}) 를 쓸 수 있다.
 */
public class LateTradeCounter extends ProcessFunction<TradeRecord, TradeRecord> {

    private static final long serialVersionUID = 1L;

    private final String clickhouseUrl;
    private final String user;
    private final String password;
    private final String jobRun;

    private transient Counter dropped;
    private transient Counter recordFailures;
    private transient DeadLetter deadLetter;

    public LateTradeCounter(JobConfig cfg) {
        this.clickhouseUrl = cfg.clickhouseUrl();
        this.user = cfg.clickhouseUser();
        this.password = cfg.clickhousePassword();
        this.jobRun = cfg.runId();
    }

    @Override
    public void open(Configuration parameters) {
        dropped = getRuntimeContext().getMetricGroup().counter("lateTradesDropped");
        recordFailures = getRuntimeContext().getMetricGroup().counter("lateTradeRecordFailures");
        deadLetter = new DeadLetter(clickhouseUrl, user, password, jobRun);
        // 적재는 비동기라 실패가 이 스레드로 돌아오지 않는다. 게이지로 꺼낸다.
        getRuntimeContext().getMetricGroup()
                .gauge("lateDeadLetterWriteFailures", () -> deadLetter.failedWrites());
    }

    @Override
    public void processElement(TradeRecord t, Context ctx, Collector<TradeRecord> out) {
        dropped.inc();
        // 세는 것만으로는 "왜 늦었는지" 를 사후에 볼 수 없다. 원본을 남긴다.
        // 여기서 실패해도 파이프라인을 멈추지 않는다 - 본말이 전도된다.
        // 큐에 넣기만 한다. 늦은 체결이 한꺼번에 쏟아질 때(실측 4,255건)
        // 여기서 동기 HTTP 를 치면 처리 스레드가 그 수만큼 묶인다.
        boolean ok = deadLetter.record("window", "late-window", t.symbol(),
                "eventMs=" + t.eventMs() + " seq=" + t.seqInMs()
                        + " price=" + t.price() + " volume=" + t.volume()
                        + " connId=" + t.connId() + " recvSeq=" + t.recvSeq());
        if (!ok) {
            recordFailures.inc();
        }
        out.collect(t);
    }
}
