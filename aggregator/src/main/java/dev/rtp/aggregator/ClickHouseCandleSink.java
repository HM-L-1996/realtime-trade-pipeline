package dev.rtp.aggregator;

import dev.rtp.model.Candle;
import dev.rtp.model.Decimals;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * 분봉을 ClickHouse HTTP 인터페이스로 적재한다.
 *
 * <p><b>이 싱크는 at-least-once 다.</b> 그리고 그건 결함이 아니라 이 프로젝트의 관측 대상이다.
 * ClickHouse 는 분산 트랜잭션(XA)을 지원하지 않으므로 Flink 의 2PC 로 exactly-once 를
 * 만들 수 없다. 장애가 나면 마지막 체크포인트 이후 구간이 다시 처리되어 같은 윈도가
 * 두 번 쓰인다.
 *
 * <p>그래서 캔들 테이블을 {@code ReplacingMergeTree} 가 아니라 {@code MergeTree} 로 뒀다.
 * 중복이 저장 계층에서 조용히 접히면 "exactly-once 가 실제로 지켜지는가" 를
 * 결과로 확인할 수 없다. 중복은 {@code candles_1m_dedup.write_count > 1} 로 드러난다.
 *
 * <p>Sink V2 를 쓴다. {@link SinkWriter#flush} 가 체크포인트 직전에 호출되므로
 * 버퍼를 비우는 시점이 체크포인트 경계와 자연히 맞는다.
 *
 * <p>JDBC 대신 HTTP 를 쓰는 이유는 둘이다. clickhouse-jdbc 를 Flink fat jar 에 넣으면
 * 셰이딩 충돌이 잦고, 여기서는 배치·플러시·재시도가 명시적으로 보이는 편이 낫다.
 */
public class ClickHouseCandleSink implements Sink<Candle>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String baseUrl;
    private final String user;
    private final String password;
    private final int batchSize;

    ClickHouseCandleSink(String baseUrl, String user, String password, int batchSize) {
        this.baseUrl = baseUrl;
        this.user = user;
        this.password = password;
        this.batchSize = batchSize;
    }

    public static ClickHouseCandleSink of(JobConfig cfg) {
        return new ClickHouseCandleSink(cfg.clickhouseUrl(), cfg.clickhouseUser(),
                cfg.clickhousePassword(), cfg.sinkBatchSize());
    }

    @Override
    public SinkWriter<Candle> createWriter(InitContext context) {
        Writer w = new Writer(baseUrl, user, password, batchSize);
        w.bindMetrics(context.metricGroup().counter("clickhouseRowsWritten"),
                context.metricGroup().counter("clickhouseFlushFailures"));
        return w;
    }

    /** JSONEachRow 한 줄. {@link #toJson} 은 테스트를 위해 패키지 공개로 둔다. */
    static String toJson(Candle c) {
        return "{\"symbol\":\"" + esc(c.symbol) + "\""
                + ",\"window_start\":" + c.windowStartMs
                // 가격·수량을 숫자 리터럴이 아니라 문자열로 넣는다. ClickHouse 는 Decimal
                // 컬럼에 문자열을 받아 정확히 파싱한다. JSON 숫자로 보내면 파서가 double 을
                // 거치며 정밀도가 깎일 수 있고, 그게 곧 검증 오차가 된다.
                + ",\"open\":\"" + Decimals.priceToString(c.open) + "\""
                + ",\"high\":\"" + Decimals.priceToString(c.high) + "\""
                + ",\"low\":\"" + Decimals.priceToString(c.low) + "\""
                + ",\"close\":\"" + Decimals.priceToString(c.close) + "\""
                + ",\"volume\":\"" + Decimals.volumeToString(c.volume) + "\""
                + ",\"trade_count\":" + c.tradeCount
                + ",\"run_id\":\"" + esc(c.runId) + "\"}";
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** 실제 적재를 수행한다. 서브태스크마다 하나씩 만들어진다. */
    static final class Writer implements SinkWriter<Candle> {

        private static final Logger log = LoggerFactory.getLogger(Writer.class);

        private static final String INSERT_SQL =
                "INSERT INTO rtp.candles_1m "
                + "(symbol, window_start, open, high, low, close, volume, trade_count, run_id) "
                + "FORMAT JSONEachRow";

        private final String baseUrl;
        private final String authHeader;
        private final int batchSize;
        private final HttpClient http;
        private final List<String> buffer;

        private Counter rowsWritten;
        private Counter flushFailures;

        Writer(String baseUrl, String user, String password, int batchSize) {
            this.baseUrl = baseUrl;
            this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
                    (user + ":" + password).getBytes(StandardCharsets.UTF_8));
            this.batchSize = batchSize;
            this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            this.buffer = new ArrayList<>(batchSize);
        }

        void bindMetrics(Counter rowsWritten, Counter flushFailures) {
            this.rowsWritten = rowsWritten;
            this.flushFailures = flushFailures;
        }

        @Override
        public void write(Candle c, Context context) throws IOException, InterruptedException {
            buffer.add(toJson(c));
            if (buffer.size() >= batchSize) {
                doFlush();
            }
        }

        /**
         * Flink 가 체크포인트 직전과 스트림 종료 시에 호출한다.
         *
         * <p>버퍼는 상태로 저장하지 않는다. 복구되면 Kafka 오프셋이 되감기면서 같은 체결이
         * 다시 흘러 캔들이 재생성되기 때문이다. 버퍼까지 복원하면 그 구간이 두 번 쓰인다.
         */
        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            doFlush();
        }

        @Override
        public void close() throws Exception {
            doFlush();
        }

        private void doFlush() throws IOException, InterruptedException {
            if (buffer.isEmpty()) {
                return;
            }
            String body = String.join("\n", buffer);
            int rows = buffer.size();

            Exception last = null;
            for (int attempt = 1; attempt <= 3; attempt++) {
                try {
                    send(body);
                    if (rowsWritten != null) {
                        rowsWritten.inc(rows);
                    }
                    buffer.clear();
                    return;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                } catch (Exception e) {
                    last = e;
                    log.warn("ClickHouse 적재 실패 (시도 {}/3): {}", attempt, e.getMessage());
                    Thread.sleep(500L * attempt);
                }
            }
            if (flushFailures != null) {
                flushFailures.inc();
            }
            // 삼켜서는 안 된다. 여기서 예외를 올려야 체크포인트가 실패하고
            // 잡이 재시작되어 마지막 성공 지점부터 다시 처리한다.
            // 조용히 넘기면 데이터가 사라진 채로 잡은 건강해 보인다 - 최악의 형태다.
            throw new IOException("ClickHouse 적재 3회 실패", last);
        }

        private void send(String body) throws Exception {
            URI uri = URI.create(baseUrl + "/?query="
                    + URLEncoder.encode(INSERT_SQL, StandardCharsets.UTF_8));

            HttpRequest req = HttpRequest.newBuilder(uri)
                    .header("Authorization", authHeader)
                    .header("Content-Type", "text/plain; charset=UTF-8")
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() / 100 != 2) {
                String detail = res.body();
                throw new IllegalStateException("HTTP " + res.statusCode() + " "
                        + (detail.length() > 300 ? detail.substring(0, 300) : detail));
            }
        }
    }
}
