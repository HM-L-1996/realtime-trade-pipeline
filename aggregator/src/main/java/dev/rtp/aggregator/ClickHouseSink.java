package dev.rtp.aggregator;

import dev.rtp.model.Candle;
import dev.rtp.model.ClickHouseHttp;
import dev.rtp.model.Decimals;
import dev.rtp.model.TradeRecord;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ClickHouse 적재 싱크.
 *
 * <p><b>이 싱크는 at-least-once 다.</b> 그리고 그건 결함이 아니라 이 프로젝트의 관측 대상이다.
 * ClickHouse 는 분산 트랜잭션(XA)을 지원하지 않으므로 Flink 의 2PC 로 exactly-once 를
 * 만들 수 없다. 장애가 나면 마지막 체크포인트 이후 구간이 다시 처리되어 같은 행이 두 번 쓰인다.
 *
 * <p>그래서 캔들 테이블을 {@code ReplacingMergeTree} 가 아니라 {@code MergeTree} 로 뒀다.
 * 중복이 저장 계층에서 조용히 접히면 "exactly-once 가 실제로 지켜지는가" 를
 * 결과로 확인할 수 없다. 중복은 {@code candles_1m_dedup.write_count > 1} 로 드러난다.
 *
 * <p>{@link SinkWriter#flush} 가 체크포인트 직전에 호출되므로 버퍼를 비우는 시점이
 * 체크포인트 경계와 자연히 맞는다.
 */
public class ClickHouseSink<T> implements Sink<T>, Serializable {

    private static final long serialVersionUID = 1L;

    /** 레코드를 JSONEachRow 한 줄로 바꾼다. */
    public interface RowMapper<T> extends Serializable {
        String toJson(T value);
    }

    private final String baseUrl;
    private final String user;
    private final String password;
    private final int batchSize;
    private final String table;
    private final String columns;
    private final String metricName;
    private final RowMapper<T> mapper;

    ClickHouseSink(JobConfig cfg, String table, String columns,
                   String metricName, RowMapper<T> mapper) {
        this.baseUrl = cfg.clickhouseUrl();
        this.user = cfg.clickhouseUser();
        this.password = cfg.clickhousePassword();
        this.batchSize = cfg.sinkBatchSize();
        this.table = table;
        this.columns = columns;
        this.metricName = metricName;
        this.mapper = mapper;
    }

    /** 집계된 분봉. */
    public static ClickHouseSink<Candle> candles(JobConfig cfg) {
        return new ClickHouseSink<>(cfg, "rtp.candles_1m",
                "symbol, window_start, open, high, low, close, volume, trade_count, run_id",
                "clickhouseCandleRows", ClickHouseSink::candleJson);
    }

    /**
     * 원본 체결.
     *
     * <p>집계 잡에 붙여 둔 이유 - 이 잡이 이미 스트림을 읽고 있어서 별도 서비스를
     * 띄울 이유가 없다. 대신 <b>집계와 보관의 수명이 묶인다</b>는 대가가 있다.
     * 잡이 죽으면 보관도 멈춘다. 보관이 독립적으로 중요해지면 그때 분리한다.
     *
     * <p>이게 있어야 {@code recv_seq} 공백으로 소스 유실을 측정할 수 있다.
     * 그 측정이 없으면 공식 캔들과의 차이가 내 버그인지 소스 유실인지 끝까지 구분되지 않는다.
     */
    public static ClickHouseSink<TradeRecord> trades(JobConfig cfg) {
        return new ClickHouseSink<>(cfg, "rtp.trades_raw",
                "symbol, event_time, seq_in_ms, ingest_time, price, volume, "
                        + "recv_seq, conn_id, kafka_partition, kafka_offset",
                "clickhouseTradeRows", ClickHouseSink::tradeJson);
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) {
        Writer<T> w = new Writer<>(baseUrl, user, password, batchSize, table, columns, mapper);
        w.bindMetrics(context.metricGroup().counter(metricName),
                context.metricGroup().counter(metricName + "FlushFailures"));
        return w;
    }

    static String candleJson(Candle c) {
        return "{\"symbol\":" + ClickHouseHttp.quote(c.symbol)
                + ",\"window_start\":" + c.windowStartMs
                // 가격·수량을 숫자 리터럴이 아니라 문자열로 넣는다. ClickHouse 는 Decimal
                // 컬럼에 문자열을 받아 정확히 파싱한다. JSON 숫자로 보내면 파서가 double 을
                // 거치며 정밀도가 깎일 수 있고, 그게 곧 검증 오차가 된다.
                + ",\"open\":" + ClickHouseHttp.quote(Decimals.priceToString(c.open))
                + ",\"high\":" + ClickHouseHttp.quote(Decimals.priceToString(c.high))
                + ",\"low\":" + ClickHouseHttp.quote(Decimals.priceToString(c.low))
                + ",\"close\":" + ClickHouseHttp.quote(Decimals.priceToString(c.close))
                + ",\"volume\":" + ClickHouseHttp.quote(Decimals.volumeToString(c.volume))
                + ",\"trade_count\":" + c.tradeCount
                + ",\"run_id\":" + ClickHouseHttp.quote(c.runId) + "}";
    }

    static String tradeJson(TradeRecord t) {
        return "{\"symbol\":" + ClickHouseHttp.quote(t.symbol())
                + ",\"event_time\":" + t.eventMs()
                + ",\"seq_in_ms\":" + t.seqInMs()
                + ",\"ingest_time\":" + t.ingestMs()
                + ",\"price\":" + ClickHouseHttp.quote(t.price())
                + ",\"volume\":" + ClickHouseHttp.quote(t.volume())
                + ",\"recv_seq\":" + t.recvSeq()
                + ",\"conn_id\":" + ClickHouseHttp.quote(t.connId())
                // Kafka 좌표는 아직 채우지 않는다. 원본 체결의 신원은 conn_id + recv_seq
                // 로 충분하고, 파티션/오프셋은 소스 자체가 아니라 전송 경로의 속성이다.
                + ",\"kafka_partition\":0,\"kafka_offset\":0}";
    }

    /** 실제 적재를 수행한다. 서브태스크마다 하나씩 만들어진다. */
    static final class Writer<T> implements SinkWriter<T> {

        private static final Logger log = LoggerFactory.getLogger(Writer.class);

        private final ClickHouseHttp ch;
        private final int batchSize;
        private final String table;
        private final String columns;
        private final RowMapper<T> mapper;
        private final List<String> buffer;

        private Counter rowsWritten;
        private Counter flushFailures;

        Writer(String baseUrl, String user, String password, int batchSize,
               String table, String columns, RowMapper<T> mapper) {
            this.ch = new ClickHouseHttp(baseUrl, user, password);
            this.batchSize = batchSize;
            this.table = table;
            this.columns = columns;
            this.mapper = mapper;
            this.buffer = new ArrayList<>(batchSize);
        }

        void bindMetrics(Counter rowsWritten, Counter flushFailures) {
            this.rowsWritten = rowsWritten;
            this.flushFailures = flushFailures;
        }

        @Override
        public void write(T value, Context context) throws IOException, InterruptedException {
            buffer.add(mapper.toJson(value));
            if (buffer.size() >= batchSize) {
                doFlush();
            }
        }

        /**
         * Flink 가 체크포인트 직전과 스트림 종료 시에 호출한다.
         *
         * <p>버퍼는 상태로 저장하지 않는다. 복구되면 Kafka 오프셋이 되감기면서 같은 체결이
         * 다시 흘러 재생성되기 때문이다. 버퍼까지 복원하면 그 구간이 두 번 쓰인다.
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
            int rows = buffer.size();
            Exception last = null;

            for (int attempt = 1; attempt <= 3; attempt++) {
                try {
                    ch.insertJsonEachRow(table, columns, buffer);
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
                    log.warn("{} 적재 실패 (시도 {}/3): {}", table, attempt, e.getMessage());
                    Thread.sleep(500L * attempt);
                }
            }
            if (flushFailures != null) {
                flushFailures.inc();
            }
            // 삼켜서는 안 된다. 여기서 예외를 올려야 체크포인트가 실패하고
            // 잡이 재시작되어 마지막 성공 지점부터 다시 처리한다.
            // 조용히 넘기면 데이터가 사라진 채로 잡은 건강해 보인다 - 최악의 형태다.
            throw new IOException(table + " 적재 3회 실패", last);
        }
    }
}
