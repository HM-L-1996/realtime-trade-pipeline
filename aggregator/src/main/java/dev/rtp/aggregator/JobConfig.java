package dev.rtp.aggregator;

import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;

/**
 * 잡 설정.
 *
 * <p>워터마크 지연, 유휴 타임아웃, 허용 지연을 전부 인자로 뺀 이유가 있다.
 * {@code validation.md} 의 "설정 변경에 따른 정확도 변화" 표가 이 값들을 바꿔가며
 * 나오는 것이고, 그 표가 이 프로젝트에서 가장 설명력 있는 결과물이다.
 * 코드를 고쳐야 실험이 되는 구조면 실험을 안 하게 된다.
 */
public record JobConfig(
        String bootstrapServers,
        String topic,
        String groupId,
        Duration windowSize,
        Duration watermarkDelay,
        Duration idleness,
        Duration allowedLateness,
        String clickhouseUrl,
        String clickhouseUser,
        String clickhousePassword,
        int sinkBatchSize,
        Duration sinkFlushInterval,
        boolean archiveTrades,
        String candlesTable,
        String runId) {

    public static JobConfig from(String[] args) {
        ParameterTool p = ParameterTool.fromArgs(args);
        return new JobConfig(
                p.get("bootstrap-servers", envOr("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")),
                p.get("topic", envOr("KAFKA_TRADES_TOPIC", "trades.raw")),
                p.get("group-id", "rtp-candle-1m"),
                Duration.ofSeconds(p.getLong("window-seconds", 60)),

                // 워터마크 지연: 늦게 잡으면 정확하지만 결과가 늦고,
                // 빨리 잡으면 늦게 온 체결이 버려진다. 이 트레이드오프가 실험 대상이다.
                Duration.ofSeconds(p.getLong("watermark-delay-seconds", 5)),

                // 유휴 타임아웃: 종목 수보다 파티션이 많으면 빈 파티션이 생기고,
                // 워터마크는 모든 파티션의 최소값을 따라가므로 전진하지 않는다.
                // 0 이면 끄고 그 상황을 그대로 관측한다.
                Duration.ofSeconds(p.getLong("idleness-seconds", 10)),

                // 허용 지연: 윈도가 닫힌 뒤에도 이만큼은 늦은 체결을 받아 재발행한다.
                // 재발행은 곧 같은 윈도의 두 번째 기록이므로 write_count 가 늘어난다.
                Duration.ofSeconds(p.getLong("allowed-lateness-seconds", 0)),

                // JDBC 가 아니라 HTTP 인터페이스 주소다. ClickHouseCandleSink 참고.
                p.get("clickhouse-url", envOr("CLICKHOUSE_URL", "http://clickhouse:8123")),
                p.get("clickhouse-user", envOr("CLICKHOUSE_USER", "rtp")),
                p.get("clickhouse-password", envOr("CLICKHOUSE_PASSWORD", "rtp")),
                p.getInt("sink-batch-size", 500),
                Duration.ofSeconds(p.getLong("sink-flush-seconds", 5)),
                // 원본 체결 보관. 적재량이 캔들의 수백 배라 끌 수 있게 둔다.
                p.getBoolean("archive-trades", true),
                // 설정 비교 실험은 별도 테이블에 쓴다. 같은 테이블에 쓰면 서로 다른
                // 조건의 결과가 섞이고, write_count 가 중복 신호로 오해된다.
                p.get("candles-table", "rtp.candles_1m"),
                p.get("run-id", "run-" + System.currentTimeMillis()));
    }

    private static String envOr(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }

    /** 실행 조건을 로그와 runId 에 남긴다. 나중에 결과를 설정과 대조하기 위한 것. */
    public String describe() {
        return ("window=%ds watermarkDelay=%ds idleness=%ds allowedLateness=%ds "
                + "topic=%s table=%s runId=%s").formatted(
                windowSize.toSeconds(), watermarkDelay.toSeconds(),
                idleness.toSeconds(), allowedLateness.toSeconds(),
                topic, candlesTable, runId);
    }
}
