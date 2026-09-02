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
        String startOffsets,
        String candlesTable,
        Duration partitionDiscovery,
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
                // 기본은 커밋된 오프셋에서 이어 읽기. earliest 로 두면 재배포할 때마다
                // 토픽 전체를 재처리하고, 싱크가 멱등하지 않으므로 같은 윈도가 다시 쓰인다
                // (실제로 재배포 한 번에 420개 윈도가 중복됐다).
                // 재처리·리플레이 실험이 필요하면 --start-offsets earliest 로 명시한다.
                p.get("start-offsets", "committed"),
                // 설정 비교 실험은 별도 테이블에 쓴다. 같은 테이블에 쓰면 서로 다른
                // 조건의 결과가 섞이고, write_count 가 중복 신호로 오해된다.
                p.get("candles-table", "rtp.candles_1m"),

                // 파티션 재발견 주기.
                //
                // **Flink 의 기본값은 꺼짐이다.** 잡이 시작할 때 한 번만 파티션을
                // 조회하고 그 뒤로는 다시 보지 않는다. 로그에 이렇게 찍힌다.
                //
                //   Starting the KafkaSourceEnumerator ... without periodic partition discovery.
                //
                // 그 상태에서 토픽 파티션을 늘리면 새 파티션은 영원히 읽히지 않는다.
                // 실측했다 - 3->7 로 늘렸더니 새 파티션 두 개에 1만 건이 쌓이는 동안
                // 잡은 RUNNING 이었고 컨슈머 lag 은 0이었다.
                // 0 이하로 주면 끄는 것이고, 그때의 거동을 보려면 실험에서 0을 준다.
                Duration.ofSeconds(p.getLong("partition-discovery-seconds", 30)),

                p.get("run-id", "run-" + System.currentTimeMillis()));
    }

    private static String envOr(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }

    /** 실행 조건을 로그와 runId 에 남긴다. 나중에 결과를 설정과 대조하기 위한 것. */
    public String describe() {
        return ("window=%ds watermarkDelay=%ds idleness=%ds allowedLateness=%ds "
                + "topic=%s offsets=%s table=%s partitionDiscovery=%ds runId=%s").formatted(
                windowSize.toSeconds(), watermarkDelay.toSeconds(),
                idleness.toSeconds(), allowedLateness.toSeconds(),
                topic, startOffsets, candlesTable,
                partitionDiscovery.toSeconds(), runId);
    }
}
