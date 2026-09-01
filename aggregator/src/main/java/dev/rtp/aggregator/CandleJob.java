package dev.rtp.aggregator;

import dev.rtp.model.Candle;
import dev.rtp.model.TradeRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 체결 스트림을 1분봉으로 접어 ClickHouse 에 적재한다.
 *
 * <pre>
 *   Kafka(trades.raw) -&gt; 워터마크 -&gt; keyBy(symbol) -&gt; 이벤트타임 텀블링 윈도 -&gt; ClickHouse
 * </pre>
 *
 * <p>이 잡의 목적은 "돌아가는 집계" 가 아니라 <b>설정을 바꿔가며 결과가 어떻게
 * 달라지는지 관측하는 것</b> 이다. 그래서 워터마크 지연·유휴 타임아웃·허용 지연이
 * 전부 인자로 나와 있다. {@link JobConfig} 참고.
 */
public final class CandleJob {

    private static final Logger log = LoggerFactory.getLogger(CandleJob.class);

    /**
     * 워터마크를 지나 도착해 윈도에 못 들어간 체결.
     *
     * <p>버리고 끝내면 "공식 캔들과 왜 다른지" 를 설명할 수 없다. 몇 건이 왜 버려졌는지
     * 세어 두어야 워터마크 설정과 정확도를 연결지을 수 있다.
     */
    public static final OutputTag<TradeRecord> LATE_TRADES =
            new OutputTag<>("late-trades", org.apache.flink.api.common.typeinfo.TypeInformation.of(TradeRecord.class));

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.from(args);
        log.info("CandleJob 시작 {}", cfg.describe());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 재시작 전략은 잡이 정하지 않는다. 클러스터 설정(FLINK_PROPERTIES)에 둔다 -
        // 이건 잡 로직이 아니라 운영 정책이고, 환경마다 달라야 한다.
        //
        // 여기 fixedDelayRestart(10, 5s) 를 박아 뒀다가 실제로 당했다:
        // TaskManager 를 재생성했더니 10회 x 5초 = 50초를 다 쓰고 잡이 FAILED 로 끝났다.
        // 컨테이너가 돌아오는 데 그보다 오래 걸렸기 때문이다.
        // K8s 에서 Pod 이 재스케줄되면 그대로 재현된다.

        KafkaSource<TradeRecord> source = KafkaSource.<TradeRecord>builder()
                .setBootstrapServers(cfg.bootstrapServers())
                .setTopics(cfg.topic())
                .setGroupId(cfg.groupId())
                .setStartingOffsets(startingOffsets(cfg))
                .setDeserializer(new TradeRecordDeserializer(cfg))
                .build();

        DataStream<TradeRecord> trades = env.fromSource(
                source, watermarks(cfg), "kafka-trades");

        SingleOutputStreamOperator<Candle> candles = trades
                .keyBy(TradeRecord::symbol)
                .window(TumblingEventTimeWindows.of(
                        Time.milliseconds(cfg.windowSize().toMillis())))
                .allowedLateness(Time.milliseconds(cfg.allowedLateness().toMillis()))
                .sideOutputLateData(LATE_TRADES)
                .aggregate(new CandleAggregate(cfg.runId()), new CandleWindowFunction())
                .name("candle-1m")
                .uid("candle-1m");   // savepoint 로 상태를 넘기려면 uid 가 고정돼야 한다

        // 버려진 늦은 체결을 세어 Prometheus 로 내보낸다. 값 자체는 버린다 -
        // 몇 건이 버려졌는지가 워터마크 설정과 정확도를 잇는 유일한 근거다.
        candles.getSideOutput(LATE_TRADES)
                .process(new LateTradeCounter(cfg))
                .name("late-trades")
                .uid("late-trades")
                .sinkTo(new DiscardingSink<>());

        candles.sinkTo(ClickHouseSink.candles(cfg))
                .name("clickhouse-candles")
                .uid("clickhouse-candles");

        // 원본 체결 보관. recv_seq 공백으로 소스 유실을 재기 위한 것이다.
        // 이게 없으면 공식 캔들과의 차이가 내 버그인지 소스 유실인지 끝까지 구분되지 않는다.
        // (--archive-trades false 로 끌 수 있다. 적재량이 캔들의 수백 배다.)
        if (cfg.archiveTrades()) {
            trades.sinkTo(ClickHouseSink.trades(cfg))
                    .name("clickhouse-trades")
                    .uid("clickhouse-trades");
        }

        env.execute("rtp-candle-1m");
    }

    /**
     * 워터마크 전략.
     *
     * <p>{@code withIdleness} 가 없으면 <b>비어 있는 파티션 하나가 전체 워터마크를
     * 멈춰 세운다.</b> 워터마크는 모든 소스 분할의 최소값을 따라가기 때문이다.
     * 종목 2개에 파티션 3개인 현재 구성에서는 파티션 하나가 항상 유휴이므로
     * 이걸 끄면 윈도가 영원히 닫히지 않는다. 그 상황 자체가 실험 대상이라
     * {@code --idleness-seconds 0} 으로 끌 수 있게 해 뒀다.
     */
    static WatermarkStrategy<TradeRecord> watermarks(JobConfig cfg) {
        WatermarkStrategy<TradeRecord> s = WatermarkStrategy
                .<TradeRecord>forBoundedOutOfOrderness(cfg.watermarkDelay())
                // 이벤트타임은 소스가 준 체결 시각이다. 수신 시각(ingestMs)이 아니다 -
                // 그걸 쓰면 지연 도착이 정상 데이터로 둔갑해 실험이 성립하지 않는다.
                .withTimestampAssigner((t, ts) -> t.eventMs());

        if (!cfg.idleness().isZero()) {
            s = s.withIdleness(cfg.idleness());
        }
        return s;
    }

    /**
     * 시작 오프셋 전략.
     *
     * <p>기본은 <b>커밋된 오프셋에서 이어 읽기</b>다. earliest 로 두면 재배포할 때마다
     * 토픽 전체를 재처리하는데, 이 파이프라인의 싱크는 멱등하지 않으므로 이미 적재된
     * 윈도가 그대로 한 번 더 쓰인다. 실제로 재배포 한 번에 420개 윈도가 중복됐다.
     *
     * <p>{@code committedOffsets(EARLIEST)} 는 커밋 오프셋이 없을 때(첫 배포)만
     * 처음부터 읽는다. 그래서 첫 실행과 재배포가 모두 의도대로 동작한다.
     *
     * <p>재처리·리플레이 실험에는 {@code --start-offsets earliest} 를 명시한다.
     * 감춰진 기본값이 아니라 <b>매번 의식적으로 켜는 스위치</b>여야 한다.
     */
    static OffsetsInitializer startingOffsets(JobConfig cfg) {
        return switch (cfg.startOffsets()) {
            case "earliest" -> OffsetsInitializer.earliest();
            case "latest" -> OffsetsInitializer.latest();
            case "committed" -> OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
            default -> throw new IllegalArgumentException(
                    "--start-offsets 는 committed|earliest|latest 중 하나여야 한다: "
                            + cfg.startOffsets());
        };
    }

    private CandleJob() {}
}
