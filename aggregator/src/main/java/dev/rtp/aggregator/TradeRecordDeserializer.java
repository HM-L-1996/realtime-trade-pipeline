package dev.rtp.aggregator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rtp.model.TradeRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka 레코드를 {@link TradeRecord} 로 푼다.
 *
 * <p><b>깨진 레코드 하나가 잡 전체를 죽이면 안 된다.</b> 스트리밍 잡은 장기 실행
 * 프로세스이고, 여기서 예외를 던지면 재시작 → 같은 레코드 → 재시작 루프에 빠진다.
 * 세어 두고 넘긴다.
 *
 * <p>거르는 것은 두 가지다.
 * <ul>
 *   <li>JSON 자체가 깨진 것 → {@code malformedJson}
 *   <li>JSON 은 멀쩡한데 가격/수량이 비어 있는 것 → {@code missingFields}.
 *       이걸 통과시키면 하류 {@code CandleAggregate} 가 숫자 파싱에서 터지고,
 *       그게 곧 재시작 루프다. <b>거르는 위치가 계측이 가능한 곳이어야 한다</b> -
 *       AggregateFunction 은 RuntimeContext 를 못 얻어 메트릭을 붙일 수 없다.
 * </ul>
 */
public class TradeRecordDeserializer implements KafkaRecordDeserializationSchema<TradeRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(TradeRecordDeserializer.class);

    private final String clickhouseUrl;
    private final String chUser;
    private final String chPassword;
    private final String jobRun;

    public TradeRecordDeserializer(JobConfig cfg) {
        this.clickhouseUrl = cfg.clickhouseUrl();
        this.chUser = cfg.clickhouseUser();
        this.chPassword = cfg.clickhousePassword();
        this.jobRun = cfg.runId();
    }

    private transient ObjectMapper mapper;
    private transient long loggedCount;
    private transient Counter malformedJson;
    private transient Counter missingFields;
    private transient Counter deadLetterFailures;
    private transient DeadLetter deadLetter;

    @Override
    public void open(DeserializationSchema.InitializationContext ctx) {
        mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 여기서는 메트릭을 붙일 수 있다. 이전 주석은 "메트릭으로 남는다" 고 했으면서
        // 실제로는 지역 변수만 증가시키고 있었다 - 조용히 사라지는 것을 계측한다는
        // 이 프로젝트 원칙을 스스로 어긴 자리였다.
        malformedJson = ctx.getMetricGroup().counter("malformedJsonRecords");
        missingFields = ctx.getMetricGroup().counter("missingFieldRecords");
        deadLetterFailures = ctx.getMetricGroup().counter("deadLetterFailures");
        deadLetter = new DeadLetter(clickhouseUrl, chUser, chPassword, jobRun);
        // 적재가 비동기라 실패는 이 스레드에서 알 수 없다. 게이지로 꺼내 본다 -
        // 그러지 않으면 "버렸다고 기록하는 것" 자체가 조용히 실패한다.
        ctx.getMetricGroup().gauge("deadLetterWriteFailures", () -> deadLetter.failedWrites());
        ctx.getMetricGroup().gauge("deadLetterQueueDropped", () -> deadLetter.droppedByBackpressure());
        ctx.getMetricGroup().gauge("deadLetterWritten", () -> deadLetter.writtenRows());
    }

    /**
     * 버린 레코드의 원본을 남긴다. 실패해도 파이프라인을 멈추지 않는다 -
     * dead letter 적재 실패로 본 파이프라인이 서면 본말이 전도된다.
     *
     * <p>{@code record()} 는 큐에 넣기만 하고 블로킹하지 않는다. 여기서 세는 것은
     * <b>큐가 가득 차 기록조차 못 한 건수</b>이고, 적재 실패는 게이지로 따로 본다.
     */
    private void toDeadLetter(String reason, String symbol, byte[] value) {
        String payload = value == null ? "" : new String(value, java.nio.charset.StandardCharsets.UTF_8);
        if (!deadLetter.record("deserialize", reason, symbol, payload)) {
            deadLetterFailures.inc();
        }
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TradeRecord> out) {
        if (record.value() == null) {
            return;
        }
        if (mapper == null) {
            mapper = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        try {
            TradeRecord t = mapper.readValue(record.value(), TradeRecord.class);
            if (t.symbol() == null || t.symbol().isBlank()) {
                missingFields.inc();
                toDeadLetter("missing-symbol", "", record.value());
                return;
            }
            // 가격/수량이 비어 있으면 하류 파싱에서 터진다. 계측 가능한 여기서 거른다.
            if (t.price() == null || t.price().isBlank()
                    || t.volume() == null || t.volume().isBlank()) {
                missingFields.inc();
                toDeadLetter("missing-field", t.symbol(), record.value());
                warnOnce("가격/수량 누락", record.partition(), record.offset());
                return;
            }
            out.collect(t);
        } catch (Exception e) {
            malformedJson.inc();
            toDeadLetter("malformed-json", "", record.value());
            warnOnce("역직렬화 실패", record.partition(), record.offset());
        }
    }

    /** 로그 폭주를 막는다. 처음 10건과 이후 1000건마다만 남긴다. */
    private void warnOnce(String what, int partition, long offset) {
        if (++loggedCount <= 10 || loggedCount % 1000 == 0) {
            log.warn("{} partition={} offset={} (누적 {}건)", what, partition, offset, loggedCount);
        }
    }

    @Override
    public TypeInformation<TradeRecord> getProducedType() {
        return TypeInformation.of(TradeRecord.class);
    }
}
