package dev.rtp.aggregator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rtp.model.TradeRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
 * 세어 두고 넘긴다. 몇 건이 버려졌는지는 메트릭으로 남는다.
 */
public class TradeRecordDeserializer implements KafkaRecordDeserializationSchema<TradeRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(TradeRecordDeserializer.class);

    private transient ObjectMapper mapper;
    private transient long malformed;

    @Override
    public void open(DeserializationSchema.InitializationContext ctx) {
        mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
                return;
            }
            out.collect(t);
        } catch (Exception e) {
            if (++malformed <= 10 || malformed % 1000 == 0) {
                log.warn("역직렬화 실패 partition={} offset={} (누적 {}건)",
                        record.partition(), record.offset(), malformed);
            }
        }
    }

    @Override
    public TypeInformation<TradeRecord> getProducedType() {
        return TypeInformation.of(TradeRecord.class);
    }
}
