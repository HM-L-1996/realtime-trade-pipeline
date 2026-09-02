package dev.rtp.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TradeRecordTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static TradeRecord sample() {
        return new TradeRecord("005930", 1000L, 0, 1005L, "85000", "10", "KRW", "c1", 7L);
    }

    @Test
    @DisplayName("Kafka 좌표는 페이로드에 실리지 않는다 - 생산자가 쓸 수 있는 값이 아니다")
    void 좌표는_전송되지_않는다() throws Exception {
        String json = MAPPER.writeValueAsString(sample().withKafkaCoords(2, 12345L));
        assertFalse(json.contains("kafkaPartition"), json);
        assertFalse(json.contains("kafka_partition"), json);
        assertFalse(json.contains("kafkaOffset"), json);
    }

    @Test
    @DisplayName("좌표를 붙이기 전에는 -1 이다 - 0 을 쓰면 '0번 파티션' 과 구별되지 않는다")
    void 미상은_음수() {
        assertEquals(TradeRecord.UNKNOWN_PARTITION, sample().kafkaPartition());
        assertEquals(TradeRecord.UNKNOWN_OFFSET, sample().kafkaOffset());
    }

    @Test
    @DisplayName("역직렬화 뒤 좌표를 붙이면 나머지 필드는 그대로다")
    void 좌표_부착() throws Exception {
        String json = MAPPER.writeValueAsString(sample());
        TradeRecord back = MAPPER.readValue(json, TradeRecord.class);
        TradeRecord withCoords = back.withKafkaCoords(2, 99L);

        assertEquals(2, withCoords.kafkaPartition());
        assertEquals(99L, withCoords.kafkaOffset());
        assertEquals("005930", withCoords.symbol());
        assertEquals(1000L, withCoords.eventMs());
        assertEquals("85000", withCoords.price());
        assertEquals("c1", withCoords.connId());
        assertEquals(7L, withCoords.recvSeq());
    }
}
