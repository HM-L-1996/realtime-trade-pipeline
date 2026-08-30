package dev.rtp.ingester;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TradeFrameParserTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static JsonNode frame(String topic, String timestamp, String price, String volume) {
        try {
            return M.readTree("""
                    {"type":"message","topic":"%s",
                     "data":{"price":"%s","volume":"%s","timestamp":"%s","currency":"KRW"}}
                    """.formatted(topic, price, volume, timestamp));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static JsonNode standard() {
        return frame("trade:kr:005930", "2026-06-18T09:30:00.123+09:00", "71800", "12");
    }

    @Test
    @DisplayName("체결 프레임을 파싱한다")
    void 파싱() {
        var t = TradeFrameParser.toTrade(standard()).orElseThrow();
        assertEquals("005930", t.symbol());
        assertEquals("KRW", t.currency());
    }

    @Test
    @DisplayName("KST 를 UTC 밀리초로 정규화한다")
    void 시간대_정규화() {
        var t = TradeFrameParser.toTrade(standard()).orElseThrow();
        long expect = OffsetDateTime.parse("2026-06-18T00:30:00.123Z").toInstant().toEpochMilli();
        assertEquals(expect, t.eventMs());
    }

    @Test
    @DisplayName("가격과 수량을 문자열로 보존한다 - double 로 바꾸면 부동소수 오차가 검증 오차로 둔갑한다")
    void 문자열_보존() {
        var t = TradeFrameParser.toTrade(
                frame("trade:kr:005930", "2026-06-18T09:30:00.123+09:00", "0.1", "0.3")).orElseThrow();
        assertEquals("0.1", t.price());
        assertEquals("0.3", t.volume());
    }

    @Test
    @DisplayName("timezone 없는 timestamp 는 버린다 - KST 로 임의 가정하면 9시간 틀어진다")
    void 시간대_없음() {
        assertTrue(TradeFrameParser.toTrade(
                frame("trade:kr:005930", "2026-06-18T09:30:00.123", "1", "1")).isEmpty());
    }

    @Test
    @DisplayName("잘못된 timestamp 는 버린다")
    void 잘못된_시각() {
        assertTrue(TradeFrameParser.toTrade(
                frame("trade:kr:005930", "not-a-time", "1", "1")).isEmpty());
        assertTrue(TradeFrameParser.toTrade(
                frame("trade:kr:005930", "", "1", "1")).isEmpty());
    }

    @Test
    @DisplayName("체결이 아닌 토픽은 무시한다")
    void 다른_토픽() {
        assertTrue(TradeFrameParser.toTrade(
                frame("orderbook:kr:005930", "2026-06-18T09:30:00.123+09:00", "1", "1")).isEmpty());
    }

    @Test
    @DisplayName("미국 체결도 파싱된다")
    void 미국_체결() {
        var t = TradeFrameParser.toTrade(
                frame("trade:us:AAPL", "2026-06-18T09:30:00.123+09:00", "243.26", "8")).orElseThrow();
        assertEquals("AAPL", t.symbol());
    }

    @Test
    @DisplayName("PONG 과 빈 문자열은 프레임이 아니다")
    void 비프레임() {
        assertTrue(TradeFrameParser.parseFrame("PONG").isEmpty());
        assertTrue(TradeFrameParser.parseFrame("").isEmpty());
        assertTrue(TradeFrameParser.parseFrame(null).isEmpty());
    }

    @Test
    @DisplayName("깨진 JSON 은 예외 없이 버린다 - 프레임 하나 때문에 연결이 죽으면 안 된다")
    void 깨진_JSON() {
        Optional<JsonNode> r = TradeFrameParser.parseFrame("{not json");
        assertTrue(r.isEmpty());
    }
}
