package dev.rtp.ingester;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/**
 * 체결 프레임 파싱. 부수효과가 없어 단위 테스트로 고정할 수 있다.
 *
 * <p>프레임 형식:
 * <pre>
 * {"type":"message","topic":"trade:kr:005930",
 *  "data":{"price":"71800","volume":"12",
 *          "timestamp":"2026-06-18T09:30:00.123+09:00","currency":"KRW"}}
 * </pre>
 */
public final class TradeFrameParser {

    private static final Logger log = LoggerFactory.getLogger(TradeFrameParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 소스에서 온 것만 담는다. 나머지 필드는 수집기가 붙인다. */
    public record ParsedTrade(String symbol, long eventMs, String price,
                              String volume, String currency) {}

    private TradeFrameParser() {}

    public static Optional<JsonNode> parseFrame(String raw) {
        if (raw == null) {
            return Optional.empty();
        }
        String s = raw.strip();
        // keepalive 응답이 순수 텍스트로 오는 경우가 있다.
        if (s.isEmpty() || "PONG".equals(s)) {
            return Optional.empty();
        }
        try {
            JsonNode n = MAPPER.readTree(s);
            return n.isObject() ? Optional.of(n) : Optional.empty();
        } catch (Exception e) {
            log.warn("JSON 파싱 실패: {}", s.length() > 120 ? s.substring(0, 120) : s);
            return Optional.empty();
        }
    }

    /** {@code type=message} 프레임에서 체결을 뽑는다. 체결이 아니면 비어 있다. */
    public static Optional<ParsedTrade> toTrade(JsonNode frame) {
        JsonNode data = frame.get("data");
        if (data == null || !data.isObject()) {
            return Optional.empty();
        }

        // topic 형식: "trade:kr:005930"
        String topic = frame.path("topic").asText("");
        String[] parts = topic.split(":");
        if (parts.length != 3 || !"trade".equals(parts[0])) {
            return Optional.empty();
        }
        String symbol = parts[2];

        String ts = data.path("timestamp").asText("");
        if (ts.isBlank()) {
            return Optional.empty();
        }

        long eventMs;
        try {
            // OffsetDateTime 은 오프셋이 없으면 파싱 자체가 실패한다.
            // KST 로 임의 가정하면 이벤트타임이 9시간 틀어지므로 그 편이 낫다.
            eventMs = OffsetDateTime.parse(ts).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            log.warn("timestamp 파싱 실패: {}", ts);
            return Optional.empty();
        }

        // 가격·수량은 문자열 그대로 넘긴다. double 로 바꾸는 순간
        // 부동소수 오차가 그대로 검증 오차로 둔갑한다.
        String price = data.path("price").asText("");
        String volume = data.path("volume").asText("");

        // 빈 값을 그대로 통과시키면 안 된다. 하류의 CandleAggregate 가
        // Decimals.parse 를 호출하면서 NumberFormatException 을 던지고,
        // 그건 잡을 죽인 뒤 같은 레코드로 재시작 루프에 빠지는 경로다.
        // "깨진 레코드 하나가 잡 전체를 죽이면 안 된다" 는 원칙을 역직렬화
        // 단계에서만 지키고 여기서 놓치고 있었다.
        if (price.isBlank() || volume.isBlank()) {
            log.warn("가격/수량이 비어 있는 체결 프레임 버림: symbol={} price='{}' volume='{}'",
                    symbol, price, volume);
            return Optional.empty();
        }

        return Optional.of(new ParsedTrade(symbol, eventMs, price, volume,
                data.path("currency").asText("")));
    }
}
