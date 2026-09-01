package dev.rtp.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Kafka 토픽 {@code trades.raw} 에 흐르는 체결 레코드.
 *
 * <p>토스 WebSocket 체결 프레임에는 체결 ID 도 시퀀스 번호도 없고, 문서가 스스로 lossy 하다고
 * 명시한다. 매수/매도 구분과 누적 거래량도 없다. 그래서 소스에서 오는 것은
 * {@code (symbol, timestamp, price, volume)} 뿐이고 이 조합은 유일하지 않다 -
 * 같은 밀리초에 같은 가격·수량의 체결이 실제로 발생한다.
 *
 * <p>나머지 필드는 전부 수집기가 붙인 것이고, 각각 검증에 쓰이는 이유가 있다.
 * <ul>
 *   <li>{@code seqInMs} - 멱등키의 마지막 조각. 이게 없으면 중복 제거가 불가능하다.
 *   <li>{@code recvSeq} - 연결별 프레임 일련번호. 소스 유실률을 재기 위한 것.
 *   <li>{@code connId} - 재연결 구분. 끊긴 구간을 식별한다.
 *   <li>{@code ingestMs} - 수신 시각. 소스 지연을 재고, 이벤트타임과 분리해 둔다.
 * </ul>
 *
 * <p>{@code price}/{@code volume} 을 문자열로 두는 것은 의도적이다. 소스가 문자열로 주는데
 * double 로 받으면 부동소수 오차가 그대로 "검증 오차"로 둔갑한다. 숫자 변환은
 * {@link java.math.BigDecimal} 로 필요한 지점에서만 한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TradeRecord(
        @JsonProperty("symbol") String symbol,
        @JsonProperty("event_ms") long eventMs,
        @JsonProperty("seq_in_ms") int seqInMs,
        @JsonProperty("ingest_ms") long ingestMs,
        @JsonProperty("price") String price,
        @JsonProperty("volume") String volume,
        @JsonProperty("currency") String currency,
        @JsonProperty("conn_id") String connId,
        @JsonProperty("recv_seq") long recvSeq) {

    // 멱등키는 (symbol, eventMs, seqInMs) 다. 그 조합을 만드는 메서드를 여기 두었었는데
    // 어디에서도 호출되지 않는 죽은 코드였다. 실제 키는 ClickHouse 의
    // trades_raw ORDER BY 에 있고, 중복 판정은 SQL 로 한다(source_continuity 뷰).
    // 코드에 키를 만드는 시늉만 있고 쓰이지 않으면 "중복 제거가 되고 있다" 는
    // 잘못된 인상을 준다.
}
