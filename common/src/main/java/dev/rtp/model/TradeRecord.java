package dev.rtp.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
        @JsonProperty("recv_seq") long recvSeq,

        // ── 아래 둘은 전송 경로의 좌표다. 소스가 준 것이 아니라 소비 시점에 붙는다. ──
        //
        // 페이로드에는 넣지 않는다(@JsonIgnore). 생산자가 쓸 수 있는 값이 아니고,
        // 넣으면 같은 체결이 토픽마다 다른 값을 갖게 되어 오해를 만든다.
        //
        // **이 둘을 채우게 된 계기가 있다.** trades_raw 에 kafka_partition /
        // kafka_offset 컬럼이 있었는데 싱크가 상수 0 을 넣고 있었다. 24만 행이
        // 전부 0/0 이었다. 파티션 배정을 확인하려고 그 컬럼을 읽었다가
        // "두 종목이 같은 파티션에 있다" 는 잘못된 결론을 낼 뻔했다 -
        // Kafka 에서 직접 확인하니 005930 은 partition 2 였다.
        // 채우지 않을 것이면 컬럼을 두지 말아야 하고, 둘 것이면 채워야 한다.
        @JsonIgnore int kafkaPartition,
        @JsonIgnore long kafkaOffset) {

    /** 아직 Kafka 를 거치지 않은 레코드. 생산 측(수집기·합성기·리플레이)이 쓴다. */
    public TradeRecord(String symbol, long eventMs, int seqInMs, long ingestMs,
                       String price, String volume, String currency,
                       String connId, long recvSeq) {
        this(symbol, eventMs, seqInMs, ingestMs, price, volume, currency,
                connId, recvSeq, UNKNOWN_PARTITION, UNKNOWN_OFFSET);
    }

    /** 아직 좌표를 모른다는 표시. 0 을 쓰면 "0번 파티션" 과 구별되지 않는다. */
    public static final int UNKNOWN_PARTITION = -1;
    public static final long UNKNOWN_OFFSET = -1L;

    /** 소비 시점에 Kafka 좌표를 붙인 사본을 돌려준다. */
    public TradeRecord withKafkaCoords(int partition, long offset) {
        return new TradeRecord(symbol, eventMs, seqInMs, ingestMs, price, volume,
                currency, connId, recvSeq, partition, offset);
    }

    // 멱등키는 (symbol, eventMs, seqInMs) 다. 그 조합을 만드는 메서드를 여기 두었었는데
    // 어디에서도 호출되지 않는 죽은 코드였다. 실제 키는 ClickHouse 의
    // trades_raw ORDER BY 에 있고, 중복 판정은 SQL 로 한다(source_continuity 뷰).
    // 코드에 키를 만드는 시늉만 있고 쓰이지 않으면 "중복 제거가 되고 있다" 는
    // 잘못된 인상을 준다.
}
