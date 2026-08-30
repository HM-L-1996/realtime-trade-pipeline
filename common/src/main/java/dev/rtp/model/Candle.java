package dev.rtp.model;

import java.util.Objects;

/**
 * 1분봉 집계 결과.
 *
 * <p>Flink POJO 규약을 따른다 - public 필드, 인자 없는 생성자. record 로 쓰면
 * Flink 가 POJO 로 인식하지 못하고 Kryo 로 떨어지며, Kryo 상태는 스키마 진화가 안 된다.
 * savepoint 로 상태를 넘기는 실험을 해야 하므로 이 형태를 유지한다.
 *
 * <p>가격·수량은 스케일된 long 이다. 이유는 {@link Decimals} 참고.
 */
public class Candle {

    public String symbol;
    /** 윈도 시작 시각 (epoch ms, UTC). */
    public long windowStartMs;

    public long open;
    public long high;
    public long low;
    public long close;
    /** 거래량. 스케일 {@link Decimals#VOLUME_SCALE}. */
    public long volume;

    public long tradeCount;

    /**
     * 잡 실행 구분자. 같은 윈도가 여러 번 기록됐을 때 어느 실행이 쓴 것인지 알기 위한 것.
     * 재처리·복구 실험에서 중복의 출처를 추적한다.
     */
    public String runId = "";

    public Candle() {}

    public Candle(String symbol, long windowStartMs, long open, long high, long low,
                  long close, long volume, long tradeCount, String runId) {
        this.symbol = symbol;
        this.windowStartMs = windowStartMs;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.tradeCount = tradeCount;
        this.runId = runId;
    }

    @Override
    public String toString() {
        return "Candle[%s %d O=%s H=%s L=%s C=%s V=%s n=%d]".formatted(
                symbol, windowStartMs,
                Decimals.priceToString(open), Decimals.priceToString(high),
                Decimals.priceToString(low), Decimals.priceToString(close),
                Decimals.volumeToString(volume), tradeCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Candle c)) {
            return false;
        }
        return windowStartMs == c.windowStartMs && open == c.open && high == c.high
                && low == c.low && close == c.close && volume == c.volume
                && tradeCount == c.tradeCount && Objects.equals(symbol, c.symbol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, windowStartMs, open, high, low, close, volume, tradeCount);
    }
}
