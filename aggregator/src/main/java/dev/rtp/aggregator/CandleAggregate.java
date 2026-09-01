package dev.rtp.aggregator;

import dev.rtp.model.Candle;
import dev.rtp.model.Decimals;
import dev.rtp.model.TradeRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 체결을 OHLCV 로 접는다.
 *
 * <p>순수 함수라 Flink 없이 단위 테스트로 고정할 수 있다. 이 프로젝트의 검증이
 * 의미를 가지려면 "집계 산식이 맞다" 는 것이 먼저 보장돼야 한다 - 그래야 공식 캔들과의
 * 차이를 워터마크나 유실 탓으로 해석할 수 있다.
 */
public class CandleAggregate
        implements AggregateFunction<TradeRecord, CandleAccumulator, Candle> {

    private static final Logger log = LoggerFactory.getLogger(CandleAggregate.class);

    private final String runId;

    /**
     * 숫자 파싱 실패로 버린 체결 수.
     *
     * <p>AggregateFunction 은 RichFunction 이 아니라 RuntimeContext 를 못 얻는다.
     * Flink 메트릭에 붙일 수 없어 프로세스 전역 카운터로 둔다 -
     * 조용히 사라지는 것보다는 낫지만, 값을 밖에서 보려면
     * 이 함수를 ProcessWindowFunction 쪽으로 옮겨야 한다. 남은 숙제다.
     */
    static final java.util.concurrent.atomic.LongAdder malformed =
            new java.util.concurrent.atomic.LongAdder();

    private transient int logged;

    public CandleAggregate(String runId) {
        this.runId = runId;
    }

    @Override
    public CandleAccumulator createAccumulator() {
        return new CandleAccumulator();
    }

    @Override
    public CandleAccumulator add(TradeRecord t, CandleAccumulator acc) {
        long price;
        long vol;
        try {
            price = Decimals.parsePrice(t.price());
            vol = Decimals.parseVolume(t.volume());
        } catch (RuntimeException e) {
            // 여기서 예외를 올리면 태스크가 죽고, 재시작하면 같은 레코드를 다시 만나
            // 재시작 루프에 빠진다. 수집기가 이미 빈 값을 거르지만 토픽에 남아 있는
            // 과거 레코드나 다른 생산자(합성기·리플레이)가 넣은 것이 있을 수 있으므로
            // 이 단계에서도 막는다. 버린 건수는 메트릭으로 남긴다.
            malformed.increment();
            if (logged++ < 10) {
                log.warn("숫자 파싱 실패로 체결 버림: symbol={} price='{}' volume='{}'",
                        t.symbol(), t.price(), t.volume());
            }
            return acc;
        }

        if (acc.symbol == null) {
            acc.symbol = t.symbol();
        }
        // 도착 순서가 아니라 이벤트타임으로 시가·종가를 정한다.
        acc.offerOpen(t.eventMs(), t.seqInMs(), price);
        acc.offerClose(t.eventMs(), t.seqInMs(), price);

        acc.high = Math.max(acc.high, price);
        acc.low = Math.min(acc.low, price);
        acc.volume += vol;
        acc.tradeCount++;
        return acc;
    }

    @Override
    public Candle getResult(CandleAccumulator acc) {
        if (acc.isEmpty()) {
            return null;
        }
        return new Candle(acc.symbol, 0L, acc.open, acc.high, acc.low,
                acc.close, acc.volume, acc.tradeCount, runId);
    }

    /**
     * 텀블링 윈도에서는 호출되지 않지만 인터페이스 계약상 필요하다.
     * 세션 윈도로 바꿀 여지를 남겨 정확히 구현해 둔다.
     */
    @Override
    public CandleAccumulator merge(CandleAccumulator a, CandleAccumulator b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        a.offerOpen(b.openEventMs, b.openSeq, b.open);
        a.offerClose(b.closeEventMs, b.closeSeq, b.close);
        a.high = Math.max(a.high, b.high);
        a.low = Math.min(a.low, b.low);
        a.volume += b.volume;
        a.tradeCount += b.tradeCount;
        return a;
    }
}
