package dev.rtp.aggregator;

import dev.rtp.model.Candle;
import dev.rtp.model.Decimals;
import dev.rtp.model.TradeRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 체결을 OHLCV 로 접는다.
 *
 * <p>순수 함수라 Flink 없이 단위 테스트로 고정할 수 있다. 이 프로젝트의 검증이
 * 의미를 가지려면 "집계 산식이 맞다" 는 것이 먼저 보장돼야 한다 - 그래야 공식 캔들과의
 * 차이를 워터마크나 유실 탓으로 해석할 수 있다.
 */
public class CandleAggregate
        implements AggregateFunction<TradeRecord, CandleAccumulator, Candle> {

    private final String runId;

    public CandleAggregate(String runId) {
        this.runId = runId;
    }

    @Override
    public CandleAccumulator createAccumulator() {
        return new CandleAccumulator();
    }

    @Override
    public CandleAccumulator add(TradeRecord t, CandleAccumulator acc) {
        long price = Decimals.parsePrice(t.price());
        long vol = Decimals.parseVolume(t.volume());

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
