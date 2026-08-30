package dev.rtp.aggregator;

import dev.rtp.model.Candle;
import dev.rtp.model.Decimals;
import dev.rtp.model.TradeRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CandleAggregateTest {

    private static final String RUN = "test-run";

    private static TradeRecord trade(long eventMs, int seq, String price, String volume) {
        return new TradeRecord("005930", eventMs, seq, eventMs, price, volume,
                "KRW", "conn", 1L);
    }

    private static Candle fold(TradeRecord... trades) {
        CandleAggregate agg = new CandleAggregate(RUN);
        CandleAccumulator acc = agg.createAccumulator();
        for (TradeRecord t : trades) {
            acc = agg.add(t, acc);
        }
        return agg.getResult(acc);
    }

    @Test
    @DisplayName("기본 OHLCV 를 계산한다")
    void 기본_집계() {
        Candle c = fold(
                trade(1000, 0, "100", "1"),
                trade(2000, 0, "120", "2"),
                trade(3000, 0, "90", "3"),
                trade(4000, 0, "110", "4"));

        assertEquals("100", Decimals.priceToString(c.open).replaceAll("\\.0+$", ""));
        assertEquals("120", Decimals.priceToString(c.high).replaceAll("\\.0+$", ""));
        assertEquals("90", Decimals.priceToString(c.low).replaceAll("\\.0+$", ""));
        assertEquals("110", Decimals.priceToString(c.close).replaceAll("\\.0+$", ""));
        assertEquals("10", Decimals.volumeToString(c.volume).replaceAll("\\.0+$", ""));
        assertEquals(4, c.tradeCount);
    }

    @Test
    @DisplayName("시가·종가는 도착 순서가 아니라 이벤트타임으로 정해진다")
    void 지연_도착시_시가종가() {
        // 도착 순서: 2000 -> 3000 -> 1000(지연) -> 4000
        // 도착 순서로 잡으면 open=2000의 가격이 되어 틀린다.
        Candle c = fold(
                trade(2000, 0, "120", "1"),
                trade(3000, 0, "90", "1"),
                trade(1000, 0, "100", "1"),   // 늦게 도착한 진짜 첫 체결
                trade(4000, 0, "110", "1"));

        assertEquals("100", Decimals.priceToString(c.open).replaceAll("\\.0+$", ""),
                "가장 이른 이벤트타임의 가격이 시가여야 한다");
        assertEquals("110", Decimals.priceToString(c.close).replaceAll("\\.0+$", ""),
                "가장 늦은 이벤트타임의 가격이 종가여야 한다");
    }

    @Test
    @DisplayName("같은 밀리초는 seqInMs 로 시가·종가를 가른다")
    void 동일_밀리초_동률() {
        // 체결ID가 없는 소스라 seqInMs 가 유일한 2차 기준이다.
        Candle c = fold(
                trade(1000, 2, "102", "1"),
                trade(1000, 0, "100", "1"),
                trade(1000, 1, "101", "1"));

        assertEquals("100", Decimals.priceToString(c.open).replaceAll("\\.0+$", ""));
        assertEquals("102", Decimals.priceToString(c.close).replaceAll("\\.0+$", ""));
    }

    @Test
    @DisplayName("체결 하나면 OHLC 가 모두 같다")
    void 단일_체결() {
        Candle c = fold(trade(1000, 0, "71800", "5"));
        assertEquals(c.open, c.high);
        assertEquals(c.high, c.low);
        assertEquals(c.low, c.close);
        assertEquals(1, c.tradeCount);
    }

    @Test
    @DisplayName("소수 가격이 정확히 보존된다 - double 이면 여기서 깨진다")
    void 소수_정밀도() {
        Candle c = fold(
                trade(1000, 0, "0.1", "0.1"),
                trade(2000, 0, "0.2", "0.2"));
        // 0.1 + 0.2 를 double 로 하면 0.30000000000000004 가 된다.
        assertEquals("0.30000000", Decimals.volumeToString(c.volume));
    }

    @Test
    @DisplayName("스케일을 넘는 자리는 버림한다 - 올려 잡으면 거래대금이 부풀어 검증이 어긋난다")
    void 스케일_버림() {
        Candle c = fold(trade(1000, 0, "100.999999", "1"));
        assertEquals("100.9999", Decimals.priceToString(c.open));
    }

    @Test
    @DisplayName("빈 누산기는 null 을 낸다")
    void 빈_누산기() {
        CandleAggregate agg = new CandleAggregate(RUN);
        assertEquals(null, agg.getResult(agg.createAccumulator()));
    }

    @Test
    @DisplayName("merge 는 이벤트타임 기준을 유지한다")
    void 병합() {
        CandleAggregate agg = new CandleAggregate(RUN);
        CandleAccumulator a = agg.createAccumulator();
        CandleAccumulator b = agg.createAccumulator();

        agg.add(trade(3000, 0, "130", "1"), a);
        agg.add(trade(4000, 0, "140", "1"), a);
        agg.add(trade(1000, 0, "110", "1"), b);
        agg.add(trade(2000, 0, "120", "1"), b);

        Candle c = agg.getResult(agg.merge(a, b));
        assertEquals("110", Decimals.priceToString(c.open).replaceAll("\\.0+$", ""));
        assertEquals("140", Decimals.priceToString(c.close).replaceAll("\\.0+$", ""));
        assertEquals(4, c.tradeCount);
    }
}
