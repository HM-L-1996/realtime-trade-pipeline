package dev.rtp.aggregator;

import dev.rtp.model.Candle;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 누산 결과에 윈도 시작 시각을 붙인다.
 *
 * <p>{@link CandleAggregate} 는 윈도 경계를 모른다. 윈도 메타데이터는 여기서만 얻을 수 있다.
 * AggregateFunction 과 ProcessWindowFunction 을 같이 쓰면 <b>모든 원소를 상태에 쌓지 않고도</b>
 * 윈도 정보를 붙일 수 있다 - ProcessWindowFunction 만 쓰면 윈도 안의 체결이 전부
 * 상태에 남아 장 시간 내내 메모리가 늘어난다.
 */
public class CandleWindowFunction
        extends ProcessWindowFunction<Candle, Candle, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String symbol, Context ctx,
                        Iterable<Candle> aggregated, Collector<Candle> out) {
        for (Candle c : aggregated) {
            if (c == null) {
                continue;   // 빈 누산기
            }
            c.windowStartMs = ctx.window().getStart();
            out.collect(c);
        }
    }
}
