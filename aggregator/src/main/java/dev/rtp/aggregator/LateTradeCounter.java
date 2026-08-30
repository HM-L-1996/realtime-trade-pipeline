package dev.rtp.aggregator;

import dev.rtp.model.TradeRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 워터마크를 지나 도착해 버려진 체결을 센다.
 *
 * <p>이 수치가 없으면 "공식 캔들과 거래량이 다르다" 에서 멈춘다.
 * 몇 건이 버려졌는지를 알아야 워터마크 지연 설정과 정확도를 잇는 표
 * ({@code validation.md}) 를 쓸 수 있다.
 */
public class LateTradeCounter extends ProcessFunction<TradeRecord, TradeRecord> {

    private static final long serialVersionUID = 1L;

    private transient Counter dropped;

    @Override
    public void open(Configuration parameters) {
        dropped = getRuntimeContext().getMetricGroup().counter("lateTradesDropped");
    }

    @Override
    public void processElement(TradeRecord t, Context ctx, Collector<TradeRecord> out) {
        dropped.inc();
        out.collect(t);
    }
}
