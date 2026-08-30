package dev.rtp.ingester;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;

/**
 * 수집기 계측.
 *
 * <p>이 지표들의 존재 이유: 공식 캔들과 집계가 어긋났을 때 "내 파이프라인 버그"인지
 * "소스 프레임 유실"인지 구분하기 위해서다. 토스 WebSocket 은 시퀀스 번호가 없고
 * 문서가 스스로 lossy 하다고 명시한다. 구분하지 못하면 validation.md 의 수치가
 * 아무것도 증명하지 못한다.
 */
public final class Metrics {

    public static final Counter FRAMES = Counter.build()
            .name("rtp_ingester_frames_total")
            .help("수신한 WebSocket 프레임 수")
            .labelNames("frame_type")
            .register();

    public static final Counter TRADES = Counter.build()
            .name("rtp_ingester_trades_total")
            .help("Kafka 로 보낸 체결 수")
            .labelNames("symbol")
            .register();

    public static final Counter PRODUCE_ERRORS = Counter.build()
            .name("rtp_ingester_produce_errors_total")
            .help("Kafka 전송 실패 수")
            .register();

    public static final Counter RECONNECTS = Counter.build()
            .name("rtp_ingester_reconnects_total")
            .help("WebSocket 재연결 횟수")
            .labelNames("reason")
            .register();

    public static final Gauge CONNECTED = Gauge.build()
            .name("rtp_ingester_connected")
            .help("WebSocket 연결 상태 (1=연결됨)")
            .register();

    /**
     * 소스 지연. 이 값이 튀는 구간이 프레임 유실 구간과 겹치는지 보기 위한 것.
     */
    public static final Histogram SOURCE_LAG = Histogram.build()
            .name("rtp_ingester_source_lag_seconds")
            .help("체결 시각 대비 수신 지연")
            .buckets(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
            .register();

    /**
     * 역행한 타임스탬프. 소스가 순서를 보장하지 않는다는 증거가 되면
     * 워터마크 전략이 바뀐다.
     */
    public static final Counter OUT_OF_ORDER = Counter.build()
            .name("rtp_ingester_out_of_order_total")
            .help("직전 체결보다 이른 event_time 이 온 횟수")
            .labelNames("symbol")
            .register();

    private Metrics() {}

    public static HTTPServer serve(int port) throws IOException {
        return new HTTPServer.Builder().withPort(port).build();
    }
}
