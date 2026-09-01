package dev.rtp.ingester;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 토스 WebSocket 체결 스트림 한 연결.
 *
 * <p>문서에서 확인한 제약이 그대로 구현 제약이 된다.
 * <ul>
 *   <li>구독은 <b>선언형 전체 교체</b>다. 배열 하나가 현재 구독 집합 전체이고 빠진 항목은
 *       자동 해제된다. 증분 추가로 착각하면 조용히 구독이 날아간다.
 *   <li>keepalive 는 JSON 이 아니라 <b>순수 텍스트 PING</b> 이다.
 *   <li>스트림이 lossy 하고 시퀀스 번호가 없다. 유실을 스트림만으로 탐지할 수 없어
 *       연결별 recvSeq 를 우리가 매긴다.
 * </ul>
 *
 * <p>JDK WebSocket 은 텍스트 메시지를 여러 조각으로 나눠 줄 수 있다(last 플래그).
 * 조각을 모아서 완성된 메시지만 큐에 넣는다 - 이걸 빠뜨리면 긴 프레임에서만
 * 간헐적으로 JSON 파싱이 깨진다.
 */
public final class TossTradeStream implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TossTradeStream.class);
    private static final String PING_TEXT = "PING";

    /** 수신 루프를 깨우기 위한 종료 신호. 실제 프레임과 겹치지 않는 값이면 된다. */
    private static final String SENTINEL = "__RTP_STREAM_CLOSED__";

    private final Config cfg;
    private final String connId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    private final BlockingQueue<String> inbox = new LinkedBlockingQueue<>();
    private final AtomicReference<String> closeReason = new AtomicReference<>();
    private final StringBuilder partial = new StringBuilder();

    private volatile long lastRecvAtMs = System.currentTimeMillis();
    private WebSocket ws;

    public TossTradeStream(Config cfg) {
        this.cfg = cfg;
    }

    public String connId() {
        return connId;
    }

    /** 예외 체인을 한 줄로 요약한다. 토큰 값이 섞이지 않도록 메시지만 쓴다. */
    private static String describeCause(Throwable e) {
        StringBuilder sb = new StringBuilder();
        for (Throwable t = e; t != null && sb.length() < 300; t = t.getCause()) {
            if (sb.length() > 0) {
                sb.append(" <- ");
            }
            sb.append(t.getClass().getSimpleName());
            if (t.getMessage() != null) {
                sb.append('(').append(t.getMessage()).append(')');
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return sb.toString();
    }

    public void connect(HttpClient http, String token) throws Exception {
        try {
            ws = http.newWebSocketBuilder()
                    .header("Authorization", "Bearer " + token)
                    .connectTimeout(Duration.ofSeconds(15))
                    .buildAsync(URI.create(cfg.wsUrl()), new Listener())
                    .get(20, TimeUnit.SECONDS);
        } catch (Exception e) {
            // 핸드셰이크 실패의 원인을 구분할 수 있게 남긴다.
            //
            // 401 을 만나면 우리는 토큰을 재발급해 재시도하는데, 원인이
            // **동시 연결 수 제한**이면 토큰을 다시 받아도 소용이 없고
            // 지수 백오프만 늘어나며 조용히 계속 실패한다.
            // 실제로 한 번 당했고, 그때 응답 본문이 없어서 두 원인을 구분하지 못했다.
            //
            // JDK WebSocket 은 실패 응답 본문을 노출하지 않는다. 그래서 최소한
            // 상태 코드와 원인 체인이라도 한 줄로 남긴다.
            log.error("WebSocket 핸드셰이크 실패 url={} 원인={}",
                    cfg.wsUrl(), describeCause(e));
            throw e;
        }

        ws.sendText(subscriptionPayload(), true);
        log.info("구독 선언 전송 connId={} symbols={}", connId, cfg.symbols());
    }

    /** 전체 교체 선언. 종목을 추가할 때도 기존 목록을 전부 다시 보내야 한다. */
    String subscriptionPayload() {
        StringBuilder codes = new StringBuilder();
        for (int i = 0; i < cfg.symbols().size(); i++) {
            if (i > 0) {
                codes.append(',');
            }
            codes.append('"').append(cfg.symbols().get(i)).append('"');
        }
        return "[{\"id\":\"sub-" + UUID.randomUUID().toString().substring(0, 8) + "\"},"
                + "{\"type\":\"trade:kr\",\"codes\":[" + codes + "]}]";
    }

    /**
     * 다음 프레임을 최대 {@code waitMs} 만큼 기다린다.
     *
     * <p>대기 시간을 호출자가 정하는 이유가 있다. 수신 타임아웃(90초)만큼 블로킹하면
     * 그 사이에 PING 주기(60초)가 지나가 버려, <b>한산한 구간에서 PING 을 한 번도
     * 못 보내고 재연결만 반복</b>하게 된다. 실제 서버에 붙여보고 발견한 버그다.
     * 호출자가 짧게 끊어 기다리면서 PING 과 유휴 판정을 스스로 관리한다.
     *
     * @return 프레임. 대기 시간 안에 아무것도 안 왔거나 파싱할 수 없으면 null.
     */
    public JsonNode poll(long waitMs) throws InterruptedException {
        String raw = inbox.poll(waitMs, TimeUnit.MILLISECONDS);
        if (raw == null || SENTINEL.equals(raw)) {
            return null;
        }
        lastRecvAtMs = System.currentTimeMillis();
        return TradeFrameParser.parseFrame(raw).orElse(null);
    }

    /** 마지막으로 무언가 받은 시각. 유휴 판정의 기준이다. */
    public long lastRecvAtMs() {
        return lastRecvAtMs;
    }

    /**
     * 서버는 180초 무활동에 끊는다. 그때까지 기다리면 유실 구간이 길어지므로
     * 우리가 먼저 끊고 재연결한다.
     */
    public void markIdleTimeout() {
        closeReason.compareAndSet(null, "recv-timeout");
    }

    public void ping() {
        WebSocket w = ws;
        if (w != null && !w.isOutputClosed()) {
            w.sendText(PING_TEXT, true);
        }
    }

    public String closeReason() {
        String r = closeReason.get();
        return r == null ? "unknown" : r;
    }

    public boolean closed() {
        return closeReason.get() != null;
    }

    @Override
    public void close() {
        WebSocket w = ws;
        if (w != null && !w.isOutputClosed()) {
            w.sendClose(WebSocket.NORMAL_CLOSURE, "bye");
        }
    }

    private final class Listener implements WebSocket.Listener {
        @Override
        public void onOpen(WebSocket webSocket) {
            log.info("WebSocket 연결됨 connId={}", connId);
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                inbox.offer(partial.toString());
                partial.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.warn("WebSocket 종료 code={} reason={}", statusCode, reason);
            closeReason.compareAndSet(null, "closed-" + statusCode);
            inbox.offer(SENTINEL);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            log.error("WebSocket 오류", error);
            closeReason.compareAndSet(null, "error-" + error.getClass().getSimpleName());
            inbox.offer(SENTINEL);
        }
    }
}
