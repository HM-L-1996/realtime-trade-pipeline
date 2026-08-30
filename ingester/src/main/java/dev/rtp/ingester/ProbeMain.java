package dev.rtp.ingester;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.Duration;

/**
 * 실제 토스 서버에 붙여 문서 기반 추측을 확인하는 진단 도구.
 *
 * <p>Kafka 없이 돈다. 확인하려는 것은 넷이다.
 * <ol>
 *   <li>OAuth2 client_credentials 토큰 발급이 되는가
 *   <li>WebSocket 핸드셰이크에 Bearer 헤더가 먹는가
 *   <li>구독 선언 형식이 맞는가 - {@code subscriptions} ack 의 실제 구조는?
 *   <li>keepalive 가 정말 순수 텍스트 PING 인가
 * </ol>
 *
 * <p>장이 닫혀 있으면 체결({@code type=message})은 오지 않는다. 그래도 1~4 는 확인된다.
 *
 * <p>토큰과 client_secret 은 어떤 경우에도 출력하지 않는다.
 *
 * <pre>
 *   java -cp ingester.jar dev.rtp.ingester.ProbeMain --seconds 30
 * </pre>
 */
public final class ProbeMain {

    private static final Logger log = LoggerFactory.getLogger(ProbeMain.class);

    public static void main(String[] args) throws Exception {
        var opts = SyntheticMain.parseArgs(args);
        long seconds = Long.parseLong(opts.getOrDefault("seconds", "30"));

        Config cfg = Config.fromEnv();
        log.info("프로브 시작 {}", cfg.redacted());

        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        // 1) 토큰
        TokenProvider tokens = new TokenProvider(cfg, http);
        String token;
        try {
            token = tokens.get();
        } catch (TokenProvider.TokenException e) {
            log.error("[1/4] 토큰 발급 실패: {}", e.getMessage());
            return;
        }
        log.info("[1/4] 토큰 발급 성공 (길이={}자, 값은 출력하지 않음)", token.length());

        // 2) 핸드셰이크 + 3) 구독
        try (TossTradeStream stream = new TossTradeStream(cfg)) {
            log.info("[2/4] WebSocket 접속 시도: {}", cfg.wsUrl());
            stream.connect(http, token);
            log.info("[2/4] 핸드셰이크 성공 connId={}", stream.connId());
            log.info("[3/4] 구독 선언 전송: {}", stream.subscriptionPayload());

            long start = System.currentTimeMillis();
            long deadline = start + seconds * 1000;
            long pingAt = start + 5_000;
            boolean pinged = false;
            int frames = 0;
            int trades = 0;

            while (System.currentTimeMillis() < deadline && !stream.closed()) {
                // 5초쯤 지나면 PING 을 한 번 보내 pong 이 오는지 본다.
                if (!pinged && System.currentTimeMillis() >= pingAt) {
                    log.info("[4/4] 텍스트 PING 전송");
                    stream.ping();
                    pinged = true;
                }

                JsonNode frame = stream.poll(1_000);
                if (frame == null) {
                    if (stream.closed()) {
                        break;
                    }
                    continue;
                }
                frames++;
                String type = frame.path("type").asText("unknown");

                // 프레임 원문을 그대로 남긴다. 문서와 다른 부분을 찾는 것이 목적이다.
                log.info("프레임 #{} type={} raw={}", frames, type, frame);

                if ("message".equals(type)) {
                    trades++;
                    TradeFrameParser.toTrade(frame).ifPresentOrElse(
                            t -> log.info("  -> 파싱됨 symbol={} eventMs={} price={} volume={}",
                                    t.symbol(), t.eventMs(), t.price(), t.volume()),
                            () -> log.warn("  -> 파싱 실패. 파서가 실제 형식과 맞지 않는다"));
                }
            }

            log.info("종료: 프레임 {}건 (체결 {}건), closeReason={}",
                    frames, trades, stream.closeReason());
            if (trades == 0) {
                log.info("체결이 0건인 것은 장 시간이 아니면 정상이다. 국내장은 평일 09:00-15:30.");
            }
        }
    }

    private ProbeMain() {}
}
