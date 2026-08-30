package dev.rtp.ingester;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * 토스 OAuth2 (client_credentials) 토큰 관리.
 *
 * <p>WebSocket 은 핸드셰이크 시점에만 토큰을 검증한다. 연결 중 만료돼도 끊기지 않는다.
 * 문제는 재연결이다 - 끊긴 뒤 다시 붙을 때 토큰이 만료돼 있으면 그때 실패한다.
 * <b>재연결 경로의 토큰 갱신이 유일한 방어선이다.</b>
 */
public final class TokenProvider {

    private static final Logger log = LoggerFactory.getLogger(TokenProvider.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 자격 증명이 예외 메시지로 새지 않도록 별도 타입으로 감싼다. */
    public static final class TokenException extends RuntimeException {
        public TokenException(String message) {
            super(message);
        }
    }

    private final URI tokenUri;
    private final String clientId;
    private final String clientSecret;
    private final long refreshMarginMs;
    private final HttpClient http;

    private String token;
    private long expiresAtMs;

    public TokenProvider(Config cfg, HttpClient http) {
        String base = cfg.apiBase().endsWith("/")
                ? cfg.apiBase().substring(0, cfg.apiBase().length() - 1)
                : cfg.apiBase();
        this.tokenUri = URI.create(base + "/oauth2/token");
        this.clientId = cfg.clientId();
        this.clientSecret = cfg.clientSecret();
        this.refreshMarginMs = cfg.tokenRefreshMarginMs();
        this.http = http;
    }

    private boolean expired() {
        return token == null || System.currentTimeMillis() >= expiresAtMs - refreshMarginMs;
    }

    public String get() {
        return get(false);
    }

    public synchronized String get(boolean force) {
        if (force || expired()) {
            issue();
        }
        return token;
    }

    private void issue() {
        String body = "grant_type=client_credentials"
                + "&client_id=" + enc(clientId)
                + "&client_secret=" + enc(clientSecret);

        HttpRequest req = HttpRequest.newBuilder(tokenUri)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> res;
        try {
            res = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (Exception e) {
            // 요청 본문에 client_secret 이 들어 있으므로 원인 예외를 그대로 올리지 않는다.
            throw new TokenException("토큰 발급 요청 실패: " + e.getClass().getSimpleName());
        }

        if (res.statusCode() != 200) {
            throw new TokenException("토큰 발급 실패: HTTP " + res.statusCode());
        }

        JsonNode node;
        try {
            node = MAPPER.readTree(res.body());
        } catch (Exception e) {
            throw new TokenException("토큰 응답 파싱 실패");
        }

        JsonNode access = node.get("access_token");
        if (access == null || access.asText().isBlank()) {
            throw new TokenException("응답에 access_token 이 없습니다");
        }

        // 문서 기준 86400초. 없으면 보수적으로 1시간으로 본다.
        long ttlS = node.has("expires_in") ? node.get("expires_in").asLong(3600) : 3600;
        token = access.asText();
        expiresAtMs = System.currentTimeMillis() + ttlS * 1000;
        log.info("토큰 발급 완료 (ttl={}s, 갱신여유={}s)", ttlS, refreshMarginMs / 1000);
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
