package dev.rtp.ingester;

import java.util.Arrays;
import java.util.List;

/** 수집기 설정. 값은 환경변수(.env)에서 온다 - 커밋되지 않는다. */
public record Config(
        String clientId,
        String clientSecret,
        String apiBase,
        String wsUrl,
        List<String> symbols,
        String bootstrapServers,
        String topic,
        long pingIntervalMs,
        long recvTimeoutMs,
        long tokenRefreshMarginMs,
        int metricsPort) {

    public static Config fromEnv() {
        String id = env("TOSS_CLIENT_ID", "");
        String secret = env("TOSS_CLIENT_SECRET", "");
        if (id.isBlank() || secret.isBlank()) {
            throw new IllegalStateException(
                    "TOSS_CLIENT_ID / TOSS_CLIENT_SECRET 가 없습니다. .env.example 을 .env 로 복사해 채우세요.");
        }
        return new Config(
                id,
                secret,
                env("TOSS_API_BASE", "https://openapi.tossinvest.com"),
                env("TOSS_WS_URL", "wss://openapi-ws.tossinvest.com/ws/v1"),
                // 범위를 넓히지 않는다 - 종목은 소수만.
                symbols(env("SYMBOLS", "005930,000660")),
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env("KAFKA_TRADES_TOPIC", "trades.raw"),
                // 문서 기준: 60초마다 PING, 180초 무활동 시 서버가 끊는다.
                60_000L,
                // 그 절반에서 수신이 멎으면 죽은 연결로 보고 우리가 먼저 끊는다.
                // 서버가 끊어주기를 기다리면 그만큼 유실 구간이 길어진다.
                90_000L,
                // 토큰 유효기간 24h. 여유를 두고 미리 갱신한다.
                3_600_000L,
                Integer.parseInt(env("METRICS_PORT", "9200")));
    }

    static List<String> symbols(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    /** 로그용. 자격 증명은 절대 남기지 않는다. */
    public String redacted() {
        String idHint = clientId.length() > 4 ? clientId.substring(0, 4) + "…" : "…";
        return "Config[wsUrl=%s, symbols=%s, bootstrap=%s, topic=%s, clientId=%s]"
                .formatted(wsUrl, symbols, bootstrapServers, topic, idHint);
    }
}
