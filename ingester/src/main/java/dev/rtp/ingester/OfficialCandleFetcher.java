package dev.rtp.ingester;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 거래소 공식 분봉을 가져온다. <b>이 프로젝트의 정답지다.</b>
 *
 * <pre>
 *   GET /api/v1/candles?symbol=005930&amp;interval=1m&amp;count=200
 * </pre>
 *
 * <p>count 최대 200. 페이지네이션은 {@code before} 로 하며 응답의 {@code nextBefore} 를 잇는다.
 *
 * <p>rate limit 수치가 문서에 명시돼 있지 않고 응답 헤더로 확인하라고만 돼 있다.
 * 캔들은 {@code MARKET_DATA_CHART} 라는 별도 한도 그룹이다. 그래서 헤더를 읽어
 * 적응적으로 도는 구조로 짰다 - 고정 sleep 은 너무 느리거나 429 를 맞거나 둘 중 하나가 된다.
 */
public final class OfficialCandleFetcher {

    private static final Logger log = LoggerFactory.getLogger(OfficialCandleFetcher.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 공식 캔들 한 봉. 가격은 문자열 그대로 보존한다 - 정답지에 부동소수 오차를 섞을 수 없다. */
    public record OfficialCandle(String symbol, long windowStartMs, String open, String high,
                                 String low, String close, String volume) {}

    /** 한 번의 조회 결과. {@code nextBefore} 가 있으면 더 과거로 이어갈 수 있다. */
    public record Page(List<OfficialCandle> candles, String nextBefore) {}

    private final Config cfg;
    private final HttpClient http;
    private final TokenProvider tokens;

    public OfficialCandleFetcher(Config cfg, HttpClient http, TokenProvider tokens) {
        this.cfg = cfg;
        this.http = http;
        this.tokens = tokens;
    }

    /**
     * 분봉을 최대 {@code count} 개 가져온다.
     *
     * @param before null 이면 최신부터. 아니면 그 시각 이전 구간
     */
    public Page fetch(String symbol, int count, String before) throws Exception {
        StringBuilder url = new StringBuilder(cfg.apiBase())
                .append("/api/v1/candles?symbol=").append(symbol)
                .append("&interval=1m&count=").append(Math.min(count, 200));
        if (before != null && !before.isBlank()) {
            url.append("&before=").append(java.net.URLEncoder.encode(before, StandardCharsets.UTF_8));
        }

        HttpResponse<String> res = send(url.toString());
        JsonNode body = MAPPER.readTree(res.body());

        // 실제 응답은 result 로 한 번 감싸여 있다(2026-08-31 확인):
        //   {"result":{"candles":[...],"nextBefore":"..."}}
        // 문서 요약에는 없던 구조라 후보를 넓게 잡아 두었다.
        JsonNode arr = body.isArray() ? body
                : firstArray(body, "result", "candles", "data", "items");

        List<OfficialCandle> out = new ArrayList<>();
        if (arr != null) {
            for (JsonNode n : arr) {
                OfficialCandle c = parse(symbol, n);
                if (c != null) {
                    out.add(c);
                }
            }
        } else {
            log.warn("캔들 배열을 찾지 못했다. 응답 형태를 확인할 것: {}",
                    abbreviate(res.body(), 300));
        }

        String next = text(body, "nextBefore");
        if (next == null) {
            next = text(body.path("result"), "nextBefore");
        }
        return new Page(out, next);
    }

    /** 응답 원문을 그대로 돌려준다. 형식을 눈으로 확인할 때 쓴다. */
    public String fetchRaw(String symbol, int count) throws Exception {
        String url = cfg.apiBase() + "/api/v1/candles?symbol=" + symbol
                + "&interval=1m&count=" + Math.min(count, 200);
        return send(url).body();
    }

    /** 429 를 만나면 헤더가 지시하는 만큼 기다렸다 재시도한다. */
    private HttpResponse<String> send(String url) throws Exception {
        for (int attempt = 1; attempt <= 4; attempt++) {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .header("Authorization", "Bearer " + tokens.get())
                    .timeout(Duration.ofSeconds(20))
                    .GET()
                    .build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());

            if (res.statusCode() == 200) {
                return res;
            }
            if (res.statusCode() == 401 && attempt == 1) {
                tokens.get(true);   // 토큰 강제 재발급 후 한 번 더
                continue;
            }
            if (res.statusCode() == 429) {
                long waitS = res.headers().firstValue("Retry-After")
                        .map(OfficialCandleFetcher::parseLongSafe)
                        .orElse(2L * attempt);
                log.warn("rate limit. {}초 대기 (시도 {}/4)", waitS, attempt);
                Thread.sleep(waitS * 1000);
                continue;
            }
            throw new IllegalStateException(
                    "캔들 조회 실패 HTTP " + res.statusCode() + " " + abbreviate(res.body(), 200));
        }
        throw new IllegalStateException("캔들 조회 재시도 소진");
    }

    static OfficialCandle parse(String symbol, JsonNode n) {
        String ts = firstText(n, "timestamp", "time", "dateTime");
        if (ts == null || ts.isBlank()) {
            return null;
        }
        long ms;
        try {
            ms = OffsetDateTime.parse(ts).toInstant().toEpochMilli();
        } catch (Exception e) {
            log.warn("캔들 timestamp 파싱 실패: {}", ts);
            return null;
        }
        return new OfficialCandle(
                symbol, ms,
                firstText(n, "openPrice", "open"),
                firstText(n, "highPrice", "high"),
                firstText(n, "lowPrice", "low"),
                firstText(n, "closePrice", "close"),
                firstText(n, "volume", "accVolume"));
    }

    private static JsonNode firstArray(JsonNode root, String... names) {
        for (String name : names) {
            JsonNode n = root.path(name);
            if (n.isArray()) {
                return n;
            }
            JsonNode nested = n.path("candles");
            if (nested.isArray()) {
                return nested;
            }
        }
        return null;
    }

    private static String firstText(JsonNode n, String... names) {
        for (String name : names) {
            JsonNode v = n.get(name);
            if (v != null && !v.isNull()) {
                return v.asText();
            }
        }
        return null;
    }

    private static String text(JsonNode n, String name) {
        JsonNode v = n.get(name);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static long parseLongSafe(String s) {
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 2L;
        }
    }

    private static String abbreviate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() > max ? s.substring(0, max) : s;
    }
}
