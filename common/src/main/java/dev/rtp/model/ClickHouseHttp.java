package dev.rtp.model;

import java.io.Serializable;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

/**
 * ClickHouse HTTP 인터페이스로 적재한다.
 *
 * <p>Flink 싱크와 공식 캔들 수집기가 같이 쓴다. 두 곳이 각자 구현하면
 * 한쪽만 고쳐지는 일이 생기고, 그러면 "내 캔들" 과 "정답지" 가 서로 다른 방식으로
 * 저장되어 대조 자체가 신뢰를 잃는다.
 *
 * <p>JDBC 를 쓰지 않는 이유 - clickhouse-jdbc 를 Flink fat jar 에 넣으면 셰이딩 충돌이
 * 잦고, 여기서는 요청 형태가 명시적으로 보이는 편이 낫다.
 */
public final class ClickHouseHttp implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String baseUrl;
    private final String authHeader;
    private transient HttpClient http;

    public ClickHouseHttp(String baseUrl, String user, String password) {
        this.baseUrl = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
                (user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    private HttpClient client() {
        if (http == null) {
            http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        }
        return http;
    }

    /**
     * JSONEachRow 로 삽입한다.
     *
     * @param table   {@code rtp.candles_1m} 같은 정규화된 테이블명
     * @param columns 삽입할 컬럼 목록
     * @param rows    각 줄이 하나의 JSON 객체
     */
    public void insertJsonEachRow(String table, String columns, List<String> rows)
            throws Exception {
        if (rows.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO " + table + " (" + columns + ") FORMAT JSONEachRow";
        URI uri = URI.create(baseUrl + "/?query="
                + URLEncoder.encode(sql, StandardCharsets.UTF_8));

        HttpRequest req = HttpRequest.newBuilder(uri)
                .header("Authorization", authHeader)
                .header("Content-Type", "text/plain; charset=UTF-8")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(
                        String.join("\n", rows), StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> res = client().send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) {
            String detail = res.body();
            throw new IllegalStateException("ClickHouse HTTP " + res.statusCode() + " "
                    + (detail.length() > 300 ? detail.substring(0, 300) : detail));
        }
    }

    /** 문자열을 JSON 값으로 안전하게 감싼다. */
    public static String quote(String s) {
        if (s == null) {
            return "\"\"";
        }
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
