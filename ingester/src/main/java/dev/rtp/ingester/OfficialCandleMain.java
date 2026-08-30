package dev.rtp.ingester;

import dev.rtp.model.ClickHouseHttp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 거래소 공식 분봉을 주기적으로 가져와 {@code rtp.candles_1m_official} 에 넣는다.
 *
 * <p><b>이것이 없으면 이 프로젝트가 성립하지 않는다.</b> 내 집계가 맞는지 판단할
 * 기준이 사라지고, {@code validation.md} 의 표를 쓸 수 없다.
 *
 * <h2>직전 분봉을 건너뛰는 이유</h2>
 * 장중에 조회하면 진행 중인 분이 <b>부분 집계</b>로 올 수 있다. 그걸 정답지로 저장하면
 * 내 집계가 맞는데도 틀린 것처럼 보인다. {@code --settle-minutes} 만큼 지난 봉만 저장한다.
 *
 * <p>공식 캔들이 나중에 값이 바뀌는지는 아직 확인되지 않았다(문서에 언급 없음).
 * 그래서 테이블을 {@code ReplacingMergeTree(fetched_at)} 로 두어 재조회가 덮어쓰게 했다 -
 * 정답지는 최신 것 하나만 있으면 되므로, 여기서는 중복을 남길 이유가 없다.
 * (내 캔들 테이블은 반대로 중복을 남긴다. 거긴 중복이 관측 대상이기 때문이다.)
 *
 * <pre>
 *   java -cp ingester.jar dev.rtp.ingester.OfficialCandleMain \
 *        --count 200 --interval-seconds 60 --settle-minutes 2
 *   java -cp ingester.jar dev.rtp.ingester.OfficialCandleMain --once
 * </pre>
 */
public final class OfficialCandleMain {

    private static final Logger log = LoggerFactory.getLogger(OfficialCandleMain.class);

    private static final String TABLE = "rtp.candles_1m_official";
    private static final String COLUMNS = "symbol, window_start, open, high, low, close, volume";

    /**
     * 공식 캔들의 {@code timestamp} 는 <b>윈도 종료 시각</b>이다.
     *
     * <p>문서에는 없는 사실이고, 실측으로 확인했다(2026-08-31). 내 캔들과 그대로 대조하면
     * 거래량이 평균 59% 어긋나고 차이가 <b>양방향</b>으로 난다 - 프레임 유실이면
     * 한쪽으로만 어긋나야 하므로 정렬 문제라는 신호였다.
     * 1분 밀어서 맞춰보니 <b>84개 윈도 전부 거래량·종가가 정확히 일치</b>했다.
     *
     * <p>컬럼 이름이 {@code window_start} 이므로 적재 시점에 시작 시각으로 바꾼다.
     * 종료 시각을 그대로 넣으면 이 테이블을 쓰는 모든 쿼리가 조용히 1분씩 어긋난다.
     */
    private static final long CANDLE_INTERVAL_MS = 60_000L;

    public static void main(String[] args) throws Exception {
        Map<String, String> a = SyntheticMain.parseArgs(args);
        int count = Integer.parseInt(a.getOrDefault("count", "200"));
        long intervalS = Long.parseLong(a.getOrDefault("interval-seconds", "60"));
        long settleMin = Long.parseLong(a.getOrDefault("settle-minutes", "2"));
        boolean once = a.containsKey("once");

        Config cfg = Config.fromEnv();
        log.info("공식 캔들 수집 시작 {} count={} settle={}분 {}",
                cfg.redacted(), count, settleMin, once ? "(1회)" : "(주기 " + intervalS + "초)");

        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10)).build();
        TokenProvider tokens = new TokenProvider(cfg, http);
        OfficialCandleFetcher fetcher = new OfficialCandleFetcher(cfg, http, tokens);
        ClickHouseHttp ch = new ClickHouseHttp(
                env("CLICKHOUSE_URL", "http://localhost:8123"),
                env("CLICKHOUSE_USER", "rtp"),
                env("CLICKHOUSE_PASSWORD", "rtp"));

        if (a.containsKey("dump")) {
            // 응답 형식을 눈으로 확인한다. 파서가 문서 추측에 기대고 있어
            // 실제와 다른 곳을 찾아내는 용도다.
            String raw = fetcher.fetchRaw(cfg.symbols().get(0), 3);
            log.info("원문 응답: {}", raw.length() > 1500 ? raw.substring(0, 1500) : raw);
            return;
        }

        do {
            long cutoff = System.currentTimeMillis() - settleMin * 60_000L;
            for (String symbol : cfg.symbols()) {
                try {
                    fetchAndStore(fetcher, ch, symbol, count, cutoff);
                } catch (Exception e) {
                    // 한 종목 실패가 나머지를 막지 않는다.
                    log.error("{} 공식 캔들 수집 실패: {}", symbol, e.getMessage());
                }
            }
            if (!once) {
                Thread.sleep(intervalS * 1000);
            }
        } while (!once);
    }

    private static void fetchAndStore(OfficialCandleFetcher fetcher, ClickHouseHttp ch,
                                      String symbol, int count, long cutoff) throws Exception {
        OfficialCandleFetcher.Page page = fetcher.fetch(symbol, count, null);

        List<String> rows = new ArrayList<>();
        int skipped = 0;
        for (var c : page.candles()) {
            if (c.windowStartMs() > cutoff) {
                // 아직 확정되지 않았을 수 있는 봉. 다음 회차에 다시 가져온다.
                skipped++;
                continue;
            }
            rows.add(toJson(c));
        }

        ch.insertJsonEachRow(TABLE, COLUMNS, rows);
        log.info("{} 공식 캔들 {}건 적재 (미확정 {}건 보류, 응답 {}건)",
                symbol, rows.size(), skipped, page.candles().size());
    }

    /** 가격을 문자열로 넣는다. JSON 숫자로 보내면 파서가 double 을 거치며 정밀도가 깎인다. */
    static String toJson(OfficialCandleFetcher.OfficialCandle c) {
        return "{\"symbol\":" + ClickHouseHttp.quote(c.symbol())
                // timestamp 는 윈도 종료 시각이다. 시작 시각으로 정규화한다.
                + ",\"window_start\":" + (c.windowStartMs() - CANDLE_INTERVAL_MS)
                + ",\"open\":" + ClickHouseHttp.quote(nz(c.open()))
                + ",\"high\":" + ClickHouseHttp.quote(nz(c.high()))
                + ",\"low\":" + ClickHouseHttp.quote(nz(c.low()))
                + ",\"close\":" + ClickHouseHttp.quote(nz(c.close()))
                + ",\"volume\":" + ClickHouseHttp.quote(nz(c.volume())) + "}";
    }

    private static String nz(String s) {
        return (s == null || s.isBlank()) ? "0" : s;
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }

    private OfficialCandleMain() {}
}
