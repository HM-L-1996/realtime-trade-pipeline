-- 검증 쿼리를 뷰로 고정해둔다. validation.md의 수치는 여기서 나온다.

-- 중복을 접은 "최종" 캔들. 같은 윈도가 여러 번 들어왔으면 마지막 것을 쓴다.
-- write_count 가 1이 아니면 그 윈도는 중복 기록된 것 — exactly-once 검증 지점.
--
-- 주의: ingested_at 이 ms 해상도라 같은 밀리초에 두 번 쓰이면 argMax 의 승자가
-- 정해지지 않는다. 실제 재기록은 초 단위로 벌어지므로 보통은 문제되지 않지만,
-- 재처리를 몰아서 돌릴 때는 동률이 날 수 있다. 그때는 run_id 로 구분한다.
CREATE VIEW IF NOT EXISTS rtp.candles_1m_dedup AS
SELECT
    symbol,
    window_start,
    argMax(open, ingested_at)        AS open,
    argMax(high, ingested_at)        AS high,
    argMax(low, ingested_at)         AS low,
    argMax(close, ingested_at)       AS close,
    argMax(volume, ingested_at)      AS volume,
    argMax(trade_count, ingested_at) AS trade_count,
    count()                          AS write_count
FROM rtp.candles_1m
GROUP BY symbol, window_start;

-- 공식 캔들 대비 차이. 이 뷰가 이 프로젝트의 채점표다.
-- 공식 캔들 기준 LEFT JOIN이므로 내 쪽에 없는 윈도(missing=1)도 드러난다.
-- ClickHouse는 기본적으로 LEFT JOIN 미매칭을 NULL이 아닌 기본값으로 채우므로
-- symbol='' 로 미매칭을 판별한다.
-- symbol이 LowCardinality(String)이라 비교 결과에도 LC가 전파되는데,
-- ClickHouse는 LowCardinality(UInt8) 컬럼 생성을 기본 차단한다(SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY).
-- CAST로 평범한 UInt8로 벗겨낸다.
CREATE VIEW IF NOT EXISTS rtp.candle_diff AS
SELECT
    o.symbol                                       AS symbol,
    o.window_start                                 AS window_start,
    CAST(m.symbol = '' AS UInt8)                   AS missing,
    m.write_count                                  AS write_count,
    m.trade_count                                  AS my_trade_count,
    m.close - o.close                              AS close_diff,
    m.volume - o.volume                            AS volume_diff,
    abs(m.volume - o.volume) / nullIf(o.volume, 0) AS volume_rel_err
FROM rtp.candles_1m_official AS o
LEFT JOIN rtp.candles_1m_dedup AS m
       ON o.symbol = m.symbol AND o.window_start = m.window_start;
