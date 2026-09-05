-- 검증 쿼리를 뷰로 고정해둔다. validation.md의 수치는 여기서 나온다.

-- 중복을 접은 "최종" 캔들. 같은 윈도가 여러 번 들어왔으면 마지막 것을 쓴다.
-- write_count 가 1이 아니면 그 윈도는 중복 기록된 것 — exactly-once 검증 지점.
--
-- **컬럼마다 argMax 를 따로 부르면 안 된다.**
--
-- ingested_at 은 ms 해상도라 같은 밀리초에 두 번 쓰이면 순서가 정해지지 않는다.
-- 그때 argMax 를 여섯 번 따로 부르면 **각 호출이 서로 다른 행을 고를 수 있다** -
-- 시가는 A 에서, 종가는 B 에서 온 캔들이 나온다. 그건 **실제로 존재한 적 없는 값**이고,
-- 공식 캔들과 대조했을 때 어느 쪽 잘못인지 영원히 설명되지 않는다.
-- "승자가 누구냐" 보다 "한 행에서 통째로 가져오느냐" 가 먼저다.
--
-- 그래서 OHLCV 를 튜플로 묶어 **한 번만** argMax 한다. 동률이어도 여섯 값이
-- 반드시 같은 행에서 온다.
--
-- 정렬 키에 run_id 를 더한 것은 "그쪽이 더 맞는 값이어서" 가 아니다. run_id 가 크다고
-- 나중에 쓴 것도 아니다. **같은 질의를 다시 돌렸을 때 같은 답이 나오게** 하려는 것이다 -
-- 검증 수치가 조회할 때마다 흔들리면 비교 자체가 성립하지 않는다.
-- 같은 실행이 같은 밀리초에 두 번 쓴 경우는 여전히 임의지만, 적어도 온전한 한 행이다.
CREATE VIEW IF NOT EXISTS rtp.candles_1m_dedup AS
SELECT
    symbol,
    window_start,
    tupleElement(latest, 1) AS open,
    tupleElement(latest, 2) AS high,
    tupleElement(latest, 3) AS low,
    tupleElement(latest, 4) AS close,
    tupleElement(latest, 5) AS volume,
    tupleElement(latest, 6) AS trade_count,
    write_count
FROM (
    SELECT
        symbol,
        window_start,
        argMax((open, high, low, close, volume, trade_count),
               (ingested_at, run_id)) AS latest,
        count()                       AS write_count
    FROM rtp.candles_1m
    GROUP BY symbol, window_start
);

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
    -- **누락 윈도에서는 차이를 내지 않고 NULL 로 둔다.**
    --
    -- LEFT JOIN 미매칭이면 m.close 와 m.volume 이 0 으로 채워진다. 그대로 빼면
    -- close_diff 가 `0 - 250500 = -250500` 이 되어 **"윈도가 통째로 없음" 이
    -- "가격이 25만 틀림" 으로 읽힌다.** volume_rel_err 도 항상 1.0(100%)이 된다.
    --
    -- 이 프로젝트에서 유실과 오차를 가르는 것이 판정의 핵심인데, 그 둘을 한 컬럼에
    -- 섞으면 뷰가 오히려 판단을 방해한다. 없는 것은 없다고 해야 한다 -
    -- 크기를 알고 싶으면 missing=1 인 행을 따로 세면 된다.
    if(m.symbol = '', NULL, m.close - o.close)     AS close_diff,
    if(m.symbol = '', NULL, m.volume - o.volume)   AS volume_diff,
    -- 상대오차만 Float 로 낸다. 눈으로 크기를 보기 위한 값이고,
    -- 판정 근거가 되는 close_diff/volume_diff 는 Decimal 뺄셈이라 정확하다.
    if(m.symbol = '', NULL,
       toFloat64(abs(m.volume - o.volume)) / nullIf(toFloat64(o.volume), 0)) AS volume_rel_err
FROM rtp.candles_1m_official AS o
LEFT JOIN rtp.candles_1m_dedup AS m
       ON o.symbol = m.symbol AND o.window_start = m.window_start;
