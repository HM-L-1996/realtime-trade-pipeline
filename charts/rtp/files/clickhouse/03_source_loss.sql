-- 소스 유실 측정.
--
-- 토스 WebSocket 은 시퀀스 번호를 주지 않고 문서가 스스로 lossy 하다고 명시한다.
-- 그래서 수집기가 연결별로 recv_seq 를 1씩 매긴다 - 이 값의 공백이 곧
-- "수집기까지는 왔는데 그 뒤로 사라진 것" 이거나 "애초에 소스가 안 준 것" 이다.
--
-- 왜 필요한가: 공식 캔들과 거래량이 어긋났을 때 원인이 두 갈래다.
--   1. 내 파이프라인 문제 (워터마크, 윈도, 체크포인트)
--   2. 소스/전송 유실
-- 이 뷰가 2번의 크기를 따로 재준다. 재지 못하면 validation.md 의 수치가
-- 무엇을 증명하는지 말할 수 없다.

-- 연결별 recv_seq 연속성.
--
-- gaps 의 부호가 두 가지 사건을 가른다.
--   gaps > 0  : 받은 개수가 기대보다 적다 -> 유실
--   gaps < 0  : 받은 개수가 기대보다 많다 -> 중복
-- 하나의 지표로 양쪽을 다 잡는다. 부호를 버리고 abs 를 쓰면 이 구분이 사라진다.
CREATE VIEW IF NOT EXISTS rtp.source_continuity AS
SELECT
    conn_id,
    count()                                   AS received,
    min(recv_seq)                             AS first_seq,
    max(recv_seq)                             AS last_seq,
    -- 연속이라면 마지막 - 처음 + 1 == 받은 개수여야 한다.
    (max(recv_seq) - min(recv_seq) + 1)       AS expected,
    (max(recv_seq) - min(recv_seq) + 1) - count() AS gaps,
    round(((max(recv_seq) - min(recv_seq) + 1) - count())
          / nullIf(toFloat64(max(recv_seq) - min(recv_seq) + 1), 0) * 100, 4) AS loss_pct,
    toString(min(ingest_time))                AS first_seen,
    toString(max(ingest_time))                AS last_seen
FROM rtp.trades_raw
GROUP BY conn_id;

-- 유실이 어느 구간에 몰렸는지. 분 단위로 끊어 본다.
-- 특정 시각에 몰려 있으면 부하나 네트워크 사건이고,
-- 고르게 퍼져 있으면 소스 자체의 상시 특성이다. 대응이 완전히 달라진다.
CREATE VIEW IF NOT EXISTS rtp.source_loss_by_minute AS
SELECT
    conn_id,
    toStartOfMinute(ingest_time)                        AS minute,
    count()                                             AS received,
    (max(recv_seq) - min(recv_seq) + 1) - count()       AS gaps
FROM rtp.trades_raw
GROUP BY conn_id, minute
ORDER BY minute;

-- 내 캔들의 체결 수를 원본 체결 수와 대조한다.
-- 두 값이 다르면 Kafka -> Flink 구간에서 빠진 것이고,
-- 두 값이 같은데 공식 캔들과 거래량이 다르면 소스가 애초에 덜 준 것이다.
-- 이 구분이 이 프로젝트가 답하려는 질문의 핵심이다.
CREATE VIEW IF NOT EXISTS rtp.trade_count_check AS
SELECT
    r.symbol                                  AS symbol,
    r.window_start                            AS window_start,
    r.raw_trades                              AS raw_trades,
    c.trade_count                             AS candle_trades,
    r.raw_trades - c.trade_count              AS diff
FROM (
    SELECT symbol,
           toStartOfMinute(event_time) AS window_start,
           count() AS raw_trades
    FROM rtp.trades_raw
    GROUP BY symbol, window_start
) AS r
INNER JOIN rtp.candles_1m_dedup AS c
        ON r.symbol = c.symbol AND r.window_start = c.window_start;
