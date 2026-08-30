-- 검증용 스키마.
--
-- 설계 판단: 캔들 테이블에 ReplacingMergeTree를 쓰지 않는다.
-- Replacing을 쓰면 (symbol, window_start) 중복이 머지 시점에 조용히 접힌다.
-- 이 프로젝트는 exactly-once가 실제로 지켜지는지를 결과로 확인하는 것이 목적이라
-- 중복이 눈에 보여야 한다. 중복 제거는 조회 시점에 명시적으로 한다.

CREATE DATABASE IF NOT EXISTS rtp;

-- Flink가 집계한 분봉
CREATE TABLE IF NOT EXISTS rtp.candles_1m
(
    symbol        LowCardinality(String),
    window_start  DateTime64(3, 'UTC'),
    open          Float64,
    high          Float64,
    low           Float64,
    close         Float64,
    volume        Float64,
    trade_count   UInt64,
    -- 관측용 메타: 같은 윈도가 몇 번, 언제 쓰였는지 추적한다
    ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3),
    run_id        String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start, ingested_at);

-- 거래소 공식 캔들 (정답지)
CREATE TABLE IF NOT EXISTS rtp.candles_1m_official
(
    symbol        LowCardinality(String),
    window_start  DateTime64(3, 'UTC'),
    open          Float64,
    high          Float64,
    low           Float64,
    close         Float64,
    volume        Float64,
    fetched_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start);

-- 원본 체결. 재처리·순서 검증, 그리고 장외 시간 리플레이의 근거가 된다.
--
-- 토스 WebSocket 체결 프레임에는 체결ID도 시퀀스 번호도 없다(문서 명시).
-- 매수/매도 구분과 누적 거래량도 없다. 따라서 소스에서 받을 수 있는 것은
-- (symbol, timestamp, price, volume) 뿐이고, 이 조합은 유일하지 않다 --
-- 같은 ms에 같은 가격·수량의 체결이 실제로 발생한다.
-- 그래서 멱등키를 수집기에서 합성한다: 같은 (symbol, event_time) 안에서의 일련번호.
CREATE TABLE IF NOT EXISTS rtp.trades_raw
(
    symbol          LowCardinality(String),
    event_time      DateTime64(3, 'UTC'),   -- 소스 timestamp (KST → UTC 정규화)
    seq_in_ms       UInt16,                 -- 수집기가 부여. 동일 ms 내 수신 순서
    ingest_time     DateTime64(3, 'UTC'),   -- 수집기 수신 시각. 소스 지연 측정용
    price           Decimal64(4),           -- 소스가 문자열로 준다. 부동소수 오차 회피
    volume          Decimal64(8),
    -- 소스 유실률 측정용: 수집기가 세는 연결별 프레임 일련번호.
    -- 스트림이 lossy하므로 "내 파이프라인 문제"와 "소스 유실"을 분리하려면 필요하다.
    recv_seq        UInt64,
    conn_id         String,                 -- 재연결 구분. 끊긴 구간 식별
    kafka_partition UInt16,
    kafka_offset    UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (symbol, event_time, seq_in_ms)
TTL toDateTime(event_time) + INTERVAL 7 DAY;
