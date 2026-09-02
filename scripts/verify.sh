#!/usr/bin/env bash
# 파이프라인 검증. 매번 즉석 SQL 을 짜면 결과가 흔들려 비교가 안 된다.
#
#   ./scripts/verify.sh k8s      (기본) kind 클러스터
#   ./scripts/verify.sh compose  docker-compose 스택
set -uo pipefail
TARGET="${1:-k8s}"

case "$TARGET" in
  k8s)     CH() { MSYS_NO_PATHCONV=1 kubectl exec -n rtp -i clickhouse-0 -- clickhouse-client --user rtp --password rtp "$@"; } ;;
  compose) CH() { MSYS_NO_PATHCONV=1 docker exec -i rtp-clickhouse clickhouse-client --user rtp --password rtp "$@"; } ;;
  *) echo "사용법: $0 [k8s|compose]"; exit 1 ;;
esac

# 쿼리 오류를 삼키지 않는다.
#
# 처음에는 stderr 를 /dev/null 로 버렸다. **그러면 쿼리가 깨진 것과 결과가 없는 것이
# 화면에서 똑같아 보인다.** 실제로 1-b 를 고치다 GROUP BY 를 빠뜨렸는데 아무 표시 없이
# 빈칸만 나왔다. 검증 스크립트가 조용히 실패하면 검증 자체를 믿을 수 없다.
q() {
  local out rc
  out=$(CH --query "$1" 2>&1); rc=$?
  if [ $rc -ne 0 ]; then
    echo "   !! 쿼리 실패 (exit $rc) — 이 항목의 결과는 없는 것이 아니라 **측정되지 않았다**"
    echo "$out" | sed -n '1,3p' | sed 's/^/      /'
    FAILED_QUERIES=$((FAILED_QUERIES + 1))
    return 0
  fi
  [ -n "$out" ] && echo "$out"
  return 0
}
FAILED_QUERIES=0

echo "════════ 검증 대상: $TARGET ════════"
echo
echo "── 1. 수집 (원본 체결) ──"
echo "   유실은 recv_seq 공백으로 판정한다. gaps>0 유실, gaps<0 중복."
q "SELECT conn_id, received, gaps, loss_pct, first_seen, last_seen
   FROM rtp.source_continuity WHERE conn_id NOT LIKE 'synth%' AND conn_id NOT LIKE 'replay%'
   ORDER BY received DESC FORMAT PrettyCompact"

echo
echo "── 1-b. 출처 점검 (합성/리플레이가 섞였는가) ──"
echo "   실험 데이터가 운영 토픽에 섞이면 수치가 나오는데 그 수치가 틀린다. 실제로 당했다."
echo "   이름 규칙만 믿지 않는다. 접두어를 빼먹은 실험 스크립트는 못 잡기 때문이다."
echo "   **연결을 전부 나열하고 가격대를 같이 본다** - 출처가 다르면 값의 범위가 다르다."
q "SELECT conn_id,
          multiIf(conn_id LIKE 'synth%%', '합성(오염)',
                  conn_id LIKE 'replay%%', '리플레이(오염)', '실수집?') AS label,
          count() AS trades,
          uniqExact(symbol) AS syms,
          toString(round(min(toFloat64(price)))) AS price_min,
          toString(round(max(toFloat64(price)))) AS price_max,
          toString(min(ingest_time)) AS first_seen
   FROM rtp.trades_raw GROUP BY conn_id, label ORDER BY trades DESC FORMAT PrettyCompact"

echo
echo "   판정: 연결이 2개 이상이면 그 자체로 의심한다. 수집기는 한 번에 하나만 붙는다."
echo "         가격대가 다른 연결이 섞여 있으면 이름이 무엇이든 **오염이다.**"
echo "         실제로 당했을 때 실수집은 1,618,000~1,641,000, 합성은 66,563~84,356 이었다."
echo "   - 오염 행이 있으면 3번 수치는 **무효다.** 수치가 나온다고 유효한 게 아니다."

echo
echo "── 2. Kafka→Flink 구간 ──"
echo "   원본 체결 수와 캔들의 trade_count 가 다르면 이 구간에서 빠진 것이다."
q "SELECT count() AS windows, countIf(diff=0) AS exact, countIf(diff!=0) AS mismatched
   FROM rtp.trade_count_check FORMAT PrettyCompact"

echo
echo "── 3. 공식 캔들 대비 정확도 (★ 채점표) ──"
q "WITH mine AS (
     SELECT symbol, window_start,
            argMax(open,ingested_at) o, argMax(high,ingested_at) h,
            argMax(low,ingested_at) l,  argMax(close,ingested_at) c,
            argMax(volume,ingested_at) v, count() wc
     FROM rtp.candles_1m GROUP BY symbol, window_start),
   off AS (
     SELECT symbol, window_start,
            argMax(open,fetched_at) open, argMax(high,fetched_at) high,
            argMax(low,fetched_at) low,   argMax(close,fetched_at) close,
            argMax(volume,fetched_at) volume
     FROM rtp.candles_1m_official GROUP BY symbol, window_start)
   SELECT count() AS matched,
          countIf(m.o=f.open)   AS open_ok,
          countIf(m.h=f.high)   AS high_ok,
          countIf(m.l=f.low)    AS low_ok,
          countIf(m.c=f.close)  AS close_ok,
          countIf(m.v=f.volume) AS vol_ok,
          countIf(m.wc>1)       AS duplicated,
          round(countIf(m.v=f.volume)/count()*100, 2) AS vol_pct
   FROM mine m INNER JOIN off f ON m.symbol=f.symbol AND m.window_start=f.window_start
   FORMAT Vertical"

echo
echo "── 4. 어긋난 윈도 (있으면) ──"
q "WITH mine AS (
     SELECT symbol, window_start, argMax(volume,ingested_at) v, argMax(close,ingested_at) c
     FROM rtp.candles_1m GROUP BY symbol, window_start),
   off AS (
     SELECT symbol, window_start, argMax(volume,fetched_at) volume, argMax(close,fetched_at) close
     FROM rtp.candles_1m_official GROUP BY symbol, window_start)
   SELECT toString(m.window_start + INTERVAL 9 HOUR) AS kst, m.symbol,
          m.v AS my_vol, f.volume AS off_vol, m.v-f.volume AS vol_diff, m.c-f.close AS close_diff
   FROM mine m INNER JOIN off f ON m.symbol=f.symbol AND m.window_start=f.window_start
   WHERE m.v != f.volume OR m.c != f.close
   ORDER BY m.window_start LIMIT 20 FORMAT PrettyCompact"

echo
echo "── 5. 버려진 레코드 (dead letter) ──"
echo "   세기만 하고 버리면 왜 버려졌는지 사후에 볼 수 없다. 원본을 남긴다."
q "SELECT stage, reason, dropped, symbols, first_seen, last_seen
   FROM rtp.dead_letter_summary FORMAT PrettyCompact"

echo
echo "── 6. 범위 ──"
q "SELECT 'mine' AS src, count() AS rows, toString(min(window_start)+INTERVAL 9 HOUR) AS first_kst,
          toString(max(window_start)+INTERVAL 9 HOUR) AS last_kst FROM rtp.candles_1m
   UNION ALL
   SELECT 'official', count(), toString(min(window_start)+INTERVAL 9 HOUR),
          toString(max(window_start)+INTERVAL 9 HOUR) FROM rtp.candles_1m_official
   FORMAT PrettyCompact"

echo
echo "※ 판정 기준"
echo "   - 3번의 vol_ok 가 matched 와 같아야 한다. 근사가 아니라 정확 일치다."
echo "   - duplicated 가 0 이 아니면 같은 윈도가 두 번 쓰였다 (at-least-once 노출)."
echo "   - 1번 gaps 가 0 이 아니면 4번의 차이를 소스 유실로 설명할 수 있다."
echo "   - matched 가 0 이면 아직 겹치는 구간이 없다는 뜻이다. 정확도 주장 금지."
echo "   - 5번이 비어 있으면 버려진 레코드가 없다는 뜻이다. 카운터와 대조할 것."
echo "   - 1-b 에 오염 행이 있으면 3번 수치는 **무효다.** 수치가 나온다고 유효한 게 아니다."

echo
if [ "$FAILED_QUERIES" -ne 0 ]; then
  echo "‼ 쿼리 ${FAILED_QUERIES}개가 실패했다. 위 결과는 불완전하다."
  exit 1
fi
