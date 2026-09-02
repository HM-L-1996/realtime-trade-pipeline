# realtime-trade-pipeline

[![build](https://github.com/HM-L-1996/realtime-trade-pipeline/actions/workflows/build.yml/badge.svg)](https://github.com/HM-L-1996/realtime-trade-pipeline/actions/workflows/build.yml)

거래소 실시간 체결을 Flink로 분 단위 캔들(OHLCV)로 집계하고,
**거래소 공식 캔들과 대조해 정확성을 수치로 증명하는** 스트리밍 파이프라인.

```
Exchange WebSocket (실시간 체결)
  → Kafka
  → Flink  (event-time window · watermark · 체크포인트 복구)
  → ClickHouse
  → 검증: 공식 캔들 API와 대조
```

## 이 프로젝트가 다루는 것

돌아가는 파이프라인을 만드는 것이 목적이 아니다.
**결과가 맞는지 증명할 수 있는 구조를 만들고, 틀어지는 지점을 찾아 기록하는 것**이 목적이다.

집계 결과에 **정답지가 존재한다**는 점이 핵심 설계다. 거래소 공식 캔들과 대조하면
내 집계가 맞는지 수치로 확인되고, 설정을 바꿨을 때 결과가 어떻게 달라지는지 **관측 가능**해진다.

```bash
./scripts/verify.sh          # 유실률 → 구간별 정합성 → 공식 캔들 대비 정확도
```

**실측 (2026-08-31, compose):** 566개 윈도, 거래량 99.29% 정확 일치(근사 아님, Decimal 비교),
소스 유실 0%(체결 303,112건 연속). 어긋난 4건은 원인까지 추적했다.

**실측 (2026-09-02, K8s · 하루치):** 730개 윈도에서 시가·고가·저가·종가 **730/730 일치**,
거래량 99.18%, 중복 0.

이 날은 파티션 재배정·소스 단절·JobManager kill 을 **일부러 일으킨 날**이다.
그래서 남은 0.82% 는 파이프라인의 오차가 아니라 **내가 만든 장애의 크기**이고,
어긋난 6개 윈도가 전부 초 단위까지 설명된다 — 넷은 연결 단절(4.009초 / 2.226초),
둘은 ±1 대칭이라 경계 배정이다. `verify.sh` 가 단절 구간을 직접 출력하므로
그 대응을 스크립트 안에서 확인할 수 있다. 자세한 것은 [validation.md](docs/validation.md).

## 대표적으로 다룬 문제

| 문제 | 결론 |
|---|---|
| 거래량이 평균 59% 어긋났다 | 소스가 lossy 하니 "유실" 로 결론 내리기 쉬웠는데, **오차의 부호가 양방향**이라는 점이 그 해석을 막았다. 공식 캔들의 `timestamp` 가 윈도 **종료** 시각이었다(문서에 없음) |
| 워터마크 지연을 0으로 줘도 늦은 데이터가 안 버려졌다 | **설정은 보장이 아니다.** 같은 입력·같은 설정에서 드롭이 0~4,255 로 갈렸고, 실제 허용치는 파티션 수와 소비 상태(실시간 추종 vs 캐치업)가 정했다 |
| 재배포하면 데이터가 어떻게 되나 | 중복과 손실은 **별개 문제**다. `earliest` 재시작은 중복 420건, `committed` 재시작은 진행 중 윈도 −67%, savepoint 재개만 둘 다 0 |
| 잡은 살아 있는데 지표가 빨간불 | 죽은 컨슈머 그룹이 lag 을 무한히 쌓고, GitOps 는 K8s 기본값 때문에 영원히 OutOfSync 였다. **그 상태를 방치하면 진짜 drift 가 묻힌다** |
| 파티션을 늘렸더니 파이프라인이 멈췄다 | Flink 는 파티션 재발견이 **기본 꺼짐**이라 새 파티션을 영원히 읽지 않는다. 그런데 **lag 은 0** 이었고, 커밋 없는 파티션을 −1 로 내는 exporter 때문에 합계는 **−5** 였다. **lag 은 건강 신호로 충분하지 않다** |
| 어디가 멈추면 데이터를 잃는가 | 소스 **2.2초** 단절 → 거래량 −1.7% **영구 유실**. JobManager **53초** 정지 → **유실 0**. 차이는 중간에 Kafka 가 있는가뿐이다. **큐 뒤는 느려질 뿐이고 큐 앞은 없어진다** |
| 유실을 무엇으로 탐지하나 | 소스가 끊긴 윈도에서 **OHLC 는 전부 정확히 일치**했다. 시가·종가는 양 끝점, 고가·저가는 극값이라 중간 체결을 잃어도 안 바뀐다. **합인 거래량만 민감하다** |
| 알람을 8개 썼는데 하나도 안 울렸다 | `rule_files` 한 줄이 없어 **한 번도 평가되지 않았다.** 파드는 Running, `helm lint` 통과. 고친 뒤에도 죽은 알람을 둘 더 만들었다(집계 없는 벡터 매칭, 틀린 그룹명). 지금은 **발화→전달까지 실측으로 확인**하고 CI 에서 `promtool` 로 검사한다 |

전부 [failure-notes.md](docs/failure-notes.md)에 "무엇을 일으켰나 → 어떻게 드러났나 →
왜 그랬나 → 어떻게 고쳤나" 로 남겼다. 자초한 기초 실수와 규명한 것을 **스스로 구분해 두었다.**

## 검증이 가능하도록 만든 설계

| 판단 | 이유 |
|---|---|
| 캔들 테이블을 `MergeTree` (`ReplacingMergeTree` 아님) | 중복을 저장 계층에서 접으면 exactly-once 가 지켜지는지 **결과로** 확인할 수 없다. `write_count > 1` 로 드러나게 두었고, 실제로 재배포 중복 420건을 그렇게 잡았다 |
| 가격·수량을 `Decimal64` (부동소수 아님) | 0.01 차이가 났을 때 **내 집계 문제인지 부동소수 문제인지** 구분할 수 없게 된다 |
| 수집기가 `recv_seq`·`conn_id` 를 부여 | 소스에 시퀀스가 없다. 이게 없으면 공식 캔들과의 차이가 **내 버그인지 소스 유실인지** 끝까지 구분되지 않는다 |
| 워터마크·허용 지연을 잡 인자로 | 코드를 고쳐야 실험이 되는 구조면 실험을 안 하게 된다 |

## 운영

로컬 docker-compose 로 먼저 검증한 뒤 K8s 로 옮겼다.
K8s 는 Helm 차트 하나(`charts/rtp`)이고 **ArgoCD 가 저장소를 클러스터 상태의 근거로 삼는다**
(`selfHeal`). Flink 잡은 Operator 의 `FlinkDeployment` 로 배포하며
`upgradeMode: savepoint` 라 재배포 시 상태를 넘긴다.

> 순서를 지킨 이유가 있다. 재시작 전략을 잘못 잡아 잡이 복구되지 못한 사고를
> **compose 에서 먼저 만났다.** K8s 에서 처음 겪었다면 클러스터 문제로 오진했을 것이다.

## 문서

- [실패 정책과 운영 기준](docs/failure-policy.md) — 무엇에 멈추고 무엇을 버리는가, SLI/SLO, 알람 등급
- [설계 판단](docs/design-decisions.md) — 왜 이 구성인지, 무엇을 포기했는지
- [장애 실험 노트](docs/failure-notes.md) — 의도적으로 깨뜨리고 고친 기록
- [검증 결과](docs/validation.md) — 공식 캔들 대비 정확도

## 실행

```bash
cp .env.example .env      # 토스증권 Open API 키를 채운다
docker compose -f infra/docker-compose.yml up -d
```

빌드는 로컬 JDK 없이 Docker 안에서 한다.

```bash
./mvnd.sh clean package
```

집계 잡 제출. 기본은 커밋된 오프셋에서 이어 읽는다 —
`earliest` 로 두면 재배포할 때마다 토픽 전체를 재처리해 같은 윈도가 다시 적재된다.

```bash
cp aggregator/target/aggregator-0.1.0.jar infra/flink/jobs/
docker exec rtp-jobmanager flink run -d /flink/jobs/aggregator-0.1.0.jar   --topic trades.raw --group-id rtp-candle-live --watermark-delay-seconds 5

# 재처리·리플레이 실험은 명시적으로 켠다
#   --start-offsets earliest
```

| 서비스 | 주소 | 용도 |
|---|---|---|
| Flink Web UI | http://localhost:8081 | 백프레셔·체크포인트 관측 |
| ClickHouse | http://localhost:8123 | 검증 쿼리 |
| Kafka | localhost:9092 | 수집기 접속 |

검증은 스크립트 하나로 고정돼 있다. 매번 즉석 쿼리를 짜면 결과가 흔들려 비교가 안 된다.

```bash
./scripts/verify.sh k8s       # kind 클러스터
./scripts/verify.sh compose   # docker-compose 스택
```

수집 유실률 → Kafka→Flink 구간 → 공식 캔들 대비 정확도 → 어긋난 윈도 순으로 찍는다.
**`matched` 가 0 이면 겹치는 구간이 없다는 뜻이므로 정확도를 주장하면 안 된다.**

개별 쿼리는 뷰로도 고정돼 있다.

```sql
-- 공식 캔들 대비 차이. missing=1 은 누락, write_count>1 은 중복 기록.
SELECT * FROM rtp.candle_diff WHERE missing OR write_count > 1;

-- 소스 유실률. recv_seq 는 연결별로 1씩 증가하므로 공백이 곧 유실이다.
-- 공식 캔들과 어긋났을 때 "내 버그" 와 "소스 유실" 을 가르는 근거.
SELECT * FROM rtp.source_continuity;

-- Kafka→Flink 구간 유실 확인. raw_trades 와 candle_trades 가 다르면 그 구간 문제다.
SELECT * FROM rtp.trade_count_check WHERE diff != 0;
```

### 장애 실험용 계측 (선택)

Prometheus·Grafana는 오버레이로 분리돼 있다. 실험 단계에서 켠다.

```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.observability.yml up -d
```

Grafana http://localhost:3000 · Prometheus http://localhost:9090

### 장외 시간 검증

국내장은 평일 09:00-15:30만 열린다. 장이 닫혀 있을 때는 합성 체결로 downstream을 돌린다.
지연 도착과 중복을 의도적으로 주입할 수 있어 장애 실험의 작업 도구이기도 하다.

```bash
docker run --rm --network rtp_default -v "$PWD:/work" -w /work   -e KAFKA_BOOTSTRAP_SERVERS=kafka:19092 eclipse-temurin:17-jre   java -cp ingester/target/ingester-0.1.0.jar dev.rtp.ingester.SyntheticMain   --rate 40 --duration 60 --late-ratio 0.05 --dup-ratio 0.02
```

## 구성

| 모듈 | 역할 |
|---|---|
| `common` | 수집기와 집계 잡이 공유하는 레코드 형식 |
| `ingester` | 토스 WebSocket 체결 → Kafka. 합성 생성기 포함 |
| `aggregator` | Flink 분봉 집계 잡 |

## 스택

Kafka · Apache Flink (Kubernetes Operator) · ClickHouse · Kubernetes
