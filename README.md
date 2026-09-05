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
./scripts/verify.sh          # 배포 리비전 → 유실률 → 출처 → 구간별 정합성 → 공식 캔들 대비 정확도
```

**대시보드 두 개는 질문이 다르다.** Grafana 는 Prometheus 를 보고
"파이프라인이 지금 건강한가" 를, Superset 은 ClickHouse 를 보고
"결과가 맞았는가" 를 묻는다. 그래서 둘이 함께 있는 것이 중복이 아니다.

**실측 (2026-08-31, compose):** 566개 윈도, 거래량 99.29% 정확 일치(근사 아님, Decimal 비교),
소스 유실 0%(체결 303,112건 연속). 어긋난 4건은 원인까지 추적했다.

**실측 (2026-09-02, K8s · 정규장 하루치):** 760개 윈도에서 시가·고가·저가·종가 **760/760 일치**,
거래량 99.21%, 중복 0.

이 날은 파티션 재배정·소스 단절·JobManager kill 을 **일부러 일으킨 날**이다.
그래서 남은 0.79% 는 파이프라인의 오차가 아니라 **내가 만든 장애의 크기**이고,
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
| 오토스케일이 정말 도움이 되나 | "켰더니 6.5배" 를 분해하니 **자원 4.69배 × 병렬화 1.39배**였다. 종목이 2개라 병렬도 6에서 **subtask 4개가 놀았다.** 키 개수만 바꾼 대조군은 2.28배. 재스케일 비용은 한 번에 **1분 59초 정지** |
| 장애 실험이 대시보드에 어떻게 보였나 | **안 보였다.** 실험 잡이 메트릭 리포터를 빠뜨려 Flink 지표를 하나도 안 내보냈는데 스크레이프 타깃은 `up` 이었다. `emptyDir` 라 이력도 사라져 있었다. 실험에서 결정적이었던 신호로 9패널을 만들고 PVC 로 옮겼다 |
| at-least-once 와 exactly-once 는 실제로 얼마나 다른가 | 같은 캔들을 ClickHouse 와 Iceberg 에 동시에 쓰고 TaskManager 를 죽였다. **값은 전 항목 일치하고 기록 횟수만 갈렸다** — ClickHouse 22행(중복 2), Iceberg 20행(중복 0) |
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

**현재 기준은 K8s 다.** CLAUDE.md 가 정한 순서대로 docker-compose 로 먼저 동작시킨 뒤
옮겼고, compose 구성도 남겨 두었다 — 같은 잡을 두 환경에서 돌려 비교할 수 있어야
"K8s 로 옮겨서 달라진 것" 을 가릴 수 있기 때문이다.

### 준비

```bash
cp .env.example .env      # 토스증권 Open API 키를 채운다
./mvnd.sh clean package   # 빌드는 로컬 JDK 없이 Docker 안에서 한다
```

### K8s (현재 기준)

클러스터·cert-manager·Flink Operator·ArgoCD 설치는 [k8s/README.md](k8s/README.md)에 있다.
여기서는 그 뒤부터다.

```bash
# 잡·수집기·대시보드 이미지를 만들어 노드에 적재한다.
# kind 는 레지스트리를 안 쓰므로 이미지를 직접 넣어야 하고,
# 그래서 매니페스트가 imagePullPolicy: Never 다.
docker build -f aggregator/Dockerfile -t rtp-aggregator:0.1.7 .
docker build -f ingester/Dockerfile   -t rtp-ingester:0.1.2 .
docker build -f superset/Dockerfile   -t rtp-superset:0.1.0 superset/

for img in rtp-aggregator:0.1.7 rtp-ingester:0.1.2 rtp-superset:0.1.0; do
  kind load docker-image "$img" --name rtp
done

# 토스 자격증명. public 저장소이므로 차트에 두지 않는다.
kubectl create secret generic toss-credentials -n rtp --from-env-file=.env

# 배포는 ArgoCD 가 한다. 저장소가 곧 클러스터 상태다.
kubectl apply -f k8s/argocd/application.yaml
```

**코드를 고치면 이미지 태그를 올린다.** 같은 태그로 다시 적재하면 매니페스트가
그대로라 Operator 가 변경을 감지하지 못하고, 파드는 노드에 캐시된 옛 이미지를
계속 쓴다(`imagePullPolicy: Never` 라 더욱 그렇다).

### 접속

| 서비스 | 주소 | 용도 |
|---|---|---|
| Flink Web UI | http://localhost:18081 | 백프레셔·체크포인트·subtask 별 처리량 |
| Grafana | http://localhost:13000 | 지표 — **지금 건강한가** (admin/admin) |
| ArgoCD | http://localhost:18080 | 배포 상태 |
| Superset | `kubectl -n rtp port-forward svc/superset 8088:8088` | 결과 — **맞았는가** (admin/admin) |
| ClickHouse | `kubectl -n rtp exec -it clickhouse-0 -- clickhouse-client -u rtp --password rtp` | 검증 쿼리 |

앞의 셋은 kind 의 `extraPortMappings` 로 호스트에 열려 있다.
**포트 매핑은 노드 컨테이너를 만들 때 정해지므로** 이미 떠 있는 클러스터에
새 포트를 열려면 클러스터를 다시 만들어야 한다. Superset 이 port-forward 인 이유다.

### docker-compose (1단계, 지금도 동작한다)

```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml -f infra/docker-compose.observability.yml up -d
```

집계 잡은 직접 제출한다. 기본은 커밋된 오프셋에서 이어 읽는다 —
`earliest` 로 두면 재배포할 때마다 토픽 전체를 재처리해 같은 윈도가 다시 적재된다
(실측 420개).

```bash
cp aggregator/target/aggregator-0.1.0.jar infra/flink/jobs/
docker exec rtp-jobmanager flink run -d /flink/jobs/aggregator-0.1.0.jar   --topic trades.raw --group-id rtp-candle-live --watermark-delay-seconds 5
```

| 서비스 | 주소 |
|---|---|
| Flink Web UI | http://localhost:8081 |
| ClickHouse | http://localhost:8123 |
| Grafana | http://localhost:3000 |

### 검증

검증은 스크립트 하나로 고정돼 있다. 매번 즉석 쿼리를 짜면 결과가 흔들려 비교가 안 된다.

```bash
./scripts/verify.sh k8s       # kind 클러스터 (기본값)
./scripts/verify.sh compose   # docker-compose 스택
```

**배포 리비전 → 수집 유실률 → 출처 점검 → Kafka→Flink 구간 → 공식 캔들 대비 정확도
→ 어긋난 윈도 → 버려진 레코드** 순으로 찍는다. 각 항목에 판정 기준이 붙어 있다.

- **`matched` 가 0 이면** 겹치는 구간이 없다는 뜻이므로 정확도를 주장하면 안 된다
- **1-b 에 가격대가 다른 연결이 섞여 있으면** 3번 수치는 무효다 (실험 데이터가
  운영 토픽을 오염시켜 한 번 당했다)
- **0번의 적용 리비전이 HEAD 와 다르면** 아래 결과는 지금 코드의 것이 아니다
  (ArgoCD 가 `Synced` 라 하면서 이전 리비전을 적용해 둔 적이 있다)

개별 쿼리는 뷰로도 고정돼 있다.

```sql
-- 공식 캔들 대비 차이. missing=1 은 누락, write_count>1 은 중복 기록.
SELECT * FROM rtp.candle_diff WHERE missing OR write_count > 1;

-- 소스 유실률. recv_seq 는 연결별로 1씩 증가하므로 공백이 곧 유실이다.
-- 공식 캔들과 어긋났을 때 "내 버그" 와 "소스 유실" 을 가르는 근거.
SELECT * FROM rtp.source_continuity;
```


## 구성

| 모듈 | 역할 |
|---|---|
| `common` | 수집기와 집계 잡이 공유하는 레코드 형식 |
| `ingester` | 토스 WebSocket 체결 → Kafka. 합성 생성기 포함 |
| `aggregator` | Flink 분봉 집계 잡 |

## 스택

Kafka · Apache Flink (Kubernetes Operator) · ClickHouse · Kubernetes
