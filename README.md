# realtime-trade-pipeline

거래소 실시간 체결 데이터를 스트림 처리해 분 단위 캔들(OHLCV)을 만들고,
**거래소 공식 캔들과 대조해 집계 정확성을 검증**하는 파이프라인.

```
Exchange WebSocket (실시간 체결)
  → Kafka
  → Flink  (event-time window · watermark · exactly-once)
  → ClickHouse
  → 검증: 공식 캔들 API와 대조
```

## 이 프로젝트의 목적

파이프라인을 "돌아가게" 만드는 것이 아니라, **스트림 처리에서 결과가 틀어지는 지점을 직접 만나고 해결하는 것**이 목적이다.
실시간 집계는 정상 경로보다 경계 상황(지연 도착, 재처리, 순서 역전, 백프레셔)에서 실력이 갈린다.

집계 결과에 **정답지가 존재한다**는 점이 이 프로젝트의 핵심 설계다.
거래소가 제공하는 공식 캔들과 대조하면 내 집계가 맞는지 수치로 확인할 수 있고,
워터마크나 exactly-once 설정을 바꿨을 때 결과가 어떻게 달라지는지 **관측 가능**해진다.

## 문서

- [설계 판단](docs/design-decisions.md) — 왜 이 구성인지, 무엇을 포기했는지
- [장애 실험 노트](docs/failure-notes.md) — 의도적으로 깨뜨리고 고친 기록
- [검증 결과](docs/validation.md) — 공식 캔들 대비 정확도

## 실행

```bash
cp .env.example .env      # 토스증권 Open API 키를 채운다
docker compose -f infra/docker-compose.yml up -d
```

| 서비스 | 주소 | 용도 |
|---|---|---|
| Flink Web UI | http://localhost:8081 | 백프레셔·체크포인트 관측 |
| ClickHouse | http://localhost:8123 | 검증 쿼리 |
| Kafka | localhost:9092 | 수집기 접속 |

검증 쿼리는 뷰로 고정돼 있다.

```sql
-- 공식 캔들 대비 차이. missing=1 은 누락, write_count>1 은 중복 기록.
SELECT * FROM rtp.candle_diff WHERE missing OR write_count > 1;
```

### 장애 실험용 계측 (선택)

Prometheus·Grafana는 오버레이로 분리돼 있다. 실험 단계에서 켠다.

```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.observability.yml up -d
```

Grafana http://localhost:3000 · Prometheus http://localhost:9090

## 스택

Kafka · Apache Flink (Kubernetes Operator) · ClickHouse · Kubernetes
