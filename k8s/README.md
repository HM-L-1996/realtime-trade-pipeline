# K8s 이전

로컬 docker-compose 에서 검증이 끝난 파이프라인을 K8s 로 옮긴다.
**설계 판단과 실험 기록은 `../docs/` 에 있다.** 여기는 절차만 둔다.

## 왜 kind 인가

Docker Desktop 의 Kubernetes 토글 대신 kind 를 쓴다.

- **멀티 노드가 된다.** "TaskManager Pod 강제 종료 → 다른 노드로 재스케줄" 실험이
  단일 노드에서는 성립하지 않는다
- **클러스터 구성이 파일로 남는다.** 지우고 다시 만들어도 같은 구성이 나와야
  실험 결과를 비교할 수 있다. GUI 토글은 재현이 안 된다
- Docker Desktop 설정을 건드리지 않는다

## 구축 순서

```bash
# 1. 클러스터 (control-plane 1 + worker 2)
kind create cluster --config k8s/cluster/kind-cluster.yaml

# 2. cert-manager — Flink Operator 의 웹훅이 요구한다
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml
kubectl wait --for=condition=Available deployment --all -n cert-manager --timeout=300s

# 3. Flink Kubernetes Operator
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink --create-namespace --wait
```

> Operator 버전은 `https://downloads.apache.org/flink/` 에 **현재 배포 중인 것만** 있다.
> 지난 버전은 404 가 난다(archive.apache.org 로 옮겨간다). 설치 전에 목록을 확인할 것.

## 매니페스트 적용

```bash
kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/base | kubectl apply -f -
```

### 왜 `--load-restrictor` 가 필요한가

ClickHouse 스키마 SQL 을 `infra/clickhouse/init/` 에서 그대로 가져다 ConfigMap 으로 만든다.
compose 와 **같은 파일**을 써야 두 환경의 스키마가 갈리지 않는다 -
복사본을 두면 한쪽만 고쳐지고, 그러면 검증 결과를 비교할 수 없다.

kustomize 는 기본적으로 루트 밖 파일 참조를 막는다. 그래서 이 플래그가 필요하다.
`kubectl apply -k` 에는 이 플래그가 없으므로 `kubectl kustomize | kubectl apply -f -` 로 넘긴다.

> ArgoCD 로 배포할 때는 `argocd-cm` 에 `kustomize.buildOptions: --load-restrictor LoadRestrictionsNone`
> 를 넣어야 한다. 잊으면 ConfigMap 생성 단계에서 실패한다.

## 스키마 변경 시 주의

ClickHouse 는 **데이터 디렉터리가 비어 있을 때만** `docker-entrypoint-initdb.d` 를 실행한다.
PVC 가 이미 차 있으면 초기화 SQL 이 건너뛰어진다. 스키마를 바꾸면 둘 중 하나다.

```bash
# 수동 적용
kubectl exec -n rtp -i clickhouse-0 -- clickhouse-client --user rtp --password rtp   --multiquery < infra/clickhouse/init/01_schema.sql

# 또는 PVC 를 지우고 재생성 (데이터 손실)
kubectl delete pvc data-clickhouse-0 -n rtp
```

## 검증된 구성 (2026-08-31)

| 구성요소 | 버전 |
|---|---|
| kind | 0.33.0 |
| Kubernetes | v1.37.0 |
| cert-manager | v1.16.2 |
| Flink Kubernetes Operator | 1.15.0 |
