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

## 배포 — Helm 차트 + ArgoCD

매니페스트는 `charts/rtp` Helm 차트 하나로 모았다. kustomize 를 쓰다가 옮겼는데,
**두 소스를 병행하면 어느 쪽이 진짜인지 모르게 되기 때문**이다.

```bash
# ArgoCD 설치
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.13.2/manifests/install.yaml

# 저장소를 클러스터에 연결
kubectl apply -f k8s/argocd/application.yaml
```

이후로는 **저장소가 클러스터 상태의 근거**다. `kubectl apply` 로 손댄 것은
다음 동기화에서 되돌아간다(`selfHeal: true`). "누가 언제 무엇을 바꿨나" 가
`git log` 로 답해진다.

차트만 직접 쓰려면:

```bash
helm template rtp charts/rtp --namespace rtp | kubectl apply -f -
```

### 스키마 SQL 을 차트가 소유한다

`charts/rtp/files/clickhouse/*.sql` 이 단일 소스이고 compose 가 그걸 마운트한다.
Helm 의 `.Files` 도, kustomize 도 **차트/루트 밖 파일을 못 읽는다.**
그래서 소유권을 차트로 옮기고 compose 가 참조하는 쪽으로 뒤집었다 -
복사본을 두면 한쪽만 고쳐지고 두 환경의 검증 결과를 비교할 수 없다.

## 스키마 변경 시 주의

ClickHouse 는 **데이터 디렉터리가 비어 있을 때만** `docker-entrypoint-initdb.d` 를 실행한다.
PVC 가 이미 차 있으면 초기화 SQL 이 건너뛰어진다. 스키마를 바꾸면 둘 중 하나다.

```bash
# 수동 적용
kubectl exec -n rtp -i clickhouse-0 -- clickhouse-client --user rtp --password rtp   --multiquery < infra/clickhouse/init/01_schema.sql

# 또는 PVC 를 지우고 재생성 (데이터 손실)
kubectl delete pvc data-clickhouse-0 -n rtp
```

## 잡 배포 (Flink Operator, Application 모드)

```bash
# 1. jar 빌드 -> 이미지 -> 클러스터 노드에 적재
./mvnd.sh -pl aggregator -am package -DskipTests
docker build -f aggregator/Dockerfile -t rtp-aggregator:0.1.0 .
kind load docker-image rtp-aggregator:0.1.0 --name rtp

# 2. RBAC (Operator 헬름 차트는 자기 네임스페이스에만 만든다)
kubectl apply -f k8s/flink/rbac.yaml

# 3. 잡
kubectl apply -f k8s/flink/candle-job.yaml
kubectl get flinkdeployment -n rtp -w
```

### compose 와 달라진 것

| | compose | K8s |
|---|---|---|
| 제출 | `flink run` (명령형) | `FlinkDeployment` CR (선언적) |
| 체크포인트 | 호스트 볼륨 공유 | **MinIO(S3)** |
| 재배포 | 사람이 savepoint 뜨고 재개 | `upgradeMode: savepoint` 로 Operator 가 처리 |
| 재시작 전략 | `FLINK_PROPERTIES` | `flinkConfiguration` |

체크포인트를 S3 에 두는 이유는 JM/TM 이 서로 다른 노드에 뜨기 때문이다.
kind 기본 StorageClass 는 ReadWriteOnce 라 노드를 넘어 공유되지 않는다.

`imagePullPolicy: Never` 가 필요하다. `kind load` 로 노드에 적재한 로컬 이미지를
레지스트리에서 다시 받으려 하면 ImagePullBackOff 가 난다.

## 검증된 구성 (2026-08-31)

| 구성요소 | 버전 |
|---|---|
| kind | 0.33.0 |
| Kubernetes | v1.37.0 |
| cert-manager | v1.16.2 |
| Flink Kubernetes Operator | 1.15.0 |
