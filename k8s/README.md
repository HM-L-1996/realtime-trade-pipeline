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

## 검증된 구성 (2026-08-31)

| 구성요소 | 버전 |
|---|---|
| kind | 0.33.0 |
| Kubernetes | v1.37.0 |
| cert-manager | v1.16.2 |
| Flink Kubernetes Operator | 1.15.0 |
