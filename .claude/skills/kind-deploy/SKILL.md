---
name: kind-deploy
description: 수집 앱 이미지를 빌드해 kind 클러스터에 배포하고 pod 상태와 로그를 확인한다.
argument-hint: "[이미지 태그, 기본값 0.0.1]"
disable-model-invocation: true
---

# kind 배포

이미지 태그: `$ARGUMENTS` (비어 있으면 `0.0.1`)

## 0. 사전 확인

```bash
kubectl version --client          # 실패하면 Docker Desktop이 꺼져 있는 것 → 사용자에게 알리고 중단
kind get clusters                 # 클러스터 이름 확인 (기본 kind)
kubectl get nodes
```

## 1. 빌드와 로드

```bash
cd datasource-ingestion
docker build -f deploy/Dockerfile -t ingestion-collector:<태그> collector
kind load docker-image ingestion-collector:<태그> --name <클러스터>
```

태그가 `0.0.1`이 아니면 deploy yaml의 `image:`도 같은 태그인지 확인한다. 다르면 고치기 전에 사용자에게 묻는다.

## 2. 렌더링 검사와 적용

```bash
kubectl kustomize deploy > /dev/null
kubectl apply -k deploy
```

## 3. 확인

```bash
kubectl get pods -n binance-ingestion -L rate-group -o wide
kubectl rollout status deployment -n binance-ingestion --timeout=120s
```

- variable-rate는 pod 2개가 **서로 다른 노드**에 있는지 확인한다.
- 각 pod 로그에서 WebSocket `connected`와 메시지 수신을 확인한다.
  ```bash
  kubectl logs -n binance-ingestion deploy/<이름> --tail=20
  ```
- `CrashLoopBackOff`, `ImagePullBackOff`가 있으면 `kubectl describe pod`로 원인을 찾는다. `ImagePullBackOff`는 대부분 `kind load`를 안 했거나 태그가 다른 경우다.

## 4. 보고

deployment별로 준비된 replicas 수, 노드 분산, 연결 상태를 표로 정리한다. 실패한 것은 원인과 함께 적는다.
