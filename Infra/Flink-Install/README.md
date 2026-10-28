# Flink Kubernetes Operator

Flink job을 Kubernetes에 띄우는 Operator다. `FlinkDeployment` 리소스를 만들면 Operator가 JobManager · TaskManager pod를 띄우고 job을 제출한다.

| 항목 | 값 |
| --- | --- |
| Operator | 1.16.1 (Flink 2.2 지원 — `flinkVersion: v2_2`) |
| Operator namespace | `flink-operator` |
| job namespace | `binance-processing` (`watchNamespaces`) |
| webhook | 끔 (cert-manager 없이 설치) |
| 설정 | [values.yaml](values.yaml) |

## 설치

```bash
helm repo add flink-operator-repo https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.16.1/
helm repo update

# job namespace를 먼저 만든다 (helm이 여기에 서비스 계정 flink와 RBAC를 만든다)
kubectl create namespace binance-processing

helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --version 1.16.1 -n flink-operator --create-namespace -f Infra/Flink-Install/values.yaml

kubectl get pods -n flink-operator
kubectl get sa -n binance-processing flink
```

## job 띄우기

job 매니페스트는 앱 쪽에 있다: [consumer/flink-app/deploy/](../../consumer/flink-app/deploy/).

```bash
kubectl apply -f consumer/flink-app/deploy/dedup.yaml
kubectl get flinkdeployment -n binance-processing       # JOB STATUS가 RUNNING이면 정상
kubectl get pods -n binance-processing                   # flink-dedup-xxx (JobManager), flink-dedup-taskmanager-1-1
kubectl logs -n binance-processing deploy/flink-dedup -f # JobManager 로그
```

Flink Web UI:

```bash
kubectl port-forward -n binance-processing svc/flink-dedup-rest 8081:8081   # http://localhost:8081
```

## 왜 이렇게 설정했나

- **webhook을 끈 이유:** webhook은 TLS가 필요해서 cert-manager를 같이 설치해야 한다. 로컬 클러스터에서는 설치할 것과 메모리를 줄이는 쪽을 골랐다. 대신 `FlinkDeployment`를 잘못 써도 apply는 성공하고, 에러는 `kubectl describe flinkdeployment`의 status에서 확인한다.
- **watchNamespaces를 지정한 이유:** Operator 권한을 job namespace로 좁힌다. job namespace가 늘면 여기에 추가하고 `helm upgrade`한다.

## 메모리

| pod | 메모리 |
| --- | --- |
| Operator | 512Mi |
| JobManager (job마다) | 1024m |
| TaskManager (job마다) | 1536m |

job 하나에 약 2.5GiB가 든다. Kafka 브로커 3대와 같은 PC에 올리면 메모리가 부족할 수 있다.
