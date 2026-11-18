# Monitoring

클러스터에 이미 설치된 kube-prometheus-stack(Prometheus · Grafana · Alertmanager)을 이 프로젝트에서 어떻게 쓰는지 정리한다. 무엇을 볼지는 [metrics.md](metrics.md).

| 항목 | 값 |
| --- | --- |
| helm 릴리스 | `monitoring` (namespace `monitoring`), chart `kube-prometheus-stack-85.2.0`, 기본값 그대로 (`helm get values`가 비어 있음) |
| 구성 | Prometheus 1, Grafana 1, Alertmanager 1, kube-state-metrics, node-exporter (노드 4대) |
| Grafana 주소 | http://graf.monitoring.dclab2.dcusoft.net (이 프로젝트 밖에서 만든 Ingress `grafana-ingress`) |
| Grafana 계정 | Secret `monitoring-grafana` (`admin-user`, `admin-password`) |
| Prometheus 주소 (클러스터 안) | `http://monitoring-kube-prometheus-prometheus.monitoring.svc:9090` |
| Prometheus 보관 기간 | 10일 |

## 접속

```bash
# Grafana 계정
kubectl get secret -n monitoring monitoring-grafana -o jsonpath='{.data.admin-user}' | base64 -d; echo
kubectl get secret -n monitoring monitoring-grafana -o jsonpath='{.data.admin-password}' | base64 -d; echo

# Prometheus UI (쿼리를 먼저 시험해 볼 때)
kubectl port-forward -n monitoring svc/monitoring-kube-prometheus-prometheus 9090
```

Grafana는 Service가 ClusterIP라 `kubectl get svc`에서는 포트가 안 보인다. ingress-nginx(NodePort 31299)가 Host 헤더 `graf.monitoring.dclab2.dcusoft.net`를 보고 Grafana로 넘긴다. 도메인이 안 잡히는 PC는 hosts 파일에 `192.168.12.1 graf.monitoring.dclab2.dcusoft.net`를 넣고 `:31299`로 연다.

## 우리 앱 메트릭을 Prometheus에 붙이는 법

Prometheus는 **라벨 `release: monitoring`이 붙은** ServiceMonitor · PodMonitor만 읽는다 (namespace는 가리지 않는다).

```bash
kubectl get prometheus -n monitoring monitoring-kube-prometheus-prometheus \
  -o jsonpath='{.spec.serviceMonitorSelector} {.spec.podMonitorSelector}'
# {"matchLabels":{"release":"monitoring"}} {"matchLabels":{"release":"monitoring"}}
```

- Service가 있는 앱(수집 앱, MinIO) → ServiceMonitor
- pod를 Operator가 만드는 앱(Flink TaskManager, Strimzi Kafka) → PodMonitor
- 매니페스트는 각 앱 옆에 둔다 (예: `consumer/flink-app/deploy/`). 이 폴더에는 모니터링 스택 자체의 것만 둔다.

## 대시보드 관리

Grafana pod에 sidecar(`grafana-sc-dashboard`)가 있어, **라벨 `grafana_dashboard: "1"`이 붙은 ConfigMap**의 JSON을 대시보드로 자동으로 불러온다. UI에서 만든 대시보드는 Export → JSON으로 받아 ConfigMap으로 저장한다.

## 지금 단순화한 것

- **Prometheus · Grafana에 PVC가 없다** (`kubectl get pvc -n monitoring`이 비어 있음). pod가 다시 뜨면 Prometheus 메트릭과 UI에서 만든 대시보드가 사라진다. 대시보드는 위처럼 ConfigMap으로 남기고, 메트릭 보관이 중요해지면 helm values에 `prometheus.prometheusSpec.storageSpec`을 넣는다.
- **설치 values가 git에 없다.** 이 프로젝트 밖에서 helm 차트를 기본값으로 설치했다. 바꿀 일이 생기면 `Infra/Monitoring/values.yaml`을 만들고 `helm upgrade monitoring prometheus-community/kube-prometheus-stack --version 85.2.0 -n monitoring -f values.yaml`로 반영한다.
- Alertmanager는 떠 있지만 알림 받을 곳(Slack 등)이 설정돼 있지 않다.
