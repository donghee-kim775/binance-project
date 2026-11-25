# Binance / 노드 · pod 운영 상태

[metrics.md](../../metrics.md) 0절의 "어디가 죽지 않았나?"와 1 · 2 · 3-3 · 4 · 5절의 ✅ 항목(자원 · pod 상태)을 한 화면에 모은 대시보드다. 데이터가 흐르는지는 [binance-raw-ingest](../binance-raw-ingest/)가 보고, 이 대시보드는 **그걸 돌리는 노드와 pod가 건강한지**를 본다.

| 항목 | 값 |
| --- | --- |
| Grafana | http://graf.monitoring.dclab2.dcusoft.net/d/binance-cluster-health |
| 정의 | [dashboard.yaml](dashboard.yaml) (ConfigMap, 라벨 `grafana_dashboard: "1"`) |
| 지표 출처 | kube-prometheus-stack 기본 수집. node-exporter(노드), cAdvisor(pod CPU · 메모리 · 네트워크 · 디스크), kube-state-metrics(pod · 노드 상태). **앱 쪽 설정이 필요 없다** |
| 변수 | `datasource` (Prometheus) |

```bash
kubectl apply -f Infra/Monitoring/dashboards/binance-cluster-health/dashboard.yaml
```

| 줄 | 패널 |
| --- | --- |
| 현상황 | 노드 Ready · 수집기 Ready / 목표 · 처리기 Ready / 전체 · Kafka broker Ready · MinIO Ready · pod 재시작 (1시간) |
| 노드 | CPU · 메모리 · 디스크 사용률, 메모리 요청 / 할당 가능, 네트워크 수신 · 송신 |
| 수집기 (`binance-ingestion`) | pod 상태 표, pod별 CPU · 메모리 |
| 처리기 (`binance-processing`) | pod 상태 표, pod별 CPU · 메모리, 메모리 / limit, CPU throttling |
| Kafka (`binancekafka`) | pod 상태 표, broker별 CPU · 메모리 · 네트워크 수신 · 송신 · 디스크 쓰기 |
| 저장소 (`binance-storage`) | pod 상태 표, pod별 CPU · 메모리 |

## 노드 이름 붙이기

node-exporter 지표의 `instance`는 `192.168.12.1:9100`처럼 IP다. 노드 이름은 `node_uname_info`의 `nodename` 라벨에 있어 join해서 붙인다.

```promql
(1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])))
  * on(instance) group_left(nodename) node_uname_info
```

- `node_uname_info`는 값이 항상 1이라 곱해도 값은 그대로고 라벨만 붙는다.
- `on(instance)`: instance가 같은 것끼리 짝짓는다. `group_left(nodename)`: 오른쪽의 nodename 라벨을 결과에 가져온다. Python으로 치면 `df.merge(uname[["instance", "nodename"]], on="instance")`.
- kube-state-metrics 지표(`kube_node_*`, `kube_pod_info`)는 처음부터 `node` 라벨이 노드 이름이라 join이 필요 없다.

## 현상황 줄

| 패널 | 쿼리 | 정상 |
| --- | --- | --- |
| 노드 Ready | `sum(kube_node_status_condition{condition="Ready", status="true"})` | 4 (그 아래면 빨강) |
| 수집기 Ready / 목표 | `sum(kube_pod_status_ready{namespace="binance-ingestion", condition="true"})`, `sum(kube_deployment_spec_replicas{namespace="binance-ingestion"})` | 두 값이 같음 |
| 처리기 Ready / 전체 | `sum(kube_pod_status_ready{namespace="binance-processing", condition="true"})`, `count(kube_pod_info{namespace="binance-processing"})` | 두 값이 같음. job 하나에 4 (JobManager 1 + TaskManager 3) |
| Kafka broker Ready | broker pod(`binance-cluster-binance-cluster-pool-.*`) Ready 수 | 3. 2면 노랑 (쓰기는 되지만 한 대 더 빠지면 `min.insync.replicas=2`를 못 채움) |
| MinIO Ready | `minio-.*` Ready 수 | 4. 3이면 노랑, 2 이하면 빨강 (쓰기가 멈춰 체크포인트 실패) |
| pod 재시작 (1시간) | `sum(increase(kube_pod_container_status_restarts_total{namespace=~"binance.*"}[1h]))` | 0. 1 이상 노랑, 3 이상 빨강 |

- **수집기는 Deployment replicas와 비교하고, 처리기는 pod 수와 비교한다.** TaskManager pod는 Deployment가 아니라 JobManager가 직접 만들어서 "목표 수"가 kube-state-metrics에 없다. 그래서 처리기는 "있는 pod 중 몇 개가 Ready인가"만 본다. TaskManager가 아예 안 뜬 경우는 이 패널로 잡히지 않는다 (전체가 4보다 작아지는 것으로 본다).
- 수집 앱을 내려 두면(`kubectl delete -k`) Deployment가 없어서 두 값 모두 `-`다.

## pod 상태 표

namespace마다 같은 표다. 쿼리 6개를 instant(지금 값)로 받아 Grafana transform `merge`로 `pod` 열이 같은 행끼리 합친다.

| 열 | 쿼리 | 보는 것 |
| --- | --- | --- |
| 노드 | `kube_pod_info`의 `node` 라벨 | 어느 노드에 떴나. 노드가 하나 빠졌을 때 영향받는 pod를 찾는다 |
| Ready | `kube_pod_status_ready{condition="true"}` | 1 = Ready(초록), 0 = NotReady(빨강) |
| 재시작 | `sum by (pod) (kube_pod_container_status_restarts_total)` | pod가 생긴 뒤 누적. 1 이상 노랑, 5 이상 빨강 |
| 가동 시간 | `time() - kube_pod_start_time` | 방금 다시 떴는지 |
| 대기 이유 | `kube_pod_container_status_waiting_reason > 0`의 `reason` | `CrashLoopBackOff`, `ImagePullBackOff`(태그 오타, Docker Hub push 안 함) |
| 마지막 종료 이유 | `kube_pod_container_status_last_terminated_reason > 0`의 `reason` | `OOMKilled`(메모리 한도), `Error` |

- 대기 이유와 종료 이유는 kube-state-metrics에서 둘 다 `reason`이라는 같은 라벨 이름을 쓴다. 그대로 합치면 한 열로 섞여서, `label_replace`로 `waiting`, `terminated`로 이름을 바꾼 뒤 합친다.
- 문제가 없으면 두 열은 `-`다.

## 노드 줄

| 패널 | 계산 | 정상 / 이상 신호 |
| --- | --- | --- |
| CPU 사용률 | 1 − idle 비율 (5분 평균) | 70% 노랑, 85% 빨강. 노드마다 core 수가 달라(dclabnode1 16, 2 8, 3 · 4 12) 같은 %라도 여유는 다르다 |
| 메모리 사용률 | 1 − `MemAvailable` ÷ `MemTotal` | page cache는 회수할 수 있어 사용으로 치지 않는다 |
| 디스크 사용률 (/) | 루트 파일시스템의 1 − 가용 ÷ 전체 | local-path PVC(Kafka 로그, MinIO, ClickHouse)가 이 디스크 위에 있다 |
| 메모리 요청 / 할당 가능 | pod memory request 합 ÷ 노드 allocatable | **실제 사용량이 아니라 스케줄러가 보는 값.** 100%에 가까우면 새 pod(Flink job 추가 등)가 Pending이 된다 |
| 네트워크 수신 · 송신 | 물리 NIC(`en*`, `eth*`)만 | Kafka 복제 트래픽이 노드 사이에 오간다. 가상 NIC(cali*, veth*)를 같이 더하면 같은 트래픽이 두 번 잡힌다 |

- **PVC별 사용량은 볼 수 없다.** `kubelet_volume_stats_*`가 비어 있다. local-path provisioner는 노드 디렉터리를 그대로 마운트해서 kubelet이 볼륨 용량을 따로 재지 않는다. 그래서 노드 디스크 사용률로 대신 본다.

## 처리기 줄

| 패널 | 계산 | 정상 / 이상 신호 |
| --- | --- | --- |
| 메모리 / limit | `flink-main-container` working set ÷ memory limit | 80% 노랑, 90% 빨강. TaskManager 1536Mi, JobManager 1024Mi. state가 heap에 있어 100%면 OOMKilled |
| CPU throttling | throttled 주기 ÷ 전체 CFS 주기 | 25% 노랑, 50% 빨강. CPU limit(0.5 core)에 걸려 멈춘 비율. 높으면 처리 · 체크포인트가 늦어진다 |

- working set은 JVM heap만이 아니라 metaspace, 네트워크 버퍼, 스레드 스택까지 포함한 컨테이너 전체 메모리다. heap만 따로 보려면 Flink reporter의 JVM 지표가 필요하다 ([metrics.md](../../metrics.md) 3-3절).

## Kafka 줄

| 패널 | 계산 | 보는 것 |
| --- | --- | --- |
| CPU (broker) | broker pod CPU core | **broker에 CPU · 메모리 limit이 없어** "한도 대비 %"가 아니라 core 수로 본다. 노드 CPU 사용률과 같이 본다 |
| 메모리 (broker) | broker pod working set | JVM heap + page cache 일부. limit이 없어 노드 메모리 사용률과 함께 본다 |
| 네트워크 수신 · 송신 | broker pod NIC | 수신 = producer 쓰기 + 복제 받기, 송신 = Flink 읽기 + 복제 보내기 |
| 디스크 쓰기 | `container_fs_writes_bytes_total` rate | 로그 세그먼트에 쌓이는 속도. 보존 기간을 곱하면 필요한 디스크 크기가 나온다 |

- broker 내부 지표(under-replicated 파티션, 요청 처리 시간)는 JMX exporter가 필요해 여기에 없다 ([metrics.md](../../metrics.md) 2절).

## 상황별로 보이는 모습

| 상황 | 보이는 곳 |
| --- | --- |
| 노드 하나 다운 | 노드 Ready 3, 그 노드의 Kafka broker · MinIO가 같이 빠져 broker Ready 2 · MinIO Ready 3 |
| Flink TaskManager OOM | 처리기 표 마지막 종료 이유 `OOMKilled`, 재시작 +1, 메모리 / limit이 100% 근처였다가 끊김 |
| 이미지 태그 오타 · push 안 함 | 표 대기 이유 `ImagePullBackOff`, Ready / 목표가 어긋남 |
| 수집 pod 하나 죽음 | 수집기 Ready 1 / 목표 2 ([binance-raw-ingest](../binance-raw-ingest/)에서는 유입이 약 절반) |
| 새 Flink job이 Pending | 처리기 전체 수가 늘지 않음, 노드 메모리 요청 / 할당 가능이 높음 |
