# 무엇을 모니터링할 것인가

수집 앱 → Kafka raw → Flink dedup → Kafka clean → (ClickHouse) 파이프라인에서 Grafana로 볼 지표를 계층별로 정리한다. Prometheus · Grafana 사용법은 [README.md](README.md).

각 지표의 **상태**:
- ✅ 지금 볼 수 있음 (kube-state-metrics · cAdvisor · node-exporter, 설정 불필요)
- 🔧 메트릭 노출 설정이 필요함 (각 절 맨 아래 "붙이는 법")

## 스트리밍 지표 모음

스트리밍 파이프라인에서 데이터가 **얼마나 들어오고, 얼마나 처리되고, 얼마나 늦고, 밀리지 않는지**를 보는 지표를 먼저 모았다. 자원(CPU · 메모리)이나 pod 상태 지표는 아래 절에 있다.

- Prometheus 이름은 exporter를 붙인 기본 이름 규칙을 따른 것이다. 붙인 뒤 Prometheus UI에서 실제 이름을 한 번 확인한다.
- Flink 지표는 `flink_taskmanager_job_task_operator_` 같은 접두어가 붙고, 라벨 `operator_name`(`raw.<routeId>`, `dedup.<routeId>` …)으로 route를 나눈다.

| 분류 | 지표 | Prometheus 이름 (예상) | 출처 | 정상 | 상태 |
| --- | --- | --- | --- | --- | --- |
| 유입량 | 수집 앱 Kafka 발행 건수 | `kafka_producer_record_send_total` (rate) | 수집 앱 micrometer | pod 2개가 비슷한 값 | 🔧 |
| 유입량 | 수집 앱 발행 에러 | `kafka_producer_record_error_total` | 수집 앱 micrometer | 0 | 🔧 |
| 유입량 | 스트림별 WebSocket 수신 건수 | 직접 만드는 카운터 | 수집 앱 코드 | 연결된 스트림은 모두 0보다 큼 | 🔧 |
| 유입량 | 토픽 초당 메시지 수 (raw · clean) | `rate(kafka_topic_partition_current_offset[1m])` 를 `topic`으로 합침 | kafka-exporter | raw ≈ clean × 2 | ✅ |
| 유입량 | 파티션별 최신 offset | `kafka_topic_partition_current_offset` | kafka-exporter | 파티션이 고르게 증가 (지금은 BTCUSDT 하나라 파티션 2만 증가) | ✅ |
| 처리량 | Flink 처리 속도 (커밋 기준) | `rate(kafka_consumergroup_current_offset[1m])` 를 `topic`으로 합침 | kafka-exporter | ≈ 유입 (10초 단위로 거침) | ✅ |
| 처리량 | 연산자 초당 입력 · 출력 | `..._operator_numRecordsInPerSecond`, `..._operator_numRecordsOutPerSecond` | Flink reporter | parse 입력 = 출력, dedup 출력 ≈ 입력 ÷ 2 | 🔧 |
| 처리량 | dedup 비율 (입력 ÷ 출력) | 위 두 값으로 계산 | Flink reporter | ≈ 2.0 | 🔧 |
| 처리량 | sink 전송 건수 · 에러 | `..._operator_numRecordsSend`, `..._operator_numRecordsSendErrors` | Flink reporter (KafkaSink) | 에러 0 | 🔧 |
| 밀림 | Flink 실시간 lag | `..._KafkaSourceReader_KafkaConsumer_records_lag_max`, `..._operator_pendingRecords` | Flink reporter (KafkaSource) | 0 근처 | 🔧 |
| 밀림 | consumer group lag (커밋 기준) | `kafka_consumergroup_lag` (빈 파티션은 -1) | kafka-exporter | 10초 톱니, 바닥이 오르지 않음 | ✅ |
| 밀림 | backpressure | `..._task_isBackPressured`, `..._task_backPressuredTimeMsPerSecond` | Flink reporter | 0 | 🔧 |
| 밀림 | 연산자 바쁨 정도 | `..._task_busyTimeMsPerSecond`, `..._task_idleTimeMsPerSecond` | Flink reporter | busy가 1000(100%)에 붙지 않음 | 🔧 |
| 지연 | 수집 앱 → dedup 지연 (`dedup_time − receive_time`) | 지금은 Kafka 데이터를 꺼내 계산 | 직접 측정 | 평소 p50 37 · p90 92ms (2026-11-18) | 수동 |
| 지연 | source 이벤트 시각 지연 | `..._operator_currentEmitEventTimeLag` | Flink reporter (KafkaSource) | 수십 ms (raw `CreateTime` 기준) | 🔧 |
| 지연 | producer 요청 지연 | `kafka_producer_request_latency_avg` | 수집 앱 micrometer | 수 ms | 🔧 |
| 지연 | broker Produce · Fetch 처리 시간 | `kafka_network_requestmetrics_totaltimems` (라벨 `request`) | Strimzi JMX | 평소 값 유지 | 🔧 |
| 체크포인트 | 소요 시간 | `flink_jobmanager_job_lastCheckpointDuration` | Flink reporter | 수 초 (지금 약 2초) | 🔧 |
| 체크포인트 | 크기 (state) | `flink_jobmanager_job_lastCheckpointSize` | Flink reporter | 일정 범위 (TTL 1분) | 🔧 |
| 체크포인트 | 완료 · 실패 횟수 | `flink_jobmanager_job_numberOfCompletedCheckpoints`, `flink_jobmanager_job_numberOfFailedCheckpoints` | Flink reporter | 완료는 10초마다 +1, 실패는 늘지 않음 | 🔧 |
| 체크포인트 | job 재시작 | `flink_jobmanager_job_numRestarts` | Flink reporter | 늘지 않음 | 🔧 |

- **지연을 상시 지표로 보는 방법은 아직 없다.** `currentEmitEventTimeLag`는 raw `CreateTime` 기준이라 수집 앱 내부(0~1ms)를 뺀 Kafka → Flink 구간에 가깝다. dedup → clean 구간은 [plan/performance/plan.md](../../plan/performance/plan.md) 2번 개선 사항.
- dedup job은 watermark(이벤트 시간)를 쓰지 않아 `currentOutputWatermark` 계열은 볼 것이 없다. 이벤트 시간 window를 쓰는 job(basis 등)이 생기면 추가한다.

## 0. 한눈에 볼 다섯 가지

대시보드 맨 위에 둘 질문이다. 하나라도 "아니오"면 아래 절로 내려가 원인을 찾는다.

| 질문 | 보는 지표 | 정상 |
| --- | --- | --- |
| 데이터가 들어오고 있나? | raw 토픽 초당 메시지 수 | 0보다 큼 (Spot BTC bookTicker만 켰을 때 약 365/s) |
| 중복이 제대로 걸러지나? | dedup 입력 ÷ 출력 | 약 2.0 (Active-Active pod 2개) |
| Flink가 밀리지 않나? | raw 토픽 consumer lag | 0 근처에서 오르내림, 계속 늘지 않음 |
| 장애가 나도 이어갈 수 있나? | 마지막 체크포인트 성공 후 지난 시간 | 10~20초 이내 (간격 10s) |
| 어디가 죽지 않았나? | pod 재시작 수, Ready 상태 | 재시작이 늘지 않음 |

2026-11-18에 spot-sov만 켜고 60초 동안 잰 값: raw +21,888, clean +10,865, 비율 2.015.

**raw 유입 · Flink 처리 속도 · consumer lag는 지금 Grafana에서 볼 수 있다.** 대시보드 이름은 `Binance / raw 유입 vs Flink 처리`이고, 정의와 패널별 계산 방법은 [dashboards/binance-raw-ingest/](dashboards/binance-raw-ingest/)에 있다. 데이터는 Kafka Exporter에서 온다.
- lag는 체크포인트 간격(10초)마다 톱니처럼 오르내린다. 그래서 "계속 늘지 않음"은 패널 **Lag 증가 속도**(유입 − 처리, 10분 평균)로 본다.
- lag가 `-1`이면 한 번도 커밋하지 않은 빈 파티션이다. 쿼리에서 `clamp_min(…, 0)`으로 0으로 바꾼다.

## 1. 수집 앱 (`binance-ingestion`)

| 지표 | 왜 보나 | 정상 / 이상 신호 | 상태 |
| --- | --- | --- | --- |
| pod Ready 수 / replicas | variable-rate는 2개가 같이 떠 있어야 Active-Active | 2/2. 1/2면 중복이 사라져 dedup 비율이 1.0으로 떨어짐 | ✅ |
| pod 재시작 수 | WebSocket · Kafka 연결 문제로 죽는지 | 늘지 않음 | ✅ |
| CPU · 메모리 | JSON 파싱 · 로그 출력 부담 | 지금 메시지마다 INFO 로그를 찍어 pod당 약 180줄/s. 스트림을 늘릴 때 먼저 확인 | ✅ |
| Kafka producer 전송률 · 에러 수 | raw 토픽에 실제로 들어가는지 | 에러 0 | 🔧 |
| producer 요청 지연 | broker가 느려지는지 | 수 ms 수준 유지 | 🔧 |
| WebSocket 재연결 횟수 (스트림별) | Binance는 연결을 24시간마다 끊는다. 그 외 재연결이 잦으면 네트워크 · 구독 문제 | 하루 1번 안팎 | 🔧 (코드에 카운터 추가) |
| 스트림별 수신 건수 | USD-M 경로 접두어(`/public`, `/market`)가 틀리면 연결은 되는데 메시지가 0 | 연결된 스트림은 모두 0보다 큼 | 🔧 (코드에 카운터 추가) |
| JVM heap · GC 시간 | 메모리 누수 | heap이 계단식으로만 오르지 않음 | 🔧 |

**붙이는 법:** `spring-boot-starter-actuator` + `micrometer-registry-prometheus` 의존성 추가 → `management.endpoints.web.exposure.include: prometheus` → 이미지 다시 빌드 → Service + ServiceMonitor. Spring Kafka가 producer 메트릭을 micrometer로 자동 등록한다.

## 2. Kafka (`binancekafka`)

| 지표 | 왜 보나 | 정상 / 이상 신호 | 상태 |
| --- | --- | --- | --- |
| broker pod Ready · 재시작 | broker 3개 중 하나라도 빠지면 replica 부족 | 3/3 | ✅ |
| broker 디스크(PVC) 사용량 | raw · clean retention(24h) 동안 쌓이는 양 | 여유 30% 이상 | ✅ |
| 토픽별 초당 메시지 수 | raw와 clean을 나란히 놓으면 0절의 비율이 보임 | raw ≈ clean × 2 | ✅ (kafka-exporter) |
| **consumer lag** (Flink 그룹, raw 토픽) | Flink가 수집 속도를 따라가는지. 파이프라인 지연의 핵심 지표 | 0 근처. 계속 늘면 Flink가 느리거나 멈춤 | ✅ (kafka-exporter) |
| under-replicated 파티션 수 | broker 하나가 뒤처지거나 빠짐 | 0 | 🔧 |
| offline 파티션 수 | 읽기 · 쓰기가 안 되는 파티션 | 0 (0이 아니면 바로 장애) | 🔧 |
| 요청 처리 시간 (Produce · Fetch) | broker가 느려지는지 | 평소 값 유지 | 🔧 |

**붙이는 법:**
- **`kafkaExporter`(consumer lag · 토픽 offset): 적용됨 (2026-11-18).** 설정은 [kafka-cluster.yaml](../Kafka/Kafka-Cluster/kafka-cluster.yaml)의 `spec.kafkaExporter`와 [kafka-exporter-podmonitor.yaml](../Kafka/Kafka-Cluster/kafka-exporter-podmonitor.yaml)에 있다. 별도 pod로 떠서 broker는 재시작되지 않았다.
- **broker JMX 지표(under-replicated, 요청 처리 시간 등): 아직 안 함.** Strimzi `Kafka` 리소스에 `metricsConfig`(JMX Prometheus exporter, ConfigMap 규칙)를 넣으면 broker가 롤링 재시작된다. 그다음 PodMonitor를 만든다. Strimzi 예제 저장소의 Grafana 대시보드(`strimzi-kafka.json`, `strimzi-kafka-exporter.json`)를 그대로 쓸 수 있다.

- Flink Kafka source는 **체크포인트가 끝날 때** offset을 커밋한다. 그래서 kafka-exporter가 보는 lag는 체크포인트 간격(10s)만큼 톱니 모양으로 오르내린다. 정확한 실시간 lag는 Flink 쪽 지표(3절 `records-lag-max`)로 본다.

## 3. Flink dedup (`binance-processing`)

### 3-1. 처리량과 중복 제거

| 지표 | 왜 보나 | 정상 / 이상 신호 | 상태 |
| --- | --- | --- | --- |
| 연산자별 초당 입력 · 출력 건수 (`numRecordsInPerSecond`, `numRecordsOutPerSecond`) | `raw.*` → `parse.*` → `dedup.*` → `clean.*` 단계마다 건수가 어떻게 줄어드는지 | parse 입력 = 출력 (줄면 형식이 깨진 메시지), dedup 출력 ≈ 입력 ÷ 2 | 🔧 |
| **dedup 비율** = dedup 입력 ÷ 출력 | 중복 제거가 기대대로 되는지 | ≈ 2.0. **1.0이면** 수집 pod 하나만 살아 있음(데이터는 정상). **2.0보다 크면** 중복이 아닌 것까지 버리는 버그 의심 | 🔧 |
| Kafka source `records-lag-max` | 실시간 lag (2절 consumer lag보다 정확) | 0 근처 | 🔧 |
| `isBackPressured`, `backPressuredTimeMsPerSecond` | 뒤 단계(clean 쓰기)가 느려 앞이 막히는지 | 0 | 🔧 |

### 3-2. 체크포인트와 복구

| 지표 | 왜 보나 | 정상 / 이상 신호 | 상태 |
| --- | --- | --- | --- |
| `lastCheckpointDuration` | 체크포인트가 timeout(1min)에 가까워지는지 | 수 초 이내 (지금 약 2초) | 🔧 |
| `lastCheckpointSize` | state(본 키 목록) 크기. TTL 1분이라 1분치 키만 남아야 함 | 일정한 범위에서 오르내림. 계속 커지면 TTL 정리가 안 됨 | 🔧 |
| `numberOfFailedCheckpoints` | MinIO 장애 · 권한 문제 | 늘지 않음. 연속 3번 실패하면 job 재시작 (`tolerable-failed-checkpoints: 3`) | 🔧 |
| 마지막 체크포인트 성공 시각 (`lastCompletedCheckpointId`가 바뀐 시각) | 체크포인트가 멈췄는지 | 10~20초마다 갱신 | 🔧 |
| `numRestarts` | job이 재시작을 반복하는지 | 늘지 않음 | 🔧 |
| `uptime` | 재시작 직후인지 | 계속 늘어남 | 🔧 |

### 3-3. 자원

| 지표 | 왜 보나 | 정상 / 이상 신호 | 상태 |
| --- | --- | --- | --- |
| TaskManager 메모리 (pod) | 한도 1536Mi. state가 heap에 있어 OOMKilled 위험 | 한도의 80% 미만 | ✅ |
| TaskManager · JobManager CPU (pod) | 요청 0.5 core | throttling이 없음 | ✅ |
| pod 재시작 · OOMKilled | 메모리 부족 | 없음 | ✅ |
| JVM heap 사용량 · GC 시간 (Flink 메트릭) | pod 메모리보다 정확한 heap 상태 | heap이 계속 차오르지 않음 | 🔧 |

**붙이는 법:** Flink 이미지의 `/opt/flink/plugins/metrics-prometheus`에 reporter가 이미 있다. `dedup.yaml`의 `flinkConfiguration`에 아래를 넣고 PodMonitor(포트 9249, 라벨 `release: monitoring`)를 만든다. 이미지 재빌드는 필요 없다.

```yaml
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: "9249"
```

- 연산자 이름이 `raw.<routeId>`, `dedup.<routeId>`처럼 route별로 붙어 있어(`DedupJob`의 `.name()`), Prometheus 라벨 `operator_name`으로 route를 나눠 볼 수 있다.

## 4. MinIO (`binance-storage`)

| 지표 | 왜 보나 | 정상 / 이상 신호 | 상태 |
| --- | --- | --- | --- |
| pod Ready 수 | erasure set 4개 중 1개 빠지면 계속 동작, 2개 빠지면 쓰기가 멈춰 체크포인트 실패 | 4/4. 3/4면 경고 | ✅ |
| PVC 사용량 | savepoint는 직접 지워야 해서 쌓일 수 있음 | 10Gi × 4 중 여유 있음 | ✅ |
| 오프라인 드라이브 · 노드 수 | pod는 떠 있는데 드라이브가 안 붙은 경우 | 0 | 🔧 |
| S3 요청 수 · 에러 수 | 체크포인트 실패의 원인이 MinIO 쪽인지 | 에러 0 | 🔧 |
| 버킷 · prefix별 사용량 (`binance/flink/<계층>/<job>`) | job별로 체크포인트가 얼마나 쌓이는지 ([plan/storage/plan.md](../../plan/storage/plan.md)) | job이 늘 때 함께 늘고, 하나만 튀지 않음 | 🔧 |

**붙이는 법:** MinIO 컨테이너에 환경 변수 `MINIO_PROMETHEUS_AUTH_TYPE=public` → ServiceMonitor (path `/minio/v2/metrics/cluster`, 포트 9000). 버킷별 사용량은 `/minio/v2/metrics/bucket`.

## 5. 클러스터와 노드

| 지표 | 왜 보나 | 정상 / 이상 신호 | 상태 |
| --- | --- | --- | --- |
| 노드 CPU · 메모리 · 디스크 | 노드 4대에 Kafka 3, MinIO 4, Flink, 수집 앱이 함께 돈다 | 노드별 메모리 80% 미만 | ✅ |
| 노드 Ready | MinIO · Kafka는 노드마다 하나씩 떠 있어 노드 하나가 빠지면 둘 다 영향 | 4/4 | ✅ |
| Flink Operator reconcile 에러 · FlinkDeployment 상태 | job이 `RUNNING`이 아닌데 아무도 모르는 경우 | 에러 0, 상태 `RUNNING` | 🔧 (Operator PodMonitor) |

1 · 2 · 3-3 · 4 · 5절의 ✅ 항목(노드 자원, pod Ready · 재시작 · OOMKilled, pod별 CPU · 메모리, Kafka broker CPU · 네트워크)은 대시보드 `Binance / 노드 · pod 운영 상태`([dashboards/binance-cluster-health/](dashboards/binance-cluster-health/))에 모았다. PVC별 사용량은 local-path가 `kubelet_volume_stats_*`를 내지 않아 노드 디스크 사용률로 대신 본다.

Grafana 기본 대시보드로도 볼 수 있다:
- **Kubernetes / Compute Resources / Namespace (Pods):** namespace를 `binance-processing` · `binancekafka` · `binance-storage` · `binance-ingestion`으로 바꿔 가며 본다
- **Node Exporter / Nodes:** 노드별 CPU · 메모리 · 디스크

## 6. 알림 후보

Alertmanager에 받을 곳(Slack 등)을 붙이면 아래부터 건다.

| 알림 | 조건 | 심각도 |
| --- | --- | --- |
| 데이터 끊김 | raw 토픽 초당 메시지 수 = 0, 2분 지속 | 높음 |
| Flink 밀림 | consumer lag가 5분 동안 계속 증가 | 높음 |
| 체크포인트 멈춤 | 마지막 체크포인트 성공 후 1분 넘게 지남 | 높음 |
| dedup 비율 이상 | 2.2 초과 (5분 평균) | 중간 |
| 수집 HA 깨짐 | variable-rate Deployment Ready < 2, 5분 지속 (dedup 비율 1.0으로도 보임) | 중간 |
| MinIO 드라이브 빠짐 | Ready < 4 | 중간 |
| pod 재시작 반복 | 15분에 3번 이상 | 중간 |

## 7. 적용 순서

| 순서 | 대상 | 이유 | 비용 |
| --- | --- | --- | --- |
| 1 | Flink reporter + PodMonitor | dedup 비율 · 체크포인트 · lag를 한 번에 봄 | `dedup.yaml` 몇 줄, 재빌드 없음 |
| 2 | Kafka exporter + JMX | 토픽 처리량과 consumer lag, broker 상태 | broker 롤링 재시작 |
| 3 | MinIO 메트릭 | 체크포인트 실패 원인 추적 | 환경 변수 하나 + ServiceMonitor |
| 4 | 수집 앱 actuator | 재연결 · 스트림별 수신 건수 (코드 추가 필요) | 의존성 · 코드 · 이미지 재빌드 |
