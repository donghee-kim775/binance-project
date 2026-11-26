# binance-basis-join

`Binance / basis join: Flink vs Kafka Streams` 대시보드. 같은 입력으로 basis를 내는 두 버전을 나란히 비교한다.

| 버전 | 코드 | 실행 | consumer group | 출력 토픽 |
| --- | --- | --- | --- | --- |
| Flink | [consumer/clean_analytics/flink-join](../../../../consumer/clean_analytics/flink-join/) | FlinkDeployment `flink-basis` (JobManager 1 + TaskManager 3) | `flink-basis` | `binance.analytics.crypto.basis` |
| Kafka Streams | [consumer/clean_analytics/kstreams-join](../../../../consumer/clean_analytics/kstreams-join/) | Deployment `kstreams-basis` (pod 2), compare overlay | `kstreams-basis` | `binance.analytics.crypto.basis.kstreams` |

지표는 Kafka Exporter(출력 offset, consumer lag)와 cAdvisor(pod CPU · 메모리 · 네트워크)만 쓴다. 두 앱에 reporter를 붙이지 않아도 된다. 색은 **Flink 주황, Kafka Streams 파랑**이다.

## 비교 실행 (BTC 하나만)

수집은 **BTC 하나**만 켠다. Spot BTC bookTicker(`spot-sov`)와 USD-M BTC markPrice(`markprice-usdm-sov`)다. 저장소 루트에서 실행한다.

```bash
# 1. 수집: BTC만 (전체 kustomization(-k)을 쓰지 않는다. 다른 섹터 · 심볼까지 다 뜬다)
kubectl apply -f datasource-ingestion/deploy/common.yaml \
              -f datasource-ingestion/deploy/variable-rate/spot-sov.yaml \
              -f datasource-ingestion/deploy/fixed-rate/markprice-usdm-sov.yaml

# 2. dedup (clean Spot을 만든다): 멈춰 둔 Flink dedup을 다시 켠다
kubectl patch flinkdeployment flink-dedup -n binance-processing --type merge -p '{"spec":{"job":{"state":"running"}}}'

# 3. join 두 버전
kubectl apply -f Infra/Kafka/Kafka-Topic/binance/analytics/crypto     # 출력 토픽 (처음 한 번)
kubectl apply -f consumer/clean_analytics/flink-join/deploy/basis.yaml                # Flink
kubectl apply -k consumer/clean_analytics/kstreams-join/compare                       # Kafka Streams (.kstreams 토픽)
```

끝나면:

```bash
kubectl delete -k consumer/clean_analytics/kstreams-join/compare
kubectl patch flinkdeployment flink-basis -n binance-processing --type merge -p '{"spec":{"job":{"state":"suspended"}}}'
kubectl patch flinkdeployment flink-dedup -n binance-processing --type merge -p '{"spec":{"job":{"state":"suspended"}}}'
# common.yaml은 지우지 않는다 (Namespace까지 지워진다)
kubectl delete -f datasource-ingestion/deploy/variable-rate/spot-sov.yaml \
               -f datasource-ingestion/deploy/fixed-rate/markprice-usdm-sov.yaml
```

- 두 join은 **커밋된 offset이 없으면 지금부터(latest) 읽는다.** 그래서 같은 시점부터 비교하려면 둘을 거의 동시에 띄운다.
- 처음 몇 분은 Flink TaskManager가 뜨고 첫 체크포인트를 하는 동안 Flink 쪽 출력이 늦게 시작한다. 5분 이상 지난 구간을 비교한다.

## 패널

| 행 | 패널 | 계산 | 보는 법 |
| --- | --- | --- | --- |
| 현상황 | 출력 · Flink / Kafka Streams | `sum(rate(kafka_topic_partition_current_offset{topic="<출력 토픽>"}[1m]))` | BTC 하나면 Flink는 약 **1/s** (markPrice 1건당 basis 1건). **Kafka Streams는 약 2/s로 보인다** (아래 주의) |
| | Lag · Flink / Kafka Streams | `sum(clamp_min(kafka_consumergroup_lag{consumergroup="<group>"}, 0))` | Spot + mark 합. Flink는 체크포인트(10s)마다 커밋해 톱니, Kafka Streams는 100ms마다 커밋해 거의 평평 |
| | CPU · Flink / Kafka Streams | `sum(rate(container_cpu_usage_seconds_total{namespace="binance-processing", pod=~"<prefix>-.*", container!=""}[5m]))` | core 단위. Flink는 JobManager까지 포함 |
| | 메모리 · Flink / Kafka Streams | `sum(container_memory_working_set_bytes{…})` | JVM이 heap을 미리 잡아 실제 state보다 크다 |
| 출력과 입력 | basis 출력 속도 | 위 출력 두 개를 한 그래프에 | 두 선이 겹쳐야 정상. 다르면 한쪽이 Spot을 못 받았거나(noSpot) 신선도(2초)에서 더 버린 것 |
| | 입력 속도 | `sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"<clean spot>\|<raw markprice>"}[1m]))` | Spot은 초당 수백 건, mark는 초당 1건 |
| consumer lag | consumer lag (그룹 · 토픽별) | `sum by (consumergroup, topic) (clamp_min(kafka_consumergroup_lag{…}, 0))` | 바닥이 계속 오르면 그 버전이 밀리는 것 |
| | 처리 속도 | `sum by (consumergroup, topic) (rate(kafka_consumergroup_current_offset{…}[1m]))` | 입력 속도와 같은 높이면 따라가고 있다 |
| 자원 | CPU · 메모리 · 네트워크 | 버전별 pod 합. 네트워크는 `container_network_{transmit,receive}_bytes_total{interface="eth0"}` | Flink는 keyBy 셔플이 TaskManager 사이 트래픽으로, Kafka Streams는 changelog · 트랜잭션이 broker 쪽 트래픽으로 잡힌다 |
| pod별 | CPU · 메모리 (pod별) | `sum by (pod) (…)` | BTC 하나면 파티션 하나라 Kafka Streams는 task를 받은 pod 하나만 바쁘다 |

## 주의: offset 증가율 ≠ 메시지 수 (exactly-once 토픽)

출력 · 처리 속도 패널은 **offset이 늘어난 속도**다. 트랜잭션으로 쓰는 토픽은 커밋할 때마다 **트랜잭션 마커(commit marker)** 가 offset을 하나 차지한다. 읽는 쪽에는 보이지 않는 레코드다.

- Kafka Streams basis는 basis 1건(초당 1건) 쓸 때마다 커밋(100ms 간격, 쓸 게 있을 때만)하므로 offset이 2씩 오른다 → 패널에는 **약 2/s**, 실제 메시지는 약 1/s.
- Flink basis는 at-least-once라 마커가 없다 → 패널 값 = 메시지 수.
- kstreams-dedup의 clean(`.kstreams`)도 같다. 초당 수백 건이면 커밋 10번당 마커 10개라 차이가 작아 잘 안 보인다.

실제 건수를 비교할 때는 아래 측정 명령의 `n`(같은 시간 동안 읽은 메시지 수)을 본다.

## 지연은 메시지로 잰다

`basis_time − receive_time`(markPrice를 collector가 받은 시각 → basis를 계산한 시각)은 메시지 안의 값이라 Prometheus로 볼 수 없다. 두 버전 모두 같은 필드를 쓰므로 같은 명령으로 잰다.

```bash
# TOPIC=binance.analytics.crypto.basis (Flink) 또는 binance.analytics.crypto.basis.kstreams (Kafka Streams)
kubectl exec -n binancekafka binance-cluster-binance-cluster-pool-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic "$TOPIC" --from-beginning --max-messages 300 --timeout-ms 20000 2>/dev/null \
| python3 -c '
import sys, json
lat = sorted(m["basis_time"] - m["receive_time"] for m in map(json.loads, sys.stdin))
pick = lambda p: lat[min(len(lat) - 1, int(len(lat) * p))]
print(f"n={len(lat)} p50={pick(0.5)}ms p90={pick(0.9)}ms p99={pick(0.99)}ms max={lat[-1]}ms")'
```

- 두 시각은 서로 다른 노드 시계다 (collector pod 노드 ↔ join pod 노드). 노드 간 차이는 수 ms 수준으로 확인해 두었다.
- Kafka Streams는 exactly-once라 `read_committed`로 읽는 쪽에는 커밋(최대 100ms) 뒤에 보인다. `basis_time`은 계산 시각이라 이 커밋 대기는 들어가지 않는다.
