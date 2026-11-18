# Binance / raw 유입 · Flink lag

[metrics.md](../../metrics.md) 0절의 두 질문을 보는 대시보드다.

- **데이터가 들어오고 있나?** → raw 토픽 초당 메시지 수
- **Flink가 밀리지 않나?** → raw 토픽 consumer lag

| 항목 | 값 |
| --- | --- |
| Grafana | http://graf.monitoring.dclab2.dcusoft.net/d/binance-raw-ingest |
| 정의 | [dashboard.yaml](dashboard.yaml) (ConfigMap, 라벨 `grafana_dashboard: "1"`) |
| 지표 출처 | Kafka Exporter (`binancekafka/binance-cluster-kafka-exporter`, 15초마다 수집) |
| 변수 | `datasource` (Prometheus), `group` (consumer group, 기본 `flink-dedup`) |

```bash
kubectl apply -f Infra/Monitoring/dashboards/binance-raw-ingest/dashboard.yaml
```

## 원재료: Kafka Exporter가 내보내는 두 값

Kafka Exporter는 Prometheus가 가져갈 때마다(15초) broker에 물어 아래 두 값을 **파티션마다** 만든다. 대시보드의 패널은 모두 이 두 값으로 계산한다.

| 지표 | 뜻 | 예 (raw bookticker, 파티션 2) |
| --- | --- | --- |
| `kafka_topic_partition_current_offset{topic, partition}` | 그 파티션에 다음에 쓰일 offset (log end offset) = 지금까지 쓰인 메시지 수 | 38973 |
| `kafka_consumergroup_lag{consumergroup, topic, partition}` | `current_offset − 그 group이 커밋한 offset` = 아직 커밋되지 않은 메시지 수 | 0 |

- **offset 1 = 메시지 1건이다.** 수집 앱 producer가 트랜잭션을 쓰지 않아서(`enable.idempotence: true`, 트랜잭션 없음) offset 사이에 트랜잭션 marker가 끼지 않는다.
- **lag가 `-1`이면 그 group이 그 파티션에 커밋한 적이 없다는 뜻이다.** 지금은 키가 `BTCUSDT` 하나라 파티션 2에만 메시지가 쌓인다. 그래서 빈 파티션 0, 1이 `-1`로 나온다. 모든 lag 쿼리는 `clamp_min(…, 0)`으로 이 값을 0으로 바꿔서 합친다.

## Flink lag가 톱니 모양인 이유

Flink의 lag는 다음 순서로 만들어진다.

1. Flink KafkaSource는 메시지를 읽자마자 처리한다. 하지만 Kafka에 offset을 **커밋하는 것은 체크포인트가 끝날 때뿐**이다. 체크포인트 간격은 10초다 (`execution.checkpointing.interval: 10s`).
2. 그래서 Kafka가 보는 committed offset은 10초마다 한 번씩만 앞으로 간다.
3. 그 사이에 들어온 메시지는 Flink가 이미 처리했더라도 lag로 잡힌다.

```text
lag
 │    ╱│    ╱│    ╱│        최고점 ≈ 초당 메시지 × (10초 + 체크포인트 소요 시간)
 │   ╱ │   ╱ │   ╱ │        220/s면 약 2,200~2,600
 │  ╱  │  ╱  │  ╱  │
 │ ╱   │ ╱   │ ╱   │
 │╱    │╱    │╱    │        체크포인트 완료 → 커밋 → 0 근처로 떨어짐
 └─────┴─────┴─────┴──> 시간 (10초 간격)
```

- **이 lag는 "Flink가 처리하지 못한 양"이 아니라 "아직 커밋되지 않은 양"이다.** 10초 단위로 거칠게 보는 지표다.
- **Flink는 복구할 때 이 committed offset을 쓰지 않는다.** 재시작하면 체크포인트(MinIO)에 저장된 offset에서 이어 읽는다. Kafka에 커밋하는 것은 이렇게 밖에서 진행 상황을 보기 위한 용도다.
- **톱니 자체는 정상이다.** 문제는 톱니의 **바닥**이 점점 올라가는 경우다. 커밋한 직후에도 남는 양이 늘고 있다는 뜻이고, Flink가 들어오는 속도를 못 따라가고 있는 것이다.
- **실시간 lag**(Flink가 실제로 읽은 위치 기준)는 Flink reporter의 `records-lag-max`로 봐야 한다. 이것은 아직 붙이지 않았다 ([metrics.md](../../metrics.md) 3-1절).

## 패널별 계산

### 1. raw 전체 초당 메시지 (stat)

```promql
sum(rate(kafka_topic_partition_current_offset{topic=~"binance.raw.*"}[1m]))
```

- 계산 순서:
    1. `rate(…[1m])`가 파티션마다 최근 1분 동안 offset이 늘어난 양을 초 단위로 나눈다. 이것이 그 파티션의 초당 메시지 수다.
    2. `sum`으로 모든 raw 토픽 · 파티션을 더한다.
- **정상:** 0보다 크다. 2026-11-18 spot-sov(BTC bookTicker, pod 2개)만 켰을 때 219.7/s였다. 지난번에는 약 365/s였다. bookTicker는 호가가 바뀔 때마다 오는 스트림이라 시장 상황에 따라 값이 달라진다.
- **색:** 0이면 빨강, 1 이상이면 초록.
- **늦게 반영된다.** `rate`는 1분 창 안의 샘플(15초 간격) 2개 이상으로 계산한다. 그래서 수집을 끄거나 켜면 0이 되거나 값이 오르기까지 최대 1분쯤 걸린다.
- `current_offset`은 counter가 아니라 gauge라서 Prometheus가 "counter가 아닐 수 있다"는 info 메시지를 낸다. 그래도 offset은 줄지 않으므로 `rate`로 계산해도 된다. 토픽을 지웠다가 다시 만들면 offset이 0부터 다시 시작하는데, `rate`는 이것을 counter reset으로 처리해서 값이 튀지 않는다.

### 2. Flink lag 합계 (stat)

```promql
sum(clamp_min(kafka_consumergroup_lag{consumergroup="$group", topic=~"binance.raw.*"}, 0))
```

- 계산 순서:
    1. 파티션마다 lag를 가져온다.
    2. `-1`(커밋한 적 없는 파티션)은 0으로 바꾼다.
    3. 모든 raw 토픽 · 파티션을 더한다.
- 보이는 값은 가장 최근 샘플 하나다. 그래서 톱니의 어느 지점에서 찍혔는지에 따라 0~2,600 사이를 오간다.
- **색:** 10,000 이상이면 노랑, 50,000 이상이면 빨강.
    - 노랑 기준 10,000은 톱니 최고점(약 2,600 ~ 3,700)의 3배 정도로 잡은 값이다.
    - 스트림을 늘리면 최고점도 함께 올라가므로 기준을 다시 잡는다.

### 3. lag 5분 증가량 (stat) — "계속 늘지 않음"을 보는 패널

```promql
  sum(min_over_time(clamp_min(kafka_consumergroup_lag{consumergroup="$group", topic=~"binance.raw.*"}, 0)[5m:15s]))
- sum(min_over_time(clamp_min(kafka_consumergroup_lag{consumergroup="$group", topic=~"binance.raw.*"}, 0)[5m:15s] offset 5m))
```

- 계산 순서:
    1. `[5m:15s]`(subquery)는 최근 5분의 lag를 15초 간격으로 다시 뽑는다.
    2. `min_over_time`은 그중 가장 작은 값을 고른다. 그러면 커밋 직후 값, 즉 **톱니의 바닥**만 남는다.
    3. 앞 줄은 "최근 5분 동안의 바닥", 뒤 줄(`offset 5m`)은 "5~10분 전 동안의 바닥"이다.
    4. 둘을 빼면 5분 사이에 바닥이 얼마나 올라갔는지가 나온다.
- 정상과 이상 신호:

    | 값 | 뜻 |
    | --- | --- |
    | 0 근처 | 커밋할 때마다 거의 다 따라잡고 있다 |
    | 계속 양수 | 바닥이 올라가고 있다. Flink가 밀리거나 체크포인트가 멈춰 커밋이 안 되는 중이다 |
    | 음수 | 밀렸던 것을 따라잡는 중이다 |

- **색:** 1,000 이상이면 노랑, 10,000 이상이면 빨강.
- **데이터가 10분 이상 쌓여야 의미가 있다.** 그 전에는 `offset 5m` 쪽 값이 비어 0이나 엉뚱한 값이 나올 수 있다.
- 바닥이 정확히 0이 아닐 수 있다. Prometheus는 15초마다 값을 찍는데, 커밋 직후 순간과 정확히 겹치지 않을 수 있기 때문이다. 이 패널은 바닥의 절대값이 아니라 **바닥이 변하는지**를 본다.

### 4. 메시지가 들어오는 raw 토픽 수 (stat)

```promql
count(sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"binance.raw.*"}[1m])) > 0) or vector(0)
```

- 계산 순서:
    1. 토픽별 초당 메시지를 구한다.
    2. 0보다 큰 토픽만 남겨 개수를 센다.
    3. 하나도 없으면 결과가 비는데, `or vector(0)`으로 0을 보여 준다.
- **정상:** 켠 수집 pod들이 구독하는 스트림 수와 같다 (spot-sov만 켜면 1).
- **이상 신호:** 이 값이 기대보다 작으면, 어떤 스트림이 연결은 됐는데 메시지가 안 오는 경우일 수 있다. 예를 들어 USD-M 경로 접두어가 틀린 경우다.

### 5. 토픽별 초당 메시지 (timeseries)

```promql
sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"binance.raw.*"}[1m])) > 0
```

- 1번을 토픽별로 나눈 것이다. `> 0`으로 메시지가 없는 토픽은 그래프에서 뺀다.
- 범례에 평균과 마지막 값을 표시한다.

### 6. 토픽별 Flink lag (timeseries)

```promql
sum by (topic) (clamp_min(kafka_consumergroup_lag{consumergroup="$group", topic=~"binance.raw.*"}, 0))
```

- 2번을 토픽별로 나눈 것이다. 위 그림 같은 10초 톱니가 보여야 정상이다.
- 범례에 최대값과 마지막 값을 표시한다. 최대값이 `초당 메시지 × 10초` 근처면 정상이다.

### 7. 파티션별 초당 메시지 (timeseries)

```promql
rate(kafka_topic_partition_current_offset{topic=~"binance.raw.*"}[1m]) > 0
```

- 합치지 않은 파티션별 값이다.
- Kafka 키가 심볼이라, 같은 심볼은 항상 같은 파티션에 들어간다. 지금은 `BTCUSDT`가 파티션 2에만 들어가 선이 하나만 보인다.
- 심볼을 늘린 뒤에도 한 파티션만 튀면 키 분포가 한쪽에 몰린 것이다. Flink source 병렬도 3 중 일부만 일하게 된다.

## 상황별로 보이는 모습

| 상황 | 1. 초당 메시지 | 2·6. lag | 3. 5분 증가량 |
| --- | --- | --- | --- |
| 정상 | 0보다 큼 | 10초 톱니 | 0 근처 |
| 수집 앱 꺼짐 · 끊김 | 1분 안에 0 | 다음 커밋 뒤 0으로 떨어지고 그대로 | 0 또는 음수 |
| Flink job 멈춤 (커밋도 멈춤) | 그대로 | 직선으로 계속 증가 | 계속 양수 (약 초당 메시지 × 300) |
| Flink가 느림 (처리량 부족) | 그대로 | 톱니 바닥이 점점 올라감 | 양수 |
| 체크포인트 실패 (MinIO 장애 등) | 그대로 | 커밋이 안 돼 계속 증가 | 계속 양수 |
| 수집 pod 하나만 살아 있음 | 약 절반 | 톱니 높이가 약 절반 | 0 근처 |

- Flink가 멈춘 것과 체크포인트만 실패하는 것은 이 대시보드로는 구분되지 않는다.
- 이 둘은 Flink reporter의 처리량(`numRecordsInPerSecond`)과 `numberOfFailedCheckpoints`를 붙여야 나뉜다.
