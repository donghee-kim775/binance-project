# Binance / raw 유입 vs Flink 처리

[metrics.md](../../metrics.md) 0절의 두 질문을 보는 대시보드다.

- **데이터가 들어오고 있나?** → Kafka 유입 속도
- **Flink가 밀리지 않나?** → 유입 vs 처리 속도, consumer lag, lag 증가 속도

구성은 "유입 10,000/s, 처리 8,000/s면 차이 2,000/s만큼 매초 쌓인다"는 생각을 그대로 패널로 옮긴 것이다. **밀림(lag) = 유입 − 처리가 쌓인 양**이라서 세 값을 나란히 놓는다.

| 항목 | 값 |
| --- | --- |
| Grafana | http://graf.monitoring.dclab2.dcusoft.net/d/binance-raw-ingest |
| 정의 | [dashboard.yaml](dashboard.yaml) (ConfigMap, 라벨 `grafana_dashboard: "1"`) |
| 지표 출처 | Kafka Exporter (`binancekafka/binance-cluster-kafka-exporter`, 15초마다 수집). Backpressure 패널만 Flink reporter |
| 변수 | `datasource` (Prometheus), `group` (consumer group, 기본 `flink-dedup`), `topic` (raw 토픽, 여러 개 선택 가능, 기본 전체), `flinkjob` (기본 `dedup`) |

```bash
kubectl apply -f Infra/Monitoring/dashboards/binance-raw-ingest/dashboard.yaml
```

| 줄 | 패널 |
| --- | --- |
| 현상황 | Kafka 유입 · Flink 처리 · Consumer Lag · Lag 시간 · Lag 증가 속도 · Flink Backpressure |
| 유입 vs 처리 속도 | 전체 · 파티션별 |
| Consumer lag | 전체 · 파티션별 |

## 원재료: Kafka Exporter가 내보내는 세 값

Kafka Exporter는 Prometheus가 가져갈 때마다(15초) broker에 물어 아래 값을 **파티션마다** 만든다. Backpressure를 뺀 패널은 모두 이 값으로 계산한다.

| 지표 | 뜻 | 예 (raw bookticker, 파티션 2) |
| --- | --- | --- |
| `kafka_topic_partition_current_offset{topic, partition}` | 그 파티션에 다음에 쓰일 offset (log end offset) = 지금까지 쓰인 메시지 수 | 76045 |
| `kafka_consumergroup_current_offset{consumergroup, topic, partition}` | 그 group이 커밋한 offset = Flink가 처리를 끝냈다고 확정한 위치 | 76045 |
| `kafka_consumergroup_lag{consumergroup, topic, partition}` | 위 두 값의 차이 = 아직 커밋되지 않은 메시지 수 | 0 |

- **offset 1 = 메시지 1건이다.** 수집 앱 producer가 트랜잭션을 쓰지 않아서(`enable.idempotence: true`, 트랜잭션 없음) offset 사이에 트랜잭션 marker가 끼지 않는다.
- **그래서 offset 증가율이 곧 초당 건수다.** 끝 offset 증가율 = 유입 속도, 커밋 offset 증가율 = 처리 속도.
- **커밋하지 않은 파티션은 lag와 커밋 offset이 `-1`이다.** 지금은 키가 `BTCUSDT` 하나라 파티션 2에만 메시지가 쌓인다. 그래서 빈 파티션 0, 1이 `-1`로 나온다. lag 합계 쿼리는 `clamp_min(…, 0)`으로 0으로 바꿔서 합치고, 파티션별 lag는 `>= 0`으로 뺀다. 커밋 offset은 `-1`에서 변하지 않으니 `rate`가 0이라 그대로 둔다.

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
- 같은 이유로 **처리 속도(커밋 기준)도 10초 단위 계단**으로 움직인다. `rate(…[1m])`로 평균을 내서 선으로 보는 것이다.
- **실시간 lag**(Flink가 실제로 읽은 위치 기준)는 Flink reporter의 `records-lag-max`로 봐야 한다. 이것은 아직 붙이지 않았다 ([metrics.md](../../metrics.md) 3-1절).

## 패널별 계산

아래 쿼리의 `$topic`은 기본값(전체)일 때 `binance.raw.*`이다. `binance\.raw\..*`처럼 쓰면 PromQL 문자열 안에서 `\.`가 잘못된 escape라 parse error가 난다.

### 현상황 줄

#### 1. Kafka 유입 (stat)

```promql
sum(rate(kafka_topic_partition_current_offset{topic=~"$topic"}[1m]))
```

- `rate(…[1m])`가 파티션마다 최근 1분 동안 offset이 늘어난 양을 초로 나눈다. `sum`으로 모든 토픽 · 파티션을 더한다.
- **정상:** 0보다 크다. spot-sov(BTC bookTicker, pod 2개)만 켜면 약 160~365/s. bookTicker는 호가가 바뀔 때마다 와서 시장 상황에 따라 달라진다.
- **색:** 0이면 빨강, 1 이상이면 초록.
- **늦게 반영된다.** 1분 창 안의 샘플(15초 간격)로 계산해서 수집을 끄거나 켜면 반영되기까지 최대 1분쯤 걸린다.
- `current_offset`은 gauge라 Prometheus가 "counter가 아닐 수 있다"는 info를 낸다. offset은 줄지 않으므로 `rate`로 계산해도 된다. 토픽을 다시 만들어 offset이 0부터 시작하면 `rate`가 counter reset으로 처리해 값이 튀지 않는다.

#### 2. Flink 처리 (stat)

```promql
sum(rate(kafka_consumergroup_current_offset{consumergroup="$group", topic=~"$topic"}[1m]))
```

- 1번과 같은 계산을 **커밋된 offset**에 한다. Flink가 처리를 끝내고 확정한 건수의 초당 값이다.
- **Flink reporter 없이 구하는 처리 속도다.** reporter의 `numRecordsOutPerSecond`(source)는 "읽은 순간" 기준이고, 이 값은 "체크포인트로 확정된 순간" 기준이다. 길게 보면 둘은 같고, 짧게 보면 이 값이 10초 단위로 거칠다.
- **정상:** 1번과 거의 같다. 2026-11-18 03:00~03:03에 유입 118 → 225/s, 처리 91 → 215/s로 따라갔다.

#### 3. Consumer Lag (stat)

```promql
sum(clamp_min(kafka_consumergroup_lag{consumergroup="$group", topic=~"$topic"}, 0))
```

- 아직 커밋되지 않은 양. 최근 샘플 하나라서 톱니의 어디서 찍혔는지에 따라 0 ~ (유입 × 약 12초)를 오간다.
- **색:** 10,000 이상 노랑, 50,000 이상 빨강. 노랑은 톱니 최고점(약 2,600 ~ 3,700)의 3배쯤이다. 스트림을 늘리면 다시 잡는다.

#### 4. Lag 시간 (stat)

```promql
  sum(clamp_min(kafka_consumergroup_lag{consumergroup="$group", topic=~"$topic"}, 0))
/ (sum(rate(kafka_topic_partition_current_offset{topic=~"$topic"}[5m])) > 0)
```

- 쌓인 건수를 지금 유입 속도로 나눠 **몇 초치가 밀려 있나**로 바꾼다. 건수는 스트림마다 속도가 달라 기준을 잡기 어렵지만, 초는 그대로 비교할 수 있다.
- 분모는 5분 평균이다. 1분이면 bookTicker 속도가 출렁일 때 값이 같이 튄다.
- `> 0`으로 유입이 0일 때는 나누지 않는다(0으로 나누면 `+Inf`). 이때는 `-`로 보인다.
- **정상:** 0 ~ 12초 (커밋 간격 10초 + 체크포인트 소요 시간). **색:** 30초 이상 노랑, 120초 이상 빨강.

#### 5. Lag 증가 속도 (stat) — "계속 늘지 않음"을 보는 패널

```promql
  sum(rate(kafka_topic_partition_current_offset{topic=~"$topic"}[10m]))
- sum(rate(kafka_consumergroup_current_offset{consumergroup="$group", topic=~"$topic"}[10m]))
```

- **유입 속도 − 처리 속도**를 그대로 뺀다. lag = 끝 offset − 커밋 offset이므로 이 값은 "10분 동안 lag가 늘어난 양 ÷ 600초"와 같다.
- 창이 10분인 이유: 10분 창의 양 끝이 톱니의 어느 지점에 걸리느냐에 따라 최대 (톱니 높이 ÷ 600초)만큼 흔들린다. 220/s면 톱니 높이 약 2,600 → ±4/s 정도다. 1분 창이면 ±40/s가 되어 밀림과 구분이 안 된다.
- 정상과 이상 신호:

    | 값 | 뜻 |
    | --- | --- |
    | 0 근처 (± 몇) | 따라가고 있다 |
    | 계속 양수 | 매초 그만큼씩 쌓이고 있다. Flink가 느리거나, 체크포인트가 멈춰 커밋이 안 되는 중 |
    | 음수 | 밀렸던 것을 따라잡는 중 |

- **색:** 20/s 이상 노랑, 100/s 이상 빨강.
- **데이터가 10분 이상 쌓여야 의미가 있다.**
- 예전 패널 "lag 5분 증가량"(5분 최소값끼리 비교해 톱니를 지운 값)을 이것으로 바꿨다. 같은 질문이지만 단위가 records/s라 유입 · 처리 패널과 바로 비교된다.

#### 6. Flink Backpressure (stat)

```promql
max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond{job_name="$flinkjob"})
```

- subtask가 1초 중 다음 연산자(clean 쓰기 등)를 기다리며 막혀 있던 시간(0~1000ms). 가장 막힌 subtask를 고른다.
- **Flink reporter 지표라 지금은 `-`로 보인다.** 붙이는 법은 [metrics.md](../../metrics.md) 3절. 붙인 뒤 Prometheus에서 이름과 `job_name` 라벨을 한 번 확인한다.
- lag가 오를 때 이 값이 높으면 원인은 뒤 단계(sink)이고, 낮으면 Flink 자체 처리 능력이나 체크포인트 쪽이다.
- **색:** 100ms 이상 노랑, 500ms 이상 빨강.

### 유입 vs 처리 속도 줄

#### 7. 유입 vs 처리 (전체, timeseries)

- 1번과 2번을 한 그래프에 그린다. 처리는 점선이다.
- 값이 없는 패널은 모두 `-`로 표시한다 (`noValue`). 속도 패널은 단위를 `/s`로 붙인다.
- 두 선이 겹치면 정상. 처리가 유입 아래로 계속 떨어져 있으면 두 선 사이 넓이만큼 lag가 쌓인다.

#### 8. 파티션별 유입 vs 처리 (timeseries)

```promql
sum by (topic, partition) (rate(kafka_topic_partition_current_offset{topic=~"$topic"}[1m])) > 0
sum by (topic, partition) (rate(kafka_consumergroup_current_offset{consumergroup="$group", topic=~"$topic"}[1m])) > 0
```

- 1 · 2번을 파티션별로 나눈 것이다. `> 0`으로 메시지가 없는 파티션은 뺀다.
- **왜 파티션별로 보나:** Kafka 키가 심볼이라 같은 심볼은 항상 같은 파티션에 들어간다. 지금은 `BTCUSDT`가 파티션 2에만 들어가 선이 한 쌍만 보인다. 심볼을 늘린 뒤 한 파티션만 유입이 튀면 키가 한쪽에 몰린 것이고, Flink source 병렬도 3 중 그 파티션을 맡은 subtask 하나만 바빠진다.
- Flink reporter의 `KafkaSourceReader_topic_partition_currentOffset`로도 파티션별 읽기 속도를 볼 수 있지만, 커밋 offset으로 같은 것을 reporter 없이 본다.

### Consumer lag 줄

#### 9. Consumer lag (전체, timeseries)

- 3번을 시간으로 그린 것이다. 10초 톱니가 보여야 정상이다 (위 그림).
- 범례에 최대값과 마지막 값을 표시한다. 최대값이 `유입 × 10~12초` 근처면 정상이다.

#### 10. 파티션별 consumer lag (timeseries)

```promql
sum by (topic, partition) (kafka_consumergroup_lag{consumergroup="$group", topic=~"$topic"} >= 0)
```

- 합계는 괜찮아 보여도 한 파티션에만 lag가 몰릴 수 있다. 8번에서 처리가 뒤처진 파티션이 있으면 여기서 그 파티션의 톱니 바닥이 오른다.
- `>= 0`으로 커밋한 적 없는 빈 파티션(`-1`)은 뺀다.

## 상황별로 보이는 모습

| 상황 | 1. 유입 | 2. 처리 | 3 · 9. lag | 5. Lag 증가 속도 |
| --- | --- | --- | --- | --- |
| 정상 | 0보다 큼 | ≈ 유입 | 10초 톱니 | 0 근처 |
| 수집 앱 꺼짐 · 끊김 | 1분 안에 0 | 다음 커밋 뒤 0 | 0으로 떨어지고 그대로 | 0 또는 음수 |
| Flink job 멈춤 (커밋도 멈춤) | 그대로 | 0 | 직선으로 계속 증가 | ≈ 유입 |
| Flink가 느림 (처리량 부족) | 그대로 | 유입보다 낮음 | 톱니 바닥이 점점 올라감 | 양수 (유입 − 처리) |
| 체크포인트 실패 (MinIO 장애 등) | 그대로 | 0 | 커밋이 안 돼 계속 증가 | ≈ 유입 |
| 수집 pod 하나만 살아 있음 | 약 절반 | ≈ 유입 | 톱니 높이가 약 절반 | 0 근처 |

- **Flink가 멈춘 것과 체크포인트만 실패하는 것은 이 대시보드로는 구분되지 않는다.** 처리 속도가 커밋 기준이라 둘 다 0으로 보인다.
- reporter를 붙이면 source `numRecordsInPerSecond`(읽은 순간 기준)가 살아 있는지와 `numberOfFailedCheckpoints`로 나뉜다. Backpressure(6번)는 "느림"의 원인이 sink 쪽인지를 알려 준다.
