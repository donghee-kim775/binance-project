# Binance / Kafka Streams 파이프라인 (raw → clean → analytics)

Kafka Streams 두 앱이 raw를 analytics까지 끝까지 처리하는지 보는 대시보드다. [binance-raw-ingest](../binance-raw-ingest/README.md)(raw 유입 vs Flink 처리)를 Kafka Streams 기준으로 바꾸고, 뒤 단계(clean → analytics)까지 늘렸다.

```text
raw spot bookTicker ──> kstreams-dedup ──> clean spot bookTicker ──┐
  (수집 pod 2개, 중복)    group kstreams-dedup                        ├──> kstreams-basis ──> analytics basis
raw usdm markPrice  ───────────────────────────────────────────────┘    group kstreams-basis
```

- **끝까지 흐르나?** → analytics 출력, basis 통과율
- **어디서 막히나?** → 단계별 유입 vs 처리, 파이프라인 lag 시간, 파티션별 lag
- **왜 막히나?** → pod CPU · 메모리 · 재시작

| 항목 | 값 |
| --- | --- |
| Grafana | http://graf.monitoring.dclab2.dcusoft.net/d/binance-kstreams-pipeline |
| 정의 | [dashboard.yaml](dashboard.yaml) (ConfigMap, 라벨 `grafana_dashboard: "1"`) |
| 지표 출처 | Kafka Exporter (토픽 끝 offset · group 커밋 offset · lag, 15초), cAdvisor, kube-state-metrics |
| 변수 | `datasource`, `dedup_group` (기본 `kstreams-dedup`), `basis_group` (기본 `kstreams-basis`) |

```bash
kubectl apply -f Infra/Monitoring/dashboards/binance-kstreams-pipeline/dashboard.yaml
```

| 줄 | 패널 |
| --- | --- |
| 현상황 | raw 유입 · dedup 처리 · clean 유입 · basis 처리 · analytics 출력 · **basis 통과율** · dedup lag 시간 · basis lag 시간 |
| 단계별 처리량 | ① raw spot → dedup · ② clean · markPrice → basis · ③ markPrice → analytics |
| consumer lag | dedup 파티션별 · basis 토픽 · 파티션별 · lag 시간 (2초 선) |
| pod 자원 | CPU · 메모리 · 재시작 |

## Flink 대시보드와 다른 점

**① lag가 톱니가 아니다.** Flink는 체크포인트(10초)마다 커밋해서 lag가 10초 톱니였다. Kafka Streams는 exactly-once에서 `commit.interval.ms` 기본값 100ms마다 커밋한다. 그래서 정상이면 lag가 0 근처에 붙어 있고, **lag 시간이 1초를 넘으면 이미 이상**이다.

**② clean · analytics의 초당 건수는 근사다.** raw는 수집 앱이 트랜잭션 없이 써서 offset 1 = 메시지 1건이다. clean · analytics는 Kafka Streams가 트랜잭션으로 쓴다. 커밋마다 그 트랜잭션이 건드린 파티션에 **commit marker가 1개씩** 들어가고, marker도 offset을 하나 먹는다.

| 토픽 | 실제 건수 | marker | offset 증가율 |
| --- | --- | --- | --- |
| clean | 초당 수백 건 | 파티션당 초당 최대 10개 (100ms 커밋) | 거의 같다 (몇 % 큼) |
| analytics | 심볼 수 × 1/s (지금 6/s) | markPrice가 1초에 한 번 몰려 와서 커밋 1번에 묶임 → 파티션당 초당 1개 안팎 | 조금 크다 |

그래서 **basis 통과율이 1을 약간 넘을 수 있다.** 정확한 건수가 필요하면 Kafka Streams 자체 메트릭(`process-rate`, sink `record-send-rate`)을 JMX로 내보내야 한다. 아직 붙이지 않았다.

**③ clean은 raw spot의 약 절반이다.** 수집 앱이 Active-Active(pod 2개)라 같은 이벤트가 raw에 두 번 들어오고, dedup이 하나를 버린다.

## 패널별 계산

### 현상황 줄

| 패널 | 쿼리 | 정상 |
| --- | --- | --- |
| raw 유입 | `sum(rate(kafka_topic_partition_current_offset{topic="binance.raw.spot.crypto.bookticker"}[1m])) + (markPrice 같은 식)` | 0보다 큼 (지금 약 800~950/s) |
| dedup 처리 | `sum(rate(kafka_consumergroup_current_offset{consumergroup="$dedup_group", topic="<raw spot>"}[1m]))` | ≈ raw spot 유입. 밀린 것을 따라잡을 때는 더 큼 |
| clean 유입 | `sum(rate(kafka_topic_partition_current_offset{topic="<clean>"}[1m]))` | ≈ raw spot의 절반 |
| basis 처리 | `sum(rate(kafka_consumergroup_current_offset{consumergroup="$basis_group", topic="<raw markPrice>"}[1m]))` | 심볼 수 × 1/s (지금 6/s) |
| analytics 출력 | `sum(rate(kafka_topic_partition_current_offset{topic="<analytics>"}[1m]))` | ≈ basis 처리 |
| **basis 통과율** | `analytics 출력(5m) / (markPrice 유입(5m) > 0)` | **0.9 이상** (초록). 0.5 미만 빨강 |
| dedup lag 시간 | `dedup lag 합 / (raw spot 유입(5m) > 0)` | 1초 미만. **2초 이상 빨강** |
| basis lag 시간 | `(clean lag + markPrice lag) / ((clean + markPrice 유입)(5m) > 0)` | 1초 미만 |

- lag는 `clamp_min(…, 0)`으로 합친다. 한 번도 커밋하지 않은 빈 파티션(지금 raw spot 파티션 1)은 lag가 `-1`로 나온다.
- 분모의 `> 0`은 유입이 0일 때 나누지 않게 한다 (`+Inf` 대신 `-`).

#### 왜 basis 통과율이 핵심인가

basis는 Spot과 markPrice의 수신 시각 차이가 **2초를 넘으면 버린다**(신선도 필터). dedup이 2초 이상 밀리면 clean의 Spot이 전부 과거 값이 되어 **basis가 하나도 나오지 않는다.** 이때 다른 패널들은 멀쩡해 보인다.

- basis 처리는 6/s 그대로다. markPrice는 바로 읽기 때문이다.
- basis lag도 0이다. 늦게 온 clean도 받자마자 처리하기 때문이다.

그래서 "끝까지 흘렀나"는 **analytics 출력 ÷ markPrice 유입**으로만 보인다. 0 근처로 떨어지면 dedup lag 시간을 본다. 2026-11-28에 실제로 dedup lag가 130초까지 쌓여 통과율이 0이 됐고, 이 패널로 원인을 찾았다.

### 단계별 처리량 줄

- **① raw spot → dedup:** 실선(유입)과 점선(처리)이 겹치면 정상이다. 점선이 계속 아래면 그 차이만큼 lag가 쌓인다.
- **② clean · markPrice → basis:** basis가 두 입력을 다 따라가는지 본다. markPrice 선은 6/s 근처라 아래쪽에 붙어 보인다.
- **③ markPrice → analytics:** 통과율을 시간으로 본 것이다. analytics(점선)가 markPrice 아래로 떨어지는 구간이 신선도 필터에서 버려진 구간이다.

### consumer lag 줄

- **dedup lag (파티션별):** Kafka 키가 심볼이라 파티션마다 양이 다르다. 지금은 파티션 2가 초당 약 1,700건으로 가장 많고, 파티션 1은 비어 있다. **한 파티션만 오르면 그 파티션을 맡은 스레드 하나가 못 따라가는 것**이다. task 하나는 스레드 하나에서만 돌아서, pod를 늘려도 한 파티션의 처리량은 늘지 않는다.
- **basis lag (토픽 · 파티션별):** clean과 markPrice를 나눠 본다.
- **lag 시간:** dedup과 basis를 초로 바꿔 한 그래프에 그린다. 빨간 2초 선을 넘으면 basis가 버려지기 시작한다.

### pod 자원 줄

```promql
sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="binance-processing", pod=~"kstreams-.*", container!="", container!="POD"}[2m]))
sum by (pod) (container_memory_working_set_bytes{…같은 라벨…})
sum by (pod) (increase(kube_pod_container_status_restarts_total{namespace="binance-processing", pod=~"kstreams-.*"}[1h]))
```

- CPU limit이 없어 throttling은 없다. **lag가 오르는데 CPU가 1코어보다 한참 낮으면** CPU가 아니라 스레드 배치(task가 한 스레드에 몰림)나 트랜잭션 커밋 대기가 원인이다.
- 메모리 limit은 512Mi다.

## 상황별로 보이는 모습

| 상황 | raw 유입 | dedup 처리 | analytics 출력 · 통과율 | lag 시간 |
| --- | --- | --- | --- | --- |
| 정상 | > 0 | ≈ raw spot | ≈ markPrice · ≈ 1 | 둘 다 < 1초 |
| 수집 끊김 | 1분 안에 0 | 0 | 0 · `-` | `-` |
| dedup이 못 따라감 | 그대로 | < raw spot | **0 근처** | **dedup > 2초** |
| dedup 재시작 (state 복원) | 그대로 | 잠깐 0, 그 뒤 유입보다 큼 | 잠깐 0 | dedup 잠깐 오름 |
| basis 멈춤 | 그대로 | ≈ raw spot | 0 | basis 계속 증가 |
| Spot 수집만 끊김 (markPrice는 옴) | 절반 아래로 | 0 | 0 (Spot이 2초 넘게 오래됨) | 0 |

## 고치는 법

패널이 많아 JSON을 손으로 쓰지 않았다. Grafana UI에서 고친 뒤 **Share → Export → JSON**으로 받아 `dashboard.yaml`의 `binance-kstreams-pipeline.json:` 아래를 바꾸고 다시 `kubectl apply` 한다. uid(`binance-kstreams-pipeline`)는 바꾸지 않는다 (링크가 바뀐다).
