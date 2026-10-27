# Kafka 토픽 목록

이 프로젝트에서 쓰는 Kafka 토픽을 한곳에 모은 표다. **왜 이렇게 나눴는지**(토픽 단위, crypto/stock 분리, 이름 규칙, 키)는 [plan/topic/plan.md](../../plan/topic/plan.md)에 있고, 이 문서는 지금 어떤 토픽이 있는지와 설정 값만 모은다. 설정 값의 이유는 [settings.md](settings.md)에 있다.

> 토픽이나 파티션이 바뀌면 이 문서와 [plan/topic/plan.md](../../plan/topic/plan.md)를 함께 고친다.

## 이름 규칙

```text
binance.<단계>.<product>.<자산종류>.<스트림>

단계     : raw (수집 원본, 중복 포함) / clean (dedup 이후) / analytics (분석 결과)
product  : spot / usdm
자산종류 : crypto / stock
스트림   : bookticker / trade / aggtrade / depth / markprice
```

- 소문자, 숫자, `.`만 쓴다. Strimzi `KafkaTopic`의 `metadata.name`을 토픽 이름과 같게 쓰기 위해서다.
- **예외:** Spot과 USD-M을 함께 쓰는 analytics 토픽은 product를 뺀다 (`binance.analytics.crypto.basis`).
- **키:** 모든 토픽이 심볼(`BTCUSDT`, `NVDAUSDT`)이다.

## 흐름

```text
ingestion pod ──> raw ──Flink dedup──> clean ──Flink 분석──> analytics
                   │                                  ▲
                   └── markprice (중복 없음) ──────────┘
```

---

## raw (11개)

수집 앱이 쓴다. variable-rate 스트림은 Active-Active라 같은 이벤트가 두 번 들어온다.

| 토픽 | 쓰는 pod | 파티션 | 보존 |
| --- | --- | --- | --- |
| `binance.raw.spot.crypto.bookticker` | spot-sov | 3 | 24h |
| `binance.raw.spot.crypto.trade` | spot-sov | 3 | 7d |
| `binance.raw.spot.crypto.depth` | spot-sov | 3 | 6h |
| `binance.raw.usdm.crypto.bookticker` | usdm-sov | 3 | 24h |
| `binance.raw.usdm.crypto.aggtrade` | usdm-sov | 3 | 7d |
| `binance.raw.usdm.crypto.depth` | usdm-sov | 3 | 6h |
| `binance.raw.usdm.crypto.markprice` | markprice-usdm-sov | 3 | 3d |
| `binance.raw.usdm.stock.bookticker` | usdm-optical, usdm-semi | 6 | 24h |
| `binance.raw.usdm.stock.aggtrade` | usdm-optical, usdm-semi | 6 | 7d |
| `binance.raw.usdm.stock.depth` | usdm-optical, usdm-semi | 6 | 6h |
| `binance.raw.usdm.stock.markprice` | markprice-usdm-optical, markprice-usdm-semi | 6 | 3d |

- Spot은 `trade`, USD-M은 `aggTrade`를 수집한다.
- 각 pod가 어느 토픽에 쓰는지는 pod ConfigMap의 `binance.subscription.streams[].topic`이 정한다 ([deploy/](../../datasource-ingestion/deploy/)).

## clean (9개)

Flink dedup job이 raw를 읽어 중복을 지우고 쓴다. 파티션 수는 raw와 같다 (co-partitioning).

| 토픽 | 원본 raw | dedup 키 | 파티션 | 보존 |
| --- | --- | --- | --- | --- |
| `binance.clean.spot.crypto.bookticker` | `binance.raw.spot.crypto.bookticker` | `u` | 3 | 24h |
| `binance.clean.spot.crypto.trade` | `binance.raw.spot.crypto.trade` | `t` | 3 | 미정 |
| `binance.clean.spot.crypto.depth` | `binance.raw.spot.crypto.depth` | `u` | 3 | 미정 |
| `binance.clean.usdm.crypto.bookticker` | `binance.raw.usdm.crypto.bookticker` | `u` | 3 | 미정 |
| `binance.clean.usdm.crypto.aggtrade` | `binance.raw.usdm.crypto.aggtrade` | `a` | 3 | 미정 |
| `binance.clean.usdm.crypto.depth` | `binance.raw.usdm.crypto.depth` | `u` | 3 | 미정 |
| `binance.clean.usdm.stock.bookticker` | `binance.raw.usdm.stock.bookticker` | `u` | 6 | 미정 |
| `binance.clean.usdm.stock.aggtrade` | `binance.raw.usdm.stock.aggtrade` | `a` | 6 | 미정 |
| `binance.clean.usdm.stock.depth` | `binance.raw.usdm.stock.depth` | `u` | 6 | 미정 |

- markprice는 clean이 없다. fixed-rate pod는 Active-Standby라 중복이 생기지 않아 raw를 바로 읽는다.
- 보존 기간은 Insight 1이 쓰는 `binance.clean.spot.crypto.bookticker`만 정해졌다. 나머지는 해당 Flink job을 설계할 때 정한다.

## analytics (1개)

| 토픽 | 쓰는 쪽 | 읽는 쪽 | 입력 | 파티션 | 보존 |
| --- | --- | --- | --- | --- | --- |
| `binance.analytics.crypto.basis` | Flink basis job | 대시보드, 알림 | `binance.clean.spot.crypto.bookticker`, `binance.raw.usdm.crypto.markprice` | 3 | 7d |

상세는 [insight_1_plan.md](../../plan/topic/insight_1_plan.md).

## Kafka 내부 토픽

직접 만들지 않지만 클러스터 설정([kafka-cluster.yaml](../../Infra/Kafka/Kafka-Cluster/kafka-cluster.yaml))으로 복제 수를 정한다.

| 토픽 | 용도 | RF | min ISR |
| --- | --- | --- | --- |
| `__consumer_offsets` | 컨슈머 그룹이 어디까지 읽었는지 | 3 | 2 (브로커 기본값) |
| `__transaction_state` | 트랜잭션 producer 상태 (Flink exactly-once sink) | 3 | 2 |

---

## 공통 토픽 설정

| 설정 | 값 |
| --- | --- |
| `replicas` | 3 |
| `min.insync.replicas` | 2 |
| `cleanup.policy` | `delete` |
| `compression.type` | `zstd` |
| `message.timestamp.type` | `LogAppendTime` (Insight 1 토픽 4개) |

## yaml 위치

토픽은 Strimzi `KafkaTopic`(`kafka.strimzi.io/v1`)으로 만든다. 아직 작성 전이고, 폴더만 있다.

```text
Infra/Kafka/Kafka-Topic/binance/
├── raw/{crypto,stock}/
├── clean/{crypto,stock}/
└── analytics/{crypto,stock}/
```

예시 형식은 [btcusdt-topic-example.yaml](../../Infra/Kafka/Kafka-Topic/btcusdt-topic-example.yaml)이다 (이 파일은 이름 규칙 이전의 예시라 대체될 예정).
