# Insight 1-1. Spot-Futures Basis — 분석 계획

> 상위 카탈로그: [analyticsInsightsREADME.md](../analyticsInsightsREADME.md)
> 토픽 흐름과 Flink job 상세: [plan/topic/insight_1_plan.md](../../plan/topic/insight_1_plan.md)
> pod 구성: [plan/ingestion/plan.md](../../plan/ingestion/plan.md) · 토픽 규칙: [plan/topic/plan.md](../../plan/topic/plan.md)

가격 변동성이 클 때 같은 심볼의 **현물(Spot) 가격**과 **무기한 선물(Perpetual) 마크 가격**이 순간적으로
벌어지는 지점을 포착한다. 베이시스가 확대되고 펀딩비가 한쪽으로 쏠리면 과열 신호로 판단한다.

이 문서는 **무엇을 계산하고 어떻게 판단하는지**를 다룬다. 데이터가 어느 pod와 토픽을 거치는지는 위 plan 문서가 기준이다.

---

## 1. 필요 데이터 (`BTCUSDT` 기준)

### `btcusdt@bookTicker` (Spot, `stream.binance.com:9443`)

Spot의 최우선 매수/매도 호가가 변경될 때마다 전달된다.

```json
{
  "u": 99397868764,
  "s": "BTCUSDT",
  "b": "78760.56000000",
  "B": "0.27575000",
  "a": "78760.57000000",
  "A": "1.66034000"
}
```

**Spot Mid Price = (b + a) / 2**

- Spot bookTicker에는 이벤트 시각 `E`가 없다. 그래서 envelope의 `event_time`은 `null`이고, 시각은 collector가 찍은 `receive_time`을 쓴다 ([mapping.md ⑦](../../datasource-ingestion/deploy/mapping.md)).

### `btcusdt@markPrice@1s` (USD-M, `fstream.binance.com/market`)

무기한 선물에만 있는 스트림. `@1s`를 붙이면 1초 주기로 push된다 (붙이지 않으면 3초). USD-M은 경로 접두어 `/market`이 필요하다.

```json
{
  "e": "markPriceUpdate",
  "E": 1788098181003,
  "s": "BTCUSDT",
  "p": "78853.70000000",
  "ap": "78853.70000000",
  "P": "78786.58836087",
  "i": "78893.28739130",
  "r": "0.00008778",
  "T": 1788105600000,
  "st": 1
}
```

**Mark Price = p**, 펀딩비 = `r`

## 2. 처리 방식

**실시간 Basis = Mark Price − Spot Mid Price**

두 토픽은 키가 같은 심볼(`BTCUSDT`)이고 파티션 수도 3으로 같아서 co-partitioning된다. Flink에서 두 스트림을 `keyBy(symbol)`로 `connect`하고, **markPrice가 올 때마다(초당 1건) 그 순간의 최신 Spot mid**와 계산한다.

- 윈도우 조인을 쓰지 않는다. bookTicker는 초당 수십~수백 건이라 윈도우 안의 모든 조합이 출력되기 때문이다.
- Spot은 심볼별 최신값만 state에 두고, 2초 넘게 오래된 Spot 값과는 계산하지 않는다 (Spot 연결 끊김 대비).

상세 코드와 출력 메시지는 [insight_1_plan.md "Flink job 2"](../../plan/topic/insight_1_plan.md)를 따른다.

## 3. 과매도 / 과매수 판단 기준

basis 출력(`binance.analytics.crypto.basis`, 초당 1건) 위에서 계산한다.

1. **통계적 임계치**
   - 롤링 윈도우 내 basis의 평균(μ)·표준편차(σ)를 실시간 업데이트 (EMA 또는 Welford's algorithm으로
     순차 갱신, 전체 윈도우 재계산 방지)
   - **Z-Score = (Current Basis − μ) / σ**
   - **과열 판정 임계치**: `|Z| > 2.0` → 1차 후보
2. **시간 윈도우 유지 조건**
   - 최소 3~5개 연속 구간 동안 `|Z| > 2.0`이 유지될 때만 알림 발송 (디바운스)
   - Flink keyed state(심볼별)에 `consecutiveCount` 유지: 임계치 초과 시 +1, 벗어나면 0으로 리셋.
     `consecutiveCount >= 3`이 되는 순간 1회 `alert: true`로 발행하고 조건이 풀릴 때까지 재발행하지 않는다.

---

## 4. 데이터 파이프라인

### 4.1 전체 흐름

```mermaid
flowchart LR
    subgraph SpotWS["stream.binance.com:9443"]
        S1[btcusdt@bookTicker]
    end
    subgraph FutWS["fstream.binance.com/market"]
        F1[btcusdt@markPrice@1s]
    end

    S1 --> PA1[spot-sov Pod #1]
    S1 --> PA2[spot-sov Pod #2]
    F1 --> PB1[markprice-usdm-sov Pod]

    PA1 --> KT1[[binance.raw.spot.crypto.bookticker<br/>key=BTCUSDT · 중복 포함]]
    PA2 --> KT1
    PB1 --> KT3[[binance.raw.usdm.crypto.markprice<br/>key=BTCUSDT]]

    KT1 --> J1([Flink job 1: dedup<br/>키 u])
    J1 --> KT2[[binance.clean.spot.crypto.bookticker]]

    KT2 --> J2([Flink job 2: basis join<br/>z-score · consecutiveCount])
    KT3 --> J2
    J2 --> OUT[[binance.analytics.crypto.basis]]
```

### 4.2 Insight 1-1이 쓰는 pod

| pod | 그룹 | HA | 이 인사이트가 쓰는 스트림 | 경로 |
| --- | --- | --- | --- | --- |
| `spot-sov` | variable-rate | Active-Active (replicas 2) | `btcusdt@bookTicker` (pod는 trade, depth도 함께 수집) | `stream.binance.com:9443/ws/...` |
| `markprice-usdm-sov` | fixed-rate | Active-Standby (replicas 1, `Recreate`) | `btcusdt@markPrice@1s` | `fstream.binance.com/market/ws/...` |

- pod는 `<product>-<섹터>`로 이름 짓는다. BTC의 섹터는 `sov`다 ([cryptoSectorREADME.md](../../docs/symbols/cryptoSectorREADME.md)).
- markPrice는 `!markPrice@arr`(USD-M 전체)를 받아 거르지 않고, 필요한 심볼만 구독한다.
- 수집 앱은 raw 이벤트를 envelope로 감싸 보내기만 한다. 중복 제거와 조인은 Flink에서 한다.

### 4.3 중복 처리

| 토픽 | 중복 | 처리 |
| --- | --- | --- |
| `binance.raw.spot.crypto.bookticker` | 있음 — `spot-sov` 2개가 같은 이벤트를 각자 발행 | Flink job 1이 `u`(order book update id)로 dedup → `binance.clean.spot.crypto.bookticker` |
| `binance.raw.usdm.crypto.markprice` | 없음 — Active-Standby | raw를 그대로 job 2에 넣는다 |

dedup state TTL은 1분이다 (두 pod 사이 도착 시차보다 넉넉하게). 상세는 [insight_1_plan.md "Flink job 1"](../../plan/topic/insight_1_plan.md).

### 4.4 Kafka 토픽

| 토픽 | 키 | 파티션 | 보존 |
| --- | --- | --- | --- |
| `binance.raw.spot.crypto.bookticker` | `BTCUSDT` | 3 | 24h |
| `binance.clean.spot.crypto.bookticker` | `BTCUSDT` | 3 | 24h |
| `binance.raw.usdm.crypto.markprice` | `BTCUSDT` | 3 | 3d |
| `binance.analytics.crypto.basis` | `BTCUSDT` | 3 | 7d |

키에는 심볼만 넣는다. product와 자산 종류는 토픽 이름에 있다. 조인하는 두 토픽(clean bookticker, raw markprice)은 키와 파티션 수가 같아야 한다.

### 4.5 남은 이슈 / 다음 단계

- **Kafka 발행:** collector가 `KafkaTemplate`으로 실제 발행한다 (`binance.raw.usdm.crypto.markprice`는 kind에서 확인함). 토픽 이름(`binance.subscription.streams[].topic`)과 키(심볼), envelope의 `receive_time`은 이미 이 설계대로 들어가 있다.
- **Flink job 1, 2 구현:** 아직 없다. `plan/consumer/`에 설계를 정리한다.
- **Spot 신선도 체크의 기준 시각:** Spot bookTicker는 `event_time`이 `null`이다. job 2의 "2초 넘은 Spot은 버림" 비교는 Spot 쪽에 `receive_time`을 써야 한다.
- **z-score 계산 위치:** job 2 안에서 할지, basis 출력을 읽는 별도 job으로 뺄지 정한다.
