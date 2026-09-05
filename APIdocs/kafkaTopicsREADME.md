# Kafka 토픽 설계 (Binance Collector)

[binanceWebSocketREADME.md](binanceWebSocketREADME.md)에서 다룬 Market Stream / User Data Stream을
Kafka에 어떻게 태울지 정리한 문서다. 전체 CDC 파이프라인(Debezium 경유) 관점은
[README.md](../README.md)의 "Kafka 토픽 설계" 절을 참고한다. 이 문서는 **Collector가 직접 발행하는
raw 토픽**과, 그 위에서 시도해볼 만한 **스트림 조인**에 집중한다.

---

## 1. 토픽 분리 원칙

시장 데이터(공개)와 계정 데이터(민감)는 **토픽, 키, 보존 정책, 접근 권한을 전부 분리**한다.
같은 클러스터를 쓰더라도 ACL과 컨슈머 그룹은 나눈다.

```text
binance.raw.trade         공개 시세  (symbol 키, 보존 짧게, 대용량)
binance.raw.kline
binance.raw.bookticker
binance.raw.markprice     USD-M 전용 (마크 가격 · 펀딩비)
binance.raw.forceorder    USD-M 전용 (강제 청산)

binance.account.order     계정 이벤트 (account 키, 보존 길게, 접근 제한)
binance.account.balance
binance.account.position  USD-M 전용
```

## 2. 토픽별 상세

| 토픽 | 소스 이벤트 | 키 | 파티션 기준 | 보존 |
| --- | --- | --- | --- | --- |
| `binance.raw.trade` | Spot/Futures `trade` | `symbol` | 심볼당 순서 보장 | 수 시간~1일 |
| `binance.raw.kline` | `kline_<interval>` | `symbol:interval` | 심볼+주기 조합 | 길게(재계산용) |
| `binance.raw.bookticker` | `bookTicker` | `symbol` | 심볼당 순서 보장 | 짧게(수 시간) |
| `binance.raw.markprice` | `markPrice` | `symbol` | 심볼당 순서 보장 | 짧게 |
| `binance.raw.forceorder` | `forceOrder` | `symbol` | 심볼당 순서 보장 | 중간 |
| `binance.account.order` | `executionReport` / `ORDER_TRADE_UPDATE` | `accountId` | 계정당 순서 보장 | 길게(감사용) |
| `binance.account.balance` | `outboundAccountPosition` / `balanceUpdate` / `ACCOUNT_UPDATE` | `accountId` | 계정당 순서 보장 | 길게 |
| `binance.account.position` | `ACCOUNT_UPDATE`의 `P`, `MARGIN_CALL` | `accountId` | 계정당 순서 보장 | 길게 |

Kafka 파티션 키를 **symbol**(시장 데이터) 또는 **accountId**(계정 데이터)로 고정해야
같은 심볼/계정의 이벤트 순서가 파티션 내에서 보존되고, 이후 조인 시 워터마크 정합성이 맞는다.

## 3. 이벤트 envelope

원본 payload를 그대로 넣지 말고 공통 envelope로 감싼다. product(Spot/USD-M/COIN-M)별로
스키마가 다르므로 하나의 DTO로 억지로 합치지 않는다.

```json
{
  "source": "binance",
  "product": "usdm-futures",
  "event_type": "ORDER_TRADE_UPDATE",
  "symbol": "BTCUSDT",
  "event_time": 1787498072701,
  "receive_time": 1787498072715,
  "payload": { "...": "원본 이벤트 그대로" }
}
```

`event_time`(거래소 기준)과 `receive_time`(로컬 수신 시각)을 둘 다 넣어야 지연 측정과
조인 시 워터마크 계산이 가능하다.

## 4. Schema Registry / 직렬화

- Avro 또는 Protobuf + Schema Registry 사용을 권장한다. JSON은 초기 개발엔 편하지만
  조인 단계에서 타입 안정성이 떨어진다.
- 가격·수량 필드는 원본이 문자열이므로, 스키마에서는 `decimal` 논리 타입으로 변환해 저장한다.
  `double`로 캐스팅하면 조인·집계에서 오차가 누적된다.

---

## 5. 인사이트 — Kafka Streams로 시도해볼 만한 Join

Binance 데이터는 **여러 스트림이 같은 시간축·같은 심볼을 공유**하기 때문에,
단일 스트림 집계보다 조인했을 때 훨씬 의미 있는 신호가 나온다. 실제로 시도해볼 만한 조인을
난이도 순으로 정리했다.

### 5.1 Spot trade ⋈ USD-M markPrice — Basis(베이시스) 계산

**타입**: KStream-KStream 윈도우 조인 (5~10초 tumbling/hopping window)

```text
binance.raw.trade (Spot, symbol=BTCUSDT)
        ⋈ (windowed by symbol, 5s)
binance.raw.markprice (USD-M, symbol=BTCUSDT)
        → basis = futures_mark_price - spot_trade_price
```

무기한 선물의 마크 가격과 현물 체결가의 괴리(베이시스)는 펀딩비와 함께 **현물-선물 차익거래
신호**로 쓰인다. 베이시스가 확대되는 구간과 펀딩비 방향을 겹쳐 보면 시장 과열/침체를 정량화할 수 있다.
두 스트림 모두 심볼 키로 파티셔닝되어 있어 Kafka Streams의 co-partitioning 요구사항을 그대로 만족한다.

### 5.2 forceOrder(청산) ⋈ trade/kline — 청산 캐스케이드의 가격 충격

**타입**: KStream-KStream 윈도우 조인 (청산 시각 기준 ±30초)

```text
binance.raw.forceorder (symbol)
        ⋈ (windowed by symbol, -5s ~ +30s)
binance.raw.trade (symbol)
        → 청산 발생 전후 가격 변화율, 거래량 급증 여부
```

대량 강제 청산 직후 가격이 얼마나, 얼마나 빨리 되돌아오는지(또는 캐스케이드로 이어지는지)를
측정할 수 있다. `forceOrder`의 `q`(청산 수량)로 이벤트를 필터링해 "대량 청산"만 조인 대상으로
좁히면 노이즈를 줄일 수 있다. Order Flow 분석(README.md의 CVD, imbalance)과 결합하면
청산 전 오더북 불균형이 선행 지표였는지도 검증 가능하다.

### 5.3 내 executionReport ⋈ 시장 bookTicker — 체결 품질(슬리피지) 측정

**타입**: KStream-KTable 조인 (KTable은 `bookTicker`를 심볼별 최신 상태로 materialize)

```text
binance.account.order (executionReport, x=TRADE)
        ⋈ (KTable lookup by symbol, 최신 mid-price)
binance.raw.bookticker → KTable<symbol, (bid, ask)>
        → slippage = 내 체결가 - 체결 시점 mid_price
```

내 주문이 체결된 순간의 시장 mid-price와 실제 체결가를 비교하면 **실행 품질(execution quality)**
을 정량화할 수 있다. 이건 계정 데이터(민감)와 시장 데이터(공개)를 조인하는 유일한 케이스이므로,
조인 결과 토픽(`market.analytics.slippage`)의 접근 권한은 계정 데이터 기준으로 제한해야 한다.

### 5.4 outboundAccountPosition ⋈ ticker — 실시간 포트폴리오 평가액

**타입**: KStream-KTable 조인 (다중 심볼 KTable)

```text
binance.account.balance (자산별 잔고 변화)
        ⋈ (KTable lookup by asset→USDT symbol)
binance.raw.ticker/bookticker → KTable<symbol, lastPrice>
        → portfolio_value_usdt = Σ (balance[asset] * price[asset+USDT])
```

잔고 변화 이벤트가 올 때마다 최신 가격 KTable과 조인해서 USDT 환산 평가액을 즉시 갱신한다.
REST로 주기적으로 `/api/v3/account`를 폴링하는 대신, 이벤트 기반으로 대시보드에 바로 반영할 수 있다.

### 5.5 상관 심볼 간 trade ⋈ trade — 리드-래그(lead-lag) 탐지

**타입**: KStream-KStream 윈도우 셀프 조인 (다른 심볼끼리)

```text
binance.raw.trade (symbol=BTCUSDT)
        ⋈ (windowed, ±2s)
binance.raw.trade (symbol=ETHUSDT)
        → BTC 변동이 ETH보다 몇 ms 먼저 반영되는지 (correlation lag)
```

BTC가 알트코인 가격을 선행하는 경향을 정량화할 수 있다. 윈도우를 짧게(수백 ms~수 초) 잡고
가격 변화율의 교차상관을 계산하면, "BTC 급락 후 N초 내 알트 매도 압력" 같은 파생 신호를 만들 수 있다.

### 5.6 depth(오더북) ⋈ trade — 스푸핑/아이스버그 탐지

**타입**: KStream-KStream 윈도우 조인 (난이도 높음, 로컬 오더북 상태 필요)

```text
binance.raw.depth (오더북 diff, 로컬 오더북 재구성 필요)
        ⋈ (windowed by symbol)
binance.raw.trade
        → 대량 호가가 체결 없이 사라지는 패턴 탐지 (spoofing 후보)
```

가장 구현 난이도가 높다. `depth` diff로 로컬 오더북을 유지하는 상태 저장소(state store)가
선행되어야 하고, "호가가 취소로 사라졌는지 vs 체결로 사라졌는지"를 `trade` 스트림과 대조해야 한다.
MVP 이후 order flow 분석이 성숙한 단계에서 시도할 만하다.

---

### 구현 시 공통 주의사항

- **co-partitioning**: Kafka Streams의 KStream-KStream/KTable 조인은 두 토픽이 같은 키·같은
  파티션 수를 가져야 한다. 위 조인은 전부 `symbol`(또는 `accountId`) 키로 통일해뒀기 때문에
  바로 조인 가능하지만, 파티션 수가 다르면 리파티셔닝 토픽이 추가로 생긴다.
- **워터마크와 지각 이벤트**: 시세 스트림은 지연이 거의 없지만 청산·체결 이벤트는 거래소 측
  지연이 있을 수 있다. grace period를 두지 않으면 늦게 도착한 이벤트가 조인에서 누락된다.
- **5.3(체결 품질)은 계정 데이터를 포함**하므로 결과 토픽도 접근 제한 대상이다. 나머지는
  전부 공개 시장 데이터 간 조인이라 별도 접근 제한이 필요 없다.
