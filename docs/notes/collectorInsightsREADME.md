# Collector 인사이트 — 웹소켓 · Kafka 토픽 정리

spot/usdm/coinm/options 4개 시장 문서([binanceWebSocketREADME.md](../binance-api/binanceWebSocketREADME.md))를 필드 단위로
전부 정리하면서 나온 인사이트를 모았다. 인사이트별로 **어떤 웹소켓 스트림을 쓸지**, **어떤 Kafka 토픽·키로
발행/조인할지**를 같이 적는다. 토픽 기본 설계는 [kafkaTopicsREADME.md](kafkaTopicsREADME.md)를 따르고,
이 문서는 거기서 빠져 있거나 수정이 필요한 부분 위주로 다룬다.

---

## 1. Spot과 Futures는 같은 파서로 못 돌린다

`trade`/`bookTicker`/`depth` 모두 필드 개수 자체가 다르다. 심볼만 바꿔서 같은 DTO에 밀어넣으면 파싱이
깨지거나 필드가 조용히 유실된다.

| 필드 | Spot | USD-M/COIN-M |
| --- | --- | --- |
| `trade.X` | 없음 | 있음 (`MARKET`/`INSURANCE_FUND`) |
| `aggTrade.M` | 있음(ignore) | 없음 |
| `bookTicker.e/E/T/ps/st` | 없음 | 있음 |
| `depth.pu/ps/st` | 없음 | 있음 |

**웹소켓**: `<symbol>@trade`, `<symbol>@aggTrade`, `<symbol>@bookTicker`, `<symbol>@depth@100ms` —
Spot(`stream.binance.com`)과 Futures(`fstream.binance.com`/`dstream.binance.com`)를 **별도 커넥션·별도
파서**로 분리한다(이미 [binanceWebSocketREADME.md](../binance-api/binanceWebSocketREADME.md#1-시장-분류와-도메인)의 원칙).

**Kafka**: 파서를 분리해도 목적지 토픽(`binance.raw.trade` 등)은 공유할 수 있다. 단, §5의 키 충돌
문제를 반드시 같이 해결해야 한다.

---

## 2. `trade`의 `X` 필드로 청산성 체결을 미리 걸러낼 수 있다

USD-M/COIN-M `trade` 스트림의 `X`가 `"INSURANCE_FUND"`면 그 체결은 청산/ADL로 발생한 것이다.
`forceOrder`를 기다리지 않고 `trade` 자체에서 청산 신호를 1차로 감지할 수 있다는 뜻이다.

**웹소켓**: `<symbol>@trade` (USD-M/COIN-M). `aggTrade`에는 `X`가 없으므로 이 용도로는 `trade`를
버릴 수 없다 — 레거시라고 무조건 `aggTrade`로 대체하면 이 신호를 잃는다.

**Kafka**: `binance.raw.trade`에 `X` 필드를 payload 그대로 보존한다. 별도 토픽을 새로 만들 필요는
없고, §8(청산 캐스케이드 조인)의 보조 신호로 같은 토픽에서 필터링(`X == 'INSURANCE_FUND'`)해서 쓴다.

---

## 3. COIN-M은 수량 단위와 kline base/quote가 반대로 뒤집힌다

`q`(수량)이 코인이 아니라 계약 수(`BTCUSD_PERP` 1계약 = 100 USD)다. 게다가 kline의 `v`(baseVolume)는
계약 수, `q`(quoteVolume)는 코인 수량으로 **Spot/USD-M과 base·quote 의미가 반대**다. 그대로 저장하면
포지션 규모·거래대금 계산이 전부 틀어진다.

**웹소켓**: `<symbol>@aggTrade`, `<symbol>@kline_<interval>` (`dstream.binance.com`).

**Kafka**: `binance.raw.trade`/`binance.raw.kline`에 넣기 전에 **계약 수 → 코인 수량 환산**
(`계약 수 × 계약당 명목가치 / 가격`)을 Collector 단에서 미리 붙이거나, 최소한 `contract_qty`와
`coin_qty`를 envelope에 둘 다 넣는다. 원본 `q`만 보고 Spot과 같은 필드로 착각해 조인하면(예: §7의
베이시스 계산에 COIN-M을 끼워 넣는 경우) 수치가 100배 단위로 틀어진다.

---

## 4. TradFi Perpetual·Delivery는 스키마는 재사용, 심볼은 동적이다

TradFi Perpetual(NVDAUSDT 등)은 페이로드 구조가 Crypto Perpetual과 완전히 같아 파서를 재사용할 수
있다. 문제는 **Delivery(만기 선물)의 심볼 자체가 분기마다 바뀐다**는 점이다. 하드코딩하면 만기가
돌아올 때마다 구독이 끊긴다.

**웹소켓**: 심볼 목록을 하드코딩하지 않고 기동 시 + 주기적으로
`GET /fapi/v1/exchangeInfo`(USD-M) / `GET /dapi/v1/exchangeInfo`(COIN-M)를 조회해서
`contractType in (CURRENT_QUARTER, NEXT_QUARTER)`인 심볼로 구독 목록을 갱신한다. 이 부분은 REST
호출이지 웹소켓이 아니다 — Collector에 "심볼 레지스트리" 갱신 루프가 별도로 필요하다는 뜻이다.

**Kafka**: 구독 심볼이 바뀔 수 있으므로, symbol을 키로 쓰는 토픽들은 **파티션 리밸런싱이 아니라
심볼 문자열 자체가 바뀌는 상황**에 대비해야 한다(`BTCUSDT_260626` → `BTCUSDT_260925`). 분석
서비스에서 "BTC 다음 분기물"을 연속 시계열로 보려면 `contractType`(`CURRENT_QUARTER` 등)을 별도
로지컬 키로 envelope에 추가해서, 실제 심볼이 롤오버돼도 조인/집계가 끊기지 않게 한다.

---

## 5. `binance.raw.*` 토픽에 product 구분자가 없다 — 심볼 충돌 위험

[kafkaTopicsREADME.md](kafkaTopicsREADME.md#2-토픽별-상세)의 현재 설계는 파티션 키가 **순수
`symbol`**이다. 그런데 `BTCUSDT`는 Spot에도, USD-M Crypto Perpetual에도 동시에 존재한다. 같은
`binance.raw.trade` 토픽에 둘 다 같은 키로 들어가면:

- 컨슈머가 payload의 `product` 필드를 안 보고 그냥 심볼로 조인하면 Spot 체결과 Futures 체결이
  섞인다.
- §7(베이시스 조인)처럼 **의도적으로** Spot과 Futures를 붙여야 하는 케이스와, 실수로 섞이는 케이스를
  키만 봐서는 구분할 수 없다.

**웹소켓**: 이 문제는 스트림 자체보다 Collector가 여러 product를 하나의 토픽에 발행하는 설계에서
생긴다.

**Kafka**: 다음 둘 중 하나를 선택해야 한다.
1. **복합 키**: 파티션 키를 `symbol` 대신 `product:symbol`(예: `spot:BTCUSDT`, `usdm:BTCUSDT`)로 바꾼다.
2. **토픽 분리**: `binance.raw.trade.spot` / `binance.raw.trade.usdm` / `binance.raw.trade.coinm`으로
   나눈다.

의도적 조인(§7)은 어차피 서로 다른 이벤트 타입(`trade` vs `markPrice`) 토픽 간 조인이라 이 문제와
무관하다. 문제는 **같은 이벤트 타입, 다른 product**를 하나의 토픽·키로 뭉쳤을 때다.

---

## 6. Options는 이 환경에서 WebSocket 검증이 안 됐다 — REST 우선

`options_market_stream.ipynb` PoC에서 `wss://nbstream.binance.com/eoptions/ws/...`가 handshake
단계에서 404를 반환했다(REST `/eapi/v1/*`는 정상). 리전/네트워크 제한일 가능성이 높지만, **운영
환경에서도 재현되면 옵션 시장 데이터는 웹소켓으로 못 받는다**는 뜻이므로 폴백 경로가 필요하다.

**웹소켓**: `<symbol>@ticker`(그릭스+IV), `<symbol>@trade`, `<underlying>@index`,
`<underlyingAsset>@openInterest@<expiration>` — 연결 가능 여부를 배포 환경에서 먼저 확인한다.

**Kafka**: 웹소켓이 막히는 환경을 가정해 REST 폴링 기반 대체 경로를 설계해둔다(`GET /eapi/v1/ticker`
주기 폴링 → 같은 `binance.raw.*` 토픽에 발행). **현재 [kafkaTopicsREADME.md](kafkaTopicsREADME.md)에는
Options용 토픽이 아예 없다** — `binance.raw.option_ticker`(key: `symbol`), `binance.raw.option_oi`
(key: `underlyingAsset:expiration`) 같은 신규 토픽 추가가 필요하다.

---

## 7. Basis(베이시스) 조인 — Spot trade ⋈ USD-M markPrice

무기한 선물 마크 가격과 현물 체결가의 괴리는 현물-선물 차익거래 신호다.

**웹소켓**: `btcusdt@trade` (Spot, `stream.binance.com`) + `btcusdt@markPrice` (USD-M, `fstream.binance.com`)

**Kafka 조인**:
```text
binance.raw.trade      (key=symbol, product=spot)
        ⋈ windowed by symbol, 5~10s tumbling/hopping
binance.raw.markprice  (key=symbol, product=usdm-futures)
        → basis = markPrice - spotTradePrice
```
§5의 키 충돌 문제 때문에, 이 조인을 하려면 오히려 `binance.raw.trade`에서 **`product=spot`인 것만
필터링**해서 조인해야 한다(복합 키를 쓴다면 `spot:BTCUSDT`로 명시).

---

## 8. 청산 캐스케이드 조인 — forceOrder ⋈ trade

대량 강제 청산 전후 가격 충격/캐스케이드 여부를 측정한다.

**웹소켓**: `!forceOrder@arr` (USD-M/COIN-M 전체 심볼) + `<symbol>@trade`

**Kafka 조인**:
```text
binance.raw.forceorder (key=symbol)
        ⋈ windowed by symbol, -5s ~ +30s
binance.raw.trade      (key=symbol, product=usdm-futures 또는 coinm-futures)
        → 청산 전후 가격 변화율, 거래량 급증 여부
```
`forceOrder.o.q`(청산 수량)로 대량 청산만 필터링해 노이즈를 줄인다. §2의 `trade.X == INSURANCE_FUND`를
같이 보면 `forceOrder` 이벤트 없이도 소규모 청산을 놓치지 않는다.

---

## 9. 체결 품질(슬리피지) 조인 — 내 executionReport ⋈ bookTicker

내 체결가와 체결 시점 시장 mid-price를 비교해 실행 품질을 측정한다. **시장 데이터(공개) + 계정
데이터(민감)를 조인하는 유일한 케이스**이므로 접근 권한 처리가 다르다.

**웹소켓**: User Data Stream(`executionReport` / `ORDER_TRADE_UPDATE`, listenKey 필요) +
`<symbol>@bookTicker`

**Kafka 조인**:
```text
binance.account.order   (key=accountId)  ← executionReport, x=TRADE
        ⋈ KTable lookup by symbol (accountId 키가 아니라 symbol로 재-key 필요)
binance.raw.bookticker  (key=symbol, KTable materialize)
        → slippage = 내 체결가 - 체결 시점 mid_price
```
`accountId` 키와 `symbol` 키가 다르므로 **`binance.account.order`를 symbol로 리파티셔닝**하는
중간 토픽이 필요하다(co-partitioning 요구사항). 조인 결과 토픽(`market.analytics.slippage`)은
계정 데이터 기준으로 접근을 제한한다.

---

## 10. 포트폴리오 평가액 조인 — balance ⋈ ticker

잔고 변화가 올 때마다 최신 가격과 조인해 USDT 환산 평가액을 즉시 갱신한다.

**웹소켓**: User Data Stream(`outboundAccountPosition`/`balanceUpdate`/`ACCOUNT_UPDATE`) +
`<symbol>@bookTicker` 또는 `<symbol>@ticker`

**Kafka 조인**:
```text
binance.account.balance (key=accountId)  ← asset별 잔고
        ⋈ KTable lookup by asset→USDT symbol (asset을 symbol로 매핑하는 재-key 필요)
binance.raw.bookticker/ticker → KTable<symbol, lastPrice>
        → portfolio_value_usdt = Σ (balance[asset] * price[asset+USDT])
```
`asset`(예: `BTC`) → `symbol`(예: `BTCUSDT`) 매핑 테이블이 별도로 필요하다. `exchangeInfo`의
`baseAsset`/`quoteAsset`으로 정적 매핑을 만들어두거나 별도 config 토픽으로 관리한다.

---

## 외부 데이터가 더 필요한 부분

아래는 이 저장소 안의 정보만으로는 확정할 수 없어서, 실제 라이브 연결로 검증하거나 공식 문서를 다시
확인해야 하는 항목이다.

- **`bookTicker`/`depth`의 `st`(symbolType) enum 값** — Crypto Perpetual/TradFi Perpetual/Delivery가
  각각 어떤 값을 갖는지 공식 문서에 명시가 없어 관찰치로만 적어뒀다. 실제 세 카테고리 심볼을
  동시에 구독해서 값을 직접 확인하는 게 제일 확실하다.
  → 이건 세 카테고리(Crypto Perpetual/TradFi Perpetual/Delivery) 심볼을 동시에 구독해서
    `st` 값을 실측하는 짧은 PoC로 바로 검증 가능하다(외부 문서 없이도 됨).
- **Options WebSocket의 실제 payload** — `options_market_stream.ipynb`가 이 환경에서 404를 받아서
  `ticker`/`trade`/`index`/`openInterest`의 필드 목록은 공식 문서 기반 추정치다. 운영 배포 환경
  (다른 리전/네트워크)에서 실제로 연결해 캡처해봐야 확정할 수 있다.
- **Options 웹소켓이 특정 리전에서 막히는지 여부** — Binance 공식 지역 제한 정책 문서나 실제 배포
  환경에서의 연결 테스트가 필요하다. 이건 이 저장소 밖의 정보라 내가 확인할 수 없다.

---

## 참고 문서

- [binanceWebSocketREADME.md](../binance-api/binanceWebSocketREADME.md) — 시장별 도메인·운영 규칙
- [kafkaTopicsREADME.md](kafkaTopicsREADME.md) — 토픽 기본 설계와 조인 아이디어 원본
- [spotMarketStreamREADME.md](../streams/spotMarketStreamREADME.md) · [usdmFuturesMarketStreamREADME.md](../streams/usdmFuturesMarketStreamREADME.md) · [coinmFuturesMarketStreamREADME.md](../streams/coinmFuturesMarketStreamREADME.md) · [optionsMarketStreamREADME.md](../streams/optionsMarketStreamREADME.md)
