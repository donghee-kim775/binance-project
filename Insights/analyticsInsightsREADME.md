# 분석 인사이트 아카이브 — 웹소켓 · 구현 방식

CDC 파이프라인 위에서 시도해볼 만한 **분석 인사이트**를 모은 문서다. 인사이트마다 (1) 무엇을 포착하려는
건지, (2) 그걸 만들려면 **어떤 웹소켓 스트림이 필요한지**, (3) **어떻게 계산/구현하는지**를 같이 적는다.
Kafka 토픽·조인 관점의 기술적 세부사항은 [collectorInsightsREADME.md](collectorInsightsREADME.md)와
[kafkaTopicsREADME.md](kafkaTopicsREADME.md)를 참고한다. 이 문서는 그 위에서 "왜 이 조인이 의미 있는
신호가 되는지"에 집중한다.

---

### Insight 내역

---

#### 1. 실시간 차익거래 기회 및 스프레드 탐지

> 바이낸스 웹소켓을 통해 들어오는 실시간 호가/체결 데이터와 DB에 저장된 상태 데이터를 결합하여
> **시장 간 혹은 거래소 간, 혹은 특정 상품 간의 미세한 가격 괴리(스프레드)를 실시간으로 포착**하는 인사이트

- **Insight 1-1. Spot-Futures 베이시스**

  가격 변동성이 클 때, 같은 심볼의 **현물(Spot) 가격**과 **무기한 선물(Perpetual) 마크 가격**이
  순간적으로 벌어지는 지점을 포착한다. 베이시스가 확대되고 펀딩비가 한쪽으로 쏠리면 과열 신호다.

  - **필요한 웹소켓**
    - `btcusdt@bookTicker` (Spot, `stream.binance.com`) — 체결가(`trade`)보다 최우선 호가가
      실시간 괴리를 더 촘촘하게 보여준다.
    - `btcusdt@markPrice` (USD-M, `fstream.binance.com`) — 마크 가격 + 펀딩비.
  - **방식**
    - Kafka Streams `KStream-KStream` 윈도우 조인, 5~10초 tumbling/hopping window, 키는 `symbol`.
    - `basis = markPrice - spotMidPrice`, `spotMidPrice = (bestBid + bestAsk) / 2`.
    - 베이시스와 펀딩비(`r`)를 같이 겹쳐서, 베이시스 확대 방향과 펀딩비 부호가 일치하는 구간만
      알림 대상으로 좁히면 노이즈가 줄어든다.
    - 자세한 토픽/키 설계는 [collectorInsightsREADME.md](collectorInsightsREADME.md#7-basis베이시스-조인--spot-trade--usd-m-markprice) 참고.
  - **과매도 / 과매수 판단 기준**
    1. **통계적 임계치**
       - 최근 N개 basis 윈도우 값으로 이동평균(μ)과 표준편차(σ)를 갱신한다(예: N=20~30개
         윈도우 ≈ 최근 2~5분치). 매번 전체 윈도우를 다시 훑지 않도록 EMA나 Welford's
         algorithm으로 순차 갱신한다.
       - `z = (basis_t - μ) / σ`로 표준화해서, `|z| > 2`(정책에 맞게 조정 가능한 임계치)를
         넘으면 과매수/과매도 후보로 판정한다.
       - `z > 0`이면 선물이 현물 대비 과열(콘탱고 확대, 롱 쪽 과열), `z < 0`이면 반대
         (백워데이션 확대, 숏 쪽 과열)로 해석한다.
    2. **시간 윈도우 유지 조건**
       - 후보 판정이 나왔다고 바로 알림을 보내지 않고, **연속 3~5개 윈도우** 동안
         `|z| > 2` 조건이 유지될 때만 알림을 발송한다.
       - 구현은 상태 저장소(`KTable`)에 `consecutiveCount`를 같이 유지한다: 임계치를 넘으면
         +1, 벗어나면 0으로 리셋. `consecutiveCount >= 3`(정책값)이 되는 순간 알림을 1회
         발행하고, 조건이 풀릴 때까지는 재발행하지 않는다(디바운스).
       - 5~10초 윈도우 기준 3~5개면 15~50초 정도 지속돼야 알림이 나가므로, 순간
         스파이크성 노이즈와 구조적으로 벌어진 괴리를 구분할 수 있다.

- **Insight 1-2. 선물 캘린더 스프레드**

  > 만기가 다른 두 선물 상품 간의 가격 차이가 좁혀지거나 벌어질 때를 포착하는 구조.

  - 가정 사항
    - 근원물 (**9월** 만기 선물) : $61,000
    - 원월물 (**12월** 만기 선물) : $63,000
    - 현재 스프레드 : $2,000 ($63,000 - $61,000)
  - **활용 전략: 선물 캘린더 스프레드 매도** — 만기가 다른 두 선물 상품 간의 가격 차이가 줄어들 때
    수익이 나는 구조. 스프레드가 비정상적으로 벌어진 시점에 진입, 좁혀지면 청산한다.
  - **필요한 웹소켓**
    - Delivery(만기 선물)의 Current Quarter / Next Quarter 심볼 각각의 `<symbol>@bookTicker`.
      심볼은 하드코딩하지 않고 `GET /fapi/v1/exchangeInfo`의 `contractType`으로 매번 조회한다
      ([usdmFuturesMarketStreamREADME.md §9](usdmFuturesMarketStreamREADME.md#9-delivery--만기-선물)).
  - **방식**
    - `KTable-KTable` 조인 — 윈도우 조인이 아니라, 두 계약의 최신 호가를 각각 `KTable`로
      materialize한 뒤 값이 갱신될 때마다 `spread = nextQuarterMid - currentQuarterMid`를 재계산한다.
    - 만기 롤오버(심볼 자체가 바뀌는 시점)에는 옛 심볼 구독을 끊고 새 심볼로 갈아타야 하므로,
      심볼 레지스트리 갱신 로직과 반드시 같이 붙는다.

- **Insight 1-3. 옵션 스트래들 매도**

  > 옵션 거래에서 콜과 풋을 같은 행사가·만기로 묶어서 거래하는 것. 스트래들 가격(콜+풋 프리미엄
  > 합)이 시장이 예상하는 변동성 폭을 과대평가하고 있으면 매도 기회다.

  - **필요한 웹소켓**
    - 같은 행사가·만기의 Call/Put 심볼 각각의 `<symbol>@ticker` (그릭스+IV+마크 가격 포함,
      [optionsMarketStreamREADME.md §1](optionsMarketStreamREADME.md#1-ticker--그릭스와-iv)).
    - ATM(등가) 행사가는 하드코딩하지 않고 `GET /eapi/v1/exchangeInfo` → `expiryDate` 기준 정렬로
      매번 골라야 한다([optionsMarketStreamREADME.md 심볼 조회 흐름](optionsMarketStreamREADME.md#심볼-조회-흐름)).
  - **방식**
    - `straddlePrice = call.markPrice + put.markPrice`를 실시간으로 계산하고, 기초자산의
      역사적 변동성(realized vol, `<underlying>@index`로 수집한 가격의 rolling stddev)과 비교해
      내재변동성(`vo`)이 과대평가된 구간을 찾는다.
    - 옵션 웹소켓은 이 환경에서 연결이 안 됐던 이력이 있으므로([optionsMarketStreamREADME.md](optionsMarketStreamREADME.md)
      상단 참고 박스), 실제 배포 환경에서 연결 가능 여부부터 확인해야 한다.

- **Insight 1-4. 타 거래소 간 스프레드**

  이 저장소는 현재 **Binance 단일 거래소만** 수집한다. 거래소 간 스프레드를 보려면 다른 거래소의
  웹소켓 수집기가 추가로 필요하다 — 이건 이 프로젝트 범위 밖이라 별도 논의가 필요하다.

---

#### 2. 대규모 유입에 따른 DB 병목 및 부하 패턴 분석 (시스템 인사이트)

> 데이터 분석 관점이 아니라, 인프라와 시스템 성능 관점에서 도출하는 인사이트.
> 바이낸스의 방대한 데이터 유입이 데이터베이스와 CDC 파이프라인에 미치는 영향을 분석한다.

이 인사이트는 시장 조인이 아니라 **시장 이벤트 발생량과 시스템 지표를 같은 시간축에서 겹쳐 보는 것**이
핵심이다. "시장이 급변할 때 파이프라인이 어디서부터 밀리는지"를 찾는다.

- **필요한 웹소켓**
  - 특별히 새로 구독할 스트림은 없다 — 이미 수집 중인 모든 스트림, 그중에서도 메시지 빈도가 가장
    높은 `<symbol>@depth@100ms`(100ms 오더북 diff)가 부하의 주요 원인일 가능성이 크다.
- **방식**
  - Collector가 이미 노출하기로 되어 있는 metric들을 시계열로 수집한다
    ([binanceAPIREADME.md](binanceAPIREADME.md#rate-limit과-장애-처리)의 `binance_event_received_total`,
    `binance_queue_size`, `binance_event_lag_ms` 등).
  - 여기에 **DB 쓰기 지연**(배치 insert latency), **Debezium WAL lag**(`cdc.public.market_events`로
    복제되기까지 걸리는 시간)을 같은 대시보드에 겹친다.
  - 시장 변동성이 큰 구간(예: 체결량 급증, `forceOrder` 빈발 구간)과 시스템 지표 스파이크가 같은
    시각에 나타나는지 상관관계를 본다. → "변동성 급증 시 어느 스트림이 파이프라인을 밀리게 하는지"를
    실증적으로 찾을 수 있다.
  - 실험적으로 `depth@100ms`를 껐다 켰다 하면서 DB insert latency/Debezium lag 변화를 측정하거나,
    단건 INSERT 대비 배치/COPY 삽입 전략을 비교하는 부하 테스트를 별도로 설계할 수 있다.
  - 이 인사이트는 마켓 데이터 자체보다 **Prometheus/Grafana 같은 운영 metric 시스템**이 먼저
    갖춰져야 의미가 있다 — 아직 이 저장소에 없다면 우선순위로 검토할 부분이다.

---

#### 3. 실시간 고래 거래 감지 및 주문장 심층 분석

> 시장 참여자들의 미시적인 행동 패턴을 실시간으로 추적하여 유의미한 시그널을 만드는 인사이트.

- **Insight 3-1. 고래 거래(대량 체결) 감지**

  - **필요한 웹소켓**: `<symbol>@aggTrade` — 하나의 taker 주문이 여러 maker와 나눠 체결된 총량을
    하나로 보여주므로, 개별 `trade`보다 "한 번의 큰 의도"를 포착하기에 유리하다.
  - **방식**
    - 심볼별로 최근 체결 수량의 rolling 분포(예: 최근 1시간 기준 상위 0.1%)를 유지하고, 이 임계값을
      넘는 `aggTrade`를 "고래 거래"로 플래그한다. 고정 절대값 임계치보다 심볼별 유동성 차이를
      반영할 수 있다.
    - `m`(isBuyerMaker)로 매수/매도 방향까지 같이 태깅해서 "고래가 사는지 파는지"를 구분한다.

- **Insight 3-2. 주문장(오더북) 심층 분석 — 매수/매도벽, 스푸핑 후보**

  - **필요한 웹소켓**: `<symbol>@depth@100ms` (오더북 diff). 반드시 REST snapshot과 동기화해서
    로컬 오더북을 재구성해야 하며, 단독 스트림만으로는 안 된다
    ([binanceAPIREADME.md 로컬 오더북](binanceAPIREADME.md#로컬-오더북)).
  - **방식**
    - Kafka Streams state store에 심볼별 로컬 오더북을 유지한다(난이도 높음,
      [kafkaTopicsREADME.md §5.6](kafkaTopicsREADME.md#56-depth오더북--trade--스푸핑아이스버그-탐지)과 동일 아이디어).
    - 특정 가격대에 비정상적으로 큰 잔량("벽")이 잡혔다가, 그 가격에서 **체결 없이** 사라지면
      스푸핑 후보로 플래그한다 — 이걸 판별하려면 같은 구간의 `<symbol>@aggTrade`와 대조해서
      "취소로 사라졌는지 vs 체결로 사라졌는지"를 확인해야 한다.
    - `README.md`에 이미 정의된 오더북 불균형 공식을 그대로 활용한다.
      ```text
      order_imbalance =
      (total_bid_quantity - total_ask_quantity)
      / (total_bid_quantity + total_ask_quantity)
      ```
    - 고래 거래(3-1) 발생 시점 전후로 `order_imbalance`가 어떻게 움직이는지 겹쳐 보면, "고래
      매수 이후 실제로 매수 압력이 이어지는지" 혹은 "매수벽만 세우고 체결 없이 취소하는 패턴인지"를
      구분할 수 있다.
    - 누적 체결량(CVD, `README.md`의 Order Flow 분석 항목)과 함께 보면 고래 거래 이후 후속
      추세 형성 여부도 같이 검증 가능하다.
  - **필요한 보조 웹소켓**: `!forceOrder@arr` — 고래 거래가 강제 청산으로 이어지는지까지 보려면
    청산 스트림을 같이 조인한다([collectorInsightsREADME.md §8](collectorInsightsREADME.md#8-청산-캐스케이드-조인--forceorder--trade)).

---

#### 4. 리드-래그(Lead-Lag) 탐지

> BTC가 알트코인 가격을 선행하는 경향을 정량화해서, "BTC 급변 후 N초 내 알트 반응"을 예측 신호로
> 쓸 수 있는지 검증하는 인사이트.

- **필요한 웹소켓**: `btcusdt@trade` + `ethusdt@trade` (같은 시장 안에서 비교해야 한다 — Spot과
  USD-M을 섞으면 [collectorInsightsREADME.md §5](collectorInsightsREADME.md#5-binanceraw-토픽에-product-구분자가-없다--심볼-충돌-위험)의
  키 충돌 문제가 그대로 적용된다).
- **방식**
  - `KStream-KStream` 셀프 조인, 짧은 윈도우(수백ms~수초)로 각 심볼의 가격 변화율(return)을 계산해
    상태 저장소에 최근 N개를 버퍼링한다.
  - 여러 lag 값(0ms, 100ms, 500ms, 1s, ...)에 대해 BTC return과 ETH return의 교차상관을 계산하고,
    상관이 최대가 되는 lag를 "선행 시간"으로 추정한다.
  - 자세한 조인 아이디어는 [kafkaTopicsREADME.md §5.5](kafkaTopicsREADME.md#55-상관-심볼-간-trade--trade--리드-래그lead-lag-탐지) 참고.

---

#### 5. 체결 품질(슬리피지) 측정

> 내 주문이 실제로 체결된 가격과, 체결 시점의 시장 mid-price를 비교해 **실행 품질**을 정량화하는
> 인사이트. 특정 시간대·주문 방식(시장가 vs 지정가)이 슬리피지를 더 유발하는지 비교하는 데 쓴다.

- **필요한 웹소켓**: User Data Stream(`executionReport` / `ORDER_TRADE_UPDATE`, listenKey 필요) +
  `<symbol>@bookTicker`
- **방식**
  - `slippage = 내 체결가 - 체결 시점 mid_price`. `bookTicker`를 심볼별 최신 상태로 materialize한
    `KTable`과 내 체결 이벤트를 조인한다.
  - 시장 데이터(공개) + 계정 데이터(민감)를 조인하는 유일한 케이스라 결과 토픽 접근 권한을 계정
    데이터 기준으로 제한해야 한다. 토픽·키 설계는 [collectorInsightsREADME.md §9](collectorInsightsREADME.md#9-체결-품질슬리피지-조인--내-executionreport--bookticker) 참고.

---

#### 6. 실시간 포트폴리오 평가액

> 잔고가 바뀔 때마다 최신 가격과 즉시 조인해서 USDT 환산 평가액을 갱신하는 인사이트. REST 폴링 없이
> 이벤트 기반으로 대시보드에 바로 반영할 수 있다.

- **필요한 웹소켓**: User Data Stream(`outboundAccountPosition` / `balanceUpdate` / `ACCOUNT_UPDATE`) +
  `<symbol>@bookTicker` 또는 `<symbol>@ticker`
- **방식**
  - `portfolio_value_usdt = Σ (balance[asset] * price[asset+USDT])`. 자산→심볼 매핑 테이블이
    별도로 필요하다(`exchangeInfo`의 `baseAsset`/`quoteAsset`으로 정적 매핑 생성).
  - 평가액 변화율을 실현/미실현 손익과 분리해서 보여주면, 포지션 손실 임계치 초과 알림 같은
    리스크 관리 기능으로도 재사용할 수 있다. 자세한 내용은 [collectorInsightsREADME.md §10](collectorInsightsREADME.md#10-포트폴리오-평가액-조인--balance--ticker) 참고.

---

#### 7. 펀딩비 사이클 기반 포지션 쏠림 · 과열 탐지

> 8시간마다 정산되는 펀딩비가 지속적으로 한 방향으로 크면, 레버리지 포지션이 한쪽으로 쏠렸다는
> 신호다. 펀딩비 극단치는 종종 단기 되돌림(스퀴즈)의 선행 지표로 쓰인다.

- **필요한 웹소켓**: `<symbol>@markPrice` (USD-M/COIN-M, `r` 필드 + `nextFundingTime`). 여러 심볼을
  한 번에 스캔하려면 `!markPrice@arr`.
- **방식**
  - 펀딩비 시계열을 rolling window(최근 3~5 사이클)로 누적하고, 연환산 기준 특정 임계값을
    지속적으로 초과하면 알림을 보낸다.
  - Insight 1-1(Spot-Futures 베이시스)과 겹쳐 보면, 펀딩비 쏠림이 실제 가격 프리미엄으로도
    나타나는지 교차검증할 수 있다.
  - 펀딩비 부호가 전환되는 시점과 이후 가격 흐름을 과거 데이터로 backtest하면, "스퀴즈 패턴"이
    통계적으로 유의미한지 검증할 수 있다.

---

#### 8. COIN-M vs USD-M 베이시스 비교 — 마진 통화별 시장 심리 차이

> 같은 기초자산(BTC)이라도 코인마진(COIN-M)과 USDT마진(USD-M) 무기한 선물의 펀딩비·베이시스가
> 다르게 움직이면, 두 마진 통화를 쓰는 참여자군의 리스크온/오프 심리 차이를 드러내는 신호다.

- **필요한 웹소켓**: `btcusdt@markPrice` (USD-M) + `btcusd_perp@markPrice` (COIN-M)
- **방식**
  - `KTable-KTable` 조인, 기초자산(BTC) 기준으로 두 마켓의 최신 펀딩비를 각각 유지하고
    `fundingRateDiff = usdmFundingRate - coinmFundingRate`를 시계열화한다.
  - 두 마켓 펀딩비가 괴리될 때 어느 쪽이 먼저 반응하는지는 Insight 4(리드-래그)와 같은 기법으로
    분석할 수 있다.
  - COIN-M의 `q`(계약 수)와 USD-M의 `q`(코인 수량) 단위가 다르므로
    ([usdmFuturesMarketStreamREADME.md](usdmFuturesMarketStreamREADME.md), [coinmFuturesMarketStreamREADME.md §3](coinmFuturesMarketStreamREADME.md#3-kline_ltintervalgt--ohlcv-캔들)),
    이 조인은 가격/펀딩비만 비교하고 수량은 섞지 않는다.

---

#### 9. 옵션 시장 심리 지표 — Put/Call 비율과 IV 스큐

> 특정 만기의 콜/풋 미결제약정 비율(Put/Call Ratio)과, 등가(ATM) 대비 외가격(OTM) 풋·콜의 내재
> 변동성 차이(스큐)는 시장의 공포/탐욕을 정량화하는 전통적인 옵션 지표다.

- **필요한 웹소켓**: `<underlyingAsset>@openInterest@<expiration>` (같은 만기의 여러 행사가 심볼
  각각) + `<symbol>@ticker` (IV 확인용, 마찬가지로 여러 행사가)
- **방식**
  - `GET /eapi/v1/exchangeInfo`로 같은 만기(`expiration`)의 전체 행사가 목록을 조회한 뒤, 행사가별
    `openInterest`/`vo`(IV)를 `KTable`로 유지한다.
  - `PCR = Put 전체 openInterest / Call 전체 openInterest`.
  - `skew = OTM put의 IV - OTM call의 IV`. 시간에 따른 PCR·스큐 추세를 만기별로 비교한다.
  - Options 웹소켓 연결 자체가 이 환경에서 검증되지 않았으므로([optionsMarketStreamREADME.md](optionsMarketStreamREADME.md)
    상단 참고 박스), REST `GET /eapi/v1/ticker` 폴링으로 먼저 프로토타입해볼 수 있다.

---

#### 10. TradFi Perpetual 정규장外 가격 괴리 탐지

> `NVDAUSDT` 같은 TradFi Perpetual은 24시간 거래되지만, 기초 주식(NVDA)은 미국 정규장 시간에만
> 실제 체결이 발생한다. 정규장이 닫힌 시간대에 TradFi Perpetual 가격이 크게 움직이면, 그건 시장의
> "예측"일 뿐이라 정규장 재개 시 실제 시가와 갭(gap)으로 수렴할 가능성이 있는 신호다.

- **필요한 웹소켓**: `nvdausdt@bookTicker` (또는 `trade`)
- **방식**
  - 미국 정규장 마감 시점의 TradFi Perpetual 가격을 기준가로 저장하고, 마감 후 누적 변동폭을
    추적한다.
  - 정규장 재개 시 실제 시가와 직전 TradFi Perpetual 가격을 비교해서 "장외 예측이 맞았는지"를
    검증하는 백테스트도 가능하다.
  - **주의**: 미국 정규장 개장/휴장 캘린더(서머타임, 공휴일 포함)는 이 저장소 안의 정보로는 알 수
    없는 **외부 데이터**다. 정확한 정규장 스케줄을 확보해야 이 인사이트를 구현할 수 있다.

---

#### 11. 실시간 시장 스캐너 — 급등락 · 거래량 순위

> 전체 심볼을 한 번에 보고 급등/급락, 거래량 급증 심볼을 실시간으로 랭킹한다. `README.md` MVP
> 목록의 "심볼별 상승률 및 거래량 순위"를 구체적인 웹소켓·구현으로 확장한 것이다.

- **필요한 웹소켓**: `!miniTicker@arr` (Spot 전체 심볼 24hr 통계) + `!markPrice@arr` (USD-M 전체
  마크 가격/펀딩비) + `!forceOrder@arr` (청산 급증 심볼도 같이 스캔)
- **방식**
  - 전체 심볼의 24시간 변동률·거래량을 `KTable`로 유지하고, 갱신될 때마다 top-N을 재계산한다
    (우선순위 큐 또는 주기적 배치 정렬).
  - 대시보드에 실시간 랭킹 테이블로 노출한다.
  - 청산(`forceOrder`) 빈도가 높은 심볼과 겹쳐 보면, "지금 변동성이 실제로 청산을 유발하고 있는
    심볼"까지 같이 하이라이트할 수 있다.

---

## 참고 문서

- [collectorInsightsREADME.md](collectorInsightsREADME.md) — 웹소켓 스키마 차이, Kafka 토픽·키 설계 인사이트
- [kafkaTopicsREADME.md](kafkaTopicsREADME.md) — 토픽 설계와 조인 원본 아이디어
- [../README.md](../README.md) — 프로젝트 전체 구조, 분석 기능 아이디어(MVP/Order Flow)
