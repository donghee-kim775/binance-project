# USD-M Futures Market Stream — `fstream.binance.com`

USDT/USDC로 정산되는 무기한·만기 선물 시장의 공개 WebSocket 스트림 정리다. API 키는 필요 없다.
2026-08-26 기준 공식 문서를 참고했다.

- 전체 개요: [binanceWebSocketREADME.md](../binance-api/binanceWebSocketREADME.md)
- 계정 이벤트: [userDataStreamREADME.md](userDataStreamREADME.md)
- 관련 시장: [coinmFuturesMarketStreamREADME.md](coinmFuturesMarketStreamREADME.md) · [optionsMarketStreamREADME.md](optionsMarketStreamREADME.md)
- 상위 API 가이드: [binanceAPIREADME.md](../binance-api/binanceAPIREADME.md)

---

## 도메인

| 항목 | 값 |
| --- | --- |
| WebSocket | `wss://fstream.binance.com` |
| REST Base | `https://fapi.binance.com` |

세 가지 하위 카테고리가 있다.

```text
fstream.binance.com
 ├─ Crypto Perpetual (암호화폐 무기한)
 │   ├─ BTCUSDT
 │   └─ ETHUSDT
 ├─ TradFi Perpetual (주식 무기한)
 │   ├─ NVDAUSDT
 │   └─ MRVLUSDT
 └─ Delivery (만기 선물)
     ├─ BTCUSDT_260626  Current Quarter
     └─ BTCUSDT_260925  Next Quarter
```

TradFi Perpetual은 실제 Nasdaq 주식 현물이 아니라 `TRADFI_PERPETUAL` 파생상품이며, 스트림 구조는
Crypto Perpetual과 동일하다. Delivery(만기 선물)의 심볼은 만기가 돌아올 때마다 바뀌므로
하드코딩하지 않고 `GET /fapi/v1/exchangeInfo`의 `contractType`으로 현재/다음 분기 심볼을
매번 조회한다.

## 연결 방식

```text
단일:  wss://fstream.binance.com/ws/btcusdt@aggTrade
결합:  wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@markPrice
```

Spot과 스트림 이름은 비슷하지만 **페이로드에 필드가 추가된다.**

---

## 스트림 목록

| 스트림 | 용도 | Spot과의 차이 |
| --- | --- | --- |
| `<symbol>@trade` | 개별 선물 체결 | `X`(체결 유형) 필드 추가 |
| `<symbol>@aggTrade` | 집계 선물 체결 | `M` 필드 없음 |
| `<symbol>@kline_<interval>` | 선물 OHLCV | Spot과 동일 구조 |
| `<symbol>@markPrice` | **마크 가격 + 펀딩비** | 선물 전용 |
| `<symbol>@markPrice@1s` | 마크 가격 1초 갱신 | 선물 전용 |
| `<symbol>@bookTicker` | 최우선 호가 | `e`/`E`/`T`/`ps`/`st` 필드 추가 |
| `<symbol>@forceOrder` | **강제 청산** | 선물 전용 |
| `!forceOrder@arr` | 전 심볼 강제 청산 | 선물 전용 |
| `<symbol>@depth@100ms` | 오더북 diff | `pu`/`ps`/`st` 필드 추가 |
| `!markPrice@arr` | 전 심볼 마크 가격 | 선물 전용 |

스트림 이름의 심볼은 Spot과 마찬가지로 **소문자**를 쓴다(예: `btcusdt@trade`).

## 1. trade — 개별 선물 체결 데이터

```json
{
  "e": "trade",             # eventType    / 이벤트 종류
  "E": 1787498072701,       # eventTime    / 이벤트 생성 시간
  "T": 1787498072700,       # tradeTime    / 실제 체결 시간
  "s": "BTCUSDT",           # symbol       / 거래쌍
  "t": 3892345678,          # tradeId      / 체결 고유 번호
  "p": "77300.91000000",    # price        / 체결 가격
  "q": "0.00300000",        # quantity     / 체결 수량
  "X": "MARKET",            # execType     / 체결 실행 유형
  "m": true                 # isBuyerMaker / true면 매수자가 maker = taker가 매도
}
```

Spot의 `trade`와 필드 구성은 거의 같지만 **`X` 필드가 추가된다.** 평소에는 `"MARKET"`이지만
청산/ADL(자동감산)로 체결된 거래는 `"INSURANCE_FUND"`로 표시된다. 즉 `X`만 보고도 일반 체결과
강제청산성 체결을 구분할 수 있다. `forceOrder` 스트림과 함께 청산 감지에 쓸 수 있는 보조 신호다.

> Binance는 `trade` 스트림을 레거시로 취급하고 신규 구현에는 `aggTrade` 사용을 권장한다.

## 2. aggTrade — 집계 선물 체결 데이터

```json
{
  "e": "aggTrade",          # eventType
  "E": 1787498072701,       # eventTime
  "a": 4041839950,          # aggTradeId   / 집계 체결 번호
  "s": "BTCUSDT",           # symbol
  "p": "77188.05000000",    # price        / 집계 체결 가격
  "q": "0.02300000",        # quantity     / 집계 체결 수량 (합산됨)
  "f": 6605832187,          # firstTradeId / 포함된 첫 tradeId
  "l": 6605832187,          # lastTradeId  / 포함된 마지막 tradeId
  "T": 1787498072700,       # tradeTime
  "m": true                 # isBuyerMaker
}
```

Spot의 `aggTrade`와 필드는 동일하되 `M`(ignore) 필드가 없다. 나머지 의미와 이중 집계 주의사항은
[spotMarketStreamREADME.md](spotMarketStreamREADME.md#2-aggtrade--집계-체결-데이터)와 같다.

## 3. kline_&lt;interval&gt; — 선물 OHLCV 캔들

```json
{
  "e": "kline",               # eventType
  "E": 1787498072701,         # eventTime
  "s": "BTCUSDT",             # symbol
  "k": {
    "t": 1787498040000,       # startTime    / 캔들 시작 시간
    "T": 1787498099999,       # closeTime    / 캔들 종료 시간
    "s": "BTCUSDT",           # symbol
    "i": "1m",                # interval
    "f": 6605832100,          # firstTradeId
    "L": 6605832187,          # lastTradeId
    "o": "77188.05000000",    # open
    "c": "77300.91000000",    # close        / 진행 중이면 현재가
    "h": "77350.00000000",    # high
    "l": "77100.00000000",    # low
    "v": "12.34500000",       # baseVolume
    "n": 87,                  # tradeCount
    "x": false,               # isClosed     / 캔들 확정 여부 (중요)
    "q": "953021.12000000",   # quoteVolume
    "V": "6.10000000",        # takerBuyBaseVolume
    "Q": "471234.50000000",   # takerBuyQuoteVolume
    "B": "0"                  # ignore
  }
}
```

Spot과 필드 구성이 완전히 동일하다. `x`(isClosed) 처리 방식과 `V`/`v` 비율 해석도 그대로 적용된다.
자세한 설명은 [spotMarketStreamREADME.md](spotMarketStreamREADME.md#3-kline_ltintervalgt--ohlcv-캔들) 참고.

## 4. bookTicker — 최우선 매수/매도

```json
{
  "e": "bookTicker",          # eventType    / Spot에는 없는 필드
  "u": 400900217,             # updateId     / 오더북 업데이트 ID
  "E": 1787498072701,         # eventTime    / Spot에는 없는 필드
  "T": 1787498072700,         # transactionTime / Spot에는 없는 필드
  "s": "BTCUSDT",             # symbol
  "ps": "BTCUSDT",            # pairSymbol   / 페어 심볼 (Delivery에서 실제 계약과 구분용)
  "b": "77300.90000000",      # bestBidPrice
  "B": "3.21000000",          # bestBidQty
  "a": "77300.91000000",      # bestAskPrice
  "A": "0.66000000",          # bestAskQty
  "st": 1                     # symbolType   / 심볼 유형 (값 의미는 공식 문서에 명시되지 않음)
}
```

Spot의 `bookTicker`에는 `e`/`E`/`T`/`ps`/`st`가 없다. 특히 `e`가 없다는 것이 Spot 문서의
핵심 포인트였는데, 선물은 반대로 **이벤트 타입이 명시된다.** `ps`/`st`는 TradFi Perpetual·
Delivery처럼 한 계좌에 여러 심볼 체계가 섞이는 USD-M 특성상 추가된 것으로 보이는 최신 필드다
(값 스키마는 비공식적으로 관찰된 것이며, 공식 문서에 enum 정의가 없어 그대로 로깅만 하고
분기 로직에 의존하지 않는 것을 권장한다).

## 5. depth — 오더북 diff

```json
{
  "e": "depthUpdate",         # eventType
  "E": 1787498072701,         # eventTime
  "T": 1787498072700,         # transactionTime / Spot에는 없는 필드
  "s": "BTCUSDT",             # symbol
  "ps": "BTCUSDT",            # pairSymbol      / Spot에는 없는 필드
  "st": 1,                    # symbolType      / Spot에는 없는 필드
  "U": 157,                   # firstUpdateId   / 이 이벤트에 포함된 첫 update ID
  "u": 160,                   # finalUpdateId   / 이 이벤트에 포함된 마지막 update ID
  "pu": 149,                  # prevFinalUpdateId / 직전 이벤트의 u값 (연속성 검증용, Spot에는 없음)
  "b": [["77300.10", "0.5"]], # bids  / 변경된 매수 호가 [가격, 수량]
  "a": [["77300.91", "0.0"]]  # asks  / 변경된 매도 호가 (수량 0이면 해당 호가 삭제)
}
```

로컬 오더북을 유지할 때는 매 이벤트의 `pu`가 직전 이벤트의 `u`와 같은지 검증한다. 어긋나면
스냅샷부터 다시 동기화해야 한다(Spot은 `pu`가 없어 `U`가 이전 이벤트의 `u+1`인지로 검증한다).
스냅샷 동기화 절차는 [binanceAPIREADME.md](../binance-api/binanceAPIREADME.md)의 "로컬 오더북" 절 참고.

## 6. markPrice — 마크 가격과 펀딩비

무기한 선물에만 있는, 청산 판정에 쓰이는 가격이다.

```json
{
  "e": "markPriceUpdate",   # eventType
  "E": 1787498072701,       # eventTime
  "s": "BTCUSDT",           # symbol
  "p": "77250.00000000",    # markPrice     / 마크 가격 (청산 기준가)
  "i": "77248.10000000",    # indexPrice    / 인덱스 가격
  "P": "77260.00000000",    # estimatedSettlePrice
  "r": "0.00010000",        # fundingRate   / 현재 펀딩비율
  "ap": "77250.00000000",   # markPrice의 이동평균 (mark price moving average)
  "T": 1787500800000,       # nextFundingTime / 다음 펀딩 시각
  "st": 1                   # symbolType (CM 마이그레이션 이후) — 1 = UM, 2 = CM
}
```

펀딩비가 양수면 롱이 숏에게 지불한다 = 롱 포지션이 과열됐다는 신호다.

## 7. forceOrder — 강제 청산

```json
{
  "e": "forceOrder",
  "E": 1787498072701,
  "o": {
    "s": "BTCUSDT",         # symbol
    "S": "SELL",            # side          / 청산 방향 (롱 청산이면 SELL)
    "o": "LIMIT",           # orderType
    "f": "IOC",             # timeInForce
    "q": "0.014",           # quantity      / 청산 수량
    "p": "9910.00",         # price
    "ap": "9910.00",        # avgPrice      / 평균 체결가
    "X": "FILLED",          # orderStatus
    "l": "0.014",           # lastFilledQty
    "z": "0.014",           # cumFilledQty
    "T": 1787498072600      # tradeTime
  }
}
```

청산 이벤트 급증은 변동성 캐스케이드의 선행 지표로 쓸 수 있다. 개별 심볼 스트림은 이벤트 자체가
드물어, 전체 심볼을 묶은 `!forceOrder@arr`로 확인하는 것이 PoC에 더 안정적이다.

## 8. TradFi Perpetual — 주식 무기한

```text
심볼: NVDAUSDT, MRVLUSDT ...
```

Nasdaq에 상장된 개별 주식을 기초자산으로 삼는 `TRADFI_PERPETUAL` 파생상품이다. **실제 주식
현물이 아니다.** 스트림 이름·페이로드 구조는 Crypto Perpetual과 완전히 동일해서 (1~5번 섹션의
`trade`/`kline`/`bookTicker`/`depth` 그대로) 위 필드 정의를 그대로 적용하면 된다. 다른 점은:

- 기초자산이 암호화폐가 아니라 주식이라 미국 증시 휴장 시간대에는 가격 변동성과 거래량이
  낮아진다(펀딩비도 이 시간대엔 잠잠해지는 경향).
  `bookTicker`의 `st`(symbolType) 값이 Crypto Perpetual과 다르게 관찰되므로, 여러 심볼을
  섞어 처리하는 파이프라인이라면 심볼명으로 분기하는 편이 `st` 값에 의존하는 것보다 안전하다.
- REST `GET /fapi/v1/exchangeInfo` 응답에서 `contractType == "TRADFI_PERPETUAL"`로 구분된다.

## 9. Delivery — 만기 선물

```text
심볼: BTCUSDT_260626 (Current Quarter), BTCUSDT_260925 (Next Quarter) ...
```

만기가 있는 선물이라 심볼 자체가 분기마다 바뀐다. 하드코딩 대신 REST로 현재 유효한 심볼을
조회해서 사용한다.

```text
1. GET /fapi/v1/exchangeInfo               -> symbols 배열
2. pair == "BTCUSDT" && contractType in
   ("CURRENT_QUARTER", "NEXT_QUARTER") 필터
3. status로 거래 가능 여부 확인 후 symbol 사용
```

스트림 페이로드 구조는 Crypto Perpetual과 동일하다(1~6번 섹션 참고). 다만 거래량이 적어
`trade`/`aggTrade`/`kline`이 한동안 오지 않을 수 있으므로, 체결 여부와 무관하게 갱신되는
`bookTicker`나 `markPrice`를 헬스체크/PoC용으로 쓰는 편이 더 안정적이다.

---

## 운영 시 주의사항

커넥션 수명, ping/pong, 재연결, 보안 체크리스트는 시장 공통이므로
[binanceWebSocketREADME.md](../binance-api/binanceWebSocketREADME.md#운영-시-주의사항)를 따른다.

---

## 참고 문서

- [USDⓈ-M Futures WebSocket Market Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams)
- [USDⓈ-M Futures User Data Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams)
- [Derivatives Change Log](https://developers.binance.com/docs/derivatives/change-log)
- [Binance Developer Documentation](https://developers.binance.com/en/docs/introduction)
