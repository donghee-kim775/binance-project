# COIN-M Futures Market Stream — `dstream.binance.com`

코인 자체로 증거금·정산을 하는 선물 시장의 공개 WebSocket 스트림 정리다. API 키는 필요 없다.
2026-08-26 기준 공식 문서를 참고했다.

- 전체 개요: [binanceWebSocketREADME.md](../binance-api/binanceWebSocketREADME.md)
- 계정 이벤트: [userDataStreamREADME.md](userDataStreamREADME.md)
- 관련 시장: [usdmFuturesMarketStreamREADME.md](usdmFuturesMarketStreamREADME.md) · [optionsMarketStreamREADME.md](optionsMarketStreamREADME.md)
- 상위 API 가이드: [binanceAPIREADME.md](../binance-api/binanceAPIREADME.md)

---

## 도메인

| 항목 | 값 |
| --- | --- |
| WebSocket | `wss://dstream.binance.com` |
| REST Base | `https://dapi.binance.com` |

두 가지 하위 카테고리가 있다.

```text
dstream.binance.com
 ├─ Crypto Perpetual   예: BTCUSD_PERP, ETHUSD_PERP
 └─ Delivery Futures
     ├─ BTCUSD_260626  Current Quarter
     └─ BTCUSD_260925  Next Quarter
```

> 표기 주의: `PREP`가 아니라 `PERP`(perpetual)다.

**수량 단위가 코인이 아니라 계약 수(contract)다.** `BTCUSD_PERP` 1계약 = 100 USD. 마진과
손익이 코인으로 정산된다. Delivery(만기 선물)의 심볼은 만기마다 바뀌므로 하드코딩하지 않고
`GET /dapi/v1/exchangeInfo`에서 매번 조회한다. COIN-M의 상태 필드명은 USD-M과 달리
`contractStatus`다.

## 스트림 목록

USD-M과 스트림 이름·구조는 대부분 동일하다. 차이는 심볼 형식(`BTCUSD_PERP`)과 수량 단위(계약 수)다.

| 스트림 | 용도 | USD-M과의 차이 |
| --- | --- | --- |
| `<symbol>@trade` | 개별 체결 (수량은 계약 수) | 동일 구조 |
| `<symbol>@aggTrade` | 집계 체결 (수량은 계약 수) | `st` 필드 추가 |
| `<symbol>@kline_<interval>` | OHLCV | 동일 구조 |
| `<symbol>@markPrice` | 마크 가격 + 펀딩비 | 동일 구조 |
| `<symbol>@bookTicker` | 최우선 호가 | `T`/`ps`/`st` 필드 추가 |
| `<symbol>@forceOrder` | 강제 청산 | 동일 구조 |
| `!forceOrder@arr` | 전 심볼 강제 청산 | 동일 |
| `<symbol>@depth@100ms` | 오더북 diff | `pu`/`ps`/`st` 필드 추가 |

스트림 이름의 심볼은 소문자를 쓴다(예: `btcusd_perp@trade`).

## 1. trade — 개별 체결 데이터

```json
{
  "e": "trade",              # eventType
  "E": 1787498072701,        # eventTime
  "T": 1787498072700,        # tradeTime
  "s": "BTCUSD_PERP",        # symbol
  "t": 606073,               # tradeId
  "p": "77300.9",            # price
  "q": "2",                  # quantity      / 계약 수(코인 아님). BTCUSD_PERP 1계약 = 100 USD
  "X": "MARKET",             # execType      / 청산 체결이면 "INSURANCE_FUND"
  "m": true                  # isBuyerMaker
}
```

USD-M `trade`와 필드는 동일하다. **`q`(수량)이 코인이 아니라 계약 수라는 점만 다르다.**
포지션 규모나 거래대금을 계산하려면 `계약 수 × 계약당 명목가치(예: 100 USD) / 가격`으로 환산해야
코인 수량이 나온다.

## 2. aggTrade — 집계 체결 데이터

```json
{
  "e": "aggTrade",          # eventType
  "E": 1591261134288,       # eventTime
  "a": 424951,               # aggTradeId
  "s": "BTCUSD_PERP",        # symbol
  "p": "9643.5",             # price
  "q": "2",                  # quantity     / 계약 수
  "f": 606073,                # firstTradeId
  "l": 606073,                # lastTradeId
  "T": 1591261134199,        # tradeTime
  "m": false                 # isBuyerMaker
}
```

`M`(ignore) 필드가 없는 것은 USD-M과 같다. 수량 단위가 계약 수라는 점에 유의한다.

## 3. kline_&lt;interval&gt; — OHLCV 캔들

```json
{
  "e": "kline",
  "E": 1591261542539,
  "s": "BTCUSD_PERP",
  "k": {
    "t": 1591261500000,      # startTime
    "T": 1591261559999,      # closeTime
    "s": "BTCUSD_PERP",      # symbol
    "i": "1m",               # interval
    "f": 606400,             # firstTradeId
    "L": 606430,             # lastTradeId
    "o": "9638.9",           # open
    "c": "9639.8",           # close
    "h": "9639.8",           # high
    "l": "9638.6",           # low
    "v": "156",              # baseVolume   / 계약 수 기준
    "n": 31,                 # tradeCount
    "x": false,              # isClosed
    "q": "1.61836886",       # quoteVolume  / 코인(BTC) 기준
    "V": "73",               # takerBuyBaseVolume
    "Q": "0.75731156",       # takerBuyQuoteVolume
    "B": "0"                 # ignore
  }
}
```

Spot/USD-M과 필드는 동일하지만, `v`(baseVolume)는 계약 수, `q`(quoteVolume)는 코인 수량이라는
점이 반대로 뒤집혀 있다(USD-M/Spot은 `v`가 base 코인, `q`가 quote 화폐). COIN-M은 정산 화폐가
코인이라 base/quote 개념 자체가 반대이기 때문이다.

## 4. bookTicker — 최우선 매수/매도

```json
{
  "e": "bookTicker",
  "u": 17242169,             # updateId
  "s": "BTCUSD_PERP",        # symbol
  "ps": "BTCUSD",            # pairSymbol   / 페어 심볼(만기 구분 없는 기초 페어)
  "b": "9548.1",             # bestBidPrice
  "B": "52",                 # bestBidQty   / 계약 수
  "a": "9548.5",             # bestAskPrice
  "A": "11",                 # bestAskQty
  "T": 1591268628155,        # transactionTime
  "E": 1591268628166,        # eventTime
  "st": 2                    # symbolType   / 값 의미는 공식 문서에 명시되지 않음(관찰상 Perpetual과 Delivery가 다른 값을 가짐)
}
```

USD-M과 마찬가지로 `e` 필드가 있다(Spot과 가장 큰 차이). `ps`/`st`는 COIN-M도 Crypto Perpetual과
Delivery가 섞여 있어 추가된 것으로 보이는 필드다.

## 5. depth — 오더북 diff

```json
{
  "e": "depthUpdate",
  "E": 1591270260907,        # eventTime
  "T": 1591270260891,        # transactionTime
  "s": "BTCUSD_PERP",        # symbol
  "ps": "BTCUSD",            # pairSymbol
  "U": 17285681,             # firstUpdateId
  "u": 17285702,             # finalUpdateId
  "pu": 17285675,            # prevFinalUpdateId / 직전 이벤트의 u값
  "st": 1,                   # symbolType
  "b": [["9548.0", "12"]],   # bids (계약 수)
  "a": [["9548.5", "5"]]     # asks (계약 수)
}
```

로컬 오더북 유지 시 `pu`로 이벤트 연속성을 검증하는 방식은 USD-M과 같다.

## 6. markPrice — 마크 가격과 펀딩비

USD-M과 동일한 구조다. `r`이 현재 펀딩비율이다.

```json
{
  "e": "markPriceUpdate",   # eventType
  "E": 1591261175000,       # eventTime
  "s": "BTCUSD_PERP",       # symbol
  "p": "9650.00000000",     # markPrice
  "i": "9650.00000000",     # indexPrice
  "P": "9630.00000000",     # estimatedSettlePrice
  "r": "0.00038167",        # fundingRate
  "T": 1591271200000        # nextFundingTime
}
```

펀딩비 해석은 USD-M과 동일하다. 양수면 롱이 숏에게 지불한다.

## 7. forceOrder — 강제 청산

페이로드 구조는 USD-M과 동일하다(`o.s`/`o.S`/`o.q`/`o.ap` 등). 차이는 `o.q`(청산 수량)가 계약
수라는 점뿐이다. 자세한 필드는
[usdmFuturesMarketStreamREADME.md](usdmFuturesMarketStreamREADME.md#7-forceorder--강제-청산) 참고.

## 8. Delivery — 만기 선물

```text
1. GET /dapi/v1/exchangeInfo               -> symbols 배열
2. pair == "BTCUSD" && contractType in
   ("CURRENT_QUARTER", "NEXT_QUARTER") 필터
3. contractStatus로 거래 가능 여부 확인       ← USD-M은 status, COIN-M은 contractStatus
```

만기 선물은 거래량이 적어 `trade`/`kline`이 한동안 오지 않을 수 있다. `markPrice`는 체결 여부와
무관하게 갱신되므로 PoC나 헬스체크로 더 안정적이다.

---

## 운영 시 주의사항

커넥션 수명, ping/pong, 재연결, 보안 체크리스트는 시장 공통이므로
[binanceWebSocketREADME.md](../binance-api/binanceWebSocketREADME.md#운영-시-주의사항)를 따른다.

---

## 참고 문서

- [COIN-M Futures WebSocket Market Streams](https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams)
- [COIN-M Futures User Data Streams](https://developers.binance.com/docs/derivatives/coin-margined-futures/user-data-streams)
- [Derivatives Change Log](https://developers.binance.com/docs/derivatives/change-log)
- [Binance Developer Documentation](https://developers.binance.com/en/docs/introduction)
