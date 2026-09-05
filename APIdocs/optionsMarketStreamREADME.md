# Options Market Stream — `nbstream.binance.com/eoptions`

BTC, ETH 등 가상자산을 기초자산으로 하는 유러피언 옵션의 공개 WebSocket 스트림 정리다.
API 키는 필요 없다. 2026-08-26 기준 공식 문서를 참고했다.

- 전체 개요: [binanceWebSocketREADME.md](binanceWebSocketREADME.md)
- 계정 이벤트: [userDataStreamREADME.md](userDataStreamREADME.md)
- 관련 시장: [usdmFuturesMarketStreamREADME.md](usdmFuturesMarketStreamREADME.md) · [coinmFuturesMarketStreamREADME.md](coinmFuturesMarketStreamREADME.md)
- 상위 API 가이드: [binanceAPIREADME.md](binanceAPIREADME.md)

---

## 도메인

| 항목 | 값 |
| --- | --- |
| WebSocket | `wss://nbstream.binance.com/eoptions` |
| REST Base | `https://eapi.binance.com` |

```text
nbstream.binance.com/eoptions
 ├─ C - Call Option : BTC-260925-100000-C
 └─ P - Put  Option : BTC-260925-100000-P
```

심볼 형식은 `기초자산-만기(YYMMDD)-행사가-타입`이다.

## 스트림 목록

Spot/Futures와 스트림 이름 체계가 다르다.

| 스트림 | 용도 |
| --- | --- |
| `<symbol>@ticker` | 옵션 시세 (그릭스 + IV 포함) |
| `<symbol>@trade` | 개별 체결 |
| `<underlying>@index` | 기초자산 지수 가격 |
| `<underlyingAsset>@openInterest@<expiration>` | 미결제약정 |

> **참고:** 이 문서의 payload는 공식 문서 기준이며, 아래 [PoC 노트북](../APIpoc/options_market_stream.ipynb)에서
> 실제 실행 시 이 환경의 WebSocket 연결(`nbstream.binance.com`)이 handshake 단계에서 404를 반환해
> 직접 캡처하지는 못했다(REST는 정상 동작 확인함). 필드가 실제와 다르면 공식 문서를 우선한다.

## 1. ticker — 그릭스와 IV

```json
{
  "e": "24hrTicker",        # eventType
  "E": 1787498072701,       # eventTime
  "s": "BTC-260925-100000-C", # symbol
  "o": "1500.00000000",     # openPrice    / 24시간 전 가격
  "h": "1650.00000000",     # highPrice
  "l": "1400.00000000",     # lowPrice
  "c": "1600.00000000",     # lastPrice
  "V": "120.50",            # volume       / 24시간 거래량 (계약 수)
  "p": "100.00000000",      # priceChange
  "P": "6.67",              # priceChangePercent
  "b": "1590.00000000",     # bestBuyPrice
  "a": "1610.00000000",     # bestSellPrice
  "d": "0.5500",            # delta
  "t": "-12.30",            # theta
  "g": "0.0012",            # gamma
  "v": "45.60",             # vega
  "vo": "0.6500",           # impliedVolatility / 내재변동성
  "mp": "1580.00000000"     # markPrice    / 마크 가격
}
```

그릭스(`d`/`t`/`g`/`v`)와 IV(`vo`), 마크 가격(`mp`)이 다른 상품에는 없는 옵션 전용 필드다.
행사가와 만기가 다른 심볼별로 별도 스트림을 구독해야 한다. 매수/매도 각각의 IV, 미결제약정 등
추가 필드가 공식 문서에 더 있을 수 있으니 정확한 전체 필드 목록은 [참고 문서](#참고-문서)의
Ticker 페이지를 확인한다.

## 2. trade — 개별 체결

```json
{
  "e": "trade",                 # eventType
  "E": 1787498072701,           # eventTime
  "T": 1787498072700,           # tradeTime
  "s": "BTC-260925-100000-C",   # symbol
  "t": 123456,                  # tradeId
  "p": "1600.00000000",         # price
  "q": "0.10",                  # quantity   / 계약 수
  "S": "BUY",                   # direction  / 매수/매도 방향
  "X": "MARKET",                # execType   / 일반 체결이면 MARKET, 블록딜이면 BLOCK
  "m": true                     # isBuyerMaker
}
```

`X`가 `"BLOCK"`이면 거래소 밖에서 협상된 대량 블록딜이 그대로 체결된 것이다. 일반 오더북 체결과
분리해서 집계하는 편이 좋다.

## 3. index — 기초자산 지수 가격

```json
{
  "e": "index",              # eventType
  "E": 1787498072701,        # eventTime
  "s": "BTCUSDT",            # symbol  / 기초자산(underlying), 옵션 심볼이 아님
  "p": "77250.00000000"      # indexPrice
}
```

특정 옵션 심볼의 유동성과 무관하게 갱신되며, 옵션 행사가 대비 내가격/외가격 판단에 쓴다.

## 4. openInterest — 미결제약정

```json
{
  "e": "openInterest",          # eventType
  "E": 1787498072701,           # eventTime
  "s": "BTC-260925-100000-C",   # symbol
  "o": "1234.50",               # openInterest / 미결제약정 (계약 수)
  "h": "1975200.00"             # openInterestUSDT / 명목가치 (USDT 환산)
}
```

스트림은 옵션 심볼이 아니라 `<underlyingAsset>@openInterest@<expiration>` 형태로 만기(예: `221125`)
단위로 구독한다. 특정 만기의 시장 전체 포지션 규모를 볼 때 쓴다.

## 심볼 조회 흐름

옵션 심볼은 만기마다 새로 생성되므로 하드코딩하지 않는다.

```text
1. GET /eapi/v1/exchangeInfo         -> optionSymbols 배열
2. underlying == "BTCUSDT" && status == "TRADING" 필터
3. expiryDate(ms epoch) 기준 정렬 -> 가장 가까운 만기 선택
```

---

## 운영 시 주의사항

커넥션 수명, ping/pong, 재연결, 보안 체크리스트는 시장 공통이므로
[binanceWebSocketREADME.md](binanceWebSocketREADME.md#운영-시-주의사항)를 따른다.

---

## 참고 문서

- [Options WebSocket Market Streams](https://developers.binance.com/docs/derivatives/options-trading/websocket-market-streams)
- [Options Trade Streams](https://developers.binance.com/docs/derivatives/options-trading/websocket-market-streams/Trade-Streams)
- [Options Open Interest Stream](https://developers.binance.com/docs/derivatives/options-trading/websocket-market-streams/Open-Interest)
- [Binance Developer Documentation](https://developers.binance.com/en/docs/introduction)
