# Spot Market Stream — `stream.binance.com`

실제 코인을 사고파는 현물 시장의 공개 WebSocket 스트림 정리다. API 키는 필요 없다.
2026-08-26 기준 공식 문서를 참고했다.

- 전체 개요: [binanceWebSocketREADME.md](binanceWebSocketREADME.md)
- 계정 이벤트: [userDataStreamREADME.md](userDataStreamREADME.md)
- 상위 API 가이드: [binanceAPIREADME.md](binanceAPIREADME.md)

---

## 도메인

| 항목 | 값 |
| --- | --- |
| WebSocket | `wss://stream.binance.com:9443` |
| REST Base | `https://api.binance.com` |

```text
stream.binance.com
 ├─ BTCUSDT   현물
 └─ ETHUSDT   현물
```

## 연결 방식

```text
단일:  wss://stream.binance.com:9443/ws/btcusdt@trade
결합:  wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@kline_1m/btcusdt@bookTicker
```

결합(combined) 스트림은 메시지가 한 겹 감싸져서 온다.

```json
{
  "stream": "btcusdt@trade",
  "data": { "e": "trade", "E": 1787497835323, "s": "BTCUSDT" }
}
```

스트림 이름의 심볼은 **소문자**를 쓴다.

---

## 1. trade — 개별 체결 데이터

```json
{
  "e": "trade",             # eventType    / 이벤트 종류
  "E": 1787497835323,       # eventTime    / 이벤트 생성 시간
  "s": "BTCUSDT",           # symbol       / 거래쌍
  "t": 6605823797,          # tradeId      / 체결 고유 번호
  "p": "77300.91000000",    # price        / 체결 가격
  "q": "0.00300000",        # quantity     / 체결 수량
  "T": 1787497835323,       # tradeTime    / 실제 체결 시간
  "m": true,                # isBuyerMaker / true면 매수자가 maker = taker가 매도(하락 압력)
  "M": true                 # ignore
}
```

`m` 필드가 매수/매도 방향 판정의 핵심이다. `m == true`면 **taker가 판 것**(공격적 매도)이다.

## 2. aggTrade — 집계 체결 데이터

하나의 taker 주문이 여러 maker 주문과 체결됐을 때, 같은 가격대 체결을 하나로 묶은 것이다.

```json
{
  "e": "aggTrade",          # eventType
  "E": 1787498072701,       # eventTime
  "s": "BTCUSDT",           # symbol
  "a": 4041839950,          # aggTradeId   / 집계 체결 번호
  "p": "77188.05000000",    # price        / 집계 체결 가격
  "q": "0.02300000",        # quantity     / 집계 체결 수량 (합산됨)
  "f": 6605832187,          # firstTradeId / 포함된 첫 tradeId
  "l": 6605832187,          # lastTradeId  / 포함된 마지막 tradeId
  "T": 1787498072700,       # tradeTime
  "m": true,                # isBuyerMaker
  "M": true                 # ignore
}
```

> `trade`와 `aggTrade`를 동시에 저장하면 **같은 체결을 이중 집계**하게 된다. 하나만 원장으로 삼는다.
> 거래량 분석 목적이면 메시지 수가 적은 `aggTrade`가, 마이크로구조 분석이면 `trade`가 유리하다.

## 3. kline_&lt;interval&gt; — OHLCV 캔들

`btcusdt@kline_1m` 형식. interval: `1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M`

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
    "v": "12.34500000",       # baseVolume   / BTC 기준 거래량
    "n": 87,                  # tradeCount   / 체결 건수
    "x": false,               # isClosed     / 캔들 확정 여부 (중요)
    "q": "953021.12000000",   # quoteVolume  / USDT 기준 거래대금
    "V": "6.10000000",        # takerBuyBaseVolume
    "Q": "471234.50000000",   # takerBuyQuoteVolume
    "B": "0"                  # ignore
  }
}
```

`x`(isClosed)가 **가장 중요한 필드**다. 진행 중 캔들은 1~2초마다 계속 갱신되어 날아오므로,
확정 캔들만 저장하려면 `x == true`인 것만 적재하고, 실시간 차트용이면 마지막 캔들을 덮어쓴다.
`V`/`v` 비율로 taker 매수 비중(공격적 매수세)을 계산할 수 있다.

## 4. bookTicker — 최우선 매수/매도

`btcusdt@bookTicker`. 최우선 호가가 바뀔 때마다 즉시 발행된다. **`e` 필드가 없다.**

```json
{
  "u": 400900217,             # updateId     / 오더북 업데이트 ID
  "s": "BTCUSDT",             # symbol
  "b": "77300.90000000",      # bestBidPrice / 최우선 매수 호가
  "B": "3.21000000",          # bestBidQty   / 최우선 매수 잔량
  "a": "77300.91000000",      # bestAskPrice / 최우선 매도 호가
  "A": "0.66000000"           # bestAskQty   / 최우선 매도 잔량
}
```

스프레드 = `a - b`, 미드프라이스 = `(a + b) / 2`.

## 5. 그 외 Spot 스트림

| 스트림 | 용도 | 갱신 주기 |
| --- | --- | --- |
| `<symbol>@depth` | 오더북 diff (증분) | 1000ms |
| `<symbol>@depth@100ms` | 오더북 diff 고속 | 100ms |
| `<symbol>@depth20@100ms` | 상위 20호가 스냅샷 | 100ms |
| `<symbol>@ticker` | 24시간 rolling 통계 | 1000ms |
| `<symbol>@miniTicker` | 축약 24시간 통계 | 1000ms |
| `!miniTicker@arr` | 전 심볼 miniTicker | 1000ms |
| `!ticker@arr` | 전 심볼 ticker | 1000ms |

`@depth`(diff)만으로는 오더북을 만들 수 없다. REST 스냅샷과 동기화가 필요하다.
자세한 절차는 [binanceAPIREADME.md](binanceAPIREADME.md)의 "로컬 오더북" 절 참고.

---

## 최소 구현 예제

```python
import asyncio, json, websockets

URL = ("wss://stream.binance.com:9443/stream?streams="
       "btcusdt@trade/btcusdt@kline_1m/btcusdt@bookTicker")

async def main():
    async for ws in websockets.connect(URL, ping_interval=20, ping_timeout=60):
        try:
            async for raw in ws:
                msg = json.loads(raw)
                stream, data = msg["stream"], msg["data"]
                print(stream, data.get("e"), data.get("p") or data.get("b"))
        except websockets.ConnectionClosed:
            continue  # 자동 재연결
```

운영 시 커넥션 수명, ping/pong, 재연결, 보안 체크리스트는
[binanceWebSocketREADME.md](binanceWebSocketREADME.md#운영-시-주의사항) 참고.

---

## 참고 문서

- [Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [Binance Developer Documentation](https://developers.binance.com/en/docs/introduction)
