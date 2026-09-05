# User Data Stream — 계정 이벤트 (잔고 · 주문 · 포지션)

**내 계정의 잔고·주문·체결·포지션 변화를 실시간으로 받는 스트림.**
REST polling(`/api/v3/openOrders` 반복 호출)을 대체하며, 지연과 rate limit 소모가 훨씬 적다.
2026-08-26 기준 공식 문서를 참고했다.

- 전체 개요: [binanceWebSocketREADME.md](binanceWebSocketREADME.md)
- Market Stream: [spotMarketStreamREADME.md](spotMarketStreamREADME.md) · [usdmFuturesMarketStreamREADME.md](usdmFuturesMarketStreamREADME.md) · [coinmFuturesMarketStreamREADME.md](coinmFuturesMarketStreamREADME.md) · [optionsMarketStreamREADME.md](optionsMarketStreamREADME.md)
- 상위 API 가이드: [binanceAPIREADME.md](binanceAPIREADME.md)

핵심 특징:

- **API 키가 필요하다.** 권한은 조회 + User Data만으로 충분하다(주문 권한 불필요).
- **심볼을 지정하지 않는다.** 계정에서 일어나는 모든 이벤트가 온다.
- **계정당 커넥션 1개**면 된다.
- Spot / USD-M / COIN-M / Options의 **User Data Stream은 각각 별개**다. 모두 받으려면 4개 커넥션.

---

## 1. Spot: 연결 방식 두 가지

Spot은 최근 방식이 바뀌었다. 신규 구현은 **WebSocket API 방식**을 쓴다.

### 방식 A: WebSocket API 세션 구독 (현재 방식, 권장)

`wss://ws-api.binance.com:443/ws-api/v3` 에 연결한 뒤, 세션 로그인 후 구독한다.
listenKey 생성/갱신/만료 처리가 전부 사라진다.

**A-1. Ed25519 키로 세션 로그인 후 구독**

```json
{ "id": "logon-1", "method": "session.logon",
  "params": { "apiKey": "...", "timestamp": 1787498072701, "signature": "<ed25519 서명>" } }
```

```json
{ "id": "sub-1", "method": "userDataStream.subscribe" }
```

`session.logon`은 **Ed25519 키에서만 동작한다.** 한 번 로그인하면 이후 요청에 서명이 필요 없다.
단, `timestamp`는 SIGNED 요청이면 로그인 후에도 여전히 넣어야 한다 — 서버가 재전송 공격을 막기
위해 요청마다 확인한다.

**A-2. HMAC 키로 단발 구독 (세션 로그인 없이)**

```json
{ "id": "sub-1", "method": "userDataStream.subscribe.signature",
  "params": { "apiKey": "...", "timestamp": 1787498072701, "signature": "<HMAC-SHA256 서명>" } }
```

이 방식은 **한 번 구독하면 계속 밀어주는(push) 방식**이다. 반면 `account.status`,
`order.status`는 **물어볼 때마다 그 순간의 스냅샷 한 번**만 주는 요청-응답 방식이다.

- `account.status`: 추가 파라미터 없이 자산별 잔고, 계정 권한, rate limit 정보를 받는다.
- `order.status`: `symbol`과 `orderId`(또는 `origClientOrderId`)가 필요하다. 존재하지 않는
  주문 ID면 `status`가 200이 아니라 에러(`code`/`msg`)로 응답이 온다.

**구독 해제**

```json
{ "id": "unsub-1", "method": "userDataStream.unsubscribe", "params": { "subscriptionId": 1 } }
```

`subscriptionId`를 생략하면 전체 구독이 해제된다.

이 방식에서 이벤트는 `event`로 한 겹 감싸져 온다.

```json
{ "event": { "e": "outboundAccountPosition", "E": 1787498072701, "...": "..." } }
```

### 방식 B: listenKey — Spot은 이미 폐지됨

> **2026-08-26 직접 확인:** `POST/PUT/DELETE https://api.binance.com/api/v3/userDataStream`은
> 이제 **API 키 검증 이전에 CloudFront/nginx 단에서 `410 Gone`을 반환**한다. 단순히 "레거시"가
> 아니라 **완전히 내려간 상태**다. Spot에서 listenKey 코드를 새로 짜면 안 되고, 위 방식 A
> (`session.logon` / `userDataStream.subscribe.signature`)만 사용한다.
>
> **Futures(USD-M/COIN-M)와 Options는 listenKey 방식만 지원**하며, 같은 날짜 기준으로
> `POST /fapi/v1/listenKey`, `POST /dapi/v1/listenKey`, `POST /eapi/v1/listenKey` 모두
> 정상 응답(키 없이 호출 시 401)함을 확인했다.

```bash
# 1. 발급 (X-MBX-APIKEY 헤더 필요, 서명 불필요) — Futures/Options 전용, Spot에는 더 이상 없음
curl -X POST 'https://fapi.binance.com/fapi/v1/listenKey' -H 'X-MBX-APIKEY: ...'
# -> {"listenKey":"pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"}

# 2. 연결 (Futures 예시; COIN-M은 dstream, Options는 nbstream/eoptions)
wss://fstream.binance.com/ws/<listenKey>

# 3. 60분마다 keepalive (안 하면 60분 후 끊김)
curl -X PUT 'https://fapi.binance.com/fapi/v1/listenKey' -H 'X-MBX-APIKEY: ...'

# 4. 종료
curl -X DELETE 'https://fapi.binance.com/fapi/v1/listenKey' -H 'X-MBX-APIKEY: ...'
```

listenKey는 **URL에 그대로 들어가므로 로그에 남기면 안 된다.** 사실상 계정 읽기 자격증명이다.

## 2. Spot User Data 이벤트

| 이벤트 | 언제 오는가 |
| --- | --- |
| `executionReport` | 주문 생성·체결·취소·거절·만료 — **가장 중요** |
| `outboundAccountPosition` | 잔고가 변한 자산 목록 |
| `balanceUpdate` | 입출금, 계정 간 이체로 인한 잔고 변화 |
| `listStatus` | OCO 등 order list 상태 변화 |
| `externalLockUpdate` | 마진 담보 등 외부 시스템의 잔고 잠금/해제 |
| `eventStreamTerminated` | 스트림 종료 통보 |

### executionReport — 주문 상태 변화

```json
{
  "e": "executionReport",       # eventType
  "E": 1787498072701,           # eventTime
  "s": "BTCUSDT",               # symbol
  "c": "myOrder-0001",          # clientOrderId    / 내가 지정한 주문 ID
  "S": "BUY",                   # side             / BUY | SELL
  "o": "LIMIT",                 # orderType        / LIMIT | MARKET | STOP_LOSS_LIMIT ...
  "f": "GTC",                   # timeInForce      / GTC | IOC | FOK
  "q": "0.01000000",            # quantity         / 주문 수량
  "p": "77000.00000000",        # price            / 주문 가격
  "P": "0.00000000",            # stopPrice
  "F": "0.00000000",            # icebergQty
  "g": -1,                      # orderListId      / OCO가 아니면 -1
  "C": "",                      # origClientOrderId / 취소 시 원 주문 ID
  "x": "TRADE",                 # executionType    / NEW CANCELED REPLACED REJECTED TRADE EXPIRED TRADE_PREVENTION
  "X": "PARTIALLY_FILLED",      # orderStatus      / NEW PARTIALLY_FILLED FILLED CANCELED REJECTED EXPIRED
  "r": "NONE",                  # rejectReason
  "i": 4293153,                 # orderId          / Binance 주문 ID
  "l": "0.00300000",            # lastExecutedQty  / 이번 체결 수량
  "z": "0.00300000",            # cumFilledQty     / 누적 체결 수량
  "L": "77000.00000000",        # lastExecutedPrice / 이번 체결 가격
  "n": "0.00000030",            # commission       / 수수료
  "N": "BNB",                   # commissionAsset  / 수수료 자산
  "T": 1787498072700,           # transactionTime
  "t": 6605832187,              # tradeId          / 체결이 아니면 -1
  "w": true,                    # isOnBook         / 오더북에 남아있는지
  "m": false,                   # isMaker          / 내가 maker였는지 (수수료 결정)
  "O": 1787498070000,           # orderCreationTime
  "Z": "231.00000000",          # cumQuoteQty      / 누적 체결 금액
  "Y": "231.00000000",          # lastQuoteQty     / 이번 체결 금액
  "Q": "0.00000000",            # quoteOrderQty
  "W": 1787498070000,           # workingTime
  "V": "EXPIRE_MAKER"           # selfTradePreventionMode
}
```

핵심 구분:

- `x`(executionType) = **이번에 무슨 일이 일어났나** (`TRADE` = 체결이 발생함)
- `X`(orderStatus) = **주문의 현재 상태** (`FILLED` = 전량 체결 완료)

`x == "TRADE"`인 이벤트만 모으면 내 체결 내역(myTrades)과 동일해진다.
평균 체결가는 `Z / z`로 계산한다.

### outboundAccountPosition — 잔고 변화

```json
{
  "e": "outboundAccountPosition",
  "E": 1787498072701,           # eventTime
  "u": 1787498072700,           # lastAccountUpdateTime
  "B": [                        # balances / 변경된 자산만
    { "a": "BTC",  "f": "0.50000000", "l": "0.00000000" },   # asset, free, locked
    { "a": "USDT", "f": "1200.0000",  "l": "770.00000000" }
  ]
}
```

`f`(free) = 주문 가능 잔고, `l`(locked) = 미체결 주문에 묶인 잔고.

### balanceUpdate — 입출금/이체

```json
{
  "e": "balanceUpdate",
  "E": 1787498072701,
  "a": "BTC",                   # asset
  "d": "0.10000000",            # balanceDelta / 증감분 (음수 가능)
  "T": 1787498072700            # clearTime
}
```

주문 체결이 아닌 **외부 요인**에 의한 변화다. 손익 계산에서 제외해야 한다.

### 그 외

```json
{ "e": "externalLockUpdate", "E": 1787498072701,
  "a": "BTC", "d": "0.10000000", "T": 1787498072700 }   # 마진 담보 등으로 잠김
```

```json
{ "e": "eventStreamTerminated", "E": 1787498072701 }    # 재연결 필요
```

## 3. USD-M Futures User Data 이벤트

```bash
# listenKey 발급 (60분 유효, 60분마다 PUT으로 갱신)
POST   https://fapi.binance.com/fapi/v1/listenKey
PUT    https://fapi.binance.com/fapi/v1/listenKey
DELETE https://fapi.binance.com/fapi/v1/listenKey

# 연결
wss://fstream.binance.com/ws/<listenKey>
```

| 이벤트 | 내용 |
| --- | --- |
| `ACCOUNT_UPDATE` | 잔고 + **포지션** 변화 |
| `ORDER_TRADE_UPDATE` | 주문 상태 변화 (Spot의 executionReport 대응) |
| `TRADE_LITE` | 체결만 담은 경량·저지연 버전 |
| `MARGIN_CALL` | 마진콜 (청산 임박 경고) |
| `ACCOUNT_CONFIG_UPDATE` | 레버리지 / 멀티에셋 모드 변경 |
| `STRATEGY_UPDATE` | 전략 주문(TWAP, VP 등) 상태 |
| `GRID_UPDATE` | 그리드 주문 상태 |
| `CONDITIONAL_ORDER_TRIGGER_REJECT` | 조건부 주문 트리거 거절 |
| `listenKeyExpired` | listenKey 만료 — **즉시 재발급 + 재연결** |

### ACCOUNT_UPDATE — 잔고와 포지션

```json
{
  "e": "ACCOUNT_UPDATE",
  "E": 1787498072701,           # eventTime
  "T": 1787498072700,           # transactionTime
  "a": {
    "m": "ORDER",               # eventReasonType / ORDER DEPOSIT WITHDRAW FUNDING_FEE ...
    "B": [                      # balances
      { "a": "USDT",            # asset
        "wb": "12000.00",       # walletBalance
        "cw": "11500.00",       # crossWalletBalance
        "bc": "0" }             # balanceChange (손익·수수료 제외한 순변동)
    ],
    "P": [                      # positions
      { "s": "BTCUSDT",         # symbol
        "pa": "0.010",          # positionAmount (음수면 숏)
        "ep": "77000.0",        # entryPrice
        "bep": "77012.3",       # breakEvenPrice
        "cr": "12.5",           # accumulatedRealizedPnl
        "up": "-3.2",           # unrealizedPnl
        "mt": "cross",          # marginType / cross | isolated
        "iw": "0",              # isolatedWallet
        "ps": "LONG" }          # positionSide / BOTH | LONG | SHORT
    ]
  }
}
```

`m`(eventReasonType)로 원인을 구분한다. `FUNDING_FEE`는 펀딩비 정산이므로 매매 손익과 분리해 집계한다.

### ORDER_TRADE_UPDATE — 주문 상태

```json
{
  "e": "ORDER_TRADE_UPDATE",
  "E": 1787498072701,
  "T": 1787498072700,
  "o": {
    "s": "BTCUSDT",             # symbol
    "c": "myOrder-0001",        # clientOrderId
    "S": "BUY",                 # side
    "o": "LIMIT",               # orderType
    "f": "GTC",                 # timeInForce
    "q": "0.010",               # originalQty
    "p": "77000",               # originalPrice
    "ap": "77000",              # averagePrice
    "sp": "0",                  # stopPrice
    "x": "TRADE",               # executionType
    "X": "FILLED",              # orderStatus
    "i": 8886774,               # orderId
    "l": "0.010",               # lastFilledQty
    "z": "0.010",               # cumFilledQty
    "L": "77000",               # lastFilledPrice
    "N": "USDT",                # commissionAsset
    "n": "0.308",               # commission
    "T": 1787498072690,         # tradeTime
    "t": 109100866,             # tradeId
    "m": false,                 # isMaker
    "R": false,                 # isReduceOnly
    "ps": "LONG",               # positionSide
    "rp": "1.20"                # realizedProfit / 이번 체결의 실현 손익
  }
}
```

Spot과 달리 **`ps`(positionSide), `R`(reduceOnly), `rp`(realizedProfit)** 이 있다.
Spot `executionReport`와 같은 DTO로 억지로 합치지 말고, 공통 envelope + product별 payload로 나눈다.

### TRADE_LITE — 저지연 체결 전용

`ORDER_TRADE_UPDATE`보다 필드를 줄여 지연을 낮춘 이벤트다. `TRADE` 실행 타입만 발행된다.

```json
{
  "e": "TRADE_LITE", "E": 1787498072701, "T": 1787498072700,
  "s": "BTCUSDT", "q": "0.010", "p": "77000", "m": false,
  "c": "myOrder-0001", "S": "BUY",
  "L": "77000",               # lastFilledPrice
  "l": "0.010",               # lastFilledQty
  "t": 109100866,             # tradeId
  "i": 8886774                # orderId
}
```

`TRADE_LITE`와 `ORDER_TRADE_UPDATE`는 **같은 체결에 대해 둘 다 온다.** 중복 집계에 주의한다.
체결 반응 속도가 중요하면 `TRADE_LITE`로 먼저 반응하고, 원장은 `ORDER_TRADE_UPDATE`로 확정한다.

### MARGIN_CALL / ACCOUNT_CONFIG_UPDATE / listenKeyExpired

```json
{ "e": "MARGIN_CALL", "E": 1787498072701,
  "cw": "3.16812045",                                 # crossWalletBalance
  "p": [ { "s": "ETHUSDT", "ps": "LONG", "pa": "1.327", "mt": "CROSSED",
           "iw": "0", "mp": "187.17", "up": "-1.166", "mm": "1.614" } ] }   # mm = 유지증거금
```

```json
{ "e": "ACCOUNT_CONFIG_UPDATE", "E": 1787498072701, "T": 1787498072700,
  "ac": { "s": "BTCUSDT", "l": 25 } }                 # 레버리지 25배로 변경
```

```json
{ "e": "listenKeyExpired", "E": 1787498072701, "listenKey": "..." }
```

`listenKeyExpired`는 **경고가 아니라 이미 끊긴 상태**다. 즉시 재발급 → 재연결 →
REST(`/fapi/v2/positionRisk`, `/fapi/v1/openOrders`)로 상태를 다시 맞춘다.

## 4. COIN-M / Options User Data

| 시장 | listenKey 엔드포인트 | WebSocket URL |
| --- | --- | --- |
| COIN-M | `POST /dapi/v1/listenKey` | `wss://dstream.binance.com/ws/<listenKey>` |
| Options | `POST /eapi/v1/listenKey` | `wss://nbstream.binance.com/eoptions/ws/<listenKey>` |

- **COIN-M**: 이벤트 종류는 USD-M과 거의 동일(`ACCOUNT_UPDATE`, `ORDER_TRADE_UPDATE`, `MARGIN_CALL`,
  `ACCOUNT_CONFIG_UPDATE`, `listenKeyExpired`). 수량이 계약 수 기준이고 손익이 코인으로 정산된다.
- **Options**: `ACCOUNT_UPDATE`, `ORDER_TRADE_UPDATE`, `RISK_LEVEL_CHANGE`(위험도 변화 경고).

---

## 운영 시 주의사항

커넥션 수명, ping/pong, 재연결, 보안 체크리스트는 시장 공통이므로
[binanceWebSocketREADME.md](binanceWebSocketREADME.md#운영-시-주의사항)를 함께 참고한다.
아래는 User Data Stream에 특히 중요한 항목이다.

- User Data 이벤트는 **at-least-once**다. 중복 도착을 전제로 멱등 키를 설계한다.
  - Spot: `orderId + tradeId + executionType`
  - USD-M: `orderId + tradeId` (없으면 `orderId + executionType + transactionTime`)
- 가격·수량은 문자열로 온다. `float`가 아니라 `Decimal` / `BigDecimal`로 파싱한다.
- 재연결 후에는 반드시 REST로 상태를 재동기화한다. 끊긴 동안의 이벤트는 재전송되지 않는다.

```text
재연결 직후 reconciliation
  Spot   : /api/v3/account  + /api/v3/openOrders  + /api/v3/myTrades
  USD-M  : /fapi/v2/account + /fapi/v1/openOrders + /fapi/v2/positionRisk
```

- Collector용 키에는 **조회 + User Data 권한만** 부여한다. 주문·출금 권한은 주지 않는다.
- 주문 기능이 필요하면 `TRADE` 권한을 가진 **별도 키 + 별도 프로세스**로 분리한다.
- listenKey, signature, API 키가 포함된 URL을 로그에 남기지 않는다.
- 시장 데이터 토픽과 계정 데이터 토픽/테이블을 분리하고 접근 권한을 다르게 준다.

---

## 최소 구현 예제

### listenKey 방식 (USD-M Futures / COIN-M / Options 공통, Spot은 불가)

```python
import asyncio, json, os, time, httpx, websockets

API_KEY = os.environ["BINANCE_API_KEY"]
REST    = "https://fapi.binance.com"
WS      = "wss://fstream.binance.com/ws"
HEADERS = {"X-MBX-APIKEY": API_KEY}

async def keepalive(client, listen_key, stop):
    while not stop.is_set():
        # 60분 만료, 그 안에 갱신
        try:
            await asyncio.wait_for(stop.wait(), timeout=1800)
        except asyncio.TimeoutError:
            await client.put(f"{REST}/fapi/v1/listenKey", headers=HEADERS)

async def main():
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{REST}/fapi/v1/listenKey", headers=HEADERS)
        listen_key = r.json()["listenKey"]

        stop = asyncio.Event()
        task = asyncio.create_task(keepalive(client, listen_key, stop))
        try:
            async with websockets.connect(f"{WS}/{listen_key}") as ws:
                async for raw in ws:
                    ev = json.loads(raw)
                    recv_ms = int(time.time() * 1000)
                    if ev["e"] == "ORDER_TRADE_UPDATE":
                        o = ev["o"]
                        print(o["s"], o["x"], o["X"],
                              "filled", o["z"], "@", o["L"],
                              "lag", recv_ms - ev["E"], "ms")
                    elif ev["e"] == "ACCOUNT_UPDATE":
                        print("balances", ev["a"]["B"])
        finally:
            stop.set()
            await task
            await client.delete(f"{REST}/fapi/v1/listenKey", headers=HEADERS)
```

> COIN-M으로 바꾸려면 `REST`를 `https://dapi.binance.com`, 경로를 `/dapi/v1/listenKey`,
> `WS`를 `wss://dstream.binance.com/ws`로 바꾼다. Options는 `eapi`/`nbstream.binance.com/eoptions/ws`.
> **Spot은 이 방식이 없다** — 아래 WebSocket API 방식만 사용한다.

### WebSocket API 방식 (Spot 권장, Ed25519)

```python
import asyncio, base64, json, os, time, websockets
from cryptography.hazmat.primitives.serialization import load_pem_private_key

API_KEY = os.environ["BINANCE_API_KEY"]
WS_API  = "wss://ws-api.binance.com:443/ws-api/v3"

with open(os.environ["BINANCE_ED25519_KEY_PATH"], "rb") as f:
    private_key = load_pem_private_key(f.read(), password=None)

def sign(params: dict) -> str:
    payload = "&".join(f"{k}={params[k]}" for k in sorted(params))
    return base64.b64encode(private_key.sign(payload.encode())).decode()

async def main():
    async with websockets.connect(WS_API) as ws:
        params = {"apiKey": API_KEY, "timestamp": int(time.time() * 1000)}
        params["signature"] = sign(params)
        await ws.send(json.dumps({"id": "logon", "method": "session.logon",
                                  "params": params}))
        print(json.loads(await ws.recv())["status"])

        await ws.send(json.dumps({"id": "sub", "method": "userDataStream.subscribe"}))

        async for raw in ws:
            msg = json.loads(raw)
            if "event" in msg:                 # 구독 이벤트는 event로 감싸짐
                ev = msg["event"]
                print(ev["e"], ev.get("s"), ev.get("X"))
            else:                              # 요청에 대한 응답
                print("response", msg.get("id"), msg.get("status"))
```

---

## 요약

| 시장 | 연결 방식 | 비고 |
| --- | --- | --- |
| Spot | WebSocket API (`session.logon` 또는 `subscribe.signature`) | listenKey는 `410 Gone`으로 폐지됨 |
| USD-M Futures | listenKey | 60분 만료, 주기적 `PUT` 갱신 필요 |
| COIN-M Futures | listenKey | 60분 만료, 손익은 코인으로 정산 |
| Options | listenKey | `RISK_LEVEL_CHANGE` 이벤트 존재 |

---

## 참고 문서

- [Spot User Data Stream](https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream)
- [Spot WebSocket API — User Data Stream Requests](https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests)
- [USDⓈ-M Futures User Data Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams)
- [USDⓈ-M Futures — Event Order Update](https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Order-Update)
- [USDⓈ-M Futures — Event Balance and Position Update](https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Balance-and-Position-Update)
- [COIN-M Futures User Data Streams](https://developers.binance.com/docs/derivatives/coin-margined-futures/user-data-streams)
- [Derivatives Change Log](https://developers.binance.com/docs/derivatives/change-log)
