# Binance API 활용 가이드

이 문서는 이 저장소의 시장 데이터 Collector에서 활용할 만한 Binance API를 정리한다. 2026-08-21 기준이며, 우선 Spot API를 대상으로 한다. USDⓈ-M Futures는 마지막 절에서 별도로 구분한다.

공식 문서:

- [Binance Developer Documentation](https://developers.binance.com/en/docs/introduction)
- [Spot REST API](https://developers.binance.com/en/docs/products/spot/rest-api)
- [Spot WebSocket Streams](https://developers.binance.com/en/docs/binance-spot-api-docs/web-socket-streams)
- [Spot WebSocket API](https://developers.binance.com/en/docs/binance-spot-api-docs/websocket-api/general-api-information)
- [Spot User Data Stream](https://developers.binance.com/en/docs/binance-spot-api-docs/user-data-stream)
- [Spot Testnet](https://developers.binance.com/docs/binance-spot-api-docs/testnet/general-info)
- [USDⓈ-M Futures](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/Introduction)

## 먼저 알아둘 구분

Binance의 WebSocket 관련 기능은 서로 역할이 다르다.

| 종류 | 목적 | API 키 |
| --- | --- | --- |
| WebSocket Market Streams | 체결, 캔들, 호가, 티커 실시간 수집 | 불필요 |
| User Data Stream | 내 잔고, 주문, 체결 상태 실시간 수신 | 필요 |
| WebSocket API | WebSocket 연결에서 조회·주문 요청 수행 | private 요청에 필요 |
| REST API | 스냅샷, 과거 데이터, 계좌 조회, 주문 | endpoint에 따라 필요 |

API 키가 있어도 공개 시세 스트림이 더 상세해지거나 빨라지는 것은 아니다. 키는 계좌 데이터와 주문 기능을 열어준다. 따라서 Collector는 다음처럼 연결을 분리하는 것이 안전하다.

```text
Market Stream ──> market event collector ──> market_events

User Data Stream ──> account event collector ──> account_events

Signed REST / WebSocket API ──> snapshot, reconciliation, command
```

시장 데이터와 개인 계좌 데이터는 토픽, 테이블, 보존 정책 및 접근 권한을 분리한다.

## 추천 구현 순서

1. `trade`, `kline`, `bookTicker` Market Stream 수집
2. `exchangeInfo`로 심볼 필터와 주문 단위 동기화
3. `depth` snapshot과 diff stream으로 로컬 오더북 구성
4. API 키에 읽기 권한만 부여하고 잔고·주문 조회 추가
5. User Data Stream으로 주문과 잔고 변경 실시간 수집
6. Spot Testnet에서 주문 생성·취소 검증
7. 실제 주문 기능이 필요할 때만 `TRADE` 권한을 가진 별도 키 사용

## Spot Market WebSocket Streams

프로덕션 combined stream 예시:

```text
wss://stream.binance.com:9443/stream?streams=
  btcusdt@trade/
  btcusdt@kline_1m/
  btcusdt@bookTicker/
  btcusdt@depth@100ms
```

실제 URL에는 줄바꿈을 넣지 않는다.

```text
wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@kline_1m/btcusdt@bookTicker
```

combined stream 메시지는 다음 형태로 감싸져 온다.

```json
{
  "stream": "btcusdt@trade",
  "data": {
    "e": "trade",
    "E": 1672515782136,
    "s": "BTCUSDT"
  }
}
```

유용한 스트림:

| 스트림 | 용도 | Collector 권장도 |
| --- | --- | --- |
| `<symbol>@trade` | 개별 체결, 가격·수량·maker 방향 | 필수 |
| `<symbol>@aggTrade` | 단일 taker 주문 기준 집계 체결 | 선택; trade와 중복 수집 주의 |
| `<symbol>@kline_<interval>` | 진행 중/종료된 캔들 | 필수 |
| `<symbol>@bookTicker` | 최우선 bid/ask와 수량 | 스프레드 계산에 권장 |
| `<symbol>@depth` | 오더북 변경 | order flow 분석에 권장 |
| `<symbol>@depth@100ms` | 빠른 오더북 변경 | 처리량을 감당할 때 사용 |
| `<symbol>@ticker` | 24시간 rolling 통계 | 대시보드용 |
| `<symbol>@miniTicker` | 축약된 24시간 통계 | 심볼 랭킹용 |
| `!miniTicker@arr` | 변경된 전체 심볼 mini ticker | 마켓 스캐너용 |

운영 시 주의사항:

- stream 이름의 symbol은 소문자를 사용한다.
- 연결은 영구적이라고 가정하지 말고 재연결과 backoff를 구현한다.
- ping frame에 pong으로 응답한다.
- combined stream 한 연결에 지나치게 많은 심볼을 몰지 않는다.
- 수신 시각과 Binance event time을 모두 저장한다.
- `trade`와 `aggTrade`를 동시에 저장하면 같은 경제적 체결을 중복 집계할 수 있다.

### 로컬 오더북

`depth` stream만 받아서는 완전한 오더북을 만들 수 없다. 다음 순서를 사용한다.

1. `<symbol>@depth` 또는 `<symbol>@depth@100ms` 이벤트를 먼저 buffer한다.
2. `GET /api/v3/depth`로 snapshot을 받는다.
3. snapshot의 `lastUpdateId`보다 이전인 buffered 이벤트를 버린다.
4. update ID가 snapshot과 이어지는지 확인한 뒤 이벤트를 순서대로 적용한다.
5. update ID gap이 발견되면 로컬 북을 폐기하고 snapshot부터 다시 시작한다.

가격 레벨 수량이 `0`이면 해당 레벨을 삭제한다. 이 과정은 심볼마다 독립적인 상태 머신으로 구현하는 편이 좋다.

## Spot REST Market Data

Base URL:

```text
https://api.binance.com
```

공개 시장 데이터만 호출한다면 다음 endpoint도 사용할 수 있다.

```text
https://data-api.binance.vision
```

| Method | Endpoint | 활용 |
| --- | --- | --- |
| `GET` | `/api/v3/ping` | 연결 확인 |
| `GET` | `/api/v3/time` | 서버 시각 확인 및 clock offset 계산 |
| `GET` | `/api/v3/exchangeInfo` | 심볼 상태, rate limit, 가격·수량 filter |
| `GET` | `/api/v3/depth` | 로컬 오더북 초기 snapshot |
| `GET` | `/api/v3/trades` | 최근 체결 |
| `GET` | `/api/v3/historicalTrades` | 과거 체결 조회; API key 필요 가능 |
| `GET` | `/api/v3/aggTrades` | 압축된 과거 체결 |
| `GET` | `/api/v3/klines` | 캔들 bootstrap 및 누락 구간 복구 |
| `GET` | `/api/v3/uiKlines` | UI 표시 목적 캔들 |
| `GET` | `/api/v3/avgPrice` | 현재 평균 가격 |
| `GET` | `/api/v3/ticker/24hr` | 24시간 rolling 통계 |
| `GET` | `/api/v3/ticker/price` | 최신 가격 |
| `GET` | `/api/v3/ticker/bookTicker` | 최우선 bid/ask snapshot |

Collector에서 특히 유용한 호출:

```bash
curl 'https://api.binance.com/api/v3/exchangeInfo?symbol=BTCUSDT'
curl 'https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000'
curl 'https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000'
```

`exchangeInfo`의 `PRICE_FILTER`, `LOT_SIZE`, `MIN_NOTIONAL` 또는 현재 제공되는 notional 관련 filter를 주문 전 반드시 적용한다. 가격과 수량은 `double`이 아니라 Java `BigDecimal` 또는 Python `Decimal`로 계산한다.

## 인증과 API 키

보안 유형은 대략 다음과 같다.

| 유형 | 의미 |
| --- | --- |
| `NONE` | 공개 데이터 |
| `USER_DATA` | 잔고, 주문, 거래 내역 등 계좌 정보 |
| `TRADE` | 주문 생성·취소 |
| `USER_STREAM` | 계좌 이벤트 구독 관리 |

권장 키 구성:

- Collector용 키: 조회 및 User Data만 허용
- 주문 서비스용 키: `TRADE` 권한을 별도 부여
- 출금 권한: 이 프로젝트에서는 활성화하지 않음
- 가능하면 IP whitelist 적용
- 개발과 운영 키 분리
- Spot과 Futures 키/권한 및 저장소 분리

환경 변수 예시:

```bash
export BINANCE_API_KEY='...'
export BINANCE_API_SECRET='...'
```

`.env`, YAML, Git 저장소, Docker image, 로그에 실제 값을 기록하지 않는다. 요청 URL 전체를 로깅하면 query parameter의 signature와 주문 정보가 노출될 수 있으므로 민감 파라미터를 masking한다.

## HMAC SIGNED REST 요청

HMAC 키를 사용하는 signed endpoint는 일반적으로 다음 값이 필요하다.

- Header: `X-MBX-APIKEY`
- Parameter: `timestamp`
- Parameter: `recvWindow` — 기본 5000ms, 가능하면 작게 유지
- Parameter: `signature`

서명 절차:

1. 실제 전송할 parameter를 URL encoding한다.
2. encoding된 query string을 API secret으로 HMAC-SHA256 서명한다.
3. 결과 hex 문자열을 `signature`로 추가한다.

Python 예시:

```python
import hashlib
import hmac
import os
import time
from urllib.parse import urlencode


def signed_query(params: dict[str, str]) -> str:
    params = {
        **params,
        "recvWindow": "5000",
        "timestamp": str(int(time.time() * 1000)),
    }
    payload = urlencode(params)
    signature = hmac.new(
        os.environ["BINANCE_API_SECRET"].encode(),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload}&signature={signature}"
```

Java 예시:

```java
static String hmacSha256(String secret, String payload) throws Exception {
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
    return HexFormat.of().formatHex(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
}
```

서버 시각과 로컬 시각 차이가 크면 `-1021` 계열 timestamp 오류가 발생할 수 있다. 주기적으로 `/api/v3/time`을 조회해 offset을 계산하되 운영체제 NTP 동기화도 유지한다.

Binance는 HMAC 외에 RSA와 Ed25519 키를 지원한다. 신규 설계에서 키 관리 체계를 갖출 수 있다면 공식 문서가 권장하는 Ed25519도 검토할 수 있다.

## 계좌 및 주문 조회 API

| Method | Endpoint | 보안 | 활용 |
| --- | --- | --- | --- |
| `GET` | `/api/v3/account` | USER_DATA | 자산별 잔고, 계좌 상태 |
| `GET` | `/api/v3/order` | USER_DATA | 특정 주문 상태 확인 |
| `GET` | `/api/v3/openOrders` | USER_DATA | 미체결 주문 복구 |
| `GET` | `/api/v3/allOrders` | USER_DATA | 주문 내역 backfill |
| `GET` | `/api/v3/myTrades` | USER_DATA | 내 체결 내역 backfill |
| `GET` | `/api/v3/account/commission` | USER_DATA | 심볼별 수수료율 확인 |
| `GET` | `/api/v3/rateLimit/order` | USER_DATA | 계정 주문 rate limit 상태 |

서비스 시작 시 권장 reconciliation:

1. `/api/v3/account`로 현재 잔고를 읽는다.
2. `/api/v3/openOrders`로 미체결 주문을 읽는다.
3. 마지막 저장 지점 이후 `/api/v3/allOrders` 및 `/api/v3/myTrades`를 보충한다.
4. User Data Stream 이벤트와 합쳐 event ID 기준으로 멱등 처리한다.

## User Data Stream

API 키가 있을 때 Collector에 가장 가치 있는 기능이다. polling 대신 다음 변경을 실시간으로 받을 수 있다.

| 이벤트 | 내용 |
| --- | --- |
| `outboundAccountPosition` | 이벤트 때문에 변경될 수 있는 자산 잔고 |
| `balanceUpdate` | 입출금 또는 계정 간 이동으로 발생한 balance delta |
| `executionReport` | 주문 생성, 취소, 거절, 체결, 만료 등 주문 상태 |
| `listStatus` | OCO 등 order list 상태 |
| `eventStreamTerminated` | 구독 또는 세션 종료 |

가장 중요한 이벤트는 `executionReport`다. 다음을 저장하는 것이 좋다.

- symbol
- client order ID와 Binance order ID
- execution type과 order status
- side, order type, time-in-force
- 주문 수량과 가격
- 이번 체결 수량·가격
- 누적 체결 수량과 quote quantity
- commission과 commission asset
- trade ID
- event time, transaction time, receive time

User Data Stream은 market stream과 별도 연결 및 별도 queue로 운영한다. 주문 이벤트는 중복 도착과 재연결을 전제로 `orderId + tradeId + executionType` 같은 안정적인 멱등 키를 설계한다.

현재 Spot API에서는 WebSocket API 세션에서 인증하고 user data 구독을 관리하는 방식을 우선 검토한다. 구현 시점의 공식 문서에서 `session.logon`, `userDataStream.subscribe` 지원 조건과 서명 방식을 다시 확인한다. 기존 listen key 방식이 필요한 환경이라면 listen key 생성·keepalive·만료 처리까지 하나의 lifecycle로 묶는다.

## 주문 API

실제 주문 권한은 테스트넷 검증 후 별도 서비스에 추가하는 것이 좋다. Collector 프로세스 자체에는 주문 권한을 주지 않는 편이 안전하다.

| Method | Endpoint | 활용 |
| --- | --- | --- |
| `POST` | `/api/v3/order/test` | 서명과 주문 파라미터 검증; matching engine에 주문하지 않음 |
| `POST` | `/api/v3/order` | 신규 주문 |
| `DELETE` | `/api/v3/order` | 주문 취소 |
| `POST` | `/api/v3/cancelReplace` | 기존 주문 취소와 새 주문 제출 결합 |
| `DELETE` | `/api/v3/openOrders` | 한 심볼의 모든 미체결 주문 취소 |

주문 요청에는 직접 생성한 고유 `newClientOrderId`를 넣는 것을 권장한다. 네트워크 timeout이나 HTTP 5xx가 발생했다고 주문 실패로 단정하지 말고 다음 순서로 확인한다.

1. User Data Stream의 `executionReport` 확인
2. 없으면 `GET /api/v3/order`로 `clientOrderId` 조회
3. 최종 상태가 확인되기 전 같은 주문을 새 ID로 무작정 재전송하지 않음

## Spot Testnet

실제 키가 있더라도 주문 기능은 별도의 Spot Testnet 키로 먼저 검증한다. 프로덕션 API 키를 테스트넷에 재사용하지 않는다.

대표적인 테스트 환경 주소:

```text
REST:              https://testnet.binance.vision/api
Market streams:    wss://stream.testnet.binance.vision/ws
WebSocket API:     wss://ws-api.testnet.binance.vision/ws-api/v3
```

환경별 base URL을 코드에 직접 분기하지 말고 설정으로 주입한다.

```yaml
binance:
  rest-base-url: ${BINANCE_REST_BASE_URL:https://api.binance.com}
  websocket-stream-url: ${BINANCE_WS_BASE_URL:wss://stream.binance.com:9443/stream}
  websocket-api-url: ${BINANCE_WS_API_URL:wss://ws-api.binance.com:443/ws-api/v3}
```

## Rate limit과 장애 처리

- `/api/v3/exchangeInfo`의 현재 rate limit 정보를 시작 시 읽는다.
- REST 응답의 `X-MBX-USED-WEIGHT-*` header를 metric으로 수집한다.
- HTTP 429에서는 `Retry-After`를 존중하고 즉시 backoff한다.
- 429를 무시하면 HTTP 418 IP ban으로 이어질 수 있다.
- endpoint별 request weight가 다르므로 호출 횟수만 세면 안 된다.
- 심볼 파라미터 없는 전체 조회는 weight가 더 클 수 있으므로 필요한 심볼만 요청한다.
- HTTP 5xx와 matching engine timeout은 주문 상태가 `UNKNOWN`일 수 있다.
- REST backfill은 pagination, start/end time, 마지막 처리 ID를 저장한다.
- WebSocket 연결은 재연결 후 누락 구간을 REST로 보충한다.

Collector metric 권장 목록:

```text
binance_ws_connected
binance_ws_reconnect_total
binance_event_received_total{event_type,symbol}
binance_event_parse_error_total
binance_queue_size
binance_event_lag_ms
binance_rest_used_weight
binance_rest_429_total
binance_order_reconcile_total
```

## USDⓈ-M Futures를 추가할 경우

Spot과 USDⓈ-M Futures는 base URL, event schema, 계좌 모델이 다르므로 같은 코드 경로에 symbol만 바꿔 넣지 않는다.

대표 base URL:

```text
REST:           https://fapi.binance.com
Market stream:  wss://fstream.binance.com
```

유용한 기능:

| 종류 | 활용 |
| --- | --- |
| mark price stream | mark price, funding rate, 다음 funding 시각 |
| liquidation stream | 강제 청산 이벤트 분석 |
| open interest REST | 미결제약정 snapshot |
| funding rate history | funding 분석 및 backfill |
| account/user stream | position, balance, order update |
| position risk REST | 현재 포지션 reconciliation |

Futures에서는 체결가 외에 mark price, index price, funding rate, position side, leverage, margin mode를 별도 필드로 저장한다. Spot의 `executionReport`와 Futures의 주문 update 이벤트를 같은 payload DTO로 억지로 통합하지 말고 공통 envelope와 product별 payload로 나누는 편이 안전하다.

## 이 프로젝트에서의 최종 추천

현재 Collector에는 다음 조합이 가장 실용적이다.

```text
1. Public market stream
   trade + kline_1m + bookTicker

2. REST bootstrap/recovery
   exchangeInfo + klines + depth

3. Authenticated account collector
   User Data Stream

4. Periodic reconciliation
   account + openOrders + allOrders + myTrades

5. Trading
   별도 서비스와 별도 TRADE 키, 먼저 Spot Testnet
```

API 키를 보유하고 있다는 이유로 Collector에 주문 권한까지 함께 주지 않는다. 시장 수집, 계좌 이벤트 수집, 주문 실행을 프로세스와 키 권한 수준에서 분리하면 장애와 키 유출의 영향 범위를 크게 줄일 수 있다.
