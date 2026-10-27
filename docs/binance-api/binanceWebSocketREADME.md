# Binance WebSocket 스트림 정리

Binance WebSocket을 시장(product)별로 나누어 정리한 문서다. 2026-08-26 기준 공식 문서를 참고했다.

- 상위 API 가이드: [binanceAPIREADME.md](binanceAPIREADME.md)
- 프로젝트 개요: [README.md](../../README.md)

시장별 상세 문서 (Market Stream):

- [spotMarketStreamREADME.md](../streams/spotMarketStreamREADME.md) — Spot (현물)
- [usdmFuturesMarketStreamREADME.md](../streams/usdmFuturesMarketStreamREADME.md) — USD-M Futures
- [coinmFuturesMarketStreamREADME.md](../streams/coinmFuturesMarketStreamREADME.md) — COIN-M Futures
- [optionsMarketStreamREADME.md](../streams/optionsMarketStreamREADME.md) — Options
- [userDataStreamREADME.md](../streams/userDataStreamREADME.md) — User Data Stream (모든 시장의 계정 이벤트)

---

## 1. 시장 분류와 도메인

Binance는 시장(product)별로 **도메인 자체가 다르다.** 심볼만 바꿔서 같은 코드에 밀어넣으면 안 되고,
product별로 커넥션·파서·저장 스키마를 분리하는 것이 안전하다. 각 시장의 심볼 체계와 스트림 목록은
위의 시장별 문서를 참고한다.

| 분류 | 시장 | Market Stream 도메인 | REST Base | 상세 문서 |
| --- | --- | --- | --- | --- |
| Spot | 현물 | `wss://stream.binance.com:9443` | `https://api.binance.com` | [spotMarketStreamREADME.md](../streams/spotMarketStreamREADME.md) |
| Derivatives | USD-M Futures | `wss://fstream.binance.com` | `https://fapi.binance.com` | [usdmFuturesMarketStreamREADME.md](../streams/usdmFuturesMarketStreamREADME.md) |
| Derivatives | COIN-M Futures | `wss://dstream.binance.com` | `https://dapi.binance.com` | [coinmFuturesMarketStreamREADME.md](../streams/coinmFuturesMarketStreamREADME.md) |
| Derivatives | Options | `wss://nbstream.binance.com/eoptions` | `https://eapi.binance.com` | [optionsMarketStreamREADME.md](../streams/optionsMarketStreamREADME.md) |

---

## 2. Market Stream vs User Data Stream

| 구분 | Market Stream | User Data Stream |
| --- | --- | --- |
| 내용 | 체결, 캔들, 호가, 티커 | 내 잔고, 주문, 체결, 포지션 |
| API 키 | 불필요 | **필요** |
| 대상 | 전체 시장 (모두에게 동일) | 내 계정만 |
| 구독 단위 | 심볼 + 스트림 이름 | 계정 단위 (심볼 지정 없음) |
| 커넥션 | 심볼 수만큼 확장 | 계정당 1개 |

**둘은 반드시 커넥션과 큐를 분리한다.** 시세 스트림은 유실돼도 REST로 backfill되지만,
계정 이벤트는 유실되면 주문 상태를 잃는다. 보존 정책과 접근 권한도 다르게 가져가야 한다.

```text
Market Stream    ──> market collector  ──> market_events   (공개, 대용량)
User Data Stream ──> account collector ──> account_events  (민감, 저용량)
```

User Data Stream의 연결 방식(Spot WebSocket API / Futures·Options listenKey)과 이벤트 상세는
[userDataStreamREADME.md](../streams/userDataStreamREADME.md) 참고.

---

## 3. 운영 시 주의사항

시장(Market Stream)과 계정(User Data Stream) 커넥션에 공통으로 적용되는 규칙이다.

### 커넥션

- **단일 커넥션은 24시간만 유효하다.** 24시간 지점에서 끊기는 것이 정상이니, 미리 예비 커넥션을 띄우고
  전환하는 방식(make-before-break)이 안전하다.
- 서버가 3분마다 ping frame을 보낸다. **10분 내 pong이 없으면 끊는다.** 대부분의 클라이언트
  라이브러리가 자동 응답하지만, 이벤트 루프가 블로킹되면 pong이 늦어져 끊긴다.
  파싱·DB 저장은 반드시 수신 루프와 분리된 큐/태스크에서 처리한다.
- 재연결은 지수 backoff + jitter로. 반복 재연결은 IP ban으로 이어진다.
- 커넥션당 최대 1024 스트림, 초당 5(Spot) ~ 10(Futures) 메시지 제한.

### 데이터

- `event_time`(E)과 `receive_time`(로컬 수신 시각)을 **둘 다 저장한다.** 차이가 지연 지표가 된다.
- User Data Stream 특유의 멱등 키 설계, at-least-once 처리, reconciliation 절차는
  [userDataStreamREADME.md](../streams/userDataStreamREADME.md#운영-시-주의사항) 참고.

### 보안

- Collector용 키에는 **조회 + User Data 권한만** 부여한다. 주문·출금 권한은 주지 않는다.
- 주문 기능이 필요하면 `TRADE` 권한을 가진 **별도 키 + 별도 프로세스**로 분리한다.
- IP whitelist를 적용한다.
- listenKey, signature, API 키가 포함된 URL을 로그에 남기지 않는다.
- 시장 데이터 토픽과 계정 데이터 토픽/테이블을 분리하고 접근 권한을 다르게 준다.

### Testnet

```text
Spot REST      : https://testnet.binance.vision/api
Spot Stream    : wss://stream.testnet.binance.vision/ws
Spot WS API    : wss://ws-api.testnet.binance.vision/ws-api/v3
Futures REST   : https://testnet.binancefuture.com
Futures Stream : wss://fstream.binancefuture.com
```

프로덕션 키를 테스트넷에 재사용하지 않는다. base URL은 코드에 분기하지 말고 설정으로 주입한다.

---

## 4. 이 프로젝트 적용 순서

```text
1. Public market stream        trade + kline_1m + bookTicker  (키 불필요, 바로 시작)
2. REST bootstrap/recovery     exchangeInfo + klines + depth
3. User Data Stream            읽기 전용 키로 executionReport / outboundAccountPosition
4. 주기적 reconciliation       account + openOrders + myTrades
5. USD-M 추가                  markPrice + forceOrder + ACCOUNT_UPDATE
6. 주문 기능                   별도 서비스 + 별도 TRADE 키, Testnet 검증 먼저
```

시장 수집 / 계정 수집 / 주문 실행을 **프로세스와 키 권한 수준에서 분리**하면
장애와 키 유출의 영향 범위가 크게 줄어든다.

Kafka 토픽 설계와 스트림 조인(join) 아이디어는 [kafkaTopicsREADME.md](../kafka/kafkaTopicsREADME.md) 참고.
PoC 코드는 [streams/poc/](../streams/poc/) 디렉터리에 시장별 노트북(`spot_market_stream.ipynb` 등)으로 있다.

---

## 참고 문서

- [Binance Developer Documentation](https://developers.binance.com/en/docs/introduction)
- [Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [Spot User Data Stream](https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream)
- [Spot WebSocket API — User Data Stream Requests](https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests)
- [USDⓈ-M Futures User Data Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams)
- [COIN-M Futures User Data Streams](https://developers.binance.com/docs/derivatives/coin-margined-futures/user-data-streams)
- [Derivatives Change Log](https://developers.binance.com/docs/derivatives/change-log)
