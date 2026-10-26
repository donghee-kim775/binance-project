# collector — Binance raw 이벤트 수집 앱

Binance WebSocket을 구독해서 받은 메시지를 **원본 그대로** envelope로 감싸 Kafka raw 토픽에 보내는
단일 Spring Boot 앱이다. 어떤 product의 어떤 심볼을, 어떤 스트림으로 받아서 어느 토픽에 보낼지는
코드가 아니라 `binance.subscription.*` 설정이 정한다 — variable-rate / fixed-rate pod는 같은 이미지에 설정만
다르게 넣어서 띄운다.

이 앱은 **producer**다 — payload를 해석하지 않고 옮기기만 한다. Active-Active 중복 제거,
Spot mid price와 mark price를 조합하는 basis 계산 같은 일은 이 앱의 책임이 아니라 raw 토픽을
구독하는 별도 Flink 앱의 책임이다. 그래서 스트림별 모델 클래스도 없다.

- `binance.subscription.streams` 항목 하나마다 WebSocket 소켓을 하나씩 연다.
- 심볼이 1개면 단일 스트림(`/ws/...`), 여러 개면 combined 스트림(`/stream?streams=...`)으로 붙는다.
- 수신한 메시지에서 모든 스트림에 공통인 심볼(`s`)과 이벤트 시각(`E`)만 꺼내 envelope를 만든다.
- Kafka는 아직 붙이지 않았다. `kafka/RawEventPublisher`가 어떤 토픽/키로 보낼지 로그로만 찍고,
  실제 `KafkaTemplate.send()` 호출은 아직 없다.

설정 값이 코드 어디로 들어가는지는 [deploy/mapping.md](../deploy/mapping.md)에 정리돼 있다.

## 패키지 구조

```
collector/
├── config/ethusdt.yml                        # 로컬에서 다른 심볼로 실행해보는 예시 설정
└── src/main/java/com/ingestion/
    ├── IngestionCollectorApplication.java   # main() — SpringApplication 부트스트랩
    ├── config/
    │   ├── BinanceProperties.java           # binance.* — Endpoint(접속 도메인, 환경별) · Subscription(구독 설정, pod별)
    │   └── AsyncConfig.java                 # 메시지 처리용 스레드풀
    ├── runner/
    │   └── CollectorRunner.java             # streams를 돌며 URL 조립 → 소켓 연결
    ├── collector/
    │   └── BinanceStreamClient.java         # WebSocket 클라이언트 (연결·재연결·수신)
    ├── service/
    │   └── MarketDataIngestService.java     # combined 메시지 풀기 → envelope 생성 → 발행 위임
    ├── model/
    │   └── EventEnvelope.java               # 공통 envelope (payload는 원본 그대로)
    └── kafka/
        └── RawEventPublisher.java           # raw 이벤트 발행 지점 (지금은 로그만)
```

패키지 하나 = 역할 하나로 나뉜다: `config`(설정) → `runner`(오케스트레이션) → `collector`(전송)
→ `service`(envelope 생성) → `kafka`(발행). 데이터는 이 순서대로 흐른다 — 자세한 흐름은
`CollectorRunner.run()`부터 호출 순서를 따라가는 게 빠르다.

## 실행

### 준비물

- JDK 17 이상 (`java -version`으로 확인)
- 별도 설치 필요 없음 — `gradlew`(Gradle Wrapper)가 알아서 Gradle을 내려받는다.
- 인터넷 연결 (Binance WebSocket에 직접 접속하므로)

### 실행 방법

기본값으로 실행 — `application.yml`의 `binance.subscription` 기본값은 `spot-sov` pod와 같다
(Spot BTCUSDT의 bookTicker · trade · depth@100ms, 소켓 3개):

```bash
cd collector
./gradlew bootRun
```

다른 구독 설정으로 실행하려면 설정 파일을 추가로 불러온다. 불러온 파일의 `binance.subscription` 블록이
기본값을 덮어쓴다 (예: ETHUSDT bookTicker만):

```bash
./gradlew bootRun --args='--spring.config.additional-location=config/ethusdt.yml'
```

`deploy/`의 ConfigMap에 들어 있는 `application.yaml` 내용을 파일로 저장해서 같은 방법으로 불러오면,
해당 pod와 똑같은 구독으로 로컬에서 띄워볼 수 있다.

빌드된 jar로 바로 실행할 때도 동일한 인자를 쓴다:

```bash
./gradlew build -x test
java -jar build/libs/ingestion-collector-0.0.1-SNAPSHOT.jar --spring.config.additional-location=config/ethusdt.yml
```

Windows에서는 `gradlew.bat bootRun`을 쓴다.

`web-application-type: none`이라 HTTP 서버는 뜨지 않고, 접속할 포트도 없다. 실행하면 콘솔에 아래와
같은 로그가 계속 찍히는지로 동작을 확인한다.

```
[spot-sov-bookTicker] connecting to wss://stream.binance.com:9443/ws/btcusdt@bookTicker   # 소켓 연결
[spot-sov-bookTicker] connected
[variable-rate] bookTicker BTCUSDT event_time=null                                            # 메시지 수신
[KAFKA:SKIPPED] topic=binance.raw.spot.crypto.bookticker key=BTCUSDT                          # 발행 (지금은 로그만)
[variable-rate] trade BTCUSDT event_time=1792724161600
[KAFKA:SKIPPED] topic=binance.raw.spot.crypto.trade key=BTCUSDT
```

- Spot bookTicker는 원본에 이벤트 시각(`E`)이 없어서 `event_time=null`로 찍힌다 (정상).
- 섹터 설정(심볼 여러 개)으로 띄우면 소켓 하나에서 여러 심볼이 섞여 오고, 키도 심볼별로 나뉜다
  (`key=LITEUSDT`, `key=CRDOUSDT`, ...).
- `connected`까지 찍히는데 메시지가 안 오면 USD-M 경로 접두어(`/public`, `/market`)가 스트림과
  맞는지 먼저 확인한다 — 틀려도 연결은 된다.
- 아무 로그도 안 찍히면 네트워크(방화벽/프록시)가 `wss://` 접속을 막고 있는지 의심한다.
- 종료는 `Ctrl+C`.

## 확인된 것

로컬 환경에서 실제 Binance에 붙여 아래 조합의 연결·수신을 확인했다.

| product | 심볼 | 스트림 | 형식 |
| --- | --- | --- | --- |
| Spot | BTCUSDT | bookTicker, trade, depth@100ms | 단일 |
| USD-M | NVDA · AVGO · MRVL | bookTicker, depth@100ms (`/public`), aggTrade (`/market`) | combined |
| USD-M | LITE · CRDO · ALAB | markPrice@1s (`/market`) | combined |

예전에 USD-M markPrice가 연결만 되고 데이터가 오지 않았던 건, `/market` 경로 접두어 없이 붙었기
때문이다. 접두어를 넣은 뒤로는 정상 수신된다.
