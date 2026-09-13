# collector — Insight 1-1용 원시 데이터 수집 POC (Spot-Futures)

`datasource-ingestion/README.md`에서 설명하는 `core/` + `app/ingestion-runner` 구조로 쪼개기 전,
**심볼 하나**만 갖고 파이프라인이 실제로 동작하는지 확인하기 위한 단일 프로세스 POC다. 심볼은
`app.symbol` 설정(기본값 BTCUSDT)으로 정해지며 코드 수정 없이 실행 인자로 바꿀 수 있다.

이 앱은 **producer**다 — Spot/USD-M 스트림을 구독해서 파싱한 raw 이벤트를 각자 토픽에 올리기만
한다. Spot mid price와 mark price를 조합해 basis를 계산하는 건 이 앱의 책임이 아니라, 두 raw
토픽을 구독하는 컨슈머(Kafka Streams `basis-join`, [1_1_plan.md §4.5](../../Insights/1.spot_futures_basis/1_1_plan.md#45-kafka-토픽--다운스트림))가 할 일이다. 그래서 코드/클래스명에 "basis"를
쓰지 않는다.

- Spot `<symbol>@bookTicker` (`stream.binance.com`)와 USD-M `<symbol>@markPrice@1s`
  (`fstream.binance.com`) 두 WebSocket에 붙는다.
- 수신한 이벤트를 파싱해서 로그로 남기고, 그대로 raw 이벤트로 발행한다 (조합·계산 없음).
- Kafka는 아직 붙이지 않았다. `kafka/RawEventPublisher`가 무엇을 어떤 토픽/키로 보낼지
  로그로만 찍고, 실제 `KafkaTemplate.send()` 호출은 아직 없다.
- 지금은 Pod 단위 분리, Active-Active, Redis dedup 없이 **한 프로세스가 두 스트림을 다 처리**한다
  (Insight 1-1 계획의 §4.2~4.4는 다음 단계).

## 패키지 구조

```
collector/
└── src/main/java/com/ingestion/
    ├── IngestionCollectorApplication.java   # main() — SpringApplication 부트스트랩
    ├── config/
    │   └── CollectorProperties.java         # app.symbol 바인딩 (@ConfigurationProperties)
    ├── runner/
    │   └── CollectorRunner.java          # 진입 로직 — ApplicationRunner.run()에서
    │                                         #   URI 조립 → WebSocket 연결 → 메시지 콜백 등록
    ├── collector/
    │   └── BinanceStreamClient.java         # WebSocket 클라이언트 (연결·재연결·수신)
    ├── model/
    │   ├── SpotBookTicker.java              # spot bookTicker 페이로드 (record)
    │   └── MarkPriceUpdate.java             # usdm markPrice 페이로드 (record)
    ├── service/
    │   └── MarketDataIngestService.java     # 수신 로깅 + 발행 위임 (계산 없음)
    └── kafka/
        └── RawEventPublisher.java           # raw 이벤트 발행 지점 (지금은 로그만)
```

패키지 하나 = 역할 하나로 나뉜다: `config`(설정) → `runner`(오케스트레이션) → `collector`(전송)
→ `model`(파싱) → `service`(로깅/위임) → `kafka`(발행). 데이터는 이 순서대로 흐른다 — 자세한 실행
흐름은 아래 "실행"보다는 코드 자체의 호출 순서(`CollectorRunner.run()`부터)를 따라가는 게 빠르다.
basis 계산은 여기 어디에도 없다 — 위 설명대로 다운스트림 컨슈머의 몫이다.

## 실행

### 준비물

- JDK 17 이상 (`java -version`으로 확인)
- 별도 설치 필요 없음 — `gradlew`(Gradle Wrapper)가 알아서 Gradle을 내려받는다.
- 인터넷 연결 (Binance WebSocket에 직접 접속하므로)

### 실행 방법

기본값(BTCUSDT)으로 실행:

```bash
cd collector
./gradlew bootRun
```

다른 심볼로 실행하려면 `--app.symbol` 인자를 덮어쓴다 (예: ETHUSDT):

```bash
./gradlew bootRun --args='--app.symbol=ethusdt'
```

`collector/config/ethusdt.yml`처럼 심볼별 설정 파일을 만들어두고 그걸 불러올 수도 있다:

```bash
./gradlew bootRun --args='--spring.config.location=config/ethusdt.yml'
```

빌드된 jar로 바로 실행할 때도 동일한 인자를 쓴다:

```bash
./gradlew build -x test
java -jar build/libs/ingestion-collector-0.0.1-SNAPSHOT.jar --app.symbol=ethusdt
```

Windows에서는 `gradlew.bat bootRun`을 쓴다.

`web-application-type: none`이라 HTTP 서버는 뜨지 않고, 접속할 포트도 없다. 실행하면 콘솔에 아래와
같은 로그가 계속 찍히는지로 동작을 확인한다.

```
[SPOT ] BTCUSDT bid=... ask=... mid=...                          # Spot bookTicker 수신
[KAFKA:SKIPPED] topic=binance.raw.bookticker key=spot:BTCUSDT     # raw 이벤트 발행 (지금은 로그만)
[FUT  ] BTCUSDT markPrice=... fundingRate=...                     # USD-M markPrice 수신
[KAFKA:SKIPPED] topic=binance.raw.markprice key=usdm:BTCUSDT      # raw 이벤트 발행 (지금은 로그만)
```

두 스트림은 서로 기다리지 않고 각자 받는 즉시 바로 발행된다 (조인 없음).

- `[SPOT]`만 계속 찍히고 `[FUT]`가 안 보인다면 markPrice 스트림이 막힌 환경일 수 있다
  (아래 "확인된 것 / 안 된 것" 참고).
- 아무 로그도 안 찍히면 네트워크(방화벽/프록시)가 `wss://` 접속을 막고 있는지 먼저 의심한다.
- 종료는 `Ctrl+C`.

## 확인된 것 / 안 된 것

- Spot bookTicker는 이 환경에서 연결·수신이 정상 동작을 확인했다.
- USD-M `markPrice` 계열 WebSocket(`markPrice`, `markPrice@1s`, `!markPrice@arr`, combined stream
  전부)은 핸드셰이크(OPEN)까지는 성공하지만 이 개발 환경에서는 데이터가 전혀 내려오지 않았다.
  같은 환경에서 `fapi.binance.com` REST(`/fapi/v1/premiumIndex`)는 정상 응답하므로, WS 연결 자체가
  막힌 게 아니라 이 네트워크에서 파생상품 스트리밍 데이터만 걸러지는 것으로 보인다(지역 제한 등
  Binance 쪽 정책 가능성). 실제 배포/실행 환경에서 다시 확인이 필요하다.
