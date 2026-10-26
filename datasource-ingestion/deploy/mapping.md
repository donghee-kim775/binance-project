# ConfigMap 설정 → 코드 매핑

`deploy/<group>/*.yaml`의 ConfigMap에 적은 `binance.subscription.*` 값이 collector 코드 어디로 들어가서 어떻게 실행되는지, **실행 순서대로** 정리한다.

> **이 앱의 책임:** Binance에서 받은 raw 메시지를 envelope로 감싸 토픽에 보내는 것까지만 한다. payload는 해석하지 않고, 중복 제거(Active-Active)와 조인은 다운스트림 Flink 앱이 한다. 그래서 스트림별 모델 클래스가 없고, `group`도 동작에 영향을 주지 않는다.
>
> **Kafka 발행:** 아직 `spring-kafka` 의존성이 꺼져 있어서 `RawEventPublisher`는 토픽과 키를 로그로만 남긴다 (`[KAFKA:SKIPPED]`).

코드 경로는 모두 `collector/src/main/java/com/ingestion/` 기준이다.

---

## 0. 전체 흐름

```text
[K8s]     ① deploy/<group>/*.yaml        ConfigMap data.application.yaml
               │  volumes → volumeMounts (Deployment가 /config 에 마운트)
               ▼
          /config/application.yaml
               │
[Spring]  ② SpringApplication.run()        IngestionCollectorApplication:12
               │  Dockerfile ENV SPRING_CONFIG_ADDITIONAL_LOCATION=optional:file:/config/
               │  → spring.config.additional-location 설정으로 config 위치 명시
               │  → 이미지 안의 application.yml을 먼저 읽고, 이 파일로 덮어씀
               ▼
          Spring Environment (모든 설정값 저장소)
               │
          ③ 시작 클래스의 어노테이션 처리      IngestionCollectorApplication:7-8
               │  run()에 넘긴 클래스가 기준 → 스캔 범위 = 그 패키지(com.ingestion)와 하위 패키지
               │
               ├─ @SpringBootApplication                                       :7
               │    ├─ @ComponentScan            @Component · @Configuration 클래스 찾기
               │    │    → CollectorRunner, MarketDataIngestService, RawEventPublisher, AsyncConfig
               │    └─ @EnableAutoConfiguration  의존성(classpath)을 보고 기본 Bean 추가
               │         → ObjectMapper (spring-boot-starter-json)
               │
               └─ @ConfigurationPropertiesScan                                 :8
                    │  @ConfigurationProperties 클래스 찾기 → Environment 값 바인딩
                    ├─ binance.endpoint.*     → BinanceProperties.Endpoint       config/BinanceProperties:56   접속 도메인 (환경별)
                    └─ binance.subscription.* → BinanceProperties.Subscription   config/BinanceProperties:80   구독 설정 (pod별)
               ▼
          ④ Bean 생성 + 생성자 주입          CollectorRunner:47, MarketDataIngestService:39
               ▼
[앱]      ⑤ CollectorRunner.run()           runner/CollectorRunner:60          소켓마다 URL 조립 → connect
               ▼
          ⑥ BinanceStreamClient.onText()    collector/BinanceStreamClient:91   [WebSocket 콜백 스레드] 수신
               ▼
          ⑦ MarketDataIngestService.onRaw() service/MarketDataIngestService:52 [스레드풀] unwrap → envelope
               ▼
          ⑧ RawEventPublisher.publish()     kafka/RawEventPublisher:25         토픽 · 키로 발행 (지금은 로그만)
```

| 단계 | 하는 일 | 파일 | 쓰이는 설정 필드 |
| --- | --- | --- | --- |
| ① | ConfigMap을 `/config`에 마운트 | `deploy/<group>/*.yaml` | — |
| ② | 설정 파일 로드 | `deploy/Dockerfile`, `IngestionCollectorApplication.java` | — |
| ③ | Bean 후보 찾기 · yaml → record 바인딩 | `IngestionCollectorApplication.java`, `config/BinanceProperties.java` | 전부 |
| ④ | Bean 생성 · 주입 | `runner/CollectorRunner.java`, `service/MarketDataIngestService.java`, `config/AsyncConfig.java` | — |
| ⑤ | 소켓 생성 | `runner/CollectorRunner.java` | `product`, `sector`, `symbols`, `streams[].stream · route-prefix` |
| ⑥ | 메시지 수신 | `collector/BinanceStreamClient.java` | — |
| ⑦ | envelope 생성 | `service/MarketDataIngestService.java`, `model/EventEnvelope.java` | `group`, `product`, `asset-class`, `sector`, `streams[].stream` |
| ⑧ | 발행 | `kafka/RawEventPublisher.java` | `streams[].topic` |

로컬 실행(`./gradlew bootRun`)은 ①이 없고, `application.yml`의 `binance.subscription` 기본값(`spot-sov` pod와 같음)으로 ②부터 시작한다.

---

## ① ConfigMap 마운트 — `deploy/<group>/*.yaml`

```yaml
# ConfigMap
data:
  application.yaml: |          ← 키 이름 = 파일 이름
    binance:                   ← 값 = 파일 내용
      subscription:
        group: fixed-rate
      ...
---
# Deployment
      containers:
        - name: collector
          volumeMounts:
            - name: application-config    ← 이 볼륨을 컨테이너의 /config 에 붙임 (컨테이너 단위)
              mountPath: /config
              readOnly: true
      volumes:
        - name: application-config        ← ConfigMap을 "application-config" 볼륨으로 포장 (Pod 단위)
          configMap:
            name: markprice-usdm-optical-config
```

- ConfigMap의 `data` 키 하나가 `mountPath` 아래 파일 하나가 된다 → `/config/application.yaml`
- `volumes`와 `volumeMounts`는 `name`이 같아야 연결된다.
- ConfigMap을 수정하면 마운트된 파일은 보통 1분 안에 바뀌지만, Spring은 시작할 때 한 번만 읽으므로 `kubectl rollout restart`가 필요하다.

---

## ② 설정 로드 — Spring Environment

[`IngestionCollectorApplication.java:12`](../collector/src/main/java/com/ingestion/IngestionCollectorApplication.java#L12)

```java
SpringApplication.run(IngestionCollectorApplication.class, args);
```

Spring Boot가 시작되면서 **Environment**(모든 설정값을 모아 둔 저장소)를 만든다. 우리 코드가 아니라 Spring Boot 내부에서 일어난다.

1. jar 안의 `application.yml`(`classpath:/`)을 읽는다. → `binance.endpoint.*` 접속 도메인, `binance.subscription.*` 로컬 기본값
2. `spring.config.additional-location`이 가리키는 `/config/application.yaml`을 읽어 같은 키를 **덮어쓴다**. → ConfigMap의 `binance.subscription.*`
3. 환경변수(`SPRING_PROFILES_ACTIVE`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `COLLECTOR_ID` 등)도 이 저장소에 함께 들어간다.
4. 활성 프로파일의 `application-<프로파일>.yml`을 읽는다. 프로파일을 안 주면 `local`이다 (`spring.profiles.default`).

**Kafka 주소: 프로파일 + 환경변수**

| 프로파일 | 켜지는 곳 | 파일 | `spring.kafka.bootstrap-servers` 기본값 |
| --- | --- | --- | --- |
| `local` | 아무것도 안 줄 때 (`bootRun`, `java -jar`) | `application-local.yml` | `localhost:9092` |
| `release` | K8s — `common.yaml`의 `SPRING_PROFILES_ACTIVE: release` | `application-release.yml` | `binance-cluster-kafka-bootstrap.kafka.svc:9092` (클러스터 내부 DNS) |

주소를 바꾸려면 환경변수 `SPRING_KAFKA_BOOTSTRAP_SERVERS`를 준다 (로컬: 셸에서 지정, K8s: `common.yaml`에 추가). `spring.kafka.bootstrap-servers`는 Spring 표준 키라, relaxed binding으로 이 환경변수가 자동으로 같은 키가 되고 yml보다 우선한다.

이 값은 [`config/KafkaConfig.java`](../collector/src/main/java/com/ingestion/config/KafkaConfig.java)가 `Environment.getProperty("spring.kafka.bootstrap-servers", "")`로 읽어 시작할 때 `[kafka] bootstrap-servers=...` 로그로 남긴다. `spring-kafka`를 켜면 Spring Boot가 같은 키로 `KafkaTemplate`을 만든다.

**`SPRING_CONFIG_ADDITIONAL_LOCATION`을 Spring이 읽는 이유** ([`deploy/Dockerfile:37`](Dockerfile#L37))

- `spring.config.additional-location`은 Spring Boot 내장 속성이다. 코드에서 따로 읽지 않는다.
- Spring Boot는 모든 속성을 환경변수로도 받는다(relaxed binding): `.` → `_`, `-` 제거, 대문자.
  → `spring.config.additional-location` = `SPRING_CONFIG_ADDITIONAL_LOCATION`
- 같은 값을 실행 인자(`--spring.config.additional-location=...`)나 JVM 속성(`-D...`)으로 넣어도 된다.

| 설정 | 의미 |
| --- | --- |
| `additional-location` | 기본 위치에 **추가**로 읽고, 같은 키는 덮어씀. jar 안의 `binance.endpoint.*`는 유지된다 |
| (비교) `location` | 기본 위치를 **대체**. jar 안의 `application.yml`을 아예 읽지 않게 되므로 쓰지 않는다 |
| `optional:` | 파일이 없어도 에러 없이 넘어감. ConfigMap 없이 `docker run`해도 기본값으로 뜬다 |

Spring Boot의 기본 위치 중 `file:./config/`는 실행 위치 기준이라 `WORKDIR /app`에서는 `/app/config/`가 된다. ConfigMap을 `/config`(루트)에 붙이므로 이 ENV로 위치를 알려 준다. (`mountPath`를 `/app/config`로 바꾸면 ENV 없이도 읽힌다.)

---

## ③ 시작 클래스 처리 — Bean 후보 찾기와 바인딩

```java
@SpringBootApplication          // :7
@ConfigurationPropertiesScan    // :8
public class IngestionCollectorApplication { ... }
```

②의 `run()`에 넘긴 `IngestionCollectorApplication`이 설정의 출발점(primary source)이다. Spring은 이 클래스에 붙은 어노테이션을 읽고, 두 스캔 모두 basePackages를 따로 주지 않았으므로 **이 클래스의 패키지(`com.ingestion`)와 그 하위 패키지**를 훑는다. 그래서 시작 클래스는 최상위 패키지에 있어야 한다 — `com.ingestion.app`으로 옮기면 `com.ingestion.config`, `com.ingestion.runner`가 범위 밖이 되어 Bean이 등록되지 않는다.

이 단계에서는 "어떤 Bean을 만들지" 목록(Bean 정의)만 모은다. 실제 객체 생성은 ④에서 한다.

### `@SpringBootApplication` — 컴포넌트 스캔 + 자동 설정

[`IngestionCollectorApplication.java:7`](../collector/src/main/java/com/ingestion/IngestionCollectorApplication.java#L7)

`@Configuration` + `@EnableAutoConfiguration` + `@ComponentScan`을 합친 어노테이션이다.

| 포함된 어노테이션 | 하는 일 | 이 앱에서 등록되는 것 |
| --- | --- | --- |
| `@ComponentScan` | `@Component`, `@Configuration` 등이 붙은 클래스를 찾는다 | `CollectorRunner`, `MarketDataIngestService`, `RawEventPublisher`, `AsyncConfig`(→ `ingestExecutor`) |
| `@EnableAutoConfiguration` | classpath의 의존성을 보고 Spring Boot가 기본 Bean을 추가한다 | `ObjectMapper` (`spring-boot-starter-json`) |
| `@Configuration` | 이 클래스 자체를 설정 클래스로 만든다 | — |

`@ComponentScan`은 `@ConfigurationProperties`만 붙은 클래스는 찾지 않는다(`@Component`가 없으므로). 그래서 아래 `@ConfigurationPropertiesScan`을 따로 붙인다.

### `@ConfigurationPropertiesScan` — yaml 키 → Java record

[`IngestionCollectorApplication.java:8`](../collector/src/main/java/com/ingestion/IngestionCollectorApplication.java#L8)의 `@ConfigurationPropertiesScan`이 `com.ingestion` 아래의 `@ConfigurationProperties` 클래스를 찾아 Bean으로 등록하고, Environment의 값을 넣는다. (스캔 대신 `@EnableConfigurationProperties({BinanceProperties.Endpoint.class, BinanceProperties.Subscription.class})`로 직접 나열해도 된다.)

두 설정은 `binance.*` 아래, 한 파일(`BinanceProperties.java`)에 모여 있지만 **값이 무엇에 따라 바뀌는지**로 nested record를 나누고, 각 record에 `@ConfigurationProperties`를 따로 붙인다. 그래서 Bean도 둘이고, 구독 설정만 필요한 `MarketDataIngestService`는 `Subscription`만 주입받는다. `@ConfigurationPropertiesScan`은 static nested 클래스도 찾는다. 바깥 `BinanceProperties`는 묶음용이라 Bean이 아니다.

| prefix | 클래스 | 무엇에 따라 바뀌나 | 어디서 넣나 | 위치 |
| --- | --- | --- | --- | --- |
| `binance.endpoint.*` | `BinanceProperties.Endpoint` | 환경 (테스트넷/프로덕션). 모든 Pod가 같음 | jar 안의 `application.yml` | [`config/BinanceProperties.java:56`](../collector/src/main/java/com/ingestion/config/BinanceProperties.java#L56) |
| `binance.subscription.*` | `BinanceProperties.Subscription` | Pod | ConfigMap | [`config/BinanceProperties.java:80`](../collector/src/main/java/com/ingestion/config/BinanceProperties.java#L80) |

ConfigMap에는 `binance.subscription`만 있으므로, `binance.endpoint`는 jar의 기본값이 그대로 남는다 (②의 `additional-location`은 같은 키만 덮어씀).

```yaml
binance:
  subscription:
    group: fixed-rate
    product: usdm
    asset-class: stock
    sector: optical
    symbols: [LITEUSDT, CRDOUSDT, ALABUSDT]
    streams:
      - stream: markPrice@1s
        route-prefix: "/market"
        topic: binance.raw.usdm.stock.markprice
```

```java
@ConfigurationProperties(prefix = "binance.subscription")
public record Subscription(            // BinanceProperties 안의 nested record
        String group,                    // binance.subscription.group
        String product,                  // binance.subscription.product
        String assetClass,               // binance.subscription.asset-class
        String sector,                   // binance.subscription.sector
        List<String> symbols,            // binance.subscription.symbols
        List<StreamSpec> streams) {      // binance.subscription.streams

    public record StreamSpec(String stream, String routePrefix, String topic) {}   // 항목 하나 = 소켓 하나
}
```

- **kebab-case → camelCase:** `asset-class` → `assetClass`, `usdm-futures` → `usdmFutures` (relaxed binding). Java 필드 이름이 맞지 않으면 null이 들어와 ⑤에서 바로 실패한다.
- **시작 시점 검증:** 두 record에 `@Validated`와 `@NotBlank` · `@NotNull` · `@NotEmpty`를 붙여, 값이 비어 있으면 바인딩 직후 `APPLICATION FAILED TO START`로 멈춘다. 에러에는 키와 줄 번호가 나온다 (예: `binance.subscription.sector` / `must not be blank`). 중첩 record와 `streams` 항목은 `@Valid`가 있어야 안쪽까지 검사된다.
    - `sector`도 필수다. pod 이름, pod label, 로그 라벨이 모두 섹터를 쓰기 때문이다. Spring은 yaml의 `sector: null`을 빈 문자열로 바인딩하므로 `@NotBlank`에 걸린다.
    - `route-prefix`는 `@NotNull`이다. Spot은 접두어가 없어 `""`가 정상값이다.
    - `restapiUrl`은 아직 쓰지 않아 검사하지 않는다.
- `streams`의 각 항목은 [`StreamSpec`](../collector/src/main/java/com/ingestion/config/BinanceProperties.java#L99)이 된다.

---

## ④ Bean 생성과 주입

Spring이 나머지 Bean을 만들면서 ③의 설정 객체를 생성자로 넣어 준다.

| Bean | 받는 것 | 위치 |
| --- | --- | --- |
| `CollectorRunner` | `MarketDataIngestService`, `BinanceProperties.Endpoint`, `BinanceProperties.Subscription` | [`runner/CollectorRunner.java:47`](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L47) |
| `MarketDataIngestService` | `RawEventPublisher`, `ObjectMapper`, `BinanceProperties.Subscription`, `collectorId` | [`service/MarketDataIngestService.java:39`](../collector/src/main/java/com/ingestion/service/MarketDataIngestService.java#L39) |
| `ingestExecutor` 스레드풀 | 코어 2개, 최대 4개 | [`config/AsyncConfig.java:19`](../collector/src/main/java/com/ingestion/config/AsyncConfig.java#L19) |

`collectorId`는 `@Value("${COLLECTOR_ID:${HOSTNAME:local}}")`로 받는다. Deployment가 `COLLECTOR_ID`에 pod 이름을 넣고, 없으면 `HOSTNAME`, 로컬 실행이면 `local`이 된다.

---

## ⑤ 소켓 생성 — `CollectorRunner.run()`

`ApplicationRunner`를 구현했기 때문에([`CollectorRunner.java:38`](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L38)), Spring이 모든 Bean을 만든 뒤 [`run()`](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L60)을 자동으로 호출한다.

| 줄 | 하는 일 | 쓰는 설정 |
| --- | --- | --- |
| [61~66](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L61-L66) | 접속 도메인 고르기 (`product` → `switch`) | `product` |
| [69](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L69) | 항목마다 반복 (항목 하나 = 소켓 하나) | `streams` |
| [74](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L74) | URL 조립: `baseUrl + route-prefix + `[`streamSuffix()`](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L105) | `route-prefix`, `symbols`, `stream` |
| [83~87](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L83-L87) | `BinanceStreamClient` 생성, 로그 라벨([`streamLabel()`](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L116)), 메시지 콜백 등록 | `product`, `sector` |
| [90](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L90) | `connect()` | — |
| [94](../collector/src/main/java/com/ingestion/runner/CollectorRunner.java#L94) | `CountDownLatch().await()`로 메인 스레드를 붙잡아 앱이 종료되지 않게 함 | — |

### `product` → 접속 도메인

키 이름이 다르므로(`usdm` ↔ `usdm-futures`) `run()` 첫 부분의 `switch`에서 변환한다.

| `product` | 사용하는 설정 | 실제 값 |
| --- | --- | --- |
| `spot` | `binance.endpoint.spot.wss-url` | `wss://stream.binance.com:9443` |
| `usdm` | `binance.endpoint.usdm-futures.wss-url` | `wss://fstream.binance.com` |

### `symbols` → 단일 / combined 형식

| 심볼 수 | 형식 | 예 |
| --- | --- | --- |
| 1개 | `/ws/<symbol>@<stream>` (단일) | `/ws/btcusdt@bookTicker` |
| 여러 개 | `/stream?streams=<a>@<stream>/<b>@<stream>/...` (combined) | `/stream?streams=liteusdt@markPrice@1s/crdousdt@markPrice@1s/alabusdt@markPrice@1s` |

Binance 스트림 이름은 소문자 심볼을 쓰므로 `streamSuffix()`에서 소문자로 바꾼다.

### `route-prefix` → 라우팅 접두어

`baseUrl`과 `/ws`, `/stream` 사이에 들어간다. USD-M에서 꼭 맞아야 한다 — **틀려도 연결은 되고 메시지만 오지 않는다.**

| product | stream | route-prefix |
| --- | --- | --- |
| spot | 전부 | `""` (접두어 없음) |
| usdm | `bookTicker`, `depth@100ms` | `/public` |
| usdm | `aggTrade`, `markPrice@1s` | `/market` |

### `sector` → 로그 라벨

로그 라벨은 `product-섹터-stream` 형식으로, pod 이름과 같은 섹터를 쓴다 (`sector`는 ③에서 필수로 검증된다).
→ `usdm-optical-markPrice@1s`, `spot-sov-bookTicker`

---

## ⑥ 메시지 수신 — `BinanceStreamClient`

| 메서드 | 위치 | 하는 일 |
| --- | --- | --- |
| `connect()` | [`collector/BinanceStreamClient.java:56`](../collector/src/main/java/com/ingestion/collector/BinanceStreamClient.java#L56) | `buildAsync()`로 논블로킹 연결 |
| `onText()` | [`BinanceStreamClient.java:91`](../collector/src/main/java/com/ingestion/collector/BinanceStreamClient.java#L91) | 조각난 프레임을 모아 메시지 하나로 만든 뒤 [99줄](../collector/src/main/java/com/ingestion/collector/BinanceStreamClient.java#L99)에서 ⑤의 콜백 호출 |
| `scheduleReconnect()` | [`BinanceStreamClient.java:133`](../collector/src/main/java/com/ingestion/collector/BinanceStreamClient.java#L133) | 끊기면 5초 뒤 재연결 |

여기까지는 **WebSocket 콜백 스레드**에서 실행된다. 수신 시각(`receive_time`)도 이 스레드에서 찍어서 넘긴다 — ⑦의 스레드풀 대기 시간이 섞이지 않게 하기 위해서다.

---

## ⑦ envelope 생성 — `MarketDataIngestService.onRaw()`

[`service/MarketDataIngestService.java:51-52`](../collector/src/main/java/com/ingestion/service/MarketDataIngestService.java#L51-L52)

`@Async("ingestExecutor")`가 붙어 있어서, ⑥에서 호출하면 바로 반환되고 실제 처리는 **스레드풀**에서 이어진다.

| 줄 | 하는 일 |
| --- | --- |
| [54](../collector/src/main/java/com/ingestion/service/MarketDataIngestService.java#L54) | JSON 파싱 후 [`unwrap()`](../collector/src/main/java/com/ingestion/service/MarketDataIngestService.java#L68)으로 combined 메시지의 `data` 꺼내기 (`{"stream": ..., "data": {...}}`) |
| [55](../collector/src/main/java/com/ingestion/service/MarketDataIngestService.java#L55) | [`toEnvelope()`](../collector/src/main/java/com/ingestion/service/MarketDataIngestService.java#L74)로 `EventEnvelope` 생성 |
| [57~58](../collector/src/main/java/com/ingestion/service/MarketDataIngestService.java#L57-L58) | 수신 로그. 첫 칸에 `group` (`[fixed-rate] markPrice LITEUSDT event_time=...`) |
| [59](../collector/src/main/java/com/ingestion/service/MarketDataIngestService.java#L59) | `eventPublisher.publish(spec.topic(), envelope)` |

payload는 해석하지 않는다. 모든 스트림에 공통인 `s`(심볼)와 `E`(이벤트 시각)만 읽으므로, 스트림 종류가 늘어도 이 코드는 바뀌지 않는다.

### envelope 필드의 출처

```json
{
  "source": "binance",
  "product": "usdm",
  "asset_class": "stock",
  "sector": "optical",
  "stream": "markPrice",
  "symbol": "LITEUSDT",
  "event_time": 1729400000123,
  "receive_time": 1729400000130,
  "collector_id": "markprice-usdm-optical-9e1f3a5b7-q1r2s",
  "payload": { "...": "data 원본 그대로" }
}
```

| envelope 필드 | 출처 |
| --- | --- |
| `product`, `asset_class`, `sector` | 설정 `binance.subscription.*`. 토픽은 자산 종류까지만 나뉘고 섹터는 토픽을 나누지 않으므로([topic/plan.md](../../plan/topic/plan.md) 고려 사항 2), 섹터는 envelope에만 남는다 |
| `stream` | 설정 `streams[].stream`에서 `@` 뒤를 뗀 값 (`depth@100ms` → `depth`) |
| `symbol`, `event_time` | 메시지 `data.s`, `data.E`. **Spot bookTicker에는 `E`가 없어 `null`** |
| `receive_time` | ⑥에서 찍은 수신 시각 |
| `collector_id` | ④의 `collectorId` (pod 이름) |

### `group`은 로그 라벨로만 쓴다

이 앱은 raw만 보내므로 `variable-rate`와 `fixed-rate`의 동작이 같다.

| 값 | Deployment에서의 의미 | 앱에서 하는 일 |
| --- | --- | --- |
| `variable-rate` | Active-Active (replicas 2) → raw 토픽에 같은 이벤트가 두 번 들어감 | 로그 라벨 |
| `fixed-rate` | Active-Standby (replicas 1) → 중복 없음 | 로그 라벨 |

중복 제거는 raw 토픽을 읽는 Flink 앱이 한다 (`binance.raw.*` → `binance.clean.*`). K8s의 `rate-group` label(`variable` / `fixed`)은 Deployment에서 따로 붙인 값이라 이 설정과 별개다.

---

## ⑧ 발행 — `RawEventPublisher.publish()`

[`kafka/RawEventPublisher.java:25`](../collector/src/main/java/com/ingestion/kafka/RawEventPublisher.java#L25)

| 항목 | 출처 |
| --- | --- |
| 토픽 | 설정 `streams[].topic` |
| 키 | envelope의 `symbol` (메시지 `data.s`). 섹터 소켓 하나로 여러 심볼이 섞여 오므로 설정이 아니라 메시지에서 정한다 |

지금은 [26줄](../collector/src/main/java/com/ingestion/kafka/RawEventPublisher.java#L26)에서 `[KAFKA:SKIPPED] topic=… key=…` 로그만 남긴다. 키를 심볼로 두는 이유는 같은 심볼의 이벤트가 같은 파티션에 순서대로 쌓여야 다운스트림 Flink의 dedup · 조인이 파티션 안에서 끝나기 때문이다.

---

## 실행 예시: `markprice-usdm-optical`

```text
①  /config/application.yaml  ← markprice-usdm-optical-config
③  group=fixed-rate  product=usdm  asset-class=stock  sector=optical
    symbols=[LITEUSDT, CRDOUSDT, ALABUSDT]
    streams=[{markPrice@1s, /market, binance.raw.usdm.stock.markprice}]
⑤  소켓 1개, 라벨 usdm-optical-markPrice@1s
    wss://fstream.binance.com/market/stream?streams=liteusdt@markPrice@1s/crdousdt@markPrice@1s/alabusdt@markPrice@1s
⑥  수신: {"stream": "liteusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": 1729400000123, "s": "LITEUSDT", ...}}
⑦  data 꺼내기 → envelope(product=usdm, asset_class=stock, sector=optical, stream=markPrice, symbol=LITEUSDT, ...)
    로그: [fixed-rate] markPrice LITEUSDT event_time=1729400000123
⑧  토픽 binance.raw.usdm.stock.markprice, 키 LITEUSDT
```

### 비교: variable-rate `spot-sov`

| 단계 | 값 | 결과 |
| --- | --- | --- |
| ⑤ product | `spot` | `wss://stream.binance.com:9443`, route-prefix `""` |
| ⑤ symbols | `[BTCUSDT]` (1개) | 단일 형식 `/ws/...` |
| ⑤ streams | 3개 | 소켓 3개 |
| ⑤⑦ sector | `sov` | 로그 라벨 `spot-sov-...`, envelope `sector: sov` ([cryptoSectorREADME.md](../../docs/symbols/cryptoSectorREADME.md)) |
| ⑦ group | `variable-rate` | 로그 라벨 `[variable-rate]` |

```text
socket 1  wss://stream.binance.com:9443/ws/btcusdt@bookTicker   → binance.raw.spot.crypto.bookticker
socket 2  wss://stream.binance.com:9443/ws/btcusdt@trade        → binance.raw.spot.crypto.trade
socket 3  wss://stream.binance.com:9443/ws/btcusdt@depth@100ms  → binance.raw.spot.crypto.depth
```

---

## 남은 작업

| # | 할 일 | 단계 | 파일 |
| --- | --- | --- | --- |
| 1 | `spring-kafka` 의존성을 켜고 `KafkaTemplate`으로 실제 발행 | ⑧ | `build.gradle`, `kafka/RawEventPublisher.java` |
| 2 | `@Async` 스레드풀(2~4개)이 같은 심볼의 메시지 순서를 섞을 수 있음. 콜백 스레드에서 바로 발행하거나 소켓별 단일 스레드로 변경 | ⑥ → ⑦ | `service/MarketDataIngestService.java`, `config/AsyncConfig.java` |
| 3 | `route-prefix`, `topic`은 Binance 규칙과 토픽 이름 규칙에서 정해지는 값 → ConfigMap에서 빼고 코드가 계산하도록 단순화 검토 | ⑤, ⑧ | `deploy/<group>/*.yaml` |
