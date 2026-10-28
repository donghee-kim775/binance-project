# Spring Boot 설정 동작 정리

수집 앱(`datasource-ingestion/collector`)이 설정 값을 **어디서 읽고, 어떤 순서로 고르고, 코드에서 어떻게 받는지** 정리한다. Spring Boot 4.1.1 기준이고, 예시는 모두 이 프로젝트의 실제 파일이다. 설정 키가 코드의 어느 줄로 가는지는 [mapping.md](../../datasource-ingestion/deploy/mapping.md)를 본다.

---

## 1. 한눈에 보기

```text
설정 출처 (전부 시작할 때 한 번 올려 둔다)
  실행 인자 · JVM 속성 · 환경변수 · 설정 파일(yml)
        │
        ▼
Spring Environment  ── 키를 찾을 때: 위 출처를 정해진 순서로 훑고, 처음 찾은 값을 쓴다
        │
        ├─ @ConfigurationProperties → BinanceProperties.Endpoint / Subscription (묶음)
        ├─ @Value                   → collectorId (COLLECTOR_ID)
        ├─ Environment.getProperty() → KafkaConfig (spring.kafka.bootstrap-servers)
        └─ Spring 내부              → spring.profiles.active, logging.level.*, spring.config.* …
```

- 우리 코드는 `System.getenv()`나 파일을 직접 읽지 않는다. 전부 Spring Environment를 거친다.
- 그래서 같은 키를 yml로 주든 환경변수로 주든 실행 인자로 주든, 코드는 바뀌지 않는다.

---

## 2. 설정 출처와 탐색 순서

Spring은 설정 출처(PropertySource)를 줄 세워 두고, **키 하나를 찾을 때 위에서부터 내려가다 처음 찾은 값을 쓴다.** 먼저 찾는 쪽이 이긴다.

| 순서 | 출처 | 예 |
| --- | --- | --- |
| 1 | 실행 인자 | `java -jar app.jar --spring.profiles.active=release` |
| 2 | JVM 시스템 속성 | `java -Dspring.profiles.active=release -jar app.jar` |
| 3 | 환경변수 | `SPRING_PROFILES_ACTIVE=release` (K8s ConfigMap `envFrom`, Deployment `env`) |
| 4 | 설정 파일 (아래 순서) | |
| 4-1 | jar 밖의 프로파일 파일 | `/config/application-release.yaml` (지금은 없음) |
| 4-2 | jar 안의 프로파일 파일 | `classpath:/application-release.yml` |
| 4-3 | jar 밖의 기본 파일 | `/config/application.yaml` (Pod별 ConfigMap 마운트) |
| 4-4 | jar 안의 기본 파일 | `classpath:/application.yml` |

설정 파일끼리의 규칙은 두 가지다.

- **프로파일 파일이 기본 파일보다 먼저다.** `application-release.yml`이 `application.yml`을 이긴다.
- **jar 밖이 jar 안보다 먼저다.** `/config/`(ConfigMap)가 `classpath:/`(jar)를 이긴다.

### 키마다 따로 찾는다

출처는 전부 올라가 있고, **탐색은 키 단위**다. 한 키는 처음 찾은 곳에서 멈추고, 다른 키는 처음부터 다시 찾는다. 그래서 최종 값은 키마다 다른 출처에서 올 수 있다.

```text
K8s Pod (usdm-sov) 에서

spring.profiles.active          ③ 환경변수 SPRING_PROFILES_ACTIVE=release        ✔ 멈춤
spring.kafka.bootstrap-servers  ③ 없음 → 4-2 application-release.yml             ✔ 멈춤
binance.subscription.sector     ③ 없음 → 4-3 /config/application.yaml (ConfigMap) ✔ 멈춤 → sov
binance.endpoint.spot.wss-url   ③ 없음 → 4-3 없음 → 4-4 application.yml          ✔ 멈춤
COLLECTOR_ID                    ③ 환경변수 (Pod 이름)                              ✔ 멈춤
```

"덮어쓴다"는 말도 파일 단위가 아니라 **키 단위**다. ConfigMap에는 `binance.subscription`만 있으므로 `binance.endpoint`는 jar의 값이 그대로 남는다. 파일 내용이 실제로 바뀌는 것도 아니다. 메모리 안의 Environment에서 우선순위만 정한다.

### 리스트는 통째로 바뀐다

일반 키는 하나씩 덮어쓰지만, 리스트(`symbols`, `streams`)는 **합쳐지지 않고 통째로 교체**된다. jar 기본값의 `streams`가 3개, ConfigMap이 1개면 결과는 1개다.

---

## 3. 프로파일

프로파일은 "실행 환경 이름"이다. 이 프로젝트는 `local`(내 PC)과 `release`(K8s) 두 개를 쓴다.

### `active`와 `default`

| 설정 | 의미 |
| --- | --- |
| `spring.profiles.active` | 지금 켤 프로파일. 실행 인자 · 환경변수로 준다 |
| `spring.profiles.default` | `active`가 **하나도 없을 때만** 쓰는 대체값 |

`application.yml`에 `spring.profiles.default: local`이 있어서, 아무것도 주지 않으면 `local`이 된다. 이 줄이 없으면 Spring 내장 이름 `default`가 쓰이고, `application-default.yml`을 찾다가 없어서 Kafka 주소가 비게 된다.

`application.yml`이 default 프로파일인 것은 **아니다.** `application.yml`은 프로파일과 상관없이 항상 읽는 기본 파일이다.

### 프로파일 파일 이름 규칙 (Spring Boot 공식 규칙)

활성 프로파일이 정해지면 Spring은 **파일 이름**으로 프로파일 파일을 찾는다.

```text
application  +  "-" + 프로파일  +  "." + 확장자
application  +  -local          +  .yml        →  application-local.yml
```

- 코드 어디에도 "local이면 이 파일" 같은 연결은 없다. 이름 규칙뿐이다.
- 구현: `StandardConfigDataReference` (`root + "-" + profile + "." + extension`), 기본 이름 `application`은 `StandardConfigDataLocationResolver.DEFAULT_CONFIG_NAMES`
- `.properties`, `.yml`, `.yaml` 모두 찾고, 같은 위치에 둘 다 있으면 `.properties`가 우선이다.
- 프로파일을 여러 개 켜면(`release,debug`) 해당 파일을 모두 읽고, 뒤에 쓴 프로파일이 이긴다.

### 프로파일이 정해지는 순서

`spring.profiles.active`는 **프로파일 파일을 읽기 전에** 정해져야 한다. 어떤 파일을 읽을지 정하는 값이기 때문이다.

```text
1. 실행 인자 · JVM 속성 · 환경변수 + 기본 파일(application.yml, /config/application.yaml)을 읽는다
2. 그 안에서 spring.profiles.active를 찾는다 → 없으면 spring.profiles.default
3. 정해진 프로파일의 파일(application-<프로파일>.yml)을 추가로 읽는다
4. 이후 모든 키는 2장의 전체 순서로 찾는다
```

그래서 `application-release.yml` 안에 `spring.profiles.active`를 쓰면 Spring이 에러를 낸다.

### 프로파일을 켜는 명령어

| 명령어 | 활성 프로파일 |
| --- | --- |
| `./gradlew bootRun`, `java -jar app.jar` | 없음 → `local` |
| `./gradlew bootRun --args='--spring.profiles.active=release'` | `release` |
| `SPRING_PROFILES_ACTIVE=release ./gradlew bootRun` | `release` |
| `docker run -e SPRING_PROFILES_ACTIVE=release ingestion-collector:0.0.1` | `release` |
| K8s 배포 (`deploy/common.yaml`의 `SPRING_PROFILES_ACTIVE: release`) | `release` |

K8s에서 실행 인자 대신 환경변수를 쓰는 이유: Dockerfile의 `ENTRYPOINT`를 건드리지 않고 ConfigMap만으로 바꿀 수 있다. 같은 이미지가 환경변수에 따라 local로도 release로도 뜬다.

---

## 4. 환경변수 이름 규칙 (relaxed binding)

환경변수 이름에는 `.`을 쓸 수 없다. Spring은 아래 규칙으로 같은 키로 취급한다.

```text
spring.profiles.active            →  SPRING_PROFILES_ACTIVE
spring.kafka.bootstrap-servers    →  SPRING_KAFKA_BOOTSTRAP_SERVERS
logging.level.com.ingestion       →  LOGGING_LEVEL_COM_INGESTION
binance.subscription.sector       →  BINANCE_SUBSCRIPTION_SECTOR
binance.subscription.streams[0].route-prefix  →  BINANCE_SUBSCRIPTION_STREAMS_0_ROUTEPREFIX

규칙: . → _ , - 는 지움, 대문자, 리스트 인덱스 [0] → _0_
```

- 그래서 Spring 표준 키(`spring.*`, `logging.*`)는 yml에 값만 적어 두고, 바꿀 때 환경변수로 덮어쓰면 된다. `${MY_VAR:기본값}` 같은 placeholder를 따로 만들 필요가 없다.
- yml 쪽 키도 같은 규칙으로 맞춘다. `asset-class` ↔ Java 필드 `assetClass`, `usdm-futures` ↔ `usdmFutures`

환경변수가 Environment에 들어가는 곳: `StandardEnvironment.customizePropertySources()`가 `System.getenv()` 전체를 `systemEnvironment` PropertySource로 등록한다 (spring-core).

---

## 5. 코드에서 값을 받는 방법

이 프로젝트는 세 가지를 쓴다. 모두 Spring Environment에서 꺼내므로 2장의 탐색 순서가 그대로 적용된다.

### `@ConfigurationProperties` — 묶음을 객체로

```java
// config/BinanceProperties.java
@Validated
@ConfigurationProperties(prefix = "binance.subscription")
public record Subscription(@NotBlank String product, @NotBlank String sector, List<String> symbols, ...) { }
```

- prefix 아래 키를 **생성자 파라미터 이름**으로 찾아 record를 만든다 (`binance.subscription.sector` → `sector`).
- `@ConfigurationPropertiesScan`(`IngestionCollectorApplication`)이 이 클래스를 찾아 Bean으로 등록한다.
- `@Validated` + `@NotBlank` 등을 붙이면 값이 비었을 때 시작 단계에서 `APPLICATION FAILED TO START`로 멈추고, 어느 키인지 알려 준다.
- 키가 틀리면 에러 없이 `null`이 들어간다. 검증을 붙이는 이유다.

### `@Value` — 값 하나를 바로

```java
// service/MarketDataIngestService.java
@Value("${COLLECTOR_ID:${HOSTNAME:local}}") String collectorId
//        ① 있으면 이것   ② 없으면 이것   ③ 그것도 없으면 "local"
```

`${이름:기본값}`은 placeholder다. `이름`을 Environment에서 찾고, 없으면 `:` 뒤의 값을 쓴다.

### `Environment.getProperty()` — 코드로 직접 꺼내기

```java
// config/KafkaConfig.java
public KafkaConfig(Environment environment) {
    this.bootstrapServers = environment.getProperty("spring.kafka.bootstrap-servers", "");   // 없으면 빈 문자열
}
```

`Environment`를 생성자로 주입받아 키 이름으로 조회한다. `@Value`와 같은 저장소를 같은 순서로 찾으므로 결과도 같다. 어노테이션 대신 코드로 꺼내므로, 어떤 키를 어디서 읽는지가 코드에 그대로 보인다.

### Spring이 대신 읽는 값

`spring.profiles.active`, `spring.config.additional-location`, `logging.level.*`처럼 Spring 표준 키는 Spring Boot 안에 이미 읽는 코드가 있다. 우리는 값만 주면 된다. `spring.kafka.*`도 Spring Boot의 `KafkaProperties`가 같은 방식으로 읽어 `KafkaTemplate`을 만든다.

### 쓰지 않는 방법

```java
System.getenv("COLLECTOR_ID");   // OS 환경변수만 봄, yml · 실행 인자를 못 봄 — 안 씀
```

---

## 6. 이 프로젝트의 설정 지도

### 파일

| 파일 | 위치 | 언제 읽나 | 내용 |
| --- | --- | --- | --- |
| `application.yml` | jar 안 | 항상 | `spring.profiles.default: local`, `binance.endpoint.*`, `binance.subscription.*`(로컬 기본값) |
| `application-local.yml` | jar 안 | 프로파일 `local` | `spring.kafka.bootstrap-servers: localhost:9092` |
| `application-release.yml` | jar 안 | 프로파일 `release` | `spring.kafka.bootstrap-servers: binance-cluster-kafka-bootstrap.kafka.svc:9092` |
| `/config/application.yaml` | Pod (ConfigMap 마운트) | K8s, 항상 | `binance.subscription.*` (Pod별 구독) |

`/config/` 위치는 Dockerfile의 `SPRING_CONFIG_ADDITIONAL_LOCATION=optional:file:/config/`가 알려 준다. `additional-location`은 기본 위치에 **추가**로 읽는다는 뜻이라 jar 안의 `application.yml`도 계속 읽힌다.

### K8s Pod에 설정이 들어가는 방법

| 무엇 | 출처 | 들어가는 방법 |
| --- | --- | --- |
| yml 파일들 | 이미지(jar) | 빌드 때 `src/main/resources` → `BOOT-INF/classes` |
| `SPRING_PROFILES_ACTIVE`, `LOGGING_LEVEL_COM_INGESTION` | `deploy/common.yaml` (모든 Pod 공통) | `envFrom` → 환경변수 |
| `COLLECTOR_ID` | Deployment `fieldRef` (Pod 이름) | `env` → 환경변수 |
| `binance.subscription.*` | `deploy/*/<pod>.yaml`의 ConfigMap | `volumes` + `volumeMounts` → 파일 `/config/application.yaml` |

평평한 스위치 몇 개는 환경변수로, 리스트가 들어 있는 구조적인 설정은 yaml 파일로 마운트한다. `binance.subscription.streams`를 환경변수로 넣으면 `BINANCE_SUBSCRIPTION_STREAMS_0_STREAM=...`처럼 읽기 어려워지기 때문이다.

### 값을 바꾸고 싶을 때

| 바꿀 것 | 로컬 | K8s |
| --- | --- | --- |
| Kafka 주소 | `SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:29092 ./gradlew bootRun` | `common.yaml`에 `SPRING_KAFKA_BOOTSTRAP_SERVERS` 추가 → `kubectl rollout restart` |
| 구독 심볼 · 스트림 | `--spring.config.additional-location=<파일>` | Pod ConfigMap 수정 → `kubectl rollout restart` |
| 접속 도메인 | `application.yml` 수정 | `application.yml` 수정 후 이미지 재빌드 |

ConfigMap을 고쳐도 Spring은 시작할 때 한 번만 읽으므로 재시작해야 반영된다.

---

## 7. 직접 확인하는 방법

- **최종 값 보기:** 로그 앞에 찍히는 값에 원하는 키를 넣어서 띄운다.
  ```bash
  java -jar app.jar '--binance.subscription.group=K=${spring.kafka.bootstrap-servers}'
  # 로그: [K=localhost:9092] bookTicker BTCUSDT ...
  ```
- **활성 프로파일:** 시작 로그의 `No active profile set, falling back to 1 default profile: "local"` 또는 `The following 1 profile is active: "release"`
- **어떤 파일을 읽었나:** `--logging.level.org.springframework.boot.context.config=TRACE`
- **설정 로딩 코드 따라가기:** `SpringApplication.run()` → `prepareEnvironment()` → `ConfigDataEnvironment.processAndApply()`에 브레이크포인트

---

## 참고

- [Spring Boot Reference — Externalized Configuration](https://docs.spring.io/spring-boot/reference/features/external-config.html) (출처 순서, 프로파일 파일, relaxed binding)
- [Spring Boot Reference — Profiles](https://docs.spring.io/spring-boot/reference/features/profiles.html)
