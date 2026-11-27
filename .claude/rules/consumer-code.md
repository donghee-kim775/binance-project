# 처리 앱(consumer/) 코드 규칙

`consumer/` 아래 Flink job과 Kafka Streams 앱을 새로 쓰거나 고칠 때 따른다. 기준이 되는 코드는 `consumer/raw_clean/kstreams-dedup`, `consumer/raw_clean/flink-dedup`, `consumer/clean_analytics/flink-join`이다. 흐름 태그 주석(`[호출]` · `[다음]` · `[스레드]`)과 문서 규칙은 CLAUDE.md를 따른다.

## 1. 프로젝트 · 패키지 구조

- 폴더: `consumer/<source>_<sink>/<flink|kstreams>-<기능>/`. gradle 프로젝트 하나 = 이미지 하나.
  - 단계 폴더는 **입력 토픽 계층 → 출력 토픽 계층**으로 이름 짓는다. 입력이 여러 계층이면 주 입력을 쓴다 (basis: clean Spot + raw markPrice → `clean_analytics`).

    | 단계 폴더 | 하는 일 | 앱 |
    | --- | --- | --- |
    | `raw_clean/` | raw → clean (중복 제거) | `flink-dedup`, `kstreams-dedup` |
    | `clean_analytics/` | clean(+raw) → analytics (join · 계산) | `flink-join`, `kstreams-join` |
    | `sink/<인사이트>/` | analytics → Kafka 밖 (Redis · ClickHouse · 알림). at-least-once + 멱등 키 | `insight_1/` (예정) |

  - 단계에 속하지 않는 것(`flink-design.md`)은 `consumer/` 바로 아래에 둔다.
- main 클래스 이름: **Flink는 `<Name>Job`**(클러스터에 제출하는 job), **Kafka Streams는 `<Name>App`**(스스로 도는 프로세스).
- 패키지 `com.consumer.<기능>` 아래:

  | 패키지 | 들어가는 것 |
  | --- | --- |
  | (루트) | Kafka Streams의 `<Name>App` (main) |
  | `config/` | Kafka Streams 설정 `KafkaConfig.create()` |
  | `common/` | 여러 job이 같이 쓰는 것: `BinanceEventEnvelope`, Kafka source · sink(Flink `KafkaIO`), Serde |
  | `<job>/` | job 하나의 코드: Topology · Processor · Function · Parser (Flink는 `<Name>Job`도 여기) |
  | `<job>/model/` | 값 묶음 record |

- **같은 일을 하는 Flink 버전과 Kafka Streams 버전은 이름을 맞춘다.** 클래스(`SpotMidParser`), record(`SpotMid`), 변수(`eventId`, `latestSpot`), 로그 접두어(`[basis]`), 출력 JSON 필드가 같아야 비교와 설명이 쉽다.
- 이미 배포된 Flink 연산자의 `uid`는 바꾸지 않는다 (savepoint에서 state · offset을 못 찾는다).

## 2. 코드 스타일 (Python처럼 읽히게)

- **값 묶음은 record.** 필드는 한 줄에 하나, 오른쪽에 예시 값 주석 (`String symbol, // BTCUSDT`).
- **상태 없는 빌더 · 유틸은 `final class` + private 생성자 + static 메서드** (`DedupTopology.build`, `KafkaConfig.create`). 클래스 주석에 객체로 만들지 않는 이유를 적지 않아도 된다.
- **체인보다 단계별 변수.** `spotRaw` → `spot` → `spotBySymbol`처럼 단계마다 이름을 붙인다.
- **단계 주석에 번호를 붙인다.** `// 1. Store 등록 (…)`, 하위 설명은 `//  - …`. main · build 메서드는 위에서 아래로 번호만 따라 읽으면 흐름이 보여야 한다.
- **라이브러리 호출의 인자가 여러 개면 한 줄에 하나씩, 오른쪽에 뜻을 적는다** (`Stores.inMemoryWindowStore(...)`, `Consumed.with(...)`, record 생성).
- **이름:**
  - camelCase. `arriveTs`, `matchesCursor` (소문자로 붙여 쓰지 않는다).
  - 한 글자 · 약어 금지: `p` → `kafkaProperties`, `it` → `matchesCursor`, `e` → `error` (람다 · catch 인자 포함).
  - 무엇인지 드러나게: store는 `…Store`, store 빌더는 `…StoreBuilder`, 테스트 드라이버는 `testDriver`.
- **자원 닫기는 `try { … } finally { x.close(); }`로 명시한다.** (try-with-resources보다 읽기 쉽다는 판단)
- **깨진 입력은 예외를 던지지 않고 버린다.** 던지면 재시작 후 같은 레코드에서 또 실패한다 (poison pill).
  - 검사는 `validate(...)` 메서드로 뺀다. `boolean`을 돌려주고, 실패 로그도 그 안에서 남긴다.
  - 로그 형식: `log.warn("[<job>] skip <대상>: <이유> — {}", truncate(원문))`. 원문은 200자로 자른다.
- **람다는 짧은 생성 · 콜백만.** 로직이 길면 클래스로 뺀다. 람다가 어떤 인터페이스 메서드의 구현인지 헷갈리면 주석에 적는다 (`// = ProcessorSupplier.get()`).
- 가격은 `BigDecimal`, JSON에 쓸 때는 `toPlainString()` 문자열 (double 오차 없이).

## 3. Kafka Streams 앱

- **설정은 `config/KafkaConfig.create()` 한 곳.** env로 받는 값: `KAFKA_APPLICATION_ID`, `KAFKA_BOOTSTRAP_SERVERS`, `NUM_STREAM_THREADS`, `STATE_DIR` (기본값은 로컬 개발용). 공통 값: `exactly_once_v2`, 내부 토픽 복제 3 · min ISR 2, producer `zstd`.
- **Topology는 `<Name>Topology.build(...)` static 메서드.** 순서: store 등록 → source → 처리 → sink. 노드 이름은 `Consumed/Produced.withName`, `Named.as`, `Materialized.as`로 붙이고, 같은 일을 하는 Flink 연산자 이름과 맞춘다.
- **main의 종료 처리는 kstreams-dedup `DedupApp`과 같게 한다:**
  `CountDownLatch latch` + `setUncaughtExceptionHandler`(→ `SHUTDOWN_CLIENT`) + `setStateListener`(ERROR → `latch.countDown()`) + shutdown hook(`close(20s)` → `countDown()`) + 깨어난 뒤 `streams.state() == ERROR`면 `System.exit(1)`.
- **비교 실행:** Flink 버전과 같은 출력 토픽에 쓰지 않도록 `<출력>_TOPIC_SUFFIX` env(`.kstreams`)를 둔다. 접미어 토픽과 env는 `compare/` kustomize overlay에 넣는다.

## 4. Flink job

- Kafka 연결은 `common/KafkaIO`(`source`, `sink`). 연산자마다 `.name()`과 `.uid()`를 같은 문자열로, `setParallelism`에 이유 주석.
- 버린 레코드는 `Counter`로 센다 (`basisNoSpot` 처럼 `<job><이유>`).
- state descriptor 이름은 savepoint 식별자라 바꾸지 않는다 (주석으로 표시).

## 5. 테스트

- `src/test/java`에 둔다 (이미지에 들어가지 않는다). Kafka Streams는 `TopologyTestDriver`(변수 `testDriver`) + `TestInputTopic` · `TestOutputTopic`.
- 테스트 메서드 이름은 **영어 camelCase**, 바로 위에 **한국어 한 줄 주석**으로 무엇을 확인하는지 적는다.

```java
// 두 pod가 보낸 같은 이벤트는 먼저 온 것만 통과한다
@Test
void sameEventFromTwoPodsPassesOnlyFirst() {
```

- 코드를 고치면 `./gradlew test`를 돌린다 (로컬에 JDK가 없으면 `eclipse-temurin:17-jdk` 컨테이너로).

## 6. 배포 매니페스트

- `deploy/<job>.yaml`, namespace `binance-processing`, 이미지 `kimdonghee125/<프로젝트 폴더 이름>:<태그>` (태그를 올려 배포).
- 파일 맨 위 주석에 무엇을 하는지 한 줄과 `docker build` · `docker push` · `kubectl apply` 명령을 적는다.
- Kafka Streams Deployment: replicas 2, RollingUpdate `maxUnavailable: 0`, `terminationGracePeriodSeconds: 30` (close 20s보다 길게).
