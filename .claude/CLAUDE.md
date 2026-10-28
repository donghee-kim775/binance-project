# Binance Real-Time Market Insight Platform

Binance WebSocket 시장 데이터를 Kafka로 수집하고 Flink로 분석하는 개인 학습 프로젝트. 설계 결정과 그 근거를 남기는 것이 구현만큼 중요하다 (면접 대비 포함).

## 저장소 구조

- `datasource-ingestion/collector/`: 수집 앱 (Spring Boot 4, Java 17). **raw 이벤트를 Kafka로 보내기만 한다.** dedup, 조인, 분석은 하지 않는다.
- `datasource-ingestion/deploy/`: 수집 앱 Dockerfile과 K8s 매니페스트 (kustomize). 설정→코드 매핑은 `deploy/mapping.md`
- 분석(dedup, basis join 등)은 **별도 Flink 앱**에서 한다. 수집 앱에 넣지 않는다.
- `plan/`: 설계 결정 기록 (`ingestion/`, `topic/`, `consumer/`)
- `Infra/`: kind 클러스터, Strimzi Kafka, Flink Operator
- `docs/`: Binance API 조사 (`binance-api/`, `streams/`), 수집 심볼 표와 섹터 분류 (`symbols/0TickerInfoTable.md`, `symbols/cryptoSectorREADME.md`), Kafka 토픽 목록과 설정 이유 (`kafka/`), 초기 메모 (`notes/`), Spring Boot 동작 정리 (`spring/`)
- `Insights/`: 인사이트 카탈로그와 인사이트별 계획
- `servingapp/`: 서빙 계층 설계

## 설계 규칙

- **토픽 이름:** `binance.<raw|clean|analytics>.<spot|usdm>.<crypto|stock>.<stream>` (소문자, 숫자, `.`만)
- **Kafka 키:** 심볼 (`BTCUSDT`). product와 자산 종류는 토픽 이름에 있으므로 키에 넣지 않는다.
- **payload:** Binance 원본을 그대로 envelope에 넣는다. 필드를 고치지 않는다.
- **USD-M 경로 접두어:** `/public` = bookTicker, depth / `/market` = aggTrade, markPrice. 틀리면 연결은 되지만 메시지가 오지 않는다.
- **Pod 그룹:**
  - variable-rate (bookTicker, trade/aggTrade, depth): Active-Active, replicas 2, RollingUpdate `maxUnavailable: 0`
  - fixed-rate (markPrice@1s): Active-Standby, replicas 1, `Recreate`
- **네임스페이스:** 수집 앱 `binance-ingestion`, 처리(Flink job) `binance-processing`, Kafka `kafka`, Flink Operator `flink-operator`
- 수집 심볼이나 스트림이 바뀌면 `0TickerInfoTable.md`, `deploy/`, `plan/topic/plan.md`, `docs/kafka/topics.md`, README 수집 대상 표를 함께 맞춘다 (`/add-symbol`).

## 코드 주석 규칙 (흐름 추적)

제어가 눈에 보이지 않게 넘어가는 지점에는 한 단계 아래를 따라갈 수 있게 태그 주석을 단다. 기존 설명 주석은 그대로 두고 태그 줄을 덧붙인다.

- `// [호출] <누가> — <언제>`: 우리 코드가 직접 부르지 않는 메서드. JDK·스프링 콜백(`@Override`), `@PostConstruct`·`@PreDestroy`, `ApplicationRunner.run`, `@Bean`
- `// [다음] <클래스.메서드>`: 이 줄에서 제어가 다른 클래스로 넘어갈 때. 람다나 인터페이스를 거치면 실제로 실행되는 구현 위치까지 적는다
- `// [스레드] <스레드 이름>`: 실행 스레드가 앞 단계와 다를 때 (`main`, `HttpClient-N-Worker-N`, `ingest-async-N`, `kafka-producer-network-thread`, `SpringApplicationShutdownHook`)

```java
// [호출] JDK HttpClient — 텍스트 프레임이 올 때마다
// [스레드] HttpClient-N-Worker-N
public CompletionStage<?> onText(...) {
    messageHandler.handle(message); // [다음] CollectorRunner의 람다 → MarketDataIngestService.onRaw
```

## 문서 작성 규칙

- 문서는 한국어로 쓴다.
- 설계 문서(`plan/`)는 **고려 사항 → 선택 옵션 → 선택 → 이유 → trade-off** 형식을 따른다 (`/decision`).
- 문서끼리 내용이 어긋나면 고치기 전에 사용자에게 어느 쪽이 맞는지 확인한다.

## 명령어

```bash
# 빌드
cd datasource-ingestion/collector && ./gradlew bootJar

# 이미지 (datasource-ingestion/ 에서)
docker build -f deploy/Dockerfile -t ingestion-collector:0.0.1 collector

# 매니페스트 렌더링 검사
kubectl kustomize datasource-ingestion/deploy

# kind 배포
kind load docker-image ingestion-collector:0.0.1
kubectl apply -k datasource-ingestion/deploy
kubectl get pods -n binance-ingestion -L rate-group
```

## 작업 규칙

- git commit, push는 사용자가 요청할 때만 한다. 브랜치 이름은 `feat/<주제>` 형식.
- PR 설명은 `.github/pull_request_template.md` 형식을 따른다 (`/pr-desc`).
