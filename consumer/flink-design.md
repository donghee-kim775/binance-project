# Flink 앱 설계안

Kafka raw 토픽을 읽어 dedup과 분석을 하는 Flink 앱의 설계안이다. 수집 앱은 raw를 Kafka로 보내기만 하고, dedup·조인·분석은 전부 여기서 한다.

> 상태: **dedup job 구현, basis job 예정.** dedup은 [flink-app/](flink-app/)에 있고 Flink Operator(`FlinkDeployment`)로 띄운다 (매니페스트 작성, 배포 검증 전). [flink-test/](flink-test/)는 경로 하나짜리 테스트 버전이다. 확정되면 `plan/consumer/plan.md`에 결정 기록으로 옮긴다.

## 전체 흐름

```text
ingestion pod ──> raw ──[dedup job]──> clean ──[basis job]──> analytics
                   │                                ▲
                   └── markprice (중복 없음) ────────┘
```

| job | 입력 | 출력 | 하는 일 |
| --- | --- | --- | --- |
| dedup | `binance.raw.*` (variable-rate 스트림) | `binance.clean.*` | `(symbol, id)`가 처음 온 것만 통과 |
| basis (Insight 1) | `binance.clean.spot.crypto.bookticker`, `binance.raw.usdm.crypto.markprice` | `binance.analytics.crypto.basis` | Spot mid와 mark price로 basis 계산 |

---

## 1. job을 무엇 기준으로 나눌까

### 고려 사항

- 수집 쪽은 rate 그룹(variable / fixed)으로 pod를 나눴다. Flink도 같은 기준으로 나눠야 할까?
- rate 그룹이 Flink에 주는 영향은 **그 토픽이 dedup을 거쳐야 하는가** 하나뿐이다.
- basis job은 variable-rate(Spot bookTicker)와 fixed-rate(markPrice)를 **같이** 읽는다.

### 선택 옵션

| 옵션 | 설명 |
| --- | --- |
| A. rate 그룹별 | variable-rate용 앱, fixed-rate용 앱 |
| B. 기능별 | dedup job, 분석 job (인사이트마다) |

### 선택

**B. 기능별.**

### 이유

- rate 그룹의 차이는 "dedup job의 입력 목록에 markPrice가 없다"로 이미 표현된다. 코드를 따로 둘 이유가 없다.
- 분석 job은 두 rate 그룹을 함께 읽는다. rate로 나누면 basis job을 어디에 둘지 애매해진다.
- basis job이 죽거나 재배포돼도 dedup은 계속 돌아야 clean 토픽이 끊기지 않는다. 체크포인트와 재시작도 job 단위다.

### trade-off

- job이 늘수록 JobManager·TaskManager pod가 늘어 kind 자원을 더 쓴다.
- clean 토픽을 한 번 거치므로 dedup과 분석을 한 job에 합칠 때보다 지연이 조금 늘어난다. 대신 clean을 다른 분석에서 재사용하고 단계별 지연을 따로 잴 수 있다.

---

## 2. dedup job을 몇 개 띄울까

### 고려 사항

- raw → clean 쌍은 9개다 ([docs/kafka/topics.md](../docs/kafka/topics.md)).
- job마다 JobManager와 TaskManager가 생긴다. kind는 PC 한 대 위에 있고 메모리가 넉넉하지 않다.
- 쌍마다 하는 일은 같고 dedup 키 필드만 다르다 (`u`, `t`, `a`).

### 선택 옵션

| 옵션 | 설명 |
| --- | --- |
| A. 토픽 쌍마다 job 하나 | 9개 job |
| B. job 하나에 경로 여러 개 | 하나의 job 안에 source → dedup → sink 파이프라인을 쌍마다 둔다 |

### 선택

**B. job 하나에 경로 여러 개.**

```java
// DedupJob.main — 경로 목록은 설정에서 받는다
List<DedupRoute> routes = List.of(
    new DedupRoute("binance.raw.spot.crypto.bookticker", "binance.clean.spot.crypto.bookticker", "u"),
    new DedupRoute("binance.raw.spot.crypto.trade",      "binance.clean.spot.crypto.trade",      "t"),
    ...
);
for (DedupRoute r : routes) {
    env.fromSource(KafkaIO.source(r.rawTopic()), ...)
       .keyBy(e -> e.symbol() + "|" + e.payload().get(r.keyField()).asText())
       .process(new DedupFunction(Duration.ofMinutes(1)))
       .sinkTo(KafkaIO.sink(r.cleanTopic()));
}
env.execute("dedup");
```

### 이유

- 자원을 job 하나만큼만 쓴다.
- 스트림이 늘면 경로 목록에 한 줄 추가하면 된다. 수집 앱이 "코드 하나 + 설정만 다르게"인 것과 같은 방식이다.

### trade-off

- 한 경로에서 문제가 나면 job 전체가 재시작되어 다른 경로도 함께 멈춘다.
- 경로마다 병렬도나 체크포인트 간격을 따로 조정하기 어렵다.
- 처음에는 Insight 1에 필요한 `spot.crypto.bookticker` 경로 하나로 시작한다.

---

## 3. dedup 키

### 고려 사항

- Active-Active pod 2개가 같은 이벤트를 각자 보내므로 raw에 같은 이벤트가 두 번 들어온다.
- 원본 payload의 id(`u` update id, `t` trade id, `a` aggTrade id)는 **심볼마다 따로 매기는 번호**다.
- 섹터 pod처럼 한 토픽에 심볼이 여러 개 섞인다.

### 선택 옵션

| 옵션 | 설명 |
| --- | --- |
| A. id만 (`keyBy(u)`) | |
| B. 심볼 + id (`keyBy(symbol, u)`) | |
| C. 메시지 전체 해시 | |

### 선택

**B. 심볼 + id.** state에는 "봤다"만 기록하고 TTL 1분 뒤 지운다.

| 스트림 | dedup 키 |
| --- | --- |
| bookTicker, depth | `(symbol, u)` |
| trade (Spot) | `(symbol, t)` |
| aggTrade (USD-M) | `(symbol, a)` |

### 이유

- A는 다른 심볼의 같은 번호를 중복으로 오해해서 버린다.
- C는 envelope의 `collector_id`, `receive_time`이 pod마다 달라서 같은 이벤트도 해시가 달라진다. payload만 해시하면 되지만 id가 있는데 굳이 쓸 이유가 없다.
- TTL 1분은 두 pod 사이 도착 시차(수십 ms)보다 넉넉하고, state가 계속 쌓이지 않게 한다.

### trade-off

- 한 pod가 1분 넘게 늦게 보낸 메시지는 중복으로 걸러지지 않는다.
- 키 수만큼 state가 생긴다. bookTicker는 초당 수백 건이라 1분 동안 수만 개 키가 유지된다.

---

## 4. 코드 구조: jar 하나, job 여러 개

### 고려 사항

- job마다 envelope 파싱과 Kafka 연결 코드가 복사되면 안 된다.
- job은 따로 배포되고 따로 재시작되어야 한다.

### 선택 옵션

| 옵션 | 설명 |
| --- | --- |
| A. job마다 Gradle 프로젝트 | |
| B. 프로젝트 하나, `main()` 클래스마다 job | 배포할 때 `entryClass`로 고른다 |

### 선택

**B.**

```text
consumer/flink-app/                   # Flink 앱 하나 (jar 하나)
├── build.gradle
├── Dockerfile
├── src/main/java/com/consumer/
│   ├── common/
│   │   └── KafkaIO.java                  KafkaSource / KafkaSink 공통 생성                  (구현)
│   ├── dedup/
│   │   ├── DedupJob.java                 main(): 경로마다 source → parse → dedup → sink     (구현)
│   │   ├── DedupRoute.java               (raw 토픽, clean 토픽, 키 필드)                   (구현)
│   │   ├── DedupKeyParser.java           envelope → (symbol, id, JSON), 깨진 메시지는 버림  (구현)
│   │   └── DedupFunction.java            KeyedProcessFunction, 처음 본 키만 통과            (구현)
│   └── basis/                                                                              (예정)
│       ├── BasisJob.java                 main(): clean bookTicker + raw markPrice → analytics
│       ├── BasisFunction.java            KeyedCoProcessFunction
│       └── model/  SpotMid, MarkPrice, Basis
└── deploy/
    ├── dedup.yaml                    # FlinkDeployment (entryClass = DedupJob)                    (구현)
    └── basis.yaml                    # (예정)
```

- dedup은 envelope를 고치지 않고 그대로 넘기므로, 전체를 객체로 바꾸지 않고 `symbol`과 id 필드만 꺼낸다. envelope 클래스(`BinanceEventEnvelope`)는 필드를 실제로 읽는 basis job을 만들 때 `common/`에 둔다.

### 이유

- 이미지 하나로 `FlinkDeployment`의 `entryClass`만 바꿔 job을 따로 띄운다.
- `common/`을 공유하므로 envelope 필드가 바뀌면 한 곳만 고친다.

### trade-off

- job 하나를 고쳐도 jar 전체를 다시 빌드한다. 다른 job의 이미지 태그를 올릴지는 따로 정해야 한다.

---

## 5. basis job: 윈도우 조인 대신 최신값 state

### 고려 사항

- Spot bookTicker는 초당 수십~수백 건, markPrice는 초당 1건이다.
- 필요한 것은 "markPrice가 왔을 때 그 순간의 최신 Spot mid"다.
- 두 토픽은 키(심볼)와 파티션 수(3)가 같다 (co-partitioning).

### 선택 옵션

| 옵션 | 설명 |
| --- | --- |
| A. 윈도우 조인 | 일정 구간 안의 두 스트림 조합을 출력 |
| B. `connect` + `KeyedCoProcessFunction` | Spot은 심볼별 최신값만 state에 두고 markPrice가 올 때 조회 |

### 선택

**B.**

```java
spotMid.keyBy(SpotMid::symbol)
    .connect(markPrice.keyBy(MarkPrice::symbol))
    .process(new KeyedCoProcessFunction<String, SpotMid, MarkPrice, Basis>() {
        ValueState<SpotMid> latestSpot;   // 심볼별 최신 Spot mid
        // processElement1: latestSpot 갱신
        // processElement2: latestSpot이 2초 안이면 basis 출력
    });
```

### 이유

- 윈도우 조인은 윈도우 안의 모든 조합을 출력한다. 10초 윈도우면 결과가 수천 건이 된다.
- 최신값 state는 심볼마다 값 하나라 state가 작고, 출력이 markPrice와 같은 초당 1건이다.

### trade-off

- Spot 연결이 끊기면 오래된 값과 계산할 수 있다. 2초 넘은 Spot 값은 쓰지 않는다.
- Spot bookTicker에는 `E`가 없어 envelope `event_time`이 `null`이다. 신선도 비교에서 Spot 쪽은 `receive_time`을 쓴다.

---

## 6. 실행 환경과 전달 보장

| 항목 | 값 | 이유 |
| --- | --- | --- |
| Flink | 2.2.1 (`flink:2.2.1-java17` 이미지) | Kafka 커넥터 최신(5.0.0-2.2)이 맞추는 버전 |
| 배포 | Flink Kubernetes Operator 1.16.1, `FlinkDeployment` (application mode), namespace `binance-processing` | job마다 따로 띄우고 따로 재시작. 1.16.1은 Flink 2.2(`v2_2`)를 지원한다 |
| Operator 설치 | webhook 끔 (cert-manager 없음) | 설치할 것과 메모리를 줄인다 ([Infra/Flink-Install/](../Infra/Flink-Install/README.md)) |
| jar | shadowJar, Flink 본체는 `compileOnly` | Flink 이미지에 있는 것을 다시 넣지 않는다. Kafka 커넥터와 Jackson만 넣는다 |
| 자원 | JobManager 1024m, TaskManager 1536m, 슬롯 1, managed memory 0.1 | state가 heap에 있어 RocksDB용 managed memory가 거의 필요 없다 |
| upgradeMode | stateless | 체크포인트 저장소(PV, S3)가 아직 없다. dedup state는 TTL 1분이라 재배포 때 잃어도 영향이 작다 |
| 테스트 | [flink-test](flink-test/)는 MiniCluster를 pod 하나로 띄움 (Operator 없이) | Operator 없이 빠르게 job 동작만 확인한다 |
| 체크포인트 | 10초 | 완료될 때 Kafka offset이 커밋된다 |
| sink 보장 | at-least-once | exactly-once는 Kafka 트랜잭션이 필요하고 읽는 쪽도 `read_committed`로 맞춰야 한다. dedup 결과가 재시작 때 한 번 더 쓰이는 정도는 감수한다 |
| 병렬도 | 파티션 수(3) 이하 | 파티션보다 많으면 노는 태스크가 생긴다 |

---

## 남은 과제

- **문서 불일치:** [plan/topic/insight_1_plan.md](../plan/topic/insight_1_plan.md)의 dedup 방식이 `keyBy(u)`로 되어 있다. 이 설계안은 `(symbol, u)`다. 어느 쪽으로 맞출지 확인한 뒤 고친다.
- **순서:** 수집 앱의 `@Async` 스레드풀이 같은 심볼의 메시지 순서를 섞을 수 있다. dedup은 순서와 상관없지만, depth처럼 순서가 중요한 스트림을 Flink에서 이어 붙이려면 수집 쪽부터 고쳐야 한다.
- **로컬 접속:** Kafka 리스너가 클러스터 안 주소만 알려 줘서, 로컬 노트북(과 로컬에서 돌리는 Flink)이 붙으려면 외부 리스너가 필요하다 ([flink-test/README.md](flink-test/README.md)).
- **Operator 배포 검증:** 로컬 PC 메모리(WSL 약 8GB)로는 Kafka 브로커 3대와 Operator + JobManager + TaskManager를 함께 올리기 어렵다. 클러스터를 옮긴 뒤 배포와 동작을 확인한다.
- **체크포인트 저장소:** stateful 업그레이드(`upgradeMode: savepoint`)를 하려면 체크포인트 · savepoint를 남길 PV나 S3가 필요하다.
- **clean 보존 기간:** `spot.crypto.bookticker`(24h) 외에는 임시로 raw와 같게 두었다.
