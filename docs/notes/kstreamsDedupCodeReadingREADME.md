# Kafka Streams dedup 앱 코드 읽는 순서

raw 토픽에는 Active-Active pod 2개가 같은 이벤트를 각자 보내 중복이 있다. [consumer/kstreams-dedup](../../consumer/kstreams-dedup/)은 이 중복을 지워 clean 토픽으로 옮기는 Kafka Streams 앱이다. 이 노트는 그 코드를 처음 읽을 때 따라갈 순서다.
**배포 yaml → `main` → Topology 조립 → 레코드 하나 → 트랜잭션 커밋 → 리밸런스 · 장애** 순서로 본다. 예시 메시지는 `binance.raw.spot.crypto.bookticker`에 실제로 들어온 것이다.

하는 일은 Flink 버전([flinkDedupCodeReadingREADME.md](flinkDedupCodeReadingREADME.md))과 같다. 그래서 장마다 **Flink에서는 무엇이 대응하나**를 같이 적었다. 실행 방법과 전환 절차는 [kstreams-dedup/README.md](../../consumer/kstreams-dedup/README.md)에 있다.

---

## 0. 큰 그림

```text
deploy/dedup.yaml (Deployment, replicas 2)
   │  Kubernetes가 pod 2개를 띄운다. Operator도 JobManager도 없다
   ▼
pod 하나 = JVM 프로세스 하나 ── DedupApp.main(args)
   │  args → DedupRoute.parse → DedupTopology.build → new KafkaStreams(topology, config) → start()
   ▼
같은 프로세스 안의 StreamThread가 레코드를 처리
   source(raw) → DedupProcessor → sink(clean)
                  └ window store "seen-<경로>" (heap) ──변경──▶ changelog 토픽
   │
   └─ 100ms마다 [clean 쓰기 + changelog 쓰기 + raw offset] 을 한 트랜잭션으로 커밋
```

| 순서 | 파일 | 보는 것 |
| --- | --- | --- |
| 1 | [deploy/dedup.yaml](../../consumer/kstreams-dedup/deploy/dedup.yaml), [Dockerfile](../../consumer/kstreams-dedup/Dockerfile), [build.gradle](../../consumer/kstreams-dedup/build.gradle) | yaml 값이 어디로 들어가나 |
| 2 | [DedupApp.java](../../consumer/kstreams-dedup/src/main/java/com/consumer/dedup/DedupApp.java) `main` | 앱 조립, 종료 · 실패 처리 |
| 3 | [DedupRoute.java](../../consumer/kstreams-dedup/src/main/java/com/consumer/dedup/DedupRoute.java) | 인자 문자열 → raw · clean 토픽, store 이름 |
| 4 | [DedupTopology.java](../../consumer/kstreams-dedup/src/main/java/com/consumer/dedup/DedupTopology.java) | 경로 하나의 파이프라인 + store |
| 5 | [DedupProcessor.java](../../consumer/kstreams-dedup/src/main/java/com/consumer/dedup/DedupProcessor.java) | 레코드 하나의 처리 |
| 6 | 다시 [DedupApp.java](../../consumer/kstreams-dedup/src/main/java/com/consumer/dedup/DedupApp.java) `config` | exactly-once 커밋 |
| 7 | — | pod 2개의 task 분배, 장애 · 재배포 |
| 8 | [DedupTopologyTest.java](../../consumer/kstreams-dedup/src/test/java/com/consumer/dedup/DedupTopologyTest.java) | broker 없이 돌려 보기 |

---

## 1. yaml · Dockerfile: 값이 어디로 가나

Flink 버전에서는 Operator가 yaml을 읽고, 값마다 다른 곳(Flink 설정, main 인자, S3 플러그인)에 나눠 줬다. 여기는 **평범한 Deployment라 컨테이너에 들어가는 것은 args와 env 둘뿐이다.** 나머지 설정은 모두 코드(`DedupApp.config`)에 있다.

| yaml · 빌드 | 누가 받나 | 코드에서 |
| --- | --- | --- |
| `image` | Dockerfile이 `installDist` 결과를 `/opt/kstreams-dedup`에 복사 | — |
| (Dockerfile) `ENTRYPOINT bin/kstreams-dedup` | gradle `application` 플러그인이 만든 실행 스크립트. `lib/*.jar`를 classpath에 넣고 `mainClass`를 실행 | `DedupApp.main` |
| (build.gradle) `applicationDefaultJvmArgs` | 실행 스크립트가 JVM 옵션으로 넘김 (`-XX:MaxRAMPercentage=60`) | — |
| `args` | **ENTRYPOINT 뒤에 붙어 `main(String[] args)`로 그대로** | `DedupRoute.parse` |
| env `KAFKA_BOOTSTRAP_SERVERS` | 환경 변수 | `config()`의 `bootstrap.servers` |
| env `NUM_STREAM_THREADS` | 환경 변수 | `config()`의 `num.stream.threads` |
| `replicas: 2` | Kubernetes | 코드는 모른다. 같은 `application.id`로 뜬 프로세스끼리 consumer group으로 묶여 task를 나눈다 |
| `terminationGracePeriodSeconds: 30` | SIGTERM 뒤 SIGKILL까지의 시간 | shutdown hook의 `close(20s)`가 이 안에 끝나야 한다 |
| `limits.memory: 512Mi` | 컨테이너 | heap = 512Mi × 60% ≈ 300Mi. store가 heap 안에 있다 |

- **Flink와 다른 점:** 체크포인트 간격이나 저장 위치 같은 설정이 yaml에 없다. Kafka Streams 설정을 바꾸려면 코드를 고치고 이미지를 다시 빌드한다. 자주 바꾸는 값만 env로 뺐다 (`NUM_STREAM_THREADS`, `STATE_DIR`).
- MinIO Secret, S3 설정, HA 설정이 없다. state가 Kafka(changelog 토픽)에 남기 때문이다.

---

## 2. `DedupApp.main`: 조립하고 기다리기만 한다

```java
List<DedupRoute> routes = args.map(DedupRoute::parse);            // (실제 코드는 for문)
Topology topology = DedupTopology.build(routes, TTL);             // 설계도
KafkaStreams streams = new KafkaStreams(topology, config());      // 아직 아무것도 안 읽음
streams.setUncaughtExceptionHandler(...);  streams.setStateListener(...);  addShutdownHook(...);
streams.start();                                                   // StreamThread가 뜬다
done.await();                                                      // main 스레드는 여기서 멈춰 있다
```

- **`main`은 레코드를 처리하지 않는다.** Flink의 `env.execute()`와 비슷하게 `streams.start()`가 처리를 시작한다. 다른 점은 처리가 **같은 프로세스 안의 StreamThread**에서 돈다는 것이다. Flink는 `main`이 JobManager에서 job graph를 만들고, 처리는 다른 pod(TaskManager)에서 했다.
- `start()`는 바로 돌아온다. 그래서 `main`이 끝나지 않도록 `CountDownLatch`로 기다린다. Python으로 치면 `threading.Event().wait()`다.
- 시작할 때 `topology.describe()`를 로그에 남긴다. 4장의 그림이 실제로 이렇게 만들어졌는지 pod 로그에서 바로 볼 수 있다.

**끝나는 길은 둘이다.**

```text
정상 종료 (kubectl rollout, pod 삭제)
  SIGTERM → [스레드] dedup-shutdown (shutdown hook)
  → streams.close(20s): 진행 중 트랜잭션 커밋, consumer group에서 나감 (남은 pod가 리밸런스로 task를 받음)
  → done.countDown() → main 끝 → 프로세스 종료

실패 (처리 중 잡히지 않은 예외)
  [스레드] StreamThread → UncaughtExceptionHandler → SHUTDOWN_CLIENT (이 인스턴스의 모든 스레드를 내림)
  → 상태 ERROR → StateListener가 failed=true, done.countDown()
  → main이 close 후 System.exit(1) → 컨테이너 종료 → Kubernetes가 pod 재시작
```

- `REPLACE_THREAD`(스레드만 새로 띄우기)를 쓰지 않은 이유는 이렇다. 같은 레코드에서 계속 실패하는데도 pod는 Running으로 보여 장애를 알아채기 어렵다. 프로세스를 끝내면 restart 횟수로 드러난다. liveness probe가 없는 것을 이것으로 대신한다.
- 상태 흐름은 `CREATED → REBALANCING → RUNNING`이다. 리밸런스가 일어날 때마다 `RUNNING → REBALANCING → RUNNING`이 로그에 찍힌다.

---

## 3. `DedupRoute.parse`: 인자 문자열 → 경로

Flink 버전과 형식이 같다.

```text
"spot.crypto.bookticker:u"
   ▼
DedupRoute(
  routeId      = "spot.crypto.bookticker",
  rawTopic     = "binance.raw.spot.crypto.bookticker",
  cleanTopic   = "binance.clean.spot.crypto.bookticker",
  eventIdField = "u")
storeName()    = "seen-spot.crypto.bookticker"
```

- 하나 추가된 것은 `storeName()`이다. 이 이름은 changelog 토픽 이름 `kstreams-dedup-seen-spot.crypto.bookticker-changelog`(`<application.id>-<store>-changelog`)에 들어간다. **이름을 바꾸면 새 토픽이 생겨 기존 state를 잃는다.** Flink의 `uid`와 같은 역할이다.

---

## 4. `DedupTopology.build`: 경로 하나의 파이프라인

```java
builder.addStateStore(inMemoryWindowStore("seen-<경로>", ttl, ttl, false));   // ① store
builder.stream(rawTopic, Consumed.withName("raw.<경로>"))                       // ② source 노드
       .process(() -> new DedupProcessor(route, ttl), Named.as("dedup.<경로>"), "seen-<경로>")  // ③ processor + store 연결
       .to(cleanTopic, Produced.withName("clean.<경로>"));                       // ④ sink 노드
```

`topology.describe()` 로그는 이렇게 나온다:

```text
Sub-topology: 0
  Source: raw.spot.crypto.bookticker (topics: [binance.raw.spot.crypto.bookticker])
    --> dedup.spot.crypto.bookticker
  Processor: dedup.spot.crypto.bookticker (stores: [seen-spot.crypto.bookticker])
    --> clean.spot.crypto.bookticker
  Sink: clean.spot.crypto.bookticker (topic: binance.clean.spot.crypto.bookticker)
```

- **① store:** 키 `"BTCUSDT|5001"` → 값 레코드 timestamp. 보존 기간과 윈도우 크기가 둘 다 TTL(1분)이고, 보존 기간이 지난 항목은 store가 스스로 지운다 (Flink의 state TTL). store를 만들면 changelog가 기본으로 켜진다. 그래서 `put` 하나가 changelog 토픽에 레코드 하나로 남는다.
- **② source:** 키 · 값을 `String`으로 받는다. Flink 버전은 Kafka key를 버리고 sink에서 다시 만들었다. 여기서는 key(심볼)가 처음부터 끝까지 그대로 따라간다.
- **③ processor:** `() -> new DedupProcessor(...)`는 팩토리다 (Python으로 치면 `lambda: DedupProcessor(route, ttl)`). **task마다 인스턴스를 하나씩** 새로 만든다. 마지막 인자로 store 이름을 넘겨야 processor 안에서 `getStateStore`로 꺼낼 수 있다.
- **④ sink:** partitioner를 지정하지 않아 producer 기본 파티셔너(key 해시)를 쓴다. 키가 raw와 같은 심볼이고 파티션 수도 3으로 같아서, clean도 raw와 같은 파티션 번호로 간다.
- **repartition이 없다.** Flink 버전에는 `keyBy("symbol|id")` 셔플이 있었다. 여기서는 raw의 키가 이미 심볼이라, 두 pod가 보낸 같은 이벤트가 같은 파티션 → 같은 task → 같은 store로 온다. 그래서 그림이 한 줄이다.
- **경로끼리는 연결이 없다.** 그래서 경로마다 sub-topology가 따로 생긴다 (0, 1, 2 …). 실행 단위인 task는 `(sub-topology, 파티션)`이다. 경로 1개 × 파티션 3 = task 3개이고, 9개를 다 켜면 27개다.

| Flink 버전 | Kafka Streams 버전 |
| --- | --- |
| `KafkaIO.source` | `builder.stream` (source 노드) |
| `DedupKeyParser` + `keyBy` + `DedupFunction` | `DedupProcessor` 하나 |
| `ValueState<Boolean>` + TTL | window store + 보존 기간 |
| `KafkaIO.sink` | `.to` (sink 노드) |
| `.uid(...)` | store 이름 (changelog 토픽 이름) |
| 연산자 chaining으로 생긴 박스 | sub-topology |
| subtask | task |

---

## 5. `DedupProcessor`: 레코드 하나 따라가기

### `init`: task가 이 스레드에 배정될 때 한 번

```text
[호출] Kafka Streams — task가 StreamThread에 할당될 때 (리밸런스로 다시 할당되면 또)
context.getStateStore("seen-spot.crypto.bookticker") → 이 task(= 파티션) 전용 store
```

- 같은 경로라도 **파티션마다 store가 따로 있다.** 파티션 0의 task는 파티션 0에 온 심볼의 키만 갖는다. Flink에서 keyed state가 key group마다 나뉘는 것과 같다.
- store를 처음 받을 때 Kafka Streams가 changelog를 읽어 채운 뒤에 `process`를 부르기 시작한다 (복원, 7장).

### `process`: raw 레코드마다

예시 입력 (pod `n2dmt`와 `s5wzv`가 같은 이벤트를 30ms 간격으로 보냄):

```json
key   = "BTCUSDT"
value = {"source":"binance","product":"spot","asset_class":"crypto","sector":"sov","stream":"bookTicker",
         "symbol":"BTCUSDT","event_time":null,"receive_time":1794471848261,
         "collector_id":"spot-sov-6995dd6c4d-n2dmt",
         "payload":{"u":102670132459,"s":"BTCUSDT","b":"73561.06000000","B":"3.08305000","a":"73561.07000000","A":"2.32501000"}}
timestamp = 1794471848263   (collector producer가 보낸 시각, CreateTime)
```

```text
① 키 만들기    envelope.symbol + "|" + envelope.payload.u → "BTCUSDT|102670132459"
               JSON이 깨졌거나 둘 중 하나가 없으면 warn 로그만 남기고 return (버림)

② 봤나?        seen.fetch(키, ts − 1분, ts + 1분)
   n2dmt 것    → 비어 있음 → ③으로
   s5wzv 것    → n2dmt가 넣은 항목이 있음 → return (버림)

③ 기록 · 출력  seen.put(키, ts, ts)                    → store에 쓰고, changelog에도 한 건
               envelope.put("dedup_time", 지금 시각)
               context.forward(record.withValue(...))  → sink → clean 토픽 (트랜잭션 안)
```

- **조회 범위가 앞뒤 1분이다.** 두 pod의 timestamp는 수십 ms 차이가 난다. timestamp가 늦은 쪽이 먼저 도착해도 잡히도록 뒤쪽(`ts + 1분`)까지 본다. 테스트 "늦은 timestamp가 먼저 도착"이 이것을 확인한다.
- **기준 시각은 레코드 timestamp다.** 벽시계가 아니다. 지우는 것도 이 task가 본 가장 늦은 timestamp(stream time)가 1분 지나야 일어난다.
- **예외를 던지지 않는다.** 던지면 2장의 실패 경로로 pod가 재시작되고, 같은 메시지를 다시 읽어 또 실패하기를 반복한다. Flink 버전의 `DedupKeyParser`와 같은 이유다.
- **락이 없다.** task 하나는 StreamThread 하나에서만 돌아서 `process`가 동시에 불리지 않는다. Flink의 mailbox 스레드와 같다.
- `forward`한 레코드는 입력의 key와 timestamp를 그대로 갖는다. clean의 timestamp = raw의 timestamp다.

출력 (clean, key `BTCUSDT`): 입력 value에 `"dedup_time":1794471848290`만 붙는다. envelope와 payload는 바꾸지 않는다.

---

## 6. `config`: exactly-once 커밋이 일어나는 방법

```java
processing.guarantee = exactly_once_v2
commit.interval.ms   = 100   (EOS일 때 기본값. 코드에 없다)
```

StreamThread 하나가 producer 하나와 트랜잭션 하나를 가진다. 100ms 동안 이 스레드의 모든 task가 쓴 것을 한 번에 커밋한다.

```text
트랜잭션 시작
  ├ clean 토픽에 forward한 레코드들
  ├ changelog 토픽에 store 변경들 (put마다 한 건)
  └ raw 토픽 consumer offset (sendOffsetsToTransaction)
트랜잭션 커밋 → 셋이 함께 보이거나, 함께 버려진다
```

- **그래서 재시작해도 clean에 중복이 다시 생기지 않는다.** 커밋 전에 죽으면 clean 쓰기도 버려지고(abort), offset도 store도 커밋 전으로 돌아간다. Flink 버전은 offset · state(체크포인트)와 clean 쓰기(at-least-once sink)가 따로 움직여서 이 보장이 없었다.
- clean을 읽는 쪽은 `isolation.level=read_committed`여야 abort된 레코드를 안 본다. flink-join의 `KafkaIO.source`가 이렇게 설정돼 있다.
- 커밋할 때마다 파티션에 commit marker가 하나씩 들어가 offset을 하나 차지한다. 그래서 clean의 offset 증가량이 메시지 수보다 조금 크다.

| 설정 | 왜 |
| --- | --- |
| `application.id = kstreams-dedup` | consumer group 이름이자 내부 토픽 접두어. 바꾸면 offset과 state를 처음부터 다시 쌓는다 |
| `num.stream.threads` (env, 기본 1) | pod 안 StreamThread 수 |
| `state.dir = /tmp/kafka-streams` | task 잠금 · offset 체크포인트 파일. store는 in-memory라 데이터는 여기 없다 |
| `replication.factor = 3`, `topic.min.insync.replicas = 2` | changelog도 다른 토픽처럼 복제 |
| `producer.compression.type = zstd` | clean 토픽 설정과 같게 (다르면 broker가 다시 압축) |

---

## 7. pod 2개: task 분배, 장애, 재배포

```text
consumer group "kstreams-dedup" (task 3개: 0_0, 0_1, 0_2 = sub-topology 0의 파티션 0·1·2)

pod A  StreamThread-1 ── task 0_0, 0_2   (store 2개)
pod B  StreamThread-1 ── task 0_1        (store 1개)
```

| 상황 | 일어나는 일 |
| --- | --- |
| 처음 뜰 때 | 리밸런스로 task를 나눠 받는다. changelog에서 store를 채운 뒤 처리를 시작한다 |
| pod 하나 죽음 | 남은 pod가 리밸런스로 그 task를 받는다 → changelog에서 store 복원 → 마지막 커밋 offset부터 처리. 커밋 안 된 트랜잭션은 abort돼서 중복이 없다 |
| 롤링 업데이트 | `maxUnavailable: 0`이라 새 pod가 먼저 뜨고, 옛 pod가 `close`하며 나간다. 리밸런스가 두 번 일어난다 |
| 스레드 수 · pod 수 > task 수 | task를 못 받은 스레드는 논다 |

- **Flink와 비교:** Flink는 TaskManager 하나가 죽으면 job 전체가 체크포인트로 돌아갔다. Kafka Streams는 죽은 pod의 task만 옮겨 가고, 나머지 task는 계속 처리한다.
- **복원 시간은 changelog 크기에 달렸다.** store에는 1분치만 있다. 그런데 changelog 토픽은 기본값으로 store 보존 기간보다 24시간 더 보관한다(`windowstore.changelog.additional.retention.ms`). 또 segment가 닫히기(기본 7일) 전에는 지우지 않는다. 그래서 `config()`에서 추가 보존을 1분, `segment.ms`를 10분으로 줄였다. 이제 복원할 때 읽는 양은 최대 십여 분치다. 이 설정은 토픽을 처음 만들 때만 적용된다.
- standby replica는 0이다. task를 옮길 때 미리 복원해 둔 복사본이 없다.

---

## 8. 테스트: broker 없이 돌려 보기

```java
driver = new TopologyTestDriver(DedupTopology.build(List.of(route), TTL), DedupApp.config());
raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 1), T0);
raw.pipeInput("BTCUSDT", envelope("pod-b", "BTCUSDT", 1), T0.plusMillis(30));
clean.readKeyValuesToList();   // 1건
```

- `TopologyTestDriver`는 Topology를 메모리 안에서 한 스레드로 돌린다. 레코드를 넣으면 sink까지 바로 동기로 흐른다. Python으로 치면 가짜 Kafka를 넣은 pytest다.
- timestamp를 직접 정해 넣으므로 TTL 경계(1분 넘게 늦은 중복)도 시험할 수 있다.
- **확인하지 못하는 것:** 트랜잭션, 리밸런스, changelog 복원. 이것들은 클러스터에서 pod를 지워 보며 확인한다 ([README](../../consumer/kstreams-dedup/README.md) "확인").

---

## 9. 헷갈리기 쉬운 점

- **"클러스터"가 없다.** pod 2개는 서로를 모른다. 둘을 묶는 것은 Kafka의 consumer group(`application.id`)뿐이다.
- **task 수는 파티션 수로 정해진다.** pod나 스레드를 늘려도 task 수(경로 × 3)보다 빨라지지 않는다.
- **store는 heap에 있지만 원본은 changelog다.** pod가 죽으면 heap의 store는 사라지고, 새로 받은 쪽이 changelog에서 다시 만든다.
- **clean은 100ms 단위로 보인다.** `read_committed`로 읽으면 커밋된 것만 보인다. `read_uncommitted`(기본값)로 읽으면 abort된 레코드까지 보여 중복처럼 보일 수 있다.
- **TTL 1분보다 늦게 온 중복은 통과한다.** Flink 버전과 같다.
- **레코드 timestamp는 collector가 보낸 시각(CreateTime)이다.** raw 토픽에 LogAppendTime을 켜면 broker가 받은 시각으로 바뀐다. 그러면 두 pod 사이 timestamp 차이도 달라진다.
