# Flink dedup job 코드 읽는 순서

raw 토픽에는 Active-Active pod 2개가 같은 이벤트를 각자 보내서 중복이 있다. 이 중복을 지워 clean 토픽으로 옮기는 dedup job을 처음 읽을 때 따라갈 순서다.
**배포 yaml → `main` → 레코드 하나 → 체크포인트** 순으로 본다. 예시는 `binance.raw.spot.crypto.bookticker`에 실제로 들어온 메시지다.

설계 이유(경로를 job 하나로 묶은 이유, 키, TTL 1분)는 [consumer/flink-design.md](../../consumer/flink-design.md), 실행 방법은 [flink-dedup/README.md](../../consumer/flink-dedup/README.md)에 있다.
레코드가 각 단계에서 어느 스레드 · 버퍼 · state를 지나는지는 [flinkDedupRuntimeFlowREADME.md](flinkDedupRuntimeFlowREADME.md)에 따로 정리했다.

---

## 0. 큰 그림

```text
deploy/dedup.yaml (FlinkDeployment)
   │  Flink Operator가 읽고 pod를 띄운다
   ▼
JobManager pod ── DedupJob.main(args) ── 파이프라인 설계도(job graph)만 만들고 제출
   │                 args 한 줄 → DedupRoute.parse → addRoute
   ▼
TaskManager pod ── 레코드를 실제로 처리
   KafkaSource → DedupKeyParser → keyBy → DedupFunction → KafkaSink
   (KafkaIO)                                              (KafkaIO)
   │
   └─ 10초마다 state + offset을 MinIO에 체크포인트
```

| 순서 | 파일 | 보는 것 |
| --- | --- | --- |
| 1 | [deploy/dedup.yaml](../../consumer/flink-dedup/deploy/dedup.yaml) | yaml 값이 어디로 들어가나 |
| 2 | [DedupJob.java](../../consumer/flink-dedup/src/main/java/com/consumer/dedup/DedupJob.java) `main` | job 조립 |
| 3 | [DedupRoute.java](../../consumer/flink-dedup/src/main/java/com/consumer/dedup/DedupRoute.java) | 인자 문자열 → raw · clean 토픽 |
| 4 | [DedupJob.java](../../consumer/flink-dedup/src/main/java/com/consumer/dedup/DedupJob.java) `addRoute` | 경로 하나의 파이프라인 |
| 5 | [KafkaIO](../../consumer/flink-dedup/src/main/java/com/consumer/common/KafkaIO.java) → [DedupKeyParser](../../consumer/flink-dedup/src/main/java/com/consumer/dedup/DedupKeyParser.java) → [DedupFunction](../../consumer/flink-dedup/src/main/java/com/consumer/dedup/DedupFunction.java) → [KafkaIO](../../consumer/flink-dedup/src/main/java/com/consumer/common/KafkaIO.java) | 레코드 하나의 변환 |
| 6 | 다시 dedup.yaml `flinkConfiguration` | 체크포인트와 재배포 |

---

## 1. `deploy/dedup.yaml`: yaml 값이 어디로 가나

collector는 Spring이 yaml을 `@ConfigurationProperties`에 바인딩했다. 여기는 **Operator가 yaml을 읽고 값마다 다른 곳에 넘긴다.** 코드가 직접 읽는 건 `args`와 환경 변수 하나뿐이다.

| yaml | 누가 받나 | 코드에서 |
| --- | --- | --- |
| `image`, `job.jarURI` | 이 이미지의 `/opt/flink/usrlib/flink-app.jar`를 실행 | [Dockerfile](../../consumer/flink-dedup/Dockerfile)이 jar를 거기 복사 |
| `job.entryClass` | 이 클래스의 `main` 호출 | `DedupJob.main` |
| `job.args` | **`main(String[] args)`에 그대로** | `DedupRoute.parse`가 해석 |
| env `KAFKA_BOOTSTRAP_SERVERS` | 컨테이너 환경 변수 | `KafkaIO.bootstrapServers()` |
| `flinkConfiguration.execution.checkpointing.*` | Flink가 직접 적용 (간격, timeout, 저장 위치) | 읽지 않음 |
| `flinkConfiguration.s3.*`, env `AWS_*`, `ENABLE_BUILT_IN_PLUGINS` | S3 플러그인이 MinIO에 붙을 때 사용 | 읽지 않음 |
| `job.parallelism`, `taskmanager.numberOfTaskSlots` | Flink가 직접 적용 | 읽지 않음 |
| `job.upgradeMode: savepoint` | 재배포 방식 | Operator 동작 (6장) |

**설정 값은 yaml에, 처리 로직만 코드에** 둔다. 그래서 체크포인트 간격이나 경로를 바꿀 때 이미지를 다시 빌드하지 않는다.

---

## 2. `DedupJob.main`: 설계도만 그린다

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
for (String arg : args) addRoute(env, DedupRoute.parse(arg));
env.execute("dedup");
```

- **`main`은 데이터를 처리하지 않는다.** `addRoute`의 `.process()`, `.keyBy()`는 job graph에 노드를 붙일 뿐이다. `execute()`를 불러야 JobManager가 그래프를 TaskManager에 나눠 주고, 그때부터 Kafka를 읽는다.
- `getExecutionEnvironment()`는 실행 위치에 맞는 환경을 준다. IDE에서는 로컬 미니 클러스터, Operator에서는 그 클러스터다.
- 체크포인트 설정은 코드에 없다. 코드에서 같은 키를 설정하면 yaml 값을 덮어쓰기 때문이다. 그래서 **IDE에서 실행하면 체크포인트가 꺼진 채로 돈다.**
- `args`가 비어 있으면 바로 예외를 던진다. yaml에서 `job.args`를 빠뜨린 경우다.
- args 한 줄 = 경로 하나 = 독립된 파이프라인 하나. 지금 `args`는 `spot.crypto.bookticker:u` 한 줄이고, 나머지 8줄은 주석이다.

---

## 3. `DedupRoute.parse`: 인자 문자열 → 경로

```text
"spot.crypto.bookticker:u"
   split(":") → ["spot.crypto.bookticker", "u"]
   검사: 앞이 점으로 3조각인가, 뒤가 비어 있지 않은가 (아니면 IllegalArgumentException)
   ▼
DedupRoute(
  routeId    = "spot.crypto.bookticker",
  rawTopic   = "binance.raw.spot.crypto.bookticker",
  cleanTopic = "binance.clean.spot.crypto.bookticker",
  eventIdField = "u")
```

- 토픽 이름 규칙 `binance.<raw|clean>.<product>.<자산종류>.<스트림>`을 `DedupRoute`의 `RAW_PREFIX` · `CLEAN_PREFIX` 상수 한 곳에서 조립한다. 그래서 yaml에 토픽 이름을 직접 쓰지 않는다.
- `DedupRoute`는 `record`다 (Python의 `@dataclass(frozen=True)`). 생성자, 접근자(`rawTopic()` 등), `equals` · `toString`이 자동으로 생기고 값이 바뀌지 않는다.

| 인자 | id 필드 |
| --- | --- |
| `*.bookticker:u` | order book update id |
| `spot.crypto.trade:t` | trade id |
| `*.aggtrade:a` | aggregate trade id |
| `*.depth:u` | 이벤트의 마지막 update id (diff depth) |

---

## 4. `DedupJob.addRoute`: 경로 하나의 파이프라인

```java
fromSource(KafkaIO.source(raw))      // String (envelope JSON)
  .process(DedupKeyParser("u"))      // → Tuple3(symbol, id, json)
  .keyBy(symbol + "|" + id)          // → "BTCUSDT|102670132459"
  .process(DedupFunction(1분))       // → Tuple2(symbol, json + dedup_time)
  .sinkTo(KafkaIO.sink(clean))
```

- `.name(...)`: Web UI와 로그에 보이는 이름.
- `.uid(...)`: savepoint의 state를 어느 연산자에 다시 붙일지 정하는 이름. 경로 이름으로 고정해서, 경로를 추가하거나 지워도 다른 경로의 state가 이어진다. 이 값을 바꾸면 그 연산자의 state는 savepoint에서 복원되지 않는다.

---

## 5. 레코드 하나 따라가기

스레드 · 직렬화 · 체크포인트 barrier까지 내려간 설명은 [flinkDedupRuntimeFlowREADME.md](flinkDedupRuntimeFlowREADME.md)에 있다.

### ① `KafkaIO.source`: Kafka value를 문자열로 받는다

```json
{"source":"binance","product":"spot","asset_class":"crypto","sector":"sov","stream":"bookTicker",
 "symbol":"BTCUSDT","event_time":null,"receive_time":1794471848261,
 "collector_id":"spot-sov-6995dd6c4d-n2dmt",
 "payload":{"u":102670132459,"s":"BTCUSDT","b":"73561.06000000","B":"3.08305000","a":"73561.07000000","A":"2.32501000"}}
```

- `setValueOnlyDeserializer`라 **Kafka key(`BTCUSDT`)는 읽을 때 버린다.** sink에서 `symbol`로 다시 만든다.
- 컨슈머 그룹은 `flink-dedup`이다. 처음 실행하면 커밋된 offset부터 읽고, 커밋이 없으면 맨 앞(`EARLIEST`)부터 읽는다. savepoint나 체크포인트에서 시작하면 거기 저장된 offset을 쓴다.

### ② `DedupKeyParser`: 키에 필요한 값만 꺼낸다

```text
envelope.symbol      → "BTCUSDT"
envelope.payload.u   → "102670132459"     (eventIdField = args의 콜론 뒤)
출력: Tuple3("BTCUSDT", "102670132459", 원본 json)
```

JSON이 깨졌거나 `symbol`이나 id가 없으면 **예외를 던지지 않고 warn 로그만 남기고 버린다.** 예외를 던지면 job이 재시작되고, 같은 메시지를 다시 읽어 또 실패하기를 반복한다.

### ③ `keyBy`: 같은 키는 같은 곳으로

`"BTCUSDT|102670132459"`의 해시로 보낼 subtask를 정한다. **같은 키는 항상 같은 DedupFunction 인스턴스와 같은 state로 간다.** 여기서 네트워크 전송(셔플)이 생긴다.

### ④ `DedupFunction`: 실제로 중복을 거른다

```text
pod n2dmt가 보낸 것 도착 → seen.value() == null → seen = true, dedup_time 추가 → 내보냄
pod s5wzv가 보낸 것 도착 → seen.value() == true → return (버림)
1분 뒤                   → TTL로 seen이 지워짐 (키가 계속 쌓이지 않게)
```

- `seen`은 `ValueState<Boolean>`라 **지금 처리 중인 키의 값만 보인다.** 변수 하나처럼 생겼지만 실제로는 키별 Map이다.
- 출력은 envelope와 payload를 그대로 두고 `dedup_time`만 붙인 JSON이다 (지연 측정용).
- TTL(1분)보다 늦게 온 중복은 걸러지지 않는다. 두 pod 사이 도착 시차는 수십 ms라 충분하다.

### ⑤ `KafkaIO.sink`: clean 토픽에 쓴다

- `Tuple2.f0`(`BTCUSDT`)이 key, `f1`(json)이 value가 된다. `FieldBytes`가 각각을 UTF-8 바이트로 바꾼다.
- partitioner를 지정하지 않아서 Kafka 기본 파티셔너(key 해시)를 쓴다. key가 raw와 같은 심볼이라 clean도 같은 파티션 규칙을 따른다.
- `AT_LEAST_ONCE`라 재시작하면 마지막 체크포인트 이후 레코드가 clean에 한 번 더 쓰일 수 있다.

---

## 6. 체크포인트와 재배포: 끊겨도 이어지는 방법

설정은 모두 dedup.yaml `flinkConfiguration`에 있다.

```text
평소 (10초마다)
  JobManager가 source에 barrier를 넣음 → barrier가 파이프라인을 따라 흐름
  → 각 연산자가 자기 state를 MinIO에 저장 (source는 offset, DedupFunction은 seen 목록)
  → 전부 끝나면 체크포인트 완료 → Kafka 컨슈머 그룹에 offset 커밋

job 실패 (TaskManager 죽음, 예외)
  → 마지막 체크포인트의 state + offset으로 되돌려 다시 시작
  → 체크포인트가 연속 3번 실패하면 job을 재시작 (tolerable-failed-checkpoints)

JobManager pod 죽음
  → K8s가 pod를 다시 띄움 → HA 기록(ConfigMap + MinIO ha/)에서 마지막 체크포인트를 찾아 이어감

재배포 (이미지 태그나 yaml을 바꿔서 apply)
  → Operator가 savepoint를 찍고 job을 멈춤 → 새 job이 그 savepoint에서 시작 (upgradeMode: savepoint)
  → uid가 같은 연산자에 state가 다시 붙음
```

| 항목 | 값 | 의미 |
| --- | --- | --- |
| `interval` | 10s | 체크포인트 간격 = offset 커밋 간격 |
| `timeout` | 1min | 이보다 오래 걸리면 그 체크포인트는 실패 |
| `min-pause` | 5s | 체크포인트 사이 최소 휴식 |
| `externalized-checkpoint-retention` | `RETAIN_ON_CANCELLATION` | job을 취소해도 마지막 체크포인트가 MinIO에 남음 |
| `dir`, `savepoint-dir` | `s3://binance/flink/clean/flink-dedup/checkpoints`, `.../savepoints` | 저장 위치. `flink/<계층>/<job>/<종류>` 규칙 ([plan/storage/plan.md](../../plan/storage/plan.md)) |

- MinIO 주소는 `s3.endpoint`가 정한다. 이 값이 없으면 AWS로 간다. 계정은 Secret `minio-auth` → env `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`로 들어간다. 이 이름은 S3 클라이언트가 찾는 이름이라 MinIO를 써도 그대로 쓴다.
- `main()`은 JobManager에서만 돌지만 체크포인트 파일은 TaskManager도 직접 쓴다. 그래서 S3 설정은 코드가 아니라 모든 pod가 읽는 `flinkConfiguration`에 있어야 한다.

---

## 7. Web UI에서 보이는 모양

연산자 사이가 forward 연결(같은 병렬도, 1:1)이면 Flink가 하나의 task로 묶는다(chaining). raw · parse(병렬도 3)가 한 박스, `keyBy`에서 끊겨 dedup · clean(병렬도 2)이 한 박스라 경로 하나는 **박스 2개**로 보인다.

```text
[raw.spot.crypto.bookticker → parse.spot.crypto.bookticker] ──keyBy──▶ [dedup.spot.crypto.bookticker → clean.spot.crypto.bookticker]
```

지금은 경로가 1개라 박스 2개, `args`를 9줄 다 켜면 18개다. Checkpoints 탭에서 체크포인트가 10초마다 완료되는지 볼 수 있다. Web UI는 `kubectl port-forward -n binance-processing svc/flink-dedup-rest 8081:8081`로 연다.

---

## 8. 헷갈리기 쉬운 점

- **코드가 yaml을 이긴다.** 같은 설정을 코드와 yaml 양쪽에 쓰면 코드 값이 적용된다. 체크포인트 설정은 이미지 재빌드 없이 바꾸려고 yaml(`flinkConfiguration`)에만 둔다. 병렬도는 연산자마다 다르게(raw · parse 3, dedup · sink 2) 주려고 `DedupJob`에서 `setParallelism`으로 정한다. 그래서 yaml의 `job.parallelism: 1`은 쓰이지 않는다.
- **Kafka key는 source에서 버리고 sink에서 다시 만든다.** 중간 단계에서 쓰는 심볼은 envelope의 `symbol` 필드다.
- **`seen`은 키마다 따로다.** `ValueState` 하나가 키별로 다른 값을 가진다.
- **clean에도 중복이 생길 수 있다.** sink가 at-least-once라 재시작 직후 몇 건이 다시 쓰일 수 있고, TTL 1분보다 늦게 온 중복도 통과한다.
- **IDE 실행은 체크포인트 없이 돈다.** 체크포인트와 MinIO는 클러스터에서 확인한다.
