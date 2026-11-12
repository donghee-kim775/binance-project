# Flink dedup job 코드 읽는 순서

raw 토픽(Active-Active pod 2개가 보낸 중복 포함)을 clean 토픽(중복 제거)으로 옮기는 dedup job을 처음 읽을 때 따라갈 순서를 정리했다.
**배포 yaml → 진입점(main) → 레코드 하나가 흐르는 순서**로 본다. 예시는 `binance.raw.spot.crypto.bookticker`에 실제로 들어온 메시지다.

설계 이유(경로를 job 하나로 묶은 이유, TTL 1분)는 [consumer/flink-design.md](../../consumer/flink-design.md), 실행 방법은 [flink-app/README.md](../../consumer/flink-app/README.md)에 있다.

---

## 0. 큰 그림

```text
deploy/dedup.yaml (FlinkDeployment)
   │  Flink Operator가 읽음
   ▼
JobManager pod ── main(args) 실행 ──▶ DedupJob.main
   │                                    │ args 9개 → DedupRoute.parse → addRoute × 9
   │                                    ▼
   │                         job graph (경로 9개가 서로 독립)
   ▼
TaskManager pod ── 레코드 처리
   KafkaSource → DedupKeyParser → keyBy → DedupFunction → KafkaSink
   (KafkaIO)                                              (KafkaIO)
```

| 순서 | 파일 | 보는 것 |
| --- | --- | --- |
| 1 | [deploy/dedup.yaml](../../consumer/flink-app/deploy/dedup.yaml) | yaml 값이 코드의 어디로 들어가나 |
| 2 | [DedupJob.java](../../consumer/flink-app/src/main/java/com/consumer/dedup/DedupJob.java) `main` | job 조립 |
| 3 | [DedupRoute.java](../../consumer/flink-app/src/main/java/com/consumer/dedup/DedupRoute.java) | 인자 문자열 → raw · clean 토픽 |
| 4 | [DedupJob.java](../../consumer/flink-app/src/main/java/com/consumer/dedup/DedupJob.java) `addRoute` | 경로 하나의 파이프라인 |
| 5 | [KafkaIO.java](../../consumer/flink-app/src/main/java/com/consumer/common/KafkaIO.java) → [DedupKeyParser.java](../../consumer/flink-app/src/main/java/com/consumer/dedup/DedupKeyParser.java) → [DedupFunction.java](../../consumer/flink-app/src/main/java/com/consumer/dedup/DedupFunction.java) → [KafkaIO.java](../../consumer/flink-app/src/main/java/com/consumer/common/KafkaIO.java) | 레코드 하나의 변환 |

---

## 1. `deploy/dedup.yaml` — yaml이 코드로 어떻게 들어가나

collector는 Spring이 yaml을 읽어 `@ConfigurationProperties`에 바인딩했다. 여기서는 **Flink Operator가 yaml을 읽고, 값마다 다른 경로로 넘긴다.**

| yaml 위치 | 어디로 들어가나 | 코드에서 받는 곳 |
| --- | --- | --- |
| `image`, `job.jarURI: local:///opt/flink/usrlib/flink-app.jar` | 이 이미지 안의 jar를 실행 | [Dockerfile](../../consumer/flink-app/Dockerfile)이 jar를 그 경로에 복사 |
| `job.entryClass: com.consumer.dedup.DedupJob` | 이 클래스의 `main` 호출 | `DedupJob.main` |
| `job.args` (9줄) | **`main(String[] args)`의 args 그대로** | `DedupJob.main`의 `for (String arg : args)` |
| `podTemplate...env.KAFKA_BOOTSTRAP_SERVERS` | 컨테이너 환경 변수 | `KafkaIO.bootstrapServers()`의 `System.getenv` |
| `job.parallelism`, `flinkConfiguration` | Flink 런타임 설정 | 코드가 읽지 않는다. Flink가 직접 적용 |
| `job.upgradeMode: stateless` | 재배포 때 state를 버리고 커밋된 offset부터 다시 읽음 | Operator 동작 |

바인딩 같은 건 없다. **args는 그냥 문자열 배열**이고, 이걸 해석하는 건 `DedupRoute.parse`다.

---

## 2. `DedupJob.main` — job 조립

**`main`은 데이터를 처리하지 않는다.** 파이프라인 설계도(job graph)만 만들고 `env.execute()`로 제출한다. 실제 레코드 처리는 그 뒤 TaskManager에서 일어난다.

- `enableCheckpointing(10_000)`: 10초마다 체크포인트를 찍는다. 체크포인트가 완료될 때 Kafka offset이 커밋된다.
- args가 비어 있으면 바로 예외를 던진다 (yaml에서 `job.args`를 빠뜨린 경우).
- args 하나마다 `addRoute`를 한 번 호출 → 경로 9개가 한 job 안에 독립된 파이프라인으로 붙는다.

---

## 3. `DedupRoute.parse` — 인자 문자열 → 경로

```text
"spot.crypto.bookticker:u"
   split(":")  →  ["spot.crypto.bookticker", "u"]
   검증: 콜론 앞이 점으로 나눠 3조각인가, id 필드가 비어 있지 않은가
   ▼
DedupRoute(
  name       = "spot.crypto.bookticker",
  rawTopic   = "binance.raw.spot.crypto.bookticker",
  cleanTopic = "binance.clean.spot.crypto.bookticker",
  idField    = "u")
```

토픽 이름 규칙 `binance.<raw|clean>.<product>.<자산종류>.<스트림>`을 여기서 조립하므로 yaml에 토픽 이름을 직접 쓰지 않는다.

| 인자 | id 필드 의미 |
| --- | --- |
| `*.bookticker:u` | order book update id |
| `spot.crypto.trade:t` | trade id |
| `*.aggtrade:a` | aggregate trade id |
| `*.depth:u` | 이벤트의 마지막 update id |

---

## 4. `DedupJob.addRoute` — 경로 하나의 파이프라인

```java
fromSource(KafkaIO.source(raw))      // String (envelope JSON)
  .process(DedupKeyParser("u"))      // → Tuple3(symbol, id, json)
  .keyBy(symbol + "|" + id)          // → "BTCUSDT|102670132459"
  .process(DedupFunction(1분))       // → Tuple2(symbol, json + dedup_time)
  .sinkTo(KafkaIO.sink(clean))
```

- `.name(...)`: Web UI와 로그에서 보이는 이름.
- `.uid(...)`: savepoint에서 state를 이어 붙일 때 연산자를 구분하는 이름. 지금은 stateless라 실제로 쓰이지 않지만, 경로를 추가 · 삭제해도 다른 경로의 state가 이어지도록 경로 이름으로 고정해 뒀다.

---

## 5. 레코드 하나 따라가기

### ① `KafkaIO.source` — Kafka value를 문자열로 받는다

```json
{"source":"binance","product":"spot","asset_class":"crypto","sector":"sov","stream":"bookTicker",
 "symbol":"BTCUSDT","event_time":null,"receive_time":1794471848261,
 "collector_id":"spot-sov-6995dd6c4d-n2dmt",
 "payload":{"u":102670132459,"s":"BTCUSDT","b":"73561.06000000","B":"3.08305000","a":"73561.07000000","A":"2.32501000"}}
```

- `setValueOnlyDeserializer`라 **Kafka key(`BTCUSDT`)는 읽을 때 버린다.** key는 sink에서 symbol로 다시 만든다.
- 컨슈머 그룹 `flink-dedup`. 커밋된 offset이 있으면 거기서부터, 없으면 맨 앞(`EARLIEST`)부터 읽는다.

### ② `DedupKeyParser` — 키에 필요한 값만 꺼낸다

```text
envelope.symbol      → "BTCUSDT"
envelope.payload.u   → "102670132459"     (idField = args의 콜론 뒤)
출력: Tuple3("BTCUSDT", "102670132459", 원본 json)
```

JSON이 깨졌거나 `symbol` · id가 없으면 **예외를 던지지 않고 warn 로그만 남기고 버린다.** 예외를 던지면 job이 재시작되고, 같은 메시지를 다시 읽어 또 실패하는 일이 반복된다.

### ③ `keyBy` — 같은 키는 같은 곳으로

`"BTCUSDT|102670132459"`의 해시로 보낼 곳을 정한다. **같은 키의 레코드는 항상 같은 DedupFunction 인스턴스, 같은 state로 간다.** 여기서 네트워크 전송(셔플)이 생긴다.

### ④ `DedupFunction` — 실제로 중복을 걸러낸다

```text
pod n2dmt가 보낸 것 도착 → seen.value() == null → seen = true, dedup_time 추가 → 내보냄
pod s5wzv가 보낸 것 도착 → seen.value() == true → return (버림)
1분 뒤                   → TTL로 seen이 지워짐 (키가 계속 쌓이지 않도록)
```

- `seen`은 `ValueState<Boolean>`라 **현재 키의 값만 보인다.** 변수 하나처럼 생겼지만 실제로는 키별 Map이다.
- 출력은 envelope와 payload를 그대로 두고 `dedup_time`만 붙인 JSON이다 (지연 측정용).
- TTL(1분)보다 늦게 도착한 중복은 걸러지지 않는다. 두 pod 사이 도착 시차는 수십 ms라 충분히 넉넉하다.

### ⑤ `KafkaIO.sink` — clean 토픽에 쓴다

- `Tuple2.f0`(`BTCUSDT`)를 key, `f1`(json)을 value로. `FieldBytes`가 각각을 UTF-8 바이트로 바꾼다.
- key가 raw와 같은 심볼이라 clean도 같은 파티션 규칙을 따른다.
- `AT_LEAST_ONCE`: 재시작하면 마지막 체크포인트 이후 레코드가 clean에 한 번 더 쓰일 수 있다.

---

## 6. Web UI에서 보이는 모양

병렬도가 1이고 연산자 사이가 forward 연결이면 Flink가 하나의 task로 묶는다(chaining). `keyBy`에서 묶음이 끊기므로 경로 하나는 **박스 2개**로 보인다.

```text
[raw.spot.crypto.bookticker → parse.spot.crypto.bookticker] ──keyBy──▶ [dedup.spot.crypto.bookticker → clean.spot.crypto.bookticker]
```

경로가 9개라 박스는 모두 18개다. Web UI는 `kubectl port-forward -n binance-processing svc/flink-dedup-rest 8081:8081`로 연다.
