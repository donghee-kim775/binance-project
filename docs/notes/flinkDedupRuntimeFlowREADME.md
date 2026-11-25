# Flink dedup job 런타임 흐름: 레코드가 5단계를 지나는 방법

[flinkDedupCodeReadingREADME.md](flinkDedupCodeReadingREADME.md)가 **코드를 읽는 순서**라면, 이 노트는 **job이 떠 있는 동안 레코드 하나가 실제로 어느 스레드 · 어느 메모리 · 어느 네트워크를 지나는지**를 따라간다.
[DedupJob.addRoute](../../consumer/flink-dedup/src/main/java/com/consumer/dedup/DedupJob.java)의 5단계를 기준으로 정리했다.

```text
1. raw     KafkaSource          (raw 토픽 → String)
2. parse   DedupKeyParser       (String → Tuple3(symbol, id, json))
3. keyBy   "symbol|id"          (연산자가 아니라 연결선. 여기서 셔플)
4. dedup   DedupFunction        (처음 본 키만 통과 → Tuple2(symbol, json+dedup_time))
5. clean   KafkaSink            (clean 토픽에 쓰기)
```

기준 환경은 경로 1개(`spot.crypto.bookticker:u`), `parallelism: 1`, TaskManager 1개, 슬롯 1개, state backend hashmap(기본값), raw 토픽 파티션 3개다.

> **지금 배포와 다른 점:** 이 노트는 흐름을 따라가기 쉽게 병렬도 1로 설명한다. 실제 배포는 `DedupJob`이 연산자마다 병렬도를 정해 raw · parse 3, dedup · sink 2이고, 슬롯이 1개라 TaskManager가 3개 뜬다. 병렬도가 늘면 subtask와 스레드가 그만큼 늘고, keyBy 셔플이 subtask 사이 네트워크를 타며, 체크포인트 barrier가 여러 입력 채널에서 정렬(alignment)된다 (6절 끝).

---

## 0. 물리 배치: 무엇이 어디서 도나

`main()`이 만든 그래프는 JobManager에서 **task 2개**로 바뀐다. 연산자 사이 연결이 forward(같은 병렬도, 1:1)면 Flink가 하나의 task로 묶고(chaining), `keyBy`에서 묶음이 끊기기 때문이다.

```text
┌─ JobManager pod ───────────────────────────────────────────────┐
│  CheckpointCoordinator   10초마다 체크포인트 시작               │
│  KafkaSourceEnumerator   raw 토픽 파티션(split)을 찾아 나눠 줌  │
└────────────────────────────────────────────────────────────────┘
            │ split 할당 · 체크포인트 트리거 (RPC)
            ▼
┌─ TaskManager pod (슬롯 1개) ───────────────────────────────────┐
│                                                                │
│  Task A  [raw → parse]               Task B  [dedup → clean]   │
│  ┌──────────────────────┐            ┌──────────────────────┐  │
│  │ main 스레드(mailbox) │ ─keyBy──▶  │ main 스레드(mailbox) │  │
│  │ + Kafka fetcher 스레드│  (버퍼)    │ + Kafka producer     │  │
│  └──────────────────────┘            │   sender 스레드      │  │
│                                      │ heap state (seen)    │  │
│                                      └──────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

- **task 하나 = main 스레드 하나.** 연산자 코드(`processElement`)는 모두 이 스레드에서 한 번에 하나씩 돈다(mailbox 모델). 체크포인트 barrier 처리와 타이머도 같은 스레드의 mailbox에 들어간다.
- 두 task는 **slot sharing**으로 같은 슬롯에 들어간다. 슬롯 1개로 job 전체가 도는 이유다.
- Kafka I/O는 별도 스레드가 맡는다. 읽기는 fetcher 스레드, 쓰기는 KafkaProducer의 sender 스레드다. main 스레드는 큐에서 꺼내거나 큐에 넣기만 한다.

| 구간 | 레코드 타입 | 이동 방식 |
| --- | --- | --- |
| Kafka → 1 | `byte[]` | 네트워크 (Kafka fetch) |
| 1 → 2 | `String` | **메서드 호출** (같은 task, chaining) |
| 2 → 3 → 4 | `Tuple3<String,String,String>` | **직렬화 → 버퍼 → 역직렬화** (task 경계) |
| 4 → 5 | `Tuple2<String,String>` | **메서드 호출** (같은 task, chaining) |
| 5 → Kafka | `byte[]` key/value | 네트워크 (Kafka produce) |

---

## 1. raw: `KafkaSource`가 레코드를 꺼낸다

[KafkaIO.source](../../consumer/flink-dedup/src/main/java/com/consumer/common/KafkaIO.java)

```text
[job 시작 시, JobManager]
KafkaSourceEnumerator
  │ binance.raw.spot.crypto.bookticker 파티션 0, 1, 2 발견
  │ parallelism 1 → reader 0번에 split 3개를 모두 할당
  ▼
[평소, TaskManager · Task A]
fetcher 스레드 ── KafkaConsumer.poll() 반복 ──▶ elementsQueue (스레드 간 큐)
                                                  │
main 스레드 ── mailbox 루프에서 pollNext() ◀──────┘
  │ ConsumerRecord(value = byte[])
  │ SimpleStringSchema로 UTF-8 → String   (key는 받지 않고 버림)
  │ split 상태에 "파티션 p의 다음 offset = n+1" 기록  ← 체크포인트 때 저장할 값
  ▼
output.collect(json)  →  바로 2단계 processElement 호출
```

- **스레드가 둘로 나뉜다.** poll이 블로킹되어도 main 스레드는 체크포인트 barrier나 다른 mailbox 작업을 처리할 수 있다.
- BTCUSDT만 수집하므로 key 해시로 파티션 하나에만 데이터가 있다. 나머지 두 split은 비어 있는 채로 poll된다.
- 시작 offset은 체크포인트나 savepoint에서 복원하면 **거기 저장된 offset**, 처음 실행이면 컨슈머 그룹 `flink-dedup`의 커밋된 offset, 그것도 없으면 `EARLIEST`다.
- **Kafka에 offset을 커밋하는 건 체크포인트가 완료된 뒤**(`notifyCheckpointComplete`)다. 커밋된 offset은 모니터링(lag 확인)용이고, 복구에 쓰는 진짜 offset은 체크포인트 안에 있다.
- `WatermarkStrategy.noWatermarks()`라 이벤트 시간 처리는 하지 않는다.

---

## 2. parse: `DedupKeyParser`가 키 재료를 꺼낸다

[DedupKeyParser.java](../../consumer/flink-dedup/src/main/java/com/consumer/dedup/DedupKeyParser.java)

```text
Task A main 스레드 (1단계에서 이어지는 같은 호출 스택)
  │ processElement(json)
  │ mapper.readTree(json)
  │   envelope.symbol      → "BTCUSDT"
  │   envelope.payload.u   → "102670132459"
  │ 실패하면 warn 로그 후 return  ← 예외를 던지지 않음 (poison pill 방지)
  ▼
out.collect(Tuple3("BTCUSDT", "102670132459", json))  →  3단계 연결선으로
```

- source와 chaining돼 있어서 **1단계 `collect()` 안에서 2단계 `processElement()`가 그대로 호출된다.** 스레드 전환도, 직렬화도 없다. object reuse가 꺼져 있으면(기본값) chain 사이에서 `serializer.copy()`를 하는데, `String`은 불변이라 복사 없이 같은 객체를 넘긴다.
- JSON 트리는 여기서 버린다. 다음 단계로는 원본 **문자열**을 넘긴다. `JsonNode`를 넘기면 3단계 셔플에서 Kryo 직렬화를 타는데, 문자열은 `StringSerializer`로 싸게 넘어간다.

---

## 3. keyBy: 연산자가 아니라 "어디로 보낼지" 정하는 연결선

[DedupJob.addRoute](../../consumer/flink-dedup/src/main/java/com/consumer/dedup/DedupJob.java) `keyBy(t -> t.f0 + "|" + t.f1)`

```text
[보내는 쪽: Task A]
out.collect(tuple)
  │ RecordWriter + KeyGroupStreamPartitioner
  │ 1) key = keySelector(tuple)                   → "BTCUSDT|102670132459"
  │ 2) keyGroup = murmurHash(key.hashCode()) % 128 (maxParallelism 기본 128)
  │ 3) 대상 subtask = keyGroup × parallelism / 128 → parallelism 1이면 항상 0
  │ 4) TupleSerializer(String×3)로 직렬화 → 네트워크 버퍼(기본 32KB)에 씀
  ▼
버퍼가 가득 차거나 buffer timeout(기본 100ms)이 지나면 내보냄
  │ 같은 TM 안 → LocalInputChannel (메모리 전달, Netty 안 거침)
  │ 다른 TM    → Netty TCP (credit 기반 흐름 제어 = backpressure)
  ▼
[받는 쪽: Task B]
main 스레드 ── 입력 버퍼에서 바이트 → Tuple3 역직렬화
  │ setKeyContextElement: keySelector로 키를 다시 계산
  │ stateBackend.setCurrentKey("BTCUSDT|102670132459")  ← key group도 이때 정해짐
  ▼
4단계 processElement 호출
```

- **keyBy는 그래프에서 노드가 아니다.** Web UI에서도 박스가 아니라 두 박스 사이의 `HASH` 화살표로 보인다. 그래서 `name` · `uid`가 없다.
- **같은 키는 항상 같은 key group, 같은 subtask, 같은 state로 간다.** pod 두 개가 보낸 같은 이벤트가 한 곳에서 만나는 이유이고, dedup이 성립하는 전제다.
- **키는 양쪽에서 두 번 계산한다.** 보내는 쪽은 목적지를 정하려고, 받는 쪽은 state의 현재 키를 맞추려고 계산한다. keySelector가 결정적(같은 입력 → 같은 키)이어야 하는 이유다.
- **같은 TM이어도 직렬화는 한다.** task 경계는 항상 바이트 버퍼로 넘어간다. chaining 구간(1→2, 4→5)과 다른 점이다.
- **지연 측정 주의:** 트래픽이 적으면 레코드가 버퍼에서 최대 100ms(`execution.buffer-timeout.interval`)를 기다릴 수 있다. ④ 지연에서 이상한 계단 모양이 보이면 이 값을 먼저 의심한다.
- **key group은 state의 단위다.** maxParallelism(128) 개로 나뉘고, 나중에 parallelism을 바꾸면 key group 단위로 subtask에 다시 나눠 준다. 그래서 maxParallelism은 한 번 정하면 savepoint 호환 때문에 바꾸기 어렵다.

---

## 4. dedup: `DedupFunction`이 처음 본 키만 통과시킨다

[DedupFunction.java](../../consumer/flink-dedup/src/main/java/com/consumer/dedup/DedupFunction.java)

```text
Task B main 스레드 (현재 키 = "BTCUSDT|102670132459")
  │ processElement(tuple, ctx, out)
  │
  │ seen.value()
  │   TtlValueState → HeapValueState
  │   → StateTable[현재 key group].get(현재 키, VoidNamespace)
  │   → TtlValue(true, 쓴 시각) 또는 null
  │   → 지금 - 쓴 시각 ≥ 1분이면 만료: null 반환 + 항목 삭제
  │
  ├─ null이 아님 → return  (다른 pod가 보낸 같은 이벤트. 아무것도 내보내지 않음)
  │
  └─ null
       │ seen.update(true)  → TtlValue(true, 지금) 저장
       │ readTree(json) → envelope.put("dedup_time", 지금) → writeValueAsString
       ▼
     out.collect(Tuple2("BTCUSDT", json'))  →  바로 5단계 호출
```

```text
시간축으로 보면
t=0ms    pod n2dmt의 사본 도착 → null → seen=true, 내보냄
t=+30ms  pod s5wzv의 사본 도착 → true → 버림 (TTL은 연장되지 않음: OnCreateAndWrite)
t=+60s   TTL 만료 → 다음 접근 때나 점진 정리(incremental cleanup) 때 삭제
t=+61s   같은 키가 또 오면 → null → 다시 통과  (TTL보다 늦은 중복은 못 거름)
```

- **`if (value() != null) ... update(true)`가 락 없이 안전한 이유:** 같은 키는 3단계 덕분에 이 subtask로만 오고, 이 subtask는 main 스레드 하나로 레코드를 하나씩 처리한다. 두 사본이 동시에 `null`을 볼 수 없다.
- **state는 JVM heap의 `StateTable`에 있다** (hashmap backend). key group마다 `CopyOnWriteStateMap`이 있어서, 체크포인트 중에도 처리를 멈추지 않고 스냅샷을 뜰 수 있다(6장).
- **TTL은 processing time 기준이다.** "1분"은 TaskManager 벽시계 기준이다.
- **`dedup_time`도 TaskManager 벽시계다.** collector pod의 `receive_time`과 빼서 지연을 잴 때는 노드 간 시계 차이가 섞인다.

### `seen.value()`는 어떻게 현재 키의 칸을 찾나

[HeapValueState.java#L71-L78](https://github.com/apache/flink/blob/c5f8802be786b7e301f8bcfdd5cc8fcf9b41ba3a/flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/HeapValueState.java#L71-L78)

```java
public V value() {
    final V result = stateTable.get(currentNamespace);   // 키를 넘기지 않는다

    if (result == null) {
        return getDefaultValue();
    }

    return result;
}
```

[StateTable.java#L146-L148](https://github.com/apache/flink/blob/c5f8802be786b7e301f8bcfdd5cc8fcf9b41ba3a/flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/StateTable.java#L146-L148): 위 `stateTable.get`의 구현

```java
public S get(N namespace) {
    return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
}
```

호출을 이어 붙이면 이렇다.

```java
// seen.value()
// ㄴ stateTable.get(currentNamespace);                                                      HeapValueState
//    ㄴ get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);   StateTable
//       ㄴ getMapForKeyGroup(keyGroupIndex).get(key, namespace);                            key group별 Map에서 조회
```

키를 인자로 받지 않는데도 현재 키의 값을 돌려주는 이유는, 키를 `StateTable`이 `keyContext`에서 직접 꺼내기 때문이다.
아래는 Flink 2.2.1 소스(`flink-runtime-2.2.1-sources.jar`)에서 레코드 하나가 `value()`까지 가는 경로다.

```text
① RecordProcessorUtils.getRecordProcessor          레코드마다
     input.setKeyContextElement(record);              먼저 키를 정하고
     input.processElement(record);                    그 다음 DedupFunction.processElement

② AbstractStreamOperator.setKeyContextElement
     key = selector.getKey(record.getValue())         keyBy 람다 t -> t.f0 + "|" + t.f1 → "BTCUSDT|5001"

③ AbstractKeyedStateBackend.setCurrentKey
     keyContext.setCurrentKey(key)
     keyContext.setCurrentKeyGroupIndex(murmurHash(key.hashCode()) % maxParallelism)

④ DedupFunction: seen.value()
⑤ TtlValueState.value()                              TTL을 켜서 한 겹 감싸져 있다. (값, 쓴 시각)을 보고 1분 지났으면 null
⑥ HeapValueState.value()                             stateTable.get(currentNamespace)   ← 위 코드
⑦ StateTable.get(namespace)
     get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace)
     → getMapForKeyGroup(keyGroupIndex).get(key, namespace)
```

- **③이 쓰고 ⑦이 읽는 `keyContext`는 같은 객체다.** `open()`의 `getState(descriptor)`가 `StateTable`을 만들 때 백엔드의 `keyContext`를 그대로 넘겨준다. 그래서 `seen`은 인자 없이도 지금 레코드의 키를 안다.
- **`currentNamespace`는 윈도우용 구분값이다.** 이 job은 윈도우가 없어서 항상 `VoidNamespace`다.
- **`update(true)`도 같은 방식이다.** `stateTable.put(currentNamespace, value)`가 `keyContext`의 현재 키 칸에 쓴다.
- **`seen.get(key)` 같은 API가 없는 이유:** 다른 키의 state는 다른 subtask에 있을 수 있다. 현재 레코드의 키로만 접근하게 막아 둔 것이다.

---

## 5. clean: `KafkaSink`가 clean 토픽에 쓴다

[KafkaIO.sink](../../consumer/flink-dedup/src/main/java/com/consumer/common/KafkaIO.java)

```text
Task B main 스레드 (4단계 collect()에서 이어지는 같은 호출 스택)
  │ KafkaWriter.write(tuple)
  │   FieldBytes(0) → key   = "BTCUSDT".getBytes(UTF-8)
  │   FieldBytes(1) → value = json'.getBytes(UTF-8)
  │   ProducerRecord("binance.clean.spot.crypto.bookticker", key, value)
  │ KafkaProducer.send(record)   ← 비동기. 내부 버퍼에 넣고 바로 return
  ▼
producer sender 스레드
  │ key 해시로 파티션 결정 (기본 파티셔너) → raw와 같은 심볼 → 파티션 규칙
  │ 배치로 묶어 브로커에 전송 → ack 수신
  ▼
clean 토픽
```

- `send()`는 기다리지 않는다. main 스레드는 바로 다음 레코드로 넘어가고, 실제 전송은 sender 스레드가 한다.
- **`AT_LEAST_ONCE`의 의미:** 체크포인트 barrier가 sink에 오면 `flush()`로 지금까지 보낸 레코드의 ack를 **모두 받은 뒤에** 체크포인트에 응답한다. 그래서 "체크포인트 완료 = 그 전 레코드는 clean에 확실히 있음"이 보장된다. 반대로 체크포인트 사이에 쓴 레코드는 복구 때 한 번 더 쓰인다(6장).
- 이 단계에는 state가 없다. 트랜잭션을 쓰지 않으므로 체크포인트에 남길 것도 없다.

---

## 6. 체크포인트: barrier가 5단계를 지나가는 방법

레코드와 같은 길로 **barrier**라는 표시가 흘러간다. barrier 앞의 레코드는 이번 체크포인트, 뒤의 레코드는 다음 체크포인트에 속한다.

```text
JobManager CheckpointCoordinator ── "체크포인트 N 시작" (10초마다) ──▶ Task A
                                                                       │
Task A main 스레드 (mailbox에 들어온 체크포인트 작업)
  1 raw    split 상태 저장: {p0: offset, p1: offset, p2: offset}
  2 parse  state 없음
           barrier N을 출력 채널(keyBy 연결선)로 내보냄
                                     │  레코드 사이에 끼어서 흘러감
                                     ▼
Task B main 스레드 (입력에서 barrier N을 만남)
           입력 채널이 1개라 정렬(alignment) 대기가 없음
  4 dedup  동기 단계: StateTable 스냅샷 (copy-on-write라 순식간)
           비동기 단계: 별도 스레드가 seen 전체를 MinIO checkpoints/에 업로드
                       → 그동안 main 스레드는 다음 레코드를 계속 처리
  5 clean  flush(): 보낸 레코드 ack를 모두 받음
           JobManager에 "N 완료" 응답
                                     │
JobManager: 모든 task가 응답 → 체크포인트 N 완료, 메타데이터 저장 (HA 기록 갱신)
  └─▶ 각 task에 notifyCheckpointComplete(N)
        1 raw  → Kafka 컨슈머 그룹 flink-dedup에 offset 커밋
```

**복구하면** (TaskManager가 죽거나 예외가 나면):

```text
마지막 완료 체크포인트 N
  1 raw    offset ← N의 offset          (그 뒤 레코드를 다시 읽음)
  4 dedup  seen   ← N 시점의 키 목록    (그 뒤에 추가된 키는 없음)
  → N 이후 레코드가 다시 처리됨 → seen에 없으니 다시 통과 → clean에 한 번 더 쓰임 (dedup_time은 다름)
```

- yaml의 `checkpointing.mode: EXACTLY_ONCE`는 **job 안의 state와 offset이 정확히 한 번 반영된다**는 뜻이다. clean 토픽까지의 end-to-end는 sink가 `AT_LEAST_ONCE`라 **at-least-once**다.
- parallelism을 올려서 Task B의 입력 채널이 여러 개가 되면, barrier가 모든 채널에 도착할 때까지 먼저 온 채널을 잠시 막는다(alignment). 이 대기가 길면 unaligned checkpoint를 고려한다.
- hashmap backend는 매번 **전체 state**를 올린다. 지금은 state가 수 MB라 문제가 없다. Checkpoints 탭에서 크기와 소요 시간이 커지면 RocksDB(incremental)를 검토한다.

---

## 7. 한눈에 보기

```text
           Task A (main 스레드 1개)                  Task B (main 스레드 1개)
        ┌───────────────────────────┐            ┌───────────────────────────┐
Kafka ──┤ 1 raw ──call──▶ 2 parse   ├──keyBy────▶│ 4 dedup ──call──▶ 5 clean ├──▶ Kafka
 raw    │ fetcher 스레드   JSON 파싱 │ 직렬화     │ heap state   producer     │   clean
        │ offset 기록     키 추출   │ 버퍼≤100ms │ seen 1분TTL  sender 스레드 │
        └───────────────────────────┘ 역직렬화   └───────────────────────────┘
 state:   offset (split)    없음      setCurrentKey   seen            없음
 uid:     raw.<route>   parse.<route>     -       dedup.<route>   clean.<route>
```

| 단계 | 실행 위치 | 입력 → 출력 | state (체크포인트 대상) | 실패하면 |
| --- | --- | --- | --- | --- |
| 1 raw | Task A, fetcher + main | `byte[]` → `String` | split별 offset | 체크포인트 offset부터 다시 읽음 |
| 2 parse | Task A, main | `String` → `Tuple3` | 없음 | 깨진 메시지는 로그 남기고 버림 |
| 3 keyBy | A → B 버퍼 | 직렬화 / 역직렬화 | 없음 (key group만 정함) | backpressure로 1·2가 느려짐 |
| 4 dedup | Task B, main | `Tuple3` → `Tuple2` | `seen` (키별 Boolean + TTL) | 체크포인트 시점 목록으로 되돌림 |
| 5 clean | Task B, main + sender | `Tuple2` → `byte[]` | 없음 | 체크포인트 이후 분량이 다시 쓰임 |
