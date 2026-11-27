# Kafka Streams basis 과열 판정(BasisStatsProcessor) 코드 읽는 순서

[consumer/clean_analytics/kstreams-join](../../consumer/clean_analytics/kstreams-join/)은 Spot mid와 mark price를 심볼로 묶어 basis를 내는 앱이다. 여기에 **basis마다 Z-score · 상태(정상 · 주의 · 과열) · 알림을 붙이는 단계**(`BasisStatsProcessor`)가 추가됐다. 이 노트는 **join까지는 이미 이해했다는 전제**로, 바뀐 부분만 따라가는 순서다.

**어디에 끼워졌나 → 무엇을 기억하나 → 한 건을 어떻게 처리하나 → 무엇을 내보내나 → 테스트로 확인** 순서로 본다. 규칙은 [1_1_plan.md 3장](../../Insights/1.spot_futures_basis/1_1_plan.md), 왜 exactly-once가 필요한지는 [1_2_delivery_guarantee.md](../../Insights/1.spot_futures_basis/1_2_delivery_guarantee.md)에 있다.

---

## 0. 큰 그림

```text
clean spot ─▶ parse.spot ─▶ latest-spot (KTable) ─┐
                                                  ├─ join "basis" ─▶ fresh.basis ─▶ basis.stats ─▶ analytics.basis
raw mark   ─▶ parse.mark ─────────────────────────┘                                │
                                                                       store "basis-stats" (심볼별 평균 · 분산 · 연속 횟수)
                                                                                     │ 변경
                                                                                     ▼
                                                                       changelog 토픽 (pod가 바뀌면 복원)

100ms마다 [analytics 쓰기 + latest-spot · basis-stats changelog 쓰기 + 입력 offset] 을 한 트랜잭션으로 커밋
```

| 순서 | 파일 | 보는 것 | 새 파일? |
| --- | --- | --- | --- |
| 0 | `git diff`, `git status` | 바뀐 범위 | — |
| 1 | [BasisTopology.java](../../consumer/clean_analytics/kstreams-join/src/main/java/com/consumer/join/basis/BasisTopology.java) | `0. store 등록`, `5. 통계`, `6. 출력` | 수정 |
| 2 | [BasisStatsRule.java](../../consumer/clean_analytics/kstreams-join/src/main/java/com/consumer/join/basis/BasisStatsRule.java), [BasisStatsProcessor.java](../../consumer/clean_analytics/kstreams-join/src/main/java/com/consumer/join/basis/BasisStatsProcessor.java) `RULE` | 규칙 값 4개 | 새 파일 |
| 3 | [model/BasisStatsState.java](../../consumer/clean_analytics/kstreams-join/src/main/java/com/consumer/join/basis/model/BasisStatsState.java) | 심볼마다 기억하는 값 | 새 파일 |
| 4 | [BasisStatsProcessor.java](../../consumer/clean_analytics/kstreams-join/src/main/java/com/consumer/join/basis/BasisStatsProcessor.java) | 레코드 한 건의 처리 (핵심) | 새 파일 |
| 5 | [model/BasisSignal.java](../../consumer/clean_analytics/kstreams-join/src/main/java/com/consumer/join/basis/model/BasisSignal.java) | 출력 JSON | 새 파일 |
| 6 | [BasisStatsTest.java](../../consumer/clean_analytics/kstreams-join/src/test/java/com/consumer/join/basis/BasisStatsTest.java) | broker 없이 돌려 보기 | 새 파일 |

```bash
cd consumer/clean_analytics/kstreams-join
git status --short .    # 새 파일 · 고친 파일
git diff src/main        # 기존 파일에서 바뀐 줄만
```

---

## 1. BasisTopology: 어디에 끼워졌나

`1~4`(spot · mark · join · fresh)는 그대로다. **단계 두 개가 늘고, 출력 타입이 바뀌었다.**

| 단계 | 코드 | kstreams-dedup에서 비슷한 것 |
| --- | --- | --- |
| `0. store 등록` | `Stores.keyValueStoreBuilder(inMemoryKeyValueStore("basis-stats"), String, JsonSerde<BasisStatsState>)` → `builder.addStateStore(...)` | `seenStoreBuilder` (그쪽은 window store) |
| `5. 통계` | `fresh.process(() -> new BasisStatsProcessor(rule, "basis-stats"), Named.as("basis.stats"), "basis-stats")` | `raw.process(() -> new DedupProcessor(...))` |
| `6. 출력` | 값 Serde가 `JsonSerde<Basis>` → `JsonSerde<BasisSignal>` | — |

- **store를 두 방식으로 만든다.**
  - `latest-spot`은 `toTable(... Materialized.as(...))`이 **알아서** 만든다.
  - `basis-stats`는 Processor가 직접 읽고 쓰는 store라 **먼저 등록**(`addStateStore`)하고, `process()`의 마지막 인자로 "이 단계가 쓴다"고 연결한다.
  - 연결을 빼면 `init`의 `getStateStore`가 실패한다.
- **repartition이 없다.** 키가 계속 심볼이라 join 결과가 같은 task 안에서 바로 stats로 넘어간다.

> **확인할 질문:** `process()`의 세 번째 인자 `BASIS_STATS_STORE`를 지우면 언제 · 어떤 에러가 나나?

---

## 2. BasisStatsRule: 규칙 값

규칙은 `BasisStatsProcessor.RULE` 상수 하나다. main에서 넘기지 않고, 테스트도 같은 값으로 돈다 (평소 구간을 warmup 60건보다 많은 80건 넣는다).

| 값 | 값 | 뜻 |
| --- | --- | --- |
| `window` | 5분 | 평균 · 표준편차를 얼마 동안의 값으로 보나 |
| `zThreshold` | 2.0 | \|Z\|가 이보다 크면 벗어남 |
| `alertConsecutive` | 3 | 몇 번 연속이면 과열 |
| `minSamples` | 60 (1분) | 이만큼 쌓이기 전에는 판정하지 않음 (warmup) |

---

## 3. BasisStatsState: 심볼마다 무엇을 기억하나

Python으로 치면 `state["BTCUSDT"] = {...}` 한 칸이다. 이 record가 store `basis-stats`의 값이다.

| 필드 | 왜 필요한가 |
| --- | --- |
| `meanBps`, `varBps` | Z 계산 (평균, 분산) |
| `bpsSampleCount` | warmup 판정 |
| `zBreachStreak` | 3회 연속 판정 |
| `lastEventTime` | 중복 · 역순 가드 (같은 markPrice가 다시 오면 버림) |

- pod가 죽으면 다른 pod가 changelog 토픽(`<application.id>-basis-stats-changelog`)을 읽어 이 값을 되살린다.
- exactly-once라 **store 변경과 출력이 같은 트랜잭션**이다. 재시작해도 같은 basis가 두 번 누적되지 않는다.

> **확인할 질문:** 이 state가 at-least-once로 복원되면(같은 basis가 두 번 반영되면) 어떤 값이 틀어지나?

---

## 4. BasisStatsProcessor: 한 건을 어떻게 처리하나

뼈대는 `DedupProcessor`와 같다. **생성자(규칙) → `init`(store 꺼내기) → `process`(한 건)**. `process`는 주석 번호 1~5를 순서대로 읽는다.

```text
statsStore.get(symbol)              없으면 BasisStatsState.empty()
1. validate                         event_time ≤ lastEventTime 이면 return (버림, 출력 없음)
                                    spot_mid ≤ 0 이면 return
2. bps     = basis × 10000 ÷ spot_mid
3. warm    = bpsSampleCount ≥ minSamples && 표준편차 > 0
   z       = warm ? (bps − 이전 평균) / 이전 표준편차 : 0
4. zBreachStreak = warm && |z| > 2 ? 이전 + 1 : 0
   status      = warmup / alert(≥3) / warning(>0) / normal
   alertStarted = zBreachStreak == 3          ← 과열이 시작된 그 한 건만
5. updated(...) → statsStore.put            평균 · 분산 갱신
   context.forward(BasisSignal)             [다음] analytics.basis sink
```

### 4.1 Z를 "갱신 전" 값으로 계산하는 이유

튄 값을 먼저 평균에 넣으면 평균이 그쪽으로 끌려가 **자기 자신의 Z가 작아진다.** 그래서 3번에서 Z를 구한 뒤, 5번에서 평균 · 분산을 갱신한다.

### 4.2 updated(): 평균 · 분산을 어떻게 갱신하나

5분 치 300건을 다 들고 있지 않고, **값 두 개(mean, var)만 갱신**한다 (지수 가중 평균).

```text
timeAlpha = 1 − e^(−Δt / 5분)          1초 간격이면 ≈ 0.0033
alpha     = max(timeAlpha, 1 / (n+1))
diff      = x − mean
mean'     = mean + alpha × diff
var'      = (1 − alpha) × (var + alpha × diff²)
```

- **`1/(n+1)`이 왜 있나:** 처음에는 `1/(n+1)`이 커서 **지금까지 전부의 단순 평균 · 분산**이 된다. 약 300건(5분)이 지나면 `timeAlpha`가 이겨서 **최근 5분 위주**로 넘어간다.
- 이게 없으면 분산이 0에서 시작해 처음 몇 분 동안 너무 작게 나온다. 그러면 평범한 값도 \|Z\| > 2가 되어 `warning`이 뜬다 (처음 만들 때 테스트 3개가 이것 때문에 실패했다).

Python으로 직접 돌려 보면 감이 온다.

```python
import math
mean, var, n = 0.0, 0.0, 0
for x in [-4, -6] * 200:
    if n == 0:
        mean = x
    else:
        alpha = max(1 - math.exp(-1 / 300), 1 / (n + 1))
        diff = x - mean
        mean += alpha * diff
        var = (1 - alpha) * (var + alpha * diff * diff)
    n += 1
print(round(mean, 3), round(var ** 0.5, 3))   # ≈ -5, ≈ 1
```

### 4.3 한 건씩 따라가 보기 (테스트 입력, warmup 10건)

평소 −4 · −6을 번갈아 40건 넣고, −15를 네 번, 마지막에 −5를 넣었을 때다. `mean`, `std`는 그 값을 넣기 **전** 값이다.

| # | bps | mean | std | z | 연속 | status | alert_started |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | −4 | 0 | 0 | 0 | 0 | warmup | |
| 3 | −4 | −5.00 | 1.00 | 0 | 0 | warmup | |
| 11 | −4 | −5.00 | 1.00 | 1.0 | 0 | normal | |
| 40 | −6 | −4.97 | 1.00 | −1.03 | 0 | normal | |
| 41 | −15 | −5.00 | 1.00 | **−10.0** | 1 | warning | |
| 42 | −15 | −5.24 | 1.83 | −5.33 | 2 | warning | |
| 43 | −15 | −5.48 | 2.34 | −4.07 | **3** | **alert** | **true** |
| 44 | −15 | −5.70 | 2.72 | −3.41 | 4 | alert | |
| 45 | −5 | −5.91 | 3.03 | 0.30 | 0 | normal | |

- 11번부터 판정한다 (표본 10건).
- 41번에서 처음 벗어나고, 43번에서 연속 3이 되어 **알림이 한 번** 붙는다. 44번은 계속 과열이지만 다시 울리지 않는다.
- 41~44번에서 평균 · 표준편차가 빠르게 따라오는 것은 아직 표본이 적어 `1/(n+1)`(약 0.024)이 쓰이기 때문이다. 운영에서 5분이 지나면 가중치가 0.0033이라 훨씬 천천히 따라온다.
- 45번에서 −5로 돌아오면 연속이 0이 되어 `normal`이다.

> **확인할 질문:** 42번 레코드가 재시작 때문에 두 번 들어왔다면(exactly-once가 아니라면) 표가 어떻게 바뀌나? 몇 번 줄에서 알림이 나나?

---

## 5. BasisSignal: 무엇을 내보내나

```json
{"symbol":"BTCUSDT","event_time":1795739292003,"mark_price":"…","spot_mid":"…","basis":"-31.63","funding_rate":"…",
 "receive_time":…,"spot_receive_time":…,"spot_age_ms":493,"basis_time":…,
 "bps":-5.21,"mean_bps":-4.98,"std_bps":0.31,"z":-0.74,"zBreachStreak":0,"status":"normal","alert_started":false}
```

- `@JsonUnwrapped Basis basis`: `basis` 안의 필드를 **중첩하지 않고 같은 층에 펼쳐** 쓴다. 그래서 기존 소비자(지연 측정 `basis_time − receive_time` 등)가 그대로 동작한다.
- 가격은 기존처럼 문자열(`BigDecimal.toPlainString()`), 통계 값은 `double`(소수 넷째 자리 반올림)이다.
- `status` 값과 대시보드 표기: `warmup`(준비 중) · `normal`(정상) · `warning`(주의) · `alert`(과열). 한국어로 바꾸는 것은 서빙 쪽에서 한다.

---

## 6. BasisStatsTest: broker 없이 돌려 보기

맨 위 주석의 **입력 만드는 법**을 먼저 본다: Spot mid를 100으로 고정하고 mark = 100 + bps/100으로 넣는다 (bps −5 → mark 99.95).

| 테스트 | processor에서 확인하는 곳 |
| --- | --- |
| `outputHasBasisAndStatsFields` | 5장 출력 JSON |
| `warmupDoesNotJudge` | 4번 `warm` 조건 |
| `normalValuesAreNormal` | 4.2 `alpha`의 `1/(n+1)` |
| `threeConsecutiveOutliersRaiseAlertOnce` | 4번 zBreachStreak · alertStarted (4.3 표의 41~44번) |
| `streakResetsWhenBackToNormal` | 4번 "아니면 0" |
| `duplicateEventTimeIsIgnored` | 1번 validate |
| `statsAreKeptPerSymbol` | store 키 = 심볼 |

```bash
# consumer/clean_analytics/kstreams-join/ 에서 (로컬에 JDK가 없으면 컨테이너로)
docker run --rm -u $(id -u):$(id -g) -e HOME=/tmp -e GRADLE_USER_HOME=/tmp/gradle \
  -v "$PWD":/work -w /work eclipse-temurin:17-jdk ./gradlew test --tests '*BasisStatsTest*'
```

**직접 해 볼 것:** `threeConsecutiveOutliersRaiseAlertOnce`의 `readAll()` 다음 줄에 `outs.forEach(System.out::println);`을 넣고 돌린다. 출력 JSON의 `z`, `zBreachStreak`, `status`가 한 건씩 바뀌는 것이 4.3 표와 같은지 본다 (결과는 `build/test-results/test/*.xml`의 `system-out`).

---

## 7. 정리: 기존 코드와 무엇이 같고 다른가

| | join (기존) | stats (추가) |
| --- | --- | --- |
| 입력 | mark + 최신 Spot | join 결과 basis |
| state | `latest-spot` (심볼별 최신 Spot, `toTable`이 만듦) | `basis-stats` (심볼별 평균 · 분산 · 연속, 직접 등록) |
| 중복이 들어오면 | 같은 결과가 한 번 더 (멱등, 무해) | **평균 · 분산 틀어짐, 거짓 알림** → exactly-once 필요 |
| 처리 위치 | 같은 task (repartition 없음) | 같은 task (repartition 없음) |
| Flink 버전 | `BasisFunction` | 없음 (Kafka Streams에만 있다) |
