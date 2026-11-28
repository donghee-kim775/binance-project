# kstreams-join

basis job의 **Kafka Streams 버전**이다. [../flink-join/](../flink-join/)의 Flink basis job과 입력 · 출력 · 계산이 같고, 두 엔진을 같은 조건에서 비교하려고 만들었다. 코드 규칙은 [.claude/rules/consumer-code.md](../../../.claude/rules/consumer-code.md)를 따른다.

```text
binance.clean.spot.crypto.bookticker ──> parse.spot ──> latest-spot (KTable: 심볼별 최신 Spot) ──┐
  (dedup 끝난 Spot 호가)                  (b + a) / 2                                            ├─ join "basis" ─> fresh.basis ─> binance.analytics.crypto.basis
binance.raw.usdm.crypto.markprice    ──> parse.mark ─────────────────────────────────────────────┘   (BasisJoiner)    (2초 신선도)     (비교 실행 땐 .kstreams)
```

## 구조

```text
kstreams-join/
├── Dockerfile                       # eclipse-temurin:17-jre + installDist 결과
├── deploy/basis.yaml                # Deployment kstreams-basis (namespace binance-processing, replicas 2)
├── compare/                         # Flink basis와 나란히 띄우는 overlay (출력 토픽 .kstreams)
└── src/main/java/com/consumer/join/
    ├── BasisApp.java                # main(): 설정 → topology → KafkaStreams → 종료 처리 (kstreams-dedup DedupApp과 같다)
    ├── config/KafkaConfig.java      # Streams 설정 (exactly_once_v2, 시작 위치 latest)
    ├── common/
    │   ├── BinanceEventEnvelope.java # 수집 앱 envelope와 같은 필드 (record)
    │   └── JsonSerde.java            # record ↔ JSON 바이트 (store · changelog · 출력)
    └── basis/
        ├── BasisTopology.java       # Spot → KTable, mark → join → 신선도 → sink
        ├── BasisParser.java         # envelope → SpotMid · MarkPrice (깨지면 null)
        ├── BasisJoiner.java         # (MarkPrice, SpotMid) → Basis 계산
        └── model/  SpotMid, MarkPrice, Basis   (flink-join과 같은 record)
```

## Flink 버전과 비교

| 항목 | Flink ([flink-join](../flink-join/)) | Kafka Streams (여기) |
| --- | --- | --- |
| 최신 Spot을 들고 있는 곳 | `BasisFunction`의 `ValueState` ("latest-spot") | `KTable` + in-memory store "latest-spot" |
| 계산 | `BasisFunction.processElement2` | `BasisJoiner.apply` |
| 셔플 | `keyBy` 두 번 (Spot, mark) | 없음. 두 토픽이 co-partition이라 한 sub-topology, task 3개 |
| state 복구 | MinIO 체크포인트 · savepoint | changelog 토픽 `kstreams-basis-latest-spot-changelog` (compact) |
| 출력 전달 보장 | at-least-once (재시작 때 한 번 더 쓰일 수 있음) | exactly-once (트랜잭션, 커밋 100ms) |
| 버린 건수 | Flink metric `basisNoSpot`, `basisStaleSpot` | 따로 세지 않는다 (inner join · filter로 조용히 버림) |
| 시작 위치 | 커밋 없으면 LATEST | 커밋 없으면 latest (Kafka Streams 기본 earliest를 바꿈) |
| 실행 | FlinkDeployment (JobManager 1 + TaskManager 3) | Deployment (pod 2) |

- basis 필드는 두 버전이 같다 (`symbol`, `event_time`, `mark_price`, `spot_mid`, `basis`, `funding_rate`, `receive_time`, `spot_receive_time`, `spot_age_ms`, `basis_time`). 그래서 `basis_time − receive_time`(collector → basis 지연)을 같은 방법으로 잴 수 있다.
- Kafka Streams 버전은 그 뒤에 **통계 필드**를 더 붙인다 (`BasisStatsProcessor`, Flink 버전에는 없다). 아래 "과열 판정" 참고.

## 과열 판정 (`BasisStatsProcessor`)

join 바로 뒤, 같은 topology · 같은 트랜잭션에서 심볼별로 계산한다 (규칙: `Insights/1.spot_futures_basis/1_1_plan.md` 3장, 전달 보장: `1_2_delivery_guarantee.md`).

| 출력 필드 | 뜻 |
| --- | --- |
| `bps` | basis ÷ spot_mid × 10,000 |
| `mean_bps`, `std_bps` | 이번 값을 넣기 **전**의 최근 평균 · 표준편차 (시간 가중 EMA, 시간 상수 5분. 처음 약 5분은 단순 누적) |
| `z` | (bps − mean_bps) / std_bps. warmup · check_data면 0 |
| `zContWarnCnt` | \|z\| > 2가 연속된 횟수 |
| `status` | `warmup`(표본 60건 미만) · `check_data`(표본은 충분한데 값이 전부 같아 표준편차 0) · `normal` · `warning`(연속 1~2) · `alert`(연속 3 이상) |
| `alert_started` | 연속이 정확히 3이 된 한 건만 `true` (과열 알림 1회) |

- 심볼별 state(평균 · 분산 · 표본 수 · 연속 횟수 · 마지막 event_time)는 store `basis-stats`에 두고 changelog로 복원한다.
- **exactly-once가 필요한 이유:** 같은 basis가 두 번 누적되면 평균 · 분산이 틀어지고 연속 횟수가 늘어 거짓 알림이 난다. 앱이 `exactly_once_v2`라 재시작해도 한 번만 반영된다.
- `event_time`이 이미 반영한 값 이하이면 버린다 (수집기 재연결로 같은 markPrice가 다시 오는 경우의 가드).
- 규칙 값은 `BasisStatsProcessor.RULE` 상수에 있다 (main에서 넘기지 않는다).
- **시각 맞추기:** Kafka Streams는 한 task 안의 두 입력 파티션 중 timestamp가 작은 레코드를 먼저 처리한다. 밀린 데이터를 따라잡을 때도 Spot과 mark의 순서가 비교적 맞는다. Flink 버전은 이 정렬이 없어 시작 위치를 LATEST로 두었다.

## 실행

```bash
# 빌드 · 테스트 (로컬에 JDK가 없으면 컨테이너로)
docker run --rm -u $(id -u):$(id -g) -e GRADLE_USER_HOME=/tmp/gradle -v "$PWD":/w -w /w eclipse-temurin:17-jdk ./gradlew test installDist --no-daemon

# 이미지
docker build -t kimdonghee125/kstreams-join:0.0.2 .
docker push kimdonghee125/kstreams-join:0.0.2

# Flink basis와 나란히 (출력: binance.analytics.crypto.basis.kstreams)
kubectl apply -k compare
kubectl logs -n binance-processing deploy/kstreams-basis | grep "\[basis\] state"   # REBALANCING -> RUNNING

# 끝나면
kubectl delete -k compare
```

입력이 있어야 결과가 나온다. 비교 실행 때 수집은 **BTC 하나만** 켠다: `spot-sov`(Spot BTC bookTicker) + `markprice-usdm-sov`(USD-M BTC markPrice), 그리고 clean을 쓰는 dedup. 켜고 끄는 전체 순서와 비교 방법은 대시보드 문서 [binance-basis-join](../../../Infra/Monitoring/dashboards/binance-basis-join/README.md)에 있다.
