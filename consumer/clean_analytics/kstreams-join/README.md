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
        ├── SpotMidParser.java       # envelope → SpotMid (깨지면 null)
        ├── MarkPriceParser.java     # envelope → MarkPrice (깨지면 null)
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

- 출력 JSON 필드는 두 버전이 같다 (`symbol`, `event_time`, `mark_price`, `spot_mid`, `basis`, `funding_rate`, `receive_time`, `spot_receive_time`, `spot_age_ms`, `basis_time`). 그래서 `basis_time − receive_time`(collector → basis 지연)을 같은 방법으로 잴 수 있다.
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
