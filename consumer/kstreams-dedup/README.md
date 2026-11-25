# kstreams-dedup

raw 토픽에서 Active-Active 수집 pod 2개가 보낸 중복을 걸러 clean 토픽에 쓰는 **Kafka Streams** 앱이다. Flink dedup([../flink-dedup/](../flink-dedup/))을 대체한다. 하는 일, 경로 인자 형식, dedup 키, TTL(1분)은 Flink 버전과 같다. 코드 읽는 순서는 [docs/notes/kstreamsDedupCodeReadingREADME.md](../../docs/notes/kstreamsDedupCodeReadingREADME.md)에 있다.

```text
binance.raw.<경로>  ──> DedupProcessor ──> binance.clean.<경로>
 (키 = 심볼, 중복 포함)   (symbol|id)를 window store에서 찾음     (dedup_time 추가)
                         처음이면 기록하고 내보냄, 있으면 버림
                         ── 읽은 offset · store 변경 · clean 쓰기를 한 트랜잭션으로 커밋 (exactly-once)
```

## Flink 버전과 다른 점

| 항목 | Flink (`flink-dedup`) | Kafka Streams (이 앱) |
| --- | --- | --- |
| 실행 | Flink Operator, JobManager 1 + TaskManager 3 (약 5.5Gi) | Deployment pod 2개 (512Mi씩) |
| 재시작 뒤 clean 중복 | 생길 수 있음 (sink at-least-once) | 생기지 않음 (`exactly_once_v2`) |
| clean이 읽는 쪽에 보이는 시점 | 바로 (at-least-once) | 커밋 간격(기본 100ms)마다. 읽는 쪽은 `read_committed` |
| 셔플 | `keyBy(symbol\|id)`로 한 번 | **없음.** raw 키가 심볼이라 같은 이벤트는 이미 같은 파티션 → 같은 task |
| state 저장 | heap state + MinIO 체크포인트 | in-memory window store + Kafka changelog 토픽 (`kstreams-dedup-seen-<경로>-changelog`) |
| 같은 심볼 순서 | dedup 병렬도 2라 섞일 수 있음 | 파티션 = task라 offset 순서 그대로 |
| consumer group | `flink-dedup` (체크포인트마다 10초 단위 커밋) | `kstreams-dedup` (100ms마다 커밋 → lag 톱니가 거의 없음) |

## 구조

```text
kstreams-dedup/
├── Dockerfile                      # eclipse-temurin:17-jre + installDist 결과
├── deploy/dedup.yaml               # Deployment kstreams-dedup (namespace binance-processing, replicas 2)
└── src/
    ├── main/java/com/consumer/dedup/
    │   ├── DedupApp.java               # main(): 상태 리스너, 종료 hook
    │   ├── config/
    │   │   └── KafkaConfig.java        # Kafka Streams 설정 (EOS, 복제 3, changelog 보존)
    │   └── dedup/
    │       ├── DedupTopology.java      # 경로마다 raw → dedup → clean, store 붙이기
    │       ├── DedupProcessor.java     # (symbol, id) 추출 → window store 조회 · 기록 → dedup_time
    │       └── DedupRoute.java         # 인자 "spot.crypto.bookticker:u" → raw · clean 토픽
    └── test/java/.../dedup/DedupTopologyTest.java   # TopologyTestDriver (broker 없이)
```

## 동작

- **"봤다" 기록:** window store에 `"BTCUSDT|5001" → 레코드 timestamp`를 넣는다. 새 레코드가 오면 앞뒤 1분 안에 같은 키가 있는지 `fetch`로 본다. 두 pod의 timestamp가 수십 ms 다르므로 어느 쪽이 먼저 와도 잡힌다.
- **지우기:** 보존 기간(1분)이 지난 항목은 store가 지운다. 기준은 벽시계가 아니라 그 task가 본 가장 늦은 레코드 timestamp(stream time)다.
- **깨진 메시지:** JSON이 아니거나 `symbol` · id 필드가 없으면 경고 로그를 남기고 버린다.
- **실패:** 잡히지 않은 예외가 나면 인스턴스를 내리고 `exit(1)`해서 Kubernetes가 pod를 다시 띄운다.
- **pod 2개:** task를 나눠 가진다 (경로 하나면 task 3개 → 2 + 1). 한 pod가 죽으면 남은 pod가 리밸런스로 받아 changelog에서 store를 복원한다. changelog는 보존 2분 · segment 10분으로 만들어(`config/KafkaConfig.create`) 복원할 양이 최대 십여 분치다 (기본값이면 하루치 이상).
- **Flink와 나란히 비교:** `kubectl apply -k compare` — 출력이 `binance.clean.spot.crypto.bookticker.kstreams`로 간다 (env `CLEAN_TOPIC_SUFFIX`). 띄우기 전에 `kstreams-dedup` group offset을 `--to-latest`로 맞춘다.

## 테스트

```bash
# 로컬에 Java가 없으면 Docker로 (consumer/kstreams-dedup/ 에서)
docker run --rm -u $(id -u):$(id -g) -e GRADLE_USER_HOME=/tmp/gradle -v "$PWD":/w -w /w eclipse-temurin:17-jdk ./gradlew test
```

| 테스트 | 확인하는 것 |
| --- | --- |
| 두 pod가 보낸 같은 이벤트 | 먼저 온 하나만 `dedup_time`을 달고 통과 |
| 늦은 timestamp가 먼저 도착 | 순서가 바뀌어도 중복으로 잡음 |
| 심볼이 다르고 id가 같음 | 둘 다 통과 (id는 심볼마다 따로 매김) |
| TTL보다 늦은 중복 | 통과 (Flink 버전과 같은 trade-off) |
| 깨진 메시지 | 버리고 다음 메시지는 처리 |

## 전환 절차 (Flink → Kafka Streams)

**두 앱을 같이 띄우지 않는다.** 둘 다 clean에 쓰므로 모든 이벤트가 두 번 들어간다. 그리고 새 consumer group(`kstreams-dedup`)은 커밋된 offset이 없어서 그대로 띄우면 raw 24시간치를 처음부터 다시 clean에 쓴다. 그래서 Flink를 멈추고 **Flink가 멈춘 위치를 넘겨받은 뒤** 띄운다.

```bash
B=binance-cluster-binance-cluster-pool-0
K="kubectl exec -n binancekafka $B -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092"

# 1. Flink dedup 정지 (savepoint를 남기고 멈춤. 나중에 되돌릴 수 있다)
kubectl patch flinkdeployment flink-dedup -n binance-processing --type merge -p '{"spec":{"job":{"state":"suspended"}}}'
kubectl get flinkdeployment flink-dedup -n binance-processing   # JOB STATUS = SUSPENDED

# 2. Flink가 마지막으로 커밋한 위치 확인 → 같은 위치로 새 group 만들기
$K --describe --group flink-dedup
#   파티션마다 "토픽,파티션,offset" 한 줄씩 파일로 만든다 (예: binance.raw.spot.crypto.bookticker,2,141184)
#   커밋한 적 없는 파티션(CURRENT-OFFSET이 -)은 0으로 둔다
kubectl exec -i -n binancekafka $B -- sh -c 'cat > /tmp/offsets.csv' < offsets.csv
$K --reset-offsets --group kstreams-dedup --from-file /tmp/offsets.csv --execute

# 3. 배포
kubectl apply -f consumer/kstreams-dedup/deploy/dedup.yaml
kubectl logs -n binance-processing deploy/kstreams-dedup -f   # state REBALANCING -> RUNNING
```

- 수집 앱이 꺼져 있어 raw lag가 0이면 2번은 `--to-latest`로 대신해도 결과가 같다.
- Flink는 체크포인트(10초)마다 커밋하므로, 마지막 커밋과 실제로 멈춘 위치 사이(최대 10초치)는 새 앱이 다시 처리한다. 새 앱의 store는 비어 있어 이 구간이 clean에 한 번 더 들어간다. 전환할 때 한 번만 생긴다.
- 되돌리기: `kubectl delete -f deploy/dedup.yaml` 후 `"state":"running"`으로 patch하면 Flink가 savepoint에서 이어 간다.

## 확인

```bash
# raw와 clean 증가량 비교 (60초). Active-Active 두 pod가 다 떠 있으면 약 2 : 1
kubectl exec -n binancekafka $B -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic binance.raw.spot.crypto.bookticker
kubectl exec -n binancekafka $B -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic binance.clean.spot.crypto.bookticker

# lag
$K --describe --group kstreams-dedup
```

- **clean의 offset 증가량은 메시지 수보다 조금 크다.** 트랜잭션을 커밋할 때마다 파티션에 commit marker가 하나씩 들어가 offset을 하나 차지한다 (100ms 간격이면 쓰는 파티션마다 초당 최대 10개). raw : clean 비율을 정확히 재려면 offset이 아니라 consumer로 메시지 수를 센다.
- 재시작해도 중복이 없는지: pod 하나를 지우고(`kubectl delete pod`) clean을 `--isolation-level read_committed`로 읽어 같은 `(symbol, u)`가 두 번 나오지 않는지 본다.

## 지금 단순화한 것

- **Prometheus 지표가 없다.** Kafka Streams 지표(처리 속도, `process-latency`, `commit-latency`)는 JMX에만 있다. lag와 처리 속도는 kafka-exporter의 consumer group `kstreams-dedup`로 본다. 대시보드 [binance-raw-ingest](../../Infra/Monitoring/dashboards/binance-raw-ingest/)의 `group` 변수를 바꾸면 된다.
- standby replica(`num.standby.replicas`)는 0이다. store가 1분치라 changelog에서 복원하는 시간이 짧아 두지 않았다.
- liveness probe가 없다. 실패하면 프로세스가 끝나도록(`exit(1)`) 해서 대신한다.
