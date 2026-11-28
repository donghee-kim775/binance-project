# flink-join

두 개 이상의 토픽을 심볼로 묶어 계산하는 Flink join job들이다. dedup([../flink-dedup/](../../raw_clean/flink-dedup/))과는 jar, 이미지, 배포를 따로 둔다. 지금은 **basis job**(Insight 1) 하나다. 설계 이유는 [../flink-design.md](../../flink-design.md) 5장에 있다. 같은 일을 하는 Kafka Streams 버전은 [../kstreams-join/](../kstreams-join/), 둘의 비교 대시보드는 [binance-basis-join](../../../Infra/Monitoring/dashboards/binance-basis-join/README.md)이다.

```text
binance.clean.spot.crypto.bookticker ──> parse.spot ──keyBy(symbol)──┐
  (dedup 끝난 Spot 호가)                  (b + a) / 2                │
                                                                     ├─connect─> basis ──> basis.stats ──> binance.analytics.crypto.basis
binance.raw.usdm.crypto.markprice    ──> parse.mark ──keyBy(symbol)──┘  최신 Spot     bps · 평균 · 표준편차 · Z
  (Active-Standby라 중복 없음, raw 그대로)  p, r, E                     state         연속 횟수 · status (state basis-stats)
```

kstreams-join과 노드 이름 · 클래스 이름(`BasisParser`, `Basis`, `BasisSignal`, `BasisStatsState`, `BasisStatsRule`) · 출력 JSON 필드가 같다.

## 구조

```text
flink-join/
├── Dockerfile                        # flink:2.2.1-java17 + /opt/flink/usrlib/flink-join.jar
├── deploy/basis.yaml                 # FlinkDeployment flink-basis (namespace binance-processing)
└── src/main/java/com/consumer/join/
    ├── common/
    │   ├── KafkaIO.java              # KafkaSource / KafkaSink. 커밋된 offset이 없으면 LATEST부터
    │   └── BinanceEventEnvelope.java # 수집 앱 envelope와 같은 필드 (record)
    └── basis/
        ├── BasisJob.java             # main(): 두 source → parse → keyBy → connect → stats → sink
        ├── BasisParser.java          # envelope → SpotMid · MarkPrice (깨지면 null). kstreams-join과 같다
        ├── ParseFunction.java        # BasisParser를 감싼 연산자. null이면 내보내지 않는다 (= 버림)
        ├── BasisFunction.java        # KeyedCoProcessFunction. 최신 Spot state + 신선도 검사 → Basis
        ├── BasisStatsFunction.java   # KeyedProcessFunction. 심볼별 EWMA 평균 · 분산 → Z · status (kstreams BasisStatsProcessor와 같다)
        ├── BasisStatsRule.java       # 판정 규칙 값 묶음 (5분 · |Z| 2.0 · 3회 · 60건)
        └── model/  SpotMid, MarkPrice, Basis, BasisSignal, BasisStatsState
```

## 동작

| 항목 | 값 |
| --- | --- |
| 출력 빈도 | markPrice 1건당 1건 (심볼마다 초당 1건). Spot이 오면 state만 바꾸고 출력하지 않는다 |
| Spot 신선도 | `abs(mark.receive_time − spot.receive_time) > 2초`이면 버린다. 둘 다 collector가 찍은 노드 시계다 (Spot bookTicker에는 `E`가 없다) |
| 버린 건수 | Flink metric `basisNoSpot`(그 심볼의 Spot을 아직 못 받음), `basisStaleSpot`(Spot이 2초 넘게 오래됨), `basisStatsOldEvent`(이미 반영한 event_time 이하 → 통계에 넣지 않음) |
| 시작 위치 | 커밋된 offset부터. 처음 배포라 커밋이 없으면 **LATEST**. 밀린 데이터를 토픽마다 다른 속도로 따라잡으면 시각이 어긋난 짝이 생긴다 |
| 병렬도 | source · parse 3 (파티션 수), basis · basis.stats · sink 2 → TaskManager 3개 (슬롯 1). basis → basis.stats는 같은 키라 셔플 없이 잇는다 (`reinterpretAsKeyedStream`) |
| 체크포인트 | `s3://binance/flink/analytics/flink-basis/` (10초, `upgradeMode: savepoint`) |
| 통계 · 판정 | basis.stats가 붙인다. 규칙과 출력 필드는 [kstreams-join README](../kstreams-join/README.md)와 같다 (warmup · check_data · normal · warning · alert) |

출력 메시지 (`binance.analytics.crypto.basis`, key = 심볼):

```json
{
  "symbol": "BTCUSDT",
  "event_time": 1788098181003,
  "mark_price": "78853.70000000",
  "spot_mid": "78760.565",
  "basis": "93.135",
  "funding_rate": "0.00008778",
  "receive_time": 1788098181012,
  "spot_receive_time": 1788098180650,
  "spot_age_ms": 362,
  "basis_time": 1788098181150,
  "bps": 11.8251,
  "mean_bps": 10.9312,
  "std_bps": 0.4923,
  "z": 1.8157,
  "zContWarnCnt": 0,
  "status": "normal",
  "alert_started": false
}
```

가격은 문자열이다 (Binance 원본처럼 double 오차 없이). `receive_time`은 markPrice 쪽, `basis_time`은 이 job이 계산한 시각이라 `basis_time − receive_time`이 collector → basis 지연이다.

## 실행 (Flink Operator)

```bash
# 출력 토픽 (저장소 루트에서)
kubectl apply -f Infra/Kafka/Kafka-Topic/binance/analytics/crypto

# 이미지 · 배포 (consumer/clean_analytics/flink-join/ 에서)
docker build -t kimdonghee125/flink-join:0.0.3 .
docker push kimdonghee125/flink-join:0.0.3
kubectl apply -f deploy/basis.yaml

kubectl get flinkdeployment -n binance-processing        # flink-basis JOB STATUS = RUNNING
kubectl logs -n binance-processing deploy/flink-basis -f # JobManager 로그

# 결과 확인
kubectl exec -n binancekafka binance-cluster-binance-cluster-pool-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic binance.analytics.crypto.basis --timeout-ms 6000 --property print.key=true
```

- dedup job(`flink-dedup`)이 떠 있어야 clean bookTicker가 들어온다.
- 코드를 고친 뒤에는 이미지 태그를 올려 빌드 · push하고 `deploy/basis.yaml`의 `image`를 바꿔 apply한다.
- MinIO Secret `minio-auth`는 dedup 배포 때 `binance-processing`에 만든 것을 같이 쓴다.

## co-partitioning 실험 (`BasisCoPartitionJob`)

`BasisJob`은 두 입력을 `keyBy(symbol)`로 다시 나눈다(셔플). Kafka에서 이미 심볼로 나뉜 데이터를 그대로 이어받을 수 있는지 보려고, **keyBy 없이 `reinterpretAsKeyedStream`으로 받는 판**을 따로 둔다. 기본 경로는 건드리지 않고, 모든 토픽 · job에 `.copart` / `-copart`를 붙여 나란히 띄운다.

```text
수집기 markprice-usdm-sov-copart ──(FlinkKeyGroupPartitioner)──▶ binance.raw.usdm.crypto.markprice.copart ─┐
raw spot ─▶ flink-dedup-copart ──(CLEAN_PARTITIONER=flink-keygroup)──▶ binance.clean.spot.crypto.bookticker.copart ─┤
                                                                                                                    ▼
                                     flink-basis-copart (병렬도 3, source → parse → check → basis → sink 한 태스크)
                                                                                                                    ▼
                                                                                    binance.analytics.crypto.basis.copart
```

**왜 쓰는 쪽까지 바꾸나:** `reinterpretAsKeyedStream`은 데이터를 옮기지 않고 "이미 keyBy된 상태"라고 믿기만 한다. 그래서 Kafka 파티션 i의 데이터가 **처음부터 그 키의 주인 subtask**에 도착해야 한다. Kafka 기본(murmur2)으로 쓰면 6심볼 중 5개가 어긋난다. 두 partitioner는 다음 두 규칙을 맞춘다.

1. keyBy였다면 갔을 subtask: `murmurHash(symbol.hashCode()) % 128` → key group → `keyGroup × 3 / 128`
2. KafkaSource split 배정: 파티션 p를 읽는 subtask = `(토픽 이름 해시 + p) % 3` (connector 5.0.0). 이걸 거꾸로 풀어 파티션을 고른다

| 쓰는 쪽 | 코드 | 설정 |
| --- | --- | --- |
| 수집기 (mark) | `datasource-ingestion/collector/.../kafka/FlinkKeyGroupPartitioner` (Flink 해시를 복사) | `deploy/compare-copart`의 ConfigMap `partitioner.class` |
| flink-dedup (clean) | `consumer/raw_clean/flink-dedup/.../common/FlinkKeyGroupPartitioner` | `compare-copart`의 env `CLEAN_PARTITIONER=flink-keygroup` |

- 두 계산이 같은지: flink-dedup `FlinkKeyGroupPartitionerTest`가 connector의 실제 split 배정 함수와 Flink key group 계산으로 확인하고, 수집기 테스트가 같은 기대값 표를 쓴다.
- 그래도 어긋나면(파티션 수 · 병렬도 · maxParallelism 변경) `SubtaskCheck`가 예외 대신 `basisWrongSubtask`로 세고 버린다.

**지켜야 하는 것** (하나를 바꾸면 전부 같이): 두 토픽 파티션 수 = basis 병렬도 = 3, maxParallelism = 128 (job · 두 partitioner), connector 버전 (올리면 flink-dedup 테스트로 확인).

```bash
# 저장소 루트에서. 기본 수집(common.yaml, spot-sov.yaml)이 떠 있어야 한다

# 1. 이미지 (태그를 올린다)
docker build -f datasource-ingestion/deploy/Dockerfile -t kimdonghee125/ingestion-collector:0.0.2 datasource-ingestion/collector
docker build -t kimdonghee125/flink-app:0.0.4 consumer/raw_clean/flink-dedup
docker build -t kimdonghee125/flink-join:0.0.4 consumer/clean_analytics/flink-join
docker push kimdonghee125/ingestion-collector:0.0.2
docker push kimdonghee125/flink-app:0.0.4
docker push kimdonghee125/flink-join:0.0.4

# 2. 쓰는 쪽 두 개 (토픽도 같이 만든다)
kubectl apply -k datasource-ingestion/deploy/compare-copart
kubectl apply -k consumer/raw_clean/flink-dedup/compare-copart

# 3. basis
kubectl apply -k consumer/clean_analytics/flink-join/compare-copart

# 4. 확인: 어긋난 레코드가 없어야 한다 (basisWrongSubtask 로그가 없어야 함), 출력이 나와야 한다
kubectl logs -n binance-processing -l app=flink-basis-copart,component=taskmanager --tail=200 | grep 'wrong subtask'
kubectl exec -n binancekafka binance-cluster-binance-cluster-pool-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic binance.analytics.crypto.basis.copart --max-messages 5 --property print.key=true

# 끄기 (common.yaml이 없어 namespace는 지워지지 않는다)
kubectl delete -k consumer/clean_analytics/flink-join/compare-copart
kubectl delete -k consumer/raw_clean/flink-dedup/compare-copart
kubectl delete -k datasource-ingestion/deploy/compare-copart
```

지연 비교는 위 "출력 JSON"의 `basis_time − receive_time`을 `binance.analytics.crypto.basis`(keyBy)와 `.copart`에서 각각 읽어 본다.
