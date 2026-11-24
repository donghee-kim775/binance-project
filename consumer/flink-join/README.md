# flink-join

두 개 이상의 토픽을 심볼로 묶어 계산하는 Flink join job들이다. dedup([../flink-dedup/](../flink-dedup/))과는 jar, 이미지, 배포를 따로 둔다. 지금은 **basis job**(Insight 1) 하나다. 설계 이유는 [../flink-design.md](../flink-design.md) 5장에 있다.

```text
binance.clean.spot.crypto.bookticker ──> SpotMidParser ───keyBy(symbol)──┐
  (dedup 끝난 Spot 호가)                  (b + a) / 2                    │
                                                                         ├─connect─> BasisFunction ──> binance.analytics.crypto.basis
binance.raw.usdm.crypto.markprice    ──> MarkPriceParser ─keyBy(symbol)──┘           심볼별 최신 Spot 하나를 state에 둠
  (Active-Standby라 중복 없음, raw 그대로)  p, r, E                                  markPrice가 오면 basis = p − mid
```

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
        ├── BasisJob.java             # main(): 두 source → parse → keyBy → connect → sink
        ├── SpotMidParser.java        # envelope → SpotMid
        ├── MarkPriceParser.java      # envelope → MarkPrice
        ├── BasisFunction.java        # KeyedCoProcessFunction. 최신 Spot state + 신선도 검사
        └── model/  SpotMid, MarkPrice, Basis
```

## 동작

| 항목 | 값 |
| --- | --- |
| 출력 빈도 | markPrice 1건당 1건 (심볼마다 초당 1건). Spot이 오면 state만 바꾸고 출력하지 않는다 |
| Spot 신선도 | `abs(mark.receive_time − spot.receive_time) > 2초`이면 버린다. 둘 다 collector가 찍은 노드 시계다 (Spot bookTicker에는 `E`가 없다) |
| 버린 건수 | Flink metric `basisNoSpot`(그 심볼의 Spot을 아직 못 받음), `basisStaleSpot`(Spot이 2초 넘게 오래됨) |
| 시작 위치 | 커밋된 offset부터. 처음 배포라 커밋이 없으면 **LATEST**. 밀린 데이터를 토픽마다 다른 속도로 따라잡으면 시각이 어긋난 짝이 생긴다 |
| 병렬도 | source · parse 3 (파티션 수), basis · sink 2 → TaskManager 3개 (슬롯 1) |
| 체크포인트 | `s3://binance/flink/analytics/flink-basis/` (10초, `upgradeMode: savepoint`) |
| 계산하지 않는 것 | z-score, alert. basis 출력 위에서 따로 계산한다 |

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
  "basis_time": 1788098181150
}
```

가격은 문자열이다 (Binance 원본처럼 double 오차 없이). `receive_time`은 markPrice 쪽, `basis_time`은 이 job이 계산한 시각이라 `basis_time − receive_time`이 collector → basis 지연이다.

## 실행 (Flink Operator)

```bash
# 출력 토픽 (저장소 루트에서)
kubectl apply -f Infra/Kafka/Kafka-Topic/binance/analytics/crypto

# 이미지 · 배포 (consumer/flink-join/ 에서)
docker build -t kimdonghee125/flink-join:0.0.1 .
docker push kimdonghee125/flink-join:0.0.1
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
