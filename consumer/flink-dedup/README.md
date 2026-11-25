# flink-dedup

Kafka raw 토픽을 읽어 처리하는 Flink 앱이다. jar 하나에 job마다 main 클래스가 하나씩 있다. 지금은 **dedup job** 하나다. 설계 이유는 [../flink-design.md](../flink-design.md)에 있다.

```text
binance.raw.<...>  ──> DedupKeyParser ──keyBy(symbol|id)──> DedupFunction ──> binance.clean.<...>
 (중복 포함)            symbol, id 추출                     처음 본 키만 통과       (dedup_time 추가)
                        형식이 깨진 메시지는 경고 후 버림     state TTL 1분
```

## 구조

```text
flink-dedup/
├── Dockerfile                        # flink:2.2.1-java17 + /opt/flink/usrlib/flink-app.jar
├── deploy/dedup.yaml                 # FlinkDeployment (namespace binance-processing, entryClass = DedupJob)
└── src/main/java/com/consumer/
    ├── common/KafkaIO.java           # KafkaSource / KafkaSink 공통 생성 (모든 job이 같이 쓴다)
    └── dedup/
        ├── DedupJob.java             # main(): 경로마다 source → parse → dedup → sink
        ├── DedupRoute.java           # job 인자 → raw · clean 경로 (경로 목록은 deploy/dedup.yaml)
        ├── DedupKeyParser.java       # envelope → (symbol, id, JSON)
        └── DedupFunction.java        # (symbol, id)가 처음이면 통과
```

## 경로

| raw | clean | dedup 키 |
| --- | --- | --- |
| `binance.raw.spot.crypto.bookticker` | `binance.clean.spot.crypto.bookticker` | `(symbol, u)` |
| `binance.raw.spot.crypto.trade` | `binance.clean.spot.crypto.trade` | `(symbol, t)` |
| `binance.raw.spot.crypto.depth` | `binance.clean.spot.crypto.depth` | `(symbol, u)` |
| `binance.raw.usdm.crypto.bookticker` | `binance.clean.usdm.crypto.bookticker` | `(symbol, u)` |
| `binance.raw.usdm.crypto.aggtrade` | `binance.clean.usdm.crypto.aggtrade` | `(symbol, a)` |
| `binance.raw.usdm.crypto.depth` | `binance.clean.usdm.crypto.depth` | `(symbol, u)` |
| `binance.raw.usdm.stock.bookticker` | `binance.clean.usdm.stock.bookticker` | `(symbol, u)` |
| `binance.raw.usdm.stock.aggtrade` | `binance.clean.usdm.stock.aggtrade` | `(symbol, a)` |
| `binance.raw.usdm.stock.depth` | `binance.clean.usdm.stock.depth` | `(symbol, u)` |

경로를 추가하려면 [deploy/dedup.yaml](deploy/dedup.yaml)의 `job.args`에 `<product>.<자산종류>.<스트림>:<id 필드>` 한 줄을 넣고, raw · clean 토픽 yaml을 [Infra/Kafka/Kafka-Topic/](../../Infra/Kafka/Kafka-Topic/)에 만든다.

## 실행 (Flink Operator)

Operator 설치는 [Infra/Flink-Install/README.md](../../Infra/Flink-Install/README.md)에 있다.

```bash
# 토픽 (저장소 루트에서)
kubectl apply -f Infra/Kafka/Kafka-Topic/binance/raw/crypto -f Infra/Kafka/Kafka-Topic/binance/raw/stock \
              -f Infra/Kafka/Kafka-Topic/binance/clean/crypto -f Infra/Kafka/Kafka-Topic/binance/clean/stock

# 이미지 · 배포 (consumer/flink-dedup/ 에서)
docker build -t kimdonghee125/flink-app:0.0.3 .
docker push kimdonghee125/flink-app:0.0.3
kubectl apply -f deploy/dedup.yaml

kubectl get flinkdeployment -n binance-processing        # JOB STATUS = RUNNING
kubectl get pods -n binance-processing                   # JobManager 1개 + TaskManager 3개 (가장 큰 병렬도 3 ÷ 슬롯 1)
kubectl logs -n binance-processing deploy/flink-dedup -f # JobManager 로그
```

- jar는 `./gradlew shadowJar`로 만든다 (`build/libs/flink-app.jar`). Flink 본체(`flink-streaming-java`, `flink-connector-base`)는 Flink 이미지에 있어 `compileOnly`로 두고, Kafka 커넥터와 Jackson만 jar에 넣는다.
- 코드를 고친 뒤에는 이미지 태그를 올려(예: `0.0.3`) 빌드 · push하고 `deploy/dedup.yaml`의 `image`를 바꿔 apply한다. 태그가 같으면 Operator가 바뀐 것을 모른다.
- Kafka에는 클러스터 안 주소(`binance-cluster-kafka-bootstrap.binancekafka.svc:9092`)로 붙는다.
- [flink-test](../flink-test/)의 Deployment와 **같이 띄우지 않는다.** 둘 다 `binance.clean.spot.crypto.bookticker`에 써서 clean에 중복이 생긴다.

### 실행 환경

| 항목 | 지금 (노드 4대) | 예전 kind |
| --- | --- | --- |
| 이미지 전달 | Docker Hub `kimdonghee125/flink-app`에 push, `imagePullPolicy: IfNotPresent`라 태그를 올려야 새로 받는다 | `kind load docker-image` |
| Kafka 주소 | `binance-cluster-kafka-bootstrap.binancekafka.svc:9092` | 같음 |
| 메모리 | job 하나에 JobManager 1Gi + TaskManager 1.5Gi × 3 (약 5.5Gi) | 노드 여유가 있으면 `taskManager.resource.memory`를 늘린다 |

## 동작 확인

브로커 pod에서 raw에 같은 이벤트를 두 번씩 넣고 clean을 읽는다.

```bash
P=binance-cluster-binance-cluster-pool-0
printf 'BTCUSDT|{"symbol":"BTCUSDT","collector_id":"pod-a","payload":{"u":1,"s":"BTCUSDT"}}\nBTCUSDT|{"symbol":"BTCUSDT","collector_id":"pod-b","payload":{"u":1,"s":"BTCUSDT"}}\n' \
  | kubectl exec -i -n binancekafka $P -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic binance.raw.spot.crypto.bookticker --reader-property parse.key=true --reader-property key.separator='|'

kubectl exec -n binancekafka $P -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic binance.clean.spot.crypto.bookticker --from-beginning --timeout-ms 6000 --property print.key=true
```

pod-a가 보낸 것 한 건만 `dedup_time`이 붙어 나오면 정상이다. Flink Web UI는 `kubectl port-forward -n binance-processing svc/flink-dedup-rest 8081:8081`.

## 지금 단순화한 것

- 병렬도는 코드(`DedupJob`)에서 정한다: raw · parse 3 (raw 파티션 수), dedup · sink 2. `deploy/dedup.yaml`의 `job.parallelism`은 코드 값에 덮여 쓰이지 않는다. 슬롯이 1개라 TaskManager가 3개 뜬다.
- 체크포인트 · savepoint는 MinIO(`s3://binance/flink/clean/flink-dedup/`)에 남고 `upgradeMode: savepoint`로 재배포 때 state를 이어받는다. MinIO가 내려가 있으면 체크포인트가 실패하고, 연속 3번 실패하면 job이 재시작한다. 배포 전에 `binance-processing`에도 Secret `minio-auth`를 만든다 ([deploy/dedup.yaml](deploy/dedup.yaml) 맨 위).
- sink는 at-least-once다.
