# flink-app

Kafka raw 토픽을 읽어 처리하는 Flink 앱이다. jar 하나에 job마다 main 클래스가 하나씩 있다. 지금은 **dedup job** 하나다. 설계 이유는 [../flink-design.md](../flink-design.md)에 있다.

```text
binance.raw.<...>  ──> DedupKeyParser ──keyBy(symbol|id)──> DedupFunction ──> binance.clean.<...>
 (중복 포함)            symbol, id 추출                     처음 본 키만 통과       (dedup_time 추가)
                        형식이 깨진 메시지는 경고 후 버림     state TTL 1분
```

## 구조

```text
flink-app/
├── Dockerfile                        # flink:2.2.1-java17 + /opt/flink/usrlib/flink-app.jar
├── deploy/dedup.yaml                 # FlinkDeployment (namespace binance-processing, entryClass = DedupJob)
└── src/main/java/com/consumer/
    ├── common/KafkaIO.java           # KafkaSource / KafkaSink 공통 생성 (모든 job이 같이 쓴다)
    └── dedup/
        ├── DedupJob.java             # main(): 경로마다 source → parse → dedup → sink
        ├── DedupRoute.java           # raw → clean 경로 목록 (9개)
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

경로를 추가하려면 [DedupRoute.java](src/main/java/com/consumer/dedup/DedupRoute.java)의 `ALL`에 한 줄 넣고, raw · clean 토픽 yaml을 [Infra/Kafka/Kafka-Topic/](../../Infra/Kafka/Kafka-Topic/)에 만든다.

## 실행 (Flink Operator)

Operator 설치는 [Infra/Flink-Install/README.md](../../Infra/Flink-Install/README.md)에 있다.

```bash
# 토픽 (저장소 루트에서)
kubectl apply -f Infra/Kafka/Kafka-Topic/binance/raw/crypto -f Infra/Kafka/Kafka-Topic/binance/raw/stock \
              -f Infra/Kafka/Kafka-Topic/binance/clean/crypto -f Infra/Kafka/Kafka-Topic/binance/clean/stock

# 이미지 · 배포 (consumer/flink-app/ 에서)
docker build -t flink-app:0.0.1 .
kind load docker-image flink-app:0.0.1 --name study
kubectl apply -f deploy/dedup.yaml

kubectl get flinkdeployment -n binance-processing        # JOB STATUS = RUNNING
kubectl get pods -n binance-processing                   # JobManager 1개 + TaskManager 1개
kubectl logs -n binance-processing deploy/flink-dedup -f # JobManager 로그
```

- jar는 `./gradlew shadowJar`로 만든다 (`build/libs/flink-app.jar`). Flink 본체(`flink-streaming-java`, `flink-connector-base`)는 Flink 이미지에 있어 `compileOnly`로 두고, Kafka 커넥터와 Jackson만 jar에 넣는다.
- 코드를 고친 뒤에는 이미지 태그를 올려(`0.0.2`) 빌드 · load하고 `deploy/dedup.yaml`의 `image`를 바꿔 apply한다. 태그가 같으면 Operator가 바뀐 것을 모른다.
- Kafka에는 클러스터 안 주소(`binance-cluster-kafka-bootstrap.kafka.svc:9092`)로 붙는다.
- [flink-test](../flink-test/)의 Deployment와 **같이 띄우지 않는다.** 둘 다 `binance.clean.spot.crypto.bookticker`에 써서 clean에 중복이 생긴다.

### 클러스터를 옮길 때

| 바꿀 곳 | kind | 다른 클러스터 |
| --- | --- | --- |
| 이미지 전달 | `kind load docker-image` | 레지스트리에 push하고 `deploy/dedup.yaml`의 `image`를 레지스트리 주소로, `imagePullPolicy`는 그대로 또는 `Always` |
| Kafka 주소 | `binance-cluster-kafka-bootstrap.kafka.svc:9092` | Kafka가 같은 클러스터 `kafka` namespace에 있으면 그대로 |
| 메모리 | job 하나에 JobManager 1Gi + TaskManager 1.5Gi | 노드 여유가 있으면 `taskManager.resource.memory`를 늘린다 |

## 동작 확인

브로커 pod에서 raw에 같은 이벤트를 두 번씩 넣고 clean을 읽는다.

```bash
P=binance-cluster-binance-cluster-pool-0
printf 'BTCUSDT|{"symbol":"BTCUSDT","collector_id":"pod-a","payload":{"u":1,"s":"BTCUSDT"}}\nBTCUSDT|{"symbol":"BTCUSDT","collector_id":"pod-b","payload":{"u":1,"s":"BTCUSDT"}}\n' \
  | kubectl exec -i -n kafka $P -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic binance.raw.spot.crypto.bookticker --reader-property parse.key=true --reader-property key.separator='|'

kubectl exec -n kafka $P -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic binance.clean.spot.crypto.bookticker --from-beginning --timeout-ms 6000 --property print.key=true
```

pod-a가 보낸 것 한 건만 `dedup_time`이 붙어 나오면 정상이다. Flink Web UI는 `kubectl port-forward -n binance-processing svc/flink-dedup-rest 8081:8081`.

## 지금 단순화한 것

- 병렬도 1, TaskManager 1개.
- `upgradeMode: stateless`. 체크포인트를 남길 저장소(PV, S3)가 없어서, 재배포하면 state(본 키 목록)는 사라지고 컨슈머 그룹 `flink-dedup`에 커밋된 offset부터 다시 읽는다. state TTL이 1분이라 영향은 재배포 직후 1분 안의 중복 몇 건이다.
- 경로 목록은 코드에 있다. 경로를 바꾸려면 이미지를 다시 빌드한다.
- sink는 at-least-once다.
