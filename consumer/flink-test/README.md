# flink-test

Flink dedup job을 로컬에서 돌려 보는 테스트 폴더다. 수집 앱 대신 노트북이 raw 토픽에 중복 메시지를 넣고, Flink job이 clean 토픽에 한 번씩만 남기는지 확인한다.

```text
notebooks/dedup_test.ipynb ──> binance.raw.spot.crypto.bookticker   (같은 이벤트 2번씩)
                                   │
                                   │ DedupTestJob (로컬 MiniCluster, ./gradlew run)
                                   │ keyBy(symbol|u) → 처음 본 것만 통과, state TTL 1분
                                   ▼
notebooks/dedup_test.ipynb <── binance.clean.spot.crypto.bookticker (dedup_time 추가)
```

| 파일 | 내용 |
| --- | --- |
| [DedupTestJob.java](src/main/java/com/consumer/flinktest/DedupTestJob.java) | Kafka source → `(symbol, u)` dedup → Kafka sink |
| [k8s/topics.yaml](k8s/topics.yaml) | raw · clean 토픽 (파티션 3, 보존 24h) |
| [notebooks/dedup_test.ipynb](notebooks/dedup_test.ipynb) | 중복 메시지 보내기 → clean 결과 확인 |

Flink 2.2.1, Kafka 커넥터 5.0.0-2.2, Java 17.

## 실행 순서

### 1. 토픽 만들기

```bash
kubectl apply -f k8s/topics.yaml
kubectl get kafkatopic -n kafka
```

### 2. 로컬에서 Kafka에 붙을 수 있게 하기

지금 Kafka 리스너는 클러스터 안(`*.kafka.svc`)만 알려 준다. bootstrap 서비스만 port-forward하면 클라이언트가 broker 주소를 받은 뒤 그 주소로 다시 붙으려다 실패한다 (advertised listener).

그래서 **브로커마다 `localhost:<포트>`를 알려 주는 리스너**를 하나 더 두고, 브로커 pod마다 port-forward한다. `Infra/Kafka/Kafka-Cluster/kafka-cluster.yaml`의 `listeners:`에 추가:

```yaml
      # 로컬(노트북, 로컬 Flink) 접속용. 브로커마다 localhost:1909N을 알려 주고, pod마다 port-forward해서 쓴다
      - name: external
        port: 9094
        type: cluster-ip
        tls: false
        configuration:
          brokers:
            - broker: 0
              advertisedHost: localhost
              advertisedPort: 19090
            - broker: 1
              advertisedHost: localhost
              advertisedPort: 19091
            - broker: 2
              advertisedHost: localhost
              advertisedPort: 19092
```

```bash
kubectl apply -f Infra/Kafka/Kafka-Cluster/kafka-cluster.yaml   # 브로커가 하나씩 재시작된다

# 터미널 하나에서 띄워 둔다
kubectl port-forward -n kafka pod/binance-cluster-binance-cluster-pool-0 19090:9094 &
kubectl port-forward -n kafka pod/binance-cluster-binance-cluster-pool-1 19091:9094 &
kubectl port-forward -n kafka pod/binance-cluster-binance-cluster-pool-2 19092:9094 &
```

### 3. Flink job 실행

```bash
./gradlew run
# 다른 주소를 쓰려면: KAFKA_BOOTSTRAP_SERVERS=localhost:19090 ./gradlew run
```

로컬 JVM 안에서 JobManager와 TaskManager가 같이 뜬다 (MiniCluster). Ctrl+C로 멈춘다.

### 4. 노트북 실행

`notebooks/dedup_test.ipynb`를 위에서부터 실행한다. 마지막에 아래처럼 나오면 성공이다.

```text
raw로 보낸 메시지: 200건, 기대하는 clean 건수: 100건
clean에서 받은 건수: 100 / 기대 100
중복: 0건
누락: 0건
```

## 테스트에서 단순화한 것

- 병렬도 1, 로컬 실행. 클러스터 배포(Flink Operator)는 하지 않는다.
- 토픽 1쌍만 다룬다. 실제 dedup job은 raw → clean 여러 쌍을 처리한다.
- sink는 at-least-once다. job이 재시작되면 마지막 체크포인트 이후 메시지가 clean에 다시 쓰일 수 있다.
