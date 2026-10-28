# Kafka 설정 값과 이유

지금 Kafka 클러스터가 어떻게 구성돼 있는지, 각 값을 왜 그렇게 정했는지, 그리고 이 구성에서 우려되는 지점을 차례로 정리한다. 실제 값은 아래 파일이 기준이다.

- 클러스터: [kafka-cluster.yaml](../../Infra/Kafka/Kafka-Cluster/kafka-cluster.yaml), [kafka-ui.yaml](../../Infra/Kafka/Kafka-Cluster/kafka-ui.yaml)
- kind 노드: [kind-study.yaml](../../Infra/kind-study.yaml)
- 토픽: [topics.md](topics.md) (결정 근거는 [plan/topic/plan.md](../../plan/topic/plan.md))
- 수집 앱의 Kafka 주소: [configuration.md](../spring/configuration.md)

---

## 1. 현재 구성

```text
kind 클러스터 "study" (PC 한 대, 노드 3개: control-plane 1 + worker 2)
└── namespace binancekafka
    ├── Strimzi Operator
    ├── Kafka "binance-cluster" (KRaft, 4.3.1)
    │   └── KafkaNodePool "binance-cluster-pool": 노드 3개 (broker + controller 겸용, PVC 5Gi씩)
    ├── Entity Operator (Topic Operator, User Operator)
    └── Kafka UI (NodePort 30080)

namespace binance-ingestion
└── 수집 pod ──> binance-cluster-kafka-bootstrap.binancekafka.svc:9092
```

| 항목 | 값 |
| --- | --- |
| 운영 방식 | Strimzi Operator (Kafka, KafkaNodePool, KafkaTopic CRD) |
| 메타데이터 관리 | KRaft (ZooKeeper 없음) |
| Kafka 버전 | 4.3.1 |
| 노드 | 3개, broker + controller 겸용 |
| 저장소 | 노드당 PVC 5Gi |
| 리스너 | `plain` 9092, `tls` 9093 (둘 다 클러스터 내부 전용) |
| 접속 주소 (클러스터 안) | `binance-cluster-kafka-bootstrap.binancekafka.svc:9092` |
| 모니터링 | Kafka UI, `localhost:30080` |

---

## 2. 클러스터 설정과 이유

### Strimzi Operator

- Kafka를 StatefulSet과 ConfigMap으로 직접 띄우면 브로커 설정, 롤링 재시작, 인증서, 토픽 생성을 전부 손으로 해야 한다.
- Strimzi는 `Kafka`, `KafkaTopic` 같은 yaml만 적으면 Operator가 그 상태로 맞춘다. 토픽도 yaml로 관리할 수 있어 Git에 남는다.

### KRaft, Kafka 4.3.1

- Kafka 4.x는 ZooKeeper를 지원하지 않는다. 메타데이터를 Kafka 노드 안의 controller 쿼럼이 직접 관리한다.
- 띄울 컴포넌트가 하나 줄어 kind 환경에서 자원이 덜 든다.

### 노드 3개

- 복제본 3개를 서로 다른 브로커에 두려면 브로커가 최소 3대 필요하다.
- 3대면 1대가 죽어도 나머지 2대로 읽기·쓰기를 계속할 수 있다 (`min.insync.replicas` 2).

### broker + controller 겸용

- controller 쿼럼은 과반이 살아 있어야 동작한다. 3개면 1개 장애까지 견딘다.
- 운영 환경에서는 controller를 따로 두는 게 일반적이지만, kind 한 대에서 노드 6개를 띄우면 자원이 부족해서 겸용으로 했다.

### 저장소: `persistent-claim` 5Gi, `deleteClaim: false`

- 브로커 pod가 재시작돼도 로그가 남아야 한다. 디스크가 날아가면 복제본을 처음부터 다시 받아야 한다.
- `deleteClaim: false`는 Kafka 리소스를 지워도 PVC를 남긴다. 실수로 클러스터를 지워도 데이터가 남는다.
- 5Gi는 kind(로컬 PC) 디스크에 맞춘 값이다. 실제 유입량은 아직 재지 않았다 (4절).

### 리스너: 내부 전용 `plain` 9092, `tls` 9093

- 수집 앱, Flink, Kafka UI가 모두 클러스터 안에 있어서 외부로 열 필요가 없다.
- 클러스터 안에서는 bootstrap Service 하나로 접속하고, 클라이언트는 broker 목록을 받아 각 broker에 직접 붙는다.
- 그래서 로컬 PC에서 bootstrap만 port-forward하면 붙지 않는다 (4절).

### Entity Operator

- Topic Operator: `KafkaTopic` yaml을 보고 토픽을 만들고 고친다.
- User Operator: `KafkaUser` yaml로 사용자와 ACL을 관리한다. 지금은 인증을 쓰지 않아 사용하지 않는다.

### Kafka UI

- 토픽, 파티션, 컨슈머 lag, 메시지 내용을 브라우저로 보려고 띄웠다.
- kind 설정에서 control-plane 노드의 30080 포트를 호스트에 열어 두었다 (`extraPortMappings`). 그래서 NodePort 30080으로 바로 접속한다.

---

## 3. 브로커 기본값과 토픽 설정

### 브로커 기본값 (`spec.kafka.config`)

| 설정 | 값 | 이유 |
| --- | --- | --- |
| `default.replication.factor` | 3 | 토픽 yaml에 `replicas`를 빼먹어도 RF 3으로 만들어지게 한다 |
| `min.insync.replicas` | 2 | `acks=all`로 보낸 메시지는 최소 2개 브로커에 써져야 성공. 3이면 브로커 1대만 죽어도 쓰기가 멈추고, 1이면 리더 하나에만 써진 상태로 리더가 죽을 때 유실된다 |
| `offsets.topic.replication.factor` | 3 | `__consumer_offsets`(컨슈머가 어디까지 읽었는지)가 사라지면 Flink가 처음부터 다시 읽거나 건너뛴다 |
| `transaction.state.log.replication.factor` / `min.isr` | 3 / 2 | Flink Kafka sink를 exactly-once(트랜잭션)로 쓸 때 쓰는 내부 토픽. 일반 토픽과 같은 기준 |
| `log.retention.hours` | 168 (7일) | 토픽에 보존 기간을 따로 안 적었을 때의 기본값. 가장 긴 raw 보존(trade 7d)과 맞춘다 |
| `log.retention.bytes` | 32MiB (33554432) | **파티션당** 용량 한도. 디스크 5Gi에 맞춘 값 (4.1) |
| `log.segment.bytes` | 16MiB (16777216) | 용량 기준 삭제는 세그먼트 단위라, 기본값 1GiB면 32MiB 한도가 동작하지 않는다 (4.1) |

`unclean.leader.election.enable`은 기본값(`false`)을 그대로 쓴다. 뒤처진 복제본이 리더가 되면 그 사이 메시지가 사라지므로, 유실보다 잠깐 쓰기가 멈추는 쪽을 택한다.

### 토픽 공통 설정

토픽별 파티션·보존은 [topics.md](topics.md)에 있다.

| 설정 | 값 | 이유 |
| --- | --- | --- |
| `replicas` | 3 | 브로커 3대에 하나씩 |
| `min.insync.replicas` | 2 | 브로커 1대 장애까지 쓰기 유지 |
| `cleanup.policy` | `delete` | 시계열 이벤트라 키별 최신값만 남기는 compaction 대상이 아니다 |
| `compression.type` | `zstd` | envelope JSON은 같은 필드 이름이 반복돼 압축률이 높다 |
| `retention.ms` | 스트림별 6h~7d | 양이 많고 금방 가치가 떨어지는 depth·bookTicker는 짧게, 재처리 가치가 큰 trade는 길게 |
| `message.timestamp.type` | `LogAppendTime` (Insight 1 토픽) | 브로커 적재 시각을 남겨 구간별 지연을 잰다 ([insight_1_plan.md](../../plan/topic/insight_1_plan.md)) |

### 파티션 수

- 키가 심볼이라 같은 심볼은 항상 같은 파티션에 순서대로 쌓인다. 파티션이 심볼보다 많으면 남는 파티션은 비어 있다.
- crypto는 지금 BTCUSDT 하나뿐이라 실제로는 파티션 1개에만 쌓인다. 심볼 추가를 대비해 3개로 잡았다.
- stock은 심볼 6개(optical 3 + semi 3)에 맞춰 6개.
- 파티션은 줄일 수 없고, 늘리면 키→파티션 대응이 바뀐다. 그래서 처음에 여유 있게 잡는다.
- Flink에서 조인하는 토픽끼리는 파티션 수를 맞춘다 (co-partitioning).

### Producer 설정 (수집 앱)

[application.yml](../../datasource-ingestion/collector/src/main/resources/application.yml)의 `spring.kafka.producer.*`에 있다. Spring Boot가 이 값으로 `KafkaTemplate`을 만들고, [BinanceRawEventPublisher.java](../../datasource-ingestion/collector/src/main/java/com/ingestion/kafka/BinanceRawEventPublisher.java)가 그것으로 발행한다.

| 설정 | 값 | 이유 |
| --- | --- | --- |
| `acks` | `all` | ISR 전부(최소 2개)에 써진 뒤 성공 처리. `min.insync.replicas`와 짝이어야 의미가 있다 |
| `enable.idempotence` | `true` | 재시도로 같은 메시지가 두 번 써지는 것을 브로커가 막는다 |
| `compression-type` | `zstd` | 토픽 `compression.type`과 같게 해서 브로커가 다시 압축하지 않게 한다 |
| `key-serializer` | `StringSerializer` | 키는 심볼 문자열 |
| `value-serializer` | `StringSerializer` | envelope는 `BinanceRawEventPublisher`가 앱의 `ObjectMapper`로 JSON 문자열로 바꾼다. `@JsonProperty`(snake_case) 규칙이 그대로 적용된다 |

kafka-clients 3.0부터 `acks=all`, `enable.idempotence=true`가 기본값이지만 의도를 남기려고 명시한다.

---

## 4. 우려 지점

### 4.1 디스크가 작아서 용량 기준 삭제가 먼저 일어난다

처음에는 `log.retention.bytes`가 파티션당 50GB라 디스크(5Gi)보다 컸다. 시간 기준(6h~7d)으로 지워지기 전에 디스크가 먼저 차면 브로커가 멈추므로, 디스크에 맞춰 줄였다.

```text
브로커마다 가진 파티션 복제본 = 전체 파티션 수 (RF 3 = 브로커 3대라 모든 브로커가 모든 파티션을 가짐)
                             = raw 45 + clean 36 + analytics 1×3 = 84개

파티션 하나의 최대 크기 ≈ retention.bytes + segment.bytes   (지금 쓰고 있는 활성 세그먼트는 지우지 않음)
                        = 32MiB + 16MiB = 48MiB

84 × 48MiB ≈ 3.9GiB  → 디스크 5Gi의 약 80%. 나머지는 내부 토픽(__consumer_offsets 등)과 여유분
```

- `log.segment.bytes`를 16MiB로 같이 줄였다. 삭제는 세그먼트 파일 단위로만 일어나서, 세그먼트(기본 1GiB)가 한도보다 크면 한도를 넘어도 지울 파일이 없다.
- **대가:** 양이 많은 토픽은 시간 보존(trade 7d 등)보다 훨씬 먼저 지워진다. 실제 보존 기간은 "32MiB가 차는 시간"이 된다.
- 84개는 모든 파티션이 꽉 찬다고 가정한 값이다. 지금 crypto는 BTCUSDT 하나라 대부분의 파티션이 비어 있어 실제 사용량은 훨씬 작다.
- 유입량을 재면 토픽별 `retention.bytes`로 나눠 준다. 많이 쓰는 토픽(depth, bookTicker)에 크게, 거의 비는 파티션에는 작게 준다.

    ```text
    파티션 하나의 필요 용량 = 파티션 초당 유입량 × 원하는 보존 시간
    ```

### 4.2 모든 노드가 PC 한 대 위에 있다

- kind 노드 3개는 같은 PC 위의 컨테이너다. 브로커를 3대로 나눠도 PC나 Docker가 죽으면 전부 같이 죽는다.
- 복제(RF 3)는 **브로커 pod 하나의 장애**만 막는다.
- 노드는 3개지만 control-plane 노드에도 브로커가 뜰 수 있어서, 브로커가 노드별로 고르게 퍼지는지는 확인이 필요하다.

### 4.3 데이터가 Kafka에 들어오기 전에 사라질 수 있다

Kafka 설정은 브로커에 써진 뒤의 데이터만 지킨다. 그 앞 구간은 수집 앱 쪽에서 막아야 한다.

```text
Binance ──①──> 수집 pod ──②──> Kafka 브로커 ──③
```

| 구간 | 무엇이 사라지나 | 막는 장치 |
| --- | --- | --- |
| ① WebSocket | 연결이 끊기거나 pod가 재시작되는 동안 온 메시지 | variable-rate pod Active-Active |
| ② pod → 브로커 | `@Async` 큐·producer 버퍼에 있던 메시지 | Active-Active, `acks=all` + idempotence |
| ③ 브로커 | 브로커 1대 장애 | RF 3, `min.insync.replicas` 2 |

- `ingestExecutor` 큐([AsyncConfig.java](../../datasource-ingestion/collector/src/main/java/com/ingestion/config/AsyncConfig.java))는 200개다. 꽉 차면 기본 정책(`AbortPolicy`)으로 작업을 거절하고 그 메시지는 사라진다.
- pod가 종료될 때 큐와 producer 버퍼에 남은 메시지도 사라진다.
- variable-rate는 다른 pod가 같은 메시지를 보내 보완된다. fixed-rate(markPrice, Active-Standby)는 보완이 없지만 1초 주기라 다음 값으로 금방 채워진다.

### 4.4 로컬 PC에서 바로 붙을 수 없다

- 리스너가 내부 전용이라 broker 주소가 클러스터 안 DNS로 광고된다.
- bootstrap Service만 port-forward하면 첫 연결은 되지만, 이후 broker 주소를 찾지 못한다.
- 로컬 개발(`local` 프로파일, `localhost:9092`)을 하려면 localhost를 광고하는 리스너를 추가하고 broker마다 port-forward하거나, `nodeport` 리스너를 추가해야 한다.

### 4.5 보안 설정이 없다

- `plain` 리스너는 인증·암호화가 없다. 클러스터 안의 누구나 모든 토픽을 읽고 쓸 수 있다.
- 지금은 공개 시장 데이터만 다뤄서 감수한다. 계정 데이터(User Data Stream)를 넣는다면 `KafkaUser`와 ACL로 토픽 접근을 나눈다.

### 4.6 Kafka UI 이미지가 고정되지 않았다

- `provectuslabs/kafka-ui:latest`라 다시 띄울 때마다 버전이 바뀔 수 있다. 버전 태그로 고정하는 게 좋다.

---

## 5. 적용하지 않은 것

| 기능 | 하는 일 | 적용하지 않은 이유 |
| --- | --- | --- |
| Rack awareness (Strimzi `rack.topologyKey`) | 같은 파티션의 복제본을 서로 다른 zone에 둔다 | 노드가 전부 한 PC라 zone이 하나다 |
| MirrorMaker 2 | 다른 Kafka 클러스터로 토픽을 비동기 복사 (DR) | 데이터센터가 하나다. 복사는 Kafka에 들어온 뒤의 일이라 수집 구간(4.3의 ①②) 유실은 막지 못한다 |
| controller 분리 | controller 전용 노드 | kind 자원 부족 (2절) |
| 인증·ACL | 사용자별 토픽 권한 | 공개 시장 데이터만 다룬다 (4.5) |

데이터센터나 zone이 2개 이상인 환경이 생기면 rack awareness부터 넣고, 리전 장애까지 견뎌야 할 때 MirrorMaker 2로 DR 클러스터를 둔다.

---

## 6. 남은 과제

- **유입량 측정 후 토픽별 `retention.bytes` 배분** (4.1)
- **`KafkaTopic` yaml 작성** ([topics.md](topics.md)의 토픽 목록, 지금은 markprice 1개)
- **`ingestExecutor` 거절 정책:** 큐가 찼을 때 버릴지(지금), 호출 스레드에서 처리할지(`CallerRunsPolicy`, WebSocket 수신이 밀림) 정한다.
- **graceful shutdown:** 종료 시 큐와 producer 버퍼를 비우고 내려가게 한다.
- **로컬 접속용 리스너** (4.4): Kafka 연동 작업 때 정한다.
