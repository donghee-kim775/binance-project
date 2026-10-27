# Kafka

이 프로젝트의 Kafka 관련 문서 모음이다.

| 문서 | 내용 |
| --- | --- |
| [topics.md](topics.md) | 전체 토픽 목록 (raw 11 · clean 9 · analytics 1 · 내부 토픽), 파티션, 보존, yaml 위치 |
| [settings.md](settings.md) | 현재 클러스터 구성과 설정 이유, 브로커 기본값·토픽·producer 설정, 우려 지점, 적용하지 않은 것 |
| [kafkaTopicsREADME.md](kafkaTopicsREADME.md) | 초기 토픽 설계 초안과 조인 아이디어 (지금 토픽 이름과 다르다) |

## 다른 곳에 있는 Kafka 관련 문서

| 위치 | 내용 |
| --- | --- |
| [plan/topic/plan.md](../../plan/topic/plan.md) | 토픽 설계 결정 (토픽 단위, crypto/stock 분리, 이름 규칙, 키·파티션) |
| [plan/topic/insight_1_plan.md](../../plan/topic/insight_1_plan.md) | Insight 1의 토픽 흐름과 Flink job |
| [Infra/Kafka/Kafka-Cluster/](../../Infra/Kafka/Kafka-Cluster/) | Strimzi 클러스터, Kafka UI 매니페스트 |
| [Infra/Kafka/Kafka-Topic/](../../Infra/Kafka/Kafka-Topic/) | `KafkaTopic` yaml (작성 예정) |
| [docs/spring/configuration.md](../spring/configuration.md) | 수집 앱의 Kafka 주소 설정 (local / release 프로파일) |

## 현재 상태

- 클러스터: Strimzi, KRaft, Kafka 4.3.1, 브로커 3대 (`kafka` 네임스페이스). 리스너는 클러스터 내부 전용.
- 토픽: 설계만 있고 `KafkaTopic` yaml은 아직 없다.
- 수집 앱: `spring-kafka`가 꺼져 있어 `[KAFKA:SKIPPED]` 로그로만 발행을 대신한다.
- 클러스터 내부 주소: `binance-cluster-kafka-bootstrap.kafka.svc:9092`
