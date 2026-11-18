# 성능 (지연) 개선 사항

파이프라인 단계별 지연에서 발견한 문제와 개선할 것을 모은다. 측정 지점 정의는 [topic/insight_1_plan.md](../topic/insight_1_plan.md)(① ~ ⑥ 지연 측정), 볼 지표는 [Infra/Monitoring/metrics.md](../../Infra/Monitoring/metrics.md).

| # | 개선 사항 | 구간 | 상태 |
| --- | --- | --- | --- |
| 1 | Flink 네트워크 버퍼 타임아웃 낮추기 | Kafka → Flink → dedup | 미적용 |
| 2 | dedup → clean 쓰기 구간 측정 방법 만들기 | dedup → clean | 미적용 |
| 3 | 시작 직후 워밍업 지연 분리 | 수집 앱 → dedup | 미적용 |
| 4 | p99 꼬리 원인 찾기 | Kafka → Flink → dedup | 미확인 |
| 5 | 노드 시계 동기화 확인 | 전 구간 | 일부 확인 |
| 6 | 지연을 상시 지표로 보기 | 전 구간 | 미적용 |

## 현재 측정 (2026-11-18)

clean 메시지 하나에서 `dedup_time − receive_time` = 554ms가 보여 원인을 확인했다. spot-sov(BTC bookTicker, pod 2개)를 60초 켜 두고 Kafka에 남은 raw 35,232건, clean 17,628건을 단계별로 나눴다.

| 구간 (수집 앱 수신 → dedup 통과) | p50 | p90 | p99 | 최대 |
| --- | --- | --- | --- | --- |
| 처음 5초 | 388ms | 651ms | 786ms | 826ms |
| 5초 이후 (평소) | 37ms | 92ms | 171ms | 313ms |

| 단계 (5초 이후) | 계산 | p50 | p90 | 비교하는 시계 |
| --- | --- | --- | --- | --- |
| 수집 앱 내부 | raw `CreateTime − receive_time` | 0ms | 1ms | 같은 pod |
| Kafka → Flink → dedup | `dedup_time − raw CreateTime` | 39ms | 102ms | 수집 pod 노드 ↔ TaskManager 노드 |
| dedup → clean 쓰기 | 잴 수 없음 (개선 사항 2) | - | - | - |

- 554ms 메시지는 수집 앱이 켜지고 4ms 뒤에 받은 첫 메시지였다 (워밍업 구간).
- 평소 지연은 대부분 Kafka → Flink → dedup 구간에서 생긴다.
- 같은 이벤트(`u`)를 두 수집 pod가 받은 시각 차이가 p50 −5ms로 일정해, 노드 간 시계 차이는 수 ms 수준으로 본다.

## 1. Flink 네트워크 버퍼 타임아웃 낮추기

- **문제:** `keyBy`에서 parse(병렬도 3) → dedup(병렬도 2)로 넘길 때 레코드를 네트워크 버퍼(32KB)에 모았다가, 버퍼가 차거나 `execution.buffer-timeout.interval`(기본 100ms)이 지나면 보낸다. 초당 약 180건이면 버퍼가 차기 전에 100ms가 먼저 지나, 레코드마다 0~100ms를 기다린다. 측정한 p50 ≈ 40ms, p90 ≈ 100ms와 맞는다.
- **개선안:** `dedup.yaml`의 `flinkConfiguration`에 `execution.buffer-timeout.interval: 10ms`를 넣는다 (이미지 재빌드 불필요).
- **기대 효과:** 이 구간 p50 39ms → 10ms 안팎, p90 102ms → 수십 ms.
- **trade-off:** 작은 버퍼를 자주 보내 CPU · 네트워크 부담이 늘어난다. 0(레코드마다 전송)은 부담이 더 크다. 지연이 중요한 경로만 낮추려면 코드에서 연산자별 `setBufferTimeout`을 쓴다.
- **확인 방법:** 같은 조건(spot-sov 60초)으로 다시 재서 p50 · p90을 비교한다. 스트림을 늘린 뒤(depth@100ms, USD-M) TaskManager CPU도 본다.
- 재배포가 `upgradeMode: savepoint`로 일어나므로 savepoint에서 이어 시작하는지도 같이 확인할 수 있다.

## 2. dedup → clean 쓰기 구간 측정 방법 만들기

- **문제:** KafkaSink가 Flink 레코드의 timestamp(= raw `CreateTime`)를 그대로 clean 레코드에 쓴다. clean `CreateTime − dedup_time`이 음수로 나와 이 구간을 잴 수 없다.
- **개선안 (후보):**
    - clean 토픽을 `message.timestamp.type: LogAppendTime`으로 바꾼다. 브로커가 쓴 시각이 남는다. 코드 변경은 없지만 원본 시각이 사라지고 브로커 노드 시계가 섞인다.
    - sink 직렬화(`KafkaRecordSerializationSchema`)에서 timestamp를 보내는 시각으로 바꾼다. 코드 변경 · 재빌드가 필요하다.
    - clean을 읽는 쪽(ClickHouse 적재, basis job)에서 읽은 시각을 기록한다. 소비자 입장의 진짜 end-to-end라 최종 기준으로 삼는다.
- **확인할 것:** 토픽 timestamp의 의미를 바꾸면 이후 소비자의 처리(보존 기간, 이벤트 시간)에 영향이 있는지.

## 3. 시작 직후 워밍업 지연 분리

- **문제:** 수집 앱이 켜진 뒤 처음 5초는 p50 388ms, 최대 826ms다. producer 메타데이터 조회 · 연결 생성, JIT 전 실행이 섞인다. 측정에 섞이면 평소 지연을 크게 잘못 읽는다.
- **개선안:** 지연을 잴 때 시작 직후 구간을 떼고 본다. 수집 앱 재시작 때마다 생기는 지연이라, 필요하면 시작할 때 Kafka 연결을 미리 맺어 둔다(첫 발행 전 메타데이터 조회).
- **확인 방법:** 롤링 업데이트(`maxUnavailable: 0`) 중에는 다른 pod가 받고 있어 clean 쪽 지연에 드러나지 않는지 본다.

## 4. p99 꼬리 원인 찾기

- **문제:** 5초 이후에도 p99 171ms, 12초 시점에 300ms를 넘는 20건이 있었다.
- **의심:** 첫 체크포인트(EXACTLY_ONCE 모드의 barrier 정렬), GC.
- **확인 방법:** 꼬리가 생긴 시각을 Flink 체크포인트 완료 시각, GC 로그와 맞춰 본다. 10초 간격으로 반복되는지 본다.

## 5. 노드 시계 동기화 확인

- **문제:** 노드를 넘는 구간은 서로 다른 노드 시계를 빼서 잰다. kind(호스트 하나) 때는 없던 문제다.
- **현재:** dclabnode1은 NTP 동기화가 켜져 있다. 나머지 노드는 ssh가 막혀 직접 확인하지 못했다. 같은 이벤트를 받은 두 수집 pod의 시각 차이로 간접 확인한 값은 수 ms다.
- **개선안:** 노드마다 NTP 상태를 확인하고, 노드 간 시계 차이를 지표(node-exporter `node_timex_offset_seconds`)로 본다.

## 6. 지연을 상시 지표로 보기

- **문제:** 지금은 Kafka에 남은 데이터를 꺼내 한 번 계산했다. 바꾼 뒤 효과를 계속 보려면 상시 지표가 필요하다.
- **개선안:** Flink Prometheus reporter로 연산자 처리량 · `records-lag-max`를 보고, Kafka exporter로 consumer lag를 본다 ([Infra/Monitoring/metrics.md](../../Infra/Monitoring/metrics.md) 7절 적용 순서).
- **참고:** Flink latency marker(`metrics.latency.interval`)는 실제 레코드가 아니라 marker라 버퍼 대기를 그대로 반영하지 않는다. 쓰려면 이 점을 감안한다.
