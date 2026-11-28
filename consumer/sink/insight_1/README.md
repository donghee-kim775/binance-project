# sink / insight_1

Insight 1(Spot–Futures basis) 결과를 **Kafka 밖**으로 내보내는 consumer 앱을 둔다. 설계는 [1_2_delivery_guarantee.md](../../../Insights/1.spot_futures_basis/1_2_delivery_guarantee.md)를 따른다.

| 폴더 | 읽는 토픽 | 쓰는 곳 | 멱등 키 | 대시보드 |
| --- | --- | --- | --- | --- |
| [flink-redis/](flink-redis/) (Flink) | `binance.analytics.crypto.basis` | Redis (최신값 + Pub/Sub) | `event_time`이 더 클 때만 갱신 (Lua) | 지금 상태 표 · KPI · 실시간 push |
| [clickhouse-kafka-engine/](clickhouse-kafka-engine/) (DDL) | `binance.analytics.crypto.basis` | ClickHouse `ReplacingMergeTree` | `ORDER BY (symbol, event_time)` | 30분 그래프 · 지연 p50 |
| alert notifier (나중에) | `binance.analytics.crypto.basis.alert` | Slack 등 | `SET alert:{symbol}:{event_time} NX` | 과열 알림 |

- 모두 `read_committed`로 읽고, 쓴 뒤 offset을 커밋한다 (at-least-once). 중복은 멱등 키로 흡수한다.
- Kafka 안의 계산(dedup, basis join, Z · 상태 · 알림)은 [clean_analytics/kstreams-join](../../clean_analytics/kstreams-join/)이 exactly-once로 한다.
- 두 sink가 채운 값을 [basis 대시보드](../../../Insights/1.spot_futures_basis/1_3_dashboard/)가 보여 준다.
- 과열 알림 발송(notifier)은 나중에 만든다. 그 전까지 알림 개수는 Redis(지금 과열 · 주의인 심볼 수)와 ClickHouse(`alert_started = 1` 이력)로 본다.
