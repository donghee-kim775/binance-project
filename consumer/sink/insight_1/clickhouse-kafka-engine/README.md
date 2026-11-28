# clickhouse-kafka-engine

`binance.analytics.crypto.basis`를 ClickHouse **Kafka Engine**으로 읽어 이력 테이블에 쌓는다. 앱을 따로 띄우지 않고 ClickHouse 안의 테이블 · Materialized View(DDL)로 만든다.

| 항목 | 값 |
| --- | --- |
| 읽기 | Kafka Engine 테이블 `basis_queue` (`isolation.level=read_committed` 명시) |
| 저장 | `basis` — `ReplacingMergeTree(basis_time)`, `ORDER BY (symbol, event_time)` |
| 커밋 | 블록 insert 후 offset 커밋 (at-least-once) → 같은 `(symbol, event_time)`은 merge 때 한 건으로 |
| 조회 | `FINAL` 또는 `argMax` (merge 전 중복이 잠깐 보일 수 있다) |
| 대시보드 | 심볼별 basis 30분 그래프 · 5분 평균 · ±2σ 밴드 · 과열 표시 · 지연 p50 · 알림 이력(`alert_started = 1`) |

설계: [1_2_delivery_guarantee.md 4.3](../../../../Insights/1.spot_futures_basis/1_2_delivery_guarantee.md). ClickHouse 설치는 [Infra/](../../../../Infra/).
