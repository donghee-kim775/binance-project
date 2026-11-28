# serving-consumer

`binance.analytics.crypto.basis`를 읽어 **Redis에 심볼별 최신값을 쓰고 Pub/Sub으로 알린다.** Gateway(WebSocket)는 Redis만 구독한다 ([servingapp/plan.md](../../../../servingapp/plan.md)).

| 항목 | 값 |
| --- | --- |
| 읽기 | `isolation.level=read_committed`, `enable.auto.commit=false` |
| 커밋 | Redis 반영 후 offset 커밋 (at-least-once) |
| 멱등 | Lua: 저장된 `event_time`보다 클 때만 `HSET` + `PUBLISH` (재처리분 · 늦은 값 무시) |
| 키 | `latest:usdm:<심볼>:basis` (hash: `event_time`, `json`) |
| 대시보드 | 지금 상태 표(basis · bps · 펀딩 · Z · 상태), KPI(감시 심볼 수 · 최대 \|Z\| · 평균 bps · 과열 · 주의 개수) |

설계: [1_2_delivery_guarantee.md 4.2](../../../../Insights/1.spot_futures_basis/1_2_delivery_guarantee.md)
