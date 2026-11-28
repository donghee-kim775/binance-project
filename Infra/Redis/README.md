# Redis

서빙 계층의 **최신값 캐시 + Pub/Sub**이다 ([servingapp/plan.md](../../servingapp/plan.md)). 기간 집계 · 이력은 ClickHouse가 맡고, Redis는 "지금 값"과 "값이 바뀌었다는 알림"만 맡는다.

| 항목 | 값 |
| --- | --- |
| 이미지 | `redis:7.4-alpine` |
| namespace | `binance-storage` (ClickHouse · MinIO와 같이) |
| 구성 | Deployment 1개, replica 1, 디스크 없음 |
| 클러스터 안 주소 | `redis.binance-storage.svc:6379` (ClusterIP, 비밀번호 없음) |
| 메모리 | `maxmemory 128mb`, `noeviction` (pod limit 192Mi) |
| 매니페스트 | [redis.yaml](redis.yaml) |

## 설치

```bash
kubectl apply -f Infra/Redis/redis.yaml
kubectl exec -n binance-storage deploy/redis -- redis-cli ping   # PONG
```

## 지금 쓰는 키 (Insight 1)

| 키 | 타입 | 쓰는 곳 | 읽는 곳 |
| --- | --- | --- | --- |
| `latest:usdm:<심볼>:basis` | hash `{event_time, json}` | [flink-redis](../../consumer/sink/insight_1/flink-redis/) | [basis 대시보드](../../Insights/1.spot_futures_basis/1_3_dashboard/) 접속 직후 snapshot |
| `latest:usdm:basis:symbols` | set | flink-redis | 대시보드 (심볼 목록. `KEYS`로 찾지 않으려고 따로 둔다) |
| `pubsub:usdm:basis` | Pub/Sub 채널 | flink-redis (Lua 안에서 `PUBLISH`) | 대시보드 (WebSocket push) |

```bash
kubectl exec -n binance-storage deploy/redis -- redis-cli SMEMBERS latest:usdm:basis:symbols
kubectl exec -n binance-storage deploy/redis -- redis-cli HGET latest:usdm:BTCUSDT:basis json
kubectl exec -it -n binance-storage deploy/redis -- redis-cli SUBSCRIBE pubsub:usdm:basis   # 초당 심볼 수만큼 온다
```

## 설정 이유

- **디스크에 쓰지 않는다 (`save ""`, `appendonly no`):** 최신값 캐시라 잃어도 된다. pod가 다시 뜨면 비어서 시작하고, Flink sink가 다음 basis(1초 안)로 다시 채운다. 원본은 Kafka analytics 토픽에 있다.
- **`noeviction`:** 메모리가 차면 쓰기를 거절한다. LRU로 최신값이 조용히 지워지면 대시보드에서 심볼이 사라진 것처럼 보이는데, 쓰기 실패는 Flink job 오류로 드러난다. 심볼마다 hash 하나라 128MB에 닿을 일은 거의 없다.
- **비밀번호 없음 + ClusterIP:** 클러스터 밖에서는 닿지 않는다. NodePort로 열거나 다른 팀이 같은 클러스터를 쓰게 되면 `requirepass`와 Secret을 붙인다.
- **Operator · Sentinel 없이 Deployment 하나:** 캐시라 복제가 없어도 데이터 손실이 없다. 내려가 있는 동안 대시보드의 표 · KPI가 멈추고, 이력 그래프(ClickHouse)는 계속 보인다.

## 지금 단순화한 것

- replica 1이다. pod가 다시 뜨는 몇 초 동안 Flink sink 쓰기가 실패한다. Flink job이 재시작해 마지막 체크포인트부터 다시 쓴다 (Lua의 `event_time` 비교로 중복은 무시된다).
- Pub/Sub은 at-most-once다. 대시보드가 잠깐 끊긴 동안의 알림은 잃지만, 다시 붙으면 snapshot을 받고 다음 basis로 이어진다.
