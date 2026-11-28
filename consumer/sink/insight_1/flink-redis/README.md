# flink-redis

`binance.analytics.crypto.basis`를 읽어 **Redis에 심볼별 최신값을 쓰고 Pub/Sub으로 알리는** Flink job이다. [basis 대시보드](../../../../Insights/1.spot_futures_basis/1_3_dashboard/)의 지금 상태 표 · KPI · 실시간 push가 이 값을 본다. 설계: [1_2_delivery_guarantee.md](../../../../Insights/1.spot_futures_basis/1_2_delivery_guarantee.md) 4.2 (serving consumer).

```text
binance.analytics.crypto.basis ──> parse.basis ──> redis.latest ──> Redis
  (kstreams-basis, exactly-once)     symbol ·         Lua 한 번:       latest:usdm:<심볼>:basis   hash {event_time, json}
  심볼당 초당 1건                    event_time       비교 → 저장       latest:usdm:basis:symbols  set
                                     (원문 유지)      → 심볼 등록       pubsub:usdm:basis          채널 → 대시보드 WebSocket
                                                      → PUBLISH
```

## 구조

```text
flink-redis/
├── Dockerfile                       # flink:2.2.1-java17 + /opt/flink/usrlib/flink-redis.jar
├── deploy/redis-sink.yaml           # FlinkDeployment flink-basis-redis (namespace binance-processing)
└── src/main/java/com/consumer/redis/
    ├── common/KafkaIO.java          # KafkaSource (read_committed, 커밋 없으면 LATEST)
    └── basis/
        ├── BasisRedisJob.java       # main(): source → parse → sink
        ├── BasisUpdateParser.java   # JSON → BasisUpdate(symbol, event_time, 원문). 깨지면 버림
        ├── RedisLatestSink.java     # Sink V2. subtask마다 writer를 만든다
        ├── RedisLatestWriter.java   # Jedis + Lua(EVALSHA). 멱등 쓰기
        └── model/BasisUpdate.java
```

## 전달 보장: at-least-once + 멱등

Kafka 밖으로 쓰는 sink라 어느 엔진이든 exactly-once 범위 밖이다 ([1_2](../../../../Insights/1.spot_futures_basis/1_2_delivery_guarantee.md) 1절).

1. `write`가 레코드마다 **동기로** Redis에 쓴다.
2. 체크포인트(10초)가 끝나면 그때 Kafka offset을 커밋한다. 그래서 커밋된 offset까지는 항상 Redis에 들어가 있다.
3. 커밋 전에 죽으면, 재시작 후 마지막 체크포인트부터 **다시 쓴다.**
4. Lua가 저장된 `event_time`보다 **클 때만** 쓰고 PUBLISH한다. 다시 쓴 값(같은 `event_time`)과 늦게 온 값(작은 `event_time`)은 무시된다.

```lua
local stored = redis.call('HGET', KEYS[1], 'event_time')
if stored and tonumber(stored) >= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'event_time', ARGV[1], 'json', ARGV[2])
redis.call('SADD', KEYS[2], ARGV[3])
redis.call('PUBLISH', ARGV[4], ARGV[2])
return 1
```

- **왜 Lua인가:** `HGET` → 비교 → `HSET`을 클라이언트에서 나눠 하면, 두 writer가 동시에 쓸 때 옛 값이 새 값을 덮을 수 있다. Lua는 Redis 안에서 한 번에(원자적으로) 돈다.
- **실측 (2026-11-28, 테스트 키):** 100 → 1(저장), 같은 100 → 0, 옛 90 → 0, 새 110 → 1. 저장된 값은 110.
- 체크포인트 모드는 `AT_LEAST_ONCE`다. 어차피 외부 sink라 정확히 한 번은 안 되고, barrier 정렬을 기다리지 않아 가볍다.

## 동작

| 항목 | 값 |
| --- | --- |
| 읽기 | `read_committed` (kstreams-basis가 abort한 트랜잭션의 basis를 쓰지 않는다). 커밋이 없으면 LATEST부터 |
| 병렬도 | 1. 입력이 심볼 수 × 1/s(지금 6/s)라 하나로 충분하다 |
| 체크포인트 | `s3://binance/flink/analytics/flink-basis-redis/` (10초, `upgradeMode: savepoint`) |
| metric | `basisRedisBroken`(깨진 JSON), `redisWritten`(반영), `redisStale`(같거나 옛 `event_time`이라 무시) |
| Redis 주소 | env `REDIS_HOST` · `REDIS_PORT` (`redis.binance-storage.svc:6379`) |

## 배포

```bash
kubectl apply -f Infra/Redis/redis.yaml                                  # Redis가 먼저

cd consumer/sink/insight_1/flink-redis
docker build -t kimdonghee125/flink-redis:0.0.1 .
docker push kimdonghee125/flink-redis:0.0.1
kubectl apply -f deploy/redis-sink.yaml

kubectl get flinkdeployment flink-basis-redis -n binance-processing      # RUNNING / STABLE
kubectl exec -n binance-storage deploy/redis -- redis-cli SMEMBERS latest:usdm:basis:symbols
```

## 지금 단순화한 것

- **테스트 코드가 없다.** Lua 멱등은 실제 Redis에 테스트 키로 확인했다. Flink 연산자 테스트(`MiniCluster`, Testcontainers Redis)는 아직 없다.
- **이력은 쓰지 않는다.** 30분 그래프 · 지연 p50은 ClickHouse Kafka 엔진이 같은 토픽을 따로 읽어 쌓는다 ([../clickhouse-kafka-engine/](../clickhouse-kafka-engine/)).
- 과열 알림 발송(notifier)은 아직 없다.
