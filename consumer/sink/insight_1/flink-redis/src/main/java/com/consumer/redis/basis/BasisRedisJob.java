package com.consumer.redis.basis;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.consumer.redis.basis.model.BasisUpdate;
import com.consumer.redis.common.KafkaIO;

/**
 * Insight 1 serving sink. analytics basis → Redis 최신값 + Pub/Sub (Insights/1.spot_futures_basis/1_2_delivery_guarantee.md 4.2).
 *
 *   binance.analytics.crypto.basis ──> parse.basis ──> redis.latest ──> Redis  latest:usdm:<심볼>:basis (hash)
 *     (kstreams-basis, EOS)            (symbol,          (Lua: event_time       latest:usdm:basis:symbols (set)
 *                                       event_time)       더 클 때만)           pubsub:usdm:basis (채널) → 대시보드
 *
 * 이력(30분 그래프 · 지연 p50)은 이 job이 아니라 ClickHouse Kafka 엔진이 쌓는다 (database/clickhouse/table/basis.sql).
 *
 * 전달 보장: at-least-once + 멱등. Kafka 밖으로 쓰는 sink라 어느 엔진이든 exactly-once 범위 밖이다.
 *   체크포인트가 끝나야 offset이 커밋되고, 그 전에 죽으면 다시 쓴다 → Lua의 event_time 비교가 흡수한다.
 */
public class BasisRedisJob {

    static final String GROUP_ID = "flink-basis-redis";
    static final String BASIS_TOPIC = "binance.analytics.crypto.basis";

    // 병렬도 1: 입력이 심볼 수 × 1/s(지금 6/s)라 하나로 충분하다. 파티션 3개를 source 하나가 다 읽는다
    static final int PARALLELISM = 1;

    public static void main(String[] args) throws Exception {
        // 1. 실행 환경 (redis-sink.yaml의 flinkConfiguration이 들어온다: 체크포인트, MinIO 경로)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String redisHost = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

        // [다음] KafkaSource → BasisUpdateParser.processElement → RedisLatestWriter.write

        // 2. analytics basis를 읽는다 (read_committed, 커밋이 없으면 지금부터)
        DataStream<String> basisRaw = env
                .fromSource(KafkaIO.source(BASIS_TOPIC, GROUP_ID),
                        WatermarkStrategy.noWatermarks(), "analytics.basis")
                .uid("analytics.basis")
                .setParallelism(PARALLELISM);

        // 3. 심볼 · event_time만 꺼낸다 (원문은 그대로). 깨진 메시지는 버린다
        DataStream<BasisUpdate> updates = basisRaw
                .process(new BasisUpdateParser())
                .name("parse.basis")
                .uid("parse.basis")
                .setParallelism(PARALLELISM);

        // 4. Redis에 쓴다 (최신값 hash + 심볼 set + PUBLISH)
        updates.sinkTo(new RedisLatestSink(redisHost, redisPort))
                .name("redis.latest")
                .uid("redis.latest")
                .setParallelism(PARALLELISM);

        env.execute("basis-redis");
    }
}
