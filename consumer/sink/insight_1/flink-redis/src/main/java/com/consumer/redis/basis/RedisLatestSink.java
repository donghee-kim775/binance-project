package com.consumer.redis.basis;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import com.consumer.redis.basis.model.BasisUpdate;

/**
 * Redis에 심볼별 최신 basis를 쓰는 Flink sink (Sink V2). 실제 쓰기는 subtask마다 하나씩 만들어지는 RedisLatestWriter가 한다.
 *
 * Sink는 JobManager에서 만들어 TaskManager로 직렬화해 보내는 "설계도"라 연결을 들고 있지 않다.
 * 연결(JedisPooled)은 TaskManager에서 createWriter가 불릴 때 만든다.
 *
 * 전달 보장: at-least-once. 체크포인트 직전에 flush가 불리는데, write가 이미 동기로 Redis에 반영하므로
 * 체크포인트(= Kafka offset 커밋)가 끝난 레코드는 항상 Redis에 들어가 있다. 재시작 때 다시 쓰이는 레코드는
 * Lua의 event_time 비교로 무시된다 (멱등).
 */
public class RedisLatestSink implements Sink<BasisUpdate> {

    private final String redisHost;
    private final int redisPort;

    public RedisLatestSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    // [호출] Flink 런타임 — subtask가 시작될 때 한 번 (TaskManager에서)
    @Override
    public SinkWriter<BasisUpdate> createWriter(WriterInitContext context) {
        return new RedisLatestWriter(redisHost, redisPort, context.metricGroup());   // [다음] RedisLatestWriter
    }
}
