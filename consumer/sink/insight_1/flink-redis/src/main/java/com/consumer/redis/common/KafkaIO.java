package com.consumer.redis.common;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Kafka source 생성 코드. flink-join의 KafkaIO.source와 같다 (sink job이라 Kafka 쓰기는 없다).
 * 프로젝트를 나눠 클래스를 공유하지 않으므로, 연결 방식이 바뀌면 같이 고친다.
 */
public final class KafkaIO {

    private KafkaIO() {
    }

    public static String bootstrapServers() {
        return System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19090");
    }

    /**
     * 커밋된 offset부터 읽는다. 처음 실행이라 커밋이 없으면 **지금부터** 읽는다 (Redis에는 최신값만 있으면 된다).
     * offset은 체크포인트가 끝날 때 커밋된다 → Redis에 쓴 뒤 커밋 (at-least-once).
     */
    public static KafkaSource<String> source(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers())
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // analytics 토픽은 kstreams-basis가 트랜잭션(exactly-once)으로 쓴다. 커밋된 것만 읽어야
                // 중단(abort)된 트랜잭션의 basis = 다시 계산될 값을 Redis에 쓰지 않는다
                .setProperty("isolation.level", "read_committed")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
