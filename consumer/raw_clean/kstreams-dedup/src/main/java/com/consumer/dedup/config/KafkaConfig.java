package com.consumer.dedup.config;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Kafka Streams 설정. DedupApp.main이 KafkaStreams를 만들 때, 테스트가 TopologyTestDriver를 만들 때 쓴다.
 *
 * 값을 들고 있지 않고 Properties를 만들어 돌려주기만 하므로 객체로 만들지 않는다 (DedupTopology와 같은 방식).
 */
public final class KafkaConfig {

    private KafkaConfig() {
    }

    // [호출] DedupApp.main — KafkaStreams를 만들기 직전 / DedupTopologyTest.setUp
    public static Properties create() {
        Properties kafkaProperties = new Properties();

        // application.id: yaml env KAFKA_APPLICATION_ID. 없으면 kstreams-dedup
        // consumer group 이름 · changelog 토픽 앞부분 · 트랜잭션 ID 앞부분이 된다 (kafka-exporter · 대시보드의 consumergroup 라벨)
        // 바꾸면 offset과 state를 처음부터 다시 쌓는다
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, env("KAFKA_APPLICATION_ID", "kstreams-dedup"));
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19090"));

        // exactly-once (트랜잭션). 커밋 간격 기본값이 100ms라 clean을 read_committed로 읽는 쪽도 거의 바로 본다
        kafkaProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // 스레드 수. task(= 경로 수 × 파티션 3)를 이 pod 안 스레드와 다른 pod가 나눠 가진다
        kafkaProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, env("NUM_STREAM_THREADS", "1"));

        // task 잠금 파일 · 오프셋 체크포인트 파일 위치 (store는 in-memory라 데이터는 여기 없다)
        kafkaProperties.put(StreamsConfig.STATE_DIR_CONFIG, env("STATE_DIR", "/tmp/kafka-streams"));

        // Kafka Streams가 만드는 내부 토픽(changelog)도 다른 토픽처럼 복제 3 · min ISR 2
        kafkaProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        kafkaProperties.put(StreamsConfig.topicPrefix("min.insync.replicas"), "2");

        // changelog 토픽안 레코드의 보존 기간 : store 보존 기간(TTL 1분) + 설정 값(현재는 1분)
        kafkaProperties.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG,
                Duration.ofMinutes(1).toMillis());

        // changelog 토픽의 segment 파일을 몇분마다 얼마마다 열지
        // - default - 7일 / 16MiB
        // - custom - 60분 / 한도 없음
        // => failover 때 restore 양을 결정 => 복구 시간 결정
        kafkaProperties.put(StreamsConfig.topicPrefix("segment.ms"),
                String.valueOf(Duration.ofMinutes(60).toMillis()));
        kafkaProperties.put(StreamsConfig.topicPrefix("retention.bytes"),
                "-1");

        // clean 토픽이 compression.type=zstd라 producer도 zstd로 보낸다 (다르면 broker가 다시 압축한다)
        kafkaProperties.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

        return kafkaProperties;
    }

    private static String env(String name, String defaultValue) {
        return System.getenv().getOrDefault(name, defaultValue);
    }
}
