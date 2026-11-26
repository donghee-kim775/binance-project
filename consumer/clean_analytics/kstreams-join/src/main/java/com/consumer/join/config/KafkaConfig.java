package com.consumer.join.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Kafka Streams 설정. BasisApp.main이 KafkaStreams를 만들 때, 테스트가 TopologyTestDriver를 만들 때 쓴다.
 * kstreams-dedup의 KafkaConfig와 같고, 다른 것은 application.id 기본값과 시작 위치(latest)다.
 *
 * 값을 들고 있지 않고 Properties를 만들어 돌려주기만 하므로 객체로 만들지 않는다.
 */
public final class KafkaConfig {

    private KafkaConfig() {
    }

    // [호출] BasisApp.main — KafkaStreams를 만들기 직전 / BasisTopologyTest.setUp
    public static Properties create() {
        Properties kafkaProperties = new Properties();

        // application.id: yaml env KAFKA_APPLICATION_ID. 없으면 kstreams-basis
        // consumer group 이름 · changelog 토픽 앞부분(kstreams-basis-latest-spot-changelog) · 트랜잭션 ID 앞부분이 된다
        // 바꾸면 offset과 state를 처음부터 다시 쌓는다
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, env("KAFKA_APPLICATION_ID", "kstreams-basis"));
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19090"));

        // exactly-once (트랜잭션). 입력은 자동으로 read_committed로 읽는다 (clean을 EOS로 쓰는 쪽의 중단된 트랜잭션을 보지 않는다)
        //  - Flink basis job은 sink가 at-least-once다. 재시작 때 basis가 한 번 더 쓰일 수 있다 (비교할 때 차이 중 하나)
        kafkaProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // 스레드 수. task(= 파티션 3개, spot · mark가 한 task로 묶임)를 이 pod 안 스레드와 다른 pod가 나눠 가진다
        kafkaProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, env("NUM_STREAM_THREADS", "1"));

        // task 잠금 파일 · 오프셋 체크포인트 파일 위치 (store는 in-memory라 데이터는 여기 없다)
        kafkaProperties.put(StreamsConfig.STATE_DIR_CONFIG, env("STATE_DIR", "/tmp/kafka-streams"));

        // 커밋된 offset이 없으면(처음 배포) 지금부터 읽는다 (Flink basis job의 LATEST와 같다)
        //  - 두 토픽의 밀린 데이터를 처음부터 읽으면 시각이 어긋난 짝이 생길 수 있다
        //  - Kafka Streams 기본값은 earliest다
        kafkaProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");

        // Kafka Streams가 만드는 내부 토픽(changelog)도 다른 토픽처럼 복제 3 · min ISR 2
        //  - latest-spot changelog는 cleanup.policy=compact라 심볼마다 최신 한 건만 남는다 (retention 설정이 필요 없다)
        kafkaProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        kafkaProperties.put(StreamsConfig.topicPrefix("min.insync.replicas"), "2");

        // analytics 토픽이 compression.type=zstd라 producer도 zstd로 보낸다 (다르면 broker가 다시 압축한다)
        kafkaProperties.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

        return kafkaProperties;
    }

    private static String env(String name, String defaultValue) {
        return System.getenv().getOrDefault(name, defaultValue);
    }
}
