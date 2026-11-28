package com.ingestion.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * co-partitioning 실험용 partitioner. 기본 배포에서는 쓰지 않는다.
 *
 * 파티션을 Kafka 기본(murmur2(키) % 파티션 수)이 아니라 "이 심볼을 처리할 Flink basis subtask" 기준으로 고른다.
 * Flink basis job(BasisCoPartitionJob)이 keyBy 없이 reinterpretAsKeyedStream으로 받으려면
 * Kafka 파티션 i의 데이터가 처음부터 그 키를 맡은 subtask에 도착해야 하기 때문이다.
 *
 * 켜는 법 (ConfigMap application.yaml, deploy/compare-copart/):
 *   spring.kafka.producer.properties.partitioner.class: com.ingestion.kafka.FlinkKeyGroupPartitioner
 *   spring.kafka.producer.properties.flink.max-parallelism: 128
 *
 * 전제: basis 병렬도 = 토픽 파티션 수, maxParallelism은 basis job과 같은 값.
 * consumer/raw_clean/flink-dedup의 FlinkKeyGroupPartitioner와 같은 계산이다. 한쪽을 바꾸면 다른 쪽도 바꾼다.
 * Flink 의존성을 수집 앱에 넣지 않으려고 Flink의 해시 계산(MathUtils.murmurHash, KeyGroupRangeAssignment)만 옮겨 왔다.
 */
public class FlinkKeyGroupPartitioner implements Partitioner {

    static final String MAX_PARALLELISM_CONFIG = "flink.max-parallelism";

    private int maxParallelism = 128;

    // [호출] KafkaProducer 생성 시 — producer 설정(spring.kafka.producer.properties.*)을 넘겨준다
    @Override
    public void configure(Map<String, ?> configs) {
        Object configured = configs.get(MAX_PARALLELISM_CONFIG);
        if (configured != null) {
            maxParallelism = Integer.parseInt(String.valueOf(configured));
        }
    }

    // [호출] KafkaProducer.send — 레코드를 보낼 때마다 (send를 부른 스레드에서)
    // [스레드] ingest-async-N
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionsForTopic(topic).size();
        if (key == null) {
            return 0;   // BinanceRawEventPublisher는 항상 심볼을 키로 보낸다. 여기 올 일은 없다
        }
        return partitionFor(
                key.toString(),    // BTCUSDT
                topic,             // binance.raw.usdm.crypto.markprice.copart
                numPartitions,     // 3 = basis 병렬도
                maxParallelism);   // 128
    }

    /** 심볼이 들어갈 파티션 번호. flink-dedup 쪽 계산과 같은지 테스트로 맞춰 본다 */
    static int partitionFor(String symbol, String topic, int numPartitions, int maxParallelism) {
        // 1. keyBy였다면 이 심볼을 맡았을 subtask (Flink KeyGroupRangeAssignment)
        //  - key group = murmurHash(hashCode) % maxParallelism
        //  - subtask   = keyGroup × 병렬도 / maxParallelism
        int keyGroup = murmurHash(symbol.hashCode()) % maxParallelism;
        int ownerSubtask = keyGroup * numPartitions / maxParallelism;

        // 2. Flink KafkaSource가 이 토픽의 파티션 0을 배정하는 subtask (connector 5.0.0 KafkaSourceEnumerator.getSplitOwner)
        //  - 파티션 p는 (startIndex + p) % 병렬도 subtask가 읽는다
        int startIndex = ((topic.hashCode() * 31) & 0x7FFFFFFF) % numPartitions;

        // 3. (startIndex + p) % n == ownerSubtask 인 p
        return Math.floorMod(ownerSubtask - startIndex, numPartitions);
    }

    /** Flink MathUtils.murmurHash(int)를 그대로 옮겼다 */
    static int murmurHash(int code) {
        code *= 0xcc9e2d51;
        code = Integer.rotateLeft(code, 15);
        code *= 0x1b873593;
        code = Integer.rotateLeft(code, 13);
        code = code * 5 + 0xe6546b64;
        code ^= 4;
        code = bitMix(code);

        if (code >= 0) {
            return code;
        } else if (code != Integer.MIN_VALUE) {
            return -code;
        } else {
            return 0;
        }
    }

    /** Flink MathUtils.bitMix(int)를 그대로 옮겼다 */
    static int bitMix(int in) {
        in ^= in >>> 16;
        in *= 0x85ebca6b;
        in ^= in >>> 13;
        in *= 0xc2b2ae35;
        in ^= in >>> 16;
        return in;
    }

    // [호출] KafkaProducer.close
    @Override
    public void close() {
    }
}
