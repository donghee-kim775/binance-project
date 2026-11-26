package com.consumer.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaPartitioner;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

/**
 * clean 토픽의 파티션을 Kafka 기본(murmur2)이 아니라 "이 키를 읽을 Flink basis subtask" 기준으로 고른다.
 * co-partitioning 실험용 (flink-join의 BasisCoPartitionJob이 keyBy 없이 reinterpretAsKeyedStream으로 받는다).
 *
 * 맞춰야 하는 두 규칙:
 *   1. keyBy가 했을 배정  : key group = murmurHash(symbol.hashCode()) % maxParallelism → 주인 subtask
 *   2. KafkaSource split 배정: 파티션 p를 읽는 subtask = (토픽 이름 해시 + p) % 병렬도
 *      → 2를 거꾸로 풀어 "주인 subtask가 읽게 될 파티션"에 쓴다
 *
 * 전제: basis 병렬도 = 토픽 파티션 수, maxParallelism은 basis job과 같은 값.
 * 수집기(datasource-ingestion)의 FlinkKeyGroupPartitioner와 같은 계산이다. 한쪽을 바꾸면 다른 쪽도 바꾼다.
 */
public class FlinkKeyGroupPartitioner implements KafkaPartitioner<Tuple2<String, String>> {

    private final int maxParallelism;

    public FlinkKeyGroupPartitioner(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }

    // [호출] KafkaSink Writer — 레코드를 보낼 때마다
    @Override
    public int partition(Tuple2<String, String> record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        int partitionIndex = partitionFor(
                record.f0,            // BTCUSDT (sink 입력의 f0 = symbol)
                targetTopic,          // binance.clean.spot.crypto.bookticker.copart
                partitions.length,    // 3 = basis 병렬도
                maxParallelism);      // 128
        return partitions[partitionIndex];
    }

    /** 심볼이 들어갈 파티션 번호. 테스트와 수집기 쪽 계산을 맞춰 보려고 따로 뺐다 */
    public static int partitionFor(String symbol, String topic, int numPartitions, int maxParallelism) {
        // 1. keyBy였다면 이 심볼을 맡았을 subtask (병렬도 = 파티션 수)
        int ownerSubtask = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                symbol,           // KeySelector가 돌려주는 키와 같은 타입(String)이어야 hashCode가 같다
                maxParallelism,   // 128
                numPartitions);   // 3

        // 2. KafkaSource가 이 토픽의 파티션 0을 배정하는 subtask (connector 5.0.0 KafkaSourceEnumerator.getSplitOwner)
        //  - 파티션 p는 (startIndex + p) % 병렬도 subtask가 읽는다
        int startIndex = ((topic.hashCode() * 31) & 0x7FFFFFFF) % numPartitions;

        // 3. (startIndex + p) % n == ownerSubtask 인 p
        return Math.floorMod(ownerSubtask - startIndex, numPartitions);
    }
}
