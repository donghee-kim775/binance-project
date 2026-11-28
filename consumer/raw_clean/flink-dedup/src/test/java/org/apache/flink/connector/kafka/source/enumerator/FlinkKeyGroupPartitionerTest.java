package org.apache.flink.connector.kafka.source.enumerator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import com.consumer.common.FlinkKeyGroupPartitioner;

/**
 * FlinkKeyGroupPartitioner가 쓴 파티션을 KafkaSource가 "그 키의 주인 subtask"에게 배정하는지 확인한다.
 *
 * connector의 split 배정 함수 KafkaSourceEnumerator.getSplitOwner는 package-private이라
 * 이 테스트만 connector와 같은 패키지에 둔다. connector 버전을 올렸을 때 배정 규칙이 바뀌면 여기서 깨진다.
 */
class FlinkKeyGroupPartitionerTest {

    static final List<String> SYMBOLS = List.of("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT");
    static final List<String> TOPICS = List.of(
            "binance.clean.spot.crypto.bookticker.copart",
            "binance.raw.usdm.crypto.markprice.copart");
    static final int PARTITIONS = 3;          // = basis 병렬도
    static final int MAX_PARALLELISM = 128;

    // 쓴 파티션을 읽는 subtask와 keyBy였다면 그 키를 맡았을 subtask가 같다
    @Test
    void readerSubtaskEqualsKeyGroupOwner() {
        for (String topic : TOPICS) {
            for (String symbol : SYMBOLS) {
                int partition = FlinkKeyGroupPartitioner.partitionFor(symbol, topic, PARTITIONS, MAX_PARALLELISM);

                int readerSubtask = KafkaSourceEnumerator.getSplitOwner(new TopicPartition(topic, partition), PARTITIONS);
                int ownerSubtask = KeyGroupRangeAssignment.assignKeyToParallelOperator(symbol, MAX_PARALLELISM, PARTITIONS);

                assertEquals(ownerSubtask, readerSubtask, topic + " " + symbol);
            }
        }
    }

    // 수집기(datasource-ingestion) FlinkKeyGroupPartitionerTest와 같은 표. 두 쪽 계산이 어긋나면 한쪽이 깨진다
    @Test
    void markPricePartitionsMatchCollectorTable() {
        String topic = "binance.raw.usdm.crypto.markprice.copart";
        List<Integer> expectedPartitions = List.of(
                1,   // BTCUSDT
                1,   // ETHUSDT
                2,   // SOLUSDT
                2,   // BNBUSDT
                1,   // XRPUSDT
                0);  // DOGEUSDT
        for (int index = 0; index < SYMBOLS.size(); index++) {
            String symbol = SYMBOLS.get(index);
            int partition = FlinkKeyGroupPartitioner.partitionFor(symbol, topic, PARTITIONS, MAX_PARALLELISM);
            assertEquals(expectedPartitions.get(index), partition, symbol);
        }
    }
}
