package com.ingestion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

class FlinkKeyGroupPartitionerTest {

    static final List<String> SYMBOLS = List.of("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT");

    // 옮겨 온 해시 계산이 Flink와 같다. 기대값은 flink-dedup FlinkKeyGroupPartitionerTest가 실제 Flink 코드로 확인한 표다
    @Test
    void markPricePartitionsMatchFlinkTable() {
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
            int partition = FlinkKeyGroupPartitioner.partitionFor(symbol, topic, 3, 128);
            assertEquals(expectedPartitions.get(index), partition, symbol);
        }
    }
}
