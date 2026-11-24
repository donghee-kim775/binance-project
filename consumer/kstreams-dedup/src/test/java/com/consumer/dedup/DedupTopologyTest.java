package com.consumer.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * broker 없이 Topology만 돌려 dedup 규칙을 확인한다 (Python의 pytest + 가짜 Kafka와 비슷).
 * 입력 시각(timestamp)을 직접 정해 넣으므로 TTL 경계도 시험할 수 있다.
 */
class DedupTopologyTest {

    private static final Instant T0 = Instant.parse("2026-11-22T00:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> raw;
    private TestOutputTopic<String, String> clean;

    @BeforeEach
    void setUp() {
        DedupRoute route = DedupRoute.parse("spot.crypto.bookticker:u");
        Properties config = DedupApp.config();
        config.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir") + "/kstreams-dedup-test");

        driver = new TopologyTestDriver(DedupTopology.build(List.of(route), DedupApp.TTL), config);
        raw = driver.createInputTopic(route.rawTopic(), new StringSerializer(), new StringSerializer());
        clean = driver.createOutputTopic(route.cleanTopic(), new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static String envelope(String collector, String symbol, long u) {
        return "{\"symbol\":\"" + symbol + "\",\"collector_id\":\"" + collector + "\",\"payload\":{\"u\":" + u + ",\"s\":\"" + symbol + "\"}}";
    }

    @Test
    void 두_pod가_보낸_같은_이벤트는_먼저_온_것만_통과() {
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 1), T0);
        raw.pipeInput("BTCUSDT", envelope("pod-b", "BTCUSDT", 1), T0.plusMillis(30));

        List<KeyValue<String, String>> out = clean.readKeyValuesToList();
        assertEquals(1, out.size());
        assertEquals("BTCUSDT", out.get(0).key);
        assertTrue(out.get(0).value.contains("\"collector_id\":\"pod-a\""));
        assertTrue(out.get(0).value.contains("\"dedup_time\":"));
    }

    @Test
    void 늦은_timestamp가_먼저_와도_중복으로_잡는다() {
        raw.pipeInput("BTCUSDT", envelope("pod-b", "BTCUSDT", 1), T0.plusMillis(30));
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 1), T0);

        assertEquals(1, clean.readKeyValuesToList().size());
    }

    @Test
    void 심볼이_다르면_같은_id라도_각각_통과() {
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 1), T0);
        raw.pipeInput("ETHUSDT", envelope("pod-a", "ETHUSDT", 1), T0);

        assertEquals(2, clean.readKeyValuesToList().size());
    }

    @Test
    void TTL보다_늦게_온_중복은_걸러지지_않는다() {
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 1), T0);
        raw.pipeInput("BTCUSDT", envelope("pod-b", "BTCUSDT", 1), T0.plus(DedupApp.TTL).plus(Duration.ofSeconds(1)));

        // flink-design.md 3장 trade-off와 같은 동작: 1분 넘게 늦은 중복은 통과한다
        assertEquals(2, clean.readKeyValuesToList().size());
    }

    @Test
    void 깨진_메시지는_버리고_다음_메시지는_처리() {
        raw.pipeInput("BTCUSDT", "not json", T0);
        raw.pipeInput("BTCUSDT", "{\"symbol\":\"BTCUSDT\",\"payload\":{}}", T0);   // u 없음
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 2), T0);

        assertEquals(1, clean.readKeyValuesToList().size());
    }
}
