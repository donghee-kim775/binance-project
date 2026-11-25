package com.consumer.dedup.dedup;

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

import com.consumer.dedup.DedupApp;
import com.consumer.dedup.config.KafkaConfig;

/**
 * broker 없이 Topology만 돌려 dedup 규칙을 확인한다 (Python의 pytest + 가짜 Kafka와 비슷).
 * 입력 시각(timestamp)을 직접 정해 넣으므로 TTL 경계도 시험할 수 있다.
 */
class DedupTopologyTest {

    private static final Instant T0 = Instant.parse("2026-11-22T00:00:00Z");

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> raw;
    private TestOutputTopic<String, String> clean;

    @BeforeEach
    void setUp() {
        DedupRoute route = DedupRoute.parse("spot.crypto.bookticker:u");
        Properties kafkaProperties = KafkaConfig.create();
        kafkaProperties.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir") + "/kstreams-dedup-test");

        testDriver = new TopologyTestDriver(DedupTopology.build(List.of(route), DedupApp.TTL), kafkaProperties);
        raw = testDriver.createInputTopic(route.rawTopic(), new StringSerializer(), new StringSerializer());
        clean = testDriver.createOutputTopic(route.cleanTopic(), new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    private static String envelope(String collector, String symbol, long u) {
        return "{\"symbol\":\"" + symbol + "\",\"collector_id\":\"" + collector + "\",\"payload\":{\"u\":" + u + ",\"s\":\"" + symbol + "\"}}";
    }

    // 두 pod가 보낸 같은 이벤트는 먼저 온 것만 통과한다
    @Test
    void sameEventFromTwoPodsPassesOnlyFirst() {
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 1), T0);
        raw.pipeInput("BTCUSDT", envelope("pod-b", "BTCUSDT", 1), T0.plusMillis(30));

        List<KeyValue<String, String>> out = clean.readKeyValuesToList();
        assertEquals(1, out.size());
        assertEquals("BTCUSDT", out.get(0).key);
        assertTrue(out.get(0).value.contains("\"collector_id\":\"pod-a\""));
        assertTrue(out.get(0).value.contains("\"dedup_time\":"));
    }

    // timestamp가 늦은 쪽이 먼저 도착해도 중복으로 잡는다 (fetch 범위가 -ttl ~ +ttl)
    @Test
    void laterTimestampArrivingFirstIsStillDuplicate() {
        raw.pipeInput("BTCUSDT", envelope("pod-b", "BTCUSDT", 1), T0.plusMillis(30));
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 1), T0);

        assertEquals(1, clean.readKeyValuesToList().size());
    }

    // 심볼이 다르면 이벤트 id가 같아도 각각 통과한다 (dedup 키 = symbol|eventId)
    @Test
    void differentSymbolsWithSameIdBothPass() {
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 1), T0);
        raw.pipeInput("ETHUSDT", envelope("pod-a", "ETHUSDT", 1), T0);

        assertEquals(2, clean.readKeyValuesToList().size());
    }

    // TTL(1분)보다 늦게 온 중복은 걸러지지 않는다
    @Test
    void duplicateLaterThanTtlIsNotFiltered() {
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 1), T0);
        raw.pipeInput("BTCUSDT", envelope("pod-b", "BTCUSDT", 1), T0.plus(DedupApp.TTL).plus(Duration.ofSeconds(1)));

        // flink-design.md 3장 trade-off와 같은 동작: 1분 넘게 늦은 중복은 통과한다
        assertEquals(2, clean.readKeyValuesToList().size());
    }

    // 깨진 메시지는 버리고 다음 메시지는 정상 처리한다
    @Test
    void brokenMessageIsSkippedAndNextIsProcessed() {
        raw.pipeInput("BTCUSDT", "not json", T0);
        raw.pipeInput("BTCUSDT", "{\"symbol\":\"BTCUSDT\",\"payload\":{}}", T0);   // u 없음
        raw.pipeInput("BTCUSDT", envelope("pod-a", "BTCUSDT", 2), T0);

        assertEquals(1, clean.readKeyValuesToList().size());
    }
}
