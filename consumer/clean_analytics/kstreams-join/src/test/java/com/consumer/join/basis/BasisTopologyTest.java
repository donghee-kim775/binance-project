package com.consumer.join.basis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import com.consumer.join.BasisApp;
import com.consumer.join.config.KafkaConfig;

/**
 * broker 없이 Topology만 돌려 basis 규칙을 확인한다 (kstreams-dedup DedupTopologyTest와 같은 방식).
 * Spot과 mark를 원하는 순서 · receive_time으로 넣어 join과 신선도 검사를 시험한다.
 */
class BasisTopologyTest {

    private static final String SPOT_TOPIC = "binance.clean.spot.crypto.bookticker";
    private static final String MARK_TOPIC = "binance.raw.usdm.crypto.markprice";
    private static final String BASIS_TOPIC = "binance.analytics.crypto.basis";

    private static final Instant T0 = Instant.parse("2026-11-22T00:00:00Z");

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> spot;
    private TestInputTopic<String, String> mark;
    private TestOutputTopic<String, String> basis;

    @BeforeEach
    void setUp() {
        Properties kafkaProperties = KafkaConfig.create();
        kafkaProperties.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir") + "/kstreams-join-test");

        testDriver = new TopologyTestDriver(
                BasisTopology.build(SPOT_TOPIC, MARK_TOPIC, BASIS_TOPIC, BasisApp.MAX_SPOT_AGE),
                kafkaProperties);
        spot = testDriver.createInputTopic(SPOT_TOPIC, new StringSerializer(), new StringSerializer());
        mark = testDriver.createInputTopic(MARK_TOPIC, new StringSerializer(), new StringSerializer());
        basis = testDriver.createOutputTopic(BASIS_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    // clean Spot bookTicker envelope (b, a만 쓴다)
    private static String spotEnvelope(String symbol, String bid, String ask, long receiveTime) {
        return "{\"symbol\":\"" + symbol + "\",\"stream\":\"bookTicker\",\"receive_time\":" + receiveTime
                + ",\"payload\":{\"u\":1,\"s\":\"" + symbol + "\",\"b\":\"" + bid + "\",\"a\":\"" + ask + "\"}"
                + ",\"dedup_time\":" + receiveTime + "}";
    }

    // raw USD-M markPrice envelope (p, r, E만 쓴다)
    private static String markEnvelope(String symbol, String markPrice, long receiveTime) {
        return "{\"symbol\":\"" + symbol + "\",\"stream\":\"markPrice\",\"receive_time\":" + receiveTime
                + ",\"payload\":{\"s\":\"" + symbol + "\",\"p\":\"" + markPrice + "\",\"r\":\"0.0001\",\"E\":" + receiveTime + "}}";
    }

    // Spot이 있는 상태에서 markPrice가 오면 basis = mark − mid 한 건을 낸다
    @Test
    void markAfterSpotEmitsBasis() {
        long now = T0.toEpochMilli();
        spot.pipeInput("BTCUSDT", spotEnvelope("BTCUSDT", "100.00000000", "102.00000000", now), T0);
        mark.pipeInput("BTCUSDT", markEnvelope("BTCUSDT", "105.00000000", now + 300), T0.plusMillis(300));

        List<KeyValue<String, String>> out = basis.readKeyValuesToList();
        assertEquals(1, out.size());
        assertEquals("BTCUSDT", out.get(0).key);
        assertTrue(out.get(0).value.contains("\"spot_mid\":\"101.00000000\""));
        assertTrue(out.get(0).value.contains("\"basis\":\"4.00000000\""));
        assertTrue(out.get(0).value.contains("\"spot_age_ms\":300"));
    }

    // Spot이 오기만 하면 table만 바뀌고 출력은 없다
    @Test
    void spotAloneEmitsNothing() {
        spot.pipeInput("BTCUSDT", spotEnvelope("BTCUSDT", "100", "102", T0.toEpochMilli()), T0);

        assertTrue(basis.isEmpty());
    }

    // 이 심볼의 Spot을 아직 못 받았으면 markPrice가 와도 출력하지 않는다
    @Test
    void markWithoutSpotIsDropped() {
        mark.pipeInput("BTCUSDT", markEnvelope("BTCUSDT", "105", T0.toEpochMilli()), T0);

        assertTrue(basis.isEmpty());
    }

    // Spot이 2초보다 오래됐으면 버린다
    @Test
    void staleSpotIsDropped() {
        long now = T0.toEpochMilli();
        spot.pipeInput("BTCUSDT", spotEnvelope("BTCUSDT", "100", "102", now), T0);
        mark.pipeInput("BTCUSDT", markEnvelope("BTCUSDT", "105", now + 3000), T0.plusSeconds(3));

        assertTrue(basis.isEmpty());
    }

    // Spot이 여러 번 오면 가장 마지막 값과 계산한다
    @Test
    void latestSpotIsUsed() {
        long now = T0.toEpochMilli();
        spot.pipeInput("BTCUSDT", spotEnvelope("BTCUSDT", "100", "102", now), T0);
        spot.pipeInput("BTCUSDT", spotEnvelope("BTCUSDT", "200", "202", now + 100), T0.plusMillis(100));
        mark.pipeInput("BTCUSDT", markEnvelope("BTCUSDT", "205", now + 200), T0.plusMillis(200));

        String value = basis.readValue();
        assertTrue(value.contains("\"spot_mid\":\"201\""));
        assertTrue(value.contains("\"basis\":\"4\""));
    }

    // 심볼이 다르면 짝짓지 않는다
    @Test
    void differentSymbolsAreNotJoined() {
        long now = T0.toEpochMilli();
        spot.pipeInput("BTCUSDT", spotEnvelope("BTCUSDT", "100", "102", now), T0);
        mark.pipeInput("ETHUSDT", markEnvelope("ETHUSDT", "105", now), T0);

        assertTrue(basis.isEmpty());
    }

    // 깨진 메시지는 버리고 다음 메시지는 정상 처리한다
    @Test
    void brokenMessagesAreSkipped() {
        long now = T0.toEpochMilli();
        spot.pipeInput("BTCUSDT", "not json", T0);
        spot.pipeInput("BTCUSDT", "{\"symbol\":\"BTCUSDT\",\"receive_time\":1,\"payload\":{}}", T0);   // b, a 없음
        mark.pipeInput("BTCUSDT", "not json", T0);
        spot.pipeInput("BTCUSDT", spotEnvelope("BTCUSDT", "100", "102", now), T0);
        mark.pipeInput("BTCUSDT", markEnvelope("BTCUSDT", "105", now), T0);

        assertEquals(1, basis.readKeyValuesToList().size());
    }
}
