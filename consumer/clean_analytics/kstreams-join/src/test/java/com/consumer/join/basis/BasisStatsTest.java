package com.consumer.join.basis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.consumer.join.config.KafkaConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * BasisStatsProcessor 규칙(Z · 연속 횟수 · 상태 · 알림 1회 · 중복 가드)을 broker 없이 확인한다.
 *
 * 입력 만들기: Spot mid를 100으로 고정하고 mark = 100 + basis로 넣는다 → bps = basis × 100.
 *   예: mark 99.95 → basis −0.05 → bps −5
 * markPrice는 1초 간격으로 넣고, 매번 직전에 Spot을 넣어 신선도(2초) 검사를 통과시킨다.
 */
class BasisStatsTest {

    private static final String SPOT_TOPIC = "binance.clean.spot.crypto.bookticker";
    private static final String MARK_TOPIC = "binance.raw.usdm.crypto.markprice";
    private static final String BASIS_TOPIC = "binance.analytics.crypto.basis";

    private static final Instant T0 = Instant.parse("2026-11-22T00:00:00Z");

    // 판정 규칙은 운영 값(BasisStatsProcessor.RULE) 그대로다. warmup이 60건이라 평소 구간은 그보다 많이(80건) 넣는다
    private static final int NORMAL_COUNT = 80;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> spot;
    private TestInputTopic<String, String> mark;
    private TestOutputTopic<String, String> basis;
    private int second;     // 지금까지 넣은 markPrice 수 = T0부터 지난 초

    @BeforeEach
    void setUp() {
        Properties kafkaProperties = KafkaConfig.create();
        kafkaProperties.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir") + "/kstreams-join-stats-test");

        testDriver = new TopologyTestDriver(
                BasisTopology.build(SPOT_TOPIC, MARK_TOPIC, BASIS_TOPIC),
                kafkaProperties);
        spot = testDriver.createInputTopic(SPOT_TOPIC, new StringSerializer(), new StringSerializer());
        mark = testDriver.createInputTopic(MARK_TOPIC, new StringSerializer(), new StringSerializer());
        basis = testDriver.createOutputTopic(BASIS_TOPIC, new StringDeserializer(), new StringDeserializer());
        second = 0;
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    // bps 하나를 1초 뒤 시각으로 넣는다 (Spot mid 100 고정)
    private void pipeBps(double bps) {
        second++;
        pipeAt(second, bps);
    }

    // 정해진 초에 넣는다 (같은 초를 다시 넣으면 같은 markPrice E = 중복)
    private void pipeAt(int atSecond, double bps) {
        Instant at = T0.plusSeconds(atSecond);
        long now = at.toEpochMilli();
        String markPrice = String.format("%.8f", 100 + bps / 100);
        spot.pipeInput("BTCUSDT", spotEnvelope(now - 100), at.minusMillis(100));
        mark.pipeInput("BTCUSDT", markEnvelope(markPrice, now), at);
    }

    // 평소 구간: bps −4 · −6을 번갈아 count번 (평균 약 −5, 표준편차 약 1)
    private void pipeNormal(int count) {
        for (int index = 0; index < count; index++) {
            pipeBps(index % 2 == 0 ? -4 : -6);
        }
    }

    private List<JsonNode> readAll() {
        return basis.readValuesToList().stream().map(BasisStatsTest::json).toList();
    }

    private static JsonNode json(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    private static String spotEnvelope(long receiveTime) {
        return "{\"symbol\":\"BTCUSDT\",\"stream\":\"bookTicker\",\"receive_time\":" + receiveTime
                + ",\"payload\":{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"99.99000000\",\"a\":\"100.01000000\"}"
                + ",\"dedup_time\":" + receiveTime + "}";
    }

    private static String markEnvelope(String markPrice, long eventTime) {
        return "{\"symbol\":\"BTCUSDT\",\"stream\":\"markPrice\",\"receive_time\":" + eventTime
                + ",\"payload\":{\"s\":\"BTCUSDT\",\"p\":\"" + markPrice + "\",\"r\":\"0.0001\",\"E\":" + eventTime + "}}";
    }

    // 출력에 basis 필드와 통계 필드가 한 단계 JSON으로 함께 나온다
    @Test
    void outputHasBasisAndStatsFields() {
        pipeBps(-5);

        JsonNode out = readAll().get(0);
        assertEquals("BTCUSDT", out.get("symbol").asText());
        assertEquals("-0.05000000", out.get("basis").asText());
        assertEquals(-5.0, out.get("bps").asDouble(), 1e-9);
        assertTrue(out.has("mean_bps"));
        assertTrue(out.has("std_bps"));
        assertTrue(out.has("z"));
        assertTrue(out.has("alert_started"));
    }

    // 표본이 minSamples보다 적으면 튀는 값이 와도 warmup이고 알림이 없다
    @Test
    void warmupDoesNotJudge() {
        pipeNormal(5);
        pipeBps(-30);

        List<JsonNode> outs = readAll();
        JsonNode last = outs.get(outs.size() - 1);
        assertEquals("warmup", last.get("status").asText());
        assertEquals(0.0, last.get("z").asDouble(), 1e-9);
        assertFalse(last.get("alert_started").asBoolean());
    }

    // 평소 값은 normal이다
    @Test
    void normalValuesAreNormal() {
        pipeNormal(NORMAL_COUNT);

        JsonNode last = readAll().get(NORMAL_COUNT - 1);
        assertEquals("normal", last.get("status").asText());
        assertTrue(Math.abs(last.get("z").asDouble()) < 2.0);
    }

    // |Z| > 2가 1~2번이면 warning, 3번째에 alert가 되고 알림은 그 한 건에만 붙는다
    @Test
    void threeConsecutiveOutliersRaiseAlertOnce() {
        pipeNormal(NORMAL_COUNT);
        readAll();      // 평소 구간 출력은 버린다

        pipeBps(-15);
        pipeBps(-15);
        pipeBps(-15);
        pipeBps(-15);

        List<JsonNode> outs = readAll();
        assertEquals(List.of("warning", "warning", "alert", "alert"),
                outs.stream().map(out -> out.get("status").asText()).toList());
        assertEquals(List.of(false, false, true, false),
                outs.stream().map(out -> out.get("alert_started").asBoolean()).toList());
        assertTrue(outs.get(0).get("z").asDouble() < -2.0);
    }

    // 벗어난 값이 끊기면 연속 횟수가 0으로 돌아가 다시 normal이 된다
    @Test
    void streakResetsWhenBackToNormal() {
        pipeNormal(NORMAL_COUNT);
        readAll();

        pipeBps(-15);
        pipeBps(-15);
        pipeBps(-5);

        List<JsonNode> outs = readAll();
        assertEquals(0, outs.get(2).get("zBreachStreak").asInt());
        assertEquals("normal", outs.get(2).get("status").asText());
    }

    // 같은 markPrice(E)가 다시 오면 통계에 넣지 않고 출력도 하지 않는다 (거짓 알림 방지)
    @Test
    void duplicateEventTimeIsIgnored() {
        pipeNormal(NORMAL_COUNT);
        readAll();

        pipeBps(-15);
        pipeBps(-15);
        pipeAt(second, -15);    // 방금 넣은 것과 같은 E → 중복

        List<JsonNode> outs = readAll();
        assertEquals(2, outs.size());
        assertEquals(2, outs.get(1).get("zBreachStreak").asInt());
        assertEquals("warning", outs.get(1).get("status").asText());
    }

    // 심볼마다 통계가 따로다
    @Test
    void statsAreKeptPerSymbol() {
        pipeNormal(NORMAL_COUNT);
        readAll();

        long now = T0.plusSeconds(second + 1).toEpochMilli();
        spot.pipeInput("ETHUSDT", spotEnvelope(now - 100).replace("BTCUSDT", "ETHUSDT"), T0.plusSeconds(second + 1));
        mark.pipeInput("ETHUSDT", markEnvelope("99.85000000", now).replace("BTCUSDT", "ETHUSDT"), T0.plusSeconds(second + 1));

        JsonNode eth = readAll().get(0);
        assertEquals("ETHUSDT", eth.get("symbol").asText());
        assertEquals("warmup", eth.get("status").asText());
    }
}
