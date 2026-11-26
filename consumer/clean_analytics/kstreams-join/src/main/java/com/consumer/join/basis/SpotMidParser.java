package com.consumer.join.basis;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.join.basis.model.SpotMid;
import com.consumer.join.common.BinanceEventEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * clean Spot bookTicker envelope → SpotMid (symbol, (b + a) / 2, receive_time). flink-join의 SpotMidParser와 같은 일을 한다.
 *
 * 형식이 깨진 메시지는 경고 후 null을 돌려준다. BasisTopology가 null을 걸러 버린다.
 * 예외를 던지면 StreamThread가 죽고, 다시 떠도 같은 메시지에서 또 실패하기 때문이다 (poison pill).
 *
 * 상태가 없어 객체로 만들지 않는다. BasisTopology가 메서드 참조(SpotMidParser::parse)로 넘긴다.
 */
final class SpotMidParser {

    private static final Logger log = LoggerFactory.getLogger(SpotMidParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final BigDecimal TWO = BigDecimal.valueOf(2);

    private SpotMidParser() {
    }

    // [호출] Kafka Streams — clean bookTicker 레코드마다 (mapValues "parse.spot")
    // [스레드] kstreams-basis-<uuid>-StreamThread-N
    static SpotMid parse(String envelopeJson) {
        try {
            // 1. envelope 파싱
            BinanceEventEnvelope envelope = MAPPER.readValue(envelopeJson, BinanceEventEnvelope.class);
            JsonNode payload = envelope.payload();

            // 2. 필요한 필드가 없으면 버린다
            if (!validate(envelope, payload, envelopeJson)) {
                return null;
            }

            // 3. mid 계산
            BigDecimal bid = new BigDecimal(payload.get("b").asText()); // 78760.56000000
            BigDecimal ask = new BigDecimal(payload.get("a").asText()); // 78760.57000000
            BigDecimal mid = bid.add(ask).divide(TWO);                   // 2로 나누면 항상 나누어떨어져 정확하다

            SpotMid spot = new SpotMid(
                    envelope.symbol(),          // BTCUSDT
                    mid,                        // 78760.565
                    envelope.receiveTime());    // 1788098180650
            return spot;
        } catch (Exception error) {
            log.warn("[basis] skip spot: not a valid envelope — {}", Logs.truncate(envelopeJson));
            return null;
        }
    }

    // 심볼, 최우선 매수(b) · 매도(a) 호가가 있어야 mid를 계산할 수 있다. 없으면 로그를 남기고 false
    private static boolean validate(BinanceEventEnvelope envelope, JsonNode payload, String envelopeJson) {
        if (envelope.symbol() == null || payload == null
                || !payload.path("b").isTextual() || !payload.path("a").isTextual()) {
            log.warn("[basis] skip spot: no symbol or payload.b/a — {}", Logs.truncate(envelopeJson));
            return false;
        }
        return true;
    }
}
