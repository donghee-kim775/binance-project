package com.consumer.join.basis;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.join.basis.model.MarkPrice;
import com.consumer.join.common.BinanceEventEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * raw USD-M markPrice envelope → MarkPrice (symbol, p, r, E, receive_time). flink-join의 MarkPriceParser와 같은 일을 한다.
 *
 * markPrice는 Active-Standby pod 하나가 보내 중복이 없으므로 raw를 바로 읽는다.
 * 형식이 깨진 메시지는 경고 후 null을 돌려준다 (SpotMidParser와 같다).
 */
final class MarkPriceParser {

    private static final Logger log = LoggerFactory.getLogger(MarkPriceParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MarkPriceParser() {
    }

    // [호출] Kafka Streams — raw markPrice 레코드마다 (mapValues "parse.mark")
    // [스레드] kstreams-basis-<uuid>-StreamThread-N
    static MarkPrice parse(String envelopeJson) {
        try {
            // 1. envelope 파싱
            BinanceEventEnvelope envelope = MAPPER.readValue(envelopeJson, BinanceEventEnvelope.class);
            JsonNode payload = envelope.payload();

            // 2. 필요한 필드가 없으면 버린다
            if (!validate(envelope, payload, envelopeJson)) {
                return null;
            }

            // 3. 필요한 값만 남긴다
            MarkPrice mark = new MarkPrice(
                    envelope.symbol(),                          // BTCUSDT
                    new BigDecimal(payload.get("p").asText()),  // 78853.70000000
                    payload.get("r").asText(),                  // 0.00008778
                    payload.get("E").asLong(),                  // 1788098181003
                    envelope.receiveTime());                    // 1788098181012
            return mark;
        } catch (Exception error) {
            log.warn("[basis] skip mark: not a valid envelope — {}", Logs.truncate(envelopeJson));
            return null;
        }
    }

    // 심볼, 마크 가격(p), 펀딩비(r), 이벤트 시각(E)이 있어야 basis를 낼 수 있다. 없으면 로그를 남기고 false
    private static boolean validate(BinanceEventEnvelope envelope, JsonNode payload, String envelopeJson) {
        if (envelope.symbol() == null || payload == null
                || !payload.path("p").isTextual() || !payload.path("r").isTextual()
                || !payload.path("E").canConvertToLong()) {
            log.warn("[basis] skip mark: no symbol or payload.p/r/E — {}", Logs.truncate(envelopeJson));
            return false;
        }
        return true;
    }
}
