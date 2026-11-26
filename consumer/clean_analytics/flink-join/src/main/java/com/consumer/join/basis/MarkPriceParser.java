package com.consumer.join.basis;

import java.math.BigDecimal;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.join.basis.model.MarkPrice;
import com.consumer.join.common.BinanceEventEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * raw USD-M markPrice envelope → MarkPrice (symbol, p, r, E, receive_time).
 *
 * markPrice는 Active-Standby pod 하나가 보내 중복이 없으므로 raw를 바로 읽는다.
 */
class MarkPriceParser extends ProcessFunction<String, MarkPrice> {

    private static final Logger log = LoggerFactory.getLogger(MarkPriceParser.class);

    private transient ObjectMapper mapper;

    // [호출] Flink 런타임 — 태스크 시작 시 한 번
    @Override
    public void open(OpenContext openContext) {
        mapper = new ObjectMapper();
    }

    // [호출] Flink 런타임 — raw markPrice 레코드마다
    @Override
    public void processElement(String envelopeJson, Context ctx, Collector<MarkPrice> out) {
        try {
            // 1. envelope 파싱
            BinanceEventEnvelope envelope = mapper.readValue(envelopeJson, BinanceEventEnvelope.class);
            JsonNode payload = envelope.payload();

            // 2. 필요한 필드가 없으면 버린다
            if (!validate(envelope, payload, envelopeJson)) {
                return;
            }

            // 3. 필요한 값만 남긴다
            MarkPrice mark = new MarkPrice(
                    envelope.symbol(),                          // BTCUSDT
                    new BigDecimal(payload.get("p").asText()),  // 78853.70000000
                    payload.get("r").asText(),                  // 0.00008778
                    payload.get("E").asLong(),                  // 1788098181003
                    envelope.receiveTime());                    // 1788098181012
            out.collect(mark);
        } catch (Exception error) {
            log.warn("[basis] skip mark: not a valid envelope — {}", Logs.truncate(envelopeJson));
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
