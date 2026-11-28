package com.consumer.join.basis;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.join.basis.model.MarkPrice;
import com.consumer.join.basis.model.SpotMid;
import com.consumer.join.common.BinanceEventEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * basis 입력 두 개의 envelope 파서. flink-join의 BasisParser와 같다 (같은 검사 · 같은 로그).
 *
 *   parseSpot : clean Spot bookTicker envelope → SpotMid (symbol, (b + a) / 2, receive_time)
 *   parseMark : raw USD-M markPrice envelope   → MarkPrice (symbol, p, r, E, receive_time)
 *               markPrice는 Active-Standby pod 하나가 보내 중복이 없으므로 raw를 바로 읽는다.
 *
 * BasisTopology의 mapValues가 부른다.
 *  - 정상 : 값을 돌려준다 → 다음 단계로 간다
 *  - 깨짐 : 경고 로그를 남기고 null을 돌려준다 → 바로 뒤 filter("valid.spot" · "valid.mark")가 버린다
 * 예외를 던지면 StreamThread가 죽고, 다시 떠도 같은 메시지에서 또 실패하기 때문이다 (poison pill).
 */
final class BasisParser {

    private static final Logger log = LoggerFactory.getLogger(BasisParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final BigDecimal TWO = BigDecimal.valueOf(2);

    private BasisParser() {
    }

    // [호출] Kafka Streams — clean bookTicker 레코드마다 (mapValues "parse.spot")
    // [스레드] kstreams-basis-<uuid>-StreamThread-N
    static SpotMid parseSpot(String envelopeJson) {
        try {
            // 1. envelope 파싱
            BinanceEventEnvelope envelope = MAPPER.readValue(envelopeJson, BinanceEventEnvelope.class);
            JsonNode payload = envelope.payload();

            // 2. 필요한 필드가 없으면 버린다
            if (!validateSpot(envelope, payload, envelopeJson)) {
                return null;
            }

            // 3. mid 계산
            BigDecimal bid = new BigDecimal(payload.get("b").asText()); // 78760.56000000
            BigDecimal ask = new BigDecimal(payload.get("a").asText()); // 78760.57000000
            BigDecimal mid = bid.add(ask).divide(TWO);                   // 2로 나누면 항상 나누어떨어져 정확하다

            SpotMid spot = new SpotMid(
                    envelope.symbol(),          // BTCUSDT
                    mid,                        // 78760.565
                    envelope.receiveTime()      // 1788098180650
            );

            return spot;
        } catch (Exception error) {
            log.warn("[basis] skip spot: not a valid envelope — {}", Logs.truncate(envelopeJson));
            return null;
        }
    }

    // [호출] Kafka Streams — raw markPrice 레코드마다 (mapValues "parse.mark")
    // [스레드] kstreams-basis-<uuid>-StreamThread-N
    static MarkPrice parseMark(String envelopeJson) {
        try {
            // 1. envelope 파싱
            BinanceEventEnvelope envelope = MAPPER.readValue(envelopeJson, BinanceEventEnvelope.class);
            JsonNode payload = envelope.payload();

            // 2. 필요한 필드가 없으면 버린다
            if (!validateMark(envelope, payload, envelopeJson)) {
                return null;
            }

            // 3. 필요한 값만 남긴다
            MarkPrice mark = new MarkPrice(
                    envelope.symbol(),                          // BTCUSDT
                    new BigDecimal(payload.get("p").asText()),  // 78853.70000000
                    payload.get("r").asText(),                  // 0.00008778
                    payload.get("E").asLong(),                  // 1788098181003
                    envelope.receiveTime()                      // 1788098181012
            );
                            
            return mark;
        } catch (Exception error) {
            log.warn("[basis] skip mark: not a valid envelope — {}", Logs.truncate(envelopeJson));
            return null;
        }
    }

    // 심볼, 최우선 매수(b) · 매도(a) 호가가 있고 정상이어야 mid를 계산할 수 있다. 아니면 로그를 남기고 false
    private static boolean validateSpot(BinanceEventEnvelope envelope, JsonNode payload, String envelopeJson) {
        if (envelope.symbol() == null || payload == null
                || !payload.path("b").isTextual() || !payload.path("a").isTextual()) {
            log.warn("[basis] skip spot: no symbol or payload.b/a — {}", Logs.truncate(envelopeJson));
            return false;
        }
        // 호가가 양수이고 뒤집히지 않아야(ask ≥ bid) mid를 믿을 수 있다
        //  - 한쪽 호가가 비어 0이 오면 mid가 반값이 되어 basis가 수십 % 로 튄다 → 거짓 과열 알림
        //  - 둘 다 0이면 뒤의 bps 계산(÷ spot_mid)이 0으로 나눈다
        //  - 숫자가 아니면 BigDecimal이 예외를 던지고 바깥 catch가 "not a valid envelope"로 버린다
        BigDecimal bid = new BigDecimal(payload.get("b").asText());
        BigDecimal ask = new BigDecimal(payload.get("a").asText());
        if (bid.signum() <= 0 || ask.signum() <= 0 || ask.compareTo(bid) < 0) {
            log.warn("[basis] skip spot: bid/ask not positive or crossed — {}", Logs.truncate(envelopeJson));
            return false;
        }
        return true;
    }

    // 심볼, 마크 가격(p), 펀딩비(r), 이벤트 시각(E)이 있어야 basis를 낼 수 있다. 없으면 로그를 남기고 false
    private static boolean validateMark(BinanceEventEnvelope envelope, JsonNode payload, String envelopeJson) {
        if (envelope.symbol() == null || payload == null
                || !payload.path("p").isTextual() || !payload.path("r").isTextual()
                || !payload.path("E").canConvertToLong()) {
            log.warn("[basis] skip mark: no symbol or payload.p/r/E — {}", Logs.truncate(envelopeJson));
            return false;
        }
        return true;
    }
}
