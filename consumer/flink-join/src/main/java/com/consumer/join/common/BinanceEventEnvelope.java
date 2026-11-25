package com.consumer.join.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * 수집 앱(collector)이 raw 토픽에 쓰는 메시지 형태. 수집 앱의 com.ingestion.model.BinanceEventEnvelope와 필드가 같다.
 * Flink 앱은 별도 jar라 클래스를 공유하지 않고 여기 다시 정의한다. 필드가 바뀌면 두 곳을 같이 고친다.
 *
 * Python으로 치면 @dataclass(frozen=True) + json.loads 결과를 담는 그릇이다.
 * clean 토픽에는 dedup job이 붙인 dedup_time처럼 여기 없는 필드가 더 있을 수 있어 모르는 필드는 무시한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BinanceEventEnvelope(
        String source,                                  // binance
        String product,                                 // spot / usdm
        @JsonProperty("asset_class") String assetClass, // crypto / stock
        String sector,                                  // sov
        String stream,                                  // bookTicker / markPrice
        String symbol,                                  // BTCUSDT
        @JsonProperty("event_time") Long eventTime,     // 1788098181003 (payload E). Spot bookTicker는 null
        @JsonProperty("receive_time") long receiveTime, // 1788098181012 collector가 받은 시각 (노드 시계)
        @JsonProperty("collector_id") String collectorId, // spot-sov-7d9f-abcde (pod 이름)
        JsonNode payload                                // Binance 원본 그대로 {"u":..., "b":..., "a":...}
) {
}
