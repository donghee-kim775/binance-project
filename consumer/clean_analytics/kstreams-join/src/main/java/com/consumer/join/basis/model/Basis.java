package com.consumer.join.basis.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * binance.analytics.crypto.basis에 쓰는 메시지 한 건 (JSON으로 바꿔 보낸다).
 *
 * 가격은 문자열로 쓴다. Binance 원본과 같은 방식이고, 읽는 쪽이 double로 받아 자릿수를 잃지 않게 한다.
 * z_score, alert는 이 job에서 계산하지 않는다 (basis 출력 위에서 따로 계산).
 * flink-join의 Basis와 필드가 같다. 두 버전의 출력을 같은 방법으로 비교하기 위해서다.
 */
public record Basis(
        String symbol,                                          // BTCUSDT
        @JsonProperty("event_time") long eventTime,             // 1788098181003  markPrice의 E
        @JsonProperty("mark_price") String markPrice,           // 78853.70000000
        @JsonProperty("spot_mid") String spotMid,               // 78760.565
        String basis,                                           // 93.135  = mark_price − spot_mid
        @JsonProperty("funding_rate") String fundingRate,       // 0.00008778
        @JsonProperty("receive_time") long receiveTime,         // 1788098181012  markPrice를 collector가 받은 시각
        @JsonProperty("spot_receive_time") long spotReceiveTime, // 1788098180650  짝지은 Spot을 collector가 받은 시각
        @JsonProperty("spot_age_ms") long spotAgeMs,            // 362  = receive_time − spot_receive_time
        @JsonProperty("basis_time") long basisTime              // 1788098181150  이 job이 계산한 시각 (지연 측정용)
) {
}
