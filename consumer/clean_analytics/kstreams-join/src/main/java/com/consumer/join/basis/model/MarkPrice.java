package com.consumer.join.basis.model;

import java.math.BigDecimal;

/**
 * USD-M markPrice@1s 한 건에서 basis 계산에 필요한 값만 남긴 것. 이 값이 올 때마다 basis를 한 건 낸다.
 * flink-join의 MarkPrice와 같다.
 */
public record MarkPrice(
        String symbol,          // BTCUSDT
        BigDecimal markPrice,   // 78853.70000000  payload p
        String fundingRate,     // 0.00008778      payload r. 계산에 안 쓰고 그대로 넘기므로 원본 문자열
        long eventTime,         // 1788098181003   payload E (Binance 시계)
        long receiveTime        // 1788098181012   envelope receive_time (노드 시계)
) {
}
