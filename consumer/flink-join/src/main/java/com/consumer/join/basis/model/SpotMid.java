package com.consumer.join.basis.model;

import java.math.BigDecimal;

/**
 * Spot bookTicker 한 건에서 basis 계산에 필요한 값만 남긴 것. BasisFunction이 심볼마다 최신 한 건을 state에 둔다.
 *
 * 가격은 BigDecimal로 둔다. Binance가 가격을 문자열("78760.56000000")로 주는 것도 double 오차를 피하려는 것이다.
 * Flink는 record를 POJO처럼 필드별로 직렬화한다 (Kryo로 넘어가지 않는다).
 */
public record SpotMid(
        String symbol,      // BTCUSDT
        BigDecimal mid,     // 78760.565  = (b + a) / 2
        long receiveTime    // 1788098180650  envelope receive_time. Spot bookTicker에는 E가 없어 이 값을 시각으로 쓴다
) {
}
