package com.consumer.join.basis;

import org.apache.kafka.streams.kstream.ValueJoiner;

import com.consumer.join.basis.model.Basis;
import com.consumer.join.basis.model.MarkPrice;
import com.consumer.join.basis.model.SpotMid;

/**
 * markPrice 한 건과 그 순간의 최신 Spot을 받아 Basis 한 건을 만든다.
 * flink-join BasisFunction.processElement2의 계산 부분과 같다. "최신 Spot을 들고 있는 일"은 여기서 하지 않고 KTable이 한다.
 *
 *   Flink           : BasisFunction = ValueState(최신 Spot) + 계산
 *   Kafka Streams   : KTable(최신 Spot) ──join──> BasisJoiner(계산)
 *
 * 신선도(2초) 검사는 여기서 하지 않는다. 계산만 하고, BasisTopology의 filter("fresh.basis")가 거른다.
 */
class BasisJoiner implements ValueJoiner<MarkPrice, SpotMid, Basis> {

    @Override
    public Basis apply(MarkPrice mark, SpotMid spot) {
        long spotAgeMs = mark.receiveTime() - spot.receiveTime();

        Basis basis = new Basis(
                mark.symbol(),                                              // symbol
                mark.eventTime(),                                           // event_time
                mark.markPrice().toPlainString(),                           // mark_price
                spot.mid().toPlainString(),                                 // spot_mid
                mark.markPrice().subtract(spot.mid()).toPlainString(),      // basis
                mark.fundingRate(),                                         // funding_rate
                mark.receiveTime(),                                         // receive_time
                spot.receiveTime(),                                         // spot_receive_time
                spotAgeMs,                                                  // spot_age_ms
                System.currentTimeMillis());                                // basis_time
        return basis;
    }
}
