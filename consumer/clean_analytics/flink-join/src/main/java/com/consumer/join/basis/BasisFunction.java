package com.consumer.join.basis;

import java.math.BigDecimal;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import com.consumer.join.basis.model.Basis;
import com.consumer.join.basis.model.MarkPrice;
import com.consumer.join.basis.model.SpotMid;

/**
 * 심볼마다 최신 Spot mid 하나를 state에 두고, markPrice가 오면 그 값과 basis를 계산한다 (consumer/flink-design.md 5장).
 *
 *   입력 1 (Spot,  초당 수십~수백 건) → latestSpot 갱신, 출력 없음
 *   입력 2 (mark,  초당 1건)          → latestSpot과 계산해 Basis 출력 → BasisStatsFunction
 *
 * kstreams-join에서는 KTable(latest-spot) + join(BasisJoiner) + filter(fresh.basis)가 나눠 하는 일을 여기서 한 번에 한다.
 *
 * 윈도우 조인을 쓰지 않는 이유: 윈도우 안의 모든 (Spot, mark) 조합이 출력된다. 필요한 것은 "mark 시점의 최신 Spot" 하나다.
 *
 * 신선도: |mark.receive_time − spot.receive_time| > 2초(2000ms)이면 버린다 (kstreams-join fresh.basis와 같은 값).
 *   - 둘 다 collector가 찍은 노드 시계라 Binance 시계(E)와 섞이지 않는다. 노드 간 NTP 동기화는 전제다.
 *   - 절댓값인 이유: Spot은 dedup job을 한 번 더 거쳐 보통 mark보다 늦게 도착하지만, mark 쪽이 밀리면
 *     state의 Spot이 mark보다 나중에 받은 값일 수 있다 (음수). 작으면 그대로 쓰고, 크면 시각이 어긋난 짝이라 버린다.
 *
 * 스레드: 한 subtask의 processElement1과 processElement2는 같은 태스크 스레드에서 번갈아 불린다.
 *   동시에 돌지 않으므로 state에 락이 필요 없다.
 */
class BasisFunction extends KeyedCoProcessFunction<String, SpotMid, MarkPrice, Basis> {

    private transient ValueState<SpotMid> latestSpot;

    // 버린 건수. Flink metric으로 노출된다 (Prometheus reporter를 켜면 대시보드에서 볼 수 있다)
    private transient Counter noSpot;     // 이 심볼의 Spot을 아직 한 번도 못 받음 (job 시작 직후, Spot을 수집하지 않는 심볼)
    private transient Counter staleSpot;  // Spot이 2초보다 오래됨 (Spot 수집 · dedup이 멈춤)

    // [호출] Flink 런타임 — 태스크 시작 시 한 번
    @Override
    public void open(OpenContext openContext) {
        // "latest-spot"은 savepoint 복원 시 식별자다. 바꾸면 state를 못 찾는다.
        // 심볼마다 값 하나라 TTL을 두지 않는다 (심볼 수만큼만 쌓인다)
        ValueStateDescriptor<SpotMid> descriptor = new ValueStateDescriptor<>("latest-spot", SpotMid.class);
        latestSpot = getRuntimeContext().getState(descriptor);

        noSpot = getRuntimeContext().getMetricGroup().counter("basisNoSpot");
        staleSpot = getRuntimeContext().getMetricGroup().counter("basisStaleSpot");
    }

    // [호출] Flink 런타임 — Spot 레코드마다 (현재 키 = 이 레코드의 심볼)
    @Override
    public void processElement1(SpotMid spot, Context ctx, Collector<Basis> out) throws Exception {
        latestSpot.update(spot);
    }

    // [호출] Flink 런타임 — markPrice 레코드마다 (현재 키 = 이 레코드의 심볼, latestSpot도 이 심볼의 값만 보인다)
    @Override
    public void processElement2(MarkPrice mark, Context ctx, Collector<Basis> out) throws Exception {
        SpotMid spot = latestSpot.value();
        if (spot == null) {
            noSpot.inc();
            return;
        }

        long spotAgeMs = mark.receiveTime() - spot.receiveTime();          // 362 ms
        if (Math.abs(spotAgeMs) > 2000) {                                  // 2초 (ms)
            staleSpot.inc();
            return;
        }

        BigDecimal basisPrice = mark.markPrice().subtract(spot.mid());     // 93.13500000 = mark − spot mid (BigDecimal이라 - 대신 subtract)

        Basis basis = new Basis(
                mark.symbol(),                                              // symbol
                mark.eventTime(),                                           // event_time
                mark.markPrice().toPlainString(),                           // mark_price
                spot.mid().toPlainString(),                                 // spot_mid
                basisPrice.toPlainString(),                                 // basis
                mark.fundingRate(),                                         // funding_rate
                mark.receiveTime(),                                         // receive_time
                spot.receiveTime(),                                         // spot_receive_time
                spotAgeMs,                                                  // spot_age_ms
                System.currentTimeMillis());                                // basis_time

        out.collect(basis); // [다음] BasisStatsFunction.processElement
    }
}
