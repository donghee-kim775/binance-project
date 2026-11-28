package com.consumer.join.basis;

import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.join.basis.model.Basis;
import com.consumer.join.basis.model.BasisSignal;
import com.consumer.join.basis.model.BasisStatsState;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * basis 한 건을 받아 bps · 최근 평균 · 표준편차 · Z · 연속 횟수 · 상태를 붙여 내보낸다 (Insight 1 과열 판정).
 * kstreams-join의 BasisStatsProcessor와 같은 계산이다 (단계 번호 · 변수 이름 · status 값 · 로그가 같다).
 *
 *   basis ──> 1. 중복 · 역순 가드 ──> 2. bps ──> 3. Z (이전 평균 · 표준편차 기준) ──> 4. 연속 · 상태 · 알림 ──> 5. state 갱신 ──> 출력
 *
 * 최근 평균 · 표준편차 = 시간 가중 EWMA (시간 상수 = rule.window, 5분).
 *   - 심볼마다 값 몇 개(averageBps, varBps)만 기억한다. 5분 치 300건을 들고 있지 않아도 된다
 *   - 새 값의 가중치 alpha = max(1 − e^(−Δt / 5분), 1/(n+1)). markPrice가 1초마다 오면 평소 alpha ≈ 0.0033
 *   - Z는 이번 값을 넣기 **전**의 평균 · 표준편차로 계산한다 (튄 값이 자기 자신을 평균에 섞어 Z를 줄이지 않게)
 *
 * 왜 exactly-once가 꼭 필요한가: 같은 basis가 두 번 들어오면 평균 · 분산이 틀어지고 연속 횟수가 1 늘어 거짓 알림이 난다.
 *   Flink는 checkpoint로 state와 Kafka 트랜잭션을 함께 커밋한다(KafkaIO.sink = EXACTLY_ONCE). 1번 가드는 입력 쪽 중복에 대한 마지막 방어선이다.
 *
 * BasisFunction과 같은 키(심볼) · 같은 병렬도라 셔플 없이 이어진다 (BasisJob의 reinterpretAsKeyedStream).
 */
class BasisStatsFunction extends KeyedProcessFunction<String, Basis, Tuple2<String, String>> {

    private static final Logger log = LoggerFactory.getLogger(BasisStatsFunction.class);

    // 과열 판정 규칙 (Insights/1.spot_futures_basis/1_1_plan.md 3장): 5분 EMA 기준 |Z| > 2가 3번 연속이면 알림
    //  - kstreams-join BasisStatsProcessor.RULE과 같은 값. 바꾸면 둘 다 바꾼다
    static final BasisStatsRule RULE = new BasisStatsRule(
            Duration.ofMinutes(5),  // window           : 시간 상수
            2.0,                    // zThreshold       : |Z|가 이보다 크면 벗어남
            3,                      // alertConsecutive : 연속 3번이면 과열 → 알림 1번
            60);                    // minSamples       : 1분(60건) 쌓이기 전에는 판정하지 않음

    private final BasisStatsRule rule = RULE;

    private transient ValueState<BasisStatsState> basisStats;
    private transient ObjectMapper mapper;

    // 버린 건수 (Flink metric). basisNoSpot · basisStaleSpot과 같은 이름 규칙
    private transient Counter oldEvent;   // 이미 반영한 event_time 이하 (중복 · 역순)

    // [호출] Flink 런타임 — 태스크 시작 시 한 번
    @Override
    public void open(OpenContext openContext) {
        // "basis-stats"는 savepoint 복원 시 식별자다. 바꾸면 state를 못 찾는다 (kstreams-join store 이름과 같게 둔다)
        ValueStateDescriptor<BasisStatsState> descriptor = new ValueStateDescriptor<>("basis-stats", BasisStatsState.class);
        basisStats = getRuntimeContext().getState(descriptor);

        mapper = new ObjectMapper();

        oldEvent = getRuntimeContext().getMetricGroup().counter("basisStatsOldEvent");
    }

    // [호출] Flink 런타임 — BasisFunction이 낸 basis마다 (현재 키 = 이 basis의 심볼, 심볼당 초당 1건)
    @Override
    public void processElement(Basis basis, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
        String symbol = basis.symbol();

        // averageBps, varBps, bpsSampleCount, zContWarnCnt, lastEventTime
        BasisStatsState prevBasisStats = basisStats.value();
        if (prevBasisStats == null) {
            prevBasisStats = BasisStatsState.empty();
        }

        // 1. 중복 · 역순 가드: 이미 반영한 시각이거나 그보다 옛 값이면 버린다
        if (basis.eventTime() <= prevBasisStats.lastEventTime()) {
            oldEvent.inc();
            log.warn("[basis] skip stats: old event_time - " + symbol + " : "
                    + basis.eventTime() + " <= " + prevBasisStats.lastEventTime());
            return;
        }

        // 2. bps = basis ÷ spot_mid × 10,000  (통계값이라 double. 가격 자체는 BigDecimal 문자열 그대로 출력한다)
        double bps = Double.parseDouble(basis.basis()) / Double.parseDouble(basis.spotMid()) * 10_000;

        // 3. Z = (이번 bps − 최근 평균) / 최근 표준편차  (이번 값을 넣기 전 기준)
        double stdBps = Math.sqrt(prevBasisStats.varBps());

        //  - 표본 부족         → warmup
        //  - 표본 충분 · std 0 → check_data (0으로 나누면 Z가 Infinity · NaN)
        boolean enoughSamples = prevBasisStats.bpsSampleCount() >= rule.minSamples();
        boolean noVariation = enoughSamples && stdBps == 0;
        boolean warm = enoughSamples && stdBps > 0;

        double z = warm ? (bps - prevBasisStats.averageBps()) / stdBps : 0.0;

        // 4. 연속 횟수 · 상태 · 알림
        //  - warm이고 |Z| > zThreshold면 직전 연속 + 1, 아니면 0
        //  - 심각도: normal(초록) → warning(노랑, 연속 1~2) → alert(빨강, 연속 ≥ 3)
        int zContWarnCnt = warm && Math.abs(z) > rule.zThreshold() ? prevBasisStats.zContWarnCnt() + 1 : 0;

        String status;
        if (!enoughSamples) {
            status = "warmup";          // 준비 중 (표본이 minSamples보다 적다)
        } else if (noVariation) {
            status = "check_data";      // 데이터 확인 필요 (표본은 충분한데 값이 전부 같아 표준편차 0 → Z를 낼 수 없다)
        } else if (zContWarnCnt >= rule.alertConsecutive()) {
            status = "alert";           // 과열 (연속 ≥ alertConsecutive)
        } else if (zContWarnCnt > 0) {
            status = "warning";         // 주의 (|Z| > 임계치, 연속 < alertConsecutive)
        } else {
            status = "normal";          // 정상
        }
        boolean alertStarted = zContWarnCnt == rule.alertConsecutive();

        // 5. state 갱신: 시간 가중 EWMA (첫 값이면 그 값으로 시작)
        BasisStatsState newBasisStats = updated(prevBasisStats, bps, basis.eventTime(), zContWarnCnt);
        basisStats.update(newBasisStats);

        BasisSignal signal = new BasisSignal(
                basis,                              // basis 필드 그대로
                round(bps),                         // bps
                round(prevBasisStats.averageBps()), // mean_bps (JSON)
                round(stdBps),                      // std_bps
                round(z),                           // z
                zContWarnCnt,                       // zContWarnCnt
                status,                             // status
                alertStarted                        // alert_started
        );

        if (alertStarted) {
            log.info("[basis] alert {}: z={} bps={} (|z| > {} × {})", symbol, round(z), round(bps),
                    rule.zThreshold(), rule.alertConsecutive());
        }

        out.collect(Tuple2.of(symbol, mapper.writeValueAsString(signal))); // [다음] KafkaSink → binance.analytics.crypto.basis
    }

    /*
    state 갱신: EWMA 방식으로 평균 · 분산을 갱신한다 (kstreams-join BasisStatsProcessor.updated와 같다).
    */
    private BasisStatsState updated(BasisStatsState prevBasisStats, double bps, long eventTime, int zContWarnCnt) {
        if (prevBasisStats.bpsSampleCount() == 0) {
            return new BasisStatsState(
                    bps,            // averageBps : 첫 값으로 시작
                    0.0,            // varBps
                    1,              // bpsSampleCount
                    0,              // zContWarnCnt
                    eventTime);     // lastEventTime
        }

        // EWMA(= 지수 가중 이동 평균) :
        // ㄴ currEWMA = prevEWMA + alpha × (x − prevEWMA)
        //                         ㄴ alpha = max(timeAlpha, nAlpha)  둘 중 큰 값
        //                                    - timeAlpha = 1 − e^(−(이번 E − 직전 E) / 시간 상수)  시간 가중치
        //                                    - nAlpha    = 1 / (표본 수 + 1)                       표본 수 가중치 (처음 ~300건)
        // EWMV(= 지수 가중 이동 분산) :
        // ㄴ currEWMV = (1 − alpha) × (prevEWMV + alpha × (x − prevEWMA)²)
        long elapsedMs = eventTime - prevBasisStats.lastEventTime();
        double timeAlpha = 1.0 - Math.exp(
                -(double) elapsedMs / rule.window().toMillis()
        );
        double nAlpha = 1.0 / (prevBasisStats.bpsSampleCount() + 1);
        double alpha = Math.max(timeAlpha, nAlpha);

        double diffBps = bps - prevBasisStats.averageBps();                                        // 이번 bps − 기존 평균 (EWMA)
        double averageBps = prevBasisStats.averageBps() + alpha * diffBps;                         // EWMA 평균을 이번 값 쪽으로 alpha만큼 당긴다
        double varBps = (1.0 - alpha) * (prevBasisStats.varBps() + alpha * Math.pow(diffBps, 2)); // 분산도 같은 가중치로. (x − 평균)²

        return new BasisStatsState(
                averageBps,                             // averageBps
                varBps,                                 // varBps
                prevBasisStats.bpsSampleCount() + 1,    // bpsSampleCount
                zContWarnCnt,                           // zContWarnCnt
                eventTime);                             // lastEventTime
    }

    // 출력용: 소수 넷째 자리까지 (대시보드 표시에 충분하고 JSON이 짧아진다)
    private static double round(double value) {
        return Math.round(value * 10_000.0) / 10_000.0;
    }
}
