package com.consumer.join.basis;

import java.time.Duration;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.join.basis.model.Basis;
import com.consumer.join.basis.model.BasisSignal;
import com.consumer.join.basis.model.BasisStatsState;

/**
 * basis 한 건을 받아 bps · 최근 평균 · 표준편차 · Z · 연속 횟수 · 상태를 붙여 내보낸다 (Insight 1 과열 판정).
 *
 *   basis ──> 1. 중복 · 역순 가드 ──> 2. bps ──> 3. Z (이전 평균 · 표준편차 기준) ──> 4. 연속 · 상태 · 알림 ──> 5. state 갱신 ──> 출력
 *
 * 최근 평균 · 표준편차 = 시간 가중 EMA (시간 상수 = rule.window, 5분).
 *   - 심볼마다 값 몇 개(averageBps, varBps)만 기억한다. 5분 치 300건을 들고 있지 않아도 된다
 *   - 새 값의 가중치 alpha = 1 − e^(−Δt / 5분). markPrice가 1초마다 오면 alpha ≈ 0.0033
 *   - 처음 약 5분은 1/(n+1)이 더 커서 단순 누적 평균 · 분산으로 쌓인다 (분산이 0에서 시작해 작게 나오는 것을 막는다)
 *   - Z는 이번 값을 넣기 **전**의 평균 · 표준편차로 계산한다 (튄 값이 자기 자신을 평균에 섞어 Z를 줄이지 않게)
 *
 * 왜 exactly-once가 꼭 필요한가: 같은 basis가 두 번 들어오면 평균 · 분산이 틀어지고 연속 횟수가 1 늘어 거짓 알림이 난다.
 *   이 앱은 exactly_once_v2라 재시작해도 store 변경 · 출력이 한 번만 반영된다. 1번 가드는 입력 쪽 중복(수집기 재연결)에 대한 마지막 방어선이다.
 *
 * 키가 심볼이고 BasisJoiner와 같은 task에서 돌아 repartition이 없다.
 * 스레드: task 하나는 StreamThread 하나에서만 돈다. store에 락이 필요 없다.
 */
class BasisStatsProcessor implements Processor<String, Basis, String, BasisSignal> {

    private static final Logger log = LoggerFactory.getLogger(BasisStatsProcessor.class);

    // 과열 판정 규칙 (Insights/1.spot_futures_basis/1_1_plan.md 3장): 5분 EMA 기준 |Z| > 2가 3번 연속이면 알림
    //  - 운영 값이 하나뿐이라 밖에서 받지 않고 여기 고정한다. 바꾸려면 이 값을 고쳐 이미지를 다시 배포한다
    static final BasisStatsRule RULE = new BasisStatsRule(
            Duration.ofMinutes(5),  // window           : 시간 상수
            2.0,                    // zThreshold       : |Z|가 이보다 크면 벗어남
            3,                      // alertConsecutive : 연속 3번이면 과열 → 알림 1번
            60);                    // minSamples       : 1분(60건) 쌓이기 전에는 판정하지 않음

    // 초기화 : 인자를 받는 순간
    private final BasisStatsRule rule = RULE;
    private final String storeName;

    // 초기화 : task 시작하는 순간
    private ProcessorContext<String, BasisSignal> context;
    private KeyValueStore<String, BasisStatsState> statsStore;

    BasisStatsProcessor(String storeName) {
        this.storeName = storeName;
    }

    // [호출] Kafka Streams — task가 이 StreamThread에 할당되어 처음 시작할 때 (store 복원이 끝난 뒤)
    @Override
    public void init(ProcessorContext<String, BasisSignal> context) {
        this.context = context;
        this.statsStore = context.getStateStore(storeName);
    }

    // [호출] Kafka Streams — fresh.basis를 통과한 basis마다 (심볼당 초당 1건)
    // [스레드] kstreams-basis-<uuid>-StreamThread-N
    @Override
    public void process(Record<String, Basis> record) {
        String symbol = record.key();
        Basis basis = record.value();

        // BasisStatStates
        // averageBps, varBps, bpsSampleCount, zContWarnCnt, lastEventTime
        BasisStatsState prevBasisStats = statsStore.get(symbol);
        if (prevBasisStats == null) {
            prevBasisStats = BasisStatsState.empty();
        }

        // 1. 중복 · 역순 가드: 
        //  - 이미 반영한 시각이거나 그보다 옛 값이면 버린다.
        if (basis.eventTime() <= prevBasisStats.lastEventTime()) {
            log.warn("[basis] skip stats: old event_time - " + symbol + " : "
                    + basis.eventTime() + " <= " + prevBasisStats.lastEventTime());
            return;
        }

        // 2. bps 계산
        // bps = (basis-spotmid)/10000
        double bps = Double.parseDouble(basis.basis()) / Double.parseDouble(basis.spotMid()) * 10_000;

        // 3. Z 계산
        // Z = (현재 bps - 최근 bps 평균) / 최근 bps 표준편차
        double stdBps = Math.sqrt(prevBasisStats.varBps());

        // warm = 표본이 60개 이상이고 표준편차가 0보다 큰가(정상 데이터인가)?
        boolean enoughSamples = prevBasisStats.bpsSampleCount() >= rule.minSamples();
        boolean noVariation = enoughSamples && stdBps == 0;
        boolean warm = enoughSamples && stdBps > 0;

        double z = warm ? (bps - prevBasisStats.averageBps()) / stdBps : 0.0;

        // 4. 연속 횟수 · 상태 · 알림
        //    ㄴ warm 상태이면서, z가 임계치를 넘었는가?
        //          | 심각도 수준 : normal -> warning -> alert 
        //          - alert    (빨강)  : 과열 알람  (세번 연속 주의 -> 과열)
        //          - warning  (노랑)  : 주의      (|z| > 임계치)
        //          - normal   (초록)  : 정상수치
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

        // 5. state 갱신: 시간 가중 EMA (첫 값이면 그 값으로 시작)
        BasisStatsState newBasisStats = updated(prevBasisStats, bps, basis.eventTime(), zContWarnCnt);
        statsStore.put(symbol, newBasisStats);

        BasisSignal signal = new BasisSignal(
                basis,                          // basis 필드 그대로
                round(bps),                     // bps
                round(prevBasisStats.averageBps()), // mean_bps (JSON)
                round(stdBps),                  // std_bps
                round(z),                       // z
                zContWarnCnt,                  // zContWarnCnt
                status,                         // status
                alertStarted                    // alert_started
        );

        if (alertStarted) {
            log.info("[basis] alert {}: z={} bps={} (|z| > {} × {})", symbol, round(z), round(bps),
                    rule.zThreshold(), rule.alertConsecutive());
        }
        
        context.forward(record.withValue(signal));   // [다음] analytics.basis sink
    }

    /*
    state 업데이트 : EMA 방식으로 각 지표 상태를 업데이트한다.
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
        
        // EMWA(=지수이동가중 average) : 
        // ㄴ currEMWA = prevEMWA + alpha x (X-prevEMWA)
        //                        ㄴ alpha = (timeAlpha(시간 가중치) or nAlpha(표본수 가중치))
        //                                   - timealpha = 1 - e^(-이전과 현재이벤트의 시간 차이 / 시간상수)
        //                                   - nAlpha = 1/(표본수 + 1)
        // EWMV(= 지수 가중 이동 분산) :
        // ㄴ currEWMV = (1 − alpha) × (prevEWMV + alpha × (x − prevEWMA)²)
        long elapsedMs = eventTime - prevBasisStats.lastEventTime();
        double timeAlpha = 1.0 - Math.exp(
            -(double) elapsedMs / rule.window().toMillis()
        );
        double nAlpha = 1.0 / (prevBasisStats.bpsSampleCount() + 1);
        double alpha = Math.max(timeAlpha, nAlpha);

        double diffBps = bps - prevBasisStats.averageBps();                                        // 이번 bps − 기존 평균 (EMA)
        double averageBps = prevBasisStats.averageBps() + alpha * diffBps;                         // EMA 평균을 이번 값 쪽으로 alpha만큼 당긴다
        double varBps = (1.0 - alpha) * (prevBasisStats.varBps() + alpha * Math.pow(diffBps, 2)); // 분산도 같은 가중치로. (x − 평균)²

        return new BasisStatsState(
                averageBps,                     // averageBps
                varBps,                         // varBps
                prevBasisStats.bpsSampleCount() + 1, // bpsSampleCount
                zContWarnCnt,                  // zContWarnCnt
                eventTime);                     // lastEventTime
    }

    // 출력용: 소수 넷째 자리까지 (대시보드 표시에 충분하고 JSON이 짧아진다)
    private static double round(double value) {
        return Math.round(value * 10_000.0) / 10_000.0;
    }
}
