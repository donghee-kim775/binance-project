package com.consumer.join.basis;

import java.math.BigDecimal;
import java.math.MathContext;
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
 *   - 심볼마다 값 몇 개(mean, var)만 기억한다. 5분 치 300건을 들고 있지 않아도 된다
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

    static final String WARMUP = "warmup";      // 준비 중 (표본이 minSamples보다 적다)
    static final String NORMAL = "normal";      // 정상
    static final String WARNING = "warning";    // 주의 (|Z| > 임계치, 연속 < alertConsecutive)
    static final String ALERT = "alert";        // 과열 (연속 ≥ alertConsecutive)

    private static final BigDecimal BPS = BigDecimal.valueOf(10_000);

    // 과열 판정 규칙 (Insights/1.spot_futures_basis/1_1_plan.md 3장): 5분 EMA 기준 |Z| > 2가 3번 연속이면 알림
    //  - 운영 값이 하나뿐이라 밖에서 받지 않고 여기 고정한다. 바꾸려면 이 값을 고쳐 이미지를 다시 배포한다
    static final BasisStatsRule RULE = new BasisStatsRule(
            Duration.ofMinutes(5),  // window           : 평균 · 표준편차의 시간 상수
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
        // meanBps, varBps, bpsSampleCount, zBreachStreak, lastEventTime
        BasisStatsState previous = statsStore.get(symbol);
        if (previous == null) {
            previous = BasisStatsState.empty();
        }

        // 1. 중복 · 역순 가드: 이미 반영한 시각이거나 그보다 옛 값이면 버린다 (수집기 재연결로 같은 markPrice가 다시 오는 경우)
        //  - spot_mid > 0은 여기서 다시 보지 않는다. BasisParser.validateSpot이 입구에서 bid · ask > 0을 보장한다
        if (basis.eventTime() <= previous.lastEventTime()) {
            log.warn("[basis] skip stats: old event_time {} <= {} — {}",
                    basis.eventTime(), previous.lastEventTime(), symbol);
            return;
        }

        // 2. bps = basis ÷ spot_mid × 10,000  (심볼마다 가격 단위가 달라 비율로 비교한다)
        double bps = new BigDecimal(basis.basis())
                .multiply(BPS)
                .divide(new BigDecimal(basis.spotMid()), MathContext.DECIMAL64)
                .doubleValue();

        // 3. Z: 이번 값을 넣기 전의 평균 · 표준편차 기준
        double stdBps = Math.sqrt(previous.varBps());
        boolean warm = previous.bpsSampleCount() >= rule.minSamples() && stdBps > 0;
        double z = warm ? (bps - previous.meanBps()) / stdBps : 0.0;

        // 4. 연속 횟수 · 상태 · 알림
        //  - warmup 동안은 판정하지 않는다 (연속 0)
        //  - 알림은 연속이 정확히 alertConsecutive가 되는 한 건만 (계속 과열이어도 다시 울리지 않는다)
        int zBreachStreak = warm && Math.abs(z) > rule.zThreshold() ? previous.zBreachStreak() + 1 : 0;
        //  - 상태: warmup → (연속 ≥ alertConsecutive) alert → (연속 > 0) warning → normal 순서로 먼저 맞는 것
        String status;
        if (!warm) {
            status = WARMUP;
        } else if (zBreachStreak >= rule.alertConsecutive()) {
            status = ALERT;
        } else if (zBreachStreak > 0) {
            status = WARNING;
        } else {
            status = NORMAL;
        }
        boolean alertStarted = zBreachStreak == rule.alertConsecutive();

        // 5. state 갱신: 시간 가중 EMA (첫 값이면 그 값으로 시작)
        BasisStatsState next = updated(previous, bps, basis.eventTime(), zBreachStreak);
        statsStore.put(symbol, next);

        BasisSignal signal = new BasisSignal(
                basis,                          // basis 필드 그대로
                round(bps),                     // bps
                round(previous.meanBps()),      // mean_bps
                round(stdBps),                  // std_bps
                round(z),                       // z
                zBreachStreak,                  // zBreachStreak
                status,                         // status
                alertStarted);                  // alert_started
        if (alertStarted) {
            log.info("[basis] alert {}: z={} bps={} (|z| > {} × {})", symbol, round(z), round(bps),
                    rule.zThreshold(), rule.alertConsecutive());
        }
        context.forward(record.withValue(signal));   // [다음] analytics.basis sink
    }

    /**
     * 지수 가중 평균 · 분산 갱신 (Welford의 EMA판).
     *   alpha = max(1 − e^(−Δt / window), 1 / (n + 1))
     *   diff  = x − mean
     *   mean' = mean + alpha × diff
     *   var'  = (1 − alpha) × (var + alpha × diff²)
     */
    private BasisStatsState updated(BasisStatsState previous, double bps, long eventTime, int zBreachStreak) {
        if (previous.bpsSampleCount() == 0) {
            return new BasisStatsState(
                    bps,            // meanBps : 첫 값으로 시작
                    0.0,            // varBps
                    1,              // bpsSampleCount
                    0,              // zBreachStreak
                    eventTime);     // lastEventTime
        }

        // 가중치: 시간 가중(5분 EMA)과 1/(n+1)(단순 누적 평균) 중 큰 값
        //  - 처음에는 1/(n+1)이 커서 단순 평균 · 분산처럼 쌓인다 (0에서 시작한 분산이 작게 나와 Z가 부풀지 않게)
        //  - 표본이 약 300건(5분)을 넘으면 시간 가중치가 커져 최근 5분 EMA로 넘어간다
        long elapsedMs = eventTime - previous.lastEventTime();
        double timeAlpha = 1.0 - Math.exp(-(double) elapsedMs / rule.window().toMillis());
        double alpha = Math.max(timeAlpha, 1.0 / (previous.bpsSampleCount() + 1));
        double diff = bps - previous.meanBps();
        double mean = previous.meanBps() + alpha * diff;
        double variance = (1.0 - alpha) * (previous.varBps() + alpha * diff * diff);

        return new BasisStatsState(
                mean,                           // meanBps
                variance,                       // varBps
                previous.bpsSampleCount() + 1,  // bpsSampleCount
                zBreachStreak,                  // zBreachStreak
                eventTime);                     // lastEventTime
    }

    // 출력용: 소수 넷째 자리까지 (대시보드 표시에 충분하고 JSON이 짧아진다)
    private static double round(double value) {
        return Math.round(value * 10_000.0) / 10_000.0;
    }
}
