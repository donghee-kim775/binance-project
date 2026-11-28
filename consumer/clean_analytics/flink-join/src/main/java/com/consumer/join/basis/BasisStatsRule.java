package com.consumer.join.basis;

import java.time.Duration;

/**
 * Z-score와 과열 판정 규칙 (Insights/1.spot_futures_basis/1_1_plan.md 3장). kstreams-join의 BasisStatsRule과 같다.
 *
 *   Z      = (지금 bps − 최근 평균) / 최근 표준편차
 *   주의   = |Z| > zThreshold 가 1번 이상 연속
 *   과열   = |Z| > zThreshold 가 alertConsecutive번 이상 연속 → 그 순간 알림 1번
 */
public record BasisStatsRule(
        Duration window,          // 5분     평균 · 표준편차를 얼마 동안의 값으로 보나 (EMA 시간 상수)
        double zThreshold,        // 2.0     |Z|가 이보다 크면 벗어난 것
        int alertConsecutive,     // 3       몇 번 연속 벗어나면 과열(알림)인가
        int minSamples            // 60      이만큼 쌓이기 전에는 표준편차가 불안정해 판정하지 않는다 (warmup)
) {
}
