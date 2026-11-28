package com.consumer.join.basis.model;

/**
 * 심볼 하나의 통계 state (store basis-stats의 값, changelog로 복원된다).
 * Python으로 치면 state[symbol] = {"averageBps": ..., "varBps": ..., ...} 한 칸이다.
 */
public record BasisStatsState(
        double averageBps,                                      // -5.12   최근 bps의 지수 가중 평균
        double varBps,                                          // 0.83    최근 bps의 지수 가중 분산
        long bpsSampleCount,                                    // 412     지금까지 반영한 bps 관측 수 (warmup 판정)
        int zContWarnCnt,                                      // 0       |Z| > 임계치가 연속된 횟수
        long lastEventTime                                      // 1788098181003  마지막으로 반영한 markPrice E (중복 · 역순 가드)
) {

    public static BasisStatsState empty() {
        return new BasisStatsState(
                0.0,    // averageBps
                0.0,    // varBps
                0,      // bpsSampleCount
                0,      // zContWarnCnt
                0);     // lastEventTime
    }
}
