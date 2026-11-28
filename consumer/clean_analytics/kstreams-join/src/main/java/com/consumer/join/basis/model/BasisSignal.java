package com.consumer.join.basis.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

/**
 * binance.analytics.crypto.basis에 쓰는 메시지 = Basis 필드 + 통계 필드 (한 단계 JSON으로 펼쳐 쓴다).
 *
 *   {"symbol":"BTCUSDT", ..., "basis_time":..., "bps":-5.21, "mean_bps":-4.98, "std_bps":0.31, "z":-0.74,
 *    "zContWarnCnt":0, "status":"normal", "alert_started":false}
 *
 * status 값과 대시보드 표기: warmup(준비 중) · check_data(데이터 확인 필요) · normal(정상) · warning(주의) · alert(과열)
 * 통계 값은 double이다 (가격이 아니라 비율 · 점수라 자릿수 오차가 문제 되지 않는다).
 */
public record BasisSignal(
        @JsonUnwrapped Basis basis,                             // symbol, event_time, mark_price, spot_mid, basis, ...
        double bps,                                             // -5.21   basis ÷ spot_mid × 10,000
        @JsonProperty("mean_bps") double averageBps,            // -4.98   최근 평균 (이 값을 넣기 전)
        @JsonProperty("std_bps") double stdBps,                 // 0.31    최근 표준편차 (이 값을 넣기 전)
        double z,                                               // -0.74   (bps − mean_bps) / std_bps. warmup이면 0
        int zContWarnCnt,                                      // 0       |z| > 임계치 연속 횟수
        String status,                                          // normal
        @JsonProperty("alert_started") boolean alertStarted     // false   과열이 시작된 그 한 건만 true
) {
}
