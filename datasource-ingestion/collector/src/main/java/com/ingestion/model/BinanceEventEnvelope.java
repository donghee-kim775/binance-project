package com.ingestion.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import tools.jackson.databind.JsonNode;

/**
 * 모든 raw 토픽이 공통으로 쓰는 메시지 형태. Binance 원본 payload를 고치지 않고 그대로 감싸고,
 * 그 위에 "어디서 · 언제 · 누가 받았는지"만 붙인다 (plan/topic/plan.md [메시지 envelope] 참고).
 * payload를 손대지 않아야 다운스트림(Flink)이 나중에 원본 기준으로 재처리할 수 있다.
 *
 * - product · assetClass · sector : ConfigMap의 binance.subscription.* 값
 * - stream                        : 구독 스트림 이름에서 업데이트 속도 접미사를 뗀 것 (depth@100ms → depth)
 * - symbol · eventTime            : 원본의 s · E. Spot bookTicker에는 E가 없어 eventTime이 null이다
 * - receiveTime                   : WebSocket 콜백에서 메시지를 받은 시각 — 비동기 큐 대기 시간이 섞이지 않게
 *                                   파싱 전에 찍는다
 * - collectorId                   : pod 이름. Active-Active에서 어느 pod가 보낸 메시지인지 추적한다
 */
public record BinanceEventEnvelope(
        String source,
        String product,
        @JsonProperty("asset_class") String assetClass,
        String sector,
        String stream,
        String symbol,
        @JsonProperty("event_time") Long eventTime,
        @JsonProperty("receive_time") long receiveTime,
        @JsonProperty("collector_id") String collectorId,
        JsonNode payload) {
}
