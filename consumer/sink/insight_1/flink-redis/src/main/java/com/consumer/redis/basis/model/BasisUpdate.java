package com.consumer.redis.basis.model;

/**
 * analytics basis 한 건에서 Redis에 쓰는 데 필요한 값. 원문 JSON은 고치지 않고 그대로 저장 · 전달한다
 * (대시보드가 BasisSignal 필드를 그대로 읽는다).
 */
public record BasisUpdate(
        String symbol,      // BTCUSDT
        long eventTime,     // 1788098181003  markPrice E. Redis 멱등 키 (더 클 때만 갱신)
        String json         // {"symbol":"BTCUSDT", ..., "status":"normal", ...}  BasisSignal JSON 원문
) {
}
