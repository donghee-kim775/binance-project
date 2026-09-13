package com.ingestion.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

import tools.jackson.databind.JsonNode;

/**
 * Spot {@code <symbol>@bookTicker} 페이로드. Spot 스트림에는 이벤트 타입("e") 필드가 없다.
 */
public record SpotBookTicker(String symbol, long updateId, BigDecimal bestBidPrice, BigDecimal bestAskPrice) {

    public static SpotBookTicker fromJson(JsonNode node) {
        return new SpotBookTicker(
                node.get("s").asString(),
                node.get("u").asLong(),
                new BigDecimal(node.get("b").asString()),
                new BigDecimal(node.get("a").asString()));
    }

    public BigDecimal midPrice() {
        return bestBidPrice.add(bestAskPrice).divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_UP);
    }
}
