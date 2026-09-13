package com.ingestion.model;

import java.math.BigDecimal;

import tools.jackson.databind.JsonNode;

/**
 * USD-M {@code <symbol>@markPrice} 페이로드. 무기한 선물에만 있는 마크 가격 · 펀딩비 스트림.
 */
public record MarkPriceUpdate(String symbol, long eventTime, BigDecimal markPrice, BigDecimal fundingRate) {

    public static MarkPriceUpdate fromJson(JsonNode node) {
        return new MarkPriceUpdate(
                node.get("s").asString(),
                node.get("E").asLong(),
                new BigDecimal(node.get("p").asString()),
                new BigDecimal(node.get("r").asString()));
    }
}
