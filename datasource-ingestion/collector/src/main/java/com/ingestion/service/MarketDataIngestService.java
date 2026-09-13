package com.ingestion.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.ingestion.kafka.RawEventPublisher;
import com.ingestion.model.MarkPriceUpdate;
import com.ingestion.model.SpotBookTicker;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 수신한 raw 이벤트를 로그로 남기고 그대로 발행한다. 이 producer는 조인·계산 없이 원본만 옮긴다 —
 * spot mid price와 mark price를 조합한 basis 계산은 다운스트림 컨슈머(Kafka Streams basis-join)의
 * 책임이다.
 *
 * onRaw* 메서드는 {@link com.ingestion.runner.CollectorRunner}의 WebSocket 콜백 스레드가
 * 아니라 별도 스레드풀({@link com.ingestion.config.AsyncConfig})에서 실행된다 — 파싱이 느려져도
 * 다음 메시지 수신을 막지 않는다. CollectorRunner가 아닌 이 빈의 메서드에 @Async를 붙인 이유는,
 * 같은 빈 안에서의 self-invocation은 스프링 AOP 프록시를 거치지 않아 @Async가 무시되기 때문이다.
 */
@Component
public class MarketDataIngestService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataIngestService.class);

    private final RawEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    public MarketDataIngestService(RawEventPublisher eventPublisher, ObjectMapper objectMapper) {
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
    }

    // Async(ingestExecutor) 붙이는 이유 :
    // WebSocket 콜백 스레드에서 직접 파싱하면, 파싱이 느려질 경우 다음 메시지 수신을 막아버린다.
    @Async("ingestExecutor")
    public void onRawSpotBookTicker(String raw) {
        try {
            JsonNode node = objectMapper.readTree(raw);
            onSpotBookTicker(SpotBookTicker.fromJson(node));
        } catch (Exception e) {
            log.error("failed to parse spot bookTicker message: {}", raw, e);
        }
    }

    @Async("ingestExecutor")
    public void onRawMarkPrice(String raw) {
        try {
            JsonNode node = objectMapper.readTree(raw);
            onMarkPrice(MarkPriceUpdate.fromJson(node));
        } catch (Exception e) {
            log.error("failed to parse markPrice message: {}", raw, e);
        }
    }

    public void onSpotBookTicker(SpotBookTicker ticker) {
        log.info("[SPOT ] {} bid={} ask={} mid={}",
                ticker.symbol(), ticker.bestBidPrice(), ticker.bestAskPrice(), ticker.midPrice());
        eventPublisher.publishBookTicker(ticker);
    }

    public void onMarkPrice(MarkPriceUpdate markPrice) {
        log.info("[FUT  ] {} markPrice={} fundingRate={}",
                markPrice.symbol(), markPrice.markPrice(), markPrice.fundingRate());
        eventPublisher.publishMarkPrice(markPrice);
    }
}
