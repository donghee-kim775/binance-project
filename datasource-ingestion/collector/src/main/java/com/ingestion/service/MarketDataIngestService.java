package com.ingestion.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.ingestion.config.BinanceProperties;
import com.ingestion.config.BinanceProperties.Subscription.StreamSpec;
import com.ingestion.kafka.RawEventPublisher;
import com.ingestion.model.EventEnvelope;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 수신한 raw 메시지를 envelope로 감싸서 그대로 발행한다. 이 producer는 payload를 해석하지 않는다 —
 * 스트림별 모델로 파싱하거나 spot mid price · mark price를 조합하는 일은 다운스트림 Flink 앱의
 * 책임이다. 여기서 꺼내는 값은 모든 스트림에 공통으로 있는 심볼(s)과 이벤트 시각(E)뿐이라,
 * 스트림 종류가 늘어나도 이 클래스는 바뀌지 않는다.
 *
 * onRaw는 {@link com.ingestion.runner.CollectorRunner}의 WebSocket 콜백 스레드가
 * 아니라 별도 스레드풀({@link com.ingestion.config.AsyncConfig})에서 실행된다 — 처리가 느려져도
 * 다음 메시지 수신을 막지 않는다. CollectorRunner가 아닌 이 빈의 메서드에 @Async를 붙인 이유는,
 * 같은 빈 안에서의 self-invocation은 스프링 AOP 프록시를 거치지 않아 @Async가 무시되기 때문이다.
 */
@Component
public class MarketDataIngestService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataIngestService.class);

    private final RawEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final BinanceProperties.Subscription subscription;
    private final String collectorId;

    // collectorId: Deployment가 넣는 COLLECTOR_ID(pod 이름) → 없으면 HOSTNAME → 로컬 실행이면 "local"
    public MarketDataIngestService(RawEventPublisher eventPublisher,
                                   ObjectMapper objectMapper,
                                   BinanceProperties.Subscription subscription,
                                   @Value("${COLLECTOR_ID:${HOSTNAME:local}}") String collectorId) {
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
        this.subscription = subscription;
        this.collectorId = collectorId;
    }

    // Async(ingestExecutor) 붙이는 이유 :
    // WebSocket 콜백 스레드에서 직접 파싱하면, 파싱이 느려질 경우 다음 메시지 수신을 막아버린다.
    @Async("ingestExecutor")
    public void onRaw(StreamSpec spec, String raw, long receiveTime) {
        try {
            JsonNode data = unwrap(objectMapper.readTree(raw));
            EventEnvelope envelope = toEnvelope(spec, data, receiveTime);

            log.info("[{}] {} {} event_time={}",
                    subscription.group(), envelope.stream(), envelope.symbol(), envelope.eventTime());
            eventPublisher.publish(spec.topic(), envelope);
        } catch (Exception e) {
            log.error("failed to handle {} message: {}", spec.stream(), raw, e);
        }
    }

    // ── combined 스트림 풀기 ──────────────────────────────────
    // * 심볼이 여러 개인 pod는 /stream?streams=... 로 붙어서 메시지가 {"stream": ..., "data": {...}}로 온다
    //   단일 스트림(/ws/...)은 감싸지 않은 원본이 바로 온다
    private JsonNode unwrap(JsonNode node) {
        return node.has("data") ? node.get("data") : node;
    }

    // ── envelope 생성 ─────────────────────────────────────────
    // * Spot bookTicker에는 이벤트 시각(E)이 없다 → eventTime = null
    private EventEnvelope toEnvelope(StreamSpec spec, JsonNode data, long receiveTime) {
        JsonNode eventTime = data.get("E");
        return new EventEnvelope(
                "binance",
                subscription.product(),
                subscription.assetClass(),
                subscription.sector(),
                spec.stream().split("@")[0], // depth@100ms → depth, markPrice@1s → markPrice
                data.get("s").asString(),
                eventTime == null ? null : eventTime.asLong(),
                receiveTime,
                collectorId,
                data);
    }
}
