package com.ingestion.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ingestion.model.EventEnvelope;

/**
 * Kafka 발행 지점. POC 단계라 실제로 발행하지 않고 로그로만 확인한다.
 * 실제 연동 시 build.gradle의 spring-kafka 의존성을 켜고 KafkaTemplate을 주입해서 사용하면 된다.
 *
 * 토픽은 ConfigMap의 binance.subscription.streams[].topic이 정하고, 키는 심볼이다 — 같은 심볼의 이벤트가 같은 파티션에
 * 순서대로 쌓여야 다운스트림 Flink의 dedup · 조인이 파티션 안에서 끝난다 (plan/topic/plan.md [키와 파티션]).
 * 섹터 pod는 소켓 하나로 여러 심볼이 섞여 오므로, 키는 config가 아니라 메시지의 s 값에서 정한다.
 *
 * 여기서는 envelope를 그대로 올리기만 한다 — 중복 제거 · basis 계산 · 조인은 이 producer의
 * 책임이 아니라 raw 토픽을 구독하는 Flink 앱의 책임이다.
 */
@Component
public class RawEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(RawEventPublisher.class);

    public void publish(String topic, EventEnvelope envelope) {
        log.info("[KAFKA:SKIPPED] topic={} key={}", topic, envelope.symbol());
    }
}
