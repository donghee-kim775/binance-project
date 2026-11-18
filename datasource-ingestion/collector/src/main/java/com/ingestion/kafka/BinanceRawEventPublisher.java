package com.ingestion.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.ingestion.model.BinanceEventEnvelope;

import tools.jackson.databind.ObjectMapper;

/**
 * Kafka 발행 지점. envelope를 JSON 문자열로 바꿔 KafkaTemplate으로 보낸다.
 * producer 설정(직렬화 · acks · idempotence · 압축)은 application.yml의 spring.kafka.producer.*가 정한다.
 *
 * 토픽은 ConfigMap의 binance.subscription.streams[].topic이 정하고, 키는 심볼이다 — 같은 심볼의 이벤트가 같은 파티션에
 * 순서대로 쌓여야 다운스트림 Flink의 dedup · 조인이 파티션 안에서 끝난다 (plan/topic/plan.md [키와 파티션]).
 * 섹터 pod는 소켓 하나로 여러 심볼이 섞여 오므로, 키는 config가 아니라 메시지의 s 값에서 정한다.
 *
 * 여기서는 envelope를 그대로 올리기만 한다 — 중복 제거 · basis 계산 · 조인은 이 producer의
 * 책임이 아니라 raw 토픽을 구독하는 Flink 앱의 책임이다.
 */
@Component
public class BinanceRawEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(BinanceRawEventPublisher.class);

    // 
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public BinanceRawEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    // send는 비동기다 — 브로커 응답(ack)은 whenComplete 콜백에서 받는다
    // 실패는 재시도(producer 내부)까지 끝난 뒤에 온다. Active-Active 그룹은 다른 pod가 같은 메시지를 보내 보완된다
    // [호출] MarketDataIngestService.onRaw — 메시지마다
    // [스레드] ingest-async-N
    public void publish(String topic, BinanceEventEnvelope envelope) {
        String value = objectMapper.writeValueAsString(envelope);
        // [다음] KafkaTemplate.send → KafkaProducer 버퍼에 넣고 바로 반환 (전송은 producer 내부 스레드가 배치로)
        kafkaTemplate.send(topic, envelope.symbol(), value)
                // [호출] 브로커 응답(ack)이나 최종 실패가 왔을 때
                // [스레드] kafka-producer-network-thread
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[kafka] send failed topic={} key={}", topic, envelope.symbol(), ex);
                    } else if (log.isDebugEnabled()) {
                        var meta = result.getRecordMetadata();
                        log.debug("[kafka] sent topic={} partition={} offset={}", meta.topic(), meta.partition(), meta.offset());
                    }
                });
    }
}
