package com.ingestion.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import jakarta.annotation.PostConstruct;

/**
 * Kafka 접속 설정. 지금은 어느 Kafka에 붙을지(bootstrap-servers)만 읽어서 시작할 때 로그로 남긴다 —
 * spring-kafka 의존성이 아직 꺼져 있어 실제 발행은 RawEventPublisher가 로그로만 대신한다.
 *
 * 주소는 프로파일별 yml이 정한다 (환경변수 SPRING_KAFKA_BOOTSTRAP_SERVERS가 있으면 그 값이 우선):
 *   - local   : application-local.yml   → localhost:9092
 *   - release : application-release.yml → binance-cluster-kafka-bootstrap.kafka.svc:9092 (K8s 내부 DNS)
 *
 * 나중에 Kafka를 연결할 때 build.gradle의 spring-kafka를 켜면, Spring Boot가 같은 키(spring.kafka.*)로
 * KafkaTemplate을 자동으로 만든다. 직렬화 · acks 같은 producer 설정이 필요하면 이 클래스에 이어서 둔다.
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    private final String bootstrapServers;

    // Spring Environment(실행 인자 · 환경변수 · yml을 우선순위대로 합친 저장소)에서 직접 꺼낸다
    // spring.kafka.bootstrap-servers → 없으면 빈 문자열 (test 프로파일처럼 프로파일 yml이 없을 때)
    // local 개발용 => localhost:9092
    // k8s release => binance-cluster-kafka-bootstrap.kafka.svc:9092
    public KafkaConfig(Environment environment) {
        this.bootstrapServers = environment.getProperty("spring.kafka.bootstrap-servers", "");
    }

    @PostConstruct
    void logBootstrapServers() {
        if (bootstrapServers.isBlank()) {
            log.warn("[kafka] bootstrap-servers 설정 없음 — 프로파일(local/release)을 확인한다");
        } else {
            log.info("[kafka] bootstrap-servers={}", bootstrapServers);
        }
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }
}
