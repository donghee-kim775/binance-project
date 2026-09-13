package com.ingestion.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ingestion.model.MarkPriceUpdate;
import com.ingestion.model.SpotBookTicker;

/**
 * Kafka 발행 지점. POC 단계라 실제로 발행하지 않고 로그로만 확인한다.
 * 실제 연동 시 build.gradle의 spring-kafka 의존성을 켜고 KafkaTemplate을 주입해서 사용하면 된다.
 *
 * 여기서는 파싱된 raw 이벤트를 그대로 각자 토픽에 올리기만 한다 — basis 계산·조인은 이 producer의
 * 책임이 아니라 두 raw 토픽을 구독하는 컨슈머(Kafka Streams basis-join)의 책임이다
 * (Insights/1.spot_futures_basis/1_1_plan.md §4.5 참고).
 */
@Component
public class RawEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(RawEventPublisher.class);

    public void publishBookTicker(SpotBookTicker ticker) {
        log.info("[KAFKA:SKIPPED] topic=binance.raw.bookticker key=spot:{}", ticker.symbol());
    }

    public void publishMarkPrice(MarkPriceUpdate markPrice) {
        log.info("[KAFKA:SKIPPED] topic=binance.raw.markprice key=usdm:{}", markPrice.symbol());
    }
}
