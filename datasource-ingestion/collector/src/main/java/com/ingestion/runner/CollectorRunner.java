package com.ingestion.runner;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

import com.ingestion.collector.BinanceStreamClient;
import com.ingestion.config.BinanceProperties;
import com.ingestion.service.MarketDataIngestService;

/**
 * 수집기(producer) POC 진입점. {@link BinanceProperties}로 받은 심볼 하나에 대해 Spot bookTicker와
 * USD-M markPrice 두 스트림을 붙여 raw 이벤트를 {@link MarketDataIngestService}로 넘긴다.
 * 파싱·발행은 여기서 하지 않는다 — {@link MarketDataIngestService#onRawSpotBookTicker}가
 * @Async로 별도 스레드풀에서 처리한다 (WebSocket 콜백 스레드를 파싱으로 묶어두지 않기 위함).
 *
 * spot과 futures 값을 조합하는 basis 계산도 여기 없다 — 그건 이 producer가 올린 두 raw 토픽을
 * 구독하는 컨슈머(Kafka Streams basis-join)가 한다 (Insights/1.spot_futures_basis/1_1_plan.md §4.5 참고).
 *
 * product+symbol별 Pod 분리, Active-Active, Redis dedup(1_1_plan.md §4.2~4.4)은
 * 아직 적용하지 않은 단일 프로세스 버전이다.
 *
 * run()이 끝나지 않고 계속 떠 있어야 하는 프로세스라, "test" 프로필에서는 비활성화한다 —
 * 그렇지 않으면 @SpringBootTest로 컨텍스트를 띄울 때마다 테스트가 영원히 끝나지 않는다.
 */
@Component
@Profile("!test")
public class CollectorRunner implements ApplicationRunner {

    private final MarketDataIngestService ingestService;
    private final BinanceProperties properties;

    private BinanceStreamClient spotClient;
    private BinanceStreamClient markPriceClient;

    public CollectorRunner(MarketDataIngestService ingestService, BinanceProperties properties) {
        this.ingestService = ingestService;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) throws InterruptedException {
        String symbol = properties.setup().symbol().toLowerCase();

        // "/market" 경로 접두어 필수 — 접두어 없는 연결은 /public 데이터만 받는데 markPrice는
        // /market(일반) 카테고리라 접두어 없이는 연결만 되고 메시지가 전혀 안 온다.
        // (developers.binance.com USDⓈ-M Futures WebSocket Market Streams 문서 참고)
        URI spotBookTickerUri = URI.create(properties.meta().spot().websocketStreamUrl() + "/ws/" + symbol + "@bookTicker");
        URI usdmMarkPriceUri = URI.create(properties.meta().usdmFutures().websocketStreamUrl() + "/market/ws/" + symbol + "@markPrice@1s");
        
        // BinanceStreamClient
        // parameter 1 : streamlabel
        // parameter 2 : websocket uri
        // parameter 3 : onMessage callback
        spotClient = new BinanceStreamClient("spot-bookTicker-" + symbol, 
                                            spotBookTickerUri, 
                                            ingestService::onRawSpotBookTicker);
        markPriceClient = new BinanceStreamClient("usdm-markPrice-" + symbol,
                                                usdmMarkPriceUri,
                                                ingestService::onRawMarkPrice);

        spotClient.connect();
        markPriceClient.connect();

        // WebSocket 연결은 비동기라 메인 스레드를 잡아두지 않으면 앱이 바로 종료된다.
        new CountDownLatch(1).await();
    }

    /**
     * 컨텍스트 종료(SIGTERM 등) 시 스프링 부트 종료 훅 스레드에서 호출된다 — run()의
     * CountDownLatch가 영원히 안 풀려도 이 콜백은 별도 스레드라 정상 동작한다.
     */
    @PreDestroy
    public void shutdown() {
        if (spotClient != null) {
            spotClient.close();
        }
        if (markPriceClient != null) {
            markPriceClient.close();
        }
    }
}
