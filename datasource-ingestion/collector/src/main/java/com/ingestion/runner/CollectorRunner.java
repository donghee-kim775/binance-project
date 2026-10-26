package com.ingestion.runner;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

import com.ingestion.collector.BinanceStreamClient;
import com.ingestion.config.BinanceProperties;
import com.ingestion.config.BinanceProperties.Subscription.StreamSpec;
import com.ingestion.service.MarketDataIngestService;

/**
 * 수집기(producer) 진입점. {@link BinanceProperties.Subscription}의 streams 항목마다 WebSocket 소켓을 하나씩
 * 열고, 받은 raw 메시지를 {@link MarketDataIngestService}로 넘긴다. 어떤 스트림을 열지는 코드가
 * 아니라 config가 정한다 — variable-rate / fixed-rate pod는 같은 이 코드에 ConfigMap만 다르게 넣는다
 * (datasource-ingestion/deploy/mapping.md 참고).
 *
 * 파싱·발행은 여기서 하지 않는다 — {@link MarketDataIngestService#onRaw}가 @Async로 별도
 * 스레드풀에서 처리한다 (WebSocket 콜백 스레드를 파싱으로 묶어두지 않기 위함). 수신 시각만은
 * 큐 대기 시간이 섞이지 않도록 콜백 스레드에서 바로 찍어서 넘긴다.
 *
 * 이 앱은 raw를 그대로 옮기기만 한다 — 중복 제거(Active-Active), basis 계산 같은 조인은
 * 다운스트림 Flink 앱의 책임이다.
 *
 * run()이 끝나지 않고 계속 떠 있어야 하는 프로세스라, "test" 프로필에서는 비활성화한다 —
 * 그렇지 않으면 @SpringBootTest로 컨텍스트를 띄울 때마다 테스트가 영원히 끝나지 않는다.
 */
@Component
@Profile("!test")
public class CollectorRunner implements ApplicationRunner {

    private final MarketDataIngestService ingestService;
    private final BinanceProperties.Endpoint endpoint;
    private final BinanceProperties.Subscription subscription;

    private final List<BinanceStreamClient> clients = new ArrayList<>();

    // 객체 생성자
    public CollectorRunner(MarketDataIngestService ingestService,
                           BinanceProperties.Endpoint endpoint,
                           BinanceProperties.Subscription subscription) {
        // 주소 참조
        this.ingestService = ingestService;
        this.endpoint = endpoint;
        this.subscription = subscription;
    }

    // 현재 args는 쓰지 않는다. 무엇을 구독할지는 생성자로 주입받은 설정이 정한다
    //  - endpoint     (binance.endpoint.*)     : jar의 application.yml
    //  - subscription (binance.subscription.*) : application.yml 기본값을 K8s ConfigMap이 덮어씀
    @Override
    public void run(ApplicationArguments args) throws InterruptedException {
        // product → 접속 도메인. 설정 키 이름이 달라(usdm ↔ usdm-futures) 여기서 변환한다
        String baseUrl = switch (subscription.product()) {
            case "spot" -> endpoint.spot().wssUrl();
            case "usdm" -> endpoint.usdmFutures().wssUrl();
            default -> throw new IllegalArgumentException("unknown product: " + subscription.product());
        };

        // streams 항목 하나 = 소켓 하나
        for (StreamSpec spec : subscription.streams()) {
        // ── URL 조립 ─────────────────────────────────────────────
            // baseUrl + routePrefix(/public · /market · "") + /ws/... 또는 /stream?streams=...
            // USD-M은 routePrefix를 틀리면 연결만 되고 메시지가 전혀 안 온다
            // (developers.binance.com USDⓈ-M Futures WebSocket Market Streams 문서 참고)
            URI uri = URI.create(baseUrl + spec.routePrefix() + streamSuffix(spec.stream()));

        // ── BinanceStreamClient ─────────────────────────────────────────────
            // parameter 1 : streamlabel  
            // (예: 
            //  - "usdm-optical-markPrice@1s"
            //  - "spot-sov-bookTicker")
            // parameter 2 : websocket uri
            // parameter 3 : onMessage callback — 수신 시각은 콜백 스레드에서 바로 찍는다
            BinanceStreamClient client = new BinanceStreamClient(
                streamLabel(spec),
                uri,
                raw -> ingestService.onRaw(spec, raw, System.currentTimeMillis())
            );

            clients.add(client);
            client.connect();
        }

        // WebSocket 연결은 비동기라 메인 스레드를 잡아두지 않으면 앱이 바로 종료된다.
        new CountDownLatch(1).await();
    }

    // ── URL 조립 ─────────────────────────────────────────────
    // * symbols → 스트림 경로. Binance 스트림 이름은 소문자 심볼을 쓴다
    //   심볼 단일    : "/ws/"              +  btcusdt@bookTicker
    //
    //   심볼 다중    : "/stream?streams="  +  liteusdt@markPrice@1s/
    //                                        crdousdt@markPrice@1s/
    //                                        ...
    //                 (combined — 메시지가 {"stream": ..., "data": {...}}로 한 겹 감싸져 온다)
    private String streamSuffix(String stream) {
        List<String> symbolList = subscription.symbols().stream()
                .map(symbol -> symbol.toLowerCase() + "@" + stream)
                .toList();

        return symbolList.size() == 1
                ? "/ws/" + symbolList.get(0)
                : "/stream?streams=" + String.join("/", symbolList);
    }

    // * 로그 구분용 라벨 — pod 이름과 같이 섹터(sov, optical)로 구분한다 (예: usdm-optical-bookTicker)
    private String streamLabel(StreamSpec spec) {
        return subscription.product() + "-" + subscription.sector() + "-" + spec.stream();
    }

    /**
     * 컨텍스트 종료(SIGTERM 등) 시 스프링 부트 종료 훅 스레드에서 호출된다 — run()의
     * CountDownLatch가 영원히 안 풀려도 이 콜백은 별도 스레드라 정상 동작한다.
     */
    @PreDestroy
    public void shutdown() {
        clients.forEach(BinanceStreamClient::close);
    }
}
