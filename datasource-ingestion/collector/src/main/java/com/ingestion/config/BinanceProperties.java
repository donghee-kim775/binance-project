package com.ingestion.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

/**
 * Binance 관련 설정(binance.*)을 한 파일에 모은 것. 값이 무엇에 따라 바뀌는지에 따라 두 record로 나눈다.
 *
 * - {@link Endpoint}     (binance.endpoint.*)     : 환경(테스트넷/프로덕션)에 따라서만 바뀜. 모든 Pod가 같음 → application.yml에만 둔다
 * - {@link Subscription} (binance.subscription.*) : Pod마다 바뀜 → ConfigMap이 덮어쓴다
 *
 * 두 값이 합쳐져 소켓 URL이 된다 (CollectorRunner.run()):
 *   URL = endpoint.<product>.wss-url + streams[].path + (심볼 1개면 /ws/..., 여러 개면 /stream?streams=...)
 *
 *   spot-sov pod  (product=spot, symbols=[BTCUSDT], stream=bookTicker, path="")
 *     wss://stream.binance.com:9443 + "" + /ws/btcusdt@bookTicker
 *     → wss://stream.binance.com:9443/ws/btcusdt@bookTicker
 *
 *   markprice-usdm-optical pod  (product=usdm, symbols=[LITEUSDT, CRDOUSDT, ALABUSDT], stream=markPrice@1s, path=/market)
 *     wss://fstream.binance.com + /market + /stream?streams=liteusdt@markPrice@1s/crdousdt@markPrice@1s/alabusdt@markPrice@1s
 *     → wss://fstream.binance.com/market/stream?streams=liteusdt@markPrice@1s/crdousdt@markPrice@1s/alabusdt@markPrice@1s
 *
 * 접속 주소도 구독 대상도 코드에 하드코딩하지 않고 설정으로 주입한다 (binanceAPIREADME.md
 * "환경별 base URL을 코드에 직접 분기하지 말고 설정으로 주입한다" 참고) — 테스트넷 전환, 도메인 변경,
 * 심볼·스트림 추가 모두 코드 수정 없이 설정만 바꾸면 된다.
 *
 * 값은 이미지 안의 application.yml 기본값을 K8s ConfigMap(/config/application.yaml)이 덮어쓴다.
 * ConfigMap에는 binance.subscription만 있으므로 binance.endpoint는 기본값이 그대로 남는다.
 * yaml의 kebab-case 키(asset-class, usdm-futures)는 스프링 relaxed binding으로 camelCase 필드
 * (assetClass, usdmFutures)에 자동으로 들어간다 (datasource-ingestion/deploy/mapping.md 참고).
 *
 * 각 record가 따로 @ConfigurationProperties를 가지므로 Bean도 둘이다 — @ConfigurationPropertiesScan은
 * static nested 클래스도 찾는다. 구독 설정만 필요한 Bean(MarketDataIngestService)은 Subscription만 주입받는다.
 * 바깥 클래스는 묶음용이라 Bean이 아니고 인스턴스도 만들지 않는다.
 */
public final class BinanceProperties {

    private BinanceProperties() {}

    /**
     * product별 접속 주소. Subscription의 product가 어느 쪽을 쓸지 고른다 (CollectorRunner.baseUrl()).
     *
     * - spot        : Spot 접속 주소
     * - usdmFutures : USD-M 선물 접속 주소 (yaml 키는 usdm-futures, product 값은 usdm)
     * - restapiUrl  : REST API base. 아직 쓰지 않는다(마켓 스트림만 구독 중) — 나중에 REST 폴백(markPrice 등)용
     * - wssUrl      : WebSocket 마켓 스트림 base. 뒤에 path(/public, /market)와 /ws, /stream이 붙는다
     */
    @Validated
    @ConfigurationProperties(prefix = "binance.endpoint")
    public record Endpoint(
        @NotNull @Valid Spot spot,
        @NotNull @Valid UsdmFutures usdmFutures
    ) {

        public record Spot(String restapiUrl, @NotBlank String wssUrl) {}

        public record UsdmFutures(String restapiUrl, @NotBlank String wssUrl) {}
    }

    /**
     * 구독 설정. 어떤 product의 어떤 심볼을, 어떤 스트림으로 받아서 어느 토픽에 보낼지 가른다 —
     * variable-rate / fixed-rate pod는 같은 이미지에 이 값만 다르게 넣어서 띄운다.
     *
     * - group      : 이 앱은 raw만 보내므로 동작은 같고, 로그 라벨로만 쓴다
     *                (Active-Active 중복 제거는 다운스트림 Flink의 책임)
     * - product    : 접속 주소(Endpoint)를 고른다
     * - assetClass : envelope에만 들어간다
     * - sector     : 종목의 역할. stock은 optical / semi, crypto는 sov / l1 등 (APIdocs/cryptoSectorREADME.md). envelope에만 들어간다
     * - symbols    : 1개면 단일 스트림(/ws/...), 여러 개면 combined 스트림(/stream?streams=...)
     * - streams    : 항목 하나 = WebSocket 소켓 하나
     */
    @Validated
    @ConfigurationProperties(prefix = "binance.subscription")
    public record Subscription(
        @NotBlank String group, // variable-rate / fixed-rate
        @NotBlank String product, // spot / usdm
        @NotBlank String assetClass, // crypto / stock
        @NotBlank String sector, // crypto : sov / l1, stock : optical / semi
        @NotEmpty List<@NotBlank String> symbols, // sov : [BTCUSDT, ETHUSDT, ...] / optical : [LITEUSDT, CRDOUSDT, ALABUSDT]
        @NotEmpty List<@Valid StreamSpec> streams // [stream1, stream2, ...] => stream1.stream / stream1.path / stream1.topic, ..
    ) {

        /**
         * 소켓 하나의 구독 정보.
         *
         * - stream : Binance 스트림 이름 (bookTicker, aggTrade, depth@100ms, markPrice@1s ...)
         * - path   : URL 경로 접두어. USD-M은 /public(bookTicker, depth) · /market(aggTrade, markPrice)으로
         *            나뉘고, Spot은 접두어가 없다(""). 틀리면 연결은 되지만 메시지가 오지 않는다
         * - topic  : 이 소켓에서 받은 이벤트를 발행할 Kafka 토픽
         */
        public record StreamSpec(
            @NotBlank String stream,
            @NotNull String path,   // Spot은 ""(접두어 없음)이라 NotBlank가 아니라 NotNull
            @NotBlank String topic
        ) {}
    }
}
