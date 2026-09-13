package com.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binance 연동 관련 설정을 한곳에 모으되, 성격이 다른 두 값을 명시적으로 분리한다.
 *
 * - {@link Setup}: 인스턴스(Pod)마다 달라지는 값. 어느 심볼을 구독할지는 코드가 아니라
 *   config로 갈라진다는 datasource-ingestion/README.md의 목표에 맞춰 여기서만 주입받는다.
 *   기본값은 application.yml의 binance.setup.symbol이며, 실행 시
 *   `--binance.setup.symbol=ethusdt` 인자나 `--spring.config.location=<yaml>`로 덮어쓴다.
 * - {@link Meta}: 환경(테스트넷/프로덕션)에 따라서만 달라지는, product별 접속 주소.
 *   코드에 도메인을 하드코딩하지 않고 설정으로 주입하는 이유는 binanceAPIREADME.md
 *   "환경별 base URL을 코드에 직접 분기하지 말고 설정으로 주입한다" 참고 — 테스트넷 전환이나
 *   도메인 변경 시 코드 수정 없이 application.yml만 바꾸면 된다. REST base는 아직 이
 *   프로젝트에서 쓰지 않지만(마켓 스트림만 구독 중), 나중에 REST 폴백(markPrice 등)을 붙일 때
 *   새 프로퍼티 클래스를 또 만들 필요 없이 여기 이미 있다.
 */
@ConfigurationProperties(prefix = "binance")
public record BinanceProperties(Setup setup, Meta meta) {

    public record Setup(String symbol) {}

    public record Meta(Spot spot, UsdmFutures usdmFutures) {

        public record Spot(String restBaseUrl, String websocketStreamUrl) {}

        public record UsdmFutures(String restBaseUrl, String websocketStreamUrl) {}
    }
}
