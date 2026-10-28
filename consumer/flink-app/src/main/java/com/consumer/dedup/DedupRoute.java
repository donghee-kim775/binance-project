package com.consumer.dedup;

import java.util.List;

/**
 * raw 토픽 하나를 dedup해서 clean 토픽 하나에 쓰는 경로.
 *
 * @param name       Flink 연산자 이름 · uid 접두어 (Web UI와 로그에서 경로를 구분한다)
 * @param rawTopic   읽을 토픽 (Active-Active pod 2개가 같은 이벤트를 각자 보내 중복이 있다)
 * @param cleanTopic 쓸 토픽
 * @param idField    Binance 원본 payload 안의 이벤트 id 필드. 심볼마다 따로 매기는 번호라 심볼과 묶어 키로 쓴다
 */
public record DedupRoute(String name, String rawTopic, String cleanTopic, String idField) {

    /**
     * 지금 수집하는 variable-rate 스트림 전부 (docs/kafka/topics.md "clean").
     * markprice는 fixed-rate(Active-Standby)라 중복이 없어 경로가 없다.
     * 스트림이 늘면 여기에 한 줄 추가하고, raw · clean 토픽 yaml을 같이 만든다.
     */
    public static final List<DedupRoute> ALL = List.of(
            route("spot", "crypto", "bookticker", "u"),
            route("spot", "crypto", "trade", "t"),
            route("spot", "crypto", "depth", "u"),
            route("usdm", "crypto", "bookticker", "u"),
            route("usdm", "crypto", "aggtrade", "a"),
            route("usdm", "crypto", "depth", "u"),
            route("usdm", "stock", "bookticker", "u"),
            route("usdm", "stock", "aggtrade", "a"),
            route("usdm", "stock", "depth", "u"));

    /** 토픽 이름 규칙 binance.<raw|clean>.<product>.<자산종류>.<스트림> 으로 raw · clean 쌍을 만든다 */
    private static DedupRoute route(String product, String assetClass, String stream, String idField) {
        String suffix = product + "." + assetClass + "." + stream;
        return new DedupRoute(suffix, "binance.raw." + suffix, "binance.clean." + suffix, idField);
    }
}
