package com.consumer.dedup;

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
     * job 인자 하나를 경로로 바꾼다. 형식: {@code <product>.<자산종류>.<스트림>:<id 필드>} (예: spot.crypto.trade:t)
     * 경로 목록은 deploy/dedup.yaml의 job.args에 있다.
     */
    public static DedupRoute parse(String arg) {
        String[] parts = arg.split(":");
        if (parts.length != 2 || parts[0].split("\\.").length != 3 || parts[1].isBlank()) {
            throw new IllegalArgumentException(
                    "경로 인자 형식은 <product>.<자산종류>.<스트림>:<id 필드> 이다 (예: spot.crypto.trade:t): " + arg);
        }
        return route(parts[0], parts[1]);
    }

    /** 토픽 이름 규칙 binance.<raw|clean>.<product>.<자산종류>.<스트림> 으로 raw · clean 쌍을 만든다 */
    private static DedupRoute route(String suffix, String idField) {
        return new DedupRoute(suffix, "binance.raw." + suffix, "binance.clean." + suffix, idField);
    }
}
