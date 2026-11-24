package com.consumer.dedup;

/**
 * raw 토픽 하나를 dedup해서 clean 토픽 하나에 쓰는 경로. Flink dedup(consumer/flink-dedup)의 DedupRoute와 같은 형식이다.
 *
 * record = Python의 @dataclass(frozen=True). 생성자 · 접근자(routeId() ...) · equals · toString이 자동으로 생긴다.
 */
public record DedupRoute(
        String routeId,     // spot.crypto.bookticker                경로 식별자. 노드 이름 · store 이름 접미어
        String rawTopic,    // binance.raw.spot.crypto.bookticker    읽을 토픽 (Active-Active라 중복 있음)
        String cleanTopic,  // binance.clean.spot.crypto.bookticker  쓸 토픽
        String eventIdField // u                                     payload의 이벤트 id 필드 (symbol과 묶어 키로 씀)
) {

    // 토픽 이름 규칙: binance.<raw|clean>.<product>.<자산종류>.<스트림>
    private static final String RAW_PREFIX = "binance.raw.";
    private static final String CLEAN_PREFIX = "binance.clean.";

    /** 인자 한 줄을 경로로 바꾼다. 경로 목록은 deploy/dedup.yaml의 args에 있다. */
    public static DedupRoute parse(String arg) {
        // "spot.crypto.bookticker:u" → ["spot.crypto.bookticker", "u"]
        String[] parts = arg.split(":");

        // 형식이 <product>.<자산종류>.<스트림>:<id 필드> 가 아니면 시작 전에 멈춘다
        if (parts.length != 2 || parts[0].split("\\.").length != 3 || parts[1].isBlank()) {
            throw new IllegalArgumentException(
                    "경로 인자 형식은 <product>.<자산종류>.<스트림>:<id 필드> 이다 (예: spot.crypto.trade:t): " + arg);
        }

        String routeId = parts[0];       // spot.crypto.bookticker
        String eventIdField = parts[1];  // u (bookticker · depth의 update id. trade는 t, aggtrade는 a)

        return new DedupRoute(
                routeId,                   // routeId
                RAW_PREFIX + routeId,      // rawTopic
                CLEAN_PREFIX + routeId,    // cleanTopic
                eventIdField);             // eventIdField
    }

    /**
     * clean 토픽 이름 뒤에 접미어를 붙인 경로. Flink dedup과 나란히 띄워 비교할 때만 쓴다 (compare/).
     * 접미어가 비어 있으면 그대로 돌려준다.
     */
    public DedupRoute withCleanSuffix(String suffix) {
        if (suffix.isEmpty()) {
            return this;
        }
        return new DedupRoute(routeId, rawTopic, cleanTopic + suffix, eventIdField);
    }

    /** 이 경로의 state store 이름. changelog 토픽 이름(<application.id>-<store>-changelog)에 들어가므로 바꾸면 state를 잃는다 */
    public String storeName() {
        return "seen-" + routeId;
    }
}
