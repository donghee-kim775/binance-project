package com.consumer.dedup;

/**
 * raw 토픽 하나를 dedup해서 clean 토픽 하나에 쓰는 경로.
 *
 * record = 괄호 안의 필드로 생성자 · 접근자(routeId(), rawTopic() ...) · equals · hashCode · toString을 자동으로 만드는 클래스.
 * Python의 @dataclass(frozen=True)와 같다. 만든 뒤에는 값을 바꿀 수 없다.
 */
public record DedupRoute(
        String routeId,     // spot.crypto.bookticker                경로 식별자 (<product>.<자산종류>.<스트림>). 연산자 이름 · uid 접미어
        String rawTopic,    // binance.raw.spot.crypto.bookticker    읽을 토픽 (Active-Active라 중복 있음)
        String cleanTopic,  // binance.clean.spot.crypto.bookticker  쓸 토픽
        String idField      // u                                     payload의 이벤트 id 필드 (symbol과 묶어 키로 씀)
) {

    // 토픽 이름 규칙: binance.<raw|clean>.<product>.<자산종류>.<스트림>
    private static final String RAW_PREFIX = "binance.raw.";
    private static final String CLEAN_PREFIX = "binance.clean.";

    /*
     * job 인자 한 줄을 경로로 바꾼다. 경로 목록은 deploy/dedup.yaml의 job.args에 있다.
    */
    public static DedupRoute parse(String arg) {
        // "spot.crypto.bookticker:u" → ["spot.crypto.bookticker", "u"]
        String[] parts = arg.split(":");

        // 형식이 <product>.<자산종류>.<스트림>:<id 필드> 가 아니면 job 시작 전에 멈춘다
        if (parts.length != 2 || parts[0].split("\\.").length != 3 || parts[1].isBlank()) {
            throw new IllegalArgumentException(
                    "경로 인자 형식은 <product>.<자산종류>.<스트림>:<id 필드> 이다 (예: spot.crypto.trade:t): " + arg);
        }

        String routeId = parts[0];  // spot.crypto.bookticker  (<product>.<자산종류>.<스트림>)
        String idField = parts[1];  // u

        return new DedupRoute(
                routeId,                   // routeId
                RAW_PREFIX + routeId,      // rawTopic
                CLEAN_PREFIX + routeId,    // cleanTopic
                idField);                  // idField
    }
}
