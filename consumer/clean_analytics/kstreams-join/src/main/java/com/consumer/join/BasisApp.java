package com.consumer.join;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.join.basis.BasisTopology;
import com.consumer.join.config.KafkaConfig;

/**
 * Kafka Streams basis 앱 (Insight 1). flink-join의 BasisJob과 같은 일을 한다.
 * clean Spot bookTicker의 최신 mid와 raw USD-M markPrice를 심볼로 묶어 basis = mark − spot mid를 analytics 토픽에 쓴다.
 *
 * Flink 버전과 달라진 것:
 *   - 클러스터가 없다. 이 프로세스(Deployment pod) 안에서 StreamThread가 돈다
 *   - 최신 Spot을 ValueState가 아니라 KTable(store latest-spot + changelog 토픽)이 들고 있다
 *   - exactly-once: 읽은 offset · store 변경 · basis 쓰기를 한 Kafka 트랜잭션으로 커밋한다 (Flink sink는 at-least-once)
 */
public class BasisApp {

    private static final Logger log = LoggerFactory.getLogger(BasisApp.class);

    // flink-join BasisJob과 같은 값
    static final String SPOT_TOPIC = "binance.clean.spot.crypto.bookticker";
    static final String MARK_TOPIC = "binance.raw.usdm.crypto.markprice";
    static final String BASIS_TOPIC = "binance.analytics.crypto.basis";

    public static void main(String[] args) throws Exception {
    // 0. prepare prop
        Properties kafkaProperties = KafkaConfig.create(); // [다음] config/KafkaConfig.create

    // 1. 출력 토픽
        // 비교 실행용: Flink basis와 같은 토픽에 쓰지 않도록 뒤에 붙인다 (예: ".kstreams"). 평소에는 비워 둔다
        String basisSuffix = env("BASIS_TOPIC_SUFFIX", "");
        String basisTopic = BASIS_TOPIC + basisSuffix;
        // 비교 실행용: 다른 dedup 결과(clean 토픽 뒤 접미어, 예: ".kstreams")를 읽는다. 평소에는 비워 둔다
        String spotSuffix = env("SPOT_TOPIC_SUFFIX", "");
        String spotTopic = SPOT_TOPIC + spotSuffix;

    // 2. topology
        Topology topology = BasisTopology.build(
                spotTopic,          // 입력 1 : 최신값만 들고 있을 쪽 (KTable)
                MARK_TOPIC,         // 입력 2 : 올 때마다 basis를 낼 쪽 (KStream)
                basisTopic);        // 출력
        log.info("[basis] spot={} output={}\n{}", spotTopic, basisTopic, topology.describe());

    // 3. KafkaStreams 생성 (topology + 설정을 묶기만 한다. 아직 Kafka에 붙지 않음)
        KafkaStreams streams = new KafkaStreams(topology, kafkaProperties);
        // + StreamThread 예외 처리
        // error 발생시 SHUTDOWN_CLIENT => StreamThread 멈춤 => 상태 ERROR
        // [호출] Kafka Streams — StreamThread에서 예외가 빠져나왔을 때 (StreamsUncaughtExceptionHandler.handle)
        // [스레드] 예외가 난 StreamThread
        streams.setUncaughtExceptionHandler(error -> {
            log.error("[basis] stream thread failed", error);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

    // 4. 종료 신호 : main은 이 신호가 올 때까지 기다린다 (처리는 StreamThread가 한다)
        CountDownLatch latch = new CountDownLatch(1);

    // 5-a. Stream 상태가 바뀌면 취할 행동
        //  error? => latch.countDown() => latch.await() 풀림
        // [호출] Kafka Streams — 상태가 바뀔 때마다 (StateListener.onChange)
        // [스레드] Kafka Streams 내부 스레드
        streams.setStateListener((newState, oldState) -> {
            log.info("[basis] state {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                latch.countDown(); // [다음] 6)의 latch.await()가 풀린다
            }
        });

    // 5-b. SIGTERM(pod 종료) → 종료 스레드 생성
        // [호출] JVM — SIGTERM 또는 System.exit 때
        // [스레드] basis-shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(20)); // KStreams 종료 : terminationGracePeriodSeconds(30)보다 짧게
            latch.countDown();
        }, "basis-shutdown") // 종료 스레드 이름
        );

    // 6. 실행 → 종료 신호까지 대기
        streams.start(); // [다음] StreamThread → parse.spot / parse.mark → BasisJoiner.apply
        latch.await();

    // 7. KStreams ERROR로 인한 종료로직
        boolean failed = streams.state() == KafkaStreams.State.ERROR;
        if (failed) {
            System.exit(1); // 5-b)의 shutdown hook이 close를 부른다
        }
    }

    private static String env(String name, String defaultValue) {
        return System.getenv().getOrDefault(name, defaultValue);
    }
}
