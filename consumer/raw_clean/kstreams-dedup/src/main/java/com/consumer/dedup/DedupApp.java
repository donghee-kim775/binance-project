package com.consumer.dedup;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.dedup.config.KafkaConfig;
import com.consumer.dedup.dedup.DedupRoute;
import com.consumer.dedup.dedup.DedupTopology;

/**
 * Kafka Streams dedup 앱. raw 토픽마다 (symbol, id)가 처음 온 것만 clean 토픽으로 보낸다.
 *
 * Flink 버전(consumer/raw_clean/flink-dedup)과 하는 일은 같고 달라진 것은 셋이다:
 *   - 클러스터가 없다. 이 프로세스(Deployment pod) 안에서 StreamThread가 돈다
 *   - exactly-once: 읽은 offset · store 변경 · clean 쓰기를 한 Kafka 트랜잭션으로 커밋한다 → 재시작해도 clean에 중복이 다시 생기지 않는다
 *   - state(본 키 목록)는 MinIO 체크포인트가 아니라 Kafka changelog 토픽에 남는다
 *
 * 경로 목록은 인자로 받는다 (deploy/dedup.yaml의 args, 형식은 Flink 버전과 같다).
 */
public class DedupApp {

    private static final Logger log = LoggerFactory.getLogger(DedupApp.class);

    // 두 pod 사이 도착 시차(수십 ms)보다 넉넉하게. Flink 버전과 같다
    public static final Duration TTL = Duration.ofMinutes(1);

    public static void main(String[] args) throws Exception {
    // 0. prepare prop
        Properties kafkaProperties = KafkaConfig.create(); // [다음] config/KafkaConfig.create
        
    // 1. validate args
        if (args.length == 0) {
            throw new IllegalArgumentException("경로 인자가 없다. deploy/dedup.yaml의 args를 확인한다");
        }

    // 2. args parsing -> topology
        String cleanSuffix = env("CLEAN_TOPIC_SUFFIX", "");

        List<DedupRoute> routes = new ArrayList<>();
        for (String arg : args) {
            DedupRoute route = DedupRoute.parse(arg); // ex) routeId, rawTopic, cleanTopic, eventIdField
            routes.add(route.withCleanSuffix(cleanSuffix));
        }

        Topology topology = DedupTopology.build(routes, TTL);

    // 3. KafkaStreams 생성 (topology + 설정을 묶기만 한다. 아직 Kafka에 붙지 않음)
        KafkaStreams streams = new KafkaStreams(topology, kafkaProperties);
        // + StreamThread 예외 처리
        // error 발생시 SHUTDOWN_CLIENT => StreaThread 멈춤 => 상태 ERROR
        streams.setUncaughtExceptionHandler(error -> {
            log.error("[dedup] stream thread failed", error);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

    // 4. 종료 신호 : main은 이 신호가 올 때까지 기다린다 (처리는 StreamThread가 한다)
        CountDownLatch latch = new CountDownLatch(1);        
        
    // 5-a. Stream 상태가 바뀌면 취할 행동
        // 상태가 변경된다.
        // StateListener.onChange() =>
        //  error? => latch.countDown() => latch.await() 풀림
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.ERROR) {
                latch.countDown();
            }
        });

    // 5-b. SIGTERM(pod 종료) → 종료 스레드 생성
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(20)); // KStreams 종료 : 처리 시간 기준 - 20초
            latch.countDown();
        }, "dedup-shutdown") // 종료 스레드 이름
        );

    // 6. 실행 → 종료 신호까지 대기
        streams.start(); // [다음] StreamThread → DedupProcessor.init → process
        latch.await();

    // 7. KStreams ERROR로 인한 종료로직
        boolean failed = streams.state() == KafkaStreams.State.ERROR;
        if (failed) {
            System.exit(1); // 7)의 shutdown hook이 close를 부른다
        }
    }

    private static String env(String name, String defaultValue) {
        return System.getenv().getOrDefault(name, defaultValue);
    }
}
