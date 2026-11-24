package com.consumer.dedup;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams dedup 앱. raw 토픽마다 (symbol, id)가 처음 온 것만 clean 토픽으로 보낸다.
 *
 * Flink 버전(consumer/flink-dedup)과 하는 일은 같고 달라진 것은 셋이다:
 *   - 클러스터가 없다. 이 프로세스(Deployment pod) 안에서 StreamThread가 돈다
 *   - exactly-once: 읽은 offset · store 변경 · clean 쓰기를 한 Kafka 트랜잭션으로 커밋한다 → 재시작해도 clean에 중복이 다시 생기지 않는다
 *   - state(본 키 목록)는 MinIO 체크포인트가 아니라 Kafka changelog 토픽에 남는다
 *
 * 경로 목록은 인자로 받는다 (deploy/dedup.yaml의 args, 형식은 Flink 버전과 같다).
 */
public class DedupApp {

    private static final Logger log = LoggerFactory.getLogger(DedupApp.class);

    // consumer group 이름이기도 하다 (kafka-exporter · 대시보드의 consumergroup 라벨). 바꾸면 offset과 state를 처음부터 다시 쌓는다
    static final String APPLICATION_ID = "kstreams-dedup";
    // 두 pod 사이 도착 시차(수십 ms)보다 넉넉하게. Flink 버전과 같다
    static final Duration TTL = Duration.ofMinutes(1);

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("경로 인자가 없다. deploy/dedup.yaml의 args를 확인한다");
        }
        // 비교 실행용: Flink dedup과 같은 clean 토픽에 쓰지 않도록 clean 토픽 이름 뒤에 붙인다 (예: ".kstreams"). 평소에는 비워 둔다
        String cleanSuffix = env("CLEAN_TOPIC_SUFFIX", "");

        List<DedupRoute> routes = new ArrayList<>();
        for (String arg : args) {
            DedupRoute route = DedupRoute.parse(arg);
            routes.add(route.withCleanSuffix(cleanSuffix));
        }

        Topology topology = DedupTopology.build(routes, TTL);
        log.info("[dedup] routes={}\n{}", routes, topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, config());

        // main 스레드는 종료 신호를 기다리기만 한다. 실제 처리는 StreamThread가 한다
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean(false);

        // 처리 중 잡히지 않은 예외 → 이 인스턴스를 내린다 → 아래 리스너가 exit(1) → Kubernetes가 pod를 다시 띄운다
        // (REPLACE_THREAD로 스레드만 다시 띄우면 같은 레코드에서 계속 실패하며 pod는 Running으로 보인다)
        // [호출] Kafka Streams — StreamThread에서 예외가 빠져나왔을 때
        streams.setUncaughtExceptionHandler(e -> {
            log.error("[dedup] stream thread failed", e);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        // [호출] Kafka Streams — 상태가 바뀔 때마다 (CREATED → REBALANCING → RUNNING, 오류 시 → ERROR)
        streams.setStateListener((newState, oldState) -> {
            log.info("[dedup] state {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                failed.set(true);
                done.countDown();
            }
        });

        // [호출] JVM — SIGTERM (pod 종료, kubectl rollout) 때
        // [스레드] JVM shutdown hook 스레드
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // 진행 중인 트랜잭션을 커밋하고 consumer group에서 나간다. terminationGracePeriodSeconds(30)보다 짧게
            streams.close(Duration.ofSeconds(20));
            done.countDown();
        }, "dedup-shutdown"));

        streams.start(); // [다음] StreamThread가 뜨고 리밸런스로 task를 받은 뒤 DedupProcessor.init → process
        done.await();

        if (failed.get()) {
            streams.close(Duration.ofSeconds(20));
            System.exit(1);
        }
    }

    static Properties config() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19090"));

        // exactly-once (트랜잭션). 커밋 간격 기본값이 100ms라 clean을 read_committed로 읽는 쪽도 거의 바로 본다
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // 스레드 수. task(= 경로 수 × 파티션 3)를 이 pod 안 스레드와 다른 pod가 나눠 가진다
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, env("NUM_STREAM_THREADS", "1"));
        // task 잠금 파일 · 오프셋 체크포인트 파일 위치 (store는 in-memory라 데이터는 여기 없다)
        p.put(StreamsConfig.STATE_DIR_CONFIG, env("STATE_DIR", "/tmp/kafka-streams"));

        // Kafka Streams가 만드는 내부 토픽(changelog)도 다른 토픽처럼 복제 3 · min ISR 2
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        p.put(StreamsConfig.topicPrefix("min.insync.replicas"), "2");

        // changelog 보존: 기본값은 store 보존 기간(1분) + 24시간이고, segment가 닫히기(기본 7일) 전에는 지우지 않는다
        //   → pod가 바뀔 때 복원하며 하루치 이상을 읽게 된다. store에 필요한 것은 1분치뿐이라 둘 다 줄인다
        //   (토픽을 처음 만들 때만 적용된다. 이미 있는 changelog는 kafka-configs로 바꾼다)
        p.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, Duration.ofMinutes(1).toMillis());
        p.put(StreamsConfig.topicPrefix("segment.ms"), String.valueOf(Duration.ofMinutes(10).toMillis()));

        // clean 토픽이 compression.type=zstd라 producer도 zstd로 보낸다 (다르면 broker가 다시 압축한다)
        p.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");
        return p;
    }

    private static String env(String name, String defaultValue) {
        return System.getenv().getOrDefault(name, defaultValue);
    }
}
