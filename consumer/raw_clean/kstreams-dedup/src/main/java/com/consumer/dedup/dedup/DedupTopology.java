package com.consumer.dedup.dedup;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

/**
 * 경로마다 raw → dedup → clean 파이프라인을 하나씩 붙인 Topology를 만든다.
 *
 *   raw.<routeId> ──> dedup.<routeId> ──> clean.<routeId>
 *   (source)          (DedupProcessor,     (sink)
 *                      store seen-<routeId>)
 *
 * 경로끼리는 연결이 없어 서로 다른 sub-topology가 된다. task = (sub-topology, 파티션)이라
 * 경로 하나(파티션 3)면 task 3개, 경로 2개면 6개다.
 */
public final class DedupTopology {

    private DedupTopology() {
    }

    public static Topology build(List<DedupRoute> routes, Duration ttl) {
        StreamsBuilder builder = new StreamsBuilder(); // 빈 설계도
        
        for (DedupRoute route : routes) {
            addRoute(builder, route, ttl); // route 별 + sub-topology 추가
        }
        
        return builder.build();
    }

    // route 별 sub-topology 생성
    //
    // 현재 topology 구조 (repartition 없음. -> shuffle 없음/task 쪼갬 없음)
    // task 0_0 : P0 -> dedup -> clean
    // task 0_1 : P1 -> dedup -> clean
    // task 0_2 : P2 -> dedup -> clean
    // 
    // => route 당 task 3개
    private static void addRoute(StreamsBuilder builder, DedupRoute route, Duration ttl) {
        String routeId = route.routeId();

        // 1) Store 등록 (store-topic 생성)
        // [x] option 1. : KeyValueStore => ttl 기능 없음. 자체 구현해야 함.
        // [o] option 2. : WindowStore => ttl 기능 있음 
        StoreBuilder<WindowStore<String, Long>> seenStoreBuilder = Stores.windowStoreBuilder(
                Stores.inMemoryWindowStore(route.storeName(),      // store-topic 이름 : application.id-<설정값>-changelog
                                            ttl,                   // changelog retention 기간
                                            Duration.ofMillis(1),  // windowSize : 구간 집계 기간 => 현재 사용 x
                                            false),                // 중복 허용 여부
                Serdes.String(),   // store 키: "BTCUSDT|5001"
                Serdes.Long()    // store 값: 레코드 timestamp
        );    
        builder.addStateStore(seenStoreBuilder);

        // 2) Source 등록 (raw 토픽 읽기)
        KStream<String, String> raw = builder.stream(
                route.rawTopic(),                   // 읽을 토픽 : binance.raw.<routeId>
                Consumed.with(
                    Serdes.String(),            // raw 키: 심볼 "BTCUSDT" (수집 앱이 심볼로 파티셔닝)
                    Serdes.String()             // raw 값: raw JSON 문자열 (Binance 원본 payload 포함)
                ).withName("source.raw." + routeId) // 노드 이름 :  (default : KSTREAM-SOURCE-0000000001)
        );

        // 3) Processor 등록 (중복 제거)
        //   KStream<>.process 인자 (=> override 아님)
        //     - ProcessorSupplier : processor를 만드는 함수. 실행 때 task마다 불려 DedupProcessor 객체를 하나씩 만든다
        //                           get() 하나뿐인 인터페이스라 클래스 대신 람다로 쓴다. () -> ... 가 곧 get()의 구현
        //                           (Kafka Streams가 supplier.get()을 부르면 이 람다 본문이 실행됨)
        //     - Named.as          : 노드 이름 (default : KSTREAM-PROCESSOR-0000000001)
        //     - String... stores  : 이 processor에 연결할 store 이름들 (여러 개 가능)
        KStream<String, String> deduped = raw.process(
                () -> new DedupProcessor(route, ttl), // processor 생성 함수 : task마다 하나씩 새로 만든다 (task별 store를 따로 가짐)
                Named.as("dedup." + routeId),         // 노드 이름
                route.storeName()                     // 연결할 store 이름 : 1)에서 등록한 store. processor가 init()에서 getStateStore로 꺼냄
        );

        // 4) Sink 등록 (clean 토픽 쓰기)
        deduped.to(
                route.cleanTopic(),                   // 쓸 토픽 : binance.clean.<routeId> (compare 실행 땐 .kstreams 접미어)
                Produced.with(
                    Serdes.String(),              // clean 키: 심볼 (raw 그대로)
                    Serdes.String()               // clean 값: envelope JSON + dedup_time
                ).withName("clean." + routeId) // 노드 이름
        );
    }
}
