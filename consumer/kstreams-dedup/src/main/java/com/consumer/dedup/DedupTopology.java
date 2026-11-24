package com.consumer.dedup;

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
final class DedupTopology {

    private DedupTopology() {
    }

    static Topology build(List<DedupRoute> routes, Duration ttl) {
        StreamsBuilder builder = new StreamsBuilder();
        for (DedupRoute route : routes) {
            addRoute(builder, route, ttl);
        }
        return builder.build();
    }

    private static void addRoute(StreamsBuilder builder, DedupRoute route, Duration ttl) {
        String routeId = route.routeId();

        // 1. "봤다" 기록 store
        //   - in-memory: state가 1분치 키뿐이라 RocksDB(디스크)까지 필요 없다. pod가 다시 뜨면 changelog 토픽에서 복원한다
        //   - 보존 기간 = 윈도우 크기 = ttl. 보존 기간보다 오래된 항목은 store가 지운다
        //   - retainDuplicates = false: 같은 키 · 같은 시각은 하나만 둔다
        StoreBuilder<WindowStore<String, Long>> store = Stores.windowStoreBuilder(
                Stores.inMemoryWindowStore(route.storeName(), ttl, ttl, false),
                Serdes.String(),   // 키: "BTCUSDT|5001"
                Serdes.Long());    // 값: 레코드 timestamp
        builder.addStateStore(store);

        // 2. raw 토픽 (키 = 심볼, 값 = envelope JSON 문자열)
        KStream<String, String> raw = builder.stream(route.rawTopic(),
                Consumed.with(Serdes.String(), Serdes.String()).withName("raw." + routeId));

        // 3. 중복 제거. store 이름을 넘겨야 processor가 getStateStore로 꺼낼 수 있다
        //    () -> new ... : task마다 processor 인스턴스를 새로 만든다 (Python으로 치면 팩토리 함수)
        KStream<String, String> deduped = raw.process(
                () -> new DedupProcessor(route, ttl),
                Named.as("dedup." + routeId),
                route.storeName());

        // 4. clean 토픽
        deduped.to(route.cleanTopic(),
                Produced.with(Serdes.String(), Serdes.String()).withName("clean." + routeId));
    }
}
