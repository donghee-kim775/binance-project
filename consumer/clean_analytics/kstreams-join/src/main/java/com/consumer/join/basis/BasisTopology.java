package com.consumer.join.basis;

import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import com.consumer.join.basis.model.Basis;
import com.consumer.join.basis.model.BasisSignal;
import com.consumer.join.basis.model.BasisStatsState;
import com.consumer.join.basis.model.MarkPrice;
import com.consumer.join.basis.model.SpotMid;
import com.consumer.join.common.JsonSerde;

/**
 * basis Topology. Spot mid와 USD-M mark price를 심볼로 묶어 basis = mark − spot mid를 낸다 (flink-join BasisJob과 같은 일).
 *
 *   clean spot bookticker ──> parse.spot ──> valid.spot ──> latest-spot (KTable: 심볼별 최신 Spot) ──┐
 *                                                                                                  ├─ join "basis" ─> fresh.basis ─> basis.stats ─> analytics.basis
 *   raw usdm markprice    ──> parse.mark ──> valid.mark ───────────────────────────────────────────┘                  (store basis-stats:
 *                                                                                                                       평균 · 분산 · 연속 횟수)
 *
 * KStream–KTable join: markPrice(stream)가 올 때마다 그 순간 KTable에 있는 같은 심볼의 Spot과 합친다.
 * Spot이 와도 table만 바뀌고 출력은 없다. Flink의 "ValueState에 최신 Spot + markPrice 때 계산"과 같은 모양이다.
 *
 * repartition이 없다: 두 토픽 모두 키가 심볼이고 파티션이 3개라(co-partitioning) 같은 심볼은 같은 파티션 번호에 있다.
 *   그래서 두 토픽이 한 sub-topology로 묶이고 task = 파티션 = 3개다. task 0_0은 spot P0 + mark P0를 같이 처리한다.
 *
 * 상태 없는 빌더라 객체로 만들지 않는다 (kstreams-dedup DedupTopology와 같다).
 */
public final class BasisTopology {

    // store 이름 = changelog 토픽 이름 일부 (<application.id>-latest-spot-changelog). 바꾸면 새 토픽이 생겨 기존 state를 잃는다
    // Flink BasisFunction의 state 이름("latest-spot")과 같게 둔다
    static final String LATEST_SPOT_STORE = "latest-spot"; // 심볼별 최근 Spot mid (SpotMid: symbol · mid · receive_time). markPrice가 올 때 조인 상대
    static final String BASIS_STATS_STORE = "basis-stats"; // 심볼별 통계 state (평균 · 분산 · 연속 횟수 · 마지막 event_time)

    private BasisTopology() {
    }

    // [호출] BasisApp.main / BasisTopologyTest.setUp
    public static Topology build(String spotTopic, String markTopic, String basisTopic) {
        StreamsBuilder builder = new StreamsBuilder(); // 빈 설계도

        // 0. store 선언
        //  - 둘 다 in-memory key-value store. 변경은 changelog 토픽(compact)에 남아 pod가 바뀌면 거기서 복원한다
        //  - exactly-once라 store 변경과 출력이 같은 트랜잭션으로 커밋된다 → 재시작해도 같은 basis가 두 번 누적되지 않는다

        // 0-1. latest-spot: 심볼별 최신 Spot (1단계 toTable이 쓰고 3단계 join이 읽는다)
        //  - changelog: kstreams-basis-latest-spot-changelog
        Materialized<String, SpotMid, KeyValueStore<Bytes, byte[]>> latestSpotStore =
                Materialized.<String, SpotMid>as(
                        Stores.inMemoryKeyValueStore(LATEST_SPOT_STORE) // 이름 = changelog 토픽 이름 일부
                )
                .withKeySerde(Serdes.String())                  // 키: 심볼
                .withValueSerde(new JsonSerde<>(SpotMid.class)) // 값: SpotMid JSON
                .withLoggingEnabled(Map.of());                  // changelog 켬 (기본값이지만 명시). 빈 Map = 토픽 설정 기본값(compact)

        // 0-2. basis-stats: 심볼별 통계 state (5단계 process "basis.stats"가 쓴다)
        //  - changelog: kstreams-basis-basis-stats-changelog
        StoreBuilder<KeyValueStore<String, BasisStatsState>> statsStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(BASIS_STATS_STORE),   // 이름 = changelog 토픽 이름 일부
                        Serdes.String(),                                   // 키: 심볼
                        new JsonSerde<>(BasisStatsState.class)             // 값: BasisStatsState JSON
                );
        builder.addStateStore(statsStoreBuilder);

        // 1. Spot: clean bookTicker → SpotMid → 심볼별 최신값 KTable
        // clean -> parsing -> KTable
        KStream<String, String> spotRaw = builder.stream(
                spotTopic,                              // 읽을 토픽 : binance.clean.spot.crypto.bookticker
                Consumed.with(
                        Serdes.String(),                // 키: 심볼 "BTCUSDT"
                        Serdes.String()                 // 값: envelope JSON 문자열 (dedup_time 포함)
                ).withName("spot.bookticker")           // 노드 이름 : Flink source uid와 같게
        );

        KStream<String, SpotMid> parsedSpot = spotRaw.mapValues(
                BasisParser::parseSpot,                 // spotMid[] / null
                Named.as("parse.spot")                  // 노드 이름 : Flink 연산자 "parse.spot"과 같게
        );

        KStream<String, SpotMid> spot = parsedSpot.filter(
                (symbol, spotMid) -> spotMid != null,   // filter 조건
                Named.as("valid.spot")                  // 노드 이름 (Flink에는 없다. Flink는 parse.spot에서 바로 버린다)
        );

        KTable<String, SpotMid> latestSpot = spot.toTable(
                Named.as(LATEST_SPOT_STORE),            // 노드 이름
                latestSpotStore                         // Materialized (store 설계서)
        );

        // 2. mark: raw markPrice → MarkPrice
        // raw -> parsing
        KStream<String, String> markRaw = builder.stream(
                markTopic,                              // 읽을 토픽 : binance.raw.usdm.crypto.markprice
                Consumed.with(
                        Serdes.String(),                // 키: 심볼 "BTCUSDT"
                        Serdes.String()                 // 값: envelope JSON 문자열
                ).withName("mark.markprice")            // 노드 이름 : Flink source uid와 같게
        );

        KStream<String, MarkPrice> parsedMark = markRaw.mapValues(
                BasisParser::parseMark,                 // MarkPrice [] / null
                Named.as("parse.mark")                  // 노드 이름 : Flink 연산자 "parse.mark"와 같게
        );

        KStream<String, MarkPrice> mark = parsedMark.filter(
                (symbol, markPrice) -> markPrice != null, // filter 조건
                Named.as("valid.mark")                    // 노드 이름 (Flink에는 없다. Flink는 parse.mark에서 바로 버린다)
        );

        // 3. join: markPrice가 올 때 최신 Spot과 합친다
        //    KStream-KTable => KStream.join(table, joiner, Joined(Join-Key Serde, Stream-value Serde, Table-vlaue Serde))
        KStream<String, Basis> joined = mark.join(
                latestSpot,
                new BasisJoiner(),                                          // ValueJoiner
                Joined.with(
                        Serdes.String(),                                    // 키: 심볼
                        new JsonSerde<>(MarkPrice.class),                   // stream 값: MarkPrice
                        new JsonSerde<>(SpotMid.class)                      // table 값: SpotMid
                ).withName("basis")
        );

        // 4. 신선도: |mark.receive_time − spot.receive_time| > 2초면 버린다 (Flink의 basisStaleSpot)
        KStream<String, Basis> fresh = joined.filter(
                (symbol, basis) -> Math.abs(basis.spotAgeMs()) <= 2000, // 2초 (ms)
                Named.as("fresh.basis"));

        // 5. 통계: bps · 최근 평균 · 표준편차 · Z · 연속 횟수 · 상태 · 알림 (Insight 1 과열 판정)
        //    a. 
        //    b. 
        KStream<String, BasisSignal> signal = fresh.process(
                () -> new BasisStatsProcessor(BASIS_STATS_STORE),              // = ProcessorSupplier.get(). 판정 규칙은 BasisStatsProcessor.RULE  [다음] BasisStatsProcessor.process
                Named.as("basis.stats"),
                BASIS_STATS_STORE);                                            // 이 단계가 쓰는 store

        // 6. analytics 토픽에 쓴다 (key = symbol, value = Basis 필드 + 통계 필드 JSON)
        signal.to(
                basisTopic,                                     // 쓸 토픽 : binance.analytics.crypto.basis (비교 실행 땐 .kstreams 접미어)
                Produced.with(
                        Serdes.String(),                        // 키: 심볼
                        new JsonSerde<>(BasisSignal.class)      // 값: BasisSignal JSON (Basis 필드를 펼쳐 쓴다)
                ).withName("analytics.basis"));                 // 노드 이름 : Flink sink uid와 같게

        return builder.build();
    }
}
