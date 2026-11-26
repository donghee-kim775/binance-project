package com.consumer.join.basis;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import com.consumer.join.basis.model.Basis;
import com.consumer.join.basis.model.MarkPrice;
import com.consumer.join.basis.model.SpotMid;
import com.consumer.join.common.JsonSerde;

/**
 * basis Topology. Spot mid와 USD-M mark price를 심볼로 묶어 basis = mark − spot mid를 낸다 (flink-join BasisJob과 같은 일).
 *
 *   clean spot bookticker ──> parse.spot ──> latest-spot (KTable: 심볼별 최신 Spot) ──┐
 *                                                                                    ├─ join "basis" ─> fresh.basis ─> analytics.basis
 *   raw usdm markprice    ──> parse.mark ────────────────────────────────────────────┘
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
    static final String LATEST_SPOT_STORE = "latest-spot";

    private BasisTopology() {
    }

    // [호출] BasisApp.main / BasisTopologyTest.setUp
    public static Topology build(String spotTopic, String markTopic, String basisTopic, Duration maxSpotAge) {
        StreamsBuilder builder = new StreamsBuilder(); // 빈 설계도
        long maxSpotAgeMs = maxSpotAge.toMillis();

        // 1. Spot: clean bookTicker → SpotMid → 심볼별 최신값 KTable
        KStream<String, String> spotRaw = builder.stream(
                spotTopic,                              // 읽을 토픽 : binance.clean.spot.crypto.bookticker
                Consumed.with(
                        Serdes.String(),                // 키: 심볼 "BTCUSDT"
                        Serdes.String()                 // 값: envelope JSON 문자열 (dedup_time 포함)
                ).withName("spot.bookticker")           // 노드 이름 : Flink source uid와 같게
        );

        KStream<String, SpotMid> parsedSpot = spotRaw.mapValues(
                SpotMidParser::parse,                   // 깨진 메시지는 null
                Named.as("parse.spot"));

        KStream<String, SpotMid> spot = parsedSpot.filter(
                (symbol, spotMid) -> spotMid != null,   // null(깨진 메시지)만 버린다
                Named.as("valid.spot"));

        //  - toTable : 같은 키(심볼)의 새 값이 오면 덮어쓴다 → 심볼마다 최신 Spot 하나 (Flink ValueState와 같다)
        //  - store   : in-memory key-value store. 변경은 changelog 토픽(compact)에 남아 pod가 바뀌면 복원한다
        //  - TTL 없음 : 심볼 수만큼만 쌓인다
        KTable<String, SpotMid> latestSpot = spot.toTable(
                Named.as(LATEST_SPOT_STORE),
                Materialized.<String, SpotMid>as(Stores.inMemoryKeyValueStore(LATEST_SPOT_STORE))
                        .withKeySerde(Serdes.String())                  // store 키: 심볼
                        .withValueSerde(new JsonSerde<>(SpotMid.class)) // store 값: SpotMid JSON
        );

        // 2. mark: raw markPrice → MarkPrice
        KStream<String, String> markRaw = builder.stream(
                markTopic,                              // 읽을 토픽 : binance.raw.usdm.crypto.markprice
                Consumed.with(
                        Serdes.String(),                // 키: 심볼 "BTCUSDT"
                        Serdes.String()                 // 값: envelope JSON 문자열
                ).withName("mark.markprice")            // 노드 이름 : Flink source uid와 같게
        );

        KStream<String, MarkPrice> parsedMark = markRaw.mapValues(
                MarkPriceParser::parse,                 // 깨진 메시지는 null
                Named.as("parse.mark"));

        KStream<String, MarkPrice> mark = parsedMark.filter(
                (symbol, markPrice) -> markPrice != null,
                Named.as("valid.mark"));

        // 3. join: markPrice가 올 때 최신 Spot과 합친다
        //  - inner join : 이 심볼의 Spot이 아직 table에 없으면 출력하지 않는다 (Flink의 basisNoSpot)
        //  - Joined     : repartition이 필요할 때 쓸 Serde (지금은 co-partitioning이라 쓰이지 않는다) + 노드 이름
        KStream<String, Basis> joined = mark.join(
                latestSpot,
                new BasisJoiner(),                                          // [다음] BasisJoiner.apply
                Joined.with(
                        Serdes.String(),                                    // 키: 심볼
                        new JsonSerde<>(MarkPrice.class),                   // stream 값: MarkPrice
                        new JsonSerde<>(SpotMid.class)                      // table 값: SpotMid
                ).withName("basis"));

        // 4. 신선도: |mark.receive_time − spot.receive_time| > 2초면 버린다 (Flink의 basisStaleSpot)
        KStream<String, Basis> fresh = joined.filter(
                (symbol, basis) -> Math.abs(basis.spotAgeMs()) <= maxSpotAgeMs,
                Named.as("fresh.basis"));

        // 5. analytics 토픽에 쓴다 (key = symbol, value = Flink 버전과 같은 JSON)
        fresh.to(
                basisTopic,                             // 쓸 토픽 : binance.analytics.crypto.basis (비교 실행 땐 .kstreams 접미어)
                Produced.with(
                        Serdes.String(),                // 키: 심볼
                        new JsonSerde<>(Basis.class)    // 값: Basis JSON
                ).withName("analytics.basis"));         // 노드 이름 : Flink sink uid와 같게

        return builder.build();
    }
}
