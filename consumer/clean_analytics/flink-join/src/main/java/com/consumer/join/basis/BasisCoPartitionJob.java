package com.consumer.join.basis;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.consumer.join.basis.model.MarkPrice;
import com.consumer.join.basis.model.SpotMid;
import com.consumer.join.common.KafkaIO;

/**
 * basis job의 co-partitioning 실험판. BasisJob과 계산은 같고, keyBy 셔플 없이 Kafka 파티션 그대로 join한다.
 *
 *   clean spot .copart ──> SpotMidParser ──> SubtaskCheck ──reinterpret(symbol)──┐
 *                                                                               ├─connect─> BasisFunction ──> basis .copart
 *   raw mark   .copart ──> MarkPriceParser ─> SubtaskCheck ──reinterpret(symbol)──┘
 *                         (모든 연산자 병렬도 3, 한 subtask 안에서 chain — 네트워크를 건너지 않는다)
 *
 * reinterpretAsKeyedStream은 데이터를 옮기지 않고 "이미 keyBy된 상태"라고 믿기만 한다.
 * 그래서 두 입력 토픽을 쓰는 쪽이 Flink 규칙으로 파티션을 골라 둬야 한다:
 *   - clean .copart : flink-dedup  (CLEAN_PARTITIONER=flink-keygroup, consumer/raw_clean/flink-dedup/compare-copart)
 *   - mark  .copart : 수집기       (partitioner.class=FlinkKeyGroupPartitioner, datasource-ingestion/deploy/compare-copart)
 *
 * 지켜야 하는 것 (하나라도 바뀌면 쓰는 쪽과 함께 바꾼다):
 *   - 병렬도 = 두 토픽의 파티션 수 = 3
 *   - maxParallelism = 128 (두 partitioner의 flink.max-parallelism과 같게)
 *   - KafkaSource split 배정 규칙 (connector 5.0.0. 버전을 올리면 flink-dedup의 FlinkKeyGroupPartitionerTest로 확인)
 */
public class BasisCoPartitionJob {

    static final String GROUP_ID = "flink-basis-copart";
    static final String SPOT_TOPIC = "binance.clean.spot.crypto.bookticker.copart";
    static final String MARK_TOPIC = "binance.raw.usdm.crypto.markprice.copart";
    static final String BASIS_TOPIC = "binance.analytics.crypto.basis.copart";

    static final Duration MAX_SPOT_AGE = Duration.ofSeconds(2);

    // 병렬도: 모든 연산자 3 = 두 토픽의 파티션 수. 같은 병렬도라 source부터 sink까지 한 태스크로 묶인다(chain)
    // BasisJob(basis 2)과 다르다: 파티션 i를 읽은 subtask i가 그 키의 state 주인이어야 해서 파티션 수에 묶인다
    static final int PARALLELISM = 3;
    // 키 → key group 계산의 칸 수. 쓰는 쪽 partitioner와 같아야 하고, 바꾸면 savepoint의 keyed state도 못 찾는다
    static final int MAX_PARALLELISM = 128;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(MAX_PARALLELISM);   // 기본값도 128이지만 partitioner와 맞춰야 하므로 고정한다

        // [다음] Spot:  KafkaSource → SpotMidParser.processElement   → SubtaskCheck.processElement → BasisFunction.processElement1
        // [다음] mark:  KafkaSource → MarkPriceParser.processElement → SubtaskCheck.processElement → BasisFunction.processElement2 → KafkaSink

        // 1. Spot: clean bookTicker(.copart) → (symbol, mid, receive_time)
        DataStream<String> spotRaw = env
                .fromSource(KafkaIO.source(SPOT_TOPIC, GROUP_ID),
                        WatermarkStrategy.noWatermarks(), "spot.bookticker")
                .uid("spot.bookticker")
                .setParallelism(PARALLELISM);

        DataStream<SpotMid> spot = spotRaw
                .process(new SpotMidParser())
                .name("parse.spot")
                .uid("parse.spot")
                .setParallelism(PARALLELISM);

        DataStream<SpotMid> spotChecked = spot
                .process(new SubtaskCheck<>(SpotMid::symbol, "spot"))
                .name("check.spot")
                .uid("check.spot")
                .setParallelism(PARALLELISM);

        // 2. mark: raw markPrice(.copart) → (symbol, p, r, E, receive_time)
        DataStream<String> markRaw = env
                .fromSource(KafkaIO.source(MARK_TOPIC, GROUP_ID),
                        WatermarkStrategy.noWatermarks(), "mark.markprice")
                .uid("mark.markprice")
                .setParallelism(PARALLELISM);

        DataStream<MarkPrice> mark = markRaw
                .process(new MarkPriceParser())
                .name("parse.mark")
                .uid("parse.mark")
                .setParallelism(PARALLELISM);

        DataStream<MarkPrice> markChecked = mark
                .process(new SubtaskCheck<>(MarkPrice::symbol, "mark"))
                .name("check.mark")
                .uid("check.mark")
                .setParallelism(PARALLELISM);

        // 3. keyBy 대신 "이미 심볼로 나뉘어 있다"고 알려 준다 (셔플 없음)
        //   - 키 타입(String)을 직접 알려 준다. BasisJob의 keyBy와 같은 키 · 같은 타입이어야 key group이 같다
        KeyedStream<SpotMid, String> spotBySymbol = DataStreamUtils.reinterpretAsKeyedStream(
                spotChecked,         // 입력
                SpotMid::symbol,     // 키: BTCUSDT
                Types.STRING);       // 키 타입
        KeyedStream<MarkPrice, String> markBySymbol = DataStreamUtils.reinterpretAsKeyedStream(
                markChecked,         // 입력
                MarkPrice::symbol,   // 키: BTCUSDT
                Types.STRING);       // 키 타입

        // 4. 두 스트림을 한 연산자에 붙인다 (입력 1 = Spot, 입력 2 = mark). 계산은 BasisJob과 같은 BasisFunction
        //   - f0 : symbol
        //   - f1 : basis JSON
        ConnectedStreams<SpotMid, MarkPrice> joined = spotBySymbol.connect(markBySymbol);

        DataStream<Tuple2<String, String>> basis = joined
                .process(new BasisFunction(MAX_SPOT_AGE))
                .name("basis")
                .uid("basis")
                .setParallelism(PARALLELISM);

        // 5. analytics 토픽(.copart)에 쓴다 (key = symbol, Kafka 기본 partitioner)
        basis.sinkTo(KafkaIO.sink(BASIS_TOPIC))
                .name("analytics.basis")
                .uid("analytics.basis")
                .setParallelism(PARALLELISM);

        env.execute("basis-copart");
    }
}
