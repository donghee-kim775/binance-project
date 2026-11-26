package com.consumer.join.basis;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.consumer.join.basis.model.MarkPrice;
import com.consumer.join.basis.model.SpotMid;
import com.consumer.join.common.KafkaIO;

/**
 * basis job (Insight 1). Spot mid와 USD-M mark price를 심볼로 묶어 basis = mark − spot mid를 낸다.
 *
 *   clean spot bookticker ──> SpotMidParser ───keyBy(symbol)──┐
 *                                                             ├─connect─> BasisFunction ──> analytics basis
 *   raw usdm markprice    ──> MarkPriceParser ─keyBy(symbol)──┘           (최신 Spot state)
 *
 * 두 토픽은 키(심볼)와 파티션 수(3)가 같다 (co-partitioning). keyBy가 어차피 다시 나누므로 Flink 안에서는
 * 필수 조건은 아니지만, 같은 심볼이 두 토픽에서 같은 파티션 번호에 있어 지연 비교가 쉽다.
 */
public class BasisJob {

    static final String GROUP_ID = "flink-basis";
    static final String SPOT_TOPIC = "binance.clean.spot.crypto.bookticker";
    static final String MARK_TOPIC = "binance.raw.usdm.crypto.markprice";
    static final String BASIS_TOPIC = "binance.analytics.crypto.basis";

    // 이보다 오래된 Spot과는 계산하지 않는다 (Spot 연결이 끊겨 state에 옛 값이 남은 경우)
    static final Duration MAX_SPOT_AGE = Duration.ofSeconds(2);

    // 병렬도 설정 (DedupJob과 같은 기준)
    // source · parse = 3 : 토픽 파티션 수(3)에 맞춘다. 같은 병렬도라 한 태스크로 묶인다(chain)
    // basis · sink   = 2 : 같은 병렬도라 묶인다. 셔플은 keyBy 두 번(Spot, mark)뿐이다
    // 슬롯을 연산자끼리 공유하므로 필요한 슬롯은 가장 큰 병렬도(3)다 → TaskManager 3개 (슬롯 1)
    static final int SOURCE_PARALLELISM = 3;
    static final int BASIS_PARALLELISM = 2;

    public static void main(String[] args) throws Exception {
        // basis.yaml의 flinkConfiguration이 여기 들어온다 (체크포인트, MinIO 경로)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 비교 실행용 (compare-kdedup overlay): 다른 dedup 결과를 읽고 다른 토픽에 쓴다. 평소에는 env가 없어 기본값 그대로
        //  - KAFKA_GROUP_ID    : consumer group (기본 flink-basis)
        //  - SPOT_TOPIC_SUFFIX : 입력 clean 토픽 뒤에 붙임 (예: .kstreams → Kafka Streams dedup 결과)
        //  - BASIS_TOPIC_SUFFIX: 출력 토픽 뒤에 붙임 (예: .kdedup.flink)
        String groupId = envOr("KAFKA_GROUP_ID", GROUP_ID);
        String spotTopic = SPOT_TOPIC + envOr("SPOT_TOPIC_SUFFIX", "");
        String basisTopic = BASIS_TOPIC + envOr("BASIS_TOPIC_SUFFIX", "");

        // [다음] Spot:  KafkaSource → SpotMidParser.processElement   → keyBy → BasisFunction.processElement1
        // [다음] mark:  KafkaSource → MarkPriceParser.processElement → keyBy → BasisFunction.processElement2 → KafkaSink

        // 1. Spot: clean bookTicker → (symbol, mid, receive_time)
        //   - 커밋된 offset이 없으면(처음 배포) 지금부터 읽는다. 두 토픽의 밀린 데이터를 각자 따라잡으면 시각이 어긋난다
        DataStream<String> spotRaw = env
                .fromSource(KafkaIO.source(spotTopic, groupId),
                        WatermarkStrategy.noWatermarks(), "spot.bookticker")
                .uid("spot.bookticker")
                .setParallelism(SOURCE_PARALLELISM);

        DataStream<SpotMid> spot = spotRaw
                .process(new SpotMidParser())
                .name("parse.spot")
                .uid("parse.spot")
                .setParallelism(SOURCE_PARALLELISM);

        // 2. mark: raw markPrice → (symbol, p, r, E, receive_time)
        DataStream<String> markRaw = env
                .fromSource(KafkaIO.source(MARK_TOPIC, groupId),
                        WatermarkStrategy.noWatermarks(), "mark.markprice")
                .uid("mark.markprice")
                .setParallelism(SOURCE_PARALLELISM);

        DataStream<MarkPrice> mark = markRaw
                .process(new MarkPriceParser())
                .name("parse.mark")
                .uid("parse.mark")
                .setParallelism(SOURCE_PARALLELISM);

        // 3. 둘 다 심볼로 나눈다. 같은 심볼의 Spot과 mark는 같은 BasisFunction 인스턴스 · 같은 state로 간다
        //   - 키 타입(String)을 직접 알려 준다. 람다 · 메서드 참조에서는 Flink가 제네릭 타입을 못 읽을 때가 있다 (DedupJob과 같다)
        KeyedStream<SpotMid, String> spotBySymbol = spot.keyBy(SpotMid::symbol, Types.STRING);
        KeyedStream<MarkPrice, String> markBySymbol = mark.keyBy(MarkPrice::symbol, Types.STRING);

        // 4. 두 스트림을 한 연산자에 붙인다 (입력 1 = Spot, 입력 2 = mark)
        //   - f0 : symbol
        //   - f1 : basis JSON
        ConnectedStreams<SpotMid, MarkPrice> joined = spotBySymbol.connect(markBySymbol);

        DataStream<Tuple2<String, String>> basis = joined
                .process(new BasisFunction(MAX_SPOT_AGE))
                .name("basis")
                .uid("basis")
                .setParallelism(BASIS_PARALLELISM);

        // 5. analytics 토픽에 쓴다 (key = symbol)
        basis.sinkTo(KafkaIO.sink(basisTopic))
                .name("analytics.basis")
                .uid("analytics.basis")
                .setParallelism(BASIS_PARALLELISM);   // basis와 같게 → chain

        env.execute("basis");
    }

    private static String envOr(String name, String defaultValue) {
        return System.getenv().getOrDefault(name, defaultValue);
    }
}
