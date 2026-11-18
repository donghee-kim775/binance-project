package com.consumer.dedup;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.consumer.common.KafkaIO;

/**
 * dedup job. raw 토픽마다 (symbol, id)가 처음 온 것만 clean 토픽으로 보낸다.
 *
 * 경로(raw → clean 쌍)마다 독립된 파이프라인을 하나의 job 안에 둔다 (consumer/flink-design.md 2장).
 * job을 경로마다 띄우면 JobManager · TaskManager가 그만큼 늘어 kind 자원이 버티지 못한다.
 *
 *   raw ──> DedupKeyParser ──keyBy(symbol|id)──> DedupFunction ──> clean
 *           (symbol, id 추출)                    (처음 본 키만 통과)
 */
public class DedupJob {

    static final String GROUP_ID = "flink-dedup";
    static final Duration STATE_TTL = Duration.ofMinutes(1);

    // 병렬도 설정
    // raw · parse = 3 : raw 토픽 파티션 수(3)에 맞춘다. 같은 병렬도라 한 태스크로 묶여(chain) JSON 파싱도 3개로 나뉜다
    // dedup · sink = 2 : 같은 병렬도라 묶인다. 셔플은 keyBy 한 번뿐이다
    //   주의: dedup이 1보다 크면 같은 심볼의 다른 eventId가 다른 subtask로 가서 clean 토픽에서 순서가 바뀔 수 있다
    static final int SOURCE_PARALLELISM = 3;
    static final int DEDUP_PARALLELISM = 2;

    public static void main(String[] args) throws Exception {

        // dedup.yaml => flinkConfiguration
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 경로 목록은 job 인자로 받는다 (deploy/dedup.yaml의 job.args). 바꾸면 Operator가 job을 다시 띄운다
        if (args.length == 0) {
            throw new IllegalArgumentException("경로 인자가 없다. deploy/dedup.yaml의 job.args를 확인한다");
        }
        for (String arg : args) {
            // env : 
            //  - StreamExecutionEnvironment
            //
            // DedupRoute.parse(arg) : 
            //  - routeId : spot.crypto.bookticker
            //  - rawTopic : binance.raw.spot.crypto.bookticker
            //  - cleanTopic : binance.clean.spot.crypto.bookticker
            //  - eventIdField : u(updateId) / t(tradeId) / a(aggTradeId)
            addRoute(env, DedupRoute.parse(arg));
        }

        env.execute("dedup");
    }

    /*
     * 경로 하나의 파이프라인을 붙인다: raw → parse → keyBy → dedup → clean
     * state operator
     *  - 1. raw : offset 기록
     *  - 4. dedup : seen state
     * stateless operator
     *  - 2. parse
     *  - 3. KeyBy
     *  - 5. sink
    */
    private static void addRoute(StreamExecutionEnvironment env, DedupRoute route) {        
        String routeId = route.routeId();
        String sourceTopic = route.rawTopic();
        String sinkTopic = route.cleanTopic();
        
        // [다음] 레코드마다 KafkaSource → DedupKeyParser.processElement → keyBy → DedupFunction.processElement → KafkaSink

        // 1. raw 토픽에서 envelope JSON 문자열을 읽는다
        //   - {"symbol":"BTCUSDT","payload":{"u":5001,"b":"97000.1",...},...}
        DataStream<String> raw = env
                .fromSource(KafkaIO.source(sourceTopic, GROUP_ID), WatermarkStrategy.noWatermarks(), "raw." + routeId)
                .name("raw." + routeId)
                .uid("raw." + routeId)
                .setParallelism(SOURCE_PARALLELISM);

        // 2. dedup 키에 필요한 값만 꺼낸다. 깨진 메시지는 여기서 버린다
        //   - f0 = symbol : BTCUSDT
        //   - f1 = eventId : 5001
        //   - f2 = { 원본 JSON 문자열 }
        DataStream<Tuple3<String, String, String>> parsed = raw
                .process(new DedupKeyParser(route.eventIdField())) // u / t
                .name("parse." + routeId)
                .setParallelism(SOURCE_PARALLELISM);   // raw와 같게 → chain

        // 3. "symbol|id"로 나눈다. 같은 키는 항상 같은 DedupFunction 인스턴스 · 같은 state로 간다 (여기서 셔플)
        //   - f0|f1 -> symbol|eventId -> BTCUSDT|5001 => hash
        KeyedStream<Tuple3<String, String, String>, String> keyed = parsed
                .keyBy(t -> t.f0 + "|" + t.f1, Types.STRING);

        // 4. 중복 제거
        //   - f0 : symbol
        //   - f1 : { 원본 JSON, dedup_time }
        DataStream<Tuple2<String, String>> deduped = keyed
                .process(new DedupFunction(STATE_TTL))
                .name("dedup." + routeId)
                .uid("dedup." + routeId)
                .setParallelism(DEDUP_PARALLELISM);

        // 5. clean 토픽에 쓴다 (key = symbol)
        //   - 원본 레코드 보냄
        deduped.sinkTo(KafkaIO.sink(sinkTopic))
                .name("clean." + routeId)
                .uid("clean." + routeId)
                .setParallelism(DEDUP_PARALLELISM);   // dedup과 같게 → chain
    }
}
