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

    public static void main(String[] args) throws Exception {

        // dedup.yaml => flinkConfiguration
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 경로 목록은 job 인자로 받는다 (deploy/dedup.yaml의 job.args). 바꾸면 Operator가 job을 다시 띄운다
        if (args.length == 0) {
            throw new IllegalArgumentException("경로 인자가 없다. deploy/dedup.yaml의 job.args를 확인한다");
        }
        for (String arg : args) {
            addRoute(env, DedupRoute.parse(arg));
        }

        env.execute("dedup");
    }

    /**
     * 경로 하나의 파이프라인을 붙인다: raw → parse → keyBy → dedup → clean
     *
     * 연산자마다 name(Web UI · 로그에 보이는 이름)과 uid(savepoint의 state를 다시 붙일 때 쓰는 이름)를
     * "<단계>.<경로>"로 준다. uid를 경로 이름으로 고정해, 경로를 추가 · 삭제해도 다른 경로의 state가
     * savepoint에서 그대로 이어진다. uid를 바꾸면 그 연산자의 state는 복원되지 않는다.
     */
    private static void addRoute(StreamExecutionEnvironment env, DedupRoute route) {
        String routeId = route.routeId();

        // [다음] 레코드마다 KafkaSource → DedupKeyParser.processElement → keyBy → DedupFunction.processElement → KafkaSink

        // 1. raw 토픽에서 envelope JSON 문자열을 읽는다
        DataStream<String> raw = env
                .fromSource(KafkaIO.source(route.rawTopic(), GROUP_ID), WatermarkStrategy.noWatermarks(), "raw." + routeId)
                .uid("raw." + routeId);

        // 2. dedup 키에 필요한 값만 꺼낸다 → (symbol, id, 원본 JSON). 깨진 메시지는 여기서 버린다
        DataStream<Tuple3<String, String, String>> parsed = raw
                .process(new DedupKeyParser(route.idField()))
                .name("parse." + routeId).uid("parse." + routeId);

        // 3. "symbol|id"로 나눈다. 같은 키는 항상 같은 DedupFunction 인스턴스 · 같은 state로 간다 (여기서 셔플)
        KeyedStream<Tuple3<String, String, String>, String> keyed = parsed
                .keyBy(t -> t.f0 + "|" + t.f1, Types.STRING);

        // 4. 키를 처음 본 것만 통과시키고 dedup_time을 붙인다 → (symbol, JSON)
        DataStream<Tuple2<String, String>> deduped = keyed
                .process(new DedupFunction(STATE_TTL))
                .name("dedup." + routeId).uid("dedup." + routeId);

        // 5. clean 토픽에 쓴다 (key = symbol)
        deduped
                .sinkTo(KafkaIO.sink(route.cleanTopic()))
                .name("clean." + routeId).uid("clean." + routeId);
    }
}
