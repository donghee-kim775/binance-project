package com.consumer.dedup;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 체크포인트가 완료될 때 Kafka offset이 커밋된다. 재시작하면 마지막 체크포인트부터 다시 읽는다
        env.enableCheckpointing(10_000);
        env.setParallelism(1);

        for (DedupRoute route : DedupRoute.ALL) {
            addRoute(env, route);
        }

        env.execute("dedup");
    }

    /**
     * 경로 하나의 파이프라인을 붙인다. uid는 경로 이름으로 고정해, 경로를 추가 · 삭제해도
     * 다른 경로의 state(본 키 목록)가 savepoint에서 그대로 이어지게 한다.
     */
    private static void addRoute(StreamExecutionEnvironment env, DedupRoute route) {
        String n = route.name();

        // [다음] 레코드마다 KafkaSource → DedupKeyParser.processElement → keyBy → DedupFunction.processElement → KafkaSink
        env.fromSource(KafkaIO.source(route.rawTopic(), GROUP_ID), WatermarkStrategy.noWatermarks(), "raw." + n)
                .uid("raw." + n)
                .process(new DedupKeyParser(route.idField()))
                .name("parse." + n).uid("parse." + n)
                .keyBy(t -> t.f0 + "|" + t.f1, Types.STRING)
                .process(new DedupFunction(STATE_TTL))
                .name("dedup." + n).uid("dedup." + n)
                .sinkTo(KafkaIO.sink(route.cleanTopic()))
                .name("clean." + n).uid("clean." + n);
    }
}
