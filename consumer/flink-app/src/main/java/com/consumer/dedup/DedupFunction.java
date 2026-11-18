package com.consumer.dedup;

import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * "symbol|id" 키마다 이미 봤는지만 기억한다. 처음 오면 dedup_time을 붙여 (심볼, JSON)으로 내보내고, 두 번째부터 버린다.
 *
 * state는 TTL 뒤 지워진다. 두 pod 사이 도착 시차(수십 ms)보다 넉넉하면 되고, 키가 계속 쌓이지 않게 한다.
 * TTL보다 늦게 온 중복은 걸러지지 않는다 (consumer/flink-design.md 3장 trade-off).
 */
class DedupFunction extends KeyedProcessFunction<String, Tuple3<String, String, String>, Tuple2<String, String>> {

    private final Duration ttl;
    private transient ValueState<Boolean> seen;
    private transient ObjectMapper mapper;

    DedupFunction(Duration ttl) {
        this.ttl = ttl;
    }

    // [호출] Flink 런타임 — 태스크 시작 시 한 번
    @Override
    public void open(OpenContext openContext) {
        // state 명세:
        // - "seen"(savepoint 복원 시 식별자, 바꾸면 state를 못 찾음)
        // - 값 타입 Boolean(직렬화기 결정)
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Types.BOOLEAN);

        // 쓴 뒤 ttl이 지나면 만료 → value()가 null. 삭제는 state backend가 접근 시 · 점진 정리로 한다
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(ttl).build());

        // Flink가 관리하는 state의 핸들. 하나의 핸들로 모든 키를 다루고, 레코드마다 현재 키의 값만 보인다
        seen = getRuntimeContext().getState(descriptor);

        // 직렬화 대상이 아니라(transient) TaskManager에서 여기서 만든다
        mapper = new ObjectMapper();
    }

    // [호출] Flink 런타임 — 레코드마다 (현재 키 = 이 레코드의 "symbol|id", state도 이 키의 것만 보인다)
    @Override
    public void processElement(Tuple3<String, String, String> in, Context ctx,
            Collector<Tuple2<String, String>> out) throws Exception {

        // state.value() 내에서 실행 중인 처리중인 Key 를 꺼냄
        // seen.value()
        // ㄴ stateTable.get(currentNamespace);
        //    ㄴ get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
        if (seen.value() != null) {
            return; // 이미 보내진 event => 해당 서브태스크에서 버림.
        }

        // update 도 위와 동일
        seen.update(true);
 
        ObjectNode envelope = (ObjectNode) mapper.readTree(in.f2);
        envelope.put("dedup_time", System.currentTimeMillis());

        out.collect(Tuple2.of(in.f0, mapper.writeValueAsString(envelope)));
    }
}
