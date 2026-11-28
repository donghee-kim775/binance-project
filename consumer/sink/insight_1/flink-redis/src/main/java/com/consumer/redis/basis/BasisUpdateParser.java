package com.consumer.redis.basis;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.redis.basis.model.BasisUpdate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * analytics basis JSON → BasisUpdate (symbol, event_time, 원문). 깨진 메시지는 경고 후 버린다.
 * 예외를 던지면 job이 재시작되고, 다시 떠도 같은 메시지에서 또 실패하기 때문이다 (poison pill).
 */
class BasisUpdateParser extends ProcessFunction<String, BasisUpdate> {

    private static final Logger log = LoggerFactory.getLogger(BasisUpdateParser.class);

    private transient ObjectMapper mapper;
    private transient Counter broken;   // 버린 건수 (Flink metric)

    // [호출] Flink 런타임 — 태스크 시작 시 한 번
    @Override
    public void open(OpenContext openContext) {
        mapper = new ObjectMapper();
        broken = getRuntimeContext().getMetricGroup().counter("basisRedisBroken");
    }

    // [호출] Flink 런타임 — analytics basis 레코드마다 (심볼당 초당 1건)
    @Override
    public void processElement(String basisJson, Context ctx, Collector<BasisUpdate> out) {
        try {
            // 1. JSON 파싱 (필요한 두 필드만 본다. 나머지는 원문 그대로 넘긴다)
            JsonNode basis = mapper.readTree(basisJson);

            // 2. 심볼 · event_time이 없으면 버린다
            if (!validate(basis, basisJson)) {
                broken.inc();
                return;
            }

            BasisUpdate update = new BasisUpdate(
                    basis.get("symbol").asText(),       // BTCUSDT
                    basis.get("event_time").asLong(),   // 1788098181003
                    basisJson);                         // 원문
            out.collect(update);   // [다음] RedisLatestWriter.write
        } catch (Exception error) {
            broken.inc();
            log.warn("[redis] skip basis: not valid JSON — {}", Logs.truncate(basisJson));
        }
    }

    // 심볼(키 이름)과 event_time(멱등 비교)이 있어야 Redis에 쓸 수 있다. 없으면 로그를 남기고 false
    private static boolean validate(JsonNode basis, String basisJson) {
        if (!basis.path("symbol").isTextual() || !basis.path("event_time").canConvertToLong()) {
            log.warn("[redis] skip basis: no symbol or event_time — {}", Logs.truncate(basisJson));
            return false;
        }
        return true;
    }
}
