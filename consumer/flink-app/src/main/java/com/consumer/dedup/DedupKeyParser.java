package com.consumer.dedup;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * envelope JSON에서 dedup 키에 필요한 값만 꺼낸다 → (symbol, id, 원본 JSON).
 *
 * 형식이 깨진 메시지(JSON 아님, symbol이나 id 없음)는 경고 로그를 남기고 버린다.
 * 예외를 던지면 job 전체가 재시작되고, 같은 메시지를 다시 읽어 또 실패하는 일이 반복되기 때문이다.
 */
class DedupKeyParser extends ProcessFunction<String, Tuple3<String, String, String>> {

    private static final Logger log = LoggerFactory.getLogger(DedupKeyParser.class);

    private final String idField;
    private transient ObjectMapper mapper;

    DedupKeyParser(String idField) {
        this.idField = idField;
    }

    // [호출] Flink 런타임 — 태스크 시작 시 한 번
    @Override
    public void open(OpenContext openContext) {
        mapper = new ObjectMapper();
    }

    // [호출] Flink 런타임 — raw 레코드마다
    @Override
    public void processElement(String json, Context ctx, Collector<Tuple3<String, String, String>> out) {
        try {
            JsonNode envelope = mapper.readTree(json);
            JsonNode symbol = envelope.path("symbol");
            JsonNode id = envelope.path("payload").path(idField);
            if (!symbol.isTextual() || id.isMissingNode() || id.isNull()) {
                log.warn("[dedup] skip: no symbol or payload.{} — {}", idField, abbreviate(json));
                return;
            }
            out.collect(Tuple3.of(symbol.asText(), id.asText(), json));
        } catch (Exception e) {
            log.warn("[dedup] skip: not JSON — {}", abbreviate(json));
        }
    }

    private static String abbreviate(String s) {
        return s.length() <= 200 ? s : s.substring(0, 200) + "...";
    }
}
