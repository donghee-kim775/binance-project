package com.consumer.dedup;

import java.time.Duration;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * raw 레코드 하나를 받아 (symbol, id)를 처음 보면 dedup_time을 붙여 내보내고, 이미 봤으면 버린다.
 * Flink 버전의 DedupKeyParser + DedupFunction을 합친 것이다.
 *
 * "봤다" 기록 = window store (키 "symbol|id" → 레코드 timestamp)
 *   - 보존 기간(ttl)이 지난 항목은 store가 스스로 지운다 (Flink의 state TTL과 같은 역할)
 *   - 기준 시각은 레코드 timestamp(collector가 보낸 시각)다. 벽시계가 아니라 "이 task가 본 가장 늦은 timestamp"
 *     (stream time)가 ttl만큼 지나야 지워진다
 *
 * 왜 repartition이 없나: raw 토픽의 키가 심볼이라 같은 심볼은 항상 같은 파티션 → 같은 task로 온다.
 *   dedup 키 "symbol|id"도 심볼을 포함하므로, 두 pod가 보낸 같은 이벤트는 반드시 같은 task의 같은 store에서 만난다.
 *   Flink는 keyBy("symbol|id")로 한 번 셔플했지만 여기서는 필요 없다.
 *
 * 스레드: task 하나는 StreamThread 하나에서만 돈다. process()가 동시에 불리지 않아 store에 락이 필요 없다.
 */
class DedupProcessor implements Processor<String, String, String, String> {

    private static final Logger log = LoggerFactory.getLogger(DedupProcessor.class);

    private final DedupRoute route;
    private final long ttlMs;

    private ProcessorContext<String, String> context;
    private WindowStore<String, Long> seen;
    private ObjectMapper mapper;

    DedupProcessor(DedupRoute route, Duration ttl) {
        this.route = route;
        this.ttlMs = ttl.toMillis();
    }

    // [호출] Kafka Streams — task가 이 StreamThread에 할당되어 처음 시작할 때 (리밸런스로 다시 할당되면 또)
    // [스레드] kstreams-dedup-<uuid>-StreamThread-N
    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        // DedupTopology가 이 processor에 붙여 둔 store. task(= 파티션)마다 따로 있다
        this.seen = context.getStateStore(route.storeName());
        this.mapper = new ObjectMapper();
    }

    // [호출] Kafka Streams — raw 레코드마다 (같은 파티션의 레코드는 offset 순서대로)
    // [스레드] kstreams-dedup-<uuid>-StreamThread-N
    @Override
    public void process(Record<String, String> record) {
        // 1. envelope에서 symbol과 이벤트 id만 꺼낸다. 깨진 메시지는 버린다
        //    (예외를 던지면 StreamThread가 죽고, 다시 떠도 같은 메시지에서 또 실패한다)
        ObjectNode envelope;
        String dedupKey;
        try {
            envelope = (ObjectNode) mapper.readTree(record.value());
            JsonNode symbol = envelope.path("symbol");                                // "BTCUSDT"
            JsonNode id = envelope.path("payload").path(route.eventIdField());        // 5001
            if (!symbol.isTextual() || id.isMissingNode() || id.isNull()) {
                log.warn("[dedup] skip: no symbol or payload.{} — {}", route.eventIdField(), truncate(record.value()));
                return;
            }
            dedupKey = symbol.asText() + "|" + id.asText();                           // "BTCUSDT|5001"
        } catch (Exception e) {
            log.warn("[dedup] skip: not JSON — {}", truncate(record.value()));
            return;
        }

        // 2. 앞뒤 ttl 안에 같은 키가 있었나. 두 pod의 timestamp는 수십 ms 차이라 어느 쪽이 먼저 와도 잡힌다
        long ts = record.timestamp();
        try (WindowStoreIterator<Long> it = seen.fetch(dedupKey, ts - ttlMs, ts + ttlMs)) {
            if (it.hasNext()) {
                return; // 이미 보낸 이벤트 → 버림
            }
        }

        // 3. 처음 본 키 → 기록하고 내보낸다. 키는 그대로 심볼이라 clean도 raw와 같은 파티션 규칙을 따른다
        seen.put(dedupKey, ts, ts);
        envelope.put("dedup_time", System.currentTimeMillis());

        Record<String, String> out = record.withValue(envelope.toString());
        context.forward(out); // [다음] sink 노드 → clean 토픽 (EOS 트랜잭션 안에서 store 변경 · offset 커밋과 함께 커밋)
    }

    private static String truncate(String s) {
        if (s == null) {
            return "null";
        }
        return s.length() <= 200 ? s : s.substring(0, 200) + "...";
    }
}
