package com.consumer.dedup.dedup;

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

    // 초기화 : 인자를 받는 순간
    private final DedupRoute route;
    private final long ttlMs;

    // 초기화 : task 시작하는 순간
    private ProcessorContext<String, String> context;
    private WindowStore<String, Long> seenStore;
    private ObjectMapper mapper;

    DedupProcessor(DedupRoute route, Duration ttl) {
        this.route = route;
        this.ttlMs = ttl.toMillis();
    }

    // [호출] Kafka Streams — task가 이 StreamThread에 할당되어 처음 시작할 때
    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.seenStore = context.getStateStore(route.storeName());
        this.mapper = new ObjectMapper();
    }

    // [호출] Kafka Streams — raw 레코드마다 (같은 파티션의 레코드는 offset 순서대로)
    @Override
    public void process(Record<String, String> record) {
        // 1. envelope에서 symbol과 이벤트 id만 꺼낸다. 
        ObjectNode envelope; // 원본 메시징
        String dedupKey; // dedup key

        // 1. envelop 에서 message 꺼낸 후 validate => 깨진 메시지는 버린다(return;)
        try {
            // parsing
            envelope = (ObjectNode) mapper.readTree(record.value());
            JsonNode symbol = envelope.path("symbol");                                     // "BTCUSDT"
            JsonNode eventId = envelope.path("payload").path(route.eventIdField());        // 5001

            // validate
            if (!validate(symbol, eventId, record.value())) {
                return; // symbol이나 이벤트 id가 없음 → 버림
            }
            
            // make dedupkey
            dedupKey = symbol.asText() + "|" + eventId.asText();                     // "BTCUSDT|5001"
        } catch (Exception e) { // 예외 처리
            log.warn("[dedup] skip: not JSON — {}", truncate(record.value()));
            return;
        }

        // 2. seenStore 검증
        long arriveTs = record.timestamp();    // record 도착 시간
        // dedup key, [arriveTs - 1분] ~ [arriveTs + 1분] cursor 선언
        WindowStoreIterator<Long> matchesCursor = seenStore.fetch(
            dedupKey, 
            arriveTs - ttlMs, 
            arriveTs + ttlMs
        );
        try {
            if (matchesCursor.hasNext()) {
                return; // 이미 보낸 이벤트 → 버림 (return으로 나가도 finally는 실행된다)
            }
        } finally {
            matchesCursor.close(); // cursor 자원 정리
        }

        // 3. seenStore 추가 / 원본 record deduptime 추가
        seenStore.put(dedupKey, arriveTs, arriveTs);         // store => key, value, ttl 만료시간
        envelope.put("dedup_time", System.currentTimeMillis());

        // 4. porcess 처리 완료 후, 다음 노드로 보내기
        //  - record 값(dedup_time 추가) 바꾼 후, 다시 패키징해서 보내기
        Record<String, String> out = record.withValue(envelope.toString());
        context.forward(out);
    }

    // 검증성 : validate 함수
    private boolean validate(JsonNode symbol, JsonNode eventId, String rawValue) {
        if (!symbol.isTextual() || eventId.isMissingNode() || eventId.isNull()) {
            log.warn("[dedup] skip: no symbol or payload.{} — {}", route.eventIdField(), truncate(rawValue));
            return false;
        }
        return true;
    }

    // 
    private static String truncate(String s) {
        if (s == null) {
            return "null";
        }
        return s.length() <= 200 ? s : s.substring(0, 200) + "...";
    }
}
