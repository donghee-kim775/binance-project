package com.consumer.redis.basis;

import java.util.List;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consumer.redis.basis.model.BasisUpdate;

import redis.clients.jedis.JedisPooled;

/**
 * basis 한 건을 Redis에 반영한다. Lua 스크립트 하나로 "비교 → 저장 → 심볼 등록 → 알림"을 원자적으로 한다.
 *
 *   latest:usdm:<심볼>:basis     hash   { event_time, json }      대시보드의 지금 상태 표 · KPI
 *   latest:usdm:basis:symbols    set    { BTCUSDT, ETHUSDT, … }    감시 심볼 목록 (KEYS로 찾지 않으려고)
 *   pubsub:usdm:basis            채널   json                       대시보드 Gateway가 구독 → WebSocket push
 *
 * 멱등: 저장된 event_time보다 클 때만 쓴다. 같거나 작으면(재처리 · 늦게 온 값) 아무것도 하지 않고 0을 돌려준다.
 *   그래서 Flink가 재시작해 마지막 체크포인트 이후를 다시 써도 Redis 값 · PUBLISH가 한 번만 반영된다.
 *
 * Python으로 치면:
 *   if stored is None or event_time > stored: hset(...); sadd(...); publish(...)
 * 이걸 클라이언트에서 GET → 비교 → SET으로 나누면 두 writer가 동시에 쓸 때 옛 값이 덮을 수 있어서 Lua로 묶는다.
 */
class RedisLatestWriter implements SinkWriter<BasisUpdate> {

    private static final Logger log = LoggerFactory.getLogger(RedisLatestWriter.class);

    static final String SYMBOLS_KEY = "latest:usdm:basis:symbols";
    static final String CHANNEL = "pubsub:usdm:basis";

    // KEYS[1] = latest:usdm:<심볼>:basis, KEYS[2] = 심볼 set
    // ARGV[1] = event_time, ARGV[2] = json, ARGV[3] = 심볼, ARGV[4] = 채널
    static final String UPDATE_LATEST_LUA = """
            local stored = redis.call('HGET', KEYS[1], 'event_time')
            if stored and tonumber(stored) >= tonumber(ARGV[1]) then
              return 0
            end
            redis.call('HSET', KEYS[1], 'event_time', ARGV[1], 'json', ARGV[2])
            redis.call('SADD', KEYS[2], ARGV[3])
            redis.call('PUBLISH', ARGV[4], ARGV[2])
            return 1
            """;

    private final JedisPooled redis;
    private final String scriptSha;   // 스크립트를 한 번 올려 두고 이후에는 SHA로 부른다 (매번 본문을 보내지 않는다)

    private final Counter written;    // Redis에 반영한 건수
    private final Counter stale;      // event_time이 같거나 옛 값이라 무시한 건수 (재처리 · 중복)

    RedisLatestWriter(String redisHost, int redisPort, SinkWriterMetricGroup metricGroup) {
        this.redis = new JedisPooled(redisHost, redisPort);
        this.scriptSha = redis.scriptLoad(UPDATE_LATEST_LUA);
        this.written = metricGroup.counter("redisWritten");
        this.stale = metricGroup.counter("redisStale");
        log.info("[redis] connected {}:{} script={}", redisHost, redisPort, scriptSha);
    }

    // [호출] Flink 런타임 — BasisUpdateParser가 낸 레코드마다
    @Override
    public void write(BasisUpdate update, Context context) {
        Object updated = redis.evalsha(
                scriptSha,
                List.of(
                        "latest:usdm:" + update.symbol() + ":basis",   // KEYS[1] 최신값 hash
                        SYMBOLS_KEY),                                   // KEYS[2] 심볼 set
                List.of(
                        Long.toString(update.eventTime()),              // ARGV[1] 멱등 비교 값
                        update.json(),                                  // ARGV[2] 저장 · 알림할 원문
                        update.symbol(),                                // ARGV[3] 심볼 set에 넣을 이름
                        CHANNEL));                                      // ARGV[4] Pub/Sub 채널
        if (Long.valueOf(1L).equals(updated)) {
            written.inc();
        } else {
            stale.inc();
        }
    }

    // [호출] Flink 런타임 — 체크포인트 직전 · 입력 끝. write가 이미 동기로 반영해서 따로 할 일이 없다
    @Override
    public void flush(boolean endOfInput) {
    }

    // [호출] Flink 런타임 — subtask가 끝날 때 (job 취소 · 재시작)
    @Override
    public void close() {
        redis.close();
    }
}
