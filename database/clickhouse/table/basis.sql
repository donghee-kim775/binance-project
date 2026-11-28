-- basis 이력 테이블 (Insight 1 대시보드의 30분 그래프 · ±2σ 밴드 · 과열 점 · 지연 p50).
-- 설계: Insights/1.spot_futures_basis/1_2_delivery_guarantee.md 4.3 (at-least-once + 멱등 테이블)
--
-- binance.analytics.crypto.basis (kstreams-basis가 exactly-once로 씀, 심볼당 초당 1건)
--   → basis_queue (Kafka 엔진, 메시지를 JSON 문자열 그대로 읽는다. librdkafka 기본값 read_committed라 커밋된 것만 본다)
--   → basis_mv   (JSON을 컬럼으로 푼다)
--   → basis      (저장. 같은 (symbol, event_time)은 merge 때 한 건으로 합쳐진다)
--
-- 만들기 (Infra/ClickHouse/README.md "테이블 만들기"):
--   kubectl exec -i -n binance-storage clickhouse-0 -- clickhouse-client --user admin --password '<비밀번호>' --multiquery \
--     < database/clickhouse/table/basis.sql

-- 1. 저장 테이블
--  - ReplacingMergeTree(basis_time): 같은 정렬 키 (symbol, event_time)가 두 번 들어오면(Kafka 엔진 재시작으로 다시 읽음)
--    merge 때 basis_time이 큰 한 건만 남긴다. merge 전에는 잠깐 두 건이 보일 수 있어 조회는 FINAL이나 argMax로 한다
--  - 가격(mark · spot · basis)은 Decimal로 정확히, 통계(bps · Z 등)는 Float64로 둔다 (출력 JSON과 같은 기준)
CREATE TABLE IF NOT EXISTS binance.basis
(
    symbol              LowCardinality(String),     -- BTCUSDT
    event_time          DateTime64(3, 'UTC'),       -- markPrice E (Binance 시각). 정렬 키 · 멱등 키
    mark_price          Decimal64(8),               -- 78853.70000000
    spot_mid            Decimal64(8),               -- 78760.565
    basis               Decimal64(8),               -- 93.135 = mark − spot mid
    funding_rate        Decimal64(8),               -- 0.00008778
    receive_time        DateTime64(3, 'UTC'),       -- markPrice를 collector가 받은 시각
    spot_receive_time   DateTime64(3, 'UTC'),       -- 짝지은 Spot을 collector가 받은 시각
    spot_age_ms         Int32,                      -- receive_time − spot_receive_time
    basis_time          DateTime64(3, 'UTC'),       -- kstreams-basis가 계산한 시각 (지연 = basis_time − receive_time)
    bps                 Float64,                    -- basis ÷ spot_mid × 10,000
    mean_bps            Float64,                    -- 이번 값을 넣기 전 EWMA 평균 (5분)
    std_bps             Float64,                    -- 이번 값을 넣기 전 EWMA 표준편차
    z                   Float64,                    -- (bps − mean_bps) / std_bps. warmup · check_data면 0
    z_cont_warn_cnt     UInt16,                     -- |Z| > 2 연속 횟수 (JSON: zContWarnCnt)
    status              LowCardinality(String),     -- warmup · check_data · normal · warning · alert
    alert_started       Bool,                       -- 과열이 시작된 그 한 건만 true

    latency_ms          Int64 MATERIALIZED dateDiff('millisecond', receive_time, basis_time)  -- collector → basis 지연
)
ENGINE = ReplacingMergeTree(basis_time)
PARTITION BY toDate(event_time)
ORDER BY (symbol, event_time)
TTL toDate(event_time) + INTERVAL 7 DAY      -- 대시보드는 30분만 본다. 일주일치는 분석 · 디버깅용
SETTINGS ttl_only_drop_parts = 1;

-- 2. Kafka에서 읽는 큐 테이블. 행을 저장하지 않고, 아래 MV가 읽어 갈 때마다 Kafka에서 가져온다
--  - 컨슈머 그룹 clickhouse-basis (README 규칙: clickhouse-<테이블>)
CREATE TABLE IF NOT EXISTS binance.basis_queue
(
    message String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'binance-cluster-kafka-bootstrap.binancekafka.svc:9092',
         kafka_topic_list = 'binance.analytics.crypto.basis',
         kafka_group_name = 'clickhouse-basis',
         kafka_format = 'JSONAsString',
         kafka_num_consumers = 1;

-- 3. 큐 → 저장 테이블. message는 BasisSignal JSON 한 건이다 (Basis 필드 + 통계 필드가 한 단계로 펼쳐져 있다)
CREATE MATERIALIZED VIEW IF NOT EXISTS binance.basis_mv TO binance.basis AS
SELECT
    JSONExtractString(message, 'symbol')                                         AS symbol,
    fromUnixTimestamp64Milli(JSONExtractInt(message, 'event_time'), 'UTC')       AS event_time,
    toDecimal64(JSONExtractString(message, 'mark_price'), 8)                     AS mark_price,
    toDecimal64(JSONExtractString(message, 'spot_mid'), 8)                       AS spot_mid,
    toDecimal64(JSONExtractString(message, 'basis'), 8)                          AS basis,
    toDecimal64(JSONExtractString(message, 'funding_rate'), 8)                   AS funding_rate,
    fromUnixTimestamp64Milli(JSONExtractInt(message, 'receive_time'), 'UTC')     AS receive_time,
    fromUnixTimestamp64Milli(JSONExtractInt(message, 'spot_receive_time'), 'UTC') AS spot_receive_time,
    JSONExtractInt(message, 'spot_age_ms')                                       AS spot_age_ms,
    fromUnixTimestamp64Milli(JSONExtractInt(message, 'basis_time'), 'UTC')       AS basis_time,
    JSONExtractFloat(message, 'bps')                                             AS bps,
    JSONExtractFloat(message, 'mean_bps')                                        AS mean_bps,
    JSONExtractFloat(message, 'std_bps')                                         AS std_bps,
    JSONExtractFloat(message, 'z')                                               AS z,
    JSONExtractUInt(message, 'zContWarnCnt')                                     AS z_cont_warn_cnt,
    JSONExtractString(message, 'status')                                         AS status,
    JSONExtractBool(message, 'alert_started')                                    AS alert_started
FROM binance.basis_queue;
