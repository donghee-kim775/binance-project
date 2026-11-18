-- 체결 테이블. 설계와 raw → 테이블 변환 설명은 trades_erd.md
--
-- clean 토픽 3개 (spot trade, usdm crypto aggTrade, usdm stock aggTrade)
--   → trades_queue (Kafka 엔진, 메시지를 JSON 문자열 그대로 읽는다)
--   → trades_mv (JSON을 컬럼으로 푼다)
--   → trades (저장)

-- 1. 저장 테이블
CREATE TABLE IF NOT EXISTS binance.trades
(
    product         LowCardinality(String),     -- envelope.product: spot / usdm
    asset_class     LowCardinality(String),     -- envelope.asset_class: crypto / stock
    sector          LowCardinality(String),     -- envelope.sector
    event_type      LowCardinality(String),     -- payload.e: trade / aggTrade
    symbol          LowCardinality(String),     -- envelope.symbol (= payload.s)
    trade_id        UInt64,                     -- payload.t (trade) 또는 payload.a (aggTrade)
    price           Decimal64(8),               -- payload.p
    qty             Decimal64(8),               -- payload.q
    fill_count      UInt32,                     -- trade: 1, aggTrade: payload.l - payload.f + 1
    is_buyer_maker  Bool,                       -- payload.m
    trade_time      DateTime64(3, 'UTC'),       -- payload.T (체결 시각)
    event_time      DateTime64(3, 'UTC'),       -- envelope.event_time (= payload.E, 거래소가 보낸 시각)
    receive_time    DateTime64(3, 'UTC'),       -- envelope.receive_time (수집 앱이 받은 시각)
    dedup_time      DateTime64(3, 'UTC'),       -- Flink dedup이 붙인 시각
    collector_id    LowCardinality(String),     -- envelope.collector_id (보낸 수집 pod)
    payload         String CODEC(ZSTD(3)),      -- Binance 원본 payload (재처리용)

    -- m에서 계산한다. m=true면 매도자가 taker(공격적 매도), m=false면 매수자가 taker(공격적 매수)
    taker_side      Enum8('BUY' = 1, 'SELL' = 2) MATERIALIZED if(is_buyer_maker, 'SELL', 'BUY'),
    -- 체결 금액 (USDT 등 quote 자산 기준). Decimal64끼리 곱하면 소수 자릿수가 16이 되어 넘치므로 Decimal128로 바꿔서 곱한다
    quote_qty       Decimal128(8) MATERIALIZED toDecimal128(toDecimal128(price, 8) * toDecimal128(qty, 8), 8)
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(trade_time)
ORDER BY (product, symbol, trade_time, trade_id)
TTL toDate(trade_time) + INTERVAL 30 DAY
SETTINGS ttl_only_drop_parts = 1;

-- 2. Kafka에서 읽는 큐 테이블. 행을 저장하지 않고, 아래 MV가 읽어 갈 때마다 Kafka에서 가져온다
CREATE TABLE IF NOT EXISTS binance.trades_queue
(
    message String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'binance-cluster-kafka-bootstrap.binancekafka.svc:9092',
         kafka_topic_list = 'binance.clean.spot.crypto.trade,binance.clean.usdm.crypto.aggtrade,binance.clean.usdm.stock.aggtrade',
         kafka_group_name = 'clickhouse-trades',
         kafka_format = 'JSONAsString',
         kafka_num_consumers = 1;

-- 3. 큐 → 저장 테이블. message는 clean 토픽의 envelope JSON 한 건이다
CREATE MATERIALIZED VIEW IF NOT EXISTS binance.trades_mv TO binance.trades AS
SELECT
    JSONExtractString(message, 'product')                          AS product,
    JSONExtractString(message, 'asset_class')                      AS asset_class,
    JSONExtractString(message, 'sector')                           AS sector,
    JSONExtractString(message, 'payload', 'e')                     AS event_type,
    JSONExtractString(message, 'symbol')                           AS symbol,
    if(event_type = 'trade',
       JSONExtractUInt(message, 'payload', 't'),
       JSONExtractUInt(message, 'payload', 'a'))                   AS trade_id,
    toDecimal64(JSONExtractString(message, 'payload', 'p'), 8)     AS price,
    toDecimal64(JSONExtractString(message, 'payload', 'q'), 8)     AS qty,
    if(event_type = 'trade', 1,
       JSONExtractUInt(message, 'payload', 'l')
         - JSONExtractUInt(message, 'payload', 'f') + 1)           AS fill_count,
    JSONExtractBool(message, 'payload', 'm')                       AS is_buyer_maker,
    fromUnixTimestamp64Milli(JSONExtractInt(message, 'payload', 'T'), 'UTC') AS trade_time,
    fromUnixTimestamp64Milli(JSONExtractInt(message, 'event_time'), 'UTC')   AS event_time,
    fromUnixTimestamp64Milli(JSONExtractInt(message, 'receive_time'), 'UTC') AS receive_time,
    fromUnixTimestamp64Milli(JSONExtractInt(message, 'dedup_time'), 'UTC')   AS dedup_time,
    JSONExtractString(message, 'collector_id')                     AS collector_id,
    JSONExtractRaw(message, 'payload')                             AS payload
FROM binance.trades_queue;
