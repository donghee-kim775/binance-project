# ConfigMap 설정 → 코드 매핑

`deploy/<group>/*.yaml`의 ConfigMap에 적은 `ingestion.*` 값이 collector 코드 어디로 들어가서 어떻게 실행되는지 정리한다.

> **현재 상태:** 코드가 실제로 읽는 값은 `binance.setup.symbol` 하나뿐이다. `ingestion.*`(group, product, sector, symbols, streams)을 받는 클래스는 아직 없어서 Spring이 무시한다. 아래 내용 중 **[구현 필요]** 로 표시한 부분은 이 설정을 반영하기 위해 앞으로 만들 코드다. 클래스 이름(`IngestionProperties`, `StreamSpec` 등)은 제안이다.

---

## 0. 설정이 앱에 들어가는 경로

```text
deploy/fixed-rate/markprice-usdm-stock-optical.yaml
  └─ ConfigMap  data.application.yaml
        │  Deployment가 /config 에 마운트 (volumeMounts)
        ▼
/config/application.yaml
        │  Dockerfile의 ENV SPRING_CONFIG_ADDITIONAL_LOCATION=optional:file:/config/
        │  → Spring이 이미지 안의 application.yml을 먼저 읽고, 이 파일로 덮어씀
        ▼
Spring Environment
        │  @ConfigurationPropertiesScan (IngestionCollectorApplication)
        ▼
@ConfigurationProperties 클래스에 바인딩
  ├─ binance.*   → BinanceProperties     (있음)
  └─ ingestion.* → IngestionProperties   [구현 필요]
        ▼
CollectorRunner.run()  →  BinanceStreamClient  →  MarketDataIngestService  →  RawEventPublisher  →  Kafka
```

| 단계 | 파일 | 상태 |
| --- | --- | --- |
| 마운트 | `deploy/<group>/*.yaml` (`volumes`, `volumeMounts`) | 있음 |
| 로드 | `deploy/Dockerfile` (`SPRING_CONFIG_ADDITIONAL_LOCATION`) | 있음 |
| 바인딩 (`binance.*`) | `config/BinanceProperties.java` | 있음 |
| 바인딩 (`ingestion.*`) | `config/IngestionProperties.java` | [구현 필요] |
| 실행 | `runner/CollectorRunner.java` | 있음. 단, 스트림이 코드에 고정 |

---

## 1. 바인딩: yaml 키 → Java 필드

```yaml
ingestion:
  group: fixed-rate
  product: usdm
  asset-class: stock
  sector: optical
  symbols: [LITEUSDT, CRDOUSDT, ALABUSDT]
  streams:
    - stream: markPrice@1s
      path: "/market"
      topic: binance.raw.usdm.stock.markprice
```

```java
// [구현 필요] config/IngestionProperties.java
@ConfigurationProperties(prefix = "ingestion")
public record IngestionProperties(
        String group,              // ingestion.group
        String product,            // ingestion.product
        String assetClass,         // ingestion.asset-class (kebab-case → camelCase 자동 변환)
        String sector,             // ingestion.sector
        List<String> symbols,      // ingestion.symbols
        List<StreamSpec> streams) { // ingestion.streams

    public record StreamSpec(String stream, String path, String topic) {}
}
```

`IngestionCollectorApplication`에 이미 `@ConfigurationPropertiesScan`이 있으므로, 이 record만 만들면 Bean으로 자동 등록된다.

---

## 2. 필드별 매핑

### ① `group` — 중복 제거 적용 여부

| 값 | 의미 | 코드에서 하는 일 |
| --- | --- | --- |
| `variable-rate` | Active-Active (replicas 2) | 발행 전 dedup 적용 (같은 이벤트를 두 pod가 모두 받음) |
| `fixed-rate` | Active-Standby (replicas 1) | dedup 생략 (중복이 생기지 않음) |

```java
// [구현 필요] RawEventPublisher
boolean dedupEnabled = "variable-rate".equals(ingestion.group());
```

- 그 밖에 로그와 metric의 태그로 쓴다.
- K8s의 `rate-group` label(`variable` / `fixed`)은 Deployment 쪽에서 따로 붙인 값이라 이 설정과 별개다.

### ② `product` — 접속 도메인과 파서 선택

`product`는 `application.yml`의 `binance.meta.*` 중 어느 주소를 쓸지 고른다. 키 이름이 다르므로(`usdm` ↔ `usdm-futures`) 코드에서 변환한다.

| `product` | 사용하는 설정 | 실제 값 |
| --- | --- | --- |
| `spot` | `binance.meta.spot.websocket-stream-url` | `wss://stream.binance.com:9443` |
| `usdm` | `binance.meta.usdm-futures.websocket-stream-url` | `wss://fstream.binance.com` |

```java
// [구현 필요] CollectorRunner
String baseUrl = switch (ingestion.product()) {
    case "spot" -> binance.meta().spot().websocketStreamUrl();
    case "usdm" -> binance.meta().usdmFutures().websocketStreamUrl();
    default -> throw new IllegalArgumentException("unknown product: " + ingestion.product());
};
```

- **파서 선택:** Spot과 USD-M은 같은 스트림이라도 필드가 다르다 (Spot bookTicker에는 `e` 필드가 없음). `product`로 어느 파서를 쓸지 정한다.
- **envelope:** `product` 필드에 그대로 들어간다.

### ③ `asset-class`, `sector` — envelope 메타데이터

URL이나 토픽 선택에는 쓰지 않고, 메시지에 붙는 정보로만 쓴다.

| 필드 | envelope 필드 | 예 | 용도 |
| --- | --- | --- | --- |
| `asset-class` | `asset_class` | `stock` | 소비자가 자산 종류 확인 |
| `sector` | `sector` | `optical` | 같은 stock 토픽 안에서 섹터 구분. crypto는 `null` |

- 토픽은 이미 `binance.raw.usdm.stock.*`처럼 자산 종류까지 나뉘어 있고, 섹터는 토픽을 나누지 않기로 했다 ([topic/plan.md](../../plan/topic/plan.md) 고려 사항 2). 그래서 섹터는 envelope에만 남는다.
- `BinanceStreamClient`의 로그 라벨(`streamLabel`)에도 넣어 어느 소켓의 로그인지 구분한다.

### ④ `symbols` — URL 조립

심볼 개수에 따라 URL 형식이 달라진다.

| 심볼 수 | 형식 | 예 |
| --- | --- | --- |
| 1개 | `/ws/<symbol>@<stream>` (단일) | `/ws/btcusdt@bookTicker` |
| 여러 개 | `/stream?streams=<a>@<stream>/<b>@<stream>/...` (combined) | `/stream?streams=liteusdt@markPrice@1s/crdousdt@markPrice@1s/alabusdt@markPrice@1s` |

```java
// [구현 필요] CollectorRunner
List<String> names = ingestion.symbols().stream()
        .map(s -> s.toLowerCase() + "@" + spec.stream())   // Binance 스트림 이름은 소문자
        .toList();
String suffix = names.size() == 1
        ? "/ws/" + names.get(0)
        : "/stream?streams=" + String.join("/", names);
```

- **combined 메시지는 한 겹 감싸져 온다:** `{"stream": "liteusdt@markPrice@1s", "data": {...}}` 형태라, 파싱 전에 `data`를 꺼내야 한다.
- **Kafka 키는 설정이 아니라 메시지에서 정한다:** 섹터 소켓 하나로 여러 심볼이 섞여 오므로, 키는 `data.s`(예: `LITEUSDT`)를 쓴다.

### ⑤ `streams` — 소켓, 파서, 토픽

`streams`의 **항목 하나 = 소켓(`BinanceStreamClient`) 하나**다. 각 항목의 세 필드가 쓰이는 곳은 다음과 같다.

| 필드 | 쓰이는 곳 | 예 |
| --- | --- | --- |
| `stream` | URL의 스트림 이름 (④에서 심볼 뒤에 붙음), 파서 선택 | `markPrice@1s` → `MarkPriceUpdate` |
| `path` | URL 경로 접두어 (`baseUrl`과 `/ws`, `/stream` 사이) | `/market` |
| `topic` | `RawEventPublisher`가 발행할 Kafka 토픽 | `binance.raw.usdm.stock.markprice` |

`path`는 USD-M에서 꼭 맞아야 한다. 틀리면 연결은 되지만 메시지가 오지 않는다.

| product | stream | path |
| --- | --- | --- |
| spot | 전부 | `""` (접두어 없음) |
| usdm | `bookTicker`, `depth@100ms` | `/public` |
| usdm | `aggTrade`, `markPrice@1s` | `/market` |

```java
// [구현 필요] CollectorRunner
for (StreamSpec spec : ingestion.streams()) {
    URI uri = URI.create(baseUrl + spec.path() + suffix(spec));     // ② + ⑤path + ④
    String label = String.join("-", ingestion.product(), sectorOrSymbol(), spec.stream());
    BinanceStreamClient client =
            new BinanceStreamClient(label, uri, raw -> ingestService.onRaw(spec, raw));
    clients.add(client);
    client.connect();
}
```

---

## 3. 전체 실행 예시: `markprice-usdm-stock-optical`

```text
① group   = fixed-rate       → dedup 생략
② product = usdm             → baseUrl = wss://fstream.binance.com,  파서 = USD-M
③ sector  = optical          → envelope.sector = "optical"
④ symbols = 3개              → combined 형식
⑤ streams = 1개              → 소켓 1개
     stream = markPrice@1s
     path   = /market
     topic  = binance.raw.usdm.stock.markprice
```

**1) 소켓 생성** (`CollectorRunner`)

```text
wss://fstream.binance.com/market/stream?streams=liteusdt@markPrice@1s/crdousdt@markPrice@1s/alabusdt@markPrice@1s
```

**2) 메시지 수신** (`BinanceStreamClient.onText` → `MarketDataIngestService`)

```json
{"stream": "liteusdt@markPrice@1s",
 "data": {"e": "markPriceUpdate", "E": 1729400000123, "s": "LITEUSDT", "p": "61.20", "r": "0.0001", ...}}
```

**3) 파싱** (`MarketDataIngestService`, `@Async` 스레드): `data`를 꺼내 `MarkPriceUpdate.fromJson(data)`

**4) envelope 생성과 발행** (`RawEventPublisher`)

```json
{
  "source": "binance",
  "product": "usdm",
  "asset_class": "stock",
  "sector": "optical",
  "stream": "markPrice",
  "symbol": "LITEUSDT",
  "event_time": 1729400000123,
  "receive_time": 1729400000130,
  "collector_id": "markprice-usdm-stock-optical-9e1f3a5b7-q1r2s",
  "payload": { "...": "data 원본 그대로" }
}
```

| envelope 필드 | 출처 |
| --- | --- |
| `product`, `asset_class`, `sector` | ConfigMap `ingestion.*` (②, ③) |
| `stream` | ConfigMap `streams[].stream` (⑤) |
| `symbol`, `event_time` | 메시지 `data.s`, `data.E` |
| `receive_time` | 수신 시각 (`System.currentTimeMillis()`) |
| `collector_id` | Deployment의 `COLLECTOR_ID` 환경 변수 (pod 이름) |

→ 토픽 `binance.raw.usdm.stock.markprice`, 키 `LITEUSDT`로 발행. fixed-rate라 dedup을 거치지 않는다.

### 비교: variable-rate `spot-btcusdt`

| 필드 | 값 | 결과 |
| --- | --- | --- |
| group | `variable-rate` | dedup 적용 |
| product | `spot` | `wss://stream.binance.com:9443`, Spot 파서 |
| sector | `null` | envelope `sector: null` |
| symbols | `[BTCUSDT]` (1개) | 단일 형식 `/ws/...` |
| streams | 3개 | 소켓 3개 |

```text
socket 1  wss://stream.binance.com:9443/ws/btcusdt@bookTicker   → binance.raw.spot.crypto.bookticker
socket 2  wss://stream.binance.com:9443/ws/btcusdt@trade        → binance.raw.spot.crypto.trade
socket 3  wss://stream.binance.com:9443/ws/btcusdt@depth@100ms  → binance.raw.spot.crypto.depth
```

---

## 4. 현재 코드와의 차이 (구현해야 할 것)

| # | 할 일 | 파일 |
| --- | --- | --- |
| 1 | `ingestion.*` 바인딩 record 추가 | `config/IngestionProperties.java` (신규) |
| 2 | 고정된 2개 소켓 대신 `streams`를 돌며 소켓 생성, URL 조립 (②④⑤) | `runner/CollectorRunner.java` |
| 3 | combined 메시지의 `data` 꺼내기, `stream`별 파서 선택 | `service/MarketDataIngestService.java` |
| 4 | trade, aggTrade, depth 모델 추가 | `model/` |
| 5 | envelope 생성, `spec.topic()`으로 발행, `group`에 따라 dedup | `kafka/RawEventPublisher.java` |
| 6 | 리팩터링이 끝나면 ConfigMap에서 `binance.setup.symbol` 제거 | `deploy/<group>/*.yaml` |

현재 실제 동작은 `binance.setup.symbol` → `BinanceProperties.Setup.symbol` → `CollectorRunner.run()`에서 Spot bookTicker와 USD-M markPrice 두 소켓을 고정으로 연다. 그래서 지금 배포하면 모든 pod가 이 두 스트림만 받는다.
