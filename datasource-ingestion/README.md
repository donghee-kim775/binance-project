# Datasource Ingestion — 폴더 구조

Binance WebSocket을 구독해 Kafka로 발행하는 부분만 다룬다. 그룹 타입(변동성 기반 / 고정 rate)과
무엇을 구독할지(심볼 / 섹터)는 코드가 아니라 config로만 갈라진다 — 실행 코드는 `app/` 하나뿐이다.

> **지금은 [`collector/`](collector/)에 Insight 1-1(Spot-Futures Basis) POC용 단일 프로세스
> Spring Boot 앱이 먼저 들어와 있다.** 아래 구조(core/app/config/deploy 분리, Pod화)는 이 POC가
> 검증된 다음 리팩터링할 목표 구조다. 자리만 미리 잡아뒀고(`.gitkeep`만 있음) 코드는 아직 없다.

```
datasource-ingestion/
├── core/                              # 공유 라이브러리 (목표 구조 — 자리만 있음, 코드 없음)
│   ├── ws/                            # 재연결·backoff·ping-pong, 24h 커넥션 전환
│   ├── parser/                        # product별 파서 (스키마가 달라 분리)
│   │   ├── spot/
│   │   ├── usdm/
│   │   ├── coinm/
│   │   └── options/
│   ├── dedup/                         # Redis SETNX 기반 dedup
│   ├── kafka/                         # producer + envelope 스키마
│   └── metrics/
│
├── app/
│   └── ingestion-runner/              # 단일 실행체. config로 동작이 결정됨 (목표 구조, 아직 없음)
│
├── config/                            # Pod 인스턴스별 구독 설정 (목표 구조, 아직 없음)
│   ├── variable-rate/                 # 심볼별 1 config = 1 Pod
│   │   ├── bookticker-spot-btcusdt.yaml
│   │   └── bookticker-spot-ethusdt.yaml
│   └── fixed-rate/                    # 섹터별 1 config = 1 Pod
│       ├── markprice-crypto-sector.yaml
│       └── markprice-tradfi-sector.yaml
│
├── deploy/                            # config 1개 = Deployment 1개, replicas=2 (목표 구조, 아직 없음)
│   ├── variable-rate/
│   └── fixed-rate/
│
└── collector/                         # ← 지금 실제로 동작하는 POC (위 구조로 쪼개지기 전 단일 프로세스)
    └── src/main/java/com/ingestion/
        ├── IngestionCollectorApplication.java   # main() — SpringApplication 부트스트랩
        ├── config/
        │   └── CollectorProperties.java         # app.symbol 바인딩 (@ConfigurationProperties)
        ├── runner/
        │   └── CollectorRunner.java          # URI 조립 → WebSocket 연결 → 메시지 콜백 등록
        ├── collector/
        │   └── BinanceStreamClient.java         # WebSocket 클라이언트 (연결·재연결·수신)
        ├── model/
        │   ├── SpotBookTicker.java              # spot bookTicker 페이로드 (record)
        │   └── MarkPriceUpdate.java             # usdm markPrice 페이로드 (record)
        ├── service/
        │   └── MarketDataIngestService.java     # 수신 로깅 + 발행 위임 (계산 없음)
        └── kafka/
            └── RawEventPublisher.java           # raw 이벤트 발행 지점 (지금은 로그만)
```

`collector/` 안은 `config`(설정) → `runner`(오케스트레이션) → `collector`(전송) → `model`(파싱)
→ `service`(로깅/위임) → `kafka`(발행) 순으로 데이터가 흐른다. **이 앱은 producer다** — spot과
futures 값을 조합하는 basis 계산은 여기 없고, 두 raw 토픽(`binance.raw.bookticker`,
`binance.raw.markprice`)을 구독하는 다운스트림 컨슈머(Kafka Streams `basis-join`)의 책임이다.

| config 필드 | variable-rate 예시 | fixed-rate 예시 |
| --- | --- | --- |
| `domain` | `stream.binance.com` | `fstream.binance.com` |
| `stream` | `btcusdt@bookTicker` | `!markPrice@arr` |
| `scope` | 심볼 1개 | 섹터(심볼 목록) |
| `filter` | 없음 | 섹터에 속한 심볼만 통과 |

## 지금 바로 실행해보려면

```bash
cd collector
./gradlew bootRun                                   # 기본값 BTCUSDT
./gradlew bootRun --args='--app.symbol=ethusdt'     # 심볼 바꿔서 실행
```

단, `app.symbol` 하나로 spot bookTicker와 usdm markPrice를 **둘 다** 결정한다 — variable-rate
(심볼별 Pod) 방향만 우연히 맞고, fixed-rate(markPrice류를 섹터로 묶는 것)는 아직 반영 안 됐다.
markPrice를 섹터 단위로 묶는 건 core/config 분리 리팩터링 때 반영할 부분이다. 자세한 준비물,
실행 결과 로그 읽는 법, 알려진 이슈는 [`collector/README.md`](collector/README.md)를 참고한다.
