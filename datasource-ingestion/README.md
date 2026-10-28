# Datasource Ingestion — 폴더 구조

Binance WebSocket을 구독해 raw 이벤트를 Kafka로 발행하는 부분만 다룬다. 그룹 타입(변동성 기반 / 고정 rate)과
무엇을 구독할지(심볼 / 섹터)는 코드가 아니라 config로만 갈라진다 — 실행 코드는 [`collector/`](collector/) 하나뿐이고,
pod마다 ConfigMap으로 구독 설정만 다르게 넣는다.

이 앱은 **producer**다 — payload를 해석하지 않고 envelope로 감싸 옮기기만 한다. Active-Active 중복 제거와
basis 계산 같은 조인은 raw 토픽을 구독하는 별도 Flink 앱의 책임이다.

```
datasource-ingestion/
├── collector/                         # 수집 앱 (Spring Boot) — 지금 실제로 동작하는 코드
│   ├── config/ethusdt.yml             # 로컬 실행용 예시 설정
│   └── src/main/java/com/ingestion/
│       ├── IngestionCollectorApplication.java   # main() — SpringApplication 부트스트랩
│       ├── config/
│       │   ├── BinanceProperties.java           # binance.* — Endpoint(접속 도메인, 환경별) · Subscription(구독 설정, pod별)
│       │   ├── AsyncConfig.java                 # 메시지 처리용 스레드풀
│       │   └── KafkaConfig.java                 # Kafka 접속 주소(spring.kafka.bootstrap-servers) — 지금은 시작 로그만
│       ├── runner/
│       │   └── CollectorRunner.java             # streams를 돌며 URL 조립 → 소켓 연결
│       ├── collector/
│       │   └── BinanceStreamClient.java         # WebSocket 클라이언트 (연결·재연결·수신)
│       ├── service/
│       │   └── MarketDataIngestService.java     # combined 메시지 풀기 → envelope 생성 → 발행 위임
│       ├── model/
│       │   └── BinanceEventEnvelope.java               # 공통 envelope (payload는 원본 그대로)
│       └── kafka/
│           └── BinanceRawEventPublisher.java           # raw 이벤트 발행 지점 (지금은 로그만)
│
├── deploy/                            # 이미지 · K8s 매니페스트 (kustomize)
│   ├── Dockerfile                     # variable / fixed 공용 이미지 1개
│   ├── kustomization.yaml             # kubectl apply -k deploy
│   ├── common.yaml                    # namespace binance-ingestion + 공통 환경 변수
│   ├── mapping.md                     # ConfigMap 설정 → 코드 매핑
│   ├── variable-rate/                 # Active-Active (replicas 2) — ConfigMap + Deployment + PDB
│   │   ├── spot-sov.yaml
│   │   ├── usdm-sov.yaml
│   │   ├── usdm-optical.yaml
│   │   └── usdm-semi.yaml
│   └── fixed-rate/                    # Active-Standby (replicas 1, Recreate) — ConfigMap + Deployment
│       ├── markprice-usdm-sov.yaml
│       ├── markprice-usdm-optical.yaml
│       └── markprice-usdm-semi.yaml
│
└── core/  app/  config/               # 예전 목표 구조의 자리 (.gitkeep만 있음, 현재 미사용)
```

pod 1개 = deploy yaml 1개 = ConfigMap 1개다. ConfigMap의 `application.yaml`이 이미지 안의 기본 설정을 덮어써서
그 pod가 무엇을 구독할지 정한다.

| 설정 필드 | variable-rate 예시 (`usdm-semi`) | fixed-rate 예시 (`markprice-usdm-optical`) |
| --- | --- | --- |
| `product` | `usdm` → `wss://fstream.binance.com` | `usdm` → `wss://fstream.binance.com` |
| `sector` | `semi` | `optical` |
| `symbols` | `[NVDAUSDT, AVGOUSDT, MRVLUSDT]` → combined | `[LITEUSDT, CRDOUSDT, ALABUSDT]` → combined |
| `streams` | bookTicker · aggTrade · depth@100ms (소켓 3개) | markPrice@1s (소켓 1개) |
| 발행 토픽 | `binance.raw.usdm.stock.{bookticker,aggtrade,depth}` | `binance.raw.usdm.stock.markprice` |

필드가 코드 어디로 들어가는지는 [deploy/mapping.md](deploy/mapping.md)를 참고한다.

## 지금 바로 실행해보려면

```bash
cd collector
./gradlew bootRun                                                              # 기본값: spot-sov와 같은 구독
./gradlew bootRun --args='--spring.config.additional-location=config/ethusdt.yml'   # 다른 구독 설정으로 실행
```

자세한 준비물, 실행 결과 로그 읽는 법, 확인된 스트림 조합은 [`collector/README.md`](collector/README.md)를 참고한다.

## K8s(kind)에 배포하려면

```bash
docker build -f deploy/Dockerfile -t ingestion-collector:0.0.1 collector
kind load docker-image ingestion-collector:0.0.1
kubectl apply -k deploy
kubectl get pods -n binance-ingestion -L rate-group
```
