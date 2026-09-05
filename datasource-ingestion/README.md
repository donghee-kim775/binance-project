# Datasource Ingestion — 폴더 구조

Binance WebSocket을 구독해 Kafka로 발행하는 부분만 다룬다. 그룹 타입(변동성 기반 / 고정 rate)과
무엇을 구독할지(심볼 / 섹터)는 코드가 아니라 config로만 갈라진다 — 실행 코드는 `app/` 하나뿐이다.

```
datasource-ingestion/
├── core/                              # 공유 라이브러리
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
│   └── ingestion-runner/              # 단일 실행체. config로 동작이 결정됨
│
├── config/                            # Pod 인스턴스별 구독 설정
│   ├── variable-rate/                 # 심볼별 1 config = 1 Pod
│   │   ├── bookticker-spot-btcusdt.yaml
│   │   └── bookticker-spot-ethusdt.yaml
│   └── fixed-rate/                    # 섹터별 1 config = 1 Pod
│       ├── markprice-crypto-sector.yaml
│       └── markprice-tradfi-sector.yaml
│
└── deploy/                            # config 1개 = Deployment 1개, replicas=2 (Active-Active)
    ├── variable-rate/
    └── fixed-rate/
```

| config 필드 | variable-rate 예시 | fixed-rate 예시 |
| --- | --- | --- |
| `domain` | `stream.binance.com` | `fstream.binance.com` |
| `stream` | `btcusdt@bookTicker` | `!markPrice@arr` |
| `scope` | 심볼 1개 | 섹터(심볼 목록) |
| `filter` | 없음 | 섹터에 속한 심볼만 통과 |
