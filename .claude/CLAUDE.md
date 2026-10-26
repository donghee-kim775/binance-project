# Binance Real-Time Market Insight Platform

Binance WebSocket 시장 데이터를 Kafka로 수집하고 Flink로 분석하는 개인 학습 프로젝트. 설계 결정과 그 근거를 남기는 것이 구현만큼 중요하다 (면접 대비 포함).

## 저장소 구조

- `datasource-ingestion/collector/`: 수집 앱 (Spring Boot 4, Java 17). **raw 이벤트를 Kafka로 보내기만 한다.** dedup, 조인, 분석은 하지 않는다.
- `datasource-ingestion/deploy/`: 수집 앱 Dockerfile과 K8s 매니페스트 (kustomize). 설정→코드 매핑은 `deploy/mapping.md`
- 분석(dedup, basis join 등)은 **별도 Flink 앱**에서 한다. 수집 앱에 넣지 않는다.
- `plan/`: 설계 결정 기록 (`ingestion/`, `topic/`, `consumer/`)
- `Infra/`: kind 클러스터, Strimzi Kafka, Flink Operator
- `docs/`: Binance API 조사 (`binance-api/`, `streams/`), 수집 심볼 표와 섹터 분류 (`symbols/0TickerInfoTable.md`, `symbols/cryptoSectorREADME.md`), 초기 메모 (`notes/`)
- `Insights/`: 인사이트 카탈로그와 인사이트별 계획
- `servingapp/`: 서빙 계층 설계

## 설계 규칙

- **토픽 이름:** `binance.<raw|clean|analytics>.<spot|usdm>.<crypto|stock>.<stream>` (소문자, 숫자, `.`만)
- **Kafka 키:** 심볼 (`BTCUSDT`). product와 자산 종류는 토픽 이름에 있으므로 키에 넣지 않는다.
- **payload:** Binance 원본을 그대로 envelope에 넣는다. 필드를 고치지 않는다.
- **USD-M 경로 접두어:** `/public` = bookTicker, depth / `/market` = aggTrade, markPrice. 틀리면 연결은 되지만 메시지가 오지 않는다.
- **Pod 그룹:**
  - variable-rate (bookTicker, trade/aggTrade, depth): Active-Active, replicas 2, RollingUpdate `maxUnavailable: 0`
  - fixed-rate (markPrice@1s): Active-Standby, replicas 1, `Recreate`
- **네임스페이스:** 수집 앱 `binance-ingestion`, Kafka `kafka`
- 수집 심볼이나 스트림이 바뀌면 `0TickerInfoTable.md`, `deploy/`, `plan/topic/plan.md`, README 수집 대상 표를 함께 맞춘다 (`/add-symbol`).

## 문서 작성 규칙

- 문서는 한국어로 쓴다.
- 설계 문서(`plan/`)는 **고려 사항 → 선택 옵션 → 선택 → 이유 → trade-off** 형식을 따른다 (`/decision`).
- 문서끼리 내용이 어긋나면 고치기 전에 사용자에게 어느 쪽이 맞는지 확인한다.

## 명령어

```bash
# 빌드
cd datasource-ingestion/collector && ./gradlew bootJar

# 이미지 (datasource-ingestion/ 에서)
docker build -f deploy/Dockerfile -t ingestion-collector:0.0.1 collector

# 매니페스트 렌더링 검사
kubectl kustomize datasource-ingestion/deploy

# kind 배포
kind load docker-image ingestion-collector:0.0.1
kubectl apply -k datasource-ingestion/deploy
kubectl get pods -n binance-ingestion -L rate-group
```

## 작업 규칙

- git commit, push는 사용자가 요청할 때만 한다. 브랜치 이름은 `feat/<주제>` 형식.
- PR 설명은 `.github/pull_request_template.md` 형식을 따른다 (`/pr-desc`).
