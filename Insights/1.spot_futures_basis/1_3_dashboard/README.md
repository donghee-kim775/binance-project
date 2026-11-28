# Insight 1-3. basis 대시보드 애플리케이션

[basis_dashboard_mock.png](../basis_dashboard_mock.png)를 실제 데이터로 띄운 웹 앱이다. 브라우저 주소: **http://203.250.35.217:30808** (dclabnode1의 외부 IP. `192.168.12.x`는 클러스터 내부망이라 PC에서 닿지 않는다. NodePort라 노드 4대 어디로 들어가도 같다)

```text
kstreams-basis ──> binance.analytics.crypto.basis ──┬─> flink-redis (Flink) ──> Redis ──┬─ latest:… (hash) ──┐
                                                    │                                   └─ pubsub:… (채널) ──┤
                                                    └─> ClickHouse Kafka 엔진 ──> binance.basis ──────────────┤
                                                                                                             ▼
                                                                         basis-dashboard (FastAPI) ──WebSocket──> 브라우저
```

| 화면 | 데이터 | 엔드포인트 |
| --- | --- | --- |
| KPI: 감시 심볼 · 과열 · 주의 · 최대 \|Z\| · 평균 basis | Redis 최신값 (브라우저가 계산) | `/ws` (접속 직후 snapshot → 1초마다 update) |
| KPI: 지연 p50 | ClickHouse `quantile(0.5)(latency_ms)`, BTCUSDT 최근 5분 | `/api/latency` (10초마다) |
| 지금 상태 표 (\|Z\| 큰 순) | Redis 최신값 | `/ws` |
| 심볼별 30분 그래프 · ±2σ 밴드 · 5분 평균 · 과열 점 | ClickHouse `binance.basis FINAL` 최근 30분 + 이후 `/ws` 값을 이어 붙임 | `/api/history` (처음 한 번) |

## 구조

```text
1_3_dashboard/
├── Dockerfile              # python:3.12-slim + uvicorn
├── requirements.txt        # fastapi · uvicorn · redis · httpx
├── deploy/dashboard.yaml   # namespace binance-serving, Deployment + NodePort 30808
└── app/
    ├── main.py             # Gateway: Redis snapshot · Pub/Sub → WebSocket, ClickHouse 조회
    └── static/
        ├── index.html      # 목업과 같은 배치 (KPI 6 · 상태 표 · 읽는 법 · 그래프 6)
        ├── style.css
        └── app.js          # 데이터 받기 · 1초에 한 번 다시 그리기 (Chart.js 4.4.1, cdnjs)
```

## 설계 ([servingapp/plan.md](../../../servingapp/plan.md)를 그대로 따른다)

| 고려 사항 | 구현 |
| --- | --- |
| Polling vs Push | WebSocket push. 서버가 Redis 채널을 **프로세스당 하나만** 구독하고 연결된 브라우저 모두에 보낸다 → 접속자가 늘어도 Redis · Kafka 부하는 그대로 |
| 접속 직후 초기값 | WebSocket을 열자마자 Redis 최신값(`snapshot`)을 한 번 보낸다. 그래프는 `/api/history`로 30분을 먼저 채운다 |
| Gateway 다중 인스턴스 | Kafka를 직접 읽지 않는다. Flink sink가 Redis에 PUBLISH하고, pod마다 Redis 채널을 구독한다 (plan 옵션 B). 상태가 없어 replica를 늘리기만 하면 된다 |
| 업데이트 빈도 제어 | 받은 값은 `pending[심볼]`에 덮어쓰기만 하고, 1초마다 모아서 한 번 보낸다 → 심볼당 최대 1/s (`PUSH_INTERVAL_SEC`) |

## 화면 읽는 법 (목업과 다른 점)

- **상태가 다섯 가지다.** 목업의 정상 · 주의 · 과열에 더해, `◌ 준비 중`(warmup, 표본 60건 = 약 1분 미만)과 `? 데이터 확인`(check_data, 값이 전부 같아 표준편차 0)이 있다. 둘 다 Z를 0으로 두고 판정하지 않는다.
- **최대 \|Z\|는 판정 중인 심볼만** 본다. 모두 warmup이면 "판정 전"으로 보인다.
- **±2σ 밴드는 warmup 동안 그리지 않는다.** 표준편차가 0에서 시작해 불안정하다.
- **그래프 선이 끊긴 구간**은 5초 넘게 basis가 없던 구간이다(파이프라인 정지, 신선도 필터로 모두 버려짐 등). 이어 그리면 없는 값을 있는 것처럼 보이게 된다.
- **지연 p50이 0 근처이거나 음수일 수 있다.** `basis_time`(kstreams-basis pod 시계) − `receive_time`(수집 pod 시계)이라 노드 시계 차이가 그대로 들어간다. 2026-11-28 실측은 −4 ~ 0 ms였다. 정확히 재려면 노드 NTP 오차가 지연보다 작아야 한다.
- **과열 점이 자주 보인다.** 지금 BTC 같은 주요 심볼은 표준편차가 0.5 bps 안팎이라, 1 bps만 움직여도 Z가 2를 넘는다. 경계 근처에서 오르내리는 문제와 함께 판정 규칙의 보완점이다 (히스테리시스 · 절대 폭 조건).

## 설치

```bash
# 0. 앞 단계: Redis · flink-redis · ClickHouse basis 테이블
kubectl apply -f Infra/Redis/redis.yaml
kubectl apply -f consumer/sink/insight_1/flink-redis/deploy/redis-sink.yaml
kubectl exec -i -n binance-storage clickhouse-0 -- clickhouse-client --user admin --password '<비밀번호>' --multiquery \
  < database/clickhouse/table/basis.sql

# 1. ClickHouse 읽기 전용 사용자 + 같은 비밀번호의 Secret (비밀번호는 파일에 남기지 않는다)
PW=$(openssl rand -hex 24)
printf "CREATE USER OR REPLACE dashboard IDENTIFIED WITH sha256_password BY '%s' SETTINGS readonly = 2;\nGRANT SELECT ON binance.basis TO dashboard;\n" "$PW" \
  | kubectl exec -i -n binance-storage clickhouse-0 -- bash -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery'
kubectl create namespace binance-serving
kubectl create secret generic clickhouse-dashboard -n binance-serving --from-literal=password="$PW"
unset PW

# 2. 앱
cd Insights/1.spot_futures_basis/1_3_dashboard
docker build -t kimdonghee125/basis-dashboard:0.0.2 .
docker push kimdonghee125/basis-dashboard:0.0.2
kubectl apply -f deploy/dashboard.yaml
```

- **`readonly = 2`인 이유:** `readonly = 1`은 설정 변경까지 막아서, HTTP 요청의 `default_format=JSONCompact`도 거절된다. `2`는 데이터 변경만 막고 쿼리별 설정은 허용한다.
- **권한은 `binance.basis` SELECT 하나**다. 대시보드가 다른 테이블을 볼 일이 없다.
- 값은 `param_<이름>`으로 넘긴다 (`{minutes:UInt32}`, `{symbol:String}`). SQL 문자열에 직접 붙이지 않는다.

## 로컬에서 돌려 보기

```bash
kubectl port-forward -n binance-storage svc/redis 6379 &
kubectl port-forward -n binance-storage svc/clickhouse 8123 &
pip install -r requirements.txt
CLICKHOUSE_PASSWORD='<dashboard 비밀번호>' uvicorn app.main:app --reload --port 8080   # http://localhost:8080
```

## 지금 단순화한 것

- 인증이 없다. NodePort라 노드에 닿는 사람은 누구나 볼 수 있다 (읽기 전용 화면이라 학습용으로 둔다).
- 과열 알림을 Slack 등으로 보내지 않는다. 화면의 KPI · 빨간 점으로만 본다.
- 테스트 코드가 없다. 2026-11-28에 클러스터 안에서 `/api/snapshot` · `/api/history` · `/api/latency` · `/ws`를 호출하고, 헤드리스 Chromium 스크린샷으로 화면을 확인했다.
