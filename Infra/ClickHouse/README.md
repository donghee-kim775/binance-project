# ClickHouse

clean 토픽의 이벤트를 쌓아 두고 리포트(봉, 거래대금, 매수 · 매도 압력, 펀딩비 등)를 조회하는 저장소다. 최신값 실시간 표시는 Redis가 맡고([servingapp/plan.md](../../servingapp/plan.md)), ClickHouse는 기간 집계를 맡는다.

| 항목 | 값 |
| --- | --- |
| 이미지 | `clickhouse/clickhouse-server:25.8` (LTS) |
| namespace | `binance-storage` |
| 구성 | StatefulSet 1개, replica 1, PVC 10Gi |
| 클러스터 안 주소 | `clickhouse.binance-storage.svc` (HTTP 8123, native 9000) |
| DB / 사용자 | `binance` / `admin` (비밀번호는 Secret `clickhouse-auth`) |
| 매니페스트 | [clickhouse.yaml](clickhouse.yaml) |
| 테이블 | [database/clickhouse/table/](../../database/clickhouse/table/) |

## 설치

```bash
kubectl apply -f Infra/ClickHouse/clickhouse.yaml   # namespace가 먼저 생긴다. pod는 Secret이 없어 대기한다
kubectl create secret generic clickhouse-auth -n binance-storage --from-literal=password='<비밀번호>'

kubectl get pods -n binance-storage -w               # clickhouse-0 이 READY 1/1
```

## 테이블 만들기

```bash
kubectl exec -i -n binance-storage clickhouse-0 -- \
  clickhouse-client --user admin --password '<비밀번호>' --multiquery \
  < database/clickhouse/table/trades.sql
```

## 접속

```bash
# pod 안에서 바로
kubectl exec -it -n binance-storage clickhouse-0 -- clickhouse-client --user admin --password '<비밀번호>'

# PC에서 (DBeaver, 브라우저 http://localhost:8123/play)
kubectl port-forward -n binance-storage svc/clickhouse 8123:8123 9000:9000
```

들어오는지 확인:

```sql
SELECT product, symbol, count(), max(trade_time) FROM binance.trades GROUP BY product, symbol;
```

## 설정 이유

- **Operator(Altinity) 대신 StatefulSet:** 노드 하나라 샤드 · 레플리카 관리가 필요 없다. Operator를 쓰면 Operator pod 메모리와 CRD를 하나 더 배워야 해서, 단일 노드에서는 얻는 게 적다. 레플리카가 필요해지면 그때 Operator로 옮긴다.
- **비밀번호는 Secret으로 따로:** 매니페스트를 git에 올리므로 비밀번호를 파일에 두지 않는다.
- **메모리:** request 1Gi, limit 2Gi. `mark_cache_size`를 기본 5GB에서 256MB로 줄이고, 계속 쌓이기만 하는 시스템 로그 테이블(`trace_log`, `metric_log` 등)을 끈다 (ConfigMap `clickhouse-config`).
- **Kafka에서 직접 읽기:** 테이블마다 Kafka 엔진 큐 + Materialized View로 clean 토픽을 읽는다. 별도 적재 앱이 필요 없다. 컨슈머 그룹은 `clickhouse-<테이블>`이다.

## 지금 단순화한 것

- replica 1이다. pod가 내려가면 그동안 리포트 조회가 안 된다. Kafka 엔진이 offset을 커밋하므로, 다시 뜨면 멈춘 곳부터 이어서 읽는다 (clean 토픽 보존 기간 안이면 데이터가 빠지지 않는다).
- 백업이 없다. PVC가 지워지면 데이터도 사라진다.
- 사용자는 `admin` 하나다. 리포트 앱이 붙을 때 읽기 전용 사용자를 SQL로 만든다.
