# binance-processing-jobs

`Binance / 처리 job 비교 (단계 · job 필터)` 대시보드. 처리 job을 **단계**와 **job** 필터로 골라 같은 패널에 나란히 본다. 파이프라인 전체를 한 화면에 펼치지 않고, 보고 싶은 job만 비교한다.

## 필터

| 필터 | 값 | 동작 |
| --- | --- | --- |
| **단계** | `dedup (raw → clean)` / `basis (clean → analytics)` | 이 단계의 job만 아래 `job` 목록에 나온다 |
| **job** | 고른 단계의 job (여러 개 · All) | 모든 패널이 고른 job만 그린다 |

| 단계 | job | 엔진 | 입력 → 출력 |
| --- | --- | --- | --- |
| dedup | `flink-dedup` | Flink | raw spot → `binance.clean.spot.crypto.bookticker` |
| dedup | `kstreams-dedup` | Kafka Streams | raw spot → `…bookticker.kstreams` |
| basis | `flink-basis` | Flink | clean(Flink dedup) + mark → `binance.analytics.crypto.basis` (F→F) |
| basis | `kstreams-basis` | Kafka Streams | clean(Flink dedup) + mark → `…basis.kstreams` (F→K) |
| basis | `flink-basis-kdedup` | Flink | clean.kstreams + mark → `…basis.kdedup.flink` (K→F) |
| basis | `kstreams-basis-kdedup` | Kafka Streams | clean.kstreams + mark → `…basis.kdedup.kstreams` (K→K) |

- `job` 목록은 Kafka Exporter의 consumer group에서 읽는다. 떠 있지 않은 job도 offset이 남아 있으면 목록에 보이고, 패널은 비어 있다(`-`).
- 색은 job마다 고정이다: Flink 계열 주황 · 노랑, Kafka Streams 계열 파랑 · 청록.

## job 이름 규칙에 기댄다

**job 이름 = consumer group = Deployment · FlinkDeployment 이름**이어야 패널이 맞게 나온다.

- **lag · 처리 속도:** `consumergroup=~"(job들)"`
- **CPU · 메모리 · 네트워크:** pod 이름 `<job>-<hash>-<id>`(Deployment, Flink JobManager) 또는 `<job>-taskmanager-N-N`(Flink TaskManager)을 `label_replace`로 job 이름으로 되돌려 합친다. 그래서 `flink-basis`만 골라도 `flink-basis-kdedup`의 pod는 섞이지 않는다.
- **출력 속도:** job → 출력 토픽 대응표가 대시보드 쿼리 안에 있다 (생성 스크립트의 `OUT_TOPIC`). **job을 새로 추가하면 이 표에도 넣는다.**

## 패널

| 행 | 패널 | 계산 |
| --- | --- | --- |
| 지금 | 입력 처리 · 출력 · lag · CPU · 메모리 · 네트워크 · Ready pod | job별 마지막 값 |
| 처리량 | 입력 토픽 유입 | 고른 job들이 읽는 토픽의 끝 offset 증가율 |
| | 입력 처리 속도 | `rate(kafka_consumergroup_current_offset{consumergroup=~…}[1m])`. 입력 유입과 같은 높이면 따라가는 중 |
| | 출력 속도 | job 출력 토픽의 끝 offset 증가율 |
| lag | job별 · job × 토픽별 | basis job은 Spot과 mark 중 어느 쪽이 밀리는지 |
| 자원 | CPU · 메모리 · 네트워크 (job별), CPU · 메모리 (pod별) | cAdvisor. pod별 그래프로 빈 파티션을 맡아 노는 pod를 찾는다 |

## 띄우기 (저장소 루트에서)

```bash
# 1. 수집 (Spot bookTicker + USD-M markPrice). 심볼 수는 두 매니페스트의 symbols로 정한다
kubectl apply -f datasource-ingestion/deploy/common.yaml \
              -f datasource-ingestion/deploy/variable-rate/spot-sov.yaml \
              -f datasource-ingestion/deploy/fixed-rate/markprice-usdm-sov.yaml

# 2. dedup 두 개. kstreams-dedup은 예전 offset이 남아 있으면 지금 위치로 옮긴 뒤 띄운다 (group이 꺼져 있어야 한다)
kubectl patch flinkdeployment flink-dedup -n binance-processing --type merge -p '{"spec":{"job":{"state":"running"}}}'
kubectl exec -n binancekafka binance-cluster-binance-cluster-pool-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group kstreams-dedup --topic binance.raw.spot.crypto.bookticker --reset-offsets --to-latest --execute
kubectl apply -k consumer/raw_clean/kstreams-dedup/compare

# 3. basis 네 개 (거의 동시에. 커밋된 offset이 없으면 모두 latest부터 읽는다)
kubectl apply -f Infra/Kafka/Kafka-Topic/binance/analytics/crypto
kubectl apply -k consumer/clean_analytics/flink-join/deploy
kubectl apply -k consumer/clean_analytics/kstreams-join/compare
kubectl apply -k consumer/clean_analytics/flink-join/compare-kdedup
kubectl apply -k consumer/clean_analytics/kstreams-join/compare-kdedup
```

끄기: Kafka Streams는 `kubectl scale deploy <이름> -n binance-processing --replicas=0`, Flink는 `job.state`를 `suspended`로 patch, 수집은 `spot-sov.yaml` · `markprice-usdm-sov.yaml`만 `kubectl delete -f` (common.yaml을 지우면 Namespace까지 지워진다).

## 주의

- **offset 증가율 ≠ 메시지 수 (Kafka Streams가 쓰는 토픽):** exactly-once 트랜잭션은 커밋마다 마커가 offset을 하나 차지한다. basis처럼 초당 몇 건인 토픽은 크게 부풀고(6/s → 약 8/s), clean처럼 초당 수백 건이면 차이가 작다. Kafka Streams job이 **입력**으로 `.kstreams` 토픽을 읽을 때의 처리 속도도 같은 이유로 조금 크게 보인다. 메시지 수 비교는 [binance-basis-join README](../binance-basis-join/README.md)의 측정 명령으로 한다 (토픽 이름만 바꿔서).
- **파티션 쏠림:** 심볼이 적으면 해시가 일부 파티션에 몰린다 (2026-11-26, 6심볼에서 raw spot P1이 비었다). 빈 파티션을 맡은 Kafka Streams task · Flink subtask는 논다.
- **처리량 한계는 이 화면만으로 알 수 없다:** 지금 부하(raw 약 640/s)는 모든 job이 여유 있게 따라간다. 한계를 재려면 group offset을 뒤로 돌려 밀린 데이터를 따라잡는 속도를 비교한다.
