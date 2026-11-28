# binance-processing-jobs

`Binance / 처리 파이프라인 비교 (dedup → basis)` 대시보드. 같은 raw를 읽는 **세 파이프라인**을 나란히 본다. 파이프라인 하나 = dedup job + basis job이고, 엔진은 처음부터 끝까지 같은 것을 쓴다.

| 파이프라인 | dedup | basis | 출력 (clean → basis) | 색 |
| --- | --- | --- | --- | --- |
| **Kafka Streams** | `kstreams-dedup` | `kstreams-basis-kdedup` | `…bookticker.kstreams` → `…basis.kdedup.kstreams` | 파랑 |
| **Flink 셔플** | `flink-dedup` | `flink-basis` (keyBy) | `…bookticker` → `…basis` | 주황 |
| **Flink co-partition** | `flink-dedup-copart` (Flink key group 규칙으로 파티셔닝) | `flink-basis-copart` (keyBy 없이 `reinterpretAsKeyedStream`) | `…bookticker.copart` → `…basis.copart` | 보라 |

- 세 파이프라인 모두 같은 raw spot bookTicker를 읽는다. mark는 Kafka Streams · Flink 셔플이 `binance.raw.usdm.crypto.markprice`, co-partition이 `…markprice.copart`(같은 구독을 Flink 규칙으로 파티셔닝하는 수집기 `markprice-usdm-sov-copart`)를 읽는다.
- 떠 있지 않은 파이프라인은 `-`로 나온다. 꺼져 있어도 consumer group offset이 남아 있으면 lag에는 값이 보인다.

## 패널

위에서 아래로 **합계 → dedup 단계 → basis 단계** 순서다. 모든 숫자가 같은 시각의 값이라 한 번에 캡처하면 단계별 숫자와 합계가 맞는다 (따로 캡처해 합계에서 빼면 시점이 달라 안 맞는다).

| 구역 | 숫자 | 무엇을 보나 |
| --- | --- | --- |
| ① 파이프라인 합계 | CPU · 메모리 · 네트워크 송신 · lag(2분 평균) · Ready pod | 엔진 선택의 결정 근거 (파이프라인 전체 비용) |
| ② dedup 단계 (raw → clean) | raw 입력 처리 · clean 출력 · lag · CPU · 메모리 · 네트워크 | 입력 스트림이 하나라 co-partition 판도 dedup은 셔플 판과 같은 일을 한다 (`keyBy(symbol\|eventId)` 그대로). Kafka Streams의 exactly-once 커밋 비용(CPU)도 여기서 본다 |
| ③ basis 단계 (clean + mark → analytics) | 입력 처리 · basis 출력 · lag · CPU · 메모리 · 네트워크 | 두 스트림 join이라 **co-partition 효과를 보는 곳**. Flink 셔플 ↔ co-partition의 네트워크 차이가 keyBy 셔플 비용이고, 남는 값은 sink · 체크포인트 · heartbeat 같은 고정 비용 |
| 그래프 | dedup · basis 단계별 입력 처리 · CPU · 네트워크 · lag, 단계별 메모리 | 시간에 따른 변화. lag 그래프는 순간값(톱니)이다 |

pod는 job 이름으로 끝까지 맞춘 정규식으로 고른다. `flink-dedup`에 `flink-dedup-copart`의 pod가 섞이지 않는다. 파이프라인 구성은 생성 스크립트의 `PIPELINES`에 있다.

basis 지연(p50 · p99)은 메시지 안의 값이라 이 화면에 없다. 출력 토픽을 직접 읽어 `basis_time − receive_time`을 계산한다.

## 띄우기

전체 순서는 [flink-join README](../../../../consumer/clean_analytics/flink-join/README.md)의 "co-partitioning 실험"을 따른다. 요약 (저장소 루트에서):

```bash
# 수집: spot-sov, markprice-usdm-sov (+ co-partition용 markprice-usdm-sov-copart)
kubectl apply -f datasource-ingestion/deploy/common.yaml -f datasource-ingestion/deploy/variable-rate/spot-sov.yaml -f datasource-ingestion/deploy/fixed-rate/markprice-usdm-sov.yaml
kubectl apply -k datasource-ingestion/deploy/compare-copart

# dedup 세 개 (새 group · 예전 offset이 남은 group은 먼저 --reset-offsets --to-latest)
kubectl patch flinkdeployment flink-dedup -n binance-processing --type merge -p '{"spec":{"job":{"state":"running"}}}'
kubectl apply -k consumer/raw_clean/flink-dedup/compare-copart
kubectl apply -k consumer/raw_clean/kstreams-dedup/compare

# basis 세 개
kubectl patch flinkdeployment flink-basis -n binance-processing --type merge -p '{"spec":{"job":{"state":"running"}}}'
kubectl apply -k consumer/clean_analytics/flink-join/compare-copart
kubectl apply -k consumer/clean_analytics/kstreams-join/compare-kdedup
```

끄기: Kafka Streams는 `kubectl scale deploy <이름> -n binance-processing --replicas=0`, Flink는 `job.state`를 `suspended`로 patch하거나 copart overlay를 `delete -k`. 수집은 매니페스트 파일만 `kubectl delete -f` (common.yaml을 지우면 Namespace까지 지워진다).

## 주의

- **basis 출력은 처리 능력이 아니다:** mark가 올 때만 1건을 내므로 6심볼이면 약 6/s로 고정된다. 처리 능력은 밀린 데이터를 따라잡는 속도로 잰다.
- **lag 크기를 엔진끼리 비교하지 않는다:** Flink는 체크포인트 때만 커밋한다(dedup 1s, basis 10s). Kafka Streams는 100ms(EOS). 그래서 Flink lag이 크게 보인다. 계속 오르는지(추세)만 본다.
- **offset 증가율 ≠ 메시지 수 (Kafka Streams 출력):** exactly-once 트랜잭션은 커밋마다 마커가 offset을 하나 차지한다. basis처럼 초당 몇 건이면 크게 부풀고(6/s → 약 8/s), clean처럼 초당 수백 건이면 차이가 작다.
- **지연은 이 화면에 없다:** `basis_time − receive_time`은 메시지 안의 값이라 출력 토픽을 직접 읽어 계산한다.
