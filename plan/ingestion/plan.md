- **WebSocket Ingestion Framework 고려 사항**
- **Ingestion Application 구조 고려 사항**
    
    💡
    
    ### **Stream Ingestion Application 측면 고려사항**
    
    ---
    
## [고려 사항 1. 구조 설계]

> Ingestion Application 을 어떻게 구성할 것인가?
    

### 궁금했던 사항.

Stream 별로 application 을 만들어서 1:1 로 관리해야하나? `topic <-> Pod`**

 
- **선택 옵션**
  
    a. **`[Topic] <-> Pod`** 1:1 ⇒ 장애 격리 최대 / 운영부담 증가
    
    b. **`[product + symbol 별] <-> Pod`** ⇒ 한 프로세스가 여러 토픽을 다루므로 장애 시 그 커넥션에 물린 토픽 전부 같이 끊김.
    
    c. `모놀리식` ⇒ 수평 스케일링 불가

- 선택 : b.
        
    특정 심볼의 특정 wss 트래픽이 늘어난다면?
    같은 심볼의 다른 wss도 선형적으로 늘어날 것. ⇒ 수평 스케일링할 때, 괜찮을거 같음.
        
    예외 사항) 단, 특정 `markprice@1s` 와 같이 traffic이 일정한 것은 섹터별로 분리
        
    ex) 코인 섹터 / 주식 섹터
        
    - 고정 rate 그룹
    - 변동성 기반 그룹
    
## [고려 사항 2. Fail-Over 및 HA부분]
    
토픽을 어떤 구조로 받을거냐? (`Active-Active` or `Active-Standby`)

- **선택 옵션**

    1. **`Active-Standby`** (replicas=1, 재시작 기반) ⇒ 가장 단순, 중복 원천 차단 / 재시작 동안 다운타임 발생

    2. **`Active-Standby`** (핫스탠바이 + leader election, K8s Lease·Postgres advisory lock) 
   
        ⇒ 다운타임 최소화, 중복 없음 / 구현 복잡도 추가
        
        ⇒ 하지만 어느 정도의 공백을 허용한다면? 지장없을듯 함.

    3. **`Active-Active`** ⇒ 페일오버 공백 거의 0 / **중복 발생이 설계상 필연** → 3번 문제를 반드시 풀어야 함, 인프라 비용도 배

- **선택 :**
  
    3. 학습 목적으로 `Active-Active` 채택. 파드 수(2개) / 심볼 수(1~2개)를 최소로 유지해 리소스 부담 제거

- ps. **중복 제거는 어떻게 할 것인가..?**

    - 중복 판별 키 : trade_id

    - 어디서 제거할 것인가?

        a. kafka 앞에 => redis 고려

        b. kafka 뒤에 => flink 에서 중복 제거

- 선택 : a.

    - 왜 Kafka 앞인가
        - 중복을 발생 지점에서 차단 → 하류 소비자 전체가 중복을 신경 쓰지 않아도 됨
        - 
    - 왜 Redis인가 (vs DB unique constraint)
        - 인메모리: 모든 메시지가 거치는 경로라 sub-ms 지연 필요
        - SET NX EX: 확인+기록을 원자적으로 처리 → 파드 간 경합 안전
        - TTL: dedup 키는 수십 초만 유효하면 됨 → 자동 만료, 메모리 일정 (~12MB 수준)
          TTL은 두 파드 간 동일 이벤트 도착 시차보다 넉넉하게 (30~60s)

    - redis 부하 측정..?

      - 

- b.가 아닌 이유

    ```
    단일 소비자라면 Flink 잡 안에서 끝낼 수 있지만, 소비자 확장성을 고려하면 dedup 결과를 Kafka 토픽으로 다시 공유해야 해서 Kafka를 한 번 더 거치고 그만큼 지연이 늘어납니다. 수집단에서 Redis로 막으면 raw 토픽 자체가 이미 중복 없는 상태라서 이 추가 단계가 필요 없습니다."
    ```

- 이후..? 펜싱 구조 고려


## [고려 사항 3. HA 부분]

`Active-Active` 라면 중복 토픽 메시징은 어떻게 중복 처리할거냐..?
    
`Acitve-Active` 구조로 페일오버 공백 처리는 해결됨.

하지만, 모든 파드가 동일한 외부 WebSocket 스트림을 동시에 수신하여 카프카로 전송하기에, 동일한 데이터가 카프카 토픽에 종복 적재되는 문제

- **선택 옵션**
    
    1. Consumer 단 Redis 기반 Deduplication (In Memory 단 필터링)
    
    2. Stream Processing Engine (Apache Flink / Kafka Streams)
    
    3. DB
        - DB Unique Constraint
    
    4. ClickHouse
        - ClickHouse ReplacingMergeTree 엔진 활용
        - ClickHouse 네이티브 Kafka Engine + `Replicated_deduplication_window`
    
- **선택 :**
