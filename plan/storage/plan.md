# MinIO 경로 설계

Flink job이 MinIO(`binance` 버킷)에 남기는 체크포인트 · savepoint · HA 기록의 경로 규칙을 정한다. MinIO 설치와 구성은 [Infra/MinIO/README.md](../../Infra/MinIO/README.md), 토픽 이름 규칙은 [topic/plan.md](../topic/plan.md)를 기준으로 한다.

```text
s3://binance/flink/<계층>/<FlinkDeployment 이름>/<checkpoints|savepoints|ha>

계층 : job이 쓰는(출력) 토픽의 단계 — raw / clean / analytics
예)    s3://binance/flink/clean/flink-dedup/checkpoints
```

## [고려 사항 1. 버킷 안에서 쓰는 주체를 나눌 것인가]

> `binance` 버킷을 Flink만 쓴다고 보고 경로를 정할 것인가, 다른 엔진이 들어올 자리를 남길 것인가?

지금은 Flink만 쓰지만 클러스터에 spark-operator가 있고, ClickHouse 백업도 S3 호환 저장소에 둘 수 있다.

- **선택 옵션**

    a. **구분하지 않음** (`binance/clean/flink-dedup/...`) ⇒ 경로가 짧음 / 다른 엔진이 들어오면 Flink job 경로와 섞이거나 그때 전체 경로를 옮겨야 함

    b. **엔진 이름을 맨 앞에** (`binance/flink/clean/flink-dedup/...`) ⇒ 경로만 보고 누가 쓴 데이터인지 앎 / 경로가 한 단계 길어짐

    c. **엔진마다 버킷** (`flink` 버킷, `spark` 버킷) ⇒ 버킷 단위로 권한 · 보관 정책을 나눔 / 버킷 Job과 Flink 설정을 엔진마다 늘려야 함

- **선택 : b.**

    - 엔진이 늘어도 기존 Flink 경로를 옮기지 않는다. 경로를 옮기면 savepoint를 새 경로로 넘기는 재배포가 필요하다.
    - 버킷은 하나(`binance`)로 두어 버킷을 만드는 Job과 `s3.endpoint` 설정은 그대로다.
    - 엔진 단위 권한도 prefix 하나(`binance/flink/*`)로 줄 수 있다.

- **감수하는 trade-off**
    - `flink/` 아래의 job 이름도 `flink-`로 시작해 `flink`가 두 번 나온다 (`flink/clean/flink-dedup`). 경로를 FlinkDeployment 이름과 똑같이 두기 위해서다 (고려 사항 3).
    - `flink/` 아래에 `jobs/` 같은 단계는 두지 않는다. 지금은 job 말고 Flink가 쓰는 공용 경로가 없다. history server 기록처럼 job이 아닌 것이 생기면 그때 `flink/<용도>/`로 옆에 둔다.

## [고려 사항 2. 경로를 나누는 순서]

> 계층(raw/clean/analytics) · 종류(checkpoints/savepoints/ha) · job을 어떤 순서로 둘 것인가?

처음에는 `s3://binance/<종류>/<job>`(`checkpoints/flink-dedup`)로 두었다. 경로만 보고는 job이 어느 계층의 데이터를 만드는지 알 수 없었다.

- **선택 옵션**

    a. **종류 → job** (`flink/checkpoints/flink-dedup`) ⇒ 단순함 / 계층을 알 수 없음, job 하나의 기록이 세 곳에 흩어짐

    b. **계층 → 종류 → job** (`flink/clean/checkpoints/flink-dedup`) ⇒ 종류별 보관 규칙(lifecycle)을 prefix 하나에 걸 수 있음 / job 하나의 기록이 세 곳에 흩어짐

    c. **계층 → job → 종류** (`flink/clean/flink-dedup/checkpoints`) ⇒ job 하나의 기록이 prefix 하나에 모임 / 종류별 보관 규칙은 job마다 걸어야 함

- **선택 : c.**

    | job | 읽는 토픽 → 쓰는 토픽 | 경로 |
    | --- | --- | --- |
    | `flink-dedup` | `binance.raw.*` → `binance.clean.*` | `s3://binance/flink/clean/flink-dedup/` |

    - 계층이 토픽 이름(`binance.<단계>....`)과 같은 단어라, 경로만 보고 어느 단계의 데이터를 만드는 job인지 안다.
    - job 하나의 기록(체크포인트, savepoint, HA)이 `binance/flink/<계층>/<job>/` 아래에 모인다. job을 없앨 때 prefix 하나만 지우면 된다 (`mc rm --recursive --force local/binance/flink/clean/flink-dedup/`).
    - 나중에 root 대신 job별 MinIO 사용자를 만들 때 정책에 prefix 하나(`binance/flink/clean/flink-dedup/*`)만 넣으면 된다.
    - 계층은 **출력** 기준으로 정한다. dedup은 raw를 읽지만 job의 state(본 키 목록)는 clean을 만들기 위한 것이다. 입력 계층은 토픽 하나에 소비자가 여럿일 수 있어 job 하나를 정하지 못한다.

- **감수하는 trade-off**
    - 종류별 보관 규칙을 prefix 하나로 걸 수 없다. 체크포인트 개수는 Flink가 직접 관리하고(기본 최근 1개), savepoint는 직접 지우므로 lifecycle 규칙이 지금은 필요 없다. 필요해지면 job마다 규칙을 건다.
    - 출력 계층이 둘 이상인 job(예: clean과 analytics를 함께 쓰는 job)은 규칙이 바로 정해지지 않는다. 그때는 더 뒤 단계(analytics)를 쓴다.

## [고려 사항 3. job을 나누면 경로는 어떻게 되는가]

> dedup을 crypto와 stock(또는 product별)으로 나눠 FlinkDeployment를 여러 개 띄우면 경로를 어떻게 나눌 것인가?

- **선택 옵션**

    a. **job 아래에 하위 경로** (`flink/clean/dedup/crypto/checkpoints`) ⇒ dedup 계열이 한 폴더에 모임 / 경로가 FlinkDeployment 이름(`flink-dedup-crypto`)과 달라 "이름의 어디까지가 job이고 어디부터가 분할 단위인지" 규칙이 하나 더 필요함. `basis-join`처럼 job 이름에 `-`가 있으면 기계적으로 나눌 수 없음

    b. **FlinkDeployment 이름을 그대로 경로에** (`flink/clean/flink-dedup-crypto/checkpoints`) ⇒ 경로 = 리소스 이름 / dedup 계열이 형제 폴더로 놓임

- **선택 : b.**

    | FlinkDeployment | 경로 |
    | --- | --- |
    | `flink-dedup-crypto` | `s3://binance/flink/clean/flink-dedup-crypto/` |
    | `flink-dedup-stock` | `s3://binance/flink/clean/flink-dedup-stock/` |
    | `flink-dedup-usdm-stock` (더 잘게 나눌 때) | `s3://binance/flink/clean/flink-dedup-usdm-stock/` |

    - 경로의 job 자리가 언제나 `metadata.name` 하나라, 나눌 때 정할 것은 FlinkDeployment 이름뿐이다.
    - K8s가 namespace 안에서 이름 중복을 막으므로 경로도 겹치지 않는다.
    - Flink가 HA 경로 뒤에 붙이는 cluster-id도 `metadata.name`이라(고려 사항 4), 경로 · Pod 이름 · HA ConfigMap 이름이 모두 같은 이름으로 찾아진다.
    - 이름 안의 순서는 토픽 이름과 같게 `<product>-<자산종류>`로 쓴다. 토픽은 `.`을 쓰지만 K8s 이름은 `.`을 쓸 수 없어 `-`로 바꾼다.

- **감수하는 trade-off**
    - dedup 계열이 한 폴더로 묶이지 않는다. 같은 `flink/clean/` 아래에 `flink-dedup-`로 시작하는 형제 폴더로 놓이므로 `mc ls local/binance/flink/clean/ | grep flink-dedup`로 함께 볼 수 있다.
    - 나누기 전 job(`flink-dedup`)의 savepoint는 새 job 이름의 경로로 자동으로 넘어가지 않는다. 나눌 때 state를 이어받으려면 `initialSavepointPath`에 옛 savepoint 경로를 직접 적는다.

## [고려 사항 4. HA 경로에 job 이름을 넣을 것인가]

> `high-availability.storageDir`에 job 이름까지 적어야 하는가?

배포해 보니 `storageDir: s3://binance/ha/flink-dedup`일 때 실제 기록은 `ha/flink-dedup/flink-dedup/`에 쌓였다. Flink가 storageDir 뒤에 `kubernetes.cluster-id`(Operator가 FlinkDeployment 이름으로 채운다)를 붙이기 때문이다.

- **선택 옵션**

    a. **job 경로 아래 `ha`까지만 적음** (`s3://binance/flink/clean/flink-dedup/ha`) ⇒ 고려 사항 2 규칙을 그대로 따름 / 실제 경로에 job 이름이 한 번 더 붙음 (`.../ha/flink-dedup/`)

    b. **계층 아래 공용 `ha`** (`s3://binance/flink/clean/ha`) ⇒ Flink가 붙이는 이름으로 job이 나뉘어 중복이 없음 / job 하나의 기록이 한 prefix에 모이지 않음

- **선택 : a.**

    - checkpoints · savepoints · ha 세 경로가 모두 `binance/flink/<계층>/<job>/` 아래에 있어 고려 사항 2의 장점(prefix 하나로 지우기 · 권한 주기)이 HA에도 그대로 적용된다.

- **감수하는 trade-off**
    - 실제 경로가 `binance/flink/clean/flink-dedup/ha/flink-dedup/`처럼 job 이름이 두 번 나온다. Flink가 붙이는 것이라 없앨 수 없다.
