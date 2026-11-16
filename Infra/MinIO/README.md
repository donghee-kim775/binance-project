# MinIO

Flink 체크포인트 · savepoint를 저장하는 S3 호환 저장소다. dedup job이 여기에 체크포인트 · savepoint를 남기고 `upgradeMode: savepoint`로 재배포 · 장애 때 state(본 키 목록)와 Kafka offset을 이어받는다 ([consumer/flink-app/deploy/dedup.yaml](../../consumer/flink-app/deploy/dedup.yaml)).

| 항목 | 값 |
| --- | --- |
| 이미지 | `pgsty/silo:RELEASE.2026-10-26T00-00-00Z` (MinIO 커뮤니티 fork SILO) |
| namespace | `binance-storage` (ClickHouse와 같은 저장 계층) |
| 구성 | 분산 모드. StatefulSet replica 4 (노드마다 1개), pod마다 PVC 10Gi, erasure set 1개 (드라이브 4, EC:2) |
| 클러스터 안 주소 | `http://minio.binance-storage.svc:9000` (S3 API), 9001 (웹 콘솔) |
| 클러스터 밖 주소 (NodePort) | `http://<노드 IP>:30901` (웹 콘솔), `http://<노드 IP>:30902` (S3 API) |
| 계정 | Secret `minio-auth` (`root-user`, `root-password`) |
| 버킷 | `binance` (Job `minio-create-buckets`가 만든다). 안에서 용도 · job별로 경로를 나눈다: `checkpoints/`, `savepoints/`, `ha/` |
| 매니페스트 | [minio.yaml](minio.yaml) |

## 설치

```bash
kubectl apply -f Infra/MinIO/minio.yaml   # namespace가 먼저 생긴다. pod와 Job은 Secret이 없어 대기한다
kubectl create secret generic minio-auth -n binance-storage \
  --from-literal=root-user='<아이디>' --from-literal=root-password='<비밀번호 8자 이상>'

kubectl get pods -n binance-storage -w        # minio-0 ~ 3 READY 1/1, minio-create-buckets-xxx Completed
kubectl logs -n binance-storage job/minio-create-buckets
```

Job은 끝나고 10분 뒤 지워진다. 버킷을 다시 만들어야 하면 `kubectl apply`를 다시 하면 된다.

웹 콘솔은 `http://<노드 IP>:30901` (노드 IP는 `kubectl get nodes -o wide`의 INTERNAL-IP). 로그인은 `minio-auth`의 아이디 · 비밀번호다.

PC에서 `mc`로 붙을 때는 S3 API 포트를 쓴다: `mc alias set binance http://<노드 IP>:30902 <아이디> <비밀번호>`.

## Flink에서 쓰기 (dedup.yaml에 적용, 배포 검증 전)

경로는 job마다 나눈다: `s3://binance/checkpoints/<job 이름>`, `s3://binance/savepoints/<job 이름>`, `s3://binance/ha/<job 이름>` (JobManager HA).

`consumer/flink-app/deploy/dedup.yaml`에 넣을 것:

```yaml
spec:
  flinkConfiguration:
    execution.checkpointing.dir: s3://binance/checkpoints/flink-dedup
    execution.checkpointing.savepoint-dir: s3://binance/savepoints/flink-dedup
    s3.endpoint: http://minio.binance-storage.svc:9000
    s3.path.style.access: "true"          # MinIO는 버킷을 도메인이 아니라 경로로 찾는다
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: ENABLE_BUILT_IN_PLUGINS   # Flink 이미지 /opt/flink/opt의 S3 플러그인을 켠다
              value: flink-s3-fs-hadoop-2.2.1.jar
            - name: AWS_ACCESS_KEY_ID
              valueFrom: { secretKeyRef: { name: minio-auth, key: root-user } }
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom: { secretKeyRef: { name: minio-auth, key: root-password } }
  job:
    upgradeMode: savepoint                # stateless → savepoint
```

- **Secret은 namespace마다 따로다.** Flink job은 `binance-processing`에 있으므로 `minio-auth`를 거기에도 만든다.
- 계정을 환경 변수로 넘겨 `flinkConfiguration`(git에 올라가는 파일)에 키가 남지 않게 한다. S3 플러그인(hadoop)은 `AWS_ACCESS_KEY_ID` · `AWS_SECRET_ACCESS_KEY`를 읽는다.
- 실제로 체크포인트가 쓰이는지는 적용할 때 Flink Web UI의 Checkpoints 탭과 MinIO 콘솔의 `binance` 버킷으로 확인한다.

## 설정 이유

- **pod마다 PV · PVC:** StatefulSet의 `volumeClaimTemplates`가 pod마다 PVC를 하나씩 만든다 (`data-minio-0` ~ `3`). PV는 클러스터 기본 StorageClass가 PVC에 맞춰 자동으로 만든다. pod가 다시 떠도 같은 이름(`minio-0`)이라 같은 PVC에 다시 붙는다. 확인: `kubectl get pvc,pv -n binance-storage`.
- **NodePort로 연다:** PC에서 콘솔과 `mc`로 바로 붙으려고 연다. Flink는 클러스터 안 주소(`minio.binance-storage.svc:9000`)를 그대로 쓴다. 포트는 kafka-ui(30080), ClickHouse(30123, 30900)와 겹치지 않게 30901 · 30902를 쓴다.
- **분산 모드 (erasure coding):** 노드가 4대라 pod를 노드마다 하나씩 4개 띄우고, 드라이브 4개를 erasure set 하나로 묶는다. 기본 패리티 EC:2라 객체를 데이터 2조각 + 패리티 2조각으로 나눠 4개 드라이브에 하나씩 둔다.
  - 버티는 장애: 드라이브(노드) 1개가 빠지면 읽기 · 쓰기가 모두 된다 (쓰기 정족수 3/4). 2개가 빠지면 읽기만 된다. 설치 때 pod 하나를 지운 상태에서 읽기 · 쓰기가 되는 것을 확인했다.
  - 왜 4개: erasure coding은 드라이브 4개가 표준 최소 구성이다. 3개로는 패리티를 1밖에 못 두고, 노드도 4대라 남는 노드가 없다.
  - trade-off: 저장 효율이 50%다 (10Gi × 4 = 40Gi 중 20Gi를 쓴다). pod 4개가 메모리를 4배 쓴다. 노드를 늘려도 pod 수를 그냥 바꿀 수 없다. 서버 주소 목록(`{0...3}`)이 디스크 포맷에 박히므로, 늘리려면 pool을 새로 추가해야 한다.
- **headless Service + `publishNotReadyAddresses`:** pod끼리 `minio-<N>.minio-headless...` 주소로 서로를 찾는다. 처음 뜰 때는 과반이 모여야 ready가 되므로, ready 전에도 DNS가 잡혀야 한다.
- **`podManagementPolicy: Parallel`:** 기본값 OrderedReady는 minio-0이 ready가 돼야 minio-1을 띄운다. 그런데 minio-0은 다른 pod가 떠야 ready가 되므로 서로 기다리다 멈춘다. 그래서 4개를 동시에 띄운다.
- **노드마다 pod 하나 (`podAntiAffinity`):** 두 pod가 한 노드에 있으면 그 노드가 죽을 때 드라이브 2개를 한꺼번에 잃어 쓰기가 멈춘다. `local-path`라 pod는 처음 뜬 노드에 묶인다.
- **Operator 대신 StatefulSet:** 단일 erasure set 하나라 Operator(MinIO Operator, Tenant CRD)까지 필요 없다. pool 추가 · 자동 재조정이 필요해지면 그때 검토한다.
- **공식 이미지 대신 SILO:** MinIO는 커뮤니티 이미지 · 바이너리 배포를 멈췄고, 원본 저장소는 2026년 4월에 archive됐다. 이후 Docker Hub의 `minio/minio` 저장소가 통째로 지워졌고 (`latest`도 받아지지 않는다), `quay.io/minio/minio`에도 태그가 없다. 그래서 PGSTY가 관리하는 커뮤니티 fork [SILO](https://github.com/pgsty/silo)를 쓴다.
  - 고른 이유: `MINIO_ROOT_*` 환경 변수, S3 API, 데이터 형식이 같아 매니페스트와 Flink 설정을 바꿀 게 없다. 이미지 안에 `mc`(`mcli`)도 있다. 1~2개월마다 릴리스하고, 릴리스마다 SLSA provenance(GitHub Actions `docker-release.yml`에서 해당 태그로 빌드됐다는 증명)와 Sigstore 서명을 낸다.
  - 다른 후보: `pgsty/minio`(SILO의 옛 이름, 2026-08-04 태그에서 멈춤), Chainguard `cgr.dev/chainguard/minio`(무료로는 `latest`만 받을 수 있어 버전 고정이 안 됨), MinIO 소스를 직접 빌드(무엇이 들었는지 알 수 있지만 보안 패치를 직접 챙겨야 함).
  - trade-off: MinIO, Inc.와 관계없는 제3자 빌드다. 보안 패치 외에 자체 수정(콘솔, phone-home 제거 등)이 들어 있고, 관리 주체가 작은 팀이라 관리가 멈출 수 있다. 체크포인트만 담는 저장소라 그 위험을 받아들인다. 멈추면 S3 호환 저장소(SeaweedFS, Garage 등)로 옮긴다.
- **이미지 태그 고정:** `latest`가 아니라 `RELEASE.*` 태그로 고정해, 재시작할 때 모르는 새 버전이 받아지지 않게 한다. 셸이 있어야 버킷 Job이 돌므로 `-distroless` 태그는 쓰지 않는다.
- **계정은 Secret으로 따로:** 매니페스트를 git에 올리므로 계정을 파일에 두지 않는다.
- **버킷은 Job으로 만든다:** Flink는 버킷이 없으면 체크포인트를 쓰지 못하고 실패한다. 설치할 때 한 번 만들어 두면 job 쪽에서 신경 쓸 게 없다.

## 지금 단순화한 것

- 노드마다 드라이브는 1개다. 드라이브(PVC)가 3개 이상 사라지면 체크포인트도 사라진다. 그때는 job이 Kafka 컨슈머 그룹의 커밋된 offset부터 다시 읽는다.
- 노드가 4대뿐이라 노드 하나가 고장 나 오래 빠지면 대신 띄울 곳이 없다. 그동안은 3/4로 돌고, 하나가 더 빠지면 쓰기가 멈춘다.
- Flink도 root 계정을 쓴다. 나중에 `binance` 버킷만 쓸 수 있는 사용자를 `mc admin user add`로 만든다.
- 오래된 체크포인트 정리는 Flink가 한다 (기본으로 최근 1개만 남긴다). savepoint는 직접 지운다.
