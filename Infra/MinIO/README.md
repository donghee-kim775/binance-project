# MinIO

Flink 체크포인트 · savepoint를 저장하는 S3 호환 저장소다. 지금 dedup job은 체크포인트를 남길 곳이 없어 `upgradeMode: stateless`로 돌고 있다 ([consumer/flink-app/README.md](../../consumer/flink-app/README.md)). MinIO가 생기면 재배포 · 장애 때 state(본 키 목록)와 Kafka offset을 체크포인트에서 이어받을 수 있다.

| 항목 | 값 |
| --- | --- |
| 이미지 | `minio/minio:RELEASE.2025-04-22T22-12-26Z` |
| namespace | `binance-storage` (ClickHouse와 같은 저장 계층) |
| 구성 | StatefulSet 1개, replica 1, PVC 10Gi |
| 클러스터 안 주소 | `http://minio.binance-storage.svc:9000` (S3 API), 9001 (웹 콘솔) |
| 계정 | Secret `minio-auth` (`root-user`, `root-password`) |
| 버킷 | `flink` (Job `minio-create-buckets`가 만든다) |
| 매니페스트 | [minio.yaml](minio.yaml) |

## 설치

```bash
kubectl apply -f Infra/MinIO/minio.yaml   # namespace가 먼저 생긴다. pod와 Job은 Secret이 없어 대기한다
kubectl create secret generic minio-auth -n binance-storage \
  --from-literal=root-user='<아이디>' --from-literal=root-password='<비밀번호 8자 이상>'

kubectl get pods -n binance-storage -w        # minio-0 READY 1/1, minio-create-buckets-xxx Completed
kubectl logs -n binance-storage job/minio-create-buckets
```

Job은 끝나고 10분 뒤 지워진다. 버킷을 다시 만들어야 하면 `kubectl apply`를 다시 하면 된다.

웹 콘솔:

```bash
kubectl port-forward -n binance-storage svc/minio 9001:9001   # http://localhost:9001
```

## Flink에서 쓰기 (다음 단계, 아직 적용 안 함)

체크포인트 경로는 job마다 나눈다: `s3://flink/checkpoints/<job 이름>`, `s3://flink/savepoints/<job 이름>`.

`consumer/flink-app/deploy/dedup.yaml`에 넣을 것:

```yaml
spec:
  flinkConfiguration:
    execution.checkpointing.dir: s3://flink/checkpoints/flink-dedup
    execution.checkpointing.savepoint-dir: s3://flink/savepoints/flink-dedup
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
- 실제로 체크포인트가 쓰이는지는 적용할 때 Flink Web UI의 Checkpoints 탭과 MinIO 콘솔의 `flink` 버킷으로 확인한다.

## 설정 이유

- **pod마다 PV · PVC:** StatefulSet의 `volumeClaimTemplates`가 pod마다 PVC를 하나씩 만든다 (`data-minio-0`). PV는 클러스터 기본 StorageClass가 PVC에 맞춰 자동으로 만든다. pod가 다시 떠도 같은 이름(`minio-0`)이라 같은 PVC에 다시 붙는다. 확인: `kubectl get pvc,pv -n binance-storage`.
- **Operator 대신 StatefulSet:** 체크포인트 저장소 하나라 분산 모드(노드 4개 이상, erasure coding)가 필요 없다. 단일 노드 · 단일 디스크로 띄운다.
- **이미지 태그 고정:** MinIO는 2025년에 커뮤니티 이미지 · 바이너리 배포를 멈췄다. `latest`는 더 이상 갱신되지 않으므로 태그를 고정한다. 이 태그가 받아지지 않으면 Docker Hub에서 남아 있는 마지막 `RELEASE.*` 태그로 바꾼다. 이 버전부터 웹 콘솔은 파일 보기 위주로 줄었다 (사용자 · 정책 관리는 `mc`로 한다).
- **계정은 Secret으로 따로:** 매니페스트를 git에 올리므로 계정을 파일에 두지 않는다.
- **버킷은 Job으로 만든다:** Flink는 버킷이 없으면 체크포인트를 쓰지 못하고 실패한다. 설치할 때 한 번 만들어 두면 job 쪽에서 신경 쓸 게 없다.

## 지금 단순화한 것

- replica 1, 디스크 1개다. PVC가 지워지면 체크포인트도 사라진다. 그때는 job이 Kafka 컨슈머 그룹의 커밋된 offset부터 다시 읽는다.
- Flink도 root 계정을 쓴다. 나중에 `flink` 버킷만 쓸 수 있는 사용자를 `mc admin user add`로 만든다.
- 오래된 체크포인트 정리는 Flink가 한다 (기본으로 최근 1개만 남긴다). savepoint는 직접 지운다.
