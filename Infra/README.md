## Data Infra

# Cert-Manager
Flink Operator webhook을 켤 때만 필요하다. 지금은 webhook을 꺼서 필수는 아니지만, 클러스터에는 v1.13.0이 설치돼 있다.
```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml
```

# Flink
Flink Kubernetes Operator 1.16.1 (cert-manager 없이 설치). 설치 방법과 설정 이유는 [Flink-Install/README.md](Flink-Install/README.md).

# ClickHouse
리포트용 저장소. clean 토픽을 Kafka 엔진으로 읽어 쌓는다. 설치 방법과 설정 이유는 [ClickHouse/README.md](ClickHouse/README.md), 테이블 설계는 [database/clickhouse/table/](../database/clickhouse/table/).

# MinIO
Flink 체크포인트 · savepoint 저장소 (S3 호환). 설치 방법과 Flink 연결 설정은 [MinIO/README.md](MinIO/README.md).
