## Data Infra

# Cert-Manager
Flink Operator webhook을 켤 때만 필요하다. 지금은 webhook을 꺼서 설치하지 않는다.
```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml
```

# Flink
Flink Kubernetes Operator 1.16.1 (cert-manager 없이 설치). 설치 방법과 설정 이유는 [Flink-Install/README.md](Flink-Install/README.md).
