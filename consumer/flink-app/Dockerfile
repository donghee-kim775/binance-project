# Flink 앱 이미지. Flink Operator가 이 이미지로 JobManager · TaskManager pod를 띄운다.
# job마다 main 클래스가 하나이고, 어떤 job을 실행할지는 FlinkDeployment의 entryClass가 정한다 (지금은 dedup job만 있다).
#
# 빌드 (consumer/flink-app/ 에서):
#   docker build -t flink-app:0.0.1 .

# ── 1. build ─────────────────────────────────────────────
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

# 의존성 레이어를 먼저 받아 캐시한다 — 소스만 바뀌면 이 단계는 재사용된다
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN ./gradlew dependencies --no-daemon > /dev/null

# shadowJar: Kafka 커넥터 · Jackson을 합친 jar 하나 (Flink 본체는 아래 이미지에 있어 넣지 않는다)
COPY src src
RUN ./gradlew shadowJar --no-daemon

# ── 2. runtime ───────────────────────────────────────────
# 공식 Flink 이미지. Flink 버전은 build.gradle의 flinkVersion, FlinkDeployment의 flinkVersion(v2_2)과 맞춘다
FROM flink:2.2.1-java17

# usrlib: Flink가 사용자 jar를 찾는 위치. FlinkDeployment의 jarURI(local:///opt/flink/usrlib/flink-app.jar)와 같다
# 이미지 기본 사용자(flink, UID 9999)로 실행된다
COPY --from=build --chown=flink:flink /workspace/build/libs/flink-app.jar /opt/flink/usrlib/flink-app.jar
