# Flink serving sink 이미지 (analytics basis → Redis). Flink Operator가 이 이미지로 JobManager · TaskManager pod를 띄운다.
#
# 빌드 (consumer/sink/insight_1/flink-redis/ 에서):
#   docker build -t kimdonghee125/flink-redis:0.0.1 .

# ── 1. build ─────────────────────────────────────────────
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

# 의존성 레이어를 먼저 받아 캐시한다 — 소스만 바뀌면 이 단계는 재사용된다
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN ./gradlew dependencies --no-daemon > /dev/null

# shadowJar: Kafka 커넥터 · Jackson · Jedis를 합친 jar 하나 (Flink 본체는 아래 이미지에 있어 넣지 않는다)
COPY src src
RUN ./gradlew shadowJar --no-daemon

# ── 2. runtime ───────────────────────────────────────────
# Flink 버전은 build.gradle의 flinkVersion, FlinkDeployment의 flinkVersion(v2_2)과 맞춘다
FROM flink:2.2.1-java17

# usrlib: FlinkDeployment의 jarURI(local:///opt/flink/usrlib/flink-redis.jar)와 같다
COPY --from=build --chown=flink:flink /workspace/build/libs/flink-redis.jar /opt/flink/usrlib/flink-redis.jar
