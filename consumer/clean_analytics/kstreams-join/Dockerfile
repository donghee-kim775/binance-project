# Kafka Streams join 앱 이미지. Flink와 달리 일반 JVM 프로세스 하나라 JRE 이미지면 된다.
#
# 빌드 (consumer/clean_analytics/kstreams-join/ 에서):
#   docker build -t kimdonghee125/kstreams-join:0.0.2 .

# ── 1. build ─────────────────────────────────────────────
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

# 의존성 레이어를 먼저 받아 캐시한다 — 소스만 바뀌면 이 단계는 재사용된다
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN ./gradlew dependencies --no-daemon > /dev/null

# installDist: build/install/kstreams-join/bin(실행 스크립트) + lib(이 앱 jar + 의존성 jar)
COPY src src
RUN ./gradlew installDist --no-daemon

# ── 2. runtime ───────────────────────────────────────────
FROM eclipse-temurin:17-jre
RUN useradd --uid 10001 --no-create-home app
COPY --from=build /workspace/build/install/kstreams-join /opt/kstreams-join
USER 10001
# 인자 없음. 설정은 Deployment의 env로 넘긴다
ENTRYPOINT ["/opt/kstreams-join/bin/kstreams-join"]
