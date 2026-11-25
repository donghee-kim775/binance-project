# Kafka Streams dedup 앱 이미지. Flink와 달리 일반 JVM 프로세스 하나라 JRE 이미지면 된다.
#
# 빌드 (consumer/kstreams-dedup/ 에서):
#   docker build -t kimdonghee125/kstreams-dedup:0.0.1 .

# ── 1. build ─────────────────────────────────────────────
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

# 의존성 레이어를 먼저 받아 캐시한다 — 소스만 바뀌면 이 단계는 재사용된다
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN ./gradlew dependencies --no-daemon > /dev/null

# installDist: build/install/kstreams-dedup/bin(실행 스크립트) + lib(이 앱 jar + 의존성 jar)
COPY src src
RUN ./gradlew installDist --no-daemon

# ── 2. runtime ───────────────────────────────────────────
FROM eclipse-temurin:17-jre
RUN useradd --uid 10001 --no-create-home app
COPY --from=build /workspace/build/install/kstreams-dedup /opt/kstreams-dedup
USER 10001
# 경로 인자는 Deployment의 args로 넘긴다 (예: spot.crypto.bookticker:u)
ENTRYPOINT ["/opt/kstreams-dedup/bin/kstreams-dedup"]
