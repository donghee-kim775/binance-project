# Flink dedup 테스트 이미지.
# Flink 클러스터(Operator) 없이, 컨테이너 하나 안에서 JobManager와 TaskManager를 같이 띄운다 (MiniCluster).
# ./gradlew run과 같은 방식이라 로컬에서 확인한 동작이 그대로 kind에서 돈다.
#
# 빌드 (consumer/flink-test/flink-test-app/ 에서):
#   docker build -t flink-test:0.0.1 .

# ── 1. build ─────────────────────────────────────────────
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

# 의존성 레이어를 먼저 받아 캐시한다 — 소스만 바뀌면 이 단계는 재사용된다
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN ./gradlew dependencies --no-daemon > /dev/null

# installDist: jar들을 lib/에, 실행 스크립트를 bin/에 모은다 (fat jar로 합치지 않아 Flink 서비스 파일이 겹치지 않는다)
COPY src src
RUN ./gradlew installDist --no-daemon

# ── 2. runtime ───────────────────────────────────────────
FROM eclipse-temurin:17-jre
WORKDIR /app

# root가 아닌 사용자로 실행. K8s runAsNonRoot가 검사할 수 있도록 숫자 UID로 지정한다
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app app
USER 10001

COPY --from=build /workspace/build/install/flink-test-app/ /app/

# build.gradle의 -Xmx768m(로컬용) 뒤에 붙어 이 값이 이긴다. 컨테이너 limit(1Gi) 안에
# 힙 + Flink managed/network 메모리(off-heap) + metaspace가 들어가도록 힙을 줄인다
ENV JAVA_OPTS="-Xmx512m"

ENTRYPOINT ["/app/bin/flink-test-app"]
