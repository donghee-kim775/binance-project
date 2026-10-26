# variable-rate / fixed-rate 공용 이미지.
# 두 그룹은 실행 코드가 같고 구독 설정(config/<group>/*.yaml)만 다르므로 이미지는 하나만 만든다.
# 어떤 스트림을 구독할지는 Deployment가 ConfigMap으로 /config/application.yaml을 넣어 결정한다.
#
# 빌드 컨텍스트는 collector/ 이다 (datasource-ingestion/ 에서 실행):
#   docker build -f deploy/Dockerfile -t ingestion-collector:0.0.1 collector

# ── 1. build ─────────────────────────────────────────────
FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

# 의존성 레이어를 먼저 받아 캐시한다 — 소스만 바뀌면 이 단계는 재사용된다
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN ./gradlew dependencies --no-daemon > /dev/null

COPY src src
RUN ./gradlew bootJar --no-daemon -x test \
    && cp build/libs/*-SNAPSHOT.jar app.jar

# ── 2. runtime ───────────────────────────────────────────
FROM eclipse-temurin:17-jre
WORKDIR /app

# root가 아닌 사용자로 실행. K8s runAsNonRoot가 검사할 수 있도록 숫자 UID로 지정한다
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app app
USER 10001

COPY --from=build /workspace/app.jar app.jar

# 컨테이너 메모리 limit 기준으로 힙 크기를 잡는다
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"

# ConfigMap이 마운트되는 위치. 파일이 없어도 기동되도록 optional 
# volumes (application-config) : application.yaml
# mountPath: /config
ENV SPRING_CONFIG_ADDITIONAL_LOCATION="optional:file:/config/"

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
