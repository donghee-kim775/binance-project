# Insight 1 basis 대시보드 이미지 (FastAPI + 정적 화면 한 장)
#
# 빌드 (Insights/1.spot_futures_basis/1_3_dashboard/ 에서):
#   docker build -t kimdonghee125/basis-dashboard:0.0.2 .
FROM python:3.12-slim

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app app

RUN useradd --uid 10001 --no-create-home app
USER 10001

EXPOSE 8080
# 설정은 Deployment의 env로 넘긴다 (REDIS_URL, CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
