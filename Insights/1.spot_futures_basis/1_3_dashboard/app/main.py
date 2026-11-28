"""Insight 1 basis 대시보드 서버 (Gateway).

    Redis  latest:usdm:<심볼>:basis (hash) ─┐
           latest:usdm:basis:symbols (set) ─┼─> /api/snapshot   지금 상태 표 · KPI (접속 직후 한 번)
           pubsub:usdm:basis (채널) ────────┴─> /ws             값이 바뀔 때 push (심볼마다 최대 1초에 1번)
    ClickHouse binance.basis ──────────────────> /api/history    심볼별 30분 그래프 · ±2σ 밴드 · 과열 점
                                            └──> /api/latency    지연 p50 (collector → basis)

설계: servingapp/plan.md (Push · Redis 초기값 · Pub/Sub fan-out · 업데이트 빈도 제어),
      Insights/1.spot_futures_basis/1_2_delivery_guarantee.md 4.2 · 4.3
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

log = logging.getLogger("basis-dashboard")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# 설정 (deploy/dashboard.yaml의 env). 기본값은 로컬 개발용
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "dashboard")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# Flink sink(consumer/sink/insight_1/flink-redis RedisLatestWriter)와 같은 이름
SYMBOLS_KEY = "latest:usdm:basis:symbols"
CHANNEL = "pubsub:usdm:basis"
PUSH_INTERVAL_SEC = 1.0          # 심볼마다 최대 1초에 1번 push (servingapp/plan.md 6절)
LATENCY_SYMBOL = "BTCUSDT"       # 지연 p50은 가장 바쁜 심볼 하나로 본다 (목업: "BTC 실측")

STATIC_DIR = Path(__file__).parent / "static"


def latest_key(symbol: str) -> str:
    return f"latest:usdm:{symbol}:basis"


class Hub:
    """WebSocket 연결들과 '아직 안 보낸 최신값'을 들고 있는 곳.

    Redis 채널에서 받은 값은 pending[symbol]에 덮어쓰기만 한다 (같은 심볼이 1초에 여러 번 와도 마지막 것만 남는다).
    1초마다 pending을 비우면서 모든 연결에 한 번에 보낸다 → 접속자 수와 상관없이 심볼당 초당 1건.
    """

    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()
        self.pending: dict[str, dict] = {}

    async def broadcast_pending(self) -> None:
        if not self.pending or not self.clients:
            return
        updates = list(self.pending.values())
        self.pending.clear()
        message = json.dumps({"type": "update", "signals": updates})
        dead = []
        for client in self.clients:
            try:
                await client.send_text(message)
            except Exception:   # 브라우저를 닫았는데 아직 disconnect를 못 받은 연결
                dead.append(client)
        for client in dead:
            self.clients.discard(client)


hub = Hub()
redis_client: aioredis.Redis
clickhouse_client: httpx.AsyncClient


async def subscribe_redis() -> None:
    """Redis 채널을 구독해 hub.pending에 최신값을 쌓는다. 끊기면 2초 뒤 다시 붙는다.

    Pub/Sub은 at-most-once라 끊긴 동안의 값은 잃는다. 최신값만 중요해서 다음 basis(1초 안)로 자연히 복구된다.
    """
    while True:
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(CHANNEL)
            log.info("[dashboard] subscribed %s", CHANNEL)
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                signal = json.loads(message["data"])
                hub.pending[signal["symbol"]] = signal
        except Exception as error:
            log.warning("[dashboard] redis subscribe failed: %s — retry in 2s", error)
            await asyncio.sleep(2)


async def push_loop() -> None:
    """1초마다 쌓인 최신값을 모든 WebSocket에 보낸다."""
    while True:
        await asyncio.sleep(PUSH_INTERVAL_SEC)
        await hub.broadcast_pending()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # [호출] uvicorn — 서버 시작 · 종료 때 한 번
    global redis_client, clickhouse_client
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    clickhouse_client = httpx.AsyncClient(
        base_url=CLICKHOUSE_URL,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=10.0,
    )
    tasks = [asyncio.create_task(subscribe_redis()), asyncio.create_task(push_loop())]
    yield
    for task in tasks:
        task.cancel()
    await clickhouse_client.aclose()
    await redis_client.aclose()


app = FastAPI(title="Insight 1 basis dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/healthz")
async def healthz() -> dict:
    await redis_client.ping()
    return {"ok": True}


async def read_snapshot() -> list[dict]:
    """Redis에서 모든 심볼의 최신 BasisSignal을 읽는다 (접속 직후 초기값, servingapp/plan.md 4절)."""
    symbols = sorted(await redis_client.smembers(SYMBOLS_KEY))
    if not symbols:
        return []
    pipe = redis_client.pipeline()
    for symbol in symbols:
        pipe.hget(latest_key(symbol), "json")
    raw_values = await pipe.execute()
    return [json.loads(raw) for raw in raw_values if raw]


@app.get("/api/snapshot")
async def snapshot() -> dict:
    return {"signals": await read_snapshot()}


async def query_clickhouse(sql: str, params: dict) -> dict:
    """ClickHouse HTTP 인터페이스. 값은 param_<이름>으로 넘긴다 (SQL 문자열에 직접 붙이지 않는다)."""
    query_params = {"default_format": "JSONCompact", **{f"param_{name}": value for name, value in params.items()}}
    response = await clickhouse_client.post("/", params=query_params, content=sql)
    response.raise_for_status()
    return response.json()


@app.get("/api/history")
async def history(minutes: int = 30) -> dict:
    """심볼별 최근 N분 시계열. FINAL로 merge 전 중복(같은 symbol · event_time)을 한 건으로 본다."""
    minutes = max(1, min(minutes, 120))
    result = await query_clickhouse(
        """
        SELECT symbol,
               toUnixTimestamp64Milli(event_time) AS t,
               bps, mean_bps, std_bps, z, status
        FROM binance.basis FINAL
        WHERE event_time >= now64(3) - toIntervalMinute({minutes:UInt32})
        ORDER BY symbol, event_time
        """,
        {"minutes": minutes},
    )
    series: dict[str, dict[str, list]] = {}
    for symbol, t, bps, mean_bps, std_bps, z, status in result["data"]:
        points = series.setdefault(symbol, {"t": [], "bps": [], "mean": [], "std": [], "status": []})
        points["t"].append(t)
        points["bps"].append(bps)
        points["mean"].append(mean_bps)
        points["std"].append(std_bps)
        points["status"].append(status)
    return {"minutes": minutes, "series": series}


@app.get("/api/latency")
async def latency() -> dict:
    """collector → basis 지연 p50 (최근 5분, 한 심볼). 노드 시계 차이만큼 음수도 나올 수 있다."""
    result = await query_clickhouse(
        """
        SELECT quantile(0.5)(latency_ms), count()
        FROM binance.basis
        WHERE symbol = {symbol:String} AND event_time >= now64(3) - INTERVAL 5 MINUTE
        """,
        {"symbol": LATENCY_SYMBOL},
    )
    p50, count = result["data"][0]
    return {"symbol": LATENCY_SYMBOL, "p50_ms": p50 if count else None, "count": count}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    # 1. 접속 직후 Redis 최신값을 한 번 보낸다 (다음 push까지 빈 화면이 되지 않게)
    await websocket.accept()
    await websocket.send_text(json.dumps({"type": "snapshot", "signals": await read_snapshot()}))
    hub.clients.add(websocket)
    # 2. 이후에는 push_loop가 보낸다. 여기서는 연결이 끊길 때까지 기다리기만 한다
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.discard(websocket)
