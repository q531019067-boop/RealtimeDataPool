"""FastAPI REST 接口。

端点：
- GET  /api/health                健康检查
- GET  /api/status                调度器状态
- GET  /api/pool                  股票池列表
- GET  /api/snapshot              单只最新快照
- GET  /api/snapshots             多只最新快照
- GET  /api/snapshots/all         最新快照（按 fetch 时间倒序）
- GET  /api/history               单只历史快照
- GET  /api/runs                  抓取运行日志
- GET  /                          监控页面

安全：内置 in-memory rate limit 中间件(60 req/min per IP,白名单 localhost)
"""

from __future__ import annotations

import logging
import time
from collections import deque
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from .scheduler import Scheduler
from .storage import Storage

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = ROOT / "web"


# ---------- Rate Limit 中间件 ----------
class _RateLimitMiddleware(BaseHTTPMiddleware):
    """简单 in-memory sliding window rate limit。

    - 60 req/min per IP(默认),可通过环境变量 RDP_RATE_LIMIT_PER_MIN 覆盖
    - localhost 白名单
    - 超过返回 429 + Retry-After header
    - 内存占用:每个 IP 一个 deque,64 个时间戳 * 8 bytes = 512B,1000 IP = 512KB
    """

    def __init__(self, app, limit_per_min: int = 60):
        super().__init__(app)
        self.limit = limit_per_min
        # {ip: deque[float]}
        self._hits: dict[str, deque] = {}

    def _check(self, ip: str) -> tuple[bool, int]:
        """返回 (allowed, retry_after_sec)."""
        if self.limit <= 0:
            return True, 0
        now = time.time()
        cutoff = now - 60.0
        dq = self._hits.get(ip)
        if dq is None:
            dq = deque()
            self._hits[ip] = dq
        # 清掉过期的
        while dq and dq[0] < cutoff:
            dq.popleft()
        if len(dq) >= self.limit:
            # 最早一个过期时间 = retry_after
            retry = max(1, int(60 - (now - dq[0])))
            return False, retry
        dq.append(now)
        return True, 0

    async def dispatch(self, request: Request, call_next) -> Response:
        # 健康检查 + 静态资源不限流
        path = request.url.path
        if path in ("/api/health", "/") or path.startswith("/assets"):
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        # localhost / 127.0.0.1 / 内网白名单
        if client_ip in ("127.0.0.1", "::1", "localhost"):
            return await call_next(request)

        allowed, retry = self._check(client_ip)
        if not allowed:
            logger.warning("Rate limit hit: %s on %s", client_ip, path)
            return JSONResponse(
                status_code=429,
                content={"detail": "Too Many Requests", "retry_after_sec": retry},
                headers={"Retry-After": str(retry)},
            )
        return await call_next(request)


def create_app(
    storage: Storage,
    scheduler: Scheduler | None = None,
    rate_limit_per_min: int | None = None,
) -> FastAPI:
    # ⚡ rate limit 优先从 env RDP_RATE_LIMIT_PER_MIN 读,默认 60
    if rate_limit_per_min is None:
        import os
        try:
            rate_limit_per_min = int(os.environ.get("RDP_RATE_LIMIT_PER_MIN", "60"))
        except ValueError:
            rate_limit_per_min = 60
    app = FastAPI(
        title="RealtimeDataPool",
        description="A 股实时盯盘数据池 — 30s 级全市场快照",
        version="0.1.0",
    )

    # ⚡ 限流中间件(放在最前面)
    if rate_limit_per_min > 0:
        app.add_middleware(_RateLimitMiddleware, limit_per_min=rate_limit_per_min)

    # 静态资源
    if WEB_DIR.exists():
        app.mount("/assets", StaticFiles(directory=WEB_DIR), name="assets")

    # ---------- HTML ----------

    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        index_file = WEB_DIR / "index.html"
        if not index_file.exists():
            return HTMLResponse("<h1>RealtimeDataPool</h1><p>监控页面未生成</p>")
        return HTMLResponse(index_file.read_text(encoding="utf-8"))

    # ---------- API ----------

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "server_time": time.time(),
            "scheduler": scheduler.status() if scheduler else None,
        }

    @app.get("/api/status")
    async def status() -> dict[str, Any]:
        if scheduler is None:
            raise HTTPException(503, "Scheduler not initialized")
        s = scheduler.status()
        # 加上数据库统计
        with storage._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM instruments").fetchone()
            snap_row = conn.execute("SELECT COUNT(*) AS c FROM snapshots").fetchone()
            run_row = conn.execute(
                "SELECT * FROM fetch_runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
        s["instruments"] = row["c"]
        s["snapshots_total"] = snap_row["c"]
        s["last_run"] = dict(run_row) if run_row else None
        return s

    @app.get("/api/pool")
    async def pool(
        category: str | None = Query(None, description="stock/etf/lof"),
    ) -> list[dict[str, str]]:
        return storage.list_instruments(category=category)

    @app.get("/api/snapshot")
    async def snapshot(code: str = Query(..., description="股票代码")) -> dict[str, Any]:
        rows = storage.query_latest([code])
        if not rows:
            raise HTTPException(404, f"No snapshot for {code}")
        return rows[0]

    @app.get("/api/snapshots")
    async def snapshots(
        codes: str = Query(..., description="逗号分隔的股票代码"),
    ) -> list[dict[str, Any]]:
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        if not code_list:
            return []
        if len(code_list) > 500:
            raise HTTPException(400, "Too many codes (max 500)")
        return storage.query_latest(code_list)

    @app.get("/api/snapshots/all")
    async def snapshots_all(
        limit: int = Query(200, ge=1, le=2000),
        sort_by: str = Query("change_pct", description="排序字段"),
        order: str = Query("desc", description="asc/desc"),
        category: str | None = Query(None, description="stock/etf/lof"),
        min_change_pct: float | None = Query(None, description="涨跌幅下限"),
        max_change_pct: float | None = Query(None, description="涨跌幅上限"),
    ) -> list[dict[str, Any]]:
        """全市场最新快照，支持排序 + 涨跌幅过滤。

        ⚡ 排序/过滤/limit 全部在 SQL 层完成(2026-07-02)。
        sort_by 白名单: change_pct / price / open / high / low /
        prev_close / volume / amount / fetched_at。其它值返回 400。
        """
        try:
            return storage.query_latest_paged(
                sort_by=sort_by,
                order=order,
                limit=limit,
                category=category,
                min_change_pct=min_change_pct,
                max_change_pct=max_change_pct,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @app.get("/api/history")
    async def history(
        code: str = Query(...),
        limit: int = Query(100, ge=1, le=1000),
        since: float | None = Query(None, description="起始时间戳"),
    ) -> list[dict[str, Any]]:
        return storage.query_history(code, limit=limit, since=since)

    @app.get("/api/runs")
    async def runs(limit: int = Query(20, ge=1, le=200)) -> list[dict[str, Any]]:
        return storage.recent_runs(limit=limit)

    return app
