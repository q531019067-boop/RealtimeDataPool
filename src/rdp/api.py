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
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .scheduler import Scheduler
from .storage import Storage

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = ROOT / "web"


def create_app(storage: Storage, scheduler: Scheduler | None = None) -> FastAPI:
    app = FastAPI(
        title="RealtimeDataPool",
        description="A 股实时盯盘数据池 — 30s 级全市场快照",
        version="0.1.0",
    )

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

        注意：sort_by 是按内存排序（数据量小，约 5400 只，OK）。
        """
        rows = storage.query_latest()
        if category:
            rows = [r for r in rows if r.get("category") == category]
        if min_change_pct is not None:
            rows = [r for r in rows if (r.get("change_pct") or 0) >= min_change_pct]
        if max_change_pct is not None:
            rows = [r for r in rows if (r.get("change_pct") or 0) <= max_change_pct]

        # 排序（None 排到最后）
        def _sort_key(r: dict[str, Any]) -> float:
            v = r.get(sort_by)
            if v is None:
                # 让 None 排到末尾（desc 方向）
                return float("-inf") if order == "desc" else float("inf")
            return float(v)

        rows.sort(key=_sort_key, reverse=(order == "desc"))
        return rows[:limit]

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