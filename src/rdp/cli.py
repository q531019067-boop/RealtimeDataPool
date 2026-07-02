"""CLI 入口。

子命令：
- serve       启动调度器 + API（默认）
- fetch       单次抓取（不入库？默认入库）
- refresh-pool 重新拉股票池
- status      打印当前状态
- shell       启动交互式 Python 环境（预加载 rdp 模块）
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import sys
from pathlib import Path

import yaml

from . import __version__
from .api import create_app
from .instruments import InstrumentPool
from .scheduler import Scheduler, is_trading_session
from .storage import Storage


ROOT = Path(__file__).resolve().parent.parent.parent  # src/rdp/cli.py -> project root


def load_config(path: Path | None = None) -> dict:
    cfg_path = path or (ROOT / "config.yaml")
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8"))


def setup_logging(cfg: dict) -> None:
    log_cfg = cfg.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_file = ROOT / log_cfg.get("file", "logs/rdp.log")
    log_file.parent.mkdir(parents=True, exist_ok=True)

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        ),
    ]
    logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)


async def _serve(cfg: dict, host: str, port: int) -> None:
    """启动调度器 + API。"""
    import uvicorn

    storage = Storage(ROOT / cfg["storage"]["db_path"])
    storage.init_schema()

    pool = await InstrumentPool.from_config(
        cfg["instruments"],
        ROOT / "data" / "instruments_cache.json",
    )
    storage.upsert_instruments(pool.instruments)

    sched = Scheduler(
        pool,
        storage,
        fetch_interval_sec=cfg["pool"]["fetch_interval_sec"],
        orderbook_interval_sec=cfg["pool"].get("orderbook_interval_sec", 300),
        cleanup_interval_sec=cfg["pool"].get("cleanup_interval_sec", 1800),
        retention_days=cfg["storage"].get("retention_days", 7),
        fetch_out_of_session=cfg["pool"]["fetch_out_of_session"],
        sources=cfg["pool"]["sources"],
        concurrency=cfg["pool"]["concurrency_per_source"],
        jitter_ms=cfg.get("fetcher", {}).get("jitter_ms", 30),
        retry_max=cfg.get("fetcher", {}).get("retry_max", 1),
    )

    app = create_app(storage, sched)

    # 后台启动调度器
    sched_task = asyncio.create_task(sched.start())

    # uvicorn
    config = uvicorn.Config(
        app, host=host, port=port, log_level="warning", access_log=False
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    logger = logging.getLogger("rdp")
    logger.info("=" * 60)
    logger.info(
        "RDP started: pool=%d codes, basic_interval=%ds, orderbook_interval=%ds, "
        "sources=%s, fetch_out_of_session=%s",
        len(pool), sched.fetch_interval_sec, sched.orderbook_interval_sec,
        sched.sources, cfg["pool"]["fetch_out_of_session"],
    )
    logger.info(
        "API:    http://%s:%d  |  DB: %s  |  log: %s",
        host, port, cfg["storage"]["db_path"],
        ROOT / cfg["logging"]["file"],
    )
    logger.info(
        "Trading session: %s",
        "ACTIVE" if is_trading_session() else "INACTIVE (will skip unless fetch_out_of_session)",
    )
    logger.info("=" * 60)

    try:
        await asyncio.gather(sched_task, server_task)
    except asyncio.CancelledError:
        pass
    finally:
        await sched.stop()
        storage.close()


async def _fetch_once(cfg: dict) -> None:
    storage = Storage(ROOT / cfg["storage"]["db_path"])
    storage.init_schema()

    pool = await InstrumentPool.from_config(
        cfg["instruments"],
        ROOT / "data" / "instruments_cache.json",
    )
    storage.upsert_instruments(pool.instruments)

    sched = Scheduler(
        pool,
        storage,
        fetch_interval_sec=cfg["pool"]["fetch_interval_sec"],
        fetch_out_of_session=True,
        sources=cfg["pool"]["sources"],
        concurrency=cfg["pool"]["concurrency_per_source"],
    )
    result = await sched.run_once()
    print(f"Fetch done: {result}")
    storage.close()


async def _refresh_pool(cfg: dict) -> None:
    pool = await InstrumentPool.from_config(
        cfg["instruments"],
        ROOT / "data" / "instruments_cache.json",
        force_refresh=True,
    )
    storage = Storage(ROOT / cfg["storage"]["db_path"])
    storage.init_schema()
    storage.upsert_instruments(pool.instruments)
    print(f"Pool refreshed: {len(pool)} codes")


def _status(cfg: dict) -> None:
    storage = Storage(ROOT / cfg["storage"]["db_path"])
    try:
        instruments = storage.list_instruments()
        runs = storage.recent_runs(limit=5)
        snapshot_count = storage.snapshot_count()
    finally:
        storage.close()
    print(f"Instruments: {len(instruments)}")
    print(f"Snapshots: {snapshot_count}")
    print(f"Last 5 runs:")
    for r in runs:
        print(
            f"  #{r['id']} {r['started_at']:.0f} src={r['source']} "
            f"ok={r['count_ok']} valid={r['count_valid']} stale={r['count_stale']} "
            f"err={r['error']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="rdp",
        description="RealtimeDataPool — A 股实时盯盘数据池",
    )
    parser.add_argument("--config", "-c", type=Path, help="配置文件路径")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_serve = sub.add_parser("serve", help="启动调度器 + API（默认常驻）")
    p_serve.add_argument("--host", default=None)
    p_serve.add_argument("--port", type=int, default=None)

    sub.add_parser("fetch", help="单次抓取")
    sub.add_parser("refresh-pool", help="重新拉股票池")
    sub.add_parser("status", help="打印状态")

    args = parser.parse_args()
    cfg = load_config(args.config)
    setup_logging(cfg)

    api_cfg = cfg.get("api", {})
    host = getattr(args, "host", None) or api_cfg.get("host", "0.0.0.0")
    port = getattr(args, "port", None) or int(api_cfg.get("port", 5080))

    if args.cmd == "serve":
        asyncio.run(_serve(cfg, host, port))
    elif args.cmd == "fetch":
        asyncio.run(_fetch_once(cfg))
    elif args.cmd == "refresh-pool":
        asyncio.run(_refresh_pool(cfg))
    elif args.cmd == "status":
        _status(cfg)


if __name__ == "__main__":
    main()
