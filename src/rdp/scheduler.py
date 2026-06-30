"""30 秒级调度器。

策略：
- 启动后立即抓一次（如在交易时段）
- 之后每 fetch_interval_sec 抓一次
- 交易时段外默认休眠（fetch_out_of_session=false）
- 每次抓取记录 fetch_runs，可查健康度
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, time as dtime
from pathlib import Path

from .fetcher import fetch_with_fallback
from .instruments import InstrumentPool
from .storage import Storage

logger = logging.getLogger(__name__)


def is_trading_session(now: datetime | None = None) -> bool:
    """判断当前是否在 A 股交易时段。

    - 集合竞价：9:15-9:25（深沪都试）
    - 早盘连续竞价：9:30-11:30
    - 午盘连续竞价：13:00-15:00
    """
    now = now or datetime.now()
    t = now.time()
    if now.weekday() >= 5:
        return False
    # 9:15-11:30:00（含 11:30:00 收盘集合竞价） — 边界包头不包尾
    if dtime(9, 15) <= t < dtime(11, 30):
        return True
    # 13:00:00-15:00:00（连续竞价；收盘瞬间 14:59:59 算，最后一秒算）
    if dtime(13, 0) <= t < dtime(15, 0):
        return True
    return False


class Scheduler:
    """调度器主类。"""

    def __init__(
        self,
        pool: InstrumentPool,
        storage: Storage,
        *,
        fetch_interval_sec: int = 30,
        fetch_out_of_session: bool = False,
        sources: list[str] | None = None,
        concurrency: int = 8,
    ):
        self.pool = pool
        self.storage = storage
        self.fetch_interval_sec = fetch_interval_sec
        self.fetch_out_of_session = fetch_out_of_session
        self.sources = sources or ["eastmoney", "sina", "tencent"]
        self.concurrency = concurrency

        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._last_run_id: int | None = None
        self._last_run_status: str = "idle"

    async def run_once(self) -> dict[str, int | float | str]:
        """执行一次抓取。返回健康度报告。"""
        t0 = time.time()
        instruments = self.pool.instruments
        run_id = self.storage.start_fetch_run(self.sources[0], len(instruments))

        try:
            quotes = await fetch_with_fallback(
                instruments, self.sources, concurrency=self.concurrency
            )
        except Exception as exc:
            self.storage.finish_fetch_run(
                run_id, count_ok=0, count_valid=0, count_stale=0, error=str(exc)
            )
            self._last_run_status = f"error: {exc}"
            logger.exception("Fetch cycle failed")
            return {"run_id": run_id, "ok": 0, "valid": 0, "elapsed": time.time() - t0}

        # 写库
        inserted = self.storage.insert_snapshots(quotes)
        count_ok = len(quotes)
        count_valid = sum(1 for q in quotes if q.price is not None)
        count_stale = sum(1 for q in quotes if q.is_stale)

        self.storage.finish_fetch_run(
            run_id, count_ok, count_valid, count_stale
        )

        elapsed = time.time() - t0
        status = (
            f"ok={count_ok}/{len(instruments)} valid={count_valid} "
            f"stale={count_stale} elapsed={elapsed:.1f}s source={self.sources[0]}"
        )
        logger.info("Fetch cycle: %s", status)
        self._last_run_id = run_id
        self._last_run_status = status

        return {
            "run_id": run_id,
            "ok": count_ok,
            "valid": count_valid,
            "stale": count_stale,
            "elapsed": elapsed,
            "source": self.sources[0],
            "inserted": inserted,
        }

    async def start(self) -> None:
        """启动调度循环。

        启动时立即执行一次 warm-up（不管交易时段），
        之后按 fetch_interval_sec 轮询，仅在交易时段抓取。
        """
        if self._running:
            logger.warning("Scheduler already running")
            return
        self._running = True
        logger.info(
            "Scheduler started: interval=%ds, sources=%s, pool=%d",
            self.fetch_interval_sec, self.sources, len(self.pool),
        )

        # Warm-up：启动立即抓一次（无论交易时段）
        try:
            await self.run_once()
        except Exception:
            logger.exception("Warm-up fetch failed")

        # 之后进入按时间间隔轮询
        while self._running:
            if self.fetch_out_of_session or is_trading_session():
                try:
                    await self.run_once()
                except Exception:
                    logger.exception("Scheduler loop iteration failed")
            else:
                logger.debug("Outside trading session, skipping")

            # 清理过期数据
            try:
                self.storage.cleanup_old_snapshots(retention_days=7)
            except Exception:
                logger.exception("Cleanup failed")

            await asyncio.sleep(self.fetch_interval_sec)

    async def stop(self) -> None:
        """停止调度循环。"""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        logger.info("Scheduler stopped")

    def status(self) -> dict[str, str | int | None]:
        return {
            "running": self._running,
            "last_run_id": self._last_run_id,
            "last_status": self._last_run_status,
            "pool_size": len(self.pool),
            "sources": self.sources,
            "interval_sec": self.fetch_interval_sec,
        }


async def _demo() -> None:  # pragma: no cover
    import yaml

    cfg = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))
    pool = await InstrumentPool.from_config(
        cfg["instruments"], Path("data/instruments_cache.json")
    )
    storage = Storage(Path(cfg["storage"]["db_path"]))
    storage.init_schema()
    storage.upsert_instruments(pool.instruments)

    sched = Scheduler(
        pool,
        storage,
        fetch_interval_sec=cfg["pool"]["fetch_interval_sec"],
        sources=cfg["pool"]["sources"],
    )
    await sched.run_once()


if __name__ == "__main__":  # pragma: no cover
    import yaml

    logging.basicConfig(level=logging.INFO)
    asyncio.run(_demo())