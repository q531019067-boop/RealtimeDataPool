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

        # ---- 运维可观测性字段 ----
        self._start_time: float = 0.0         # scheduler 启动时间（用于 uptime）
        self._cycle_count: int = 0            # 已完成的抓取周期数
        self._last_cycle_at: float = 0.0      # 上一次 run_once 结束时间（用于 drift）
        self._last_session_state: bool | None = None  # 上一刻交易时段状态（用于状态切换日志）
        self._idle_heartbeat_remaining: int = 20  # 空转期心跳间隔（按 cycle 计）

    async def run_once(self) -> dict[str, int | float | str]:
        """执行一次抓取。返回健康度报告。"""
        t0 = time.time()
        cycle_id = self._cycle_count  # 用抓取前的快照，错误路径也累计
        instruments = self.pool.instruments
        run_id = self.storage.start_fetch_run(self.sources[0], len(instruments))

        # 计算与上一周期的时间差（drift）
        gap_since_last = (
            t0 - self._last_cycle_at if self._last_cycle_at > 0 else None
        )

        fetch_t0 = time.time()
        try:
            quotes = await fetch_with_fallback(
                instruments, self.sources, concurrency=self.concurrency
            )
        except Exception as exc:
            self.storage.finish_fetch_run(
                run_id, count_ok=0, count_valid=0, count_stale=0, error=str(exc)
            )
            self._last_run_status = f"error: {exc}"
            self._last_cycle_at = time.time()
            self._cycle_count += 1
            logger.exception(
                "Fetch cycle #%d FAILED in %.1fs: %s",
                cycle_id, time.time() - t0, exc,
            )
            return {"run_id": run_id, "ok": 0, "valid": 0, "elapsed": time.time() - t0}

        fetch_elapsed = time.time() - fetch_t0

        # 写库（独立计时，便于排查 DB 慢）
        db_t0 = time.time()
        inserted = self.storage.insert_snapshots(quotes)
        db_elapsed = time.time() - db_t0

        count_ok = len(quotes)
        count_valid = sum(1 for q in quotes if q.price is not None)
        count_stale = sum(1 for q in quotes if q.is_stale)
        count_ob = sum(
            1 for q in quotes if q.bid_prices and q.bid_prices[0] is not None
        )
        valid_pct = count_valid / max(count_ok, 1) * 100

        self.storage.finish_fetch_run(
            run_id, count_ok, count_valid, count_stale
        )

        elapsed = time.time() - t0
        now_ts = time.time()

        # 数据源时间戳 vs 本地时间（衡量"数据有多新"）
        ts_vals = [q.timestamp for q in quotes if q.timestamp > 0]
        ts_max = max(ts_vals) if ts_vals else 0.0
        ts_min = min(ts_vals) if ts_vals else 0.0
        data_age = (now_ts - ts_max) if ts_max > 0 else None

        # drift: 实际两周期间隔 vs 目标 30s
        drift_str = ""
        if gap_since_last is not None:
            drift = gap_since_last - self.fetch_interval_sec
            sign = "+" if drift >= 0 else ""
            drift_str = f" gap={gap_since_last:.1f}s(Δ{drift:+.1f}s)"
        data_age_str = f" data_age={data_age:.1f}s" if data_age is not None else ""

        status = (
            f"cycle=#{cycle_id} src={self.sources[0]} "
            f"ok={count_ok}/{len(instruments)} valid={count_valid}({valid_pct:.1f}%) "
            f"stale={count_stale} ob={count_ob} "
            f"fetch={fetch_elapsed:.1f}s db={db_elapsed:.2f}s total={elapsed:.1f}s"
            f"{drift_str}{data_age_str}"
        )
        logger.info("Fetch cycle: %s", status)

        # 慢周期警告：超出 1.5× 间隔
        if elapsed > self.fetch_interval_sec * 1.5:
            logger.warning(
                "SLOW cycle #%d: %.1fs (>1.5× interval=%ds). "
                "Possible bottleneck: network/db/source-throttling. "
                "fetch=%.1fs db=%.2fs",
                cycle_id, elapsed, self.fetch_interval_sec,
                fetch_elapsed, db_elapsed,
            )
        # 数据质量警告
        if count_ok > 0 and valid_pct < 90:
            logger.warning(
                "LOW data quality cycle #%d: valid=%.1f%% (%d/%d). "
                "Inspect source health.",
                cycle_id, valid_pct, count_valid, count_ok,
            )
        if data_age is not None and data_age > 60:
            logger.warning(
                "STALE data cycle #%d: latest quote is %.1fs old "
                "(source timestamp drift).",
                cycle_id, data_age,
            )

        self._last_run_id = run_id
        self._last_run_status = status
        self._last_cycle_at = now_ts
        self._cycle_count += 1

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
        self._start_time = time.time()
        logger.info(
            "Scheduler started: interval=%ds, sources=%s, pool=%d, "
            "fetch_out_of_session=%s",
            self.fetch_interval_sec, self.sources, len(self.pool),
            self.fetch_out_of_session,
        )
        logger.info(
            "Trading session NOW: %s",
            "ACTIVE" if is_trading_session() else "INACTIVE",
        )
        self._last_session_state = is_trading_session()

        # Warm-up：启动立即抓一次（无论交易时段）
        try:
            await self.run_once()
        except Exception:
            logger.exception("Warm-up fetch failed")

        # 之后进入按时间间隔轮询
        while self._running:
            in_session = is_trading_session()
            # 交易时段状态切换检测
            if (
                self._last_session_state is not None
                and self._last_session_state != in_session
            ):
                logger.info(
                    "Trading session %s → %s (cycle #%d, uptime=%.0fs)",
                    "ACTIVE" if self._last_session_state else "INACTIVE",
                    "ACTIVE" if in_session else "INACTIVE",
                    self._cycle_count,
                    time.time() - self._start_time,
                )
            self._last_session_state = in_session

            if self.fetch_out_of_session or in_session:
                try:
                    await self.run_once()
                except Exception:
                    logger.exception("Scheduler loop iteration failed")
            else:
                # 空转期心跳：每 ~20 个空转周期打一次（让你 4 小时盘后回来知道进程还活着）
                self._idle_heartbeat_remaining -= 1
                if self._idle_heartbeat_remaining <= 0:
                    self._idle_heartbeat_remaining = 20
                    logger.info(
                        "Heartbeat: scheduler idle (outside session) — "
                        "uptime=%.0fs, cycles=%d, last=%s",
                        time.time() - self._start_time,
                        self._cycle_count,
                        self._last_run_status,
                    )
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