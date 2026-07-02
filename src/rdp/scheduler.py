"""调度器。

策略：
- 启动后立即 warm-up 一次（basic + orderbook）
- 之后两阶段：
  * basic phase：每 fetch_interval_sec（默认 30s）跑一次全市场基础行情
  * orderbook phase：每 orderbook_interval_sec（默认 300s）从腾讯补一次盘口
- 盘口与基础行情解耦，削减 90%+ 腾讯请求，降低反爬限流概率
- 每次抓取记录 fetch_runs，可查健康度
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, time as dtime
from pathlib import Path

from .fetcher import TencentFetcher, fetch_with_fallback
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
        orderbook_interval_sec: int = 300,
        cleanup_interval_sec: int = 1800,  # P2: 30min 一次
        retention_days: int = 7,
        fetch_out_of_session: bool = False,
        sources: list[str] | None = None,
        concurrency: int = 8,
        jitter_ms: int = 30,
        retry_max: int = 1,
    ):
        self.pool = pool
        self.storage = storage
        self.fetch_interval_sec = fetch_interval_sec
        self.orderbook_interval_sec = orderbook_interval_sec
        self.cleanup_interval_sec = cleanup_interval_sec
        self.retention_days = retention_days
        self.fetch_out_of_session = fetch_out_of_session
        self.sources = sources or ["eastmoney", "sina", "tencent"]
        self.concurrency = concurrency
        self.jitter_ms = jitter_ms
        self.retry_max = retry_max

        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._last_run_id: int | None = None
        self._last_run_status: str = "idle"

        # ---- 运维可观测性字段 ----
        self._start_time: float = 0.0         # scheduler 启动时间（用于 uptime）
        self._cycle_count: int = 0            # 已完成的 basic 抓取周期数
        self._last_cycle_at: float = 0.0      # 上一次 run_once 结束时间（用于 drift）
        self._last_orderbook_at: float = 0.0  # 上一次盘口补全时间
        self._orderbook_count: int = 0        # 盘口补全累计次数
        self._last_session_state: bool | None = None  # 上一刻交易时段状态（用于状态切换日志）
        self._idle_heartbeat_remaining: int = 20  # 空转期心跳间隔（按 cycle 计）
        # P2 修复：cleanup 不再每周期跑（30s/次太频，DELETE 会扫表 200M+ 行），
        # 改为每 cleanup_interval_sec 一次（默认 1800s = 30min）。
        self._last_cleanup_at: float = 0.0

    async def run_once(self) -> dict[str, int | float | str]:
        """CLI one-shot 入口：跑一次完整 basic + orderbook。

        返回值包含 basic 周期统计 + orderbook 周期统计（如果跑了），
        方便 CLI 一次性输出完整结果。
        """
        basic_result = await self._run_basic()
        orderbook_result: dict[str, int | float | str] = {}
        if basic_result.get("ok", 0) > 0:
            orderbook_result = await self._run_orderbook()
        # ⚡ 合并 orderbook 统计(observability)
        return {**basic_result, "orderbook": orderbook_result}

    async def _run_basic(self) -> dict[str, int | float | str]:
        """基础行情周期：拉一次全市场基础字段，写库。

        盘口字段保持 None（占位），由 _run_orderbook 单独补全。
        """
        t0 = time.time()
        cycle_id = self._cycle_count
        instruments = self.pool.instruments
        # ⚡ 用 self.sources[0] 占位(实际源由 fetch_with_fallback 返回),
        # 在 fetch 完后用 storage.update_fetch_run_source 修正(P2-3 优化)。
        run_id = self.storage.start_fetch_run(self.sources[0], len(instruments))

        gap_since_last = (
            t0 - self._last_cycle_at if self._last_cycle_at > 0 else None
        )

        fetch_t0 = time.time()
        try:
            quotes, source_used = await fetch_with_fallback(
                instruments, self.sources,
                concurrency=self.concurrency,
                jitter_ms=self.jitter_ms,
                retry_max=self.retry_max,
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
        # ⚡ 修正 fetch_runs.source 为实际命中源
        if source_used and source_used != self.sources[0]:
            self.storage.update_fetch_run_source(run_id, source_used)

        elapsed = time.time() - t0
        now_ts = time.time()

        ts_vals = [q.timestamp for q in quotes if q.timestamp > 0]
        ts_max = max(ts_vals) if ts_vals else 0.0
        # data_age 可能为负(源时钟比本地快 0-1.5s),abs 后取 max(0, ...) 保证 ≥ 0
        data_age = max(0.0, now_ts - ts_max) if ts_max > 0 else None

        drift_str = ""
        if gap_since_last is not None:
            drift = gap_since_last - self.fetch_interval_sec
            drift_str = f" gap={gap_since_last:.1f}s(Δ{drift:+.1f}s)"
        data_age_str = f" data_age={data_age:.1f}s" if data_age is not None else ""

        # ⚡ 状态行 src= 用实际命中的源(可能 fallback 了)
        status = (
            f"cycle=#{cycle_id} src={source_used or self.sources[0]} "
            f"ok={count_ok}/{len(instruments)} valid={count_valid}({valid_pct:.1f}%) "
            f"stale={count_stale} ob={count_ob} "
            f"fetch={fetch_elapsed:.1f}s db={db_elapsed:.2f}s total={elapsed:.1f}s"
            f"{drift_str}{data_age_str}"
        )
        logger.info("Fetch cycle: %s", status)

        # ⚡ 优化 2026-07-02：SLOW 阈值从 1.5× → 2.5×。
        # 实盘 5h 数据:avg fetch 258s / p95 825s,1.5× 阈值 = 45s,86% 周期都触警 = 全是噪音。
        # 改 2.5× = 75s,只在真正慢的时候报警(<10%)。
        if elapsed > self.fetch_interval_sec * 2.5:
            logger.warning(
                "SLOW cycle #%d: %.1fs (>2.5× interval=%ds). "
                "Possible bottleneck: network/db/source-throttling. "
                "fetch=%.1fs db=%.2fs",
                cycle_id, elapsed, self.fetch_interval_sec,
                fetch_elapsed, db_elapsed,
            )
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
            "source": source_used or self.sources[0],  # ⚡ 实际命中源
            "inserted": inserted,
        }

    async def _run_orderbook(self) -> dict[str, int | float | str]:
        """盘口补全周期：从腾讯拉所有非停牌 codes 的盘口五档，in-place 更新最新 snapshot。

        削减 90%+ 腾讯请求的设计核心：默认每 5min 跑一次（10 个 basic 周期一次），
        相比原来每 30s 一次，腾讯请求量从 ~11000/min 降到 ~1100/min。
        """
        t0 = time.time()
        # 记录尝试时间而不只是成功时间。失败/no-target 时也按正常周期重试，
        # 避免每个 basic tick（30s）持续冲击腾讯接口。
        self._last_orderbook_at = t0
        # 从 DB 拉最新 snapshot，决定要给哪些 code 补盘口
        latest_quotes = self.storage.query_latest()
        targets = [
            q for q in latest_quotes
            if not q.get("is_stale") and q.get("price") is not None
        ]
        if not targets:
            logger.info("Orderbook cycle: no valid quotes to enrich")
            return {"updated": 0, "total": 0, "elapsed": 0.0}

        codes = [q["code"] for q in targets]
        instruments = [self.pool.by_code(c) for c in codes]
        instruments = [i for i in instruments if i is not None]
        if not instruments:
            return {"updated": 0, "total": 0, "elapsed": 0.0}

        logger.info(
            "Orderbook cycle: enriching %d codes from tencent (every %ds)",
            len(instruments), self.orderbook_interval_sec,
        )

        fetch_t0 = time.time()
        try:
            async with TencentFetcher(
                concurrency=self.concurrency,
                jitter_ms=self.jitter_ms,
                retry_max=self.retry_max,
            ) as tencent:
                ob_map = await tencent.fetch_orderbook_batch(instruments)
        except Exception as exc:
            logger.exception("Orderbook fetch crashed")
            return {"updated": 0, "total": len(instruments), "elapsed": time.time() - t0, "error": str(exc)}

        fetch_elapsed = time.time() - fetch_t0

        # 写库：只更新最新 snapshot 的 4 个盘口字段 + orderbook_fetched_at
        db_t0 = time.time()
        orderbook_at = time.time()
        updated = 0
        for code, ob in ob_map.items():
            if self.storage.update_snapshot_orderbook(
                code,
                ob["bid_prices"], ob["bid_vols"],
                ob["ask_prices"], ob["ask_vols"],
                orderbook_at,
            ):
                updated += 1
        db_elapsed = time.time() - db_t0

        elapsed = time.time() - t0
        self._orderbook_count += 1
        self._last_orderbook_at = orderbook_at
        logger.info(
            "Orderbook cycle: updated=%d/%d fetch=%.1fs db=%.2fs total=%.1fs",
            updated, len(instruments), fetch_elapsed, db_elapsed, elapsed,
        )
        return {
            "updated": updated,
            "total": len(instruments),
            "fetch": fetch_elapsed,
            "db": db_elapsed,
            "elapsed": elapsed,
        }

    async def start(self) -> None:
        """启动调度循环。

        两阶段独立节拍：
        - basic phase：每 fetch_interval_sec（默认 30s）
        - orderbook phase：每 orderbook_interval_sec（默认 300s）

        启动时立即 warm-up 一次 basic + 一次 orderbook（无论交易时段）。
        """
        if self._running:
            logger.warning("Scheduler already running")
            return
        self._running = True
        self._start_time = time.time()
        logger.info(
            "Scheduler started: basic_interval=%ds, orderbook_interval=%ds, "
            "sources=%s, pool=%d, fetch_out_of_session=%s",
            self.fetch_interval_sec, self.orderbook_interval_sec,
            self.sources, len(self.pool), self.fetch_out_of_session,
        )
        logger.info(
            "Trading session NOW: %s",
            "ACTIVE" if is_trading_session() else "INACTIVE",
        )
        self._last_session_state = is_trading_session()

        # Warm-up：basic + 一次 orderbook（无论交易时段）
        try:
            await self._run_basic()
        except Exception:
            logger.exception("Warm-up basic fetch failed")
        try:
            await self._run_orderbook()
        except Exception:
            logger.exception("Warm-up orderbook fetch failed")

        # warm-up 已经完成一个完整周期；先等待一个 basic interval，避免启动时双抓。
        await asyncio.sleep(self.fetch_interval_sec)

        # 之后进入双节拍轮询
        while self._running:
            in_session = is_trading_session()
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

            # ---- basic phase ----
            if self.fetch_out_of_session or in_session:
                try:
                    await self._run_basic()
                except Exception:
                    logger.exception("Basic fetch iteration failed")
            else:
                self._idle_heartbeat_remaining -= 1
                if self._idle_heartbeat_remaining <= 0:
                    self._idle_heartbeat_remaining = 20
                    logger.info(
                        "Heartbeat: scheduler idle (outside session) — "
                        "uptime=%.0fs, basic_cycles=%d, orderbook_cycles=%d, last=%s",
                        time.time() - self._start_time,
                        self._cycle_count, self._orderbook_count,
                        self._last_run_status,
                    )
                logger.debug("Outside trading session, skipping basic")

            # ---- orderbook phase（独立节拍）----
            if (self.fetch_out_of_session or in_session) and (
                self._last_orderbook_at == 0.0 or
                time.time() - self._last_orderbook_at >= self.orderbook_interval_sec
            ):
                try:
                    await self._run_orderbook()
                except Exception:
                    logger.exception("Orderbook iteration failed")

            # 清理过期数据 — P2 修复：每 cleanup_interval_sec 跑一次，不再每 30s
            if (
                self._last_cleanup_at == 0.0
                or time.time() - self._last_cleanup_at >= self.cleanup_interval_sec
            ):
                try:
                    self.storage.cleanup_old_snapshots(retention_days=self.retention_days)
                    self._last_cleanup_at = time.time()
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
            "orderbook_interval_sec": self.orderbook_interval_sec,
            "basic_cycles": self._cycle_count,
            "orderbook_cycles": self._orderbook_count,
            "last_orderbook_at": self._last_orderbook_at,
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
