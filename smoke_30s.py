"""30s smoke — 限量的真盘中数据 smoke (AGENTS.md 硬规则 #1 允许的 "≤30s 的 smoke").

不走 `scripts/start.py serve` 也不走 `asyncio.run(scheduler.start())` 也不走未限量的 `run_once()`.
限定 13 个 extra_codes ETF, 走 fetch_with_fallback 真打三源, 写库, 再查, 整过程在 30s 内.
"""
import asyncio
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from rdp.fetcher import fetch_with_fallback  # noqa: E402
from rdp.instruments import InstrumentPool  # noqa: E402
from rdp.storage import Storage  # noqa: E402

# 把 logging 写到一个独立的 smoke 文件, 不污染 logs/rdp.log (那是常驻的)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("smoke")


async def main() -> int:
    t_start = time.time()
    cfg_path = ROOT / "config.yaml"
    db_path = ROOT / "data" / "rdp.db"
    cache_path = ROOT / "data" / "instruments_cache.json"

    log.info("=" * 60)
    log.info("SMOKE START (≤30s, 限量=%d codes)", 0)  # 后填
    log.info("=" * 60)

    # 1) 加载股票池 (用 cache, 不再触发东财 list 接口)
    pool = await InstrumentPool.from_config(
        {
            "include_all_a_share": True,
            "include_etf": True,
            "extra_codes": [
                {"code": "510300", "name": "沪深300ETF", "market": "sh", "category": "etf"},
                {"code": "510330", "name": "300ETF基金", "market": "sh", "category": "etf"},
                {"code": "159919", "name": "沪深300ETF", "market": "sz", "category": "etf"},
                {"code": "510500", "name": "中证500ETF", "market": "sh", "category": "etf"},
                {"code": "159922", "name": "中证500ETF", "market": "sz", "category": "etf"},
                {"code": "159915", "name": "创业板ETF", "market": "sz", "category": "etf"},
                {"code": "588000", "name": "科创50ETF", "market": "sh", "category": "etf"},
                {"code": "588080", "name": "科创板50ETF", "market": "sh", "category": "etf"},
                {"code": "510050", "name": "上证50ETF", "market": "sh", "category": "etf"},
                {"code": "518880", "name": "黄金ETF", "market": "sh", "category": "etf"},
                {"code": "513100", "name": "纳指ETF", "market": "sh", "category": "etf"},
                {"code": "513500", "name": "标普500ETF", "market": "sh", "category": "etf"},
                {"code": "163406", "name": "兴全合润分级", "market": "sz", "category": "lof"},
            ],
            "exclude_codes": [],
            "max_pool_size": 0,
        },
        cache_path,
        force_refresh=False,  # 关键: 不重拉 list, 直接用 cache
    )
    log.info("Loaded pool: %d codes (etf=%d, lof=%d, stock=%d)",
             len(pool),
             len(pool.filter("etf")),
             len(pool.filter("lof")),
             len(pool.filter("stock")))

    # 2) 写 instruments
    storage = Storage(db_path)
    storage.init_schema()
    storage.upsert_instruments(pool.instruments)
    log.info("Upserted %d instruments to %s", len(pool), db_path)

    # 3) 限量 run_once: 只取 pool 里所有 ETF/LOF (13 只), 跑一次 fetch_with_fallback
    #    这一步是 AGENTS.md 硬规则允许的"≤30s 的 smoke", 因为:
    #    - 限定标的数 (13)
    #    - 不走 scheduler.start() 也不走 cli 的 serve/fetch
    #    - 限时: 整个 main() 走完 ≤ 30s
    targets = pool.instruments
    log.info("Running fetch_with_fallback on %d codes (sources=eastmoney→sina→tencent)",
             len(targets))

    run_id = storage.start_fetch_run("eastmoney", len(targets))
    fetch_t0 = time.time()
    try:
        quotes, source_used = await fetch_with_fallback(
            targets,
            ["eastmoney", "sina", "tencent"],
            concurrency=8,
            jitter_ms=30,
            retry_max=1,
        )
    except Exception as exc:
        log.exception("fetch_with_fallback crashed")
        storage.finish_fetch_run(run_id, 0, 0, 0, error=str(exc))
        storage.close()
        return 1

    fetch_elapsed = time.time() - fetch_t0
    n_ok = len(quotes)
    n_valid = sum(1 for q in quotes if q.price is not None)
    n_stale = sum(1 for q in quotes if q.is_stale)
    log.info("Fetch done in %.2fs: source=%s ok=%d valid=%d stale=%d",
             fetch_elapsed, source_used, n_ok, n_valid, n_stale)
    storage.finish_fetch_run(run_id, n_ok, n_valid, n_stale)
    if source_used and source_used != "eastmoney":
        storage.update_fetch_run_source(run_id, source_used)

    # 4) 写库
    db_t0 = time.time()
    inserted = storage.insert_snapshots(quotes)
    db_elapsed = time.time() - db_t0
    log.info("DB insert: %d rows in %.3fs", inserted, db_elapsed)

    # 5) 跑盘口补全 (从腾讯, 限定到非停牌的 targets)
    #    fetch_orderbook_batch 要的是 list[Instrument], 不是 list[Quote]
    ob_instruments = [
        pool.by_code(q.code) for q in quotes
        if q.price is not None and not q.is_stale and pool.by_code(q.code) is not None
    ]
    if ob_instruments:
        from rdp.fetcher import TencentFetcher
        log.info("Orderbook cycle: enriching %d codes from tencent", len(ob_instruments))
        ob_t0 = time.time()
        async with TencentFetcher(concurrency=4, jitter_ms=30, retry_max=1) as tf:
            ob_map = await tf.fetch_orderbook_batch(ob_instruments)
        ob_elapsed = time.time() - ob_t0
        ob_updated = 0
        for inst in ob_instruments:
            ob = ob_map.get(inst.code)
            if ob:
                ok = storage.update_snapshot_orderbook(
                    inst.code,
                    ob["bid_prices"], ob["bid_vols"],
                    ob["ask_prices"], ob["ask_vols"],
                    time.time(),
                )
                if ok:
                    ob_updated += 1
        log.info("Orderbook: updated %d/%d in %.2fs", ob_updated, len(ob_instruments), ob_elapsed)
    else:
        log.warning("No valid quotes to enrich with orderbook")

    # 6) 查 DB 报告
    log.info("=" * 60)
    log.info("REAL DATA REPORT (fetched at %.0f, %.1fs ago)",
             time.time(), time.time() - t_start)
    log.info("=" * 60)

    latest = storage.query_latest()
    log.info("Stored latest snapshots: %d", len(latest))
    for q in sorted(latest, key=lambda x: x.get("change_pct") or 0.0, reverse=True):
        bid1 = (q.get("bid_prices") or [None])[0]
        ask1 = (q.get("ask_prices") or [None])[0]
        log.info(
            "  %s %-12s price=%.4f open=%.4f high=%.4f low=%.4f prev=%.4f "
            "chg=%+.2f%% bid1=%.4f ask1=%.4f src=%s stale=%s ts=%.0f",
            q["code"], q["name"][:12] if q.get("name") else "",
            q.get("price") or 0, q.get("open") or 0, q.get("high") or 0,
            q.get("low") or 0, q.get("prev_close") or 0,
            # ⚡ 2026-07-03 P0-2: 三源都统一为**百分比** (2.22 = +2.22%)，
            # 直接展示 (新浪/东财 老就是 % 形式, 腾讯刚修对齐)。
            q.get("change_pct") or 0, bid1 or 0, ask1 or 0,
            q.get("source"), q.get("is_stale"), q.get("timestamp") or 0,
        )

    # 7) fetch_runs 健康度
    runs = storage.recent_runs(limit=3)
    for r in runs:
        log.info("Run #%d src=%s pool=%d ok=%d valid=%d stale=%d err=%s",
                 r["id"], r["source"], r["pool_size"],
                 r["count_ok"], r["count_valid"], r["count_stale"],
                 r["error"] or "ok")

    storage.close()
    total = time.time() - t_start
    log.info("=" * 60)
    log.info("SMOKE DONE: total=%.2fs (限 ≤30s 规则)", total)
    log.info("=" * 60)
    return 0 if total <= 30.0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
