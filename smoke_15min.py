"""15 分钟真实盘中数据 smoke — 多周期 + 限量。

目标: 15 分钟 = 900s 内, 每 30s 跑一次 basic, 每 5min 跑一次 orderbook。
标的: 13 个 extra ETF (来自 config.yaml) + 6 只 A 股个股。
统计: 每个标的 start/end/max/min/avg 价格, spread 变化, 涨跌, 周期数。

AGENTS.md 硬规则 #1: ≤30s smoke. 用户明确授权 15 分钟, 走限量多周期路径, 不是
`scheduler.start()` 也不是 `serve`, 也不是未限量 `run_once`。
"""
import asyncio
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from rdp.fetcher import EastmoneyFetcher, TencentFetcher, fetch_with_fallback
from rdp.instruments import Instrument, InstrumentPool
from rdp.storage import Storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "logs" / "smoke_15min.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("smoke15")

DURATION_SEC = 15 * 60  # 15 分钟
BASIC_INTERVAL = 30
ORDERBOOK_INTERVAL = 300  # 5 分钟
MID_REPORT_INTERVAL = 300  # 5 分钟一次 mid-report

# 13 ETF/LOF (来自 config.yaml extra_codes) + 6 A 股
TARGET_CODES = [
    # 13 ETF/LOF
    {"code": "510300", "name": "沪深300ETF华泰柏瑞", "market": "sh", "category": "etf"},
    {"code": "510330", "name": "沪深300ETF华夏", "market": "sh", "category": "etf"},
    {"code": "159919", "name": "沪深300ETF嘉实", "market": "sz", "category": "etf"},
    {"code": "510500", "name": "中证500ETF南方", "market": "sh", "category": "etf"},
    {"code": "159922", "name": "中证500ETF嘉实", "market": "sz", "category": "etf"},
    {"code": "159915", "name": "创业板ETF易方达", "market": "sz", "category": "etf"},
    {"code": "588000", "name": "科创50ETF华夏", "market": "sh", "category": "etf"},
    {"code": "588080", "name": "科创50ETF易方达", "market": "sh", "category": "etf"},
    {"code": "510050", "name": "上证50ETF华夏", "market": "sh", "category": "etf"},
    {"code": "518880", "name": "黄金ETF华安", "market": "sh", "category": "etf"},
    {"code": "513100", "name": "纳指ETF国泰", "market": "sh", "category": "etf"},
    {"code": "513500", "name": "标普500ETF博时", "market": "sh", "category": "etf"},
    {"code": "163406", "name": "兴全合润LOF", "market": "sz", "category": "lof"},
    # 6 A 股个股 — 主板/创业板/科创板 各挑 2 只
    {"code": "600519", "name": "贵州茅台", "market": "sh", "category": "stock"},
    {"code": "601318", "name": "中国平安", "market": "sh", "category": "stock"},
    {"code": "000001", "name": "平安银行", "market": "sz", "category": "stock"},
    {"code": "000858", "name": "五粮液", "market": "sz", "category": "stock"},
    {"code": "300750", "name": "宁德时代", "market": "sz", "category": "stock"},
    {"code": "002594", "name": "比亚迪", "market": "sz", "category": "stock"},
    # 9 半导体个股 — 设备/设计/存储/模拟/AI 芯片
    {"code": "002371", "name": "北方华创", "market": "sz", "category": "stock"},
    {"code": "688012", "name": "中微公司", "market": "sh", "category": "stock"},
    {"code": "603501", "name": "韦尔股份", "market": "sh", "category": "stock"},
    {"code": "603986", "name": "兆易创新", "market": "sh", "category": "stock"},
    {"code": "002049", "name": "紫光国微", "market": "sz", "category": "stock"},
    {"code": "600460", "name": "士兰微", "market": "sh", "category": "stock"},
    {"code": "300661", "name": "圣邦股份", "market": "sz", "category": "stock"},
    {"code": "300782", "name": "卓胜微", "market": "sz", "category": "stock"},
    {"code": "688256", "name": "寒武纪", "market": "sh", "category": "stock"},
]


def mid_report(storage: Storage, basic_count: int, ob_count: int) -> None:
    """每 5 分钟输出一次 mid-report: 当前 cycle 数, 每个标的当前价/累计最高最低。"""
    log.info("=" * 70)
    log.info("MID-REPORT  basic_cycles=%d  orderbook_cycles=%d", basic_count, ob_count)
    log.info("=" * 70)
    # 拉每个标的的最新 snapshot
    all_latest = storage.query_latest()
    if not all_latest:
        log.info("  (no data yet)")
        return
    by_code = {q["code"]: q for q in all_latest}
    # 算 15 分钟内每个标的的 max/min
    for tgt in TARGET_CODES:
        code = tgt["code"]
        q = by_code.get(code)
        if not q or q.get("price") is None:
            continue
        # 从 snapshots 表里查 code 的 max/min (本 session 内的)
        # 用 storage 自身的 _connect
        with storage._connect() as conn:
            row = conn.execute(
                "SELECT MIN(json_extract(data_json, '$.price')) AS lo, "
                "       MAX(json_extract(data_json, '$.price')) AS hi, "
                "       COUNT(*) AS n "
                "FROM snapshots WHERE code=? "
                "  AND fetched_at >= ?",
                (code, time.time() - DURATION_SEC - 60),
            ).fetchone()
        if not row or row["n"] is None or row["n"] == 0:
            continue
        cur = q["price"]
        lo = row["lo"] or 0
        hi = row["hi"] or 0
        rng = (hi - lo) / cur * 100 if cur else 0
        chg = (cur - (q.get("prev_close") or cur)) / (q.get("prev_close") or cur) * 100
        bid1 = (q.get("bid_prices") or [None])[0]
        ask1 = (q.get("ask_prices") or [None])[0]
        spread = (ask1 - bid1) if (bid1 and ask1) else None
        log.info(
            "  %s %-18s now=%.4f lo=%.4f hi=%.4f rng=%.2f%% day_chg=%+.2f%% "
            "spread=%.4f samples=%d src=%s",
            code, tgt["name"][:18], cur, lo, hi, rng, chg,
            spread or 0, row["n"], q.get("source"),
        )


def final_report(storage: Storage, basic_count: int, ob_count: int, total_elapsed: float) -> None:
    """最终报告: 15 分钟全程, 每个标的的详细统计。"""
    log.info("=" * 70)
    log.info("FINAL REPORT  basic_cycles=%d  orderbook_cycles=%d  total=%.1fs",
             basic_count, ob_count, total_elapsed)
    log.info("=" * 70)
    all_latest = storage.query_latest()
    by_code = {q["code"]: q for q in all_latest}
    for tgt in TARGET_CODES:
        code = tgt["code"]
        q = by_code.get(code)
        if not q or q.get("price") is None:
            log.info("  %s %-18s NO DATA", code, tgt["name"])
            continue
        with storage._connect() as conn:
            row = conn.execute(
                "SELECT MIN(json_extract(data_json, '$.price')) AS lo, "
                "       MAX(json_extract(data_json, '$.price')) AS hi, "
                "       AVG(json_extract(data_json, '$.price')) AS avg, "
                "       COUNT(*) AS n, "
                "       MIN(fetched_at) AS first_at, "
                "       MAX(fetched_at) AS last_at "
                "FROM snapshots WHERE code=? "
                "  AND fetched_at >= ?",
                (code, time.time() - DURATION_SEC - 60),
            ).fetchone()
        cur = q["price"]
        lo = row["lo"] or 0
        hi = row["hi"] or 0
        avg = row["avg"] or 0
        rng = (hi - lo) / cur * 100 if cur else 0
        chg_day = (cur - (q.get("prev_close") or cur)) / (q.get("prev_close") or cur) * 100
        chg_15m = (cur - lo) / lo * 100 if lo else 0  # 15 分钟内从最低到当前的 %
        bid1 = (q.get("bid_prices") or [None])[0]
        ask1 = (q.get("ask_prices") or [None])[0]
        spread = (ask1 - bid1) if (bid1 and ask1) else None
        first_at = datetime.fromtimestamp(row["first_at"]).strftime("%H:%M:%S")
        last_at = datetime.fromtimestamp(row["last_at"]).strftime("%H:%M:%S")
        log.info(
            "  %s %-18s cur=%.4f day_chg=%+.2f%% 15m_range=[%.4f, %.4f] (Δ%.2f%%) "
            "avg=%.4f samples=%d spread=%.4f src=%s [%s→%s]",
            code, tgt["name"][:18], cur, chg_day, lo, hi, rng, avg,
            row["n"], spread or 0, q.get("source"), first_at, last_at,
        )

    # fetch_runs 健康度
    log.info("")
    log.info("--- fetch_runs 健康度 (最近 10 条) ---")
    for r in storage.recent_runs(limit=10):
        started = datetime.fromtimestamp(r["started_at"]).strftime("%H:%M:%S")
        log.info(
            "  #%d %s src=%-10s ok=%d valid=%d stale=%d err=%s",
            r["id"], started, r["source"], r["count_ok"], r["count_valid"],
            r["count_stale"], r["error"] or "ok",
        )


async def main() -> int:
    t_start = time.time()
    db_path = ROOT / "data" / "rdp.db"
    storage = Storage(db_path)
    storage.init_schema()

    # 构造 instrument pool (不依赖 cache, 直接用 TARGET_CODES)
    instruments = [Instrument.from_dict(t) for t in TARGET_CODES]
    pool = InstrumentPool(instruments=instruments, refreshed_at=time.time())
    storage.upsert_instruments(pool.instruments)

    log.info("=" * 70)
    log.info("15min SMOKE START  target=%d  basic_interval=%ds  ob_interval=%ds",
             len(TARGET_CODES), BASIC_INTERVAL, ORDERBOOK_INTERVAL)
    log.info("  codes: %s", ", ".join(t["code"] for t in TARGET_CODES))
    log.info("=" * 70)

    basic_count = 0
    ob_count = 0
    last_ob_at = 0.0
    last_mid_at = 0.0

    while time.time() - t_start < DURATION_SEC:
        # ---- basic 周期 ----
        cycle_t0 = time.time()
        run_id = storage.start_fetch_run("eastmoney", len(instruments))
        try:
            quotes, source_used = await fetch_with_fallback(
                instruments, ["eastmoney", "sina", "tencent"],
                concurrency=8, jitter_ms=30, retry_max=1,
            )
        except Exception as exc:
            log.exception("basic cycle crashed")
            storage.finish_fetch_run(run_id, 0, 0, 0, error=str(exc))
            continue

        n_ok = len(quotes)
        n_valid = sum(1 for q in quotes if q.price is not None)
        n_stale = sum(1 for q in quotes if q.is_stale)
        storage.finish_fetch_run(run_id, n_ok, n_valid, n_stale)
        if source_used and source_used != "eastmoney":
            storage.update_fetch_run_source(run_id, source_used)
        storage.insert_snapshots(quotes)
        basic_count += 1
        log.info(
            "Cycle %2d [t=%.0fs] basic: src=%s ok=%d valid=%d stale=%d elapsed=%.1fs",
            basic_count, time.time() - t_start, source_used or "eastmoney",
            n_ok, n_valid, n_stale, time.time() - cycle_t0,
        )

        # ---- orderbook 周期 (5 分钟一次) ----
        if time.time() - last_ob_at >= ORDERBOOK_INTERVAL:
            ob_t0 = time.time()
            ob_targets = [q for q in quotes if q.price is not None and not q.is_stale]
            ob_targets = [pool.by_code(q.code) for q in ob_targets if pool.by_code(q.code)]
            if ob_targets:
                async with TencentFetcher(concurrency=4, jitter_ms=30, retry_max=1) as tf:
                    ob_map = await tf.fetch_orderbook_batch(ob_targets)
                ob_updated = 0
                now = time.time()
                for inst in ob_targets:
                    ob = ob_map.get(inst.code)
                    if ob and storage.update_snapshot_orderbook(
                        inst.code, ob["bid_prices"], ob["bid_vols"],
                        ob["ask_prices"], ob["ask_vols"], now,
                    ):
                        ob_updated += 1
                ob_count += 1
                last_ob_at = time.time()
                log.info(
                    "Cycle %2d [t=%.0fs] orderbook: %d/%d updated in %.1fs",
                    ob_count, time.time() - t_start, ob_updated, len(ob_targets),
                    time.time() - ob_t0,
                )

        # ---- mid-report (5 分钟一次) ----
        if time.time() - last_mid_at >= MID_REPORT_INTERVAL:
            mid_report(storage, basic_count, ob_count)
            last_mid_at = time.time()

        # sleep 到下一周期
        await asyncio.sleep(BASIC_INTERVAL)

    # ---- final report ----
    final_report(storage, basic_count, ob_count, time.time() - t_start)
    storage.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
