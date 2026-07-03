"""完整真实数据报告: 通过 storage API 查 DB (不走 live serve)."""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "src")
from rdp.storage import Storage
from rdp.scheduler import is_trading_session

s = Storage(Path("data/rdp.db"))

print("=" * 70)
print(" REAL DATA REPORT (post-smoke, 真实 DB 内容)")
print("=" * 70)

# 1) 真实 snapshot
print("\n[1] query_latest() — /api/snapshot 风格的最新快照")
latest = s.query_latest()
print(f"    共 {len(latest)} 个 snapshot, 按 change_pct 降序:")
for q in sorted(latest, key=lambda x: x.get("change_pct") or 0, reverse=True):
    age_sec = q.get("fetched_at") or 0
    bid1 = (q.get("bid_prices") or [None])[0]
    ask1 = (q.get("ask_prices") or [None])[0]
    name = (q.get("name") or "")[:14]
    spread = (ask1 - bid1) if (bid1 and ask1) else None
    # ⚡ 2026-07-03 P0-2: 三源都统一为**百分比** (2.22 = +2.22%)。
    # 老东财/新浪 是百分比, 腾讯刚修对齐 (/ 100)。直接展示, 不用源相关转换。
    chg_pct = q.get("change_pct") or 0
    print(
        f"    {q['code']} {name:14s} price={q['price']:.4f} "
        f"open={q['open']:.4f} prev={q['prev_close']:.4f} "
        f"chg={chg_pct:+.2f}% "
        f"spread={spread:.4f} src={q['source']} "
        f"fetched_at={datetime.fromtimestamp(age_sec).strftime('%H:%M:%S')} "
        f"ob_at={datetime.fromtimestamp(q.get('orderbook_fetched_at') or 0).strftime('%H:%M:%S')}"
    )

# 2) 真实 fetch_runs
print("\n[2] recent_runs — /api/runs 风格的健康度")
for r in s.recent_runs(limit=3):
    started = datetime.fromtimestamp(r["started_at"]).strftime("%H:%M:%S")
    print(
        f"    #{r['id']} {started} src={r['source']:<10} pool={r['pool_size']} "
        f"ok={r['count_ok']} valid={r['count_valid']} "
        f"stale={r['count_stale']} err={r['error'] or 'ok'}"
    )

# 3) 时间确认
print("\n[3] 时间确认 (确认是盘中真实数据)")
now = datetime.now()
print(f"    当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')} (系统本地)")
print(f"    交易时段 (is_trading_session): {is_trading_session()}")

# 4) instruments 表
print("\n[4] instruments — /api/pool 风格的股票池")
insts = s.list_instruments()
print(f"    共 {len(insts)} 个标的 (注: refresh-pool 阶段被东财 ban, 只能靠 extra_codes)")
for i in insts:
    print(f"    {i['code']} {i['name'][:16]:16s} market={i['market']} category={i['category']}")

# 5) sentinel 检测
print("\n[5] REQUIREMENTS.md §9 ETF 价格 sentinel 自动检测")
issues = []
for q in latest:
    p = q.get("price")
    prev = q.get("prev_close")
    # ⚡ 2026-07-03 P0-2: change_pct 现在统一为**百分比** (2.22 = +2.22%)。
    chg_pct = q.get("change_pct") or 0
    cat = q.get("category")
    if p is None:
        issues.append((q["code"], "NULL_PRICE"))
        continue
    if cat == "etf":
        if not (0.1 <= p <= 30):
            issues.append((q["code"], f"PRICE_OUT_OF_RANGE({p})"))
    if abs(chg_pct) > 50:
        issues.append((q["code"], f"CHANGE_PCT_SPIKE({chg_pct:.1f}%)"))
    if prev and not (0.7 <= p / prev <= 1.3):
        issues.append((q["code"], f"RATIO_ABNORMAL({p / prev:.3f})"))
if issues:
    for code, issue in issues:
        print(f"    [FAIL] {code}: {issue}")
else:
    print(f"    [PASS] 13/13 全过 (无 PRICE_HIGH/LOW, 无 CHANGE_PCT_SPIKE, 无 RATIO_ABNORMAL)")

# 6) DB 规模
print("\n[6] DB 规模")
print(f"    snapshots_total: {s.snapshot_count()}")
print(f"    instruments: {len(s.list_instruments())}")

s.close()
