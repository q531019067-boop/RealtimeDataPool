"""全量体检 — 不改任何代码, 只读 db + logs, 列问题清单。"""
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "src")
from rdp.storage import Storage

ROOT = Path(".")
DB = ROOT / "data" / "rdp.db"
LOGS = ROOT / "logs"

s = Storage(DB)
print("=" * 78)
print(" 全量体检 — 收盘后复盘 (15:05)")
print("=" * 78)

# ---- 1) DB 总体 ----
print("\n[1] DB 总体")
print("-" * 78)
with s._connect() as conn:
    rows = conn.execute("SELECT COUNT(*) AS c FROM snapshots").fetchone()
    snap_count = rows["c"]
    rows = conn.execute("SELECT COUNT(*) AS c FROM instruments").fetchone()
    inst_count = rows["c"]
    rows = conn.execute("SELECT COUNT(*) AS c FROM fetch_runs").fetchone()
    runs_count = rows["c"]
    rows = conn.execute("SELECT MIN(fetched_at), MAX(fetched_at) FROM snapshots").fetchone()
    first_at = datetime.fromtimestamp(rows[0]).strftime("%Y-%m-%d %H:%M:%S") if rows[0] else None
    last_at = datetime.fromtimestamp(rows[1]).strftime("%Y-%m-%d %H:%M:%S") if rows[1] else None
    rows = conn.execute("SELECT MIN(started_at), MAX(started_at) FROM fetch_runs").fetchone()
    first_run = datetime.fromtimestamp(rows[0]).strftime("%H:%M:%S") if rows[0] else None
    last_run = datetime.fromtimestamp(rows[1]).strftime("%H:%M:%S") if rows[1] else None
print(f"  snapshots: {snap_count}  ({first_at} → {last_at})")
print(f"  instruments: {inst_count}")
print(f"  fetch_runs: {runs_count}  ({first_run} → {last_run})")
print(f"  db file: {(DB).stat().st_size} bytes")

# ---- 2) fetch_runs 健康度 (按时间顺序) ----
print("\n[2] fetch_runs 健康度 (按时间顺序)")
print("-" * 78)
with s._connect() as conn:
    rows = conn.execute(
        "SELECT id, started_at, ended_at, source, pool_size, "
        "       count_ok, count_valid, count_stale, error "
        "FROM fetch_runs ORDER BY id ASC"
    ).fetchall()
# 检查序号是否连续
ids = [r["id"] for r in rows]
expected = list(range(min(ids), max(ids) + 1)) if ids else []
missing = set(expected) - set(ids)
dup = [i for i, c in Counter(ids).items() if c > 1]
print(f"  序号范围: {min(ids)}-{max(ids)}  实际数: {len(ids)}  期望: {len(expected)}")
if missing:
    print(f"  ❌ 跳号: {sorted(missing)}")
if dup:
    print(f"  ❌ 重复: {dup}")
if not missing and not dup:
    print(f"  ✅ 序号连续无重复")

# source 分布
src_count = Counter(r["source"] for r in rows)
print(f"  source 分布: {dict(src_count)}")
# 跟实际 fetch 命中率
src_valid = defaultdict(lambda: [0, 0])
for r in rows:
    src_valid[r["source"]][0] += 1
    src_valid[r["source"]][1] += r["count_valid"] or 0
for src, (n, v) in src_valid.items():
    print(f"    {src}: {n} runs, total_valid={v}")

# 异常 run
err_runs = [r for r in rows if r["error"]]
if err_runs:
    print(f"  ⚠️ 有 error 的 run: {len(err_runs)}")
    for r in err_runs[:5]:
        print(f"    #{r['id']} {datetime.fromtimestamp(r['started_at']).strftime('%H:%M:%S')} err={r['error']}")
else:
    print(f"  ✅ 无 error run")

# cycle 间隔 (30s 期望)
gaps = []
prev = None
for r in rows:
    if prev is not None:
        gaps.append(r["started_at"] - prev)
    prev = r["started_at"]
if gaps:
    gaps_filt = [g for g in gaps if g < 600]  # 排除跨 session 的 gap
    print(f"  cycle 间隔 (n={len(gaps_filt)}): avg={sum(gaps_filt)/len(gaps_filt):.1f}s  "
          f"min={min(gaps_filt):.1f}  max={max(gaps_filt):.1f}")
    drift = [g - 30 for g in gaps_filt]
    if max(abs(d) for d in drift) > 5:
        print(f"  ⚠️ 间隔漂移 > 5s 出现 {sum(1 for d in drift if abs(d) > 5)} 次")

# ---- 3) WARNING/ERROR 统计 ----
print("\n[3] 日志 WARNING/ERROR 统计")
print("-" * 78)
log_file = LOGS / "smoke_15min.log"
warn_counter = Counter()
err_counter = Counter()
if log_file.exists():
    text = log_file.read_text(encoding="utf-8")
    for line in text.splitlines():
        if "[WARNING]" in line:
            # 抽 message 主体 (去掉时间戳和模块名)
            m = re.search(r"\[WARNING\] \S+:\s*(.*)", line)
            if m:
                # 去掉数字 (ratio / valid 数量) 找 pattern
                msg = re.sub(r"\d+(?:\.\d+)?(?:/\d+)?", "N", m.group(1))
                warn_counter[msg] += 1
        if "[ERROR]" in line:
            m = re.search(r"\[ERROR\] \S+:\s*(.*)", line)
            if m:
                msg = re.sub(r"\d+(?:\.\d+)?(?:/\d+)?", "N", m.group(1))
                err_counter[msg] += 1
print(f"  WARNING 类型 ({len(warn_counter)} 种):")
for msg, n in warn_counter.most_common():
    print(f"    [{n:3d}] {msg[:120]}")
print(f"  ERROR 类型 ({len(err_counter)} 种):")
for msg, n in err_counter.most_common():
    print(f"    [{n:3d}] {msg[:120]}")
print(f"  总 WARNING={sum(warn_counter.values())}  ERROR={sum(err_counter.values())}")

# ---- 4) 28 个标的的样本数 (按 code) ----
print("\n[4] 28 个标的的样本数 (按 code)")
print("-" * 78)
with s._connect() as conn:
    rows = conn.execute(
        "SELECT code, COUNT(*) AS n, "
        "       MIN(fetched_at) AS first_at, MAX(fetched_at) AS last_at "
        "FROM snapshots GROUP BY code ORDER BY code"
    ).fetchall()
sample_n = [r["n"] for r in rows]
print(f"  标的数: {len(rows)}")
print(f"  samples: min={min(sample_n)}  max={max(sample_n)}  "
      f"avg={sum(sample_n)/len(sample_n):.1f}")
low = [r for r in rows if r["n"] < 24]
if low:
    print(f"  ⚠️ 样本数 < 24 的标的 (数据缺失):")
    for r in low:
        first = datetime.fromtimestamp(r["first_at"]).strftime("%H:%M:%S")
        last = datetime.fromtimestamp(r["last_at"]).strftime("%H:%M:%S")
        print(f"    {r['code']} n={r['n']} ({first} → {last})")
else:
    print(f"  ✅ 所有标的 ≥ 24 个 sample")

# ---- 5) 盘口覆盖率 + stale ----
print("\n[5] 盘口覆盖率 + stale")
print("-" * 78)
with s._connect() as conn:
    rows = conn.execute(
        "SELECT "
        "  COUNT(*) AS total, "
        "  SUM(CASE WHEN json_extract(data_json, '$.bid_prices[0]') IS NOT NULL "
        "           AND json_extract(data_json, '$.bid_prices[0]') != 'null' "
        "      THEN 1 ELSE 0 END) AS with_ob, "
        "  SUM(CASE WHEN is_stale = 1 THEN 1 ELSE 0 END) AS stale_n, "
        "  SUM(CASE WHEN json_extract(data_json, '$.price') IS NULL "
        "           OR json_extract(data_json, '$.price') = 'null' "
        "      THEN 1 ELSE 0 END) AS no_price "
        "FROM snapshots"
    ).fetchone()
total = rows["total"] or 0
with_ob = rows["with_ob"] or 0
stale_n = rows["stale_n"] or 0
no_price = rows["no_price"] or 0
print(f"  total: {total}")
print(f"  盘口已补 (bid1 not null): {with_ob} ({with_ob/total*100:.1f}%)")
print(f"  stale: {stale_n} ({stale_n/total*100:.1f}%)")
print(f"  price=null: {no_price} ({no_price/total*100:.1f}%)")
if with_ob / total < 0.6:
    print(f"  ❌ 盘口覆盖率 < 60% — 老 bug: OB 周期只 update 最新行, basic 30s 插新行")
if stale_n > 0:
    print(f"  ⚠️ 有 stale 数据 (停牌/缺失)")

# 按 source 看盘口
print("\n  按 source 看盘口:")
with s._connect() as conn:
    rows = conn.execute(
        "SELECT source, COUNT(*) AS n, "
        "       SUM(CASE WHEN json_extract(data_json, '$.bid_prices[0]') IS NOT NULL "
        "                AND json_extract(data_json, '$.bid_prices[0]') != 'null' "
        "           THEN 1 ELSE 0 END) AS with_ob "
        "FROM snapshots GROUP BY source"
    ).fetchall()
for r in rows:
    ob_rate = (r["with_ob"] or 0) / r["n"] * 100
    print(f"    {r['source']:<10s} n={r['n']:4d}  with_ob={r['with_ob']:4d} ({ob_rate:.1f}%)")

# ---- 6) query_latest 是否返回无盘口的 (用户场景) ----
print("\n[6] /api/snapshot 模拟 — query_latest 是否带盘口")
print("-" * 78)
latest = s.query_latest()
no_ob = [q for q in latest if (q.get("bid_prices") or [None])[0] is None]
print(f"  latest 总数: {len(latest)}")
print(f"  无盘口的: {len(no_ob)} ({len(no_ob)/len(latest)*100:.1f}%)")
if no_ob:
    print(f"  ❌ 用户调 /api/snapshot 有 {len(no_ob)/len(latest)*100:.0f}% 概率拿到 [None, None, ...]")
    print(f"  示例无盘口标的:")
    for q in no_ob[:5]:
        print(f"    {q['code']} {q.get('name', '')[:12]:12s} src={q['source']} "
              f"price={q.get('price')} bid1={(q.get('bid_prices') or [None])[0]}")

# ---- 7) 重复 fetch_runs (我用 print 重复了) ----
print("\n[7] fetch_runs 重复检查")
print("-" * 78)
log_text = log_file.read_text(encoding="utf-8")
# 抓 final_report 之后的重复
final_section = log_text.split("FINAL REPORT")[-1] if "FINAL REPORT" in log_text else ""
lines_with_id = re.findall(r"#\d+ \d\d:\d\d:\d\d src=\S+\s+ok=\d+", final_section)
counts = Counter(lines_with_id)
dups = {k: v for k, v in counts.items() if v > 1}
if dups:
    print(f"  ⚠️ final_report 里 run 打印重复: {len(dups)} 条")
    for k, v in list(dups.items())[:5]:
        print(f"    [{v}x] {k}")
else:
    print(f"  ✅ final_report 打印无重复")

# ---- 8) 收市后 is_stale (15:00 后) ----
print("\n[8] 收市后 stale (15:00 后)")
print("-" * 78)
ts_1500 = datetime(2026, 7, 3, 15, 0, 0).timestamp()
with s._connect() as conn:
    rows = conn.execute(
        "SELECT COUNT(*) AS c FROM snapshots WHERE fetched_at > ?",
        (ts_1500,),
    ).fetchone()
    rows_stale = conn.execute(
        "SELECT COUNT(*) AS c FROM snapshots WHERE fetched_at > ? AND is_stale = 1",
        (ts_1500,),
    ).fetchone()
print(f"  15:00 后 snapshot: {rows['c']}")
print(f"  15:00 后 stale: {rows_stale['c']}")

s.close()
print("\n" + "=" * 78)
print(" 体检完成 — 问题汇总见下一段")
print("=" * 78)
