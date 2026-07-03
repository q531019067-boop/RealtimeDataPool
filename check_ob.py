"""检查盘口覆盖情况."""
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "src")
from rdp.storage import Storage

s = Storage(Path("data/rdp.db"))

# 1) 518880 最新 snapshot
print("===== 518880 最新 snapshot (query_latest) =====")
latest = s.query_latest(["518880"])
d = latest[0]
print(f"  src={d['source']} price={d['price']:.4f} ts={d.get('timestamp')}")
print(f"  bid_prices={d.get('bid_prices')}")
print(f"  ask_prices={d.get('ask_prices')}")
print(f"  bid_vols={d.get('bid_vols')}")
print(f"  ask_vols={d.get('ask_vols')}")
print(f"  orderbook_fetched_at={d.get('orderbook_fetched_at')}")
if d.get("orderbook_fetched_at"):
    ob_at = datetime.fromtimestamp(d["orderbook_fetched_at"]).strftime("%H:%M:%S")
    print(f"  orderbook_fetched_at (formatted) = {ob_at}")
print()

# 2) 同 code 不同 source 的盘口覆盖
print("===== 518880 最近 8 个 snapshot (按 id 倒序) =====")
with s._connect() as conn:
    rows = conn.execute(
        "SELECT id, source, fetched_at, data_json FROM snapshots "
        "WHERE code='518880' ORDER BY id DESC LIMIT 8"
    ).fetchall()
for r in rows:
    d = json.loads(r["data_json"])
    bid1 = (d.get("bid_prices") or [None])[0]
    ask1 = (d.get("ask_prices") or [None])[0]
    ob_at_ts = d.get("orderbook_fetched_at")
    ob_str = datetime.fromtimestamp(ob_at_ts).strftime("%H:%M:%S") if ob_at_ts else "—"
    fetched = datetime.fromtimestamp(r["fetched_at"]).strftime("%H:%M:%S")
    print(
        f"  id={r['id']:3d} src={r['source']:<10s} fetched={fetched} "
        f"price={d.get('price'):.4f} bid1={bid1} ask1={ask1} ob_at={ob_str}"
    )

# 3) 002594 比亚迪
print()
print("===== 002594 比亚迪 最近 5 个 snapshot =====")
with s._connect() as conn:
    rows = conn.execute(
        "SELECT id, source, fetched_at, data_json FROM snapshots "
        "WHERE code='002594' ORDER BY id DESC LIMIT 5"
    ).fetchall()
for r in rows:
    d = json.loads(r["data_json"])
    bid1 = (d.get("bid_prices") or [None])[0]
    ask1 = (d.get("ask_prices") or [None])[0]
    ob_at_ts = d.get("orderbook_fetched_at")
    ob_str = datetime.fromtimestamp(ob_at_ts).strftime("%H:%M:%S") if ob_at_ts else "—"
    fetched = datetime.fromtimestamp(r["fetched_at"]).strftime("%H:%M:%S")
    print(
        f"  id={r['id']:3d} src={r['source']:<10s} fetched={fetched} "
        f"price={d.get('price'):.4f} bid1={bid1} ask1={ask1} ob_at={ob_str}"
    )

# 4) 统计: 盘口有数据的 snapshot 比例
print()
print("===== 盘口覆盖率统计 =====")
with s._connect() as conn:
    row = conn.execute(
        "SELECT "
        "  COUNT(*) AS total, "
        "  SUM(CASE WHEN json_extract(data_json, '$.bid_prices[0]') IS NOT NULL "
        "           AND json_extract(data_json, '$.bid_prices[0]') != 'null' "
        "      THEN 1 ELSE 0 END) AS with_ob "
        "FROM snapshots"
    ).fetchone()
    total = row["total"] or 0
    with_ob = row["with_ob"] or 0
    print(f"  total snapshots: {total}")
    print(f"  with bid1 (盘口已补): {with_ob} ({with_ob / total * 100:.1f}%)")

s.close()
