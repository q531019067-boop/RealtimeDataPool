"""验证 query_latest 优先返回有盘口的最新行."""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "src")
from rdp.storage import Storage

s = Storage(Path("data/rdp.db"))
latest = s.query_latest()
print(f"query_latest returns: {len(latest)} codes")
print()
print("--- 验证: 全部 28 个标的 ---")
no_ob = 0
with_ob = 0
for q in latest:
    bid1 = (q.get("bid_prices") or [None])[0]
    ask1 = (q.get("ask_prices") or [None])[0]
    if bid1 is None:
        no_ob += 1
    else:
        with_ob += 1
    ob_at_ts = q.get("orderbook_fetched_at")
    ob_at = (
        datetime.fromtimestamp(ob_at_ts).strftime("%H:%M:%S") if ob_at_ts else "—"
    )
    print(
        f"  {q['code']} {q.get('name', '')[:14]:14s} "
        f"price={q.get('price') or 0:.4f} "
        f"bid1={bid1} ask1={ask1} ob_at={ob_at} src={q.get('source')}"
    )
print()
print(f"  统计: 有盘口 {with_ob}/{len(latest)}, 无盘口 {no_ob}/{len(latest)}")
s.close()
