"""快速 smoke test 验证新的 scheduler 日志格式。
仅用 50 只样本，2 个周期，覆盖：
- cycle_id
- 分阶段耗时 (fetch / db / total)
- drift (gap since last cycle)
- data_age
- 慢周期 WARNING（如果触发）
- 源降级 WARNING（如果触发）
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, r"C:\Users\Administrator\Downloads\GitHub\RealtimeDataPool\src")

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)

from rdp.scheduler import Scheduler, is_trading_session
from rdp.instruments import Instrument
from rdp.storage import Storage


class _MiniPool:
    def __init__(self, insts):
        self.instruments = insts

    def __len__(self):
        return len(self.instruments)


async def main():
    storage = Storage(Path(r"C:\Users\Administrator\Downloads\GitHub\RealtimeDataPool\data\rdp.db"))
    storage.init_schema()

    # 50 只去重样本
    raw = [
        ("600000", "sh"), ("600519", "sh"), ("600036", "sh"), ("600276", "sh"),
        ("601318", "sh"), ("000001", "sz"), ("000002", "sz"), ("000333", "sz"),
        ("000651", "sz"), ("000858", "sz"), ("600030", "sh"), ("600887", "sh"),
        ("601166", "sh"), ("601398", "sh"), ("601988", "sh"), ("000063", "sz"),
        ("000568", "sz"), ("000725", "sz"), ("000776", "sz"), ("000792", "sz"),
        ("600009", "sh"), ("600010", "sh"), ("600011", "sh"), ("600015", "sh"),
        ("600016", "sh"), ("600018", "sh"), ("600019", "sh"), ("600025", "sh"),
        ("600028", "sh"), ("600031", "sh"), ("600048", "sh"), ("600050", "sh"),
        ("600061", "sh"), ("600085", "sh"), ("600089", "sh"), ("600104", "sh"),
        ("600111", "sh"), ("600150", "sh"), ("600188", "sh"), ("600196", "sh"),
        ("600297", "sh"), ("600309", "sh"), ("600340", "sh"), ("600346", "sh"),
        ("600362", "sh"), ("600383", "sh"), ("600406", "sh"), ("601012", "sh"),
        ("601628", "sh"), ("600030", "sh"),  # dup
    ]
    seen = set()
    insts = []
    for code, mkt in raw:
        if code in seen:
            continue
        seen.add(code)
        insts.append(Instrument(code=code, name=f"T{code}", market=mkt, category="stock"))

    print(f"=== Test pool size: {len(insts)}")
    print(f"=== Session: {'ACTIVE' if is_trading_session() else 'INACTIVE'}")
    print()

    pool = _MiniPool(insts)
    sched = Scheduler(
        pool=pool,  # type: ignore[arg-type]
        storage=storage,
        fetch_interval_sec=30,
        sources=["eastmoney"],
    )

    for i in range(2):
        print(f"--- cycle {i+1} ---")
        r = await sched.run_once()
        print(f"   result ok={r.get('ok')} valid={r.get('valid')} elapsed={r.get('elapsed', 0):.1f}s")
        if i == 0:
            await asyncio.sleep(3)  # 触发 drift log

    print("=== DONE")
    storage.close()


if __name__ == "__main__":
    asyncio.run(main())
