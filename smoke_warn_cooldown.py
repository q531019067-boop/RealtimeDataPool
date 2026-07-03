"""3 分钟 mini smoke, 专门验证 WARNING 60s 静默窗口。

预期: 6 个 basic 周期, 东财持续 DEGRADED, 改前会有 6 个 WARNING,
改后只有 1 个 WARNING (后 5 个被静默), 状态切换 (recovered) 仍 INFO。
"""
import asyncio
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "logs" / "smoke_warn.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("warn_test")

from rdp.fetcher import _DEGRADE_WARN_LAST_AT, _DEGRADE_WARN_COOLDOWN, fetch_with_fallback
from rdp.instruments import Instrument

TARGETS = [
    Instrument(code="510300", name="沪深300ETF", market="sh", category="etf"),
    Instrument(code="518880", name="黄金ETF", market="sh", category="etf"),
    Instrument(code="159915", name="创业板ETF", market="sz", category="etf"),
]


async def main():
    # 重置模块级静默缓存
    _DEGRADE_WARN_LAST_AT.clear()
    log.info("=" * 60)
    log.info("WARN COOLDOWN TEST: 6 cycles × 30s = 180s")
    log.info("  cooldown=%ds  expected: 1 WARNING + 5 DEBUG suppressions",
             _DEGRADE_WARN_COOLDOWN)
    log.info("=" * 60)
    for i in range(1, 7):
        quotes, source = await fetch_with_fallback(
            TARGETS, ["eastmoney", "sina", "tencent"],
            concurrency=4, jitter_ms=30, retry_max=1,
        )
        n_valid = sum(1 for q in quotes if q.price is not None)
        log.info("Cycle %d done: source=%s, valid=%d/3", i, source or "none", n_valid)
        if i < 6:
            await asyncio.sleep(30)
    log.info("=" * 60)
    log.info("Test complete — 检查 smoke_warn.log 看 WARNING 数量")


if __name__ == "__main__":
    asyncio.run(main())
