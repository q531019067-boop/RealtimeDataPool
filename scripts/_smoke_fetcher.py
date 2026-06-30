"""临时脚本：手动测试 fetcher。"""
import asyncio
import sys

sys.path.insert(0, "src")
from rdp.instruments import Instrument
from rdp.fetcher import EastmoneyFetcher, SinaFetcher, TencentFetcher


async def test():
    samples = [
        Instrument(code="000001", name="平安银行", market="sz"),
        Instrument(code="600519", name="贵州茅台", market="sh"),
        Instrument(code="300750", name="宁德时代", market="sz"),
        Instrument(code="510300", name="沪深300ETF", market="sh"),
    ]
    for cls in [EastmoneyFetcher, SinaFetcher, TencentFetcher]:
        print(f"\n=== {cls.name} ===")
        async with cls(concurrency=4) as f:
            if hasattr(f, "fetch_batch_with_orderbook"):
                results = await f.fetch_batch_with_orderbook(samples)
            else:
                results = await f.fetch_batch(samples)
            for r in results:
                if r and r.price is not None:
                    bid = r.bid_prices[0]
                    ask = r.ask_prices[0]
                    print(
                        f"  {r.code} {r.name} price={r.price} pct={r.change_pct:.2f}% vol={r.volume} bid={bid} ask={ask}"
                    )
                else:
                    print(f"  {r.code if r else '-'} no data")


asyncio.run(test())