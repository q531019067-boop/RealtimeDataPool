"""诊断: f170 是什么单位?"""
import asyncio, aiohttp

codes = [
    ("1.510300", "510300 沪深300ETF", "etf"),
    ("1.510500", "510500 中证500ETF", "etf"),
    ("1.600519", "600519 贵州茅台", "stock"),
    ("0.000001", "000001 平安银行", "stock"),
    ("1.518880", "518880 黄金ETF", "etf"),
]


async def main():
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"}
    async with aiohttp.ClientSession() as s:
        for secid, label, cat in codes:
            url = f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f43,f60,f152,f169,f170"
            async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                d = (await r.json(content_type=None)).get("data") or {}
            print(
                f"{label:30s} cat={cat:5s} "
                f"f43={d.get('f43')} f60={d.get('f60')} f152={d.get('f152')} "
                f"f169={d.get('f169')} f170={d.get('f170')}"
            )
            # 算一下: 真实涨跌 vs f170 不同解读
            f43, f60, f152, f169, f170 = d.get("f43"), d.get("f60"), d.get("f152") or 2, d.get("f169"), d.get("f170")
            if all(v is not None for v in [f43, f60, f170]):
                # 假设 price scale=1000 (etf/lof) or 100 (stock)
                price_scale = 1000 if cat in ("etf", "lof") else (10 ** int(f152))
                price = f43 / price_scale
                prev = f60 / price_scale
                real_pct = (price - prev) / prev * 100  # 真实涨跌 %
                # 几种解读 f170
                print(
                    f"  真实涨跌={real_pct:+.4f}%  "
                    f"f170/100={f170 / 100:.4f}  "
                    f"f170/10000={f170 / 10000:.6f}"
                )


asyncio.run(main())
