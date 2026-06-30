"""测试东方财富不同接口找盘口。"""
import asyncio
import aiohttp


async def debug():
    secid = "0.000001"
    # 测试不同 URL
    urls = [
        f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f19,f20,f21,f22,f23,f24,f25,f26,f27,f28&invt=2",
        f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f19,f20,f21,f22,f23,f24,f25,f26,f27,f28&fltt=2",
        # 也许是 fields 顺序问题，要按数字升序
        f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f1,f2,f3,f4,f5,f19,f20,f21,f22,f23,f24,f25,f26,f27,f28,f29,f30",
        # 尝试科创板/创业板格式
        f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f24,f25,f26,f27,f28,f29,f30",
    ]
    for url in urls:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"}) as r:
                payload = await r.json(content_type=None)
        data = payload.get("data", {})
        # 看是否有任何非空值
        non_empty = {k: v for k, v in data.items() if v not in (None, "", 0, 0.0)}
        print(f"URL: {url[60:]}")
        print(f"  Non-empty: {len(non_empty)} keys")
        if non_empty:
            for k in sorted(non_empty.keys(), key=lambda x: int(x[1:])):
                print(f"    {k}={non_empty[k]}")
        print()


asyncio.run(debug())