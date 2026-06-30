"""调试腾讯/东方财富接口字段位置。"""
import asyncio
import aiohttp


async def debug_tencent():
    sym = "sz000001"
    url = f"https://qt.gtimg.cn/q={sym}"
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            text = await r.text(encoding="gbk")
    print("Raw:", text)
    print()
    fields = text.split("=")[1].strip().strip(";").strip('"').split("~")
    print(f"Total fields: {len(fields)}")
    for i, f in enumerate(fields[:55]):
        print(f"  [{i}]={f!r}")


asyncio.run(debug_tencent())