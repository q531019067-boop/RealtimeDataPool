"""Bug A partial-save 行为验证: 模拟 page 80 之后被 ban, 看是否能保存 8000 条 partial."""
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import aiohttp
from rdp.instruments import InstrumentPool, _EASTMONEY_LIST_URL, _PAGE_SIZE


class _FakeSession:
    """Monkeypatch aiohttp.ClientSession, 让 page >= 80 抛 ServerDisconnectedError."""

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def get(self, url, **kwargs):
        # 从 url 里抓 page=N
        page = 1
        if "pn=" in url:
            page = int(url.split("pn=")[1].split("&")[0])
        return _FakeResp(page)


class _FakeResp:
    def __init__(self, page: int):
        self.page = page

    async def __aenter__(self):
        if self.page >= 80:
            raise aiohttp.ClientConnectionError(
                f"Simulated: page {self.page} disconnected (东财 ban)"
            )
        return self

    async def __aexit__(self, *a):
        return False

    def raise_for_status(self):
        return None

    async def json(self, content_type=None):
        # 返回 100 条假数据
        items = [
            {"f12": f"{self.page:04d}{i:02d}", "f14": f"测试{self.page}_{i}", "f13": 0 if self.page % 2 else 1}
            for i in range(100)
        ]
        return {"data": {"total": 10000, "diff": {str(i): it for i, it in enumerate(items)}}}


async def main():
    print("=" * 60)
    print("Bug A 验证: page 80 之后模拟断连, 期望 partial >= 7900")
    print("=" * 60)
    # 替换 aiohttp.ClientSession
    orig = aiohttp.ClientSession
    aiohttp.ClientSession = _FakeSession
    try:
        pool = await InstrumentPool._fetch_eastmoney()
    finally:
        aiohttp.ClientSession = orig

    print(f"\n[结果] partial pool: {len(pool)} codes")
    print(f"      refreshed_at: {pool.refreshed_at}")
    print(f"      sample: {[i.to_dict() for i in pool.instruments[:3]]}")
    print(f"      sample_last: {[i.to_dict() for i in pool.instruments[-3:]]}")
    if len(pool) >= 7900:
        print(f"\n  ✅ PASS: partial-save 工作正常 (page 79 之前都成功, 保留 {len(pool)} 条)")
        return 0
    else:
        print(f"\n  ❌ FAIL: 预期 >= 7900, 实际 {len(pool)}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
