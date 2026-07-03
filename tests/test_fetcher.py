"""Fetcher 解析逻辑测试（不发起真实请求）。"""

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from rdp.fetcher import (
    EastmoneyFetcher,
    Quote,
    _AdaptiveLimiter,
    _parse_sina,
    _parse_tencent,
    fetch_with_fallback,
)
from rdp.instruments import Instrument


class TestSinaParser:
    def test_basic(self):
        inst = Instrument(code="000001", name="x", market="sz")
        # 至少 10 个字段
        fields = ["平安银行", "10.50", "10.00", "10.55", "10.80", "10.30", "10.54", "10.56", "100000", "1055000"]
        fields += [""] * 30  # 凑到 40 个
        q = _parse_sina(inst, fields)
        assert q is not None
        assert q.code == "000001"
        assert q.price == pytest.approx(10.55)
        assert q.prev_close == pytest.approx(10.00)
        assert q.change == pytest.approx(0.55)
        assert q.change_pct == pytest.approx(5.5, abs=1e-6)
        assert q.bid_prices[0] == pytest.approx(10.54)
        assert q.ask_prices[0] == pytest.approx(10.56)
        # 成交量是股 → 转手
        assert q.volume == pytest.approx(1000.0)

    def test_too_short(self):
        inst = Instrument(code="x", name="x", market="sz")
        assert _parse_sina(inst, ["only", "3", "fields"]) is None


class TestTencentParser:
    def test_basic(self):
        inst = Instrument(code="600000", name="x", market="sh")
        # 至少 50 字段
        fields = [""] * 50
        fields[1] = "浦发银行"
        fields[3] = "10.50"  # 当前价
        fields[4] = "10.00"  # 昨收
        fields[5] = "10.20"  # 今开
        fields[6] = "12345"  # 成交量
        fields[9] = "10.49"  # 买一价
        fields[10] = "100"   # 买一量
        fields[19] = "10.51" # 卖一价
        fields[20] = "200"   # 卖一量
        fields[30] = "20260703145959"  # 行情时间（北京时间）
        fields[31] = "0.50"   # 涨跌额
        fields[32] = "5.00"   # 涨跌幅（已经是百分比）
        fields[33] = "10.80"  # 最高
        fields[34] = "10.30"  # 最低
        fields[37] = "105.5"  # 成交额（万元）
        fields[38] = "1.25"   # 换手率（已经是百分比）
        fields[44] = "300.0"  # 流通市值（亿元）
        fields[45] = "400.0"  # 总市值（亿元）
        q = _parse_tencent(inst, fields)
        assert q is not None
        assert q.name == "浦发银行"
        assert q.price == pytest.approx(10.50)
        assert q.change_pct == pytest.approx(5.0)
        assert q.bid_prices[0] == pytest.approx(10.49)
        assert q.ask_prices[0] == pytest.approx(10.51)
        assert q.amount == pytest.approx(1_055_000)
        assert q.turnover_pct == pytest.approx(1.25)
        assert q.float_cap == pytest.approx(30_000_000_000)
        assert q.market_cap == pytest.approx(40_000_000_000)
        assert q.timestamp == datetime(
            2026, 7, 3, 14, 59, 59, tzinfo=ZoneInfo("Asia/Shanghai")
        ).timestamp()
        assert q.orderbook_fetched_at == q.fetched_at

    def test_too_short(self):
        inst = Instrument(code="x", name="x", market="sz")
        assert _parse_tencent(inst, ["x"] * 10) is None


# ===== P0-3 修复测试：ETF/LOF 用 scale=1000，东财 f152 不可靠 =====
class TestEastmoneyPriceScaling:
    """P0-3 修复：东财对 ETF/LOF 的 f152 字段返回 2 但实际是 3 位小数，
    导致 510300/510500/159915 等 ETF 的 price/open/high/low/prev_close 10x 偏大。
    修法：scale 用 category 强制覆盖，etf/lof → 1000，stock → f152 或默认 2。
    """

    def _fetch_with(self, inst, data: dict):
        """直接调 EastmoneyFetcher.fetch 但 monkeypatch 网络层只跑解析逻辑。
        为避免真实网络，构造一个 EastmoneyFetcher 实例并 stub 掉 _polite_get。
        """
        from rdp.fetcher import EastmoneyFetcher

        class StubSession:
            def __init__(self, payload):
                self.payload = payload

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

        class StubResp:
            def __init__(self, payload):
                self.payload = payload
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                return False
            def raise_for_status(self):
                return None
            async def json(self, content_type=None):
                return self.payload

        f = EastmoneyFetcher(concurrency=1)
        # 手动标记 started,塞 stub session
        f._session = StubSession(StubResp({"data": data}))
        # ⚡ 2026-07-02:_sem 改名为 _limiter,用 _AdaptiveLimiter
        from rdp.fetcher import _AdaptiveLimiter
        f._limiter = _AdaptiveLimiter(1)

        async def stub_get(url, **kw):
            return StubResp({"data": data})

        # 直接调 fetch（覆盖 _polite_get）
        import asyncio as _aio

        async def run():
            orig_polite = f._polite_get

            async def fake_polite(url, **kw):
                return StubResp({"data": data})

            f._polite_get = fake_polite
            try:
                return await f.fetch(inst)
            finally:
                f._polite_get = orig_polite

        return _aio.run(run())

    def test_stock_f152_2(self):
        """股票 + f152=2 → scale=100,price 正常"""
        inst = Instrument(code="000001", name="Pingan", market="sz", category="stock")
        q = self._fetch_with(
            inst,
            {"f43": 1023, "f152": 2, "f86": 1700000000, "f60": 1000, "f58": "平安银行"},
        )
        assert q is not None
        assert q.price == pytest.approx(10.23)
        assert q.prev_close == pytest.approx(10.00)

    def test_etf_f152_2_uses_1000(self):
        """⚠️ 关键测试：ETF + f152=2（东财不可靠值）→ scale 强制 1000,price 正确
        510300 真价 ~4.91 元,东财 f43=4912 f152=2。
        修复前: 4912/100 = 49.12 ❌
        修复后: 4912/1000 = 4.912 ✅
        """
        inst = Instrument(code="510300", name="300ETF", market="sh", category="etf")
        q = self._fetch_with(
            inst,
            {"f43": 4912, "f152": 2, "f86": 1700000000, "f60": 4998, "f58": "沪深300ETF"},
        )
        assert q is not None
        assert q.price == pytest.approx(4.912), f"expected 4.912 got {q.price}"
        assert q.prev_close == pytest.approx(4.998), f"expected 4.998 got {q.prev_close}"
        # 关键:不能是 49.12
        assert q.price < 10, f"ETF price should be < 10, got {q.price} (regression?)"

    def test_lof_f152_2_uses_1000(self):
        """LOF 也走 scale=1000"""
        inst = Instrument(code="163406", name="LOF", market="sz", category="lof")
        q = self._fetch_with(
            inst,
            {"f43": 1523, "f152": 2, "f86": 1700000000, "f60": 1500, "f58": "兴全合润"},
        )
        assert q is not None
        assert q.price == pytest.approx(1.523)
        assert q.price < 10

    def test_etf_f152_3_uses_1000(self):
        """ETF + f152=3 → scale=1000（同样正确）"""
        inst = Instrument(code="510300", name="300ETF", market="sh", category="etf")
        q = self._fetch_with(
            inst,
            {"f43": 4912, "f152": 3, "f86": 1700000000, "f60": 4998, "f58": "沪深300ETF"},
        )
        assert q is not None
        assert q.price == pytest.approx(4.912)

    def test_stock_f152_3_uses_1000(self):
        """股票 + f152=3（高价股,如某些港股通）→ scale=1000"""
        inst = Instrument(code="600519", name="Maotai", market="sh", category="stock")
        q = self._fetch_with(
            inst,
            {"f43": 119613, "f152": 3, "f86": 1700000000, "f60": 119301, "f58": "贵州茅台"},
        )
        assert q is not None
        assert q.price == pytest.approx(119.613)

    def test_change_etf_normalized(self):
        """ETF 涨跌额/change 也要按 scale 1000 算"""
        inst = Instrument(code="510300", name="300ETF", market="sh", category="etf")
        q = self._fetch_with(
            inst,
            {"f43": 4912, "f152": 2, "f86": 1700000000, "f60": 4998,
             "f169": -86, "f170": -1.72, "f58": "300ETF"},
        )
        assert q is not None
        # f169=-86 raw, scale=1000 → -0.086
        assert q.change == pytest.approx(-0.086)
        # f170=-1.72 % already in percent
        assert q.change_pct == pytest.approx(-0.0172)


# ===== fetch_with_fallback 新签名测试 =====
class TestFetchWithFallbackSignature:
    """⚡ 优化：fetch_with_fallback 现在返回 (results, source_used) 让
    Scheduler 知道实际命中了哪个源(可能 fallback 到 sina/tencent)。"""

    def test_signature_returns_tuple(self):
        """签名应该是 (results, source_used) 不是 list[Quote]"""
        import inspect

        from rdp.fetcher import fetch_with_fallback
        sig = inspect.signature(fetch_with_fallback)
        ret = sig.return_annotation
        # 现在是 tuple[list[Quote], str | None]
        assert "tuple" in str(ret).lower() or "Tuple" in str(ret)


class TestFetchWithFallbackCoverage:
    @pytest.mark.asyncio
    async def test_partial_primary_falls_back_using_pool_coverage(self, monkeypatch):
        instruments = [
            Instrument(code=f"{i:06d}", name=str(i), market="sz") for i in range(10)
        ]

        class PartialFetcher:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def fetch_batch(self, requested):
                return [Quote(code=requested[0].code, price=1.0)]

        class HealthyFetcher(PartialFetcher):
            async def fetch_batch(self, requested):
                return [Quote(code=i.code, price=1.0) for i in requested]

        monkeypatch.setitem(__import__("rdp.fetcher", fromlist=["FETCHER_REGISTRY"]).FETCHER_REGISTRY,
                            "partial-test", PartialFetcher)
        monkeypatch.setitem(__import__("rdp.fetcher", fromlist=["FETCHER_REGISTRY"]).FETCHER_REGISTRY,
                            "healthy-test", HealthyFetcher)

        quotes, source = await fetch_with_fallback(
            instruments, ["partial-test", "healthy-test"], jitter_ms=0
        )
        assert source == "healthy-test"
        assert len(quotes) == len(instruments)

    @pytest.mark.asyncio
    async def test_returns_best_source_when_all_are_degraded(self, monkeypatch):
        instruments = [
            Instrument(code=f"{i:06d}", name=str(i), market="sz") for i in range(10)
        ]

        def fetcher_with_count(count):
            class StubFetcher:
                def __init__(self, **kwargs):
                    pass

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    return None

                async def fetch_batch(self, requested):
                    return [Quote(code=i.code, price=1.0) for i in requested[:count]]

            return StubFetcher

        registry = __import__("rdp.fetcher", fromlist=["FETCHER_REGISTRY"]).FETCHER_REGISTRY
        monkeypatch.setitem(registry, "better-test", fetcher_with_count(6))
        monkeypatch.setitem(registry, "worse-test", fetcher_with_count(2))

        quotes, source = await fetch_with_fallback(
            instruments, ["better-test", "worse-test"], jitter_ms=0
        )
        assert source == "better-test"
        assert len(quotes) == len(instruments)
        assert sum(q.price is not None for q in quotes) == 6
        assert sum(q.is_stale for q in quotes) == 4

    @pytest.mark.asyncio
    async def test_healthy_threshold_still_marks_missing_codes_stale(self, monkeypatch):
        instruments = [
            Instrument(code=f"{i:06d}", name=str(i), market="sz") for i in range(10)
        ]

        class SeventyPercentFetcher:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def fetch_batch(self, requested):
                return [Quote(code=i.code, price=1.0) for i in requested[:7]]

        registry = __import__("rdp.fetcher", fromlist=["FETCHER_REGISTRY"]).FETCHER_REGISTRY
        monkeypatch.setitem(registry, "seventy-test", SeventyPercentFetcher)

        quotes, source = await fetch_with_fallback(
            instruments, ["seventy-test"], jitter_ms=0
        )
        assert source == "seventy-test"
        assert len(quotes) == 10
        assert sum(q.is_stale for q in quotes) == 3


class TestAdaptiveLimiter:
    @pytest.mark.asyncio
    async def test_first_high_p95_immediately_halves_concurrency(self, monkeypatch):
        import rdp.fetcher as fetcher_module

        class Response:
            status = 200
            headers = {}

        class Session:
            async def get(self, *args, **kwargs):
                return Response()

        monkeypatch.setattr(fetcher_module, "JITTER_PENALTY_MIN_MS", 0)
        monkeypatch.setattr(fetcher_module, "JITTER_PENALTY_MAX_MS", 0)
        fetcher = EastmoneyFetcher(concurrency=8, jitter_ms=0)
        fetcher._session = Session()
        fetcher._limiter = _AdaptiveLimiter(8)
        for _ in range(100):
            fetcher._lat_tracker.add(2.0)

        await fetcher._polite_get("https://example.invalid")

        assert fetcher._throttle_state == "throttled"
        assert fetcher._limiter.current == 4

    @pytest.mark.asyncio
    async def test_cancelled_woken_waiter_returns_reserved_slot(self):
        limiter = _AdaptiveLimiter(1)
        await limiter.acquire()
        waiter = asyncio.create_task(limiter.acquire())
        await asyncio.sleep(0)

        await limiter.release()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        assert limiter.in_flight == 0
