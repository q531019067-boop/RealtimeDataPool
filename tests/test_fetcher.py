"""Fetcher 解析逻辑测试（不发起真实请求）。"""

import pytest

from rdp.fetcher import _parse_sina, _parse_tencent
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
        fields[37] = "10.80" # 最高
        fields[38] = "10.30" # 最低
        fields[44] = "5.00"  # 涨跌幅
        fields[45] = "0.50"  # 涨跌额
        fields[49] = "3000000000"  # 流通市值
        q = _parse_tencent(inst, fields)
        assert q is not None
        assert q.name == "浦发银行"
        assert q.price == pytest.approx(10.50)
        assert q.change_pct == pytest.approx(5.0)
        assert q.bid_prices[0] == pytest.approx(10.49)
        assert q.ask_prices[0] == pytest.approx(10.51)

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
        f._sem = __import__("asyncio").Semaphore(1)

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
