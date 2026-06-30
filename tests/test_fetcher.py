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