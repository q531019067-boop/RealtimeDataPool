"""Instrument 数据模型测试。"""

from rdp.instruments import Instrument


class TestInstrument:
    def test_secid_sh(self):
        i = Instrument(code="600000", name="浦发", market="sh")
        assert i.secid == "1.600000"

    def test_secid_sz(self):
        i = Instrument(code="000001", name="平安", market="sz")
        assert i.secid == "0.000001"

    def test_sina_symbol(self):
        i = Instrument(code="600000", name="x", market="sh")
        assert i.sina_symbol == "sh600000"

    def test_to_from_dict(self):
        i = Instrument(code="510300", name="沪深300ETF", market="sh", category="etf")
        d = i.to_dict()
        j = Instrument.from_dict(d)
        assert j == i

    def test_category_default(self):
        i = Instrument(code="000001", name="x", market="sz")
        assert i.category == "stock"