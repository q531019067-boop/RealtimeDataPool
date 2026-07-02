"""Instrument 数据模型测试。"""

from rdp.instruments import Instrument, InstrumentPool, _apply_pool_config


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


# ===== P0-2 修复测试：extra_codes 支持 dict 格式 + Pool 配置裁剪 =====
class TestApplyPoolConfig:
    """P0-2 修复：5h 实盘发现 cache 漏 ETF，需要让 extra_codes 支持
    完整 Instrument 信息（dict 格式），即使池子里没有也能创建出来。
    """

    def _make_pool(self):
        return InstrumentPool(
            instruments=[
                Instrument(code="000001", name="平安银行", market="sz", category="stock"),
                Instrument(code="600519", name="贵州茅台", market="sh", category="stock"),
            ],
            refreshed_at=0.0,
        )

    def test_extra_codes_str_format_still_works(self):
        """旧 str 格式：纯代码,池子里能找到才追加"""
        pool = self._make_pool()
        cfg = {
            "include_all_a_share": True,
            "include_etf": True,
            "extra_codes": ["600519"],  # 池子已有,跳过
            "exclude_codes": [],
            "max_pool_size": 0,
        }
        out = _apply_pool_config(pool, cfg)
        codes = {i.code for i in out.instruments}
        assert codes == {"000001", "600519"}

    def test_extra_codes_str_format_missing_skipped(self):
        """str 格式：池子里没有的 code 静默跳过(原行为)"""
        pool = self._make_pool()
        cfg = {
            "include_all_a_share": True,
            "include_etf": True,
            "extra_codes": ["510300"],  # 池子没有,被跳过
            "exclude_codes": [],
            "max_pool_size": 0,
        }
        out = _apply_pool_config(pool, cfg)
        codes = {i.code for i in out.instruments}
        assert "510300" not in codes
        assert codes == {"000001", "600519"}

    def test_extra_codes_dict_format_creates_new(self):
        """⚠️ 关键测试：dict 格式:即使池子没有,也会创建"""
        pool = self._make_pool()
        cfg = {
            "include_all_a_share": True,
            "include_etf": True,
            "extra_codes": [
                {"code": "510300", "name": "沪深300ETF", "market": "sh", "category": "etf"},
            ],
            "exclude_codes": [],
            "max_pool_size": 0,
        }
        out = _apply_pool_config(pool, cfg)
        codes = {i.code for i in out.instruments}
        assert "510300" in codes, "dict 格式 extra 必须被追加"
        # 找 510300
        etf = next(i for i in out.instruments if i.code == "510300")
        assert etf.category == "etf"
        assert etf.market == "sh"
        assert etf.name == "沪深300ETF"

    def test_extra_codes_dict_no_duplicate(self):
        """dict 格式追加时,池子里已有的不重复"""
        pool = self._make_pool()
        cfg = {
            "include_all_a_share": True,
            "include_etf": True,
            "extra_codes": [
                {"code": "000001", "name": "别名", "market": "sz", "category": "stock"},
            ],
            "exclude_codes": [],
            "max_pool_size": 0,
        }
        out = _apply_pool_config(pool, cfg)
        codes = [i.code for i in out.instruments]
        assert codes.count("000001") == 1
        # 保留原 instrument(名字是"平安银行"不是"别名")
        kept = next(i for i in out.instruments if i.code == "000001")
        assert kept.name == "平安银行"

    def test_extra_codes_mixed_format(self):
        """混合 str + dict 格式都支持"""
        pool = self._make_pool()
        cfg = {
            "include_all_a_share": True,
            "include_etf": True,
            "extra_codes": [
                "000002",  # 池子没有,str 会被跳过
                {"code": "510300", "name": "300ETF", "market": "sh", "category": "etf"},
                {"code": "510500", "name": "500ETF", "market": "sh", "category": "etf"},
            ],
            "exclude_codes": [],
            "max_pool_size": 0,
        }
        out = _apply_pool_config(pool, cfg)
        codes = {i.code for i in out.instruments}
        assert codes == {"000001", "600519", "510300", "510500"}

    def test_extra_codes_invalid_dict_skipped(self):
        """dict 缺字段 → 跳过 + WARNING,不崩"""
        pool = self._make_pool()
        cfg = {
            "include_all_a_share": True,
            "include_etf": True,
            "extra_codes": [
                {"code": "510300"},  # 缺 name/market/category
            ],
            "exclude_codes": [],
            "max_pool_size": 0,
        }
        # 不应抛异常
        out = _apply_pool_config(pool, cfg)
        codes = {i.code for i in out.instruments}
        assert "510300" not in codes  # 被跳过

    def test_exclude_codes(self):
        """exclude_codes 仍然工作"""
        pool = self._make_pool()
        cfg = {
            "include_all_a_share": True,
            "include_etf": True,
            "extra_codes": [],
            "exclude_codes": ["000001"],
            "max_pool_size": 0,
        }
        out = _apply_pool_config(pool, cfg)
        codes = {i.code for i in out.instruments}
        assert "000001" not in codes
        assert "600519" in codes
