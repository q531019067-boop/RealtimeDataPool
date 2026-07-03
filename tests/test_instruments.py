"""Instrument 数据模型测试。"""

import time

import pytest

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


# ===== 性能优化测试：by_code O(1) dict 索引 =====
class TestInstrumentPoolByCode:
    """⚡ 优化：by_code O(N) 线性扫 → O(1) dict 索引。
    实盘 5h 复盘发现 scheduler._run_orderbook 调 by_code 12K 次
    × 池子 12K = 1.44 亿次比较，每 5 min 卡一下。"""

    def test_by_code_returns_correct_instrument(self):
        pool = InstrumentPool(
            instruments=[
                Instrument(code="000001", name="Pingan", market="sz", category="stock"),
                Instrument(code="600519", name="Maotai", market="sh", category="stock"),
            ],
        )
        assert pool.by_code("000001").name == "Pingan"
        assert pool.by_code("600519").name == "Maotai"
        assert pool.by_code("999999") is None

    def test_by_code_index_rebuilt_on_init(self):
        """__post_init__ 应该自动建 _index"""
        pool = InstrumentPool(
            instruments=[Instrument(code="X", name="x", market="sh", category="stock")],
        )
        assert "X" in pool._index
        assert pool._index["X"].name == "x"

    def test_by_code_works_with_12000_instruments(self):
        """⚡ 性能:12K 池子,by_code 1000 次应该 < 100ms(O(1) 字典)"""
        import time

        from rdp.instruments import Instrument
        # 构造 12K instrument
        insts = [
            Instrument(code=f"{i:06d}", name=f"N{i}", market="sh", category="stock")
            for i in range(12000)
        ]
        pool = InstrumentPool(instruments=insts)
        t0 = time.time()
        for i in range(1000):
            pool.by_code(f"{i % 12000:06d}")
        elapsed = time.time() - t0
        # 1000 次 dict lookup 应该 < 10ms,留 100ms 缓冲
        assert elapsed < 0.1, f"by_code 1000 次耗时 {elapsed*1000:.1f}ms (应 < 100ms)"

    def test_from_json_rebuilds_index(self):
        """从 JSON 加载后 _index 也能用"""
        pool = InstrumentPool.from_json(
            '{"refreshed_at": 0, "instruments": [{"code": "X", "name": "x", "market": "sh", "category": "stock"}]}'
        )
        assert pool.by_code("X") is not None

    def test_apply_pool_config_rebuilds_index(self):
        """_apply_pool_config 出来的 pool._index 也能用"""
        src = InstrumentPool(
            instruments=[Instrument(code="X", name="x", market="sh", category="stock")],
        )
        out = _apply_pool_config(
            src,
            {"include_all_a_share": True, "include_etf": True,
             "extra_codes": [{"code": "Y", "name": "y", "market": "sz", "category": "etf"}],
             "exclude_codes": [], "max_pool_size": 0},
        )
        assert out.by_code("X") is not None
        assert out.by_code("Y") is not None
        assert out.by_code("Z") is None


class TestInstrumentPoolCacheSafety:
    def test_legacy_empty_cache_is_treated_as_partial(self):
        pool = InstrumentPool.from_json(
            '{"refreshed_at": 1, "instruments": []}'
        )
        assert pool.is_partial is True

    @pytest.mark.asyncio
    async def test_partial_refresh_does_not_overwrite_complete_cache(
        self, tmp_path, monkeypatch
    ):
        cache_path = tmp_path / "instruments.json"
        complete = InstrumentPool(
            instruments=[Instrument("000001", "平安", "sz")],
            refreshed_at=time.time() - 86400,
        )
        cache_path.write_text(complete.to_json(), encoding="utf-8")

        async def fake_fetch(cls):
            return InstrumentPool(
                instruments=[], refreshed_at=time.time(), is_partial=True
            )

        monkeypatch.setattr(
            InstrumentPool, "_fetch_eastmoney", classmethod(fake_fetch)
        )
        result = await InstrumentPool.from_config(
            {}, cache_path, force_refresh=True
        )

        assert result.codes() == ["000001"]
        cached_after = InstrumentPool.from_json(cache_path.read_text(encoding="utf-8"))
        assert cached_after.codes() == ["000001"]
        assert cached_after.is_partial is False

    @pytest.mark.asyncio
    async def test_partial_cache_uses_short_ttl(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "instruments.json"
        partial = InstrumentPool(
            instruments=[Instrument("000001", "平安", "sz")],
            refreshed_at=time.time() - 301,
            is_partial=True,
        )
        cache_path.write_text(partial.to_json(), encoding="utf-8")
        called = False

        async def fake_fetch(cls):
            nonlocal called
            called = True
            return InstrumentPool(
                instruments=[
                    Instrument("000001", "平安", "sz"),
                    Instrument("600000", "浦发", "sh"),
                ],
                refreshed_at=time.time(),
            )

        monkeypatch.setattr(
            InstrumentPool, "_fetch_eastmoney", classmethod(fake_fetch)
        )
        result = await InstrumentPool.from_config({}, cache_path)

        assert called is True
        assert result.codes() == ["000001", "600000"]
        assert result.is_partial is False
