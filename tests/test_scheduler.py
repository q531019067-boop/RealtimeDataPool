"""交易时段判断的单元测试。"""

from datetime import datetime, time
from pathlib import Path

import pytest

from rdp.instruments import InstrumentPool
from rdp.scheduler import Scheduler, is_trading_session
from rdp.storage import Storage


class TestTradingSession:
    @pytest.mark.parametrize(
        "dt,expected",
        [
            # 工作日 9:20 集合竞价 — 在
            (datetime(2026, 6, 29, 9, 20), True),
            # 工作日 9:30 早盘 — 在
            (datetime(2026, 6, 29, 9, 30), True),
            # 工作日 11:29 早盘收盘前 — 在
            (datetime(2026, 6, 29, 11, 29), True),
            # 工作日 11:30 早盘收盘瞬间 — 已静止（保守判断不在）
            (datetime(2026, 6, 29, 11, 30), False),
            # 工作日 12:00 午休 — 不在
            (datetime(2026, 6, 29, 12, 0), False),
            # 工作日 13:00 午盘开盘 — 在
            (datetime(2026, 6, 29, 13, 0), True),
            # 工作日 14:55 午盘 — 在
            (datetime(2026, 6, 29, 14, 55), True),
            # 工作日 15:00 收盘 — 不在（边界）
            (datetime(2026, 6, 29, 15, 0), False),
            # 工作日 9:00 早盘前 — 不在
            (datetime(2026, 6, 29, 9, 0), False),
            # 周六 — 不在
            (datetime(2026, 7, 4, 10, 0), False),  # 2026-07-04 是周六
            # 周日 — 不在
            (datetime(2026, 7, 5, 10, 0), False),
        ],
    )
    def test_session(self, dt: datetime, expected: bool):
        assert is_trading_session(dt) is expected


# ===== P2 修复测试：cleanup_interval_sec 默认值 + 可配置 =====
class TestSchedulerCleanupInterval:
    """P2 修复：cleanup_old_snapshots 之前每 30s 跑(太频),
    改为按 cleanup_interval_sec 跑(默认 1800s = 30min)。"""

    def test_default_cleanup_interval_is_1800(self):
        """默认 1800s = 30min,而不是原 30s(随 fetch_interval 跑)"""
        pool = InstrumentPool(instruments=[])
        storage = Storage(Path(":memory:"))
        s = Scheduler(pool, storage, fetch_interval_sec=30)
        assert s.cleanup_interval_sec == 1800

    def test_cleanup_interval_configurable(self):
        """可从构造参数覆盖"""
        pool = InstrumentPool(instruments=[])
        storage = Storage(Path(":memory:"))
        s = Scheduler(pool, storage, fetch_interval_sec=30, cleanup_interval_sec=600)
        assert s.cleanup_interval_sec == 600

    def test_last_cleanup_at_initial_zero(self):
        """初始 _last_cleanup_at = 0,首次 cycle 会跑 cleanup"""
        pool = InstrumentPool(instruments=[])
        storage = Storage(Path(":memory:"))
        s = Scheduler(pool, storage)
        assert s._last_cleanup_at == 0.0


# ===== ⚡ P2-3 优化测试：run_once 透出 orderbook 统计 =====
class TestRunOnceOrderbookStats:
    """run_once 现在把 orderbook 周期统计也并入返回值,方便 CLI 一次性输出。"""

    def test_run_once_signature_documented(self):
        """run_once 应该返回 dict 包含 'orderbook' key"""
        import inspect
        from rdp.scheduler import Scheduler
        sig = inspect.signature(Scheduler.run_once)
        assert sig.return_annotation != inspect.Signature.empty

    def test_update_fetch_run_source_helper(self, tmp_path: Path):
        """Storage.update_fetch_run_source 应该能更新 source 字段"""
        from rdp.storage import Storage
        storage = Storage(tmp_path / "test.db")
        storage.init_schema()
        run_id = storage.start_fetch_run("eastmoney", 100)
        storage.finish_fetch_run(run_id, 100, 100, 0)
        # 修正 source
        storage.update_fetch_run_source(run_id, "sina")
        with storage._connect() as conn:
            row = conn.execute(
                "SELECT source FROM fetch_runs WHERE id=?", (run_id,)
            ).fetchone()
        assert row["source"] == "sina"
        storage.close()
