"""交易时段判断的单元测试。"""

from datetime import datetime, time

import pytest

from rdp.scheduler import is_trading_session


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