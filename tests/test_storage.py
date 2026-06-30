"""存储层测试。"""

from pathlib import Path

import pytest

from rdp.fetcher import Quote
from rdp.instruments import Instrument
from rdp.storage import Storage


@pytest.fixture
def storage(tmp_path: Path) -> Storage:
    s = Storage(tmp_path / "test.db")
    s.init_schema()
    yield s
    s.close()


class TestStorage:
    def test_upsert_instruments(self, storage: Storage):
        insts = [
            Instrument(code="000001", name="平安", market="sz"),
            Instrument(code="600000", name="浦发", market="sh", category="etf"),
        ]
        storage.upsert_instruments(insts)
        all_inst = storage.list_instruments()
        assert len(all_inst) == 2
        assert storage.list_instruments(category="etf")[0]["code"] == "600000"

    def test_insert_and_query_snapshot(self, storage: Storage):
        storage.upsert_instruments([Instrument(code="000001", name="平安", market="sz")])
        q = Quote(
            code="000001", name="平安", market="sz", price=10.5, change_pct=1.5,
            source="test", fetched_at=1234567890.0,
        )
        storage.insert_snapshot(q)
        rows = storage.query_latest(["000001"])
        assert len(rows) == 1
        assert rows[0]["price"] == 10.5
        assert rows[0]["change_pct"] == 1.5

    def test_query_latest_returns_only_latest(self, storage: Storage):
        storage.upsert_instruments([Instrument(code="000001", name="x", market="sz")])
        for ts in [1000.0, 2000.0, 3000.0]:
            storage.insert_snapshot(Quote(code="000001", name="x", market="sz", price=ts, fetched_at=ts))
        rows = storage.query_latest(["000001"])
        assert len(rows) == 1
        assert rows[0]["price"] == 3000.0

    def test_history(self, storage: Storage):
        storage.upsert_instruments([Instrument(code="000001", name="x", market="sz")])
        for i in range(10):
            storage.insert_snapshot(Quote(code="000001", name="x", market="sz", price=i, fetched_at=float(i)))
        hist = storage.query_history("000001", limit=5)
        assert len(hist) == 5
        # 最新在前
        assert hist[0]["price"] == 9

    def test_fetch_runs(self, storage: Storage):
        rid = storage.start_fetch_run("eastmoney", pool_size=100)
        storage.finish_fetch_run(rid, count_ok=95, count_valid=90, count_stale=5)
        runs = storage.recent_runs()
        assert runs[0]["count_ok"] == 95
        assert runs[0]["count_valid"] == 90
        assert runs[0]["source"] == "eastmoney"

    def test_cleanup(self, storage: Storage):
        storage.upsert_instruments([Instrument(code="x", name="x", market="sz")])
        # 插入一条"很老"的快照
        old_ts = 1000.0  # 1970 年
        storage.insert_snapshot(Quote(code="x", name="x", market="sz", fetched_at=old_ts))
        deleted = storage.cleanup_old_snapshots(retention_days=1)
        assert deleted >= 1
        assert storage.snapshot_count() == 0

    def test_meta(self, storage: Storage):
        storage.set_meta("version", "0.1.0")
        assert storage.get_meta("version") == "0.1.0"
        storage.set_meta("version", "0.2.0")
        assert storage.get_meta("version") == "0.2.0"