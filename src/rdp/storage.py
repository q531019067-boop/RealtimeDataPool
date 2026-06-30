"""SQLite 存储层。

设计：
- WAL 模式（高并发读 + 单线程写）
- 每次抓取作为一条 snapshot 记录，data_json 存完整数据（含盘口五档）
- instruments 表缓存股票池元信息
- fetch_runs 表记录每次抓取的健康度
- 自动清理 retention_days 之前的快照
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .fetcher import Quote
from .instruments import Instrument

logger = logging.getLogger(__name__)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS instruments (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    market TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'stock',
    refreshed_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    fetched_at REAL NOT NULL,
    source TEXT NOT NULL,
    is_stale INTEGER NOT NULL DEFAULT 0,
    data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snapshots_code_time
    ON snapshots(code, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_time
    ON snapshots(fetched_at DESC);

CREATE TABLE IF NOT EXISTS fetch_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at REAL NOT NULL,
    ended_at REAL,
    source TEXT NOT NULL,
    pool_size INTEGER NOT NULL,
    count_ok INTEGER NOT NULL DEFAULT 0,
    count_valid INTEGER NOT NULL DEFAULT 0,
    count_stale INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_fetch_runs_time
    ON fetch_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Storage:
    """SQLite 存储管理器。

    使用方式：
        storage = Storage(path)
        storage.init_schema()
        storage.upsert_instruments(pool)
        storage.insert_snapshot(quote)
        df = storage.query_latest(["000001"])
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.commit()
        logger.info("Storage schema initialized: %s", self.db_path)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """写连接：长连接 + WAL，便于高频插入。"""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, timeout=30.0)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ---------- instruments ----------

    def upsert_instruments(self, instruments: list[Instrument]) -> None:
        if not instruments:
            return
        rows = [
            (i.code, i.name, i.market, i.category, time.time()) for i in instruments
        ]
        with self._write() as conn:
            conn.executemany(
                """
                INSERT INTO instruments (code, name, market, category, refreshed_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    name=excluded.name,
                    market=excluded.market,
                    category=excluded.category,
                    refreshed_at=excluded.refreshed_at
                """,
                rows,
            )
        logger.info("Upserted %d instruments", len(rows))

    def list_instruments(self, category: str | None = None) -> list[dict[str, str]]:
        with self._connect() as conn:
            if category:
                rows = conn.execute(
                    "SELECT code, name, market, category FROM instruments WHERE category=? ORDER BY code",
                    (category,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT code, name, market, category FROM instruments ORDER BY code"
                ).fetchall()
        return [dict(r) for r in rows]

    # ---------- snapshots ----------

    def insert_snapshots(self, quotes: list[Quote]) -> int:
        if not quotes:
            return 0
        rows = [
            (
                q.code,
                q.fetched_at,
                q.source,
                1 if q.is_stale else 0,
                json.dumps(q.to_dict(), ensure_ascii=False),
            )
            for q in quotes
        ]
        with self._write() as conn:
            conn.executemany(
                """INSERT INTO snapshots (code, fetched_at, source, is_stale, data_json)
                   VALUES (?, ?, ?, ?, ?)""",
                rows,
            )
        return len(rows)

    def insert_snapshot(self, quote: Quote) -> None:
        self.insert_snapshots([quote])

    def query_latest(self, codes: list[str] | None = None) -> list[dict[str, Any]]:
        """查指定代码的最新快照。

        返回字段：code, name, fetched_at, source, is_stale, ...全部 quote 字段。
        """
        sql = """
            SELECT s.code, s.fetched_at, s.source, s.is_stale, s.data_json, i.name, i.market, i.category
            FROM snapshots s
            JOIN instruments i ON s.code = i.code
            WHERE s.id = (
                SELECT MAX(id) FROM snapshots WHERE code = s.code
            )
        """
        params: tuple = ()
        if codes:
            placeholders = ",".join(["?"] * len(codes))
            sql += f" AND s.code IN ({placeholders})"
            params = tuple(codes)
        sql += " ORDER BY s.code"

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        out: list[dict[str, Any]] = []
        for r in rows:
            data = json.loads(r["data_json"])
            out.append(data)
        return out

    def query_history(
        self, code: str, limit: int = 100, since: float | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT fetched_at, source, data_json FROM snapshots WHERE code=? "
        params: list[Any] = [code]
        if since is not None:
            sql += "AND fetched_at >= ? "
            params.append(since)
        sql += "ORDER BY fetched_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [
            {"fetched_at": r["fetched_at"], "source": r["source"], **json.loads(r["data_json"])}
            for r in rows
        ]

    def snapshot_count(self, code: str | None = None) -> int:
        with self._connect() as conn:
            if code:
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM snapshots WHERE code=?", (code,)
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS c FROM snapshots").fetchone()
        return int(row["c"])

    def cleanup_old_snapshots(self, retention_days: int) -> int:
        cutoff = time.time() - retention_days * 86400
        with self._write() as conn:
            cur = conn.execute("DELETE FROM snapshots WHERE fetched_at < ?", (cutoff,))
            deleted = cur.rowcount
        logger.info("Cleaned up %d snapshots older than %d days", deleted, retention_days)
        return deleted

    # ---------- fetch_runs ----------

    def start_fetch_run(self, source: str, pool_size: int) -> int:
        with self._write() as conn:
            cur = conn.execute(
                "INSERT INTO fetch_runs (started_at, source, pool_size) VALUES (?, ?, ?)",
                (time.time(), source, pool_size),
            )
            return int(cur.lastrowid)

    def finish_fetch_run(
        self,
        run_id: int,
        count_ok: int,
        count_valid: int,
        count_stale: int,
        error: str | None = None,
    ) -> None:
        with self._write() as conn:
            conn.execute(
                """UPDATE fetch_runs
                   SET ended_at=?, count_ok=?, count_valid=?, count_stale=?, error=?
                   WHERE id=?""",
                (time.time(), count_ok, count_valid, count_stale, error, run_id),
            )

    def recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT id, started_at, ended_at, source, pool_size,
                          count_ok, count_valid, count_stale, error
                   FROM fetch_runs
                   ORDER BY started_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---------- meta ----------

    def set_meta(self, key: str, value: str) -> None:
        with self._write() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def get_meta(self, key: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None