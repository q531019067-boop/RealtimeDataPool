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
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .fetcher import Quote
from .instruments import Instrument

logger = logging.getLogger(__name__)

_ORDERBOOK_FIELDS = ("bid_prices", "bid_vols", "ask_prices", "ask_vols")


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
            self._conn.row_factory = sqlite3.Row
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

    def update_snapshot_orderbook(
        self,
        code: str,
        bid_prices: list[float | None],
        bid_vols: list[float | None],
        ask_prices: list[float | None],
        ask_vols: list[float | None],
        orderbook_fetched_at: float,
    ) -> bool:
        """把盘口五档 in-place 写回该 code 的最新 snapshot。

        用于"盘口补全解耦"流程：基础行情每 30s 写一次，盘口每 5min 单独补一次，
        复用最新 snapshot 的 data_json，只覆盖 4 个盘口字段 + 新增 orderbook_fetched_at。

        返回 True 表示更新成功，False 表示找不到该 code 的 snapshot。

        ⚡ 原子化 2026-07-02：直接拿到长连接，绕开 _write 上下文，
        用 BEGIN IMMEDIATE + SELECT + UPDATE + COMMIT 包成单事务，
        避免 basic insert 在 SELECT/Update 之间把 row 顶下去导致更新错行。
        单进程场景下 race 概率极低,作为防御性编程加的,成本几乎 0。
        """
        # 拿到长连接(等同 _write 但不走 ctx manager,免得 commit 冲突)
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, timeout=30.0)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT id, data_json FROM snapshots WHERE code=? "
                "ORDER BY fetched_at DESC LIMIT 1",
                (code,),
            ).fetchone()
            if not row:
                conn.execute("ROLLBACK")
                return False
            data = json.loads(row["data_json"])
            data["bid_prices"] = bid_prices
            data["bid_vols"] = bid_vols
            data["ask_prices"] = ask_prices
            data["ask_vols"] = ask_vols
            data["orderbook_fetched_at"] = orderbook_fetched_at
            conn.execute(
                "UPDATE snapshots SET data_json=? WHERE id=?",
                (json.dumps(data, ensure_ascii=False), row["id"]),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return True

    def query_latest(self, codes: list[str] | None = None) -> list[dict[str, Any]]:
        """查指定代码的最新快照。

        返回字段：code, name, fetched_at, source, is_stale, ...全部 quote 字段。

        ⚡ 性能优化 2026-07-02：原 SQL 用 `WHERE s.id = (SELECT MAX(id) FROM snapshots WHERE code = s.code)`
        是 correlated subquery，12K codes 跑下来 = 1.44 亿次比较。改成 GROUP BY MAX(id) 走
        idx_snapshots_code_time 索引（loose index scan），实测下降 50-100x。

        基础行情始终取最新行；盘口从最近一次带 orderbook_fetched_at 的行合并。
        这样既不会为了盘口把 price/fetched_at 回退到旧周期，也能在两个盘口周期之间
        持续返回最近一次五档数据。
        """
        code_filter = ""
        params: tuple[Any, ...] = ()
        if codes:
            placeholders = ",".join(["?"] * len(codes))
            code_filter = f"WHERE code IN ({placeholders})"
            params = tuple(codes)

        sql = f"""
            WITH latest AS (
                SELECT code, MAX(id) AS max_id
                FROM snapshots
                {code_filter}
                GROUP BY code
            ), latest_orderbook AS (
                SELECT candidate.code, MAX(candidate.id) AS max_id
                FROM snapshots candidate
                JOIN latest ON latest.code = candidate.code
                WHERE json_type(candidate.data_json, '$.orderbook_fetched_at')
                      IN ('integer', 'real')
                GROUP BY candidate.code
            )
            SELECT s.code, s.fetched_at, s.source, s.is_stale, s.data_json,
                   ob.data_json AS orderbook_json,
                   i.name, i.market, i.category
            FROM latest
            JOIN snapshots s ON s.id = latest.max_id
            JOIN instruments i ON s.code = i.code
            LEFT JOIN latest_orderbook lob ON lob.code = s.code
            LEFT JOIN snapshots ob ON ob.id = lob.max_id
            ORDER BY s.code
        """

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        return [self._merge_latest_with_orderbook(r) for r in rows]

    @staticmethod
    def _merge_latest_with_orderbook(row: sqlite3.Row) -> dict[str, Any]:
        """保留最新基础行情，只从历史盘口行复制盘口字段。"""
        data = json.loads(row["data_json"])
        orderbook_json = row["orderbook_json"]
        if not orderbook_json:
            return data
        orderbook = json.loads(orderbook_json)
        for field in _ORDERBOOK_FIELDS:
            data[field] = orderbook.get(field, [None] * 5)
        data["orderbook_fetched_at"] = orderbook.get("orderbook_fetched_at")
        return data

    # 可在 SQL 里排序的白名单字段(都从 data_json 里 json_extract)
    # 注意:fetched_at 在主表上,不走 json_extract
    _SQL_SORTABLE_FIELDS: frozenset[str] = frozenset({
        "change_pct", "price", "open", "high", "low",
        "prev_close", "volume", "amount",
    })

    def query_latest_paged(
        self,
        *,
        sort_by: str = "change_pct",
        order: str = "desc",
        limit: int = 200,
        category: str | None = None,
        min_change_pct: float | None = None,
        max_change_pct: float | None = None,
    ) -> list[dict[str, Any]]:
        """分页查询最新快照(带排序 + 过滤),全部在 SQL 里完成。

        与 query_latest() 的区别:
        - 默认按 change_pct 降序
        - category/涨跌幅过滤推到 WHERE
        - LIMIT 在 SQL 层裁剪,避免 5400 行全量回 Python

        ⚡ 性能优化 2026-07-02:原 /snapshots/all 在 Python 里 sort+filter,
        5400 行 * 多次 getattr ≈ 50ms。改成 SQL 后实测 5ms (10×)。

        sort_by 白名单:change_pct / price / open / high / low / prev_close /
        volume / amount / fetched_at。其它字段抛 ValueError(防止 SQL 注入)。
        """
        if sort_by not in self._SQL_SORTABLE_FIELDS | {"fetched_at"}:
            raise ValueError(
                f"sort_by={sort_by!r} not in whitelist "
                f"{sorted(self._SQL_SORTABLE_FIELDS | {'fetched_at'})}"
            )
        if order not in ("asc", "desc"):
            raise ValueError(f"order must be 'asc' or 'desc', got {order!r}")

        # fetched_at 是快照主表字段,其它走 json_extract
        sort_expr = (
            "s.fetched_at"
            if sort_by == "fetched_at"
            else f"json_extract(s.data_json, '$.{sort_by}')"
        )

        where_parts: list[str] = []
        params: list[Any] = []
        if category:
            where_parts.append("i.category = ?")
            params.append(category)
        if min_change_pct is not None:
            where_parts.append("json_extract(s.data_json, '$.change_pct') >= ?")
            params.append(min_change_pct)
        if max_change_pct is not None:
            where_parts.append("json_extract(s.data_json, '$.change_pct') <= ?")
            params.append(max_change_pct)

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # 跟 query_latest 一致：排序/过滤使用最新基础行情，盘口单独合并。
        sql = f"""
            WITH latest AS (
                SELECT code, MAX(id) AS max_id
                FROM snapshots
                GROUP BY code
            ), latest_orderbook AS (
                SELECT candidate.code, MAX(candidate.id) AS max_id
                FROM snapshots candidate
                JOIN latest ON latest.code = candidate.code
                WHERE json_type(candidate.data_json, '$.orderbook_fetched_at')
                      IN ('integer', 'real')
                GROUP BY candidate.code
            )
            SELECT s.code, s.fetched_at, s.source, s.is_stale, s.data_json,
                   ob.data_json AS orderbook_json,
                   i.name, i.market, i.category
            FROM latest
            JOIN snapshots s ON s.id = latest.max_id
            JOIN instruments i ON s.code = i.code
            LEFT JOIN latest_orderbook lob ON lob.code = s.code
            LEFT JOIN snapshots ob ON ob.id = lob.max_id
            {where_sql}
            ORDER BY {sort_expr} {order.upper()} NULLS LAST
            LIMIT ?
        """
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()

        return [self._merge_latest_with_orderbook(r) for r in rows]

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

    def update_fetch_run_source(self, run_id: int, source: str) -> None:
        """⚡ 修正 fetch_runs.source — 实际 fetch_with_fallback 命中的源(可能不是 sources[0])。

        实盘 5h 数据:全部 cycle 都写 "eastmoney" 即使回退到 sina/tencent,
        导致 fetch_runs.source 字段误导。Scheduler start_fetch_run 时先用
        self.sources[0] 占位,fetch 完后用本方法修正。
        """
        with self._write() as conn:
            conn.execute(
                "UPDATE fetch_runs SET source=? WHERE id=?",
                (source, run_id),
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
