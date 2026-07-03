"""A股股票池管理：沪深 A 股 + 主要 ETF。

设计：
- 首次启动从东方财富拉全市场股票列表，缓存到本地 JSON
- 增量更新机制：每天启动时检查 refresh_interval，过期则重新拉取
- 支持手工额外追加 / 排除
- 全 A 股池规模约 5400 只，包含 ETF 后约 6000+ 只
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# 东方财富股票列表接口（沪深京 A 股 + ETF + LOF 等）
# ⚠️ P0-2 历史教训：2026-07-02 实盘 5h 复盘发现，原注释的 m:t 语义**完全反了**。
# 实际通过 m:t 探测验证（pn=1, pz=20, fid=f3）：
#   m:0+t:6   → 深圳 A 股主板（00*）
#   m:0+t:80  → 深圳 创业板（30*，**不是上交所 ETF**）
#   m:0+t:81  → 北交所（92*, 83*, 87*，**不是上交所 LOF**）
#   m:1+t:2   → 沪市 A 股主板（60*，**不是深交所主板**）
#   m:1+t:23  → 沪市 科创板（68*，**不是深交所创业板**）
#   m:1+t:22  → 空
# 结论：原代码用 m:0 表示沪市、m:1 表示深市是反的，应该是 m:0=深 m:1=沪。
# 另外原 fs 列表**没有包含任何 ETF 段**，所以 cache 拉下来 0 个 51* / 15* ETF，
# 5h 跑了 12,378 个 code 全部 category="stock"。
_EASTMONEY_LIST_URL = (
    "https://push2.eastmoney.com/api/qt/clist/get"
    "?pn={page}&pz={size}&po=1&fid=f3"
    # 沪深 A 股 + 创业板 + 科创板 + 北交所（已验证）
    "&fs=m:0+t:6,m:0+t:80,m:0+t:81,m:1+t:2,m:1+t:23"
    # 已知 ETF/LOF 段（待 refresh-pool 时验证 2026-07-02 后东财是否还返回 ETF 数据）：
    # ,m:1+t:8,m:0+t:8,m:1+t:80,m:0+t:80  ← 实测被东财 ban 期间无法验证
    "&fields=f12,f14,f13"  # f12=代码 f14=名称 f13=市场(0=深 1=沪)
)

_PAGE_SIZE = 100  # 东方财富硬上限：单页最多 100 条
_MIN_EXPECTED_POOL_SIZE = 1000  # 全 A 股正常应 5000+；低于此值视为被截断
_DEFAULT_CACHE_TTL_SEC = 24 * 3600  # 缓存 1 天
_PARTIAL_CACHE_TTL_SEC = 5 * 60  # 残缺/空池只缓存 5 分钟，尽快重试


@dataclass(frozen=True)
class Instrument:
    """单只可交易标的。"""

    code: str  # 6 位原始代码，如 "000001"
    name: str  # 中文名称
    market: str  # "sh" / "sz" — 用于拼接实时接口的 secid
    category: str = "stock"  # "stock" / "etf" / "lof"

    @property
    def secid(self) -> str:
        """东方财富 secid 格式：1.600000（沪） / 0.000001（深）。"""
        prefix = "1" if self.market == "sh" else "0"
        return f"{prefix}.{self.code}"

    @property
    def sina_symbol(self) -> str:
        """新浪代码格式：sh600000 / sz000001。"""
        return f"{self.market}{self.code}"

    @property
    def tencent_symbol(self) -> str:
        """腾讯代码格式：sh600000 / sz000001（同新浪）。"""
        return f"{self.market}{self.code}"

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "name": self.name,
            "market": self.market,
            "category": self.category,
        }

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> Instrument:
        return cls(
            code=d["code"],
            name=d["name"],
            market=d["market"],
            category=d.get("category", "stock"),
        )


@dataclass
class InstrumentPool:
    """股票池管理。

    使用方式：
        pool = await InstrumentPool.from_config(config, cache_path)
        codes = pool.codes()           # 所有代码
        subset = pool.filter(["stock"]) # 按分类过滤
    """

    instruments: list[Instrument] = field(default_factory=list)
    refreshed_at: float = 0.0
    is_partial: bool = False
    # ⚡ 性能优化 2026-07-02：O(1) dict 索引。
    # 原 by_code 是 O(N) 线性扫，scheduler 的 _run_orderbook 调用 12K 次
    # × 12K = 1.44 亿次比较。改成 dict 索引后 = 12K 次。
    # _index 在 from_json / from_config / __post_init__ 等入口处惰性建一次。
    _index: dict[str, Instrument] = field(default_factory=dict, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._rebuild_index()

    def _rebuild_index(self) -> None:
        """重建 code -> Instrument 索引。改 instruments 后必须调。"""
        self._index = {i.code: i for i in self.instruments}

    def codes(self) -> list[str]:
        return [i.code for i in self.instruments]

    def by_code(self, code: str) -> Instrument | None:
        # ⚡ O(1) — 由 _index 索引
        return self._index.get(code)

    def filter(self, category: str | None = None) -> list[Instrument]:
        if category is None:
            return list(self.instruments)
        return [i for i in self.instruments if i.category == category]

    def __len__(self) -> int:
        return len(self.instruments)

    def to_json(self) -> str:
        return json.dumps(
            {
                "refreshed_at": self.refreshed_at,
                "is_partial": self.is_partial,
                "instruments": [i.to_dict() for i in self.instruments],
            },
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, raw: str) -> InstrumentPool:
        d = json.loads(raw)
        # __post_init__ 会自动建 _index
        instruments = [Instrument.from_dict(x) for x in d["instruments"]]
        return cls(
            instruments=instruments,
            refreshed_at=d.get("refreshed_at", 0.0),
            # 兼容旧缓存：空池即使没有 is_partial 标记也不能视为健康缓存。
            is_partial=bool(d.get("is_partial", False) or not instruments),
        )

    @classmethod
    async def from_config(
        cls,
        pool_cfg: dict[str, Any],
        cache_path: Path,
        force_refresh: bool = False,
    ) -> InstrumentPool:
        """根据配置加载股票池。

        优先级：健康 cache → fetch fresh → stale 健康 cache → partial/empty。

        partial/empty cache 使用 5 分钟短 TTL；新抓取结果若比已有健康缓存更差，
        不覆盖旧缓存，避免一次限流让整个股票池消失 24 小时。
        """
        cached_pool: InstrumentPool | None = None
        if cache_path.exists():
            try:
                cached_pool = cls.from_json(cache_path.read_text(encoding="utf-8"))
                ttl = _PARTIAL_CACHE_TTL_SEC if cached_pool.is_partial else _DEFAULT_CACHE_TTL_SEC
                if not force_refresh and time.time() - cached_pool.refreshed_at < ttl:
                    logger.info(
                        "Loaded %s instrument pool from cache: %d codes (age %.1f min)",
                        "PARTIAL" if cached_pool.is_partial else "complete",
                        len(cached_pool),
                        (time.time() - cached_pool.refreshed_at) / 60,
                    )
                    return _apply_pool_config(cached_pool, pool_cfg)
            except Exception as exc:
                logger.warning("Failed to load instrument cache: %s", exc)

        try:
            pool = await cls._fetch_eastmoney()
        except Exception as exc:
            logger.error("Failed to fetch instrument list: %s", exc)
            if cached_pool is not None and len(cached_pool) > 0:
                logger.warning("Using stale cache as fallback")
                pool = cached_pool
            else:
                logger.warning("No instruments loaded — pool is empty")
                pool = cls(instruments=[], refreshed_at=time.time(), is_partial=True)

        if (
            pool.is_partial
            and cached_pool is not None
            and len(cached_pool) > len(pool)
        ):
            logger.warning(
                "Fetched PARTIAL pool (%d codes); preserving larger stale cache (%d codes)",
                len(pool), len(cached_pool),
            )
            return _apply_pool_config(cached_pool, pool_cfg)

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(pool.to_json(), encoding="utf-8")
        logger.info(
            "Refreshed instrument pool: %d codes%s",
            len(pool), " (PARTIAL, short TTL)" if pool.is_partial else "",
        )

        return _apply_pool_config(pool, pool_cfg)

    @classmethod
    async def _fetch_eastmoney(cls) -> InstrumentPool:
        """从东方财富分页拉取所有沪深 A 股 + ETF/LOF。

        2026-07-03 P0-1 修复：per-page retry + partial-save。

        历史教训（2026-07-03 13:50 实测）：
        拉 82/123 page 时被限流 Server disconnected,
        整个 8200 条已抓数据在异常抛出时**全部丢失**,
        回退到空 cache + 13 个 extra_codes, 99% 标的丢失。

        现在的行为：
        - 每个 page 拉取失败时 retry 3 次, 退避 0.5s/1s/2s + jitter
        - 重试都失败时, **不再 raise**, 把已抓的 page 全部写为 partial pool
          返回 (callers 把 partial pool 写 cache, 下次有 8200 总比 0 强)
        - partial pool 会显式标记 `is_partial=True`，只使用 5 分钟短 TTL；
          若已有更完整的健康缓存，则保留旧缓存且不被 partial/empty 覆盖
        """
        all_items: list[Instrument] = []
        page = 1
        total_expected: int | None = None
        last_page_failed = False
        last_page_error: str | None = None
        hit_safety_cap = False

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"},
        ) as session:
            while True:
                url = _EASTMONEY_LIST_URL.format(page=page, size=_PAGE_SIZE)
                items: list[dict] | None = None

                # ⚡ Per-page retry: 3 次尝试, 指数退避 + jitter
                # 捕获 ClientError/Timeout/JSONDecodeError (限流时偶发)
                for attempt in range(3):
                    try:
                        async with session.get(url) as resp:
                            resp.raise_for_status()
                            payload = await resp.json(content_type=None)
                        data = payload.get("data") or {}
                        diff = data.get("diff") or {}
                        # 东方财富的 diff 是 dict（key="0","1",...）需要转 list
                        items = list(diff.values()) if isinstance(diff, dict) else list(diff)
                        if not items:
                            # 正常结束: page 返回空 = 已经拉完
                            break
                        total_expected = total_expected or data.get("total", 0)
                        # 成功, 退出 retry loop
                        break
                    except (
                        aiohttp.ClientError,
                        asyncio.TimeoutError,
                        json.JSONDecodeError,
                    ) as exc:
                        if attempt < 2:
                            backoff = 0.5 * (2 ** attempt) + random.uniform(0, 0.3)
                            logger.warning(
                                "Eastmoney list page %d attempt %d failed: %s, retry in %.1fs",
                                page, attempt + 1, exc, backoff,
                            )
                            await asyncio.sleep(backoff)
                        else:
                            # 3 次都失败: 不 raise, 标记 partial
                            last_page_failed = True
                            last_page_error = str(exc)
                            items = None
                            break

                if not items and not last_page_failed:
                    # 正常结束 (page 返回空列表)
                    break

                if last_page_failed:
                    # 整页失败, 已抓的所有 page 都保留
                    logger.error(
                        "Eastmoney list STOPPED at page %d after 3 failed attempts: %s. "
                        "Returning PARTIAL pool: %d / %s expected items.",
                        page, last_page_error, len(all_items), total_expected,
                    )
                    break

                # 处理 page items (正常路径)
                for it in items:
                    code = it.get("f12")
                    name = it.get("f14")
                    market_id = it.get("f13")
                    if not code or not name:
                        continue
                    market = "sh" if market_id == 1 else "sz"
                    # 启发式分类：包含 ETF/LOF 关键字
                    nm = name.upper()
                    if "ETF" in nm:
                        category = "etf"
                    elif "LOF" in nm:
                        category = "lof"
                    else:
                        category = "stock"
                    all_items.append(
                        Instrument(code=code, name=name, market=market, category=category)
                    )
                logger.info(
                    "Fetched page %d: %d items (total %d / %s expected)",
                    page, len(items), len(all_items), total_expected,
                )
                if len(items) < _PAGE_SIZE:
                    break
                page += 1
                if page > 200:  # 安全上限：20000 只
                    logger.warning("Hit pagination safety cap")
                    hit_safety_cap = True
                    break
                # 礼貌延时，避免被反爬
                await asyncio.sleep(0.3)

        is_partial = (
            last_page_failed
            or hit_safety_cap
            or len(all_items) < _MIN_EXPECTED_POOL_SIZE
            or (total_expected is not None and len(all_items) < total_expected)
        )
        return cls(
            instruments=all_items,
            refreshed_at=time.time(),
            is_partial=is_partial,
        )


def _apply_pool_config(pool: InstrumentPool, cfg: dict[str, Any]) -> InstrumentPool:
    """按配置裁剪股票池：exclude / extra / max。

    extra_codes 支持两种格式：
    - str: 6 位代码（仅在池子里能找到对应 Instrument 时才追加）
    - dict: {code, name, market, category} 完整 Instrument 信息（即使池子里没有也会创建）
    """
    include_all = cfg.get("include_all_a_share", True)
    include_etf = cfg.get("include_etf", True)
    extra_codes = cfg.get("extra_codes", []) or []
    exclude_codes = set(cfg.get("exclude_codes", []))
    max_size = int(cfg.get("max_pool_size", 0) or 0)

    kept: list[Instrument] = []
    for inst in pool.instruments:
        if inst.code in exclude_codes:
            continue
        if inst.category == "stock" and not include_all:
            continue
        if inst.category in ("etf", "lof") and not include_etf:
            continue
        kept.append(inst)

    # 追加 extra（如果不在池里）
    existing_codes = {i.code for i in kept}
    extras_added = 0
    for item in extra_codes:
        if isinstance(item, str):
            # 旧格式：纯代码
            code = item
            if code in existing_codes:
                continue
            ref = pool.by_code(code)
            if ref is not None:
                kept.append(ref)
                existing_codes.add(code)
                extras_added += 1
        elif isinstance(item, dict):
            # 新格式：完整 Instrument 信息
            try:
                inst = Instrument.from_dict(item)
            except Exception as exc:
                logger.warning("extra_codes dict parse failed for %r: %s", item, exc)
                continue
            if inst.code in existing_codes:
                continue
            kept.append(inst)
            existing_codes.add(inst.code)
            extras_added += 1
        else:
            logger.warning("extra_codes item not str or dict: %r", item)

    # 限制最大尺寸（调试 / 测试用）
    if max_size > 0 and len(kept) > max_size:
        kept = kept[:max_size]

    logger.info(
        "Pool configured: kept=%d (excluded=%d, extras_added=%d, max=%s)",
        len(kept),
        len(pool.instruments) - len(kept),
        extras_added,
        max_size if max_size else "unlimited",
    )
    # ⚡ __post_init__ 会自动建 _index
    return InstrumentPool(
        instruments=kept,
        refreshed_at=pool.refreshed_at,
        is_partial=pool.is_partial,
    )


async def _demo() -> None:  # pragma: no cover — 手动验证用
    pool = await InstrumentPool.from_config(
        {"include_all_a_share": True, "include_etf": True, "extra_codes": [], "exclude_codes": []},
        Path("data/instruments_cache.json"),
        force_refresh=True,
    )
    print(f"Total: {len(pool)}")
    print(f"Stocks: {len(pool.filter('stock'))}")
    print(f"ETF: {len(pool.filter('etf'))}")
    print("Sample:", [i.to_dict() for i in pool.instruments[:5]])


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_demo())
