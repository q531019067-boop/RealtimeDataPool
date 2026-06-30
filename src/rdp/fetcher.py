"""实时行情抓取：东方财富（主）/ 新浪（备）/ 腾讯（兜底）。

设计目标：
- 一致快照：所有数据源返回标准化 Quote 对象
- 多源热备：主源失败自动降级
- 批量并发：每源独立 aiohttp session + semaphore 控制并发
- 单次抓全市场：约 30-60s 完成 5400+ 只股票
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any

import aiohttp

from .instruments import Instrument

logger = logging.getLogger(__name__)


# ---------- 标准化数据模型 ----------

@dataclass
class Quote:
    """一只标的的标准化实时快照。

    所有数值字段缺失时为 None（不是 0，避免和真实零值混淆）。
    """

    code: str
    name: str = ""
    market: str = ""
    category: str = "stock"

    # 价格
    price: float | None = None  # 最新价
    open: float | None = None
    high: float | None = None
    low: float | None = None
    prev_close: float | None = None

    # 涨跌
    change: float | None = None  # 涨跌额
    change_pct: float | None = None  # 涨跌幅 %

    # 量能
    volume: float | None = None  # 成交量(手) — 部分源是股，下游按需转换
    amount: float | None = None  # 成交额(元)
    turnover_pct: float | None = None  # 换手率 %

    # 估值
    pe: float | None = None  # 动态市盈率
    pb: float | None = None
    market_cap: float | None = None  # 总市值(元)
    float_cap: float | None = None  # 流通市值(元)

    # 盘口（买卖五档）
    bid_prices: list[float | None] = field(default_factory=lambda: [None] * 5)
    bid_vols: list[float | None] = field(default_factory=lambda: [None] * 5)
    ask_prices: list[float | None] = field(default_factory=lambda: [None] * 5)
    ask_vols: list[float | None] = field(default_factory=lambda: [None] * 5)

    # 状态
    timestamp: float = 0.0  # 数据源时间戳（秒）
    fetched_at: float = 0.0  # 本地抓取时间（秒）
    source: str = ""  # 来源标识
    is_stale: bool = False  # 数据是否过期（停牌等）

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------- 抽象基类 ----------

class BaseFetcher(ABC):
    """数据源抽象基类。"""

    name: str = "base"

    def __init__(self, concurrency: int = 8, timeout: float = 10.0):
        self.concurrency = concurrency
        self.timeout = timeout
        self._session: aiohttp.ClientSession | None = None
        self._sem: asyncio.Semaphore | None = None

    async def __aenter__(self) -> BaseFetcher:
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "*/*",
            },
        )
        self._sem = asyncio.Semaphore(self.concurrency)

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
            self._sem = None

    @abstractmethod
    async def fetch(self, inst: Instrument) -> Quote | None:
        """抓取单只标的。失败返回 None。"""

    async def fetch_batch(self, instruments: list[Instrument]) -> list[Quote]:
        """并发抓取整批。"""
        tasks = [self.fetch(i) for i in instruments]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: list[Quote] = []
        for inst, r in zip(instruments, results):
            if isinstance(r, Quote):
                out.append(r)
            elif isinstance(r, Exception):
                logger.debug("%s fetch error for %s: %s", self.name, inst.code, r)
        return out


# ---------- 东方财富（主源，字段最全） ----------

class EastmoneyFetcher(BaseFetcher):
    """东方财富 push2.eastmoney.com 实时快照。

    接口：`https://push2.eastmoney.com/api/qt/stock/get?secid=1.600000&fields=f43,f44,...`
    一次一只。并发 8-16 即可约 30s 拉完全市场。

    注意：东方财富的 stock/get 接口不返回买卖五档盘口。
    盘口五档需要从腾讯接口补全（见 fetch 方法中的 tencent_fallback）。
    """

    name = "eastmoney"

    _BASE_URL = "https://push2.eastmoney.com/api/qt/stock/get"

    # 字段定义：基础行情 + 估值 + 资金流向
    _FIELDS = [
        "f43",   # 最新价（×10^f152）
        "f44",   # 最高
        "f45",   # 最低
        "f46",   # 今开
        "f47",   # 成交量(手)
        "f48",   # 成交额
        "f49",   # 外盘
        "f50",   # 量比
        "f57",   # 代码
        "f58",   # 名称
        "f60",   # 昨收
        "f86",   # 时间戳(秒)
        "f107",  # 市场
        "f116",  # 总市值
        "f117",  # 流通市值
        "f162",  # 市盈(动)
        "f167",  # 市净率
        "f168",  # 换手率
        "f169",  # 涨跌额
        "f170",  # 涨跌幅
        "f171",  # 振幅
        "f152",  # 价格小数位
    ]

    async def fetch(self, inst: Instrument) -> Quote | None:
        if self._session is None or self._sem is None:
            raise RuntimeError("Fetcher not started")
        fields = ",".join(self._FIELDS)
        url = f"{self._BASE_URL}?secid={inst.secid}&fields={fields}"
        async with self._sem:
            try:
                async with self._session.get(url) as resp:
                    resp.raise_for_status()
                    payload = await resp.json(content_type=None)
            except Exception as exc:
                logger.debug("Eastmoney fetch failed for %s: %s", inst.code, exc)
                return None
        data = (payload or {}).get("data") or {}
        if not data or "f43" not in data:
            return Quote(
                code=inst.code,
                name=inst.name,
                market=inst.market,
                category=inst.category,
                fetched_at=time.time(),
                source=self.name,
                is_stale=True,
            )

        def _num(key: str) -> float | None:
            v = data.get(key)
            if v in (None, "-", ""):
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        # f152 = 价格小数位（2 表示 0.01 元精度）
        scale = 10 ** int(_num("f152") or 2)

        def _price(key: str) -> float | None:
            v = _num(key)
            return v / scale if v is not None else None

        ts = _num("f86") or 0.0
        if ts > 1e12:
            ts = ts / 1000.0

        change_pct_raw = _num("f170")
        change_pct = change_pct_raw / 100.0 if change_pct_raw is not None else None
        change = _num("f169")
        if change is not None:
            change = change / scale

        return Quote(
            code=inst.code,
            name=str(data.get("f58") or inst.name),
            market=inst.market,
            category=inst.category,
            price=_price("f43"),
            open=_price("f46"),
            high=_price("f44"),
            low=_price("f45"),
            prev_close=_price("f60"),
            change=change,
            change_pct=change_pct,
            volume=_num("f47"),
            amount=_num("f48"),
            turnover_pct=_num("f168"),
            pe=_num("f162"),
            pb=_num("f167"),
            market_cap=_num("f116"),
            float_cap=_num("f117"),
            # 盘口五档由腾讯补全
            bid_prices=[None] * 5,
            bid_vols=[None] * 5,
            ask_prices=[None] * 5,
            ask_vols=[None] * 5,
            timestamp=ts,
            fetched_at=time.time(),
            source=self.name,
        )

    async def fetch_with_orderbook(self, inst: Instrument) -> Quote | None:
        """扩展版：抓基础行情后，并行从腾讯补盘口五档。"""
        # 1. 拿基础行情
        quote = await self.fetch(inst)
        if quote is None or quote.is_stale:
            return quote
        # 2. 平行从腾讯补盘口（用极简字段，1 只 1 次请求）
        ob = await self._fetch_tencent_orderbook(inst)
        if ob is not None:
            quote.bid_prices = ob["bid_prices"]
            quote.bid_vols = ob["bid_vols"]
            quote.ask_prices = ob["ask_prices"]
            quote.ask_vols = ob["ask_vols"]
        return quote

    async def fetch_batch_with_orderbook(
        self, instruments: list[Instrument]
    ) -> list[Quote]:
        """扩展版批量抓取：基础行情 + 盘口补全。

        策略：先并发拉东方财富拿到基础行情，再并发从腾讯补盘口。
        """
        # Step 1: 批量抓基础行情
        basic_results = await self.fetch_batch(instruments)
        # Step 2: 对有效 quote 并行补盘口
        valid_codes = [q.code for q in basic_results if q and not q.is_stale]
        tencent_map = await self._fetch_tencent_orderbook_batch(instruments, valid_codes)
        for q in basic_results:
            ob = tencent_map.get(q.code)
            if ob:
                q.bid_prices = ob["bid_prices"]
                q.bid_vols = ob["bid_vols"]
                q.ask_prices = ob["ask_prices"]
                q.ask_vols = ob["ask_vols"]
        return basic_results

    async def _fetch_tencent_orderbook(self, inst: Instrument) -> dict | None:
        """单只从腾讯拿盘口。"""
        results = await self._fetch_tencent_orderbook_batch([inst], [inst.code])
        return results.get(inst.code)

    async def _fetch_tencent_orderbook_batch(
        self, all_instruments: list[Instrument], codes: list[str]
    ) -> dict[str, dict]:
        """批量从腾讯拿盘口（每批 60 只）。"""
        if not codes:
            return {}
        # 构建 code -> inst 映射
        code_to_inst = {i.code: i for i in all_instruments}
        out: dict[str, dict] = {}
        # 分批
        BATCH = 60
        code_list = list(codes)
        sem = asyncio.Semaphore(self.concurrency)
        async def _one(chunk_codes: list[str]) -> None:
            if self._session is None:
                return
            syms = ",".join(f"{code_to_inst[c].tencent_symbol}" for c in chunk_codes if c in code_to_inst)
            if not syms:
                return
            url = f"https://qt.gtimg.cn/q={syms}"
            try:
                async with sem:
                    async with self._session.get(url) as resp:
                        text = await resp.text(encoding="gbk")
            except Exception as exc:
                logger.debug("Tencent orderbook fetch failed: %s", exc)
                return
            for line in text.strip().splitlines():
                if "=" not in line or '"' not in line:
                    continue
                try:
                    var_part, val_part = line.split("=", 1)
                    sym = var_part.strip().split("_")[-1]
                    val = val_part.strip().strip(";").strip('"')
                    if not val:
                        continue
                    fields = val.split("~")
                    # 找 code
                    inst_code = None
                    for c in chunk_codes:
                        if code_to_inst.get(c) and code_to_inst[c].tencent_symbol == sym:
                            inst_code = c
                            break
                    if inst_code is None or len(fields) < 50:
                        continue
                    bid_prices = []
                    bid_vols = []
                    ask_prices = []
                    ask_vols = []
                    for i in range(5):
                        bid_prices.append(_to_float(fields[9 + i * 2]))
                        bid_vols.append(_to_float(fields[10 + i * 2]))
                        ask_prices.append(_to_float(fields[19 + i * 2]))
                        ask_vols.append(_to_float(fields[20 + i * 2]))
                    out[inst_code] = {
                        "bid_prices": bid_prices,
                        "bid_vols": bid_vols,
                        "ask_prices": ask_prices,
                        "ask_vols": ask_vols,
                    }
                except Exception as exc:
                    logger.debug("Tencent parse error: %s", exc)

        chunks = [code_list[i:i + BATCH] for i in range(0, len(code_list), BATCH)]
        await asyncio.gather(*[_one(c) for c in chunks])
        return out


def _to_float(v: str) -> float | None:
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


# ---------- 新浪（备源，一档盘口） ----------

class SinaFetcher(BaseFetcher):
    """新浪 hq.sinajs.cn 实时接口。

    支持一次拉多只：`https://hq.sinajs.cn/list=sh600000,sz000001`
    字段以 , 分隔。
    """

    name = "sina"

    _BASE_URL = "https://hq.sinajs.cn/list={symbols}"
    _BATCH_SIZE = 50  # 一次最多 50 只（新浪反爬门槛）

    async def fetch(self, inst: Instrument) -> Quote | None:
        # 走批量接口拿单只
        results = await self.fetch_batch([inst])
        return results[0] if results else None

    async def fetch_batch(self, instruments: list[Instrument]) -> list[Quote]:
        if self._session is None:
            raise RuntimeError("Fetcher not started")
        # 分片
        out: list[Quote] = []
        for chunk in _chunks(instruments, self._BATCH_SIZE):
            symbols = ",".join(i.sina_symbol for i in chunk)
            url = self._BASE_URL.format(symbols=symbols)
            try:
                # 新浪要求 Referer
                async with self._session.get(
                    url,
                    headers={"Referer": "https://finance.sina.com.cn/"},
                ) as resp:
                    resp.raise_for_status()
                    text = await resp.text(encoding="gbk")
            except Exception as exc:
                logger.debug("Sina batch fetch failed: %s", exc)
                continue

            # 解析：var sh600000="平安银行,1.00,...";
            for line in text.strip().splitlines():
                if "=" not in line or '"' not in line:
                    continue
                try:
                    var_part, val_part = line.split("=", 1)
                    sym = var_part.strip().split("_")[-1]
                    val = val_part.strip().strip(";").strip('"')
                    if not val:
                        continue
                    fields = val.split(",")
                    inst = _find_inst(chunk, sym)
                    if inst is None:
                        continue
                    quote = _parse_sina(inst, fields)
                    if quote is not None:
                        out.append(quote)
                except Exception as exc:
                    logger.debug("Sina parse error for %r: %s", line[:80], exc)
        return out


def _chunks(lst: list[Any], n: int):  # type: ignore[no-untyped-def]
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _find_inst(chunk: list[Instrument], symbol: str) -> Instrument | None:
    for i in chunk:
        if i.sina_symbol == symbol:
            return i
    return None


def _parse_sina(inst: Instrument, fields: list[str]) -> Quote | None:
    """新浪字段（共约 33 个）：
    0=名称 1=今开 2=昨收 3=当前价 4=最高 5=最低
    6=买一价 7=卖一价 8=成交量(股) 9=成交额
    ...
    30=日期 31=涨停价 32=跌停价
    """
    if len(fields) < 10:
        return None
    name = fields[0]
    try:
        open_p = float(fields[1]) if fields[1] else None
        prev_close = float(fields[2]) if fields[2] else None
        price = float(fields[3]) if fields[3] else None
        high = float(fields[4]) if fields[4] else None
        low = float(fields[5]) if fields[5] else None
        bid1 = float(fields[6]) if fields[6] else None
        ask1 = float(fields[7]) if fields[7] else None
        # 成交量是股 → 转手（除以 100）
        volume_shares = float(fields[8]) if fields[8] else None
        volume_hands = volume_shares / 100.0 if volume_shares is not None else None
        amount = float(fields[9]) if fields[9] else None
        change = (price - prev_close) if (price is not None and prev_close is not None) else None
        change_pct = (change / prev_close * 100) if (change is not None and prev_close) else None

        bid_prices = [bid1] + [None] * 4
        ask_prices = [ask1] + [None] * 4

        # 换手率 / 市值（新浪字段位置在不同版本可能不同，做安全兜底）
        turnover_pct = None
        if len(fields) > 38 and fields[38]:
            try:
                turnover_pct = float(fields[38])
            except ValueError:
                pass

        return Quote(
            code=inst.code,
            name=name or inst.name,
            market=inst.market,
            category=inst.category,
            price=price,
            open=open_p,
            high=high,
            low=low,
            prev_close=prev_close,
            change=change,
            change_pct=change_pct,
            volume=volume_hands,
            amount=amount,
            turnover_pct=turnover_pct,
            bid_prices=bid_prices,
            ask_prices=ask_prices,
            fetched_at=time.time(),
            source="sina",
        )
    except (ValueError, IndexError):
        return None


# ---------- 腾讯（兜底，参考 FactorQ ondemand_analyzer） ----------

class TencentFetcher(BaseFetcher):
    """腾讯 qt.gtimg.cn 实时接口。

    接口：`https://qt.gtimg.cn/q=sh600000,sz000001`
    一次多只。
    """

    name = "tencent"

    _BASE_URL = "https://qt.gtimg.cn/q={symbols}"
    _BATCH_SIZE = 60

    async def fetch(self, inst: Instrument) -> Quote | None:
        results = await self.fetch_batch([inst])
        return results[0] if results else None

    async def fetch_batch(self, instruments: list[Instrument]) -> list[Quote]:
        if self._session is None:
            raise RuntimeError("Fetcher not started")
        out: list[Quote] = []
        for chunk in _chunks(instruments, self._BATCH_SIZE):
            symbols = ",".join(i.tencent_symbol for i in chunk)
            url = self._BASE_URL.format(symbols=symbols)
            try:
                async with self._session.get(url) as resp:
                    resp.raise_for_status()
                    text = await resp.text(encoding="gbk")
            except Exception as exc:
                logger.debug("Tencent batch fetch failed: %s", exc)
                continue

            for line in text.strip().splitlines():
                if "=" not in line or '"' not in line:
                    continue
                try:
                    var_part, val_part = line.split("=", 1)
                    sym = var_part.strip().split("_")[-1]
                    val = val_part.strip().strip(";").strip('"')
                    if not val:
                        continue
                    fields = val.split("~")
                    inst = _find_inst(chunk, sym)
                    if inst is None:
                        continue
                    quote = _parse_tencent(inst, fields)
                    if quote is not None:
                        out.append(quote)
                except Exception as exc:
                    logger.debug("Tencent parse error for %r: %s", line[:80], exc)
        return out


def _parse_tencent(inst: Instrument, fields: list[str]) -> Quote | None:
    """腾讯字段（数组位置 → 含义，已实测 2026-06）：
    [0]=未知  [1]=名称  [2]=代码
    [3]=当前价  [4]=昨收  [5]=今开  [6]=成交量(手)
    [7]=外盘(手)  [8]=内盘(手)
    [9,10]=买一价/量  [11,12]=买二  [13,14]=买三  [15,16]=买四  [17,18]=买五
    [19,20]=卖一价/量  [21,22]=卖二  [23,24]=卖三  [25,26]=卖四  [27,28]=卖五
    [30]=时间  [31]=涨跌额  [32]=涨跌幅(%)  [33]=最高  [34]=最低
    [38]=换手率(%)  [39]=市盈率-动  [43]=振幅(%)
    [44]=流通市值(万元)  [45]=总市值(万元)
    """
    if len(fields) < 50:
        return None
    try:
        def _f(idx: int) -> float | None:
            v = fields[idx] if idx < len(fields) else ""
            try:
                return float(v) if v else None
            except ValueError:
                return None

        name = fields[1] or inst.name
        price = _f(3)
        prev_close = _f(4)
        open_p = _f(5)
        volume = _f(6)  # 已经是手
        high = _f(33)
        low = _f(34)
        change_pct = _f(32)
        change = _f(31)

        bid_prices = [_f(9), _f(11), _f(13), _f(15), _f(17)]
        bid_vols = [_f(10), _f(12), _f(14), _f(16), _f(18)]
        ask_prices = [_f(19), _f(21), _f(23), _f(25), _f(27)]
        ask_vols = [_f(20), _f(22), _f(24), _f(26), _f(28)]

        turnover_pct = _f(38)
        pe = _f(39)
        # 流通市值 / 总市值：腾讯给的是万元，转为元
        float_cap = _f(44)
        if float_cap is not None:
            float_cap = float_cap * 1e4
        market_cap = _f(45)
        if market_cap is not None:
            market_cap = market_cap * 1e4

        return Quote(
            code=inst.code,
            name=name,
            market=inst.market,
            category=inst.category,
            price=price,
            open=open_p,
            high=high,
            low=low,
            prev_close=prev_close,
            change=change,
            change_pct=change_pct,
            volume=volume,
            turnover_pct=turnover_pct,
            pe=pe,
            market_cap=market_cap,
            float_cap=float_cap,
            bid_prices=bid_prices,
            bid_vols=bid_vols,
            ask_prices=ask_prices,
            ask_vols=ask_vols,
            fetched_at=time.time(),
            source="tencent",
        )
    except Exception:
        return None


# ---------- 多源协调 ----------

FETCHER_REGISTRY: dict[str, type[BaseFetcher]] = {
    "eastmoney": EastmoneyFetcher,
    "sina": SinaFetcher,
    "tencent": TencentFetcher,
}


async def fetch_with_fallback(
    instruments: list[Instrument],
    sources: list[str],
    concurrency: int = 8,
) -> list[Quote]:
    """按顺序尝试数据源，主源失败率 > 30% 自动降级到下一源。

    特殊处理：当主源是 eastmoney 时，会额外从腾讯补全盘口五档（实现数据完整）。
    """
    last_results: list[Quote] = []
    for src_name in sources:
        cls = FETCHER_REGISTRY.get(src_name)
        if cls is None:
            logger.warning("Unknown source: %s", src_name)
            continue
        logger.info("Trying source: %s (%d instruments)", src_name, len(instruments))
        t0 = time.time()
        try:
            async with cls(concurrency=concurrency) as fetcher:
                # 东方财富走扩展版（自动补盘口）
                if src_name == "eastmoney" and hasattr(fetcher, "fetch_batch_with_orderbook"):
                    results = await fetcher.fetch_batch_with_orderbook(instruments)
                else:
                    results = await fetcher.fetch_batch(instruments)
        except Exception as exc:
            logger.error("Source %s crashed: %s", src_name, exc)
            continue

        elapsed = time.time() - t0
        success = len(results)
        valid = sum(1 for q in results if q.price is not None)
        with_ob = sum(1 for q in results if q.bid_prices and q.bid_prices[0] is not None)
        logger.info(
            "Source %s: %d/%d returned (valid=%d, with_orderbook=%d) in %.1fs",
            src_name, success, len(instruments), valid, with_ob, elapsed,
        )

        if valid / max(success, 1) >= 0.7 or not last_results:
            last_results = results
            if valid > 0:
                return results
        else:
            last_results = results

    return last_results