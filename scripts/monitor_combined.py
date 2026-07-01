#!/usr/bin/env python
"""
多股票合并实时监控脚本
─────────────────────────
用法：
  1. 先开一个 PowerShell 启动 RDP 服务：
     cd RealtimeDataPool; uv run python scripts/start.py serve
  2. 再开另一个 PowerShell 运行本脚本：
     cd RealtimeDataPool; uv run python scripts/monitor_combined.py

监控标的：立讯精密(sz002475) + 大唐发电(sh601991)
功能：
  - 每 60 秒从 RDP API 取两只股票最新快照
  - 叠加上日 K 线数据（来自 RetailQuant parquet）
  - 运行 4 个策略 + 2 个自写检测
  - 打印对齐的合并报告表格
  - 满足条件时醒目弹窗通知
"""

from __future__ import annotations

import sys
import threading
import time
from datetime import datetime, timedelta, time as dtime
from pathlib import Path

import pandas as pd
import requests

# ============================================================
#  RetailQuant 策略导入（绕过 __init__.py 链，不触发 loguru 等依赖）
# ============================================================
import importlib.util

_RETAIL = Path(__file__).resolve().parent.parent.parent / "RetailQuant"
_STRATEGY = _RETAIL / "rquant" / "strategy"


def _load_module(name: str, path: Path) -> None:
    """加载模块到 sys.modules[name]，支持相对导入"""
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)


# 1. 创建占位包（让相对导入 ..base / ..registry 能解析）
for _pkg in ["rquant", "rquant.strategy", "rquant.strategy.grid",
             "rquant.strategy.volume_breakout", "rquant.strategy.factor",
             "rquant.strategy.mean_reversion"]:
    if _pkg not in sys.modules:
        sys.modules[_pkg] = type(sys)("placeholders")

# 2. 加载 base + registry（策略的公共依赖）
_load_module("rquant.strategy.base", _STRATEGY / "base.py")
_load_module("rquant.strategy.registry", _STRATEGY / "registry.py")

# 3. 加载四个策略
_load_module("rquant.strategy.grid.grid_martingale", _STRATEGY / "grid" / "grid_martingale.py")
GridMartingale = sys.modules["rquant.strategy.grid.grid_martingale"].GridMartingale

_load_module("rquant.strategy.volume_breakout.vp_breakout", _STRATEGY / "volume_breakout" / "vp_breakout.py")
VpBreakout = sys.modules["rquant.strategy.volume_breakout.vp_breakout"].VpBreakout

_load_module("rquant.strategy.factor.multi_factor", _STRATEGY / "factor" / "multi_factor.py")
MultiFactor = sys.modules["rquant.strategy.factor.multi_factor"].MultiFactor

_load_module("rquant.strategy.mean_reversion.rsi_reversion", _STRATEGY / "mean_reversion" / "rsi_reversion.py")
RsiMeanReversion = sys.modules["rquant.strategy.mean_reversion.rsi_reversion"].RsiMeanReversion

# ============================================================
#  配置
# ============================================================
API = "http://localhost:5080"
INTERVAL = 60  # 秒

STOCKS = [
    {"code": "002475", "name": "立讯精密", "parquet": "sz002475.parquet"},
    {"code": "601991", "name": "大唐发电", "parquet": "sh601991.parquet"},
]

# 策略实例化
GRID = GridMartingale()
VPB = VpBreakout()
MULTIFACTOR = MultiFactor()
RSI_REV = RsiMeanReversion()

# ============================================================
#  技术指标（移植自 RetailQuant/rquant/strategy/base.py）
# ============================================================

def ma(df: pd.DataFrame, n: int) -> float:
    if len(df) < n:
        return float(df["close"].iloc[-1])
    return float(df["close"].tail(n).mean())


def prev_ma(df: pd.DataFrame, n: int) -> float:
    if len(df) < n + 1:
        return float(df["close"].iloc[-1])
    return float(df["close"].iloc[-(n + 1):-1].mean())


def highest(df: pd.DataFrame, n: int) -> float:
    if len(df) < n:
        return float(df["high"].max())
    return float(df["high"].tail(n).max())


def lowest(df: pd.DataFrame, n: int) -> float:
    if len(df) < n:
        return float(df["low"].min())
    return float(df["low"].tail(n).min())


def rsi(df: pd.DataFrame, n: int = 14) -> float:
    if len(df) < n + 1:
        return 50.0
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0).tail(n)
    loss = (-delta.where(delta < 0, 0.0)).tail(n)
    avg_gain = gain.mean()
    avg_loss = loss.mean()
    if avg_loss == 0:
        return 100.0
    return float(100 - 100 / (1 + avg_gain / avg_loss))


def vol_ratio(df: pd.DataFrame, n: int = 5) -> float:
    if len(df) < n + 1:
        return 1.0
    today_v = float(df["volume"].iloc[-1])
    avg_v = float(df["volume"].iloc[-(n + 1):-1].mean())
    return today_v / avg_v if avg_v > 0 else 1.0


def atr(df: pd.DataFrame, n: int = 14) -> float:
    if len(df) < n + 1:
        return float(df["close"].iloc[-1] * 0.02)
    high = df["high"].tail(n)
    low = df["low"].tail(n)
    cp = df["close"].shift(1).tail(n)
    tr = pd.concat([high - low, (high - cp).abs(), (low - cp).abs()], axis=1).max(axis=1)
    return float(tr.mean())


# ============================================================
#  信号检测
# ============================================================

ENABLE_POPUP = False          # 弹窗开关：False=关，True=开
_last_alarm: dict[str, int] = {}


def _fire(tag: str, msg: str, score: int, price: float,
          stock_name: str, code: str) -> bool:
    """弹窗告警。带股票信息的去重 key"""
    if not ENABLE_POPUP:
        return False
    alarm_key = f"{code}:{tag}"
    now_ts = int(time.time())
    if _last_alarm.get(alarm_key, 0) > now_ts - 120:
        return False
    _last_alarm[alarm_key] = now_ts

    t = datetime.now().strftime("%H:%M:%S")

    def _popup() -> None:
        try:
            import ctypes
            prefix = "sz" if code.startswith(("0", "3")) else "sh"
            body = (
                f"股票: {stock_name}({prefix}{code})\n"
                f"时间: {t}\n"
                f"价格: ¥{price:.2f}\n"
                f"信心: {score}/100\n"
                f"信号: {msg}"
            )
            ctypes.windll.user32.MessageBoxW(0, body, f"⚡ {tag} ⚡", 0)
        except Exception:
            pass

    threading.Thread(target=_popup, daemon=True).start()

    bar = "█▓▒░" * 12
    prefix = "sz" if code.startswith(("0", "3")) else "sh"
    print(f"\n\n{bar}")
    print(f"  ⚡  {tag}  ⚡")
    print(f"  {bar}")
    print(f"  时间: {t}")
    print(f"  品种: {stock_name}({prefix}{code})")
    print(f"  价格: ¥{price:.2f}")
    print(f"  信心: {score}/100")
    print(f"  信号: {msg}")
    print(f"{bar}\n")
    return True


# ============================================================
#  策略分析（RetailQuant 四个策略）
# ============================================================

def _display_width(s: str) -> int:
    """计算字符串在终端中的显示宽度（CJK/全角/emoji=2，ASCII=1）"""
    w = 0
    for ch in s:
        cp = ord(ch)
        # CJK 统一表意文字（含扩展）
        if (0x4E00 <= cp <= 0x9FFF or 0x3400 <= cp <= 0x4DBF or
            0x20000 <= cp <= 0x2A6DF or 0x2A700 <= cp <= 0x2B81F or
            0x2B820 <= cp <= 0x2CEAF or 0xF900 <= cp <= 0xFAFF or
            0x2F800 <= cp <= 0x2FA1F):
            w += 2
        # 全角字符、CJK 标点、假名、韩文
        elif (0xFF01 <= cp <= 0xFF60 or 0xFFE0 <= cp <= 0xFFE6 or
              0x3000 <= cp <= 0x303F or 0x3040 <= cp <= 0x30FF or
              0xAC00 <= cp <= 0xD7AF or 0x1100 <= cp <= 0x11FF or
              0x3130 <= cp <= 0x318F or 0xA960 <= cp <= 0xA97F or
              0xD7B0 <= cp <= 0xD7FF):
            w += 2
        # Emoji / 杂项符号
        elif (0x1F300 <= cp <= 0x1F9FF or 0x2600 <= cp <= 0x27BF or
              0x1FA00 <= cp <= 0x1FAFF or 0x2300 <= cp <= 0x23FF):
            w += 2
        # 零宽字符
        elif cp in (0x200B, 0x200C, 0x200D, 0xFEFF, 0x200E, 0x200F):
            w += 0
        else:
            w += 1
    return w


def _pad_col(s: str, width: int) -> str:
    """按显示宽度用空格右填充字符串"""
    current = _display_width(s)
    if current >= width:
        return s
    return s + " " * (width - current)


def run_strategies_for_stock(code: str, stock_name: str,
                              df: pd.DataFrame, snap: dict) -> None:
    """对单只股票运行 4 个策略 + 2 个自写检测，打印表格，仅自写检测弹窗"""
    price = snap.get("price") or float(df["close"].iloc[-1])

    # ── 收集 Row ──
    rows: list[dict] = []

    pairs = [
        ("GridMartingale",      "网格",     GRID),
        ("VpBreakout",          "量价突破", VPB),
        ("MultiFactor",         "多因子",   MULTIFACTOR),
        ("RsiMeanReversion",    "RSI回归",  RSI_REV),
    ]

    for sid, label, strategy in pairs:
        try:
            sig = strategy.signal_buy(code, stock_name, "stock", df)
            if sig is not None:
                score = int(sig.confidence)
                rows.append({"策略": label, "类别": sid, "操作": "BUY",
                            "得分": f"{score:.0f}%", "说明": sig.reason})
            else:
                rows.append({"策略": label, "类别": sid, "操作": "HOLD",
                            "得分": "—", "说明": "无信号"})
        except Exception as exc:
            rows.append({"策略": label, "类别": sid, "操作": "ERR",
                        "得分": "—", "说明": str(exc)[:40]})

    # 自写检测
    act_s, sc_ss, desc_s = _shrink_analysis(df, snap)
    rows.append({"策略": "缩量止跌", "类别": "自定义", "操作": act_s, "得分": sc_ss, "说明": desc_s})

    act_r, sc_r, desc_r = _reversal_analysis(df)
    rows.append({"策略": "反弹趋势", "类别": "自定义", "操作": act_r, "得分": sc_r, "说明": desc_r})

    # ── 预计算操作列显示文本（含 marker） ──
    for r in rows:
        op = r["操作"]
        if op == "BUY":
            r["_op_col"] = "✅ BUY"
        elif op == "ERR":
            r["_op_col"] = "⚡ ERR"
        else:
            r["_op_col"] = "   HOLD"

    # ── 打印表格（按终端显示宽度对齐） ──
    w1 = max(max(_display_width(r["策略"]) for r in rows), _display_width("策略"))
    w2 = max(max(_display_width(r["类别"]) for r in rows), _display_width("类别"))
    w3 = max(max(_display_width(r["_op_col"]) for r in rows), _display_width("操作"))
    w4 = max(max(_display_width(r["得分"]) for r in rows), _display_width("得分"))
    sep = "  " + "─" * (w1 + w2 + w3 + w4 + 30)

    # 股票标题
    prefix = "sz" if code.startswith(("0", "3")) else "sh"
    header = f"{stock_name}({prefix}{code})  ¥{price:.2f}"
    print(f"\n  {header}")
    print(sep)
    print(f"  {_pad_col('策略', w1)}  {_pad_col('类别', w2)}  {_pad_col('操作', w3)}  {_pad_col('得分', w4)}  说明")
    print(sep)
    for r in rows:
        print(f"  {_pad_col(r['策略'], w1)}  {_pad_col(r['类别'], w2)}  "
              f"{_pad_col(r['_op_col'], w3)}  {_pad_col(r['得分'], w4)}  {r['说明']}")
    print(sep)

    # ── 弹窗：仅自写检测触发 ──
    score_s = int(sc_ss.replace("%", "")) if sc_ss.endswith("%") else 0
    if act_s == "BUY" and score_s >= 55:
        _fire("缩量止跌", desc_s, score_s, price, stock_name, code)
    score_r = int(sc_r.replace("%", "")) if sc_r.endswith("%") else 0
    if act_r == "BUY" and score_r >= 50:
        _fire("反弹趋势", desc_r, score_r, price, stock_name, code)


# ============================================================
#  自定义检测：缩量止跌 + 反弹趋势（仅这两个会弹窗）
# ============================================================

def _shrink_analysis(df: pd.DataFrame, snap: dict) -> tuple[str, str, str]:
    """缩量止跌检测 — 返回 (操作, 得分, 说明)"""
    if len(df) < 20:
        return ("—", "—", "数据不足")
    close = snap.get("price") or df["close"].iloc[-1]
    vr = vol_ratio(df, 5)
    low_10 = lowest(df, 10)
    high_10 = highest(df, 10)
    rsi_val = rsi(df, 14)
    price_pos = (close - low_10) / (high_10 - low_10) if high_10 > low_10 else 0.5

    reasons: list[str] = []
    score = 0
    if vr < 0.6:  score += 30; reasons.append(f"量比{vr:.2f}")
    elif vr < 0.8: score += 20; reasons.append(f"量比{vr:.2f}")
    if price_pos < 0.20: score += 25; reasons.append(f"低位{price_pos:.0%}")
    elif price_pos < 0.35: score += 15
    if rsi_val < 28: score += 30; reasons.append(f"RSI={rsi_val:.1f}")
    elif rsi_val < 35: score += 20; reasons.append(f"RSI={rsi_val:.1f}")
    chgs = df["close"].pct_change().tail(3)
    if len(chgs) >= 3 and -0.005 < float(chgs.mean()) < 0.008:
        score += 15; reasons.append("跌幅收窄")
    bid = sum(v for v in (snap.get("bid_vols") or []) if v is not None)
    ask = sum(v for v in (snap.get("ask_vols") or []) if v is not None)
    if ask > 0 and bid / ask > 1.3:
        score += 15; reasons.append(f"买盘强{bid/ask:.1f}x")

    if score >= 55:
        return ("BUY", f"{score:.0f}%", "|".join(reasons))
    if reasons:
        return ("HOLD", f"{score:.0f}%", "|".join(reasons))
    return ("HOLD", "—", "无信号")


def _reversal_analysis(df: pd.DataFrame) -> tuple[str, str, str]:
    """反弹趋势检测 — 返回 (操作, 得分, 说明)"""
    if len(df) < 20:
        return ("—", "—", "数据不足")
    close = float(df["close"].iloc[-1])
    rsi_val = rsi(df, 14)
    ma5 = ma(df, 5)
    ma20 = ma(df, 20)
    vr = vol_ratio(df, 5)
    low_3d = lowest(df.tail(min(3, len(df))), 3) if len(df) >= 3 else lowest(df, 3)

    reasons: list[str] = []
    score = 0
    if low_3d > 0:
        r = close / low_3d - 1
        if r > 0.03: score += 30; reasons.append(f"反弹{r*100:.1f}%")
        elif r > 0.015: score += 20
    if 28 < rsi_val <= 48: score += 20; reasons.append(f"RSI={rsi_val:.1f}")
    elif rsi_val > 48: score += 10
    if close > ma5: score += 15; reasons.append(f">MA5({ma5:.2f})")
    if vr > 1.5: score += 25; reasons.append(f"量比{vr:.2f}")
    elif vr > 1.0: score += 12
    if len(df) >= 21:
        ma5_p = prev_ma(df, 5); ma20_p = prev_ma(df, 20)
        if ma5_p <= ma20_p and ma5 > ma20:
            score += 25; reasons.append("MA5金叉MA20")

    if score >= 50:
        return ("BUY", f"{score:.0f}%", "|".join(reasons))
    if reasons:
        return ("HOLD", f"{score:.0f}%", "|".join(reasons))
    return ("HOLD", "—", "无信号")


# ============================================================
#  休市休眠
# ============================================================

def sleep_until_trading() -> tuple[bool, bool]:
    """非交易时段深度休眠，返回 (刚睡醒?, 是否新的一天?)"""
    now = datetime.now()
    t = now.time()

    # ── 周末 ──
    if now.weekday() >= 5:
        days_ahead = 7 - now.weekday()
        next_open = now.replace(hour=9, minute=15, second=0, microsecond=0) + timedelta(days=days_ahead)
        sec = (next_open - now).total_seconds()
        print(f"\n  [{datetime.now().strftime('%H:%M:%S')}] 休市（周末），停止拉取")
        time.sleep(max(1, min(sec, 21600)))
        return True, True

    # ── 午间休市 11:30-13:00 ──
    if dtime(11, 30) <= t < dtime(13, 0):
        next_open = now.replace(hour=13, minute=0, second=0, microsecond=0)
        sec = (next_open - now).total_seconds()
        if sec > 0:
            print(f"\n  [{datetime.now().strftime('%H:%M:%S')}] 休市（午间），停止拉取")
            time.sleep(sec)
            return True, False

    # ── 收盘后 (>=15:00) / 盘前 (<9:15) ──
    if t >= dtime(15, 0) or t < dtime(9, 15):
        if t >= dtime(15, 0):
            next_day = now + timedelta(days=1)
            while next_day.weekday() >= 5:
                next_day += timedelta(days=1)
            next_open = next_day.replace(hour=9, minute=15, second=0, microsecond=0)
        else:
            next_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        sec = (next_open - now).total_seconds()
        if sec > 0:
            print(f"\n  [{datetime.now().strftime('%H:%M:%S')}] 休市（收盘），停止拉取")
            time.sleep(min(sec, 21600))
            return True, True

    return False, False


# ============================================================
#  主程序
# ============================================================

def main() -> None:
    """合并监控主循环：每 60s 同时分析两只股票"""
    # ── 1. 加载日线数据 ──
    stock_data: dict[str, dict] = {}
    for sc in STOCKS:
        parquet_path = _RETAIL / "data" / "parquet" / sc["parquet"]
        if not parquet_path.exists():
            print(f"  ❌ 历史数据不存在: {parquet_path}")
            return
        df = pd.read_parquet(parquet_path)
        df = df.sort_values("date").reset_index(drop=True)
        stock_data[sc["code"]] = {
            "df_daily": df,
            "name": sc["name"],
            "parquet": sc["parquet"],
            "today_open": 0.0,
            "today_high": 0.0,
            "today_low": 0.0,
            "cycle": 0,
        }
        print(f"  ✅ {sc['name']}({sc['code']}) 历史数据: {len(df)} 条")

    # ── 2. 检查 RDP 服务 ──
    try:
        r = requests.get(f"{API}/api/health", timeout=5)
        r.raise_for_status()
        print(f"  ✅ RDP 服务连接成功")
    except Exception:
        print(f"\n  ❌ RDP 服务未响应 — 请先运行:")
        print(f"     cd RealtimeDataPool; uv run python scripts/start.py serve")
        return

    # ── 3. 合并监控循环 ──
    _was_sleeping = False

    print(f"\n  {'─' * 56}")
    print(f"  合并监控：立讯精密(002475) + 大唐发电(601991)")
    print(f"  抓取间隔：{INTERVAL}s，分析间隔：{INTERVAL}s")
    print(f"  {'─' * 56}")

    while True:
        try:
            # ── 休市检查（首次循环跳过） ──
            all_first = all(sd["cycle"] == 0 for sd in stock_data.values())
            if not all_first:
                slept, new_day = sleep_until_trading()
                if slept:
                    _was_sleeping = True
                    for code, sd in stock_data.items():
                        sd["cycle"] = 0
                        if new_day:
                            sd["today_open"] = sd["today_high"] = sd["today_low"] = 0.0
                    continue
                if _was_sleeping:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始交易，开始拉取")
                    _was_sleeping = False

            # ── 批量获取快照 ──
            codes_str = ",".join(sc["code"] for sc in STOCKS)
            resp = requests.get(f"{API}/api/snapshots?codes={codes_str}", timeout=6)
            snapshots: list[dict] = resp.json()

            # 建立 code → snap 映射
            snap_map: dict[str, dict] = {s["code"]: s for s in snapshots if "code" in s}

            # ── 对每只股票分别分析 ──
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"\n{'═' * 56}")
            print(f"  [{ts}] 第 {max(sd['cycle'] for sd in stock_data.values()) + 1} 轮分析")
            print(f"{'═' * 56}")

            for sc in STOCKS:
                code = sc["code"]
                sd = stock_data[code]
                snap = snap_map.get(code)
                df = sd["df_daily"]

                sd["cycle"] += 1

                if snap is None:
                    prefix = "sz" if code.startswith(("0", "3")) else "sh"
                    print(f"\n  ⚠ {sc['name']}({prefix}{code}) 无快照数据，跳过")
                    continue

                price = snap.get("price", 0.0)

                # ── 维护当日 K 线 ──
                if sd["cycle"] == 1:
                    sd["today_open"] = snap.get("open", price)
                    sd["today_high"] = snap.get("high", price)
                    sd["today_low"] = snap.get("low", price)
                else:
                    sd["today_high"] = max(sd["today_high"], snap.get("high", price))
                    low_candidate = snap.get("low", price)
                    if low_candidate is not None and low_candidate > 0:
                        sd["today_low"] = min(sd["today_low"], low_candidate) if sd["today_low"] > 0 else low_candidate

                # ── 运行策略分析 ──
                # 拼接当天 K 线到 df 尾部
                today_date = datetime.now().strftime("%Y-%m-%d")
                new_row = pd.DataFrame([{
                    "date": today_date,
                    "open": sd["today_open"],
                    "high": sd["today_high"],
                    "low": sd["today_low"],
                    "close": price,
                    "volume": snap.get("volume", 0.0),
                    "amount": snap.get("amount", 0.0),
                }])
                df_with_today = pd.concat([df, new_row], ignore_index=True)

                run_strategies_for_stock(code, sc["name"], df_with_today, snap)

        except requests.ConnectionError:
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] ⚠ RDP 连接断开，5秒后重试...", end=" " * 20)
            time.sleep(5)
        except KeyboardInterrupt:
            print(f"\n\n  合并监控已停止。")
            break
        except Exception as exc:
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] ⚠ {exc}", end="")
            time.sleep(5)

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
