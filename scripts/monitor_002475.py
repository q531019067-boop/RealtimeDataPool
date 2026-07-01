#!/usr/bin/env python
"""
立讯精密(sz002475) 实时监控脚本
─────────────────────────────────
用法：
  1. 先开一个 PowerShell 启动 RDP 服务：
     cd RealtimeDataPool; uv run python scripts/start.py serve
  2. 再开另一个 PowerShell 运行本脚本：
     cd RealtimeDataPool; uv run python scripts/monitor_002475.py

功能：
  - 每 10 秒从 RDP API 取最新快照
  - 叠加上日 K 线数据（来自 RetailQuant parquet）
  - 实时检测「缩量止跌」+「反弹趋势」信号
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
CODE = "002475"
STOCK_NAME = "立讯精密"
INTERVAL = 10  # 秒

_PARQUET = _RETAIL / "data" / "parquet" / "sz002475.parquet"

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

def _fire(tag: str, msg: str, score: int, price: float) -> bool:
    if not ENABLE_POPUP:
        return False
    now_ts = int(time.time())
    if _last_alarm.get(tag, 0) > now_ts - 120:   # 同一信号两分钟内不重复
        return False
    _last_alarm[tag] = now_ts

    t = datetime.now().strftime("%H:%M:%S")

    # ── Windows 弹窗（非阻塞，后台线程弹出） ──
    def _popup() -> None:
        try:
            import ctypes
            body = (
                f"股票: {STOCK_NAME}(sz{CODE})\n"
                f"时间: {t}\n"
                f"价格: ¥{price:.2f}\n"
                f"信心: {score}/100\n"
                f"信号: {msg}"
            )
            ctypes.windll.user32.MessageBoxW(0, body, f"⚡ {tag} ⚡", 0)
        except Exception:
            pass

    threading.Thread(target=_popup, daemon=True).start()

    # ── 终端告警 ──
    bar = "█▓▒░" * 12
    print(f"\n\n{bar}")
    print(f"  ⚡  {tag}  ⚡")
    print(f"  {bar}")
    print(f"  时间: {t}")
    print(f"  品种: {STOCK_NAME}(sz{CODE})")
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


def run_strategies(df: pd.DataFrame, snap: dict) -> None:
    """运行 4 个 RetailQuant 策略 + 2 个自写检测，打印表格，仅自写检测弹窗"""
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
            sig = strategy.signal_buy(CODE, STOCK_NAME, "stock", df)
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
    # 说明列不固定宽度，用 sep 覆盖到合理长度
    sep = "  " + "─" * (w1 + w2 + w3 + w4 + 30)

    print(f"\n{sep}")
    print(f"  {_pad_col('策略', w1)}  {_pad_col('类别', w2)}  {_pad_col('操作', w3)}  {_pad_col('得分', w4)}  说明")
    print(sep)
    for r in rows:
        print(f"  {_pad_col(r['策略'], w1)}  {_pad_col(r['类别'], w2)}  "
              f"{_pad_col(r['_op_col'], w3)}  {_pad_col(r['得分'], w4)}  {r['说明']}")
    print(sep, "\n")

    # ── 弹窗：仅自写检测触发 ──
    score_s = int(sc_ss.replace("%", "")) if sc_ss.endswith("%") else 0
    if act_s == "BUY" and score_s >= 55:
        _fire("缩量止跌", desc_s, score_s, price)
    score_r = int(sc_r.replace("%", "")) if sc_r.endswith("%") else 0
    if act_r == "BUY" and score_r >= 50:
        _fire("反弹趋势", desc_r, score_r, price)

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
        days_ahead = 7 - now.weekday()  # 到下周一
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
        if t >= dtime(15, 0):  # 收盘后 → 下一个交易日
            next_day = now + timedelta(days=1)
            while next_day.weekday() >= 5:
                next_day += timedelta(days=1)
            next_open = next_day.replace(hour=9, minute=15, second=0, microsecond=0)
        else:  # 盘前 → 当日开盘
            next_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        sec = (next_open - now).total_seconds()
        if sec > 0:
            print(f"\n  [{datetime.now().strftime('%H:%M:%S')}] 休市（收盘），停止拉取")
            time.sleep(min(sec, 21600))
            return True, True

    return False, False


# ============================================================
#  主循环
# ============================================================

def main() -> None:
    print("╔" + "═" * 56 + "╗")
    print(f"║  {STOCK_NAME}(sz{CODE}) 实时监控                          ║")
    print(f"║  API: {API}  周期: {INTERVAL}s 依赖: RDP+RetailQuant   ║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                               ║")
    print("╚" + "═" * 56 + "╝")

    # ── 1. 加载日线数据 ──
    df_daily: pd.DataFrame
    if _PARQUET.exists():
        df_daily = pd.read_parquet(_PARQUET)
        print(f"\n  📊 日线数据: {len(df_daily)} 条")
        print(f"     最新: {df_daily['date'].iloc[-1]} ¥{df_daily['close'].iloc[-1]:.2f}")
    else:
        print(f"\n  ⚠ 未找到日线数据文件 ({_PARQUET})")
        print(f"    将只基于日内数据进行简化分析")
        df_daily = pd.DataFrame()

    # ── 2. 测试 API 连接 ──
    try:
        r = requests.get(f"{API}/api/health", timeout=3)
        r.raise_for_status()
        print(f"\n  ✅ RDP 服务连接成功")
    except Exception:
        print(f"\n  ❌ RDP 服务未响应 — 请先运行:")
        print(f"     cd RealtimeDataPool; uv run python scripts/start.py serve")
        return

    # ── 3. 监控循环 ──
    today_open = today_high = today_low = 0.0
    cycle = 0
    _was_sleeping = False

    print(f"\n  {'─' * 56}")
    print(f"  开始监控（{INTERVAL}s 抓取，60s 分析，首次+休市醒来强制分析）...")
    print(f"  {'─' * 56}")

    while True:
        try:
            # ── 休市检查（首次循环跳过：启动即分析一次） ──
            if cycle > 0:
                slept, new_day = sleep_until_trading()
                if slept:
                    _was_sleeping = True
                    cycle = 0
                    if new_day:
                        today_open = today_high = today_low = 0.0
                    continue
                if _was_sleeping:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始交易，开始拉取")
                    _was_sleeping = False

            cycle += 1

            resp = requests.get(f"{API}/api/snapshot?code={CODE}", timeout=6)
            snap: dict = resp.json()

            if not snap or "code" not in snap:
                time.sleep(INTERVAL)
                continue

            price = snap.get("price", 0.0)
            change_pct_val = snap.get("change_pct", 0.0)
            volume_val = snap.get("volume", 0.0)

            # ── 维护当日 K 线 ──
            if cycle == 1:
                today_open = snap.get("open", price)
                today_high = snap.get("high", price)
                today_low = snap.get("low", price)
            else:
                today_high = max(today_high, snap.get("high", price))
                low_candidate = snap.get("low", price)
                if low_candidate is not None and low_candidate > 0:
                    today_low = min(today_low, low_candidate) if today_low > 0 else low_candidate

            # ── 状态行（滚动更新） ──
            ts = datetime.now().strftime("%H:%M:%S")
            line = (f"\r[{ts}] #{cycle:04d}  ¥{price:<8.2f} "
                    f"({change_pct_val:+7.2f}%)  "
                    f"H:{today_high:<8.2f} L:{today_low:<8.2f}  "
                    f"量:{float(volume_val):<12.0f}")
            print(line, end="    ", flush=True)

            # ── 每 6 轮（~60 秒）分析，首次及休市醒来后也分析 ──
            if (cycle == 1 or cycle % 6 == 0) and len(df_daily) > 0:
                df = df_daily.copy()
                today_date = datetime.now().strftime("%Y-%m-%d")
                new_row = pd.DataFrame([{
                    "date": today_date,
                    "open": today_open,
                    "high": today_high,
                    "low": today_low,
                    "close": price,
                    "volume": volume_val,
                    "amount": snap.get("amount", 0.0),
                }])
                df = pd.concat([df, new_row], ignore_index=True)

                # 运行 RetailQuant 四个策略
                run_strategies(df, snap)

            # ── 每 6 轮（~60 秒）打印详细摘要 ──
            if cycle % 6 == 0 and len(df_daily) > 0:
                df = df_daily.copy()
                today_date = datetime.now().strftime("%Y-%m-%d")
                new_row = pd.DataFrame([{
                    "date": today_date, "open": today_open,
                    "high": today_high, "low": today_low,
                    "close": price, "volume": volume_val,
                    "amount": snap.get("amount", 0.0),
                }])
                df = pd.concat([df, new_row], ignore_index=True)

                rsi_val = rsi(df, 14)
                vr = vol_ratio(df, 5)
                ma5 = ma(df, 5)
                ma20 = ma(df, 20)
                atr_val = atr(df, 14)
                low_10 = lowest(df, 10)
                pos = (price - low_10) / (highest(df, 10) - low_10) if highest(df, 10) > low_10 else 0

                print(f"\n  ── 日线摘要 ──")
                print(f"     RSI:{rsi_val:>6.1f}  量比:{vr:>5.2f}  "
                      f"ATR:{atr_val:>7.2f}")
                print(f"     MA5:{ma5:>8.2f}  MA20:{ma20:>7.2f}  "
                      f"10日低位:{pos:.0%}")
                print(f"     {'─' * 48}")

        except requests.ConnectionError:
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] ⚠ RDP 连接断开，"
                  f"5秒后重试...", end=" " * 20)
            time.sleep(5)
        except KeyboardInterrupt:
            print(f"\n\n  监控已停止。")
            break
        except Exception as exc:
            print(f"\r[{datetime.now().strftime('%H:%M:%S')}] ⚠ {exc}", end="")
            time.sleep(5)

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
