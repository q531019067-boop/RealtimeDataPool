---
name: RealtimeDataPool
description: A 股 30s 级全市场实时盯盘数据池。多源热备（eastmoney/sina/tencent），盘口与基础行情解耦，本地监控页面 + REST API。
---

# RealtimeDataPool — 项目身份

本项目的 reins 由 `.harness/reins/` 管理。每个 rein 是一个聚焦子域的 agent。

## 子 reins
- `rdp-engineer`：核心代码工程师。**绝不可启动 live fetch 进程**（必须读 log 拿运行时信息）。

## 项目根目录
`C:\Users\Administrator\Downloads\GitHub\RealtimeDataPool`

## 关键事实
- 仓库：https://github.com/GGG1235/RealtimeDataPool
- 栈：Python 3.12 + aiohttp + FastAPI + SQLite (WAL)
- 周期：basic 30s / orderbook 300s
- 数据源：eastmoney（主）→ sina（备）→ tencent（兜底 + 盘口）
- 已知预存 test bug：`tests/test_fetcher.py::TestTencentParser::test_basic`（fields[44] vs fields[32]）
