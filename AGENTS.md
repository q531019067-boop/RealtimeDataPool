# RealtimeDataPool — Agent Guidelines

项目级入口。session 进来先读这个。

## 硬规则

1. **绝不跑 live 进程**:`scripts/start.py serve|fetch`、`asyncio.run(scheduler.start())`、未限量的 `run_once()`。
   只读 `logs/*.log`、跑 pytest、跑 ≤30s 的 smoke。
2. **log 必须标日期**: `从 2026-06-29 18:22-20:59 的 serve.out.log 看…`, 不标就是误导。
3. **不擅自 commit/push**: 改完先 diff 给用户看,确认后再 commit。commit message 说明**为什么**。

## 架构速查

- **栈**: Python 3.12 + aiohttp + FastAPI + SQLite (WAL)
- **数据源**: eastmoney (主) → sina (备) → tencent (兜底 + 盘口)
- **节拍**: basic 30s + orderbook 300s (解耦, tencent 请求量 -90%)
- **反爬**: jitter + retry + p95 限流检测 + 自适应并发降级
- **可观测性**: 每周期一行 `Fetch cycle:` 摘要 + 三类 WARNING (SLOW / LOW data / STALE)
- **关键文件**: `src/rdp/{scheduler, fetcher, storage, api, instruments, cli}.py`、`tests/`、`config.yaml`、`AGENTS.md`、`REQUIREMENTS.md`

## 已知坑

- `tests/test_fetcher.py::TestTencentParser::test_basic` — 预存 test bug, 与生产代码无关。
- eastmoney 限流是细水长流式 (HTTP 200 但悄悄丢包/排队), 不是粗暴 429。特征: valid 数量下跌 + elapsed 持续上涨。
- `instruments.py` 把北交所 (83*/87*/92*) 误归 sz — 数据能拉对, ROI 低, 先不动。
- `web/` 前端非本仓库 own。