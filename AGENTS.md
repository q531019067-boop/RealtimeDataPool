# RealtimeDataPool — Agent Guidelines

> 任何 AI agent (Mavis / OpenCode / Cursor / Codex / Aider / Devin ...) 进入这个项目，**先读这个文件**。
> 也被 `agents/rdp-engineer/agent.md` 引用。

## 硬规则（违反任何一条 = 这次工作失败）

### 1. **绝不跑 live fetch 进程**
- `python scripts/start.py serve` 启动长驻服务
- `python scripts/start.py fetch` 拉全池（12373 只，25-400s，会被 180s 超时杀）
- 任何未限量的 `run_once()` 调用
- `asyncio.run(scheduler.start())` 启动主循环
- **读** `logs/rdp.log` / `logs/serve.out.log` / `logs/serve.err.log` 获取运行时信息
- **跑** `scripts/_smoke_*.py`（**必须 ≤100 只样本，总耗时 ≤30s**）
- **跑** `pytest tests/` 单元测试
- 跑 import-only / 静态分析 / py_compile

**原因**：live 进程被超时杀会留下半完成的 live 数据，用户也会被中断。

### 2. **引用历史 log 时必须标注日期**
- "从 18:00 的 spike 看..."（不标日期就是误导）
- "从 2026-06-29 18:22-20:59 的 serve.out.log 看..."

### 3. **不要自作主张打包 / 提交 / 推送**
- 改完先 diff 给用户看
- 等用户确认后再 commit + push
- commit message 要说明**为什么改**，不只说"做了什么"

## Agent 组织

| 文件 | 作用 |
|------|------|
| `AGENTS.md` (本文件) | 项目级规则 + 入口。session 进来先看这个。 |
| `agents/rdp-engineer/agent.md` | 核心代码工程师的具体职责 + 架构知识。daemon 注入到 system prompt。 |
| `agents/<name>/` (后续) | 别的子域专家（前端、因子、部署...）有需要再开。 |

## Daemon 同步说明

mavis daemon 实际读的是全局位置 `~/.mavis/agents/rdp-engineer/agent.md`，不是项目里的 `agents/rdp-engineer/agent.md`。**改项目里那份后必须同步到 global**：

```powershell
Get-Content 'C:\Users\Administrator\Downloads\GitHub\RealtimeDataPool\agents\rdp-engineer\agent.md' -Raw -Encoding UTF8 |
    Set-Content 'C:\Users\Administrator\.mavis\agents\rdp-engineer\agent.md' -Encoding UTF8
```

（用户在 `agents/rdp-engineer/agent.md` 顶部也写了这个 reminder，下次别忘了。）

## 关键架构事实（速查）

- **基础行情 / 盘口补全解耦**（commit `ee7e2a3`）：basic 30s + orderbook 300s
- **反爬三件套**（commit `d4683f1`）：`_LatencyTracker` p95 + `_polite_get` jitter/retry + 限流状态机
- **每周期可观测性**（commit `9665172`）：cycle_id / phase 时长 / drift / data_age + 三类 WARNING
- **数据源**：eastmoney（主）→ sina（备）→ tencent（兜底 + 盘口）
- **栈**：Python 3.12 + aiohttp + FastAPI + SQLite (WAL)

## 已知遗留

- `tests/test_fetcher.py::TestTencentParser::test_basic`：**预存 test bug**（test data 写到 `fields[44]`，代码读 `fields[32]`）。**跟生产代码无关**——`git stash` 后也失败。要修等用户定。
- `PowerShell 5.1` 在 zh-CN 系统下输出 GBK 乱码：不是真错误，看英文部分。
- eastmoney 是**细水长流式限流**（HTTP 200 但悄悄丢包/排队），不是粗暴 429 封。特征：valid 数量下跌 + elapsed 持续上涨。
