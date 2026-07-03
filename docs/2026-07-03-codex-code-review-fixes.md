# 2026-07-03 Codex 代码审查修复记录

> 本次修改由 **OpenAI Codex** 完成。未执行 commit 或 push；提交与推送由仓库维护者完成。

## 修改背景

对行情抓取、SQLite 最新快照、股票池缓存、调度退出和 API 静态页面路径进行代码审查后，确认存在数据单位错误、旧价格冒充最新行情、空缓存覆盖健康股票池等问题。本次修改直接修复这些问题并增加回归测试。

## 修改事项

1. **修正腾讯行情字段单位**
   - `change_pct`、`turnover_pct` 使用接口已返回的百分比值，不再二次除以 100。
   - 成交额由万元转换为元；流通/总市值由亿元转换为元。
   - 新增腾讯行情时间戳解析，并记录 `orderbook_fetched_at`。

2. **保持最新价格，同时合并最近盘口**
   - `query_latest` 和 `query_latest_paged` 始终选最新基础行情行。
   - 买卖五档从最近带盘口的行合并，不再为了盘口返回旧价格、旧 `fetched_at` 或旧 `is_stale`。

3. **保护股票池缓存**
   - `InstrumentPool` 新增向后兼容的 `is_partial` 标记。
   - 少于 1000 个标的、空池和残缺池只使用 5 分钟短 TTL。
   - 新抓取的 partial/empty 结果不再覆盖更完整的旧缓存。

4. **为最终缺失标的写入 stale 快照**
   - 多源抓取后仍缺失的代码会生成 `is_stale=True` 占位快照，避免 API 继续暴露旧行情。

5. **修复自适应并发状态机**
   - 首次检测到高 p95 时立即将并发减半。
   - 持续高 p95 时，按 50 请求冷却间隔继续降级。
   - 修复 waiter 被唤醒后取消导致的并发 slot 泄漏。

6. **修复服务退出流程**
   - uvicorn 退出后不再等待无限运行的 scheduler task。
   - `finally` 中显式停止并取消后台任务，然后关闭 SQLite 连接。

7. **修复监控页面路径**
   - API 的 `WEB_DIR` 从错误的 `src/web` 改为项目根目录 `web`。

8. **补充测试并清理静态检查问题**
   - 新增腾讯真实字段、最新价格/盘口合并、partial cache、stale 占位、自适应限流和页面路径测试。
   - 修复原腾讯解析测试夹具，并清理 Ruff 报告的机械性问题。

## 兼容性

- SQLite schema 没有变化，无需迁移数据库。
- 新生成的 Quote JSON 增加 `orderbook_fetched_at`，旧数据仍可读取。
- 股票池缓存 JSON 增加 `is_partial`；旧缓存没有该字段时按完整缓存处理，但空旧缓存会自动识别为 partial。

## 验证

- `uv run pytest -q`：66 passed。
- `uv run ruff check src tests`：通过。
- 未运行 `serve`、`fetch`、scheduler live 进程或未限量 `run_once()`。

## 建议 PR 文案

### 标题

`[Codex] fix: 修复行情一致性、股票池缓存与服务退出`

### 正文

> 本 PR 由 **OpenAI Codex** 修改并准备。
>
> 修改事项：
> - 修正腾讯涨跌幅、换手率、成交额、市值和时间戳解析；
> - 最新基础行情与最近盘口分离合并，避免返回旧价格；
> - partial/empty 股票池使用短 TTL，且不覆盖健康缓存；
> - 缺失标的写入 stale 占位快照；
> - 修复自适应并发首次不降级及取消时 slot 泄漏；
> - 修复 uvicorn/scheduler 退出等待问题；
> - 修复监控页面目录；
> - 新增回归测试并清理 Ruff 问题。
>
> 验证：`uv run pytest -q`（66 passed），`uv run ruff check src tests`（通过）。
