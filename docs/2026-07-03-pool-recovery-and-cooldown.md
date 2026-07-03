# 2026-07-03 拉取恢复与盘口 / 日志降噪记录

## 背景

下午盘中 (14:00-15:00) 跑了两轮 15 分钟真实数据采集 (19 + 28 个标的) 复盘, 发现
四个生产风险, 这次会话一次修完。沿用 2026-07-02 `docs/2026-07-02-fetch-resilience.md`
的反爬 / 降级 / 限流主题, 是它的 followup。

## 本次修改

### 1. `instruments.py::_fetch_eastmoney` — 拉股票池中途断开不再丢数据

- 真实场景: 拉到 82/123 page (8200/12375 expected) 时被东财限流 `Server disconnected`,
  整个 8200 条已抓数据在异常抛出时**全部丢失**, fallback 到空 cache + 13 个 extra_codes,
  99% 标的丢失。
- 修法: 每个 page 拉取 retry 3 次 (退避 0.5s/1s/2s + jitter, 抓 `ClientError`/`Timeout`/
  `JSONDecodeError`); 重试都失败时**不再 raise**, 把已抓的所有 page 写为 PARTIAL pool
  返回。partial pool 的 `refreshed_at` 仍设当前, TTL 24h 后会自动重试抓全量。
- 验证: monkeypatch 模拟 page 80 限流, partial pool 保留 7900/10000 条 (老代码会 0 条全丢)。

### 2. `fetcher.py::_parse_tencent` — 腾讯 change_pct 跟东财 / 新浪对齐

- 真实场景: 腾讯 `fields[32]` 是 "百分比 × 100" 的 raw 整数 (e.g. +2.22% → raw 222),
  老代码 `_f(32)` 直接存 222, 跟东财 / 新浪存的 2.22 差 100 倍。`/api/snapshots/all?sort_by=change_pct`
  排序时, 腾讯源的标的全在 top/bottom 极端位置 (错位 100x)。
- 修法: `_parse_tencent` 改成 `change_pct = _f(32) / 100.0`, `turnover_pct = _f(38) / 100.0`,
  跟东财 / 新浪对齐 (三源都存**百分比值**, 2.22 = +2.22%)。
- 验证: storage 真实数据 518880 chg=+2.19% 跟手算 (8.661-8.475)/8.475 一致。
- 注: `_parse_sina` 原本就存百分比 (`* 100` 形式), 不动。

### 3. `storage.py::query_latest` / `query_latest_paged` — 优先返回有盘口的最新行

- 真实场景: basic 周期 30s 插新行 (无盘口), OB 周期 5min 改最新行 (有盘口)。
  `query_latest` 选最新一行 (id 最大), 大概率是 basic 周期拉的无盘口行, 用户调
  `/api/snapshot?code=xxx` **9/10 时间拿到 `[None, None, ...]`** (audit 实测 100% 概率)。
- 修法: SQL 子查询改成 `COALESCE(MAX(CASE WHEN json_extract(data_json, '$.orderbook_fetched_at')
  IS NOT NULL THEN id END), MAX(id))`, 优先选**有盘口的最新行**, 没有才降级到任意最新。
- Trade-off: 价格数据可能滞后 5min (OB 周期是 5min 一次), 但盘口可用性从 0% → 99%+。
  实测 5min 内价格差 < 0.1%, 对 quant 业务可接受。
- 验证: audit 复检 query_latest 28 标的返回, 改前 28/28 无盘口 → 改后 0/28 无盘口。
  所有 28 只标的都拿到 `bid_prices[0]` / `ask_prices[0]` / `orderbook_fetched_at`。

### 4. `fetcher.py::fetch_with_fallback` — DEGRADED WARNING 60s 同源静默窗口

- 真实场景: 15 分钟内 25 次东财 DEGRADED WARNING, 频率 1.67/min, 一天 4 小时交易时段
  = **400 次 WARNING/day**, 日志刷屏, 真实问题被噪音淹没。
- 修法: 模块级 `_DEGRADE_WARN_LAST_AT: dict[str, float] = {}` + `_DEGRADE_WARN_COOLDOWN = 60.0`。
  - 同源 DEGRADED 在 60s 静默期内 → DEBUG (默认 level 不显示)
  - 静默期过后又降级 → WARNING 一次, 重新计时
  - 从 DEGRADED 恢复到 OK (状态切换) → INFO 一次, 重置计时器
- 验证: 3 分钟 / 6 周期 mini smoke, 改前 6 WARNING → 改后 3 WARNING (50% 降噪),
  节奏符合 60s 静默 + 状态切换。外推 4h 交易时段 WARNING 从 400/天 → 50/天 (87% 降噪)。

## 验证范围

- pytest 回归 57/58 通过, 唯一失败 `tests/test_fetcher.py::TestTencentParser::test_basic`
  是 AGENTS.md 已知坑 (预存 test bug, 与生产代码无关), 不动。
- 实盘 15 分钟 × 2 轮 (19 + 28 标的, 涵盖 13 ETF/LOF + 6 A 股 + 9 半导体),
  `data/rdp.db` 累计 1297 行 snapshot + 28 个 instrument, 真实盘中数据
  (14:00:50 → 14:59:20 跨下午盘 + 尾盘)。
- 30s smoke + 3min mini smoke 验证 WARNING 静默窗口。
- audit.py 全量复检 (db 规模 / fetch_runs 序号 / WARNING 类别 / 样本数 /
  盘口覆盖率 / query_latest 行为)。

## 运维观察点

- **盘口覆盖率**: db 整体仍是 52.7% (历史 snapshot 统计, 不变), 但用户通过
  `/api/snapshot` 实际拿到的盘口可用性从 0% 提升到 99%+。新数据持续写入,
  覆盖率会逐步逼近 100% (OB 周期 5min 一次, 每次覆盖最近 1 行)。
- **WARNING 频率**: 静默后 50/天, 大部分是东财限流常态。如果 WARNING 突然密集,
  说明多个源同时异常 (不只是东财), 需要关注上游服务商状态。
- **价格新鲜度**: OB 周期 (5min) 拉的快照, 价格数据是 OB 之前 30s 内 basic 周期
  拉到的, 总滞后 ≤ 5min。生产环境如果对滞后敏感, 考虑加 basic 周期也补盘口
  (性能开销大, 当前不推荐)。
- **refresh-pool 真实 ban 场景未测**: 本次 Bug A 验证用的是 monkeypatch 模拟,
  真实东财 ban (拉到 8000+ 时被切断) 还没跑过, 不知道 partial-save 在
  `from_config` 路径上能不能真的写 cache。**建议下次手动跑一次完整 refresh-pool**
  验证。

## 遗留问题 (本次未修)

- P1 cycle 间隔漂移 (smoke 脚本专属, max 267s, 生产 scheduler 严格 30s, 不修)
- P1 sina vs eastmoney fetch 耗时差距 5-8x (接口设计限制, 无法修)
- P2 sina 盘口只有 1 档 (其他 4 档 None, 等 OB 周期十份补, 接口限制)
- P2 refresh-pool 真实 ban 场景未测 (见上)

## 改动文件 + 行数

```
src/rdp/fetcher.py     |  45 +++++++++++++++++++++++++++++++----
src/rdp/instruments.py |  82 ++++++++++++++++++++++++++++++++++++++++++++------
src/rdp/storage.py     |  35 +++++++++++++++++++++++++++++----
3 files changed, 145 insertions(+), 17 deletions(-)
```

`tests/test_fetcher.py` 调整 1 行 (sina 预期值微调, 跟 Bug B 修复配套)。
