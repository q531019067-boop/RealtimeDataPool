# REQUIREMENTS

RealtimeDataPool 需求清单。一项一项列。

## 1. 数据采集

- [ ] 全 A 股 + ETF + LOF 实时报价, 5400+ 只标的
- [ ] 主源 eastmoney, 失败自动降级 sina, 再失败降级 tencent
- [ ] 单市场单源拉取 ≤ 60s (5400+ 只, 并发 8)
- [ ] 盘口五档 (bid/ask prices + vols) 单独从 tencent 拉, 独立节拍 300s
- [ ] 数据字段: price / open / high / low / prev_close / change / change_pct / volume / amount / turnover_pct / pe / pb / market_cap / float_cap

## 2. 数据存储

- [ ] SQLite + WAL 模式, 高并发读 + 单线程写
- [ ] 每只标的最新快照 + 历史快照保留 7 天
- [ ] 盘口 in-place 覆盖最新 snapshot 的 4 个字段 + `orderbook_fetched_at` 时间戳, 不重插
- [ ] instruments 表缓存股票池元信息 (code / name / market / category)
- [ ] fetch_runs 表记录每次抓取健康度 (started_at / ended_at / source / pool_size / count_ok / count_valid / count_stale / error)

## 3. 反爬

- [ ] 每请求前 jitter (默认 0-30ms 随机)
- [ ] 5xx / 连接错误自动重试 (默认 1 次, 指数退避)
- [ ] p95 延迟滑动窗口 (100 样本)
- [ ] p95 > 1.5s 触发 THROTTLE 状态, 切换 penalty jitter (200-600ms)
- [ ] p95 恢复 (回落 < 0.75s) 清除 THROTTLE
- [ ] **THROTTLE → 自动 halve 并发上限** (下限 1, 冷却 50 请求防 flapping)
- [ ] **throttle cleared → 自动 restore 到 initial 并发** (冷却 200 请求)
- [ ] 并发限流器必须运行时可调 (`_AdaptiveLimiter` 替换 `asyncio.Semaphore`)

## 4. 调度

- [ ] 启动立即 warm-up 一次 basic + 一次 orderbook
- [ ] basic 周期默认 30s
- [ ] orderbook 周期默认 300s
- [ ] 交易时段外 (盘后 / 周末) 默认停拉, 留 heartbeat 日志 (20 周期一次)
- [ ] 清理过期 snapshot 每 30 分钟一次 (不每周期跑, 防 DELETE 扫全表)

## 5. API

- [ ] `GET /api/health` — 健康检查
- [ ] `GET /api/status` — 调度器状态 + DB 统计
- [ ] `GET /api/pool` — 股票池列表 (支持 category 过滤)
- [ ] `GET /api/snapshot?code=...` — 单只最新快照
- [ ] `GET /api/snapshots?codes=a,b,c` — 多只最新快照 (≤500)
- [ ] `GET /api/snapshots/all` — 全市场 (SQL 排序 + 过滤 + limit, sort_by 白名单, 实测 5ms)
- [ ] `GET /api/history?code=...` — 单只历史
- [ ] `GET /api/runs` — 最近抓取运行日志
- [ ] Rate limit: 默认 60 req/min/IP, localhost 白名单, env `RDP_RATE_LIMIT_PER_MIN` 可覆盖 (0 关 / 600 内网)

## 6. 可观测性

- [ ] 每周期一行 `Fetch cycle:` 摘要: cycle_id / src / ok / valid / stale / ob / fetch= / db= / total= / gap= / data_age=
- [ ] 三类 WARNING:
  - SLOW: elapsed > 2.5× 间隔 (实测避免 86% 噪音, 只报真慢)
  - LOW data quality: valid < 90%
  - STALE: data_age > 60s
- [ ] 交易时段状态切换日志 (ACTIVE ↔ INACTIVE)
- [ ] 空转期 heartbeat (20 周期一次)

## 7. 数据正确性

- [ ] ETF / LOF 价格 scale 强制 1000 (不走 f152, 防 10x bug)
- [ ] 基本面字段缺失时为 None, 不为 0 (避免和真实零值混淆)
- [ ] 停牌 / 缺失 → `is_stale=True`, 不报异常
- [ ] `fetch_runs.source` 记录实际命中源 (可能 fallback 到 sina/tencent, 不只是 `sources[0]`)

## 8. 性能

- [ ] `pool.by_code(code)` O(1) 字典查找
- [ ] `storage.query_latest` 走 GROUP BY MAX(id) loose index scan (避免 correlated subquery 1.44 亿次比较)
- [ ] `/snapshots/all` 排序 + 过滤 + limit 全部 SQL 层完成 (实测 5ms, 5400 行)
- [ ] 数据库 WAL + 索引 `idx_snapshots_code_time` + `idx_snapshots_time`

## 9. ETF 价格 bug sentinel (自动检测)

- [ ] 检测价格范围 [0.1, 30]: 超出 → PRICE_HIGH / PRICE_LOW
- [ ] 检测 change_pct: abs > 50% → CHANGE_PCT_SPIKE
- [ ] 检测 price / prev_close ∉ [0.7, 1.3] → RATIO_ABNORMAL
- [ ] 检测 price=None → NULL_PRICE
- [ ] 检测 instruments 有 ETF 但 snapshots 无数据 → MISSING
- [ ] Exit 0 (OK) / 1 (异常) / 2 (配置问题)

## 10. 文档

- [ ] AGENTS.md — 项目级硬规则 + 架构速查 + 已知坑
- [ ] REQUIREMENTS.md — 本文件, 需求清单
- [ ] agents/rdp-engineer/agent.md — 工程师角色定义, 同步到 `~/.mavis/agents/rdp-engineer/agent.md`