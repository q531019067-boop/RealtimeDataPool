# RealtimeDataPool

> A 股实时盯盘数据池 — 30 秒级全市场快照，多源热备，REST 查询 + 实时监控页面。

完全独立实现，零商业数据源依赖（东方财富 / 新浪 / 腾讯 三源热备），可作为 FactorQ / RetailQuant 等量化项目的实时数据底座。

---

## ✨ 特性

- **30 秒级全市场快照**：约 5400 只沪深 A 股 + ETF/LOF，每 30s 抓取一次
- **三源热备**：东方财富（主源，字段最全含买卖五档）→ 新浪（备源）→ 腾讯（兜底）
- **数据完整**：价 / 量 / 额 / 涨跌 / 换手 / 估值 / 市值 / 买卖五档（主源）/ 抓取时间戳
- **REST 查询**：FastAPI 暴露 `/api/snapshot`、`/api/snapshots`、`/api/history`、`/api/runs`、`/api/pool` 等
- **可视化监控**：暗色行情软件风格页面，支持搜索 / 排序 / 涨跌幅过滤 / 单只详情（含盘口 + 历史）
- **零配置部署**：SQLite 单文件，启动即可

---

## 📂 项目结构

```
RealtimeDataPool/
├── src/rdp/
│   ├── instruments.py    # 股票池管理（沪深 A 股 + ETF）
│   ├── fetcher.py        # 三源抓取器 + 解析逻辑
│   ├── storage.py        # SQLite 存储
│   ├── scheduler.py      # 30s 调度器 + 交易时段判断
│   ├── api.py            # FastAPI REST
│   └── cli.py            # CLI 入口
├── web/
│   ├── index.html        # 监控页面
│   ├── style.css         # 暗色行情风格
│   └── app.js            # 前端逻辑
├── tests/                # 单元测试
├── data/                 # 运行期数据（缓存 + 数据库）
├── logs/                 # 日志
├── config.yaml           # 配置
├── pyproject.toml
└── README.md
```

---

## ⚡ 快速开始

### 1. 安装依赖

```bash
# 用 uv（推荐）
uv sync

# 或 pip
pip install -e .
```

### 2. 首次运行：拉股票池

```bash
# 跨平台
python scripts/start.py refresh-pool

# Windows
rdp.bat refresh-pool

# Linux/Mac
./rdp.sh refresh-pool
```

> 首次会从东方财富拉全市场股票列表（约 5400 只 + ETF），缓存到 `data/instruments_cache.json`。

### 3. 启动服务（调度器 + API）

```bash
python scripts/start.py serve
# 浏览器打开 http://localhost:5080
```

### 4. 其它常用命令

```bash
python scripts/start.py fetch           # 单次抓取（不入库？默认入库）
python scripts/start.py status          # 查看状态
python scripts/start.py --help          # 所有命令
```

---

## 🔌 与 FactorQ 集成

FactorQ 的 `advisor/signal_scanner.py` 和 `ondemand_analyzer.py` 现在每次都要现拉腾讯接口实时价。可以替换为查本服务的 `/api/snapshot`：

```python
import requests

def live_price(code: str) -> float | None:
    """替换原来的腾讯接口调用。"""
    try:
        r = requests.get(f"http://localhost:5080/api/snapshot?code={code}", timeout=2)
        r.raise_for_status()
        return r.json().get("price")
    except Exception:
        return None
```

需要批量时：

```python
def live_prices(codes: list[str]) -> dict[str, float]:
    r = requests.get(
        "http://localhost:5080/api/snapshots",
        params={"codes": ",".join(codes)},
        timeout=5,
    )
    return {x["code"]: x["price"] for x in r.json() if x.get("price")}
```

---

## 📡 API 端点

| 端点 | 说明 |
| --- | --- |
| `GET /` | 监控页面 |
| `GET /api/health` | 健康检查 |
| `GET /api/status` | 调度器 + DB 状态 |
| `GET /api/pool?category=stock` | 股票池列表 |
| `GET /api/snapshot?code=000001` | 单只最新快照 |
| `GET /api/snapshots?codes=000001,600000` | 多只最新快照（≤500 只） |
| `GET /api/snapshots/all?sort_by=change_pct&order=desc&min_change_pct=5` | 全市场排序 + 过滤 |
| `GET /api/history?code=000001&limit=100` | 单只历史快照 |
| `GET /api/runs` | 抓取运行日志 |

完整字段见 `Quote` 数据类（约 25 个字段）。

---

## ⚙️ 配置

`config.yaml` 主要字段：

```yaml
pool:
  fetch_interval_sec: 30              # 抓取周期（30s 级别）
  fetch_out_of_session: false         # 非交易时段是否继续抓
  sources: [eastmoney, sina, tencent] # 主备顺序
  concurrency_per_source: 8           # 每源并发上限
  batch_size: 100

instruments:
  include_all_a_share: true
  include_etf: true
  extra_codes: []                     # 手工追加
  exclude_codes: []                   # 排除

storage:
  db_path: data/rdp.db
  retention_days: 7                   # 快照保留天数

api:
  host: 0.0.0.0
  port: 5080
```

---

## 🧪 测试

```bash
python -m pytest tests/ -v
```

覆盖：交易时段判断、Instrument 模型、SQLite 存储、Sina/Tencent 字段解析。

---

## 🛠️ 设计要点

- **多源降级**：主源成功率 < 70% 自动降级；多源全挂时保留空快照 + is_stale=True，下游可识别。
- **WAL 模式**：SQLite WAL，单线程写 + 多线程读，写入 5400 条/30s 无压力。
- **JSON 存盘口**：买卖五档放 data_json，避免 10+ 列稀疏表。
- **缓存股票池**：东方财富拉一次缓存 24h，避免每次启动都触发反爬。
- **交易时段判断**：默认非交易时段不抓（节省带宽）；可配 `fetch_out_of_session: true` 强制抓。

---

## 🪪 License

MIT