// RealtimeDataPool 监控页面 JS
// 30s 自动刷新 · 排序 · 过滤 · 搜索 · 详情面板

const REFRESH_MS = 30 * 1000;
const API = {
  status: () => fetch('/api/status').then(r => r.json()),
  snapshot: code => fetch(`/api/snapshot?code=${code}`).then(r => r.json()),
  snapshots: codes => fetch(`/api/snapshots?codes=${codes.join(',')}`).then(r => r.json()),
  snapshotsAll: () => fetch('/api/snapshots/all?limit=10000').then(r => r.json()),
  history: code => fetch(`/api/history?code=${code}&limit=50`).then(r => r.json()),
};

const state = {
  data: [],         // 全部快照
  pool: [],         // 股票池（带 name）
  poolMap: {},      // code -> {name, market, category}
  sortBy: 'change_pct',
  sortOrder: 'desc',
  filter: { text: '', category: '', minPct: null, maxPct: null, onlyActive: false },
  refreshTimer: null,
  lastRefreshAt: 0,
};

// ---------- 工具函数 ----------
const fmt = {
  num(v, digits = 2) {
    if (v == null || isNaN(v)) return '—';
    return Number(v).toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
  },
  pct(v) {
    if (v == null) return '—';
    const sign = v > 0 ? '+' : '';
    return `${sign}${v.toFixed(2)}%`;
  },
  amount(v) {
    if (v == null) return '—';
    const yi = v / 1e8;
    if (Math.abs(yi) >= 1) return `${yi.toFixed(2)}亿`;
    return `${(v / 1e4).toFixed(2)}万`;
  },
  cap(v) {
    if (v == null) return '—';
    return `${(v / 1e8).toFixed(2)}亿`;
  },
  hand(v) {
    if (v == null) return '—';
    return Math.round(v).toLocaleString('zh-CN');
  },
  time(ts) {
    if (!ts) return '—';
    const d = new Date(ts * 1000);
    return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}`;
  },
};

function colorClass(v, reverse = false) {
  if (v == null || v === 0) return 'flat';
  if (reverse) return v > 0 ? 'down' : 'up';
  return v > 0 ? 'up' : 'down';
}

// ---------- 数据加载 ----------
async function loadPool() {
  const rows = await fetch('/api/pool').then(r => r.json());
  state.pool = rows;
  state.poolMap = {};
  rows.forEach(r => { state.poolMap[r.code] = r; });
}

async function loadAllSnapshots() {
  const [status, snapshots] = await Promise.all([
    API.status().catch(() => null),
    API.snapshotsAll().catch(() => []),
  ]);
  state.data = snapshots;
  state.lastRefreshAt = Date.now();
  // 合并 pool 名称（API 已含 name，但 pool 信息更全）
  snapshots.forEach(s => {
    const p = state.poolMap[s.code];
    if (p) {
      s.market = p.market;
      s.category = p.category;
      if (!s.name) s.name = p.name;
    }
  });
  updateStats(status);
  render();
}

function updateStats(status) {
  if (!status) return;
  document.getElementById('stat-pool').textContent = status.instruments ?? '—';
  document.getElementById('stat-valid').textContent =
    state.data.filter(s => s.price != null).length;
  document.getElementById('stat-stale').textContent =
    state.data.filter(s => s.is_stale).length;
  if (status.last_run && status.last_run.started_at) {
    document.getElementById('stat-updated').textContent =
      fmt.time(status.last_run.started_at);
    document.getElementById('stat-source').textContent = status.last_run.source || '—';
  }
  const runBadge = document.getElementById('stat-running');
  if (status.running) {
    runBadge.textContent = '运行中';
    runBadge.className = 'badge ok';
  } else {
    runBadge.textContent = '已停止';
    runBadge.className = 'badge err';
  }
  // 下次刷新倒计时
  updateCountdown();
}

function updateCountdown() {
  const el = document.getElementById('stat-next');
  const remain = Math.max(0, REFRESH_MS - (Date.now() - state.lastRefreshAt));
  el.textContent = `${Math.ceil(remain / 1000)}s`;
}

// ---------- 渲染 ----------
function applyFilter(data) {
  const f = state.filter;
  return data.filter(r => {
    if (f.text) {
      const t = f.text.toLowerCase();
      if (!r.code.includes(t) && !(r.name || '').toLowerCase().includes(t)) return false;
    }
    if (f.category && r.category !== f.category) return false;
    const pct = r.change_pct;
    if (f.minPct != null && (pct == null || pct < f.minPct)) return false;
    if (f.maxPct != null && (pct == null || pct > f.maxPct)) return false;
    if (f.onlyActive && (r.price == null || r.is_stale)) return false;
    return true;
  });
}

function applySort(data) {
  const key = state.sortBy;
  const desc = state.sortOrder === 'desc';
  return [...data].sort((a, b) => {
    let va = a[key], vb = b[key];
    if (va == null && vb == null) return 0;
    if (va == null) return desc ? 1 : -1;
    if (vb == null) return desc ? -1 : 1;
    return desc ? vb - va : va - vb;
  });
}

function renderRow(r) {
  const tr = document.createElement('tr');
  tr.dataset.code = r.code;

  const cls = colorClass(r.change_pct);
  tr.innerHTML = `
    <td>${r.code}</td>
    <td>${r.name || '—'}${r.is_stale ? ' <span class="muted">[停]</span>' : ''}</td>
    <td class="right ${cls}">${fmt.num(r.price)}</td>
    <td class="right ${cls}">${fmt.pct(r.change_pct)}</td>
    <td class="right ${cls}">${r.change != null ? (r.change >= 0 ? '+' : '') + fmt.num(r.change) : '—'}</td>
    <td class="right">${fmt.num(r.open)}</td>
    <td class="right">${fmt.num(r.high)}</td>
    <td class="right">${fmt.num(r.low)}</td>
    <td class="right">${fmt.num(r.prev_close)}</td>
    <td class="right">${fmt.hand(r.volume)}</td>
    <td class="right">${fmt.amount(r.amount)}</td>
    <td class="right">${r.turnover_pct != null ? r.turnover_pct.toFixed(2) + '%' : '—'}</td>
    <td class="right">${r.pe != null ? fmt.num(r.pe) : '—'}</td>
    <td class="right">${fmt.cap(r.market_cap)}</td>
    <td class="right">${formatBidAsk(r)}</td>
    <td class="orderbook-cell">${formatOrderMini(r)}</td>
  `;
  tr.addEventListener('click', () => showDetail(r.code));
  return tr;
}

function formatBidAsk(r) {
  const bp = r.bid_prices?.[0], ap = r.ask_prices?.[0];
  return `<span class="up">${fmt.num(bp)}</span> / <span class="down">${fmt.num(ap)}</span>`;
}

function formatOrderMini(r) {
  let lines = [];
  for (let i = 4; i >= 0; i--) {
    const p = r.bid_prices?.[i], v = r.bid_vols?.[i];
    lines.push(`<div class="bid">B${i+1} ${fmt.num(p, 3)} × ${fmt.hand(v)}</div>`);
  }
  for (let i = 0; i < 5; i++) {
    const p = r.ask_prices?.[i], v = r.ask_vols?.[i];
    lines.push(`<div class="ask">S${i+1} ${fmt.num(p, 3)} × ${fmt.hand(v)}</div>`);
  }
  return `<div class="order-mini">${lines.join('')}</div>`;
}

function render() {
  const filtered = applyFilter(state.data);
  const sorted = applySort(filtered);
  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '';
  const frag = document.createDocumentFragment();
  // 限制渲染数量防止卡顿
  const MAX_ROWS = 1000;
  const rows = sorted.slice(0, MAX_ROWS);
  rows.forEach(r => frag.appendChild(renderRow(r)));
  tbody.appendChild(frag);

  document.getElementById('shown-count').textContent = filtered.length;
  document.getElementById('total-count').textContent = state.data.length;
  document.querySelectorAll('th').forEach(th => {
    th.classList.toggle('sorted', th.dataset.sort === state.sortBy);
  });
}

// ---------- 详情面板 ----------
async function showDetail(code) {
  const panel = document.getElementById('detail-panel');
  panel.classList.remove('hidden');

  document.getElementById('detail-code').textContent = code;
  document.getElementById('detail-name').textContent = '加载中…';
  document.getElementById('detail-quote').innerHTML = '';
  document.getElementById('detail-orderbook').innerHTML = '';
  document.getElementById('detail-history').innerHTML = '';

  const [snap, hist] = await Promise.all([
    API.snapshot(code).catch(() => null),
    API.history(code).catch(() => []),
  ]);
  if (!snap) {
    document.getElementById('detail-name').textContent = '无数据';
    return;
  }

  document.getElementById('detail-name').textContent = snap.name || code;
  const kv = [
    ['最新价', fmt.num(snap.price)], ['涨跌幅', fmt.pct(snap.change_pct)], ['涨跌额', fmt.num(snap.change)],
    ['今开', fmt.num(snap.open)], ['昨收', fmt.num(snap.prev_close)], ['最高', fmt.num(snap.high)], ['最低', fmt.num(snap.low)],
    ['成交量', fmt.hand(snap.volume) + ' 手'], ['成交额', fmt.amount(snap.amount)], ['换手率', snap.turnover_pct != null ? snap.turnover_pct.toFixed(2) + '%' : '—'],
    ['市盈率', fmt.num(snap.pe)], ['市净率', fmt.num(snap.pb)], ['总市值', fmt.cap(snap.market_cap)], ['流通市值', fmt.cap(snap.float_cap)],
    ['来源', snap.source], ['抓取时间', fmt.time(snap.fetched_at)],
  ];
  document.getElementById('detail-quote').innerHTML = kv
    .map(([k, v]) => `<div class="k">${k}</div><div class="v">${v}</div>`).join('');

  // 盘口
  const ob = document.getElementById('detail-orderbook');
  for (let i = 4; i >= 0; i--) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="b">${fmt.num(snap.bid_prices?.[i], 3)}</td>
      <td class="b">${fmt.hand(snap.bid_vols?.[i])}</td>
      <td>买${i+1}</td>
      <td class="a">${fmt.num(snap.ask_prices?.[i], 3)}</td>
      <td class="a">${fmt.hand(snap.ask_vols?.[i])}</td>
    `;
    ob.appendChild(tr);
  }

  // 历史
  const hb = document.getElementById('detail-history');
  hist.slice().reverse().forEach(h => {
    const tr = document.createElement('tr');
    const cls = colorClass(h.change_pct);
    tr.innerHTML = `
      <td>${fmt.time(h.fetched_at)}</td>
      <td>${fmt.num(h.price)}</td>
      <td class="${cls}">${fmt.pct(h.change_pct)}</td>
      <td>${fmt.amount(h.amount)}</td>
    `;
    hb.appendChild(tr);
  });
}

document.getElementById('detail-close').addEventListener('click', () => {
  document.getElementById('detail-panel').classList.add('hidden');
});

// ---------- 事件绑定 ----------
function bindEvents() {
  document.getElementById('search').addEventListener('input', e => {
    state.filter.text = e.target.value.trim();
    render();
  });
  document.getElementById('category').addEventListener('change', e => {
    state.filter.category = e.target.value;
    render();
  });
  document.getElementById('min-pct').addEventListener('input', e => {
    state.filter.minPct = e.target.value === '' ? null : parseFloat(e.target.value);
    render();
  });
  document.getElementById('max-pct').addEventListener('input', e => {
    state.filter.maxPct = e.target.value === '' ? null : parseFloat(e.target.value);
    render();
  });
  document.getElementById('only-active').addEventListener('change', e => {
    state.filter.onlyActive = e.target.checked;
    render();
  });
  document.getElementById('refresh-now').addEventListener('click', () => loadAllSnapshots());

  document.querySelectorAll('th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.dataset.sort;
      if (state.sortBy === key) {
        state.sortOrder = state.sortOrder === 'desc' ? 'asc' : 'desc';
      } else {
        state.sortBy = key;
        state.sortOrder = key === 'code' || key === 'name' ? 'asc' : 'desc';
      }
      render();
    });
  });
}

// ---------- 初始化 ----------
async function init() {
  bindEvents();
  try {
    await loadPool();
  } catch (e) {
    console.error('Failed to load pool', e);
  }
  await loadAllSnapshots();

  state.refreshTimer = setInterval(async () => {
    await loadAllSnapshots();
  }, REFRESH_MS);

  setInterval(updateCountdown, 1000);
}

init().catch(err => {
  console.error('Init failed', err);
  document.body.innerHTML = `<div style="padding:40px;color:#ff4d4f">初始化失败：${err.message}</div>`;
});