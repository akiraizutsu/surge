/* Momentum Screener - Frontend Logic */

// ── State ──
let currentIndex = 'sp500';
let activeTab = 'sp500';
let screeningData = null;
let allResults = {};  // {sp500: data, nasdaq100: data, nikkei225: data}
let sortKey = 'rank';
let sortAsc = true;
let activeSubTab = 'momentum';
let pollTimer = null;
let watchlistTickers = new Set();
let showWatchlistOnly = false;

// Chart instances
let chartBar = null;
let chartDoughnut = null;
let chartScatter = null;
let chartBreadth = null;

// ── Color System ──
function hexToHsl(hex) {
  let r = parseInt(hex.slice(1, 3), 16) / 255;
  let g = parseInt(hex.slice(3, 5), 16) / 255;
  let b = parseInt(hex.slice(5, 7), 16) / 255;
  const max = Math.max(r, g, b), min = Math.min(r, g, b);
  let h, s, l = (max + min) / 2;
  if (max === min) {
    h = s = 0;
  } else {
    const d = max - min;
    s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
    switch (max) {
      case r: h = ((g - b) / d + (g < b ? 6 : 0)) / 6; break;
      case g: h = ((b - r) / d + 2) / 6; break;
      case b: h = ((r - g) / d + 4) / 6; break;
    }
  }
  return [Math.round(h * 360), Math.round(s * 100), Math.round(l * 100)];
}

function applyColor(hex) {
  const [h] = hexToHsl(hex);
  document.documentElement.style.setProperty('--hue', h);
  localStorage.setItem('theme-color', hex);
  // Rebuild charts with new colors if data exists
  if (screeningData) renderCharts(screeningData);
}

function getThemeColors(count) {
  const hue = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--hue')) || 220;
  const colors = [];
  for (let i = 0; i < count; i++) {
    const h = (hue + (i * 360 / count)) % 360;
    colors.push(`hsl(${h}, 65%, 55%)`);
  }
  return colors;
}

function getPrimaryColor(lightness = 55) {
  const hue = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--hue')) || 220;
  return `hsl(${hue}, 65%, ${lightness}%)`;
}

// ── Dark Mode ──
function toggleDark() {
  document.documentElement.classList.toggle('dark');
  const isDark = document.documentElement.classList.contains('dark');
  localStorage.setItem('dark-mode', isDark);
  document.getElementById('darkIcon').textContent = isDark ? '\u2600' : '\u263E';
  if (screeningData) renderCharts(screeningData);
}

function initDarkMode() {
  const stored = localStorage.getItem('dark-mode');
  const isDark = stored === 'true';
  if (isDark) document.documentElement.classList.add('dark');
  document.getElementById('darkIcon').textContent = isDark ? '\u2600' : '\u263E';
}

// ── Tab Switching ──
function switchTab(idx) {
  activeTab = idx;
  document.querySelectorAll('.index-tab').forEach(btn => btn.classList.remove('active'));
  const tabId = { sp500: 'tabSP500', nasdaq100: 'tabNAS100', nikkei225: 'tabNK225' }[idx];
  document.getElementById(tabId).classList.add('active');

  // Render data for this tab if available
  const data = allResults[idx];
  if (data) {
    screeningData = data;
    renderDashboard(data);
    document.getElementById('statusText').textContent = '最終更新: ' + data.generated_at;
  }
}

function isJapanIndex() {
  return activeTab === 'nikkei225' || (screeningData && screeningData.index === '日経225');
}

function formatPrice(price) {
  if (isJapanIndex()) return '¥' + Math.round(price).toLocaleString();
  return '$' + price;
}

// ── Screening ──
async function runScreening() {
  const btn = document.getElementById('btnRun');
  btn.disabled = true;
  btn.textContent = '分析中...';

  document.getElementById('progressArea').classList.remove('hidden');
  document.getElementById('emptyState').classList.add('hidden');
  document.getElementById('statusText').textContent = '';

  try {
    const resp = await fetch('/api/screen', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        index: 'all',
        top_n: parseInt(document.getElementById('topN').value),
      }),
    });

    if (!resp.ok) {
      const err = await resp.json();
      throw new Error(err.error || 'スクリーニングの開始に失敗しました');
    }

    // Start polling
    pollProgress();
  } catch (e) {
    btn.disabled = false;
    btn.textContent = 'スクリーニング実行';
    document.getElementById('statusText').textContent = 'エラー: ' + e.message;
    document.getElementById('progressArea').classList.add('hidden');
  }
}

function pollProgress() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    try {
      const resp = await fetch('/api/status');
      const status = await resp.json();

      document.getElementById('progressBar').style.width = status.progress_pct + '%';
      document.getElementById('progressText').textContent = status.progress_pct + '%';

      if (!status.running) {
        clearInterval(pollTimer);
        pollTimer = null;

        document.getElementById('btnRun').disabled = false;
        document.getElementById('btnRun').textContent = 'スクリーニング実行';

        if (status.error) {
          document.getElementById('statusText').textContent = 'エラー: ' + status.error;
          document.getElementById('progressArea').classList.add('hidden');
        } else if (status.has_result) {
          // Fetch all results
          const allResp = await fetch('/api/results');
          if (allResp.ok) {
            allResults = await allResp.json();
          }
          // Show tabs
          document.getElementById('indexTabs').classList.remove('hidden');
          // Render active tab
          const data = allResults[activeTab];
          if (data) {
            screeningData = data;
            renderDashboard(data);
            document.getElementById('statusText').textContent = '最終更新: ' + data.generated_at;
          } else {
            // Fallback: show first available
            const firstKey = Object.keys(allResults)[0];
            if (firstKey) {
              activeTab = firstKey;
              switchTab(firstKey);
            }
          }
          document.getElementById('progressArea').classList.add('hidden');
        }
      }
    } catch (e) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }, 1000);
}

// ── Render Dashboard ──
function renderDashboard(data) {
  // Summary cards
  document.getElementById('summaryCards').classList.remove('hidden');
  document.getElementById('cardScreened').textContent = data.total_screened;
  document.getElementById('cardAvgScore').textContent = data.summary.avg_score;
  document.getElementById('cardOverheat').textContent = data.summary.overheat_count;
  document.getElementById('cardGolden').textContent = data.summary.golden_cross_count;

  // Breadth card
  const lb = data.latest_breadth;
  if (lb) {
    const pct = lb.breadth_pct;
    const pctColor = pct > 0 ? 'text-emerald-500' : pct < 0 ? 'text-rose-400' : 'text-primary-600 dark:text-primary-400';
    document.getElementById('cardBreadth').className = `text-2xl font-bold ${pctColor}`;
    document.getElementById('cardBreadth').textContent = (pct > 0 ? '+' : '') + pct + '%';
    document.getElementById('cardBreadthDetail').textContent = `${lb.advances}↑ / ${lb.declines}↓`;
  }

  // Charts
  document.getElementById('chartsArea').classList.remove('hidden');
  document.getElementById('rsiChartArea').classList.remove('hidden');
  renderCharts(data);

  // ADL chart
  loadBreadthChart(activeTab);

  // Sub-tabs and tables
  const hasContrarian = data.value_gap_ranking && data.value_gap_ranking.length > 0;
  if (hasContrarian) {
    document.getElementById('subTabs').classList.remove('hidden');
  } else {
    document.getElementById('subTabs').classList.add('hidden');
  }
  switchSubTab(activeSubTab);
}

function renderCharts(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const textColor = isDark ? '#9ca3af' : '#6b7280';
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const ranking = data.momentum_ranking;

  Chart.defaults.font.family = "'Zen Kaku Gothic New', 'Ubuntu', system-ui, sans-serif";

  // Bar chart - momentum scores
  const barCtx = document.getElementById('chartBar').getContext('2d');
  if (chartBar) chartBar.destroy();

  const barColors = ranking.map(r => r.technicals.overheat ? '#e8a0a0' : getPrimaryColor(55));

  chartBar = new Chart(barCtx, {
    type: 'bar',
    data: {
      labels: ranking.map(r => r.ticker),
      datasets: [{
        label: 'Momentum Score',
        data: ranking.map(r => r.momentum_score),
        backgroundColor: barColors,
        borderRadius: 4,
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            afterLabel: (ctx) => {
              const r = ranking[ctx.dataIndex];
              return `RSI: ${r.technicals.rsi} | 1M: ${r.technicals.ret_1m}%`;
            }
          }
        }
      },
      scales: {
        x: { grid: { color: gridColor }, ticks: { color: textColor } },
        y: { grid: { display: false }, ticks: { color: textColor, font: { size: 11 } } },
      }
    }
  });

  // Doughnut chart - sector distribution
  const doughCtx = document.getElementById('chartDoughnut').getContext('2d');
  if (chartDoughnut) chartDoughnut.destroy();

  const sectors = Object.entries(data.sector_distribution);
  const sectorColors = getThemeColors(sectors.length);

  chartDoughnut = new Chart(doughCtx, {
    type: 'doughnut',
    data: {
      labels: sectors.map(s => s[0]),
      datasets: [{
        data: sectors.map(s => s[1]),
        backgroundColor: sectorColors,
        borderWidth: 2,
        borderColor: isDark ? '#111827' : '#ffffff',
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'right',
          labels: { color: textColor, padding: 12, font: { size: 12 } }
        }
      }
    }
  });

  // Scatter chart - RSI vs 1M return
  const scatterCtx = document.getElementById('chartScatter').getContext('2d');
  if (chartScatter) chartScatter.destroy();

  chartScatter = new Chart(scatterCtx, {
    type: 'scatter',
    data: {
      datasets: [{
        label: 'Stocks',
        data: ranking.map(r => ({ x: r.technicals.rsi, y: r.technicals.ret_1m, ticker: r.ticker })),
        backgroundColor: ranking.map(r => r.technicals.overheat ? 'rgba(232,160,160,0.7)' : getPrimaryColor(55) + 'b3'),
        pointRadius: 6,
        pointHoverRadius: 9,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const d = ctx.raw;
              return `${d.ticker}: RSI ${d.x}, 1ヶ月 ${d.y}%`;
            }
          }
        }
      },
      scales: {
        x: {
          title: { display: true, text: 'RSI', color: textColor },
          grid: { color: gridColor },
          ticks: { color: textColor },
          min: 30, max: 100,
        },
        y: {
          title: { display: true, text: '1ヶ月リターン (%)', color: textColor },
          grid: { color: gridColor },
          ticks: { color: textColor },
        }
      }
    }
  });
}

// ── Sub-tab Switching ──
function switchSubTab(tab) {
  activeSubTab = tab;
  document.querySelectorAll('#subTabs .index-tab').forEach(btn => btn.classList.remove('active'));
  if (tab === 'momentum') {
    document.getElementById('subTabMomentum').classList.add('active');
    document.getElementById('tableArea').classList.remove('hidden');
    document.getElementById('contrarianTableArea').classList.add('hidden');
    if (screeningData) renderTable(screeningData.momentum_ranking);
  } else {
    document.getElementById('subTabContrarian').classList.add('active');
    document.getElementById('tableArea').classList.add('hidden');
    document.getElementById('contrarianTableArea').classList.remove('hidden');
    if (screeningData && screeningData.value_gap_ranking) {
      renderContrarianTable(screeningData.value_gap_ranking);
    }
  }
}

// ── Contrarian Table ──
function renderContrarianTable(ranking) {
  const tbody = document.getElementById('contrarianTableBody');
  const retClass = (v) => v > 0 ? 'text-emerald-600 dark:text-emerald-400' : v < 0 ? 'text-rose-400 dark:text-rose-300' : '';
  const fmtPct = (v) => v != null ? (v > 0 ? '+' : '') + v + '%' : '-';

  tbody.innerHTML = ranking.map(r => {
    const recMap = { strong_buy: '強い買い', buy: '買い', hold: '中立', sell: '売り', strong_sell: '強い売り' };
    const recLabel = recMap[r.recommendation] || r.recommendation || '-';
    const recClass = (r.recommendation === 'buy' || r.recommendation === 'strong_buy')
      ? 'text-emerald-600 dark:text-emerald-400 font-medium' : '';

    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 cursor-pointer transition-colors" onclick='showContrarianDetail(${JSON.stringify(r).replace(/'/g, "&#39;")})'>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-slate-400 text-xs sm:text-sm whitespace-nowrap">${r.rank}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 font-semibold text-primary-600 dark:text-primary-400 text-xs sm:text-sm whitespace-nowrap">${r.ticker}</td>
      <td class="px-4 py-3 text-slate-500 dark:text-gray-400 hidden lg:table-cell text-xs max-w-[180px] truncate">${r.name}</td>
      <td class="px-4 py-3 text-xs text-slate-600 dark:text-gray-400 whitespace-nowrap hidden md:table-cell">${r.sector}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-bold text-xs sm:text-sm whitespace-nowrap">${r.value_gap_score}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell">${formatPrice(r.price)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell">${formatPrice(r.target_price)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap text-emerald-600 dark:text-emerald-400 font-semibold">+${r.target_gap_pct}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap ${retClass(r.ret_1m)}">${fmtPct(r.ret_1m)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell">${r.rsi}</td>
      <td class="px-4 py-3 text-right font-mono text-sm whitespace-nowrap hidden lg:table-cell">${r.pe_forward || '-'}</td>
      <td class="px-4 py-3 text-right font-mono text-sm whitespace-nowrap hidden lg:table-cell ${retClass(r.eps_growth)}">${fmtPct(r.eps_growth)}</td>
      <td class="px-4 py-3 text-right font-mono text-sm whitespace-nowrap hidden lg:table-cell ${retClass(r.revenue_growth)}">${fmtPct(r.revenue_growth)}</td>
      <td class="px-4 py-3 text-center text-xs whitespace-nowrap hidden sm:table-cell ${recClass}">${recLabel}</td>
    </tr>`;
  }).join('');
}

// ── Contrarian Detail Modal ──
function showContrarianDetail(stock) {
  document.getElementById('modalTitle').textContent = `${stock.ticker} - ${stock.name}`;
  const fmtPct = (v) => v != null ? (v > 0 ? '+' : '') + v + '%' : '-';
  const fmtVal = (v) => v != null && v !== 0 ? v : '-';
  const recMap = { strong_buy: '強い買い', buy: '買い', hold: '中立', sell: '売り', strong_sell: '強い売り' };

  document.getElementById('modalContent').innerHTML = `
    <div class="grid grid-cols-2 gap-4 mb-6">
      <div class="bg-amber-50 dark:bg-amber-950/30 rounded-xl p-4">
        <div class="text-xs text-slate-500 dark:text-gray-400">バリュー乖離スコア</div>
        <div class="text-3xl font-bold text-amber-600 dark:text-amber-400">${stock.value_gap_score}</div>
      </div>
      <div class="bg-emerald-50 dark:bg-emerald-950/30 rounded-xl p-4">
        <div class="text-xs text-slate-500 dark:text-gray-400">目標株価との乖離</div>
        <div class="text-3xl font-bold text-emerald-600 dark:text-emerald-400">+${stock.target_gap_pct}%</div>
      </div>
    </div>

    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">株価情報</h3>
    <div class="grid grid-cols-3 gap-3 mb-6">
      ${[
        ['現在株価', formatPrice(stock.price)],
        ['目標株価', formatPrice(stock.target_price)],
        ['セクター', stock.sector],
        ['1ヶ月', fmtPct(stock.ret_1m)],
        ['3ヶ月', fmtPct(stock.ret_3m)],
        ['RSI', stock.rsi],
        ['50日MA乖離', fmtPct(stock.ma50_dev)],
        ['200日MA乖離', fmtPct(stock.ma200_dev)],
        ['アナリスト推奨', recMap[stock.recommendation] || '-'],
      ].map(([label, val]) => `
        <div class="bg-slate-50 dark:bg-gray-800 rounded-lg p-2.5 text-center border border-slate-100 dark:border-gray-700">
          <div class="text-[10px] text-slate-400 dark:text-gray-500">${label}</div>
          <div class="font-semibold text-sm text-slate-900 dark:text-gray-100">${val}</div>
        </div>
      `).join('')}
    </div>

    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">ファンダメンタルズ</h3>
    <div class="grid grid-cols-3 gap-3">
      ${[
        ['時価総額', stock.market_cap_b ? (isJapanIndex() ? '¥' : '$') + stock.market_cap_b + 'B' : '-'],
        ['PER (予想)', fmtVal(stock.pe_forward)],
        ['PER (実績)', fmtVal(stock.pe_trailing)],
        ['PBR', fmtVal(stock.pb)],
        ['EPS', fmtVal(stock.eps)],
        ['配当利回り', stock.dividend_yield ? stock.dividend_yield + '%' : '-'],
        ['EPS成長率', fmtPct(stock.eps_growth)],
        ['売上成長率', fmtPct(stock.revenue_growth)],
      ].map(([label, val]) => `
        <div class="bg-slate-50 dark:bg-gray-800 rounded-lg p-2.5 text-center border border-slate-100 dark:border-gray-700">
          <div class="text-[10px] text-slate-400 dark:text-gray-500">${label}</div>
          <div class="font-semibold text-sm text-slate-900 dark:text-gray-100">${val}</div>
        </div>
      `).join('')}
    </div>
  `;

  document.getElementById('modal').classList.remove('hidden');
  document.getElementById('modal').classList.add('flex');
}

// ── Breadth Chart ──
async function loadBreadthChart(index) {
  try {
    const resp = await fetch(`/api/breadth/${index}`);
    if (!resp.ok) return;
    const json = await resp.json();
    if (json.data && json.data.length > 0) {
      document.getElementById('breadthChartArea').classList.remove('hidden');
      renderBreadthChart(json.data);
    }
  } catch (e) { /* ignore */ }
}

function renderBreadthChart(breadthData) {
  const isDark = document.documentElement.classList.contains('dark');
  const textColor = isDark ? '#9ca3af' : '#6b7280';
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';

  // Use last 90 days
  const recent = breadthData.slice(-90);
  const labels = recent.map(d => d.date);
  const adlValues = recent.map(d => d.adl);
  const diffValues = recent.map(d => d.ad_diff);

  const ctx = document.getElementById('chartBreadth').getContext('2d');
  if (chartBreadth) chartBreadth.destroy();

  chartBreadth = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'ADL (累積)',
          data: adlValues,
          borderColor: getPrimaryColor(45),
          backgroundColor: 'transparent',
          fill: false,
          tension: 0.3,
          pointRadius: 0,
          pointHitRadius: 8,
          borderWidth: 2,
          yAxisID: 'y',
        },
        {
          label: '日次 AD差分',
          data: diffValues,
          type: 'bar',
          backgroundColor: diffValues.map(v => v >= 0 ? 'rgba(52,211,153,0.4)' : 'rgba(251,113,133,0.4)'),
          borderWidth: 0,
          yAxisID: 'y1',
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          labels: { color: textColor, font: { size: 11 } }
        },
        tooltip: {
          callbacks: {
            afterBody: (items) => {
              const idx = items[0]?.dataIndex;
              if (idx == null) return '';
              const d = recent[idx];
              return `上昇: ${d.advances} / 下降: ${d.declines} (${d.breadth_pct > 0 ? '+' : ''}${d.breadth_pct}%)`;
            }
          }
        }
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: {
            color: textColor,
            maxTicksLimit: 12,
            font: { size: 10 },
          },
        },
        y: {
          position: 'left',
          title: { display: true, text: 'ADL', color: textColor },
          grid: { color: gridColor },
          ticks: { color: textColor },
        },
        y1: {
          position: 'right',
          title: { display: true, text: '日次 AD', color: textColor },
          grid: { display: false },
          ticks: { color: textColor },
        }
      }
    }
  });
}

// ── Table ──
function renderTable(ranking) {
  let filtered = ranking;
  if (showWatchlistOnly) {
    filtered = ranking.filter(r => watchlistTickers.has(r.ticker));
  }

  const tbody = document.getElementById('tableBody');
  tbody.innerHTML = filtered.map(r => {
    const t = r.technicals;
    const retClass = (v) => v > 0 ? 'text-emerald-600 dark:text-emerald-400' : v < 0 ? 'text-rose-400 dark:text-rose-300' : '';
    const rsiClass = t.rsi > 70 ? 'text-rose-400 font-semibold' : t.rsi < 30 ? 'text-emerald-500 font-semibold' : '';

    const starred = watchlistTickers.has(r.ticker);
    const starCell = `<td class="px-2 py-3 text-center">
      <button onclick="toggleStar('${r.ticker}', event)"
        class="text-lg leading-none cursor-pointer hover:scale-110 transition-transform ${starred ? 'text-amber-400' : 'text-slate-300 dark:text-gray-600'}"
        aria-label="${starred ? 'ウォッチリストから削除' : 'ウォッチリストに追加'}">
        ${starred ? '&#9733;' : '&#9734;'}
      </button>
    </td>`;

    const sqScore = r.squeeze_score;
    const sqClass = sqScore != null && sqScore >= 70 ? 'text-rose-500 font-bold' : '';

    // RS label badge
    const rsLabelMap = {
      prime: { text: '本命', cls: 'bg-primary-50 dark:bg-primary-950/30 text-primary-600 dark:text-primary-400' },
      short_term: { text: '短期', cls: 'bg-amber-50 dark:bg-amber-900/20 text-amber-600 dark:text-amber-400' },
      sector_driven: { text: '劣後', cls: 'bg-slate-100 dark:bg-gray-800 text-slate-500 dark:text-gray-400' },
      theme: { text: 'テーマ', cls: 'bg-violet-50 dark:bg-violet-900/20 text-violet-500 dark:text-violet-400' },
    };
    const rsInfo = rsLabelMap[t.rs_label] || null;

    let status = '';
    if (t.overheat) status += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-rose-50 dark:bg-rose-900/20 text-rose-500 dark:text-rose-300 font-medium">過熱</span>';
    if (t.golden_cross) status += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400 font-medium">GC</span>';
    if (rsInfo) status += `<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full ${rsInfo.cls} font-medium">${rsInfo.text}</span>`;

    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 cursor-pointer transition-colors" onclick='showDetail(${JSON.stringify(r).replace(/'/g, "&#39;")})'>
      ${starCell}
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-slate-400 text-xs sm:text-sm whitespace-nowrap">${r.rank}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 font-semibold text-primary-600 dark:text-primary-400 text-xs sm:text-sm whitespace-nowrap">${r.ticker}</td>
      <td class="px-4 py-3 text-slate-500 dark:text-gray-400 hidden lg:table-cell text-xs max-w-[180px] truncate">${r.name}</td>
      <td class="px-4 py-3 text-xs text-slate-600 dark:text-gray-400 whitespace-nowrap hidden md:table-cell">${r.sector}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm text-slate-900 dark:text-gray-100 whitespace-nowrap hidden sm:table-cell">${formatPrice(r.price)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-bold text-xs sm:text-sm whitespace-nowrap">${r.momentum_score}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 whitespace-nowrap"><div class="flex items-center gap-0.5 sm:gap-1">${status || '<span class="text-slate-300 dark:text-gray-600">-</span>'}</div></td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell ${sqClass}">${sqScore != null ? sqScore.toFixed(1) : '-'}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden lg:table-cell ${retClass(t.ret_1d)}">${t.ret_1d > 0 ? '+' : ''}${t.ret_1d}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden lg:table-cell ${retClass(t.ret_1w)}">${t.ret_1w > 0 ? '+' : ''}${t.ret_1w}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap ${retClass(t.ret_1m)}">${t.ret_1m > 0 ? '+' : ''}${t.ret_1m}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell ${retClass(t.ret_3m)}">${t.ret_3m > 0 ? '+' : ''}${t.ret_3m}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell ${rsiClass}">${t.rsi}</td>
      <td class="px-4 py-3 text-right font-mono text-sm whitespace-nowrap hidden lg:table-cell">${t.vol_ratio}x</td>
      <td class="px-4 py-3 text-right font-mono text-sm whitespace-nowrap hidden lg:table-cell ${retClass(t.rs_1m)}">${t.rs_1m != null ? (t.rs_1m > 0 ? '+' : '') + t.rs_1m + '%' : '-'}</td>
      <td class="px-4 py-3 text-right font-mono text-sm whitespace-nowrap hidden lg:table-cell ${retClass(t.rs_3m)}">${t.rs_3m != null ? (t.rs_3m > 0 ? '+' : '') + t.rs_3m + '%' : '-'}</td>
    </tr>`;
  }).join('');
}

// ── Sort ──
document.querySelectorAll('.sortable').forEach(th => {
  th.addEventListener('click', () => {
    const key = th.dataset.key;
    if (sortKey === key) sortAsc = !sortAsc;
    else { sortKey = key; sortAsc = true; }

    if (!screeningData) return;
    const ranking = [...screeningData.momentum_ranking];

    ranking.sort((a, b) => {
      let va, vb;
      if (['ret_1d', 'ret_1w', 'ret_1m', 'ret_3m', 'rsi', 'vol_ratio', 'rs_1m', 'rs_3m'].includes(key)) {
        va = a.technicals[key]; vb = b.technicals[key];
      } else if (key === 'squeeze_score') {
        va = a.squeeze_score ?? -1; vb = b.squeeze_score ?? -1;
      } else {
        va = a[key]; vb = b[key];
      }
      if (typeof va === 'string') return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
      return sortAsc ? va - vb : vb - va;
    });

    renderTable(ranking);

    // Update header indicators
    document.querySelectorAll('.sortable').forEach(el => el.classList.remove('sort-asc', 'sort-desc'));
    th.classList.add(sortAsc ? 'sort-asc' : 'sort-desc');
  });
});

// ── Detail Modal ──
function showDetail(stock) {
  const t = stock.technicals;
  const f = stock.fundamentals;
  const si = stock.short_interest || {};

  document.getElementById('modalTitle').textContent = `${stock.ticker} - ${stock.name}`;

  const fmtPct = (v) => v != null ? (v > 0 ? '+' : '') + v + '%' : '-';
  const fmtVal = (v) => v != null && v !== 0 ? v : '-';

  document.getElementById('modalContent').innerHTML = `
    <div class="grid grid-cols-2 gap-4 mb-6">
      <div class="bg-primary-50 dark:bg-primary-950/30 rounded-xl p-4">
        <div class="text-xs text-slate-500 dark:text-gray-400">モメンタムスコア</div>
        <div class="text-3xl font-bold text-primary-600 dark:text-primary-400">${stock.momentum_score}</div>
      </div>
      <div class="bg-slate-50 dark:bg-gray-800 rounded-xl p-4">
        <div class="text-xs text-slate-500 dark:text-gray-400">セクター</div>
        <div class="text-lg font-semibold text-slate-900 dark:text-gray-100">${stock.sector}</div>
      </div>
    </div>

    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">テクニカル指標</h3>
    <div class="grid grid-cols-3 gap-3 mb-6">
      ${[
        ['株価', formatPrice(stock.price)],
        ['1日', fmtPct(t.ret_1d)],
        ['1週間', fmtPct(t.ret_1w)],
        ['1ヶ月', fmtPct(t.ret_1m)],
        ['3ヶ月', fmtPct(t.ret_3m)],
        ['RSI', t.rsi],
        ['出来高比', t.vol_ratio + 'x'],
        ['50日MA乖離', fmtPct(t.ma50_dev)],
        ['200日MA乖離', fmtPct(t.ma200_dev)],
        ['MACDヒスト', fmtPct(t.macd_hist_pct)],
        ['ゴールデンクロス', t.golden_cross ? 'はい' : 'いいえ'],
        ['過熱', t.overheat ? 'はい' : 'いいえ'],
        ['セクターETF', t.sector_etf || '-'],
        ['RS 1M', fmtPct(t.rs_1m)],
        ['RS 3M', fmtPct(t.rs_3m)],
        ['RS判定', {'prime':'本命','short_term':'短期のみ','sector_driven':'劣後','theme':'テーマ依存'}[t.rs_label] || '-'],
      ].map(([label, val]) => `
        <div class="bg-slate-50 dark:bg-gray-800 rounded-lg p-2.5 text-center border border-slate-100 dark:border-gray-700">
          <div class="text-[10px] text-slate-400 dark:text-gray-500">${label}</div>
          <div class="font-semibold text-sm text-slate-900 dark:text-gray-100">${val}</div>
        </div>
      `).join('')}
    </div>

    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">ショートスクイーズ分析</h3>
    <div class="grid grid-cols-2 gap-4 mb-6">
      <div class="bg-rose-50 dark:bg-rose-950/20 rounded-xl p-4">
        <div class="text-xs text-slate-500 dark:text-gray-400">スクイーズスコア</div>
        <div class="text-3xl font-bold text-rose-500">${stock.squeeze_score != null ? stock.squeeze_score.toFixed(1) : '-'}</div>
      </div>
      <div class="grid grid-cols-2 gap-2">
        ${[
          ['空売り比率', si.short_pct_of_float != null ? (si.short_pct_of_float * 100).toFixed(1) + '%' : '-'],
          ['DtC (日)', si.short_ratio != null ? si.short_ratio.toFixed(1) : '-'],
          ['前月比変化', si.short_change_pct != null ? (si.short_change_pct > 0 ? '+' : '') + si.short_change_pct.toFixed(1) + '%' : '-'],
          ['空売り株数', si.shares_short != null ? (si.shares_short / 1e6).toFixed(1) + 'M' : '-'],
        ].map(([label, val]) => `
          <div class="bg-slate-50 dark:bg-gray-800 rounded-lg p-2 text-center border border-slate-100 dark:border-gray-700">
            <div class="text-[10px] text-slate-400 dark:text-gray-500">${label}</div>
            <div class="font-semibold text-xs text-slate-900 dark:text-gray-100">${val}</div>
          </div>
        `).join('')}
      </div>
    </div>

    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">ファンダメンタルズ</h3>
    <div class="grid grid-cols-3 gap-3">
      ${[
        ['時価総額', f.market_cap_b ? (isJapanIndex() ? '¥' + f.market_cap_b + 'B' : '$' + f.market_cap_b + 'B') : '-'],
        ['PER (実績)', fmtVal(f.pe_trailing)],
        ['PER (予想)', fmtVal(f.pe_forward)],
        ['PBR', fmtVal(f.pb)],
        ['配当利回り', f.dividend_yield ? f.dividend_yield + '%' : '-'],
        ['売上成長率', fmtPct(f.revenue_growth)],
        ['EPS成長率', fmtPct(f.earnings_growth)],
        ['EPS', fmtVal(f.eps)],
        ['目標株価', f.target_price ? (isJapanIndex() ? '¥' + Math.round(f.target_price).toLocaleString() : '$' + f.target_price) : '-'],
        ['推奨', f.recommendation || '-'],
      ].map(([label, val]) => `
        <div class="bg-slate-50 dark:bg-gray-800 rounded-lg p-2.5 text-center border border-slate-100 dark:border-gray-700">
          <div class="text-[10px] text-slate-400 dark:text-gray-500">${label}</div>
          <div class="font-semibold text-sm text-slate-900 dark:text-gray-100">${val}</div>
        </div>
      `).join('')}
    </div>
  `;

  document.getElementById('modal').classList.remove('hidden');
  document.getElementById('modal').classList.add('flex');
}

function closeModal() {
  document.getElementById('modal').classList.add('hidden');
  document.getElementById('modal').classList.remove('flex');
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') closeModal();
});

// ── Watchlist ──
async function loadWatchlist() {
  try {
    const resp = await fetch('/api/watchlist');
    const data = await resp.json();
    watchlistTickers = new Set(data);
  } catch (e) { /* ignore */ }
}

async function toggleStar(ticker, event) {
  event.stopPropagation();
  if (watchlistTickers.has(ticker)) {
    await fetch(`/api/watchlist/${ticker}`, { method: 'DELETE' });
    watchlistTickers.delete(ticker);
  } else {
    await fetch('/api/watchlist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker }),
    });
    watchlistTickers.add(ticker);
  }
  if (screeningData) renderTable(screeningData.momentum_ranking);
}

function toggleWatchlistView() {
  showWatchlistOnly = !showWatchlistOnly;
  const btn = document.getElementById('btnWatchlist');
  btn.classList.toggle('bg-primary-50', showWatchlistOnly);
  btn.classList.toggle('dark:bg-primary-950/30', showWatchlistOnly);
  btn.classList.toggle('text-primary-600', showWatchlistOnly);
  btn.classList.toggle('border-primary-300', showWatchlistOnly);
  if (screeningData) renderTable(screeningData.momentum_ranking);
}

// ── Init ──
async function init() {
  initDarkMode();
  await loadWatchlist();

  // Restore color
  const savedColor = localStorage.getItem('theme-color');
  if (savedColor) {
    document.getElementById('colorPicker').value = savedColor;
    applyColor(savedColor);
  } else {
    applyColor('#06b6d4');
  }

  document.getElementById('colorPicker').addEventListener('input', (e) => {
    applyColor(e.target.value);
  });

  // Check for existing results
  fetch('/api/status').then(r => r.json()).then(async status => {
    if (status.has_result) {
      const allResp = await fetch('/api/results');
      if (allResp.ok) {
        allResults = await allResp.json();
        const keys = Object.keys(allResults);
        if (keys.length > 0) {
          document.getElementById('indexTabs').classList.remove('hidden');
          if (allResults[activeTab]) {
            switchTab(activeTab);
          } else {
            switchTab(keys[0]);
          }
          return; // Data loaded, don't show empty state
        }
      }
    }
    if (status.running) {
      document.getElementById('btnRun').disabled = true;
      document.getElementById('btnRun').textContent = '分析中...';
      document.getElementById('progressArea').classList.remove('hidden');
      pollProgress();
      return;
    }
    // No results and not running — show empty state
    document.getElementById('emptyState').classList.remove('hidden');
  });
}

init();
