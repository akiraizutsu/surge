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
let chartSectorRotation = null;

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
  const isDark = stored === null ? true : stored === 'true'; // default: dark
  if (isDark) document.documentElement.classList.add('dark');
  document.getElementById('darkIcon').textContent = isDark ? '\u2600' : '\u263E';
}

// ── Tab Switching ──
function switchTab(idx) {
  activeTab = idx;
  document.querySelectorAll('#indexTabs .index-tab').forEach(btn => btn.classList.remove('active'));
  const tabId = { sp500: 'tabSP500', nasdaq100: 'tabNAS100', nikkei225: 'tabNK225', growth250: 'tabGrowth250' }[idx];
  document.getElementById(tabId)?.classList.add('active');

  // Render data for this tab if available
  const data = allResults[idx];
  if (data) {
    screeningData = data;
    renderDashboard(data);
    document.getElementById('statusText').textContent = '最終更新: ' + data.generated_at;
  }
}

function isJapanIndex() {
  return !!window.IS_JAPAN_PAGE;
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
        index: window.IS_JAPAN_PAGE ? 'japan_all' : 'us_all',
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
            // Fallback: prefer page-appropriate index
            const preferred = window.IS_JAPAN_PAGE
              ? ['nikkei225', 'growth250']
              : ['sp500', 'nasdaq100'];
            const firstKey = preferred.find(k => allResults[k]) || Object.keys(allResults)[0];
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

// ── Market Regime Card ──
function renderRegimeCard(regime) {
  const el = document.getElementById('regimeCard');
  if (!el) return;
  if (!regime) { el.classList.add('hidden'); return; }

  const confPct = Math.round((regime.confidence || 0) * 100);
  const signals = (regime.signals || []).map(s =>
    `<li class="flex items-start gap-1.5"><span class="text-slate-400 dark:text-gray-500 mt-0.5">•</span><span>${s}</span></li>`
  ).join('');

  el.innerHTML = `
    <div class="card mt-4 border-l-4" style="border-left-color:${regime.color}">
      <div class="flex flex-wrap items-start justify-between gap-3">
        <div class="flex items-center gap-2">
          <span class="text-2xl leading-none">${regime.dot}</span>
          <div>
            <div class="text-xs text-slate-500 dark:text-gray-400 font-medium uppercase tracking-wide">相場地合い</div>
            <div class="text-lg font-bold text-slate-800 dark:text-white">${regime.regime_label}</div>
          </div>
        </div>
        <div class="text-right">
          <div class="text-xs text-slate-500 dark:text-gray-400">信頼度</div>
          <div class="text-2xl font-bold" style="color:${regime.color}">${confPct}%</div>
        </div>
      </div>
      <p class="mt-2 text-sm text-slate-600 dark:text-gray-300">${regime.description || ''}</p>
      <div class="mt-2 rounded-lg px-3 py-2 text-sm font-medium" style="background:${regime.color}1a;color:${regime.color}">
        ${regime.implication || ''}
      </div>
      ${signals ? `<ul class="mt-3 space-y-1 text-xs text-slate-500 dark:text-gray-400">${signals}</ul>` : ''}
    </div>
  `;
  el.classList.remove('hidden');
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

  // Regime card
  renderRegimeCard(data.regime || null);

  // Charts
  document.getElementById('chartsArea').classList.remove('hidden');
  document.getElementById('rsiChartArea').classList.remove('hidden');
  renderCharts(data);

  // ADL chart
  loadBreadthChart(activeTab);

  // Sub-tabs and tables — always show; hide tab buttons when no data
  document.getElementById('subTabs').classList.remove('hidden');
  const hasContrarian = data.value_gap_ranking && data.value_gap_ranking.length > 0;
  const hasTimeArb   = data.time_arb_ranking  && data.time_arb_ranking.length > 0;
  const hasSmallcap  = data.smallcap_ranking   && data.smallcap_ranking.length > 0;
  const contrarianBtn = document.getElementById('subTabContrarian');
  const timeArbBtn    = document.getElementById('subTabTimeArb');
  const smallcapBtn   = document.getElementById('subTabSmallcap');
  if (contrarianBtn) contrarianBtn.style.display = hasContrarian ? '' : 'none';
  if (timeArbBtn)    timeArbBtn.style.display    = hasTimeArb   ? '' : 'none';
  if (smallcapBtn)   smallcapBtn.style.display   = hasSmallcap  ? '' : 'none';
  // Fall back to momentum if active tab has no data
  if (!hasContrarian && activeSubTab === 'contrarian') activeSubTab = 'momentum';
  if (!hasTimeArb   && activeSubTab === 'time_arb')   activeSubTab = 'momentum';
  if (!hasSmallcap  && activeSubTab === 'smallcap')   activeSubTab = 'momentum';
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
  const tabBtnMap = {
    momentum: 'subTabMomentum', contrarian: 'subTabContrarian',
    rotation: 'subTabRotation', breakout: 'subTabBreakout',
    time_arb: 'subTabTimeArb', smallcap: 'subTabSmallcap',
  };
  document.getElementById(tabBtnMap[tab])?.classList.add('active');

  // Hide all sub-tab areas
  ['tableArea', 'contrarianTableArea', 'sectorRotationArea', 'breakoutTableArea',
   'timeArbTableArea', 'smallcapTableArea'].forEach(id => {
    document.getElementById(id)?.classList.add('hidden');
  });

  if (tab === 'momentum') {
    document.getElementById('tableArea').classList.remove('hidden');
    if (screeningData) renderTable(screeningData.momentum_ranking);
  } else if (tab === 'contrarian') {
    document.getElementById('contrarianTableArea').classList.remove('hidden');
    if (screeningData && screeningData.value_gap_ranking) renderContrarianTable(screeningData.value_gap_ranking);
  } else if (tab === 'rotation') {
    document.getElementById('sectorRotationArea').classList.remove('hidden');
    if (screeningData && screeningData.sector_rotation) {
      renderSectorRotationChart(screeningData.sector_rotation);
      renderRotationTable(screeningData.sector_rotation);
    }
  } else if (tab === 'breakout') {
    document.getElementById('breakoutTableArea').classList.remove('hidden');
    if (screeningData && screeningData.breakout_ranking) renderBreakoutTable(screeningData.breakout_ranking);
  } else if (tab === 'time_arb') {
    document.getElementById('timeArbTableArea').classList.remove('hidden');
    if (screeningData && screeningData.time_arb_ranking) renderTimeArbTable(screeningData.time_arb_ranking);
  } else if (tab === 'smallcap') {
    document.getElementById('smallcapTableArea').classList.remove('hidden');
    if (screeningData && screeningData.smallcap_ranking) renderSmallcapTable(screeningData.smallcap_ranking);
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

    const cfBtnC = isJapanIndex()
      ? `<button onclick="showCfModal('${r.ticker}',event)" class="text-[9px] font-bold px-1 py-0.5 rounded bg-primary-100 dark:bg-primary-900/30 text-primary-600 dark:text-primary-400 hover:bg-primary-200 dark:hover:bg-primary-900/50 transition-colors leading-none cursor-pointer">CF</button>`
      : '';
    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 cursor-pointer transition-colors" onclick='showContrarianDetail(${JSON.stringify(r).replace(/'/g, "&#39;")})'>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-slate-400 text-xs sm:text-sm whitespace-nowrap">${r.rank}</td>
      <td class="px-2 py-2 sm:py-3 text-center">${cfBtnC}</td>
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
    const cfBtn = isJapanIndex()
      ? `<button onclick="showCfModal('${r.ticker}',event)" class="text-[9px] font-bold px-1 py-0.5 rounded bg-primary-100 dark:bg-primary-900/30 text-primary-600 dark:text-primary-400 hover:bg-primary-200 dark:hover:bg-primary-900/50 transition-colors leading-none cursor-pointer">CF</button>`
      : '';
    const starCell = `<td class="px-2 py-3 text-center">
      <div class="flex flex-col items-center gap-0.5">
        <button onclick="toggleStar('${r.ticker}', event)"
          class="text-lg leading-none cursor-pointer hover:scale-110 transition-transform ${starred ? 'text-amber-400' : 'text-slate-300 dark:text-gray-600'}"
          aria-label="${starred ? 'ウォッチリストから削除' : 'ウォッチリストに追加'}">
          ${starred ? '&#9733;' : '&#9734;'}
        </button>
        ${cfBtn}
      </div>
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
    if (t.is_breakout) status += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-sky-50 dark:bg-sky-900/20 text-sky-600 dark:text-sky-400 font-medium">52W</span>';
    if (t.bb_squeeze) status += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-orange-50 dark:bg-orange-900/20 text-orange-500 dark:text-orange-400 font-medium">BB圧</span>';
    const f = r.fundamentals;
    if (f && f.days_to_earnings != null && f.days_to_earnings >= 0 && f.days_to_earnings <= 14) {
      const urgency = f.days_to_earnings <= 3 ? 'bg-rose-50 dark:bg-rose-900/20 text-rose-500' : 'bg-slate-100 dark:bg-gray-800 text-slate-500';
      status += `<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full ${urgency} font-medium">決算${f.days_to_earnings}日</span>`;
    }

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

// ── Sector Rotation Chart ──
function renderSectorRotationChart(rotation) {
  const isDark = document.documentElement.classList.contains('dark');
  const textColor = isDark ? '#9ca3af' : '#6b7280';
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const colors = getThemeColors(rotation.length);

  const ctx = document.getElementById('chartSectorRotation').getContext('2d');
  if (chartSectorRotation) chartSectorRotation.destroy();

  chartSectorRotation = new Chart(ctx, {
    type: 'bubble',
    data: {
      datasets: rotation.map((s, i) => ({
        label: s.sector,
        data: [{ x: s.ret_3m_avg, y: s.ret_1m_avg, r: Math.max(4, Math.min(s.stock_count / 2, 25)) }],
        backgroundColor: colors[i] + 'aa',
        borderColor: colors[i],
        borderWidth: 2,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const s = rotation[ctx.datasetIndex];
              return `${s.sector}: 1M ${s.ret_1m_avg}% / 3M ${s.ret_3m_avg}% (${s.stock_count}銘柄) [${s.trend}]`;
            }
          }
        }
      },
      scales: {
        x: {
          title: { display: true, text: '3Mリターン (%)', color: textColor },
          grid: { color: gridColor },
          ticks: { color: textColor },
        },
        y: {
          title: { display: true, text: '1Mリターン (%)', color: textColor },
          grid: { color: gridColor },
          ticks: { color: textColor },
        }
      }
    }
  });
}

function renderRotationTable(rotation) {
  const tbody = document.getElementById('rotationTableBody');
  const retClass = (v) => v > 0 ? 'text-emerald-600 dark:text-emerald-400' : v < 0 ? 'text-rose-400 dark:text-rose-300' : '';
  const trendColors = {
    '加速': 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400',
    '安定': 'bg-primary-50 dark:bg-primary-950/30 text-primary-600 dark:text-primary-400',
    '回復': 'bg-amber-50 dark:bg-amber-900/20 text-amber-600 dark:text-amber-400',
    '減速': 'bg-slate-100 dark:bg-gray-800 text-slate-500 dark:text-gray-400',
    '衰退': 'bg-rose-50 dark:bg-rose-900/20 text-rose-500 dark:text-rose-300',
  };

  tbody.innerHTML = rotation.map(s => `
    <tr class="border-b border-slate-100 dark:border-gray-800">
      <td class="px-2 sm:px-4 py-2 sm:py-3 font-medium text-sm whitespace-nowrap">${s.sector}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-center text-xs text-slate-400 hidden sm:table-cell">${s.etf}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-sm whitespace-nowrap ${retClass(s.ret_1m_avg)}">${s.ret_1m_avg > 0 ? '+' : ''}${s.ret_1m_avg}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-sm whitespace-nowrap ${retClass(s.ret_3m_avg)}">${s.ret_3m_avg > 0 ? '+' : ''}${s.ret_3m_avg}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-sm whitespace-nowrap hidden sm:table-cell ${retClass(s.rs_1m_avg)}">${s.rs_1m_avg > 0 ? '+' : ''}${s.rs_1m_avg}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-center text-sm hidden sm:table-cell">${s.stock_count}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-center"><span class="inline-block px-2 py-0.5 text-[10px] rounded-full font-medium ${trendColors[s.trend] || ''}">${s.trend}</span></td>
    </tr>
  `).join('');
}

// ── Breakout Table ──
function renderBreakoutTable(ranking) {
  const tbody = document.getElementById('breakoutTableBody');
  const retClass = (v) => v > 0 ? 'text-emerald-600 dark:text-emerald-400' : v < 0 ? 'text-rose-400 dark:text-rose-300' : '';

  tbody.innerHTML = ranking.map(r => {
    let statusBadges = '';
    if (r.is_breakout) statusBadges += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400 font-medium">新高値</span>';
    if (r.bb_squeeze) statusBadges += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-amber-50 dark:bg-amber-900/20 text-amber-600 dark:text-amber-400 font-medium">BB圧縮</span>';

    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 transition-colors">
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-slate-400 text-sm whitespace-nowrap">${r.rank}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 font-semibold text-primary-600 dark:text-primary-400 text-sm whitespace-nowrap">${r.ticker}</td>
      <td class="px-4 py-3 text-slate-500 dark:text-gray-400 hidden lg:table-cell text-xs max-w-[180px] truncate">${r.name}</td>
      <td class="px-4 py-3 text-xs text-slate-600 dark:text-gray-400 whitespace-nowrap hidden md:table-cell">${r.sector}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-sm whitespace-nowrap">${formatPrice(r.price)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-sm text-slate-400 whitespace-nowrap">${formatPrice(r.high_52w)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-sm whitespace-nowrap ${retClass(r.dist_from_high)}">${r.dist_from_high > 0 ? '+' : ''}${r.dist_from_high}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-sm whitespace-nowrap hidden sm:table-cell">${r.bb_width}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 whitespace-nowrap"><div class="flex items-center gap-1">${statusBadges || '-'}</div></td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-bold text-sm whitespace-nowrap">${r.momentum_score}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-sm whitespace-nowrap hidden sm:table-cell">${r.rsi}</td>
    </tr>`;
  }).join('');
}

// ── Time Arbitrage Table ──
function renderTimeArbTable(ranking) {
  const tbody = document.getElementById('timeArbTableBody');
  const retClass = (v) => v > 0 ? 'text-emerald-600 dark:text-emerald-400' : v < 0 ? 'text-rose-400 dark:text-rose-300' : '';
  const fmtPct = (v) => v != null ? (v > 0 ? '+' : '') + v + '%' : '-';
  const isJP = isJapanIndex();
  const cfUnit = isJP ? '億円' : '$B';

  if (!ranking || ranking.length === 0) {
    tbody.innerHTML = '<tr><td colspan="10" class="px-4 py-8 text-center text-sm text-slate-400 dark:text-gray-500">データなし（売り込まれた銘柄が見つからないか、CF基準を満たす銘柄がありませんでした）</td></tr>';
    return;
  }

  tbody.innerHTML = ranking.map(r => {
    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 transition-colors">
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-slate-400 text-xs sm:text-sm whitespace-nowrap">${r.rank}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 font-semibold text-primary-600 dark:text-primary-400 text-xs sm:text-sm whitespace-nowrap">${r.ticker}</td>
      <td class="px-4 py-3 text-slate-500 dark:text-gray-400 hidden lg:table-cell text-xs max-w-[160px] truncate">${r.name}</td>
      <td class="px-4 py-3 text-xs text-slate-600 dark:text-gray-400 whitespace-nowrap hidden md:table-cell">${r.sector}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap">${formatPrice(r.price)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-bold text-xs sm:text-sm whitespace-nowrap text-amber-500 dark:text-amber-400">${r.arb_score}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap text-emerald-600 dark:text-emerald-400">+${r.capex_growth}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap ${retClass(r.ni_change)}">${fmtPct(r.ni_change)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell">${r.opcf_val}<span class="text-[10px] text-slate-400 ml-0.5">${cfUnit}</span></td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap ${retClass(r.ret_1m)}">${fmtPct(r.ret_1m)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell">${r.rsi}</td>
    </tr>`;
  }).join('');
}

// ── Small-cap Momentum Table ──
function renderSmallcapTable(ranking) {
  const tbody = document.getElementById('smallcapTableBody');
  const retClass = (v) => v > 0 ? 'text-emerald-600 dark:text-emerald-400' : v < 0 ? 'text-rose-400 dark:text-rose-300' : '';
  const fmtPct = (v) => v != null ? (v > 0 ? '+' : '') + v + '%' : '-';
  const isJP = isJapanIndex();

  const fmtCap = (cap) => {
    if (isJP) return (cap * 10).toFixed(0) + '億円';
    return '$' + cap.toFixed(1) + 'B';
  };

  if (!ranking || ranking.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" class="px-4 py-8 text-center text-sm text-slate-400 dark:text-gray-500">データなし（取得した銘柄プール内に該当する小型・中型株がありませんでした）</td></tr>';
    return;
  }

  tbody.innerHTML = ranking.map(r => {
    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 transition-colors">
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-slate-400 text-xs sm:text-sm whitespace-nowrap">${r.rank}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 font-semibold text-primary-600 dark:text-primary-400 text-xs sm:text-sm whitespace-nowrap">${r.ticker}</td>
      <td class="px-4 py-3 text-slate-500 dark:text-gray-400 hidden lg:table-cell text-xs max-w-[160px] truncate">${r.name}</td>
      <td class="px-4 py-3 text-xs text-slate-600 dark:text-gray-400 whitespace-nowrap hidden md:table-cell">${r.sector}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap">${formatPrice(r.price)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell">${fmtCap(r.market_cap_b)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-bold text-xs sm:text-sm whitespace-nowrap">${r.momentum_score}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap ${retClass(r.ret_1m)}">${fmtPct(r.ret_1m)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell ${retClass(r.ret_3m)}">${fmtPct(r.ret_3m)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell">${r.rsi}</td>
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
// ── Sprint 1 helpers ──────────────────────────────────────────────────────────

const TAG_COLORS = {
  '出来高先行型':    'bg-indigo-50 dark:bg-indigo-950/30 text-indigo-700 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800',
  '高値更新初動型':  'bg-emerald-50 dark:bg-emerald-950/30 text-emerald-700 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800',
  'BB圧縮ブレイク型':'bg-cyan-50 dark:bg-cyan-950/30 text-cyan-700 dark:text-cyan-300 border border-cyan-200 dark:border-cyan-800',
  '押し目継続型':    'bg-teal-50 dark:bg-teal-950/30 text-teal-700 dark:text-teal-300 border border-teal-200 dark:border-teal-800',
  '短期過熱型':      'bg-rose-50 dark:bg-rose-950/30 text-rose-700 dark:text-rose-300 border border-rose-200 dark:border-rose-800',
  '決算先回り型':    'bg-violet-50 dark:bg-violet-950/30 text-violet-700 dark:text-violet-300 border border-violet-200 dark:border-violet-800',
  '需給主導型':      'bg-pink-50 dark:bg-pink-950/30 text-pink-700 dark:text-pink-300 border border-pink-200 dark:border-pink-800',
  '指数逆行強者':    'bg-amber-50 dark:bg-amber-950/30 text-amber-700 dark:text-amber-300 border border-amber-200 dark:border-amber-800',
  'リバーサル初期型':'bg-slate-100 dark:bg-gray-800 text-slate-600 dark:text-gray-400 border border-slate-200 dark:border-gray-700',
};

function renderScoreBreakdown(components) {
  if (!components || components.length === 0) return '';
  const bars = components.map(c => {
    const pct = c.percentile_value || 0;
    const barColor = pct >= 70 ? '#10b981' : pct >= 40 ? '#f59e0b' : '#f43f5e';
    const fmtRaw = (name, val) => {
      if (val == null) return '-';
      if (name === 'vol_ratio') return val.toFixed(2) + 'x';
      if (name === 'rsi') return val.toFixed(1);
      return (val >= 0 ? '+' : '') + val.toFixed(2) + '%';
    };
    return `
      <div class="flex items-center gap-2 py-1">
        <div class="w-20 text-[11px] text-slate-500 dark:text-gray-400 text-right shrink-0">${c.label}</div>
        <div class="flex-1 bg-slate-100 dark:bg-gray-700 rounded-full h-2 overflow-hidden">
          <div class="h-2 rounded-full transition-all" style="width:${Math.min(pct,100)}%;background:${barColor}"></div>
        </div>
        <div class="w-10 text-[11px] font-mono text-slate-700 dark:text-gray-300 text-right shrink-0">${pct.toFixed(0)}%ile</div>
        <div class="w-14 text-[11px] font-mono text-slate-400 dark:text-gray-500 text-right shrink-0">${fmtRaw(c.component_name, c.raw_value)}</div>
      </div>`;
  }).join('');
  return `
    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-2 tracking-wider">スコア内訳</h3>
    <div class="bg-slate-50 dark:bg-gray-800/50 rounded-xl p-3 mb-5">${bars}</div>`;
}

function renderTagSection(tags) {
  if (!tags || tags.length === 0) return '';
  const badges = tags.map(tag => {
    const cls = TAG_COLORS[tag.tag_name] || 'bg-slate-100 dark:bg-gray-800 text-slate-600 dark:text-gray-400 border border-slate-200 dark:border-gray-700';
    const conf = tag.confidence ? `<span class="opacity-60 text-[9px]"> ${(tag.confidence * 100).toFixed(0)}%</span>` : '';
    const reason = tag.reason_text ? ` title="${tag.reason_text.replace(/"/g, '&quot;')}"` : '';
    return `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-medium cursor-help ${cls}"${reason}>${tag.tag_name}${conf}</span>`;
  }).join(' ');

  const details = tags.map(tag => `
    <div class="text-[11px] text-slate-500 dark:text-gray-400 leading-relaxed py-1 border-b border-slate-100 dark:border-gray-800 last:border-0">
      <span class="font-medium text-slate-700 dark:text-gray-300">${tag.tag_name}</span> — ${tag.reason_text || ''}
    </div>`).join('');

  return `
    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-2 tracking-wider">選定理由タグ</h3>
    <div class="mb-2 flex flex-wrap gap-1.5">${badges}</div>
    <div class="bg-slate-50 dark:bg-gray-800/50 rounded-xl px-3 py-2 mb-5">${details}</div>`;
}

function renderQuestionsSection(questions) {
  if (!questions || questions.length === 0) return '';
  const items = questions.map((q, i) => `
    <div class="flex gap-2.5 py-2 border-b border-slate-100 dark:border-gray-800 last:border-0">
      <span class="shrink-0 w-5 h-5 rounded-full bg-primary-100 dark:bg-primary-950/50 text-primary-600 dark:text-primary-400 text-[10px] font-bold flex items-center justify-center mt-0.5">${i+1}</span>
      <p class="text-[12px] text-slate-700 dark:text-gray-300 leading-relaxed">${q}</p>
    </div>`).join('');
  return `
    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-2 tracking-wider">確認論点</h3>
    <div class="bg-amber-50/50 dark:bg-amber-950/10 border border-amber-200 dark:border-amber-900/40 rounded-xl px-3 py-1 mb-5">${items}</div>`;
}

async function showDetail(stock) {
  const t = stock.technicals;
  const f = stock.fundamentals;
  const si = stock.short_interest || {};

  document.getElementById('modalTitle').textContent = `${stock.ticker} - ${stock.name}`;

  const fmtPct = (v) => v != null ? (v > 0 ? '+' : '') + v + '%' : '-';
  const fmtVal = (v) => v != null && v !== 0 ? v : '-';

  // ── Render static sections immediately (synchronous) ──────────────────────
  document.getElementById('modalContent').innerHTML = `
    <div id="explainSection" class="mb-2">
      <div class="text-[11px] text-slate-400 dark:text-gray-500 py-3 text-center">分析データを読み込み中...</div>
    </div>

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
        ['52W高値', t.high_52w ? formatPrice(t.high_52w) : '-'],
        ['52W安値', t.low_52w ? formatPrice(t.low_52w) : '-'],
        ['52W乖離', t.dist_from_high != null ? (t.dist_from_high > 0 ? '+' : '') + t.dist_from_high + '%' : '-'],
        ['BB幅', t.bb_width != null ? t.bb_width + '%' : '-'],
        ['決算日', f.earnings_date || '-'],
        ['決算まで', f.days_to_earnings != null ? f.days_to_earnings + '日' : '-'],
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
        ['配当利回り', f.dividend_yield ? f.dividend_yield.toFixed(2) + '%' : '-'],
        ['EPS', fmtVal(f.eps)],
        ...(isJapanIndex() ? [
          ['ROE', f.roe != null ? (f.roe * 100).toFixed(1) + '%' : '-'],
          ['売上高 (億円)', f.revenue_b != null ? f.revenue_b.toLocaleString() : '-'],
          ['純利益 (億円)', f.net_income_b != null ? f.net_income_b.toLocaleString() : '-'],
        ] : [
          ['売上成長率', fmtPct(f.revenue_growth)],
          ['EPS成長率', fmtPct(f.earnings_growth)],
          ['目標株価', f.target_price ? '$' + f.target_price : '-'],
          ['推奨', f.recommendation || '-'],
        ]),
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

  // ── Load score breakdown / tags / questions ───────────────────────────────
  // Use embedded data if available (fresh screening), otherwise fetch from API
  let explainData = null;
  if (stock.score_components && stock.score_components.length > 0) {
    explainData = {
      score_components: stock.score_components,
      tags: stock.tags || [],
      questions: stock.questions || [],
    };
  } else {
    try {
      const resp = await fetch(`/api/stock/${encodeURIComponent(stock.ticker)}/explain`);
      if (resp.ok) explainData = await resp.json();
    } catch (e) { /* silently ignore network errors */ }
  }

  const explainEl = document.getElementById('explainSection');
  if (explainEl) {
    if (explainData) {
      explainEl.innerHTML =
        renderScoreBreakdown(explainData.score_components) +
        renderTagSection(explainData.tags) +
        renderQuestionsSection(explainData.questions);
    } else {
      explainEl.innerHTML = '';  // hide loading spinner if no data
    }
  }
}

function closeModal() {
  document.getElementById('modal').classList.add('hidden');
  document.getElementById('modal').classList.remove('flex');
}

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
  // Set default active tab based on page
  activeTab = window.IS_JAPAN_PAGE ? 'nikkei225' : 'sp500';

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
            const preferred = window.IS_JAPAN_PAGE
              ? ['nikkei225', 'growth250']
              : ['sp500', 'nasdaq100'];
            const firstKey = preferred.find(k => allResults[k]) || keys[0];
            switchTab(firstKey);
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

// ── CF Analysis Modal ──

let _cfChartAnnual = null;
let _cfChartFcf = null;
let _cfChartQuarterly = null;
let _cfCurrentTicker = null;

function showCfModal(ticker, event) {
  if (event) event.stopPropagation();
  _cfCurrentTicker = ticker;
  document.getElementById('cfModalTitle').textContent = ticker.replace('.T', '');
  document.getElementById('cfModalSubtitle').textContent = '';
  document.getElementById('cfModalLoading').classList.remove('hidden');
  document.getElementById('cfModalError').classList.add('hidden');
  document.getElementById('cfModalContent').classList.add('hidden');
  document.getElementById('cfModal').classList.remove('hidden');
  document.getElementById('cfModal').classList.add('flex');
  _loadCfData(ticker);
}

function closeCfModal() {
  document.getElementById('cfModal').classList.add('hidden');
  document.getElementById('cfModal').classList.remove('flex');
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') { closeCfModal(); closeModal(); }
});

async function refreshCfData() {
  if (!_cfCurrentTicker) return;
  // Clear cache then reload
  await fetch(`/api/cf_cache/clear/${encodeURIComponent(_cfCurrentTicker)}`, { method: 'DELETE' });
  document.getElementById('cfModalLoading').classList.remove('hidden');
  document.getElementById('cfModalContent').classList.add('hidden');
  document.getElementById('cfModalError').classList.add('hidden');
  _loadCfData(_cfCurrentTicker);
}

async function _loadCfData(ticker) {
  try {
    const resp = await fetch(`/api/cf_analysis/${encodeURIComponent(ticker)}`);
    if (!resp.ok) {
      const err = await resp.json();
      throw new Error(err.error || 'データ取得失敗');
    }
    const data = await resp.json();
    document.getElementById('cfModalLoading').classList.add('hidden');
    _renderCfModal(data);
  } catch (e) {
    document.getElementById('cfModalLoading').classList.add('hidden');
    document.getElementById('cfModalError').classList.remove('hidden');
    document.getElementById('cfModalError').textContent = 'エラー: ' + e.message;
  }
}

function _renderCfModal(data) {
  const unit = data.unit || '億円';
  const fmt = (v) => v != null ? v.toLocaleString() : '-';
  const fmtU = (v) => v != null ? v.toLocaleString() + ' ' + unit : '-';

  document.getElementById('cfModalTitle').textContent =
    `${data.ticker.replace('.T', '')} — CF分析`;
  document.getElementById('cfModalSubtitle').textContent = data.company_name;

  // ── Summary Cards ──
  const s = data.summary || {};
  const trendColor = s.fcf_trend === '増加' ? 'text-emerald-500' :
                     s.fcf_trend === '減少' ? 'text-rose-400' : 'text-slate-500';
  document.getElementById('cfSummaryCards').innerHTML = [
    ['直近FCF', fmtU(s.latest_fcf)],
    ['3年平均FCF', fmtU(s.avg_fcf_3y)],
    ['3年平均営業CF', fmtU(s.avg_operating_cf_3y)],
    ['FCFトレンド', s.fcf_trend || '-'],
  ].map(([label, val], i) => `
    <div class="bg-slate-50 dark:bg-gray-800 rounded-xl p-3 border border-slate-100 dark:border-gray-700">
      <div class="text-[10px] text-slate-400 dark:text-gray-500">${label}</div>
      <div class="font-bold text-sm mt-0.5 ${i === 3 ? trendColor : 'text-primary-600 dark:text-primary-400'}">${val}</div>
    </div>
  `).join('');

  // ── M&A Capacity ──
  const ma = data.ma_capacity || {};
  document.getElementById('cfMaCards').innerHTML = [
    ['ネットキャッシュ', fmtU(ma.net_cash)],
    ['年間FCF (3年平均)', fmtU(ma.annual_fcf)],
    ['M&A 実弾 (3年分)', fmtU(ma.capacity_3y)],
    ['M&A 実弾 (5年分)', fmtU(ma.capacity_5y)],
  ].map(([label, val]) => `
    <div class="bg-white dark:bg-gray-900 rounded-lg p-3 border border-amber-100 dark:border-amber-900/30">
      <div class="text-[10px] text-amber-600 dark:text-amber-400">${label}</div>
      <div class="font-bold text-sm mt-0.5 text-amber-700 dark:text-amber-300">${val}</div>
    </div>
  `).join('');

  // ── Charts ──
  const isDark = document.documentElement.classList.contains('dark');
  const textColor = isDark ? '#9ca3af' : '#6b7280';
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';

  const timeline = data.timeline || [];
  const labels = timeline.map(e => e.period);

  // Annual CF 三区分
  if (_cfChartAnnual) _cfChartAnnual.destroy();
  _cfChartAnnual = new Chart(
    document.getElementById('cfChartAnnual').getContext('2d'), {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label: '営業CF', data: timeline.map(e => e.operating_cf),
            backgroundColor: 'rgba(52,211,153,0.7)', borderColor: 'rgba(52,211,153,1)', borderWidth: 1 },
          { label: '投資CF', data: timeline.map(e => e.investing_cf),
            backgroundColor: 'rgba(251,113,133,0.7)', borderColor: 'rgba(251,113,133,1)', borderWidth: 1 },
          { label: '財務CF', data: timeline.map(e => e.financing_cf),
            backgroundColor: 'rgba(147,197,253,0.7)', borderColor: 'rgba(147,197,253,1)', borderWidth: 1 },
        ],
      },
      options: _cfChartOpts(textColor, gridColor, unit),
    }
  );

  // FCF 年次
  if (_cfChartFcf) _cfChartFcf.destroy();
  const fcfColors = timeline.map(e =>
    e.fcf >= 0 ? 'rgba(52,211,153,0.75)' : 'rgba(251,113,133,0.75)'
  );
  _cfChartFcf = new Chart(
    document.getElementById('cfChartFcf').getContext('2d'), {
      type: 'bar',
      data: {
        labels,
        datasets: [{ label: 'FCF', data: timeline.map(e => e.fcf),
          backgroundColor: fcfColors, borderWidth: 0 }],
      },
      options: _cfChartOpts(textColor, gridColor, unit),
    }
  );

  // Quarterly CF
  const qData = (data.quarterly || []).filter(e => e.operating_cf != null);
  const qContainer = document.getElementById('cfChartQuarterly').parentElement;
  if (_cfChartQuarterly) { _cfChartQuarterly.destroy(); _cfChartQuarterly = null; }
  if (qData.length === 0) {
    // EDINET quarterly reports don't include CF statements
    document.getElementById('cfChartQuarterly').style.display = 'none';
    if (!qContainer.querySelector('.cf-no-quarterly')) {
      const msg = document.createElement('div');
      msg.className = 'cf-no-quarterly flex items-center justify-center h-full text-xs text-slate-400 dark:text-gray-500';
      msg.textContent = '四半期CFデータなし（JQUANTS_API_KEY未設定または取得エラー）';
      qContainer.appendChild(msg);
    }
  } else {
    document.getElementById('cfChartQuarterly').style.display = '';
    const noMsg = qContainer.querySelector('.cf-no-quarterly');
    if (noMsg) noMsg.remove();
    _cfChartQuarterly = new Chart(
      document.getElementById('cfChartQuarterly').getContext('2d'), {
        type: 'bar',
        data: {
          labels: qData.map(e => e.period),
          datasets: [{ label: '営業CF (四半期)', data: qData.map(e => e.operating_cf),
            backgroundColor: qData.map(e => (e.operating_cf || 0) >= 0 ? 'rgba(52,211,153,0.65)' : 'rgba(251,113,133,0.65)'),
            borderWidth: 0 }],
        },
        options: _cfChartOpts(textColor, gridColor, unit),
      }
    );
  }

  // ── Detail Table ──
  const retCls = (v) => v == null ? '' : v >= 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-400 dark:text-rose-300';
  document.getElementById('cfDetailTable').innerHTML = [...timeline].reverse().map(e => `
    <tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/30">
      <td class="px-3 py-2 font-medium whitespace-nowrap">${e.period}</td>
      <td class="px-3 py-2 text-right font-mono ${retCls(e.operating_cf)}">${fmt(e.operating_cf)}</td>
      <td class="px-3 py-2 text-right font-mono ${retCls(e.investing_cf)}">${fmt(e.investing_cf)}</td>
      <td class="px-3 py-2 text-right font-mono ${retCls(e.financing_cf)}">${fmt(e.financing_cf)}</td>
      <td class="px-3 py-2 text-right font-mono text-slate-400">${fmt(e.capex)}</td>
      <td class="px-3 py-2 text-right font-mono font-semibold ${retCls(e.fcf)}">${fmt(e.fcf)}</td>
    </tr>
  `).join('');

  document.getElementById('cfModalContent').classList.remove('hidden');
}

function _cfChartOpts(textColor, gridColor, unit) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { labels: { color: textColor, font: { size: 11 } } },
      tooltip: {
        callbacks: {
          label: (ctx) => `${ctx.dataset.label}: ${(ctx.raw || 0).toLocaleString()} ${unit}`,
        },
      },
    },
    scales: {
      x: { ticks: { color: textColor, font: { size: 10 } }, grid: { display: false } },
      y: { ticks: { color: textColor, font: { size: 10 } }, grid: { color: gridColor } },
    },
  };
}

init();
