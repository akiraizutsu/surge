/* Momentum Screener - Frontend Logic */

// ── State ──
let currentIndex = 'sp500';
let activeTab = 'sp500';
let screeningData = null;
let allResults = {};  // {sp500: data, nasdaq100: data, nikkei225: data}
let sortKey = localStorage.getItem('surge-sort-key') || 'rank';
let sortAsc = localStorage.getItem('surge-sort-asc') !== 'false';
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
let chartQualityMatrix = null;

// ETA tracking
let screeningStartTime = null;
let etaRateHistory = [];

// Comparison mode
let comparisonTickers = [];

// Column visibility (persisted in localStorage)
const DEFAULT_HIDDEN_COLS = ['ret_1d', 'ret_1w', 'vol_ratio', 'rs_1m', 'rs_3m'];
let hiddenColumns = JSON.parse(localStorage.getItem('surge-hidden-cols') || 'null') || [...DEFAULT_HIDDEN_COLS];

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
  document.documentElement.classList.add('dark-transition');
  document.documentElement.classList.toggle('dark');
  const isDark = document.documentElement.classList.contains('dark');
  localStorage.setItem('dark-mode', isDark);
  document.getElementById('darkIcon').textContent = isDark ? '\u2600' : '\u263E';
  if (screeningData) renderCharts(screeningData);
  setTimeout(() => document.documentElement.classList.remove('dark-transition'), 350);
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
  if (price == null || isNaN(price)) return '-';
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
  document.getElementById('errorRecovery')?.classList.add('hidden');

  // ETA tracking
  screeningStartTime = Date.now();
  etaRateHistory = [];
  const etaEl = document.getElementById('etaText');
  if (etaEl) etaEl.textContent = '';

  // Browser notification permission (user gesture context)
  if ('Notification' in window && Notification.permission === 'default') {
    Notification.requestPermission();
  }

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
      // If 409 (already running), try to clear stale state and retry once
      if (resp.status === 409) {
        await fetch('/api/clear_error', { method: 'POST' });
        const retry = await fetch('/api/screen', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            index: window.IS_JAPAN_PAGE ? 'japan_all' : 'us_all',
            top_n: parseInt(document.getElementById('topN').value),
          }),
        });
        if (retry.ok) {
          pollProgress();
          return;
        }
        const retryErr = await retry.json();
        throw new Error(retryErr.error || 'スクリーニングの開始に失敗しました');
      }
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

      // ETA calculation
      if (status.progress_pct > 3 && status.running && screeningStartTime) {
        const elapsed = (Date.now() - screeningStartTime) / 1000;
        const rate = status.progress_pct / elapsed;
        etaRateHistory.push(rate);
        if (etaRateHistory.length > 5) etaRateHistory.shift();
        const avgRate = etaRateHistory.reduce((a, b) => a + b, 0) / etaRateHistory.length;
        const remaining = (100 - status.progress_pct) / avgRate;
        const mins = Math.floor(remaining / 60);
        const secs = Math.floor(remaining % 60);
        const etaEl = document.getElementById('etaText');
        if (etaEl) etaEl.textContent = mins > 0 ? `(残り約${mins}分${secs}秒)` : `(残り約${secs}秒)`;
      }

      if (!status.running) {
        clearInterval(pollTimer);
        pollTimer = null;

        document.getElementById('btnRun').disabled = false;
        document.getElementById('btnRun').textContent = 'スクリーニング実行';
        const etaEl = document.getElementById('etaText');
        if (etaEl) etaEl.textContent = '';

        if (status.error) {
          // Error recovery UI
          document.getElementById('progressArea').classList.add('hidden');
          const errorArea = document.getElementById('errorRecovery');
          if (errorArea) {
            errorArea.classList.remove('hidden');
            document.getElementById('errorMessage').textContent = status.error;
            document.getElementById('errorRetryBtn').onclick = async () => {
              await fetch('/api/clear_error', { method: 'POST' });
              errorArea.classList.add('hidden');
              runScreening();
            };
          } else {
            document.getElementById('statusText').textContent = 'エラー: ' + status.error;
          }
          // Browser notification on error
          if ('Notification' in window && Notification.permission === 'granted') {
            const n = new Notification('Surge - スクリーニングエラー', {
              body: status.error,
              icon: '/static/favicon.svg',
              tag: 'screening-error',
            });
            setTimeout(() => n.close(), 8000);
          }
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
            const firstKey = preferred.find(k => allResults[k]);
            if (firstKey) {
              activeTab = firstKey;
              switchTab(firstKey);
            }
            // No fallback to other page's indices
          }
          document.getElementById('progressArea').classList.add('hidden');
          // Browser notification on success
          if ('Notification' in window && Notification.permission === 'granted') {
            const total = allResults[activeTab]?.total_screened || 0;
            const n = new Notification('Surge - スクリーニング完了', {
              body: `${total}銘柄の分析が完了しました`,
              icon: '/static/favicon.svg',
              tag: 'screening-complete',
            });
            setTimeout(() => n.close(), 8000);
          }
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

  // Sprint 6: Daily report panel + change detection
  renderDailyReport(data.daily_report || null, data.changes || null);
  updateNotificationBadge();

  // Charts
  document.getElementById('chartsArea').classList.remove('hidden');
  document.getElementById('rsiChartArea').classList.remove('hidden');
  renderCharts(data);

  // ADL chart
  loadBreadthChart(activeTab);

  // Sprint 4: show weight preset bar
  document.getElementById('weightPresetBar')?.classList.remove('hidden');

  // Sub-tabs and tables — always show; hide tab buttons when no data
  document.getElementById('subTabs').classList.remove('hidden');
  const hasContrarian = data.value_gap_ranking && data.value_gap_ranking.length > 0;
  const hasTimeArb    = data.time_arb_ranking  && data.time_arb_ranking.length > 0;
  const hasSmallcap   = data.smallcap_ranking  && data.smallcap_ranking.length > 0;
  const hasRotation   = data.sector_rotation   && data.sector_rotation.length > 0;
  const hasBreakout   = data.breakout_ranking  && data.breakout_ranking.length > 0;
  const hasQuality      = data.momentum_ranking    && data.momentum_ranking.some(r => r.quality_score != null);
  const hasSeed         = data.seed_ranking        && data.seed_ranking.length > 0;
  const hasUsAdvanced   = data.us_advanced_ranking && data.us_advanced_ranking.length > 0;
  const hasCorrelation  = data.sector_correlations && data.sector_correlations.sectors && data.sector_correlations.sectors.length >= 3;
  const contrarianBtn   = document.getElementById('subTabContrarian');
  const timeArbBtn      = document.getElementById('subTabTimeArb');
  const smallcapBtn     = document.getElementById('subTabSmallcap');
  const rotationBtn     = document.getElementById('subTabRotation');
  const breakoutBtn     = document.getElementById('subTabBreakout');
  const qualityBtn      = document.getElementById('subTabQuality');
  const seedBtn         = document.getElementById('subTabSeed');
  const usAdvancedBtn   = document.getElementById('subTabUsAdvanced');
  const correlationBtn  = document.getElementById('subTabCorrelation');
  if (contrarianBtn)  contrarianBtn.style.display  = hasContrarian  ? '' : 'none';
  if (timeArbBtn)     timeArbBtn.style.display     = hasTimeArb     ? '' : 'none';
  if (smallcapBtn)    smallcapBtn.style.display    = hasSmallcap    ? '' : 'none';
  if (rotationBtn)    rotationBtn.style.display    = hasRotation    ? '' : 'none';
  if (breakoutBtn)    breakoutBtn.style.display    = hasBreakout    ? '' : 'none';
  if (qualityBtn)     qualityBtn.style.display     = hasQuality     ? '' : 'none';
  if (seedBtn)        seedBtn.style.display        = hasSeed        ? '' : 'none';
  if (usAdvancedBtn)  usAdvancedBtn.style.display  = hasUsAdvanced  ? '' : 'none';
  if (correlationBtn) correlationBtn.style.display = hasCorrelation ? '' : 'none';
  // Fall back to momentum if active tab has no data
  if (!hasContrarian  && activeSubTab === 'contrarian')   activeSubTab = 'momentum';
  if (!hasTimeArb     && activeSubTab === 'time_arb')     activeSubTab = 'momentum';
  if (!hasSmallcap    && activeSubTab === 'smallcap')     activeSubTab = 'momentum';
  if (!hasRotation    && activeSubTab === 'rotation')     activeSubTab = 'momentum';
  if (!hasBreakout    && activeSubTab === 'breakout')     activeSubTab = 'momentum';
  if (!hasQuality     && activeSubTab === 'quality')      activeSubTab = 'momentum';
  if (!hasSeed        && activeSubTab === 'seed')         activeSubTab = 'momentum';
  if (!hasUsAdvanced  && activeSubTab === 'us_advanced')  activeSubTab = 'momentum';
  if (!hasCorrelation && activeSubTab === 'correlation')  activeSubTab = 'momentum';
  switchSubTab(activeSubTab);

  // Check user alerts against current data
  checkAlerts(data.momentum_ranking);
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
    quality: 'subTabQuality', seed: 'subTabSeed',
    us_advanced: 'subTabUsAdvanced', correlation: 'subTabCorrelation',
  };
  document.getElementById(tabBtnMap[tab])?.classList.add('active');

  // Hide all sub-tab areas
  ['tableArea', 'contrarianTableArea', 'sectorRotationArea', 'breakoutTableArea',
   'timeArbTableArea', 'smallcapTableArea', 'qualityMatrixArea', 'seedTableArea',
   'usAdvancedTableArea', 'correlationMatrixArea'].forEach(id => {
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
  } else if (tab === 'quality') {
    document.getElementById('qualityMatrixArea').classList.remove('hidden');
    if (screeningData && screeningData.momentum_ranking) renderQualityMatrix(screeningData.momentum_ranking);
    refreshDataQuality();
  } else if (tab === 'seed') {
    document.getElementById('seedTableArea').classList.remove('hidden');
    if (screeningData && screeningData.seed_ranking) renderSeedTable(screeningData.seed_ranking);
  } else if (tab === 'us_advanced') {
    document.getElementById('usAdvancedTableArea')?.classList.remove('hidden');
    if (screeningData && screeningData.us_advanced_ranking) renderUsAdvancedTable(screeningData.us_advanced_ranking);
  } else if (tab === 'correlation') {
    document.getElementById('correlationMatrixArea')?.classList.remove('hidden');
    if (screeningData && screeningData.sector_correlations) renderCorrelationMatrix(screeningData.sector_correlations);
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
    <div class="grid grid-cols-3 gap-3 mb-6">
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

    ${(stock.vg_analyst_gap != null) ? `
    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">Sprint 5: Value Gap 分解</h3>
    <div class="grid grid-cols-2 gap-3">
      ${[
        ['アナリスト乖離', stock.vg_analyst_gap, 'text-amber-600 dark:text-amber-400', 'アナリスト目標価格と推奨の複合'],
        ['キャッシュバリュー', stock.vg_cash_value, 'text-sky-600 dark:text-sky-400', '低PERと配当利回り'],
        ['クオリティバリュー', stock.vg_quality_value, 'text-emerald-600 dark:text-emerald-400', 'EPS・売上成長の継続'],
        ['期待リセット', stock.vg_expectation_reset, 'text-violet-600 dark:text-violet-400', '株価下落+RSI低下（失望完了）'],
      ].map(([l, v, cls, desc]) => `
        <div class="bg-slate-50 dark:bg-gray-800 rounded-xl p-3 border border-slate-100 dark:border-gray-700">
          <div class="text-[10px] text-slate-400 dark:text-gray-500 mb-1">${l}</div>
          <div class="text-2xl font-bold ${cls}">${v != null ? v.toFixed(1) : '-'}</div>
          <div class="text-[9px] text-slate-400 dark:text-gray-500 mt-1">${desc}</div>
        </div>
      `).join('')}
    </div>` : ''}
  `;

  document.getElementById('modal').classList.remove('hidden');
  document.getElementById('modal').classList.add('flex');
}

// ── Seed Table (Sprint 5) ──
function renderSeedTable(ranking) {
  const tbody = document.getElementById('seedTableBody');
  if (!tbody) return;
  const fmtPct = (v) => v != null ? (v > 0 ? '+' : '') + v + '%' : '-';
  const retClass = (v) => v > 0 ? 'text-emerald-600 dark:text-emerald-400' : v < 0 ? 'text-rose-400 dark:text-rose-300' : '';

  tbody.innerHTML = ranking.map((r, idx) => {
    const tags = (r.seed_tags || []).map(t => `<span class="inline-block px-1.5 py-0.5 text-[9px] rounded-full bg-teal-100 dark:bg-teal-900/30 text-teal-700 dark:text-teal-400 font-medium whitespace-nowrap">${t}</span>`).join('');
    const t = r.technicals || {};
    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 cursor-pointer transition-colors" onclick='showDetail(${JSON.stringify(r).replace(/'/g, "&#39;")})'>
      <td class="px-3 py-2 text-slate-400 text-xs">${idx + 1}</td>
      <td class="px-3 py-2 font-semibold text-primary-600 dark:text-primary-400 text-xs">${r.ticker}</td>
      <td class="px-3 py-2 text-slate-500 dark:text-gray-400 text-xs hidden md:table-cell max-w-[150px] truncate">${r.name}</td>
      <td class="px-3 py-2 text-right">
        <span class="text-lg font-bold text-teal-600 dark:text-teal-400">${r.seed_score != null ? r.seed_score.toFixed(1) : '-'}</span>
      </td>
      <td class="px-3 py-2 text-right font-bold text-xs ${r.capital_grade ? ({'A':'text-emerald-600','B':'text-sky-500','C':'text-amber-500','D':'text-orange-500','F':'text-rose-500'}[r.capital_grade] || '') : ''}">${r.capital_grade || '-'}</td>
      <td class="px-3 py-2 text-right font-mono text-xs">${formatPrice(r.price)}</td>
      <td class="px-3 py-2 text-right font-mono text-xs hidden sm:table-cell ${retClass(t.ret_1m)}">${fmtPct(t.ret_1m)}</td>
      <td class="px-3 py-2 text-xs hidden lg:table-cell">
        <div class="flex flex-wrap gap-1">${tags}</div>
      </td>
      <td class="px-3 py-2 text-xs text-slate-500 dark:text-gray-400 hidden xl:table-cell max-w-[180px] truncate">${r.seed_note || '-'}</td>
    </tr>`;
  }).join('');
}

// ── Sprint 7: US Advanced Table ──
function renderUsAdvancedTable(ranking) {
  const tbody = document.getElementById('usAdvancedTableBody');
  if (!tbody) return;

  const dirColor = (d) => {
    if (!d) return 'text-slate-400';
    if (d.includes('上方') || d.includes('買い') || d.includes('ポジティブ') || d.includes('コール')) return 'text-emerald-600 dark:text-emerald-400';
    if (d.includes('下方') || d.includes('売り') || d.includes('ネガティブ') || d.includes('プット')) return 'text-rose-400 dark:text-rose-300';
    return 'text-slate-500 dark:text-gray-400';
  };

  tbody.innerHTML = ranking.map((r, idx) => {
    const adv = r.us_advanced || {};
    const eps = adv.eps_revision || {};
    const inst = adv.institutional_flow || {};
    const drift = adv.earnings_drift || {};
    const score = r.us_advanced_score;
    const tags = (r.us_advanced_tags || []).map(t =>
      `<span class="inline-block px-1.5 py-0.5 text-[9px] rounded-full bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-400 font-medium whitespace-nowrap">${t}</span>`
    ).join('');
    const scoreColor = score >= 70 ? 'text-emerald-600 dark:text-emerald-400' : score >= 55 ? 'text-amber-600 dark:text-amber-400' : 'text-slate-500 dark:text-gray-400';
    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 cursor-pointer transition-colors" onclick='showDetail(${JSON.stringify(r).replace(/'/g, "&#39;")})'>
      <td class="px-3 py-2 text-slate-400 text-xs">${idx + 1}</td>
      <td class="px-3 py-2 font-semibold text-primary-600 dark:text-primary-400 text-xs">${r.ticker}</td>
      <td class="px-3 py-2 text-slate-500 dark:text-gray-400 text-xs hidden md:table-cell max-w-[150px] truncate">${r.name}</td>
      <td class="px-3 py-2 text-right font-bold text-lg ${scoreColor}">${score != null ? score.toFixed(1) : '-'}</td>
      <td class="px-3 py-2 text-xs hidden sm:table-cell ${dirColor(eps.direction)}">${eps.direction || '-'}</td>
      <td class="px-3 py-2 text-xs hidden sm:table-cell ${dirColor(inst.direction)}">${inst.direction || '-'}${inst.ownership_pct ? ` <span class="text-slate-400">${inst.ownership_pct}%</span>` : ''}</td>
      <td class="px-3 py-2 text-xs hidden lg:table-cell ${dirColor(drift.direction)}">${drift.direction || '-'}</td>
      <td class="px-3 py-2 text-xs hidden lg:table-cell"><div class="flex flex-wrap gap-1">${tags || '<span class="text-slate-300">-</span>'}</div></td>
    </tr>`;
  }).join('');
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

// ── Column Visibility ──
function toggleColumnMenu() {
  const menu = document.getElementById('colMenu');
  if (!menu) return;
  menu.classList.toggle('hidden');
}

function toggleColumn(col) {
  const idx = hiddenColumns.indexOf(col);
  if (idx >= 0) hiddenColumns.splice(idx, 1);
  else hiddenColumns.push(col);
  localStorage.setItem('surge-hidden-cols', JSON.stringify(hiddenColumns));
  // Update checkmarks in menu
  document.querySelectorAll('#colMenu [data-col]').forEach(el => {
    const check = el.querySelector('.col-check');
    if (check) check.textContent = hiddenColumns.includes(el.dataset.col) ? '' : '✓';
  });
  // Re-apply visibility to table headers and cells
  applyColumnVisibility();
}

function applyColumnVisibility() {
  document.querySelectorAll('[data-key]').forEach(th => {
    const key = th.dataset.key;
    const colIdx = th.cellIndex;
    const hide = hiddenColumns.includes(key);
    th.style.display = hide ? 'none' : '';
    // Apply to all body cells in same column
    const table = th.closest('table');
    if (table) {
      table.querySelectorAll(`tbody tr`).forEach(tr => {
        const td = tr.cells[colIdx];
        if (td) td.style.display = hide ? 'none' : '';
      });
    }
  });
}

// ── CSV Export ──
function exportCSV() {
  if (!screeningData || !screeningData.momentum_ranking) return;
  const ranking = screeningData.momentum_ranking;
  const headers = ['Rank','Ticker','Name','Sector','Price','Score','RSI','1M%','3M%','Vol Ratio','MA50 Dev%','RS1M','RS3M','OBV Slope','Max DD 3M%','Entry Difficulty'];
  const rows = ranking.map(r => {
    const t = r.technicals;
    return [r.rank, r.ticker, `"${(r.name||'').replace(/"/g,'""')}"`, `"${r.sector}"`, r.price, r.momentum_score,
      t.rsi, t.ret_1m, t.ret_3m, t.vol_ratio, t.ma50_dev, t.rs_1m, t.rs_3m,
      t.obv_slope || '', t.max_drawdown_3m || '', r.entry_difficulty || ''].join(',');
  });
  const csv = '\uFEFF' + headers.join(',') + '\n' + rows.join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  const idx = screeningData.index || activeTab;
  a.href = url;
  a.download = `surge_${idx}_${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
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
      ? `<button onclick="showCfModal('${r.ticker}',event)" class="w-7 h-7 flex items-center justify-center rounded-md bg-primary-500/10 hover:bg-primary-500/20 text-primary-500 dark:text-primary-400 text-[10px] font-bold transition-colors cursor-pointer shrink-0">CF</button>`
      : '';
    const compareChecked = comparisonTickers.includes(r.ticker);
    const cmpCls = compareChecked
      ? 'bg-primary-500 border-primary-500 text-white'
      : 'bg-transparent border-slate-300 dark:border-gray-600 text-transparent hover:border-primary-400';
    const starCell = `<td class="px-1.5 sm:px-2 py-2 sm:py-3">
      <div class="flex items-center gap-2.5">
        <button onclick="toggleCompare('${r.ticker}', event)"
          class="w-5 h-5 rounded-[5px] border-[1.5px] flex items-center justify-center text-[11px] leading-none transition-all cursor-pointer shrink-0 ${cmpCls}" title="比較に追加">
          ${compareChecked ? '&#10003;' : '&#10003;'}
        </button>
        <button onclick="toggleStar('${r.ticker}', event)"
          class="text-lg leading-none cursor-pointer hover:scale-110 transition-transform shrink-0 ${starred ? 'text-amber-400' : 'text-slate-300 dark:text-gray-600 hover:text-amber-300'}"
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
    if (t.obv_divergence === 'bullish_div') status += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-emerald-50 dark:bg-emerald-900/20 text-emerald-600 dark:text-emerald-400 font-medium">OBV↑乖離</span>';
    else if (t.obv_divergence === 'bearish_div') status += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-rose-50 dark:bg-rose-900/20 text-rose-500 dark:text-rose-300 font-medium">OBV↓乖離</span>';
    if (t.adx >= 25) status += `<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full ${t.adx >= 40 ? 'bg-violet-100 dark:bg-violet-900/30 text-violet-600 dark:text-violet-400' : 'bg-indigo-50 dark:bg-indigo-900/20 text-indigo-500 dark:text-indigo-400'} font-medium">ADX${Math.round(t.adx)}</span>`;
    if (!isJapanIndex()) {
      if (t.ema9_compliant) status += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-emerald-50 dark:bg-emerald-900/20 text-emerald-600 dark:text-emerald-400 font-medium">W9EMA↑</span>';
      else if (t.ema9_broken) status += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-rose-100 dark:bg-rose-900/30 text-rose-600 dark:text-rose-300 font-medium">W9EMA割れ</span>';
    }
    const f = r.fundamentals;
    if (f && f.days_to_earnings != null && f.days_to_earnings >= 0 && f.days_to_earnings <= 14) {
      const urgency = f.days_to_earnings <= 3 ? 'bg-rose-50 dark:bg-rose-900/20 text-rose-500' : 'bg-slate-100 dark:bg-gray-800 text-slate-500';
      status += `<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full ${urgency} font-medium">決算${f.days_to_earnings}日</span>`;
    }
    if (r.entry_difficulty && r.entry_difficulty !== '様子見') {
      const ED_BG = {
        '良好':         'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400',
        '押し待ち候補': 'bg-sky-100 dark:bg-sky-900/30 text-sky-700 dark:text-sky-400',
        '初動監視':     'bg-violet-100 dark:bg-violet-900/30 text-violet-700 dark:text-violet-400',
        '追いかけ注意': 'bg-rose-100 dark:bg-rose-900/30 text-rose-700 dark:text-rose-400',
        'ボラ高注意':   'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400',
        '決算通過待ち': 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400',
        '地合い依存強め':'bg-slate-100 dark:bg-gray-800 text-slate-600 dark:text-gray-400',
      };
      const edCls = ED_BG[r.entry_difficulty] || 'bg-gray-100 dark:bg-gray-800 text-gray-500';
      status += `<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full ${edCls} font-medium">${r.entry_difficulty}</span>`;
    }
    // Sprint 6: NEW badge for new entries
    const changes = screeningData && screeningData.changes;
    if (changes && changes.new_entries && changes.new_entries.some(e => e.ticker === r.ticker)) {
      status += '<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-teal-100 dark:bg-teal-900/30 text-teal-700 dark:text-teal-400 font-bold">NEW</span>';
    }
    if (changes && changes.score_surges && changes.score_surges.some(e => e.ticker === r.ticker)) {
      const se = changes.score_surges.find(e => e.ticker === r.ticker);
      status += `<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400 font-bold">↑${se.score_delta > 0 ? '+' : ''}${se.score_delta}</span>`;
    }

    // Mini sparkline from returns: 3M ago → 1M ago → 1W ago → 1D ago → now (normalized)
    const sparkPts = [0, t.ret_3m - t.ret_1m, t.ret_3m - t.ret_1w, t.ret_3m - t.ret_1d, t.ret_3m].map(v => v || 0);
    const spkMin = Math.min(...sparkPts), spkMax = Math.max(...sparkPts);
    const spkRange = spkMax - spkMin || 1;
    const spkH = 20, spkW = 40;
    const spkPath = sparkPts.map((v, i) => `${i === 0 ? 'M' : 'L'}${(i / 4) * spkW},${spkH - ((v - spkMin) / spkRange) * spkH}`).join(' ');
    const spkColor = t.ret_1m >= 0 ? '#10b981' : '#f43f5e';
    const sparkSvg = `<svg width="${spkW}" height="${spkH}" class="inline-block align-middle hidden sm:inline-block"><path d="${spkPath}" fill="none" stroke="${spkColor}" stroke-width="1.5" stroke-linecap="round"/></svg>`;

    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 cursor-pointer transition-colors" onclick='showDetail(${JSON.stringify(r).replace(/'/g, "&#39;")})'>
      ${starCell}
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-slate-400 text-xs sm:text-sm whitespace-nowrap">${r.rank}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 font-semibold text-primary-600 dark:text-primary-400 text-xs sm:text-sm whitespace-nowrap"><span class="mr-1.5">${r.ticker}</span>${sparkSvg}</td>
      <td class="px-4 py-3 text-slate-500 dark:text-gray-400 hidden lg:table-cell text-xs max-w-[180px] truncate">${r.name}</td>
      <td class="px-4 py-3 text-xs text-slate-600 dark:text-gray-400 whitespace-nowrap hidden md:table-cell">${r.sector}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm text-slate-900 dark:text-gray-100 whitespace-nowrap hidden sm:table-cell">${formatPrice(r.price)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-bold text-xs sm:text-sm whitespace-nowrap">${r.momentum_score}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 whitespace-nowrap"><div class="flex items-center gap-0.5 sm:gap-1">${status || '<span class="text-slate-300 dark:text-gray-600">-</span>'}</div></td>
      ${!isJapanIndex() ? `<td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell ${sqClass}">${sqScore != null ? sqScore.toFixed(1) : '-'}</td>` : ''}
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

  // Sector ETF return comparison bar chart
  renderSectorETFCompare(rotation);
}

let _chartSectorETF = null;
function renderSectorETFCompare(rotation) {
  const canvas = document.getElementById('chartSectorETF');
  if (!canvas || !rotation || rotation.length === 0) return;
  const isDark = document.documentElement.classList.contains('dark');
  const textColor = isDark ? '#9ca3af' : '#6b7280';
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';

  const sectors = rotation.map(s => s.sector.length > 8 ? s.sector.slice(0, 7) + '…' : s.sector);
  const stock1m = rotation.map(s => s.ret_1m_avg);
  const etf1m = rotation.map(s => s.etf_1m || 0);

  if (_chartSectorETF) _chartSectorETF.destroy();
  _chartSectorETF = new Chart(canvas.getContext('2d'), {
    type: 'bar',
    data: {
      labels: sectors,
      datasets: [
        { label: 'セクター平均 1M', data: stock1m, backgroundColor: 'hsl(var(--hue) 65% 55% / 0.7)', borderRadius: 3 },
        { label: 'ETF 1M', data: etf1m, backgroundColor: isDark ? 'rgba(148,163,184,0.4)' : 'rgba(100,116,139,0.4)', borderRadius: 3 },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false, indexAxis: 'y',
      plugins: {
        legend: { labels: { color: textColor, font: { size: 10 } } },
        tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.x > 0 ? '+' : ''}${ctx.parsed.x}%` } },
      },
      scales: {
        x: { grid: { color: gridColor }, ticks: { color: textColor, callback: v => v + '%' } },
        y: { grid: { display: false }, ticks: { color: textColor, font: { size: 10 } } },
      },
    },
  });
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

// ── Quality Matrix Chart ──
function renderQualityMatrix(ranking) {
  const isDark = document.documentElement.classList.contains('dark');
  const textColor = isDark ? '#9ca3af' : '#6b7280';
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';

  const ctx = document.getElementById('chartQualityMatrix').getContext('2d');
  if (chartQualityMatrix) chartQualityMatrix.destroy();

  // Build entry difficulty color map
  const ED_COLOR = {
    '良好':         '#10b981',
    '押し待ち候補': '#0ea5e9',
    '初動監視':     '#8b5cf6',
    '追いかけ注意': '#f43f5e',
    'ボラ高注意':   '#f97316',
    '決算通過待ち': '#f59e0b',
    '地合い依存強め':'#64748b',
    '様子見':       '#9ca3af',
  };

  const points = ranking
    .filter(r => r.quality_score != null && r.momentum_score != null)
    .map(r => ({
      x: r.momentum_score,
      y: r.quality_score,
      ticker: r.ticker,
      entry_difficulty: r.entry_difficulty || '様子見',
      color: ED_COLOR[r.entry_difficulty] || '#9ca3af',
    }));

  chartQualityMatrix = new Chart(ctx, {
    type: 'scatter',
    data: {
      datasets: [{
        data: points.map(p => ({ x: p.x, y: p.y })),
        backgroundColor: points.map(p => p.color + 'cc'),
        borderColor: points.map(p => p.color),
        borderWidth: 1.5,
        pointRadius: 6,
        pointHoverRadius: 9,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const p = points[ctx.dataIndex];
              return `${p.ticker} | モメンタム:${p.x} 品質:${p.y.toFixed(1)} [${p.entry_difficulty}]`;
            }
          }
        },
      },
      scales: {
        x: {
          min: 0, max: 100,
          title: { display: true, text: 'モメンタムスコア', color: textColor },
          grid: { color: gridColor },
          ticks: { color: textColor },
        },
        y: {
          min: 0, max: 100,
          title: { display: true, text: '品質スコア', color: textColor },
          grid: { color: gridColor },
          ticks: { color: textColor },
        },
      },
    },
    plugins: [{
      // Draw 4-quadrant lines at 50/50
      id: 'quadrantLines',
      beforeDraw(chart) {
        const { ctx, chartArea, scales } = chart;
        const xMid = scales.x.getPixelForValue(50);
        const yMid = scales.y.getPixelForValue(50);
        ctx.save();
        ctx.setLineDash([4, 4]);
        ctx.strokeStyle = isDark ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.12)';
        ctx.lineWidth = 1;
        // Vertical line
        ctx.beginPath();
        ctx.moveTo(xMid, chartArea.top);
        ctx.lineTo(xMid, chartArea.bottom);
        ctx.stroke();
        // Horizontal line
        ctx.beginPath();
        ctx.moveTo(chartArea.left, yMid);
        ctx.lineTo(chartArea.right, yMid);
        ctx.stroke();
        // Quadrant labels
        ctx.font = `11px system-ui`;
        ctx.fillStyle = isDark ? 'rgba(255,255,255,0.18)' : 'rgba(0,0,0,0.15)';
        ctx.fillText('高品質 低モメンタム',  chartArea.left + 8,  chartArea.top + 16);
        ctx.fillText('高品質 高モメンタム',  xMid + 8,           chartArea.top + 16);
        ctx.fillText('低品質 低モメンタム',  chartArea.left + 8,  chartArea.bottom - 8);
        ctx.fillText('低品質 高モメンタム',  xMid + 8,           chartArea.bottom - 8);
        ctx.restore();
      }
    }],
  });

  // Render legend and table below chart
  renderQualityLegend(points);
}

function renderQualityLegend(points) {
  const el = document.getElementById('qualityMatrixLegend');
  if (!el) return;

  const ED_BG = {
    '良好':         'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400',
    '押し待ち候補': 'bg-sky-100 dark:bg-sky-900/30 text-sky-700 dark:text-sky-400',
    '初動監視':     'bg-violet-100 dark:bg-violet-900/30 text-violet-700 dark:text-violet-400',
    '追いかけ注意': 'bg-rose-100 dark:bg-rose-900/30 text-rose-700 dark:text-rose-400',
    'ボラ高注意':   'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400',
    '決算通過待ち': 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400',
    '地合い依存強め':'bg-slate-100 dark:bg-gray-800 text-slate-600 dark:text-gray-400',
    '様子見':       'bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-500',
  };

  // Group by entry_difficulty
  const groups = {};
  points.forEach(p => {
    if (!groups[p.entry_difficulty]) groups[p.entry_difficulty] = [];
    groups[p.entry_difficulty].push(p);
  });

  const order = ['良好','押し待ち候補','初動監視','追いかけ注意','ボラ高注意','決算通過待ち','地合い依存強め','様子見'];

  el.innerHTML = order
    .filter(label => groups[label] && groups[label].length > 0)
    .map(label => {
      const cls = ED_BG[label] || 'bg-gray-100 dark:bg-gray-800 text-gray-500';
      const tickers = groups[label].map(p => p.ticker).join(', ');
      return `<div class="flex items-start gap-2 py-1.5 border-b border-slate-100 dark:border-gray-800 last:border-0">
        <span class="shrink-0 inline-block px-2 py-0.5 text-[11px] font-semibold rounded-full ${cls} whitespace-nowrap">${label}</span>
        <span class="text-[11px] text-slate-500 dark:text-gray-400 leading-relaxed">${tickers}</span>
      </div>`;
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
    localStorage.setItem('surge-sort-key', sortKey);
    localStorage.setItem('surge-sort-asc', sortAsc);

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
    const pct = Math.max(0, Math.min(isNaN(c.percentile_value) ? 0 : (c.percentile_value || 0), 100));
    const barColor = pct >= 70 ? '#10b981' : pct >= 40 ? '#f59e0b' : '#f43f5e';
    const fmtRaw = (name, val) => {
      if (val == null || isNaN(val)) return '-';
      if (name === 'vol_ratio') return val.toFixed(2) + 'x';
      if (name === 'rsi') return val.toFixed(1);
      return (val >= 0 ? '+' : '') + val.toFixed(2) + '%';
    };
    return `
      <div class="flex items-center gap-2 py-1">
        <div class="w-20 text-[11px] text-slate-500 dark:text-gray-400 text-right shrink-0">${c.label}</div>
        <div class="flex-1 bg-slate-100 dark:bg-gray-700 rounded-full h-2 overflow-hidden">
          <div class="h-2 rounded-full transition-all" style="width:${pct}%;background:${barColor}"></div>
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

    ${stock.quality_score != null ? `
    <div class="grid grid-cols-2 gap-4 mb-6">
      <div class="bg-emerald-50 dark:bg-emerald-950/20 rounded-xl p-4">
        <div class="text-xs text-slate-500 dark:text-gray-400">品質スコア</div>
        <div class="text-3xl font-bold text-emerald-600 dark:text-emerald-400">${stock.quality_score.toFixed(1)}</div>
        ${stock.quality_components ? `<div class="mt-2 text-[10px] text-slate-400 dark:text-gray-500 space-y-0.5">
          <div>ATR%: ${stock.quality_components.atr_pct != null ? stock.quality_components.atr_pct + '%' : '-'} &nbsp;上下出来高比: ${stock.quality_components.up_down_vol_ratio != null ? stock.quality_components.up_down_vol_ratio : '-'}</div>
          <div>ギャップ率: ${stock.quality_components.gap_dep != null ? stock.quality_components.gap_dep : '-'} &nbsp;ヒゲ比率: ${stock.quality_components.wick_ratio != null ? stock.quality_components.wick_ratio : '-'}</div>
        </div>` : ''}
      </div>
      <div class="bg-slate-50 dark:bg-gray-800 rounded-xl p-4 flex flex-col justify-between">
        <div class="text-xs text-slate-500 dark:text-gray-400">エントリー難易度</div>
        ${stock.entry_difficulty ? (() => {
          const ED_BG = {
            '良好':         'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400',
            '押し待ち候補': 'bg-sky-100 dark:bg-sky-900/30 text-sky-700 dark:text-sky-400',
            '初動監視':     'bg-violet-100 dark:bg-violet-900/30 text-violet-700 dark:text-violet-400',
            '追いかけ注意': 'bg-rose-100 dark:bg-rose-900/30 text-rose-700 dark:text-rose-400',
            'ボラ高注意':   'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400',
            '決算通過待ち': 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400',
            '地合い依存強め':'bg-slate-100 dark:bg-gray-800 text-slate-600 dark:text-gray-400',
            '様子見':       'bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-500',
          };
          const edCls = ED_BG[stock.entry_difficulty] || 'bg-gray-100 dark:bg-gray-800 text-gray-500';
          return `<span class="inline-block mt-2 px-2.5 py-1 text-sm font-semibold rounded-full ${edCls}">${stock.entry_difficulty}</span>`;
        })() : '-'}
      </div>
    </div>` : ''}

    ${(stock.seed_score != null && stock.seed_score >= 20) ? `
    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">種まき度スコア（日本株）</h3>
    <div class="bg-teal-50 dark:bg-teal-950/20 rounded-xl p-4 mb-6 border border-teal-100 dark:border-teal-900/30">
      <div class="flex items-center justify-between mb-3">
        <div>
          <div class="text-xs text-slate-500 dark:text-gray-400">種まき度スコア</div>
          <div class="text-3xl font-bold text-teal-600 dark:text-teal-400">${stock.seed_score.toFixed(1)}</div>
        </div>
        <div class="flex flex-wrap gap-1 justify-end">
          ${(stock.seed_tags || []).map(tag => `<span class="inline-block px-2 py-0.5 text-[10px] rounded-full bg-teal-100 dark:bg-teal-900/40 text-teal-700 dark:text-teal-400 font-medium">${tag}</span>`).join('')}
        </div>
      </div>
      ${stock.seed_note ? `<div class="text-xs text-slate-500 dark:text-gray-400">${stock.seed_note}</div>` : ''}
      ${stock.seed_components ? `
      <div class="grid grid-cols-5 gap-1.5 mt-3">
        ${[['設備投資', stock.seed_components.capex_surge], ['CF黒字', stock.seed_components.ocf_positive], ['売上', stock.seed_components.revenue_growth], ['利益減', stock.seed_components.earnings_dip], ['株価低下', stock.seed_components.price_disappoint]].map(([l, v]) => `
          <div class="text-center">
            <div class="text-[9px] text-slate-400 dark:text-gray-500">${l}</div>
            <div class="text-xs font-semibold text-teal-600 dark:text-teal-400">${v != null ? v.toFixed(0) : '-'}</div>
          </div>`).join('')}
      </div>` : ''}
    </div>` : ''}

    ${stock.capital_score != null ? `
    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">資本配分スコア</h3>
    <div class="bg-indigo-50 dark:bg-indigo-950/20 rounded-xl p-4 mb-6 border border-indigo-100 dark:border-indigo-900/30">
      <div class="flex items-center justify-between mb-3">
        <div>
          <div class="text-xs text-slate-500 dark:text-gray-400">資本配分スコア</div>
          <div class="text-3xl font-bold text-indigo-600 dark:text-indigo-400">${stock.capital_score.toFixed(1)}</div>
        </div>
        <span class="text-2xl font-bold px-3 py-1 rounded-lg ${{'A':'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400','B':'bg-sky-100 dark:bg-sky-900/30 text-sky-700 dark:text-sky-400','C':'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400','D':'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400','F':'bg-rose-100 dark:bg-rose-900/30 text-rose-600 dark:text-rose-400'}[stock.capital_grade] || 'bg-gray-100 text-gray-500'}">${stock.capital_grade || '-'}</span>
      </div>
      ${stock.capital_components ? `
      <div class="grid grid-cols-4 gap-2">
        ${[['営業CF', 'ocf_stability'], ['設備投資', 'capex_consistency'], ['FCF', 'fcf_quality'], ['ネットキャッシュ', 'net_cash_strength'], ['希薄化', 'dilution_risk'], ['負債耐性', 'debt_tolerance'], ['株主還元', 'shareholder_return'], ['M&A余力', 'mna_capacity']].map(([l, k]) => {
          const v = stock.capital_components[k] ?? 0;
          const pips = [1,2,3,4,5].map(i => `<span class="inline-block w-2 h-2 rounded-full ${i <= v ? 'bg-indigo-500 dark:bg-indigo-400' : 'bg-slate-200 dark:bg-gray-700'}"></span>`).join('');
          return `<div class="bg-white dark:bg-gray-800 rounded-lg p-2 text-center border border-slate-100 dark:border-gray-700">
            <div class="text-[9px] text-slate-400 dark:text-gray-500 mb-1">${l}</div>
            <div class="flex gap-0.5 justify-center">${pips}</div>
          </div>`;
        }).join('')}
      </div>` : ''}
    </div>` : ''}

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
        ['OBVスロープ', t.obv_slope != null ? (t.obv_slope > 0 ? '+' : '') + t.obv_slope + '%' : '-'],
        ['OBV乖離', {'bullish_div':'強気乖離','bearish_div':'弱気乖離','none':'-'}[t.obv_divergence] || '-'],
        ['最大DD(3M)', t.max_drawdown_3m != null ? t.max_drawdown_3m + '%' : '-'],
        ['現在DD', t.current_drawdown != null ? t.current_drawdown + '%' : '-'],
        ['ADX', t.adx != null ? t.adx + (t.adx >= 25 ? ' (強トレンド)' : '') : '-'],
      ].map(([label, val]) => `
        <div class="bg-slate-50 dark:bg-gray-800 rounded-lg p-2.5 text-center border border-slate-100 dark:border-gray-700">
          <div class="text-[10px] text-slate-400 dark:text-gray-500">${label}</div>
          <div class="font-semibold text-sm text-slate-900 dark:text-gray-100">${val}</div>
        </div>
      `).join('')}
    </div>

    ${(t.support_levels && t.support_levels.length > 0) || (t.resistance_levels && t.resistance_levels.length > 0) ? `
    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">サポート / レジスタンス</h3>
    <div class="mb-6 grid grid-cols-2 gap-3">
      <div class="bg-emerald-50 dark:bg-emerald-950/20 rounded-xl p-3 border border-emerald-100 dark:border-emerald-900/30">
        <div class="text-[10px] text-emerald-600 dark:text-emerald-400 font-medium mb-2">サポート (支持線)</div>
        ${(t.support_levels || []).map((lv, i) => `<div class="text-sm font-mono text-emerald-700 dark:text-emerald-300">${formatPrice(lv)} <span class="text-[10px] text-emerald-500/60">S${i+1}</span></div>`).join('') || '<div class="text-xs text-slate-400">なし</div>'}
      </div>
      <div class="bg-rose-50 dark:bg-rose-950/20 rounded-xl p-3 border border-rose-100 dark:border-rose-900/30">
        <div class="text-[10px] text-rose-600 dark:text-rose-400 font-medium mb-2">レジスタンス (抵抗線)</div>
        ${(t.resistance_levels || []).map((lv, i) => `<div class="text-sm font-mono text-rose-700 dark:text-rose-300">${formatPrice(lv)} <span class="text-[10px] text-rose-500/60">R${i+1}</span></div>`).join('') || '<div class="text-xs text-slate-400">なし</div>'}
      </div>
    </div>` : ''}

    ${!isJapanIndex() && t.ema9 != null ? `
    <h3 class="text-xs font-medium text-slate-500 dark:text-gray-400 mb-3 tracking-wider">週足9EMAトレンド準拠</h3>
    <div class="mb-6 bg-white dark:bg-gray-900 rounded-xl border border-slate-200 dark:border-gray-800 p-4">
      <div class="flex items-center justify-between mb-3">
        <div class="flex items-center gap-2">
          ${t.ema9_compliant
            ? '<span class="inline-flex items-center gap-1 px-2.5 py-1 rounded-full bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400 text-sm font-semibold">✓ W9EMA準拠中</span>'
            : t.ema9_broken
              ? '<span class="inline-flex items-center gap-1 px-2.5 py-1 rounded-full bg-rose-100 dark:bg-rose-900/30 text-rose-600 dark:text-rose-300 text-sm font-semibold">✗ W9EMA割れ</span>'
              : '<span class="inline-flex items-center gap-1 px-2.5 py-1 rounded-full bg-slate-100 dark:bg-gray-800 text-slate-500 text-sm font-semibold">― 中立</span>'
          }
        </div>
        <div class="text-right">
          <div class="text-xs text-slate-400 dark:text-gray-500">準拠スコア</div>
          <div class="text-2xl font-bold ${t.ema9_score >= 4 ? 'text-emerald-500' : t.ema9_score >= 2 ? 'text-amber-500' : 'text-rose-400'}">${t.ema9_score}<span class="text-sm text-slate-400 font-normal">/5</span></div>
        </div>
      </div>
      <div class="grid grid-cols-4 gap-2 text-xs">
        <div class="text-center bg-slate-50 dark:bg-gray-800 rounded-lg p-2">
          <div class="text-slate-400 dark:text-gray-500">W9EMA値</div>
          <div class="font-semibold mt-0.5">${t.ema9 ? formatPrice(t.ema9) : '-'}</div>
        </div>
        <div class="text-center bg-slate-50 dark:bg-gray-800 rounded-lg p-2">
          <div class="text-slate-400 dark:text-gray-500">乖離率</div>
          <div class="font-semibold mt-0.5 ${t.ema9_pct > 0 ? 'text-emerald-500' : 'text-rose-400'}">${t.ema9_pct != null ? (t.ema9_pct > 0 ? '+' : '') + t.ema9_pct + '%' : '-'}</div>
        </div>
        <div class="text-center bg-slate-50 dark:bg-gray-800 rounded-lg p-2">
          <div class="text-slate-400 dark:text-gray-500">連続週数</div>
          <div class="font-semibold mt-0.5">${t.days_above_ema9 != null ? t.days_above_ema9 + '週' : '-'}</div>
        </div>
        <div class="text-center bg-slate-50 dark:bg-gray-800 rounded-lg p-2">
          <div class="text-slate-400 dark:text-gray-500">EMAスタック</div>
          <div class="font-semibold mt-0.5 ${t.ema_stack ? 'text-emerald-500' : 'text-slate-400'}">${t.ema_stack ? '✓ 完成' : '未完成'}</div>
        </div>
      </div>
      <div class="mt-3 text-[10px] text-slate-400 dark:text-gray-500 space-y-0.5">
        <div>スコア内訳: 終値 &gt; 週足W9EMA かつ上向き(+2) ／ 週足EMAスタック W9&gt;W21&gt;W50(+2) ／ 2週以上連続上抜け(+1)</div>
        <div>EMAスタック(週足): W9EMA ${t.ema_stack ? '>' : '≤'} W21EMA ${t.ema_stack ? '>' : '≤'} W50EMA</div>
      </div>
    </div>` : ''}

    ${!isJapanIndex() ? `
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
    </div>` : ''}

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

function showConfirmDialog(title, message) {
  return new Promise(resolve => {
    const overlay = document.createElement('div');
    overlay.className = 'fixed inset-0 z-[60] flex items-center justify-center bg-black/40 backdrop-blur-sm';
    overlay.innerHTML = `
      <div class="bg-white dark:bg-gray-900 rounded-xl border border-slate-200 dark:border-gray-700 p-6 max-w-sm w-full mx-4 shadow-xl">
        <h3 class="text-lg font-bold text-slate-900 dark:text-gray-100">${title}</h3>
        <p class="text-sm text-slate-600 dark:text-gray-400 mt-2">${message}</p>
        <div class="flex gap-3 mt-4">
          <button id="_confirmCancel" class="flex-1 px-3 py-2 text-sm font-medium rounded-lg border border-slate-200 dark:border-gray-700 text-slate-600 dark:text-gray-400 hover:bg-slate-50 dark:hover:bg-gray-800 transition-colors cursor-pointer">キャンセル</button>
          <button id="_confirmOk" class="flex-1 px-3 py-2 text-sm font-medium rounded-lg bg-rose-500 text-white hover:bg-rose-600 transition-colors cursor-pointer">削除</button>
        </div>
      </div>`;
    document.body.appendChild(overlay);
    overlay.querySelector('#_confirmCancel').onclick = () => { overlay.remove(); resolve(false); };
    overlay.querySelector('#_confirmOk').onclick = () => { overlay.remove(); resolve(true); };
    overlay.onclick = (e) => { if (e.target === overlay) { overlay.remove(); resolve(false); } };
  });
}

async function toggleStar(ticker, event) {
  event.stopPropagation();
  if (watchlistTickers.has(ticker)) {
    const confirmed = await showConfirmDialog(
      'ウォッチリスト削除',
      `${ticker} をウォッチリストから削除しますか？`
    );
    if (!confirmed) return;
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
function hidePageLoader() {
  const el = document.getElementById('pageLoader');
  if (!el) return;
  el.style.transition = 'opacity 0.3s ease';
  el.style.opacity = '0';
  setTimeout(() => el.remove(), 320);
}

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
          // Only show tabs relevant to this page
          const pageIndices = window.IS_JAPAN_PAGE
            ? ['nikkei225', 'growth250']
            : ['sp500', 'nasdaq100'];
          if (allResults[activeTab]) {
            switchTab(activeTab);
          } else {
            const firstKey = pageIndices.find(k => allResults[k]);
            if (firstKey) {
              switchTab(firstKey);
            } else {
              // No data for this page's indices — show empty state
              hidePageLoader();
              document.getElementById('emptyState')?.classList.remove('hidden');
              return;
            }
          }
          hidePageLoader();
          return; // Data loaded, don't show empty state
        }
      }
    }
    if (status.running) {
      document.getElementById('btnRun').disabled = true;
      document.getElementById('btnRun').textContent = '分析中...';
      document.getElementById('progressArea').classList.remove('hidden');
      hidePageLoader();
      pollProgress();
      return;
    }
    // No results and not running — show empty state
    hidePageLoader();
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

// ── Sprint 4: Weight Preset Re-scoring ───────────────────────────────────────

let activePreset = 'balanced';
let weightPresets = null;  // loaded once from /api/weight_presets

async function loadWeightPresets() {
  if (weightPresets) return weightPresets;
  try {
    const resp = await fetch('/api/weight_presets');
    if (resp.ok) weightPresets = await resp.json();
  } catch (e) { /* ignore */ }
  return weightPresets;
}

async function applyWeightPreset(presetKey) {
  activePreset = presetKey;
  // Update button active state
  document.querySelectorAll('.preset-btn').forEach(btn => {
    const isActive = btn.dataset.preset === presetKey;
    btn.classList.toggle('active-preset', isActive);
    btn.classList.toggle('bg-primary-50', isActive);
    btn.classList.toggle('dark:bg-primary-950/30', isActive);
    btn.classList.toggle('text-primary-600', isActive);
    btn.classList.toggle('dark:text-primary-400', isActive);
    btn.classList.toggle('border-primary-300', isActive);
    btn.classList.toggle('dark:border-primary-700', isActive);
  });
  const presets = await loadWeightPresets();
  if (!presets || !screeningData || !screeningData.momentum_ranking) return;

  const preset = presets[presetKey];
  if (!preset) return;
  const weights = preset.weights;

  // Build original rank map
  const origRank = {};
  screeningData.momentum_ranking.forEach(r => { origRank[r.ticker] = r.rank; });

  // Re-score each stock using stored percentile values
  const rescored = screeningData.momentum_ranking
    .filter(r => r.score_components && r.score_components.length > 0)
    .map(r => {
      const newScore = r.score_components.reduce((sum, c) => {
        const w = weights[c.component_name] ?? 0;
        return sum + (c.percentile_value / 100) * w * 100;
      }, 0);
      return { ...r, _preset_score: Math.round(newScore * 10) / 10 };
    });

  if (rescored.length === 0) {
    // No score_components available → fall back to original ranking
    renderTable(screeningData.momentum_ranking);
    renderPresetBadge(preset.label, null);
    return;
  }

  // Sort by new score
  rescored.sort((a, b) => b._preset_score - a._preset_score);

  // Inject rank_delta before rendering
  rescored.forEach((r, i) => {
    r._new_rank = i + 1;
    r._rank_delta = origRank[r.ticker] - (i + 1);  // positive = rose
  });

  renderTableWithPreset(rescored, preset.label);
}

function renderTableWithPreset(ranking, presetLabel) {
  let filtered = ranking;
  if (showWatchlistOnly) {
    filtered = ranking.filter(r => watchlistTickers.has(r.ticker));
  }
  const tbody = document.getElementById('tableBody');
  const retClass = (v) => v > 0 ? 'text-emerald-600 dark:text-emerald-400' : v < 0 ? 'text-rose-400 dark:text-rose-300' : '';

  tbody.innerHTML = filtered.map(r => {
    const t = r.technicals;
    const rsiClass = t.rsi > 70 ? 'text-rose-400 font-semibold' : t.rsi < 30 ? 'text-emerald-500 font-semibold' : '';
    const starred = watchlistTickers.has(r.ticker);
    const delta = r._rank_delta ?? 0;
    const deltaHtml = delta === 0
      ? '<span class="text-slate-300 dark:text-gray-600 text-[10px]">—</span>'
      : delta > 0
        ? `<span class="text-emerald-500 text-[10px] font-bold">▲${delta}</span>`
        : `<span class="text-rose-400 text-[10px] font-bold">▼${Math.abs(delta)}</span>`;

    return `<tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 cursor-pointer transition-colors" onclick='showDetail(${JSON.stringify(r).replace(/'/g, "&#39;")})'>
      <td class="px-2 py-3 text-center">
        <button onclick="toggleStar('${r.ticker}', event)" class="text-lg leading-none cursor-pointer hover:scale-110 transition-transform ${starred ? 'text-amber-400' : 'text-slate-300 dark:text-gray-600'}">
          ${starred ? '&#9733;' : '&#9734;'}
        </button>
      </td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-slate-400 text-xs sm:text-sm whitespace-nowrap">${r._new_rank}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 font-semibold text-primary-600 dark:text-primary-400 text-xs sm:text-sm whitespace-nowrap">${r.ticker}</td>
      <td class="px-4 py-3 text-slate-500 dark:text-gray-400 hidden lg:table-cell text-xs max-w-[180px] truncate">${r.name}</td>
      <td class="px-4 py-3 text-xs text-slate-600 dark:text-gray-400 whitespace-nowrap hidden md:table-cell">${r.sector}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell">${formatPrice(r.price)}</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-bold text-xs sm:text-sm whitespace-nowrap">
        ${r._preset_score != null ? r._preset_score.toFixed(1) : r.momentum_score}
        <div class="text-[9px] leading-none mt-0.5">${deltaHtml}</div>
      </td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap ${retClass(t.ret_1m)}">${t.ret_1m > 0 ? '+' : ''}${t.ret_1m}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell ${retClass(t.ret_3m)}">${t.ret_3m > 0 ? '+' : ''}${t.ret_3m}%</td>
      <td class="px-2 sm:px-4 py-2 sm:py-3 text-right font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:table-cell ${rsiClass}">${t.rsi}</td>
    </tr>`;
  }).join('');

  renderPresetBadge(presetLabel, ranking.length);
}

function renderPresetBadge(label, count) {
  const el = document.getElementById('presetBadge');
  if (!el) return;
  if (activePreset === 'balanced') {
    el.classList.add('hidden');
    return;
  }
  el.classList.remove('hidden');
  el.textContent = `ウェイト: ${label}${count != null ? ` (${count}銘柄)` : ''}`;
}


// ── Sprint 4: Session History Modal ──────────────────────────────────────────

async function showHistoryModal() {
  document.getElementById('historyModal').classList.remove('hidden');
  document.getElementById('historyModal').classList.add('flex');
  document.getElementById('historyList').innerHTML =
    '<div class="text-center text-slate-400 py-8 text-sm">読み込み中...</div>';

  try {
    const resp = await fetch('/api/history');
    if (!resp.ok) throw new Error('fetch failed');
    const sessions = await resp.json();
    renderHistoryList(sessions);
  } catch (e) {
    document.getElementById('historyList').innerHTML =
      '<div class="text-center text-rose-400 py-8 text-sm">読み込み失敗</div>';
  }
}

function closeHistoryModal() {
  document.getElementById('historyModal').classList.add('hidden');
  document.getElementById('historyModal').classList.remove('flex');
}

function renderHistoryList(sessions) {
  const el = document.getElementById('historyList');
  if (!sessions || sessions.length === 0) {
    el.innerHTML = '<div class="text-center text-slate-400 py-8 text-sm">履歴なし</div>';
    return;
  }
  const indexLabel = { sp500: 'S&P 500', nasdaq100: 'NASDAQ 100', nikkei225: '日経225', growth250: 'グロース250' };
  el.innerHTML = sessions.map(s => {
    const idxLabel = indexLabel[s.index_name] || s.index_name;
    const regime = s.regime_label ? `<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full bg-primary-50 dark:bg-primary-950/30 text-primary-600 dark:text-primary-400 font-medium">${s.regime_label}</span>` : '';
    const tickers = s.top_tickers && s.top_tickers.length > 0
      ? s.top_tickers.join(' · ')
      : '-';
    return `<div class="flex items-start justify-between gap-3 py-3 border-b border-slate-100 dark:border-gray-800 last:border-0">
      <div class="min-w-0">
        <div class="flex items-center gap-2 flex-wrap mb-1">
          <span class="text-xs font-semibold text-slate-700 dark:text-gray-300">${idxLabel}</span>
          <span class="text-[10px] text-slate-400 dark:text-gray-500">${s.generated_at}</span>
          ${regime}
        </div>
        <div class="text-[11px] text-slate-500 dark:text-gray-400">上位: ${tickers}</div>
        <div class="text-[10px] text-slate-400 dark:text-gray-500 mt-0.5">${s.total_screened}銘柄スクリーニング</div>
      </div>
      <div class="shrink-0">
        <button onclick="showBacktestPanel(${s.id}, '${s.index_name}', '${s.generated_at}')"
          class="text-[11px] px-2.5 py-1.5 rounded-lg bg-primary-50 dark:bg-primary-950/30 text-primary-600 dark:text-primary-400 hover:bg-primary-100 dark:hover:bg-primary-900/50 font-medium transition-colors cursor-pointer whitespace-nowrap">
          バックテスト
        </button>
      </div>
    </div>`;
  }).join('');
}


// ── Sprint 4: Backtest Panel ──────────────────────────────────────────────────

let _backtestSessionId = null;
let _backtestIndexName = null;
let _backtestDate = null;

function showBacktestPanel(sessionId, indexName, sessionDate) {
  _backtestSessionId = sessionId;
  _backtestIndexName = indexName;
  _backtestDate = sessionDate;

  document.getElementById('backtestSessionInfo').textContent =
    `セッション #${sessionId} — ${sessionDate}`;
  document.getElementById('backtestResult').innerHTML = '';
  document.getElementById('backtestPanel').classList.remove('hidden');
}

async function runBacktest() {
  const horizon = parseInt(document.getElementById('backtestHorizon').value);
  const topN    = parseInt(document.getElementById('backtestTopN').value);
  const btn     = document.getElementById('backtestRunBtn');

  btn.disabled = true;
  btn.textContent = '実行中...';
  document.getElementById('backtestResult').innerHTML =
    '<div class="text-center text-slate-400 py-6 text-sm">データ取得中（10〜30秒かかることがあります）...</div>';

  try {
    const resp = await fetch('/api/backtest/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: _backtestSessionId,
        horizon_days: horizon,
        top_n: topN,
      }),
    });
    const data = await resp.json();
    renderBacktestResult(data);
  } catch (e) {
    document.getElementById('backtestResult').innerHTML =
      `<div class="text-center text-rose-400 py-6 text-sm">エラー: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = '実行';
  }
}

function renderBacktestResult(bt) {
  const el = document.getElementById('backtestResult');
  if (bt.error) {
    el.innerHTML = `<div class="text-rose-400 text-sm py-4 text-center">${bt.error}</div>`;
    return;
  }
  const s = bt.stats;
  const retCls = (v) => v == null ? '' : v >= 0 ? 'text-emerald-600 dark:text-emerald-400 font-semibold' : 'text-rose-400 dark:text-rose-300 font-semibold';
  const fmtR   = (v) => v == null ? '-' : (v >= 0 ? '+' : '') + v.toFixed(2) + '%';

  el.innerHTML = `
    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-5">
      <div class="bg-slate-50 dark:bg-gray-800 rounded-xl p-3 text-center">
        <div class="text-[10px] text-slate-400 mb-1">平均リターン</div>
        <div class="text-lg font-bold ${retCls(s.avg_return)}">${fmtR(s.avg_return)}</div>
      </div>
      <div class="bg-slate-50 dark:bg-gray-800 rounded-xl p-3 text-center">
        <div class="text-[10px] text-slate-400 mb-1">中央値リターン</div>
        <div class="text-lg font-bold ${retCls(s.median_return)}">${fmtR(s.median_return)}</div>
      </div>
      <div class="bg-slate-50 dark:bg-gray-800 rounded-xl p-3 text-center">
        <div class="text-[10px] text-slate-400 mb-1">勝率</div>
        <div class="text-lg font-bold text-primary-600 dark:text-primary-400">${s.win_rate != null ? s.win_rate.toFixed(1) + '%' : '-'}</div>
      </div>
      <div class="bg-slate-50 dark:bg-gray-800 rounded-xl p-3 text-center">
        <div class="text-[10px] text-slate-400 mb-1">超過リターン</div>
        <div class="text-lg font-bold ${retCls(s.excess_return)}">${fmtR(s.excess_return)}</div>
      </div>
    </div>
    <div class="text-[10px] text-slate-400 dark:text-gray-500 mb-3">
      ベンチマーク: ${bt.benchmark_ticker} ${fmtR(s.benchmark_return)} / ${bt.horizon_days}営業日後 / ${s.sample_size}銘柄
    </div>
    <div class="overflow-x-auto">
      <table class="w-full text-xs">
        <thead>
          <tr class="border-b border-slate-200 dark:border-gray-700 text-slate-500">
            <th class="px-2 py-2 text-left">#</th>
            <th class="px-2 py-2 text-left">銘柄</th>
            <th class="px-2 py-2 text-right">エントリー価格</th>
            <th class="px-2 py-2 text-right">エグジット価格</th>
            <th class="px-2 py-2 text-right">リターン</th>
            <th class="px-2 py-2 text-right">vs ベンチ</th>
          </tr>
        </thead>
        <tbody>
          ${bt.detail.map(d => `
            <tr class="border-b border-slate-100 dark:border-gray-800">
              <td class="px-2 py-2 text-slate-400">${d.rank}</td>
              <td class="px-2 py-2 font-semibold text-primary-600 dark:text-primary-400">${d.ticker}</td>
              <td class="px-2 py-2 text-right font-mono">${d.entry_price != null ? d.entry_price.toFixed(2) : '-'}</td>
              <td class="px-2 py-2 text-right font-mono">${d.exit_price != null ? d.exit_price.toFixed(2) : '-'}</td>
              <td class="px-2 py-2 text-right font-mono ${retCls(d.return_pct)}">${fmtR(d.return_pct)}</td>
              <td class="px-2 py-2 text-right font-mono ${retCls(d.vs_benchmark)}">${fmtR(d.vs_benchmark)}</td>
            </tr>`).join('')}
        </tbody>
      </table>
    </div>`;
}

// ── Sprint 6: Daily Report ────────────────────────────────────────────────────

function renderDailyReport(report, changes) {
  const el = document.getElementById('dailyReportPanel');
  if (!el) return;
  if (!report && (!changes || !changes.new_entries || changes.new_entries.length === 0)) {
    el.classList.add('hidden');
    return;
  }

  const r = report || {};
  const highlights = r.highlights || [];
  const wlAlerts   = r.watchlist_alerts || [];
  const initCands  = r.initial_candidates || [];
  const streakCands = r.streak_candidates || [];
  const cautionCands = r.caution_candidates || [];

  const hasAnyContent = highlights.length || wlAlerts.length || initCands.length || streakCands.length || cautionCands.length;
  if (!hasAnyContent) { el.classList.add('hidden'); return; }

  const mkBadge = (items, color) => items.map(c =>
    `<span class="inline-flex items-center gap-1 px-2 py-1 text-xs rounded-lg ${color} font-medium">
       <span class="font-bold">${c.ticker}</span>
       <span class="opacity-75">${c.reason || ''}</span>
     </span>`
  ).join('');

  el.innerHTML = `
    <div class="card border border-teal-200 dark:border-teal-900/40 bg-teal-50/40 dark:bg-teal-950/20">
      <div class="flex items-center gap-2 mb-3">
        <span class="text-lg">📋</span>
        <h3 class="font-semibold text-slate-800 dark:text-gray-100 text-sm">日次レポート</h3>
        ${r.regime_text ? `<span class="ml-auto text-xs text-teal-600 dark:text-teal-400 font-medium">${r.regime_text}</span>` : ''}
      </div>
      ${highlights.length ? `
        <ul class="space-y-1 mb-3">
          ${highlights.map(h => `<li class="text-xs text-slate-600 dark:text-gray-300 flex items-start gap-1.5"><span class="text-teal-500 mt-0.5">•</span>${h}</li>`).join('')}
        </ul>` : ''}
      ${wlAlerts.length ? `
        <div class="mb-3">
          <div class="text-[10px] uppercase tracking-wide text-rose-500 dark:text-rose-400 font-semibold mb-1">ウォッチリスト</div>
          <ul class="space-y-1">
            ${wlAlerts.map(a => `<li class="text-xs text-slate-600 dark:text-gray-300 flex items-start gap-1.5"><span class="text-rose-400 mt-0.5">!</span>${a}</li>`).join('')}
          </ul>
        </div>` : ''}
      <div class="grid grid-cols-1 sm:grid-cols-3 gap-3 mt-2">
        ${initCands.length ? `
          <div>
            <div class="text-[10px] uppercase tracking-wide text-violet-500 dark:text-violet-400 font-semibold mb-1.5">🌱 初動候補</div>
            <div class="flex flex-wrap gap-1">${mkBadge(initCands, 'bg-violet-100 dark:bg-violet-900/30 text-violet-700 dark:text-violet-300')}</div>
          </div>` : ''}
        ${streakCands.length ? `
          <div>
            <div class="text-[10px] uppercase tracking-wide text-emerald-600 dark:text-emerald-400 font-semibold mb-1.5">💪 継続強者</div>
            <div class="flex flex-wrap gap-1">${mkBadge(streakCands, 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300')}</div>
          </div>` : ''}
        ${cautionCands.length ? `
          <div>
            <div class="text-[10px] uppercase tracking-wide text-rose-500 dark:text-rose-400 font-semibold mb-1.5">⚠️ 過熱注意</div>
            <div class="flex flex-wrap gap-1">${mkBadge(cautionCands, 'bg-rose-100 dark:bg-rose-900/30 text-rose-700 dark:text-rose-300')}</div>
          </div>` : ''}
      </div>
    </div>
  `;
  el.classList.remove('hidden');
}


// ── Sprint 6: Notification Drawer ─────────────────────────────────────────────

const EVENT_TYPE_LABELS = {
  new_entry:    { icon: '🆕', label: '新規ランクイン', cls: 'border-teal-200 dark:border-teal-800 bg-teal-50/50 dark:bg-teal-950/20' },
  dropped:      { icon: '📉', label: 'ランクアウト',   cls: 'border-rose-200 dark:border-rose-800 bg-rose-50/50 dark:bg-rose-950/20' },
  score_surge:  { icon: '🚀', label: 'スコア急上昇',   cls: 'border-yellow-200 dark:border-yellow-800 bg-yellow-50/50 dark:bg-yellow-950/20' },
  score_drop:   { icon: '⬇️', label: 'スコア急落',     cls: 'border-orange-200 dark:border-orange-800 bg-orange-50/50 dark:bg-orange-950/20' },
  earnings_soon:{ icon: '📅', label: '決算接近',       cls: 'border-amber-200 dark:border-amber-800 bg-amber-50/50 dark:bg-amber-950/20' },
  near_52w_high:{ icon: '🏔️', label: '52W高値接近',   cls: 'border-sky-200 dark:border-sky-800 bg-sky-50/50 dark:bg-sky-950/20' },
};

async function updateNotificationBadge() {
  try {
    const idx = activeTab ? (activeTab.includes('nikkei') || activeTab.includes('growth') ? activeTab : activeTab) : null;
    const url = idx ? `/api/watchlist/events/unread_count` : '/api/watchlist/events/unread_count';
    const res = await fetch(url);
    if (!res.ok) return;
    const counts = await res.json();
    const total = Object.values(counts).reduce((s, v) => s + v, 0);
    const badge = document.getElementById('notifBadge');
    if (!badge) return;
    if (total > 0) {
      badge.textContent = total > 99 ? '99+' : total;
      badge.classList.remove('hidden');
    } else {
      badge.classList.add('hidden');
    }
  } catch (_) {}
}

async function showNotificationDrawer() {
  const drawer = document.getElementById('notificationDrawer');
  const list   = document.getElementById('notificationList');
  if (!drawer || !list) return;

  drawer.classList.remove('hidden');
  drawer.classList.add('flex');
  list.innerHTML = '<p class="text-sm text-slate-400 text-center py-8">読み込み中...</p>';

  try {
    const res = await fetch(`/api/watchlist/events?limit=80`);
    const events = await res.json();
    if (!Array.isArray(events) || events.length === 0) {
      list.innerHTML = '<p class="text-sm text-slate-400 dark:text-gray-500 text-center py-8">通知はありません</p>';
      return;
    }
    list.innerHTML = events.map(ev => {
      const meta = EVENT_TYPE_LABELS[ev.event_type] || { icon: '🔔', label: ev.event_type, cls: 'border-slate-200 dark:border-gray-700' };
      const payload = ev.payload || {};
      const unreadCls = ev.is_read ? 'opacity-60' : '';
      const scorePart = payload.score != null ? ` スコア${payload.score}` : '';
      const deltaPart = payload.score_delta != null ? ` (${payload.score_delta > 0 ? '+' : ''}${payload.score_delta})` : '';
      const rankPart  = payload.rank ? ` #${payload.rank}` : (payload.prev_rank ? ` 前回#${payload.prev_rank}` : '');
      const detail    = `${rankPart}${scorePart}${deltaPart}`;
      return `
        <div class="flex items-start gap-3 p-3 rounded-xl border ${meta.cls} ${unreadCls}">
          <span class="text-lg leading-none mt-0.5">${meta.icon}</span>
          <div class="flex-1 min-w-0">
            <div class="flex items-center gap-2 flex-wrap">
              <span class="font-semibold text-sm text-slate-800 dark:text-gray-100">${ev.ticker}</span>
              <span class="text-xs px-1.5 py-0.5 rounded-full bg-white/60 dark:bg-gray-800/60 text-slate-500 dark:text-gray-400">${meta.label}</span>
              ${!ev.is_read ? '<span class="w-2 h-2 rounded-full bg-rose-500 flex-shrink-0"></span>' : ''}
            </div>
            ${detail ? `<p class="text-xs text-slate-500 dark:text-gray-400 mt-0.5">${detail}</p>` : ''}
            <p class="text-[10px] text-slate-400 dark:text-gray-600 mt-1">${ev.created_at || ''}</p>
          </div>
        </div>`;
    }).join('');
  } catch (e) {
    list.innerHTML = `<p class="text-sm text-rose-400 text-center py-8">読み込み失敗: ${e.message}</p>`;
  }
}

function closeNotificationDrawer() {
  const drawer = document.getElementById('notificationDrawer');
  if (drawer) { drawer.classList.add('hidden'); drawer.classList.remove('flex'); }
}

async function markAllNotificationsRead() {
  try {
    await fetch('/api/watchlist/events/read', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ all: true }),
    });
    // Refresh drawer and badge
    const badge = document.getElementById('notifBadge');
    if (badge) badge.classList.add('hidden');
    await showNotificationDrawer();
  } catch (_) {}
}

// ── Data Quality ─────────────────────────────────────────────────────────────

const HEALTH_COLORS = {
  ok:      'text-emerald-500',
  degraded:'text-amber-400',
  error:   'text-rose-400',
  unknown: 'text-slate-400 dark:text-gray-500',
};

async function refreshDataQuality() {
  const el = document.getElementById('dataQualityStatus');
  if (!el) return;
  try {
    const res = await fetch('/api/data_quality/status');
    const sources = await res.json();
    if (!sources.length) {
      el.innerHTML = '<span class="text-slate-400">データなし（スクリーニング後に更新されます）</span>';
      return;
    }
    const rows = sources.map(s => {
      const color = HEALTH_COLORS[s.health_status] || HEALTH_COLORS.unknown;
      const age = s.age_hours != null ? `${s.age_hours}h前` : '−';
      const staleWarn = s.stale ? ' <span class="text-amber-400">⚠ 古い</span>' : '';
      return `<div class="flex items-center gap-3 py-1 border-b border-slate-100 dark:border-gray-800 last:border-0">
        <span class="${color} font-medium w-16 shrink-0">${s.health_label}</span>
        <span class="text-slate-600 dark:text-gray-300 flex-1">${s.source_name}</span>
        <span class="text-slate-400 dark:text-gray-500 shrink-0">${age}${staleWarn}</span>
      </div>`;
    }).join('');
    el.innerHTML = rows;
  } catch (_) {
    el.innerHTML = '<span class="text-rose-400">取得失敗</span>';
  }
}

// ── Sector Correlation Matrix ──
function renderCorrelationMatrix(corrData) {
  const container = document.getElementById('correlationContent');
  if (!container || !corrData) return;
  const sectors = corrData.sectors;
  const matrix = corrData.matrix;
  const isDark = document.documentElement.classList.contains('dark');

  function corrColor(v) {
    if (v >= 0.7) return isDark ? 'rgba(239,68,68,0.4)' : 'rgba(239,68,68,0.3)';
    if (v >= 0.4) return isDark ? 'rgba(251,146,60,0.3)' : 'rgba(251,146,60,0.25)';
    if (v >= 0.1) return isDark ? 'rgba(250,204,21,0.2)' : 'rgba(250,204,21,0.15)';
    if (v >= -0.1) return isDark ? 'rgba(148,163,184,0.1)' : 'rgba(148,163,184,0.08)';
    if (v >= -0.4) return isDark ? 'rgba(96,165,250,0.2)' : 'rgba(96,165,250,0.15)';
    return isDark ? 'rgba(59,130,246,0.4)' : 'rgba(59,130,246,0.3)';
  }

  const shortName = (s) => s.length > 10 ? s.slice(0, 9) + '…' : s;

  let html = '<div class="overflow-x-auto"><table class="text-xs w-full border-collapse">';
  html += '<tr><th class="p-1"></th>';
  sectors.forEach(s => { html += `<th class="p-1 text-center font-medium text-slate-500 dark:text-gray-400 whitespace-nowrap" title="${s}">${shortName(s)}</th>`; });
  html += '</tr>';
  for (let i = 0; i < sectors.length; i++) {
    html += `<tr><th class="p-1 text-right font-medium text-slate-500 dark:text-gray-400 whitespace-nowrap pr-2" title="${sectors[i]}">${shortName(sectors[i])}</th>`;
    for (let j = 0; j < sectors.length; j++) {
      const v = matrix[i][j];
      const bg = corrColor(v);
      const bold = i === j ? 'font-bold' : '';
      html += `<td class="p-1 text-center ${bold} text-slate-700 dark:text-gray-300 border border-slate-100 dark:border-gray-800" style="background:${bg}" title="${sectors[i]} × ${sectors[j]}: ${v}">${v.toFixed(2)}</td>`;
    }
    html += '</tr>';
  }
  html += '</table></div>';
  html += `<div class="mt-3 flex items-center gap-4 text-[10px] text-slate-400 dark:text-gray-500">
    <span>相関の読み方:</span>
    <span style="display:inline-block;width:12px;height:12px;background:${corrColor(0.8)};border-radius:2px"></span> 強い正の相関 (0.7+)
    <span style="display:inline-block;width:12px;height:12px;background:${corrColor(0)};border-radius:2px"></span> 無相関
    <span style="display:inline-block;width:12px;height:12px;background:${corrColor(-0.5)};border-radius:2px"></span> 負の相関
  </div>`;
  container.innerHTML = html;
}

// ── Stock Comparison ──
function toggleCompare(ticker, event) {
  event.stopPropagation();
  const idx = comparisonTickers.indexOf(ticker);
  if (idx >= 0) {
    comparisonTickers.splice(idx, 1);
  } else if (comparisonTickers.length < 3) {
    comparisonTickers.push(ticker);
  }
  updateCompareFloating();
  if (screeningData) renderTable(screeningData.momentum_ranking);
}

function updateCompareFloating() {
  const el = document.getElementById('compareFloating');
  if (!el) return;
  const cnt = document.getElementById('compareCount');
  if (cnt) cnt.textContent = comparisonTickers.length;
  if (comparisonTickers.length >= 2) {
    el.classList.remove('hidden');
  } else {
    el.classList.add('hidden');
  }
}

function showComparisonModal() {
  if (!screeningData || comparisonTickers.length < 2) return;
  const stocks = comparisonTickers.map(tk => screeningData.momentum_ranking.find(r => r.ticker === tk)).filter(Boolean);
  if (stocks.length < 2) return;

  const fmtPct = (v) => v != null ? (v > 0 ? '+' : '') + Number(v).toFixed(2) + '%' : '-';
  const fmtVal = (v) => v != null && v !== 0 ? v : '-';

  const metrics = [
    ['モメンタムスコア', s => s.momentum_score, 'high'],
    ['株価', s => formatPrice(s.price), null],
    ['RSI', s => s.technicals.rsi, null],
    ['1ヶ月リターン', s => fmtPct(s.technicals.ret_1m), 'high'],
    ['3ヶ月リターン', s => fmtPct(s.technicals.ret_3m), 'high'],
    ['出来高比', s => s.technicals.vol_ratio ? s.technicals.vol_ratio + 'x' : '-', 'high'],
    ['50日MA乖離', s => fmtPct(s.technicals.ma50_dev), null],
    ['200日MA乖離', s => fmtPct(s.technicals.ma200_dev), null],
    ['品質スコア', s => s.quality_score != null ? s.quality_score.toFixed(1) : '-', 'high'],
    ['エントリー難易度', s => s.entry_difficulty || '-', null],
    ['OBVスロープ', s => s.technicals.obv_slope != null ? s.technicals.obv_slope + '%' : '-', 'high'],
    ['OBV乖離', s => ({'bullish_div':'強気','bearish_div':'弱気','none':'-'}[s.technicals.obv_divergence] || '-'), null],
    ['最大DD(3M)', s => s.technicals.max_drawdown_3m != null ? s.technicals.max_drawdown_3m + '%' : '-', 'low'],
    ['現在DD', s => s.technicals.current_drawdown != null ? s.technicals.current_drawdown + '%' : '-', 'low'],
    ['ADX', s => s.technicals.adx != null ? s.technicals.adx : '-', 'high'],
    ['52W乖離', s => s.technicals.dist_from_high != null ? s.technicals.dist_from_high + '%' : '-', null],
    ['BB幅', s => s.technicals.bb_width != null ? s.technicals.bb_width + '%' : '-', null],
    ['セクター', s => s.sector || '-', null],
    ['RS判定', s => ({'prime':'本命','short_term':'短期','sector_driven':'劣後','theme':'テーマ'}[s.technicals.rs_label] || '-'), null],
  ];

  let rows = '';
  for (const [label, getter, bestDir] of metrics) {
    const values = stocks.map(getter);
    const numVals = values.map(v => parseFloat(String(v).replace(/[^-\d.]/g, '')));
    let bestIdx = -1;
    if (bestDir && numVals.some(v => !isNaN(v))) {
      if (bestDir === 'high') bestIdx = numVals.indexOf(Math.max(...numVals.filter(v => !isNaN(v))));
      else bestIdx = numVals.indexOf(Math.min(...numVals.filter(v => !isNaN(v))));
    }
    const cells = values.map((v, i) => {
      const cls = i === bestIdx ? 'text-emerald-600 dark:text-emerald-400 font-semibold' : '';
      return `<td class="px-3 py-2 text-center text-sm ${cls}">${v}</td>`;
    }).join('');
    rows += `<tr class="border-b border-slate-100 dark:border-gray-800"><td class="px-3 py-2 text-xs font-medium text-slate-500 dark:text-gray-400 whitespace-nowrap">${label}</td>${cells}</tr>`;
  }

  const headers = stocks.map(s => `<th class="px-3 py-2 text-center text-sm font-bold text-slate-900 dark:text-gray-100">${s.ticker}<div class="text-[10px] font-normal text-slate-400">${s.name || ''}</div></th>`).join('');

  document.getElementById('compareContent').innerHTML = `
    <div class="overflow-x-auto">
      <table class="w-full border-collapse">
        <thead><tr class="border-b-2 border-slate-200 dark:border-gray-700"><th class="px-3 py-2 text-left text-xs text-slate-400">指標</th>${headers}</tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;

  const modal = document.getElementById('compareModal');
  modal.classList.remove('hidden');
  modal.classList.add('flex');
}

function closeCompareModal() {
  const modal = document.getElementById('compareModal');
  modal.classList.add('hidden');
  modal.classList.remove('flex');
}

function clearComparison() {
  comparisonTickers = [];
  updateCompareFloating();
  if (screeningData) renderTable(screeningData.momentum_ranking);
}

// ── Alert Builder ──
let savedAlerts = JSON.parse(localStorage.getItem('surge-alerts') || '[]');

function showAlertBuilder() {
  const overlay = document.createElement('div');
  overlay.className = 'fixed inset-0 z-[55] flex items-center justify-center bg-black/40 backdrop-blur-sm';
  overlay.id = 'alertBuilderOverlay';

  const existingList = savedAlerts.map((a, i) => `
    <div class="flex items-center justify-between bg-slate-50 dark:bg-gray-800 rounded-lg px-3 py-2 text-xs">
      <span class="text-slate-700 dark:text-gray-300">${a.label}</span>
      <button onclick="removeAlert(${i})" class="text-rose-400 hover:text-rose-600 cursor-pointer text-sm">&times;</button>
    </div>`).join('');

  overlay.innerHTML = `
    <div class="bg-white dark:bg-gray-900 rounded-xl border border-slate-200 dark:border-gray-700 p-6 max-w-md w-full mx-4 shadow-xl">
      <div class="flex items-center justify-between mb-4">
        <h3 class="text-lg font-bold text-slate-900 dark:text-gray-100">アラートビルダー</h3>
        <button onclick="document.getElementById('alertBuilderOverlay')?.remove()" class="text-slate-400 hover:text-slate-600 dark:hover:text-gray-200 text-2xl leading-none cursor-pointer">&times;</button>
      </div>
      <p class="text-xs text-slate-500 dark:text-gray-400 mb-4">条件に合致する銘柄がスクリーニング結果に見つかった場合、通知センターに表示されます。</p>
      <div class="space-y-3 mb-4">
        <div class="grid grid-cols-3 gap-2">
          <select id="alertField" class="text-xs px-2 py-1.5 rounded-lg border border-slate-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-slate-700 dark:text-gray-300">
            <option value="rsi">RSI</option>
            <option value="momentum_score">スコア</option>
            <option value="ret_1m">1Mリターン</option>
            <option value="vol_ratio">出来高比</option>
            <option value="adx">ADX</option>
            <option value="max_drawdown_3m">最大DD</option>
          </select>
          <select id="alertOp" class="text-xs px-2 py-1.5 rounded-lg border border-slate-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-slate-700 dark:text-gray-300">
            <option value="<">&lt;</option>
            <option value=">">&gt;</option>
            <option value="<=">&le;</option>
            <option value=">=">&ge;</option>
          </select>
          <input id="alertValue" type="number" placeholder="値" class="text-xs px-2 py-1.5 rounded-lg border border-slate-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-slate-700 dark:text-gray-300">
        </div>
        <button onclick="addAlert()" class="w-full px-3 py-2 text-xs font-medium bg-primary-500 text-white rounded-lg hover:bg-primary-600 transition-colors cursor-pointer">条件を追加</button>
      </div>
      ${savedAlerts.length ? `<div class="space-y-1.5 mb-3"><div class="text-[10px] text-slate-400 dark:text-gray-500 uppercase tracking-wider mb-1">保存済みアラート</div>${existingList}</div>` : ''}
      <div class="text-[10px] text-slate-400 dark:text-gray-500">※アラートはlocalStorageに保存されます。スクリーニング実行時に自動チェックされます。</div>
    </div>`;
  document.body.appendChild(overlay);
  overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
}

function addAlert() {
  const field = document.getElementById('alertField').value;
  const op = document.getElementById('alertOp').value;
  const val = parseFloat(document.getElementById('alertValue').value);
  if (isNaN(val)) return;
  const fieldLabels = { rsi: 'RSI', momentum_score: 'スコア', ret_1m: '1M%', vol_ratio: '出来高比', adx: 'ADX', max_drawdown_3m: '最大DD' };
  savedAlerts.push({ field, op, value: val, label: `${fieldLabels[field] || field} ${op} ${val}` });
  localStorage.setItem('surge-alerts', JSON.stringify(savedAlerts));
  document.getElementById('alertBuilderOverlay')?.remove();
  showAlertBuilder(); // re-render
}

function removeAlert(idx) {
  savedAlerts.splice(idx, 1);
  localStorage.setItem('surge-alerts', JSON.stringify(savedAlerts));
  document.getElementById('alertBuilderOverlay')?.remove();
  showAlertBuilder();
}

function checkAlerts(ranking) {
  if (!savedAlerts.length || !ranking) return;
  const matches = [];
  for (const alert of savedAlerts) {
    for (const r of ranking) {
      let val;
      if (['rsi', 'ret_1m', 'vol_ratio', 'adx', 'max_drawdown_3m'].includes(alert.field)) {
        val = r.technicals?.[alert.field];
      } else {
        val = r[alert.field];
      }
      if (val == null) continue;
      const pass =
        alert.op === '<' ? val < alert.value :
        alert.op === '>' ? val > alert.value :
        alert.op === '<=' ? val <= alert.value :
        alert.op === '>=' ? val >= alert.value : false;
      if (pass) matches.push({ ticker: r.ticker, alert: alert.label, value: val });
    }
  }
  if (matches.length > 0) {
    const el = document.getElementById('alertResults');
    if (el) {
      el.classList.remove('hidden');
      el.innerHTML = `<div class="text-[10px] text-amber-500 dark:text-amber-400 uppercase tracking-wider font-semibold mb-1.5">アラート一致 (${matches.length}件)</div>` +
        matches.slice(0, 10).map(m => `<div class="text-xs text-slate-600 dark:text-gray-300 py-0.5"><span class="font-bold text-primary-500">${m.ticker}</span> ${m.alert} → ${typeof m.value === 'number' ? m.value.toFixed(1) : m.value}</div>`).join('');
    }
  }
}

init();
