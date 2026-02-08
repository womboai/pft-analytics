import './style.css';
import { fetchNetworkData, formatPFT, formatAddress } from './api';
import type { NetworkData } from './api';

// Polling interval in milliseconds (60 seconds)
const REFRESH_INTERVAL_MS = 60000;

// Store the interval ID so we can clear it if needed
let refreshIntervalId: number | null = null;

// Render all dashboard data to the DOM
function renderDashboard(data: NetworkData) {
  const totals = data.network_totals;
  const avgPftPerReward = totals.total_rewards_paid > 0
    ? totals.total_pft_distributed / totals.total_rewards_paid
    : 0;
  const successRate = totals.total_submissions > 0
    ? (totals.total_rewards_paid / totals.total_submissions) * 100
    : 0;
  const avgPftPerEarner = totals.unique_earners > 0
    ? totals.total_pft_distributed / totals.unique_earners
    : 0;

  // Network metrics (all 8 metrics restored)
  document.getElementById('network-totals')!.innerHTML = `
    <h2>Network Metrics</h2>
    <div class="totals-grid">
      <div class="total-card accent-gold">
        <div class="value">${formatPFT(totals.total_pft_distributed)}</div>
        <div class="label">Total PFT Paid</div>
      </div>
      <div class="total-card accent-cyan">
        <div class="value">${totals.unique_earners}</div>
        <div class="label">Unique Earners</div>
      </div>
      <div class="total-card accent-green">
        <div class="value">${totals.total_rewards_paid}</div>
        <div class="label">Tasks Rewarded</div>
      </div>
      <div class="total-card">
        <div class="value">${totals.total_submissions}</div>
        <div class="label">Submissions</div>
      </div>
      <div class="total-card">
        <div class="value">${totals.unique_submitters}</div>
        <div class="label">Active Submitters</div>
      </div>
      <div class="total-card">
        <div class="value">${formatPFT(avgPftPerReward)}</div>
        <div class="label">Avg Reward</div>
      </div>
      <div class="total-card accent-purple">
        <div class="value">${successRate.toFixed(1)}%</div>
        <div class="label">Success Rate</div>
      </div>
      <div class="total-card">
        <div class="value">${formatPFT(avgPftPerEarner)}</div>
        <div class="label">Avg Earnings</div>
      </div>
    </div>
  `;

  // Network Health panel
  const health = data.network_health;
  if (health) {
    const statusColor = health.endpoint_status === 'online' ? '#00ff00' : '#ff3344';
    const statusLabel = health.endpoint_status === 'online' ? 'Online' : 'Offline';
    const latencyColor = health.ws_latency_ms < 500 ? 'accent-green' : health.ws_latency_ms < 2000 ? 'accent-gold' : '';
    const driftWarning = health.seconds_since_close > 30;
    const driftColor = driftWarning ? 'accent-gold' : 'accent-green';

    document.getElementById('network-health')!.innerHTML = `
      <h2>Network Health</h2>
      <div class="totals-grid">
        <div class="total-card ${latencyColor}">
          <div class="value">${health.ws_latency_ms}ms</div>
          <div class="label">WS Latency</div>
        </div>
        <div class="total-card accent-cyan">
          <div class="value">${health.ledger_index.toLocaleString()}</div>
          <div class="label">Ledger Index</div>
        </div>
        <div class="total-card ${driftColor}">
          <div class="value">${health.seconds_since_close}s</div>
          <div class="label">Since Last Close</div>
        </div>
        <div class="total-card">
          <div class="value"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${statusColor};margin-right:6px;box-shadow:0 0 8px ${statusColor};"></span>${statusLabel}</div>
          <div class="label">Endpoint</div>
        </div>
      </div>
    `;
  }

  // Task lifecycle section hidden for now (see WOMBO-715)

  // Leaderboard - table-based layout with Balance + Earned
  const leaderboardRows = data.rewards.leaderboard.slice(0, 10).map((entry, i) => `
    <tr class="${i < 3 ? 'top-' + (i + 1) : ''}" data-address="${entry.address}">
      <td class="rank-cell">#${i + 1}</td>
      <td class="address-cell">
        <span class="address" data-full-address="${entry.address}">
          ${formatAddress(entry.address)}
          <span class="address-tooltip">${entry.address}</span>
        </span>
        <a href="https://explorer.testnet.postfiat.org/accounts/${entry.address}" target="_blank" rel="noopener noreferrer" class="explorer-link" title="View on XRPL Explorer">↗</a>
      </td>
      <td class="balance-cell">${formatPFT(entry.balance)}</td>
      <td class="earned-cell">${formatPFT(entry.total_pft)}</td>
    </tr>
  `).join('');

  document.getElementById('leaderboard')!.innerHTML = `
    <h2>Leaderboard</h2>
    <table class="leaderboard-table">
      <thead>
        <tr>
          <th class="rank-header">#</th>
          <th class="address-header">Address</th>
          <th class="balance-header">Bal</th>
          <th class="earned-header">Earned</th>
        </tr>
      </thead>
      <tbody>
        ${leaderboardRows}
      </tbody>
    </table>
  `;

  // Daily activity table with continuous timeline (most recent first)
  const rawDailyData = data.rewards.daily_activity;

  // Build a continuous 14-day timeline ending today
  const today = new Date();
  const dateMap = new Map(rawDailyData.map(d => [d.date, d.pft]));
  const continuousData: Array<{ date: string; pft: number }> = [];

  // Build data with most recent first (i=0 is today, i=13 is 13 days ago)
  for (let i = 0; i <= 13; i++) {
    const date = new Date(today);
    date.setDate(date.getDate() - i);
    const dateStr = date.toISOString().split('T')[0];
    continuousData.push({
      date: dateStr,
      pft: dateMap.get(dateStr) || 0
    });
  }

  const maxPft = Math.max(...continuousData.map(d => d.pft), 1);

  // Helper to format date as "Jan 31" (UTC dates from blockchain)
  const formatDateLabel = (dateStr: string) => {
    const date = new Date(dateStr + 'T00:00:00Z'); // Parse as UTC
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
  };

  const dailyHtml = continuousData.map(d => {
    const barWidth = d.pft > 0 ? Math.max((d.pft / maxPft) * 100, 3) : 0;
    const isEmpty = d.pft === 0;
    return `
      <div class="daily-row${isEmpty ? ' empty' : ''}">
        <div class="daily-date">${formatDateLabel(d.date)}</div>
        <div class="daily-amount${isEmpty ? ' empty' : ''}">${isEmpty ? '0' : formatPFT(d.pft)}</div>
        <div class="daily-bar-container">
          <div class="daily-bar" style="width: ${barWidth}%"></div>
        </div>
      </div>
    `;
  }).join('');

  document.getElementById('daily-activity')!.innerHTML = `
    <h2>Daily Distribution <span class="section-note-inline">(UTC, 14 days)</span></h2>
    <div class="daily-table">
      ${dailyHtml}
    </div>
  `;

  // Top submitters
  const submittersHtml = data.submissions.top_submitters.slice(0, 10).map((entry, i) => `
    <div class="submitter-row">
      <div class="rank">#${i + 1}</div>
      <div class="address-cell">
        <span class="address" data-full-address="${entry.address}">
          ${formatAddress(entry.address)}
          <span class="address-tooltip">${entry.address}</span>
        </span>
        <a href="https://explorer.testnet.postfiat.org/accounts/${entry.address}" target="_blank" rel="noopener noreferrer" class="explorer-link" title="View on XRPL Explorer">↗</a>
      </div>
      <div class="count">${entry.submissions}</div>
    </div>
  `).join('');

  document.getElementById('submitters')!.innerHTML = `
    <h2>Top Submitters</h2>
    <div class="submitters-list">
      ${submittersHtml}
    </div>
  `;

  // Update timestamps
  updateTimestamps(data);

  // Re-setup interactive handlers after DOM update
  setupAddressCopyHandlers();
  setupAddressSearch();
}

// Update footer and header timestamps
function updateTimestamps(data: NetworkData) {
  const genTime = new Date(data.metadata.generated_at);
  document.getElementById('last-updated')!.textContent = genTime.toLocaleString();

  const formattedDate = genTime.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric'
  });
  const ledgerIndex = data.metadata.ledger_index;
  const ledgerFormatted = ledgerIndex?.toLocaleString() || 'N/A';
  const ledgerLink = ledgerIndex 
    ? `<a href="https://explorer.testnet.postfiat.org/ledgers/${ledgerIndex}" target="_blank" rel="noopener">Ledger #${ledgerFormatted}</a>`
    : `Ledger #${ledgerFormatted}`;
  const githubLink = `<a href="https://github.com/womboai/pft-analytics" target="_blank" rel="noopener">Open Source</a>`;
  document.getElementById('data-timestamp')!.innerHTML = `${formattedDate} • ${ledgerLink} • ${githubLink}`;
}

// Show refresh indicator
function showRefreshIndicator() {
  const indicator = document.getElementById('refresh-indicator');
  if (indicator) {
    indicator.classList.add('refreshing');
  }
}

// Hide refresh indicator
function hideRefreshIndicator() {
  const indicator = document.getElementById('refresh-indicator');
  if (indicator) {
    indicator.classList.remove('refreshing');
  }
}

// Start auto-refresh polling
function startPolling() {
  // Clear any existing interval
  if (refreshIntervalId !== null) {
    clearInterval(refreshIntervalId);
  }

  refreshIntervalId = window.setInterval(async () => {
    try {
      showRefreshIndicator();
      const freshData = await fetchNetworkData();
      renderDashboard(freshData);
      console.log('Dashboard refreshed at', new Date().toLocaleTimeString());
    } catch (error) {
      console.error('Auto-refresh failed:', error);
      // Don't break the UI - just log the error and wait for next interval
    } finally {
      hideRefreshIndicator();
    }
  }, REFRESH_INTERVAL_MS);
}

async function init() {
  const app = document.querySelector<HTMLDivElement>('#app')!;

  app.innerHTML = `
    <header class="header">
      <h1>PFT Task Node Analytics</h1>
      <p class="data-timestamp" id="data-timestamp"></p>
      <div id="refresh-indicator" class="refresh-indicator">
        <span class="refresh-dot"></span>
        <span class="refresh-text">Refreshing...</span>
      </div>
    </header>
    <div class="search-container">
      <div class="search-input-wrapper">
        <input type="text" id="address-search" class="address-search" placeholder="Search by address..." />
        <button type="button" id="search-clear" class="search-clear" aria-label="Clear search">&times;</button>
      </div>
      <div id="search-results-info" class="search-results-info" style="display: none;"></div>
      <div id="search-no-results" class="search-no-results" style="display: none;">No matching addresses</div>
    </div>
    <main class="dashboard">
      <section id="network-totals" class="section full-width">
        <h2>Network Metrics</h2>
        <div class="loading">Loading on-chain data...</div>
      </section>
      <section id="leaderboard" class="section">
        <h2>Top Earners</h2>
        <div class="loading">Loading...</div>
      </section>
      <section id="daily-activity" class="section">
        <h2>Daily Activity</h2>
        <div class="loading">Loading...</div>
      </section>
      <section id="submitters" class="section">
        <h2>Most Active Submitters</h2>
        <div class="loading">Loading...</div>
      </section>
      <section id="network-health" class="section full-width">
        <h2>Network Health</h2>
        <div class="loading">Loading...</div>
      </section>
    </main>
    <footer class="footer">
      <p>XRPL • Updated <span id="last-updated">--</span> • 60s refresh</p>
    </footer>
  `;

  try {
    const data = await fetchNetworkData();

    // Initial render
    renderDashboard(data);

    // Start auto-refresh polling
    startPolling();

  } catch (error) {
    console.error('Failed to fetch analytics:', error);
    document.getElementById('network-totals')!.innerHTML = `
      <h2>Network Overview</h2>
      <div class="error">Failed to load network data. Run: python3 scripts/scan_network.py --output public/data/network.json</div>
    `;
  }
}

// Click-to-copy functionality for addresses
function setupAddressCopyHandlers() {
  const addresses = document.querySelectorAll('.address[data-full-address]');

  addresses.forEach(el => {
    el.addEventListener('click', async (e) => {
      e.stopPropagation();
      const fullAddress = el.getAttribute('data-full-address');
      if (!fullAddress) return;

      try {
        await navigator.clipboard.writeText(fullAddress);
        // Flash the address to confirm copy
        el.classList.add('copied');
        setTimeout(() => el.classList.remove('copied'), 200);
        showCopyFeedback();
      } catch (err) {
        console.error('Failed to copy address:', err);
        showCopyFeedback('Copy failed');
      }
    });
  });
}

// Show "Copied!" feedback toast
function showCopyFeedback(message: string = 'Copied!') {
  // Remove any existing feedback
  const existing = document.querySelector('.copy-feedback');
  if (existing) existing.remove();

  const feedback = document.createElement('div');
  feedback.className = 'copy-feedback';
  feedback.textContent = message;
  document.body.appendChild(feedback);

  // Remove after animation completes
  setTimeout(() => feedback.remove(), 1500);
}

// Address search/filter functionality
function setupAddressSearch() {
  const searchInput = document.getElementById('address-search') as HTMLInputElement;
  const clearBtn = document.getElementById('search-clear') as HTMLButtonElement;
  const noResultsEl = document.getElementById('search-no-results');
  const resultsInfoEl = document.getElementById('search-results-info');

  if (!searchInput || !noResultsEl || !clearBtn || !resultsInfoEl) return;

  // Update clear button visibility based on input value
  function updateClearButton() {
    clearBtn.style.display = searchInput.value.length > 0 ? 'block' : 'none';
  }

  // Initialize clear button state
  updateClearButton();

  searchInput.addEventListener('input', () => {
    updateClearButton();
    const query = searchInput.value.toLowerCase().trim();
    const leaderboardRows = document.querySelectorAll('.leaderboard-table tbody tr');
    const submitterRows = document.querySelectorAll('.submitter-row');
    const allRows = document.querySelectorAll('.leaderboard-table tbody tr, .submitter-row');

    if (!query) {
      // Clear search state - show all rows normally
      allRows.forEach(row => {
        row.classList.remove('search-match', 'search-dimmed');
      });
      noResultsEl.style.display = 'none';
      resultsInfoEl.style.display = 'none';
      return;
    }

    let matchCount = 0;
    let earnerRank: number | null = null;
    let submitterRank: number | null = null;
    let matchedAddress: string | null = null;
    let matchedBalance: string | null = null;
    let matchedEarned: string | null = null;

    // Find rank in leaderboard (earners)
    leaderboardRows.forEach((row, index) => {
      const addressEl = row.querySelector('.address[data-full-address]');
      if (!addressEl) return;

      const fullAddress = addressEl.getAttribute('data-full-address')?.toLowerCase() || '';

      if (fullAddress.includes(query)) {
        row.classList.add('search-match');
        row.classList.remove('search-dimmed');
        matchCount++;
        if (earnerRank === null) {
          earnerRank = index + 1; // 1-indexed
          matchedAddress = addressEl.getAttribute('data-full-address');
          // Extract balance and earned from table cells
          const balanceCell = row.querySelector('.balance-cell');
          const earnedCell = row.querySelector('.earned-cell');
          matchedBalance = balanceCell?.textContent || '0';
          matchedEarned = earnedCell?.textContent || '0';
        }
      } else {
        row.classList.remove('search-match');
        row.classList.add('search-dimmed');
      }
    });

    // Find rank in submitters
    submitterRows.forEach((row, index) => {
      const addressEl = row.querySelector('.address[data-full-address]');
      if (!addressEl) return;

      const fullAddress = addressEl.getAttribute('data-full-address')?.toLowerCase() || '';

      if (fullAddress.includes(query)) {
        row.classList.add('search-match');
        row.classList.remove('search-dimmed');
        matchCount++;
        if (submitterRank === null) submitterRank = index + 1; // 1-indexed
      } else {
        row.classList.remove('search-match');
        row.classList.add('search-dimmed');
      }
    });

    // Show/hide no results message and wallet summary
    if (matchCount === 0) {
      noResultsEl.style.display = 'block';
      resultsInfoEl.style.display = 'none';
    } else {
      noResultsEl.style.display = 'none';

      // Build wallet summary panel when we have a match
      if (matchedAddress && earnerRank !== null) {
        resultsInfoEl.innerHTML = `
          <div class="wallet-summary">
            <div class="wallet-summary-row wallet-address">
              <span class="wallet-label">Address</span>
              <span class="wallet-value address-mono">${matchedAddress}</span>
            </div>
            <div class="wallet-summary-grid">
              <div class="wallet-summary-item">
                <span class="wallet-label">Balance</span>
                <span class="wallet-value accent">${matchedBalance}</span>
              </div>
              <div class="wallet-summary-item">
                <span class="wallet-label">Total Earned</span>
                <span class="wallet-value accent">${matchedEarned}</span>
              </div>
              <div class="wallet-summary-item">
                <span class="wallet-label">Earner Rank</span>
                <span class="wallet-value">#${earnerRank} <span class="wallet-subtext">of ${leaderboardRows.length}</span></span>
              </div>
              <div class="wallet-summary-item">
                <span class="wallet-label">Submitter Rank</span>
                <span class="wallet-value">${submitterRank !== null ? `#${submitterRank} <span class="wallet-subtext">of ${submitterRows.length}</span>` : '<span class="wallet-subtext">Not ranked</span>'}</span>
              </div>
            </div>
          </div>
        `;
      } else {
        // Fallback for non-earner matches (submitters only)
        const parts: string[] = [];
        if (earnerRank !== null) {
          parts.push(`Earner #${earnerRank}`);
        }
        if (submitterRank !== null) {
          parts.push(`Submitter #${submitterRank}`);
        }
        resultsInfoEl.innerHTML = `Found: ${parts.join(' &bull; ')}`;
      }
      resultsInfoEl.style.display = 'block';
    }
  });

  // Clear button click handler
  clearBtn.addEventListener('click', () => {
    searchInput.value = '';
    searchInput.dispatchEvent(new Event('input'));
    searchInput.focus();
  });
}

init();
