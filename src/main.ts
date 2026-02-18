import './style.css';
import { fetchNetworkData, formatPFT, formatAddress } from './api';
import type { NetworkData } from './api';

// Polling interval in milliseconds (60 seconds)
const REFRESH_INTERVAL_MS = 60000;
const DEV_ACTIVITY_LOOKBACK_DAYS = 7;

// Store the interval ID so we can clear it if needed
let refreshIntervalId: number | null = null;

function formatRelativeTime(isoDate: string): string {
  const eventTs = Date.parse(isoDate);
  if (!Number.isFinite(eventTs)) return 'Unknown';

  const elapsedMs = Date.now() - eventTs;
  const isFuture = elapsedMs < 0;
  const ms = Math.abs(elapsedMs);
  const seconds = Math.max(0, Math.floor(ms / 1000));
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  const weeks = Math.floor(days / 7);
  const months = Math.floor(days / 30);

  if (seconds < 60) return isFuture ? 'in a few seconds' : 'just now';
  if (minutes < 60) return isFuture ? `${minutes} min from now` : `${minutes} min ago`;
  if (hours < 24) return isFuture ? `${hours}h from now` : `${hours}h ago`;
  if (days < 7) return isFuture ? `${days}d from now` : `${days}d ago`;
  if (weeks < 8) return isFuture ? `${weeks}w from now` : `${weeks}w ago`;
  return isFuture ? `${months}mo from now` : `${months}mo ago`;
}

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

  // Build a continuous 14-day timeline ending today.
  // Missing days are treated as 0 to avoid "n/a" artifacts.
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
      pft: dateMap.get(dateStr) ?? 0,
    });
  }

  const maxPft = Math.max(...continuousData.map(d => d.pft), 1);

  // Helper to format date as "Jan 31" (UTC dates from blockchain)
  const formatDateLabel = (dateStr: string) => {
    const date = new Date(dateStr + 'T00:00:00Z'); // Parse as UTC
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
  };

  const dailyHtml = continuousData.map(d => {
    const pft = d.pft;
    const barWidth = pft > 0 ? Math.max((pft / maxPft) * 100, 3) : 0;
    const isEmpty = pft === 0;
    return `
      <div class="daily-row${isEmpty ? ' empty' : ''}">
        <div class="daily-date">${formatDateLabel(d.date)}</div>
        <div class="daily-amount${isEmpty ? ' empty' : ''}">${isEmpty ? '0' : formatPFT(pft)}</div>
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

  const devActivity = data.dev_activity;
  const devRows = devActivity?.events
    .slice(0, 60)
    .map((entry, index) => {
      const eventTime = formatRelativeTime(entry.occurred_at);
      const typeLabel = entry.type === 'merged_pr' ? 'Merged PR' : 'Commit';
      const actorLogin = entry.actor_login || 'unknown';
      const actorLabel = actorLogin === 'unknown' ? 'Unknown' : `@${formatAddress(actorLogin)}`;
      const actorProfileUrl = actorLogin === 'unknown' ? null : `https://github.com/${actorLogin}`;
      const repoLabel = entry.repo_full_name || 'Private Repository';
      const eventTitle = entry.title || 'Commit activity update';
      const eventLink = entry.url || null;
      return `
        <div class="dev-feed-row ${eventLink ? 'clickable' : ''}" data-full-address="${actorLogin}" data-event-url="${eventLink ?? ''}" role="${eventLink ? 'link' : 'group'}" tabindex="${eventLink ? 0 : -1}" aria-label="${eventLink ? `Open ${typeLabel}: ${eventTitle}` : `${typeLabel} event for ${actorLabel} (no artifact link)`}">
          <div class="dev-feed-header">
            <div class="dev-feed-rank">#${index + 1}</div>
            ${actorProfileUrl
              ? `<a href="${actorProfileUrl}" target="_blank" rel="noopener noreferrer" class="dev-feed-author" title="Open GitHub profile">${actorLabel}</a>`
              : `<span class="dev-feed-author" title="Unknown author">${actorLabel}</span>`
            }
            <span class="dev-feed-type ${entry.type}">${typeLabel}</span>
            ${eventLink
              ? `<a href="${eventLink}" target="_blank" rel="noopener noreferrer" class="explorer-link" title="Open event">↗</a>`
              : `<span class="explorer-link disabled" title="No artifact link available">↗</span>`
            }
          </div>
          <div class="dev-feed-title" title="${eventTitle}">${eventTitle}</div>
          <div class="dev-feed-meta">
            <span>${repoLabel}</span>
            <span>${eventTime}</span>
            ${entry.pr_number ? `<span>PR #${entry.pr_number}</span>` : ''}
          </div>
        </div>
      `;
    })
    .join('');

  const statsText = devActivity
    ? `<div class="section-note-inline">Events: ${devActivity.stats.total_events_7d} · Contributors: ${devActivity.stats.unique_contributors_7d}</div>`
    : '<div class="section-note-inline">No updates yet</div>';

  const devRowsList = devRows || `<div class="dev-feed-empty">No activity in the last ${devActivity?.lookback_days ?? DEV_ACTIVITY_LOOKBACK_DAYS} days</div>`;

  document.getElementById('submitters')!.innerHTML = `
    <h2>PFT Team Code Updates</h2>
    ${statsText}
    <div class="dev-feed-list">
      ${devRowsList}
    </div>
  `;

  // Update timestamps
  updateTimestamps(data);

  // Re-setup interactive handlers after DOM update
  setupAddressCopyHandlers();
  setupAddressSearch();
  setupDevFeedRowLinks();
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

function setupDevFeedRowLinks() {
  const rows = document.querySelectorAll<HTMLElement>('.dev-feed-row.clickable[data-event-url]');
  rows.forEach((row) => {
    const eventUrl = row.getAttribute('data-event-url');
    if (!eventUrl) return;

    const openEvent = (e?: Event) => {
      if (e && e instanceof MouseEvent) {
        const target = e.target as HTMLElement;
        if (target.closest('.explorer-link, .dev-feed-author')) {
          return;
        }
      }
      window.open(eventUrl, '_blank', 'noopener,noreferrer');
    };

    row.addEventListener('click', (event) => {
      openEvent(event);
    });

    row.addEventListener('keydown', (event) => {
      const keyboard = event as KeyboardEvent;
      if (keyboard.key === 'Enter' || keyboard.key === ' ') {
        keyboard.preventDefault();
        openEvent();
      }
    });
  });
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
        <h2>PFT Team Code Updates</h2>
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
    const devFeedRows = document.querySelectorAll('.dev-feed-row');
    const allRows = document.querySelectorAll('.leaderboard-table tbody tr, .dev-feed-row');

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
    let devFeedRank: number | null = null;
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

    // Find rank in team activity feed
    devFeedRows.forEach((row, index) => {
      const fullAddress = row.getAttribute('data-full-address')?.toLowerCase() || '';

      if (fullAddress.includes(query)) {
        row.classList.add('search-match');
        row.classList.remove('search-dimmed');
        matchCount++;
        if (devFeedRank === null) devFeedRank = index + 1; // 1-indexed
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
                <span class="wallet-label">Team Activity Match</span>
                <span class="wallet-value">${devFeedRank !== null ? `#${devFeedRank} <span class="wallet-subtext">of ${devFeedRows.length}</span>` : '<span class="wallet-subtext">No team activity match</span>'}</span>
              </div>
            </div>
          </div>
        `;
      } else {
        // Fallback for non-earner matches
        const parts: string[] = [];
        if (earnerRank !== null) {
          parts.push(`Earner #${earnerRank}`);
        }
        if (devFeedRank !== null) {
          parts.push(`Team Activity #${devFeedRank}`);
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
