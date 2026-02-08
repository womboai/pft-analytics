#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { Client } from 'xrpl';

const DEFAULT_RPC_URL = 'wss://ws.testnet.postfiat.org';
const DEFAULT_MEMO_ADDRESS = 'rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7';
const DEFAULT_PRIMARY_REWARD_ADDRESSES = [
  'rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk',
  'rKt4peDozpRW9zdYGiTZC54DSNU3Af6pQE',
  'rJNwqDPKSkbqDPNoNxbW6C3KCS84ZaQc96',
];
const DEFAULT_FALLBACK_RELAY_ADDRESSES = [
  'rKddMw1hqMGwfgJvzjbWQHtBQT8hDcZNCP',
  'rs3YdBLJHFGhcPKtbMwQgkbrpo1YjyajTP',
  'rscWPz4aA4GtQKH5tYvVFwteiSSjTjounC',
  'rJNBxuus1TjCq3pikYUsKUXXwyBsJqQAt9',
  'rD9LaW5h5AeGoHsPARWNegfZs3XpeNrv9w',
  'rBDbRYd8H7gB6mdNTRssgNvsw8Z6c4riDb',
  'rhczhWeG3eSohzcH5jw8m8Ynca9cgH4eZm',
  'rPThdoLYRDNkcook9MxGP4WV7tiN5xnsTJ',
  'rEpmxYQXvAffdiBu21ewXXZcqzmhapn2Dm',
];
const TREASURY_WALLETS = [
  'rDZN9ggR1Lmu83752m6SRfW1Uv9iJpJao2',
  'ragLo13ZfV5VHFP1c8g9VvPLUhBjQN7uzt',
  'rrp8KuszsPZTYgTCGf9TC495HA5rrw7VYa',
  'rJnpKqcmXz3vqWPZtvZW2o43bggDfC8ZMr',
];

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '..');
const DEFAULT_OUT_DIR = path.join(PROJECT_ROOT, 'monitor', 'reset-monitor');
const GMAIL_DIR = path.resolve(PROJECT_ROOT, '..', 'agent_integrations', 'gmail');
const GMAIL_CLI = path.join(GMAIL_DIR, 'gmail_cli.py');

function parseArgs(argv) {
  const args = {
    once: false,
    intervalSeconds: Number(process.env.RESET_MONITOR_POLL_SECONDS ?? 20),
    outDir: process.env.RESET_MONITOR_OUT_DIR ?? DEFAULT_OUT_DIR,
    rpcUrl: process.env.PFT_RPC_WS_URL ?? DEFAULT_RPC_URL,
    pythonBin: process.env.RESET_MONITOR_PYTHON_BIN ?? 'python3',
    resetBaseline: false,
    recoveryRatio: Number(process.env.RESET_MONITOR_RECOVERY_RATIO ?? 0.9),
    ledgerRollbackThreshold: Number(process.env.RESET_MONITOR_LEDGER_ROLLBACK_THRESHOLD ?? 1000),
    balanceDropRatio: Number(process.env.RESET_MONITOR_BALANCE_DROP_RATIO ?? 0.2),
    minBalanceForDropPft: Number(process.env.RESET_MONITOR_MIN_BALANCE_FOR_DROP_PFT ?? 1000),
    dropWalletCountThreshold: Number(process.env.RESET_MONITOR_DROP_WALLET_COUNT_THRESHOLD ?? 2),
    missingWalletCountThreshold: Number(process.env.RESET_MONITOR_MISSING_WALLET_COUNT_THRESHOLD ?? 2),
    emailEnabled: process.env.RESET_MONITOR_EMAIL_ENABLED !== '0',
    emailAccount: process.env.RESET_MONITOR_EMAIL_ACCOUNT ?? 'wombo',
    emailTo: process.env.RESET_MONITOR_EMAIL_TO ?? null,
    emailOnRecovery: process.env.RESET_MONITOR_EMAIL_ON_RECOVERY !== '0',
    extraWallets: [],
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--once') args.once = true;
    else if (arg === '--reset-baseline') args.resetBaseline = true;
    else if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg.startsWith('--python=')) args.pythonBin = arg.split('=')[1];
    else if (arg === '--python' && argv[i + 1]) {
      args.pythonBin = argv[i + 1];
      i += 1;
    }
    else if (arg === '--no-email') args.emailEnabled = false;
    else if (arg.startsWith('--email-account=')) args.emailAccount = arg.split('=')[1];
    else if (arg === '--email-account' && argv[i + 1]) {
      args.emailAccount = argv[i + 1];
      i += 1;
    } else if (arg.startsWith('--email-to=')) args.emailTo = arg.split('=')[1];
    else if (arg === '--email-to' && argv[i + 1]) {
      args.emailTo = argv[i + 1];
      i += 1;
    } else if (arg === '--no-email-on-recovery') args.emailOnRecovery = false;
    else if (arg.startsWith('--interval=')) args.intervalSeconds = Number(arg.split('=')[1]);
    else if (arg === '--interval' && argv[i + 1]) {
      args.intervalSeconds = Number(argv[i + 1]);
      i += 1;
    } else if (arg.startsWith('--out-dir=')) args.outDir = arg.split('=')[1];
    else if (arg === '--out-dir' && argv[i + 1]) {
      args.outDir = argv[i + 1];
      i += 1;
    } else if (arg.startsWith('--rpc=')) args.rpcUrl = arg.split('=')[1];
    else if (arg === '--rpc' && argv[i + 1]) {
      args.rpcUrl = argv[i + 1];
      i += 1;
    } else if (arg.startsWith('--extra-wallet=')) {
      args.extraWallets.push(arg.split('=')[1]);
    } else if (arg === '--extra-wallet' && argv[i + 1]) {
      args.extraWallets.push(argv[i + 1]);
      i += 1;
    }
  }

  if (!Number.isFinite(args.intervalSeconds) || args.intervalSeconds < 5) {
    args.intervalSeconds = 20;
  }

  return args;
}

function printHelp() {
  console.log(`
PFT reset monitor

Usage:
  node scripts/reset_monitor.mjs [options]

  Options:
  --once                     Take one snapshot and exit
  --interval <seconds>       Poll interval in seconds (default: 20, min: 5)
  --out-dir <path>           Output directory (default: monitor/reset-monitor)
  --rpc <url>                XRPL WebSocket URL
  --python <path>            Python binary for Gmail integration (default: python3)
  --email-to <address>       Alert recipient (default: authenticated Gmail account)
  --email-account <name>     Gmail account alias used by gmail_cli.py (default: wombo)
  --no-email                 Disable email notifications
  --no-email-on-recovery     Only send detection email (skip resolved email)
  --extra-wallet <address>   Add an extra wallet to watch (can repeat)
  --reset-baseline           Discard previous baseline and start fresh
  --help                     Show this help

Output:
  snapshots/*.json           Raw wallet snapshots
  state.json                 Monitor state (baseline, last snapshot, incident status)
  latest.md                  Current monitor summary
  incidents/<id>/report.md   Detailed impact report while incident is active
`);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readJsonMaybe(filePath) {
  if (!fs.existsSync(filePath)) return null;
  try {
    return readJson(filePath);
  } catch {
    return null;
  }
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function writeText(filePath, value) {
  fs.writeFileSync(filePath, value, 'utf8');
}

function nowIsoFileSafe(dateIso) {
  return dateIso.replace(/[:.]/g, '-');
}

function resolveStatePath(outDir, filePath) {
  if (!filePath) return null;
  return path.isAbsolute(filePath) ? filePath : path.join(outDir, filePath);
}

function toStatePath(outDir, absolutePath) {
  return path.relative(outDir, absolutePath);
}

function round(value, digits = 6) {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function formatPft(value, digits = 2) {
  return Number(value).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatPct(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return 'n/a';
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
}

function resolveEmailRecipient({ emailEnabled, explicitEmailTo, emailAccount, pythonBin }) {
  if (!emailEnabled) return null;
  if (explicitEmailTo) return explicitEmailTo;
  try {
    const pythonSnippet = [
      'import sys',
      `sys.path.insert(0, ${JSON.stringify(GMAIL_DIR)})`,
      'from gmail_client import GmailClient',
      `client = GmailClient.from_env(account=${JSON.stringify(emailAccount)})`,
      'profile = client.service.users().getProfile(userId="me").execute()',
      'print(profile.get("emailAddress", ""))',
    ].join('\n');
    const output = execFileSync(pythonBin, ['-c', pythonSnippet], {
      cwd: GMAIL_DIR,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const email = output.trim();
    if (email) return email;
  } catch (error) {
    const message = error?.stderr?.toString?.() ?? error?.message ?? String(error);
    console.error(`email recipient resolution failed: ${message}`);
  }
  return null;
}

function sendEmailAlert({ emailEnabled, emailAccount, to, subject, body, html = false, pythonBin }) {
  if (!emailEnabled || !to) return false;
  try {
    const args = [
      GMAIL_CLI,
      '--account',
      emailAccount,
      'send',
      '--to',
      to,
      '--subject',
      subject,
      '--body',
      body,
    ];
    if (html) args.push('--html');
    execFileSync(pythonBin, args, {
      cwd: GMAIL_DIR,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    return true;
  } catch (error) {
    const message = error?.stderr?.toString?.() ?? error?.message ?? String(error);
    console.error(`email alert send failed: ${message}`);
    return false;
  }
}

function buildIncidentEmail({ incident, baseline, current, reportPath }) {
  const before = baseline.totals.reward_balance_pft;
  const now = current.totals.reward_balance_pft;
  const delta = now - before;
  const deltaPct = before > 0 ? (delta / before) * 100 : null;
  const lines = [
    `PFT reset monitor detected an incident.`,
    '',
    `Incident: ${incident.id}`,
    `Status: ${incident.status}`,
    `Detected at: ${incident.started_at}`,
    `Current snapshot: ${current.timestamp_iso}`,
    '',
    `Ledger: ${baseline.ledger_index} -> ${current.ledger_index}`,
    `Reward wallets total: ${formatPft(before, 6)} -> ${formatPft(now, 6)} ` +
      `(${delta >= 0 ? '+' : ''}${formatPft(delta, 6)}, ${formatPct(deltaPct)})`,
    '',
    'Trigger reasons:',
    ...incident.reasons.map((reason) => `- ${reason}`),
    '',
    `Detailed report: ${reportPath}`,
  ];
  return lines.join('\n');
}

function buildResolvedEmail({ incident, baseline, current, reportPath }) {
  const before = baseline.totals.reward_balance_pft;
  const now = current.totals.reward_balance_pft;
  const delta = now - before;
  const deltaPct = before > 0 ? (delta / before) * 100 : null;
  const lines = [
    `PFT reset monitor marked incident as resolved.`,
    '',
    `Incident: ${incident.id}`,
    `Started: ${incident.started_at}`,
    `Resolved: ${incident.resolved_at}`,
    '',
    `Reward wallets total at baseline: ${formatPft(before, 6)} PFT`,
    `Reward wallets total now: ${formatPft(now, 6)} PFT`,
    `Net change vs baseline: ${delta >= 0 ? '+' : ''}${formatPft(delta, 6)} (${formatPct(deltaPct)})`,
    '',
    `Detailed report: ${reportPath}`,
  ];
  return lines.join('\n');
}

function loadRewardAddresses(projectRoot) {
  const networkPath = path.join(projectRoot, 'public', 'data', 'network.json');
  const networkData = readJsonMaybe(networkPath);
  const fromSnapshot = networkData?.metadata?.reward_addresses;
  if (Array.isArray(fromSnapshot) && fromSnapshot.length > 0) {
    return fromSnapshot;
  }
  return [...DEFAULT_PRIMARY_REWARD_ADDRESSES, ...DEFAULT_FALLBACK_RELAY_ADDRESSES];
}

function loadMemoAddress(projectRoot) {
  const networkPath = path.join(projectRoot, 'public', 'data', 'network.json');
  const networkData = readJsonMaybe(networkPath);
  return networkData?.metadata?.memo_address ?? DEFAULT_MEMO_ADDRESS;
}

function normalizeWallet(address) {
  return address.trim();
}

function buildWalletList({ rewardAddresses, memoAddress, extraWallets }) {
  const primarySet = new Set(DEFAULT_PRIMARY_REWARD_ADDRESSES);
  const all = [];
  const seen = new Set();

  function push(address, role, label) {
    const normalized = normalizeWallet(address);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    all.push({ address: normalized, role, label });
  }

  for (const address of rewardAddresses) {
    const role = primarySet.has(address) ? 'primary' : 'relay';
    push(address, role, role === 'primary' ? 'primary_reward' : 'relay_reward');
  }

  push(memoAddress, 'memo', 'memo_wallet');
  for (const address of TREASURY_WALLETS) {
    push(address, 'treasury', 'treasury_wallet');
  }
  for (const address of extraWallets) {
    push(address, 'extra', 'extra_wallet');
  }

  return all;
}

async function fetchWalletState(client, wallet) {
  try {
    const response = await client.request({
      command: 'account_info',
      account: wallet.address,
      ledger_index: 'validated',
    });
    const data = response.result.account_data;
    return {
      ...wallet,
      exists: true,
      balance_pft: Number(data.Balance) / 1_000_000,
      sequence: Number(data.Sequence),
      owner_count: Number(data.OwnerCount ?? 0),
      error: null,
    };
  } catch (error) {
    const message = error?.data?.error_message ?? error?.data?.error ?? error?.message ?? String(error);
    return {
      ...wallet,
      exists: false,
      balance_pft: 0,
      sequence: null,
      owner_count: null,
      error: message,
    };
  }
}

function computeTotals(walletStates) {
  const totals = {
    primary_balance_pft: 0,
    relay_balance_pft: 0,
    reward_balance_pft: 0,
    memo_balance_pft: 0,
    treasury_balance_pft: 0,
    extra_balance_pft: 0,
    watched_balance_pft: 0,
    missing_wallets: 0,
  };

  for (const wallet of walletStates) {
    totals.watched_balance_pft += wallet.balance_pft;
    if (!wallet.exists) totals.missing_wallets += 1;
    if (wallet.role === 'primary') totals.primary_balance_pft += wallet.balance_pft;
    if (wallet.role === 'relay') totals.relay_balance_pft += wallet.balance_pft;
    if (wallet.role === 'memo') totals.memo_balance_pft += wallet.balance_pft;
    if (wallet.role === 'treasury') totals.treasury_balance_pft += wallet.balance_pft;
    if (wallet.role === 'extra') totals.extra_balance_pft += wallet.balance_pft;
  }

  totals.reward_balance_pft = totals.primary_balance_pft + totals.relay_balance_pft;

  for (const key of Object.keys(totals)) {
    if (key.endsWith('_pft')) totals[key] = round(totals[key], 6);
  }

  return totals;
}

async function fetchSnapshot(client, wallets) {
  const ledger = await client.request({ command: 'ledger_current' });
  const ledgerIndex = Number(ledger.result.ledger_current_index);

  const walletStates = await Promise.all(wallets.map((wallet) => fetchWalletState(client, wallet)));
  const totals = computeTotals(walletStates);

  return {
    version: 1,
    timestamp_iso: new Date().toISOString(),
    ledger_index: ledgerIndex,
    wallets: walletStates,
    totals,
  };
}

function walletMap(snapshot) {
  return new Map(snapshot.wallets.map((wallet) => [wallet.address, wallet]));
}

function compareWallets(previous, current) {
  const previousByAddress = walletMap(previous);
  const rows = [];
  for (const wallet of current.wallets) {
    const previousWallet = previousByAddress.get(wallet.address);
    if (!previousWallet) continue;
    const delta = wallet.balance_pft - previousWallet.balance_pft;
    const deltaPct = previousWallet.balance_pft > 0 ? (delta / previousWallet.balance_pft) * 100 : null;
    rows.push({
      address: wallet.address,
      role: wallet.role,
      before_balance_pft: previousWallet.balance_pft,
      after_balance_pft: wallet.balance_pft,
      delta_balance_pft: delta,
      delta_pct: deltaPct,
      before_exists: previousWallet.exists,
      after_exists: wallet.exists,
    });
  }
  return rows;
}

function detectReset(previous, current, config) {
  const reasons = [];
  const details = {
    ledger_rollback: false,
    dropped_reward_wallets: [],
    missing_reward_wallets: 0,
  };

  if (current.ledger_index + config.ledgerRollbackThreshold < previous.ledger_index) {
    details.ledger_rollback = true;
    reasons.push(
      `Ledger index dropped from ${previous.ledger_index} to ${current.ledger_index} ` +
      `(threshold ${config.ledgerRollbackThreshold})`
    );
  }

  const diffs = compareWallets(previous, current);
  const droppedRewardWallets = diffs.filter((row) => {
    if (row.role !== 'primary' && row.role !== 'relay') return false;
    if (row.before_balance_pft < config.minBalanceForDropPft) return false;
    return row.after_balance_pft <= row.before_balance_pft * config.balanceDropRatio;
  });

  details.dropped_reward_wallets = droppedRewardWallets;
  if (droppedRewardWallets.length >= config.dropWalletCountThreshold) {
    reasons.push(
      `${droppedRewardWallets.length} reward wallets dropped below ${(config.balanceDropRatio * 100).toFixed(0)}% ` +
      `of previous balance`
    );
  }

  const missingRewardWallets = current.wallets.filter(
    (wallet) => (wallet.role === 'primary' || wallet.role === 'relay') && !wallet.exists
  );
  details.missing_reward_wallets = missingRewardWallets.length;
  if (missingRewardWallets.length >= config.missingWalletCountThreshold) {
    reasons.push(`${missingRewardWallets.length} reward wallets are missing (account_info failed)`);
  }

  return {
    triggered: reasons.length > 0,
    reasons,
    details,
  };
}

function loadState(stateFile) {
  const state = readJsonMaybe(stateFile);
  if (state && typeof state === 'object') return state;
  return {
    version: 1,
    baseline_snapshot_file: null,
    last_snapshot_file: null,
    incident: null,
  };
}

function buildWalletDiffRows(baseline, current) {
  const diffs = compareWallets(baseline, current)
    .filter((row) => ['primary', 'relay', 'memo'].includes(row.role))
    .sort((a, b) => Math.abs(b.delta_balance_pft) - Math.abs(a.delta_balance_pft));
  const header = [
    '| Wallet | Role | Before (PFT) | Current (PFT) | Delta (PFT) | Delta % |',
    '|---|---|---:|---:|---:|---:|',
  ];
  const body = diffs.map((row) => [
    `| ${row.address} | ${row.role} | ${formatPft(row.before_balance_pft, 6)} | ` +
    `${formatPft(row.after_balance_pft, 6)} | ` +
    `${row.delta_balance_pft >= 0 ? '+' : ''}${formatPft(row.delta_balance_pft, 6)} | ` +
    `${formatPct(row.delta_pct)} |`,
  ]);
  return [...header, ...body].join('\n');
}

function renderIncidentReport({ incident, baseline, current }) {
  const before = baseline.totals.reward_balance_pft;
  const now = current.totals.reward_balance_pft;
  const delta = now - before;
  const deltaPct = before > 0 ? (delta / before) * 100 : null;

  const lines = [];
  lines.push(`# PFT Reset Incident: ${incident.id}`);
  lines.push('');
  lines.push(`- Status: **${incident.status.toUpperCase()}**`);
  lines.push(`- Started: ${incident.started_at}`);
  lines.push(`- Last update: ${current.timestamp_iso}`);
  if (incident.status === 'resolved' && incident.resolved_at) {
    lines.push(`- Resolved: ${incident.resolved_at}`);
  }
  lines.push('');
  lines.push('## Trigger Signals');
  for (const reason of incident.reasons) {
    lines.push(`- ${reason}`);
  }
  lines.push('');
  lines.push('## Reward Wallet Impact');
  lines.push('');
  lines.push('| Metric | Baseline | Current | Delta |');
  lines.push('|---|---:|---:|---:|');
  lines.push(
    `| Reward wallets total | ${formatPft(before, 6)} | ${formatPft(now, 6)} | ` +
    `${delta >= 0 ? '+' : ''}${formatPft(delta, 6)} (${formatPct(deltaPct)}) |`
  );
  lines.push(
    `| Primary wallets total | ${formatPft(baseline.totals.primary_balance_pft, 6)} | ` +
    `${formatPft(current.totals.primary_balance_pft, 6)} | ` +
    `${(current.totals.primary_balance_pft - baseline.totals.primary_balance_pft) >= 0 ? '+' : ''}` +
    `${formatPft(current.totals.primary_balance_pft - baseline.totals.primary_balance_pft, 6)} |`
  );
  lines.push(
    `| Relay wallets total | ${formatPft(baseline.totals.relay_balance_pft, 6)} | ` +
    `${formatPft(current.totals.relay_balance_pft, 6)} | ` +
    `${(current.totals.relay_balance_pft - baseline.totals.relay_balance_pft) >= 0 ? '+' : ''}` +
    `${formatPft(current.totals.relay_balance_pft - baseline.totals.relay_balance_pft, 6)} |`
  );
  lines.push(`| Ledger index | ${baseline.ledger_index} | ${current.ledger_index} | ${current.ledger_index - baseline.ledger_index} |`);
  lines.push('');
  lines.push('## Wallet-Level Diff');
  lines.push('');
  lines.push(buildWalletDiffRows(baseline, current));
  lines.push('');
  lines.push('## Suggested Follow-Up Checks');
  lines.push('- Verify if balances were swept back to users and at what rate.');
  lines.push('- Check whether task submission flow (`memo` payments) resumed.');
  lines.push('- Compare post-reset reward issuance velocity against baseline.');
  lines.push('');
  return `${lines.join('\n')}\n`;
}

function renderLatestStatus({ snapshot, state, baseline }) {
  const lines = [];
  lines.push('# PFT Reset Monitor Status');
  lines.push('');
  lines.push(`- Last snapshot: ${snapshot.timestamp_iso}`);
  lines.push(`- Ledger index: ${snapshot.ledger_index}`);
  lines.push(`- Reward wallets total: ${formatPft(snapshot.totals.reward_balance_pft, 6)} PFT`);
  lines.push(`- Primary wallets total: ${formatPft(snapshot.totals.primary_balance_pft, 6)} PFT`);
  lines.push(`- Relay wallets total: ${formatPft(snapshot.totals.relay_balance_pft, 6)} PFT`);
  lines.push(`- Memo wallet balance: ${formatPft(snapshot.totals.memo_balance_pft, 6)} PFT`);
  if (baseline) {
    lines.push(`- Baseline snapshot: ${baseline.timestamp_iso} (ledger ${baseline.ledger_index})`);
  }
  if (state.incident && state.incident.status === 'active') {
    lines.push(`- Active incident: ${state.incident.id}`);
  } else {
    lines.push('- Active incident: none');
  }
  lines.push('');
  return `${lines.join('\n')}\n`;
}

function createIncidentId(timestampIso) {
  return `incident-${nowIsoFileSafe(timestampIso).replace(/Z$/, '')}`;
}

async function runStep(context) {
  const {
    client,
    wallets,
    outDir,
    stateFile,
    snapshotsDir,
    incidentsDir,
    latestFile,
    state,
    config,
  } = context;

  const snapshot = await fetchSnapshot(client, wallets);
  const snapshotName = `${nowIsoFileSafe(snapshot.timestamp_iso)}.json`;
  const snapshotPath = path.join(snapshotsDir, snapshotName);
  writeJson(snapshotPath, snapshot);
  const snapshotStatePath = toStatePath(outDir, snapshotPath);

  const previousSnapshot = state.last_snapshot_file
    ? readJsonMaybe(resolveStatePath(outDir, state.last_snapshot_file))
    : null;

  if (!state.baseline_snapshot_file) {
    state.baseline_snapshot_file = snapshotStatePath;
  }

  if (config.resetBaseline) {
    state.baseline_snapshot_file = snapshotStatePath;
    state.incident = null;
  }

  const baselineSnapshot = state.baseline_snapshot_file
    ? readJsonMaybe(resolveStatePath(outDir, state.baseline_snapshot_file))
    : null;

  const detection = previousSnapshot ? detectReset(previousSnapshot, snapshot, config) : { triggered: false, reasons: [] };

  if (detection.triggered && (!state.incident || state.incident.status !== 'active')) {
    const incidentId = createIncidentId(snapshot.timestamp_iso);
    state.incident = {
      id: incidentId,
      status: 'active',
      started_at: snapshot.timestamp_iso,
      reasons: detection.reasons,
      baseline_snapshot_file: state.baseline_snapshot_file,
      detected_snapshot_file: snapshotStatePath,
      alert_sent_at: null,
      alert_email_to: null,
      resolved_alert_sent_at: null,
      resolved_at: null,
      resolved_snapshot_file: null,
    };
  }

  if (state.incident && state.incident.status === 'active') {
    const incidentDir = path.join(incidentsDir, state.incident.id);
    ensureDir(incidentDir);

    const incidentBaseline = readJsonMaybe(resolveStatePath(outDir, state.incident.baseline_snapshot_file));
    if (incidentBaseline) {
      const currentReward = snapshot.totals.reward_balance_pft;
      const baselineReward = incidentBaseline.totals.reward_balance_pft;
      let resolvedNow = false;
      if (baselineReward > 0 && currentReward >= baselineReward * config.recoveryRatio) {
        state.incident.status = 'resolved';
        state.incident.resolved_at = snapshot.timestamp_iso;
        state.incident.resolved_snapshot_file = snapshotStatePath;
        state.baseline_snapshot_file = snapshotStatePath;
        resolvedNow = true;
      }

      const report = renderIncidentReport({
        incident: state.incident,
        baseline: incidentBaseline,
        current: snapshot,
      });
      const reportPath = path.join(incidentDir, 'report.md');
      writeText(reportPath, report);
      writeJson(path.join(incidentDir, 'incident.json'), state.incident);

      if (!state.incident.alert_sent_at) {
        const emailSubject = `[PFT Monitor] Reset detected (${state.incident.id})`;
        const emailBody = buildIncidentEmail({
          incident: state.incident,
          baseline: incidentBaseline,
          current: snapshot,
          reportPath,
        });
        const sent = sendEmailAlert({
          emailEnabled: config.emailEnabled,
          emailAccount: config.emailAccount,
          to: config.emailTo,
          subject: emailSubject,
          body: emailBody,
          pythonBin: config.pythonBin,
        });
        if (sent) {
          state.incident.alert_sent_at = snapshot.timestamp_iso;
          state.incident.alert_email_to = config.emailTo;
          console.log(`  email-alert: sent detection email to ${config.emailTo}`);
        }
      }

      if (resolvedNow && config.emailOnRecovery && !state.incident.resolved_alert_sent_at) {
        const resolvedSubject = `[PFT Monitor] Incident resolved (${state.incident.id})`;
        const resolvedBody = buildResolvedEmail({
          incident: state.incident,
          baseline: incidentBaseline,
          current: snapshot,
          reportPath,
        });
        const sentResolved = sendEmailAlert({
          emailEnabled: config.emailEnabled,
          emailAccount: config.emailAccount,
          to: config.emailTo,
          subject: resolvedSubject,
          body: resolvedBody,
          pythonBin: config.pythonBin,
        });
        if (sentResolved) {
          state.incident.resolved_alert_sent_at = snapshot.timestamp_iso;
          console.log(`  email-alert: sent resolved email to ${config.emailTo}`);
        }
      }
    }
  }

  state.last_snapshot_file = snapshotStatePath;
  state.version = 1;
  writeJson(stateFile, state);

  const baselineForStatus = state.baseline_snapshot_file
    ? readJsonMaybe(resolveStatePath(outDir, state.baseline_snapshot_file))
    : null;
  writeText(latestFile, renderLatestStatus({ snapshot, state, baseline: baselineForStatus }));

  const incidentLabel = state.incident ? `${state.incident.id} (${state.incident.status})` : 'none';
  console.log(
    `[${snapshot.timestamp_iso}] ledger=${snapshot.ledger_index} reward=${formatPft(snapshot.totals.reward_balance_pft, 6)} ` +
    `primary=${formatPft(snapshot.totals.primary_balance_pft, 6)} relay=${formatPft(snapshot.totals.relay_balance_pft, 6)} ` +
    `incident=${incidentLabel}`
  );
  if (detection.triggered) {
    for (const reason of detection.reasons) {
      console.log(`  reset-signal: ${reason}`);
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    return;
  }

  const rewardAddresses = loadRewardAddresses(PROJECT_ROOT);
  const memoAddress = loadMemoAddress(PROJECT_ROOT);
  const wallets = buildWalletList({
    rewardAddresses,
    memoAddress,
    extraWallets: args.extraWallets,
  });

  const outDir = path.resolve(args.outDir);
  const snapshotsDir = path.join(outDir, 'snapshots');
  const incidentsDir = path.join(outDir, 'incidents');
  const stateFile = path.join(outDir, 'state.json');
  const latestFile = path.join(outDir, 'latest.md');

  ensureDir(outDir);
  ensureDir(snapshotsDir);
  ensureDir(incidentsDir);

  const state = loadState(stateFile);
  if (args.resetBaseline) {
    state.baseline_snapshot_file = null;
    state.incident = null;
  }

  const config = {
    resetBaseline: args.resetBaseline,
    recoveryRatio: args.recoveryRatio,
    ledgerRollbackThreshold: args.ledgerRollbackThreshold,
    balanceDropRatio: args.balanceDropRatio,
    minBalanceForDropPft: args.minBalanceForDropPft,
    dropWalletCountThreshold: args.dropWalletCountThreshold,
    missingWalletCountThreshold: args.missingWalletCountThreshold,
    emailEnabled: args.emailEnabled,
    emailAccount: args.emailAccount,
    pythonBin: args.pythonBin,
    emailTo: resolveEmailRecipient({
      emailEnabled: args.emailEnabled,
      explicitEmailTo: args.emailTo,
      emailAccount: args.emailAccount,
      pythonBin: args.pythonBin,
    }),
    emailOnRecovery: args.emailOnRecovery,
  };

  if (config.emailEnabled && !config.emailTo) {
    console.log('email alerts disabled: could not resolve alert recipient');
    config.emailEnabled = false;
  }

  console.log(
    `monitor starting: rpc=${args.rpcUrl} wallets=${wallets.length} out=${outDir} ` +
    `email=${config.emailEnabled ? `${config.emailTo} (${config.emailAccount})` : 'off'}`
  );
  const client = new Client(args.rpcUrl);
  await client.connect();

  let stopRequested = false;
  process.on('SIGINT', () => { stopRequested = true; });
  process.on('SIGTERM', () => { stopRequested = true; });

  try {
    while (!stopRequested) {
      try {
        await runStep({
          client,
          wallets,
          outDir,
          stateFile,
          snapshotsDir,
          incidentsDir,
          latestFile,
          state,
          config,
        });
      } catch (error) {
        console.error(`step failed: ${error?.message ?? String(error)}`);
      }

      if (args.once || stopRequested) break;
      await sleep(args.intervalSeconds * 1000);
    }
  } finally {
    await client.disconnect();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
