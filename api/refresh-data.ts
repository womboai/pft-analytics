/**
 * PFT Network Analytics Scanner (Vercel Serverless Function)
 *
 * Queries TaskNode wallet transaction history via XRPL account_tx.
 * Triggered by Vercel Cron every minute.
 */

import { put } from '@vercel/blob';
import { Client, type AccountTxResponse, type TransactionMetadata } from 'xrpl';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

// Vercel Serverless Function config - 60 second timeout for XRPL queries
export const config = {
  maxDuration: 60,
};

// Constants
const RPC_WS_URL = 'wss://ws.testnet.postfiat.org';
const RIPPLE_EPOCH = 946684800;

// TaskNode addresses
// Primary reward wallets (distribute to many users)
const PRIMARY_REWARD_ADDRESSES = [
  'rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk', // Primary reward wallet
  'rKt4peDozpRW9zdYGiTZC54DSNU3Af6pQE', // Secondary reward wallet
  'rJNwqDPKSkbqDPNoNxbW6C3KCS84ZaQc96', // Additional reward wallet
];

// Minimum funding threshold to be considered a relay wallet (in PFT)
const RELAY_FUNDING_THRESHOLD = 10000;
const MEMO_ADDRESS = 'rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7'; // Receives task memos
const DEBUG_WALLET = 'rDqf4nowC2PAZgn1UGHDn46mcUMREYJrsr';

// System accounts to exclude (built dynamically with discovered relays)
const BASE_SYSTEM_ACCOUNTS = new Set([...PRIMARY_REWARD_ADDRESSES, MEMO_ADDRESS, 'rrrrrrrrrrrrrrrrrrrrrhoLvTp']);

// Type definitions
interface RewardEntry {
  hash: string;
  recipient: string;
  pft: number;
  timestamp: number;
  date: string;
}

interface SubmissionEntry {
  hash: string;
  sender: string;
  timestamp: number;
  date: string;
}

interface InferredTask {
  submitter: string;
  first_submission_ts: number;
  verification_ts?: number;
  reward_ts?: number;
  reward_amount?: number;
  status: 'pending' | 'completed' | 'expired';
}

interface LeaderboardEntry {
  address: string;
  total_pft: number;
  balance: number;
}

interface DailyActivity {
  date: string;
  pft: number;
  tx_count: number;
}

interface DailySubmission {
  date: string;
  submissions: number;
}

interface TopSubmitter {
  address: string;
  submissions: number;
}

interface RewardsAnalysis {
  total_pft_distributed: number;
  unique_recipients: number;
  total_reward_transactions: number;
  leaderboard: LeaderboardEntry[];
  daily_activity: DailyActivity[];
  recent_rewards: RewardEntry[];
}

interface SubmissionsAnalysis {
  total_submissions: number;
  unique_submitters: number;
  top_submitters: TopSubmitter[];
  daily_submissions: DailySubmission[];
  recent_submissions: SubmissionEntry[];
}

interface TaskLifecycleAnalysis {
  total_tasks_inferred: number;
  tasks_completed: number;
  tasks_pending: number;
  tasks_expired: number;
  completion_rate: number;
  avg_time_to_reward_hours: number;
  daily_lifecycle: Array<{
    date: string;
    submitted: number;
    completed: number;
    expired: number;
  }>;
}

interface RewardsAnalysisInternal extends RewardsAnalysis {
  reward_events: RewardEntry[];
  rewards_by_recipient: Map<string, number>;
  balances_map: Map<string, number>;
}

interface SubmissionsAnalysisInternal extends SubmissionsAnalysis {
  submission_events: SubmissionEntry[];
  submissions_by_sender: Map<string, number>;
}

interface NetworkAnalytics {
  metadata: {
    generated_at: string;
    ledger_index: number;
    reward_addresses: string[];
    memo_address: string;
    reward_txs_fetched: number;
    memo_txs_fetched: number;
  };
  network_totals: {
    total_pft_distributed: number;
    unique_earners: number;
    total_rewards_paid: number;
    total_submissions: number;
    unique_submitters: number;
  };
  rewards: RewardsAnalysis;
  submissions: SubmissionsAnalysis;
  task_lifecycle: TaskLifecycleAnalysis;
}

// Transaction from account_tx response - can be in tx or tx_json field
interface TxData {
  TransactionType?: string;
  Account?: string;
  Destination?: string;
  Amount?: string | number | { currency: string; value: string };
  DeliverMax?: string | number | { currency: string; value: string };  // XRPL uses DeliverMax for payments
  hash?: string;
  date?: number;
  Memos?: Array<{
    Memo?: {
      MemoType?: string;
      MemoData?: string;
    };
  }>;
}

interface TxWrapper {
  tx?: TxData;
  tx_json?: TxData;  // xrpl.js v4+ uses tx_json
  meta?: TransactionMetadata;
  hash?: string;
}

// Helper to get transaction data from wrapper (handles both tx and tx_json)
function getTxData(txWrapper: TxWrapper): TxData | undefined {
  return txWrapper.tx_json || txWrapper.tx;
}

// Helper functions
function unixFromRipple(rippleTs: number): number {
  return rippleTs + RIPPLE_EPOCH;
}

function formatDate(unixTs: number): string {
  return new Date(unixTs * 1000).toISOString().split('T')[0];
}

function parsePftAmount(amount: unknown): number | null {
  // PFT is native currency on this XRPL fork, amount is in drops (divide by 1,000,000)
  if (typeof amount === 'string') {
    try {
      const drops = parseInt(amount, 10);
      return drops / 1_000_000;
    } catch {
      return null;
    }
  } else if (typeof amount === 'number') {
    return amount / 1_000_000;
  }
  return null;
}

function round(value: number, decimals: number = 2): number {
  return Math.round(value * Math.pow(10, decimals)) / Math.pow(10, decimals);
}

// Fetch PFT balance (native currency on this XRPL fork)
async function fetchAccountBalance(client: Client, address: string): Promise<number> {
  try {
    const response = await client.request({
      command: 'account_info',
      account: address,
      ledger_index: 'validated',
    });

    // Balance is in drops (1 PFT = 1,000,000 drops)
    const balanceDrops = response.result.account_data.Balance;
    return parseInt(balanceDrops, 10) / 1_000_000;
  } catch {
    // Account not found or other error
    return 0.0;
  }
}

// Fetch all transactions for an account
async function fetchAllAccountTx(
  client: Client,
  account: string,
  maxTxs: number = 5000
): Promise<TxWrapper[]> {
  const allTxs: TxWrapper[] = [];
  let marker: unknown = undefined;

  while (allTxs.length < maxTxs) {
    const request: {
      command: 'account_tx';
      account: string;
      limit: number;
      forward: boolean;
      marker?: unknown;
    } = {
      command: 'account_tx',
      account,
      limit: 400,
      forward: false, // newest first
    };

    if (marker) {
      request.marker = marker;
    }

    const response = (await client.request(request)) as AccountTxResponse;
    const txs = response.result.transactions || [];

    if (txs.length === 0) {
      break;
    }

    allTxs.push(...(txs as TxWrapper[]));
    marker = response.result.marker;

    if (!marker) {
      break;
    }
  }

  return allTxs;
}

// Treasury wallets - funded with large amounts but NOT reward distributors
// These are holding/distribution wallets that send non-reward payments
const TREASURY_WALLETS = [
  'rDZN9ggR1Lmu83752m6SRfW1Uv9iJpJao2',
  'ragLo13ZfV5VHFP1c8g9VvPLUhBjQN7uzt',
  'rrp8KuszsPZTYgTCGf9TC495HA5rrw7VYa',
  'rJnpKqcmXz3vqWPZtvZW2o43bggDfC8ZMr',
];

// Upper threshold - wallets funded with > 100K are likely treasury, not relay
const RELAY_FUNDING_MAX = 100000;

// Discover relay wallets dynamically by scanning memo wallet outbound payments
// Relay wallets are addresses funded by memo wallet with >= RELAY_FUNDING_THRESHOLD PFT
// but less than RELAY_FUNDING_MAX (to exclude treasury wallets)
async function discoverRelayWallets(memoTxs: TxWrapper[]): Promise<string[]> {
  const fundedByMemo = new Map<string, number>();
  
  for (const txWrapper of memoTxs) {
    const tx = getTxData(txWrapper);
    if (!tx) continue;
    if (tx.TransactionType !== 'Payment') continue;
    if (tx.Account !== MEMO_ADDRESS) continue;
    
    const pft = parsePftAmount(tx.DeliverMax) ?? parsePftAmount(tx.Amount);
    if (pft === null || pft <= 0) continue;
    
    const recipient = tx.Destination || '';
    // Skip known primary reward addresses
    if (PRIMARY_REWARD_ADDRESSES.includes(recipient)) continue;
    // Skip memo address itself
    if (recipient === MEMO_ADDRESS) continue;
    // Skip known treasury wallets
    if (TREASURY_WALLETS.includes(recipient)) continue;
    
    fundedByMemo.set(recipient, (fundedByMemo.get(recipient) || 0) + pft);
  }
  
  // Filter to addresses funded with >= min threshold and < max threshold (relay wallets)
  // This excludes treasury wallets funded with millions
  const relayWallets = Array.from(fundedByMemo.entries())
    .filter(([, amount]) => amount >= RELAY_FUNDING_THRESHOLD && amount < RELAY_FUNDING_MAX)
    .map(([addr]) => addr);
  
  return relayWallets;
}

// Analyze reward transactions
async function analyzeRewardTransactions(
  client: Client,
  txs: TxWrapper[],
  rewardAddresses: string[]
): Promise<RewardsAnalysisInternal> {
  const participants = new Set<string>();
  const rewardsByRecipient = new Map<string, number>();
  const rewardsByDay = new Map<string, number>();
  const txCountByDay = new Map<string, number>();
  let totalPft = 0;
  const rewardList: RewardEntry[] = [];
  
  // Build system accounts set dynamically
  const systemAccounts = new Set([...rewardAddresses, MEMO_ADDRESS, 'rrrrrrrrrrrrrrrrrrrrrhoLvTp']);

  for (const txWrapper of txs) {
    const tx = getTxData(txWrapper);
    if (!tx) continue;

    // Only outgoing payments from reward addresses
    if (tx.TransactionType !== 'Payment') continue;
    if (!tx.Account || !rewardAddresses.includes(tx.Account)) continue;

    // Parse PFT amount (DeliverMax is used in newer XRPL, fallback to Amount)
    const pft = parsePftAmount(tx.DeliverMax) ?? parsePftAmount(tx.Amount);
    if (pft === null || pft <= 0) continue;

    const recipient = tx.Destination || '';
    if (systemAccounts.has(recipient)) continue;

    // Get timestamp
    const closeTime = tx.date || 0;
    const unixTs = closeTime ? unixFromRipple(closeTime) : 0;
    const day = unixTs ? formatDate(unixTs) : 'unknown';

    // Aggregate
    participants.add(recipient);
    rewardsByRecipient.set(recipient, (rewardsByRecipient.get(recipient) || 0) + pft);
    rewardsByDay.set(day, (rewardsByDay.get(day) || 0) + pft);
    txCountByDay.set(day, (txCountByDay.get(day) || 0) + 1);
    totalPft += pft;

    const rewardHash =
      tx.hash ||
      txWrapper.hash ||
      `${tx.Account}-${recipient}-${unixTs}-${pft}`;

    rewardList.push({
      hash: rewardHash,
      recipient,
      pft,
      timestamp: unixTs,
      date: day,
    });
  }

  // Fetch balances for all recipients
  const balances = new Map<string, number>();
  const addresses = Array.from(rewardsByRecipient.keys());

  // Fetch balances in parallel batches of 10
  const batchSize = 10;
  for (let i = 0; i < addresses.length; i += batchSize) {
    const batch = addresses.slice(i, i + batchSize);
    const balancePromises = batch.map((addr) => fetchAccountBalance(client, addr));
    const balanceResults = await Promise.all(balancePromises);
    batch.forEach((addr, idx) => {
      balances.set(addr, balanceResults[idx]);
    });
  }

  // Build leaderboard with balances
  const leaderboard: LeaderboardEntry[] = Array.from(rewardsByRecipient.entries())
    .map(([address, totalPft]) => ({
      address,
      total_pft: round(totalPft),
      balance: round(balances.get(address) || 0),
    }))
    .sort((a, b) => {
      // Sort by balance first, then by total_pft
      if (b.balance !== a.balance) return b.balance - a.balance;
      return b.total_pft - a.total_pft;
    });

  // Daily activity
  const dailyActivity: DailyActivity[] = Array.from(rewardsByDay.entries())
    .map(([date, pft]) => ({
      date,
      pft: round(pft),
      tx_count: txCountByDay.get(date) || 0,
    }))
    .sort((a, b) => a.date.localeCompare(b.date));

  return {
    total_pft_distributed: round(totalPft),
    unique_recipients: participants.size,
    total_reward_transactions: rewardList.length,
    leaderboard,
    daily_activity: dailyActivity,
    recent_rewards: rewardList.slice(0, 50),
    reward_events: rewardList,
    rewards_by_recipient: rewardsByRecipient,
    balances_map: balances,
  };
}

// Analyze memo transactions (task submissions)
function analyzeMemoTransactions(txs: TxWrapper[]): SubmissionsAnalysisInternal {
  const submitters = new Set<string>();
  const submissionsBySender = new Map<string, number>();
  const submissionsByDay = new Map<string, number>();
  let totalSubmissions = 0;
  const submissionList: SubmissionEntry[] = [];

  for (const txWrapper of txs) {
    const tx = getTxData(txWrapper);
    if (!tx) continue;

    // Only incoming payments to memo address
    if (tx.TransactionType !== 'Payment') continue;
    if (tx.Destination !== MEMO_ADDRESS) continue;

    const sender = tx.Account || '';
    if (BASE_SYSTEM_ACCOUNTS.has(sender)) continue;

    // Check for pf.ptr memo (hex: 70662e707472)
    const memos = tx.Memos || [];
    const hasPtrMemo = memos.some((m) => {
      const memoType = m.Memo?.MemoType || '';
      return memoType.toLowerCase().includes('70662e707472');
    });

    if (!hasPtrMemo) continue;

    // Get timestamp
    const closeTime = tx.date || 0;
    const unixTs = closeTime ? unixFromRipple(closeTime) : 0;
    const day = unixTs ? formatDate(unixTs) : 'unknown';

    // Aggregate
    submitters.add(sender);
    submissionsBySender.set(sender, (submissionsBySender.get(sender) || 0) + 1);
    submissionsByDay.set(day, (submissionsByDay.get(day) || 0) + 1);
    totalSubmissions++;

    submissionList.push({
      hash: tx.hash || '',
      sender,
      timestamp: unixTs,
      date: day,
    });
  }

  // Top submitters
  const topSubmitters: TopSubmitter[] = Array.from(submissionsBySender.entries())
    .map(([address, submissions]) => ({ address, submissions }))
    .sort((a, b) => b.submissions - a.submissions);

  // Daily submissions
  const dailySubmissions: DailySubmission[] = Array.from(submissionsByDay.entries())
    .map(([date, submissions]) => ({ date, submissions }))
    .sort((a, b) => a.date.localeCompare(b.date));

  return {
    total_submissions: totalSubmissions,
    unique_submitters: submitters.size,
    top_submitters: topSubmitters,
    daily_submissions: dailySubmissions,
    recent_submissions: submissionList.slice(0, 50),
    submission_events: submissionList,
    submissions_by_sender: submissionsBySender,
  };
}

function correlateTaskLifecycle(
  submissions: SubmissionEntry[],
  rewards: RewardEntry[]
): TaskLifecycleAnalysis {
  const SUBMISSION_WINDOW_SEC = 30 * 60;
  const VERIFICATION_WINDOW_SEC = 10 * 60;
  const REWARD_WINDOW_SEC = 30 * 60;
  const EXPIRY_WINDOW_SEC = 24 * 60 * 60;

  const submissionsBySender = new Map<string, SubmissionEntry[]>();
  submissions.forEach((entry) => {
    if (!submissionsBySender.has(entry.sender)) {
      submissionsBySender.set(entry.sender, []);
    }
    submissionsBySender.get(entry.sender)!.push(entry);
  });

  const inferredTasks: InferredTask[] = [];

  submissionsBySender.forEach((entries, sender) => {
    const sorted = entries.slice().sort((a, b) => a.timestamp - b.timestamp);
    let current: InferredTask | null = null;

    for (const entry of sorted) {
      if (!current) {
        current = {
          submitter: sender,
          first_submission_ts: entry.timestamp,
          status: 'pending',
        };
        continue;
      }

      const delta = entry.timestamp - current.first_submission_ts;

      if (delta > SUBMISSION_WINDOW_SEC) {
        inferredTasks.push(current);
        current = {
          submitter: sender,
          first_submission_ts: entry.timestamp,
          status: 'pending',
        };
        continue;
      }

      if (!current.verification_ts && delta <= VERIFICATION_WINDOW_SEC) {
        current.verification_ts = entry.timestamp;
        continue;
      }

      inferredTasks.push(current);
      current = {
        submitter: sender,
        first_submission_ts: entry.timestamp,
        status: 'pending',
      };
    }

    if (current) {
      inferredTasks.push(current);
    }
  });

  const rewardsByRecipient = new Map<string, RewardEntry[]>();
  rewards.forEach((reward) => {
    if (!rewardsByRecipient.has(reward.recipient)) {
      rewardsByRecipient.set(reward.recipient, []);
    }
    rewardsByRecipient.get(reward.recipient)!.push(reward);
  });
  rewardsByRecipient.forEach((entries) => entries.sort((a, b) => a.timestamp - b.timestamp));

  const usedRewardTxs = new Set<string>();
  const nowTs = Math.floor(Date.now() / 1000);

  for (const task of inferredTasks) {
    const lastSubmissionTs = task.verification_ts || task.first_submission_ts;
    const rewardWindowEnd = lastSubmissionTs + REWARD_WINDOW_SEC;
    const rewardEvents = rewardsByRecipient.get(task.submitter) || [];

    for (const reward of rewardEvents) {
      if (usedRewardTxs.has(reward.hash)) continue;
      if (reward.timestamp < lastSubmissionTs) continue;
      if (reward.timestamp > rewardWindowEnd) break;

      task.reward_ts = reward.timestamp;
      task.reward_amount = reward.pft;
      task.status = 'completed';
      usedRewardTxs.add(reward.hash);
      break;
    }

    if (task.status !== 'completed') {
      task.status = nowTs - lastSubmissionTs >= EXPIRY_WINDOW_SEC ? 'expired' : 'pending';
    }
  }

  const dailyLifecycleMap = new Map<string, { submitted: number; completed: number; expired: number }>();
  const ensureDay = (date: string) => {
    if (!dailyLifecycleMap.has(date)) {
      dailyLifecycleMap.set(date, { submitted: 0, completed: 0, expired: 0 });
    }
    return dailyLifecycleMap.get(date)!;
  };

  inferredTasks.forEach((task) => {
    const submittedDate = formatDate(task.first_submission_ts);
    ensureDay(submittedDate).submitted += 1;

    if (task.status === 'completed' && task.reward_ts) {
      const completedDate = formatDate(task.reward_ts);
      ensureDay(completedDate).completed += 1;
    } else if (task.status === 'expired') {
      const expiryDate = formatDate(task.first_submission_ts + EXPIRY_WINDOW_SEC);
      ensureDay(expiryDate).expired += 1;
    }
  });

  const dailyLifecycle = Array.from(dailyLifecycleMap.entries())
    .map(([date, counts]) => ({ date, ...counts }))
    .sort((a, b) => a.date.localeCompare(b.date));

  const completedTasks = inferredTasks.filter((task) => task.status === 'completed');
  const avgTimeToReward =
    completedTasks.length > 0
      ? completedTasks.reduce((sum, task) => sum + (task.reward_ts! - task.first_submission_ts), 0) /
        completedTasks.length /
        3600
      : 0;

  return {
    total_tasks_inferred: inferredTasks.length,
    tasks_completed: completedTasks.length,
    tasks_pending: inferredTasks.filter((task) => task.status === 'pending').length,
    tasks_expired: inferredTasks.filter((task) => task.status === 'expired').length,
    completion_rate: inferredTasks.length > 0 ? (completedTasks.length / inferredTasks.length) * 100 : 0,
    avg_time_to_reward_hours: round(avgTimeToReward, 2),
    daily_lifecycle: dailyLifecycle,
  };
}

// Pre-reset baseline snapshot (Feb 4, 2026 - ledger 6060472)
// Transaction history was wiped during XRPL testnet reset Feb 4-6
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const baselineData = JSON.parse(
  readFileSync(join(__dirname, '..', 'data', 'baseline-pre-reset.json'), 'utf-8')
) as NetworkAnalytics;

// Merge pre-reset baseline with post-reset live data
async function mergeWithBaseline(
  client: Client,
  postResetRewards: RewardsAnalysisInternal,
  postResetSubmissions: SubmissionsAnalysisInternal,
  postResetLifecycle: TaskLifecycleAnalysis,
): Promise<{
  rewards: RewardsAnalysis;
  submissions: SubmissionsAnalysis;
  taskLifecycle: TaskLifecycleAnalysis;
  networkTotals: NetworkAnalytics['network_totals'];
}> {
  // --- Leaderboard merge ---
  // Build baseline lookup with both earned and balance from snapshot
  const baselineLookup = new Map<string, { total_pft: number; balance: number }>();
  for (const entry of baselineData.rewards.leaderboard) {
    baselineLookup.set(entry.address, { total_pft: entry.total_pft, balance: entry.balance });
  }

  // All known earning addresses (baseline + post-reset detected)
  const allEarnerAddresses = new Set([
    ...baselineData.rewards.leaderboard.map(e => e.address),
    ...postResetRewards.rewards_by_recipient.keys(),
  ]);

  // Fetch balances for all addresses (post-reset scanner already fetched some)
  const mergedBalances = new Map(postResetRewards.balances_map);
  const missingBalanceAddrs = [...allEarnerAddresses].filter(addr => !mergedBalances.has(addr));
  const batchSize = 10;
  for (let i = 0; i < missingBalanceAddrs.length; i += batchSize) {
    const batch = missingBalanceAddrs.slice(i, i + batchSize);
    const results = await Promise.all(batch.map(addr => fetchAccountBalance(client, addr)));
    batch.forEach((addr, idx) => mergedBalances.set(addr, results[idx]));
  }

  // Compute earned for each address:
  // - Baseline users: baseline_earned + max(0, current_balance - baseline_balance)
  //   This adds the balance delta (post-reset rewards minus fees) to accurate baseline earned
  // - New post-reset users: use scanner total (or balance if scanner missed rewards)
  const leaderboard: LeaderboardEntry[] = [...allEarnerAddresses]
    .map(address => {
      const balance = round(mergedBalances.get(address) || 0);
      const bl = baselineLookup.get(address);
      let totalPft: number;
      if (bl) {
        const balanceDelta = Math.max(0, balance - bl.balance);
        totalPft = round(bl.total_pft + balanceDelta);
      } else {
        // New user: use higher of scanner-detected or balance
        const scannerTotal = postResetRewards.rewards_by_recipient.get(address) || 0;
        totalPft = round(Math.max(scannerTotal, balance));
      }
      return { address, total_pft: totalPft, balance };
    })
    .sort((a, b) => b.balance !== a.balance ? b.balance - a.balance : b.total_pft - a.total_pft)
    .slice(0, 25);

  // --- Daily activity merge ---
  const dailyActivityMap = new Map<string, { pft: number; tx_count: number }>();
  for (const d of baselineData.rewards.daily_activity) {
    dailyActivityMap.set(d.date, { pft: d.pft, tx_count: d.tx_count });
  }
  for (const d of postResetRewards.daily_activity) {
    const existing = dailyActivityMap.get(d.date);
    if (existing) {
      dailyActivityMap.set(d.date, { pft: existing.pft + d.pft, tx_count: existing.tx_count + d.tx_count });
    } else {
      dailyActivityMap.set(d.date, { pft: d.pft, tx_count: d.tx_count });
    }
  }
  const daily_activity: DailyActivity[] = Array.from(dailyActivityMap.entries())
    .map(([date, { pft, tx_count }]) => ({ date, pft: round(pft), tx_count }))
    .sort((a, b) => a.date.localeCompare(b.date));

  // --- Submissions merge ---
  const mergedSubmitters = new Map<string, number>();
  for (const s of baselineData.submissions.top_submitters) {
    mergedSubmitters.set(s.address, s.submissions);
  }
  for (const [addr, count] of postResetSubmissions.submissions_by_sender) {
    mergedSubmitters.set(addr, (mergedSubmitters.get(addr) || 0) + count);
  }
  const top_submitters: TopSubmitter[] = Array.from(mergedSubmitters.entries())
    .map(([address, submissions]) => ({ address, submissions }))
    .sort((a, b) => b.submissions - a.submissions)
    .slice(0, 25);

  const dailySubsMap = new Map<string, number>();
  for (const d of baselineData.submissions.daily_submissions) {
    dailySubsMap.set(d.date, d.submissions);
  }
  for (const d of postResetSubmissions.daily_submissions) {
    dailySubsMap.set(d.date, (dailySubsMap.get(d.date) || 0) + d.submissions);
  }
  const daily_submissions: DailySubmission[] = Array.from(dailySubsMap.entries())
    .map(([date, submissions]) => ({ date, submissions }))
    .sort((a, b) => a.date.localeCompare(b.date));

  // --- Task lifecycle merge ---
  const dailyLifecycleMap = new Map<string, { submitted: number; completed: number; expired: number }>();
  for (const d of baselineData.task_lifecycle.daily_lifecycle) {
    dailyLifecycleMap.set(d.date, { submitted: d.submitted, completed: d.completed, expired: d.expired });
  }
  for (const d of postResetLifecycle.daily_lifecycle) {
    const existing = dailyLifecycleMap.get(d.date);
    if (existing) {
      dailyLifecycleMap.set(d.date, {
        submitted: existing.submitted + d.submitted,
        completed: existing.completed + d.completed,
        expired: existing.expired + d.expired,
      });
    } else {
      dailyLifecycleMap.set(d.date, { submitted: d.submitted, completed: d.completed, expired: d.expired });
    }
  }
  const daily_lifecycle = Array.from(dailyLifecycleMap.entries())
    .map(([date, counts]) => ({ date, ...counts }))
    .sort((a, b) => a.date.localeCompare(b.date));

  const mergedTasksInferred = baselineData.task_lifecycle.total_tasks_inferred + postResetLifecycle.total_tasks_inferred;
  const mergedTasksCompleted = baselineData.task_lifecycle.tasks_completed + postResetLifecycle.tasks_completed;
  const mergedTasksPending = postResetLifecycle.tasks_pending;
  const mergedTasksExpired = baselineData.task_lifecycle.tasks_expired + postResetLifecycle.tasks_expired;

  // --- Network totals ---
  const baselineEarnerAddrs = new Set(baselineData.rewards.leaderboard.map(e => e.address));
  const postResetEarnerAddrs = new Set(postResetRewards.rewards_by_recipient.keys());
  const earnerOverlap = [...baselineEarnerAddrs].filter(a => postResetEarnerAddrs.has(a)).length;

  const baselineSubmitterAddrs = new Set(baselineData.submissions.top_submitters.map(s => s.address));
  const postResetSubmitterAddrs = new Set(postResetSubmissions.submissions_by_sender.keys());
  const submitterOverlap = [...baselineSubmitterAddrs].filter(a => postResetSubmitterAddrs.has(a)).length;

  // Sum earned across all addresses using same formula as leaderboard
  let totalPftDistributed = 0;
  for (const addr of allEarnerAddresses) {
    const balance = mergedBalances.get(addr) || 0;
    const bl = baselineLookup.get(addr);
    if (bl) {
      totalPftDistributed += bl.total_pft + Math.max(0, balance - bl.balance);
    } else {
      const scannerTotal = postResetRewards.rewards_by_recipient.get(addr) || 0;
      totalPftDistributed += Math.max(scannerTotal, balance);
    }
  }
  totalPftDistributed = round(totalPftDistributed);
  const uniqueEarners = baselineData.network_totals.unique_earners + postResetRewards.unique_recipients - earnerOverlap;
  const totalRewardsPaid = baselineData.network_totals.total_rewards_paid + postResetRewards.total_reward_transactions;
  const totalSubmissions = baselineData.network_totals.total_submissions + postResetSubmissions.total_submissions;
  const uniqueSubmitters = baselineData.network_totals.unique_submitters + postResetSubmissions.unique_submitters - submitterOverlap;

  return {
    rewards: {
      total_pft_distributed: totalPftDistributed,
      unique_recipients: uniqueEarners,
      total_reward_transactions: totalRewardsPaid,
      leaderboard,
      daily_activity,
      recent_rewards: postResetRewards.recent_rewards,
    },
    submissions: {
      total_submissions: totalSubmissions,
      unique_submitters: uniqueSubmitters,
      top_submitters,
      daily_submissions,
      recent_submissions: postResetSubmissions.recent_submissions,
    },
    taskLifecycle: {
      total_tasks_inferred: mergedTasksInferred,
      tasks_completed: mergedTasksCompleted,
      tasks_pending: mergedTasksPending,
      tasks_expired: mergedTasksExpired,
      completion_rate: mergedTasksInferred > 0 ? (mergedTasksCompleted / mergedTasksInferred) * 100 : 0,
      avg_time_to_reward_hours: postResetLifecycle.avg_time_to_reward_hours,
      daily_lifecycle,
    },
    networkTotals: {
      total_pft_distributed: totalPftDistributed,
      unique_earners: uniqueEarners,
      total_rewards_paid: totalRewardsPaid,
      total_submissions: totalSubmissions,
      unique_submitters: uniqueSubmitters,
    },
  };
}

// Main handler - using Vercel's API
import type { VercelRequest, VercelResponse } from '@vercel/node';

export default async function handler(request: VercelRequest, response: VercelResponse) {
  // Verify cron secret in production
  const authHeader = request.headers['authorization'] as string | undefined;
  if (process.env.CRON_SECRET && authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return response.status(401).send('Unauthorized');
  }

  const startTime = Date.now();
  let client: Client | null = null;

  try {
    // Connect to XRPL
    client = new Client(RPC_WS_URL);
    await client.connect();

    // Get current ledger index for liveness indicator
    const ledgerResponse = await client.request({ command: 'ledger_current' });
    const ledgerIndex = ledgerResponse.result.ledger_current_index;

    // Fetch memo transactions first (needed for relay discovery)
    const memoTxs = await fetchAllAccountTx(client, MEMO_ADDRESS, 5000);
    
    // Dynamically discover relay wallets funded by memo address
    const relayWallets = await discoverRelayWallets(memoTxs);
    const allRewardAddresses = [...PRIMARY_REWARD_ADDRESSES, ...relayWallets];
    
    // Fetch reward transactions from ALL reward addresses (primary + discovered relays)
    const rewardTxArrays = await Promise.all(
      allRewardAddresses.map((addr) => fetchAllAccountTx(client!, addr, 5000))
    );
    const rewardTxs = rewardTxArrays.flat();

    if (process.env.DEBUG_WALLET_ANALYSIS === '1') {
      const debugTxs = await fetchAllAccountTx(client, DEBUG_WALLET, 5000);
      let incomingTotal = 0;
      const incomingBySender = new Map<string, number>();
      for (const txWrapper of debugTxs) {
        const tx = getTxData(txWrapper);
        if (!tx) continue;
        if (tx.TransactionType !== 'Payment') continue;
        if (tx.Destination !== DEBUG_WALLET) continue;
        const pft = parsePftAmount(tx.DeliverMax) ?? parsePftAmount(tx.Amount);
        if (pft === null || pft <= 0) continue;
        incomingTotal += pft;
        const sender = tx.Account || 'unknown';
        incomingBySender.set(sender, (incomingBySender.get(sender) || 0) + pft);
      }
    }

    // Analyze post-reset data
    const rewardsInternal = await analyzeRewardTransactions(client, rewardTxs, allRewardAddresses);
    const submissionsInternal = analyzeMemoTransactions(memoTxs);
    const taskLifecycle = correlateTaskLifecycle(
      submissionsInternal.submission_events,
      rewardsInternal.reward_events
    );

    // Merge pre-reset baseline with post-reset live data
    const merged = await mergeWithBaseline(client, rewardsInternal, submissionsInternal, taskLifecycle);

    // Combine into final analytics object
    const analytics: NetworkAnalytics = {
      metadata: {
        generated_at: new Date().toISOString(),
        ledger_index: ledgerIndex,
        reward_addresses: allRewardAddresses,
        memo_address: MEMO_ADDRESS,
        reward_txs_fetched: rewardTxs.length,
        memo_txs_fetched: memoTxs.length,
      },
      network_totals: merged.networkTotals,
      rewards: merged.rewards,
      submissions: merged.submissions,
      task_lifecycle: merged.taskLifecycle,
    };

    // Write to Vercel Blob (overwrite existing file each time)
    const blob = await put('network.json', JSON.stringify(analytics, null, 2), {
      access: 'public',
      contentType: 'application/json',
      addRandomSuffix: false,
      allowOverwrite: true,
      cacheControlMaxAge: 60, // 60s CDN cache — dashboard refreshes every minute
    });

    const elapsedMs = Date.now() - startTime;

    return response.status(200).json({
      success: true,
      elapsed_ms: elapsedMs,
      blob_url: blob.url,
      summary: {
        reward_txs: rewardTxs.length,
        memo_txs: memoTxs.length,
        total_pft_distributed: analytics.network_totals.total_pft_distributed,
        unique_earners: analytics.network_totals.unique_earners,
        total_submissions: analytics.network_totals.total_submissions,
      },
      ...(process.env.DEBUG_WALLET_ANALYSIS === '1'
        ? {
            debug: {
              leaderboard_top10: analytics.rewards.leaderboard.slice(0, 10),
            },
          }
        : {}),
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    console.error('Refresh data error:', errorMessage);

    return response.status(500).json({
      success: false,
      error: errorMessage,
      elapsed_ms: Date.now() - startTime,
    });
  } finally {
    // Clean up XRPL connection
    if (client?.isConnected()) {
      await client.disconnect();
    }
  }
}
