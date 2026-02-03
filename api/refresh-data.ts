/**
 * PFT Network Analytics Scanner (Vercel Serverless Function)
 *
 * Queries TaskNode wallet transaction history via XRPL account_tx.
 * Triggered by Vercel Cron every minute.
 */

import { put } from '@vercel/blob';
import { Client, type AccountTxResponse, type TransactionMetadata } from 'xrpl';

// Vercel Serverless Function config - 60 second timeout for XRPL queries
export const config = {
  maxDuration: 60,
};

// Constants
const RPC_WS_URL = 'wss://rpc.testnet.postfiat.org:6007';
const RIPPLE_EPOCH = 946684800;

// TaskNode addresses
const REWARD_ADDRESSES = [
  'rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk', // Primary reward wallet
  'rKt4peDozpRW9zdYGiTZC54DSNU3Af6pQE', // Secondary reward wallet
  'rJNwqDPKSkbqDPNoNxbW6C3KCS84ZaQc96', // Additional reward wallet
  'rKddMw1hqMGwfgJvzjbWQHtBQT8hDcZNCP', // Memo-funded reward relay (funded by memo addr)
];
const MEMO_ADDRESS = 'rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7'; // Receives task memos
const DEBUG_WALLET = 'rDqf4nowC2PAZgn1UGHDn46mcUMREYJrsr';

// System accounts to exclude
const SYSTEM_ACCOUNTS = new Set([...REWARD_ADDRESSES, MEMO_ADDRESS, 'rrrrrrrrrrrrrrrrrrrrrhoLvTp']);

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
}

interface SubmissionsAnalysisInternal extends SubmissionsAnalysis {
  submission_events: SubmissionEntry[];
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
    // #region agent log
    if (address === DEBUG_WALLET) {
      fetch('http://127.0.0.1:7242/ingest/65fd5333-ce3c-47a5-9a12-4a91675ab968',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'api/refresh-data.ts:217',message:'account_info balance for debug wallet',data:{address,balanceDrops},timestamp:Date.now(),sessionId:'debug-session',runId:'pre-fix',hypothesisId:'H4'})}).catch(()=>{});
    }
    // #endregion
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

  // #region agent log
  fetch('http://127.0.0.1:7242/ingest/65fd5333-ce3c-47a5-9a12-4a91675ab968',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'api/refresh-data.ts:270',message:'account_tx fetch summary',data:{account,total_txs:allTxs.length,max_txs:maxTxs,hit_max:allTxs.length>=maxTxs,has_marker:Boolean(marker)},timestamp:Date.now(),sessionId:'debug-session',runId:'pre-fix',hypothesisId:'H2'})}).catch(()=>{});
  // #endregion
  return allTxs;
}

// Analyze reward transactions
async function analyzeRewardTransactions(
  client: Client,
  txs: TxWrapper[]
): Promise<RewardsAnalysisInternal> {
  const participants = new Set<string>();
  const rewardsByRecipient = new Map<string, number>();
  const rewardsByDay = new Map<string, number>();
  const txCountByDay = new Map<string, number>();
  let totalPft = 0;
  const rewardList: RewardEntry[] = [];

  // #region agent log
  fetch('http://127.0.0.1:7242/ingest/65fd5333-ce3c-47a5-9a12-4a91675ab968',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'api/refresh-data.ts:297',message:'analyzeRewardTransactions start',data:{tx_count:txs.length,reward_addresses:REWARD_ADDRESSES.length},timestamp:Date.now(),sessionId:'debug-session',runId:'pre-fix',hypothesisId:'H1'})}).catch(()=>{});
  // #endregion
  for (const txWrapper of txs) {
    const tx = getTxData(txWrapper);
    if (!tx) continue;

    // Only outgoing payments from reward addresses
    if (tx.TransactionType !== 'Payment') continue;
    if (!tx.Account || !REWARD_ADDRESSES.includes(tx.Account)) continue;

    const candidateRecipient = tx.Destination || '';
    if (candidateRecipient === DEBUG_WALLET) {
      const meta = txWrapper.meta as unknown as { delivered_amount?: unknown; DeliveredAmount?: unknown } | undefined;
      // #region agent log
      fetch('http://127.0.0.1:7242/ingest/65fd5333-ce3c-47a5-9a12-4a91675ab968',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'api/refresh-data.ts:306',message:'debug wallet payment raw fields',data:{hash:tx.hash,amount:tx.Amount,deliverMax:tx.DeliverMax,delivered_amount:meta?.delivered_amount,DeliveredAmount:meta?.DeliveredAmount},timestamp:Date.now(),sessionId:'debug-session',runId:'pre-fix',hypothesisId:'H3'})}).catch(()=>{});
      // #endregion
    }

    // Parse PFT amount (DeliverMax is used in newer XRPL, fallback to Amount)
    const pft = parsePftAmount(tx.DeliverMax) ?? parsePftAmount(tx.Amount);
    if (pft === null || pft <= 0) continue;

    const recipient = tx.Destination || '';
    if (SYSTEM_ACCOUNTS.has(recipient)) continue;

    // #region agent log
    if (recipient === DEBUG_WALLET) {
      const meta = txWrapper.meta as unknown as { delivered_amount?: unknown; DeliveredAmount?: unknown } | undefined;
      fetch('http://127.0.0.1:7242/ingest/65fd5333-ce3c-47a5-9a12-4a91675ab968',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'api/refresh-data.ts:317',message:'reward tx for debug wallet',data:{hash:tx.hash,amount:tx.Amount,deliverMax:tx.DeliverMax,delivered_amount:meta?.delivered_amount,DeliveredAmount:meta?.DeliveredAmount},timestamp:Date.now(),sessionId:'debug-session',runId:'pre-fix',hypothesisId:'H3'})}).catch(()=>{});
    }
    // #endregion
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
    })
    .slice(0, 25);

  // #region agent log
  const debugEntry = leaderboard.find((entry) => entry.address === DEBUG_WALLET);
  if (debugEntry) {
    fetch('http://127.0.0.1:7242/ingest/65fd5333-ce3c-47a5-9a12-4a91675ab968',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'api/refresh-data.ts:367',message:'debug wallet leaderboard entry',data:{address:debugEntry.address,total_pft:debugEntry.total_pft,balance:debugEntry.balance},timestamp:Date.now(),sessionId:'debug-session',runId:'pre-fix',hypothesisId:'H3'})}).catch(()=>{});
  }
  // #endregion
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
    if (SYSTEM_ACCOUNTS.has(sender)) continue;

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
    .sort((a, b) => b.submissions - a.submissions)
    .slice(0, 25);

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

    // Fetch reward transactions from ALL reward addresses
    const rewardTxArrays = await Promise.all(
      REWARD_ADDRESSES.map((addr) => fetchAllAccountTx(client!, addr, 5000))
    );
    const rewardTxs = rewardTxArrays.flat();

    // Fetch memo transactions
    const memoTxs = await fetchAllAccountTx(client, MEMO_ADDRESS, 5000);

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
      const topSenders = Array.from(incomingBySender.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([sender, total]) => ({ sender, total: round(total) }));
      // #region agent log
      fetch('http://127.0.0.1:7242/ingest/65fd5333-ce3c-47a5-9a12-4a91675ab968',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'api/refresh-data.ts:629',message:'debug wallet incoming summary',data:{address:DEBUG_WALLET,total_incoming:round(incomingTotal),unique_senders:incomingBySender.size,top_senders:topSenders,tx_count:debugTxs.length},timestamp:Date.now(),sessionId:'debug-session',runId:'pre-fix',hypothesisId:'H5'})}).catch(()=>{});
      // #endregion
    }

    // Analyze
    const rewardsInternal = await analyzeRewardTransactions(client, rewardTxs);
    const submissionsInternal = analyzeMemoTransactions(memoTxs);
    const taskLifecycle = correlateTaskLifecycle(
      submissionsInternal.submission_events,
      rewardsInternal.reward_events
    );

    const { reward_events, ...rewards } = rewardsInternal;
    const { submission_events, ...submissions } = submissionsInternal;

    // Combine into final analytics object
    const analytics: NetworkAnalytics = {
      metadata: {
        generated_at: new Date().toISOString(),
        ledger_index: ledgerIndex,
        reward_addresses: REWARD_ADDRESSES,
        memo_address: MEMO_ADDRESS,
        reward_txs_fetched: rewardTxs.length,
        memo_txs_fetched: memoTxs.length,
      },
      network_totals: {
        total_pft_distributed: rewards.total_pft_distributed,
        unique_earners: rewards.unique_recipients,
        total_rewards_paid: rewards.total_reward_transactions,
        total_submissions: submissions.total_submissions,
        unique_submitters: submissions.unique_submitters,
      },
      rewards,
      submissions,
      task_lifecycle: taskLifecycle,
    };

    // Write to Vercel Blob (overwrite existing file each time)
    const blob = await put('network.json', JSON.stringify(analytics, null, 2), {
      access: 'public',
      contentType: 'application/json',
      addRandomSuffix: false,
      allowOverwrite: true,
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
