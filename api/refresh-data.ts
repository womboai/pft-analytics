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
const REWARD_ADDRESS = 'rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk'; // Sends PFT rewards
const MEMO_ADDRESS = 'rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7'; // Receives task memos

// System accounts to exclude
const SYSTEM_ACCOUNTS = new Set([REWARD_ADDRESS, MEMO_ADDRESS, 'rrrrrrrrrrrrrrrrrrrrrhoLvTp']);

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

interface NetworkAnalytics {
  metadata: {
    generated_at: string;
    reward_address: string;
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
}

// Transaction from account_tx response - can be in tx or tx_json field
interface TxData {
  TransactionType?: string;
  Account?: string;
  Destination?: string;
  Amount?: string | number | { currency: string; value: string };
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

// Fetch account balance
async function fetchAccountBalance(client: Client, address: string): Promise<number> {
  try {
    const response = await client.request({
      command: 'account_info',
      account: address,
      ledger_index: 'validated',
    });

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

// Analyze reward transactions
async function analyzeRewardTransactions(
  client: Client,
  txs: TxWrapper[]
): Promise<RewardsAnalysis> {
  const participants = new Set<string>();
  const rewardsByRecipient = new Map<string, number>();
  const rewardsByDay = new Map<string, number>();
  const txCountByDay = new Map<string, number>();
  let totalPft = 0;
  const rewardList: RewardEntry[] = [];

  for (const txWrapper of txs) {
    const tx = getTxData(txWrapper);
    if (!tx) continue;

    // Only outgoing payments from reward address
    if (tx.TransactionType !== 'Payment') continue;
    if (tx.Account !== REWARD_ADDRESS) continue;

    // Parse PFT amount
    const pft = parsePftAmount(tx.Amount);
    if (pft === null || pft <= 0) continue;

    const recipient = tx.Destination || '';
    if (SYSTEM_ACCOUNTS.has(recipient)) continue;

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

    rewardList.push({
      hash: tx.hash || '',
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
  };
}

// Analyze memo transactions (task submissions)
function analyzeMemoTransactions(txs: TxWrapper[]): SubmissionsAnalysis {
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

    // Fetch reward transactions
    const rewardTxs = await fetchAllAccountTx(client, REWARD_ADDRESS, 5000);

    // Fetch memo transactions
    const memoTxs = await fetchAllAccountTx(client, MEMO_ADDRESS, 5000);

    // Analyze
    const rewards = await analyzeRewardTransactions(client, rewardTxs);
    const submissions = analyzeMemoTransactions(memoTxs);

    // Combine into final analytics object
    const analytics: NetworkAnalytics = {
      metadata: {
        generated_at: new Date().toISOString(),
        reward_address: REWARD_ADDRESS,
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
    };

    // Write to Vercel Blob
    const blob = await put('network.json', JSON.stringify(analytics, null, 2), {
      access: 'public',
      contentType: 'application/json',
      addRandomSuffix: false,
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
