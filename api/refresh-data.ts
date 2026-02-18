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
const ARCHIVE_RPC_WS_URL = 'wss://ws-archive.testnet.postfiat.org';
const LIVE_RPC_WS_URL = 'wss://ws.testnet.postfiat.org';
const RPC_WS_URL = process.env.PFT_RPC_WS_URL || ARCHIVE_RPC_WS_URL;
const RIPPLE_EPOCH = 946684800;
const OFFICIAL_LEADERBOARD_URL = 'https://tasknode.postfiat.org/api/leaderboard';
const TASKNODE_JWT =
  process.env.PFT_TASKNODE_JWT ||
  process.env.TASKNODE_JWT ||
  process.env.TASKNODE_API_TOKEN ||
  '';
const DEFAULT_NETWORK_DATA_URL =
  'https://dclwht8rlliznsdz.public.blob.vercel-storage.com/network.json';
const NETWORK_DATA_URL = process.env.PFT_NETWORK_DATA_URL || DEFAULT_NETWORK_DATA_URL;
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const DAY_MS = 24 * 60 * 60 * 1000;

// TaskNode addresses
// Primary reward wallets (distribute to many users)
const PRIMARY_REWARD_ADDRESSES = [
  'rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk', // Primary reward wallet
  'rKt4peDozpRW9zdYGiTZC54DSNU3Af6pQE', // Secondary reward wallet
  'rJNwqDPKSkbqDPNoNxbW6C3KCS84ZaQc96', // Additional reward wallet
];
const KNOWN_REWARD_RELAYS = [
  // Observed in official Task Node wallet feeds as a reward sender (pf.ptr memos).
  'rKddMw1hqMGwfgJvzjbWQHtBQT8hDcZNCP',
];

// Minimum funding threshold to be considered a relay wallet (in PFT)
const RELAY_FUNDING_THRESHOLD = 10000;
const RELAY_BEHAVIOR_LOOKBACK_DAYS = 30;
const RELAY_BEHAVIOR_MIN_MEMO_FUNDING_PFT = 100;
const RELAY_BEHAVIOR_MIN_PTR_TXS = 3;
const RELAY_BEHAVIOR_MIN_UNIQUE_RECIPIENTS = 2;
const RELAY_BEHAVIOR_MIN_TOTAL_PFT = 100;
const RELAY_BEHAVIOR_CANDIDATE_SCAN_LIMIT = 60;
const RELAY_BEHAVIOR_TX_FETCH_LIMIT = 2000;
const MEMO_ADDRESS = 'rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7'; // Receives task memos
const DEBUG_WALLET = 'rDqf4nowC2PAZgn1UGHDn46mcUMREYJrsr';
const GITHUB_API_BASE = 'https://api.github.com';
const GITHUB_ORG = 'postfiatorg';
const GITHUB_REQUEST_TIMEOUT_MS = 12_000;
const DEV_ACTIVITY_LOOKBACK_DAYS = Number(
  process.env.PFT_DEV_ACTIVITY_LOOKBACK_DAYS || process.env.PFT_DEV_ACTIVITY_LOOKBACK || '7'
);
const DEV_ACTIVITY_ACTIVE_CONTRIBUTOR_DAYS = Number(
  process.env.PFT_DEV_ACTIVITY_ACTIVE_CONTRIBUTOR_DAYS ||
    process.env.PFT_DEV_ACTIVITY_ACTIVE_WINDOW_DAYS ||
    '30'
);
const DEV_ACTIVITY_MAX_EVENTS = Number(process.env.PFT_DEV_ACTIVITY_MAX_EVENTS || '120');
const DEV_ACTIVITY_MAX_REPOS = Number(process.env.PFT_DEV_ACTIVITY_MAX_REPOS || '50');
const DEV_ACTIVITY_KEEP_DAYS = Number(process.env.PFT_DEV_ACTIVITY_KEEP_DAYS || '45');
const DEV_ACTIVITY_SUMMARY_MODEL =
  process.env.PFT_GPT_MODEL ||
  process.env.PFT_OPENAI_MODEL ||
  process.env.OPENAI_MODEL ||
  'gpt-4o-mini';
const DEV_ACTIVITY_SUMMARY_FALLBACK_MODELS = [
  DEV_ACTIVITY_SUMMARY_MODEL,
  'gpt-4o-mini',
  'gpt-4o',
  'gpt-4.1-mini',
];
const DEV_ACTIVITY_LLM_DAILY_ALERT_USD = Number(
  process.env.PFT_LLM_DAILY_ALERT_USD || '100'
);
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';

// System accounts to exclude (built dynamically with discovered relays)
const BASE_SYSTEM_ACCOUNTS = new Set([...PRIMARY_REWARD_ADDRESSES, MEMO_ADDRESS, 'rrrrrrrrrrrrrrrrrrrrrhoLvTp']);

const OPENAI_MODEL_PRICING: Record<string, { inputUsdPerMillion: number; outputUsdPerMillion: number }> = {
  'gpt-5': { inputUsdPerMillion: 1.25, outputUsdPerMillion: 10 },
  'gpt-4o': { inputUsdPerMillion: 5, outputUsdPerMillion: 15 },
  'gpt-4o-mini': { inputUsdPerMillion: 0.15, outputUsdPerMillion: 0.6 },
  'gpt-4.1-mini': { inputUsdPerMillion: 0.4, outputUsdPerMillion: 1.6 },
};

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

interface RelayBehaviorMatch {
  address: string;
  ptr_tx_count: number;
  unique_recipients: number;
  total_pft: number;
  last_ptr_reward_date: string | null;
  memo_funding_total_pft: number;
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

interface GitHubSearchCommitRepository {
  full_name?: string;
  name?: string;
  owner?: { login?: string };
}

interface GitHubSearchCommitItem {
  sha?: string;
  html_url?: string;
  commit?: {
    message?: string;
    author?: { date?: string };
    committer?: { date?: string };
  };
  author?: { login?: string } | null;
  repository?: GitHubSearchCommitRepository;
}

interface GitHubPull {
  number?: number;
  title?: string;
  html_url?: string;
  merged_at?: string | null;
  merged_by?: { login?: string } | null;
  user?: { login?: string } | null;
  repository?: GitHubSearchCommitRepository;
}

interface DevContributor {
  github_login: string;
  source: 'postfiatorg_recent_commit_or_pr';
  last_seen_at: string;
}

interface DevContributionEvent {
  id: string;
  type: 'commit' | 'merged_pr';
  occurred_at: string;
  actor_login: string;
  repo_full_name: string;
  repo_owner: string;
  repo_name: string;
  title: string;
  summary: string;
  summary_is_llm_generated?: boolean;
  url: string;
  sha?: string;
  pr_number?: number;
  is_postfiatorg_repo: boolean;
}

interface DevFeedStats {
  total_events_7d: number;
  postfiatorg_events_7d: number;
  external_repo_events_7d: number;
  unique_contributors_7d: number;
}

interface DevFeedSpend {
  estimated_usd_today: number;
  threshold_usd: number;
  threshold_exceeded: boolean;
  last_alert_at?: string;
  run_estimated_usd: number;
  run_event_count: number;
}

interface DevActivity {
  generated_at: string;
  lookback_days: number;
  active_contributor_window_days: number;
  contributors: DevContributor[];
  events: DevContributionEvent[];
  stats: DevFeedStats;
  spend_monitor: DevFeedSpend;
}

interface HistoricalDevActivity {
  events: DevContributionEvent[];
  contributors: DevContributor[];
  spend: DevFeedSpend | null;
  generatedAt: string;
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

interface NetworkHealthMetrics {
  ws_latency_ms: number;
  ledger_index: number;
  ledger_close_time: string;
  ledger_close_unix: number;
  seconds_since_close: number;
  endpoint_status: 'online' | 'offline';
  endpoint_url: string;
}

interface RewardsAnalysisInternal extends RewardsAnalysis {
  reward_events: RewardEntry[];
  rewards_by_recipient: Map<string, number>;
  balances_map: Map<string, number>;
  excluded_non_ptr_reward_txs: number;
  excluded_non_ptr_reward_pft: number;
  non_task_daily_activity: DailyActivity[];
  non_task_recent_distributions: RewardEntry[];
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
    official_leaderboard_rows?: number;
    historical_daily_rows?: number;
    excluded_non_ptr_reward_txs?: number;
    excluded_non_ptr_reward_pft?: number;
    relay_wallets_discovered_funding?: string[];
    relay_wallets_discovered_behavior?: string[];
    relay_behavior_candidates_scanned?: number;
    relay_behavior_lookback_days?: number;
    relay_behavior_matches?: RelayBehaviorMatch[];
    github_repos_scanned?: number;
    github_external_repos_scanned?: number;
    github_active_contributors_scanned?: number;
    github_events_collected?: number;
    github_request_failures?: string[];
    github_error_count?: number;
  };
  network_totals: {
    total_pft_distributed: number;
    unique_earners: number;
    total_rewards_paid: number;
    total_submissions: number;
    unique_submitters: number;
  };
  rewards: RewardsAnalysis;
  dev_activity?: DevActivity;
  non_task_distributions: {
    total_pft_distributed: number;
    total_transactions: number;
    daily_activity: DailyActivity[];
    recent_distributions: RewardEntry[];
  };
  submissions: SubmissionsAnalysis;
  task_lifecycle: TaskLifecycleAnalysis;
  network_health: NetworkHealthMetrics;
}

async function connectClientWithFallback(): Promise<{ client: Client; endpoint: string }> {
  const candidates = Array.from(
    new Set([RPC_WS_URL, ARCHIVE_RPC_WS_URL, LIVE_RPC_WS_URL].filter(Boolean))
  );

  let lastError: unknown = null;
  for (const endpoint of candidates) {
    const candidate = new Client(endpoint);
    try {
      await candidate.connect();
      return { client: candidate, endpoint };
    } catch (error) {
      lastError = error;
      if (candidate.isConnected()) {
        await candidate.disconnect();
      }
    }
  }

  throw new Error(
    `Unable to connect to any XRPL endpoint (${candidates.join(', ')}): ${
      lastError instanceof Error ? lastError.message : String(lastError)
    }`
  );
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

function hasPfPtrMemo(tx: TxData): boolean {
  const memos = tx.Memos || [];
  return memos.some((m) => {
    const memoType = m.Memo?.MemoType || '';
    return memoType.toLowerCase().includes('70662e707472');
  });
}

function round(value: number, decimals: number = 2): number {
  return Math.round(value * Math.pow(10, decimals)) / Math.pow(10, decimals);
}

function clampInt(value: number, fallback: number, min = 0, max = Number.MAX_SAFE_INTEGER): number {
  const parsed = Number.isFinite(value) ? Math.floor(value) : fallback;
  if (parsed < min) return min;
  if (parsed > max) return max;
  return parsed;
}

function normalizePositiveInt(value: number, fallback: number, max = Number.MAX_SAFE_INTEGER): number {
  return clampInt(value, fallback, 1, max);
}

function dateDaysAgo(days: number): Date {
  const normalizedDays = Math.max(0, days);
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  now.setDate(now.getDate() - normalizedDays);
  return now;
}

function dateToIsoSeconds(value: Date): string {
  return value.toISOString();
}

function parseDateMs(value: string): number {
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : 0;
}

function formatEventTime(value: string): string {
  return new Date(value).toISOString();
}

function dedupeAndSort<T extends { id: string }>(rows: T[]): T[] {
  const deduped = new Map<string, T>();
  for (const row of rows) {
    deduped.set(row.id, row);
  }
  return Array.from(deduped.values());
}

function coalesceTrimmed(value: unknown, fallback = ''): string {
  if (typeof value !== 'string') return fallback;
  const trimmed = value.trim();
  return trimmed.length ? trimmed : fallback;
}

function getGithubHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    Accept: 'application/vnd.github+json',
    'User-Agent': 'pft-analytics-refresh-data',
  };
  if (OPENAI_API_KEY.length > 0 && OPENAI_API_KEY.startsWith('sk-')) {
    // no-op: intentionally keep in case of accidental key reuse in tests
  }
  if (process.env.GITHUB_TOKEN || process.env.PFT_GITHUB_TOKEN) {
    headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN || process.env.PFT_GITHUB_TOKEN}`;
  }
  return headers;
}

function sanitizeCommitMessage(value: string): string {
  return value
    .replace(/\r/g, '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)[0] || 'Untitled commit';
}

function isPostfiatorgRepo(fullName: string): boolean {
  return fullName.startsWith(`${GITHUB_ORG}/`);
}

function parseRepoOwnerAndName(fullName: string): { owner: string; name: string } {
  const [owner, name] = fullName.split('/');
  return { owner: owner || 'unknown', name: name || fullName };
}

function getModelFallbackChain(): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const model of DEV_ACTIVITY_SUMMARY_FALLBACK_MODELS) {
    const normalized = coalesceTrimmed(model, '').toLowerCase();
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

function getModelPricing(model: string): { inputUsdPerMillion: number; outputUsdPerMillion: number } {
  return (
    OPENAI_MODEL_PRICING[model] || {
      inputUsdPerMillion: 1,
      outputUsdPerMillion: 2,
    }
  );
}

function estimateSummaryCostUsd(
  promptTokens: number,
  completionTokens: number,
  model: string,
): number {
  const pricing = getModelPricing(model);
  const inputCost = (Math.max(0, promptTokens) / 1_000_000) * pricing.inputUsdPerMillion;
  const outputCost = (Math.max(0, completionTokens) / 1_000_000) * pricing.outputUsdPerMillion;
  return round(inputCost + outputCost, 6);
}

function estimateTextTokens(value: string): number {
  return Math.max(1, Math.ceil(coalesceTrimmed(value).length / 4));
}

function formatSafeModelName(model: string): string {
  return model.trim().toLowerCase();
}

async function githubGet<T>(path: string): Promise<{ ok: true; data: T; status: number } | { ok: false; status: number; error: string }> {
  try {
    const abortController = new AbortController();
    const timeout = setTimeout(() => abortController.abort(), GITHUB_REQUEST_TIMEOUT_MS);
    const response = await fetch(path, {
      method: 'GET',
      headers: getGithubHeaders(),
      signal: abortController.signal,
    });
    clearTimeout(timeout);
    if (!response.ok) {
      const message = await response.text();
      return { ok: false, status: response.status, error: `${response.status}: ${message || 'github request failed'}` };
    }
    const data = (await response.json()) as T;
    return { ok: true, data, status: response.status };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

async function openaiSummarizeBatch(events: DevContributionEvent[]): Promise<Record<string, string>> {
  if (!OPENAI_API_KEY || events.length === 0) {
    return {};
  }

  const summaries: Record<string, string> = {};
  const chunks = [];
  const chunkSize = 20;

  for (let i = 0; i < events.length; i += chunkSize) {
    chunks.push(events.slice(i, i + chunkSize));
  }

  for (const chunk of chunks) {
    let attemptError: string | null = null;
    for (const model of getModelFallbackChain()) {
      try {
        const payload = chunk
          .map((event, idx) => `${idx + 1}. ${event.type} on ${event.repo_full_name}: ${event.title}`)
          .join('\n');
        const request = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${OPENAI_API_KEY}`,
          },
          body: JSON.stringify({
            model: formatSafeModelName(model),
            temperature: 0.2,
            max_tokens: 180,
            messages: [
              {
                role: 'system',
                content:
                  'You summarize software contribution events for a dashboard. Return compact one sentence summaries.',
              },
              {
                role: 'user',
                content: `Summarize each event below in one short sentence (max 18 words). Return strict JSON only as an array with objects {"index": number, "summary": string}. Use event order exactly as provided.\n\n${payload}`,
              },
            ],
          }),
        });

        if (!request.ok) {
          const errBody = await request.text();
          throw new Error(`OpenAI ${request.status}: ${errBody}`);
        }
        const result = (await request.json()) as {
          choices?: Array<{ message?: { content?: string } }>;
          usage?: { prompt_tokens?: number; completion_tokens?: number };
        };
        const content = result.choices?.[0]?.message?.content?.trim() || '';
        if (!content) {
          throw new Error('OpenAI returned empty content');
        }
        const parsed = JSON.parse(content);
        if (!Array.isArray(parsed)) {
          throw new Error('OpenAI returned non-array response');
        }
        for (const row of parsed) {
          const idx = typeof row?.index === 'number' ? row.index - 1 : -1;
          const summary = coalesceTrimmed(row?.summary, '');
          if (idx >= 0 && idx < chunk.length && summary) {
            summaries[chunk[idx].id] = summary;
          }
        }
        break;
      } catch (error) {
        attemptError = error instanceof Error ? error.message : String(error);
      }
    }
    if (attemptError) {
      for (const event of chunk) {
        summaries[event.id] = event.type === 'merged_pr'
          ? `Merged PR in ${event.repo_name} by ${event.actor_login}.`
          : `Commit by ${event.actor_login} in ${event.repo_name}.`;
      }
    }
  }

  return summaries;
}

async function fetchHistoricalDevActivity(): Promise<HistoricalDevActivity> {
  try {
    const historyUrl = new URL(NETWORK_DATA_URL);
    historyUrl.searchParams.set('history_ts', Date.now().toString());

    const response = await fetch(historyUrl.toString(), { cache: 'no-store' });
    if (!response.ok) {
      return { events: [], contributors: [], spend: null, generatedAt: '' };
    }

    const payload = (await response.json()) as {
      dev_activity?: {
        events?: unknown;
        contributors?: unknown;
        spend_monitor?: unknown;
      };
      metadata?: { generated_at?: string };
    };

    const historicalEvents = Array.isArray(payload?.dev_activity?.events)
      ? (payload.dev_activity.events as DevContributionEvent[])
      : [];
    const historicalContributors = Array.isArray(payload?.dev_activity?.contributors)
      ? (payload.dev_activity.contributors as DevContributor[])
      : [];
    const spend =
      payload?.dev_activity?.spend_monitor && typeof payload.dev_activity.spend_monitor === 'object'
        ? (payload.dev_activity.spend_monitor as DevFeedSpend)
        : null;
    const generatedAt = coalesceTrimmed(payload?.metadata?.generated_at, '');

    return {
      events: historicalEvents.filter((event) => {
        if (!event || typeof event.id !== 'string' || !event.occurred_at) {
          return false;
        }
        return true;
      }),
      contributors: historicalContributors.filter((contributor) => typeof contributor.github_login === 'string'),
      spend,
      generatedAt,
    };
  } catch {
    return { events: [], contributors: [], spend: null, generatedAt: '' };
  }
}

async function fetchPostfiatorgRepos(): Promise<string[]> {
  try {
    const repos: string[] = [];
    let page = 1;
    while (page <= 2) {
      const path = new URL(`${GITHUB_API_BASE}/orgs/${GITHUB_ORG}/repos`);
      path.searchParams.set('per_page', '100');
      path.searchParams.set('page', String(page));
      path.searchParams.set('type', 'all');

      const response = await githubGet<unknown[]>(path.toString());
      if (!response.ok) {
        break;
      }
      const batch = response.data;
      if (!Array.isArray(batch) || batch.length === 0) {
        break;
      }
      for (const repo of batch) {
        const repoName = coalesceTrimmed((repo as { full_name?: string }).full_name, '');
        if (!repoName) continue;
        repos.push(repoName);
      }
      if (batch.length < 100) break;
      page += 1;
    }
    return repos;
  } catch {
    return [];
  }
}

function toCommitEvent(
  repoFullName: string,
  item: GitHubSearchCommitItem,
  isPostfiatorg: boolean,
): DevContributionEvent | null {
  const sha = coalesceTrimmed(item.sha, '');
  const occurredAt = coalesceTrimmed(item.commit?.author?.date || item.commit?.committer?.date, '');
  const url = coalesceTrimmed(item.html_url, '');
  const actorLogin = coalesceTrimmed(item.author?.login, 'unknown');
  const message = coalesceTrimmed(item.commit?.message, '');

  if (!sha || !occurredAt || !url || !actorLogin) {
    return null;
  }
  const title = sanitizeCommitMessage(message);
  const { owner, name } = parseRepoOwnerAndName(repoFullName);

  return {
    id: `commit:${repoFullName}:${sha}`,
    type: 'commit',
    occurred_at: formatEventTime(occurredAt),
    actor_login: actorLogin,
    repo_full_name: repoFullName,
    repo_owner: owner,
    repo_name: name,
    title,
    summary: title,
    summary_is_llm_generated: false,
    url,
    sha,
    is_postfiatorg_repo: isPostfiatorg,
  };
}

function toPullRequestEvent(
  repoFullName: string,
  item: GitHubPull,
  isPostfiatorg: boolean,
): DevContributionEvent | null {
  const mergedAt = coalesceTrimmed(item.merged_at, '');
  const url = coalesceTrimmed(item.html_url, '');
  const actorLogin = coalesceTrimmed(item.merged_by?.login || item.user?.login, 'unknown');
  const title = coalesceTrimmed(item.title, '');
  const number = item.number;

  if (!mergedAt || !url || !actorLogin) {
    return null;
  }
  const summaryFallback = `Merged PR #${number || ''} in ${parseRepoOwnerAndName(repoFullName).name}`;
  const { owner, name } = parseRepoOwnerAndName(repoFullName);
  return {
    id: `pull:${repoFullName}:${number || item.title}`,
    type: 'merged_pr',
    occurred_at: formatEventTime(mergedAt),
    actor_login: actorLogin,
    repo_full_name: repoFullName,
    repo_owner: owner,
    repo_name: name,
    title: title || `PR #${number || 'merged'}`,
    summary: summaryFallback,
    summary_is_llm_generated: false,
    url,
    pr_number: number,
    is_postfiatorg_repo: isPostfiatorg,
  };
}

async function fetchCommitsForRepo(
  repoFullName: string,
  since: string,
  maxPages = 2
): Promise<DevContributionEvent[]> {
  const events: DevContributionEvent[] = [];
  const isPostfiatorg = isPostfiatorgRepo(repoFullName);

  for (let page = 1; page <= maxPages; page += 1) {
    const path = new URL(`${GITHUB_API_BASE}/repos/${repoFullName}/commits`);
    path.searchParams.set('per_page', '100');
    path.searchParams.set('page', String(page));
    path.searchParams.set('since', since);

    const response = await githubGet<unknown[]>(path.toString());
    if (!response.ok) return events;
    if (!Array.isArray(response.data)) return events;

    for (const row of response.data) {
      const event = toCommitEvent(repoFullName, row as GitHubSearchCommitItem, isPostfiatorg);
      if (event) {
        events.push(event);
      }
    }
    if (response.data.length < 100) {
      break;
    }
  }

  return events;
}

async function fetchMergedPullsForRepo(
  repoFullName: string,
  since: string,
  maxPages = 2
): Promise<DevContributionEvent[]> {
  const events: DevContributionEvent[] = [];
  const isPostfiatorg = isPostfiatorgRepo(repoFullName);
  const sinceTs = parseDateMs(since);

  for (let page = 1; page <= maxPages; page += 1) {
    const path = new URL(`${GITHUB_API_BASE}/repos/${repoFullName}/pulls`);
    path.searchParams.set('state', 'closed');
    path.searchParams.set('sort', 'updated');
    path.searchParams.set('direction', 'desc');
    path.searchParams.set('per_page', '100');
    path.searchParams.set('page', String(page));

    const response = await githubGet<unknown[]>(path.toString());
    if (!response.ok) return events;
    if (!Array.isArray(response.data)) return events;

    for (const row of response.data) {
      const mergedAt = coalesceTrimmed((row as GitHubPull).merged_at, '');
      if (!mergedAt || parseDateMs(mergedAt) < sinceTs) {
        continue;
      }
      const event = toPullRequestEvent(repoFullName, row as GitHubPull, isPostfiatorg);
      if (event) {
        events.push(event);
      }
    }
    if (response.data.length < 100) {
      break;
    }
  }

  return events;
}

async function discoverContributorRepos(login: string, since: string): Promise<string[]> {
  if (!login || login === 'unknown') return [];
  const repos = new Set<string>();
  const q = `author:${login} committer-date:>${since.slice(0, 10)}`;
  const path = new URL(`${GITHUB_API_BASE}/search/commits`);
  path.searchParams.set('q', q);
  path.searchParams.set('sort', 'committer-date');
  path.searchParams.set('order', 'desc');
  path.searchParams.set('per_page', '100');

  const response = await githubGet<{ items?: GitHubSearchCommitItem[] }>(path.toString());
  if (!response.ok || !response.data?.items) {
    return [];
  }

  for (const row of response.data.items) {
    const repoName = coalesceTrimmed(row.repository?.full_name, '');
    if (repoName && !isPostfiatorgRepo(repoName)) {
      repos.add(repoName);
    }
  }

  return Array.from(repos);
}

function buildDevActivityStats(
  rows: DevContributionEvent[],
  lookbackTs: number
): DevFeedStats {
  const recent = rows.filter((row) => parseDateMs(row.occurred_at) >= lookbackTs);
  const uniqueContributors = new Set<string>(recent.map((row) => row.actor_login));
  let externalCount = 0;
  for (const row of recent) {
    if (!row.is_postfiatorg_repo) {
      externalCount += 1;
    }
  }
  return {
    total_events_7d: recent.length,
    postfiatorg_events_7d: recent.length - externalCount,
    external_repo_events_7d: externalCount,
    unique_contributors_7d: uniqueContributors.size,
  };
}

function normalizeDailyActivity(input: unknown): DailyActivity[] {
  if (!Array.isArray(input)) {
    return [];
  }

  const daily: DailyActivity[] = [];
  for (const entry of input) {
    if (!entry || typeof entry !== 'object') {
      continue;
    }
    const row = entry as { date?: unknown; pft?: unknown; tx_count?: unknown };
    if (typeof row.date !== 'string' || !ISO_DATE_RE.test(row.date)) {
      continue;
    }

    const pftRaw = typeof row.pft === 'number' ? row.pft : Number(row.pft);
    const txRaw = typeof row.tx_count === 'number' ? row.tx_count : Number(row.tx_count);
    if (!Number.isFinite(pftRaw) || !Number.isFinite(txRaw)) {
      continue;
    }

    daily.push({
      date: row.date,
      pft: round(Math.max(0, pftRaw)),
      tx_count: Math.max(0, Math.round(txRaw)),
    });
  }

  return daily.sort((a, b) => a.date.localeCompare(b.date));
}

function mergeDailyActivityHistory(existing: DailyActivity[], latest: DailyActivity[]): DailyActivity[] {
  const merged = new Map<string, { pft: number; tx_count: number }>();

  for (const d of existing) {
    merged.set(d.date, { pft: d.pft, tx_count: d.tx_count });
  }

  for (const d of latest) {
    // For any date present in the latest chain scan, trust latest values.
    // This allows same-day corrections and avoids permanently inflating days
    // when earlier scans had temporary overcounts.
    merged.set(d.date, { pft: round(d.pft), tx_count: d.tx_count });
  }

  return Array.from(merged.entries())
    .map(([date, stats]) => ({ date, pft: round(stats.pft), tx_count: stats.tx_count }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function fillDailyActivityGaps(rows: DailyActivity[]): DailyActivity[] {
  if (rows.length === 0) {
    return [];
  }

  const sorted = rows.slice().sort((a, b) => a.date.localeCompare(b.date));
  const byDate = new Map(sorted.map((row) => [row.date, row]));
  const start = new Date(`${sorted[0].date}T00:00:00.000Z`).getTime();
  const end = new Date(`${sorted[sorted.length - 1].date}T00:00:00.000Z`).getTime();
  const filled: DailyActivity[] = [];

  for (let ts = start; ts <= end; ts += DAY_MS) {
    const date = new Date(ts).toISOString().slice(0, 10);
    const row = byDate.get(date);
    filled.push(
      row || {
        date,
        pft: 0,
        tx_count: 0,
      }
    );
  }

  return filled;
}

function limitAndSortEvents(events: DevContributionEvent[]): DevContributionEvent[] {
  const rows = dedupeAndSort(events);
  rows.sort((a, b) => {
    const aTime = parseDateMs(a.occurred_at);
    const bTime = parseDateMs(b.occurred_at);
    return bTime - aTime;
  });
  return rows.slice(0, normalizePositiveInt(DEV_ACTIVITY_MAX_EVENTS, 120, 9999));
}

async function collectDevContributionFeed(): Promise<{
  events: DevContributionEvent[];
  contributors: DevContributor[];
  metadata: {
    orgRepos: number;
    externalRepos: number;
    activeContributors: number;
    failures: string[];
    eventCount: number;
  };
  spend: {
    run_estimated_usd: number;
    run_event_count: number;
  };
}> {
  const failures: string[] = [];
  const activeContributors = new Set<string>();
  const contributorMap = new Map<string, DevContributor>();

  const lookbackIso7 = dateToIsoSeconds(dateDaysAgo(DEV_ACTIVITY_LOOKBACK_DAYS));
  const lookbackIso30 = dateToIsoSeconds(dateDaysAgo(DEV_ACTIVITY_ACTIVE_CONTRIBUTOR_DAYS));
  const lookbackTs7 = parseDateMs(lookbackIso7);
  const lookbackTs30 = parseDateMs(lookbackIso30);
  const keepThresholdTs = parseDateMs(dateToIsoSeconds(dateDaysAgo(DEV_ACTIVITY_KEEP_DAYS)));
  const today = new Date().toISOString().slice(0, 10);

  const orgRepos = await fetchPostfiatorgRepos();
  if (orgRepos.length === 0) {
    failures.push('No postfiatorg repos discovered');
  }

  const orgEvents: DevContributionEvent[] = [];
  for (const repo of orgRepos) {
    try {
      const commits30 = await fetchCommitsForRepo(repo, lookbackIso30);
      const prs30 = await fetchMergedPullsForRepo(repo, lookbackIso30);
      const snapshot = [...commits30, ...prs30];
      snapshot.forEach((row) => {
        if (parseDateMs(row.occurred_at) >= lookbackTs30) {
          activeContributors.add(row.actor_login);
          contributorMap.set(row.actor_login, {
            github_login: row.actor_login,
            source: 'postfiatorg_recent_commit_or_pr',
            last_seen_at: row.occurred_at,
          });
        }
        if (parseDateMs(row.occurred_at) >= lookbackTs7) {
          orgEvents.push(row);
        }
      });
    } catch (error) {
      failures.push(`org repo fetch failed: ${repo}`);
      if (process.env.NODE_ENV !== 'production') {
        console.warn(`org repo fetch failed: ${repo}`, error);
      }
    }
  }

  const externalRepoSet = new Set<string>();
  for (const contributor of Array.from(activeContributors).slice(0, DEV_ACTIVITY_MAX_REPOS)) {
    try {
      const discovered = await discoverContributorRepos(contributor, lookbackIso30);
      for (const repo of discovered) {
        if (!repo.startsWith(`${GITHUB_ORG}/`)) {
          externalRepoSet.add(repo);
          if (externalRepoSet.size >= DEV_ACTIVITY_MAX_REPOS) {
            break;
          }
        }
      }
    } catch (error) {
      failures.push(`discover repos failed for ${contributor}`);
      if (process.env.NODE_ENV !== 'production') {
        console.warn(`discover repos failed for ${contributor}`, error);
      }
    }
  }

  const externalEvents: DevContributionEvent[] = [];
  for (const repo of externalRepoSet) {
    try {
      const commits7 = await fetchCommitsForRepo(repo, lookbackIso7);
      const prs7 = await fetchMergedPullsForRepo(repo, lookbackIso7);
      const rowset = [...commits7, ...prs7];
      rowset.forEach((row) => {
        if (parseDateMs(row.occurred_at) >= lookbackTs7) {
          externalEvents.push(row);
        }
      });
    } catch (error) {
      failures.push(`external repo fetch failed: ${repo}`);
      if (process.env.NODE_ENV !== 'production') {
        console.warn(`external repo fetch failed: ${repo}`, error);
      }
    }
  }

  const historical = await fetchHistoricalDevActivity();
  const priorEvents = (historical.events || []).filter((event) =>
    parseDateMs(event.occurred_at) >= keepThresholdTs
  );
  const knownSummaries = new Map<string, string>();
  const knownSummaryGenerated = new Map<string, boolean>();
  for (const cached of priorEvents) {
    if (cached?.id && typeof cached.summary === 'string' && cached.summary.length) {
      knownSummaries.set(cached.id, cached.summary);
      if (typeof cached.summary_is_llm_generated === 'boolean') {
        knownSummaryGenerated.set(cached.id, cached.summary_is_llm_generated);
      }
    }
  }

  const combinedEvents = limitAndSortEvents([...priorEvents, ...orgEvents, ...externalEvents]);
  const eventsWindow = combinedEvents.filter((event) => parseDateMs(event.occurred_at) >= lookbackTs7);
  const contributors = Array.from(contributorMap.values())
    .filter((value) => value.github_login !== 'unknown')
    .sort((a, b) => (a.github_login > b.github_login ? 1 : -1));

  const eventsNeedingSummary: DevContributionEvent[] = [];
  for (const event of eventsWindow) {
    const existingSummary = knownSummaries.get(event.id);
    if (!existingSummary || existingSummary === event.summary || existingSummary === event.title) {
      eventsNeedingSummary.push(event);
    }
    if (existingSummary) {
      event.summary = existingSummary;
      if (knownSummaryGenerated.has(event.id)) {
        event.summary_is_llm_generated = knownSummaryGenerated.get(event.id);
      }
    } else if (!event.summary) {
      event.summary = event.title;
      event.summary_is_llm_generated = false;
    }
  }

  let totalRunCost = 0;
  if (eventsNeedingSummary.length > 0 && OPENAI_API_KEY) {
    try {
      const summaryMap = await openaiSummarizeBatch(eventsNeedingSummary.slice(0, 20));
      eventsNeedingSummary.forEach((event) => {
        const mapped = summaryMap[event.id];
        if (mapped) {
          event.summary = mapped;
          knownSummaries.set(event.id, mapped);
          event.summary_is_llm_generated = true;
          knownSummaryGenerated.set(event.id, true);
        }
      });
      const estimatedInput = eventsNeedingSummary.slice(0, 20).reduce(
        (sum, event) =>
          sum + estimateTextTokens(`commit ${event.repo_full_name} ${event.title} ${event.actor_login}`),
        0
      );
      const estimatedOutput = eventsNeedingSummary.slice(0, 20).reduce(
        (sum) => sum + 16,
        0
      );
      const model = formatSafeModelName(DEV_ACTIVITY_SUMMARY_FALLBACK_MODELS[0]);
      totalRunCost = estimateSummaryCostUsd(estimatedInput, estimatedOutput, model);
    } catch (error) {
      failures.push(`openai summarize failed: ${error instanceof Error ? error.message : String(error)}`);
      if (process.env.NODE_ENV !== 'production') {
        console.warn('openai summarize failed', error);
      }
      for (const event of eventsNeedingSummary) {
        event.summary = event.type === 'merged_pr'
          ? `Merged PR in ${event.repo_name} by ${event.actor_login}.`
          : `Commit in ${event.repo_name} by ${event.actor_login}.`;
        knownSummaries.set(event.id, event.summary);
        event.summary_is_llm_generated = false;
        knownSummaryGenerated.set(event.id, false);
      }
    }
  } else {
    for (const event of eventsWindow) {
      event.summary = event.summary || (event.type === 'merged_pr'
        ? `Merged PR in ${event.repo_name} by ${event.actor_login}.`
        : `Commit in ${event.repo_name} by ${event.actor_login}.`);
      event.summary_is_llm_generated = false;
      knownSummaryGenerated.set(event.id, false);
    }
  }

  const sorted = limitAndSortEvents(eventsWindow);
  const todaySpend = historical.generatedAt.startsWith(today)
    ? historical.spend?.estimated_usd_today ?? 0
    : 0;
  const runSummaries = eventsNeedingSummary.length;
  const estimatedUsdToday = round(todaySpend + totalRunCost, 6);
  if (estimatedUsdToday > DEV_ACTIVITY_LLM_DAILY_ALERT_USD) {
    failures.push(
      `llm-cost-threshold-exceeded-${new Date().toISOString()}: estimated ${estimatedUsdToday} > ${DEV_ACTIVITY_LLM_DAILY_ALERT_USD}`
    );
  }

  return {
    events: sorted,
    contributors,
    metadata: {
      orgRepos: orgRepos.length,
      externalRepos: externalRepoSet.size,
      activeContributors: contributors.length,
      failures,
      eventCount: sorted.length,
    },
    spend: {
      run_estimated_usd: round(totalRunCost, 6),
      run_event_count: runSummaries,
    },
  };
}

async function fetchHistoricalDailyActivity(): Promise<DailyActivity[]> {
  try {
    const historyUrl = new URL(NETWORK_DATA_URL);
    historyUrl.searchParams.set('history_ts', Date.now().toString());

    const response = await fetch(historyUrl.toString(), { cache: 'no-store' });
    if (!response.ok) {
      return [];
    }

    const payload = (await response.json()) as { rewards?: { daily_activity?: unknown } };
    return normalizeDailyActivity(payload?.rewards?.daily_activity);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`Failed to fetch historical daily activity: ${message}`);
    return [];
  }
}

async function fetchHistoricalNonTaskDailyActivity(): Promise<DailyActivity[]> {
  try {
    const historyUrl = new URL(NETWORK_DATA_URL);
    historyUrl.searchParams.set('history_ts', Date.now().toString());

    const response = await fetch(historyUrl.toString(), { cache: 'no-store' });
    if (!response.ok) {
      return [];
    }

    const payload = (await response.json()) as { non_task_distributions?: { daily_activity?: unknown } };
    return normalizeDailyActivity(payload?.non_task_distributions?.daily_activity);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`Failed to fetch historical non-task daily activity: ${message}`);
    return [];
  }
}

async function fetchOfficialMonthlyRewards(): Promise<Map<string, number>> {
  const result = new Map<string, number>();
  try {
    const headers: Record<string, string> = {
      accept: 'application/json',
    };
    if (TASKNODE_JWT) {
      headers.authorization = `Bearer ${TASKNODE_JWT}`;
    }

    const response = await fetch(OFFICIAL_LEADERBOARD_URL, {
      headers,
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const payload = (await response.json()) as { rows?: Array<Record<string, unknown>> };
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    for (const row of rows) {
      const address = row.wallet_address;
      const monthlyRaw = row.monthly_rewards;
      if (typeof address !== 'string' || address.length === 0) {
        continue;
      }
      const monthly =
        typeof monthlyRaw === 'number'
          ? monthlyRaw
          : typeof monthlyRaw === 'string'
            ? Number(monthlyRaw)
            : NaN;
      if (!Number.isFinite(monthly)) {
        continue;
      }
      result.set(address, round(monthly));
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const hint = TASKNODE_JWT ? '' : ' (set PFT_TASKNODE_JWT to enable auth)';
    console.warn(`Failed to fetch official leaderboard: ${message}${hint}`);
  }
  return result;
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

async function discoverBehaviorRelayWallets(
  client: Client,
  memoTxs: TxWrapper[],
  knownRewardSenders: string[]
): Promise<{ scanned_candidates: number; matches: RelayBehaviorMatch[] }> {
  const fundedByMemo = new Map<string, { total_pft: number; last_funded_ripple: number }>();
  const known = new Set([
    ...knownRewardSenders,
    ...TREASURY_WALLETS,
    MEMO_ADDRESS,
    'rrrrrrrrrrrrrrrrrrrrrhoLvTp',
  ]);

  for (const txWrapper of memoTxs) {
    const tx = getTxData(txWrapper);
    if (!tx) continue;
    if (tx.TransactionType !== 'Payment') continue;
    if (tx.Account !== MEMO_ADDRESS) continue;

    const pft = parsePftAmount(tx.DeliverMax) ?? parsePftAmount(tx.Amount);
    if (pft === null || pft <= 0) continue;

    const recipient = tx.Destination || '';
    if (!recipient || known.has(recipient)) continue;

    const prev = fundedByMemo.get(recipient) || { total_pft: 0, last_funded_ripple: 0 };
    fundedByMemo.set(recipient, {
      total_pft: prev.total_pft + pft,
      last_funded_ripple: Math.max(prev.last_funded_ripple, tx.date || 0),
    });
  }

  const candidates = Array.from(fundedByMemo.entries())
    .filter(([, stats]) => stats.total_pft >= RELAY_BEHAVIOR_MIN_MEMO_FUNDING_PFT)
    .sort((a, b) => {
      if (b[1].total_pft !== a[1].total_pft) return b[1].total_pft - a[1].total_pft;
      return b[1].last_funded_ripple - a[1].last_funded_ripple;
    })
    .slice(0, RELAY_BEHAVIOR_CANDIDATE_SCAN_LIMIT)
    .map(([address]) => address);

  const lookbackSeconds = RELAY_BEHAVIOR_LOOKBACK_DAYS * 24 * 60 * 60;
  const nowRipple = Math.floor(Date.now() / 1000) - RIPPLE_EPOCH;
  const cutoffRipple = nowRipple - lookbackSeconds;
  const nonRewardSystemAccounts = new Set([
    ...knownRewardSenders,
    ...TREASURY_WALLETS,
    MEMO_ADDRESS,
    'rrrrrrrrrrrrrrrrrrrrrhoLvTp',
  ]);

  const matches: RelayBehaviorMatch[] = [];

  for (const address of candidates) {
    const txs = await fetchAllAccountTx(client, address, RELAY_BEHAVIOR_TX_FETCH_LIMIT);
    let ptrTxCount = 0;
    let totalPft = 0;
    let lastPtrRipple = 0;
    const uniqueRecipients = new Set<string>();

    for (const txWrapper of txs) {
      const tx = getTxData(txWrapper);
      if (!tx) continue;
      if (tx.TransactionType !== 'Payment') continue;
      if (tx.Account !== address) continue;
      if (!tx.date || tx.date < cutoffRipple) continue;

      const pft = parsePftAmount(tx.DeliverMax) ?? parsePftAmount(tx.Amount);
      if (pft === null || pft <= 0) continue;
      if (!hasPfPtrMemo(tx)) continue;

      const recipient = tx.Destination || '';
      if (!recipient || recipient === address) continue;
      if (nonRewardSystemAccounts.has(recipient)) continue;

      ptrTxCount += 1;
      totalPft += pft;
      uniqueRecipients.add(recipient);
      lastPtrRipple = Math.max(lastPtrRipple, tx.date);
    }

    if (
      ptrTxCount >= RELAY_BEHAVIOR_MIN_PTR_TXS &&
      uniqueRecipients.size >= RELAY_BEHAVIOR_MIN_UNIQUE_RECIPIENTS &&
      totalPft >= RELAY_BEHAVIOR_MIN_TOTAL_PFT
    ) {
      matches.push({
        address,
        ptr_tx_count: ptrTxCount,
        unique_recipients: uniqueRecipients.size,
        total_pft: round(totalPft),
        last_ptr_reward_date: lastPtrRipple ? formatDate(unixFromRipple(lastPtrRipple)) : null,
        memo_funding_total_pft: round(fundedByMemo.get(address)?.total_pft || 0),
      });
    }
  }

  matches.sort((a, b) => {
    if (b.ptr_tx_count !== a.ptr_tx_count) return b.ptr_tx_count - a.ptr_tx_count;
    if (b.unique_recipients !== a.unique_recipients) return b.unique_recipients - a.unique_recipients;
    return b.total_pft - a.total_pft;
  });

  return {
    scanned_candidates: candidates.length,
    matches,
  };
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
  const nonTaskByDay = new Map<string, number>();
  const nonTaskTxCountByDay = new Map<string, number>();
  const seenRewardHashes = new Set<string>();
  let totalPft = 0;
  let excludedNonPtrPft = 0;
  let excludedNonPtrTxs = 0;
  const rewardList: RewardEntry[] = [];
  const nonTaskList: RewardEntry[] = [];
  
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

    const rewardHash =
      tx.hash ||
      txWrapper.hash ||
      `${tx.Account}-${recipient}-${tx.date || 0}-${tx.DeliverMax || tx.Amount || 'unknown'}`;
    if (seenRewardHashes.has(rewardHash)) continue;
    seenRewardHashes.add(rewardHash);

    // Get timestamp
    const closeTime = tx.date || 0;
    const unixTs = closeTime ? unixFromRipple(closeTime) : 0;
    const day = unixTs ? formatDate(unixTs) : 'unknown';

    if (!hasPfPtrMemo(tx)) {
      excludedNonPtrPft += pft;
      excludedNonPtrTxs += 1;
      nonTaskByDay.set(day, (nonTaskByDay.get(day) || 0) + pft);
      nonTaskTxCountByDay.set(day, (nonTaskTxCountByDay.get(day) || 0) + 1);
      nonTaskList.push({
        hash: rewardHash,
        recipient,
        pft,
        timestamp: unixTs,
        date: day,
      });
      continue;
    }

    // Aggregate
    participants.add(recipient);
    rewardsByRecipient.set(recipient, (rewardsByRecipient.get(recipient) || 0) + pft);
    rewardsByDay.set(day, (rewardsByDay.get(day) || 0) + pft);
    txCountByDay.set(day, (txCountByDay.get(day) || 0) + 1);
    totalPft += pft;

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
  const nonTaskDailyActivity: DailyActivity[] = Array.from(nonTaskByDay.entries())
    .map(([date, pft]) => ({
      date,
      pft: round(pft),
      tx_count: nonTaskTxCountByDay.get(date) || 0,
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
    excluded_non_ptr_reward_txs: excludedNonPtrTxs,
    excluded_non_ptr_reward_pft: round(excludedNonPtrPft),
    non_task_daily_activity: nonTaskDailyActivity,
    non_task_recent_distributions: nonTaskList.slice(0, 50),
  };
}

// Analyze memo transactions (task submissions)
function analyzeMemoTransactions(txs: TxWrapper[]): SubmissionsAnalysisInternal {
  const submitters = new Set<string>();
  const submissionsBySender = new Map<string, number>();
  const submissionsByDay = new Map<string, number>();
  const seenSubmissionHashes = new Set<string>();
  let totalSubmissions = 0;
  const submissionList: SubmissionEntry[] = [];

  for (const txWrapper of txs) {
    const tx = getTxData(txWrapper);
    if (!tx) continue;

    // Only incoming payments to memo address
    if (tx.TransactionType !== 'Payment') continue;
    if (tx.Destination !== MEMO_ADDRESS) continue;

    const submissionHash = tx.hash || txWrapper.hash || '';
    if (submissionHash && seenSubmissionHashes.has(submissionHash)) continue;
    if (submissionHash) seenSubmissionHashes.add(submissionHash);

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
      hash: submissionHash,
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
  chainDailyActivity: DailyActivity[],
  officialMonthlyRewards: Map<string, number>,
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
    ...officialMonthlyRewards.keys(),
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
      let trackedTotalPft: number;
      if (bl) {
        const balanceDelta = Math.max(0, balance - bl.balance);
        trackedTotalPft = round(bl.total_pft + balanceDelta);
      } else {
        // New user: use higher of scanner-detected or balance
        const scannerTotal = postResetRewards.rewards_by_recipient.get(address) || 0;
        trackedTotalPft = round(Math.max(scannerTotal, balance));
      }
      const officialTotalPft = officialMonthlyRewards.get(address);
      const totalPft = officialTotalPft !== undefined ? round(officialTotalPft) : trackedTotalPft;
      return { address, total_pft: totalPft, balance };
    })
    .sort((a, b) => b.balance !== a.balance ? b.balance - a.balance : b.total_pft - a.total_pft)
    .slice(0, 25);

  // --- Daily activity merge ---
  const dailyActivityMap = new Map<string, { pft: number; tx_count: number }>();
  for (const d of baselineData.rewards.daily_activity) {
    dailyActivityMap.set(d.date, { pft: d.pft, tx_count: d.tx_count });
  }
  for (const d of chainDailyActivity) {
    const existing = dailyActivityMap.get(d.date);
    if (existing) {
      dailyActivityMap.set(d.date, {
        pft: Math.max(existing.pft, d.pft),
        tx_count: Math.max(existing.tx_count, d.tx_count),
      });
    } else {
      dailyActivityMap.set(d.date, { pft: d.pft, tx_count: d.tx_count });
    }
  }
  const mergedDailyActivity: DailyActivity[] = Array.from(dailyActivityMap.entries())
    .map(([date, { pft, tx_count }]) => ({ date, pft: round(pft), tx_count }))
    .sort((a, b) => a.date.localeCompare(b.date));
  const daily_activity = fillDailyActivityGaps(mergedDailyActivity);

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
    const officialTotal = officialMonthlyRewards.get(addr);
    if (officialTotal !== undefined) {
      totalPftDistributed += officialTotal;
    } else {
      const balance = mergedBalances.get(addr) || 0;
      const bl = baselineLookup.get(addr);
      if (bl) {
        totalPftDistributed += bl.total_pft + Math.max(0, balance - bl.balance);
      } else {
        const scannerTotal = postResetRewards.rewards_by_recipient.get(addr) || 0;
        totalPftDistributed += Math.max(scannerTotal, balance);
      }
    }
  }
  totalPftDistributed = round(totalPftDistributed);
  const uniqueEarnersEstimate = baselineData.network_totals.unique_earners + postResetRewards.unique_recipients - earnerOverlap;
  const uniqueEarners = Math.max(uniqueEarnersEstimate, officialMonthlyRewards.size);
  const totalRewardsPaid = daily_activity.reduce((sum, day) => sum + day.tx_count, 0);
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
  let connectedEndpoint = RPC_WS_URL;

  try {
    // Connect to XRPL and measure latency
    const connectStart = Date.now();
    const connection = await connectClientWithFallback();
    client = connection.client;
    connectedEndpoint = connection.endpoint;
    const wsLatencyMs = Date.now() - connectStart;

    // Get current ledger index for liveness indicator
    const ledgerResponse = await client.request({ command: 'ledger_current' });
    const ledgerIndex = ledgerResponse.result.ledger_current_index;

    // Fetch server info for ledger close time
    const serverInfo = await client.request({ command: 'server_info' });
    const validatedLedger = serverInfo.result.info.validated_ledger as
      (typeof serverInfo.result.info.validated_ledger & { close_time?: number }) | undefined;
    const closeTimeRipple = validatedLedger?.close_time;
    const ledgerCloseTime = closeTimeRipple
      ? new Date((closeTimeRipple + RIPPLE_EPOCH) * 1000).toISOString()
      : new Date().toISOString();
    const ledgerCloseUnix = closeTimeRipple
      ? closeTimeRipple + RIPPLE_EPOCH
      : Math.floor(Date.now() / 1000);
    const secondsSinceClose = Math.floor(Date.now() / 1000) - ledgerCloseUnix;

    // Fetch memo transactions first (needed for relay discovery)
    const memoTxs = await fetchAllAccountTx(client, MEMO_ADDRESS, 5000);
    
    // Dynamically discover relay wallets funded by memo address
    const fundingRelayWallets = await discoverRelayWallets(memoTxs);
    const baseRewardAddresses = Array.from(
      new Set([...PRIMARY_REWARD_ADDRESSES, ...KNOWN_REWARD_RELAYS, ...fundingRelayWallets])
    );
    const behaviorRelayDiscovery = await discoverBehaviorRelayWallets(
      client,
      memoTxs,
      baseRewardAddresses
    );
    const behaviorRelayWallets = behaviorRelayDiscovery.matches.map((m) => m.address);
    const allRewardAddresses = Array.from(
      new Set([...baseRewardAddresses, ...behaviorRelayWallets])
    );
    
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
    const historicalDailyActivity = await fetchHistoricalDailyActivity();
    const chainDailyActivity = mergeDailyActivityHistory(
      historicalDailyActivity,
      rewardsInternal.daily_activity
    );
    const historicalNonTaskDailyActivity = await fetchHistoricalNonTaskDailyActivity();
    const chainNonTaskDailyActivity = mergeDailyActivityHistory(
      historicalNonTaskDailyActivity,
      rewardsInternal.non_task_daily_activity
    );
    const nonTaskTotalPft = round(
      chainNonTaskDailyActivity.reduce((sum, day) => sum + day.pft, 0)
    );
    const nonTaskTotalTxs = chainNonTaskDailyActivity.reduce((sum, day) => sum + day.tx_count, 0);
    const officialMonthlyRewards = await fetchOfficialMonthlyRewards();
    const devFeed = await collectDevContributionFeed();
    const todaysSpend = devFeed.spend.run_estimated_usd;
    const spendMonitor: DevFeedSpend = {
      estimated_usd_today: devFeed.spend.run_estimated_usd,
      threshold_usd: DEV_ACTIVITY_LLM_DAILY_ALERT_USD,
      threshold_exceeded: todaysSpend >= DEV_ACTIVITY_LLM_DAILY_ALERT_USD,
      last_alert_at: todaysSpend >= DEV_ACTIVITY_LLM_DAILY_ALERT_USD ? new Date().toISOString() : undefined,
      run_estimated_usd: devFeed.spend.run_estimated_usd,
      run_event_count: devFeed.spend.run_event_count,
    };

    // Merge pre-reset baseline with post-reset live data
    const merged = await mergeWithBaseline(
      client,
      rewardsInternal,
      submissionsInternal,
      taskLifecycle,
      chainDailyActivity,
      officialMonthlyRewards
    );

    // Combine into final analytics object
    const analytics: NetworkAnalytics = {
      metadata: {
        generated_at: new Date().toISOString(),
        ledger_index: ledgerIndex,
        reward_addresses: allRewardAddresses,
        memo_address: MEMO_ADDRESS,
        reward_txs_fetched: rewardTxs.length,
        memo_txs_fetched: memoTxs.length,
        official_leaderboard_rows: officialMonthlyRewards.size,
        historical_daily_rows: chainDailyActivity.length,
        excluded_non_ptr_reward_txs: nonTaskTotalTxs,
        excluded_non_ptr_reward_pft: nonTaskTotalPft,
        relay_wallets_discovered_funding: fundingRelayWallets,
        relay_wallets_discovered_behavior: behaviorRelayWallets,
        relay_behavior_candidates_scanned: behaviorRelayDiscovery.scanned_candidates,
        relay_behavior_lookback_days: RELAY_BEHAVIOR_LOOKBACK_DAYS,
        relay_behavior_matches: behaviorRelayDiscovery.matches,
        github_repos_scanned: devFeed.metadata.orgRepos,
        github_external_repos_scanned: devFeed.metadata.externalRepos,
        github_active_contributors_scanned: devFeed.metadata.activeContributors,
        github_events_collected: devFeed.metadata.eventCount,
        github_error_count: devFeed.metadata.failures.length,
        github_request_failures: devFeed.metadata.failures,
      },
      network_totals: merged.networkTotals,
      rewards: merged.rewards,
      dev_activity: {
        generated_at: new Date().toISOString(),
        lookback_days: DEV_ACTIVITY_LOOKBACK_DAYS,
        active_contributor_window_days: DEV_ACTIVITY_ACTIVE_CONTRIBUTOR_DAYS,
        contributors: devFeed.contributors,
        events: devFeed.events,
        stats: buildDevActivityStats(
          devFeed.events,
          dateDaysAgo(DEV_ACTIVITY_LOOKBACK_DAYS).getTime()
        ),
        spend_monitor: spendMonitor,
      },
      non_task_distributions: {
        total_pft_distributed: nonTaskTotalPft,
        total_transactions: nonTaskTotalTxs,
        daily_activity: chainNonTaskDailyActivity,
        recent_distributions: rewardsInternal.non_task_recent_distributions,
      },
      submissions: merged.submissions,
      task_lifecycle: merged.taskLifecycle,
      network_health: {
        ws_latency_ms: wsLatencyMs,
        ledger_index: ledgerIndex,
        ledger_close_time: ledgerCloseTime,
        ledger_close_unix: ledgerCloseUnix,
        seconds_since_close: secondsSinceClose,
        endpoint_status: 'online' as const,
        endpoint_url: connectedEndpoint,
      },
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
        excluded_non_ptr_reward_txs: analytics.non_task_distributions.total_transactions,
        excluded_non_ptr_reward_pft: analytics.non_task_distributions.total_pft_distributed,
        relay_wallets_discovered_funding: fundingRelayWallets.length,
        relay_wallets_discovered_behavior: behaviorRelayWallets.length,
        new_behavior_relay_wallets: behaviorRelayWallets,
        dev_activity_events: analytics.dev_activity?.events.length ?? 0,
        dev_activity_stats: analytics.dev_activity?.stats,
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
