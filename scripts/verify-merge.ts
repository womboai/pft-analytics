/**
 * Verify that pre-reset baseline is correctly merged with post-reset data.
 *
 * Usage:
 *   npx tsx scripts/verify-merge.ts [blob-url]
 *
 * If no URL is provided, fetches from localhost:3000/api/refresh-data first,
 * then reads the blob URL from the response.
 */

import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const baselinePath = join(__dirname, '..', 'data', 'baseline-pre-reset.json');
const baseline = JSON.parse(readFileSync(baselinePath, 'utf-8'));

interface LeaderboardEntry {
  address: string;
  total_pft: number;
  balance: number;
}

interface NetworkAnalytics {
  network_totals: {
    total_pft_distributed: number;
    unique_earners: number;
    total_rewards_paid: number;
    total_submissions: number;
    unique_submitters: number;
  };
  rewards: {
    leaderboard: LeaderboardEntry[];
    daily_activity: Array<{ date: string; pft: number; tx_count: number }>;
  };
  submissions: {
    top_submitters: Array<{ address: string; submissions: number }>;
    daily_submissions: Array<{ date: string; submissions: number }>;
  };
  task_lifecycle: {
    total_tasks_inferred: number;
    tasks_completed: number;
    daily_lifecycle: Array<{ date: string; submitted: number; completed: number; expired: number }>;
  };
}

let passed = 0;
let failed = 0;

function check(name: string, ok: boolean, detail?: string) {
  if (ok) {
    console.log(`  ✓ ${name}`);
    passed++;
  } else {
    console.log(`  ✗ ${name}${detail ? ` — ${detail}` : ''}`);
    failed++;
  }
}

async function fetchMergedData(url?: string): Promise<NetworkAnalytics> {
  if (url) {
    const res = await fetch(url);
    return res.json() as Promise<NetworkAnalytics>;
  }

  // Try localhost refresh endpoint first
  try {
    const refreshRes = await fetch('http://localhost:3000/api/refresh-data');
    const refreshJson = await refreshRes.json();
    if (refreshJson.blob_url) {
      const blobRes = await fetch(refreshJson.blob_url);
      return blobRes.json() as Promise<NetworkAnalytics>;
    }
  } catch {
    // fall through
  }

  // Try production blob
  const prodRes = await fetch('https://pft.w.ai/api/refresh-data');
  const prodJson = await prodRes.json();
  if (prodJson.blob_url) {
    const blobRes = await fetch(prodJson.blob_url);
    return blobRes.json() as Promise<NetworkAnalytics>;
  }

  throw new Error('Could not fetch merged data from any source');
}

async function main() {
  const url = process.argv[2];
  console.log('Fetching merged data...');
  const merged = await fetchMergedData(url);

  console.log('\n1. Baseline coverage');
  const mergedAddresses = new Set(merged.rewards.leaderboard.map((e: LeaderboardEntry) => e.address));
  const baselineAddresses = baseline.rewards.leaderboard.map((e: LeaderboardEntry) => e.address);
  const missingFromMerged = baselineAddresses.filter((a: string) => !mergedAddresses.has(a));
  check(
    'Every baseline address in merged leaderboard',
    missingFromMerged.length === 0,
    missingFromMerged.length > 0 ? `Missing: ${missingFromMerged.join(', ')}` : undefined,
  );

  console.log('\n2. Monotonic totals');
  let allMonotonic = true;
  for (const baseEntry of baseline.rewards.leaderboard) {
    const mergedEntry = merged.rewards.leaderboard.find((e: LeaderboardEntry) => e.address === baseEntry.address);
    if (mergedEntry && mergedEntry.total_pft < baseEntry.total_pft) {
      check(`${baseEntry.address} total_pft >= baseline`, false, `${mergedEntry.total_pft} < ${baseEntry.total_pft}`);
      allMonotonic = false;
    }
  }
  if (allMonotonic) check('All leaderboard entries >= baseline total_pft', true);

  console.log('\n3. No duplicates');
  const addressCounts = new Map<string, number>();
  for (const e of merged.rewards.leaderboard) {
    addressCounts.set(e.address, (addressCounts.get(e.address) || 0) + 1);
  }
  const dupes = [...addressCounts.entries()].filter(([, c]) => c > 1);
  check('No duplicate addresses in leaderboard', dupes.length === 0, dupes.map(([a]) => a).join(', '));

  console.log('\n4. Network totals');
  check(
    `total_pft_distributed >= ${baseline.network_totals.total_pft_distributed}`,
    merged.network_totals.total_pft_distributed >= baseline.network_totals.total_pft_distributed,
    `Got ${merged.network_totals.total_pft_distributed}`,
  );
  check(
    `unique_earners >= ${baseline.network_totals.unique_earners}`,
    merged.network_totals.unique_earners >= baseline.network_totals.unique_earners,
    `Got ${merged.network_totals.unique_earners}`,
  );
  check(
    `total_rewards_paid >= ${baseline.network_totals.total_rewards_paid}`,
    merged.network_totals.total_rewards_paid >= baseline.network_totals.total_rewards_paid,
    `Got ${merged.network_totals.total_rewards_paid}`,
  );
  check(
    `total_submissions >= ${baseline.network_totals.total_submissions}`,
    merged.network_totals.total_submissions >= baseline.network_totals.total_submissions,
    `Got ${merged.network_totals.total_submissions}`,
  );

  console.log('\n5. Date continuity');
  const mergedDates = new Set(merged.rewards.daily_activity.map((d: { date: string }) => d.date));
  const baselineDates = baseline.rewards.daily_activity.map((d: { date: string }) => d.date);
  const missingDates = baselineDates.filter((d: string) => !mergedDates.has(d));
  check('All baseline dates present in merged daily_activity', missingDates.length === 0, missingDates.join(', '));

  const dateCounts = new Map<string, number>();
  for (const d of merged.rewards.daily_activity) {
    dateCounts.set(d.date, (dateCounts.get(d.date) || 0) + 1);
  }
  const dupeDates = [...dateCounts.entries()].filter(([, c]) => c > 1);
  check('No duplicate dates in daily_activity', dupeDates.length === 0, dupeDates.map(([d]) => d).join(', '));

  console.log('\n6. Balance sanity');
  const totalBalance = merged.rewards.leaderboard.reduce((s: number, e: LeaderboardEntry) => s + e.balance, 0);
  const ratio = totalBalance / merged.network_totals.total_pft_distributed;
  check(
    `Sum of balances / total_distributed within reasonable range`,
    ratio > 0.5 && ratio < 1.5,
    `Ratio: ${ratio.toFixed(3)} (balance sum: ${totalBalance.toFixed(0)}, distributed: ${merged.network_totals.total_pft_distributed.toFixed(0)})`,
  );

  console.log('\n7. Task lifecycle');
  check(
    `total_tasks_inferred >= ${baseline.task_lifecycle.total_tasks_inferred}`,
    merged.task_lifecycle.total_tasks_inferred >= baseline.task_lifecycle.total_tasks_inferred,
    `Got ${merged.task_lifecycle.total_tasks_inferred}`,
  );
  check(
    `tasks_completed >= ${baseline.task_lifecycle.tasks_completed}`,
    merged.task_lifecycle.tasks_completed >= baseline.task_lifecycle.tasks_completed,
    `Got ${merged.task_lifecycle.tasks_completed}`,
  );

  console.log(`\n${'='.repeat(50)}`);
  console.log(`Results: ${passed} passed, ${failed} failed`);
  console.log(`\nKey metrics:`);
  console.log(`  Total PFT distributed: ${merged.network_totals.total_pft_distributed.toLocaleString()}`);
  console.log(`  Unique earners: ${merged.network_totals.unique_earners}`);
  console.log(`  Total submissions: ${merged.network_totals.total_submissions}`);
  console.log(`  Leaderboard entries: ${merged.rewards.leaderboard.length}`);
  console.log(`  Daily activity span: ${merged.rewards.daily_activity[0]?.date} → ${merged.rewards.daily_activity[merged.rewards.daily_activity.length - 1]?.date}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error('Verification failed:', err);
  process.exit(1);
});
