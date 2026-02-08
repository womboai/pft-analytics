// Network analytics data from XRPL chain scan

export interface NetworkData {
  metadata: {
    generated_at: string;
    ledger_index: number;
    reward_addresses: string[];
    memo_address: string;
  };
  network_totals: {
    total_pft_distributed: number;
    unique_earners: number;
    total_rewards_paid: number;
    total_submissions: number;
    unique_submitters: number;
  };
  rewards: {
    leaderboard: Array<{ address: string; total_pft: number; balance: number }>;
    daily_activity: Array<{ date: string; pft: number; tx_count: number }>;
  };
  submissions: {
    daily_submissions: Array<{ date: string; submissions: number }>;
    top_submitters: Array<{ address: string; submissions: number }>;
  };
  task_lifecycle: {
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
  };
  network_health: {
    ws_latency_ms: number;
    ledger_index: number;
    ledger_close_time: string;
    ledger_close_unix: number;
    seconds_since_close: number;
    endpoint_status: 'online' | 'offline';
    endpoint_url: string;
  };
}

const DEFAULT_BLOB_URL = 'https://dclwht8rlliznsdz.public.blob.vercel-storage.com/network.json';

export async function fetchNetworkData(): Promise<NetworkData> {
  // Always use production blob for fresh data (dev and prod)
  const baseUrl = import.meta.env.VITE_DATA_URL || DEFAULT_BLOB_URL;
  
  // Add cache-busting parameter to prevent browser/CDN caching stale data
  const dataUrl = `${baseUrl}?t=${Date.now()}`;
  
  const response = await fetch(dataUrl, {
    cache: 'no-store' // Ensure fresh data on every request
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch network data: ${response.statusText}`);
  }
  return response.json();
}

// Format address for display (truncate middle)
export function formatAddress(addr: string): string {
  if (addr.length <= 12) return addr;
  return `${addr.slice(0, 6)}...${addr.slice(-4)}`;
}

// Format PFT with commas
export function formatPFT(amount: number): string {
  return amount.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
}
