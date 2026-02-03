# PFT Analytics - Agent Context

Real-time on-chain analytics dashboard for the Post Fiat network.

## Project Overview

This is a lightweight analytics dashboard that displays live data from the Post Fiat XRPL network. It uses a simple architecture: a Vercel cron job queries the blockchain every minute and stores the results in Vercel Blob, which the static frontend then fetches.

## Tech Stack

- **Frontend**: Vite + TypeScript (vanilla, no React)
- **Backend**: Vercel Serverless Function (`/api/refresh-data.ts`)
- **Storage**: Vercel Blob (public JSON file)
- **Data Source**: XRPL WebSocket RPC (`wss://rpc.testnet.postfiat.org:6007`)

## Key Files

| File | Purpose |
|------|---------|
| `api/refresh-data.ts` | Serverless function that scans XRPL and writes to Blob |
| `src/main.ts` | Frontend entry point, renders dashboard |
| `src/api.ts` | Fetches data from Blob URL |
| `src/types.ts` | TypeScript interfaces for network data |
| `vercel.json` | Cron schedule (every minute), cache headers |

## Architecture

```
Vercel Cron (1min) → Serverless Function → XRPL Query → Vercel Blob → Frontend
```

### Data Flow

1. **Cron triggers** `/api/refresh-data` every minute
2. **Function connects** to XRPL via WebSocket
3. **Fetches transactions** from reward wallet and memo wallet
4. **Computes analytics**: leaderboard, totals, daily activity
5. **Writes JSON** to Vercel Blob (public URL)
6. **Frontend fetches** from Blob on load + periodic refresh

## Key Addresses

| Wallet | Address | Role |
|--------|---------|------|
| Reward | `rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk` | Distributes PFT rewards |
| Memo | `rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7` | Receives task submissions (pf.ptr memos) |

## Development Commands

```bash
npm install      # Install dependencies
npm run dev      # Start Vite dev server (localhost:5173)
npm run build    # Build for production
vercel dev       # Run with serverless functions locally
```

## Code Style

- Vanilla TypeScript, no framework dependencies
- Functional style, minimal abstraction
- Types defined in `src/types.ts`
- Components are simple render functions in `src/components/`

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `CRON_SECRET` | Optional | Protects cron endpoint from unauthorized calls |
| `BLOB_READ_WRITE_TOKEN` | Yes (production) | Vercel Blob storage access |

## Common Tasks

### Adding a new metric to the dashboard

1. Update `NetworkAnalytics` interface in `api/refresh-data.ts`
2. Compute the metric in `analyzeRewardTransactions` or `analyzeMemoTransactions`
3. Add to `network_totals` or create new section
4. Update `src/types.ts` to match
5. Render in `src/main.ts` or create new component

### Changing the refresh interval

Edit `vercel.json`:
```json
"crons": [
  {
    "path": "/api/refresh-data",
    "schedule": "*/5 * * * *"  // Every 5 minutes
  }
]
```

### Testing the refresh function locally

```bash
# With Vercel CLI
vercel dev

# Then call:
curl http://localhost:3000/api/refresh-data
```

## Deployment

Deployed to Vercel. Push to main triggers automatic deployment.

- **Production**: https://pft.w.ai
- **Blob URL**: Configured in Vercel project settings
