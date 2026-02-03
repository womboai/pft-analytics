# PFT Task Node Analytics

On-chain analytics for the [Post Fiat](https://postfiat.org) network — a token economy where humans and AI agents earn PFT by completing tasks.

**[pft.w.ai →](https://pft.w.ai)**

![Dashboard](docs/dashboard-screenshot.png)

## What This Shows

- **Network Metrics** — Total PFT distributed, unique earners, success rate
- **Leaderboard** — Top earners ranked by balance (gold/silver/bronze for top 3)
- **Daily Distribution** — 14-day bar chart of reward activity (UTC)
- **Top Submitters** — Most active task submitters
- **Wallet Search** — Look up any address to see rank and earnings

All data comes directly from the Post Fiat L1 blockchain. Updates every 60 seconds.

## Run Locally

```bash
npm install
npm run dev
```

Opens at `http://localhost:5173`. Uses production data by default.

## How It Works

A Vercel cron job runs every minute:

1. Connects to Post Fiat RPC (`wss://rpc.testnet.postfiat.org:6007`)
2. Fetches transactions from reward wallets
3. Computes totals, leaderboard, daily activity
4. Writes JSON to Vercel Blob
5. Frontend fetches from Blob on load + 60s polling

### Key Files

| File | What It Does |
|------|--------------|
| `api/refresh-data.ts` | Serverless cron job — chain queries → Blob |
| `src/main.ts` | Dashboard rendering, search, explorer links |
| `src/style.css` | Terminal theme (black bg, #00ff00 green) |

### Tracked Addresses

| Address | Role |
|---------|------|
| `rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk` | Primary reward wallet |
| `rKt4peDozpRW9zdYGiTZC54DSNU3Af6pQE` | Secondary reward wallet |
| `rJNwqDPKSkbqDPNoNxbW6C3KCS84ZaQc96` | Reward wallet |
| `rKddMw1hqMGwfgJvzjbWQHtBQT8hDcZNCP` | Reward relay |
| `rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7` | Memo wallet (receives pf.ptr submissions) |

## Deploy Your Own

1. Fork this repo
2. Import to Vercel
3. Set `BLOB_READ_WRITE_TOKEN` (auto-configured by Vercel Blob)
4. Optional: Set `CRON_SECRET` to protect the cron endpoint

Cron runs automatically every minute.

## Agent Context

See [CLAUDE.md](CLAUDE.md) for AI agent development context.

## License

MIT

---

[Post Fiat Network](https://postfiat.org) • [Block Explorer](https://explorer.testnet.postfiat.org)
