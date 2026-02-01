# PFT Analytics

Real-time on-chain analytics dashboard for the Post Fiat network.

**[Live Demo →](https://pft.w.ai)**

![Dashboard Screenshot](docs/dashboard-screenshot.png)

## Features

- **Real-time Data** — Updates every minute via automated on-chain scanning
- **Network Totals** — Total PFT distributed, unique earners, task submissions
- **Leaderboard** — Top earners ranked by balance and total earnings with XRPL explorer links
- **Daily Distribution Chart** — 14-day visualization of PFT rewards activity
- **Wallet Search** — Find any address with instant rank lookup

## Tech Stack

| Layer | Technology |
|-------|------------|
| Frontend | [Vite](https://vitejs.dev/) + TypeScript |
| Hosting | [Vercel](https://vercel.com/) (Serverless + Cron + Blob) |
| Blockchain | [XRPL](https://xrpl.org/) (Post Fiat fork) |

## Quick Start

```bash
# Install dependencies
npm install

# Run development server
npm run dev
```

The dashboard will be available at `http://localhost:5173`.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        VERCEL CRON                               │
│                    (every minute)                                │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SERVERLESS FUNCTION                            │
│                  /api/refresh-data.ts                            │
│                                                                  │
│   1. Connect to XRPL via WebSocket                               │
│   2. Fetch reward + submission transactions                      │
│   3. Compute leaderboard, totals, daily activity                 │
│   4. Write JSON to Vercel Blob                                   │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      VERCEL BLOB                                 │
│                    network.json                                  │
│                                                                  │
│   Public URL with 60s cache + stale-while-revalidate             │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                       FRONTEND                                   │
│                     Vite + TypeScript                            │
│                                                                  │
│   Static site fetches from Blob URL on load + auto-refresh       │
└─────────────────────────────────────────────────────────────────┘
```

### Data Sources

All data is sourced directly from the XRPL chain via WebSocket RPC:

| Wallet | Address | Purpose |
|--------|---------|---------|
| Reward Wallet | `rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk` | Distributes PFT rewards |
| Memo Wallet | `rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7` | Receives task submission pointers |

## Project Structure

```
pft-analytics/
├── api/
│   └── refresh-data.ts    # Vercel serverless function (cron job)
├── src/
│   ├── main.ts            # Frontend entry point
│   ├── api.ts             # Data fetching utilities
│   ├── types.ts           # TypeScript interfaces
│   ├── style.css          # Styles
│   └── components/        # UI components
├── public/
│   └── data/              # Static data fallback
├── vercel.json            # Vercel config (cron schedule, headers)
└── index.html             # HTML template
```

## Deployment

### Vercel (Recommended)

1. Fork this repository
2. Import to Vercel
3. Add environment variables:
   - `CRON_SECRET` — Optional secret to protect the cron endpoint
   - `BLOB_READ_WRITE_TOKEN` — Vercel Blob storage token (auto-configured)
4. Deploy

The cron job will automatically run every minute to refresh data.

### Local Development with Live Data

For local development, the frontend fetches from the production Blob URL by default. To test the refresh function locally:

```bash
# Install Vercel CLI
npm i -g vercel

# Run locally with serverless functions
vercel dev
```

## Contributing

Contributions are welcome! Here's how to get started:

1. **Fork** the repository
2. **Create a branch** for your feature (`git checkout -b feature/amazing-feature`)
3. **Make your changes** and test locally
4. **Commit** with clear messages (`git commit -m 'Add amazing feature'`)
5. **Push** to your branch (`git push origin feature/amazing-feature`)
6. **Open a Pull Request**

### Development Guidelines

- Keep the codebase simple — vanilla TypeScript, no heavy frameworks
- Maintain real-time data freshness as a priority
- Test with both live XRPL data and local mocks
- Follow existing code style

### Ideas for Contributions

- [ ] Dark mode support
- [ ] Historical data export (CSV)
- [ ] Mobile-responsive improvements
- [ ] Additional chart visualizations
- [ ] WebSocket live updates (replace polling)

## License

MIT — see [LICENSE](LICENSE) for details.

## For AI Agents

See [CLAUDE.md](CLAUDE.md) for project context and development patterns.

---

Built with data from the [Post Fiat Network](https://postfiat.org/).
