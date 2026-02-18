# Relay Discovery Validation - 2026-02-18

## Summary
Implemented a dual-path reward-sender discovery upgrade in `api/refresh-data.ts`:

1. Existing funding-path discovery (memo wallet funding thresholds)
2. New behavior-path discovery (repeated `pf.ptr` emissions to multiple recipients)

Added anti-noise filtering to prevent 1-drop memo spam from being classified as a reward relay.

## Detection Logic Added

- `discoverBehaviorRelayWallets(...)` scans memo-funded candidates and evaluates outbound payment behavior.
- Candidate prefilter: memo funding >= `100` PFT.
- Behavioral thresholds:
  - lookback: `30` days
  - min `pf.ptr` tx count: `3`
  - min unique recipients: `2`
  - min total distributed PFT: `100`

The min-total-PFT threshold is the anti-spam guard that suppresses micro-transfer false positives.

## Validation Run (local chain scan)

- Funding relay wallets discovered: `23`
- Behavior candidates scanned: `15`
- Behavior matches: `0`
- New behavior-discovered relay wallets: `[]`
- Combined reward sender wallet count: `27`

## Result

No new legitimate relay wallets were found by behavior-based discovery in this run.
The upgraded logic remains active and will surface future behavior-based matches in metadata.
