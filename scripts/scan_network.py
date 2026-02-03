#!/usr/bin/env python3
"""
PFT Network Analytics Scanner (v2)

Queries TaskNode wallet transaction history directly via account_tx.
Much faster than scanning every ledger.
"""

import asyncio
import json
import ssl
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    import websockets
except ImportError:
    print("Install websockets: pip install websockets")
    sys.exit(1)

# PFT XRPL testnet RPC
RPC_WS_URL = "wss://rpc.testnet.postfiat.org:6007"
RIPPLE_EPOCH = 946684800

# TaskNode addresses (must match api/refresh-data.ts)
REWARD_ADDRESSES = [
    "rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk",  # Primary reward wallet
    "rKt4peDozpRW9zdYGiTZC54DSNU3Af6pQE",  # Secondary reward wallet
    "rJNwqDPKSkbqDPNoNxbW6C3KCS84ZaQc96",  # Additional reward wallet
    "rKddMw1hqMGwfgJvzjbWQHtBQT8hDcZNCP",  # Memo-funded reward relay
]
MEMO_ADDRESS = "rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7"  # Receives task memos

# System accounts to exclude
SYSTEM_ACCOUNTS = set(REWARD_ADDRESSES + [MEMO_ADDRESS, "rrrrrrrrrrrrrrrrrrrrrhoLvTp"])


def unix_from_ripple(ripple_ts: int) -> int:
    return ripple_ts + RIPPLE_EPOCH


def format_iso(unix_ts: int) -> str:
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()


def parse_pft_amount(amount) -> float | None:
    """Parse PFT from Amount (drops, like XRP - divide by 1,000,000)."""
    # PFT is native currency on this XRPL fork, amount is in drops (integer string)
    if isinstance(amount, str):
        try:
            drops = int(amount)
            return drops / 1_000_000  # Convert drops to PFT
        except:
            return None
    elif isinstance(amount, int):
        return amount / 1_000_000
    return None


async def rpc_call(ws, payload: dict) -> dict:
    await ws.send(json.dumps(payload))
    return json.loads(await ws.recv())


async def fetch_account_balance(ws, address: str) -> float:
    """Fetch current PFT balance for an address."""
    payload = {
        "command": "account_info",
        "account": address,
        "ledger_index": "validated"
    }
    resp = await rpc_call(ws, payload)
    result = resp.get("result", {})

    # Check for account not found
    if "error" in result:
        return 0.0

    account_data = result.get("account_data", {})
    balance_drops = account_data.get("Balance", "0")

    try:
        return int(balance_drops) / 1_000_000
    except (ValueError, TypeError):
        return 0.0


async def fetch_account_tx(ws, account: str, limit: int = 400, marker=None) -> dict:
    """Fetch transaction history for an account."""
    payload = {
        "command": "account_tx",
        "account": account,
        "limit": limit,
        "forward": False,  # newest first
    }
    if marker:
        payload["marker"] = marker
    resp = await rpc_call(ws, payload)
    return resp.get("result", {})


async def fetch_all_account_tx(ws, account: str, max_txs: int = 5000) -> list:
    """Fetch all transactions for account up to max_txs."""
    all_txs = []
    marker = None

    while len(all_txs) < max_txs:
        result = await fetch_account_tx(ws, account, limit=400, marker=marker)
        txs = result.get("transactions", [])
        if not txs:
            break
        all_txs.extend(txs)
        marker = result.get("marker")
        if not marker:
            break
        print(f"  Fetched {len(all_txs)} transactions...", file=sys.stderr)

    return all_txs


async def analyze_reward_transactions(ws, txs: list) -> dict:
    """Analyze outgoing PFT rewards from the reward address."""
    participants = set()
    rewards_by_recipient = defaultdict(float)
    rewards_by_day = defaultdict(float)
    tx_count_by_day = defaultdict(int)
    total_pft = 0.0
    reward_list = []

    for tx_wrapper in txs:
        tx = tx_wrapper.get("tx", {})
        meta = tx_wrapper.get("meta", {})

        # Only outgoing payments
        if tx.get("TransactionType") != "Payment":
            continue
        if tx.get("Account") not in REWARD_ADDRESSES:
            continue

        # Parse PFT amount
        pft = parse_pft_amount(tx.get("Amount"))
        if pft is None or pft <= 0:
            continue

        recipient = tx.get("Destination", "")
        if recipient in SYSTEM_ACCOUNTS:
            continue

        # Get timestamp
        close_time = tx.get("date", 0)
        unix_ts = unix_from_ripple(close_time) if close_time else 0
        day = datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d") if unix_ts else "unknown"

        # Aggregate
        participants.add(recipient)
        rewards_by_recipient[recipient] += pft
        rewards_by_day[day] += pft
        tx_count_by_day[day] += 1
        total_pft += pft

        reward_list.append({
            "hash": tx.get("hash", ""),
            "recipient": recipient,
            "pft": pft,
            "timestamp": unix_ts,
            "date": day,
        })

    # Fetch balances for all recipients
    print(f"  Fetching balances for {len(rewards_by_recipient)} addresses...", file=sys.stderr)
    balances = {}
    for i, addr in enumerate(rewards_by_recipient.keys()):
        balances[addr] = await fetch_account_balance(ws, addr)
        if (i + 1) % 10 == 0:
            print(f"    Fetched {i + 1}/{len(rewards_by_recipient)} balances...", file=sys.stderr)

    # Build leaderboard with balances
    leaderboard = sorted(
        [
            {
                "address": addr,
                "total_pft": round(amt, 2),
                "balance": round(balances.get(addr, 0.0), 2)
            }
            for addr, amt in rewards_by_recipient.items()
        ],
        key=lambda x: (x["balance"], x["total_pft"]),
        reverse=True
    )

    # Daily activity
    daily = sorted(
        [{"date": d, "pft": round(amt, 2), "tx_count": tx_count_by_day[d]} for d, amt in rewards_by_day.items()],
        key=lambda x: x["date"]
    )

    return {
        "total_pft_distributed": round(total_pft, 2),
        "unique_recipients": len(participants),
        "total_reward_transactions": len(reward_list),
        "leaderboard": leaderboard[:25],
        "daily_activity": daily,
        "recent_rewards": reward_list[:50],
    }


def analyze_memo_transactions(txs: list) -> dict:
    """Analyze incoming memo transactions (task submissions/verifications)."""
    submitters = set()
    submissions_by_sender = defaultdict(int)
    submissions_by_day = defaultdict(int)
    total_submissions = 0
    submission_list = []

    for tx_wrapper in txs:
        tx = tx_wrapper.get("tx", {})

        # Only incoming payments to memo address
        if tx.get("TransactionType") != "Payment":
            continue
        if tx.get("Destination") != MEMO_ADDRESS:
            continue

        sender = tx.get("Account", "")
        if sender in SYSTEM_ACCOUNTS:
            continue

        # Check for memo (pf.ptr)
        memos = tx.get("Memos", [])
        has_ptr_memo = any(
            "70662e707472" in m.get("Memo", {}).get("MemoType", "").lower()
            for m in memos
        )

        if not has_ptr_memo:
            continue

        # Get timestamp
        close_time = tx.get("date", 0)
        unix_ts = unix_from_ripple(close_time) if close_time else 0
        day = datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d") if unix_ts else "unknown"

        # Aggregate
        submitters.add(sender)
        submissions_by_sender[sender] += 1
        submissions_by_day[day] += 1
        total_submissions += 1

        submission_list.append({
            "hash": tx.get("hash", ""),
            "sender": sender,
            "timestamp": unix_ts,
            "date": day,
        })

    # Most active submitters
    top_submitters = sorted(
        [{"address": addr, "submissions": count} for addr, count in submissions_by_sender.items()],
        key=lambda x: x["submissions"],
        reverse=True
    )

    # Daily submissions
    daily = sorted(
        [{"date": d, "submissions": count} for d, count in submissions_by_day.items()],
        key=lambda x: x["date"]
    )

    return {
        "total_submissions": total_submissions,
        "unique_submitters": len(submitters),
        "top_submitters": top_submitters[:25],
        "daily_submissions": daily,
        "recent_submissions": submission_list[:50],
    }


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="PFT Network Analytics (v2)")
    parser.add_argument("--max-txs", type=int, default=5000, help="Max transactions per account")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file")
    args = parser.parse_args()

    print(f"Connecting to {RPC_WS_URL}...", file=sys.stderr)

    context = ssl._create_unverified_context()
    async with websockets.connect(RPC_WS_URL, ssl=context) as ws:
        # Get current ledger index for liveness indicator
        ledger_resp = await rpc_call(ws, {"command": "ledger_current"})
        ledger_index = ledger_resp.get("result", {}).get("ledger_current_index", 0)

        # Fetch reward transactions from all reward wallets
        reward_txs = []
        for reward_address in REWARD_ADDRESSES:
            print(f"\nFetching reward transactions from {reward_address}...", file=sys.stderr)
            addr_txs = await fetch_all_account_tx(ws, reward_address, args.max_txs)
            reward_txs.extend(addr_txs)
            print(f"  Got {len(addr_txs)} transactions", file=sys.stderr)

        # Fetch memo transactions
        print(f"\nFetching memo transactions to {MEMO_ADDRESS}...", file=sys.stderr)
        memo_txs = await fetch_all_account_tx(ws, MEMO_ADDRESS, args.max_txs)
        print(f"  Got {len(memo_txs)} transactions", file=sys.stderr)

        # Analyze
        print("\nAnalyzing...", file=sys.stderr)
        rewards = await analyze_reward_transactions(ws, reward_txs)
        submissions = analyze_memo_transactions(memo_txs)

        # Combine
        analytics = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "ledger_index": ledger_index,
                "reward_addresses": REWARD_ADDRESSES,
                "memo_address": MEMO_ADDRESS,
                "reward_txs_fetched": len(reward_txs),
                "memo_txs_fetched": len(memo_txs),
            },
            "network_totals": {
                "total_pft_distributed": rewards["total_pft_distributed"],
                "unique_earners": rewards["unique_recipients"],
                "total_rewards_paid": rewards["total_reward_transactions"],
                "total_submissions": submissions["total_submissions"],
                "unique_submitters": submissions["unique_submitters"],
            },
            "rewards": rewards,
            "submissions": submissions,
            "task_lifecycle": {
                "total_tasks_inferred": 0,
                "tasks_completed": 0,
                "tasks_pending": 0,
                "tasks_expired": 0,
                "completion_rate": 0,
                "avg_time_to_reward_hours": 0,
                "daily_lifecycle": [],
            },
        }

        # Output
        output_json = json.dumps(analytics, indent=2)

        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(output_json)
            print(f"\nWrote {args.output}", file=sys.stderr)
        else:
            print(output_json)

        # Summary
        print(f"\n{'='*50}", file=sys.stderr)
        print(f"NETWORK SUMMARY", file=sys.stderr)
        print(f"{'='*50}", file=sys.stderr)
        print(f"Total PFT distributed: {rewards['total_pft_distributed']:,.2f}", file=sys.stderr)
        print(f"Unique earners: {rewards['unique_recipients']}", file=sys.stderr)
        print(f"Total rewards: {rewards['total_reward_transactions']}", file=sys.stderr)
        print(f"Total submissions: {submissions['total_submissions']}", file=sys.stderr)
        print(f"Unique submitters: {submissions['unique_submitters']}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
