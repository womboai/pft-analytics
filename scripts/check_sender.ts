import { Client } from 'xrpl';

const RPC_WS_URL = 'wss://rpc.testnet.postfiat.org:6007';
const UNKNOWN_SENDER = 'rKddMw1hqMGwfgJvzjbWQHtBQT8hDcZNCP';

async function main() {
  const client = new Client(RPC_WS_URL);
  await client.connect();
  
  // Get account info
  try {
    const accountInfo = await client.request({
      command: 'account_info',
      account: UNKNOWN_SENDER,
      ledger_index: 'validated',
    });
    const balanceDrops = accountInfo.result.account_data.Balance;
    const balance = parseInt(balanceDrops, 10) / 1_000_000;
    console.log(`Account: ${UNKNOWN_SENDER}`);
    console.log(`Current balance: ${balance.toLocaleString()} PFT`);
  } catch (e) {
    console.log('Account not found or error');
  }
  
  // Fetch transactions to understand its activity pattern
  let allTxs: any[] = [];
  let marker: any = undefined;
  
  while (allTxs.length < 500) {
    const request: any = {
      command: 'account_tx',
      account: UNKNOWN_SENDER,
      limit: 400,
      forward: false,
    };
    if (marker) request.marker = marker;
    
    const response = await client.request(request);
    const txs = response.result.transactions || [];
    if (txs.length === 0) break;
    allTxs.push(...txs);
    marker = response.result.marker;
    if (!marker) break;
  }
  
  console.log(`Total transactions: ${allTxs.length}`);
  
  // Analyze outgoing payments - who does this address send to?
  const recipients: Record<string, number> = {};
  let totalSent = 0;
  
  for (const txWrapper of allTxs) {
    const tx = txWrapper.tx_json || txWrapper.tx;
    if (!tx) continue;
    if (tx.TransactionType !== 'Payment') continue;
    if (tx.Account !== UNKNOWN_SENDER) continue;
    
    const amount = tx.DeliverMax || tx.Amount;
    let pft: number | null = null;
    if (typeof amount === 'string') {
      pft = parseInt(amount, 10) / 1_000_000;
    }
    if (pft === null || pft <= 0) continue;
    
    const recipient = tx.Destination || '';
    recipients[recipient] = (recipients[recipient] || 0) + pft;
    totalSent += pft;
  }
  
  console.log(`\nTotal PFT sent: ${totalSent.toLocaleString()}`);
  console.log(`Unique recipients: ${Object.keys(recipients).length}`);
  console.log('\nTop recipients:');
  const sorted = Object.entries(recipients)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20);
  for (const [addr, amount] of sorted) {
    console.log(`  ${addr}: ${amount.toLocaleString()} PFT`);
  }
  
  await client.disconnect();
}

main().catch(console.error);
