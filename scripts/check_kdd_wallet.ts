import { Client } from 'xrpl';

const RPC_WS_URL = 'wss://rpc.testnet.postfiat.org:6007';
const WALLET = 'rKddMw1hqMGwfgJvzjbWQHtBQT8hDcZNCP';

async function main() {
  const client = new Client(RPC_WS_URL);
  await client.connect();
  
  // Check where this wallet got its funds FROM
  let allTxs: any[] = [];
  let marker: any = undefined;
  
  while (true) {
    const request: any = {
      command: 'account_tx',
      account: WALLET,
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
  
  // Incoming payments (funding sources)
  console.log('\n=== INCOMING (funding) ===');
  for (const txWrapper of allTxs) {
    const tx = txWrapper.tx_json || txWrapper.tx;
    if (!tx || tx.TransactionType !== 'Payment') continue;
    if (tx.Destination !== WALLET) continue;
    
    const amount = tx.DeliverMax || tx.Amount;
    let pft = 0;
    if (typeof amount === 'string') pft = parseInt(amount, 10) / 1_000_000;
    
    console.log(`  FROM ${tx.Account}: ${pft.toLocaleString()} PFT`);
  }
  
  // Outgoing payments
  console.log('\n=== OUTGOING ===');
  for (const txWrapper of allTxs) {
    const tx = txWrapper.tx_json || txWrapper.tx;
    if (!tx || tx.TransactionType !== 'Payment') continue;
    if (tx.Account !== WALLET) continue;
    
    const amount = tx.DeliverMax || tx.Amount;
    let pft = 0;
    if (typeof amount === 'string') pft = parseInt(amount, 10) / 1_000_000;
    
    console.log(`  TO ${tx.Destination}: ${pft.toLocaleString()} PFT`);
  }
  
  await client.disconnect();
}

main().catch(console.error);
