import { Client } from 'xrpl';

const RPC_WS_URL = 'wss://rpc.testnet.postfiat.org:6007';
const KNOWN_REWARD_ADDRS = [
  'rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk',
  'rKt4peDozpRW9zdYGiTZC54DSNU3Af6pQE', 
  'rJNwqDPKSkbqDPNoNxbW6C3KCS84ZaQc96',
];
const MEMO_ADDRESS = 'rwdm72S9YVKkZjeADKU2bbUMuY4vPnSfH7';

async function main() {
  const client = new Client(RPC_WS_URL);
  await client.connect();

  // Collect all unique addresses that have received from ANY known reward address
  const allRecipients = new Set<string>();
  
  for (const rewardAddr of KNOWN_REWARD_ADDRS) {
    let marker: any = undefined;
    while (true) {
      const request: any = {
        command: 'account_tx',
        account: rewardAddr,
        limit: 400,
        forward: false,
      };
      if (marker) request.marker = marker;
      
      const response = await client.request(request);
      const txs = response.result.transactions || [];
      if (txs.length === 0) break;
      
      for (const txWrapper of txs as any[]) {
        const tx = txWrapper.tx_json || txWrapper.tx;
        if (!tx || tx.TransactionType !== 'Payment') continue;
        if (tx.Account !== rewardAddr) continue;
        const dest = tx.Destination;
        if (dest && dest !== MEMO_ADDRESS && !KNOWN_REWARD_ADDRS.includes(dest)) {
          allRecipients.add(dest);
        }
      }
      
      marker = response.result.marker;
      if (!marker) break;
    }
  }
  
  console.log(`Found ${allRecipients.size} unique recipients from known reward addresses`);
  
  // Now for each recipient, check if they received from any OTHER addresses too
  // This would indicate additional reward addresses
  const otherSenders = new Map<string, number>();
  let checked = 0;
  
  for (const recipient of Array.from(allRecipients).slice(0, 30)) {
    checked++;
    let marker: any = undefined;
    
    while (true) {
      const request: any = {
        command: 'account_tx',
        account: recipient,
        limit: 400,
        forward: false,
      };
      if (marker) request.marker = marker;
      
      const response = await client.request(request);
      const txs = response.result.transactions || [];
      if (txs.length === 0) break;
      
      for (const txWrapper of txs as any[]) {
        const tx = txWrapper.tx_json || txWrapper.tx;
        if (!tx || tx.TransactionType !== 'Payment') continue;
        if (tx.Destination !== recipient) continue;
        
        const sender = tx.Account;
        if (sender && !KNOWN_REWARD_ADDRS.includes(sender) && sender !== MEMO_ADDRESS) {
          const amount = tx.DeliverMax || tx.Amount;
          let pft = 0;
          if (typeof amount === 'string') {
            pft = parseInt(amount, 10) / 1_000_000;
          }
          if (pft > 100) { // Only significant amounts
            otherSenders.set(sender, (otherSenders.get(sender) || 0) + 1);
          }
        }
      }
      
      marker = response.result.marker;
      if (!marker) break;
    }
    
    if (checked % 10 === 0) console.log(`Checked ${checked}/${allRecipients.size}...`);
  }
  
  console.log('\n=== Addresses sending >100 PFT to multiple recipients ===');
  const sorted = Array.from(otherSenders.entries())
    .filter(([_, count]) => count > 1)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20);
  
  for (const [addr, count] of sorted) {
    console.log(`${addr}: sent to ${count} recipients`);
  }
  
  await client.disconnect();
}

main().catch(console.error);
