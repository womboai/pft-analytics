import { Client } from 'xrpl';

const RPC_WS_URL = 'wss://rpc.testnet.postfiat.org:6007';
const WALLET = 'rh7eykJ99XnGTm2iNBzeD5A9MLqnb7kXCp';
const REWARD_ADDRESSES = [
  'rGBKxoTcavpfEso7ASRELZAMcCMqKa8oFk',
  'rKt4peDozpRW9zdYGiTZC54DSNU3Af6pQE', 
  'rJNwqDPKSkbqDPNoNxbW6C3KCS84ZaQc96',
];
const RIPPLE_EPOCH = 946684800;

async function main() {
  const client = new Client(RPC_WS_URL);
  await client.connect();
  
  // Fetch all transactions for the wallet
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
  
  console.log(`Total transactions for ${WALLET}: ${allTxs.length}`);
  
  // Find all incoming payments from reward addresses
  let totalFromRewardAddrs = 0;
  let rewardTxCount = 0;
  const rewardsBySource: Record<string, number> = {};
  
  // Find ALL incoming payments
  let totalAllIncoming = 0;
  let allIncomingCount = 0;
  const allSenders: Record<string, number> = {};
  
  for (const txWrapper of allTxs) {
    const tx = txWrapper.tx_json || txWrapper.tx;
    if (!tx) continue;
    if (tx.TransactionType !== 'Payment') continue;
    if (tx.Destination !== WALLET) continue;
    
    const amount = tx.DeliverMax || tx.Amount;
    let pft: number | null = null;
    if (typeof amount === 'string') {
      pft = parseInt(amount, 10) / 1_000_000;
    } else if (typeof amount === 'number') {
      pft = amount / 1_000_000;
    }
    if (pft === null || pft <= 0) continue;
    
    const sender = tx.Account || '';
    
    // Track all incoming
    totalAllIncoming += pft;
    allIncomingCount++;
    allSenders[sender] = (allSenders[sender] || 0) + pft;
    
    // Track from reward addresses
    if (REWARD_ADDRESSES.includes(sender)) {
      totalFromRewardAddrs += pft;
      rewardTxCount++;
      rewardsBySource[sender] = (rewardsBySource[sender] || 0) + pft;
    }
  }
  
  console.log('\n=== FROM REWARD ADDRESSES ===');
  console.log(`Total PFT from reward addresses: ${totalFromRewardAddrs.toLocaleString()}`);
  console.log(`Number of reward transactions: ${rewardTxCount}`);
  console.log('By source:');
  for (const [addr, amount] of Object.entries(rewardsBySource)) {
    console.log(`  ${addr}: ${amount.toLocaleString()} PFT`);
  }
  
  console.log('\n=== ALL INCOMING PAYMENTS ===');
  console.log(`Total ALL incoming PFT: ${totalAllIncoming.toLocaleString()}`);
  console.log(`Number of incoming transactions: ${allIncomingCount}`);
  console.log('Top senders:');
  const sortedSenders = Object.entries(allSenders)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);
  for (const [addr, amount] of sortedSenders) {
    const isReward = REWARD_ADDRESSES.includes(addr) ? ' (REWARD ADDR)' : '';
    console.log(`  ${addr}: ${amount.toLocaleString()} PFT${isReward}`);
  }
  
  // Get current balance
  const accountInfo = await client.request({
    command: 'account_info',
    account: WALLET,
    ledger_index: 'validated',
  });
  const balanceDrops = accountInfo.result.account_data.Balance;
  const balance = parseInt(balanceDrops, 10) / 1_000_000;
  console.log(`\nCurrent balance: ${balance.toLocaleString()} PFT`);
  
  await client.disconnect();
}

main().catch(console.error);
