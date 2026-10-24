# balance-engine

Live balances and discovery of every coin, token and NFT a wallet holds, on EVM chains, Solana and Bitcoin. Built for non-custodial wallets: it takes public addresses only and uses no API keys.

- **Several public RPCs per chain.** Endpoints are ranked by health and speed. A node that rate-limits, errors or sits behind a firewall is benched for a while, and a slow one is hedged. EVM nodes must report the right chain id, and a node that falls behind the others is passed over.
- **The index suggests, the chain decides.** Token lists, transfer logs, Blockscout and Solana DAS only name candidates. Every amount reported is read from the chain in that round, at one block. A failed read is reported as an error, never as a zero.
- **Standards.** Native coins, ERC-20, ERC-721, ERC-1155, ERC-404, SPL Token, Token-2022, and Metaplex NFTs (Token Metadata, editions, pNFT, Core, compressed).
- **Trust.** Every holding is marked `Verified` (on a shipped list, or added by the user), `Unverified`, or `Spam` (lures, links, invisible characters, impersonating a listed symbol, flagged by an index).
- **Live.** One task per chain polls at the chain's pace (EVM 15 s, Solana 20 s, Bitcoin 60 s) and does a full pass every 3 minutes. `kick` triggers a pass now, and `pause` / `resume` follow the app between foreground and background.

It depends on the sibling `registry` crate (`path = "../registry"`) for chains, RPC lists and token lists.

```rust
use balance_engine::{Checkpoint, Engine, Target};

let engine = Engine::new("My Wallet/1.0"); // your wallet's User-Agent
let targets = vec![Target { chain, address, tokens, checkpoint: Checkpoint::default() }];

// One full pass.
for update in engine.scan(&targets).await {
    // update.holdings: the full set for that chain; update.error: Some if it failed
}

// Live until dropped. Return false from the callback to stop (wallet locked).
let watch = engine.watch(targets, std::sync::Arc::new(|update| {
    save(update.checkpoint.clone()); // store it; pass it back next time
    true
}));
watch.kick(Some("ethereum")); // after a send
watch.pause();                // app in background
watch.resume();
```

**Limits.** On an EVM chain without a working Blockscout, a token outside the shipped lists that arrived before the first scan cannot be found without a paid indexer. Transfers that arrive later are caught by the log scan, where a node serves address-less log queries. Users can always add a token by its contract address.

**Checks:** `cargo test`, plus `cargo test -- --ignored` for live, read-only checks against mainnet.
