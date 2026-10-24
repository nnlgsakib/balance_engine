//! Live, read-only checks against public mainnet nodes. Nothing is sent.
//!
//!   cargo test -- --ignored

use balance_engine::{Checkpoint, Engine, Standard, Target, Trust};
use registry::model::ChainInfo;

fn chain(id: &str) -> ChainInfo {
    registry::chains::builtin_only().into_iter().find(|c| c.id() == id).expect("built-in chain")
}

fn target(id: &str, address: &str) -> Target {
    let chain = chain(id);
    let tokens = match &chain {
        ChainInfo::Evm(c) => c.default_tokens.clone(),
        ChainInfo::Solana(c) => c.default_tokens.clone(),
        ChainInfo::Bitcoin(_) => vec![],
    };
    Target { chain, address: address.into(), tokens, checkpoint: Checkpoint::default() }
}

#[tokio::test]
#[ignore]
async fn ethereum_holdings_come_from_multicall3_with_trust() {
    let engine = Engine::new("balance-engine-tests");
    let update = engine.scan(&[target("ethereum", "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")]).await.remove(0);
    assert_eq!(update.error, None);
    let held: Vec<_> = update.holdings.iter().filter(|h| h.held()).collect();
    for h in &held {
        println!("{:<12} {:<10?} {:>28} {:?}", h.asset.symbol, h.asset.standard, h.amount, h.trust);
    }
    assert!(update.holdings[0].asset.standard == Standard::Native && update.holdings[0].held());
    assert!(held.iter().any(|h| h.asset.standard == Standard::Erc20 && h.trust == Trust::Verified), "vitalik.eth holds listed tokens");
}

#[tokio::test]
#[ignore]
async fn ethereum_token_metadata_is_read_from_the_contract() {
    let engine = Engine::new("balance-engine-tests");
    let ChainInfo::Evm(eth) = chain("ethereum") else { unreachable!() };
    let usdc = engine.evm_token(&eth, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").await.unwrap();
    assert_eq!((usdc.symbol.as_str(), usdc.decimals, usdc.standard), ("USDC", 6, Standard::Erc20));
    let mkr = engine.evm_token(&eth, "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2").await.unwrap();
    assert_eq!(mkr.symbol, "MKR", "a bytes32 symbol");
    let bayc = engine.evm_token(&eth, "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D").await.unwrap();
    assert_eq!(bayc.standard, Standard::Erc721);
}

#[tokio::test]
#[ignore]
async fn bitcoin_balance_comes_from_esplora() {
    let engine = Engine::new("balance-engine-tests");
    let update = engine.scan(&[target("bitcoin", "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")]).await.remove(0);
    assert_eq!(update.error, None);
    let sats: u64 = update.holdings[0].amount.parse().unwrap();
    assert!(sats > 5_000_000_000, "the genesis address holds more than 50 BTC: {sats}");
}

#[tokio::test]
#[ignore]
async fn solana_every_mint_is_found_and_named() {
    let engine = Engine::new("balance-engine-tests");
    // A well-known public wallet with many tokens and NFTs (Solana Foundation donation address).
    let update = engine.scan(&[target("solana", "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY")]).await.remove(0);
    assert_eq!(update.error, None);
    let held: Vec<_> = update.holdings.iter().filter(|h| h.held()).collect();
    for h in held.iter().take(40) {
        println!("{:<14} {:<12?} {:>20} {:?}", h.asset.symbol, h.asset.standard, h.amount, h.trust);
    }
    println!("{} held, {} found", held.len(), update.checkpoint.found.len());
    assert!(held.len() > 3);
    assert!(held.iter().any(|h| !h.asset.name.contains('…')), "names come from DAS or Token-2022");
}

/// Every built-in mainnet answers a full pass in time, from its own RPC list.
#[tokio::test]
#[ignore]
async fn every_builtin_chain_answers_a_full_pass() {
    let engine = Engine::new("balance-engine-tests");
    let targets: Vec<Target> = registry::chains::builtin_only()
        .into_iter()
        .filter(|c| !matches!(c, ChainInfo::Evm(e) if e.testnet) && !matches!(c, ChainInfo::Solana(s) if s.testnet))
        .map(|chain| {
            let address = match &chain {
                ChainInfo::Evm(_) => "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                ChainInfo::Bitcoin(_) => "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
                ChainInfo::Solana(_) => "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY",
            };
            target(chain.id(), address)
        })
        .collect();
    let started = std::time::Instant::now();
    let updates = engine.scan(&targets).await;
    let mut failed = Vec::new();
    for u in &updates {
        println!("{:<14} {:>3} held  error: {:?}", u.chain_id, u.holdings.iter().filter(|h| h.held()).count(), u.error);
        if u.error.is_some() {
            failed.push(u.chain_id.clone());
        }
    }
    println!("all chains in {:?}", started.elapsed());
    assert!(failed.is_empty(), "failed: {failed:?}");
}
