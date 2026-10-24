//! Solana: SOL, SPL Token and Token-2022 tokens, and Metaplex NFTs of every
//! kind.
//!
//! Discovery here is complete: a node lists every token account a wallet
//! owns, under both token programs, so every mint held is found. Names and
//! NFT kinds come from DAS (the cluster's public node serves it), else from
//! the Token-2022 metadata extension, else the mint is shown shortened
//! until a later round names it.
//! Core and compressed NFTs have no token account; only DAS lists them.

use crate::{
    model::{Asset, Checkpoint, Standard},
    rpc::{Failure, Opts},
    Ctx, Found, Progress,
};
use registry::model::{SolanaChain, TokenInfo};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

pub const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

const DAS_TIMEOUT: Duration = Duration::from_secs(20);
/// Small pages: one NFT can carry its image inline, and a page of a hundred
/// spam NFTs has been seen at 50 MB.
const DAS_PAGES: u64 = 2;
const DAS_LIMIT: u64 = 50;
/// DAS lookups of unnamed mints per round (public limit: 40 a method per 10 s).
const LOOKUPS_PER_ROUND: usize = 10;
const DAS_EVERY_SECS: u64 = 180;
/// Longest the NFT listing, or the name lookups, may take in a round.
const DAS_BUDGET: Duration = Duration::from_secs(10);
/// `getMultipleAccounts` takes at most 100 keys.
const ACCOUNTS_PER_CALL: usize = 100;
const MAX_FOUND: usize = 2000;

fn short(address: &str) -> String {
    if address.len() <= 12 {
        return address.to_string();
    }
    format!("{}…{}", &address[..4], &address[address.len() - 4..])
}

fn clean(value: Option<&Value>, max: usize) -> Option<String> {
    let text: String = value?.as_str()?.chars().filter(|c| !c.is_control()).take(max).collect();
    let text = text.trim().trim_matches('\u{0}').trim();
    (!text.is_empty()).then(|| text.to_string())
}

pub(crate) fn native(chain: &SolanaChain) -> Asset {
    Asset {
        id: format!("solana:{}:native", chain.id),
        chain_id: chain.id.clone(),
        standard: Standard::Native,
        contract: None,
        token_id: None,
        symbol: chain.native_currency.symbol.clone(),
        name: chain.native_currency.name.clone(),
        decimals: chain.native_currency.decimals,
        icon: chain.icon.clone(),
        coingecko_id: chain.native_currency.coingecko_id.clone(),
        flagged: false,
    }
}

fn token_asset(chain: &SolanaChain, token: &TokenInfo) -> Asset {
    spl(chain, &token.address, token.symbol.clone(), token.name.clone(), token.decimals, token.icon.clone(), token.coingecko_id.clone())
}

fn spl(chain: &SolanaChain, mint: &str, symbol: String, name: String, decimals: u8, icon: Option<String>, coingecko_id: Option<String>) -> Asset {
    Asset {
        // Not lower-cased: a mint is case-sensitive.
        id: format!("solana:{}:{mint}", chain.id),
        chain_id: chain.id.clone(),
        standard: Standard::Spl,
        contract: Some(mint.to_string()),
        token_id: None,
        symbol,
        name,
        decimals,
        icon,
        coingecko_id,
        flagged: false,
    }
}

fn nft(chain: &SolanaChain, mint: &str, standard: Standard, symbol: String, name: String) -> Asset {
    Asset {
        id: format!("{}:{mint}", chain.id),
        chain_id: chain.id.clone(),
        standard,
        contract: Some(mint.to_string()),
        token_id: None,
        symbol,
        name,
        decimals: 0,
        icon: None,
        coingecko_id: None,
        flagged: false,
    }
}

/// The NFT kind of a DAS record; `None` for fungible tokens and collections.
fn standard_of(asset: &Value) -> Option<Standard> {
    let compressed = asset.pointer("/compression/compressed").and_then(Value::as_bool).unwrap_or(false);
    match asset.get("interface")?.as_str()? {
        "MplBubblegumV2" => Some(Standard::Compressed),
        "V1_NFT" | "LEGACY_NFT" | "V2_NFT" if compressed => Some(Standard::Compressed),
        "V1_NFT" | "LEGACY_NFT" | "V2_NFT" => Some(Standard::Metaplex),
        "V1_PRINT" => Some(Standard::Edition),
        "ProgrammableNFT" => Some(Standard::Programmable),
        "MplCoreAsset" => Some(Standard::Core),
        _ => None,
    }
}

/// NFT kinds that live in a token account, so the chain itself says whether
/// one is held. Core and compressed ones do not.
fn in_token_account(standard: Standard) -> bool {
    matches!(standard, Standard::Metaplex | Standard::Edition | Standard::Programmable)
}

fn failed(chain: &SolanaChain, f: Failure) -> String {
    format!("Could not reach {}: {}", chain.name, f.message)
}

/// Token accounts merged into one amount per mint, in first-seen order.
fn merge_accounts(lists: &[&Value], held: &mut Vec<(String, u128, u8)>) {
    for list in lists {
        for account in list.get("value").and_then(Value::as_array).into_iter().flatten() {
            let info = account.pointer("/account/data/parsed/info");
            let Some(mint) = info.and_then(|i| i.get("mint")).and_then(Value::as_str) else { continue };
            let amount = info
                .and_then(|i| i.pointer("/tokenAmount/amount"))
                .and_then(Value::as_str)
                .and_then(|a| a.parse::<u128>().ok())
                .unwrap_or(0);
            let decimals = info
                .and_then(|i| i.pointer("/tokenAmount/decimals"))
                .and_then(Value::as_u64)
                .and_then(|d| u8::try_from(d).ok())
                .unwrap_or(0);
            match held.iter_mut().find(|(m, ..)| m == mint) {
                Some(entry) => entry.1 = entry.1.saturating_add(amount),
                None => held.push((mint.to_string(), amount, decimals)),
            }
        }
    }
}

fn slot_of(value: &Value) -> u64 {
    value.pointer("/context/slot").and_then(Value::as_u64).unwrap_or(0)
}

pub(crate) async fn poll(cx: &Ctx<'_>, chain: &SolanaChain, cp: &mut Checkpoint, pr: &mut Progress) -> Result<Option<Found>, String> {
    let owner = cx.address.trim();
    let mut config = json!({ "commitment": "confirmed" });
    if pr.slot > 0 {
        // A node behind what was already seen answers -32016 and the next is
        // asked: amounts never go back in time.
        config["minContextSlot"] = json!(pr.slot);
    }
    let mut parsed = config.clone();
    parsed["encoding"] = json!("jsonParsed");
    let opts = Opts::plain();
    let balance_params = json!([owner, config]);
    let spl_params = json!([owner, { "programId": TOKEN_PROGRAM }, parsed]);
    let t22_params = json!([owner, { "programId": TOKEN_2022_PROGRAM }, parsed]);
    let (balance, spl_accounts, t22_accounts) = tokio::join!(
        cx.pool.call(&chain.rpc, "getBalance", &balance_params, opts),
        cx.pool.call(&chain.rpc, "getTokenAccountsByOwner", &spl_params, opts),
        cx.pool.call(&chain.rpc, "getTokenAccountsByOwner", &t22_params, opts),
    );
    let balance = balance.map_err(|f| failed(chain, f))?;
    let spl_accounts = spl_accounts.map_err(|f| failed(chain, f))?;
    let t22_accounts = t22_accounts.map_err(|f| failed(chain, f))?;
    pr.slot = pr.slot.max(slot_of(&balance)).max(slot_of(&spl_accounts)).max(slot_of(&t22_accounts));
    let lamports = balance.get("value").and_then(Value::as_u64).ok_or("unreadable SOL balance")?;
    let mut held: Vec<(String, u128, u8)> = Vec::new();
    merge_accounts(&[&spl_accounts, &t22_accounts], &mut held);

    // What is already known about each mint.
    let mut known: HashMap<String, Asset> = HashMap::new();
    for token in cx.tokens.iter().chain(registry::popular::popular_tokens(&chain.id)) {
        known.entry(token.address.clone()).or_insert_with(|| token_asset(chain, token));
    }
    for asset in &cp.found {
        if let Some(mint) = &asset.contract {
            known.entry(mint.clone()).or_insert_with(|| asset.clone());
        }
    }
    let now = crate::unix_now();
    if cx.full && now.saturating_sub(cp.indexed_at) >= DAS_EVERY_SECS {
        cp.indexed_at = now;
        if let Ok(Some(items)) = tokio::time::timeout(DAS_BUDGET, das_owned(cx, chain, owner)).await {
            // Core and compressed NFTs are known only from DAS: what it no
            // longer lists is no longer held.
            cp.found.retain(|a| in_token_account(a.standard) || !a.is_nft());
            for asset in items.iter().filter_map(|item| from_das(chain, item)) {
                if asset.is_nft() && !in_token_account(asset.standard) {
                    remember(cp, &asset);
                }
            }
        }
    }

    // Names for held mints no list names yet: the Token-2022 extension for
    // all of them, then DAS one mint at a time, a few per round, so a wallet
    // full of airdrops does not hit the public node's limits.
    let placeholder = |mint: &str, known: &HashMap<String, Asset>| known.get(mint).is_none_or(|a| a.name == short(mint));
    let unnamed: Vec<String> = held
        .iter()
        .filter(|(mint, amount, _)| *amount > 0 && placeholder(mint, &known) && !pr.looked_up.contains(mint))
        .map(|(mint, ..)| mint.clone())
        .collect();
    for (mint, (symbol, name)) in token_2022_names(cx, chain, &unnamed).await {
        let decimals = held.iter().find(|(m, ..)| *m == mint).map_or(0, |h| h.2);
        let asset = spl(chain, &mint, symbol, name, decimals, None, None);
        remember(cp, &asset);
        known.insert(mint, asset);
    }
    let lookups: Vec<String> = unnamed.into_iter().filter(|m| placeholder(m, &known)).take(LOOKUPS_PER_ROUND).collect();
    let answers = tokio::time::timeout(DAS_BUDGET, futures_util::future::join_all(lookups.iter().map(|mint| get_asset(cx, chain, mint))))
        .await
        .unwrap_or_else(|_| vec![None; lookups.len()]);
    for (mint, answer) in lookups.into_iter().zip(answers) {
        pr.looked_up.insert(mint.clone());
        if let Some(asset) = answer.as_ref().and_then(|item| from_das(chain, item)) {
            remember(cp, &asset);
            known.insert(mint, asset);
        }
    }

    let mut out: Found = vec![(native(chain), lamports.to_string())];
    for token in cx.tokens {
        let amount = held.iter().find(|(m, ..)| *m == token.address).map_or(0, |h| h.1);
        out.push((token_asset(chain, token), amount.to_string()));
    }
    for (mint, amount, decimals) in &held {
        if *amount == 0 || cx.tokens.iter().any(|t| t.address == *mint) {
            if *amount == 0 {
                // An NFT sent away leaves an empty account behind.
                cp.found.retain(|a| !(a.is_nft() && a.contract.as_deref() == Some(mint.as_str())));
            }
            continue;
        }
        let asset = known.get(mint).cloned().unwrap_or_else(|| {
            let name = short(mint);
            let asset = spl(chain, mint, name.clone(), name, *decimals, None, None);
            remember(cp, &asset);
            asset
        });
        out.push((asset, amount.to_string()));
    }
    // Token-account NFTs no longer in any account are gone.
    cp.found.retain(|a| {
        !(in_token_account(a.standard) && !held.iter().any(|(m, n, _)| *n > 0 && a.contract.as_deref() == Some(m.as_str())))
    });
    for asset in cp.found.iter().filter(|a| a.is_nft() && !in_token_account(a.standard)) {
        out.push((asset.clone(), "1".into()));
    }
    let excess = cp.found.len().saturating_sub(MAX_FOUND);
    cp.found.drain(..excess);
    Ok(Some(out))
}

fn remember(cp: &mut Checkpoint, asset: &Asset) {
    match cp.found.iter_mut().find(|a| a.id == asset.id) {
        Some(existing) => *existing = asset.clone(),
        None => cp.found.push(asset.clone()),
    }
}

/// An asset from one DAS record: an NFT of any kind, or a fungible token's
/// name. Burnt ones are left out.
fn from_das(chain: &SolanaChain, item: &Value) -> Option<Asset> {
    if item.get("burnt").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    let mint = item.get("id")?.as_str()?;
    let meta = item.pointer("/content/metadata");
    let symbol = clean(meta.and_then(|m| m.get("symbol")), 16)
        .or_else(|| clean(item.pointer("/token_info/symbol"), 16));
    let name = clean(meta.and_then(|m| m.get("name")), 64);
    if let Some(standard) = standard_of(item) {
        let name = name.unwrap_or_else(|| short(mint));
        return Some(nft(chain, mint, standard, symbol.unwrap_or_default(), name));
    }
    match item.get("interface")?.as_str()? {
        "FungibleToken" | "FungibleAsset" => {
            let decimals = item.pointer("/token_info/decimals").and_then(Value::as_u64).and_then(|d| u8::try_from(d).ok())?;
            let symbol = symbol.unwrap_or_else(|| short(mint));
            let name = name.unwrap_or_else(|| symbol.clone());
            Some(spl(chain, mint, symbol, name, decimals, None, None))
        }
        _ => None,
    }
}

fn das_nodes(cx: &Ctx<'_>, chain: &SolanaChain) -> Vec<String> {
    cx.pool.order(&chain.rpc).into_iter().filter(|u| cx.pool.caps(u).das != Some(false)).collect()
}

/// Notes that a node has no DAS (devnet's public node does not).
fn no_das(cx: &Ctx<'_>, url: &str, f: &Failure) {
    if f.code == Some(-32601) || f.message.to_ascii_lowercase().contains("method not found") {
        cx.pool.set_caps(url, |c| c.das = Some(false));
    }
}

/// The NFTs DAS lists for the owner. `None` when no node answered in full.
async fn das_owned(cx: &Ctx<'_>, chain: &SolanaChain, owner: &str) -> Option<Vec<Value>> {
    let opts = Opts { timeout: DAS_TIMEOUT, hedge: false, chain: None };
    for url in das_nodes(cx, chain) {
        let mut items = Vec::new();
        let mut complete = true;
        for page in 1..=DAS_PAGES {
            let params = json!({ "ownerAddress": owner, "page": page, "limit": DAS_LIMIT, "displayOptions": { "showFungible": false } });
            match cx.pool.attempt(&url, "getAssetsByOwner", &params, opts).await {
                Ok(result) => {
                    let list = result.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
                    let last = (list.len() as u64) < DAS_LIMIT;
                    items.extend(list);
                    cx.pool.set_caps(&url, |c| c.das = Some(true));
                    if last {
                        break;
                    }
                }
                Err(f) => {
                    no_das(cx, &url, &f);
                    complete = false;
                    break;
                }
            }
        }
        if complete {
            return Some(items);
        }
    }
    None
}

/// One mint's DAS record: its name, symbol, decimals or NFT kind.
async fn get_asset(cx: &Ctx<'_>, chain: &SolanaChain, mint: &str) -> Option<Value> {
    let opts = Opts { timeout: DAS_TIMEOUT, hedge: false, chain: None };
    for url in das_nodes(cx, chain) {
        match cx.pool.attempt(&url, "getAsset", &json!({ "id": mint }), opts).await {
            Ok(item) if item.is_object() => return Some(item),
            Ok(_) => return None,
            Err(f) => no_das(cx, &url, &f),
        }
    }
    None
}

/// Names from the Token-2022 metadata extension, read from the mints
/// themselves, for mints DAS did not name.
async fn token_2022_names(cx: &Ctx<'_>, chain: &SolanaChain, mints: &[String]) -> Vec<(String, (String, String))> {
    let mut out = Vec::new();
    for chunk in mints.chunks(ACCOUNTS_PER_CALL) {
        let params = json!([chunk, { "encoding": "jsonParsed", "commitment": "confirmed" }]);
        let Ok(result) = cx.pool.call(&chain.rpc, "getMultipleAccounts", &params, Opts::plain()).await else { continue };
        for (mint, account) in chunk.iter().zip(result.get("value").and_then(Value::as_array).into_iter().flatten()) {
            let extensions = account.pointer("/data/parsed/info/extensions").and_then(Value::as_array);
            let Some(state) = extensions
                .into_iter()
                .flatten()
                .find(|e| e.get("extension").and_then(Value::as_str) == Some("tokenMetadata"))
                .and_then(|e| e.get("state"))
            else {
                continue;
            };
            let symbol = clean(state.get("symbol"), 16).unwrap_or_else(|| short(mint));
            let name = clean(state.get("name"), 64).unwrap_or_else(|| symbol.clone());
            out.push((mint.to_string(), (symbol, name)));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::tests::{ok, rpc_error};
    use crate::rpc::Pool;
    use std::sync::Arc;

    const OWNER: &str = "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY";
    const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    const FROG: &str = "FroGmint1111111111111111111111111111111111";
    const T22: &str = "T22mint11111111111111111111111111111111111";
    const CORE: &str = "CoreAsset111111111111111111111111111111111";

    fn chain() -> SolanaChain {
        match registry::chains::builtin_only().into_iter().find(|c| c.id() == "solana") {
            Some(registry::model::ChainInfo::Solana(mut s)) => {
                s.rpc = vec!["https://a.example".into(), "https://b.example".into()];
                s
            }
            _ => panic!("solana is built in"),
        }
    }

    fn account(mint: &str, amount: &str, decimals: u64) -> Value {
        json!({ "account": { "data": { "parsed": { "info": { "mint": mint, "tokenAmount": { "amount": amount, "decimals": decimals } } } } } })
    }

    fn node(url: &str, body: Option<&Value>) -> crate::rpc::FakeReply {
        let body = body.cloned().unwrap_or_default();
        let method = body["method"].as_str().unwrap_or("").to_string();
        // The first node is behind the slot already seen.
        if url.contains("a.example") && body.to_string().contains("minContextSlot") {
            return rpc_error(-32016, "Minimum context slot has not been reached");
        }
        match method.as_str() {
            "getBalance" => ok(json!({ "context": { "slot": 500 }, "value": 2_000_000_000u64 })),
            "getTokenAccountsByOwner" if body["params"][1]["programId"] == json!(TOKEN_PROGRAM) => ok(json!({
                "context": { "slot": 501 },
                "value": [account(USDC, "1500000", 6), account(FROG, "7", 0), account(USDC, "500000", 6)]
            })),
            "getTokenAccountsByOwner" => ok(json!({ "context": { "slot": 501 }, "value": [account(T22, "42", 2)] })),
            "getAssetsByOwner" => ok(json!({ "items": [
                { "id": FROG, "interface": "ProgrammableNFT", "content": { "metadata": { "name": "Frog #7", "symbol": "FROG" } } },
                { "id": CORE, "interface": "MplCoreAsset", "content": { "metadata": { "name": "Core One" } } },
                { "id": "Burnt11111111111111111111111111111111111111", "interface": "V1_NFT", "burnt": true }
            ] })),
            "getAsset" if body["params"]["id"] == json!(FROG) => ok(json!(
                { "id": FROG, "interface": "ProgrammableNFT", "content": { "metadata": { "name": "Frog #7", "symbol": "FROG" } } }
            )),
            "getAsset" => ok(json!(null)),
            "getMultipleAccounts" => {
                let value: Vec<Value> = body["params"][0]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|id| {
                        if *id == json!(T22) {
                            json!({ "data": { "parsed": { "info": { "decimals": 2, "extensions": [ { "extension": "tokenMetadata", "state": { "name": "Twenty Two", "symbol": "TT" } } ] } } } })
                        } else {
                            json!({ "data": { "parsed": { "info": { "decimals": 0 } } } })
                        }
                    })
                    .collect();
                ok(json!({ "value": value }))
            }
            _ => rpc_error(-32601, "Method not found"),
        }
    }

    #[tokio::test]
    async fn every_mint_is_found_summed_named_and_checked_against_the_slot() {
        let pool = Pool::fake(Arc::new(node));
        let sol = chain();
        let info = registry::model::ChainInfo::Solana(sol.clone());
        let usdc = TokenInfo {
            address: USDC.into(),
            symbol: "USDC".into(),
            name: "USD Coin".into(),
            decimals: 6,
            token_type: registry::model::TokenType::Spl,
            coingecko_id: Some("usd-coin".into()),
            icon: None,
            custom: false,
            discovered: false,
        };
        let tokens = [usdc];
        let cx = Ctx { pool: &pool, chain: &info, address: OWNER, tokens: &tokens, full: true };
        let mut cp = Checkpoint::default();
        let mut pr = Progress::default();
        let out = poll(&cx, &sol, &mut cp, &mut pr).await.unwrap().unwrap();
        let got: Vec<(&str, &str, Standard)> = out.iter().map(|(a, n)| (a.name.as_str(), n.as_str(), a.standard)).collect();
        assert_eq!(
            got,
            [
                ("Solana", "2000000000", Standard::Native),
                ("USD Coin", "2000000", Standard::Spl),
                ("Frog #7", "7", Standard::Programmable),
                ("Twenty Two", "42", Standard::Spl),
                ("Core One", "1", Standard::Core),
            ]
        );
        assert_eq!(pr.slot, 501);
        // The next round asks for that slot; the first node is behind, the second answers.
        assert!(poll(&cx, &sol, &mut cp, &mut pr).await.unwrap().is_some());
    }

    #[test]
    fn das_kinds_map_to_standards() {
        let kind = |interface: &str, compressed: bool| standard_of(&json!({ "interface": interface, "compression": { "compressed": compressed } }));
        assert_eq!(kind("V1_NFT", false), Some(Standard::Metaplex));
        assert_eq!(kind("V1_NFT", true), Some(Standard::Compressed));
        assert_eq!(kind("MplBubblegumV2", false), Some(Standard::Compressed));
        assert_eq!(kind("V1_PRINT", false), Some(Standard::Edition));
        assert_eq!(kind("ProgrammableNFT", false), Some(Standard::Programmable));
        assert_eq!(kind("MplCoreAsset", false), Some(Standard::Core));
        assert_eq!(kind("FungibleToken", false), None);
    }
}
