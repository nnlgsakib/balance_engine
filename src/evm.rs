//! EVM chains: native coin, ERC-20, ERC-721, ERC-1155 and ERC-404.
//!
//! Candidates come from the wallet's list, the chain's popular list, what was
//! found before, transfers into the wallet since the last scan (where a node
//! serves address-less log queries), and Blockscout when it answers. Every
//! amount is then read from the chain, all at one block, through Multicall3
//! when the chain has it and call by call when it does not.

use crate::{
    abi::{self, Call},
    model::{Asset, Checkpoint, Standard},
    rpc::{Failure, Opts},
    Ctx, Found, Progress,
};
use futures_util::{
    stream::{self, StreamExt},
    FutureExt,
};
use registry::model::{EvmChain, HistoryApi, TokenInfo, TokenType};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const MULTICALL_CHUNK: usize = 200;
/// Calls at once when the chain has no Multicall3.
const PARALLEL_CALLS: usize = 8;
/// First log range tried, in blocks; shrunk per node on "too wide".
const LOG_SPAN: u64 = 500;
const LOG_SPAN_MIN: u64 = 10;
/// Blocks behind the head a scan will go back to. Older transfers are only
/// found through lists and Blockscout.
const MAX_BACKFILL: u64 = 2000;
const LOG_CHUNKS: usize = 5;
/// Transfers taken from one round of logs. An airdrop farm can send
/// thousands; what is held is still read from lists and the index.
const MAX_HITS: usize = 500;
const MAX_IDS_PER_LOG: usize = 50;
/// ERC-721 Enumerable ids read for a collection held without known ids.
const ENUMERATE_MAX: usize = 50;
const INDEX_EVERY_SECS: u64 = 30 * 60;
const INDEX_PAGES: usize = 3;
const INDEX_TIMEOUT: Duration = Duration::from_secs(8);
/// Longest the log scan, or the index, may take in a round.
const SOURCE_BUDGET: Duration = Duration::from_secs(12);
const MAX_FOUND_FUNGIBLE: usize = 500;
const MAX_FOUND_NFT: usize = 2000;

fn hex_u64(value: &Value) -> Option<u64> {
    u64::from_str_radix(value.as_str()?.trim_start_matches("0x"), 16).ok()
}

fn short(address: &str) -> String {
    if address.len() <= 12 {
        return address.to_string();
    }
    format!("{}…{}", &address[..6], &address[address.len() - 4..])
}

pub(crate) fn native(chain: &EvmChain) -> Asset {
    Asset {
        id: format!("evm:{}:native", chain.chain_id),
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

fn token_asset(chain: &EvmChain, token: &TokenInfo) -> Asset {
    Asset {
        id: format!("evm:{}:{}", chain.chain_id, token.address.to_lowercase()),
        chain_id: chain.id.clone(),
        standard: match token.token_type {
            TokenType::Erc721 => Standard::Erc721,
            TokenType::Erc1155 => Standard::Erc1155,
            TokenType::Erc20 | TokenType::Spl => Standard::Erc20,
        },
        contract: Some(token.address.clone()),
        token_id: None,
        symbol: token.symbol.clone(),
        name: token.name.clone(),
        decimals: token.decimals,
        icon: token.icon.clone(),
        coingecko_id: token.coingecko_id.clone(),
        flagged: false,
    }
}

fn fungible(chain: &EvmChain, contract: &str, standard: Standard, symbol: String, name: String, decimals: u8, flagged: bool) -> Asset {
    let contract = contract.to_lowercase();
    Asset {
        id: format!("evm:{}:{contract}", chain.chain_id),
        chain_id: chain.id.clone(),
        standard,
        contract: Some(contract),
        token_id: None,
        symbol,
        name,
        decimals,
        icon: None,
        coingecko_id: None,
        flagged,
    }
}

fn nft(chain: &EvmChain, contract: &str, id: &str, standard: Standard, symbol: String, name: String, flagged: bool) -> Asset {
    let contract = contract.to_lowercase();
    Asset {
        id: format!("{}:{contract}:{id}", chain.id),
        chain_id: chain.id.clone(),
        standard,
        contract: Some(contract),
        token_id: Some(id.to_string()),
        symbol,
        name,
        decimals: 0,
        icon: None,
        coingecko_id: None,
        flagged,
    }
}

/// The candidates of one round, in report order, without repeats.
#[derive(Default)]
struct Candidates {
    list: Vec<(Asset, bool)>,
    ids: HashSet<String>,
}

impl Candidates {
    fn add(&mut self, asset: Asset, always: bool) {
        if self.ids.insert(asset.id.clone()) {
            self.list.push((asset, always));
        }
    }

    fn has_contract(&self, contract: &str) -> bool {
        self.list.iter().any(|(a, _)| a.contract.as_deref().is_some_and(|c| c.eq_ignore_ascii_case(contract)))
    }
}

/// A token seen moving into the wallet, before its kind is known for sure.
#[derive(Clone, Debug, PartialEq)]
enum Hit {
    /// ERC-20 `Transfer` (or the fungible half of an ERC-404).
    Fungible(String),
    /// ERC-721 `Transfer`: contract, id.
    Nft721(String, String),
    /// ERC-1155 `TransferSingle` / `TransferBatch`: contract, id.
    Nft1155(String, String),
}

impl Hit {
    fn contract(&self) -> &str {
        match self {
            Hit::Fungible(c) | Hit::Nft721(c, _) | Hit::Nft1155(c, _) => c,
        }
    }
}

pub(crate) async fn poll(cx: &Ctx<'_>, chain: &EvmChain, cp: &mut Checkpoint, pr: &mut Progress) -> Result<Option<Found>, String> {
    let owner = cx.address.trim().to_lowercase();
    abi::address_word(&owner).ok_or("not an EVM address")?;
    let head = head(cx, chain).await?;
    if !cx.full && head <= pr.head && pr.error.is_none() && !pr.last.is_empty() {
        return Ok(None);
    }
    pr.head = head;

    let mut cands = Candidates::default();
    cands.add(native(chain), true);
    for token in cx.tokens {
        cands.add(token_asset(chain, token), true);
    }
    for held in pr.last.iter().filter(|h| h.held()) {
        cands.add(held.asset.clone(), false);
    }
    if cx.full {
        let key = chain.chain_id.to_string();
        for token in registry::popular::popular_tokens(&key) {
            cands.add(token_asset(chain, token), false);
        }
        for asset in &cp.found {
            cands.add(asset.clone(), false);
        }
    }

    // The optional sources run side by side, each within its own budget, so
    // a slow index or a stingy node cannot sink the round.
    let now = crate::unix_now();
    let index_base = blockscout(chain).map(str::to_string).filter(|_| cx.full && now.saturating_sub(cp.indexed_at) >= INDEX_EVERY_SECS);
    if index_base.is_some() {
        cp.indexed_at = now;
    }
    let indexing = async {
        match index_base {
            Some(base) => tokio::time::timeout(SOURCE_BUDGET, index(cx, chain, base, owner.clone())).await.unwrap_or_default(),
            None => Vec::new(),
        }
    };
    // Cut short, the log cursor stays at the last range fully read.
    let scanning = async { tokio::time::timeout(SOURCE_BUDGET, scan_logs(cx, chain, &owner, cp, head)).await.unwrap_or_default() };
    let (mut hits, indexed) = tokio::join!(scanning, indexing);
    for asset in indexed {
        remember(cp, &asset);
        cands.add(asset, false);
    }

    // Hits on contracts already known need no metadata.
    hits.retain(|hit| match hit {
        Hit::Fungible(c) => !cands.has_contract(c),
        Hit::Nft721(..) | Hit::Nft1155(..) => true,
    });
    let mut known: HashMap<String, Asset> = HashMap::new();
    for asset in cands.list.iter().map(|(a, _)| a).chain(cp.found.iter()) {
        if let Some(contract) = &asset.contract {
            known.entry(contract.to_lowercase()).or_insert_with(|| asset.clone());
        }
    }
    let unknown: Vec<Hit> = hits.iter().filter(|h| !known.contains_key(&h.contract().to_lowercase())).cloned().collect();
    let described = describe(cx, chain, unknown).await;
    for hit in &hits {
        let contract = hit.contract().to_lowercase();
        let Some(info) = known.get(&contract).map(Info::from_asset).or_else(|| described.get(&contract).cloned()) else {
            continue;
        };
        let asset = match (hit, info.standard) {
            (Hit::Nft721(_, id), Standard::Erc721 | Standard::Erc404) | (Hit::Nft1155(_, id), Standard::Erc1155) => {
                nft(chain, &contract, id, info.standard, info.symbol.clone(), info.name.clone(), false)
            }
            (Hit::Fungible(_) | Hit::Nft721(..), Standard::Erc20 | Standard::Erc404) => {
                fungible(chain, &contract, info.standard, info.symbol.clone(), info.name.clone(), info.decimals, false)
            }
            _ => continue,
        };
        remember(cp, &asset);
        cands.add(asset, false);
    }

    let amounts = read(cx, chain, &owner, head, &cands.list).await?;
    let mut out: Found = Vec::new();
    let mut collections = Vec::new();
    for ((asset, always), amount) in cands.list.into_iter().zip(amounts) {
        let held = amount != "0";
        if asset.is_nft() && !held {
            // Gone from the wallet: no need to keep asking.
            cp.found.retain(|a| a.id != asset.id);
        }
        if held && asset.standard == Standard::Erc721 && asset.token_id.is_none() {
            collections.push((asset.clone(), amount.clone()));
        }
        if always || held {
            out.push((asset, amount));
        }
    }
    for item in enumerate(cx, chain, &owner, head, &collections, &out).await {
        remember(cp, &item);
        out.push((item, "1".into()));
    }
    trim_found(cp);
    Ok(Some(out))
}

/// Keeps a found asset for the next rounds, once.
fn remember(cp: &mut Checkpoint, asset: &Asset) {
    if !cp.found.iter().any(|a| a.id == asset.id) {
        cp.found.push(asset.clone());
    }
}

/// Keeps the found list bounded, the oldest first out.
fn trim_found(cp: &mut Checkpoint) {
    let (nfts, fungibles): (Vec<Asset>, Vec<Asset>) = cp.found.drain(..).partition(Asset::is_nft);
    let skip_f = fungibles.len().saturating_sub(MAX_FOUND_FUNGIBLE);
    let skip_n = nfts.len().saturating_sub(MAX_FOUND_NFT);
    cp.found = fungibles.into_iter().skip(skip_f).chain(nfts.into_iter().skip(skip_n)).collect();
}

/// The latest block, from a node that is not behind the others.
async fn head(cx: &Ctx<'_>, chain: &EvmChain) -> Result<u64, String> {
    let opts = Opts { hedge: false, ..Opts::evm(chain.chain_id) };
    let mut last = "no node answered".to_string();
    for url in cx.pool.order(&chain.rpc) {
        match cx.pool.attempt(&url, "eth_blockNumber", &json!([]), opts).await {
            Ok(value) => match hex_u64(&value) {
                Some(head) if cx.pool.note_head(&chain.id, &url, head) => return Ok(head),
                Some(_) => last = "every node is behind".into(),
                None => last = "a node sent an unreadable block number".into(),
            },
            Err(f) => last = f.message,
        }
    }
    Err(format!("Could not reach {}: {last}", chain.name))
}

// --- transfer logs ----------------------------------------------------------

/// "This range is too wide for me", in the words public nodes use.
fn too_wide(f: &Failure) -> bool {
    let m = f.message.to_ascii_lowercase();
    matches!(f.code, Some(-32005 | -32602 | 35 | -32614))
        || ["range", "limit", "too many", "exceed", "more than", "larger than", "too large"].iter().any(|s| m.contains(s))
}

/// "I do not serve log queries like this at all."
fn refuses_logs(f: &Failure) -> bool {
    let m = f.message.to_ascii_lowercase();
    matches!(f.code, Some(-32701 | -32601))
        || ["specify an address", "not supported", "not allowed", "not available", "disabled", "int64"].iter().any(|s| m.contains(s))
}

fn parse_logs(logs: &Value, hits: &mut Vec<Hit>) {
    for log in logs.as_array().into_iter().flatten() {
        if hits.len() >= MAX_HITS {
            return;
        }
        if log.get("removed").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let Some(contract) = log.get("address").and_then(Value::as_str).map(str::to_lowercase) else { continue };
        let topics: Vec<&str> = log.get("topics").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str).collect();
        let data = log.get("data").and_then(Value::as_str).and_then(abi::unhex).unwrap_or_default();
        let hit_list: Vec<Hit> = match topics.first().copied() {
            Some(abi::TRANSFER) if topics.len() == 3 => vec![Hit::Fungible(contract)],
            Some(abi::TRANSFER) if topics.len() == 4 => abi::unhex(topics[3])
                .map(|w| vec![Hit::Nft721(contract, abi::word_dec(&w))])
                .unwrap_or_default(),
            Some(abi::TRANSFER_SINGLE) => data.get(..32).map(|w| vec![Hit::Nft1155(contract, abi::word_dec(w))]).unwrap_or_default(),
            Some(abi::TRANSFER_BATCH) => abi::uint_array(&data, 0, MAX_IDS_PER_LOG)
                .unwrap_or_default()
                .into_iter()
                .map(|id| Hit::Nft1155(contract.clone(), id))
                .collect(),
            _ => vec![],
        };
        for hit in hit_list {
            if !hits.contains(&hit) {
                hits.push(hit);
            }
        }
    }
}

async fn logs_range(cx: &Ctx<'_>, chain: &EvmChain, url: &str, topic: &str, from: u64, to: u64) -> Result<Vec<Hit>, Failure> {
    let opts = Opts { hedge: false, ..Opts::evm(chain.chain_id) };
    let range = |topics: Value| json!([{ "fromBlock": format!("0x{from:x}"), "toBlock": format!("0x{to:x}"), "topics": topics }]);
    let fungible_or_721 = range(json!([abi::TRANSFER, null, topic]));
    let multi = range(json!([[abi::TRANSFER_SINGLE, abi::TRANSFER_BATCH], null, null, topic]));
    let (a, b) = tokio::join!(
        cx.pool.attempt(url, "eth_getLogs", &fungible_or_721, opts),
        cx.pool.attempt(url, "eth_getLogs", &multi, opts),
    );
    let mut hits = Vec::new();
    parse_logs(&a?, &mut hits);
    parse_logs(&b?, &mut hits);
    Ok(hits)
}

/// Tokens moved into the wallet since the last scan. Best effort: many
/// public nodes refuse address-less log queries, and each node's limits are
/// learned as it is asked. The cursor only moves over ranges actually read.
async fn scan_logs(cx: &Ctx<'_>, chain: &EvmChain, owner: &str, cp: &mut Checkpoint, head: u64) -> Vec<Hit> {
    if cp.block == 0 || cp.block > head {
        // First run (or a node reset): start from now. What is already held
        // is found through the lists and the index.
        cp.block = head;
        return vec![];
    }
    let Some(topic) = abi::address_topic(owner) else { return vec![] };
    let capable: Vec<String> = cx.pool.order(&chain.rpc).into_iter().filter(|u| cx.pool.caps(u).logs != Some(false)).collect();
    if capable.is_empty() {
        cp.block = head;
        return vec![];
    }
    let mut from = (cp.block + 1).max(head.saturating_sub(MAX_BACKFILL));
    let mut hits = Vec::new();
    let mut chunks = 0;
    let mut node = 0;
    while from <= head && chunks < LOG_CHUNKS && node < capable.len() && hits.len() < MAX_HITS {
        let url = &capable[node];
        let span = match cx.pool.caps(url).log_span {
            0 => LOG_SPAN,
            known => known,
        };
        let to = (from + span - 1).min(head);
        match logs_range(cx, chain, url, &topic, from, to).await {
            Ok(found) => {
                hits.extend(found);
                cx.pool.set_caps(url, |c| {
                    c.logs = Some(true);
                    if c.log_span == 0 {
                        c.log_span = span;
                    }
                });
                cp.block = to;
                from = to + 1;
                chunks += 1;
            }
            Err(f) if refuses_logs(&f) => {
                cx.pool.set_caps(url, |c| c.logs = Some(false));
                node += 1;
            }
            Err(f) if too_wide(&f) && span > LOG_SPAN_MIN => {
                cx.pool.set_caps(url, |c| c.log_span = (span / 4).max(LOG_SPAN_MIN));
            }
            Err(f) if too_wide(&f) => {
                // Refused even the narrowest range: not a node for this.
                cx.pool.set_caps(url, |c| c.logs = Some(false));
                node += 1;
            }
            Err(_) => node += 1,
        }
    }
    hits.truncate(MAX_HITS);
    hits
}

// --- Blockscout (best effort) ---------------------------------------------

fn blockscout(chain: &EvmChain) -> Option<&str> {
    match &chain.history_api {
        Some(HistoryApi::Blockscout { base_url }) => Some(base_url.trim_end_matches('/')),
        _ => None,
    }
}

fn query_value(value: &Value) -> String {
    let text = value.as_str().map_or_else(|| value.to_string(), str::to_string);
    text.bytes()
        .map(|b| if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) { (b as char).to_string() } else { format!("%{b:02X}") })
        .collect()
}

fn text(value: Option<&Value>, max: usize) -> Option<String> {
    let s = abi::sanitize(value?.as_str()?)?;
    Some(s.chars().take(max).collect())
}

fn lenient_u8(value: Option<&Value>) -> Option<u8> {
    match value? {
        Value::String(s) => s.trim().parse().ok(),
        Value::Number(n) => n.as_u64().and_then(|n| u8::try_from(n).ok()),
        _ => None,
    }
}

async fn pages(cx: &Ctx<'_>, first: String) -> Vec<Value> {
    let mut items = Vec::new();
    let mut url = first.clone();
    for _ in 0..INDEX_PAGES {
        let Ok(page) = cx.pool.get(std::slice::from_ref(&url), INDEX_TIMEOUT).await else { break };
        items.extend(page.get("items").and_then(Value::as_array).cloned().unwrap_or_default());
        let Some(next) = page.get("next_page_params").and_then(Value::as_object) else { break };
        let query: Vec<String> = next.iter().map(|(k, v)| format!("{}={}", query_value(&json!(k)), query_value(v))).collect();
        url = format!("{first}&{}", query.join("&"));
    }
    items
}

/// What Blockscout says the wallet holds: candidates only, with their names.
/// Many instances are now behind a firewall or a key; then this finds nothing
/// and the host is benched for a long while.
async fn index(cx: &Ctx<'_>, chain: &EvmChain, base: String, owner: String) -> Vec<Asset> {
    let tokens = pages(cx, format!("{base}/api/v2/addresses/{owner}/tokens?type=ERC-20"));
    let nfts = pages(cx, format!("{base}/api/v2/addresses/{owner}/nft?type=ERC-721%2CERC-1155%2CERC-404"));
    let (tokens, nfts) = tokio::join!(tokens, nfts);
    let mut out = Vec::new();
    for item in tokens {
        let Some(token) = item.get("token") else { continue };
        let Some(contract) = token.get("address_hash").or_else(|| token.get("address")).and_then(Value::as_str) else { continue };
        if abi::address_word(contract).is_none() {
            continue;
        }
        let Some(decimals) = lenient_u8(token.get("decimals")).filter(|d| *d <= 36) else { continue };
        let symbol = text(token.get("symbol"), 32).unwrap_or_else(|| short(contract));
        let name = text(token.get("name"), 64).unwrap_or_else(|| symbol.clone());
        let flagged = token.get("reputation").and_then(Value::as_str).is_some_and(|r| r != "ok");
        out.push(fungible(chain, contract, Standard::Erc20, symbol, name, decimals, flagged));
    }
    for item in nfts {
        let Some(token) = item.get("token") else { continue };
        let Some(contract) = token.get("address_hash").or_else(|| token.get("address")).and_then(Value::as_str) else { continue };
        let Some(id) = item.get("id").and_then(Value::as_str).filter(|id| abi::dec_word(id).is_some()) else { continue };
        let standard = match token.get("type").and_then(Value::as_str) {
            Some("ERC-721") => Standard::Erc721,
            Some("ERC-1155") => Standard::Erc1155,
            Some("ERC-404") => Standard::Erc404,
            _ => continue,
        };
        if abi::address_word(contract).is_none() {
            continue;
        }
        let symbol = text(token.get("symbol"), 32).unwrap_or_else(|| short(contract));
        let name = text(token.get("name"), 64).unwrap_or_else(|| symbol.clone());
        let flagged = token.get("reputation").and_then(Value::as_str).is_some_and(|r| r != "ok");
        out.push(nft(chain, contract, id, standard, symbol, name, flagged));
    }
    out
}

// --- reading from the chain -----------------------------------------------

/// What a contract says it is.
#[derive(Clone, Debug, PartialEq)]
struct Info {
    standard: Standard,
    symbol: String,
    name: String,
    decimals: u8,
}

impl Info {
    fn from_asset(asset: &Asset) -> Info {
        Info { standard: asset.standard, symbol: asset.symbol.clone(), name: asset.name.clone(), decimals: asset.decimals }
    }
}

/// The kind of a contract from its answers. ERC-165 first (an ERC-721 that
/// also answers `decimals()` is not fungible), then what its transfers showed,
/// then `decimals()` for ERC-20.
fn kind(erc721: Option<bool>, erc1155: Option<bool>, decimals: Option<u8>, hint: &Hit) -> Option<Standard> {
    if erc721 == Some(true) {
        return Some(if decimals.is_some_and(|d| d > 0) { Standard::Erc404 } else { Standard::Erc721 });
    }
    if erc1155 == Some(true) {
        return Some(Standard::Erc1155);
    }
    match hint {
        // BAYC answers false to every interface id: its transfers say what it is.
        Hit::Nft721(..) => Some(Standard::Erc721),
        Hit::Nft1155(..) => Some(Standard::Erc1155),
        Hit::Fungible(_) => decimals.map(|_| Standard::Erc20),
    }
}

fn interface(id: [u8; 4]) -> abi::Word {
    let mut word = [0u8; 32];
    word[..4].copy_from_slice(&id);
    word
}

/// Names and kinds of contracts seen for the first time.
async fn describe(cx: &Ctx<'_>, chain: &EvmChain, hits: Vec<Hit>) -> HashMap<String, Info> {
    let mut contracts: Vec<(String, Hit)> = Vec::new();
    for hit in hits {
        if !contracts.iter().any(|(c, _)| c.eq_ignore_ascii_case(hit.contract())) {
            contracts.push((hit.contract().to_lowercase(), hit));
        }
    }
    let mut calls = Vec::new();
    for (contract, _) in &contracts {
        for data in [
            abi::call(abi::SUPPORTS_INTERFACE, &[interface(abi::ERC721)]),
            abi::call(abi::SUPPORTS_INTERFACE, &[interface(abi::ERC1155)]),
            abi::call(abi::NAME, &[]),
            abi::call(abi::SYMBOL, &[]),
            abi::call(abi::DECIMALS, &[]),
        ] {
            calls.push(Call { target: contract.clone(), data });
        }
    }
    let Ok(answers) = run(cx, chain, &calls, None, "latest").await else { return HashMap::new() };
    let mut out = HashMap::new();
    for ((contract, hint), a) in contracts.iter().zip(answers.chunks(5)) {
        let ok = |i: usize| a.get(i).and_then(|r| r.as_ref());
        let decimals = ok(4).and_then(|d| abi::decode_decimals(d));
        let Some(standard) = kind(ok(0).and_then(|d| abi::decode_bool(d)), ok(1).and_then(|d| abi::decode_bool(d)), decimals, hint) else {
            continue;
        };
        let symbol = ok(3).and_then(|d| abi::decode_string(d)).unwrap_or_else(|| short(contract));
        let name = ok(2).and_then(|d| abi::decode_string(d)).unwrap_or_else(|| symbol.clone());
        let decimals = if matches!(standard, Standard::Erc20 | Standard::Erc404) { decimals.unwrap_or(0) } else { 0 };
        out.insert(contract.to_lowercase(), Info { standard, symbol, name, decimals });
    }
    out
}

/// The call that reads how much of `asset` the owner holds, and how to read
/// its answer.
/// `None` for an asset that cannot be asked (a malformed address): it reads
/// as 0 rather than sinking the whole batch.
fn balance_call(asset: &Asset, owner: &str, owner_word: abi::Word) -> Option<(Call, Answer)> {
    let contract = asset.contract.clone().filter(|c| abi::address_word(c).is_some());
    Some(match (asset.standard, asset.token_id.as_deref()) {
        (Standard::Native, _) => (
            Call { target: abi::MULTICALL3.into(), data: abi::call(abi::GET_ETH_BALANCE, &[owner_word]) },
            Answer::Amount,
        ),
        (Standard::Erc721 | Standard::Erc404, Some(id)) => (
            Call { target: contract?, data: abi::call(abi::OWNER_OF, &[abi::dec_word(id)?]) },
            Answer::Owner(owner.to_string()),
        ),
        (Standard::Erc1155, Some(id)) => (
            Call { target: contract?, data: abi::call(abi::BALANCE_OF_ID, &[owner_word, abi::dec_word(id)?]) },
            Answer::Amount,
        ),
        _ => (Call { target: contract?, data: abi::call(abi::BALANCE_OF, &[owner_word]) }, Answer::Amount),
    })
}

enum Answer {
    Amount,
    /// ERC-721 `ownerOf`: 1 when it names the owner, else 0.
    Owner(String),
}

impl Answer {
    fn read(&self, data: Option<&Vec<u8>>) -> String {
        match (self, data) {
            (Answer::Amount, Some(d)) => abi::amount(d),
            (Answer::Owner(owner), Some(d)) => {
                let named = d.get(..32).and_then(abi::word_address);
                if named.as_deref() == Some(owner.as_str()) { "1" } else { "0" }.into()
            }
            // The contract refused (or the address has no code): it holds none.
            (_, None) => "0".into(),
        }
    }
}

/// The amount of each candidate, in order, all at block `head`.
async fn read(cx: &Ctx<'_>, chain: &EvmChain, owner: &str, head: u64, cands: &[(Asset, bool)]) -> Result<Vec<String>, String> {
    let owner_word = abi::address_word(owner).ok_or("not an EVM address")?;
    let mut calls = Vec::new();
    let mut answers = Vec::new();
    let mut index = Vec::new();
    for (i, (asset, _)) in cands.iter().enumerate() {
        if let Some((call, answer)) = balance_call(asset, owner, owner_word) {
            calls.push(call);
            answers.push(answer);
            index.push(i);
        }
    }
    let results = run(cx, chain, &calls, Some(owner), &format!("0x{head:x}")).await?;
    let mut out = vec!["0".to_string(); cands.len()];
    for ((i, answer), result) in index.into_iter().zip(answers).zip(results) {
        out[i] = answer.read(result.as_ref());
    }
    Ok(out)
}

/// Ids of ERC-721 collections held without any known id, from ERC-721
/// Enumerable where the contract has it.
async fn enumerate(cx: &Ctx<'_>, chain: &EvmChain, owner: &str, head: u64, collections: &[(Asset, String)], out: &Found) -> Vec<Asset> {
    let Some(owner_word) = abi::address_word(owner) else { return vec![] };
    let mut calls = Vec::new();
    let mut of = Vec::new();
    for (collection, count) in collections {
        let contract = collection.contract.clone().unwrap_or_default();
        let known = out.iter().any(|(a, _)| a.token_id.is_some() && a.contract.as_deref() == Some(contract.to_lowercase().as_str()));
        if known {
            continue;
        }
        let count: usize = count.parse().unwrap_or(0).min(ENUMERATE_MAX);
        for i in 0..count {
            calls.push(Call { target: contract.clone(), data: abi::call(abi::TOKEN_OF_OWNER_BY_INDEX, &[owner_word, abi::usize_word(i)]) });
            of.push(collection);
        }
    }
    if calls.is_empty() {
        return vec![];
    }
    let Ok(results) = run(cx, chain, &calls, None, &format!("0x{head:x}")).await else { return vec![] };
    of.into_iter()
        .zip(results)
        .filter_map(|(c, r)| {
            let id = abi::amount(&r?);
            Some(nft(chain, c.contract.as_deref()?, &id, Standard::Erc721, c.symbol.clone(), c.name.clone(), false))
        })
        .collect()
}

/// Runs calls at one block. `Some(data)` = the contract answered, `None` =
/// it refused. A node that cannot answer fails the whole batch: a wrong zero
/// is worse than no answer.
async fn run(cx: &Ctx<'_>, chain: &EvmChain, calls: &[Call], owner: Option<&str>, block: &str) -> Result<Vec<Option<Vec<u8>>>, String> {
    if calls.is_empty() {
        return Ok(vec![]);
    }
    let opts = Opts::evm(chain.chain_id);
    let multicall = match cx.pool.multicall(chain.chain_id) {
        Some(known) => known,
        None => match cx.pool.call(&chain.rpc, "eth_getCode", &json!([abi::MULTICALL3, "latest"]), opts).await {
            Ok(code) => {
                let deployed = code.as_str().is_some_and(|c| c.len() > 2);
                cx.pool.set_multicall(chain.chain_id, deployed);
                deployed
            }
            Err(_) => false,
        },
    };
    if multicall {
        let mut out = Vec::with_capacity(calls.len());
        let mut fallback = false;
        for chunk in calls.chunks(MULTICALL_CHUNK) {
            let data = abi::aggregate3(chunk).ok_or("bad call")?;
            let params = json!([{ "to": abi::MULTICALL3, "data": abi::hex(&data) }, block]);
            match cx.pool.call(&chain.rpc, "eth_call", &params, opts).await {
                Ok(ret) => {
                    let bytes = ret.as_str().and_then(abi::unhex).ok_or("unreadable multicall answer")?;
                    let results = abi::decode_aggregate3(&bytes).filter(|r| r.len() == chunk.len()).ok_or("unreadable multicall answer")?;
                    out.extend(results.into_iter().map(|(ok, data)| ok.then_some(data)));
                }
                Err(f) if f.trouble => return Err(format!("Could not reach {}: {}", chain.name, f.message)),
                Err(_) => {
                    // Reverted: no working Multicall3 here after all.
                    cx.pool.set_multicall(chain.chain_id, false);
                    fallback = true;
                    break;
                }
            }
        }
        if !fallback {
            return Ok(out);
        }
    }
    let results: Vec<Result<Option<Vec<u8>>, String>> = stream::iter(calls.iter().cloned())
        .map(|call: Call| {
            async move {
            let (method, params) = if call.target.eq_ignore_ascii_case(abi::MULTICALL3) && call.data.starts_with(&abi::GET_ETH_BALANCE) {
                ("eth_getBalance", json!([owner.unwrap_or_default(), block]))
            } else {
                ("eth_call", json!([{ "to": call.target, "data": abi::hex(&call.data) }, block]))
            };
            match cx.pool.call(&chain.rpc, method, &params, opts).await {
                Ok(ret) if method == "eth_getBalance" => {
                    let n = hex_u64_big(&ret);
                    Ok(Some(n))
                }
                Ok(ret) => Ok(ret.as_str().and_then(abi::unhex)),
                Err(f) if f.trouble => Err(format!("Could not reach {}: {}", chain.name, f.message)),
                Err(_) => Ok(None),
            }
            }
            .boxed()
        })
        .buffered(PARALLEL_CALLS)
        .collect()
        .await;
    results.into_iter().collect()
}

/// A hex quantity (`0x1bc16d674ec80000`) as a 32-byte word.
fn hex_u64_big(value: &Value) -> Vec<u8> {
    let digits = value.as_str().unwrap_or("0x0").trim_start_matches("0x");
    let padded = format!("{digits:0>64}");
    abi::unhex(&padded[padded.len().saturating_sub(64)..]).unwrap_or_else(|| vec![0; 32])
}

// --- "Add custom token" ---------------------------------------------------

/// Name, symbol and decimals of a fungible token.
type Meta = (String, String, u8);

/// What `token` found. Pure, so it is testable.
fn classify_contract(
    erc721: Option<bool>,
    erc1155: Option<bool>,
    owner_of: bool,
    name: Option<String>,
    symbol: Option<String>,
    decimals: Option<u8>,
    total_supply: bool,
) -> Result<(Standard, Option<Meta>), String> {
    if erc721 == Some(true) {
        return Ok((Standard::Erc721, None));
    }
    if erc1155 == Some(true) {
        return Ok((Standard::Erc1155, None));
    }
    // ERC-165 is optional in practice (BAYC answers false to everything):
    // `ownerOf(0)` answering is the fallback signal.
    if owner_of {
        return Ok((Standard::Erc721, None));
    }
    match (name, symbol, decimals, total_supply) {
        (Some(name), Some(symbol), Some(decimals), true) => Ok((Standard::Erc20, Some((name, symbol, decimals)))),
        // A name and symbol but no decimals: almost certainly an NFT.
        (Some(_), Some(_), None, true) => Ok((Standard::Erc721, None)),
        _ => Err("no token contract at that address — check the chain and contract".into()),
    }
}

/// Reads a contract's kind, name, symbol and decimals, for adding it by hand.
pub(crate) async fn token(pool: &crate::rpc::Pool, chain: &EvmChain, contract: &str) -> Result<Asset, String> {
    let contract = contract.trim();
    if abi::address_word(contract).is_none() {
        return Err("contract address must be 0x + 40 hex chars".into());
    }
    let calls = [
        abi::call(abi::SUPPORTS_INTERFACE, &[interface(abi::ERC721)]),
        abi::call(abi::SUPPORTS_INTERFACE, &[interface(abi::ERC1155)]),
        abi::call(abi::OWNER_OF, &[[0u8; 32]]),
        abi::call(abi::NAME, &[]),
        abi::call(abi::SYMBOL, &[]),
        abi::call(abi::DECIMALS, &[]),
        abi::call(abi::TOTAL_SUPPLY, &[]),
    ];
    let opts = Opts::evm(chain.chain_id);
    let answers = futures_util::future::join_all(calls.iter().map(|data| {
        let params = json!([{ "to": contract, "data": abi::hex(data) }, "latest"]);
        async move { pool.call(&chain.rpc, "eth_call", &params, opts).await }
    }))
    .await;
    if let Some(f) = answers.iter().filter_map(|a| a.as_ref().err()).find(|f| f.trouble) {
        if answers.iter().all(Result::is_err) {
            return Err(format!("Could not reach {}: {}", chain.name, f.message));
        }
    }
    let bytes: Vec<Option<Vec<u8>>> = answers.into_iter().map(|a| a.ok().and_then(|v| v.as_str().and_then(abi::unhex))).collect();
    let got = |i: usize| bytes[i].as_deref().filter(|b| !b.is_empty());
    let (standard, meta) = classify_contract(
        got(0).and_then(abi::decode_bool),
        got(1).and_then(abi::decode_bool),
        got(2).is_some(),
        got(3).and_then(abi::decode_string),
        got(4).and_then(abi::decode_string),
        got(5).and_then(abi::decode_decimals),
        got(6).is_some(),
    )?;
    let (name, symbol, decimals) = meta.unwrap_or_else(|| (short(contract), short(contract), 0));
    Ok(fungible(chain, contract, standard, symbol, name, decimals, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::tests::{ok, rpc_error};
    use crate::rpc::{FakeReply, Pool};
    use std::sync::{Arc, Mutex};

    fn chain() -> EvmChain {
        let json = r#"{"kind":"evm","id":"testnet-x","name":"X","chainId":777,"slip44":60,
            "derivationPath":"m/44'/60'/0'/0/0","nativeCurrency":{"symbol":"XT","name":"X Token","decimals":18},
            "rpc":["https://a.example/rpc","https://b.example/rpc"]}"#;
        registry::loader::parse_evm_chain(json, false).unwrap()
    }

    const OWNER: &str = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";

    fn method(body: Option<&Value>) -> String {
        body.and_then(|b| b["method"].as_str()).unwrap_or("").to_string()
    }

    /// A chain without Multicall3: balances come call by call.
    fn plain_node(head: u64, balances: HashMap<String, String>) -> impl Fn(&str, Option<&Value>) -> FakeReply + Send + Sync {
        move |_url, body| {
            let body = body.cloned().unwrap_or_default();
            match method(Some(&body)).as_str() {
                "eth_chainId" => ok(json!("0x309")),
                "eth_blockNumber" => ok(json!(format!("0x{head:x}"))),
                "eth_getCode" => ok(json!("0x")),
                "eth_getBalance" => ok(json!("0xde0b6b3a7640000")),
                "eth_getLogs" => rpc_error(-32701, "Please specify an address in your request"),
                "eth_call" => {
                    let to = body["params"][0]["to"].as_str().unwrap_or("").to_lowercase();
                    match balances.get(&to) {
                        Some(n) => ok(json!(abi::hex(&abi::dec_word(n).unwrap()))),
                        None => rpc_error(3, "execution reverted"),
                    }
                }
                _ => rpc_error(-32601, "method not found"),
            }
        }
    }

    fn token(address: &str, symbol: &str) -> TokenInfo {
        TokenInfo {
            address: address.into(),
            symbol: symbol.into(),
            name: symbol.into(),
            decimals: 6,
            token_type: TokenType::Erc20,
            coingecko_id: None,
            icon: None,
            custom: false,
            discovered: false,
        }
    }

    #[tokio::test]
    async fn without_multicall_each_balance_is_its_own_call_and_zero_found_tokens_are_not_shown() {
        let usdc = "0x1111111111111111111111111111111111111111";
        let found = "0x2222222222222222222222222222222222222222";
        let balances = HashMap::from([(usdc.to_string(), "2500000".to_string()), (found.to_string(), "0".to_string())]);
        let pool = Pool::fake(Arc::new(plain_node(100, balances)));
        let evm = chain();
        let info = registry::model::ChainInfo::Evm(evm.clone());
        let tokens = [token(usdc, "USDC")];
        let cx = Ctx { pool: &pool, chain: &info, address: OWNER, tokens: &tokens, full: true };
        let mut cp = Checkpoint { found: vec![fungible(&evm, found, Standard::Erc20, "F".into(), "F".into(), 6, false)], ..Default::default() };
        let mut pr = Progress::default();
        let out = poll(&cx, &evm, &mut cp, &mut pr).await.unwrap().unwrap();
        let amounts: Vec<(&str, &str)> = out.iter().map(|(a, n)| (a.id.as_str(), n.as_str())).collect();
        assert_eq!(amounts, [("evm:777:native", "1000000000000000000"), ("evm:777:0x1111111111111111111111111111111111111111", "2500000")]);
        assert_eq!(cp.block, 100, "first run starts the log cursor at the head");
        assert_eq!(pool.multicall(777), Some(false));
    }

    /// A listed token with a malformed address (a digit too many) reads as 0;
    /// the rest of the batch, through Multicall3, still answers.
    #[tokio::test]
    async fn a_malformed_token_address_does_not_sink_the_batch() {
        let pool = Pool::fake(Arc::new(|_url, body| {
            let body = body.cloned().unwrap_or_default();
            match method(Some(&body)).as_str() {
                "eth_chainId" => ok(json!("0x309")),
                "eth_blockNumber" => ok(json!("0x10")),
                "eth_getCode" => ok(json!("0x6080")),
                "eth_call" => {
                    // Every call in the batch answers 7.
                    let data = abi::unhex(body["params"][0]["data"].as_str().unwrap()).unwrap();
                    let count = abi::decode_aggregate3_count(&data);
                    let mut ret = abi::usize_word(32).to_vec();
                    ret.extend_from_slice(&abi::usize_word(count));
                    for i in 0..count {
                        ret.extend_from_slice(&abi::usize_word(32 * count + i * 128));
                    }
                    for _ in 0..count {
                        for w in [1usize, 64, 32, 7] {
                            ret.extend_from_slice(&abi::usize_word(w));
                        }
                    }
                    ok(json!(abi::hex(&ret)))
                }
                _ => rpc_error(-32601, "method not found"),
            }
        }));
        let evm = chain();
        let info = registry::model::ChainInfo::Evm(evm.clone());
        let tokens = [token("0x94b008aA00579c1307B0EF2c499aD197aE3a7b183", "BAD"), token("0x1111111111111111111111111111111111111111", "OK")];
        let cx = Ctx { pool: &pool, chain: &info, address: OWNER, tokens: &tokens, full: false };
        let out = poll(&cx, &evm, &mut Checkpoint::default(), &mut Progress::default()).await.unwrap().unwrap();
        let amounts: Vec<(&str, &str)> = out.iter().map(|(a, n)| (a.symbol.as_str(), n.as_str())).collect();
        assert_eq!(amounts, [("XT", "7"), ("BAD", "0"), ("OK", "7")]);
    }

    #[tokio::test]
    async fn a_node_that_cannot_answer_fails_the_round_instead_of_reporting_zero() {
        let pool = Pool::fake(Arc::new(|_url, body| match method(body).as_str() {
            "eth_chainId" => ok(json!("0x309")),
            "eth_blockNumber" => ok(json!("0x10")),
            "eth_getCode" => ok(json!("0x")),
            _ => FakeReply { status: 503, body: "busy".into(), delay_ms: 0 },
        }));
        let evm = chain();
        let info = registry::model::ChainInfo::Evm(evm.clone());
        let cx = Ctx { pool: &pool, chain: &info, address: OWNER, tokens: &[], full: true };
        let err = poll(&cx, &evm, &mut Checkpoint::default(), &mut Progress::default()).await.unwrap_err();
        assert!(err.contains("Could not reach X"), "{err}");
    }

    #[tokio::test]
    async fn logs_shrink_on_too_wide_and_a_refusing_node_is_skipped() {
        let asked = Arc::new(Mutex::new(Vec::<(String, u64)>::new()));
        let seen = asked.clone();
        let pool = Pool::fake(Arc::new(move |url, body| {
            let body = body.cloned().unwrap_or_default();
            match method(Some(&body)).as_str() {
                "eth_chainId" => ok(json!("0x309")),
                "eth_getLogs" if url.contains("a.example") => rpc_error(-32701, "Please specify an address"),
                "eth_getLogs" => {
                    let p = &body["params"][0];
                    let from = hex_u64(&p["fromBlock"]).unwrap();
                    let to = hex_u64(&p["toBlock"]).unwrap();
                    seen.lock().unwrap().push((url.to_string(), to - from + 1));
                    if to - from + 1 > 100 {
                        rpc_error(-32005, "limit exceeded")
                    } else if p["topics"][0] == json!(abi::TRANSFER) && from == 401 {
                        ok(json!([{ "address": "0xAAAAaaaaAAAAaaaaAAAAaaaaAAAAaaaaAAAAaaaa", "topics": [abi::TRANSFER, "0x0", "0x0"], "data": "0x" },
                                  { "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "topics": [abi::TRANSFER, "0x0", "0x0", "0x07"], "data": "0x" }]))
                    } else {
                        ok(json!([]))
                    }
                }
                _ => rpc_error(-32601, "method not found"),
            }
        }));
        let evm = chain();
        let info = registry::model::ChainInfo::Evm(evm.clone());
        let cx = Ctx { pool: &pool, chain: &info, address: OWNER, tokens: &[], full: false };
        let mut cp = Checkpoint { block: 400, ..Default::default() };
        let hits = scan_logs(&cx, &evm, OWNER, &mut cp, 1000).await;
        assert_eq!(pool.caps("https://a.example/rpc").logs, Some(false));
        assert_eq!(pool.caps("https://b.example/rpc").log_span, 31, "500 / 4 / 4");
        assert_eq!(cp.block, 555, "five chunks of 31 blocks read, the cursor only over those");
        assert_eq!(
            hits,
            [Hit::Fungible("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()), Hit::Nft721("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(), "7".into())]
        );
    }

    #[tokio::test]
    async fn an_nft_no_longer_owned_leaves_the_found_list() {
        let other = "0x0000000000000000000000000000000000000001";
        let pool = Pool::fake(Arc::new(move |_url, body| {
            let body = body.cloned().unwrap_or_default();
            match method(Some(&body)).as_str() {
                "eth_chainId" => ok(json!("0x309")),
                "eth_blockNumber" => ok(json!("0x64")),
                "eth_getCode" => ok(json!("0x")),
                "eth_getBalance" => ok(json!("0x0")),
                "eth_getLogs" => ok(json!([])),
                "eth_call" => ok(json!(abi::hex(&abi::address_word(other).unwrap()))),
                _ => rpc_error(-32601, "method not found"),
            }
        }));
        let evm = chain();
        let info = registry::model::ChainInfo::Evm(evm.clone());
        let cx = Ctx { pool: &pool, chain: &info, address: OWNER, tokens: &[], full: true };
        let ape = nft(&evm, "0x3333333333333333333333333333333333333333", "7", Standard::Erc721, "APE".into(), "Ape".into(), false);
        let mut cp = Checkpoint { block: 100, found: vec![ape], ..Default::default() };
        let out = poll(&cx, &evm, &mut cp, &mut Progress::default()).await.unwrap().unwrap();
        assert_eq!(out.len(), 1, "only the native coin is reported");
        assert!(cp.found.is_empty(), "ownerOf names someone else");
    }

    #[test]
    fn contract_kinds_follow_erc165_then_transfers_then_decimals() {
        let fungible_hit = Hit::Fungible("0x1".into());
        let nft_hit = Hit::Nft721("0x1".into(), "1".into());
        assert_eq!(kind(Some(true), None, Some(18), &fungible_hit), Some(Standard::Erc404));
        assert_eq!(kind(Some(true), None, None, &fungible_hit), Some(Standard::Erc721));
        assert_eq!(kind(Some(false), Some(true), None, &fungible_hit), Some(Standard::Erc1155));
        assert_eq!(kind(Some(false), Some(false), None, &nft_hit), Some(Standard::Erc721), "BAYC");
        assert_eq!(kind(None, None, Some(6), &fungible_hit), Some(Standard::Erc20));
        assert_eq!(kind(None, None, None, &fungible_hit), None);

        let named = || (Some("Ape".to_string()), Some("APE".to_string()));
        assert_eq!(classify_contract(Some(true), None, false, named().0, named().1, Some(18), true).unwrap().0, Standard::Erc721);
        assert_eq!(classify_contract(Some(false), Some(false), true, None, None, None, false).unwrap().0, Standard::Erc721);
        assert_eq!(classify_contract(None, None, false, named().0, named().1, None, true).unwrap().0, Standard::Erc721);
        assert_eq!(classify_contract(None, Some(true), false, None, None, None, false).unwrap().0, Standard::Erc1155);
        let erc20 = classify_contract(None, None, false, named().0, named().1, Some(6), true).unwrap();
        assert_eq!(erc20, (Standard::Erc20, Some(("Ape".into(), "APE".into(), 6))));
        assert!(classify_contract(None, None, false, named().0, None, Some(6), true).is_err());
        assert!(classify_contract(None, None, false, None, None, None, false).is_err());
    }
}
