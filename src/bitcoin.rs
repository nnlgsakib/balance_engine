//! Bitcoin: the balance of one address from Esplora-compatible APIs
//! (mempool.space, blockstream.info), one after another until one answers.

use crate::{
    model::{Asset, Standard},
    rpc::TIMEOUT,
    Ctx, Found,
};
use registry::model::{BitcoinChain, HistoryApi};
use serde_json::Value;

pub(crate) fn native(chain: &BitcoinChain) -> Asset {
    Asset {
        id: "bitcoin:native".into(),
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

/// The API bases to ask: the chain's own list, else its history source.
fn bases(chain: &BitcoinChain) -> Vec<String> {
    let mut bases: Vec<String> = chain.rpc.iter().map(|b| b.trim_end_matches('/').to_string()).collect();
    if let Some(HistoryApi::Esplora { base_url }) = &chain.history_api {
        let base = base_url.trim_end_matches('/').to_string();
        if !bases.contains(&base) {
            bases.push(base);
        }
    }
    bases
}

/// Confirmed plus unconfirmed: what was received minus what was spent, the
/// same as the sum of unspent outputs.
fn balance(stats: &Value) -> Option<u64> {
    let side = |key: &str| -> Option<i128> {
        let s = stats.get(key)?;
        Some(i128::from(s.get("funded_txo_sum")?.as_u64()?) - i128::from(s.get("spent_txo_sum")?.as_u64()?))
    };
    let total = side("chain_stats")? + side("mempool_stats").unwrap_or(0);
    Some(total.max(0) as u64)
}

pub(crate) async fn poll(cx: &Ctx<'_>, chain: &BitcoinChain) -> Result<Option<Found>, String> {
    let address = cx.address.trim();
    if address.is_empty() || !address.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err("not a Bitcoin address".into());
    }
    let urls: Vec<String> = bases(chain).into_iter().map(|b| format!("{b}/address/{address}")).collect();
    if urls.is_empty() {
        return Err(format!("{} has no API to read balances from", chain.name));
    }
    let stats = cx.pool.get(&urls, TIMEOUT).await.map_err(|e| format!("Could not reach {}: {e}", chain.name))?;
    let sats = balance(&stats).ok_or_else(|| format!("{} sent an unreadable balance", chain.name))?;
    Ok(Some(vec![(native(chain), sats.to_string())]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::tests::reply;
    use crate::rpc::Pool;
    use serde_json::json;
    use std::sync::Arc;

    fn chain() -> BitcoinChain {
        match registry::chains::builtin_only().into_iter().find(|c| c.id() == "bitcoin") {
            Some(registry::model::ChainInfo::Bitcoin(b)) => b,
            _ => panic!("bitcoin is built in"),
        }
    }

    #[test]
    fn the_registry_lists_two_apis_and_the_history_one_is_not_repeated() {
        let b = chain();
        assert_eq!(bases(&b), ["https://mempool.space/api", "https://blockstream.info/api"]);
    }

    #[tokio::test]
    async fn unconfirmed_coins_count_and_a_down_api_falls_over_to_the_next() {
        let pool = Pool::fake(Arc::new(|url, _| {
            if url.contains("mempool.space") {
                reply(502, json!({}))
            } else {
                reply(200, json!({
                    "chain_stats": { "funded_txo_sum": 150_000, "spent_txo_sum": 50_000 },
                    "mempool_stats": { "funded_txo_sum": 2_000, "spent_txo_sum": 500 }
                }))
            }
        }));
        let b = chain();
        let info = registry::model::ChainInfo::Bitcoin(b.clone());
        let cx = Ctx { pool: &pool, chain: &info, address: "bc1qxyz0123", tokens: &[], full: true };
        let out = poll(&cx, &b).await.unwrap().unwrap();
        assert_eq!(out[0].1, "101500");
        assert_eq!(balance(&json!({ "chain_stats": { "funded_txo_sum": 1, "spent_txo_sum": 5 } })), Some(0));
    }
}
