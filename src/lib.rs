//! Balances and discovery for a non-custodial wallet: every coin, token and
//! NFT an address holds, on every chain in the registry, kept live.
//!
//! - **Addresses only.** The engine never sees a key.
//! - **No API keys.** Public RPCs (several per chain, with failover),
//!   Multicall3, Esplora, the Solana node's own DAS, Blockscout when it answers.
//! - **The index suggests, the chain decides.** Lists, transfer logs and
//!   indexes only name candidates; every amount reported is read from the
//!   chain in that round. A failed read is an error, never a zero.
//! - **Brand-neutral.** The wallet passes its own User-Agent.
//!
//! One module per chain family (`evm`, `solana`, `bitcoin`) with the same
//! entry point; a new family is one module and one arm in `poll` and
//! `cadence`.
//!
//! ```no_run
//! # async fn demo(chain: registry::model::ChainInfo) {
//! use balance_engine::{Engine, Target, Checkpoint};
//! let engine = Engine::new("My Wallet/1.0");
//! let target = Target { chain, address: "0x…".into(), tokens: vec![], checkpoint: Checkpoint::default() };
//! for update in engine.scan(&[target]).await {
//!     println!("{} holds {} assets", update.address, update.holdings.len());
//! }
//! # }
//! ```

mod abi;
mod bitcoin;
mod evm;
pub mod model;
mod rpc;
mod solana;
pub mod spam;
mod watch;

pub use model::{Asset, Checkpoint, Holding, Reason, Standard, Target, Trust, Update};
pub use spam::looks_like_spam;
pub use watch::{OnUpdate, Watch};

use futures_util::future::{BoxFuture, FutureExt};
use registry::model::{ChainInfo, EvmChain, TokenInfo};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Longest a round may take before it counts as failed.
const ROUND_DEADLINE: Duration = Duration::from_secs(30);

pub(crate) type Found = Vec<(Asset, String)>;

/// What one round needs to know.
pub(crate) struct Ctx<'a> {
    pub pool: &'a rpc::Pool,
    pub chain: &'a ChainInfo,
    pub address: &'a str,
    pub tokens: &'a [TokenInfo],
    /// A full pass: popular lists, found assets and indexes too.
    pub full: bool,
}

/// Where a target's polling stands between rounds. Not persisted.
#[derive(Default, Debug)]
pub(crate) struct Progress {
    /// EVM: the head read last round.
    pub head: u64,
    /// Solana: the highest slot an answer came from.
    pub slot: u64,
    pub last: Vec<model::Holding>,
    pub last_full: Option<Instant>,
    pub error: Option<String>,
    /// Solana: mints already looked up by name this session.
    pub looked_up: std::collections::HashSet<String>,
}

pub(crate) fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// How often a family is polled while live.
pub(crate) fn cadence(chain: &ChainInfo) -> Duration {
    match chain {
        ChainInfo::Evm(_) => Duration::from_secs(15),
        ChainInfo::Solana(_) => Duration::from_secs(20),
        ChainInfo::Bitcoin(_) => Duration::from_secs(60),
    }
}

/// One round on whichever family the chain is. Boxed where the lifetimes
/// are concrete, so the round can run in a spawned task (rust-lang/rust#102211).
fn poll<'a>(cx: &'a Ctx<'a>, cp: &'a mut Checkpoint, pr: &'a mut Progress) -> BoxFuture<'a, Result<Option<Found>, String>> {
    match cx.chain {
        ChainInfo::Evm(chain) => evm::poll(cx, chain, cp, pr).boxed(),
        ChainInfo::Solana(chain) => solana::poll(cx, chain, cp, pr).boxed(),
        ChainInfo::Bitcoin(chain) => bitcoin::poll(cx, chain).boxed(),
    }
}

/// Ids whose amount or presence differs between two rounds.
fn changed(before: &[Holding], after: &[Holding]) -> Vec<String> {
    let old: HashMap<&str, &str> = before.iter().map(|h| (h.asset.id.as_str(), h.amount.as_str())).collect();
    let new: HashMap<&str, &str> = after.iter().map(|h| (h.asset.id.as_str(), h.amount.as_str())).collect();
    let mut ids: Vec<String> = after
        .iter()
        .filter(|h| old.get(h.asset.id.as_str()) != Some(&h.amount.as_str()))
        .map(|h| h.asset.id.clone())
        .collect();
    ids.extend(before.iter().filter(|h| !new.contains_key(h.asset.id.as_str())).map(|h| h.asset.id.clone()));
    ids
}

struct Inner {
    pool: rpc::Pool,
    /// Last holdings per (chain, address), so a watch started after a scan
    /// does not report everything as new.
    last: Mutex<HashMap<String, Vec<Holding>>>,
}

/// The engine. Cheap to clone; clones share endpoint health and memory.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

fn key(target: &Target) -> String {
    format!("{}:{}", target.chain.id(), target.address)
}

impl Engine {
    /// `user_agent` names the wallet to the nodes and APIs it asks, e.g.
    /// `"Example Wallet/1.0"`.
    pub fn new(user_agent: &str) -> Engine {
        Engine { inner: Arc::new(Inner { pool: rpc::Pool::new(user_agent), last: Mutex::default() }) }
    }

    #[cfg(test)]
    pub(crate) fn with_pool(pool: rpc::Pool) -> Engine {
        Engine { inner: Arc::new(Inner { pool, last: Mutex::default() }) }
    }

    /// One full pass over every target, chains at once. Each result says
    /// whether it is fresh (`error` empty) or only the last good holdings.
    pub async fn scan(&self, targets: &[Target]) -> Vec<Update> {
        let rounds = targets.iter().map(|target| async move {
            let mut cp = target.checkpoint.clone();
            let mut pr = self.progress(target);
            self.round(target, &mut cp, &mut pr, true, true).await
        });
        futures_util::future::join_all(rounds).await.into_iter().flatten().collect()
    }

    /// Keeps the targets live until the `Watch` is dropped. Updates come only
    /// when something changed; `on_update` returning `false` stops that
    /// target's polling (the wallet was locked or switched).
    pub fn watch(&self, targets: Vec<Target>, on_update: OnUpdate) -> Watch {
        watch::start(self.clone(), targets, on_update)
    }

    /// Kind, name, symbol and decimals of an EVM contract, read from it, for
    /// adding a token by address.
    pub async fn evm_token(&self, chain: &EvmChain, contract: &str) -> Result<Asset, String> {
        evm::token(&self.inner.pool, chain, contract).await
    }

    pub(crate) fn progress(&self, target: &Target) -> Progress {
        let last = self.inner.last.lock().ok().and_then(|m| m.get(&key(target)).cloned()).unwrap_or_default();
        let last_full = (!last.is_empty()).then(Instant::now);
        Progress { last, last_full, ..Progress::default() }
    }

    /// One round for one target. Returns an update when something changed,
    /// or always when `always`.
    pub(crate) async fn round(&self, target: &Target, cp: &mut Checkpoint, pr: &mut Progress, full: bool, always: bool) -> Option<Update> {
        let cx = Ctx { pool: &self.inner.pool, chain: &target.chain, address: &target.address, tokens: &target.tokens, full };
        let before = cp.clone();
        let result = tokio::time::timeout(ROUND_DEADLINE, poll(&cx, cp, pr))
            .await
            .unwrap_or_else(|_| Err(format!("{} took too long to answer", target.chain.name())));
        let update = |holdings: Vec<Holding>, changed: Vec<String>, cp: &Checkpoint, error: Option<String>| Update {
            chain_id: target.chain.id().to_string(),
            address: target.address.clone(),
            holdings,
            changed,
            checkpoint: cp.clone(),
            error,
        };
        match result {
            Ok(None) => always.then(|| update(pr.last.clone(), vec![], cp, None)),
            Ok(Some(found)) => {
                let holdings: Vec<Holding> = found
                    .into_iter()
                    .map(|(asset, amount)| Holding { trust: spam::classify(&asset, &target.chain, &target.tokens), asset, amount })
                    .collect();
                let changed = changed(&pr.last, &holdings);
                let recovered = pr.error.take().is_some();
                if full {
                    pr.last_full = Some(Instant::now());
                }
                pr.last = holdings.clone();
                if let Ok(mut last) = self.inner.last.lock() {
                    last.insert(key(target), holdings.clone());
                }
                let quiet = changed.is_empty() && *cp == before && !recovered;
                (always || !quiet).then(|| update(holdings, changed, cp, None))
            }
            Err(error) => {
                let new = pr.error.as_deref() != Some(error.as_str());
                pr.error = Some(error.clone());
                (always || new).then(|| update(pr.last.clone(), vec![], cp, Some(error)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::tests::reply;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    fn bitcoin() -> ChainInfo {
        registry::chains::builtin_only().into_iter().find(|c| c.id() == "bitcoin").unwrap()
    }

    fn target() -> Target {
        Target { chain: bitcoin(), address: "bc1qtest".into(), tokens: vec![], checkpoint: Checkpoint::default() }
    }

    /// An Esplora whose balance the test sets.
    fn esplora(sats: Arc<AtomicU64>, asked: Arc<AtomicUsize>) -> rpc::Pool {
        rpc::Pool::fake(Arc::new(move |_, _| {
            asked.fetch_add(1, Ordering::SeqCst);
            let funded = sats.load(Ordering::SeqCst);
            if funded == u64::MAX {
                return reply(503, json!({}));
            }
            reply(200, json!({ "chain_stats": { "funded_txo_sum": funded, "spent_txo_sum": 0 } }))
        }))
    }

    #[tokio::test]
    async fn a_failed_round_keeps_the_last_holdings_and_says_so() {
        let sats = Arc::new(AtomicU64::new(5000));
        let engine = Engine::with_pool(esplora(sats.clone(), Arc::default()));
        let t = target();
        let first = engine.scan(std::slice::from_ref(&t)).await.remove(0);
        assert_eq!(first.holdings[0].amount, "5000");
        assert_eq!(first.holdings[0].trust, Trust::Verified);
        sats.store(u64::MAX, Ordering::SeqCst);
        let failed = engine.scan(std::slice::from_ref(&t)).await.remove(0);
        assert!(failed.error.is_some());
        assert_eq!(failed.holdings[0].amount, "5000", "the last good amount, not a zero");
    }

    #[tokio::test(start_paused = true)]
    async fn a_watch_reports_changes_only_and_obeys_kick_pause_and_stop() {
        let sats = Arc::new(AtomicU64::new(1000));
        let asked = Arc::new(AtomicUsize::new(0));
        let engine = Engine::with_pool(esplora(sats.clone(), asked.clone()));
        engine.scan(&[target()]).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Update>();
        let keep = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let keep_cb = keep.clone();
        let watch = engine.watch(vec![target()], Arc::new(move |u| {
            let _ = tx.send(u);
            keep_cb.load(Ordering::SeqCst)
        }));

        // Nothing changed: rounds run, no update.
        tokio::time::sleep(Duration::from_secs(200)).await;
        assert!(rx.try_recv().is_err());
        let before = asked.load(Ordering::SeqCst);
        assert!(before >= 3);

        // Paused: the network is not asked.
        watch.pause();
        tokio::time::sleep(Duration::from_secs(300)).await;
        let paused_at = asked.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(300)).await;
        assert_eq!(asked.load(Ordering::SeqCst), paused_at);

        // A deposit while paused shows straight after resume.
        sats.store(2500, Ordering::SeqCst);
        watch.resume();
        let update = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        assert_eq!(update.holdings[0].amount, "2500");
        assert_eq!(update.changed, ["bitcoin:native"]);

        // A kick does not wait for the cadence; a false answer stops the task.
        keep.store(false, Ordering::SeqCst);
        sats.store(3000, Ordering::SeqCst);
        watch.kick(Some("bitcoin"));
        let update = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await.unwrap().unwrap();
        assert_eq!(update.holdings[0].amount, "3000");
        sats.store(4000, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(600)).await;
        assert!(rx.try_recv().is_err(), "stopped after the callback said so");
        assert_eq!(watch.checkpoints().len(), 1);
    }
}
