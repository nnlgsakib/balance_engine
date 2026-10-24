//! Every request the engine makes goes through one `Pool`: JSON-RPC to a
//! chain's nodes, and plain JSON GETs to Esplora and Blockscout.
//!
//! Public endpoints come and go, rate-limit and fall behind, so each call is
//! tried across all of a chain's endpoints:
//! - best first: not benched, fewest recent failures, then fastest;
//! - a node that rate-limits, errors or is blocked by a firewall sits out for
//!   a while (benched) and is asked last;
//! - a slow node is hedged: after `HEDGE_AFTER` the next one is asked too and
//!   the first answer wins;
//! - an error that is the request's own answer (a revert, bad params) is
//!   returned at once, never retried elsewhere;
//! - an EVM node is asked its chain id before its first use; one for another
//!   chain is never used for this one.

use futures_util::{
    future::{BoxFuture, FutureExt},
    stream::{FuturesUnordered, StreamExt},
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const CONNECT: Duration = Duration::from_secs(5);
pub(crate) const TIMEOUT: Duration = Duration::from_secs(10);
const HEDGE_AFTER: Duration = Duration::from_millis(1500);
/// Requests at once to one host. Public nodes limit per connection.
const PER_HOST: usize = 4;
const BENCH_ERROR: Duration = Duration::from_secs(30);
const BENCH_RATE_MAX: Duration = Duration::from_secs(120);
pub(crate) const BENCH_LAG: Duration = Duration::from_secs(60);
/// A firewall page (Cloudflare) does not go away in seconds.
const BENCH_BLOCKED: Duration = Duration::from_secs(30 * 60);
/// Largest answer read. Balances and token lists are far smaller.
const MAX_BODY: usize = 8 * 1024 * 1024;
/// Blocks a node may trail the best head seen before it is passed over.
const MAX_LAG: u64 = 3;

/// A failed request. `trouble` = the node could not answer (down, limited,
/// behind, blocked): another may. Otherwise the request itself was refused.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Failure {
    pub code: Option<i64>,
    pub message: String,
    pub trouble: bool,
}

impl Failure {
    fn trouble(code: Option<i64>, message: impl Into<String>) -> Failure {
        Failure { code, message: message.into(), trouble: true }
    }
}

/// JSON-RPC errors that describe the node, not the request (union of what
/// EVM and Solana nodes answer when busy, limited, behind or lacking a method).
fn is_node_trouble(code: Option<i64>, message: &str) -> bool {
    let busy = matches!(
        code,
        Some(-32601 | -32603 | -32005 | -32004 | -32014 | -32016 | -32009 | -32046 | -32002 | -32001 | -32701 | 429 | 403)
    );
    let m = message.to_ascii_lowercase();
    busy || ["header not found", "unknown block", "missing trie node", "rate limit", "too many requests", "capacity"]
        .iter()
        .any(|s| m.contains(s))
}

#[derive(Default)]
struct Health {
    fails: u32,
    /// Milliseconds, smoothed. 0 = never answered.
    latency: f64,
    benched_until: Option<Instant>,
    /// The last rate-limit bench, doubled on the next one.
    rate_bench: Duration,
}

/// What one endpoint has shown it can do. `None` = not tried yet.
#[derive(Default, Clone, Copy, Debug, PartialEq)]
pub(crate) struct Caps {
    /// Answers `eth_getLogs` without a contract address.
    pub logs: Option<bool>,
    /// Widest log range it took, in blocks. 0 = unknown.
    pub log_span: u64,
    /// Answers DAS (`getAssetsByOwner`).
    pub das: Option<bool>,
}

#[derive(Default)]
struct State {
    /// By host: a provider limits per client, whichever path is asked.
    health: HashMap<String, Health>,
    /// By full URL.
    caps: HashMap<String, Caps>,
    /// (URL, chain id) → whether the node said it is that chain.
    admitted: HashMap<(String, u64), bool>,
    /// Best head seen per chain (registry id).
    heads: HashMap<String, u64>,
    /// Chain id → Multicall3 is deployed.
    multicall: HashMap<u64, bool>,
}

struct Reply {
    status: u16,
    body: String,
    retry_after: Option<u64>,
}

#[cfg(test)]
pub(crate) struct FakeReply {
    pub status: u16,
    pub body: String,
    pub delay_ms: u64,
}

#[cfg(test)]
pub(crate) type Fake = Arc<dyn Fn(&str, Option<&Value>) -> FakeReply + Send + Sync>;

/// How one JSON-RPC call is made.
#[derive(Clone, Copy)]
pub(crate) struct Opts {
    pub timeout: Duration,
    /// Ask the next node too if the first is slow. Not for heavy reads
    /// (logs, DAS), which would double the load for nothing.
    pub hedge: bool,
    /// EVM chain id every node must report before it is used.
    pub chain: Option<u64>,
}

impl Opts {
    pub fn evm(chain_id: u64) -> Opts {
        Opts { timeout: TIMEOUT, hedge: true, chain: Some(chain_id) }
    }

    pub fn plain() -> Opts {
        Opts { timeout: TIMEOUT, hedge: true, chain: None }
    }
}

pub(crate) struct Pool {
    client: reqwest::Client,
    state: Mutex<State>,
    hosts: Mutex<HashMap<String, Arc<Semaphore>>>,
    #[cfg(test)]
    fake: Option<Fake>,
}

fn host(url: &str) -> String {
    url.split("://").nth(1).unwrap_or(url).split(['/', '?']).next().unwrap_or(url).to_ascii_lowercase()
}

fn looks_html(body: &str) -> bool {
    let start = body.trim_start();
    start.starts_with('<') && start.get(..64).unwrap_or(start).to_ascii_lowercase().contains("html")
}

impl Pool {
    pub fn new(user_agent: &str) -> Pool {
        let client = reqwest::Client::builder()
            // Several addresses per name, some unreachable (IPv6 with no
            // route): each gets a slice of this, then the next is tried.
            .connect_timeout(CONNECT)
            .timeout(TIMEOUT)
            .user_agent(user_agent)
            .build()
            .unwrap_or_default();
        Pool {
            client,
            state: Mutex::default(),
            hosts: Mutex::default(),
            #[cfg(test)]
            fake: None,
        }
    }

    #[cfg(test)]
    pub fn fake(fake: Fake) -> Pool {
        let mut pool = Pool::new("balance-engine-tests");
        pool.fake = Some(fake);
        pool
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    async fn permit(&self, url: &str) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let gate = {
            let mut hosts = self.hosts.lock().unwrap_or_else(|p| p.into_inner());
            hosts.entry(host(url)).or_insert_with(|| Arc::new(Semaphore::new(PER_HOST))).clone()
        };
        gate.acquire_owned().await.ok()
    }

    async fn send(&self, url: &str, body: Option<&Value>, timeout: Duration) -> Result<Reply, String> {
        #[cfg(test)]
        if let Some(fake) = &self.fake {
            let reply = fake(url, body);
            tokio::time::sleep(Duration::from_millis(reply.delay_ms)).await;
            if reply.status == 0 {
                return Err("could not connect".into());
            }
            return Ok(Reply { status: reply.status, body: reply.body, retry_after: None });
        }
        let request = match body {
            Some(body) => self.client.post(url).json(body),
            None => self.client.get(url).header("accept", "application/json"),
        };
        let response = request.timeout(timeout).send().await.map_err(|e| {
            if e.is_timeout() { "timed out" } else { "could not connect" }.to_string()
        })?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()?.trim().parse::<u64>().ok());
        // Read in pieces, up to a cap: a public index can answer with tens of
        // megabytes (NFTs with images inlined), more than a phone should hold.
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "the answer was cut off".to_string())? {
            if bytes.len() + chunk.len() > MAX_BODY {
                return Err("the answer was too large".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let body = String::from_utf8_lossy(&bytes).into_owned();
        Ok(Reply { status, body, retry_after })
    }

    // --- health -----------------------------------------------------------

    pub fn bench(&self, url: &str, for_: Duration) {
        let mut st = self.state();
        let h = st.health.entry(host(url)).or_default();
        h.fails += 1;
        h.benched_until = Some(Instant::now() + for_);
    }

    fn rate_limited(&self, url: &str, retry_after: Option<u64>) {
        let mut st = self.state();
        let h = st.health.entry(host(url)).or_default();
        let next = match retry_after {
            Some(secs) => Duration::from_secs(secs.clamp(1, 600)),
            None if h.rate_bench.is_zero() => BENCH_ERROR,
            None => (h.rate_bench * 2).min(BENCH_RATE_MAX),
        };
        h.rate_bench = next;
        h.fails += 1;
        h.benched_until = Some(Instant::now() + next);
    }

    fn answered(&self, url: &str, started: Instant) {
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        let mut st = self.state();
        let h = st.health.entry(host(url)).or_default();
        h.fails = 0;
        h.rate_bench = Duration::ZERO;
        h.benched_until = None;
        h.latency = if h.latency == 0.0 { ms } else { h.latency * 0.7 + ms * 0.3 };
    }

    fn failed(&self, url: &str) {
        self.state().health.entry(host(url)).or_default().fails += 1;
    }

    /// The endpoints in the order to ask them. Benched ones stay, last, so a
    /// chain whose every node is benched still gets an answer when one recovers.
    pub fn order(&self, urls: &[String]) -> Vec<String> {
        let st = self.state();
        let now = Instant::now();
        let mut ranked: Vec<(bool, u32, u64, usize, &String)> = urls
            .iter()
            .enumerate()
            .map(|(i, url)| {
                let h = st.health.get(&host(url));
                let benched = h.and_then(|h| h.benched_until).is_some_and(|t| t > now);
                let fails = h.map_or(0, |h| h.fails);
                // Unmeasured counts as middling, so a measured fast node goes
                // first and the registry's order breaks ties.
                let latency = h.map_or(400.0, |h| if h.latency == 0.0 { 400.0 } else { h.latency }) as u64;
                (benched, fails, latency / 100, i, url)
            })
            .collect();
        ranked.sort_by_key(|(benched, fails, latency, i, _)| (*benched, *fails, *latency, *i));
        ranked.into_iter().map(|(.., url)| url.clone()).collect()
    }

    pub fn caps(&self, url: &str) -> Caps {
        self.state().caps.get(url).copied().unwrap_or_default()
    }

    pub fn set_caps(&self, url: &str, change: impl FnOnce(&mut Caps)) {
        change(self.state().caps.entry(url.to_string()).or_default());
    }

    pub fn multicall(&self, chain_id: u64) -> Option<bool> {
        self.state().multicall.get(&chain_id).copied()
    }

    pub fn set_multicall(&self, chain_id: u64, deployed: bool) {
        self.state().multicall.insert(chain_id, deployed);
    }

    /// Records a node's head. `false` (and benched) when it trails the best
    /// head seen on the chain: its answers would be stale, and a balance must
    /// never go back in time because a lagging node answered.
    pub fn note_head(&self, chain: &str, url: &str, head: u64) -> bool {
        let best = self.state().heads.get(chain).copied().unwrap_or(0);
        if head + MAX_LAG < best {
            self.bench(url, BENCH_LAG);
            return false;
        }
        self.state().heads.insert(chain.to_string(), best.max(head));
        true
    }

    // --- JSON-RPC ---------------------------------------------------------

    async fn request(&self, url: &str, method: &str, params: &Value, timeout: Duration) -> Result<Value, Failure> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let _permit = self.permit(url).await;
        let started = Instant::now();
        let reply = match self.send(url, Some(&body), timeout).await {
            Ok(reply) => reply,
            Err(message) => {
                self.bench(url, BENCH_ERROR);
                return Err(Failure::trouble(None, message));
            }
        };
        if reply.status == 429 {
            self.rate_limited(url, reply.retry_after);
            return Err(Failure::trouble(Some(429), "rate limited"));
        }
        if reply.status >= 500 {
            self.bench(url, BENCH_ERROR);
            return Err(Failure::trouble(Some(reply.status.into()), format!("node error ({})", reply.status)));
        }
        if looks_html(&reply.body) {
            self.bench(url, BENCH_BLOCKED);
            return Err(Failure::trouble(Some(reply.status.into()), "blocked by the node's firewall"));
        }
        let Ok(mut value) = serde_json::from_str::<Value>(&reply.body) else {
            self.bench(url, BENCH_ERROR);
            return Err(Failure::trouble(None, "the node sent something unreadable"));
        };
        if let Some(error) = value.get("error").filter(|e| !e.is_null()) {
            let code = error.get("code").and_then(Value::as_i64);
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("the node refused this")
                .chars()
                .take(200)
                .collect::<String>();
            let trouble = is_node_trouble(code, &message) || reply.status == 403;
            if code == Some(429) || message.to_ascii_lowercase().contains("rate limit") {
                self.rate_limited(url, reply.retry_after);
            } else if trouble {
                self.failed(url);
            } else {
                self.answered(url, started);
            }
            return Err(Failure { code, message, trouble });
        }
        if !(200..300).contains(&reply.status) {
            self.bench(url, BENCH_ERROR);
            return Err(Failure::trouble(Some(reply.status.into()), format!("node error ({})", reply.status)));
        }
        match value.as_object_mut().and_then(|o| o.remove("result")) {
            Some(result) => {
                self.answered(url, started);
                Ok(result)
            }
            None => {
                self.failed(url);
                Err(Failure::trouble(None, "the node sent no result"))
            }
        }
    }

    /// Whether `url` is chain `chain_id`, asked once and remembered.
    async fn admit(&self, url: &str, chain_id: u64) -> Result<(), Failure> {
        let known = self.state().admitted.get(&(url.to_string(), chain_id)).copied();
        let ok = match known {
            Some(ok) => ok,
            None => {
                let answer = self.request(url, "eth_chainId", &json!([]), TIMEOUT).await?;
                let reported = answer
                    .as_str()
                    .and_then(|h| u64::from_str_radix(h.trim_start_matches("0x"), 16).ok());
                let ok = reported == Some(chain_id);
                self.state().admitted.insert((url.to_string(), chain_id), ok);
                ok
            }
        };
        if ok {
            Ok(())
        } else {
            Err(Failure::trouble(None, "this node is for another network"))
        }
    }

    /// One JSON-RPC call to one endpoint, after the chain-id check.
    ///
    /// Returned boxed (as `call` is): a future that borrows its arguments,
    /// awaited inside a spawned task, otherwise trips the compiler's `Send`
    /// check (rust-lang/rust#102211).
    pub fn attempt<'a>(&'a self, url: &'a str, method: &'a str, params: &'a Value, opts: Opts) -> BoxFuture<'a, Result<Value, Failure>> {
        async move {
            if let Some(chain_id) = opts.chain {
                self.admit(url, chain_id).await?;
            }
            self.request(url, method, params, opts.timeout).await
        }
        .boxed()
    }

    /// One JSON-RPC call, across the endpoints until one answers.
    pub fn call<'a>(&'a self, urls: &'a [String], method: &'a str, params: &'a Value, opts: Opts) -> BoxFuture<'a, Result<Value, Failure>> {
        async move {
            let order = self.order(urls);
            let mut pending: FuturesUnordered<BoxFuture<'_, Result<Value, Failure>>> = FuturesUnordered::new();
            let mut next = 0;
            let mut worst: Option<Failure> = None;
            loop {
                if pending.is_empty() {
                    let Some(url) = order.get(next) else { break };
                    pending.push(self.attempt(url, method, params, opts));
                    next += 1;
                }
                let done = if opts.hedge && next < order.len() {
                    match tokio::time::timeout(HEDGE_AFTER, pending.next()).await {
                        Ok(done) => done,
                        Err(_) => {
                            pending.push(self.attempt(&order[next], method, params, opts));
                            next += 1;
                            continue;
                        }
                    }
                } else {
                    pending.next().await
                };
                match done {
                    Some(Ok(value)) => return Ok(value),
                    Some(Err(failure)) if !failure.trouble => return Err(failure),
                    Some(Err(failure)) => worst = Some(telling(worst, failure)),
                    None => {}
                }
            }
            Err(worst.unwrap_or_else(|| Failure::trouble(None, "this network has no RPC endpoints")))
        }
        .boxed()
    }

    // --- plain GET ----------------------------------------------------------

    /// A JSON GET, across `urls` (the same resource at different hosts) until
    /// one answers. A 404 is an answer: `Null`.
    pub async fn get(&self, urls: &[String], timeout: Duration) -> Result<Value, String> {
        let mut last = "no source to ask".to_string();
        for url in self.order(urls) {
            let _permit = self.permit(&url).await;
            let started = Instant::now();
            let reply = match self.send(&url, None, timeout).await {
                Ok(reply) => reply,
                Err(message) => {
                    self.bench(&url, BENCH_ERROR);
                    last = message;
                    continue;
                }
            };
            match reply.status {
                404 => {
                    self.answered(&url, started);
                    return Ok(Value::Null);
                }
                429 => {
                    self.rate_limited(&url, reply.retry_after);
                    last = "rate limited".into();
                    continue;
                }
                401..=403 => {
                    // Blocked, or now behind a key: not coming back soon.
                    self.bench(&url, BENCH_BLOCKED);
                    last = format!("refused ({})", reply.status);
                    continue;
                }
                s if !(200..300).contains(&s) => {
                    self.bench(&url, BENCH_ERROR);
                    last = format!("error ({s})");
                    continue;
                }
                _ => {}
            }
            if looks_html(&reply.body) {
                self.bench(&url, BENCH_BLOCKED);
                last = "blocked by a firewall".into();
                continue;
            }
            match serde_json::from_str(&reply.body) {
                Ok(value) => {
                    self.answered(&url, started);
                    return Ok(value);
                }
                Err(_) => {
                    self.bench(&url, BENCH_ERROR);
                    last = "unreadable answer".into();
                }
            }
        }
        Err(last)
    }
}

/// The more useful of two failures to show: one a node explained beats one
/// that only says it could not connect.
fn telling(previous: Option<Failure>, next: Failure) -> Failure {
    match previous {
        Some(p) if p.code.is_some() && next.code.is_none() => p,
        _ => next,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub fn reply(status: u16, body: Value) -> FakeReply {
        FakeReply { status, body: body.to_string(), delay_ms: 0 }
    }

    pub fn ok(result: Value) -> FakeReply {
        reply(200, json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    pub fn rpc_error(code: i64, message: &str) -> FakeReply {
        reply(200, json!({ "jsonrpc": "2.0", "id": 1, "error": { "code": code, "message": message } }))
    }

    fn urls() -> Vec<String> {
        vec!["https://a.example/rpc".into(), "https://b.example/rpc".into()]
    }

    #[tokio::test]
    async fn a_rate_limited_node_is_benched_and_the_next_answers() {
        let hits = Arc::new(AtomicUsize::new(0));
        let seen = hits.clone();
        let pool = Pool::fake(Arc::new(move |url, _| {
            if url.contains("a.example") {
                seen.fetch_add(1, Ordering::SeqCst);
                reply(429, json!({}))
            } else {
                ok(json!("0x1"))
            }
        }));
        let opts = Opts { hedge: false, ..Opts::plain() };
        assert_eq!(pool.call(&urls(), "eth_blockNumber", &json!([]), opts).await.unwrap(), json!("0x1"));
        assert_eq!(pool.order(&urls())[0], "https://b.example/rpc", "the limited node goes last");
        pool.call(&urls(), "eth_blockNumber", &json!([]), opts).await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1, "a benched node is not asked while another answers");
    }

    #[tokio::test]
    async fn node_trouble_moves_on_but_a_revert_is_the_answer() {
        let pool = Pool::fake(Arc::new(|url, body| {
            let method = body.and_then(|b| b["method"].as_str()).unwrap_or("").to_string();
            match (url.contains("a.example"), method.as_str()) {
                (true, "eth_getBalance") => rpc_error(-32000, "header not found"),
                (_, "eth_getBalance") => ok(json!("0x5")),
                (_, _) => rpc_error(3, "execution reverted"),
            }
        }));
        let opts = Opts { hedge: false, ..Opts::plain() };
        assert_eq!(pool.call(&urls(), "eth_getBalance", &json!([]), opts).await.unwrap(), json!("0x5"));
        let revert = pool.call(&urls(), "eth_call", &json!([]), opts).await.unwrap_err();
        assert!(!revert.trouble && revert.message == "execution reverted");
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_node_is_hedged() {
        let pool = Pool::fake(Arc::new(|url, _| {
            if url.contains("a.example") {
                FakeReply { delay_ms: 8000, ..ok(json!("slow")) }
            } else {
                ok(json!("fast"))
            }
        }));
        let started = tokio::time::Instant::now();
        let answer = pool.call(&urls(), "x", &json!([]), Opts::plain()).await.unwrap();
        assert_eq!(answer, json!("fast"));
        let took = started.elapsed();
        assert!(took >= HEDGE_AFTER && took < Duration::from_secs(3), "{took:?}");
    }

    #[tokio::test]
    async fn a_node_for_another_chain_is_never_used() {
        let pool = Pool::fake(Arc::new(|url, body| {
            let method = body.and_then(|b| b["method"].as_str()).unwrap_or("").to_string();
            match (url.contains("a.example"), method.as_str()) {
                (true, "eth_chainId") => ok(json!("0x38")), // BSC, not Ethereum
                (false, "eth_chainId") => ok(json!("0x1")),
                (true, _) => ok(json!("wrong chain")),
                (false, _) => ok(json!("right chain")),
            }
        }));
        let opts = Opts { hedge: false, ..Opts::evm(1) };
        for _ in 0..2 {
            assert_eq!(pool.call(&urls(), "eth_getBalance", &json!([]), opts).await.unwrap(), json!("right chain"));
        }
    }

    #[tokio::test]
    async fn a_firewall_page_benches_for_long_and_a_lagging_head_is_refused() {
        let pool = Pool::fake(Arc::new(|url, _| {
            if url.contains("a.example") {
                FakeReply { status: 403, body: "<!DOCTYPE html><html><title>Just a moment...</title>".into(), delay_ms: 0 }
            } else {
                reply(200, json!({ "items": [] }))
            }
        }));
        let got = pool.get(&urls(), TIMEOUT).await.unwrap();
        assert_eq!(got, json!({ "items": [] }));
        let st = pool.state();
        let until = st.health.get("a.example").and_then(|h| h.benched_until).unwrap();
        assert!(until > Instant::now() + Duration::from_secs(20 * 60));
        drop(st);

        assert!(pool.note_head("ethereum", "https://a.example/rpc", 100));
        assert!(pool.note_head("ethereum", "https://b.example/rpc", 98), "within the allowed lag");
        assert!(!pool.note_head("ethereum", "https://b.example/rpc", 90), "too far behind");
    }
}
