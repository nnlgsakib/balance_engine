//! Keeping balances live: one task per chain, polling at the chain's pace
//! while the caller is in the foreground, and right away when kicked (after
//! a send, or on a push from a websocket later).

use crate::{cadence, Checkpoint, Engine, Progress, Target, Update};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{watch, Notify};

/// A pass over the whole list (popular tokens, found assets, the index) at
/// least this often. Rounds in between read what is held and what arrived.
pub(crate) const FULL_EVERY: Duration = Duration::from_secs(180);
const MAX_WAIT: Duration = Duration::from_secs(120);

pub type OnUpdate = Arc<dyn Fn(Update) -> bool + Send + Sync>;

struct Shared {
    paused: watch::Sender<bool>,
    kicks: Vec<(String, Arc<Notify>)>,
    checkpoints: Mutex<HashMap<String, (String, Checkpoint)>>,
}

/// Live polling of a set of targets. Dropping it stops every task.
pub struct Watch {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    shared: Arc<Shared>,
}

/// ±20%, so many wallets (and many chains) do not ask in step.
fn jitter(d: Duration) -> Duration {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |t| t.subsec_nanos());
    d.mul_f64(0.8 + f64::from(nanos % 400) / 1000.0)
}

pub(crate) fn start(engine: Engine, targets: Vec<Target>, on_update: OnUpdate) -> Watch {
    let (paused, _) = watch::channel(false);
    let kicks: Vec<(String, Arc<Notify>)> = targets.iter().map(|t| (t.chain.id().to_string(), Arc::new(Notify::new()))).collect();
    let checkpoints = targets.iter().map(|t| (t.chain.id().to_string(), (t.address.clone(), t.checkpoint.clone()))).collect();
    let shared = Arc::new(Shared { paused, kicks, checkpoints: Mutex::new(checkpoints) });
    let tasks = targets
        .into_iter()
        .zip(shared.kicks.iter().map(|(_, k)| k.clone()).collect::<Vec<_>>())
        .map(|(target, kick)| tokio::spawn(run(engine.clone(), target, kick, shared.clone(), on_update.clone())))
        .collect();
    Watch { tasks, shared }
}

async fn run(engine: Engine, target: Target, kick: Arc<Notify>, shared: Arc<Shared>, on_update: OnUpdate) {
    let mut paused = shared.paused.subscribe();
    let mut cp = target.checkpoint.clone();
    let mut pr: Progress = engine.progress(&target);
    let base = cadence(&target.chain);
    let mut wait = base;
    let mut full = false;
    loop {
        if *paused.borrow() {
            if paused.wait_for(|p| !*p).await.is_err() {
                return;
            }
            full = true;
        } else {
            tokio::select! {
                _ = tokio::time::sleep(jitter(wait)) => {}
                _ = kick.notified() => full = true,
            }
            if *paused.borrow() {
                continue;
            }
        }
        let due = full || pr.last_full.is_none_or(|t| t.elapsed() >= FULL_EVERY);
        full = false;
        let update = engine.round(&target, &mut cp, &mut pr, due, false).await;
        wait = if pr.error.is_some() { (wait * 2).min(MAX_WAIT.max(base)) } else { base };
        if let Ok(mut saved) = shared.checkpoints.lock() {
            saved.insert(target.chain.id().to_string(), (target.address.clone(), cp.clone()));
        }
        if let Some(update) = update {
            if !on_update(update) {
                return;
            }
        }
    }
}

impl Watch {
    /// A full pass now, on one chain or on all.
    pub fn kick(&self, chain_id: Option<&str>) {
        for (id, notify) in &self.shared.kicks {
            if chain_id.is_none_or(|c| c == id) {
                notify.notify_one();
            }
        }
    }

    /// Stops asking the network (the app went to the background).
    pub fn pause(&self) {
        self.shared.paused.send_replace(true);
    }

    /// Carries on, with a full pass straight away.
    pub fn resume(&self) {
        self.shared.paused.send_replace(false);
    }

    /// Where each chain's scan stands: (chain id, address, checkpoint), for
    /// the caller to store.
    pub fn checkpoints(&self) -> Vec<(String, String, Checkpoint)> {
        self.shared
            .checkpoints
            .lock()
            .map(|m| m.iter().map(|(chain, (address, cp))| (chain.clone(), address.clone(), cp.clone())).collect())
            .unwrap_or_default()
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
