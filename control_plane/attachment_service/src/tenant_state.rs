use std::{collections::HashMap, sync::Arc, time::Duration};

use control_plane::attachment_service::NodeAvailability;
use pageserver_api::{
    models::{LocationConfig, LocationConfigMode, TenantConfig},
    shard::{ShardIdentity, TenantShardId},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use utils::{
    generation::Generation,
    id::NodeId,
    seqwait::{SeqWait, SeqWaitError},
};

use crate::{
    compute_hook::ComputeHook,
    node::Node,
    persistence::Persistence,
    reconciler::{attached_location_conf, secondary_location_conf, ReconcileError, Reconciler},
    scheduler::{ScheduleError, Scheduler},
    service, PlacementPolicy, Sequence,
};

pub(crate) struct TenantState {
    pub(crate) tenant_shard_id: TenantShardId,

    pub(crate) shard: ShardIdentity,

    // Runtime only: sequence used to coordinate when updating this object while
    // with background reconcilers may be running.  A reconciler runs to a particular
    // sequence.
    pub(crate) sequence: Sequence,

    // Latest generation number: next time we attach, increment this
    // and use the incremented number when attaching
    pub(crate) generation: Generation,

    // High level description of how the tenant should be set up.  Provided
    // externally.
    pub(crate) policy: PlacementPolicy,

    // Low level description of exactly which pageservers should fulfil
    // which role.  Generated by `Self::schedule`.
    pub(crate) intent: IntentState,

    // Low level description of how the tenant is configured on pageservers:
    // if this does not match `Self::intent` then the tenant needs reconciliation
    // with `Self::reconcile`.
    pub(crate) observed: ObservedState,

    // Tenant configuration, passed through opaquely to the pageserver.  Identical
    // for all shards in a tenant.
    pub(crate) config: TenantConfig,

    /// If a reconcile task is currently in flight, it may be joined here (it is
    /// only safe to join if either the result has been received or the reconciler's
    /// cancellation token has been fired)
    pub(crate) reconciler: Option<ReconcilerHandle>,

    /// Optionally wait for reconciliation to complete up to a particular
    /// sequence number.
    pub(crate) waiter: std::sync::Arc<SeqWait<Sequence, Sequence>>,

    /// Indicates sequence number for which we have encountered an error reconciling.  If
    /// this advances ahead of [`Self::waiter`] then a reconciliation error has occurred,
    /// and callers should stop waiting for `waiter` and propagate the error.
    pub(crate) error_waiter: std::sync::Arc<SeqWait<Sequence, Sequence>>,

    /// The most recent error from a reconcile on this tenant
    /// TODO: generalize to an array of recent events
    /// TOOD: use a ArcSwap instead of mutex for faster reads?
    pub(crate) last_error: std::sync::Arc<std::sync::Mutex<String>>,
}

#[derive(Default, Clone, Debug)]
pub(crate) struct IntentState {
    pub(crate) attached: Option<NodeId>,
    pub(crate) secondary: Vec<NodeId>,
}

#[derive(Default, Clone)]
pub(crate) struct ObservedState {
    pub(crate) locations: HashMap<NodeId, ObservedStateLocation>,
}

/// Our latest knowledge of how this tenant is configured in the outside world.
///
/// Meaning:
///     * No instance of this type exists for a node: we are certain that we have nothing configured on that
///       node for this shard.
///     * Instance exists with conf==None: we *might* have some state on that node, but we don't know
///       what it is (e.g. we failed partway through configuring it)
///     * Instance exists with conf==Some: this tells us what we last successfully configured on this node,
///       and that configuration will still be present unless something external interfered.
#[derive(Clone)]
pub(crate) struct ObservedStateLocation {
    /// If None, it means we do not know the status of this shard's location on this node, but
    /// we know that we might have some state on this node.
    pub(crate) conf: Option<LocationConfig>,
}
pub(crate) struct ReconcilerWaiter {
    // For observability purposes, remember the ID of the shard we're
    // waiting for.
    pub(crate) tenant_shard_id: TenantShardId,

    seq_wait: std::sync::Arc<SeqWait<Sequence, Sequence>>,
    error_seq_wait: std::sync::Arc<SeqWait<Sequence, Sequence>>,
    error: std::sync::Arc<std::sync::Mutex<String>>,
    seq: Sequence,
}

#[derive(thiserror::Error, Debug)]
pub enum ReconcileWaitError {
    #[error("Timeout waiting for shard {0}")]
    Timeout(TenantShardId),
    #[error("shutting down")]
    Shutdown,
    #[error("Reconcile error on shard {0}: {1}")]
    Failed(TenantShardId, String),
}

impl ReconcilerWaiter {
    pub(crate) async fn wait_timeout(&self, timeout: Duration) -> Result<(), ReconcileWaitError> {
        tokio::select! {
            result = self.seq_wait.wait_for_timeout(self.seq, timeout)=> {
                result.map_err(|e| match e {
                    SeqWaitError::Timeout => ReconcileWaitError::Timeout(self.tenant_shard_id),
                    SeqWaitError::Shutdown => ReconcileWaitError::Shutdown
                })?;
            },
            result = self.error_seq_wait.wait_for(self.seq) => {
                result.map_err(|e| match e {
                    SeqWaitError::Shutdown => ReconcileWaitError::Shutdown,
                    SeqWaitError::Timeout => unreachable!()
                })?;

                return Err(ReconcileWaitError::Failed(self.tenant_shard_id, self.error.lock().unwrap().clone()))
            }
        }

        Ok(())
    }
}

/// Having spawned a reconciler task, the tenant shard's state will carry enough
/// information to optionally cancel & await it later.
pub(crate) struct ReconcilerHandle {
    sequence: Sequence,
    handle: JoinHandle<()>,
    cancel: CancellationToken,
}

/// When a reconcile task completes, it sends this result object
/// to be applied to the primary TenantState.
pub(crate) struct ReconcileResult {
    pub(crate) sequence: Sequence,
    /// On errors, `observed` should be treated as an incompleted description
    /// of state (i.e. any nodes present in the result should override nodes
    /// present in the parent tenant state, but any unmentioned nodes should
    /// not be removed from parent tenant state)
    pub(crate) result: Result<(), ReconcileError>,

    pub(crate) tenant_shard_id: TenantShardId,
    pub(crate) generation: Generation,
    pub(crate) observed: ObservedState,
}

impl IntentState {
    pub(crate) fn new() -> Self {
        Self {
            attached: None,
            secondary: vec![],
        }
    }
    pub(crate) fn all_pageservers(&self) -> Vec<NodeId> {
        let mut result = Vec::new();
        if let Some(p) = self.attached {
            result.push(p)
        }

        result.extend(self.secondary.iter().copied());

        result
    }

    pub(crate) fn single(node_id: Option<NodeId>) -> Self {
        Self {
            attached: node_id,
            secondary: vec![],
        }
    }

    /// When a node goes offline, we update intents to avoid using it
    /// as their attached pageserver.
    ///
    /// Returns true if a change was made
    pub(crate) fn notify_offline(&mut self, node_id: NodeId) -> bool {
        if self.attached == Some(node_id) {
            self.attached = None;
            self.secondary.push(node_id);
            true
        } else {
            false
        }
    }
}

impl ObservedState {
    pub(crate) fn new() -> Self {
        Self {
            locations: HashMap::new(),
        }
    }
}

impl TenantState {
    pub(crate) fn new(
        tenant_shard_id: TenantShardId,
        shard: ShardIdentity,
        policy: PlacementPolicy,
    ) -> Self {
        Self {
            tenant_shard_id,
            policy,
            intent: IntentState::default(),
            generation: Generation::new(0),
            shard,
            observed: ObservedState::default(),
            config: TenantConfig::default(),
            reconciler: None,
            sequence: Sequence(1),
            waiter: Arc::new(SeqWait::new(Sequence(0))),
            error_waiter: Arc::new(SeqWait::new(Sequence(0))),
            last_error: Arc::default(),
        }
    }

    /// For use on startup when learning state from pageservers: generate my [`IntentState`] from my
    /// [`ObservedState`], even if it violates my [`PlacementPolicy`].  Call [`Self::schedule`] next,
    /// to get an intent state that complies with placement policy.  The overall goal is to do scheduling
    /// in a way that makes use of any configured locations that already exist in the outside world.
    pub(crate) fn intent_from_observed(&mut self) {
        // Choose an attached location by filtering observed locations, and then sorting to get the highest
        // generation
        let mut attached_locs = self
            .observed
            .locations
            .iter()
            .filter_map(|(node_id, l)| {
                if let Some(conf) = &l.conf {
                    if conf.mode == LocationConfigMode::AttachedMulti
                        || conf.mode == LocationConfigMode::AttachedSingle
                        || conf.mode == LocationConfigMode::AttachedStale
                    {
                        Some((node_id, conf.generation))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        attached_locs.sort_by_key(|i| i.1);
        if let Some((node_id, _gen)) = attached_locs.into_iter().last() {
            self.intent.attached = Some(*node_id);
        }

        // All remaining observed locations generate secondary intents.  This includes None
        // observations, as these may well have some local content on disk that is usable (this
        // is an edge case that might occur if we restarted during a migration or other change)
        self.observed.locations.keys().for_each(|node_id| {
            if Some(*node_id) != self.intent.attached {
                self.intent.secondary.push(*node_id);
            }
        });
    }

    pub(crate) fn schedule(&mut self, scheduler: &mut Scheduler) -> Result<(), ScheduleError> {
        // TODO: before scheduling new nodes, check if any existing content in
        // self.intent refers to pageservers that are offline, and pick other
        // pageservers if so.

        // Build the set of pageservers already in use by this tenant, to avoid scheduling
        // more work on the same pageservers we're already using.
        let mut used_pageservers = self.intent.all_pageservers();
        let mut modified = false;

        use PlacementPolicy::*;
        match self.policy {
            Single => {
                // Should have exactly one attached, and zero secondaries
                if self.intent.attached.is_none() {
                    let node_id = scheduler.schedule_shard(&used_pageservers)?;
                    self.intent.attached = Some(node_id);
                    used_pageservers.push(node_id);
                    modified = true;
                }
                if !self.intent.secondary.is_empty() {
                    self.intent.secondary.clear();
                    modified = true;
                }
            }
            Double(secondary_count) => {
                // Should have exactly one attached, and N secondaries
                if self.intent.attached.is_none() {
                    let node_id = scheduler.schedule_shard(&used_pageservers)?;
                    self.intent.attached = Some(node_id);
                    used_pageservers.push(node_id);
                    modified = true;
                }

                while self.intent.secondary.len() < secondary_count {
                    let node_id = scheduler.schedule_shard(&used_pageservers)?;
                    self.intent.secondary.push(node_id);
                    used_pageservers.push(node_id);
                    modified = true;
                }
            }
        }

        if modified {
            self.sequence.0 += 1;
        }

        Ok(())
    }

    fn dirty(&self) -> bool {
        if let Some(node_id) = self.intent.attached {
            let wanted_conf = attached_location_conf(self.generation, &self.shard, &self.config);
            match self.observed.locations.get(&node_id) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {}
                Some(_) | None => {
                    return true;
                }
            }
        }

        for node_id in &self.intent.secondary {
            let wanted_conf = secondary_location_conf(&self.shard, &self.config);
            match self.observed.locations.get(node_id) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {}
                Some(_) | None => {
                    return true;
                }
            }
        }

        false
    }

    pub(crate) fn maybe_reconcile(
        &mut self,
        result_tx: tokio::sync::mpsc::UnboundedSender<ReconcileResult>,
        pageservers: &Arc<HashMap<NodeId, Node>>,
        compute_hook: &Arc<ComputeHook>,
        service_config: &service::Config,
        persistence: &Arc<Persistence>,
    ) -> Option<ReconcilerWaiter> {
        // If there are any ambiguous observed states, and the nodes they refer to are available,
        // we should reconcile to clean them up.
        let mut dirty_observed = false;
        for (node_id, observed_loc) in &self.observed.locations {
            let node = pageservers
                .get(node_id)
                .expect("Nodes may not be removed while referenced");
            if observed_loc.conf.is_none()
                && !matches!(node.availability, NodeAvailability::Offline)
            {
                dirty_observed = true;
                break;
            }
        }

        if !self.dirty() && !dirty_observed {
            tracing::info!("Not dirty, no reconciliation needed.");
            return None;
        }

        // Reconcile already in flight for the current sequence?
        if let Some(handle) = &self.reconciler {
            if handle.sequence == self.sequence {
                return Some(ReconcilerWaiter {
                    tenant_shard_id: self.tenant_shard_id,
                    seq_wait: self.waiter.clone(),
                    error_seq_wait: self.error_waiter.clone(),
                    error: self.last_error.clone(),
                    seq: self.sequence,
                });
            }
        }

        // Reconcile in flight for a stale sequence?  Our sequence's task will wait for it before
        // doing our sequence's work.
        let old_handle = self.reconciler.take();

        let cancel = CancellationToken::new();
        let mut reconciler = Reconciler {
            tenant_shard_id: self.tenant_shard_id,
            shard: self.shard,
            generation: self.generation,
            intent: self.intent.clone(),
            config: self.config.clone(),
            observed: self.observed.clone(),
            pageservers: pageservers.clone(),
            compute_hook: compute_hook.clone(),
            service_config: service_config.clone(),
            cancel: cancel.clone(),
            persistence: persistence.clone(),
        };

        let reconcile_seq = self.sequence;

        tracing::info!("Spawning Reconciler for sequence {}", self.sequence);
        let join_handle = tokio::task::spawn(async move {
            // Wait for any previous reconcile task to complete before we start
            if let Some(old_handle) = old_handle {
                old_handle.cancel.cancel();
                if let Err(e) = old_handle.handle.await {
                    // We can't do much with this other than log it: the task is done, so
                    // we may proceed with our work.
                    tracing::error!("Unexpected join error waiting for reconcile task: {e}");
                }
            }

            // Early check for cancellation before doing any work
            // TODO: wrap all remote API operations in cancellation check
            // as well.
            if reconciler.cancel.is_cancelled() {
                return;
            }

            let result = reconciler.reconcile().await;
            result_tx
                .send(ReconcileResult {
                    sequence: reconcile_seq,
                    result,
                    tenant_shard_id: reconciler.tenant_shard_id,
                    generation: reconciler.generation,
                    observed: reconciler.observed,
                })
                .ok();
        });

        self.reconciler = Some(ReconcilerHandle {
            sequence: self.sequence,
            handle: join_handle,
            cancel,
        });

        Some(ReconcilerWaiter {
            tenant_shard_id: self.tenant_shard_id,
            seq_wait: self.waiter.clone(),
            error_seq_wait: self.error_waiter.clone(),
            error: self.last_error.clone(),
            seq: self.sequence,
        })
    }
}
