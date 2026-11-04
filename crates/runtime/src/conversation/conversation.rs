use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use fact_kernel::FactExtractionTokenUsage;
use tokio::sync::RwLock;

#[path = "context_plane.rs"]
mod context_plane;
#[path = "evidence_terminal_plane.rs"]
mod evidence_terminal_plane;
#[path = "frame_refusal.rs"]
pub(crate) mod frame_refusal;
#[path = "provider_plane.rs"]
mod provider_plane;
use provider_plane::TurnProviderState;
#[path = "tool_plane.rs"]
pub(crate) mod tool_plane;
#[path = "turn_engine.rs"]
mod turn_engine;
pub(crate) use turn_engine::{capture_member_consumption, original_memory_authorization_identity, OriginalHostPreparedMember,
    PreparedHostMemberAssessment, SharedPolicyOwner};

/// T35: Lightweight cancellation token (tokio-util not available in dep tree).
#[derive(Default, Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
    parents: Vec<CancellationToken>,
    signal_window: std::sync::Mutex<Option<(Instant, Instant)>>,
    member_commit_controls: std::sync::Mutex<Vec<std::sync::Weak<MemberCommitControl>>>,
    /// Test-fixture only: where this scope was cancelled from.
    #[cfg(feature = "test-fixtures")]
    cancel_site: std::sync::Mutex<Option<String>>,
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;


/// Test-fixture only: the last cancellation signals with their source line and
/// token identity, so an unexplained turn cancellation can be attributed.
#[cfg(feature = "test-fixtures")]
static CANCEL_SITES: std::sync::OnceLock<std::sync::Mutex<Vec<(usize, String)>>> =
    std::sync::OnceLock::new();

#[cfg(feature = "test-fixtures")]
pub(crate) fn record_cancel_site(token_id: usize, site: &str) {
    let slot = CANCEL_SITES.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    if let Ok(mut guard) = slot.lock() {
        guard.push((token_id, site.to_string()));
        if guard.len() > 32 {
            guard.remove(0);
        }
    }
}

#[cfg(feature = "test-fixtures")]
pub(crate) fn cancel_sites() -> Vec<(usize, String)> {
    let slot = CANCEL_SITES.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    slot.lock().map(|guard| guard.clone()).unwrap_or_default()
}

/// Shared original-turn input control. Signal and member transaction
/// registration are serialized so control cannot race past registration.
#[derive(Debug, Default)]
pub(crate) struct OriginalTurnInputControl {
    requested: AtomicBool,
    approval_waiters: std::sync::atomic::AtomicUsize,
    members: std::sync::Mutex<Vec<std::sync::Weak<MemberCommitControl>>>,
}
pub(crate) struct OriginalApprovalWait(Arc<OriginalTurnInputControl>);
impl Drop for OriginalApprovalWait {
    fn drop(&mut self) { self.0.approval_waiters.fetch_sub(1, Ordering::AcqRel); }
}
impl OriginalTurnInputControl {
    pub(crate) fn begin_approval_wait(self: &Arc<Self>) -> OriginalApprovalWait {
        self.approval_waiters.fetch_add(1, Ordering::AcqRel);
        OriginalApprovalWait(Arc::clone(self))
    }
    pub(crate) fn signal_if_approval_waiting(&self) {
        if self.approval_waiters.load(Ordering::Acquire) != 0 { self.signal(); }
    }
    pub(crate) fn load(&self, ordering: Ordering) -> bool { self.requested.load(ordering) }
    pub(crate) fn swap(&self, value: bool, ordering: Ordering) -> bool { self.requested.swap(value, ordering) }
    pub(crate) fn signal(&self) {
        let mut members = self.members.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        self.requested.store(true, Ordering::Release);
        members.retain(|weak| {
            if let Some(control) = weak.upgrade() {
                crate::RuntimeTransactionCommitControl::request_cancel(control.as_ref()); true
            } else { false }
        });
    }
    pub(crate) fn register(&self, control: &Arc<MemberCommitControl>) {
        let mut members = self.members.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        members.retain(|weak| weak.strong_count() > 0);
        members.push(Arc::downgrade(control));
        if self.requested.load(Ordering::Acquire) {
            crate::RuntimeTransactionCommitControl::request_cancel(control.as_ref());
        }
    }
}

/// Local decision for one member transaction; Unknown is retained until the
/// original transaction owner resolves it. This does not authenticate Session
/// ingress or replace the durable transaction source fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberCommitState { Ready, Running, CancelRequested, Committing, Committed, RolledBack, Unknown }
#[derive(Debug)]
pub(crate) struct MemberCommitControl(std::sync::Mutex<MemberCommitState>);
impl MemberCommitControl {
    pub(crate) fn state(&self) -> MemberCommitState {
        *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn original_noncommit_reconciled(&self) -> Result<bool, String> {
        self.0.lock().map(|state| *state == MemberCommitState::RolledBack)
            .map_err(|_| "member transaction control unavailable".into())
    }

    /// Only the original checked owner may call this after authenticating the
    /// same member's durable noncommit decision and receipt.
    pub(crate) fn reconcile_original_noncommit(&self) -> Result<(), String> {
        let mut state = self.0.lock().map_err(|_| "member transaction control unavailable")?;
        match *state {
            MemberCommitState::Unknown => { *state = MemberCommitState::RolledBack; Ok(()) }
            MemberCommitState::RolledBack => Ok(()),
            _ => Err("member transaction control conflicts with noncommit".into()),
        }
    }
}
impl crate::RuntimeTransactionCommitControl for MemberCommitControl {
    fn request_cancel(&self) -> bool {
        let mut state = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match *state {
            MemberCommitState::Ready | MemberCommitState::Running => { *state = MemberCommitState::CancelRequested; true }
            MemberCommitState::CancelRequested => true,
            _ => false,
        }
    }
    fn begin(&self) -> Result<bool, String> {
        let mut state = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match *state {
            MemberCommitState::Ready => { *state = MemberCommitState::Running; Ok(true) }
            MemberCommitState::CancelRequested => { *state = MemberCommitState::RolledBack; Ok(false) }
            _ => Err("member transaction control already used".into()),
        }
    }
    fn before_commit(&self) {}
    fn commit_wins(&self) -> Result<bool, String> {
        let mut state = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match *state {
            MemberCommitState::Running => { *state = MemberCommitState::Committing; Ok(true) }
            MemberCommitState::CancelRequested => Ok(false),
            _ => Err("member transaction decision state invalid".into()),
        }
    }
    fn finish(&self, outcome: crate::RuntimeTransactionOutcome) {
        let mut state = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = match outcome {
            crate::RuntimeTransactionOutcome::Committed if *state == MemberCommitState::Committing => MemberCommitState::Committed,
            crate::RuntimeTransactionOutcome::RolledBack if matches!(*state, MemberCommitState::Ready | MemberCommitState::CancelRequested | MemberCommitState::Running) => MemberCommitState::RolledBack,
            _ => MemberCommitState::Unknown,
        };
    }
}

#[derive(Clone, Default, Debug)]
pub struct CancellationToken(Arc<CancellationState>);

impl CancellationToken {
    pub fn new() -> Self {
        Self(Arc::new(CancellationState::default()))
    }

    /// Test-fixture only: stable identity of this cancellation scope.
    #[cfg(feature = "test-fixtures")]
    pub(crate) fn identity(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }

    pub fn is_cancelled(&self) -> bool {
        let mut states = vec![self];
        while let Some(token) = states.pop() {
            if token.0.cancelled.load(Ordering::SeqCst) {
                return true;
            }
            states.extend(token.0.parents.iter());
        }
        false
    }

    #[inline(never)]
    #[track_caller]
    pub fn cancel(&self) {
        #[cfg(feature = "test-fixtures")]
        {
            let site = format!("{}", std::panic::Location::caller());
            if let Ok(mut guard) = self.0.cancel_site.lock() {
                if guard.is_none() {
                    *guard = Some(site.clone());
                }
            }
            // The cancel call stack is the decisive evidence: it names the
            // scope that escalated a cancellation.  It is written at cancel
            // time so a run that later fails in another phase still keeps it.
            {
                use std::io::Write as _;
                let path = std::env::temp_dir().join("k05-cancel-diagnostic.log");
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                {
                    let _ = writeln!(
                        file,
                        "cancel token_id={:x} site={site} backtrace={}",
                        self.identity(),
                        std::backtrace::Backtrace::force_capture()
                    );
                }
            }
        }
        let _ = self.cancel_first_signal();
    }

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn last_cancel_site(&self) -> Option<String> {
        let own = self.0.cancel_site.lock().ok().and_then(|guard| guard.clone());
        let parents = self
            .0
            .parents
            .iter()
            .filter_map(|parent| parent.last_cancel_site())
            .collect::<Vec<_>>();
        if parents.is_empty() {
            own
        } else {
            Some(format!("{own:?} parents={parents:?}"))
        }
    }

    #[inline(never)]
    #[track_caller]
    pub(crate) fn cancel_first_signal(&self) -> bool {
        let began = Instant::now();
        let mut controls = self.0.member_commit_controls.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        controls.retain(|weak| {
            if let Some(control) = weak.upgrade() {
                crate::RuntimeTransactionCommitControl::request_cancel(control.as_ref());
                true
            } else { false }
        });
        if !self.0.cancelled.swap(true, Ordering::SeqCst) {
            #[cfg(feature = "test-fixtures")]
            {
                // A turn's cancellation token can be cancelled from any layer
                // that holds a clone.  Record which token was cancelled and
                // from where, so a captured child trace can attribute an
                // unexplained "turn cancelled" to a concrete source line.
                let id = Arc::as_ptr(&self.0) as usize;
                let site = format!("{}", std::panic::Location::caller());
                crate::conversation::record_cancel_site(id, &site);
            }
            *self
                .0
                .signal_window
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((began, Instant::now()));
            self.0.notify.notify_waiters();
            true
        } else {
            false
        }
    }

    pub(crate) fn member_commit_control(&self) -> Arc<MemberCommitControl> {
        let control = Arc::new(MemberCommitControl(std::sync::Mutex::new(MemberCommitState::Ready)));
        self.register_member_commit_control(&control);
        control
    }

    pub(crate) fn register_member_commit_control(&self, control: &Arc<MemberCommitControl>) {
        let mut states = vec![self.0.clone()];
        let mut visited = std::collections::BTreeSet::new();
        while let Some(state) = states.pop() {
            if !visited.insert(Arc::as_ptr(&state) as usize) { continue; }
            let mut controls = state.member_commit_controls.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            controls.retain(|weak| weak.strong_count() > 0);
            controls.push(Arc::downgrade(control));
            if state.cancelled.load(Ordering::SeqCst) {
                crate::RuntimeTransactionCommitControl::request_cancel(control.as_ref());
            }
            states.extend(state.parents.iter().map(|parent| Arc::clone(&parent.0)));
        }
    }

    pub async fn cancelled(&self) {
        let mut states = vec![self.0.clone()];
        let mut index = 0;
        while index < states.len() {
            let parents: Vec<_> = states[index]
                .parents
                .iter()
                .map(|parent| parent.0.clone())
                .collect();
            states.extend(parents);
            index += 1;
        }
        loop {
            let notified: Vec<_> = states
                .iter()
                .map(|state| Box::pin(state.notify.notified()))
                .collect();
            if self.is_cancelled() {
                return;
            }
            let _ = futures::future::select_all(notified).await;
        }
    }

    pub(crate) fn linked(turn: Self, step: Self) -> Self {
        Self(Arc::new(CancellationState {
            cancelled: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
            parents: vec![turn, step],
            signal_window: std::sync::Mutex::new(None),
            member_commit_controls: std::sync::Mutex::new(Vec::new()),
            #[cfg(feature = "test-fixtures")]
            cancel_site: std::sync::Mutex::new(None),
        }))
    }

    /// Local wake ordering only, never durable source authentication. Overlap,
    /// an unfinished signal publication, or a foreign chain is unknown.
    pub(crate) fn precedes_other_signals(&self, chain: &Self, cleanup: &Self) -> bool {
        let Some((_, ended)) = *self
            .0
            .signal_window
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        else {
            return false;
        };
        let mut pending = vec![chain, cleanup];
        let mut found = false;
        while let Some(token) = pending.pop() {
            if Arc::ptr_eq(&token.0, &self.0) {
                found = true;
                continue;
            }
            if token.0.cancelled.load(Ordering::SeqCst) {
                let window = *token
                    .0
                    .signal_window
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !window.is_some_and(|(began, _)| ended < began) {
                    return false;
                }
            }
            pending.extend(token.0.parents.iter());
        }
        found
    }
}

#[cfg(test)]
mod scoped_stop_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn r188_cleanup_preserves_timeout_and_cancelled_primary_result() {
        use crate::execution_core::graph::ResourceResultClass;
        for expected in [
            ResourceResultClass::TimedOut,
            ResourceResultClass::Cancelled,
        ] {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let stop = CancellationToken::new();
            let physical_stop = stop.clone();
            let producer = ProviderExecution::new(
                async move {
                    let _sender = tx;
                    physical_stop.cancelled().await;
                    Err(RuntimeError::new("secondary producer stop acknowledgement"))
                },
                stop,
            );
            let cancellation = CancellationToken::new();
            if expected == ResourceResultClass::Cancelled {
                cancellation.cancel();
            }
            let events = futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|event| (event, rx))
            });
            let run = consume_owned_provider_stream(
                Box::pin(events),
                cancellation,
                Some(ProviderStreamTimeoutPolicy {
                    idle: Duration::from_millis(20),
                    heartbeat_grace: Duration::ZERO,
                }),
                ModelStreamReducer::new(None, None, "r188-primary".into()),
                None,
                None,
                Some(producer),
            )
            .await;
            assert_eq!(run.resource_result_class, expected);
            let diagnostic = run.failure.unwrap().to_string();
            assert!(!diagnostic.contains("secondary"));
            assert!(
                diagnostic.contains(if expected == ResourceResultClass::Cancelled {
                    "cancelled"
                } else {
                    "stalled"
                })
            );
        }
    }

    #[tokio::test]
    async fn r188_producer_panic_has_one_failed_completion_and_no_success() {
        let bus = Arc::new(crate::CowdEventBus::new());
        let mut observed = bus.subscribe();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let producer = ProviderExecution::new(
            async move {
                let _sender = tx;
                panic!("controlled original producer panic");
                #[allow(unreachable_code)]
                Ok(ProviderLocalExit::Completed)
            },
            CancellationToken::new(),
        );
        let events = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        });
        let run = consume_owned_provider_stream(
            Box::pin(events),
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(Some(bus), None, "r188-panic".into()),
            None,
            None,
            Some(producer),
        )
        .await;
        assert!(run
            .failure
            .unwrap()
            .to_string()
            .contains("producer panicked"));
        let mut statuses = Vec::new();
        while let Ok(event) = observed.try_recv() {
            if let crate::CowdEvent::ModelStepCompleted { status, .. } = event {
                statuses.push(status);
            }
        }
        assert_eq!(statuses, vec!["failed"]);
    }

    #[tokio::test]
    async fn r188_exact_step_stop_is_not_a_turn_or_sibling_stop() {
        let turn = CancellationToken::new();
        let step_a = CancellationToken::new();
        let step_b = CancellationToken::new();
        let a = CancellationToken::linked(turn.clone(), step_a.clone());
        let b = CancellationToken::linked(turn.clone(), step_b.clone());
        step_a.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), a.cancelled())
            .await
            .unwrap();
        assert!(!turn.is_cancelled());
        assert!(!step_b.is_cancelled());
        assert!(!b.is_cancelled());
        turn.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), b.cancelled())
            .await
            .unwrap();
        assert!(!step_b.is_cancelled());
    }

    #[test]
    fn r189_operational_step_signal_cannot_borrow_a_later_user_signal() {
        let user = CancellationToken::new();
        let step = CancellationToken::new();
        let cleanup = CancellationToken::new();
        let chain = CancellationToken::linked(user.clone(), step.clone());
        step.cancel();
        user.cancel();
        cleanup.cancel();
        assert!(!user.precedes_other_signals(&chain, &cleanup));
        let first = CancellationToken::new();
        let next_step = CancellationToken::new();
        let next_cleanup = CancellationToken::new();
        let next_chain = CancellationToken::linked(first.clone(), next_step.clone());
        first.cancel();
        next_cleanup.cancel();
        assert!(first.precedes_other_signals(&next_chain, &next_cleanup));
        assert!(!first.precedes_other_signals(&CancellationToken::new(), &next_cleanup));
    }
}
use futures::stream::Stream;
use harness_contract::{
    context::{
        ArtifactWriteDescriptor, CompactionReceipt, ContextGovernanceDecision,
        ContextPressureState, ContextTurnReport, EvidenceAccessRef, EvidenceAuditProjection,
        EvidenceRef, StablePrefixMetrics, ToolExposureMetrics, ToolObservation,
    },
    knowledge::KnowledgeTurnReport,
    skill::{AgentSkillProfile, SkillCapabilityProfile},
    strategy::{
        ExecutionCandidateKind, StrategyCandidateCostSummary, StrategyExperienceRecord,
        StrategyInput, StrategyWorkloadFingerprint,
    },
    turn::{
        SessionInputEnvelope, SessionInputProjection, SessionInputReceipt, TurnId,
        TurnInboxSnapshot, TurnInputCheckpoint,
    },
};
use memory::cognitive::CognitiveContextManager;
use memory::compression::session::{
    CompactionSourceRange, SessionCheckpointBuildContext, SessionCompactor,
    SessionSemanticCheckpoint,
};
use memory::config::MemoryConfig as CcMemoryConfig;
use memory::types::{Message as MemMessage, MessageRole as MemMessageRole};

const MAX_RUNTIME_PROVIDER_RETRIES_PER_MODEL: u8 = 1;
const DEFAULT_RUNTIME_PROVIDER_RETRY_DELAY: Duration = Duration::from_millis(250);
use memory::{MemoryKernel, MemoryTurnContext};
use model_protocol::telemetry::SessionTracer;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::budget_policy::{
    clamp_context_budget_ratio_bp, ProviderOutputBudget, ProviderOutputBudgetInputs,
    RuntimeBudgetInputs, RuntimeBudgetPlan,
};

const fn provider_retry_is_fenced(
    prior_effect_receipt_observed: bool,
    current_effect_receipts: usize,
) -> bool {
    prior_effect_receipt_observed || current_effect_receipts > 0
}

#[derive(Debug)]
struct EvaluationProviderTokenLeaseState {
    lease_id: String,
    limit: u64,
    /// Evaluation capacity is telemetry by default; it must not become a
    /// hidden execution stop condition for a real task.
    enforced: bool,
    remaining: u64,
    variance_remaining: u64,
    telemetry_consumed: u64,
    input_consumed: u64,
    output_consumed: u64,
    cached_consumed: u64,
    outstanding: usize,
    breached: bool,
}

#[derive(Debug)]
pub(crate) struct EvaluationProviderTokenLease {
    state: std::sync::Mutex<EvaluationProviderTokenLeaseState>,
}

impl EvaluationProviderTokenLease {
    fn new(lease_id: &str, limit: u64, enforced: bool) -> Result<Self, RuntimeError> {
        if lease_id.trim().is_empty() || limit == 0 {
            return Err(RuntimeError::new(
                "evaluation provider token lease identity/limit is invalid",
            ));
        }
        Ok(Self {
            state: std::sync::Mutex::new(EvaluationProviderTokenLeaseState {
                lease_id: lease_id.to_string(),
                limit,
                enforced,
                remaining: limit,
                variance_remaining: (limit / 20).max(1),
                telemetry_consumed: 0,
                input_consumed: 0,
                output_consumed: 0,
                cached_consumed: 0,
                outstanding: 0,
                breached: false,
            }),
        })
    }

    pub(crate) fn snapshot(&self) -> Result<EvaluationProviderTokenLeaseSnapshot, RuntimeError> {
        let lease = self
            .state
            .lock()
            .map_err(|_| RuntimeError::new("evaluation provider token lease lock poisoned"))?;
        Ok(EvaluationProviderTokenLeaseSnapshot {
            lease_id: lease.lease_id.clone(),
            limit: lease.limit,
            consumed: if lease.enforced {
                lease.limit.saturating_sub(lease.remaining)
            } else {
                lease.telemetry_consumed
            },
            input_consumed: lease.input_consumed,
            output_consumed: lease.output_consumed,
            cached_consumed: lease.cached_consumed,
            outstanding: lease.outstanding,
            breached: lease.breached,
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct EvaluationProviderTokenLeaseRegistry {
    leases: std::sync::Mutex<BTreeMap<String, Arc<EvaluationProviderTokenLease>>>,
}

impl EvaluationProviderTokenLeaseRegistry {
    #[cfg(test)]
    pub(crate) fn install(
        self: &Arc<Self>,
        session_id: &str,
        lease_id: &str,
        limit: u64,
    ) -> Result<EvaluationProviderTokenLeaseGuard, RuntimeError> {
        self.install_with_mode(session_id, lease_id, limit, true)
    }

    /// Install an evaluation lease that records usage without using the
    /// nominal observation capacity as an execution stop condition.
    pub(crate) fn install_advisory(
        self: &Arc<Self>,
        session_id: &str,
        lease_id: &str,
        limit: u64,
    ) -> Result<EvaluationProviderTokenLeaseGuard, RuntimeError> {
        self.install_with_mode(session_id, lease_id, limit, false)
    }

    fn install_with_mode(
        self: &Arc<Self>,
        session_id: &str,
        lease_id: &str,
        limit: u64,
        enforced: bool,
    ) -> Result<EvaluationProviderTokenLeaseGuard, RuntimeError> {
        if session_id.trim().is_empty() {
            return Err(RuntimeError::new(
                "evaluation provider token lease session identity is invalid",
            ));
        }
        let lease = Arc::new(EvaluationProviderTokenLease::new(
            lease_id, limit, enforced,
        )?);
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| RuntimeError::new("evaluation provider token registry lock poisoned"))?;
        if leases.contains_key(session_id) {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease already exists for session `{session_id}`"
            )));
        }
        leases.insert(session_id.to_string(), Arc::clone(&lease));
        Ok(EvaluationProviderTokenLeaseGuard {
            session_id: session_id.to_string(),
            registry: Arc::clone(self),
            lease,
        })
    }

    pub(crate) fn get(
        &self,
        session_id: &str,
    ) -> Result<Option<Arc<EvaluationProviderTokenLease>>, RuntimeError> {
        self.leases
            .lock()
            .map(|leases| leases.get(session_id).cloned())
            .map_err(|_| RuntimeError::new("evaluation provider token registry lock poisoned"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvaluationProviderTokenLeaseSnapshot {
    pub lease_id: String,
    pub limit: u64,
    pub consumed: u64,
    pub input_consumed: u64,
    pub output_consumed: u64,
    pub cached_consumed: u64,
    pub outstanding: usize,
    pub breached: bool,
}

pub(crate) struct EvaluationProviderTokenLeaseGuard {
    session_id: String,
    registry: Arc<EvaluationProviderTokenLeaseRegistry>,
    lease: Arc<EvaluationProviderTokenLease>,
}

impl Drop for EvaluationProviderTokenLeaseGuard {
    fn drop(&mut self) {
        let Ok(mut leases) = self.registry.leases.lock() else {
            return;
        };
        if leases
            .get(&self.session_id)
            .is_some_and(|current| Arc::ptr_eq(current, &self.lease))
        {
            leases.remove(&self.session_id);
        }
    }
}

impl EvaluationProviderTokenLeaseGuard {
    pub(crate) fn lease(&self) -> Arc<EvaluationProviderTokenLease> {
        Arc::clone(&self.lease)
    }

    pub(crate) fn snapshot(&self) -> Result<EvaluationProviderTokenLeaseSnapshot, RuntimeError> {
        self.lease.snapshot()
    }
}

struct EvaluationProviderTokenReservation {
    lease: Arc<EvaluationProviderTokenLease>,
    reserved: u64,
    reconciled: bool,
    dispatched: bool,
    advisory: bool,
}

impl EvaluationProviderTokenReservation {
    fn acquire(
        lease: Option<&Arc<EvaluationProviderTokenLease>>,
        request: &mut ApiRequest,
    ) -> Result<Option<Self>, RuntimeError> {
        let Some(lease) = lease else {
            return Ok(None);
        };
        let mut state = lease
            .state
            .lock()
            .map_err(|_| RuntimeError::new("evaluation provider token lease lock poisoned"))?;
        if !state.enforced {
            state.outstanding = state.outstanding.saturating_add(1);
            drop(state);
            return Ok(Some(Self {
                lease: Arc::clone(lease),
                reserved: 0,
                reconciled: false,
                dispatched: false,
                advisory: true,
            }));
        }
        if state.breached {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease `{}` is already breached",
                state.lease_id
            )));
        }
        // Reserve a conservative upper bound before touching the provider.
        // Input estimation, protocol framing and the normal request safety
        // margin are all charged; the remainder becomes the provider-enforced
        // maximum output. The Session-scoped Arc is shared by Team children
        // and their parent without charging unrelated conversations.
        let input_reserve = request
            .budget
            .input_total_tokens()
            .saturating_add(request.budget.protocol_overhead_tokens)
            .saturating_add(request.budget.safety_margin_tokens);
        let available = state.remaining.saturating_add(state.variance_remaining);
        if input_reserve >= available {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease `{}` has {} tokens remaining but request input reserves {}",
                state.lease_id, available, input_reserve
            )));
        }
        let output_reserve = request
            .budget
            .requested_output_tokens
            .min(available.saturating_sub(input_reserve));
        if output_reserve == 0 {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease `{}` has no output capacity",
                state.lease_id
            )));
        }
        request.budget.requested_output_tokens = output_reserve;
        let reserved = input_reserve.saturating_add(output_reserve);
        let primary = reserved.min(state.remaining);
        state.remaining = state.remaining.saturating_sub(primary);
        state.variance_remaining = state
            .variance_remaining
            .saturating_sub(reserved.saturating_sub(primary));
        state.outstanding = state.outstanding.saturating_add(1);
        drop(state);
        Ok(Some(Self {
            lease: Arc::clone(lease),
            reserved,
            reconciled: false,
            dispatched: false,
            advisory: false,
        }))
    }

    fn mark_dispatched(&mut self) {
        self.dispatched = true;
    }

    fn reconcile(&mut self, usage: impl Into<Option<TokenUsage>>) {
        if self.reconciled {
            return;
        }
        // Missing/uncertain usage retains the original dispatched charge.
        let Some(usage) = usage.into() else {
            return;
        };
        let input = u64::from(usage.input_tokens);
        let output = u64::from(usage.output_tokens);
        let cached = u64::from(usage.cache_creation_input_tokens)
            .saturating_add(u64::from(usage.cache_read_input_tokens));
        let actual = input.saturating_add(output).saturating_add(cached);
        if let Ok(mut lease) = self.lease.state.lock() {
            if self.advisory || !lease.enforced {
                lease.telemetry_consumed = lease.telemetry_consumed.saturating_add(actual);
                lease.input_consumed = lease.input_consumed.saturating_add(input);
                lease.output_consumed = lease.output_consumed.saturating_add(output);
                lease.cached_consumed = lease.cached_consumed.saturating_add(cached);
                lease.outstanding = lease.outstanding.saturating_sub(1);
                self.reconciled = true;
                return;
            }
            lease.input_consumed = lease.input_consumed.saturating_add(input);
            lease.output_consumed = lease.output_consumed.saturating_add(output);
            lease.cached_consumed = lease.cached_consumed.saturating_add(cached);
            if actual <= self.reserved {
                lease.remaining = lease
                    .remaining
                    .saturating_add(self.reserved.saturating_sub(actual))
                    .min(lease.limit);
            } else {
                // Provider tokenizers are authoritative while the preflight
                // budget is necessarily an estimate. A measured variance is
                // not a global lease breach when still-unreserved Session
                // headroom can pay it. Outstanding concurrent reservations
                // have already been removed from `remaining`, so this charge
                // cannot steal their capacity or exceed the hard lease.
                let unreserved_delta = actual.saturating_sub(self.reserved);
                let available = lease.remaining.saturating_add(lease.variance_remaining);
                if unreserved_delta <= available {
                    let primary = unreserved_delta.min(lease.remaining);
                    lease.remaining = lease.remaining.saturating_sub(primary);
                    lease.variance_remaining = lease
                        .variance_remaining
                        .saturating_sub(unreserved_delta.saturating_sub(primary));
                } else {
                    lease.breached = true;
                    lease.remaining = 0;
                }
            }
            lease.outstanding = lease.outstanding.saturating_sub(1);
            self.reconciled = true;
        }
    }
}

impl Drop for EvaluationProviderTokenReservation {
    fn drop(&mut self) {
        if self.reconciled {
            return;
        }
        if let Ok(mut lease) = self.lease.state.lock() {
            if !self.advisory && !self.dispatched {
                lease.remaining = lease
                    .remaining
                    .saturating_add(self.reserved)
                    .min(lease.limit);
            }
            lease.outstanding = lease.outstanding.saturating_sub(1);
        }
    }
}

/// One pre-dispatch reservation transaction across the explicitly bound
/// evaluation budget and the delegated child budget. If either admission
/// fails, every reservation already acquired by this function is dropped
/// before the caller can reach Provider IO.
struct ProviderTokenReservationSet {
    evaluation: Option<EvaluationProviderTokenReservation>,
    delegated: Option<crate::execution_core::budget::DurableProviderBudgetReservation>,
}

impl ProviderTokenReservationSet {
    fn acquire(
        evaluation_lease: Option<&Arc<EvaluationProviderTokenLease>>,
        delegated_budget: Option<&(
            crate::execution_core::budget::ParentExecutionBudgetLedger,
            harness_contract::context::ChildExecutionBudgetReservation,
        )>,
        model: &str,
        request: &mut ApiRequest,
    ) -> Result<Self, RuntimeError> {
        let evaluation = EvaluationProviderTokenReservation::acquire(evaluation_lease, request)?;
        let delegated = delegated_budget
            .map(|(ledger, child)| {
                let input_reserve = request
                    .budget
                    .input_total_tokens()
                    .saturating_add(request.budget.protocol_overhead_tokens)
                    .saturating_add(request.budget.safety_margin_tokens);
                ledger
                    .reserve_provider(
                        child,
                        format!("provider-budget:{}", uuid::Uuid::new_v4()),
                        model,
                        input_reserve,
                        request.budget.requested_output_tokens,
                    )
                    .map_err(RuntimeError::new)
            })
            .transpose()?;
        if let Some(reservation) = delegated.as_ref() {
            request.budget.requested_output_tokens = reservation.granted_output_tokens;
        }
        Ok(Self {
            evaluation,
            delegated,
        })
    }

    fn mark_dispatched(&mut self) {
        if let Some(reservation) = self.evaluation.as_mut() {
            reservation.mark_dispatched();
        }
        // A durable delegated reservation is intentionally not rolled back
        // after this point: crash/unknown usage conservatively consumes it.
    }

    fn reconcile(&mut self, usage: impl Into<Option<TokenUsage>>) -> Result<(), RuntimeError> {
        let usage = usage.into();
        if let Some(reservation) = self.evaluation.as_mut() {
            reservation.reconcile(usage);
        }
        if let Some(reservation) = self.delegated.as_mut() {
            reservation.reconcile(usage).map_err(RuntimeError::new)?;
        }
        Ok(())
    }
}
use crate::compact::{
    apply_compaction_summary, estimate_session_tokens, plan_session_compaction, CompactionConfig,
};
use crate::config::{RuntimeFeatureConfig, SessionCompactConfig as RuntimeSessionCompactConfig};
use crate::context_runtime::{
    ContextAuthority, ContextEnvelope, ContextEnvelopeRequest, ContextIdentity, ContextItem,
    ContextOmission, ContextProfile, ContextRole, ContextRuntimeKernel, ContextSourceKind,
    ContextVisibility, PersistedContextEnvelope, ResumeContextPacket, RuntimeContextFactDecision,
    RuntimeContextGovernanceReport, ToolTracePacket, ToolTraceStatus,
    CONTEXT_RENDER_FORMATTER_VERSION, PERSISTED_CONTEXT_ENVELOPE_SCHEMA_VERSION,
};
use crate::context_tool_exposure::{ToolExposurePlanner, ToolExposurePolicy, ToolExposureState};
use crate::fact_extraction::{
    FactExtractionRuntimeEvent, RuleFactExtractor, RuntimeFactExtractionInput,
    RuntimeFactExtractionPolicy, RuntimeFactExtractionScheduler, RuntimeFactExtractionTrigger,
    RuntimeFactExtractor,
};
use crate::governed_tool_executor::{
    GovernedToolAdmission, GovernedToolExecutionContext, GovernedToolExecutor, GovernedToolFuture,
    GovernedToolTaskTerminal,
};
use crate::governed_tool_plan::{
    GovernedToolCompiler, GovernedToolPlan, GovernedToolPolicyValidationReport,
};
use crate::hooks::{HookAbortSignal, HookProgressReporter, HookRunResult, HookRunner};
use crate::knowledge_activation::KnowledgeActivationRuntime;
use crate::permissions::{PermissionContext, PermissionPolicy};
use crate::runtime_control::RuntimeControlPolicy;
use crate::runtime_harness::{RuntimeAiKernel, RuntimeAiKernelTrace};
use crate::session::{ContentBlock, ConversationMessage, MessageEvent, Session, SessionEventLog};
use crate::skill::{
    memory_candidate_from_skill_activation, skill_memory_candidate_session_event,
    SkillActivationEngine, SkillActivationInput, SkillMemoryPolicy,
};
use crate::tool_invocation::{
    now_ms, ToolFailureKind, ToolInvocationRecord, DEFAULT_OUTPUT_REF_MIN_LINES,
};
use crate::usage::UsageTracker;
use crate::PromptAssembly;
use crate::{
    HistoryView, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
};
use model_protocol::usage::TokenUsage;

fn provider_output_budget_hint(
    model: &str,
    context_window: u32,
    max_output_override: Option<u32>,
) -> u32 {
    let max_output = provider::model_max_output_resolution(model, max_output_override);
    let budget = ProviderOutputBudget::derive(ProviderOutputBudgetInputs {
        context_window_tokens: u64::from(context_window),
        max_output_tokens: u64::from(max_output.tokens),
        fixed_input_tokens: 0,
        required_input_tokens: 0,
        protocol_overhead_tokens: 0,
        safety_margin_tokens: 0,
    });
    u32::try_from(budget.requested_output_tokens).unwrap_or(u32::MAX)
}

fn provider_transport_policy(
    context_window: u32,
    request: &ApiRequest,
) -> crate::ProviderTransportPolicy {
    let prompt_chars = request.prompt.estimated_chars();
    let message_chars = request
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .map(|block| match block {
            ContentBlock::Text { text } => text.chars().count(),
            ContentBlock::ToolResult { output, .. } => output.chars().count(),
            _ => 0,
        })
        .sum::<usize>();
    crate::ProviderTransportPolicy::derive(
        context_window,
        prompt_chars.saturating_add(message_chars),
    )
}

fn retrieve_tool_evidence_from_sandbox(
    sandbox: Option<&Arc<std::sync::Mutex<memory::ToolOutputSandbox>>>,
    input: &str,
) -> Result<String, String> {
    let request: serde_json::Value = serde_json::from_str(input)
        .map_err(|error| format!("invalid evidence retrieval input: {error}"))?;
    let evidence_ref = request
        .get("evidence_ref")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "evidence_ref is required".to_string())?;
    let evidence_id = evidence_ref.strip_prefix("tool://").unwrap_or(evidence_ref);
    let query = request
        .get("query")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    let limit = request
        .get("limit")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(4)
        .clamp(1, 16) as usize;
    let sandbox = sandbox.ok_or_else(|| "tool evidence sandbox is unavailable".to_string())?;
    let sandbox = sandbox
        .lock()
        .map_err(|_| "tool evidence sandbox lock poisoned".to_string())?;
    let mut snippets = if query.is_empty() {
        sandbox.read(evidence_id, limit)
    } else {
        sandbox.search(evidence_id, query, limit)
    };
    if snippets.is_empty() && !query.is_empty() {
        let normalized_query = query
            .split(|character: char| !character.is_alphanumeric())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        if normalized_query != query && !normalized_query.is_empty() {
            snippets = sandbox.search(evidence_id, &normalized_query, limit);
        }
    }
    if snippets.is_empty() {
        return Err(format!(
            "no indexed evidence matched `{evidence_ref}`; use the session evidence API for the immutable raw payload"
        ));
    }
    serde_json::to_string_pretty(
        &snippets
            .iter()
            .map(|snippet| {
                serde_json::json!({
                    "range_start": snippet.line_start,
                    "range_end": snippet.line_end,
                    "content": snippet.content,
                })
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| format!("evidence retrieval serialization failed: {error}"))
}

fn classify_model_step_intent(text: String, calls: Vec<ModelToolCall>) -> ModelStepIntent {
    if calls.is_empty() {
        ModelStepIntent::FinalAnswer { text }
    } else {
        // Function identity is owned by the exposed tool catalog and the
        // tool's typed input contract. Names such as
        // `permission_status` are ordinary tools; they must never create an
        // Agent, Team, approval, or replan merely because of a substring.
        // Stateful orchestration remains the responsibility of Runtime's
        // validated native control contracts.
        ModelStepIntent::ToolCalls { calls }
    }
}

fn unexposed_model_tool_names(
    calls: &[ModelToolCall],
    exposed_tool_ids: &BTreeSet<String>,
) -> Vec<String> {
    calls
        .iter()
        .filter(|call| !exposed_tool_ids.contains(&call.name))
        .map(|call| call.name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn normalized_tool_identity(name: &str) -> String {
    name.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn canonicalize_model_tool_names<T: ToolExecutor>(calls: &mut [ModelToolCall], tool_executor: &T) {
    for call in calls {
        if let Some(canonical) = tool_executor.resolve_tool_name(&call.name) {
            if canonical == call.name {
                continue;
            }
            tracing::info!(
                provider_tool_name = %call.name,
                canonical_tool_name = %canonical,
                "resolved provider tool alias through the authoritative tool catalog"
            );
            call.name = canonical;
        }
    }
}

/// An explicit user requirement to actually form a team is an acceptance
/// constraint, not merely a prose preference. It takes precedence over a
/// heuristic strategy recommendation: otherwise a correctly parsed user
/// requirement can disappear just because the lightweight classifier chose a
/// different execution pattern. Generic complex work remains model-directed
/// and is never forced through this path.
fn enforce_explicit_team_requirement(
    _objective: &str,
    first_step: bool,
    decision: &crate::execution_core::RuntimeExecutionDecision,
    intent: ModelStepIntent,
) -> ModelStepIntent {
    let Some(obligation) = decision.collaboration_obligation.as_ref() else {
        return intent;
    };
    if !first_step {
        return intent;
    }

    tracing::info!(
        explicit_team = true,
        minimum_team_count = obligation.minimum_team_count,
        intent_kind = ?match &intent {
            ModelStepIntent::ToolCalls { .. } => "tool_calls",
            ModelStepIntent::FinalAnswer { .. } => "final_answer",
            ModelStepIntent::Replan { .. } => "replan",
        },
        "explicit team requirement enforcing orchestration call"
    );

    // The first move belongs entirely to the model. Team/role names are
    // display metadata authored by the model, never a hardcoded Runtime
    // decision; Runtime never substitutes a builtin Team here. If the model
    // finishes without a verified Team execution, the parent final-answer
    // acceptance gate re-prompts it to orchestrate (see host.rs), and only
    // after bounded attempts reports an honest partial result.
    intent
}

fn apply_explicit_team_requirement(
    enabled: bool,
    objective: &str,
    first_step: bool,
    decision: &crate::execution_core::RuntimeExecutionDecision,
    intent: ModelStepIntent,
) -> ModelStepIntent {
    if enabled {
        enforce_explicit_team_requirement(objective, first_step, decision, intent)
    } else {
        intent
    }
}

fn is_agent_collaboration_action_name(name: &str) -> bool {
    harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .iter()
        .any(|action| name.eq_ignore_ascii_case(action))
}

fn is_agent_collaboration_action(call: &ModelToolCall) -> bool {
    is_agent_collaboration_action_name(&call.name)
}

/// Fully assembled request payload sent to the upstream model client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRequest {
    pub preparation: crate::ProviderRequestPreparation,
    pub bound_step: Option<Arc<crate::step_snapshot::BoundStep>>,
    pub prompt: PromptAssembly,
    pub messages: HistoryView,
    /// Runtime-selected primary model ID.
    pub model: String,
    /// Runtime-owned one-shot reasoning policy for this provider attempt.
    /// The transport adapter decides whether the selected model supports the
    /// requested effort; unsupported backends retain their configured policy.
    pub reasoning_effort_override: Option<String>,
    /// Whether Runtime reused a previously compiled immutable request basis.
    /// This is diagnostic metadata only and is never serialized to Provider.
    pub request_compiler_cache_hit: bool,
    /// Request-local capacity contract used for diagnostics and ledger
    /// reconciliation. Provider must not mutate routing or budget ownership.
    pub budget: crate::context_ledger::RequestBudgetReport,
    /// Request-local durable evidence coordinates. The Provider adapter uses
    /// this only after producing its exact protocol body and before network IO.
    pub provider_evidence_context: Option<crate::ProviderRequestEvidenceContext>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProviderContextInventory {
    pub tool_count: usize,
    pub tool_schema_tokens: u64,
    pub catalog_revision: u64,
    pub exposure_revision: u64,
    pub schema_fingerprint: u64,
    pub provider_registry_revision: u64,
}

/// Streamed events emitted while processing a single assistant turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssistantItemKind {
    Text,
    PublicReasoning,
    PrivateReasoning,
    ToolCall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssistantEvent {
    /// The provider/model that actually accepted this request. This is emitted
    /// only after the provider has produced a protocol event, so it is never
    /// mistaken for a configured fallback that was merely considered.
    ProviderModel {
        identity: harness_contract::outcome::ProviderIdentity,
    },
    ItemStarted {
        index: u32,
        provider_item_id: Option<String>,
        kind: AssistantItemKind,
    },
    ItemCompleted {
        index: u32,
    },
    TextDelta(String),
    /// Provider-approved reasoning summary safe for public projection.
    ReasoningSummaryDelta(String),
    /// Provider-private reasoning retained only for protocol round-trip. It
    /// must never be projected as a public reasoning summary.
    PrivateReasoningDelta(String),
    /// P1-7: Thinking signature that must be preserved and passed back
    /// to the provider in subsequent requests.
    SignatureDelta(String),
    ToolUse {
        id: String,
        name: String,
        input: String,
    },
    Usage(TokenUsage),
    MessageStop,
    /// Provider terminal reason for the current message (e.g. `end_turn` vs
    /// `length`). Forwarded so the runtime can tell a normal finish from a
    /// max-token truncation (PLAN W2 Step1). It does not itself end the step;
    /// `MessageStop` does.
    MessageBoundary {
        stop_reason: Option<String>,
    },
    /// P0-2: Tool execution lifecycle events for real-time SSE visualization
    ToolStart {
        id: String,
        name: String,
        preview: String,
    },
    ToolProgress {
        id: String,
        name: String,
        progress: String,
    },
    ToolComplete {
        id: String,
        name: String,
        result_summary: String,
        exit_code: Option<i32>,
    },
}

/// Whether an assistant event represents real semantic progress (and may
/// therefore refresh the provider idle deadline). Empty lifecycle frames —
/// item boundaries and the initial provider-identity frame — must not, or a
/// peer could keep a stalled stream "alive" by sending only those (PLAN C6-1).
fn assistant_event_is_semantic(event: &AssistantEvent) -> bool {
    match event {
        AssistantEvent::ProviderModel { .. }
        | AssistantEvent::ItemStarted { .. }
        | AssistantEvent::ItemCompleted { .. }
        // A boundary only carries the terminal reason; it is not progress and
        // must not refresh the idle deadline (a peer could emit stop_reason on
        // every frame). See PLAN W2 Step2/Step3.
        | AssistantEvent::MessageBoundary { .. } => false,
        AssistantEvent::TextDelta(text)
        | AssistantEvent::ReasoningSummaryDelta(text)
        | AssistantEvent::PrivateReasoningDelta(text)
        | AssistantEvent::SignatureDelta(text) => !text.is_empty(),
        AssistantEvent::ToolUse { .. }
        | AssistantEvent::Usage(_)
        | AssistantEvent::MessageStop
        | AssistantEvent::ToolStart { .. }
        | AssistantEvent::ToolProgress { .. }
        | AssistantEvent::ToolComplete { .. } => true,
    }
}

#[derive(Debug)]
struct ModelStreamItemState {
    identity: crate::CausalItemIdentity,
    kind: AssistantItemKind,
    content: String,
    completed: bool,
}

struct ModelStreamReducer {
    bound_step: Option<Arc<crate::step_snapshot::BoundStep>>,
    bus: Option<Arc<crate::CowdEventBus>>,
    event_store: Option<Arc<RuntimeEventStore>>,
    session_id: String,
    model_step_id: String,
    items: BTreeMap<u32, ModelStreamItemState>,
    active_text: Option<u32>,
    active_public_reasoning: Option<u32>,
    active_private_reasoning: Option<u32>,
    synthetic_index: u32,
    text: String,
    public_reasoning: String,
    private_reasoning: String,
    signature: String,
    calls: Vec<ModelToolCall>,
    usage: TokenUsage,
    usage_observed: bool,
    effective_provider_identity: Option<harness_contract::outcome::ProviderIdentity>,
    first_event_at: Option<Instant>,
    first_text_at: Option<Instant>,
    /// The provider's terminal reason, kept once (the first boundary wins).
    stop_reason: Option<String>,
    terminal_presentation: Option<(String, String)>,
}

#[derive(Debug, Clone)]
pub(crate) struct EarlyToolCandidate {
    pub call: ModelToolCall,
    pub identity: crate::CausalItemIdentity,
    pub ready_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ToolLifecycleReuse {
    /// The actual early execution owner retained its record before publication.
    PendingOriginal {
        record: crate::ToolInvocationRecord,
        producer: harness_contract::projection::RuntimeActivityBinding,
    },
    /// The effect is recovered from its original durable receipt. No new
    /// invocation lifecycle may be manufactured for it.
    DurableEffectReplay { original_lifecycle: Option<crate::DurableRuntimeEvent> },
    /// The actual lifecycle owner could not be recovered; never invent one.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EarlyToolExecutionReceipt {
    pub lifecycle: ToolLifecycleReuse,
    pub call: ModelToolCall,
    pub outcome: crate::RuntimeToolExecutionOutcome,
    pub ready_at_ms: u64,
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EarlyToolDeferral {
    pub tool_call_id: String,
    pub reason: String,
    pub ready_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EarlyToolFailureKind {
    AuthorizationCommitUnknown,
    WorkerOutcomeUnknown,
}

/// Original identity and operation whose outcome could not be established.
/// This is an infrastructure failure, never a policy refusal receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EarlyToolFailure {
    pub identity: crate::CausalItemIdentity,
    pub tool_call_id: String,
    pub owner_ticket: Option<crate::execution_core::NodeExecutionTicket>,
    pub kind: EarlyToolFailureKind,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EarlyToolDispatchResult {
    Executed(EarlyToolExecutionReceipt),
    Deferred(EarlyToolDeferral),
    Failed(EarlyToolFailure),
}

pub(crate) type EarlyToolDispatchFuture =
    Pin<Box<dyn Future<Output = EarlyToolDispatchResult> + Send + 'static>>;

pub(crate) trait EarlyToolDispatcher: Send + Sync {
    fn dispatch(&self, candidate: EarlyToolCandidate) -> EarlyToolDispatchFuture;
}

/// Append-only model-step plan used while Provider items are still arriving.
///
/// It does not compile or execute an open DAG. It only freezes every explicitly
/// completed Tool call once, rejects identity reuse with changed arguments, and
/// lets the finalized batch compiler remain the sole DAG authority at seal.
#[derive(Debug, Default)]
struct ModelStepToolPlan {
    calls: BTreeMap<String, ModelToolCall>,
    order: Vec<String>,
    sealed: bool,
}

impl ModelStepToolPlan {
    fn append(
        &mut self,
        candidate: EarlyToolCandidate,
    ) -> Result<Option<EarlyToolCandidate>, String> {
        if self.sealed {
            return Err("model step tool plan is already sealed".to_string());
        }
        if let Some(existing) = self.calls.get(&candidate.call.id) {
            if existing == &candidate.call {
                return Ok(None);
            }
            return Err(format!(
                "provider reused tool call id `{}` with changed name, arguments, or dependencies",
                candidate.call.id
            ));
        }
        self.order.push(candidate.call.id.clone());
        self.calls
            .insert(candidate.call.id.clone(), candidate.call.clone());
        Ok(Some(candidate))
    }

    fn seal(&mut self, finalized_calls: &[ModelToolCall]) -> Result<(), String> {
        if self.sealed {
            return Err("model step tool plan was sealed more than once".to_string());
        }
        self.sealed = true;

        let mut finalized = BTreeMap::new();
        for call in finalized_calls {
            if finalized.insert(call.id.clone(), call).is_some() {
                return Err(format!(
                    "provider finalized duplicate tool call id `{}`",
                    call.id
                ));
            }
        }
        for call_id in &self.order {
            let appended = self.calls.get(call_id).ok_or_else(|| {
                format!("model step append order references missing call `{call_id}`")
            })?;
            let Some(sealed) = finalized.get(call_id) else {
                return Err(format!(
                    "completed tool call `{call_id}` disappeared before model step seal"
                ));
            };
            if appended != *sealed {
                return Err(format!(
                    "completed tool call `{call_id}` changed before model step seal"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(feature = "test-fixtures")]
pub(crate) fn k02_apply_fixture_event(bus: &crate::CowdEventBus, event: AssistantEvent) -> Result<(), String> {
    let mut reducer = ModelStreamReducer::new(Some(Arc::new(bus.clone())), None, "fixture-session".into());
    reducer.apply(event, Instant::now()).map(|_| ()).map_err(|error| error.to_string())
}

impl ModelStreamReducer {
    fn new(
        bus: Option<Arc<crate::CowdEventBus>>,
        event_store: Option<Arc<RuntimeEventStore>>,
        session_id: String,
    ) -> Self {
        let model_step_id = bus.as_ref().map_or_else(
            || format!("{session_id}:model-step:unscoped"),
            |bus| bus.next_model_step_id(),
        );
        if let Some(bus) = &bus {
            bus.emit(crate::CowdEvent::ModelStepStarted {
                model_step_id: model_step_id.clone(),
            });
        }
        Self {
            bus,
            bound_step: None,
            event_store,
            session_id,
            model_step_id,
            items: BTreeMap::new(),
            active_text: None,
            active_public_reasoning: None,
            active_private_reasoning: None,
            synthetic_index: u32::MAX,
            text: String::new(),
            public_reasoning: String::new(),
            private_reasoning: String::new(),
            signature: String::new(),
            calls: Vec::new(),
            usage: TokenUsage::default(),
            usage_observed: false,
            effective_provider_identity: None,
            first_event_at: None,
            first_text_at: None,
            stop_reason: None,
            terminal_presentation: None,
        }
    }

    fn with_terminal_presentation(
        mut self,
        presentation_id: impl Into<String>,
        attempt_id: impl Into<String>,
    ) -> Self {
        self.terminal_presentation = Some((presentation_id.into(), attempt_id.into()));
        self
    }

    fn with_bound_step(mut self, step: Option<Arc<crate::step_snapshot::BoundStep>>) -> Self {
        self.bound_step = step;
        self
    }

    fn next_synthetic_index(&mut self) -> u32 {
        let index = self.synthetic_index;
        self.synthetic_index = self.synthetic_index.saturating_sub(1);
        index
    }

    fn item_identity(
        &self,
        index: u32,
        provider_item_id: Option<&str>,
        kind: AssistantItemKind,
    ) -> crate::CausalItemIdentity {
        let item_id = provider_item_id
            .filter(|id| !id.trim().is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("{}:item:{index}", self.model_step_id));
        let segment_kind = match kind {
            AssistantItemKind::Text => "text",
            AssistantItemKind::PublicReasoning => "reasoning-summary",
            AssistantItemKind::PrivateReasoning => "private-reasoning",
            AssistantItemKind::ToolCall => "tool-call",
        };
        crate::CausalItemIdentity {
            model_step_id: self.model_step_id.clone(),
            item_id: item_id.clone(),
            segment_id: format!("{item_id}:{segment_kind}:0"),
            causal_sequence: self
                .bus
                .as_ref()
                .map_or(0, |bus| bus.next_causal_sequence()),
            delta_sequence: 0,
            tool_call_id: (kind == AssistantItemKind::ToolCall).then_some(item_id),
            causal_parent_ids: Vec::new(),
        }
    }

    fn start_item(&mut self, index: u32, provider_item_id: Option<&str>, kind: AssistantItemKind) {
        if self.items.contains_key(&index) {
            return;
        }
        let identity = self.item_identity(index, provider_item_id, kind);
        if kind != AssistantItemKind::PrivateReasoning {
            if let (Some(bus), Some(public_kind)) = (&self.bus, public_causal_item_kind(kind)) {
                let binding = reasoning_activity_binding(bus, &identity, kind);
                bus.emit_causal_with_activity_binding(
                    identity.clone(),
                    crate::CowdEvent::ItemStarted { kind: public_kind },
                    binding,
                );
            }
        }
        match kind {
            AssistantItemKind::Text => self.active_text = Some(index),
            AssistantItemKind::PublicReasoning => self.active_public_reasoning = Some(index),
            AssistantItemKind::PrivateReasoning => self.active_private_reasoning = Some(index),
            AssistantItemKind::ToolCall => {}
        }
        self.items.insert(
            index,
            ModelStreamItemState {
                identity,
                kind,
                content: String::new(),
                completed: false,
            },
        );
    }

    fn ensure_active(&mut self, kind: AssistantItemKind) -> u32 {
        let active = match kind {
            AssistantItemKind::Text => self.active_text,
            AssistantItemKind::PublicReasoning => self.active_public_reasoning,
            AssistantItemKind::PrivateReasoning => self.active_private_reasoning,
            AssistantItemKind::ToolCall => None,
        };
        if let Some(index) = active {
            return index;
        }
        let index = self.next_synthetic_index();
        self.start_item(index, None, kind);
        index
    }

    fn append_public_delta(&mut self, index: u32, value: &str, reasoning: bool) {
        let Some(item) = self.items.get_mut(&index) else {
            return;
        };
        item.content.push_str(value);
        item.identity.delta_sequence = item.identity.delta_sequence.saturating_add(1);
        // Hard gate: a structured JSON contract must never leak into the
        // visible Markdown stream as raw JSON deltas. Once the item starts
        // with `{`, buffer it and defer a single synthesized Markdown item
        // until the item completes.
        if !reasoning && item.content.trim_start().starts_with('{') {
            return;
        }
        if let Some(bus) = &self.bus {
            if reasoning && item.identity.delta_sequence == 1 {
                bus.emit(crate::CowdEvent::ExecutionPhase {
                    status: harness_contract::projection::ExecutionLiveStatus::Thinking,
                    detail: Some("public_reasoning_summary".to_string()),
                });
            }
            let event = if reasoning {
                crate::CowdEvent::ReasoningSummaryDelta {
                    summary: value.to_string(),
                }
            } else {
                crate::CowdEvent::TextDelta {
                    text: value.to_string(),
                }
            };
            let binding = reasoning_activity_binding(bus, &item.identity, item.kind);
            bus.emit_causal_with_activity_binding(item.identity.clone(), event, binding);
            if !reasoning {
                if let Some((presentation_id, attempt_id)) = &self.terminal_presentation {
                    let byte_end = u64::try_from(self.text.len()).unwrap_or(u64::MAX);
                    let byte_start =
                        byte_end.saturating_sub(u64::try_from(value.len()).unwrap_or(u64::MAX));
                    bus.emit(crate::CowdEvent::TerminalDelivery {
                        delivery: harness_contract::live::TerminalDeliveryEvent::TextDelta {
                            presentation_id: presentation_id.clone(),
                            attempt_id: attempt_id.clone(),
                            byte_start,
                            byte_end,
                            delta: value.to_string(),
                        },
                    });
                }
            }
        }
    }

    fn complete_item(&mut self, index: u32) -> Result<Option<EarlyToolCandidate>, RuntimeError> {
        let Some(item) = self.items.get_mut(&index) else {
            return Ok(None);
        };
        if item.completed {
            return Ok(None);
        }
        item.completed = true;
        let ready_tool_identity =
            (item.kind == AssistantItemKind::ToolCall).then(|| item.identity.clone());
        match item.kind {
            AssistantItemKind::Text if self.active_text == Some(index) => self.active_text = None,
            AssistantItemKind::PublicReasoning if self.active_public_reasoning == Some(index) => {
                self.active_public_reasoning = None;
            }
            AssistantItemKind::PrivateReasoning if self.active_private_reasoning == Some(index) => {
                self.active_private_reasoning = None;
            }
            _ => {}
        }
        if item.kind == AssistantItemKind::Text && item.content.trim_start().starts_with('{') {
            let visible = visible_markdown_from_json(&item.content);
            if let Some(bus) = &self.bus {
                bus.emit_synthetic_text_item("json-answer", &visible);
            }
        }
        let Some(kind) = public_causal_item_kind(item.kind) else {
            return Ok(None);
        };
        let planned_tool = item
            .identity
            .tool_call_id
            .as_deref()
            .and_then(|tool_call_id| self.calls.iter().find(|call| call.id == tool_call_id))
            .cloned();
        if let Some(store) = &self.event_store {
            let mut refs = vec![
                RuntimeEventRef {
                    kind: "model_step".to_string(),
                    id: item.identity.model_step_id.clone(),
                },
                RuntimeEventRef {
                    kind: "model_item".to_string(),
                    id: item.identity.item_id.clone(),
                },
            ];
            if let Some(context) = self
                .bus
                .as_ref()
                .and_then(|bus| bus.current_execution_context())
            {
                refs.push(RuntimeEventRef {
                    kind: "execution".to_string(),
                    id: context.execution_id,
                });
                refs.push(RuntimeEventRef {
                    kind: "session".to_string(),
                    id: context.session_id,
                });
                refs.push(RuntimeEventRef {
                    kind: "turn".to_string(),
                    id: context.turn_id,
                });
            }
            let payload = serde_json::json!({
                "model_step_id": item.identity.model_step_id,
                "item_id": item.identity.item_id,
                "segment_id": item.identity.segment_id,
                "causal_sequence": item.identity.causal_sequence,
                "kind": kind,
                "content": item.content,
                "tool_call_id": item.identity.tool_call_id,
                "tool_name": planned_tool.as_ref().map(|call| call.name.as_str()),
                "causal_parent_ids": item.identity.causal_parent_ids,
            });
            let mut input = RuntimeEventInput {
                stream_id: format!("session:{}", self.session_id),
                scope: RuntimeEventScope::Session,
                kind: "model.item_completed".to_string(),
                status: Some("completed".to_string()),
                actor: Some("conversation_runtime.model_stream".to_string()),
                refs,
                payload,
            };
            if let Some(binding) = self
                .bus
                .as_ref()
                .and_then(|bus| reasoning_activity_binding(bus, &item.identity, item.kind))
            {
                input = input.with_activity_binding(binding).map_err(|error| {
                    RuntimeError::new(format!(
                        "public reasoning activity binding is invalid: {error}"
                    ))
                })?;
            }
            store.append(input).map_err(|error| {
                RuntimeError::new(format!(
                    "persist completed model item `{}`: {error}",
                    item.identity.item_id
                ))
            })?;
        }
        if let Some(bus) = &self.bus {
            let binding = reasoning_activity_binding(bus, &item.identity, item.kind);
            bus.emit_causal_with_activity_binding(
                item.identity.clone(),
                crate::CowdEvent::ItemCompleted {
                    kind,
                    tool_name: planned_tool.as_ref().map(|call| call.name.clone()),
                    tool_input: planned_tool.as_ref().map(|call| call.input.clone()),
                },
                binding,
            );
        }
        Ok(ready_tool_identity.and_then(|identity| {
            planned_tool.map(|call| EarlyToolCandidate {
                call,
                identity,
                ready_at_ms: now_ms(),
            })
        }))
    }

    fn complete_incomplete_items(&mut self) -> Result<(), RuntimeError> {
        let incomplete = self
            .items
            .iter()
            .filter_map(|(index, item)| (!item.completed).then_some(*index))
            .collect::<Vec<_>>();
        for index in incomplete {
            let _ = self.complete_item(index)?;
        }
        Ok(())
    }

    fn early_tool_start_enabled(&self) -> bool {
        self.effective_provider_identity
            .as_ref()
            .and_then(|identity| identity.capabilities.get("early_tool_start"))
            .is_some_and(|value| value == "enabled")
    }

    fn apply(
        &mut self,
        event: AssistantEvent,
        observed_at: Instant,
    ) -> Result<(bool, Option<EarlyToolCandidate>), RuntimeError> {
        self.first_event_at.get_or_insert(observed_at);
        let mut ready = None;
        let stop = match event {
            AssistantEvent::ProviderModel { identity } => {
                self.effective_provider_identity = Some(identity);
                false
            }
            AssistantEvent::ItemStarted {
                index,
                provider_item_id,
                kind,
            } => {
                self.start_item(index, provider_item_id.as_deref(), kind);
                false
            }
            AssistantEvent::ItemCompleted { index } => {
                ready = self.complete_item(index)?;
                false
            }
            AssistantEvent::TextDelta(delta) => {
                if !delta.is_empty() { if let Some(bus) = &self.bus { bus.observe_semantic_progress(); } }
                self.first_text_at.get_or_insert(observed_at);
                self.text.push_str(&delta);
                let index = self.ensure_active(AssistantItemKind::Text);
                self.append_public_delta(index, &delta, false);
                false
            }
            AssistantEvent::ReasoningSummaryDelta(delta) => {
                if !delta.is_empty() { if let Some(bus) = &self.bus { bus.observe_semantic_progress(); } }
                self.public_reasoning.push_str(&delta);
                let index = self.ensure_active(AssistantItemKind::PublicReasoning);
                self.append_public_delta(index, &delta, true);
                false
            }
            AssistantEvent::PrivateReasoningDelta(delta) => {
                if !delta.is_empty() { if let Some(bus) = &self.bus { bus.observe_semantic_progress(); } }
                self.private_reasoning.push_str(&delta);
                let index = self.ensure_active(AssistantItemKind::PrivateReasoning);
                if let Some(item) = self.items.get_mut(&index) {
                    item.content.push_str(&delta);
                }
                false
            }
            AssistantEvent::SignatureDelta(delta) => {
                self.signature.push_str(&delta);
                false
            }
            AssistantEvent::ToolUse { id, name, input } => {
                let index = self
                    .items
                    .iter()
                    .find_map(|(index, item)| {
                        (item.kind == AssistantItemKind::ToolCall
                            && item.identity.tool_call_id.as_deref() == Some(id.as_str()))
                        .then_some(*index)
                    })
                    .unwrap_or_else(|| {
                        let index = self.next_synthetic_index();
                        self.start_item(index, Some(id.as_str()), AssistantItemKind::ToolCall);
                        index
                    });
                if let Some(item) = self.items.get_mut(&index) {
                    item.content = input.clone();
                    item.identity.tool_call_id = Some(id.clone());
                }
                self.calls.push(ModelToolCall {
                    binding: self.bound_step.as_ref().map(|step| {
                        crate::step_snapshot::StepCallBinding::provider(Arc::clone(step), &id)
                    }),
                    id,
                    name,
                    input,
                    depends_on: Vec::new(),
                });
                false
            }
            AssistantEvent::Usage(value) => {
                self.usage = value;
                self.usage_observed = true;
                false
            }
            AssistantEvent::MessageStop => {
                // A successful Provider terminal closes synthetic items that
                // did not have a protocol-level item-stop frame. Persistence
                // must succeed before the model step can become terminal.
                self.emit_public_action_summary_if_needed()?;
                self.complete_incomplete_items()?;
                true
            }
            AssistantEvent::MessageBoundary { stop_reason } => {
                // The first terminal reason is the only one that counts: a
                // provider repeating it must not reset the boundary.
                if self.stop_reason.is_none() {
                    self.stop_reason = stop_reason;
                }
                false
            }
            AssistantEvent::ToolStart { .. }
            | AssistantEvent::ToolProgress { .. }
            | AssistantEvent::ToolComplete { .. } => false,
        };
        Ok((stop, ready))
    }

    fn emit_public_action_summary_if_needed(&mut self) -> Result<(), RuntimeError> {
        if !self.public_reasoning.trim().is_empty() || self.calls.is_empty() {
            return Ok(());
        }
        // A public action summary is a business activity, not transport
        // diagnostics. Internal probes without a canonical activity owner
        // must not create an orphan lifecycle event.
        if self
            .bus
            .as_ref()
            .and_then(|bus| bus.current_activity_binding())
            .is_none()
        {
            return Ok(());
        }
        let summary = public_action_summary(&self.text, &self.calls);
        if summary.is_empty() {
            return Ok(());
        }
        let index = self.next_synthetic_index();
        self.start_item(
            index,
            Some("runtime-public-action-summary"),
            AssistantItemKind::PublicReasoning,
        );
        self.append_public_delta(index, &summary, true);
        Ok(())
    }

    fn finish(self, consumer_status: &str) -> CollectedProviderStream {
        // This is the reducer's local result, not a lifecycle publication.
        // The outer owned exchange alone publishes the merged completion.
        tracing::trace!(
            consumer_status,
            "provider consumer reduced; producer settlement pending"
        );
        CollectedProviderStream {
            text: self.text,
            public_reasoning: self.public_reasoning,
            private_reasoning: self.private_reasoning,
            signature: self.signature,
            calls: self.calls,
            usage: self.usage,
            usage_observed: self.usage_observed,
            effective_provider_identity: self.effective_provider_identity,
            first_event_at: self.first_event_at,
            first_text_at: self.first_text_at,
            stop_reason: self.stop_reason,
            early_tool_receipts: Vec::new(),
            early_tool_deferrals: Vec::new(),
            early_tool_failures: Vec::new(),
            response_completed_at_ms: now_ms(),
        }
    }
}

fn visible_markdown_from_json(text: &str) -> String {
    let zh = user_reply_language(text) == "zh";
    let fallback = if zh {
        "正在整理最终答案…".to_string()
    } else {
        "Preparing final answer…".to_string()
    };
    let Ok(serde_json::Value::Object(object)) =
        serde_json::from_str::<serde_json::Value>(text.trim())
    else {
        return fallback;
    };
    for field in ["answer", "summary", "final_answer", "content"] {
        if let Some(serde_json::Value::String(value)) = object.get(field) {
            if !value.trim().is_empty() {
                return value.clone();
            }
        }
    }
    fallback
}

/// Detect the language of the user's original message so runtime-generated
/// replies follow it instead of hardcoding one language.
pub(crate) fn user_reply_language(text: &str) -> &'static str {
    if text
        .chars()
        .any(|character| matches!(character, '\u{3400}'..='\u{4dbf}' | '\u{4e00}'..='\u{9fff}'))
    {
        "zh"
    } else {
        "en"
    }
}

fn public_causal_item_kind(kind: AssistantItemKind) -> Option<crate::CausalItemKind> {
    match kind {
        AssistantItemKind::Text => Some(crate::CausalItemKind::Text),
        AssistantItemKind::PublicReasoning => Some(crate::CausalItemKind::PublicReasoning),
        AssistantItemKind::ToolCall => Some(crate::CausalItemKind::ToolCall),
        AssistantItemKind::PrivateReasoning => None,
    }
}

fn reasoning_activity_binding(
    bus: &crate::CowdEventBus,
    identity: &crate::CausalItemIdentity,
    kind: AssistantItemKind,
) -> Option<harness_contract::projection::RuntimeActivityBinding> {
    if kind != AssistantItemKind::PublicReasoning {
        return bus.current_activity_binding();
    }
    let mut binding = bus.current_activity_binding()?;
    let owner_activity_id = binding.activity_id.clone();
    binding.activity_id = crate::cowd_event::owned_child_activity_id(
        &binding,
        "reasoning",
        &format!("{}:{}", identity.model_step_id, identity.item_id),
    );
    binding.parent_activity_id = Some(owner_activity_id.clone());
    binding.initiator_activity_id = Some(owner_activity_id);
    binding.node_id = None;
    binding.skill_id = None;
    binding.skill_revision = None;
    binding.skill_activation_id = None;
    binding.tool_contract_id = None;
    binding.tool_call_id = None;
    binding.approval_id = None;
    Some(binding)
}

#[derive(Default)]
struct CollectedProviderStream {
    text: String,
    public_reasoning: String,
    private_reasoning: String,
    signature: String,
    calls: Vec<ModelToolCall>,
    usage: TokenUsage,
    usage_observed: bool,
    effective_provider_identity: Option<harness_contract::outcome::ProviderIdentity>,
    first_event_at: Option<Instant>,
    first_text_at: Option<Instant>,
    /// The provider's terminal reason (`length` means the step was truncated by
    /// the output ceiling and may be continued with a bounded prefix).
    stop_reason: Option<String>,
    early_tool_receipts: Vec<EarlyToolExecutionReceipt>,
    early_tool_deferrals: Vec<EarlyToolDeferral>,
    early_tool_failures: Vec<EarlyToolFailure>,
    response_completed_at_ms: u64,
}

impl CollectedProviderStream {
    fn observed_usage(&self, completed: bool) -> Option<TokenUsage> {
        (self.usage_observed && completed).then_some(self.usage)
    }
}

#[derive(Debug, Clone, Copy)]
struct ProviderStreamTimeoutPolicy {
    idle: Duration,
    heartbeat_grace: Duration,
}

struct ProviderStreamRun {
    collected: CollectedProviderStream,
    failure: Option<RuntimeError>,
    resource_result_class: crate::execution_core::graph::ResourceResultClass,
}

const FAILED_PROVIDER_EARLY_TOOL_DRAIN_GRACE: Duration = Duration::from_millis(100);

#[cfg(test)]
async fn consume_provider_stream(
    stream: Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>,
    cancellation: CancellationToken,
    timeout_policy: Option<ProviderStreamTimeoutPolicy>,
    reducer: ModelStreamReducer,
    early_dispatcher: Option<Arc<dyn EarlyToolDispatcher>>,
) -> ProviderStreamRun {
    consume_provider_stream_with_activity(
        stream,
        cancellation,
        timeout_policy,
        reducer,
        early_dispatcher,
        None,
    )
    .await
}

#[cfg(test)]
async fn consume_provider_stream_with_activity(
    stream: Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>,
    cancellation: CancellationToken,
    timeout_policy: Option<ProviderStreamTimeoutPolicy>,
    reducer: ModelStreamReducer,
    early_dispatcher: Option<Arc<dyn EarlyToolDispatcher>>,
    transport_activity: Option<provider::TransportActivity>,
) -> ProviderStreamRun {
    consume_owned_provider_stream(
        stream,
        cancellation,
        timeout_policy,
        reducer,
        early_dispatcher,
        transport_activity,
        None,
    )
    .await
}

async fn consume_owned_provider_stream(
    stream: Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>,
    cancellation: CancellationToken,
    timeout_policy: Option<ProviderStreamTimeoutPolicy>,
    reducer: ModelStreamReducer,
    early_dispatcher: Option<Arc<dyn EarlyToolDispatcher>>,
    transport_activity: Option<provider::TransportActivity>,
    execution: Option<ProviderExecution<'_>>,
) -> ProviderStreamRun {
    use futures::FutureExt;
    let completion_bus = reducer.bus.clone();
    let model_step_id = reducer.model_step_id.clone();
    let consumer = consume_provider_stream_inner(
        stream,
        cancellation,
        timeout_policy,
        reducer,
        early_dispatcher,
        transport_activity,
    );
    let (consumed, producer_result) = match execution {
        Some(execution) => execution.drive(consumer).await,
        None => (
            std::panic::AssertUnwindSafe(consumer)
                .catch_unwind()
                .await
                .map_err(|_| ()),
            Ok(ProviderLocalExit::Completed),
        ),
    };
    let mut run = consumed.unwrap_or_else(|_| ProviderStreamRun {
        collected: CollectedProviderStream::default(),
        failure: Some(RuntimeError::new("provider consumer panicked")),
        resource_result_class: crate::execution_core::graph::ResourceResultClass::Failed,
    });
    if let Ok(ProviderLocalExit::UserCancelled(proof)) = &producer_result {
        if run.resource_result_class == crate::execution_core::graph::ResourceResultClass::Cancelled || run.failure.is_none() {
            let error = run.failure.take().unwrap_or_else(|| RuntimeError::with_provider_failure_metadata(
                "provider operation stopped by original authorized cancellation", None, false,
                crate::execution_core::graph::ResourceResultClass::Cancelled));
            run.failure = Some(error.with_authenticated_provider_cancellation((**proof).clone()));
            run.resource_result_class = crate::execution_core::graph::ResourceResultClass::Cancelled;
        }
    }
    if matches!(producer_result, Ok(ProviderLocalExit::Stopped)) && run.failure.is_none() {
        run.failure = Some(RuntimeError::with_provider_failure_metadata(
            "provider operation stopped",
            None,
            false,
            crate::execution_core::graph::ResourceResultClass::Cancelled,
        ));
        run.resource_result_class = crate::execution_core::graph::ResourceResultClass::Cancelled;
    }
    if let Err(error) = producer_result {
        // Receiver cancellation/timeout is the primary result. The producer's
        // expected stop acknowledgement must not relabel it as Failed.
        if run.failure.is_none() {
            run.failure = Some(error);
            run.resource_result_class = crate::execution_core::graph::ResourceResultClass::Failed;
        }
    }
    // Publish exactly once, only after the owned physical future has returned
    // and its result and protocol validation have joined the consumer result.
    if let Some(bus) = completion_bus {
        bus.emit(crate::CowdEvent::ModelStepCompleted {
            model_step_id,
            status: if run.failure.is_some() {
                "failed"
            } else {
                "completed"
            }
            .into(),
        });
    }
    run
}

async fn consume_provider_stream_inner(
    mut stream: Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>,
    cancellation: CancellationToken,
    timeout_policy: Option<ProviderStreamTimeoutPolicy>,
    mut reducer: ModelStreamReducer,
    early_dispatcher: Option<Arc<dyn EarlyToolDispatcher>>,
    transport_activity: Option<provider::TransportActivity>,
) -> ProviderStreamRun {
    use futures::StreamExt;

    let mut failure = None;
    let mut resource_result_class = crate::execution_core::graph::ResourceResultClass::Completed;
    let mut early_workers = Vec::new();
    let mut early_worker_identities = Vec::new();
    let mut early_tool_deferrals = Vec::new();
    let mut tool_plan = ModelStepToolPlan::default();
    // Transport heartbeats only prove that the socket is alive. They must not
    // extend a model turn indefinitely when the provider emits no semantic
    // event (text/reasoning/tool/usage/stop). Keep one deadline owned by the
    // semantic stream and refresh it only after an actual AssistantEvent.
    let mut semantic_deadline = timeout_policy.map(|policy| {
        Instant::now()
            .checked_add(policy.idle.saturating_add(policy.heartbeat_grace))
            .unwrap_or_else(Instant::now)
    });
    // Raw content that never reaches a step boundary (tool call / terminal) is
    // the one pattern a peer can use to stream forever: refresh the idle window
    // with non-empty deltas but make no syntactically meaningful progress. Bound
    // it per attempt (generously above any legitimate single-step output) so such
    // a stream fails instead of running without bound (PLAN W2 Step2 / N1).
    // Reasoning deltas are bounded separately (and much more generously) because
    // a legitimate "think first, act later" step can be far larger than the
    // answer itself; killing it would be a capability loss (PLAN W2 / review).
    const MAX_UNBOUNDED_CONTENT_BYTES: u64 = 8 * 1024 * 1024;
    const MAX_UNBOUNDED_REASONING_BYTES: u64 = 64 * 1024 * 1024;
    let mut unbounded_content_bytes: u64 = 0;
    let mut unbounded_reasoning_bytes: u64 = 0;
    loop {
        let next = if let Some(policy) = timeout_policy {
            loop {
                let idle_window = policy.idle.saturating_add(policy.heartbeat_grace);
                let remaining = semantic_deadline
                    .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                    .unwrap_or(idle_window);
                if remaining.is_zero() {
                    resource_result_class =
                        crate::execution_core::graph::ResourceResultClass::TimedOut;
                    failure = Some(RuntimeError::new(format!(
                        "stream stalled after {}s without semantic activity",
                        idle_window.as_secs()
                    )));
                    break None;
                }
                let generation = transport_activity
                    .as_ref()
                    .map(provider::TransportActivity::generation)
                    .unwrap_or_default();
                let activity_changed = async {
                    if let Some(activity) = &transport_activity {
                        activity.changed_since(generation).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                let polled = tokio::select! {
                    () = cancellation.cancelled() => {
                        resource_result_class =
                            crate::execution_core::graph::ResourceResultClass::Cancelled;
                        failure = Some(RuntimeError::new(
                            "turn cancelled during provider stream",
                        ));
                        None
                    }
                    next = tokio::time::timeout(remaining, stream.next()) => {
                        match next {
                            Ok(next) => next,
                            Err(_) => {
                                resource_result_class =
                                    crate::execution_core::graph::ResourceResultClass::TimedOut;
                                failure = Some(RuntimeError::new(format!(
                                    "stream stalled after {}s without semantic activity",
                                    idle_window.as_secs()
                                )));
                                None
                            }
                        }
                    }
                    () = activity_changed => {
                        crate::execution_core::performance::observe_count(
                            "provider_transport_pulse_total",
                            1,
                        );
                        continue;
                    }
                };
                break polled;
            }
        } else {
            tokio::select! {
                () = cancellation.cancelled() => {
                    resource_result_class =
                        crate::execution_core::graph::ResourceResultClass::Cancelled;
                    failure = Some(RuntimeError::new(
                        "turn cancelled during provider stream",
                    ));
                    None
                }
                next = stream.next() => next,
            }
        };
        let Some(event) = next else {
            break;
        };
        if event.is_ok() {
            if let Some(activity) = &transport_activity {
                activity.observe_consumer_event();
            }
        }
        match event {
            Ok(event) => {
                // Accumulate raw content until a step boundary resets it. A peer
                // cannot keep a step alive with content alone (PLAN W2 Step2).
                match &event {
                    AssistantEvent::TextDelta(text) => {
                        unbounded_content_bytes =
                            unbounded_content_bytes.saturating_add(text.len() as u64);
                    }
                    AssistantEvent::ReasoningSummaryDelta(text)
                    | AssistantEvent::PrivateReasoningDelta(text) => {
                        // Reasoning is not a step boundary, so it accounts
                        // against its own, far larger bound.
                        unbounded_reasoning_bytes =
                            unbounded_reasoning_bytes.saturating_add(text.len() as u64);
                    }
                    AssistantEvent::ToolUse { .. } | AssistantEvent::MessageStop => {
                        unbounded_content_bytes = 0;
                        unbounded_reasoning_bytes = 0;
                    }
                    _ => {}
                }
                let content_exhausted = unbounded_content_bytes > MAX_UNBOUNDED_CONTENT_BYTES;
                let reasoning_exhausted = unbounded_reasoning_bytes > MAX_UNBOUNDED_REASONING_BYTES;
                if content_exhausted || reasoning_exhausted {
                    resource_result_class =
                        crate::execution_core::graph::ResourceResultClass::Failed;
                    failure = Some(RuntimeError::new(format!(
                        "provider stream exceeded {} MiB of content / {} MiB of reasoning without a step boundary",
                        MAX_UNBOUNDED_CONTENT_BYTES / (1024 * 1024),
                        MAX_UNBOUNDED_REASONING_BYTES / (1024 * 1024)
                    )));
                    break;
                }
                // Only semantic progress refreshes the idle deadline. Empty
                // lifecycle frames (content_block_stop / message_start / item
                // boundaries) must not keep a stalled stream alive (PLAN C6-1).
                if timeout_policy.is_some() && assistant_event_is_semantic(&event) {
                    if let Some(policy) = timeout_policy {
                        semantic_deadline = Instant::now()
                            .checked_add(policy.idle.saturating_add(policy.heartbeat_grace))
                            .or(semantic_deadline);
                    }
                }
                let (stop, ready) = match reducer.apply(event, Instant::now()) {
                    Ok(applied) => applied,
                    Err(error) => {
                        resource_result_class =
                            crate::execution_core::graph::ResourceResultClass::Failed;
                        failure = Some(error);
                        break;
                    }
                };
                if let Some(candidate) = ready {
                    match tool_plan.append(candidate) {
                        Ok(Some(candidate)) => {
                            if reducer.early_tool_start_enabled() {
                                if let Some(dispatcher) = &early_dispatcher {
                                    let dispatcher = Arc::clone(dispatcher);
                                    early_worker_identities.push((candidate.call.id.clone(), candidate.identity.clone()));
                                    early_workers.push(tokio::spawn(async move {
                                        dispatcher.dispatch(candidate).await
                                    }));
                                }
                            } else if early_dispatcher.is_some() {
                                early_tool_deferrals.push(EarlyToolDeferral {
                                    tool_call_id: candidate.call.id,
                                    reason:
                                        "provider early_tool_start gate is not performance-certified"
                                            .to_string(),
                                    ready_at_ms: candidate.ready_at_ms,
                                });
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            resource_result_class =
                                crate::execution_core::graph::ResourceResultClass::Failed;
                            failure = Some(RuntimeError::with_provider_failure_metadata(
                                format!("tool_protocol_violation: {error}"),
                                None,
                                true,
                                crate::execution_core::graph::ResourceResultClass::Failed,
                            ));
                            break;
                        }
                    }
                }
                if stop {
                    break;
                }
            }
            Err(error) => {
                resource_result_class = error.provider_resource_result();
                failure = Some(error);
                break;
            }
        }
    }
    drop(stream);
    let response_completed_at_ms = now_ms();
    let mut collected = reducer.finish(if failure.is_some() {
        "failed"
    } else {
        "completed"
    });
    if failure.is_none() {
        if let Err(error) = tool_plan.seal(&collected.calls) {
            resource_result_class = crate::execution_core::graph::ResourceResultClass::Failed;
            failure = Some(RuntimeError::with_provider_failure_metadata(
                format!("tool_protocol_violation: {error}"),
                None,
                true,
                crate::execution_core::graph::ResourceResultClass::Failed,
            ));
        }
    }

    // An early read is speculative until the whole provider frame is valid.
    // Preserve workers that finish inside a tiny global drain window, but do
    // not let an approval wait, host stall or malformed trailing frame hold
    // the foreground turn indefinitely. Aborting is safe here because only
    // descriptor-certified read-only candidates enter this lane.
    let provider_failed = failure.is_some();
    // A speculative early read must not hold the foreground turn whatever the
    // provider outcome is: bound the drain and abort the read-only workers that
    // outlive it, on success as well as failure (PLAN R-05). Aborting is safe
    // because only descriptor-certified read-only candidates enter this lane.
    let joined = if early_workers.is_empty() {
        Vec::new()
    } else {
        match tokio::time::timeout(
            FAILED_PROVIDER_EARLY_TOOL_DRAIN_GRACE,
            futures::future::join_all(early_workers.iter_mut()),
        )
        .await
        {
            Ok(results) => results,
            Err(_) => {
                let mut aborted = 0_u64;
                for worker in &early_workers {
                    if !worker.is_finished() {
                        worker.abort();
                        aborted = aborted.saturating_add(1);
                    }
                }
                crate::execution_core::performance::observe_count(
                    if provider_failed {
                        "early_tool_aborted_after_provider_failure_total"
                    } else {
                        "early_tool_aborted_after_provider_success_total"
                    },
                    aborted,
                );
                futures::future::join_all(early_workers).await
            }
        }
    };
    let mut early_tool_receipts = Vec::new();
    let mut early_tool_failures = Vec::new();
    for (result, (tool_call_id, identity)) in joined.into_iter().zip(early_worker_identities) {
        match result {
            Ok(EarlyToolDispatchResult::Executed(receipt)) => early_tool_receipts.push(receipt),
            Ok(EarlyToolDispatchResult::Deferred(deferral)) => early_tool_deferrals.push(deferral),
            Ok(EarlyToolDispatchResult::Failed(failure)) => early_tool_failures.push(failure),
            Err(error) => early_tool_failures.push(EarlyToolFailure {
                identity, tool_call_id, owner_ticket: None,
                kind: EarlyToolFailureKind::WorkerOutcomeUnknown, detail: error.to_string(),
            }),
        }
    }
    collected.early_tool_receipts = early_tool_receipts;
    collected.early_tool_deferrals = early_tool_deferrals;
    collected.early_tool_failures = early_tool_failures;
    collected.response_completed_at_ms = response_completed_at_ms;
    ProviderStreamRun {
        collected,
        failure,
        resource_result_class,
    }
}

fn preview_chars(value: &str, max_chars: usize) -> String {
    let mut preview: String = value.chars().take(max_chars).collect();
    if value.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
}

fn public_action_summary(text: &str, calls: &[ModelToolCall]) -> String {
    const MAX_PUBLIC_ACTION_CHARS: usize = 640;
    let visible = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if !visible.is_empty() {
        return preview_chars(&visible, MAX_PUBLIC_ACTION_CHARS);
    }
    let mut names = Vec::new();
    for name in calls
        .iter()
        .map(|call| call.name.trim())
        .filter(|name| !name.is_empty())
    {
        if !names.iter().any(|existing| *existing == name) {
            names.push(name);
        }
    }
    if names.is_empty() {
        return String::new();
    }
    let zh = user_reply_language(text) == "zh";
    let shown = names.iter().take(8).copied().collect::<Vec<_>>().join(", ");
    let omitted = names.len().saturating_sub(8);
    if omitted == 0 {
        if zh {
            format!("准备调用 {} 个工具：{shown}", calls.len())
        } else {
            format!("About to call {} tools: {shown}", calls.len())
        }
    } else {
        if zh {
            format!(
                "准备调用 {} 个工具：{shown}，另有 {omitted} 类",
                calls.len()
            )
        } else {
            format!(
                "About to call {} tools: {shown}, plus {omitted} more",
                calls.len()
            )
        }
    }
}

fn millis_since(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn rate_per_second(count: u64, duration_ms: u64) -> Option<f64> {
    if count == 0 || duration_ms == 0 {
        return None;
    }
    Some(count as f64 / (duration_ms as f64 / 1_000.0))
}

/// PLAN E1/E3/E5: the first request of every turn must already expose the
/// orchestration verb, the canonical fact reader and the read-only reduction
/// primitive.
///
/// Regression for the live-scenario failure where `tool_orchestrate`,
/// `execute_code` and `read` were all deferred: with only `read_file` /
/// `grep_search` / `glob_search` visible the model had no way to batch, no way
/// to reduce across files, and no way to resolve the `tool://` reference its
/// own truncation marker told it to use. It therefore re-issued the same
/// single-file read for 26 rounds, 767k input tokens, and verified nothing.
#[test]
fn first_request_exposes_orchestrate_reduce_and_canonical_read() {
    let bootstrap = bootstrap_tool_ids(harness_contract::tool::ToolPermissionMode::ReadOnly);

    for required in [
        // the verb: one instruction drives a bounded program
        "tool_orchestrate",
        // read-only reduction: gather/aggregate/branch without line-by-line reads
        "execute_code",
        // canonical fact reader: makes `tool://` evidence refs retrievable
        "read_ref",
        // baseline evidence tools
        "read_file",
        "grep_search",
        "glob_search",
    ] {
        assert!(
            bootstrap.iter().any(|tool| tool.as_str() == required),
            "bootstrap must expose `{required}` on the first request; got {bootstrap:?}"
        );
    }
}

fn bootstrap_tool_ids(
    _maximum_permission: harness_contract::tool::ToolPermissionMode,
) -> Vec<String> {
    // PLAN E1/D4: the built-in working set is DERIVED from each tool's declared
    // exposure tier, not hand-listed. A hand-written list had already drifted -
    // it named Runtime-owned tools as if they were built-ins, and it had to be
    // edited by hand every time a tool's exposure changed.
    let mut bootstrap = tools::bootstrap_builtin_tool_ids()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    // Runtime-owned tools that are not in the built-in catalog.
    bootstrap.extend(
        ["runtime_capabilities", "context_retrieve", "read_ref"]
            .into_iter()
            .map(str::to_owned),
    );
    // The semantic collaboration actions are always available. They write only
    // Runtime collaboration facts; concrete workspace/external effects keep
    // their independent permission gates. `agent_action_batch` applies several
    // of them in ONE provider round through the same per-action path.
    bootstrap.extend(
        harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
            .iter()
            .map(|tool| (*tool).to_owned()),
    );
    bootstrap.push("agent_action_batch".to_owned());
    bootstrap.sort_unstable();
    bootstrap.dedup();
    bootstrap
}

fn tool_exposure_for_catalog(
    discovery: &harness_contract::tool::ToolDiscoveryReceipt,
    maximum_permission: harness_contract::tool::ToolPermissionMode,
) -> ToolExposureState {
    let policy = ToolExposurePolicy {
        allowed_ids: discovery
            .descriptors
            .iter()
            .map(|descriptor| descriptor.canonical_id.clone())
            .collect(),
        maximum_permission,
        supports_dynamic_exposure: true,
    };
    ToolExposurePlanner.plan(discovery, bootstrap_tool_ids(maximum_permission), &policy)
}

#[derive(Debug, Default)]
struct TurnToolExposureMetrics {
    projection: ToolExposureMetrics,
    activated_ids: BTreeSet<String>,
    invoked_ids: BTreeSet<String>,
    pending_search_round: bool,
    schema_stats_baseline: Option<(u64, u64)>,
}

impl TurnToolExposureMetrics {
    fn reset(&mut self, schema_stats_baseline: (u64, u64)) {
        *self = Self::default();
        self.schema_stats_baseline = Some(schema_stats_baseline);
    }

    fn observe_catalog_lookup(&mut self, elapsed: Duration) {
        self.projection.catalog_lookups = self.projection.catalog_lookups.saturating_add(1);
        self.projection.catalog_lookup_micros = self
            .projection
            .catalog_lookup_micros
            .saturating_add(elapsed.as_micros().min(u128::from(u64::MAX)) as u64);
    }

    fn observe_provider_request(
        &mut self,
        inventory: ProviderContextInventory,
        schema_stats: (u64, u64),
    ) {
        self.projection.provider_requests = self.projection.provider_requests.saturating_add(1);
        if self.pending_search_round {
            self.projection.tool_search_additional_rounds = self
                .projection
                .tool_search_additional_rounds
                .saturating_add(1);
            self.pending_search_round = false;
        }
        self.projection.schema_tokens_max = self
            .projection
            .schema_tokens_max
            .max(inventory.tool_schema_tokens);
        let baseline = self.schema_stats_baseline.get_or_insert(schema_stats);
        self.projection.schema_compilations = schema_stats.0.saturating_sub(baseline.0);
        self.projection.schema_cache_hits = schema_stats.1.saturating_sub(baseline.1);
    }

    fn observe_search(&mut self, receipt: &harness_contract::tool::ToolActivationReceipt) {
        self.projection.tool_search_calls = self.projection.tool_search_calls.saturating_add(1);
        self.observe_activation(receipt);
    }

    fn observe_activation(&mut self, receipt: &harness_contract::tool::ToolActivationReceipt) {
        use harness_contract::tool::ToolActivationStatus;

        self.projection.activation_candidates = self
            .projection
            .activation_candidates
            .saturating_add(receipt.decisions.len() as u64);
        self.pending_search_round = true;
        for decision in &receipt.decisions {
            match decision.status {
                ToolActivationStatus::Activated => {
                    if self.activated_ids.insert(decision.canonical_id.clone()) {
                        self.projection.activations = self.projection.activations.saturating_add(1);
                    }
                }
                ToolActivationStatus::NotFound => {
                    self.projection.descriptor_misses =
                        self.projection.descriptor_misses.saturating_add(1);
                }
                ToolActivationStatus::Denied => {
                    self.projection.permission_rejections =
                        self.projection.permission_rejections.saturating_add(1);
                }
                ToolActivationStatus::Unavailable => {
                    self.projection.unavailable_descriptors =
                        self.projection.unavailable_descriptors.saturating_add(1);
                }
            }
        }
    }

    fn observe_invalid_search(&mut self) {
        self.projection.tool_search_calls = self.projection.tool_search_calls.saturating_add(1);
        self.projection.descriptor_misses = self.projection.descriptor_misses.saturating_add(1);
        self.pending_search_round = true;
    }

    fn observe_invocation(&mut self, tool_name: &str) {
        if self.activated_ids.contains(tool_name) {
            self.invoked_ids.insert(tool_name.to_string());
        }
    }

    fn projection(&self) -> ToolExposureMetrics {
        let mut projection = self.projection.clone();
        projection.activated_invocations =
            self.activated_ids.intersection(&self.invoked_ids).count() as u64;
        projection.activation_precision_bp = (!self.activated_ids.is_empty()).then(|| {
            ratio_bp(
                projection.activated_invocations,
                self.activated_ids.len() as u64,
            )
        });
        // Runtime cannot know the task-specific set of Tools that should have
        // been exposed. A paired evaluator with frozen ground truth owns recall.
        projection.activation_recall_bp = None;
        projection
    }
}

fn ratio_bp(numerator: u64, denominator: u64) -> u16 {
    if denominator == 0 {
        return 0;
    }
    u16::try_from(
        numerator
            .saturating_mul(10_000)
            .saturating_div(denominator)
            .min(10_000),
    )
    .unwrap_or(10_000)
}

#[derive(Debug, Default)]
struct TurnStablePrefixMetrics {
    projection: StablePrefixMetrics,
}

impl TurnStablePrefixMetrics {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn observe_request(&mut self, request: &ApiRequest) {
        let wire = request.prompt.wire_system_text().unwrap_or_default();
        let stable_bytes = request.prompt.stable_system_bytes();
        let stable = wire.as_bytes().get(..stable_bytes).unwrap_or_default();
        let fingerprint = format!(
            "{:016x}",
            model_protocol::fingerprint::stable_hash_bytes(stable)
        );
        if !self.projection.stable_prefix_fingerprint.is_empty()
            && self.projection.stable_prefix_fingerprint != fingerprint
        {
            self.projection.wire_identity_failures =
                self.projection.wire_identity_failures.saturating_add(1);
        }
        if stable.len() != stable_bytes {
            self.projection.wire_identity_failures =
                self.projection.wire_identity_failures.saturating_add(1);
        }
        self.projection.provider_requests = self.projection.provider_requests.saturating_add(1);
        self.projection.stable_prefix_fingerprint = fingerprint;
        self.projection.stable_prefix_bytes = stable.len() as u64;
        self.projection.runtime_system_bytes_max = self.projection.runtime_system_bytes_max.max(
            request
                .prompt
                .runtime_system_text()
                .map_or(0, |text| text.len()) as u64,
        );
        if request.request_compiler_cache_hit {
            self.projection.request_compiler_cache_hits = self
                .projection
                .request_compiler_cache_hits
                .saturating_add(1);
        } else {
            self.projection.request_compiler_compilations = self
                .projection
                .request_compiler_compilations
                .saturating_add(1);
        }
    }

    fn observe_usage(&mut self, usage: TokenUsage) {
        self.projection.native_cache_creation_input_tokens = self
            .projection
            .native_cache_creation_input_tokens
            .saturating_add(u64::from(usage.cache_creation_input_tokens));
        self.projection.native_cache_read_input_tokens = self
            .projection
            .native_cache_read_input_tokens
            .saturating_add(u64::from(usage.cache_read_input_tokens));
    }
}

#[cfg(test)]
mod tool_exposure_contract_tests {
    use super::bootstrap_tool_ids;
    use harness_contract::tool::ToolPermissionMode;

    #[test]
    fn agent_action_loop_is_bootstrapped_independent_of_business_write_ceiling() {
        for ceiling in [
            ToolPermissionMode::ReadOnly,
            ToolPermissionMode::WorkspaceWrite,
        ] {
            let tools = bootstrap_tool_ids(ceiling);
            assert!(harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
                .iter()
                .all(|required| tools.iter().any(|tool| tool == required)));
            assert!(tools.iter().any(|tool| tool == "runtime_capabilities"));
            assert!(tools.iter().any(|tool| tool == "context_retrieve"));
        }
    }
}

fn contract_permission_mode(
    mode: crate::PermissionMode,
) -> harness_contract::tool::ToolPermissionMode {
    match mode {
        crate::PermissionMode::ReadOnly => harness_contract::tool::ToolPermissionMode::ReadOnly,
        crate::PermissionMode::WorkspaceWrite => {
            harness_contract::tool::ToolPermissionMode::WorkspaceWrite
        }
        crate::PermissionMode::DangerFullAccess => {
            harness_contract::tool::ToolPermissionMode::DangerFullAccess
        }
    }
}

fn denied_capability_assessment(
    mut assessment: harness_contract::policy::CapabilityAssessment,
    reason: &str,
    approval_ref: &str,
) -> harness_contract::policy::CapabilityAssessment {
    assessment.path = harness_contract::policy::AuthorizationPath::HardDeny;
    assessment.lease = None;
    assessment.gap = Some(harness_contract::policy::CapabilityGap {
        fingerprint: assessment
            .gap
            .as_ref()
            .map(|gap| gap.fingerprint.clone())
            .unwrap_or_else(|| assessment.assessment_id.clone()),
        kind: harness_contract::policy::CapabilityGapKind::ApprovalRequired,
        capability: assessment.capability.clone(),
        requested_scopes: assessment.requested_scopes.clone(),
        required_mode: assessment.required_mode,
        active_ceiling: assessment.active_ceiling,
        parent_ceiling: assessment.parent_ceiling,
        reason: reason.to_string(),
        safe_alternatives: assessment
            .gap
            .as_ref()
            .map(|gap| gap.safe_alternatives.clone())
            .unwrap_or_default(),
        recoverable: false,
    });
    assessment
        .evidence_refs
        .push(format!("approval:{approval_ref}:{reason}"));
    assessment
}

fn emit_approval_resolution_event(
    cowd: Option<&crate::CowdEventBus>,
    queue: &crate::ApprovalQueue,
    resolution: &Result<crate::ApprovalResolution, String>,
) {
    let (Some(cowd), Ok(resolution)) = (cowd, resolution) else {
        return;
    };
    let Some(request) = queue.get(resolution.approval_id()) else {
        return;
    };
    cowd.emit(crate::cowd_event::CowdEvent::ApprovalResolved {
        request_id: request.approval_id,
        status: request.status,
        scope: request.decision.as_ref().map(|decision| decision.scope),
        actor_id: request
            .decision
            .as_ref()
            .map(|decision| decision.actor.actor_id.clone()),
    });
}

fn apply_runtime_budget_to_control_policy(
    policy: &mut RuntimeControlPolicy,
    budget_plan: &RuntimeBudgetPlan,
) {
    policy.context.yolo_budget_tokens = budget_plan.runtime_control_budget.yolo_budget_tokens;
    policy.context.collaboration_budget_tokens = budget_plan
        .runtime_control_budget
        .collaboration_budget_tokens;
    policy.context.review_budget_tokens = budget_plan.runtime_control_budget.review_budget_tokens;
}

fn knowledge_hard_gate_active(system_prompt: &[String]) -> bool {
    system_prompt
        .iter()
        .any(|fragment| fragment.contains("<hard_gate action=\"block\">"))
}

/// Streaming API contract. Implementors produce AssistantEvents lazily.
/// Consumers poll the stream and process each event as it arrives.
///
/// For backward compatibility, a `collect()` call gathers all events
/// into a Vec (same as the old sync signature).
pub trait ApiClient {
    fn configured_context_window(&self, _model: &str) -> Option<u32> {
        None
    }

    fn prepare_request_budget<'a>(
        &'a self,
        _request: &'a mut ApiRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn step_tool_definitions(&self) -> Option<Vec<crate::ProviderToolDefinition>> {
        None
    }

    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>;

    fn stream_with_transport_activity(&mut self, request: ApiRequest) -> ApiClientStream<'_> {
        ApiClientStream {
            events: self.stream(request),
            transport_activity: None,
            execution: None,
        }
    }

    fn provider_available(&self) -> bool {
        true
    }

    fn provider_name_for_model(&self, _model: &str) -> Option<String> {
        None
    }

    fn configure_tool_exposure(
        &mut self,
        _projection: harness_contract::tool::ToolExposureProjection,
    ) {
    }

    /// Require the next provider request to select one of the currently
    /// exposed tools. The production provider client consumes this setting
    /// exactly once; non-provider test clients may ignore it.
    fn configure_tool_choice_required(&mut self, _required: bool) {}

    /// Optionally bind a required request to one already-exposed native tool.
    /// This is a wire-level selection constraint, not permission or semantic
    /// authority: Runtime still validates the tool input and every resulting
    /// receipt. The default keeps lightweight test clients source-compatible.
    fn configure_tool_choice(&mut self, required: bool, _required_tool_name: Option<String>) {
        self.configure_tool_choice_required(required);
    }

    fn configure_provider_wire_evidence(
        &mut self,
        _writer: Option<Arc<dyn crate::ProviderWireEvidenceWriter>>,
    ) {
    }

    fn configure_turn_cancellation(
        &mut self,
        control: Option<Arc<crate::execution_core::services::TurnCancellationControl>>,
    ) -> Result<(), RuntimeError> {
        if control.is_some() {
            return Err(RuntimeError::new(
                "ApiClient has no physical cancellation source owner",
            ));
        }
        Ok(())
    }

    fn context_inventory(&self) -> ProviderContextInventory {
        ProviderContextInventory::default()
    }

    /// Lifetime Tool schema compilation/cache counters for this client.
    fn tool_schema_cache_stats(&self) -> (u64, u64) {
        (0, 0)
    }

    /// PLAN D1b: liveness push-forward verdicts observed while the last provider
    /// stream was polled. The turn owner drains these and turns each into a
    /// non-forcing nudge on a later request. The default is empty because
    /// lightweight test clients have no stream guard.
    fn take_liveness_stalls(&mut self) -> Vec<crate::liveness::LivenessVerdict> {
        Vec::new()
    }

    /// Convenience: collect all events synchronously (backward compat).
    fn stream_collect(&mut self, request: ApiRequest) -> Result<Vec<AssistantEvent>, RuntimeError> {
        // Drive the same lazy, locally owned provider future inside the runtime
        // that owns its IO; synchronous callers do not create a detached task.
        let collect = async {
            let mut pinned = self.stream(request);
            use futures::StreamExt;
            let mut events = Vec::new();
            while let Some(event) = pinned.next().await {
                events.push(event?);
            }
            Ok(events)
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(collect)
        } else {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    RuntimeError::new(format!("build stream collector runtime: {error}"))
                })?;
            runtime.block_on(collect)
        }
    }
}

pub struct ApiClientStream<'a> {
    pub events: Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + 'a>>,
    pub transport_activity: Option<provider::TransportActivity>,
    pub(crate) execution: Option<ProviderExecution<'a>>,
}

/// The single physical future stays outside the consumer. It is never spawned
/// or handed to a Drop reaper. A retained backend can continue polling it after
/// its receiver, or its original caller, has gone away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProviderLocalExit {
    Completed,
    Stopped,
    UserCancelled(Box<crate::provider_runtime_client::AuthenticatedProviderCancellation>),
}

pub(crate) struct ProviderExecution<'a> {
    work:
        Option<Pin<Box<dyn Future<Output = Result<ProviderLocalExit, RuntimeError>> + Send + 'a>>>,
    stop: CancellationToken,
    result: Option<Result<ProviderLocalExit, RuntimeError>>,
    polled: bool,
}

impl<'a> ProviderExecution<'a> {
    pub(crate) fn new(
        work: impl Future<Output = Result<ProviderLocalExit, RuntimeError>> + Send + 'a,
        stop: CancellationToken,
    ) -> Self {
        use futures::FutureExt;
        Self {
            work: Some(Box::pin(async move {
                std::panic::AssertUnwindSafe(work)
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(RuntimeError::new("provider producer panicked")))
            })),
            stop,
            result: None,
            polled: false,
        }
    }

    fn poll_work(&mut self, cx: &mut std::task::Context<'_>) {
        self.polled = true;
        if let Some(work) = &mut self.work {
            if let std::task::Poll::Ready(result) = work.as_mut().poll(cx) {
                // Destroy the completed future (including its lease captures)
                // before exposing its result to the backend.
                self.work = None;
                self.result = Some(result);
            }
        }
    }

    async fn finish(mut self) -> Result<ProviderLocalExit, RuntimeError> {
        self.stop.cancel();
        futures::future::poll_fn(|cx| {
            self.poll_work(cx);
            if self.work.is_none() {
                std::task::Poll::Ready(self.result.take().unwrap_or_else(|| {
                    Err(RuntimeError::new(
                        "physical producer has no completion result",
                    ))
                }))
            } else {
                std::task::Poll::Pending
            }
        })
        .await
    }

    pub(crate) async fn drive<T>(
        mut self,
        consumer: impl Future<Output = T>,
    ) -> (Result<T, ()>, Result<ProviderLocalExit, RuntimeError>) {
        use futures::FutureExt;
        let mut consumer = Box::pin(std::panic::AssertUnwindSafe(consumer).catch_unwind());
        let consumed = futures::future::poll_fn(|cx| {
            let already_started = self.polled;
            if already_started {
                self.poll_work(cx);
            }
            // A consumer already stopped before dispatch must not start IO just
            // because the producer happens to be polled first.
            if let std::task::Poll::Ready(result) = consumer.as_mut().poll(cx) {
                return std::task::Poll::Ready(result);
            }
            if !already_started {
                self.poll_work(cx);
            }
            consumer.as_mut().poll(cx)
        })
        .await;
        // Drop the receiver before waiting for producer completion. Otherwise a
        // full event queue would keep the two halves waiting on each other.
        drop(consumer);
        (consumed.map_err(|_| ()), self.finish().await)
    }
}

impl<'a> ApiClientStream<'a> {
    /// Plain ApiClient users still drive exactly the same producer. Dropping
    /// this adapter destroys local IO synchronously; there is no background task.
    pub(crate) fn into_stream(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + 'a>> {
        struct Driven<'a> {
            events: Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + 'a>>,
            execution: Option<ProviderExecution<'a>>,
            terminal: Option<Result<AssistantEvent, RuntimeError>>,
            failed: bool,
        }
        impl Stream for Driven<'_> {
            type Item = Result<AssistantEvent, RuntimeError>;
            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                if self.failed {
                    return std::task::Poll::Ready(None);
                }
                if let Some(execution) = &mut self.execution {
                    execution.poll_work(cx);
                    if matches!(
                        execution.result,
                        Some(Err(_)) | Some(Ok(ProviderLocalExit::Stopped | ProviderLocalExit::UserCancelled(_)))
                    ) {
                        let error = match execution.result.take().unwrap() {
                            Err(error) => error,
                            Ok(ProviderLocalExit::UserCancelled(proof)) => {
                                RuntimeError::with_provider_failure_metadata("provider operation stopped by original authorized cancellation", None, false,
                                    crate::execution_core::graph::ResourceResultClass::Cancelled).with_authenticated_provider_cancellation(*proof)
                            }
                            Ok(ProviderLocalExit::Stopped) => {
                                RuntimeError::with_provider_failure_metadata(
                                    "provider operation stopped",
                                    None,
                                    false,
                                    crate::execution_core::graph::ResourceResultClass::Cancelled,
                                )
                            }
                            Ok(ProviderLocalExit::Completed) => unreachable!(),
                        };
                        self.failed = true;
                        self.terminal = None;
                        return std::task::Poll::Ready(Some(Err(error)));
                    }
                }
                let done = self
                    .execution
                    .as_ref()
                    .is_none_or(|execution| execution.work.is_none());
                if self.terminal.is_some() {
                    return if done {
                        std::task::Poll::Ready(self.terminal.take())
                    } else {
                        std::task::Poll::Pending
                    };
                }
                match self.events.as_mut().poll_next(cx) {
                    std::task::Poll::Ready(Some(event @ Ok(AssistantEvent::MessageStop)))
                        if !done =>
                    {
                        self.terminal = Some(event);
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                    std::task::Poll::Ready(None) if !done => std::task::Poll::Pending,
                    other => other,
                }
            }
        }
        Box::pin(Driven {
            events: self.events,
            execution: self.execution,
            terminal: None,
            failed: false,
        })
    }
}
/// Per-invocation model-delivery policy resolved from an immutable execution
/// binding. This is not proof that delivery occurred; Conversation owns that
/// proof after a matching Provider request returns a valid response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ToolModelDeliveryRequirement {
    #[default]
    Bounded,
    Exact {
        obligation_ids: Vec<String>,
    },
}

impl ToolModelDeliveryRequirement {
    #[must_use]
    pub fn exact(mut obligation_ids: Vec<String>) -> Self {
        obligation_ids.sort();
        obligation_ids.dedup();
        if obligation_ids.is_empty() {
            Self::Bounded
        } else {
            Self::Exact { obligation_ids }
        }
    }

    #[must_use]
    pub fn obligation_ids(&self) -> &[String] {
        match self {
            Self::Bounded => &[],
            Self::Exact { obligation_ids } => obligation_ids,
        }
    }

    #[must_use]
    pub const fn is_exact(&self) -> bool {
        matches!(self, Self::Exact { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedModelReceipt {
    provider_invocation_id: String,
    tool_name: String,
    obligation_ids: Vec<String>,
    raw_ref: EvidenceRef,
    model_receipt_sha256: String,
    raw_tokens: u64,
    receipt_tokens: u64,
    omitted_tokens: u64,
    complete: bool,
    presentation_bytes: u64,
    presentation: String,
    source: Option<NativeToolSource>,
    read_body: Option<crate::context_evidence::ReadBodySeal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeToolSource {
    call: ModelToolCall,
    execution: crate::CowdExecutionContext,
    activity_owner: Option<harness_contract::projection::RuntimeActivityBinding>,
    tool_owner: Option<harness_contract::projection::RuntimeActivityBinding>,
}

/// Original provider call retained by the turn producer, not reconstructed
/// from a later tool request or model-visible refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginalRootCall {
    call: ModelToolCall,
    execution: crate::CowdExecutionContext,
    owner: harness_contract::projection::RuntimeActivityBinding,
}

impl OriginalRootCall {
    pub(crate) fn binding_value(&self) -> serde_json::Value {
        serde_json::json!({"call_id":self.call.id,"tool_name":self.call.name,
            "original_input":self.call.input,"step_binding":self.call.binding,
            "depends_on":self.call.depends_on,"execution":self.execution,"owner":self.owner})
    }
    pub(crate) fn matches_owner(&self, context: &crate::CowdExecutionContext,
        owner: &harness_contract::projection::RuntimeActivityBinding) -> bool {
        &self.execution == context && &self.owner == owner
    }
    pub(crate) fn matches_workspace(&self, services: &crate::RuntimeServices) -> bool {
        self.call.binding.as_ref().and_then(|binding| binding.resolve(None).ok())
            .is_some_and(|step| step.lease().workspace_root() == services.workspace_root())
    }
    pub(crate) fn matches_request(&self, request: &crate::RuntimeToolExecutionRequest) -> bool {
        let Some(binding) = request.step_binding.as_ref() else { return false; };
        let Some(invocation) = request.compiled_invocation.as_ref() else { return false; };
        let (Ok(original), Ok(effective)) = (harness_contract::step::parse_strict_json(&self.call.input),
            harness_contract::step::parse_strict_json(&request.input)) else { return false; };
        let Some(original_binding) = self.call.binding.as_ref() else { return false; };
        let Ok(mut derived) = original_binding.prepare_invocation(None, &self.call.name, &original) else { return false; };
        let explicit = invocation.explicit_dependencies.iter().map(|dependency| &dependency.depends_on).collect::<BTreeSet<_>>();
        if explicit.len() != invocation.explicit_dependencies.len()
            || explicit != self.call.depends_on.iter().collect::<BTreeSet<_>>()
            || invocation.explicit_dependencies.iter().any(|edge| edge.invocation_id != self.call.id
                || edge.reason != "model_explicit_dependency")
            || invocation.compiled_dependencies.iter().any(|edge| edge.invocation_id != self.call.id) {
            return false;
        }
        // Dependencies are authored by the original plan compiler after the
        // pinned tool invocation is prepared; every other field is compared
        // structurally with the invocation re-derived from that pinned Step.
        derived.explicit_dependencies = invocation.explicit_dependencies.clone();
        derived.compiled_dependencies = invocation.compiled_dependencies.clone();
        self.call.id == request.tool_use_id && self.call.name == request.tool_name
            && self.call.binding.as_ref() == Some(binding) && binding.effect_idempotency_key() == request.idempotency_key
            && original == effective && effective == invocation.intent.normalized_input && &derived == invocation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeReadAcquisition {
    seal: crate::context_evidence::ReadAcquisitionSeal,
    event_id: String,
    cursor: u64,
    index_event_id: String,
}

#[derive(Debug, Clone)]
struct NativeWireReceipt {
    context: crate::ProviderRequestEvidenceContext,
    request_id: String,
    step_render: Option<harness_contract::step::StepRenderRef>,
    wire_sha256: String,
    wire_artifact: harness_contract::context::ArtifactRef,
    packed_sequence: usize,
    outcome_sequence: Option<usize>,
    results: Vec<crate::context_evidence::WireToolResultSeal>,
}

/// Object-safe asynchronous contract for model-requested Tool execution.
///
/// Implementors must keep native asynchronous work on the calling Runtime.
/// Explicitly blocking libraries and process adapters are isolated by
/// `ToolExecutionPlane`; callers must not construct private runtimes or
/// threads.
#[async_trait::async_trait]
pub trait ToolExecutor: Send + Sync + 'static {
    /// Bind immutable bounded-process context from this executor's original
    /// owner before both policy assessment and execution. This never grants a
    /// capability; the bounded owner still validates the live lease.
    fn bind_bounded_process_request(
        &self,
        _request: &mut crate::RuntimeToolExecutionRequest,
    ) -> Result<(), ToolError> {
        Ok(())
    }

    fn bounded_process_authority(
        &self,
        _request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<crate::tool_host::BoundedProcessAuthority, ToolError> {
        Err(ToolError::new(
            "bounded process live authority is unsupported by this executor",
        ))
    }
    fn bounded_process_lease(
        &self,
        _request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<tools::ToolHostLease, ToolError> {
        Err(ToolError::new(
            "bounded process execution is unsupported by this executor",
        ))
    }

    fn bounded_process_plane(&self) -> Option<Arc<crate::ToolExecutionPlane>> {
        None
    }

    fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
        None
    }

    fn prepare_step_catalog(
        &self,
        _ceiling: harness_contract::tool::ToolPermissionMode,
    ) -> Result<Option<tools::ToolHostLease>, ToolError> {
        Ok(self.pin_step_catalog())
    }

    fn tool_discovery_receipt_for_step(
        &self,
        lease: &tools::ToolHostLease,
        ceiling: harness_contract::tool::ToolPermissionMode,
    ) -> harness_contract::tool::ToolDiscoveryReceipt {
        let available = self
            .available_tool_names()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut receipt = lease.catalog_receipt();
        receipt.descriptors.retain(|descriptor| {
            available.contains(&descriptor.canonical_id)
                && ceiling.permits(descriptor.required_permission)
        });
        receipt.activation_candidates.retain(|name| {
            receipt
                .descriptors
                .iter()
                .any(|descriptor| descriptor.canonical_id == *name)
        });
        receipt
    }

    fn validate_step_tool_input(
        &self,
        binding: Option<&crate::step_snapshot::StepCallBinding>,
        tool_name: &str,
        input: &str,
    ) -> Result<String, ToolError> {
        let Some(binding) = binding else {
            if tool_name == "agent_action_batch" || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&tool_name) {
                return Err(ToolError::new("original action Step binding missing"));
            }
            harness_contract::step::parse_strict_json(input).map_err(|error| ToolError::new(error.to_string()))?;
            return Ok(input.to_string());
        };
        let step = binding
            .resolve(self.pin_step_catalog())
            .map_err(|error| ToolError::new(error.to_string()))?;
        let value = step
            .validate_call(
                &binding.origin,
                &binding.snapshot.identity.session_id,
                &binding.origin.call_id,
                tool_name,
                input,
                chrono::Utc::now().timestamp_millis(),
            )
            .map_err(|error| ToolError::new(error.to_string()))?;
        serde_json::to_string(&value).map_err(|error| ToolError::new(error.to_string()))
    }

    /// Compile each call against the same pinned catalogue retained in its
    /// private call carrier; no later catalogue pin can substitute for it.
    fn prepare_step_invocations(
        &self,
        requests: &[crate::tool_dispatch::ToolRequest],
        calls: &[ModelToolCall],
        pinned: Option<&std::sync::Arc<tools::ToolHostLease>>,
        original_executor: &std::sync::Arc<dyn ToolExecutor>,
    ) -> Result<Vec<crate::governed_tool_plan::OriginalPinnedInvocation>, ToolError> {
        if (self as *const Self as *const ()) != (original_executor.as_ref() as *const dyn ToolExecutor as *const ()) {
            return Err(ToolError::new("original tool executor differs"));
        }
        let mut invocations = Vec::with_capacity(requests.len());
        for request in requests {
            let mut matching = calls.iter().filter(|call| call.id == request.tool_use_id);
            let call = matching.next().ok_or_else(|| ToolError::new("original tool call missing"))?;
            if matching.next().is_some() || call.name != request.tool_name {
                return Err(ToolError::new("original tool call identity mismatch"));
            }
            let (lease, invocation) = if let Some(binding) = call.binding.as_ref() {
                let pinned = pinned.ok_or_else(|| ToolError::new("original Step catalogue unavailable"))?;
                let step = binding.resolve(Some(pinned.as_ref().clone())).map_err(|error| ToolError::new(error.to_string()))?;
                let input = step.validate_call(&binding.origin, &binding.snapshot.identity.session_id,
                    &request.tool_use_id, &request.tool_name, &request.input, chrono::Utc::now().timestamp_millis())
                    .map_err(|error| ToolError::new(error.to_string()))?;
                let mut invocation = step.lease().prepare_governed_invocation(&request.tool_use_id,
                    &request.tool_name, &input, &request.depends_on);
                invocation.origin = Some(binding.origin.clone());
                (Some(std::sync::Arc::clone(pinned)), invocation)
            } else {
                if request.tool_name == "agent_action_batch" || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&request.tool_name.as_str()) {
                    return Err(ToolError::new("original action Step binding missing"));
                }
                let input = harness_contract::step::parse_strict_json(&request.input).map_err(|error| ToolError::new(error.to_string()))?;
                if let Some(pinned) = pinned {
                    pinned.validate_input(&request.tool_name, &input).map_err(|error| ToolError::new(error.to_string()))?;
                    (Some(std::sync::Arc::clone(pinned)), pinned.prepare_governed_invocation(&request.tool_use_id, &request.tool_name, &input, &request.depends_on))
                } else {
                    let validated = original_executor.validate_tool_input(&request.tool_name, &request.input)?;
                    if harness_contract::step::parse_strict_json(&validated).map_err(|error| ToolError::new(error.to_string()))? != input {
                        return Err(ToolError::new("native original input requires replan"));
                    }
                    let mut invocations = original_executor.prepare_governed_invocations(std::slice::from_ref(request));
                    if invocations.len() != 1 { return Err(ToolError::new("native original invocation unavailable")); }
                    (None, invocations.remove(0))
                }
            };
            if invocation.invocation_id != call.id || invocation.intent.tool_name != call.name {
                return Err(ToolError::new("pinned original invocation identity mismatch"));
            }
            invocations.push(crate::governed_tool_plan::OriginalPinnedInvocation::new(invocation, lease, std::sync::Arc::clone(original_executor)));
        }
        Ok(invocations)
    }

    /// Forward an original admitted single action; implementations without
    /// the original owner cannot manufacture this capability.
    async fn execute_root_action_with_invocation(
        &self, _request: &crate::RuntimeToolExecutionRequest,
        _capability: &crate::agentic::action_invocation::VerifiedActionInvocation,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        Err(ToolError::new("unknown: original action carrier unsupported"))
    }

    async fn execute_step_authorized_invocation_output(
        &self,
        _binding: Option<&crate::step_snapshot::StepCallBinding>,
        provider_invocation_id: &str,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        self.execute_authorized_invocation_output(
            provider_invocation_id,
            authorization,
            tool_name,
            input,
        )
        .await
    }

    /// Execute a Tool using the bounded-output contract consumed by Runtime.
    async fn execute_output(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError>;

    /// Execute a Provider-originated invocation while preserving its identity
    /// into the acquisition receipt. Compatibility executors may ignore the
    /// identity; production scoped executors override this method.
    async fn execute_invocation_output(
        &self,
        _provider_invocation_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        self.execute_output(tool_name, input).await
    }

    /// Resolve whether this concrete invocation carries a frozen exact
    /// Provider-model observation obligation. The default preserves ordinary
    /// bounded tool behavior.
    fn model_delivery_requirement(
        &self,
        _tool_name: &str,
        _input: &str,
    ) -> ToolModelDeliveryRequirement {
        ToolModelDeliveryRequirement::Bounded
    }

    /// Runtime-attested evidence produced by this executor so far.
    ///
    /// The default is empty for lightweight/test executors. Production
    /// delegated executors override it with their typed receipt ledger. This
    /// keeps ConversationHost acceptance on the same facts consumed by the
    /// outer Agent validator instead of reparsing tool-result text.
    fn observed_evidence_snapshot(&self) -> Vec<harness_contract::context::ObservedEvidence> {
        Vec::new()
    }

    fn durable_tool_effect_parent(
        &self,
    ) -> Option<(
        harness_contract::execution_graph::ExecutionParentBinding,
        u32,
    )> {
        None
    }

    /// Whether this executor is the sole durable intent/receipt owner for the
    /// concrete tool. The conversation plane owns effects by default. A
    /// scoped delegated executor may override this when it must normalize the
    /// input and persist typed evidence at a narrower authority boundary.
    /// Exactly one layer may return true for an invocation.
    fn owns_durable_tool_effect(&self, _tool_name: &str) -> bool {
        false
    }

    /// A durable delegated leaf acquires its own Tool quota and scope. Its
    /// orchestration caller must not acquire the same resource while waiting.
    fn owns_tool_resource_admission(&self, _tool_name: &str) -> bool {
        false
    }

    /// Validate provider-supplied input before Runtime publishes the ToolUse
    /// block, negotiates permission, or reserves execution capacity, and hand
    /// back the exact bytes Runtime will dispatch.
    /// Production executors bind this check to their pinned catalog; the
    /// fallback keeps lightweight test executors compatible while still
    /// rejecting malformed JSON at the protocol boundary.
    ///
    /// The returned string is authoritative for everything downstream of this
    /// boundary: the transcript's ToolUse block, the governed plan and the
    /// physical dispatch must all describe the same input. A production
    /// executor therefore repairs the one-shot shape corruption it owns - a
    /// field the schema types as array/object/integer arriving as a
    /// JSON-encoded string - here, where the schema is already pinned, instead
    /// of leaving that repair in the dispatch path where the caller can no
    /// longer reach it.
    fn validate_tool_input(&self, _tool_name: &str, input: &str) -> Result<String, ToolError> {
        serde_json::from_str::<serde_json::Value>(input)
            .map(|_| input.to_string())
            .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))
    }

    /// What the schema boundary changed about this exact input.
    ///
    /// Called with the bytes the model sent, before any repair, so the answer
    /// describes the model's frame rather than the repaired one. The default is
    /// empty: an executor that owns no pinned catalog has nothing to report.
    /// Production executors override it so a salvaged cut-off frame is reported
    /// instead of silently repaired.
    fn tool_input_coercions(
        &self,
        _tool_name: &str,
        _input: &str,
    ) -> Vec<harness_contract::tool::ToolInputCoercion> {
        Vec::new()
    }

    fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        match self.pin_step_catalog() {
            Some(lease) => self.tool_discovery_receipt_for_step(
                &lease,
                harness_contract::tool::ToolPermissionMode::DangerFullAccess,
            ),
            None => harness_contract::tool::ToolDiscoveryReceipt {
                query: "catalog-unavailable".into(),
                catalog_revision: 0,
                descriptors: Vec::new(),
                activation_candidates: Vec::new(),
            },
        }
    }

    fn registered_tool_effect(
        &self,
        _tool_name: &str,
        _input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        None
    }

    fn prepare_governed_invocations(
        &self,
        requests: &[crate::tool_dispatch::ToolRequest],
    ) -> Vec<harness_contract::tool::GovernedToolInvocation> {
        let catalog_revision = self.tool_discovery_receipt().catalog_revision;
        requests
            .iter()
            .filter_map(|request| {
                let input = serde_json::from_str::<serde_json::Value>(&request.input).ok()?;
                let effect = self.registered_tool_effect(&request.tool_name, &input)?;
                Some(harness_contract::tool::GovernedToolInvocation {
                    origin: None,
                    contract_version: 1,
                    invocation_id: request.tool_use_id.clone(),
                    intent: harness_contract::tool::ToolIntent {
                        invocation_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                        normalized_input: input,
                    },
                    resource_demand: harness_contract::tool::ResourceDemand {
                        tool_slots: 1,
                        process_slots: u32::from(effect.spawns_process),
                        network_slots: u32::from(effect.uses_network),
                        cpu_weight: if effect.spawns_process { 2 } else { 1 },
                        memory_bytes: 0,
                        scopes: effect
                            .scopes
                            .iter()
                            .filter_map(|scope| {
                                scope.target.clone().map(|key| {
                                    harness_contract::tool::ResourceScopeDemand {
                                        key,
                                        access: if scope.operation
                                            == harness_contract::policy::PermissionOperation::Read
                                        {
                                            harness_contract::tool::ResourceAccess::Read
                                        } else {
                                            harness_contract::tool::ResourceAccess::Write
                                        },
                                    }
                                })
                            })
                            .collect(),
                    },
                    explicit_dependencies: request
                        .depends_on
                        .iter()
                        .map(|depends_on| harness_contract::tool::ToolDependency {
                            invocation_id: request.tool_use_id.clone(),
                            depends_on: depends_on.clone(),
                            reason: "model_explicit_dependency".to_string(),
                        })
                        .collect(),
                    compiled_dependencies: Vec::new(),
                    catalog_revision,
                    descriptor_set_hash: effect.descriptor_hash.clone(),
                    idempotency_key: format!(
                        "{}:{}:{}",
                        request.tool_name, request.tool_use_id, effect.descriptor_hash
                    ),
                    effect,
                })
            })
            .collect()
    }

    async fn execute_authorized_output(
        &self,
        _authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        _input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        Err(ToolError::new(format!(
            "tool `{tool_name}` has no authorized execution implementation"
        )))
    }

    async fn execute_authorized_invocation_output(
        &self,
        _provider_invocation_id: &str,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        self.execute_authorized_output(authorization, tool_name, input)
            .await
    }

    fn has_registered_tools(&self) -> bool {
        !self.available_tool_names().is_empty()
    }

    fn available_tool_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// Resolve a provider-emitted spelling to the one catalog-owned tool id.
    ///
    /// Production executors should delegate to their pinned catalog. The
    /// fallback supports embedded/test executors while remaining fail-closed
    /// when normalized names are ambiguous.
    fn resolve_tool_name(&self, requested: &str) -> Option<String> {
        let available = self.available_tool_names();
        if available.iter().any(|name| name == requested) {
            return Some(requested.to_string());
        }
        let identity = normalized_tool_identity(requested);
        if let Some(canonical) = match identity.as_str() {
            "read" => Some("read_file"),
            "write" => Some("edit"),
            "edit" => Some("edit"),
            "glob" => Some("glob_search"),
            "grep" => Some("grep_search"),
            _ => None,
        }
        .filter(|canonical| available.iter().any(|name| name == canonical))
        {
            return Some(canonical.to_string());
        }
        let mut matches = available
            .into_iter()
            .filter(|name| normalized_tool_identity(name) == identity);
        let resolved = matches.next()?;
        matches.next().is_none().then_some(resolved)
    }

    fn has_tool(&self, tool_name: &str) -> bool {
        self.resolve_tool_name(tool_name).is_some()
    }

    fn classify_tool_safety(
        &self,
        _tool_name: &str,
        _input: &str,
    ) -> Option<crate::tool_orchestrator::ToolSafetyCategory> {
        None
    }

    fn collaboration_runtime_available(&self) -> bool {
        false
    }

    fn mission_runtime_available(&self) -> bool {
        false
    }

    fn bind_execution_decision(&self, _decision: crate::execution_core::RuntimeExecutionDecision) {}
}

/// Tool execution lifecycle callback for real-time visualization.
/// Inspired by hermes-agent stream_consumer.py tool_progress_callback.
pub trait ToolCallback: Send + Sync {
    /// Called when a tool starts executing.
    fn on_tool_start(&self, id: &str, name: &str, preview: &str);
    /// Called when a tool reports progress.
    fn on_tool_progress(&self, id: &str, name: &str, progress: &str);
    /// Called when a tool finishes executing.
    fn on_tool_complete(&self, id: &str, name: &str, result_summary: &str, exit_code: Option<i32>);
    /// Called when token usage data is available (typically after each stream completes).
    /// Default implementation is a no-op so existing implementors don't break.
    fn on_usage(&self, _usage: &TokenUsage) {}
}

/// Memory lifecycle callback for real-time TUI visualization.
/// Follows the same pattern as [`ToolCallback`] so the CLI crate can
/// forward memory events to the TUI render loop.
pub trait MemoryCallback: Send + Sync {
    /// Called when memory context entries are prepared for injection into
    /// the system prompt. Each tuple is `(layer, content, relevance)`.
    fn on_memory_update(&self, entries: Vec<(String, String, f64)>, status: &str);
    /// Called after post-turn memory housekeeping completes
    /// (micro-compact, drift, seeds).
    fn on_memory_stats(&self, total_entries: usize, vector_count: usize, layers: Vec<String>);
}

/// An in-process carrier for the original owner's batch result. This is not
/// serialized and is never an authorization or durable receipt by itself.
#[derive(Clone)]
pub struct PendingAgentBatch(std::sync::Arc<PendingAgentBatchValue>);

pub struct OriginalReplayedPartial {
    pub(crate) replay: std::sync::Arc<crate::agentic::OriginalReplayedAction>,
    /// Original verified physical receipt; the physical driver closes separately.
    pub(crate) physical_receipt: Option<std::sync::Arc<crate::DurableRuntimeEvent>>,
}

/// Same-call packet ceiling outcome retained by the existing node owner.
/// Private fields are original in-process facts, never a new capability.
pub struct OriginalPacketScopePending {
    ticket: crate::execution_core::NodeExecutionTicket,
    fact: std::sync::Arc<crate::agent_in_process_worker::OriginalPacketScopeDenial>,
    request: std::sync::Arc<crate::RuntimeToolExecutionRequest>,
    receipt: Option<std::sync::Arc<crate::execution_core::graph::CommittedCallRefusal>>,
}
impl OriginalPacketScopePending {
    pub(crate) fn matches(&self, ticket: &crate::execution_core::NodeExecutionTicket,
        store: &std::sync::Arc<crate::RuntimeEventStore>, call_id: &str) -> bool {
        &self.ticket == ticket && self.fact.belongs_to(store) && self.request.tool_use_id == call_id
            && crate::execution_core::graph::executors::agent_tool::validate_original_call_request(ticket, &self.request).is_ok()
    }
    pub(crate) fn fact(&self) -> &std::sync::Arc<crate::agent_in_process_worker::OriginalPacketScopeDenial> { &self.fact }
    pub(crate) fn request(&self) -> &crate::RuntimeToolExecutionRequest { &self.request }
    pub(crate) fn receipt(&self) -> Option<&std::sync::Arc<crate::execution_core::graph::CommittedCallRefusal>> { self.receipt.as_ref() }
}

pub struct OriginalBoundedAcquisitionPending {
    ticket: crate::execution_core::NodeExecutionTicket,
    fact: std::sync::Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure>,
    receipt: Option<std::sync::Arc<crate::execution_core::graph::CommittedCallRefusal>>,
}
impl OriginalBoundedAcquisitionPending {
    pub(crate) fn matches(&self, ticket: &crate::execution_core::NodeExecutionTicket,
        store: &std::sync::Arc<crate::RuntimeEventStore>, call_id: &str) -> bool {
        &self.ticket == ticket && self.fact.belongs_to(store) && self.fact.request().tool_use_id == call_id
            && crate::execution_core::graph::executors::agent_tool::validate_original_call_request(ticket, self.fact.request()).is_ok()
    }
    pub(crate) fn fact(&self) -> &std::sync::Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure> { &self.fact }
    pub(crate) fn receipt(&self) -> Option<&std::sync::Arc<crate::execution_core::graph::CommittedCallRefusal>> { self.receipt.as_ref() }
}

/// A boundary recorded by the original sequential producer, not a receipt or authorization.
pub struct OriginalTerminalBatchBoundary {
    pub(crate) sink: crate::ToolProgressSink,
    pub(crate) publication: std::sync::Arc<crate::tool_host::OriginalStopPublication>,
    pub(crate) index: usize,
    pub(crate) total: usize,
}

pub enum PendingAgentBatchValue {
    OriginalBoundedAcquisition(std::sync::Arc<OriginalBoundedAcquisitionPending>),
    OriginalPacketScope(std::sync::Arc<OriginalPacketScopePending>),
    OriginalReplay(OriginalReplayedPartial),
    Prefix(crate::tool_host::AgentBatchCommittedPrefixRead),
    Submission(crate::tool_host::OriginalMemberSubmitOutcome),
    Recorded(Vec<(usize, std::sync::Arc<crate::tool_host::OriginalMemberSubmitOutcome>)>),
    /// Same live owner-bound call transport, never a durable fact.
    RecordedSink(crate::ToolProgressSink),
    OriginalCall(std::sync::Arc<crate::tool_host::OriginalMemberCallCarrier>),
    /// Direct original-owner publication, including current, previous and
    /// failure when the original sink could not record the stop.
    OriginalStopPublication(std::sync::Arc<crate::tool_host::OriginalStopPublication>),
    /// Original sequential producer's terminal boundary; Runtime must authenticate it.
    OriginalTerminalBoundary(OriginalTerminalBatchBoundary),
    /// A separate unresolved phase, never evidence that a member may be settled.
    UnresolvedContext,
    /// Preserve an independently returned stop beside the same-call prefix.
    WithCause { primary: PendingAgentBatch, cause: PendingAgentBatch },
    Calls(Vec<(String, PendingAgentBatch)>),
}

impl PendingAgentBatch {
    pub fn new(value: PendingAgentBatchValue) -> Self { Self(std::sync::Arc::new(value)) }
    /// Called at the original sequential loop's terminal return; does not authenticate a receipt.
    pub fn original_terminal_boundary(
        sink: crate::ToolProgressSink, publication: std::sync::Arc<crate::tool_host::OriginalStopPublication>,
        index: usize, total: usize,
    ) -> Result<Self, String> {
        if total == 0 || total > 25 || index >= total { return Err("original terminal batch bounds differ".into()); }
        let crate::tool_host::OriginalStopPublication::Recorded(stop) = publication.as_ref() else {
            return Err("original terminal publication unresolved".into());
        };
        let crate::tool_host::OriginalStopRead::Available(Some(current)) = sink.original_member_stop() else {
            return Err("original terminal stop unavailable".into());
        };
        let publications = sink.checked_original_stop_publications()?;
        if publications.len() != 1 || !std::sync::Arc::ptr_eq(&publications[0], &publication)
            || !std::sync::Arc::ptr_eq(stop, &current) || !stop.recorded_in_original_sink()
            || !stop.belongs_to_original_sink(&sink) || stop.coordinate().index() != index
            || !matches!(stop.reason(), crate::tool_host::OriginalBatchMemberStopReason::ConfirmedNoncommit(_)) {
            return Err("original terminal publication differs".into());
        }
        let members = sink.checked_original_members()?;
        if members.len() != index || members.iter().enumerate().any(|(expected, (actual, result))|
            expected != *actual || !matches!(result.as_ref(), crate::tool_host::OriginalMemberSubmitOutcome::Committed(_))) {
            return Err("original terminal prefix unavailable".into());
        }
        Ok(Self::new(PendingAgentBatchValue::OriginalTerminalBoundary(OriginalTerminalBatchBoundary {
            sink, publication, index, total,
        })))
    }
    pub fn value(&self) -> &PendingAgentBatchValue { &self.0 }
    pub fn primary_value(&self) -> &PendingAgentBatchValue {
        match self.value() {
            PendingAgentBatchValue::WithCause { primary, .. } => primary.primary_value(),
            value => value,
        }
    }
    pub(crate) fn original_terminal_boundary_ref(&self) -> Option<&OriginalTerminalBatchBoundary> {
        match self.value() {
            PendingAgentBatchValue::OriginalTerminalBoundary(boundary) => Some(boundary),
            PendingAgentBatchValue::WithCause { primary, cause } => match (primary.original_terminal_boundary_ref(), cause.original_terminal_boundary_ref()) {
                (Some(_), Some(_)) => None,
                (Some(boundary), None) | (None, Some(boundary)) => Some(boundary),
                (None, None) => None,
            },
            _ => None,
        }
    }
    pub(crate) fn same_original_terminal_carrier(
        &self, request: &crate::RuntimeToolExecutionRequest, boundary: &OriginalTerminalBatchBoundary,
    ) -> bool {
        match self.value() {
            PendingAgentBatchValue::WithCause { primary, cause } =>
                primary.same_original_terminal_carrier(request, boundary) && cause.same_original_terminal_carrier(request, boundary),
            PendingAgentBatchValue::OriginalTerminalBoundary(value) => std::ptr::eq(value, boundary),
            PendingAgentBatchValue::OriginalCall(carrier) => carrier.original_request() == request
                && carrier.original_request().tool_progress.same_original_channels(&request.tool_progress),
            PendingAgentBatchValue::RecordedSink(sink) => request.tool_progress.same_original_channels(sink),
            PendingAgentBatchValue::Recorded(recorded) => {
                let Ok(actual) = request.tool_progress.checked_original_members() else { return false; };
                recorded.len() == actual.len() && recorded.iter().zip(&actual).all(|((i, a), (j, b))|
                    i == j && std::sync::Arc::ptr_eq(a, b))
            },
            PendingAgentBatchValue::OriginalStopPublication(publication) => std::sync::Arc::ptr_eq(publication, &boundary.publication),
            _ => false,
        }
    }
    pub fn original_stop_publication(&self) -> Option<&std::sync::Arc<crate::tool_host::OriginalStopPublication>> {
        match self.value() {
            PendingAgentBatchValue::OriginalStopPublication(publication) => Some(publication),
            PendingAgentBatchValue::OriginalTerminalBoundary(boundary) => Some(&boundary.publication),
            PendingAgentBatchValue::OriginalCall(carrier) if carrier.stop_publications().len() == 1 =>
                carrier.stop_publications().first(),
            PendingAgentBatchValue::WithCause { primary, cause } =>
                cause.original_stop_publication().or_else(|| primary.original_stop_publication()),
            _ => None,
        }
    }

    /// Visit every directly returned publication without flattening calls or
    /// selecting one stop as the winner. The original node owner still has
    /// to authenticate each publication and the original request sink.
    pub fn visit_original_stop_publications(
        &self,
        visitor: &mut dyn FnMut(&std::sync::Arc<crate::tool_host::OriginalStopPublication>),
    ) {
        match self.value() {
            PendingAgentBatchValue::OriginalStopPublication(publication) => visitor(publication),
            PendingAgentBatchValue::OriginalTerminalBoundary(boundary) => visitor(&boundary.publication),
            PendingAgentBatchValue::WithCause { primary, cause } => {
                primary.visit_original_stop_publications(visitor);
                cause.visit_original_stop_publications(visitor);
            }
            PendingAgentBatchValue::Calls(calls) => {
                for (_, pending) in calls { pending.visit_original_stop_publications(visitor); }
            }
            _ => {}
        }
    }

    pub(crate) fn with_unresolved_marker(self) -> Self {
        if self.original_terminal_boundary_ref().is_some() {
            Self::with_cause(self, Some(Self::new(PendingAgentBatchValue::UnresolvedContext)))
        } else { self }
    }
    pub fn with_cause(primary: PendingAgentBatch, cause: Option<PendingAgentBatch>) -> Self {
        match cause {
            Some(cause) => Self::new(PendingAgentBatchValue::WithCause { primary, cause }),
            None => primary,
        }
    }
}
impl std::fmt::Debug for PendingAgentBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { f.write_str("PendingAgentBatch(..)") }
}
// This equality only preserves ToolError value semantics for clones of the
// same in-process carrier; it is never used to authenticate a fact.
impl PartialEq for PendingAgentBatch {
    fn eq(&self, other: &Self) -> bool { std::sync::Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for PendingAgentBatch {}

/// Original in-process driver failure; identity equality never authenticates it.
#[derive(Debug, Clone)]
struct OriginalChildDriveError(std::sync::Arc<crate::execution_core::graph::ExecutionRunnerError>);
impl PartialEq for OriginalChildDriveError {
    fn eq(&self, other: &Self) -> bool { std::sync::Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalChildDriveError {}

#[derive(Debug, Clone)]
struct OriginalChildCloseError(std::sync::Arc<crate::execution_core::graph::NodeExecutorError>);
impl PartialEq for OriginalChildCloseError {
    fn eq(&self, other: &Self) -> bool { std::sync::Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalChildCloseError {}

#[derive(Debug, Clone)]
struct OriginalChildReceiptError(std::sync::Arc<crate::execution_core::graph::ExecutionCommitError>);
impl PartialEq for OriginalChildReceiptError {
    fn eq(&self, other: &Self) -> bool { std::sync::Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalChildReceiptError {}

#[derive(Debug, Clone)]
struct OriginalChildCancellationError(std::sync::Arc<crate::RuntimeServicesError>);
impl PartialEq for OriginalChildCancellationError {
    fn eq(&self, other: &Self) -> bool { std::sync::Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalChildCancellationError {}

#[derive(Debug, Clone)]
pub(crate) struct OriginalPacketScopeDenialHandle(std::sync::Arc<crate::agent_in_process_worker::OriginalPacketScopeDenial>);
impl PartialEq for OriginalPacketScopeDenialHandle {
    fn eq(&self, other: &Self) -> bool { std::sync::Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalPacketScopeDenialHandle {}

#[derive(Debug, Clone)]
pub(crate) struct OriginalPacketScopeReceiptHandle(std::sync::Arc<crate::execution_core::graph::CommittedCallRefusal>);
impl PartialEq for OriginalPacketScopeReceiptHandle {
    fn eq(&self, other: &Self) -> bool { std::sync::Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalPacketScopeReceiptHandle {}

#[derive(Debug, Clone)]
pub(crate) struct OriginalPacketScopeRequestHandle(std::sync::Arc<crate::RuntimeToolExecutionRequest>);
impl PartialEq for OriginalPacketScopeRequestHandle {
    fn eq(&self, other: &Self) -> bool { std::sync::Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalPacketScopeRequestHandle {}

#[derive(Debug, Clone)]
struct OriginalBoundedAcquisitionFailureHandle(std::sync::Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure>);
impl PartialEq for OriginalBoundedAcquisitionFailureHandle {
    fn eq(&self, other: &Self) -> bool { std::sync::Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for OriginalBoundedAcquisitionFailureHandle {}

/// Error returned when a tool invocation fails locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    message: String,
    unknown: bool,
    failure: Option<harness_contract::tool::ToolExecutionFailure>,
    pre_effect_refusal: Option<crate::execution_core::graph::PreEffectRefusal>,
    original_bounded_acquisition_failure: Option<OriginalBoundedAcquisitionFailureHandle>,
    original_packet_scope_denial: Option<OriginalPacketScopeDenialHandle>,
    original_packet_scope_receipt: Option<OriginalPacketScopeReceiptHandle>,
    original_packet_scope_request: Option<OriginalPacketScopeRequestHandle>,
    pending_agent_batch: Option<PendingAgentBatch>,
    secondary_failures: Vec<(String, ToolError)>,
    original_child_drive: Option<OriginalChildDriveError>,
    original_child_close: Option<OriginalChildCloseError>,
    original_child_receipt: Option<OriginalChildReceiptError>,
    original_child_cancel: Option<OriginalChildCancellationError>,
}

impl ToolError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            unknown: false,
            failure: None,
            pre_effect_refusal: None,
            original_bounded_acquisition_failure: None,
            original_packet_scope_denial: None,
            original_packet_scope_receipt: None,
            original_packet_scope_request: None,
            pending_agent_batch: None,
            secondary_failures: Vec::new(),
            original_child_drive: None,
            original_child_close: None,
            original_child_receipt: None,
            original_child_cancel: None,
        }
    }

    #[must_use]
    pub fn from_failure(failure: harness_contract::tool::ToolExecutionFailure) -> Self {
        Self {
            message: failure.message.clone(),
            unknown: failure.class == harness_contract::tool::ToolExecutionFailureClass::EffectUncertain,
            failure: Some(failure),
            pre_effect_refusal: None,
            original_bounded_acquisition_failure: None,
            original_packet_scope_denial: None,
            original_packet_scope_receipt: None,
            original_packet_scope_request: None,
            pending_agent_batch: None,
            secondary_failures: Vec::new(),
            original_child_drive: None,
            original_child_close: None,
            original_child_receipt: None,
            original_child_cancel: None,
        }
    }

    pub(crate) fn from_original_child_drive(
        error: crate::execution_core::graph::ExecutionRunnerError,
    ) -> Self {
        let mut original = Self::new(error.to_string());
        original.original_child_drive = Some(OriginalChildDriveError(std::sync::Arc::new(error)));
        original
    }

    pub(crate) fn from_original_child_close(
        error: crate::execution_core::graph::NodeExecutorError,
    ) -> Self {
        let mut original = Self::new(error.to_string());
        original.original_child_close = Some(OriginalChildCloseError(std::sync::Arc::new(error)));
        original
    }

    pub(crate) fn from_original_child_receipt(
        error: crate::execution_core::graph::ExecutionCommitError,
    ) -> Self {
        let mut original = Self::new(error.to_string());
        original.original_child_receipt = Some(OriginalChildReceiptError(std::sync::Arc::new(error)));
        original
    }

    pub(crate) fn from_original_child_cancel(error: crate::RuntimeServicesError) -> Self {
        let mut original = Self::new(error.to_string());
        original.original_child_cancel = Some(OriginalChildCancellationError(std::sync::Arc::new(error)));
        original
    }

    /// The original effect may have committed and must remain unresolved.
    #[must_use]
    pub fn unknown(message: impl Into<String>) -> Self {
        Self { message: message.into(), unknown: true, failure: None, pre_effect_refusal: None, original_bounded_acquisition_failure: None, original_packet_scope_denial: None, original_packet_scope_receipt: None, original_packet_scope_request: None, pending_agent_batch: None, secondary_failures: Vec::new(), original_child_drive: None, original_child_close: None, original_child_receipt: None, original_child_cancel: None }
    }

    /// Carry the original producer boundary; Runtime still authenticates and closes the outer effect.
    pub fn original_terminal_batch(pending: PendingAgentBatch) -> Self {
        if !matches!(pending.value(), PendingAgentBatchValue::OriginalTerminalBoundary(_)) {
            return Self::unknown_with_agent_batch("original terminal boundary unavailable", pending);
        }
        // The member decision is preserved by the typed carrier; the outer effect
        // remains unresolved until its original Runtime owner authenticates it.
        let mut error = Self::unknown("original member terminal awaits original batch closure");
        error.pending_agent_batch = Some(pending);
        error
    }

    /// Preserve the original owner result without interpreting it as delivery.
    pub fn unknown_with_agent_batch(message: impl Into<String>, pending: PendingAgentBatch) -> Self {
        let mut error = Self::unknown(message);
        error.pending_agent_batch = Some(pending.with_unresolved_marker());
        error
    }

    pub fn unknown_with_optional_agent_batch(message: impl Into<String>, pending: Option<PendingAgentBatch>) -> Self {
        let mut error = Self::unknown(message);
        error.pending_agent_batch = pending.map(PendingAgentBatch::with_unresolved_marker);
        error
    }

    /// Preserve the original typed failure while recording that its physical
    /// effect or cleanup remains unresolved. The original refusal remains
    /// evidence, but cannot be consumed as a settled no-dispatch decision.
    pub(crate) fn with_unresolved_context(
        mut self, context: impl AsRef<str>, pending: Option<PendingAgentBatch>,
    ) -> Self {
        let context = context.as_ref();
        self.message = format!("{}; {}", self.message, context);
        self.unknown = true;
        self.pending_agent_batch = match (self.pending_agent_batch.take(), pending) {
            (Some(original), Some(extra)) if original != extra =>
                Some(PendingAgentBatch::with_cause(original, Some(extra))),
            (Some(original), _) => Some(original),
            (None, extra) => extra,
        };
        if let Some(original) = self.pending_agent_batch.take() {
            self.pending_agent_batch = Some(original.with_unresolved_marker());
        }
        self
    }

    pub(crate) fn with_unresolved_cause(
        self, phase: &str, cause: ToolError, pending: Option<PendingAgentBatch>,
    ) -> Self {
        let context = format!("child {phase} failed: {cause}");
        let pending = match (pending, cause.pending_agent_batch().cloned()) {
            (Some(primary), Some(extra)) if primary != extra => Some(PendingAgentBatch::with_cause(primary, Some(extra))),
            (Some(primary), _) => Some(primary),
            (None, extra) => extra,
        };
        let mut original = self.with_unresolved_context(context, pending);
        original.secondary_failures.push((phase.to_string(), cause));
        original
    }

    pub fn pending_agent_batch(&self) -> Option<&PendingAgentBatch> {
        self.pending_agent_batch.as_ref()
    }

    #[must_use]
    pub fn is_unknown(&self) -> bool { self.unknown }

    #[must_use]
    pub fn failure(&self) -> Option<&harness_contract::tool::ToolExecutionFailure> {
        self.failure.as_ref()
    }

    pub(crate) fn with_original_bounded_acquisition_failure(mut self, fact: std::sync::Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure>) -> Self {
        self.original_bounded_acquisition_failure = Some(OriginalBoundedAcquisitionFailureHandle(fact));
        self
    }
    pub(crate) fn original_bounded_acquisition_failure(&self) -> Option<&std::sync::Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure>> {
        self.original_bounded_acquisition_failure.as_ref().map(|fact| &fact.0)
    }
    pub(crate) fn original_bounded_acquisition_pending(&self, ticket: &crate::execution_core::NodeExecutionTicket,
        receipt: Option<crate::execution_core::graph::CommittedCallRefusal>) -> Option<PendingAgentBatch> {
        let fact = std::sync::Arc::clone(self.original_bounded_acquisition_failure()?);
        Some(PendingAgentBatch::new(PendingAgentBatchValue::OriginalBoundedAcquisition(std::sync::Arc::new(
            OriginalBoundedAcquisitionPending { ticket: ticket.clone(), fact, receipt: receipt.map(std::sync::Arc::new) },
        ))))
    }

    pub(crate) fn with_original_packet_scope_denial(mut self, fact: std::sync::Arc<crate::agent_in_process_worker::OriginalPacketScopeDenial>) -> Self {
        self.original_packet_scope_denial = Some(OriginalPacketScopeDenialHandle(fact));
        self
    }
    pub(crate) fn original_packet_scope_denial(&self) -> Option<&std::sync::Arc<crate::agent_in_process_worker::OriginalPacketScopeDenial>> {
        self.original_packet_scope_denial.as_ref().map(|fact| &fact.0)
    }

    pub(crate) fn with_original_packet_scope_receipt(mut self, receipt: crate::execution_core::graph::CommittedCallRefusal) -> Self {
        self.original_packet_scope_receipt = Some(OriginalPacketScopeReceiptHandle(std::sync::Arc::new(receipt)));
        self
    }

    pub(crate) fn with_original_packet_scope_request(mut self, request: std::sync::Arc<crate::RuntimeToolExecutionRequest>) -> Self {
        self.original_packet_scope_request = Some(OriginalPacketScopeRequestHandle(request));
        self
    }

    pub(crate) fn original_packet_scope_pending(&self, ticket: &crate::execution_core::NodeExecutionTicket) -> Option<PendingAgentBatch> {
        let fact = self.original_packet_scope_denial.as_ref()?.0.clone();
        let request = std::sync::Arc::clone(&self.original_packet_scope_request.as_ref()?.0);
        let receipt = self.original_packet_scope_receipt.as_ref().map(|receipt| std::sync::Arc::clone(&receipt.0));
        Some(PendingAgentBatch::new(PendingAgentBatchValue::OriginalPacketScope(std::sync::Arc::new(
            OriginalPacketScopePending { ticket: ticket.clone(), fact, request, receipt },
        ))))
    }

    pub(crate) fn from_pre_effect_refusal(
        refusal: crate::execution_core::graph::PreEffectRefusal,
        message: String,
    ) -> Self {
        let mut error = Self::new(message);
        error.pre_effect_refusal = Some(refusal);
        error
    }

    pub(crate) fn pre_effect_refusal_for(
        &self,
        call_id: &str,
        tool_name: &str,
        input: &str,
        binding: Option<&crate::step_snapshot::StepCallBinding>,
    ) -> Option<&str> {
        if self.unknown { return None; }
        self.pre_effect_refusal
            .as_ref()?
            .reason_for(call_id, tool_name, input, binding?)
    }

    #[must_use]
    pub fn model_text(&self) -> String {
        self.failure.as_ref().map_or_else(
            || self.message.clone(),
            |failure| {
                serde_json::json!({
                    "status": "failed",
                    "error": failure,
                    "recovery": if failure.class == harness_contract::tool::ToolExecutionFailureClass::InputContract {
                        "repair_arguments_once"
                    } else {
                        "replan"
                    }
                })
                .to_string()
            },
        )
    }
}

impl Display for ToolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.unknown && self.failure.is_some() {
            write!(f, "{}; {}", self.model_text(), self.message)
        } else {
            write!(f, "{}", self.model_text())
        }
    }
}

impl std::error::Error for ToolError {}

#[cfg(test)]
mod original_child_error_tests {
    use super::*;

    #[test]
    fn uncertain_cleanup_preserves_original_typed_failure_and_pending() {
        let original = harness_contract::tool::ToolExecutionFailure::input_contract(
            "task_claim", "original failure", None, Vec::new(),
        );
        let pending = PendingAgentBatch::new(PendingAgentBatchValue::RecordedSink(
            crate::ToolProgressSink::default(),
        ));
        let error = ToolError::from_failure(original.clone())
            .with_unresolved_context("close failed", Some(pending.clone()));
        assert!(error.is_unknown());
        assert_eq!(error.failure(), Some(&original));
        assert_eq!(error.pending_agent_batch(), Some(&pending));
        assert!(error.to_string().contains("original failure"));
        assert!(error.to_string().contains("close failed"));
        let cause = ToolError::from_failure(harness_contract::tool::ToolExecutionFailure::input_contract(
            "close", "secondary failure", None, Vec::new(),
        ));
        let combined = error.with_unresolved_cause("close", cause.clone(), None);
        assert_eq!(combined.failure(), Some(&original));
        assert_eq!(combined.secondary_failures[0], ("close".to_string(), cause));
        let original_drive = ToolError::from_original_child_drive(
            crate::execution_core::graph::ExecutionRunnerError::Driver("original".into()),
        );
        let combined = original_drive.clone().with_unresolved_cause("close", ToolError::new("close failed"), None);
        assert_eq!(combined.original_child_drive, original_drive.original_child_drive);
        let close = ToolError::from_original_child_close(
            crate::execution_core::graph::NodeExecutorError::Cancel { node_id: "node".into(), reason: "pending".into() },
        );
        let receipt = ToolError::from_original_child_receipt(
            crate::execution_core::graph::ExecutionCommitError::InvalidCommand("unknown".into()),
        );
        let combined = combined.with_unresolved_cause("close", close.clone(), None)
            .with_unresolved_cause("receipt", receipt.clone(), None);
        assert_eq!(combined.secondary_failures[1].1.original_child_close, close.original_child_close);
        assert_eq!(combined.secondary_failures[2].1.original_child_receipt, receipt.original_child_receipt);
    }
}

/// Unsettled original batch calls, retained in invocation order.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ToolBatchUnresolved {
    Unknown { call_id: String, detail: String, terminal_detail: String, commit_error: Option<String> },
    MissingReceipt { call_id: String, terminal_detail: String, commit_error: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartialToolBatch {
    pub member_batches: Vec<(String, PendingAgentBatch)>,
    pub messages: Vec<ConversationMessage>,
    pub unresolved: Vec<ToolBatchUnresolved>,
}

/// Error returned when a conversation turn cannot be completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    message: String,
    context_capacity_failure: Option<crate::context_ledger::ContextCapacityFailure>,
    preparation_failure: Option<crate::ProviderPreparationFailure>,
    provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope,
    provider_account_key: Option<String>,
    provider_context_window_limit: Option<u32>,
    provider_tool_protocol_failure: bool,
    tool_exposure_miss: bool,
    provider_resource_result: crate::execution_core::graph::ResourceResultClass,
    provider_retry_after: Option<Duration>,
    provider_retryable: bool,
    provider_usage: Option<TokenUsage>,
    effect_receipts: Vec<EarlyToolExecutionReceipt>,
    early_tool_failures: Vec<EarlyToolFailure>,
    /// Runtime already answered this exact malformed frame in-step and the
    /// model re-sent it unchanged.
    repeated_tool_frame_refusal: bool,
    authenticated_provider_cancellation: Option<Box<crate::provider_runtime_client::AuthenticatedProviderCancellation>>,
    authenticated_graph_cancellation: bool,
    tool_effect_unknown: Option<ToolError>,
    original_packet_scope_error: Option<ToolError>,
    original_bounded_acquisition_error: Option<ToolError>,
    partial_tool_batch: Option<PartialToolBatch>,
}

impl RuntimeError {
    /// Preserve the original typed failure when turn cleanup also fails.
    pub(crate) fn with_turn_cleanup_failure(mut self, reason: &str) -> Self {
        self.message = format!("{}; turn cleanup failed: {reason}", self.message);
        self
    }

    pub(crate) fn from_original_bounded_acquisition_error(error: ToolError) -> Self {
        let mut result = Self::new(error.to_string());
        result.original_bounded_acquisition_error = Some(error);
        result
    }
    pub(crate) fn original_bounded_acquisition_error(&self) -> Option<&ToolError> {
        self.original_bounded_acquisition_error.as_ref()
    }

    pub(crate) fn from_original_packet_scope_error(error: ToolError) -> Self {
        let mut result = Self::new(error.to_string());
        result.original_packet_scope_error = Some(error);
        result
    }
    pub(crate) fn original_packet_scope_error(&self) -> Option<&ToolError> { self.original_packet_scope_error.as_ref() }
    pub(crate) fn from_tool_effect_unknown(error: ToolError) -> Self {
        let mut runtime_error = Self::new(error.to_string());
        runtime_error.tool_effect_unknown = Some(error);
        runtime_error
    }
    pub(crate) fn with_partial_tool_batch(mut self, partial: PartialToolBatch) -> Self {
        self.partial_tool_batch = Some(partial);
        self
    }
    pub(crate) fn partial_tool_batch(&self) -> Option<&PartialToolBatch> {
        self.partial_tool_batch.as_ref()
    }
    pub(crate) fn tool_effect_unknown(&self) -> Option<&ToolError> {
        self.tool_effect_unknown.as_ref()
    }
    #[must_use]
    pub fn preparation(
        failure: crate::ProviderPreparationFailure,
        message: impl Into<String>,
    ) -> Self {
        let mut error = Self::new(message);
        error.preparation_failure = Some(failure);
        error
    }
    pub const fn preparation_failure(&self) -> Option<crate::ProviderPreparationFailure> {
        self.preparation_failure
    }

    pub fn context_capacity(
        failure: crate::context_ledger::ContextCapacityFailure,
        message: impl Into<String>,
    ) -> Self {
        let mut error = Self::new(message);
        error.context_capacity_failure = Some(failure);
        error
    }

    #[must_use]
    pub const fn context_capacity_failure(
        &self,
    ) -> Option<crate::context_ledger::ContextCapacityFailure> {
        self.context_capacity_failure
    }

    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            context_capacity_failure: None,
            preparation_failure: None,
            provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope::Request,
            provider_account_key: None,
            provider_context_window_limit: None,
            provider_tool_protocol_failure: false,
            tool_exposure_miss: false,
            provider_resource_result: crate::execution_core::graph::ResourceResultClass::Failed,
            provider_retry_after: None,
            provider_retryable: false,
            provider_usage: None,
            effect_receipts: Vec::new(),
            early_tool_failures: Vec::new(),
            repeated_tool_frame_refusal: false,
            authenticated_provider_cancellation: None,
            authenticated_graph_cancellation: false,
            tool_effect_unknown: None,
            original_packet_scope_error: None,
            original_bounded_acquisition_error: None,
            partial_tool_batch: None,
        }
    }

    #[must_use]
    pub fn with_provider_context_window_limit(
        message: impl Into<String>,
        provider_context_window_limit: Option<u32>,
    ) -> Self {
        Self {
            message: message.into(),
            context_capacity_failure: None,
            preparation_failure: None,
            provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope::Request,
            provider_account_key: None,
            provider_context_window_limit,
            provider_tool_protocol_failure: false,
            tool_exposure_miss: false,
            provider_resource_result: crate::execution_core::graph::ResourceResultClass::Failed,
            provider_retry_after: None,
            provider_retryable: false,
            provider_usage: None,
            effect_receipts: Vec::new(),
            early_tool_failures: Vec::new(),
            repeated_tool_frame_refusal: false,
            authenticated_provider_cancellation: None,
            authenticated_graph_cancellation: false,
            tool_effect_unknown: None,
            original_packet_scope_error: None,
            original_bounded_acquisition_error: None,
            partial_tool_batch: None,
        }
    }

    #[must_use]
    pub fn with_provider_failure_metadata(
        message: impl Into<String>,
        provider_context_window_limit: Option<u32>,
        provider_tool_protocol_failure: bool,
        provider_resource_result: crate::execution_core::graph::ResourceResultClass,
    ) -> Self {
        Self::with_provider_failure_metadata_and_retry_after(
            message,
            provider_context_window_limit,
            provider_tool_protocol_failure,
            provider_resource_result,
            None,
            false,
        )
    }

    #[must_use]
    pub fn with_provider_failure_metadata_and_retry_after(
        message: impl Into<String>,
        provider_context_window_limit: Option<u32>,
        provider_tool_protocol_failure: bool,
        provider_resource_result: crate::execution_core::graph::ResourceResultClass,
        provider_retry_after: Option<Duration>,
        provider_retryable: bool,
    ) -> Self {
        Self::with_provider_failure_metadata_retry_after_and_scope(
            message,
            provider_context_window_limit,
            provider_tool_protocol_failure,
            provider_resource_result,
            provider_retry_after,
            provider_retryable,
            model_protocol::provider_failure::ProviderFailureScope::Request,
        )
    }

    #[must_use]
    pub fn with_provider_failure_metadata_retry_after_and_scope(
        message: impl Into<String>,
        provider_context_window_limit: Option<u32>,
        provider_tool_protocol_failure: bool,
        provider_resource_result: crate::execution_core::graph::ResourceResultClass,
        provider_retry_after: Option<Duration>,
        provider_retryable: bool,
        provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope,
    ) -> Self {
        Self {
            message: message.into(),
            context_capacity_failure: None,
            preparation_failure: None,
            provider_failure_scope,
            provider_account_key: None,
            provider_context_window_limit,
            provider_tool_protocol_failure,
            tool_exposure_miss: false,
            provider_resource_result,
            provider_retry_after,
            provider_retryable,
            provider_usage: None,
            effect_receipts: Vec::new(),
            early_tool_failures: Vec::new(),
            repeated_tool_frame_refusal: false,
            authenticated_provider_cancellation: None,
            authenticated_graph_cancellation: false,
            tool_effect_unknown: None,
            original_packet_scope_error: None,
            original_bounded_acquisition_error: None,
            partial_tool_batch: None,
        }
    }

    #[must_use]
    pub fn with_tool_exposure_miss(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            context_capacity_failure: None,
            preparation_failure: None,
            provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope::Request,
            provider_account_key: None,
            provider_context_window_limit: None,
            provider_tool_protocol_failure: true,
            tool_exposure_miss: true,
            // Exposure activation is a local Runtime continuation, not a
            // provider-capacity failure and must not reduce provider limits.
            provider_resource_result: crate::execution_core::graph::ResourceResultClass::Completed,
            provider_retry_after: None,
            provider_retryable: false,
            provider_usage: None,
            effect_receipts: Vec::new(),
            early_tool_failures: Vec::new(),
            repeated_tool_frame_refusal: false,
            authenticated_provider_cancellation: None,
            authenticated_graph_cancellation: false,
            tool_effect_unknown: None,
            original_packet_scope_error: None,
            original_bounded_acquisition_error: None,
            partial_tool_batch: None,
        }
    }

    pub(crate) fn early_tool_failure(failures: Vec<EarlyToolFailure>) -> Self {
        let description = failures.iter().map(|failure| format!(
            "call={} step={} kind={:?} detail={}",
            failure.tool_call_id, failure.identity.model_step_id, failure.kind, failure.detail
        )).collect::<Vec<_>>().join("; ");
        let mut error = Self::new(format!("early tool infrastructure outcome unknown: {description}"));
        error.early_tool_failures = failures;
        error
    }

    pub(crate) fn early_tool_failures(&self) -> &[EarlyToolFailure] {
        &self.early_tool_failures
    }

    pub(crate) fn authenticated_provider_cancellation(&self) -> Option<&crate::provider_runtime_client::AuthenticatedProviderCancellation> {
        self.authenticated_provider_cancellation.as_deref()
    }

    pub(crate) fn from_authenticated_graph_cancellation() -> Self {
        let mut error = Self::new("original authorized execution cancelled");
        error.authenticated_graph_cancellation = true;
        error
    }

    pub(crate) fn is_authenticated_graph_cancellation(&self) -> bool {
        self.authenticated_graph_cancellation
    }
    fn with_authenticated_provider_cancellation(mut self, proof: crate::provider_runtime_client::AuthenticatedProviderCancellation) -> Self {
        self.authenticated_provider_cancellation = Some(Box::new(proof));
        self
    }

    #[must_use]
    pub fn with_provider_usage(mut self, usage: impl Into<Option<TokenUsage>>) -> Self {
        self.provider_usage = usage.into();
        self
    }

    #[must_use]
    pub(crate) fn with_provider_account_key(mut self, account_key: Option<String>) -> Self {
        self.provider_account_key = account_key;
        self
    }

    #[must_use]
    pub(crate) fn with_effect_receipts(mut self, receipts: Vec<EarlyToolExecutionReceipt>) -> Self {
        self.effect_receipts = receipts;
        self
    }

    #[must_use]
    pub(crate) fn effect_receipts(&self) -> &[EarlyToolExecutionReceipt] {
        &self.effect_receipts
    }

    #[must_use]
    pub const fn provider_context_window_limit(&self) -> Option<u32> {
        self.provider_context_window_limit
    }

    #[must_use]
    pub const fn is_provider_tool_protocol_failure(&self) -> bool {
        self.provider_tool_protocol_failure
    }

    /// Mark the failure as a frame Runtime had already answered in-step.
    ///
    /// The host's protocol recovery exists for a model that has not yet been
    /// told what was wrong. A frame that comes back byte-identical after
    /// Runtime handed over the exact shape to send has been told, so spending
    /// the single governed retry on it buys one more identical frame and
    /// nothing else.
    #[must_use]
    pub const fn with_repeated_tool_frame_refusal(mut self) -> Self {
        self.repeated_tool_frame_refusal = true;
        self
    }

    #[must_use]
    pub const fn is_repeated_tool_frame_refusal(&self) -> bool {
        self.repeated_tool_frame_refusal
    }

    #[must_use]
    pub const fn is_tool_exposure_miss(&self) -> bool {
        self.tool_exposure_miss
    }

    #[must_use]
    pub const fn provider_resource_result(
        &self,
    ) -> crate::execution_core::graph::ResourceResultClass {
        self.provider_resource_result
    }

    #[must_use]
    pub const fn provider_retry_after(&self) -> Option<Duration> {
        self.provider_retry_after
    }

    #[must_use]
    pub const fn provider_retryable(&self) -> bool {
        self.provider_retryable
    }

    #[must_use]
    pub const fn provider_failure_scope(
        &self,
    ) -> model_protocol::provider_failure::ProviderFailureScope {
        self.provider_failure_scope
    }

    #[must_use]
    pub fn provider_account_key(&self) -> Option<&str> {
        self.provider_account_key.as_deref()
    }

    #[must_use]
    pub const fn provider_usage(&self) -> Option<TokenUsage> {
        self.provider_usage
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RuntimeError {}

/// Summary of one completed runtime turn, including tool results and usage.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnSummary {
    /// The terminal answer selected by the execution graph synthesizer.
    /// Callers must not infer the result from the session transcript.
    pub final_answer: String,
    /// Canonical goal outcome committed by the graph terminal. Text alone is
    /// insufficient here: a blocked execution may intentionally produce an
    /// honest explanatory message, which must not be reported as success to a
    /// parent Agent or protocol reducer.
    pub terminal_completion: harness_contract::goal::GoalCompletion,
    pub assistant_messages: Vec<ConversationMessage>,
    pub tool_results: Vec<ConversationMessage>,
    pub iterations: usize,
    pub usage: TokenUsage,
    pub model_telemetry: crate::cowd_event::RunModelTelemetry,
    pub auto_compaction: Option<AutoCompactionEvent>,
    pub ai_kernel_trace: RuntimeAiKernelTrace,
    pub context_turn_report: ContextTurnReport,
    /// Invocation-level proof that an exact ToolResult survived packing and
    /// was consumed by a valid Provider continuation in this turn.
    pub model_observations: Vec<harness_contract::context::ProviderModelObservationAttestation>,
    pub duplicate_tool_calls: u64,
    /// Canonical workspace paths targeted by write-capable tool calls during
    /// this turn, including calls rejected before execution. This is distinct
    /// from the final workspace diff: mutate-and-restore and same-bytes writes
    /// remain observable to evaluation and audit consumers.
    pub write_attempt_paths: Vec<String>,
    pub max_tool_concurrency_observed: usize,
    pub parallel_tool_batches: usize,
}

/// One provider decision. Model steps are deliberately side-effect free with
/// respect to tools: they may request work, but only a ToolBatch executor may
/// perform it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelStepIntent {
    FinalAnswer { text: String },
    ToolCalls { calls: Vec<ModelToolCall> },
    Replan { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelToolCall {
    pub binding: Option<crate::step_snapshot::StepCallBinding>,
    pub id: String,
    pub name: String,
    pub input: String,
    pub depends_on: Vec<String>,
}

#[derive(Clone)]
struct ConversationProviderResourceHandles {
    manager: Option<Arc<crate::execution_core::graph::ExecutionResourceManager>>,
    config: Arc<std::sync::RwLock<crate::ProviderResourceConfig>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelStepResult {
    /// Present only for the dedicated, nonterminal, zero-tool user-input
    /// interpretation purpose. Err retains the real step/usage but grants no adoption.
    pub(crate) input_interpretation:
        Option<Result<harness_contract::input_disposition::ModelInputDispositionBatch, String>>,
    pub intent: ModelStepIntent,
    pub assistant_message: ConversationMessage,
    /// Runtime-authored results for individual provider tool calls rejected
    /// before dispatch. These messages must commit atomically with the
    /// assistant ToolUse frame so every provider tool-call id has a truthful
    /// continuation even when valid siblings proceed to a ToolBatch.
    pub preflight_tool_results: Vec<ConversationMessage>,
    pub usage: TokenUsage,
    pub model: Option<String>,
    /// Ordered provider/model candidates actually attempted for this
    /// successful step, including failed fallbacks before the winner.
    pub models_used: Vec<String>,
    pub first_token_latency_ms: Option<u64>,
    pub active_stream_duration_ms: Option<u64>,
    pub wall_duration_ms: u64,
    /// Read-only calls completed through the governed early lane while the
    /// Provider was still streaming. The graph ToolBatch consumes these
    /// durable receipts instead of executing the effect a second time.
    pub(crate) early_tool_receipts: Vec<EarlyToolExecutionReceipt>,
    /// Calls observed at item completion but deliberately retained for the
    /// finalized ToolBatch, with a machine-auditable safety reason.
    pub(crate) early_tool_deferrals: Vec<EarlyToolDeferral>,
    pub(crate) response_completed_at_ms: u64,
    /// Whether this response was requested under the one-shot, zero-tool
    /// terminal checkpoint. Graph owners must enforce the boundary from the
    /// returned step rather than infer it from local scheduling state.
    pub text_only_response: bool,
}

impl ModelStepResult {
    /// Classify an empty completion before it can become a Goal candidate.
    /// Reasoning-only responses retain the original bounded continuation lane.
    pub(crate) fn into_host_result(self) -> Result<Self, RuntimeError> {
        let empty_final =
            matches!(&self.intent, ModelStepIntent::FinalAnswer { text } if text.trim().is_empty());
        let has_reasoning = self
            .assistant_message
            .blocks
            .iter()
            .any(|block| match block {
                ContentBlock::Thinking { thinking, .. } => !thinking.trim().is_empty(),
                ContentBlock::ReasoningSummary { text } => !text.trim().is_empty(),
                _ => false,
            });
        if self.input_interpretation.is_none()
            && empty_final
            && (self.text_only_response || !has_reasoning)
        {
            // Use the existing failed-step owner, including its Goal policy and
            // receipt-aware bounded recovery. A missing usage observation must
            // stay unknown rather than turn the aggregate's default zero into
            // a claimed physical provider measurement.
            return Err(RuntimeError::new(
                "provider completed without a visible final answer or tool calls",
            )
            .with_provider_usage(self.assistant_message.usage)
            .with_effect_receipts(self.early_tool_receipts));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreDispatchRefusalSource {
    Compiler,
    Strategy,
    LegacyOwner,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreDispatchRefusal {
    pub(crate) call_id: String,
    pub(crate) reason: String,
    pub(crate) source: PreDispatchRefusalSource,
}

impl From<(String, String)> for PreDispatchRefusal {
    fn from((call_id, reason): (String, String)) -> Self {
        Self { call_id, reason, source: PreDispatchRefusalSource::LegacyOwner }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolBatchStepResult {
    pub(crate) orchestrated_search_coverage: Option<std::collections::BTreeSet<String>>,
    pub(crate) member_batches: Vec<(String, PendingAgentBatch)>,
    pub(crate) refused_before_dispatch: Vec<PreDispatchRefusal>,
    pub messages: Vec<ConversationMessage>,
    pub failed: usize,
    pub max_concurrency_observed: usize,
    pub parallel_batches: usize,
}

/// Details about automatic session compaction applied during a turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoCompactionEvent {
    pub removed_message_count: usize,
    pub compaction_receipt: Option<CompactionReceipt>,
}

/// P1-05: Callback for generator-style turn injection after tool results.
pub struct TurnCallback {
    pub on_tool_result: Box<dyn Fn(&str, &str) -> Option<String> + Send + Sync>,
}
impl TurnCallback {
    pub fn new<F: Fn(&str, &str) -> Option<String> + Send + Sync + 'static>(f: F) -> Self {
        Self {
            on_tool_result: Box::new(f),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
struct PreparedVisionPayload {
    tool: String,
    status: String,
    image_path: String,
    media_type: String,
    prompt: String,
    image_base64: String,
    size_bytes: Option<u64>,
}

fn prepared_vision_payload(
    tool_name: &str,
    output: &str,
    is_error: bool,
) -> Option<PreparedVisionPayload> {
    if is_error || tool_name != "vision_analyze" {
        return None;
    }
    let payload = serde_json::from_str::<PreparedVisionPayload>(output).ok()?;
    if payload.tool != "vision_analyze"
        || payload.status != "prepared"
        || payload.image_base64.trim().is_empty()
        || !payload.media_type.starts_with("image/")
    {
        return None;
    }
    Some(payload)
}

fn vision_index_summary(payload: &PreparedVisionPayload) -> String {
    format!(
        "vision_analyze prepared image input: path={}, media_type={}, size_bytes={}, prompt={}",
        payload.image_path,
        payload.media_type,
        payload.size_bytes.unwrap_or_default(),
        payload.prompt
    )
}

fn vision_tool_model_receipt(payload: &PreparedVisionPayload, raw_ref: &EvidenceRef) -> String {
    format!(
        "Tool `vision_analyze` completed. Raw evidence ref: tool://{}. Image input is attached as a structured vision block for the next model call. path={}, media_type={}, size_bytes={}, prompt={}",
        raw_ref.id(),
        payload.image_path,
        payload.media_type,
        payload.size_bytes.unwrap_or_default(),
        payload.prompt
    )
}

fn vision_user_message(payload: &PreparedVisionPayload) -> ConversationMessage {
    ConversationMessage {
        role: crate::session::MessageRole::User,
        blocks: vec![
            ContentBlock::Text {
                text: format!(
                    "Analyze the attached image for this request. Original image path: {}. Prompt: {}",
                    payload.image_path, payload.prompt
                ),
            },
            ContentBlock::Image {
                media_type: payload.media_type.clone(),
                data: payload.image_base64.clone(),
                source_path: Some(payload.image_path.clone()),
            },
        ],
        usage: None,
    }
}

/// Build a structured user message that carries an image as multimodal input.
///
/// Surface adapters use this to hand already-downloaded media to runtime without
/// asking the model to first call a preparation tool. The caller is still
/// expected to pass the natural-language request as the actual turn prompt.
pub fn image_user_message_from_path(
    image_path: impl AsRef<Path>,
    media_type: impl AsRef<str>,
    prompt: impl AsRef<str>,
) -> Result<ConversationMessage, RuntimeError> {
    let image_path = image_path.as_ref();
    let media_type =
        normalize_image_media_type(image_path, media_type.as_ref()).ok_or_else(|| {
            RuntimeError::new(format!(
                "unsupported image media type `{}` for {}",
                media_type.as_ref(),
                image_path.display()
            ))
        })?;
    let image_data = std::fs::read(image_path).map_err(|error| {
        RuntimeError::new(format!(
            "failed to read image attachment {}: {error}",
            image_path.display()
        ))
    })?;
    let image_base64 = base64::engine::general_purpose::STANDARD.encode(image_data);
    Ok(ConversationMessage {
        role: crate::session::MessageRole::User,
        blocks: vec![
            ContentBlock::Text {
                text: format!(
                    "Structured image attachment for the current turn. Source path: {}. Request: {}",
                    image_path.display(),
                    prompt.as_ref()
                ),
            },
            ContentBlock::Image {
                media_type,
                data: image_base64,
                source_path: Some(image_path.display().to_string()),
            },
        ],
        usage: None,
    })
}

fn normalize_image_media_type(path: &Path, media_type: &str) -> Option<String> {
    let media_type = media_type.trim();
    if media_type.starts_with("image/") {
        return Some(media_type.to_string());
    }
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("png") => Some("image/png".to_string()),
        Some("jpg" | "jpeg") => Some("image/jpeg".to_string()),
        Some("gif") => Some("image/gif".to_string()),
        Some("webp") => Some("image/webp".to_string()),
        _ => None,
    }
}

/// Coordinates the model loop, tool execution, hooks, and session updates.
struct RecoveredTurnStrategyIdentity {
    decision_id: String,
    decision_lease: String,
    revision: u64,
    policy_version: String,
    selected_candidate: harness_contract::strategy::ExecutionCandidateKind,
    status: crate::execution_core::TurnStrategyDecisionStatus,
    resource_snapshot: harness_contract::strategy::StrategyResourceSnapshot,
    candidate_estimates: Vec<harness_contract::strategy::ExecutionCandidateEstimate>,
    collaboration_obligation: Option<harness_contract::strategy::CollaborationExecutionObligation>,
    pattern: harness_contract::core::ExecutionPattern,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionContextProjectionCacheKey {
    session_id: String,
    projection_generation: u64,
    index_revision: u64,
    memory_revision: u64,
    reality_snapshot: String,
    binding_fingerprint: String,
    query_digest: String,
    model_window: u32,
}

#[derive(Debug, Clone)]
struct SessionContextProjectionCacheEntry {
    key: SessionContextProjectionCacheKey,
    items: Vec<ContextItem>,
}

#[derive(Debug, Clone)]
struct SessionMemoryProjection {
    initialized: bool,
    history_revision: u64,
    source_count: usize,
    messages: Arc<Vec<MemMessage>>,
    converted_messages: u64,
    rebuilds: u64,
}

impl Default for SessionMemoryProjection {
    fn default() -> Self {
        Self {
            initialized: false,
            history_revision: 0,
            source_count: 0,
            messages: Arc::new(Vec::new()),
            converted_messages: 0,
            rebuilds: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionReadHead {
    pub message_count: usize,
    pub history_revision: u64,
    pub history_bytes: usize,
    pub history_tokens: u64,
    pub updated_at_ms: u64,
    pub model: Option<String>,
}

#[derive(Clone)]
struct SessionProviderWireEvidenceWriter {
    artifacts: Arc<crate::ArtifactStore>,
    session_port: Arc<dyn crate::SessionRuntimeJournalPort>,
    cache_dimensions: Arc<std::sync::Mutex<TurnCacheDimensionsObservation>>,
    native_wire_receipts: Arc<std::sync::Mutex<Vec<NativeWireReceipt>>>,
}

// Construct the wire artifact before entering its asynchronous write stage.
// Keep the envelope and serializer identical to the original writer.
fn provider_wire_evidence_payload(
    context: &crate::ProviderRequestEvidenceContext,
    evidence: &crate::ProviderWireEvidence,
) -> Result<Vec<u8>, RuntimeError> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 4,
        "session_id": context.session_id,
        "execution_id": context.execution_id,
        "turn_id": context.turn_id,
        "request_sequence": context.request_sequence,
        "request_compiler_cache_hit": context.request_compiler_cache_hit,
        "budget": context.budget,
        "provider_request": evidence,
    }))
    .map_err(|error| RuntimeError::new(error.to_string()))
}

#[cfg(test)]
pub(crate) fn r189_session_provider_writer_for_test(
    services: &crate::RuntimeServices,
) -> Arc<dyn crate::ProviderWireEvidenceWriter> {
    Arc::new(SessionProviderWireEvidenceWriter {
        artifacts: services.artifact_store().clone(),
        session_port: services
            .session_journal_port()
            .expect("original Session journal"),
        cache_dimensions: Arc::new(std::sync::Mutex::new(
            TurnCacheDimensionsObservation::default(),
        )),
        native_wire_receipts: Arc::new(std::sync::Mutex::new(Vec::new())),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProviderAttemptEpoch {
    execution_id: String,
    turn_id: String,
}

impl ProviderAttemptEpoch {
    fn new(execution_id: impl Into<String>, turn_id: impl Into<String>) -> Option<Self> {
        let execution_id = execution_id.into();
        let turn_id = turn_id.into();
        (!execution_id.trim().is_empty() && !turn_id.trim().is_empty()).then_some(Self {
            execution_id,
            turn_id,
        })
    }

    fn matches_payload(&self, payload: &serde_json::Value) -> bool {
        payload
            .get("execution_id")
            .and_then(serde_json::Value::as_str)
            == Some(self.execution_id.as_str())
            && payload.get("turn_id").and_then(serde_json::Value::as_str)
                == Some(self.turn_id.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderAttemptCacheState {
    Pending,
    Known,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProviderAttemptObservation {
    cache_state: ProviderAttemptCacheState,
    usage: Option<model_protocol::usage::TokenUsage>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RecoveredProviderUsage {
    pub(super) input_tokens: u64,
    pub(super) output_tokens: u64,
    pub(super) cache_creation_input_tokens: u64,
    pub(super) cache_read_input_tokens: u64,
}

impl ProviderAttemptObservation {
    const fn pending() -> Self {
        Self {
            cache_state: ProviderAttemptCacheState::Pending,
            usage: None,
        }
    }
}

/// Turn-local projection of the durable ProviderRequestPacked/Outcome pair.
/// Packed is the physical-attempt fact and starts unknown; only its matching
/// durable outcome may settle it. Epoch partitioning prevents a late outcome
/// from a previous turn changing the current terminal's cache truth.
#[derive(Debug, Default)]
struct TurnCacheDimensionsObservation {
    current_epoch: Option<ProviderAttemptEpoch>,
    current_recovery_authoritative: bool,
    epochs: std::collections::BTreeMap<
        ProviderAttemptEpoch,
        std::collections::BTreeMap<String, ProviderAttemptObservation>,
    >,
}

impl TurnCacheDimensionsObservation {
    #[cfg(test)]
    fn begin_epoch(
        &mut self,
        epoch: Option<ProviderAttemptEpoch>,
        recovered: std::collections::BTreeMap<String, ProviderAttemptObservation>,
    ) {
        self.begin_epoch_with_recovery_authority(epoch, recovered, true);
    }

    fn begin_epoch_with_recovery_authority(
        &mut self,
        epoch: Option<ProviderAttemptEpoch>,
        recovered: std::collections::BTreeMap<String, ProviderAttemptObservation>,
        recovery_authoritative: bool,
    ) {
        self.current_epoch = epoch.clone();
        self.current_recovery_authoritative = recovery_authoritative;
        if let Some(epoch) = epoch {
            self.epochs.insert(epoch, recovered);
        }
        self.prune_settled_old_epochs();
    }

    fn record_packed(&mut self, epoch: Option<ProviderAttemptEpoch>, request_id: String) {
        let Some(epoch) = epoch else {
            return;
        };
        self.epochs
            .entry(epoch)
            .or_default()
            .entry(request_id)
            .or_insert_with(ProviderAttemptObservation::pending);
    }

    fn record_outcome(
        &mut self,
        epoch: Option<ProviderAttemptEpoch>,
        request_id: String,
        usage: Option<model_protocol::usage::TokenUsage>,
        known: bool,
    ) {
        let Some(epoch) = epoch else {
            return;
        };
        if let Some(attempt) = self
            .epochs
            .get_mut(&epoch)
            .and_then(|attempts| attempts.get_mut(&request_id))
        {
            *attempt = ProviderAttemptObservation {
                cache_state: if known && usage.is_some() {
                    ProviderAttemptCacheState::Known
                } else {
                    ProviderAttemptCacheState::Unknown
                },
                usage,
            };
        }
        self.prune_settled_old_epochs();
    }

    fn apply_durable_payload(
        attempts: &mut std::collections::BTreeMap<String, ProviderAttemptObservation>,
        kind: &str,
        payload: &serde_json::Value,
    ) {
        let Some(request_id) = payload
            .get("request_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
        else {
            return;
        };
        match kind {
            "context.provider_request_packed" => {
                attempts
                    .entry(request_id)
                    .or_insert_with(ProviderAttemptObservation::pending);
            }
            "context.provider_attempt_outcome" => {
                let usage = payload
                    .get("usage")
                    .and_then(provider_attempt_usage_from_value);
                if let Some(attempt) = attempts.get_mut(&request_id) {
                    *attempt = ProviderAttemptObservation {
                        cache_state: if usage.is_some()
                            && payload
                                .get("cache_dimensions_status")
                                .and_then(serde_json::Value::as_str)
                                == Some("known")
                        {
                            ProviderAttemptCacheState::Known
                        } else {
                            ProviderAttemptCacheState::Unknown
                        },
                        usage,
                    };
                }
            }
            _ => {}
        }
    }

    fn all_known(&self) -> bool {
        self.current_recovery_authoritative
            && self
                .current_epoch
                .as_ref()
                .and_then(|epoch| self.epochs.get(epoch))
                .is_some_and(|attempts| {
                    !attempts.is_empty()
                        && attempts
                            .values()
                            .all(|attempt| attempt.cache_state == ProviderAttemptCacheState::Known)
                })
    }

    fn recovered_usage(&self) -> RecoveredProviderUsage {
        self.current_epoch
            .as_ref()
            .and_then(|epoch| self.epochs.get(epoch))
            .into_iter()
            .flat_map(std::collections::BTreeMap::values)
            .filter_map(|attempt| attempt.usage)
            .fold(RecoveredProviderUsage::default(), |mut total, usage| {
                total.input_tokens = total
                    .input_tokens
                    .saturating_add(u64::from(usage.input_tokens));
                total.output_tokens = total
                    .output_tokens
                    .saturating_add(u64::from(usage.output_tokens));
                total.cache_creation_input_tokens = total
                    .cache_creation_input_tokens
                    .saturating_add(u64::from(usage.cache_creation_input_tokens));
                total.cache_read_input_tokens = total
                    .cache_read_input_tokens
                    .saturating_add(u64::from(usage.cache_read_input_tokens));
                total
            })
    }

    fn current_attempt_count(&self) -> usize {
        self.current_epoch
            .as_ref()
            .and_then(|epoch| self.epochs.get(epoch))
            .map_or(0, std::collections::BTreeMap::len)
    }

    fn prune_settled_old_epochs(&mut self) {
        let current_epoch = self.current_epoch.as_ref();
        self.epochs.retain(|epoch, attempts| {
            Some(epoch) == current_epoch
                || attempts
                    .values()
                    .any(|attempt| attempt.cache_state == ProviderAttemptCacheState::Pending)
        });
    }
}

fn provider_attempt_usage_from_value(
    usage: &serde_json::Value,
) -> Option<model_protocol::usage::TokenUsage> {
    let read = |field: &str| {
        usage
            .get(field)
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
    };
    Some(model_protocol::usage::TokenUsage {
        input_tokens: read("input_tokens")?,
        output_tokens: read("output_tokens")?,
        cache_creation_input_tokens: read("cache_creation_input_tokens")?,
        cache_read_input_tokens: read("cache_read_input_tokens")?,
    })
}

#[async_trait::async_trait]
impl crate::ProviderWireEvidenceWriter for SessionProviderWireEvidenceWriter {
    async fn persist_physical(
        &self,
        context: &crate::ProviderRequestEvidenceContext,
        observation: crate::provider_runtime_client::ProviderPhysicalObservation,
        source: Option<&crate::execution_core::services::AuthorizedRequestedCancellation>,
    ) -> Result<crate::provider_runtime_client::AcknowledgedProviderPhysicalObservation, RuntimeError>
    {
        use crate::provider_runtime_client::{
            AcknowledgedProviderPhysicalObservation, ProviderPhysicalObservation,
        };
        observation.validate(context)?;
        let status = match &observation {
            ProviderPhysicalObservation::ResponseOpened { .. } => "opened",
            ProviderPhysicalObservation::CancellationObserved { .. } => "cancel_observed",
        };
        let event = crate::RuntimeSessionEvent::new(
            context.session_id.clone(),
            context.request_sequence,
            crate::RuntimeSessionEventKind::ProviderPhysicalObservation,
            serde_json::to_value(&observation)
                .map_err(|error| RuntimeError::new(error.to_string()))?,
            observation.occurred_at_ms(),
        )
        .with_status(status)
        .with_ref(crate::RuntimeSessionEventRef {
            ref_type: "execution".into(),
            id: context.execution_id.clone(),
            label: None,
        })
        .with_ref(crate::RuntimeSessionEventRef {
            ref_type: "turn".into(),
            id: context.turn_id.clone(),
            label: None,
        })
        .with_ref(crate::RuntimeSessionEventRef {
            ref_type: "provider_request".into(),
            id: observation.identity().request_id.clone(),
            label: None,
        })
        .with_ref(crate::RuntimeSessionEventRef {
            ref_type: "provider_response".into(),
            id: observation.identity().response_id.clone(),
            label: None,
        });
        let written = match (&observation, source) {
            (ProviderPhysicalObservation::ResponseOpened { .. }, None) => {
                self.session_port.append_event(&event).await
            }
            (ProviderPhysicalObservation::CancellationObserved { .. }, Some(source)) => {
                let request = source
                    .claim_event_append(&event, &observation)
                    .map_err(RuntimeError::new)?;
                self.session_port
                    .append_event_if_claim_current(&request)
                    .await
            }
            _ => {
                return Err(RuntimeError::new(
                    "physical phase requires its exact authenticated source capability",
                ));
            }
        }
        .map_err(|error| RuntimeError::new(error.to_string()))?;
        AcknowledgedProviderPhysicalObservation::from_written(&observation, written.sequence)
    }
    async fn persist(
        &self,
        context: &crate::ProviderRequestEvidenceContext,
        evidence: crate::ProviderWireEvidence,
    ) -> Result<(), RuntimeError> {
        let result_seals = crate::context_evidence::wire_tool_result_seals(&evidence.wire_request);
        let epoch = ProviderAttemptEpoch::new(&context.execution_id, &context.turn_id);
        let request_id = evidence.request_context.request_id.clone();
        let payload = provider_wire_evidence_payload(context, &evidence)?;
        let visibility_scope = format!("session:{}", context.session_id);
        let artifact = self
            .artifacts
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: "application/vnd.cowd.provider-wire-request+json".to_string(),
                    visibility_scope: visibility_scope.clone(),
                    expected_bytes: Some(payload.len() as u64),
                    original_name: Some(format!(
                        "provider-wire-request-{}-{}.json",
                        context.request_sequence, evidence.request_context.request_id
                    )),
                },
                &payload,
            )
            .await
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let staging_owner = format!(
            "staging:provider-request:{}:{}",
            context.session_id, evidence.request_context.request_id
        );
        self.artifacts
            .pin(
                &artifact,
                &staging_owner,
                now_ms().saturating_add(crate::ARTIFACT_STAGING_PIN_TTL_MS),
            )
            .map_err(|error| RuntimeError::new(error.to_string()))?;

        let event = crate::RuntimeSessionEvent::new(
            context.session_id.clone(),
            context.request_sequence,
            crate::RuntimeSessionEventKind::ProviderRequestPacked,
            serde_json::json!({
                "type": "ProviderRequestPacked",
                "schema_version": 5,
                "execution_id": context.execution_id,
                "turn_id": context.turn_id,
                "request_sequence": context.request_sequence,
                "request_id": evidence.request_context.request_id,
                "model": evidence.request_context.profile.model,
                "diagnostic_send_reason": evidence.request_context.diagnostic_send_reason,
                "send_mode": if evidence.request_context.diagnostic_send_reason.is_some() { "diagnostic_unauthenticated" } else { "non_diagnostic_unverified" },
                "protocol": evidence.wire_request.protocol,
                "body_sha256": evidence.wire_request.body_sha256,
                "step_render": evidence.step_render,
                "cache_identity_sha256": evidence.prefix_observation.cache_identity_sha256,
                "prefix_predecessor_request_id": evidence.prefix_observation.predecessor_request_id,
                "model_visible_prompt_bytes": evidence.prefix_observation.prompt_bytes,
                "reusable_prefix_bytes": evidence.prefix_observation.reusable_prefix_bytes,
                "structural_reuse_ratio_bp": evidence.prefix_observation.structural_reuse_ratio_bp,
                "exact_prefix_extension": evidence.prefix_observation.exact_extension,
                "prefix_invalidation_reason": evidence.prefix_observation.invalidation_reason,
                // PLAN A1: which segment moved, and the per-segment byte
                // composition of the model-visible prompt. Observability only.
                "prefix_divergence_segment": evidence.prefix_observation.divergence_segment,
                "prompt_composition": evidence.prefix_observation.composition,
                "cache_cold_leader": evidence.prefix_observation.cold_leader,
                "cache_common_prefix_leader": evidence.prefix_observation.common_prefix_leader,
                "cache_warmup_bypassed_low_reuse": evidence.prefix_observation.warmup_bypassed_low_reuse,
                "waited_for_cache_warmup": evidence.prefix_observation.waited_for_warmup,
                "artifact": artifact.clone(),
            }),
            now_ms(),
        )
        .with_ref(crate::RuntimeSessionEventRef {
            ref_type: "artifact".to_string(),
            id: artifact.selector.clone(),
            label: Some("exact provider wire request".to_string()),
        });
        let packed_receipt = match self.session_port.append_event(&event).await {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Err(cleanup_error) = self.artifacts.unpin(&artifact, &staging_owner) {
                    tracing::warn!(
                        error = %cleanup_error,
                        artifact = %artifact.selector,
                        "provider evidence staging pin cleanup failed"
                    );
                }
                if let Err(cleanup_error) = self.artifacts.delete(&artifact, &visibility_scope) {
                    tracing::warn!(
                        error = %cleanup_error,
                        artifact = %artifact.selector,
                        "unreferenced provider evidence artifact cleanup failed"
                    );
                }
                return Err(RuntimeError::new(error.to_string()));
            }
        };
        self.cache_dimensions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_packed(epoch, request_id);

        let durable_owner = format!(
            "provider-request:{}:{}",
            context.session_id, evidence.request_context.request_id
        );
        if let Err(error) = self.artifacts.pin(
            &artifact,
            &durable_owner,
            crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
        ) {
            let _ = self.artifacts.pin(
                &artifact,
                &staging_owner,
                crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
            );
            return Err(RuntimeError::new(error.to_string()));
        }
        if let Err(error) = self.artifacts.unpin(&artifact, &staging_owner) {
            tracing::warn!(
                error = %error,
                artifact = %artifact.selector,
                "provider evidence retained an extra staging pin"
            );
        }
        if let Some(results) = result_seals {
            let mut receipts = self
                .native_wire_receipts
                .lock()
                .map_err(|_| RuntimeError::new("native wire receipt projection poisoned"))?;
            if receipts
                .iter()
                .any(|receipt| receipt.request_id == evidence.request_context.request_id)
            {
                return Err(RuntimeError::new(
                    "duplicate physical native provider request",
                ));
            }
            receipts.push(NativeWireReceipt {
                context: context.clone(),
                request_id: evidence.request_context.request_id.clone(),
                step_render: evidence.step_render.clone(),
                wire_sha256: evidence.wire_request.body_sha256.clone(),
                wire_artifact: artifact,
                packed_sequence: packed_receipt.sequence,
                outcome_sequence: None,
                results,
            });
        }
        Ok(())
    }

    async fn persist_outcome(
        &self,
        context: &crate::ProviderRequestEvidenceContext,
        outcome: crate::ProviderAttemptOutcomeEvidence,
    ) -> Result<(), RuntimeError> {
        // Validate before append (and before updating usage/cache projections).
        // This is the existing packed-request receipt set, not a new authority
        // inferred from a sequence or a model-supplied diagnostic object.
        let protocol_diagnostic = if let Some(diagnostic) = &outcome.protocol_diagnostic {
            let receipts = self
                .native_wire_receipts
                .lock()
                .map_err(|_| RuntimeError::new("native wire receipt projection poisoned"))?;
            let mut matches = receipts.iter().filter(|receipt| {
                receipt.context == *context && receipt.request_id == outcome.request_id
            });
            let original = matches.next().ok_or_else(|| {
                RuntimeError::new("Provider diagnostic has no original packed request receipt")
            })?;
            if matches.next().is_some() {
                return Err(RuntimeError::new(
                    "Provider diagnostic packed request is ambiguous",
                ));
            }
            let step = original.step_render.as_ref().ok_or_else(|| {
                RuntimeError::new("Provider diagnostic packed request has no original Step")
            })?;
            let mut metadata = diagnostic.outcome_metadata(context, &outcome, step)?;
            metadata["packed_sequence"] = serde_json::json!(original.packed_sequence);
            metadata["packed_wire_sha256"] = serde_json::json!(original.wire_sha256);
            Some(metadata)
        } else {
            None
        };
        let usage_known = outcome.usage.is_some();
        let cache_dimensions_known = usage_known && outcome.cache_dimensions_observed;
        let epoch = ProviderAttemptEpoch::new(&context.execution_id, &context.turn_id);
        let request_id = outcome.request_id.clone();
        let cache_hit_ratio_bp = outcome
            .usage
            .map(model_protocol::usage::TokenUsage::cache_hit_ratio_bp);
        let usage = outcome.usage.map(|usage| {
            serde_json::json!({
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                "cache_read_input_tokens": usage.cache_read_input_tokens,
            })
        });
        let mut payload = serde_json::json!({
            "type": "ProviderAttemptOutcome",
            "schema_version": 2,
            "execution_id": context.execution_id,
            "turn_id": context.turn_id,
            "request_sequence": context.request_sequence,
            "request_id": outcome.request_id,
            "logical_attempt": outcome.logical_attempt,
            "terminal_status": outcome.terminal_status,
            "usage_status": if usage_known { "known" } else { "unknown" },
            "cache_dimensions_status": if cache_dimensions_known { "known" } else { "unknown" },
            "usage": usage,
            "cache_hit_ratio_bp": cache_hit_ratio_bp,
        });
        // Additive optional metadata: old outcomes stay byte-shape compatible;
        // the nested diagnostic has its own schema version. No historical
        // event is rewritten, and no pre-append persisted=true flag is minted.
        if let Some(diagnostic) = protocol_diagnostic {
            payload["protocol_diagnostic"] = diagnostic;
        }
        let event = crate::RuntimeSessionEvent::new(
            context.session_id.clone(),
            context.request_sequence,
            crate::RuntimeSessionEventKind::ProviderAttemptOutcome,
            payload,
            now_ms(),
        )
        .with_status(if usage_known {
            "observed"
        } else {
            "usage_unknown"
        });
        let outcome_receipt = self
            .session_port
            .append_event(&event)
            .await
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if let Ok(mut receipts) = self.native_wire_receipts.lock() {
            if let Some(receipt) = receipts.iter_mut().find(|receipt| {
                receipt.context == *context && receipt.request_id == outcome.request_id
            }) {
                receipt.outcome_sequence = (outcome.terminal_status == "completed"
                    && outcome.logical_attempt == context.attempt)
                    .then_some(outcome_receipt.sequence);
            }
        }
        self.cache_dimensions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_outcome(epoch, request_id, outcome.usage, cache_dimensions_known);
        Ok(())
    }
}

/// Temporary view of the same Runtime, not another turn cancellation owner.
pub(crate) struct ScopedModelStop<'a, C, T> {
    runtime: &'a mut ConversationRuntime<C, T>,
}

impl<C, T> std::ops::Deref for ScopedModelStop<'_, C, T> {
    type Target = ConversationRuntime<C, T>;
    fn deref(&self) -> &Self::Target {
        self.runtime
    }
}
impl<C, T> std::ops::DerefMut for ScopedModelStop<'_, C, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.runtime
    }
}
impl<C: ApiClient, T: ToolExecutor> ConversationRuntime<C, T> {
    pub(crate) fn scoped_model_stop(
        &mut self,
        stop: CancellationToken,
    ) -> Result<ScopedModelStop<'_, C, T>, String> {
        // Step waiters stop on either the turn-level cancellation or this
        // step's stop, so the field stays a linked token. Turn-level
        // predicates never read this field any more: they query the shared
        // `TurnCancellationControl`, so a step stop firing here can no longer
        // read as a turn cancellation.
        let turn_token = self.turn_cancellation_token()?;
        self.cancellation_token = CancellationToken::linked(turn_token, stop);
        // A step stop also wakes turn-level waiters through the shared home,
        // without setting any turn-level cancellation state.
        if let Some(control) = self.turn_cancellation_control.as_ref() {
            control.notify_step_stop();
        }
        Ok(ScopedModelStop { runtime: self })
    }
}

impl<C: ApiClient, T> ConversationRuntime<C, T> {
    pub fn install_provider_cancellation_control(
        &mut self,
        control: Option<Arc<crate::execution_core::services::TurnCancellationControl>>,
    ) -> Result<(), RuntimeError> {
        // The shared home is kept on the runtime as well, so turn-level
        // predicates in this instance answer from the same state as every
        // other instance of this turn.
        self.api_client.configure_turn_cancellation(control.clone())?;
        self.turn_cancellation_control = control;
        Ok(())
    }
}
pub struct ConversationRuntime<C, T> {
    session_id: String,
    session: Arc<RwLock<Session>>, // tokio::sync::RwLock
    session_input_stream: crate::session_input::SessionInputStream,
    original_session_claim: Option<Arc<crate::execution_core::services::OriginalSessionClaim>>,
    original_policy_input: Option<crate::session_input::SessionInputStream>,
    original_policy_control_requested: Arc<OriginalTurnInputControl>,
    original_policy_control_parent_owned: bool,
    /// Inputs consumed at a provider checkpoint. The graph-owned host drains
    /// this compact receipt list and converts relevant corrections into Goal
    /// observations/revisions in the same graph commit; raw input remains in
    /// the durable Session store.
    consumed_session_inputs: std::sync::Mutex<Vec<crate::session_input::SessionInputRecord>>,
    api_client: C,
    tool_executor: Arc<T>,
    permission_policy: PermissionPolicy,
    permission_fingerprint: u64,
    system_prompt: Vec<String>,
    usage_tracker: UsageTracker,
    cache_dimensions_observation: Arc<std::sync::Mutex<TurnCacheDimensionsObservation>>,
    hook_runner: HookRunner,
    cowd_bus: Option<Arc<crate::cowd_event::CowdEventBus>>,
    turn_callback: Option<Arc<TurnCallback>>,
    profiler: crate::context_profiler::ContextProfiler,
    subsystem_budget_ratio_bp: u32,
    session_compaction_config: RuntimeSessionCompactConfig,
    semantic_checkpoint_enabled: bool,
    model_context_window: u32,
    model_context_window_source: provider::ModelContextWindowSource,
    model_context_windows: BTreeMap<String, u32>,
    provider_max_output_override: Option<u32>,
    /// Hard evaluation budget explicitly bound to this Session execution
    /// domain. Root and delegated conversations share the same Arc; unrelated
    /// Sessions never consult ambient process state.
    evaluation_provider_token_lease: Option<Arc<EvaluationProviderTokenLease>>,
    delegated_provider_budget: Option<(
        crate::execution_core::budget::ParentExecutionBudgetLedger,
        harness_contract::context::ChildExecutionBudgetReservation,
    )>,
    calibrated_model_context_windows: std::sync::Mutex<BTreeMap<String, u32>>,
    hook_abort_signal: HookAbortSignal,
    hook_progress_reporter: Arc<std::sync::Mutex<Option<Box<dyn HookProgressReporter + Send>>>>,
    session_tracer: Option<SessionTracer>,
    /// Optional cognitive memory manager – `None` when memory is disabled.
    memory_manager: Option<Arc<CognitiveContextManager>>,
    /// Immutable input cuts observed in actual Provider streams of the original
    /// admitted turn. This is a readset, never a source cache or writer authority.
    memory_source_uses:
        Arc<std::sync::Mutex<(String, Option<Vec<memory::store::MemoryEntryRevision>>)>>,
    checkpoint_workspace_id: String,
    execution_identity: Option<harness_contract::execution::ExecutionIdentity>,
    /// Runtime-owned owner for post-turn maintenance tasks.
    maintenance_supervisor:
        Option<Arc<crate::execution_core::services::RuntimeMaintenanceSupervisor>>,
    /// Human-readable memory status message. `None` when healthy; `Some(msg)` when degraded.
    memory_status: Option<String>,
    /// Runtime-owned Fact/Matrix recall boundary for this conversation.  It
    /// is populated only from a compiled Binding, never from a Surface field.
    reality_recall: Option<(
        crate::RealityRecallPort,
        harness_contract::agent::AgentBindingSnapshot,
    )>,
    /// Startup-selected Knowledge adapter. It is cloned from RuntimeServices;
    /// turns never reopen a backend or infer one from the config directory.
    knowledge_activation: Option<KnowledgeActivationRuntime>,
    /// Latest lease-filtered Fact/Matrix recall report, retained for runtime
    /// audit and projections without turning Gateway into a context assembler.
    last_reality_recall_report: std::sync::Mutex<Option<crate::RealityRecallReport>>,
    /// Context sources that failed for the current request. Rendered as an
    /// explicit prompt marker so the model can tell a degradation from absence.
    degraded_context_sources: std::sync::Mutex<Vec<crate::context_runtime::ContextSourceKind>>,
    /// Optional tool callback for real-time visualization (P0-2).
    tool_callback: Option<Arc<dyn ToolCallback>>,
    /// Optional Gateway-owned Session application port for durable Runtime
    /// evidence and context records.
    session_journal_port: Option<Arc<dyn crate::SessionRuntimeJournalPort>>,
    /// Authorized durable history reader used for current-Session context
    /// navigation. It never broadens retrieval to another Session implicitly.
    session_history_reader: Option<Arc<session::SessionHistoryReader>>,
    /// Shared process-local hot plane. Current-Session context is cold-paged
    /// from the durable reader once and then queried here without database I/O.
    hot_state: Option<Arc<crate::execution_core::hot_state::RuntimeHotStatePlane>>,
    session_context_projection_cache: std::sync::Mutex<Option<SessionContextProjectionCacheEntry>>,
    /// Incremental user/assistant text projection consumed by Memory recall.
    /// Session history remains canonical; this avoids rebuilding its full
    /// converted form on every turn.
    session_memory_projection: tokio::sync::Mutex<SessionMemoryProjection>,
    memory_context_revision: AtomicU64,
    current_context_cache_hit: AtomicBool,
    current_context_source_latency_ms: std::sync::Mutex<BTreeMap<String, u64>>,
    /// Runtime-selected Artifact plane shared by attachments and raw evidence.
    artifact_store: Option<Arc<crate::ArtifactStore>>,
    /// Durable execution lifecycle store. Session-domain events never use it.
    runtime_event_store: Option<Arc<RuntimeEventStore>>,
    /// Sole durable Outcome writer supplied by RuntimeServices.
    outcome_service: Option<Arc<crate::execution_core::OutcomeService>>,
    /// Immutable, asynchronously maintained Outcome read projection.
    outcome_projector: Option<Arc<crate::OutcomeProjector>>,
    routing_mode: crate::RoutingMode,
    runtime_config_revision: String,
    active_provider_identity: std::sync::Mutex<Option<harness_contract::outcome::ProviderIdentity>>,
    provider_selection_receipt: std::sync::Mutex<Option<crate::ProviderSelectionReceipt>>,
    /// Optional event log for time-travel debugging and session rebuild.
    event_log: Option<std::sync::Mutex<SessionEventLog>>,
    /// Runtime-local searchable index for oversized tool outputs.
    tool_output_sandbox: Option<Arc<std::sync::Mutex<memory::ToolOutputSandbox>>>,
    /// Optional SSE callback for real-time streaming events to WebUI.
    /// Receives pre-formatted JSON event strings.
    sse_callback: Option<Arc<dyn Fn(String) + Send + Sync>>,
    /// Optional memory lifecycle callback for TUI memory events.
    memory_callback: Option<Arc<dyn MemoryCallback>>,
    /// Runtime-owned approval policy, durable queue, Grant registry and waiter.
    approval_coordinator: Option<Arc<crate::ApprovalCoordinator>>,
    /// Skill capability profiles already inspected by the Skill asset layer and
    /// visible to this runtime.
    skill_profiles: Vec<SkillCapabilityProfile>,
    /// Agent-scoped Skill visibility and adapter policy.
    agent_skill_profile: AgentSkillProfile,
    /// Original delegated objective, not the generic child-turn start hint.
    /// Selection-only context: the Provider already receives its scoped brief.
    skill_task_objective: Option<String>,
    skill_instruction_source: Option<Arc<dyn crate::RuntimeSkillInstructionSource>>,
    skill_binding_pins: Vec<harness_contract::skill::SkillInstructionPin>,
    skill_binding_tool_refs: Vec<String>,
    skill_instruction_pins:
        std::sync::Mutex<BTreeMap<String, (String, harness_contract::skill::SkillInstructionPin)>>,
    /// Immutable identity supplied by Runtime for memory operations. A child
    /// Agent Run uses its Binding instance ID rather than the primary-turn
    /// placeholder, so concurrent instances do not share an ambient author.
    memory_agent_id: String,
    /// Optional reusable Definition lineage for Binding-scoped memory recall.
    memory_definition_lineage_id: Option<String>,
    /// Optional Team visibility boundary supplied by the Agent Binding.
    memory_team_id: Option<String>,
    /// Explicit Binding lease for memory recall. Both primary conversations
    /// and child Agents receive only the scopes in their compiled Binding.
    memory_read_scopes: Vec<harness_contract::agent::CognitiveReadScope>,
    /// P2-2: Current project phase (Discovery→Planning→Building→Reviewing→Shipping→Graduated).
    project_phase: String,
    /// Current model ID (used for provider fallback chain lookup).
    model: Option<String>,
    /// RuntimeServices-owned provider fallback policy. A turn snapshots this
    /// list once before dispatch, so config reloads affect subsequent turns
    /// without changing an in-flight candidate order.
    fallbacks: Arc<std::sync::RwLock<Vec<String>>>,
    /// T35: Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
    /// The shared home for turn-level cancellation state. Every instance that
    /// holds this Arc answers the same question, so turn-level predicates
    /// never depend on which runtime instance they run in.
    turn_cancellation_control: Option<std::sync::Arc<crate::execution_core::services::TurnCancellationControl>>,
    /// Latest assembled context envelope used by a real turn.
    last_context_envelope: std::sync::Mutex<Option<ContextEnvelope>>,
    /// Active context profile used to assemble the next runtime envelope.
    context_profile: std::sync::Mutex<ContextProfile>,
    /// Effective runtime control policy loaded from configuration.
    runtime_control_policy: RuntimeControlPolicy,
    /// Runtime-owned context supplied by outer orchestration layers.
    external_context_items: std::sync::Mutex<Vec<ContextItem>>,
    /// Immutable user-role prefix supplied only by a frozen delegated packet.
    /// Dynamic context remains in the regular per-turn tail.
    immutable_user_prefix: std::sync::Mutex<Vec<String>>,
    /// Shared immutable user context for the exact Team cache cohort.
    cache_cohort_user_prefix: std::sync::Mutex<Vec<String>>,
    /// One-shot instructions injected by an owner checkpoint for exactly the
    /// next provider request. They become part of that request's durable
    /// context envelope, but never mutate the user transcript or leak into
    /// unrelated later model steps.
    next_model_context_items: std::sync::Mutex<Vec<ContextItem>>,
    /// One governed checkpoint can require a conclusion from evidence already
    /// held by the turn. It affects only the next provider request.
    next_model_text_only: AtomicBool,
    /// One governed checkpoint can narrow exactly the next provider request to
    /// a named subset of already-discovered tools. This never grants a tool or
    /// widens its permission/resource lease, and normal exposure is restored
    /// after the request.
    next_model_tool_allowlist: std::sync::Mutex<Option<BTreeSet<String>>>,
    /// The narrowed tool set represents an already-admitted business
    /// obligation and one actual call is required on the next request.
    next_model_tool_required: AtomicBool,
    /// A stricter one-shot provider selection for a governed native action.
    /// It is only used after Runtime has reduced the eligible schema set to a
    /// single control-plane action; it never encodes a Team topology.
    next_model_required_tool_name: std::sync::Mutex<Option<String>>,
    /// A successful tool_search activation creates a one-request execution
    /// handoff. The following automatic provider request receives the newly
    /// activated schemas but temporarily hides tool_search so discovery cannot
    /// loop in place. Normal discovery visibility resumes afterwards.
    next_model_tool_activation_notice: std::sync::Mutex<Option<BTreeSet<String>>>,
    /// One governed checkpoint can lower the cognitive budget of exactly one
    /// provider request after deterministic evidence acquisition is complete.
    next_model_reasoning_effort: std::sync::Mutex<Option<String>>,
    /// Bounded short-term tool trace context for subsequent turns.
    tool_trace_context_items: std::sync::Mutex<Vec<ContextItem>>,
    /// Governance observations produced by tool calls in the active turn.
    turn_tool_observations: std::sync::Mutex<Vec<ToolObservation>>,
    /// Stable projections emitted by the sole governed Tool compiler during
    /// the active turn. Finalization forwards these exact plan identities to
    /// harness, policy, evidence, and growth receipts.
    turn_governed_tool_plans:
        std::sync::Mutex<Vec<harness_contract::tool::GovernedToolPlanProjection>>,
    /// Sole strategy identity for the admitted turn. Host creates it before
    /// graph compilation; every later checkpoint reads or revises this state.
    active_turn_strategy:
        std::sync::Mutex<Option<crate::execution_core::TurnStrategyDecisionState>>,
    /// Revisioned tool set visible to the next provider request.
    tool_exposure_state: std::sync::Mutex<Option<ToolExposureState>>,
    /// Turn-local Tool exposure evidence and provider-account circuit state.
    /// The circuit survives Host model-node replans and resets at the next
    /// turn epoch together with request metrics.
    turn_tool_exposure_metrics: std::sync::Mutex<TurnProviderState>,
    /// Tools coupled to the PromptOnly Skill selected for the active turn.
    /// Runtime folds these into the first provider exposure so Skill guidance
    /// and its executable capability arrive atomically.
    active_skill_tool_refs: std::sync::Mutex<BTreeSet<String>>,
    skill_exposed_tool_ids: std::sync::Mutex<BTreeSet<String>>,
    /// Provider visibility changes must be monotonically ordered. A governed
    /// text-only checkpoint temporarily withdraws every schema; the next
    /// normal model step must be able to restore the catalog rather than be
    /// rejected as an older projection by the provider client.
    tool_exposure_revision: AtomicU64,
    /// Digest of the last model-visible tool-id set. Reasons and catalog
    /// observations may change without creating a new Provider schema epoch.
    tool_exposure_digest: AtomicU64,
    request_compiler: crate::PreparedRequestCompiler,
    /// Actual Provider wire-prefix and native-cache evidence for the active turn.
    turn_stable_prefix_metrics: std::sync::Mutex<TurnStablePrefixMetrics>,
    /// Stable evidence projections emitted during the active turn.
    turn_evidence_audits: std::sync::Mutex<Vec<EvidenceAuditProjection>>,
    /// Exact receipts generated in this turn. These contain metadata and
    /// digests only; canonical bytes remain in Session/artifact storage.
    turn_generated_model_receipts: std::sync::Mutex<Vec<GeneratedModelReceipt>>,
    turn_native_tool_sources: std::sync::Mutex<BTreeMap<String, NativeToolSource>>,
    turn_original_root_calls: std::sync::Mutex<BTreeMap<String, OriginalRootCall>>,
    turn_native_root_owners:
        std::sync::Mutex<BTreeMap<String, crate::context_evidence::RootReadOwner>>,
    native_wire_receipts: Arc<std::sync::Mutex<Vec<NativeWireReceipt>>>,
    /// Generated receipts promoted only after their byte-identical ToolResult
    /// appears in a valid, committed Provider continuation.
    turn_model_observations:
        std::sync::Mutex<Vec<harness_contract::context::ProviderModelObservationAttestation>>,
    /// Per-turn component accounting and tool-result lease consumption.
    turn_context_ledger: std::sync::Mutex<crate::context_ledger::ContextLedger>,
    /// Latest context governance report emitted by a completed turn.
    last_context_turn_report: std::sync::Mutex<Option<ContextTurnReport>>,
    /// Compaction is decided before a provider request but reported with the
    /// completed turn so UI/audit consumers retain a single turn receipt.
    turn_preflight_compaction: std::sync::Mutex<Option<AutoCompactionEvent>>,
    /// Knowledge activation report prepared from the active memory packet.
    turn_knowledge_report:
        std::sync::Mutex<Option<harness_contract::knowledge::KnowledgeTurnReport>>,
    /// Sole bounded execution boundary for synchronous Tool implementations.
    tool_execution_plane: Arc<crate::ToolExecutionPlane>,
    authorization_negotiator: crate::AuthorizationNegotiator,
    /// Every root and delegated Conversation shares the Runtime Provider
    /// admission owner. AgentTask leases govern Agent slots, not Provider
    /// transport, so child conversations must not bypass this boundary.
    provider_resources: Arc<std::sync::RwLock<ConversationProviderResourceHandles>>,
    /// Runtime-owned service class propagated from the durable graph/Agent
    /// binding. Models and surfaces cannot promote this value.
    execution_service_class: crate::execution_core::graph::ExecutionServiceClass,
    /// Maximum duration for a single tool execution. `None` means no timeout.
    tool_timeout: Option<Duration>,
    /// Only the root user turn may turn an explicit team requirement into a
    /// mandatory orchestration call. Delegated AgentTask turns retain their
    /// inherited wording as context but remain leaf protocol work unless a
    /// future packet explicitly grants subdelegation.
    explicit_team_escalation: bool,
    /// Runtime-issued absolute model-step ceiling for the next and subsequent
    /// turns owned by this host. `0` means derive the normal main-turn lease.
    /// Delegated Agent workers bind this from their immutable budget packet.
    model_step_limit_override: AtomicUsize,
    /// Runtime-owned Focus policy copied from the immutable AgentTaskPacket.
    /// `0` disables the delegated novelty gate for normal root turns.
    delegated_focus_novelty_target_bp: AtomicU64,
    delegated_focus_acceptance_scopes: std::sync::Mutex<Vec<String>>,
    delegated_focus_required_output_fields: std::sync::Mutex<Vec<String>>,
    /// Present only for a Gateway-owned durable Session ingress. It fences
    /// provider/tool/terminal side effects against generation and claim loss.
    session_execution_fence: Option<crate::SessionExecutionFence>,
}

/// An assessment returned by the actual bound policy evaluation and its
/// successful event append. It cannot be deserialized or constructed by a
/// caller from an arbitrary event or a model-visible error.
pub struct BoundPolicyAssessment {
    event: crate::DurableRuntimeEvent,
}

/// The original ticket and invocation verified against its pinned ToolHost.
/// Scope assessment accepts this handle rather than a caller-authored effect.
pub(crate) struct PreparedCallEffect {
    ticket: crate::execution_core::NodeExecutionTicket,
    request: serde_json::Value,
    workspace_root: std::path::PathBuf,
}

impl PreparedCallEffect {
    pub(crate) fn matches(&self, ticket: &crate::execution_core::NodeExecutionTicket,
        request: &crate::RuntimeToolExecutionRequest) -> bool {
        &self.ticket == ticket && serde_json::to_value(request).is_ok_and(|value| value == self.request)
    }
    pub(crate) fn matches_workspace(&self, services: &crate::RuntimeServices) -> bool {
        self.workspace_root == services.workspace_root()
    }
}

impl BoundPolicyAssessment {
    pub(crate) fn event(&self) -> &crate::DurableRuntimeEvent { &self.event }
}

pub(crate) enum ToolAuthorizationDecision {
    Authorized(crate::ToolExecutionPolicyDecision),
    Gap {
        assessment: harness_contract::policy::CapabilityAssessment,
        effective: crate::EffectiveToolAuthorizationDescriptor,
        /// The event returned by the original assessment append, when it
        /// succeeded. It is not itself a per-call refusal authorization.
        assessment_event: Option<BoundPolicyAssessment>,
    },
}

struct ConversationGovernedToolContext<'a, C, T> {
    runtime: &'a ConversationRuntime<C, T>,
    pending_tool_uses: &'a [(String, String, String)],
    prompter: &'a crate::permissions::SharedPrompter,
    iterations: usize,
    plan_id: &'a str,
    plan_revision: u64,
    precompleted: &'a BTreeMap<String, (ConversationMessage, Option<String>)>,
}

impl<C, T> GovernedToolExecutionContext for ConversationGovernedToolContext<'_, C, T>
where
    C: ApiClient + Sync,
    T: ToolExecutor,
{
    type Output = (ConversationMessage, Option<String>);
    type Admission = Option<crate::ToolExecutionAdmission>;
    type Receipt = (ConversationMessage, Option<String>);

    fn local_ceiling(&self) -> usize {
        crate::governed_tool_plan::default_parallel_tool_concurrency()
    }

    fn precompleted(
        &self,
        task: &crate::GovernedToolPlanTask,
    ) -> Option<(GovernedToolTaskTerminal<Self::Output>, Self::Receipt)> {
        let output = self.precompleted.get(&task.tool_call_id)?.clone();
        let terminal = if conversation_tool_result_is_error(&output.0) {
            GovernedToolTaskTerminal::FailedOutput {
                error: conversation_tool_result_text(&output.0),
                output: output.clone(),
            }
        } else {
            GovernedToolTaskTerminal::Succeeded(output.clone())
        };
        Some((terminal, output))
    }

    fn is_cancelled(&self) -> bool {
        self.runtime.cancellation_token.is_cancelled()
    }

    fn wait_for_cancellation(&self) -> GovernedToolFuture<'_, ()> {
        Box::pin(self.runtime.cancellation_token.cancelled())
    }

    fn try_admit<'a>(
        &'a self,
        _task: &'a crate::governed_tool_plan::GovernedToolPlanTask,
    ) -> GovernedToolFuture<'a, GovernedToolAdmission<Self::Admission>> {
        Box::pin(async { GovernedToolAdmission::Granted(None) })
    }

    fn execute<'a>(
        &'a self,
        task: &'a crate::governed_tool_plan::GovernedToolPlanTask,
        admission: &'a mut Self::Admission,
    ) -> GovernedToolFuture<'a, Result<Self::Output, String>> {
        Box::pin(async move {
            let input = self
                .pending_tool_uses
                .get(task.original_call_index)
                .map(|(_, _, input)| input.as_str())
                .ok_or_else(|| {
                    format!(
                        "governed tool task `{}` references missing original call index {}",
                        task.tool_call_id, task.original_call_index
                    )
                })?;
            self.runtime
                .execute_single_tool(
                    task,
                    self.plan_id,
                    self.plan_revision,
                    input,
                    self.prompter,
                    self.iterations,
                    admission,
                )
                .await
                .map(|message| self.runtime.collect_tool_result_message(message).1)
                .map_err(|error| error.to_string())
        })
    }

    fn classify_output(&self, output: &Self::Output) -> Result<(), String> {
        if conversation_tool_result_is_error(&output.0) {
            Err(conversation_tool_result_text(&output.0))
        } else {
            Ok(())
        }
    }

    fn commit_terminal<'a>(
        &'a self,
        task: &'a crate::governed_tool_plan::GovernedToolPlanTask,
        terminal: &'a GovernedToolTaskTerminal<Self::Output>,
    ) -> GovernedToolFuture<'a, Result<Self::Receipt, String>> {
        Box::pin(async move {
            match terminal {
                GovernedToolTaskTerminal::Succeeded(output)
                | GovernedToolTaskTerminal::FailedOutput { output, .. } => Ok(output.clone()),
                _ => {
                    let message = ConversationMessage::tool_result(
                        task.tool_call_id.clone(),
                        task.tool_name.clone(),
                        conversation_tool_terminal_reason(terminal),
                        true,
                    );
                    self.runtime
                        .session
                        .write()
                        .await
                        .push_message(message.clone())
                        .map_err(|error| error.to_string())?;
                    let sequence = self
                        .runtime
                        .session_head()
                        .await
                        .message_count
                        .wrapping_sub(1);
                    self.runtime.record_message_event(&message, sequence);
                    Ok(self.runtime.collect_tool_result_message(message).1)
                }
            }
        })
    }

    fn on_task_started(&self, task: &crate::governed_tool_plan::GovernedToolPlanTask) {
        let input = self
            .pending_tool_uses
            .get(task.original_call_index)
            .map_or("", |(_, _, input)| input.as_str());
        self.runtime.emit_tool_started(
            &task.tool_call_id,
            &task.tool_name,
            input,
            &task.depends_on,
        );
    }

    fn on_task_terminal(
        &self,
        task: &crate::governed_tool_plan::GovernedToolPlanTask,
        terminal: &GovernedToolTaskTerminal<Self::Output>,
        receipt: Option<&Self::Receipt>,
    ) {
        // A failed durability barrier is not a tool terminal event.
        if receipt.is_none() { return; }
        let (summary, failed) = receipt.map_or_else(
            || (conversation_tool_terminal_reason(terminal), true),
            |(message, _)| {
                (
                    conversation_tool_result_text(message),
                    conversation_tool_result_is_error(message),
                )
            },
        );
        self.runtime.emit_tool_completed(
            &task.tool_call_id,
            &task.tool_name,
            &summary,
            Some(i32::from(failed)),
            &task.depends_on,
        );
    }
}

enum MemoryManagerComposition {
    Automatic,
    HostSelected(Option<Arc<CognitiveContextManager>>),
}

fn close_controlled_recovery_gap(
    assessment: &mut harness_contract::policy::CapabilityAssessment,
    reason: &str,
    evidence: &str,
) {
    if let Some(gap) = assessment.gap.as_mut() {
        gap.recoverable = false;
        gap.reason.push_str("; ");
        gap.reason.push_str(reason);
    }
    assessment.evidence_refs.push(evidence.to_string());
}

fn turn_strategy_status_name(
    status: crate::execution_core::TurnStrategyDecisionStatus,
) -> &'static str {
    use crate::execution_core::TurnStrategyDecisionStatus;
    match status {
        TurnStrategyDecisionStatus::Selected => "selected",
        TurnStrategyDecisionStatus::Running => "running",
        TurnStrategyDecisionStatus::Downgraded => "downgraded",
        TurnStrategyDecisionStatus::EarlyStopped => "early_stopped",
        TurnStrategyDecisionStatus::Partial => "partial",
        TurnStrategyDecisionStatus::WaitingExternalDecision => "waiting_external_decision",
        TurnStrategyDecisionStatus::Completed => "completed",
        TurnStrategyDecisionStatus::Cancelled => "cancelled",
        TurnStrategyDecisionStatus::Failed => "failed",
    }
}

fn turn_strategy_event_kind_allowed(kind: &str) -> bool {
    matches!(
        kind,
        "runtime.strategy.selected"
            | "runtime.strategy.downgraded"
            | "runtime.strategy.early_stopped"
            | "runtime.strategy.outcome"
    )
}

/// Initialise standalone Memory when no embedding composition root exists.
#[must_use]
fn initialize_automatic_memory_manager(
    feature_config: &RuntimeFeatureConfig,
    budget_plan: &RuntimeBudgetPlan,
    provider_resources: Arc<std::sync::RwLock<ConversationProviderResourceHandles>>,
) -> (Option<Arc<CognitiveContextManager>>, Option<String>) {
    let mem_cfg = build_cc_memory_config_with_budget(feature_config, budget_plan);
    let llm_summarizer = mem_cfg
        .compression
        .llm
        .is_configured()
        .then(|| {
            let registry = Arc::new(
                crate::ProviderRegistry::new_with_context_windows(
                    feature_config.providers().clone(),
                    feature_config.model_context_windows().clone(),
                )
                .map_err(|rejected| rejected.diagnostics.errors.join("; "))?,
            );
            crate::RuntimeMemorySummarizer::new(
                registry,
                Arc::new(crate::ProviderTransportPool::default()),
                Arc::new(crate::ProviderClientTemplateCache::default()),
                mem_cfg.compression.llm.model.clone(),
                2048,
            )
            .map(|summarizer| {
                summarizer.with_resource_source(move || {
                    let handles = provider_resources
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    handles
                        .manager
                        .as_ref()
                        .map(|manager| (Arc::clone(manager), Arc::clone(&handles.config)))
                })
            })
        })
        .transpose();
    let llm_summarizer = match llm_summarizer {
        Ok(summarizer) => summarizer.map(|summarizer| {
            Arc::new(summarizer) as Arc<dyn memory::compression::llm_summarizer::LlmSummarizer>
        }),
        Err(error) => {
            tracing::warn!(%error, "standalone Memory summarizer unavailable; using fallback");
            None
        }
    };
    match tokio::runtime::Handle::try_current() {
        Ok(_) => {
            // Standalone callers inside a runtime need a separate runtime to
            // avoid nested enter_runtime. Host-composed callers never enter
            // this branch because they inject their selected owner directly.
            let handle = std::thread::spawn(move || -> Result<_, String> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("failed to create memory init runtime: {error}"))?;
                rt.block_on(open_automatic_memory_manager(mem_cfg, llm_summarizer))
                    .map_err(|error| error.to_string())
            });
            match handle.join() {
                Ok(Ok(manager)) => {
                    tracing::debug!(
                        "memory: standalone CognitiveContextManager initialised with explicit per-turn identity"
                    );
                    (Some(Arc::new(manager)), None)
                }
                Ok(Err(error)) => automatic_memory_initialization_failure(error),
                Err(_) => {
                    let message = "Memory system unavailable: initialization thread panicked. \
                                   Context will NOT persist between turns."
                        .to_string();
                    tracing::error!("{message}");
                    (None, Some(message))
                }
            }
        }
        Err(_) => match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                match runtime.block_on(open_automatic_memory_manager(mem_cfg, llm_summarizer)) {
                    Ok(manager) => {
                        tracing::debug!(
                            "memory: standalone CognitiveContextManager initialised with explicit per-turn identity"
                        );
                        (Some(Arc::new(manager)), None)
                    }
                    Err(error) => automatic_memory_initialization_failure(error.to_string()),
                }
            }
            Err(error) => {
                let message = format!(
                    "Memory system unavailable: failed to create runtime: {error}. \
                     Memory features will NOT work."
                );
                tracing::error!("{message}");
                (None, Some(message))
            }
        },
    }
}

async fn open_automatic_memory_manager(
    config: memory::config::MemoryConfig,
    llm_summarizer: Option<Arc<dyn memory::compression::llm_summarizer::LlmSummarizer>>,
) -> Result<CognitiveContextManager, memory::MemoryError> {
    match llm_summarizer {
        Some(summarizer) => CognitiveContextManager::new_with_summarizer(config, summarizer).await,
        None => CognitiveContextManager::new(config).await,
    }
}

fn automatic_memory_initialization_failure(
    error: impl std::fmt::Display,
) -> (Option<Arc<CognitiveContextManager>>, Option<String>) {
    let message = format!(
        "Memory system unavailable: {error}. Context will NOT persist between turns. \
         Check your memory store paths, vector API credentials, and ~/.cowd/memory/ directory."
    );
    tracing::error!("{message}");
    (None, Some(message))
}

/// Convert a [`RuntimeFeatureConfig`] memory section into a [`CcMemoryConfig`]
/// suitable for [`CognitiveContextManager::new`].
#[doc(alias = "memory")]
#[doc(alias = "CognitiveContextManager")]
#[must_use]
pub fn build_cc_memory_config(feature_config: &RuntimeFeatureConfig) -> CcMemoryConfig {
    let model = feature_config.resolved_model();
    let model_context_window = model.as_deref().map_or(0, |model| {
        provider::model_context_window_with_overrides(
            model,
            Some(feature_config.model_context_windows()),
        )
    });
    let model_max_output = model.as_deref().map_or(0, |model| {
        provider_output_budget_hint(
            model,
            model_context_window,
            feature_config
                .provider_resources()
                .max_output_tokens_override(),
        )
    });
    let ratio_bp =
        clamp_context_budget_ratio_bp(feature_config.context_budget().subsystem_budget_ratio_bp);
    let plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
        model_context_window,
        model_max_output_tokens: model_max_output,
        subsystem_budget_ratio_bp: ratio_bp,
        profile: ContextProfile::MainTurn,
        autonomy_mode: None,
        expected_parallel_branches: 1,
        expected_verification_passes: 0,
    });
    build_cc_memory_config_with_budget(feature_config, &plan)
}

pub fn build_cc_memory_config_with_budget(
    feature_config: &RuntimeFeatureConfig,
    budget_plan: &RuntimeBudgetPlan,
) -> CcMemoryConfig {
    use memory::config::{
        BudgetConfig, CompressionConfig, DriftConfig, ExtractorConfig, StoreConfig,
    };

    let mem = feature_config.memory();
    let config_home = crate::cowd_dirs::config_home_dir();
    let blob_dir = if let Some(store_path) = mem.store_path.as_ref() {
        store_path.join("blobs")
    } else {
        let registry = storage::StorageRegistry::default_for_config_home(&config_home);
        let blob_dir = registry
            .endpoint(&storage::StorageDomainId::Blobs)
            .map(|endpoint| endpoint.as_handle().path)
            .unwrap_or_else(|_| registry.layout.blobs.clone());
        blob_dir
    };

    let mut vector_config = memory::config::VectorConfig {
        enabled: mem.vector.enabled,
        model: mem.vector.model.clone(),
        api_url: mem.vector.api_url.clone(),
        api_key: mem.vector.api_key.clone(),
        dimension: mem.vector.dimension,
        timeout_secs: mem.vector.timeout_secs,
        batch_size: mem.vector.batch_size,
        max_input_tokens: mem.vector.max_input_tokens,
    };
    if vector_config.enabled
        && !vector_config.model.trim().is_empty()
        && vector_config.api_url.trim().is_empty()
    {
        if let Some(provider) = feature_config
            .providers()
            .resolve_full(&vector_config.model)
        {
            if !matches!(
                model_protocol::provider_config::ProviderProtocol::effective_for_provider(provider),
                Ok(model_protocol::provider_config::ProviderProtocol::Anthropic) | Err(_)
            ) && !provider.base_url.trim().is_empty()
            {
                vector_config.api_url =
                    format!("{}/embeddings", provider.base_url.trim_end_matches('/'));
                if vector_config.api_key.trim().is_empty() {
                    vector_config.api_key = provider.api_key.clone();
                }
            }
        }
    }

    let mut llm_config = memory::config::LlmSummarizerConfig::default();
    let explicit_llm = &feature_config.compression().llm;
    if explicit_llm.is_configured() {
        llm_config.enabled = true;
        llm_config.model = explicit_llm.model.clone();
    } else if mem.extraction.auto_extract {
        if let Some(model) = feature_config.resolved_model() {
            llm_config.enabled = true;
            llm_config.model = model;
        }
    }

    CcMemoryConfig {
        store: StoreConfig {
            blob_dir,
            enable_vector_index: mem.store_enable_vector_index && mem.vector.enabled,
            cache_capacity: 512,
            vector: vector_config,
        },
        compression: CompressionConfig {
            micro_threshold: 50,
            session_threshold: 10,
            enable_deep_compression: feature_config.compression().deep.enabled,
            aggressiveness: 0.5,
            llm: llm_config,
        },
        budget: BudgetConfig {
            context_window: budget_plan.memory_retrieval_budget.context_window,
            reserved_system: u64::from(mem.layers.l1_max_tokens)
                + u64::from(mem.layers.l2_max_tokens),
            reserved_response: budget_plan.memory_retrieval_budget.reserved_response,
            warning_threshold: 0.70,
            critical_threshold: 0.90,
            runtime_managed: false,
            l0_reserved: 0,
            l1_working: 0,
            l2_project: 0,
            l3_deep: 0,
            l3_checkpoint: 0,
            l4_shared: 0,
        },
        layers: memory::config::LayerConfig {
            l0_enabled: mem.layers.l0_enabled,
            l1_max_tokens: mem.layers.l1_max_tokens,
            l2_max_tokens: mem.layers.l2_max_tokens,
            l3_search_limit: mem.layers.l3_search_limit,
            l4_enabled: mem.layers.l4_enabled,
        },
        extractor: ExtractorConfig {
            enabled: mem.extraction.auto_extract,
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.6,
            extractor_debounce_secs: 30,
        },
        governance: memory::config::GovernanceConfig {
            enabled: mem.governance.enabled,
            startup_delay_secs: mem.governance.startup_delay_secs,
            deep_scan_hour_local: mem.governance.deep_scan_hour_local,
            max_candidates: mem.governance.max_candidates,
            stale_threshold_bp: mem.governance.stale_threshold_bp,
            low_confidence_threshold_bp: mem.governance.low_confidence_threshold_bp,
        },
        drift: DriftConfig::default(),
        perf: memory::config::PerfBudget::default(),
        tuning: Default::default(),
        identity: memory::config::MemoryIdentityConfig {
            role: mem.identity.role.clone(),
            language: mem.identity.language.clone(),
        },
    }
}

fn extract_tool_info(msg: &ConversationMessage) -> (String, String) {
    if let Some(ContentBlock::ToolResult {
        tool_use_id,
        tool_name,
        ..
    }) = msg.blocks.first()
    {
        (tool_use_id.clone(), tool_name.clone())
    } else {
        (String::new(), String::new())
    }
}

fn conversation_tool_result_is_error(message: &ConversationMessage) -> bool {
    message
        .blocks
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolResult { is_error: true, .. }))
}

fn conversation_tool_result_text(message: &ConversationMessage) -> String {
    message
        .blocks
        .iter()
        .find_map(|block| match block {
            ContentBlock::ToolResult { output, .. } => Some(output.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "tool returned an error without a result payload".to_string())
}

fn conversation_tool_terminal_reason(
    terminal: &GovernedToolTaskTerminal<(ConversationMessage, Option<String>)>,
) -> String {
    match terminal {
        GovernedToolTaskTerminal::Succeeded(_) => "tool completed".to_string(),
        GovernedToolTaskTerminal::FailedOutput { error, .. }
        | GovernedToolTaskTerminal::Failed { error } => error.clone(),
        GovernedToolTaskTerminal::Refused { reason }
        | GovernedToolTaskTerminal::Cancelled { reason }
        | GovernedToolTaskTerminal::Panicked { reason } => reason.clone(),
        GovernedToolTaskTerminal::Blocked {
            predecessor_id,
            reason,
        } => format!("blocked by predecessor `{predecessor_id}`: {reason}"),
    }
}

fn compacted_source_messages(
    messages: &[ConversationMessage],
    start: usize,
    end_exclusive: usize,
) -> &[ConversationMessage] {
    let start = start.min(messages.len());
    let end_exclusive = end_exclusive.min(messages.len()).max(start);
    &messages[start..end_exclusive]
}

fn deterministic_checkpoint_id(
    session_id: &str,
    message_start: usize,
    message_end: usize,
    previous_summary: Option<&str>,
) -> String {
    // This identifier crosses process boundaries through durable events, so
    // it cannot use DefaultHasher (whose algorithm is not a persistence
    // contract). Length-prefix every field to avoid ambiguous concatenation.
    let mut hasher = Sha256::new();
    let message_start_bytes = message_start.to_le_bytes();
    let message_end_bytes = message_end.to_le_bytes();
    for field in [
        session_id.as_bytes(),
        message_start_bytes.as_slice(),
        message_end_bytes.as_slice(),
        previous_summary.unwrap_or_default().as_bytes(),
    ] {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field);
    }
    format!("checkpoint-{session_id}-{:x}", hasher.finalize())
}

fn conversation_messages_to_mem_messages(messages: &[ConversationMessage]) -> Vec<MemMessage> {
    messages
        .iter()
        .enumerate()
        .map(|(idx, msg)| {
            let role = match msg.role {
                crate::session::MessageRole::System => MemMessageRole::System,
                crate::session::MessageRole::User => MemMessageRole::User,
                crate::session::MessageRole::Assistant => MemMessageRole::Assistant,
                crate::session::MessageRole::Tool => MemMessageRole::Tool,
            };
            let content = msg
                .blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.clone()),
                    ContentBlock::ReasoningSummary { text } => {
                        Some(format!("[reasoning summary]\n{text}"))
                    }
                    ContentBlock::Image {
                        media_type,
                        source_path,
                        ..
                    } => Some(format!(
                        "[image media_type={} source_path={}]",
                        media_type,
                        source_path.as_deref().unwrap_or("<inline>")
                    )),
                    // Private Provider reasoning is never copied into Memory.
                    ContentBlock::Thinking { .. } => None,
                    ContentBlock::ToolUse { id, name, input } => {
                        Some(format!("[tool_use id={id} name={name}]\n{input}"))
                    }
                    ContentBlock::ToolResult {
                        tool_use_id,
                        tool_name,
                        output,
                        is_error,
                    } => Some(format!(
                        "[tool_result id={tool_use_id} name={tool_name} error={is_error}]\n{output}"
                    )),
                })
                .collect::<Vec<_>>()
                .join("\n\n");
            let (tool_use_id, tool_name) = msg.blocks.iter().fold((None, None), |acc, block| {
                if let ContentBlock::ToolResult {
                    tool_use_id,
                    tool_name,
                    ..
                } = block
                {
                    (Some(tool_use_id.clone()), Some(tool_name.clone()))
                } else {
                    acc
                }
            });
            MemMessage {
                turn_index: idx,
                role,
                content,
                tool_use_id,
                tool_name,
                pinned: false,
            }
        })
        .collect()
}

fn conversation_messages_to_context_mem_messages(
    messages: &[ConversationMessage],
    start_index: usize,
) -> Vec<MemMessage> {
    messages
        .iter()
        .enumerate()
        .map(|(offset, message)| {
            let role = match message.role {
                crate::session::MessageRole::User => MemMessageRole::User,
                crate::session::MessageRole::Assistant => MemMessageRole::Assistant,
                crate::session::MessageRole::Tool => MemMessageRole::Tool,
                crate::session::MessageRole::System => MemMessageRole::User,
            };
            let content = message
                .blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" ");
            let (tool_use_id, tool_name) = match message.role {
                crate::session::MessageRole::Tool => {
                    let tool_use_id = message.blocks.iter().find_map(|block| match block {
                        ContentBlock::ToolResult { tool_use_id, .. } => Some(tool_use_id.clone()),
                        _ => None,
                    });
                    let tool_name = message.blocks.iter().find_map(|block| match block {
                        ContentBlock::ToolResult { tool_name, .. } if !tool_name.is_empty() => {
                            Some(tool_name.clone())
                        }
                        _ => None,
                    });
                    (tool_use_id, tool_name)
                }
                _ => (None, None),
            };
            MemMessage {
                turn_index: start_index + offset,
                role,
                content,
                tool_use_id,
                tool_name,
                pinned: false,
            }
        })
        .collect()
}

fn is_append_only_projection(
    initialized: bool,
    previous_revision: u64,
    previous_count: usize,
    current_revision: u64,
    current_count: usize,
) -> bool {
    initialized
        && current_count >= previous_count
        && current_revision.wrapping_sub(previous_revision)
            == current_count.saturating_sub(previous_count) as u64
}

fn conversation_message_text(message: &ConversationMessage) -> String {
    message
        .blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn current_turn_messages<'a>(
    messages: &'a [ConversationMessage],
    user_input: &str,
) -> &'a [ConversationMessage] {
    let requested = user_input.trim();
    let turn_start = messages
        .iter()
        .rposition(|message| {
            message.role == crate::session::MessageRole::User
                && !requested.is_empty()
                && conversation_message_text(message).trim() == requested
        })
        .or_else(|| {
            messages
                .iter()
                .rposition(|message| message.role == crate::session::MessageRole::User)
        })
        .unwrap_or(0);
    &messages[turn_start..]
}

fn context_query_terms(query: &str) -> Vec<String> {
    query
        .split(|character: char| {
            character.is_whitespace()
                || matches!(
                    character,
                    ',' | '.'
                        | ';'
                        | ':'
                        | '!'
                        | '?'
                        | '，'
                        | '。'
                        | '；'
                        | '：'
                        | '！'
                        | '？'
                        | '('
                        | ')'
                        | '（'
                        | '）'
                )
        })
        .map(str::trim)
        .filter(|term| term.chars().count() >= 2)
        .map(str::to_lowercase)
        .collect()
}

fn context_text_relevance(text: &str, query_terms: &[String]) -> f32 {
    if query_terms.is_empty() {
        return 0.0;
    }
    let searchable = text.to_lowercase();
    let matches = query_terms
        .iter()
        .filter(|term| searchable.contains(term.as_str()))
        .count();
    matches as f32 / query_terms.len() as f32
}

fn session_message_context_text(content_json: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(content_json) else {
        return String::new();
    };
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|block| block.get("text").and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn revalidate_context_binding(
    current_session_id: &str,
    items: Vec<ContextItem>,
) -> (Vec<ContextItem>, Vec<ContextOmission>) {
    let expected_prefix = format!("session://{current_session_id}/");
    let mut selected = Vec::with_capacity(items.len());
    let mut omitted = Vec::new();
    for item in items {
        let session_scoped = item.source == ContextSourceKind::Conversation
            && item.source_lifecycle == crate::context_runtime::ContextSourceLifecycle::Session;
        let valid = !session_scoped
            || item
                .evidence
                .iter()
                .all(|reference| reference.starts_with(&expected_prefix));
        if valid {
            selected.push(item);
        } else {
            omitted.push(ContextOmission {
                source: item.source,
                reason: "final Binding fence rejected cross-Session candidate".to_string(),
                token_estimate: item.token_estimate,
            });
        }
    }
    (selected, omitted)
}

fn memory_project_id_for_session(session: &Session) -> Option<String> {
    let root = session.workspace_root.clone()?;
    Some(memory_project_id_for_workspace(&root))
}

/// Derive the Memory project scope used by every Runtime entry for a workspace.
///
/// Active context tools use this function as well, so passive injection and
/// model-initiated retrieval cannot drift into different project namespaces.
#[must_use]
pub fn memory_project_id_for_workspace(root: &Path) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    root.display().to_string().hash(&mut hasher);
    let hash = hasher.finish();
    let name = root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("workspace")
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{name}-{hash:016x}")
}

fn fact_extraction_trigger_for_turn(
    user_input: &str,
    profile: ContextProfile,
) -> Option<RuntimeFactExtractionTrigger> {
    if profile == ContextProfile::DeepInvestigation {
        return Some(RuntimeFactExtractionTrigger::DeepInvestigation);
    }
    let lowered = user_input.to_ascii_lowercase();
    let normalized = user_input.replace(char::is_whitespace, " ");
    let contains = |needle: &str| lowered.contains(needle) || normalized.contains(needle);
    if [
        "FACT:",
        "事实",
        "记忆",
        "规则",
        "原则",
        "约定",
        "偏好",
        "冲突",
        "矛盾",
        "更新",
        "remember",
        "rule",
        "preference",
        "conflict",
        "contradiction",
    ]
    .iter()
    .any(|needle| contains(needle))
    {
        return Some(RuntimeFactExtractionTrigger::TurnEnd);
    }
    None
}

fn count_failed_tool_results(messages: &[ConversationMessage]) -> usize {
    messages
        .iter()
        .filter(|message| {
            message
                .blocks
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolResult { is_error: true, .. }))
        })
        .count()
}

fn eval_override_selection(
    override_: &str,
    _requires_write: bool,
    _sample_source: &str,
) -> Result<
    Option<(
        harness_contract::strategy::ExecutionCandidateKind,
        harness_contract::core::ExecutionPattern,
    )>,
    RuntimeError,
> {
    let normalized = override_.trim().to_ascii_lowercase();
    let selection = match normalized.as_str() {
        "auto" => return Ok(None),
        // Direct 是评测候选身份，不是移除安全门的许可。只读复杂任务和
        // 盲审规约同样可能携带 policy gates；裁剪为 Direct 图会让请求在
        // 模型调用前物化失败。Execute 图不会强制调用工具或启用并行，
        // 因而既保留完整策略门，也保持 Direct 候选的串行执行语义。
        "direct" => (
            harness_contract::strategy::ExecutionCandidateKind::Direct,
            harness_contract::core::ExecutionPattern::Execute,
        ),
        // Parallel 同样只增加并行修饰符，不以 Explore 图替换原有安全门。
        "parallel_tools" | "parallel" => (
            harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
            harness_contract::core::ExecutionPattern::Execute,
        ),
        unknown => {
            return Err(RuntimeError::new(format!(
                "unsupported eval-only strategy override `{unknown}`"
            )));
        }
    };
    Ok(Some(selection))
}

fn apply_eval_strategy_override(
    decision: &mut crate::execution_core::RuntimeExecutionDecision,
) -> Result<(), RuntimeError> {
    if std::env::var("COWD_EVAL_HARNESS").as_deref() != Ok("1")
        || std::env::var("COWD_EVAL_CORPUS_ID").as_deref() != Ok("auto-strategy-v1")
    {
        return Ok(());
    }
    let Ok(override_) = std::env::var("COWD_EVAL_STRATEGY_OVERRIDE") else {
        return Ok(());
    };
    let Some((candidate, pattern)) = eval_override_selection(
        &override_,
        decision.strategy.understanding.requires_write,
        &decision.strategy.resource_snapshot.sample_source,
    )?
    else {
        return Ok(());
    };
    decision
        .strategy
        .retarget(
            pattern,
            format!(
                "pre-registered auto-strategy-v1 evaluation override selected {}",
                candidate.as_str()
            ),
        )
        .map_err(RuntimeError::new)?;
    decision.strategy.selected_candidate = candidate;
    if candidate == harness_contract::strategy::ExecutionCandidateKind::ParallelTools
        && !decision
            .strategy
            .modifiers
            .contains(&harness_contract::core::ExecutionModifier::Parallel)
    {
        decision
            .strategy
            .modifiers
            .push(harness_contract::core::ExecutionModifier::Parallel);
    }
    decision.compile_target = crate::execution_core::ExecutionPatternCatalog::current()
        .find(pattern)
        .map_or(
            crate::execution_core::RuntimeCompileTarget::InlineModel,
            |spec| spec.compile_target,
        );
    decision.lease.locked_pattern = pattern;
    Ok(())
}

/// Install a deterministic, marker-scoped cost fixture only for the isolated
/// browser acceptance harness.  The fixture is deliberately unavailable in a
/// normal Gateway process and does not accept data from the HTTP request: the
/// test process selects a fixed name and the submitted prompt must carry the
/// matching marker.  This lets STR-07 exercise the real admission,
/// persistence, projection and surface path without pretending that prose in
/// a prompt can alter the strategy cost model.
fn apply_e2e_strategy_fixture(input: &mut StrategyInput, prompt: &str) -> Result<(), RuntimeError> {
    if std::env::var("COWD_E2E_HARNESS").as_deref() != Ok("1") {
        return Ok(());
    }
    let Ok(fixture) = std::env::var("COWD_E2E_STRATEGY_FIXTURE") else {
        return Ok(());
    };
    apply_named_e2e_strategy_fixture(input, prompt, fixture.trim())
}

fn apply_named_e2e_strategy_fixture(
    input: &mut StrategyInput,
    prompt: &str,
    fixture: &str,
) -> Result<(), RuntimeError> {
    match fixture {
        "" => Ok(()),
        "explicit-team-negative" => {
            if !prompt.contains("[cowd-e2e:explicit-team-negative]") {
                return Ok(());
            }
            input.candidate_costs.insert(
                ExecutionCandidateKind::Team,
                StrategyCandidateCostSummary {
                    sample_count: 3,
                    average_critical_path_ms: 200_000,
                    average_total_tokens: 50_000,
                    average_coordination_cost_ms: 20_000,
                    calibration_source: "e2e:explicit-team-negative".to_string(),
                },
            );
            input.resource_snapshot.provider_concurrency_penalty_bp = 10_000;
            input.resource_snapshot.sample_source = format!(
                "{};e2e-fixture=explicit-team-negative",
                input.resource_snapshot.sample_source
            );
            input.resource_snapshot.provenance =
                harness_contract::core::MeasureProvenance::Observed;
            Ok(())
        }
        unknown => Err(RuntimeError::new(format!(
            "unsupported e2e-only strategy fixture `{unknown}`"
        ))),
    }
}

fn strategy_experience_record(trace: &RuntimeAiKernelTrace) -> StrategyExperienceRecord {
    let context_pressure = !trace.context_epoch.omitted.is_empty()
        || trace
            .context_alignment
            .as_ref()
            .map(|alignment| !alignment.aligned)
            .unwrap_or(false);
    let succeeded = trace.verification_report.can_finalize
        && trace.bench_result.passed
        && trace.regression_gate.allowed;
    StrategyExperienceRecord::from_decision(
        &trace.execution_decision.strategy,
        succeeded,
        trace.verification_blocked,
        context_pressure,
        false,
        now_ms(),
    )
}

fn strategy_experience_projection(trace: &RuntimeAiKernelTrace) -> serde_json::Value {
    let record = strategy_experience_record(trace);
    serde_json::json!({
        "domain": format!("{:?}", record.domain),
        "complexity": format!("{:?}", record.complexity),
        "risk": format!("{:?}", record.risk),
        "selected_pattern": record.selected_pattern.as_str(),
        "succeeded": record.succeeded,
        "verification_blocked": record.verification_blocked,
        "context_pressure": record.context_pressure,
        "multi_agent_positive_lift": record.multi_agent_positive_lift,
        "calibration_status": "outcome_projection_only",
        "persisted_for_routing": true,
        "store_ref": "runtime_event_store/outcome-projector",
    })
}

fn matrix_missing_evidence(trace: &RuntimeAiKernelTrace) -> Vec<String> {
    let mut missing = trace
        .verification_report
        .unsupported_required_claims
        .iter()
        .map(|claim| format!("unsupported_required_claim: {}", claim.statement))
        .collect::<Vec<_>>();
    missing.extend(
        trace
            .verification_report
            .not_run_claims
            .iter()
            .map(|claim| format!("not_run_claim: {}", claim.statement)),
    );
    if trace
        .context_alignment
        .as_ref()
        .map(|alignment| !alignment.aligned)
        .unwrap_or(false)
    {
        missing.push("context_epoch_envelope_alignment".to_string());
    }
    if !trace.context_epoch.omitted.is_empty() {
        missing.push(format!(
            "context_omitted_items:{}",
            trace.context_epoch.omitted.len()
        ));
    }
    missing
}

fn growth_maintenance_candidates(
    trace: &RuntimeAiKernelTrace,
) -> Vec<memory::MaintenanceCandidate> {
    trace
        .growth_event
        .memory_candidates
        .iter()
        .map(|candidate| {
            let now = chrono::Utc::now();
            memory::MaintenanceCandidate {
                id: candidate.id.clone(),
                kind: match candidate.kind {
                    harness_contract::growth::GrowthMemoryCandidateKind::Conflict => {
                        memory::MaintenanceCandidateKind::Conflict
                    }
                    harness_contract::growth::GrowthMemoryCandidateKind::Stale => {
                        memory::MaintenanceCandidateKind::Stale
                    }
                    harness_contract::growth::GrowthMemoryCandidateKind::AuthorityPromotion => {
                        memory::MaintenanceCandidateKind::AuthorityPromotion
                    }
                    harness_contract::growth::GrowthMemoryCandidateKind::RelationshipRefresh => {
                        memory::MaintenanceCandidateKind::RelationshipRefresh
                    }
                },
                status: memory::MaintenanceCandidateStatus::Open,
                entry_ids: Vec::new(),
                summary: candidate.summary.clone(),
                reason: format!(
                    "ai_growth:{}; confidence_bp={}",
                    candidate.reason, candidate.confidence_bp
                ),
                confidence: candidate.confidence_bp as f32 / 10_000.0,
                source: Some("ai_growth".to_string()),
                source_ref: Some(trace.growth_event.id.clone()),
                created_at: now,
                updated_at: now,
            }
        })
        .collect()
}

#[path = "static_tool_executor.rs"]
mod static_tool_executor;
pub use static_tool_executor::StaticToolExecutor;
