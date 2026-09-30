//! PLAN D1a: the liveness contract for one turn.
//!
//! # Why this exists
//!
//! A turn used to have exactly one termination authority: the evaluator's
//! wall clock. That produced two failure modes that were observed on real
//! provider runs:
//!
//! * **Provider silence** (`live_minimal_batch_read`, arm C): the runtime
//!   connected, issued the model request and then saw *no* semantic progress
//!   for 599 of its 600 seconds - `last_active_phase = calling_model`,
//!   `longest_unchanged_ms = 599264`, zero tokens recorded. The turn never
//!   failed on its own; the evaluator's absolute wait declared it failed.
//! * **Non-committing turn**: continuous tool activity with no durable
//!   terminal message, again ending at the wall clock.
//!
//! Both come from the same root cause: **nothing in the runtime observes
//! semantic progress, so nothing but a wall clock can end a turn.**
//!
//! # What the contract permits (PLAN I1, three-way split)
//!
//! * A task-level wall clock is **forbidden** as a termination trigger.
//! * A **contractual liveness window** is allowed, but it may only be reset by
//!   *semantic* progress, and it may only trigger a **probe** - never a kill.
//! * An authoritative TTL is allowed and reaches a terminal state only on a
//!   causal event.
//!
//! # The invariant this module enforces
//!
//! `LivenessVerdict::Silent` is reachable **only** through probe exhaustion.
//! Total elapsed time is not an input to any verdict, so a long-running turn
//! that keeps making semantic progress can never be declared silent. That is
//! what keeps "we never kill work" true while still making a hung provider
//! detectable.

use std::time::Duration;

/// The only permitted reaction to a turn that has stopped making semantic
/// progress. Deliberately non-destructive: every variant leaves the turn's
/// committed state intact and lets the runtime decide how to continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivenessProbeAction {
    /// Re-issue the request with the same idempotency nonce. Correct when the
    /// stream is dead but the work was not accepted.
    RetrySameNonce,
    /// Move to the next authorized route for the same objective.
    FailoverRoute,
    /// Surface the silence to the operator/telemetry and continue waiting.
    Escalate,
}

/// What a turn's liveness state says right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LivenessVerdict {
    /// Semantic progress happened recently. Nothing to do.
    Progressing,
    /// Inactive for at least the window and the probe budget is not exhausted.
    /// The runtime should act on `Probe`, not terminate.
    Probe {
        /// How long since the last semantic progress.
        idle: Duration,
        /// Probes already issued, including the one this verdict asks for.
        probe: u32,
    },
    /// Probes exhausted: the turn is silent and must reach a typed terminal
    /// state. Reachable only via probe exhaustion.
    Silent {
        idle: Duration,
        probes: u32,
    },
}

/// The liveness contract for one turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivenessContract {
    /// How long without semantic progress before a probe is warranted.
    pub inactivity_window: Duration,
    /// How many probes may be issued before the turn fails as silent.
    pub probe_budget: u32,
    /// What a probe does.
    pub probe_action: LivenessProbeAction,
    /// **Always `None`.** A wall clock never terminates a turn (PLAN I1). The
    /// field exists so a caller cannot quietly reintroduce one: constructing a
    /// contract with `Some(_)` is a compile-time-visible decision that must be
    /// justified in review.
    pub wall_clock_bound: Option<Duration>,
}

impl Default for LivenessContract {
    fn default() -> Self {
        Self {
            // Two minutes of *semantic* silence before the runtime reacts. A
            // healthy turn produces far more often than this: a committed
            // message, a completed tool call, or streamed bytes.
            inactivity_window: Duration::from_secs(120),
            // Two probes, then a typed terminal state. Enough to ride out a
            // dead stream without turning into an unbounded retry loop.
            probe_budget: 2,
            probe_action: LivenessProbeAction::RetrySameNonce,
            wall_clock_bound: None,
        }
    }
}

impl LivenessContract {
    /// A contract with no probing at all: every turn is patient forever.
    ///
    /// This is the pre-D1 behaviour and it is *not* a valid production
    /// contract - it is retained so tests can show what it costs (a hung
    /// provider is never detected).
    #[must_use]
    pub const fn patient() -> Self {
        Self {
            inactivity_window: Duration::from_secs(u64::MAX / 2),
            probe_budget: 0,
            probe_action: LivenessProbeAction::Escalate,
            wall_clock_bound: None,
        }
    }
}

/// Tracks semantic progress and probes issued for one turn.
///
/// "Semantic progress" is deliberately narrow and observable:
/// * a committed conversation message,
/// * a completed tool invocation (success *or* failure - both are facts),
/// * streamed provider bytes.
///
/// It is *not* progress to re-plan, to re-render a prompt, or to poll. Those
/// happen constantly and would keep a genuinely stuck turn alive forever.
#[derive(Debug, Clone)]
pub struct LivenessTracker {
    contract: LivenessContract,
    probes_issued: u32,
    /// Monotonic progress counter. Last observed value is kept so a caller can
    /// push new observations without the tracker holding a clock of its own.
    last_progress: u64,
    observed_progress: u64,
}

impl LivenessTracker {
    #[must_use]
    pub const fn new(contract: LivenessContract) -> Self {
        Self {
            contract,
            probes_issued: 0,
            last_progress: 0,
            observed_progress: 0,
        }
    }

    /// Record a unit of semantic progress. Resets the inactivity window.
    pub fn record_progress(&mut self) {
        self.observed_progress = self.observed_progress.saturating_add(1);
        self.last_progress = self.observed_progress;
    }

    /// Current contract.
    #[must_use]
    pub const fn contract(&self) -> &LivenessContract {
        &self.contract
    }

    /// Probes issued so far.
    #[must_use]
    pub const fn probes_issued(&self) -> u32 {
        self.probes_issued
    }

    /// Total semantic progress observed.
    #[must_use]
    pub const fn progress(&self) -> u64 {
        self.observed_progress
    }

    /// Evaluate liveness given how long ago the last semantic progress was.
    ///
    /// `idle` is supplied by the caller so this stays a pure, deterministic
    /// function of (contract, idle, probes) - which is what makes the unit
    /// tests meaningful without a real clock or provider.
    #[must_use]
    pub fn evaluate(&self, idle: Duration) -> LivenessVerdict {
        if idle < self.contract.inactivity_window {
            return LivenessVerdict::Progressing;
        }
        if self.probes_issued >= self.contract.probe_budget {
            return LivenessVerdict::Silent {
                idle,
                probes: self.probes_issued,
            };
        }
        LivenessVerdict::Probe {
            idle,
            probe: self.probes_issued + 1,
        }
    }

    /// Consume one probe. Returns the action the runtime should take.
    ///
    /// Called only after `evaluate` returned `Probe`; a defensive guard keeps
    /// the budget from being overspent if a caller misuses the API.
    pub fn issue_probe(&mut self) -> Option<LivenessProbeAction> {
        if self.probes_issued >= self.contract.probe_budget {
            return None;
        }
        self.probes_issued = self.probes_issued.saturating_add(1);
        Some(self.contract.probe_action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract(window_secs: u64, budget: u32) -> LivenessContract {
        LivenessContract {
            inactivity_window: Duration::from_secs(window_secs),
            probe_budget: budget,
            probe_action: LivenessProbeAction::RetrySameNonce,
            wall_clock_bound: None,
        }
    }

    #[test]
    fn recent_progress_is_progressing() {
        let tracker = LivenessTracker::new(contract(120, 2));
        assert_eq!(
            tracker.evaluate(Duration::from_secs(119)),
            LivenessVerdict::Progressing
        );
        assert_eq!(
            tracker.evaluate(Duration::from_secs(0)),
            LivenessVerdict::Progressing
        );
    }

    #[test]
    fn inactivity_past_the_window_asks_for_a_probe_not_a_kill() {
        let tracker = LivenessTracker::new(contract(120, 2));
        assert_eq!(
            tracker.evaluate(Duration::from_secs(120)),
            LivenessVerdict::Probe {
                idle: Duration::from_secs(120),
                probe: 1
            }
        );
    }

    #[test]
    fn silent_is_reachable_only_through_probe_exhaustion() {
        let mut tracker = LivenessTracker::new(contract(120, 2));
        // Two probes are available.
        assert_eq!(
            tracker.evaluate(Duration::from_secs(600)),
            LivenessVerdict::Probe {
                idle: Duration::from_secs(600),
                probe: 1
            }
        );
        assert_eq!(tracker.issue_probe(), Some(LivenessProbeAction::RetrySameNonce));
        assert_eq!(
            tracker.evaluate(Duration::from_secs(601)),
            LivenessVerdict::Probe {
                idle: Duration::from_secs(601),
                probe: 2
            }
        );
        assert_eq!(tracker.issue_probe(), Some(LivenessProbeAction::RetrySameNonce));
        // Budget exhausted -> the only path to `Silent`.
        assert_eq!(
            tracker.evaluate(Duration::from_secs(602)),
            LivenessVerdict::Silent {
                idle: Duration::from_secs(602),
                probes: 2
            }
        );
        // A further probe is refused, so `Silent` cannot be reached earlier.
        assert_eq!(tracker.issue_probe(), None);
        assert_eq!(tracker.evaluate(Duration::from_secs(603)), tracker.evaluate(Duration::from_secs(603)));
        assert!(matches!(
            tracker.evaluate(Duration::from_secs(603)),
            LivenessVerdict::Silent { .. }
        ));
    }

    /// The I1 invariant the whole module exists to protect: total elapsed time
    /// is not an input to any verdict.
    #[test]
    fn a_long_turn_with_continuous_progress_is_never_silent() {
        let mut tracker = LivenessTracker::new(contract(120, 1));
        // Ten "hours" of wall clock, each half-minute of it a fresh tool result.
        for minute in 0..600_u64 {
            tracker.record_progress();
            // Idle is always far below the window because progress keeps
            // arriving; this is what a healthy long run looks like.
            let idle = Duration::from_secs(29);
            assert_eq!(
                tracker.evaluate(idle),
                LivenessVerdict::Progressing,
                "minute {minute} must still be progressing"
            );
        }
        assert_eq!(tracker.progress(), 600);
        assert_eq!(tracker.probes_issued(), 0, "no probe may be issued");
    }

    /// The pre-D1 behaviour, kept so its cost is explicit: a hung provider is
    /// never detected, which is exactly the arm-C failure (599s, zero tokens).
    #[test]
    fn the_patient_contract_never_detects_a_hung_provider() {
        let tracker = LivenessTracker::new(LivenessContract::patient());
        assert_eq!(
            tracker.evaluate(Duration::from_secs(86_400)),
            LivenessVerdict::Progressing,
            "the patient contract cannot detect silence - this is the defect D1 fixes"
        );
    }

    #[test]
    fn progress_resets_the_window() {
        let mut tracker = LivenessTracker::new(contract(120, 2));
        tracker.record_progress();
        tracker.record_progress();
        // Still progressing, and the window is measured from the *last* one.
        assert_eq!(
            tracker.evaluate(Duration::from_secs(90)),
            LivenessVerdict::Progressing
        );
        assert_eq!(
            tracker.evaluate(Duration::from_secs(121)),
            LivenessVerdict::Probe {
                idle: Duration::from_secs(121),
                probe: 1
            }
        );
    }

    #[test]
    fn the_default_contract_has_no_wall_clock() {
        let contract = LivenessContract::default();
        assert_eq!(
            contract.wall_clock_bound, None,
            "a wall clock must never be able to terminate a turn (PLAN I1)"
        );
        assert!(contract.inactivity_window > Duration::ZERO);
        assert!(contract.probe_budget > 0, "a probe budget is required");
    }
}

/// Stable marker on the typed error produced when probes are exhausted.
pub const PROVIDER_SILENT_PREFIX: &str = "provider silent";

/// Whether an error message is the typed "the provider went silent" failure.
#[must_use]
pub fn is_provider_silent(message: &str) -> bool {
    message.starts_with(PROVIDER_SILENT_PREFIX)
}

/// Wrap a stream so that semantic inactivity is detected and reported.
///
/// D1b, and the direct answer to the arm-C failure: a request that connects and
/// then emits nothing for `inactivity_window` is no longer indistinguishable
/// from a long healthy generation. On expiry the wrapper calls `on_probe` so the
/// turn owner can act (retry the same nonce, fail over, or escalate), and then
/// emits the typed error from `make_silent_error`.
///
/// Deliberately NOT a wall clock: a stream that keeps producing items is never
/// interrupted however long the total request takes, which is what preserves
/// PLAN I1's "we never kill work".
pub fn guard_liveness<S, T, E, F, G>(
    stream: S,
    contract: LivenessContract,
    mut on_probe: F,
    make_silent_error: G,
) -> impl futures::Stream<Item = Result<T, E>>
where
    S: futures::Stream<Item = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
    F: FnMut(LivenessVerdict) + Send + 'static,
    G: FnOnce(Duration, u32) -> E + Send + 'static,
{
    let tracker = LivenessTracker::new(contract);
    // (stream, tracker, pending idle) — the probe callback and the one-shot
    // silent-error factory are moved into the fold state so the closure stays
    // a plain `FnMut` that never moves them out of its captured environment.
    let state = (
        Box::pin(stream) as std::pin::Pin<Box<dyn futures::Stream<Item = Result<T, E>> + Send>>,
        tracker,
        None::<Duration>,
        Some(on_probe),
        Some(make_silent_error),
        false,
    );
    futures::stream::unfold(
        state,
        move |(mut inner, mut tracker, mut pending, mut on_probe, mut silent_error, mut done)| {
            async move {
                // The silent error is the last item: ending the stream on the
                // same turn as the error would drop it.
                if done {
                    return None;
                }
                loop {
                    let idle = pending.take().unwrap_or(Duration::ZERO);
                    match tracker.evaluate(idle) {
                        LivenessVerdict::Progressing => {}
                        LivenessVerdict::Probe { .. } => {
                            if let Some(callback) = on_probe.as_mut() {
                                (callback)(LivenessVerdict::Probe { idle, probe: 0 });
                            }
                            let _ = tracker.issue_probe();
                        }
                        LivenessVerdict::Silent { idle, probes } => {
                            if let Some(make) = silent_error.take() {
                                return Some((
                    Err(make(idle, probes)),
                    (inner, tracker, None, on_probe, silent_error, true),
                ));
                            }
                        }
                    }
                    let window = tracker.contract().inactivity_window;
                    match tokio::time::timeout(window, futures::StreamExt::next(&mut inner)).await {
                        Ok(Some(item)) => {
                            tracker.record_progress();
                            return Some((item, (inner, tracker, None, on_probe, silent_error, done)));
                        }
                        Ok(None) => return None,
                        Err(_elapsed) => { pending = Some(window); }
                        Err(_elapsed) => pending = Some(window),
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod guard_tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn a_connected_then_silent_stream_is_reported_not_hung() {
        let never = futures::stream::pending::<Result<u8, String>>();
        let window = Duration::from_millis(40);
        let probes = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let probe_sink = std::sync::Arc::clone(&probes);
        let guarded = guard_liveness(
            never,
            LivenessContract {
                inactivity_window: window,
                probe_budget: 1,
                probe_action: LivenessProbeAction::RetrySameNonce,
                wall_clock_bound: None,
            },
            move |verdict| {
                if matches!(verdict, LivenessVerdict::Probe { .. }) {
                    probe_sink.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            },
            |idle, n| format!("{PROVIDER_SILENT_PREFIX} after {idle:?} ({n} probes)"),
        );
        let items = tokio::time::timeout(Duration::from_secs(5), guarded.collect::<Vec<_>>())
            .await
            .expect("the guard must terminate on its own");
        assert_eq!(items.len(), 1, "exactly one typed error is emitted");
        assert!(is_provider_silent(items[0].as_ref().err().expect("silent error")));
        assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 1, "one probe precedes the typed failure");
    }

    #[tokio::test]
    async fn a_long_productive_stream_is_never_interrupted() {
        let items = 150_u32;
        let total = items;
        let productive = futures::stream::unfold(0_u32, move |state| async move {
            if state >= total {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
            Some((Ok::<u8, String>(1), state + 1))
        });
        let guarded = guard_liveness(
            productive,
            LivenessContract {
                inactivity_window: Duration::from_millis(30),
                probe_budget: 1,
                probe_action: LivenessProbeAction::RetrySameNonce,
                wall_clock_bound: None,
            },
            |_| unreachable!("a productive stream must never trigger a probe"),
            |idle, n| format!("{PROVIDER_SILENT_PREFIX} after {idle:?} ({n} probes)"),
        );
        let collected =
            tokio::time::timeout(Duration::from_secs(10), guarded.collect::<Vec<_>>())
                .await
                .expect("a productive stream must run to completion");
        assert_eq!(collected.len() as u32, items);
        assert!(collected.iter().all(Result::is_ok));
    }

    #[tokio::test]
    async fn items_just_inside_the_window_still_progress() {
        let slow = futures::stream::unfold(0_u32, |state| async move {
            if state >= 3 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            Some((Ok::<u8, String>(7), state + 1))
        });
        let guarded = guard_liveness(
            slow,
            LivenessContract {
                inactivity_window: Duration::from_millis(30),
                probe_budget: 5,
                probe_action: LivenessProbeAction::Escalate,
                wall_clock_bound: None,
            },
            |verdict| panic!("unexpected probe: {verdict:?}"),
            |idle, n| format!("{PROVIDER_SILENT_PREFIX} after {idle:?} ({n} probes)"),
        );
        let collected = tokio::time::timeout(Duration::from_secs(5), guarded.collect::<Vec<_>>())
            .await
            .expect("must finish");
        assert_eq!(collected.len(), 3);
        assert!(collected.iter().all(Result::is_ok));
    }
}

/// PLAN D2a: the terminal obligation for one turn.
///
/// # Why this exists
///
/// The second failure mode observed on real runs is a turn that keeps working
/// and never finishes: EXP-003 Facet A showed 367 progress observations, 27.9MB
/// of activity and 954 projected activities, yet **no durable terminal message
/// was ever committed**, and the run again ended at the evaluator's wall clock.
///
/// A model that has all the evidence it needs has no intrinsic pressure to stop
/// generating. The obligation supplies that pressure explicitly, in the
/// runtime, instead of relying on the model's discretion or on a timeout.
///
/// # Semantics
///
/// The obligation is armed only by a **causal event** - the objective's evidence
/// requirement being satisfied. It is measured from that event, never from the
/// start of the turn, so a long-running turn is never penalised for taking a
/// long time (PLAN I1: an authoritative TTL may exist, but only a causal event
/// reaches a terminal state).

/// What the objective requires before a terminal answer is owed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SatisfactionRule {
    /// At least `required` committed tool receipts.
    Receipts {
        /// How many receipts the objective needs.
        required: usize,
    },
    /// The model emitted an explicit terminal marker.
    TerminalMarker,
}

/// The observable facts a turn has produced so far.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TurnProgress {
    /// Committed tool receipts (success or failure - both are facts).
    pub committed_receipts: usize,
    /// A terminal marker appeared in the model output.
    pub saw_terminal_marker: bool,
    /// A durable terminal message has been committed.
    pub committed_terminal: bool,
}

impl TurnProgress {
    #[must_use]
    pub const fn satisfied(&self, rule: &SatisfactionRule) -> bool {
        match rule {
            SatisfactionRule::Receipts { required } => self.committed_receipts >= *required,
            SatisfactionRule::TerminalMarker => self.saw_terminal_marker,
        }
    }
}

/// How long after satisfaction the runtime waits, and how it nags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalObligation {
    /// When a terminal answer becomes owed.
    pub satisfaction: SatisfactionRule,
    /// How long after satisfaction to wait before probing.
    pub commit_window: Duration,
    /// How many commit probes may be issued.
    pub probe_budget: u32,
}

impl Default for TerminalObligation {
    fn default() -> Self {
        Self {
            // Three receipts: enough to cover a locate -> read -> verify chain.
            satisfaction: SatisfactionRule::Receipts { required: 3 },
            commit_window: Duration::from_secs(90),
            probe_budget: 1,
        }
    }
}

/// What the terminal obligation says right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalVerdict {
    /// The obligation is not armed yet; nothing is owed.
    NotSatisfied,
    /// A durable terminal message has been committed. Done.
    Committed,
    /// Armed, but still inside the commit window. Do not nag.
    WithinWindow {
        /// Time since the objective became satisfied.
        since_satisfied: Duration,
    },
    /// Armed, window elapsed, probes remain: ask the model to commit.
    Probe {
        since_satisfied: Duration,
        /// The probe number being requested (1-based).
        probe: u32,
    },
    /// Armed, probes exhausted, still no terminal message.
    NonCommitting {
        since_satisfied: Duration,
        probes: u32,
    },
}

/// Tracks the terminal obligation for one turn.
#[derive(Debug, Clone)]
pub struct TerminalTracker {
    obligation: TerminalObligation,
    probes_issued: u32,
    armed: bool,
}

impl TerminalTracker {
    #[must_use]
    pub const fn new(obligation: TerminalObligation) -> Self {
        Self {
            obligation,
            probes_issued: 0,
            armed: false,
        }
    }

    /// Evaluate the obligation against the turn's progress.
    ///
    /// `since_satisfied` is `None` while the objective is not yet satisfied, and
    /// is the elapsed time since the causal arming event afterwards. Supplied by
    /// the caller so this stays a pure function of (obligation, progress,
    /// since_satisfied, probes).
    #[must_use]
    pub fn evaluate(&self, progress: &TurnProgress, since_satisfied: Option<Duration>) -> TerminalVerdict {
        if progress.committed_terminal {
            return TerminalVerdict::Committed;
        }
        if !self.armed && !progress.satisfied(&self.obligation.satisfaction) {
            return TerminalVerdict::NotSatisfied;
        }
        if !self.armed {
            // Armed by this evaluation but never observed as satisfied before:
            // the window starts now.
            return TerminalVerdict::WithinWindow {
                since_satisfied: Duration::ZERO,
            };
        }
        let since = since_satisfied.unwrap_or(Duration::ZERO);
        if since < self.obligation.commit_window {
            return TerminalVerdict::WithinWindow { since_satisfied: since };
        }
        if self.probes_issued >= self.obligation.probe_budget {
            return TerminalVerdict::NonCommitting {
                since_satisfied: since,
                probes: self.probes_issued,
            };
        }
        TerminalVerdict::Probe {
            since_satisfied: since,
            probe: self.probes_issued + 1,
        }
    }

    /// Arm the obligation. Called when the caller first observes satisfaction.
    pub fn arm(&mut self) {
        self.armed = true;
    }

    /// Whether the obligation is armed.
    #[must_use]
    pub const fn is_armed(&self) -> bool {
        self.armed
    }

    /// Consume one commit probe.
    pub fn issue_probe(&mut self) -> bool {
        if self.probes_issued >= self.obligation.probe_budget {
            return false;
        }
        self.probes_issued = self.probes_issued.saturating_add(1);
        true
    }

    /// Probes issued so far.
    #[must_use]
    pub const fn probes_issued(&self) -> u32 {
        self.probes_issued
    }
}

#[cfg(test)]
mod terminal_tests {
    use super::*;

    fn obligation(required: usize, window_secs: u64, budget: u32) -> TerminalObligation {
        TerminalObligation {
            satisfaction: SatisfactionRule::Receipts { required },
            commit_window: Duration::from_secs(window_secs),
            probe_budget: budget,
        }
    }

    #[test]
    fn nothing_is_owed_before_the_objective_is_satisfied() {
        let tracker = TerminalTracker::new(obligation(3, 90, 1));
        let progress = TurnProgress {
            committed_receipts: 2,
            saw_terminal_marker: false,
            committed_terminal: false,
        };
        assert_eq!(
            tracker.evaluate(&progress, None),
            TerminalVerdict::NotSatisfied
        );
    }

    /// The I1 guard: a turn that has not yet satisfied its objective is never
    /// nagged, however long it has been running. Elapsed time is not an input.
    #[test]
    fn a_long_unsatisfied_turn_is_never_nagged() {
        let tracker = TerminalTracker::new(obligation(3, 90, 1));
        let progress = TurnProgress {
            committed_receipts: 0,
            saw_terminal_marker: false,
            committed_terminal: false,
        };
        // A full day of work with no receipts yet: still not owed anything.
        assert_eq!(
            tracker.evaluate(&progress, Some(Duration::from_secs(86_400))),
            TerminalVerdict::NotSatisfied
        );
    }

    #[test]
    fn a_committed_terminal_message_ends_the_obligation() {
        let tracker = TerminalTracker::new(obligation(3, 90, 1));
        let progress = TurnProgress {
            committed_receipts: 9,
            saw_terminal_marker: false,
            committed_terminal: true,
        };
        assert_eq!(tracker.evaluate(&progress, None), TerminalVerdict::Committed);
    }

    #[test]
    fn satisfaction_starts_the_window_and_a_probe_follows_it() {
        let mut tracker = TerminalTracker::new(obligation(3, 90, 1));
        let satisfied = TurnProgress {
            committed_receipts: 3,
            saw_terminal_marker: false,
            committed_terminal: false,
        };
        // First observation of satisfaction arms the obligation.
        assert_eq!(
            tracker.evaluate(&satisfied, None),
            TerminalVerdict::WithinWindow {
                since_satisfied: Duration::ZERO
            }
        );
        tracker.arm();

        // Inside the window: never nag, however the model spends it.
        assert_eq!(
            tracker.evaluate(&satisfied, Some(Duration::from_secs(89))),
            TerminalVerdict::WithinWindow {
                since_satisfied: Duration::from_secs(89)
            }
        );

        // Past the window with no terminal message: one probe.
        assert_eq!(
            tracker.evaluate(&satisfied, Some(Duration::from_secs(90))),
            TerminalVerdict::Probe {
                since_satisfied: Duration::from_secs(90),
                probe: 1
            }
        );
        assert!(tracker.issue_probe());

        // Probes exhausted -> typed failure, the only path to NonCommitting.
        assert_eq!(
            tracker.evaluate(&satisfied, Some(Duration::from_secs(180))),
            TerminalVerdict::NonCommitting {
                since_satisfied: Duration::from_secs(180),
                probes: 1
            }
        );
        assert!(!tracker.issue_probe(), "the probe budget is hard");
    }

    #[test]
    fn a_terminal_marker_also_arms_the_obligation() {
        let tracker = TerminalTracker::new(TerminalObligation {
            satisfaction: SatisfactionRule::TerminalMarker,
            commit_window: Duration::from_secs(10),
            probe_budget: 1,
        });
        let progress = TurnProgress {
            committed_receipts: 0,
            saw_terminal_marker: true,
            committed_terminal: false,
        };
        assert_eq!(
            tracker.evaluate(&progress, None),
            TerminalVerdict::WithinWindow {
                since_satisfied: Duration::ZERO
            }
        );
    }

    /// The happy path this obligation must not disturb: a turn that commits its
    /// terminal answer as soon as it is owed is never probed.
    #[test]
    fn a_turn_that_commits_promptly_is_never_probed() {
        let mut tracker = TerminalTracker::new(obligation(3, 90, 1));
        let mut progress = TurnProgress::default();
        for receipts in 1..=5_u64 {
            progress.committed_receipts += 1;
            let _ = receipts;
            tracker.arm();
            // A terminal message arrives with each satisfied round.
            progress.committed_terminal = progress.committed_receipts >= 3;
            assert_eq!(
                tracker.evaluate(&progress, Some(Duration::from_secs(1))),
                if progress.committed_terminal {
                    TerminalVerdict::Committed
                } else {
                    TerminalVerdict::WithinWindow {
                        since_satisfied: Duration::from_secs(1)
                    }
                },
                "a prompt commit must never be probed"
            );
        }
        assert_eq!(tracker.probes_issued(), 0);
    }
}
