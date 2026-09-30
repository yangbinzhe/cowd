//! PLAN D1a: liveness OBSERVATION and push-forward actions for one turn.
//!
//! # Why the earlier design was wrong
//!
//! An earlier revision of this module treated "no semantic progress" as a
//! failure to be reported: its `Silent` verdict reached a terminal state after a
//! probe budget, i.e. it KILLED the turn. That was built on an undiagnosed
//! symptom and it was the wrong medicine.
//!
//! The arm-C run that motivated it turned out to be nothing of the kind: the
//! runtime's own status machine advanced queued -> preparing_context ->
//! calling_model -> calling_tool -> calling_model over revisions 1..27 with 122
//! timeline events, while only ONE durable message existed. "599s without
//! progress" was an artefact of measuring durable messages, not of the runtime
//! being idle - and it was the same mixed read+derive task that took arm A 32
//! rounds on its own.
//!
//! # What this module is now
//!
//! An OBSERVER plus a ladder of PUSH-FORWARD actions. It never terminates a
//! turn, and total elapsed time is not an input to anything. When semantic
//! progress stalls, the runtime does something that helps the model finish:
//!
//! 1. `Refocus`       - say what is still missing, concretely;
//! 2. `Summarize`     - compress what is already known so the next step is cheap;
//! 3. `RequestCommit` - ask for the terminal answer (evidence already suffices);
//! 4. `Ask`           - ask the model whether it can continue. Only the MODEL
//!                      decides a task is finished or impossible.
//!
//! This is PLAN I1 taken one step further: the framework does not own the right
//! to declare a turn failed on a wall clock.

use std::time::Duration;

/// What the runtime should DO about a turn that stopped making semantic
/// progress. Every variant is constructive; none of them ends the turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivenessAction {
    /// Say what is still missing, concretely. Not "you are idle".
    Refocus,
    /// Compress what is already known so the next step is cheap.
    Summarize,
    /// Ask for the terminal answer (the evidence already suffices).
    RequestCommit,
    /// Ask the model whether it can continue. The model owns the verdict on
    /// whether the task is finished or impossible; the runtime never asserts it
    /// from a clock.
    Ask,
}

/// Which push-forward action corresponds to escalation `step` of `budget`.
///
/// The ladder is constructive and ends by ASKING the model: a framework must
/// not decide from a clock that the work is over.
#[must_use]
pub const fn action_for_step(step: u32, budget: u32) -> LivenessAction {
    if budget == 0 || step >= budget {
        return LivenessAction::Ask;
    }
    match step {
        1 => LivenessAction::Refocus,
        2 => LivenessAction::Summarize,
        3 => LivenessAction::RequestCommit,
        _ => LivenessAction::Ask,
    }
}

/// What a turn's liveness state says right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LivenessVerdict {
    /// Semantic progress happened recently. Nothing to do.
    Progressing,
    /// Inactive for at least the window. `action` is what the runtime should DO
    /// to help the turn continue. There is deliberately no terminal variant.
    Stalled {
        /// How long since the last semantic progress.
        idle: Duration,
        /// Which push-forward step of the ladder this is (1-based).
        step: u32,
        /// What to do.
        action: LivenessAction,
    },
}

/// The liveness contract for one turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivenessContract {
    /// How long without semantic progress before the runtime should help.
    pub inactivity_window: Duration,
    /// How many push-forward steps the ladder has. The last one is always
    /// `Ask`, because only the model may declare a task finished or impossible.
    pub escalation_budget: u32,
    /// **Always `None`.** A wall clock never terminates a turn (PLAN I1). The
    /// field exists so a caller cannot quietly reintroduce one: constructing a
    /// contract with `Some(_)` is a compile-time-visible decision that must be
    /// justified in review.
    pub wall_clock_bound: Option<Duration>,
}

impl Default for LivenessContract {
    fn default() -> Self {
        Self {
            // Two minutes of *semantic* inactivity before the runtime reacts. A
            // healthy turn produces far more often than this: a committed
            // message, a completed tool call, or streamed bytes.
            inactivity_window: Duration::from_secs(120),
            // Refocus -> Summarize -> RequestCommit -> Ask (the model decides).
            escalation_budget: 4,
            wall_clock_bound: None,
        }
    }
}

impl LivenessContract {
    /// A contract with no escalation at all: every turn is patient forever.
    ///
    /// This is the pre-D1 behaviour and it is *not* a valid production
    /// contract - it is retained so tests can show what it costs (a stalled turn
    /// is never helped).
    #[must_use]
    pub const fn patient() -> Self {
        Self {
            inactivity_window: Duration::from_secs(u64::MAX / 2),
            escalation_budget: 0,
            wall_clock_bound: None,
        }
    }
}

/// Tracks semantic progress and escalations issued for one turn.
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
    escalations_issued: u32,
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
            escalations_issued: 0,
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

    /// Escalations issued so far.
    #[must_use]
    pub const fn escalations_issued(&self) -> u32 {
        self.escalations_issued
    }

    /// Total semantic progress observed.
    #[must_use]
    pub const fn progress(&self) -> u64 {
        self.observed_progress
    }

    /// Evaluate liveness given how long ago the last semantic progress was.
    ///
    /// `idle` is supplied by the caller so this stays a pure, deterministic
    /// function of (contract, idle, escalations) - which is what makes the unit
    /// tests meaningful without a real clock or provider.
    #[must_use]
    pub fn evaluate(&self, idle: Duration) -> LivenessVerdict {
        if idle < self.contract.inactivity_window {
            return LivenessVerdict::Progressing;
        }
        let step = self.escalations_issued.saturating_add(1);
        LivenessVerdict::Stalled {
            idle,
            step,
            action: action_for_step(step, self.contract.escalation_budget),
        }
    }

    /// Consume one escalation step and return what the runtime should DO.
    ///
    /// Never ends the turn: the ladder's last step is `Ask`, which hands the
    /// verdict to the model rather than to a clock.
    pub fn escalate(&mut self) -> LivenessAction {
        let step = self.escalations_issued.saturating_add(1);
        let action = action_for_step(step, self.contract.escalation_budget);
        if self.escalations_issued < self.contract.escalation_budget {
            self.escalations_issued = step;
        }
        action
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract(window_secs: u64, budget: u32) -> LivenessContract {
        LivenessContract {
            inactivity_window: Duration::from_secs(window_secs),
            escalation_budget: budget,
            wall_clock_bound: None,
        }
    }

    #[test]
    fn recent_progress_is_progressing() {
        let tracker = LivenessTracker::new(contract(120, 4));
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
    fn inactivity_past_the_window_asks_the_runtime_to_help() {
        let tracker = LivenessTracker::new(contract(120, 4));
        assert_eq!(
            tracker.evaluate(Duration::from_secs(120)),
            LivenessVerdict::Stalled {
                idle: Duration::from_secs(120),
                step: 1,
                action: LivenessAction::Refocus,
            }
        );
    }

    /// The push-forward ladder escalates constructively and ends by asking the
    /// model, never by terminating.
    #[test]
    fn the_ladder_escalates_and_ends_by_asking_the_model() {
        let mut tracker = LivenessTracker::new(contract(120, 4));
        let mut actions = Vec::new();
        for _ in 0..8 {
            if let LivenessVerdict::Stalled { .. } = tracker.evaluate(Duration::from_secs(600)) {
                actions.push(tracker.escalate());
            }
        }
        assert_eq!(
            actions,
            vec![
                LivenessAction::Refocus,
                LivenessAction::Summarize,
                LivenessAction::RequestCommit,
                LivenessAction::Ask,
                LivenessAction::Ask,
                LivenessAction::Ask,
                LivenessAction::Ask,
                LivenessAction::Ask,
            ],
            "the ladder must climb to Ask and then keep asking, never terminate"
        );
    }

    /// The I1 invariant the whole module exists to protect: total elapsed time
    /// is not an input to any verdict.
    #[test]
    fn a_long_turn_with_continuous_progress_is_never_stalled() {
        let mut tracker = LivenessTracker::new(contract(120, 4));
        for minute in 0..600_u64 {
            tracker.record_progress();
            let idle = Duration::from_secs(29);
            assert_eq!(
                tracker.evaluate(idle),
                LivenessVerdict::Progressing,
                "minute {minute} must still be progressing"
            );
        }
        assert_eq!(tracker.progress(), 600);
        assert_eq!(tracker.escalations_issued(), 0, "nothing to help with");
    }

    /// The pre-D1 behaviour, kept so its cost is explicit: a stalled turn is
    /// never helped, which is exactly the arm-C / Facet-A shape.
    #[test]
    fn the_patient_contract_never_helps_a_stalled_turn() {
        let tracker = LivenessTracker::new(LivenessContract::patient());
        assert_eq!(
            tracker.evaluate(Duration::from_secs(86_400)),
            LivenessVerdict::Progressing,
            "the patient contract never notices a stalled turn"
        );
        assert_eq!(
            action_for_step(1, 0),
            LivenessAction::Ask,
            "an empty ladder degenerates to asking the model"
        );
    }

    #[test]
    fn progress_resets_the_window() {
        let mut tracker = LivenessTracker::new(contract(120, 4));
        tracker.record_progress();
        tracker.record_progress();
        assert_eq!(
            tracker.evaluate(Duration::from_secs(90)),
            LivenessVerdict::Progressing
        );
        assert!(matches!(
            tracker.evaluate(Duration::from_secs(121)),
            LivenessVerdict::Stalled { step: 1, .. }
        ));
    }

    #[test]
    fn the_default_contract_has_no_wall_clock() {
        let contract = LivenessContract::default();
        assert_eq!(
            contract.wall_clock_bound, None,
            "a wall clock must never be able to terminate a turn (PLAN I1)"
        );
        assert!(contract.inactivity_window > Duration::ZERO);
        assert!(contract.escalation_budget > 0, "an escalation ladder is required");
        assert_eq!(
            action_for_step(contract.escalation_budget, contract.escalation_budget),
            LivenessAction::Ask,
            "the last rung must hand the verdict to the model"
        );
    }
}

/// Wrap a stream so that semantic inactivity produces a push-forward hint
/// instead of a termination.
///
/// This is the D1b mechanism, corrected: the earlier version emitted a terminal
/// `provider silent` error, which killed the turn - and the diagnosis it was
/// built on turned out to be wrong (see the module docs). Now, when no item
/// arrives within the inactivity window, the wrapper reports the verdict to
/// `on_stall` and KEEPS WAITING: the stream is never cut off, and a productive
/// stream is never interrupted however long it runs.
///
/// The one thing this still guarantees is that a silent producer cannot be
/// mistaken for a healthy one: the caller is told, with the real idle time and
/// the ladder step, so it can help the model instead of guessing.
pub fn guard_liveness<S, T, F>(
    stream: S,
    contract: LivenessContract,
    mut on_stall: F,
) -> impl futures::Stream<Item = T>
where
    // No `'static` bound: the provider event stream borrows its client, and a
    // borrowed stream must stay guardable rather than forced to be owned.
    S: futures::Stream<Item = T>,
    F: FnMut(LivenessVerdict),
{
    let tracker = LivenessTracker::new(contract);
    // `last_notified` keeps the ladder from repeating itself: once the runtime
    // has been told what to do at a given rung, it is told again only when the
    // ladder actually advances, or never - repeating "you are idle" is noise.
    futures::stream::unfold(
        (
            Box::pin(stream),
            tracker,
            None::<Duration>,
            Some(on_stall),
            0_u32,
        ),
        move |(mut inner, mut tracker, mut pending, mut on_stall, mut last_notified)| async move {
            loop {
                let idle = pending.take().unwrap_or(Duration::ZERO);
                if let LivenessVerdict::Stalled { step, .. } = tracker.evaluate(idle) {
                    if step > last_notified && step <= tracker.contract().escalation_budget {
                        last_notified = step;
                        if let Some(callback) = on_stall.as_mut() {
                            (callback)(tracker.evaluate(idle));
                        }
                        let _ = tracker.escalate();
                    }
                }
                let window = tracker.contract().inactivity_window;
                match tokio::time::timeout(window, futures::StreamExt::next(&mut inner)).await {
                    Ok(Some(item)) => {
                        tracker.record_progress();
                        return Some((item, (inner, tracker, None, on_stall, last_notified)));
                    }
                    Ok(None) => return None,
                    Err(_elapsed) => pending = Some(window),
                }
            }
        },
    )
}

#[cfg(test)]
mod guard_tests {
    use super::*;
    use futures::StreamExt;

    /// A stream that yields nothing keeps being waited on, and the runtime is
    /// told what to DO about it. Nothing is terminated.
    #[tokio::test]
    async fn a_silent_producer_is_reported_and_still_waited_on() {
        let never = futures::stream::pending::<u8>();
        let window = Duration::from_millis(30);
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = std::sync::Arc::clone(&seen);
        let guarded = guard_liveness(
            never,
            LivenessContract {
                inactivity_window: window,
                escalation_budget: 4,
                wall_clock_bound: None,
            },
            move |verdict| {
                if let LivenessVerdict::Stalled { action, .. } = verdict {
                    sink.lock().unwrap().push(action);
                }
            },
        );
        // The stream has no items, so collect cannot finish: prove the guard did
        // not terminate it by taking only what the guard reports within a bound.
        let mut guarded = std::pin::pin!(guarded);
        let first = tokio::time::timeout(Duration::from_millis(400), guarded.next()).await;
        assert!(
            first.is_err(),
            "the guard must not end a silent stream - that was the old, wrong design"
        );
        let actions = seen.lock().unwrap().clone();
        assert_eq!(
            actions,
            vec![
                LivenessAction::Refocus,
                LivenessAction::Summarize,
                LivenessAction::RequestCommit,
                LivenessAction::Ask,
            ],
            "the runtime must be told to help, in a constructive order"
        );
    }

    /// The I1 invariant, end to end: a stream that keeps producing items is
    /// never interrupted, however long the total run takes.
    #[tokio::test]
    async fn a_long_productive_stream_is_never_interrupted() {
        let items = 150_u32;
        let productive = futures::stream::unfold(0_u32, move |state| async move {
            if state >= items {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
            Some((Ok::<u8, String>(1), state + 1))
        });
        let guarded = guard_liveness(
            productive,
            LivenessContract {
                inactivity_window: Duration::from_millis(30),
                escalation_budget: 4,
                wall_clock_bound: None,
            },
            |verdict| unreachable!("a productive stream must never be stalled: {verdict:?}"),
        );
        let collected =
            tokio::time::timeout(Duration::from_secs(10), guarded.collect::<Vec<_>>())
                .await
                .expect("a productive stream must run to completion");
        assert_eq!(collected.len(), items as usize);
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
                escalation_budget: 4,
                wall_clock_bound: None,
            },
            |verdict| panic!("unexpected stall: {verdict:?}"),
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
    /// How long after satisfaction to wait before asking for the commit.
    pub commit_window: Duration,
    /// How many commit requests may be issued.
    pub commit_budget: u32,
}

impl Default for TerminalObligation {
    fn default() -> Self {
        Self {
            // Three receipts: enough to cover a locate -> read -> verify chain.
            satisfaction: SatisfactionRule::Receipts { required: 3 },
            commit_window: Duration::from_secs(90),
            commit_budget: 1,
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
    /// Armed, window elapsed, requests remain: ask the model to commit.
    RequestCommit {
        since_satisfied: Duration,
        /// The request number being made (1-based).
        request: u32,
    },
    /// Armed, requests exhausted, still no terminal message.
    NonCommitting {
        since_satisfied: Duration,
        requests: u32,
    },
}

/// Tracks the terminal obligation for one turn.
#[derive(Debug, Clone)]
pub struct TerminalTracker {
    obligation: TerminalObligation,
    requests_issued: u32,
    armed: bool,
}

impl TerminalTracker {
    #[must_use]
    pub const fn new(obligation: TerminalObligation) -> Self {
        Self {
            obligation,
            requests_issued: 0,
            armed: false,
        }
    }

    /// Evaluate the obligation against the turn's progress.
    ///
    /// `since_satisfied` is `None` while the objective is not yet satisfied, and
    /// is the elapsed time since the causal arming event afterwards. Supplied by
    /// the caller so this stays a pure function of (obligation, progress,
    /// since_satisfied, requests).
    #[must_use]
    pub fn evaluate(
        &self,
        progress: &TurnProgress,
        since_satisfied: Option<Duration>,
    ) -> TerminalVerdict {
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
        if self.requests_issued >= self.obligation.commit_budget {
            return TerminalVerdict::NonCommitting {
                since_satisfied: since,
                requests: self.requests_issued,
            };
        }
        TerminalVerdict::RequestCommit {
            since_satisfied: since,
            request: self.requests_issued + 1,
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

    /// The obligation this tracker was built with.
    ///
    /// The caller owns the satisfaction clock, so it needs the rule to decide
    /// when the causal arming event has occurred.
    #[must_use]
    pub const fn obligation(&self) -> &TerminalObligation {
        &self.obligation
    }

    /// Consume one commit request.
    pub fn request_commit(&mut self) -> bool {
        if self.requests_issued >= self.obligation.commit_budget {
            return false;
        }
        self.requests_issued = self.requests_issued.saturating_add(1);
        true
    }

    /// Commit requests issued so far.
    #[must_use]
    pub const fn requests_issued(&self) -> u32 {
        self.requests_issued
    }
}

#[cfg(test)]
mod terminal_tests {
    use super::*;

    fn obligation(required: usize, window_secs: u64, budget: u32) -> TerminalObligation {
        TerminalObligation {
            satisfaction: SatisfactionRule::Receipts { required },
            commit_window: Duration::from_secs(window_secs),
            commit_budget: budget,
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
        assert_eq!(
            tracker.evaluate(&progress, Some(Duration::from_secs(86_400))),
            TerminalVerdict::NotSatisfied
        );
    }

    #[test]
    fn the_arming_rule_is_the_same_rule_evaluate_uses() {
        // The caller arms the tracker from `obligation().satisfaction`, so the
        // rule it reads must be the one `evaluate` enforces.
        let tracker = TerminalTracker::new(obligation(3, 90, 1));
        assert_eq!(
            tracker.obligation().satisfaction,
            SatisfactionRule::Receipts { required: 3 }
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
    fn satisfaction_starts_the_window_and_a_request_follows_it() {
        let mut tracker = TerminalTracker::new(obligation(3, 90, 1));
        let satisfied = TurnProgress {
            committed_receipts: 3,
            saw_terminal_marker: false,
            committed_terminal: false,
        };
        assert_eq!(
            tracker.evaluate(&satisfied, None),
            TerminalVerdict::WithinWindow {
                since_satisfied: Duration::ZERO
            }
        );
        tracker.arm();
        assert_eq!(
            tracker.evaluate(&satisfied, Some(Duration::from_secs(89))),
            TerminalVerdict::WithinWindow {
                since_satisfied: Duration::from_secs(89)
            }
        );
        assert_eq!(
            tracker.evaluate(&satisfied, Some(Duration::from_secs(90))),
            TerminalVerdict::RequestCommit {
                since_satisfied: Duration::from_secs(90),
                request: 1
            }
        );
        assert!(tracker.request_commit());
        assert_eq!(
            tracker.evaluate(&satisfied, Some(Duration::from_secs(180))),
            TerminalVerdict::NonCommitting {
                since_satisfied: Duration::from_secs(180),
                requests: 1
            }
        );
        assert!(!tracker.request_commit(), "the request budget is hard");
    }

    #[test]
    fn a_terminal_marker_also_arms_the_obligation() {
        let tracker = TerminalTracker::new(TerminalObligation {
            satisfaction: SatisfactionRule::TerminalMarker,
            commit_window: Duration::from_secs(10),
            commit_budget: 1,
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

    /// The happy path this obligation must not disturb.
    #[test]
    fn a_turn_that_commits_promptly_is_never_asked() {
        let mut tracker = TerminalTracker::new(obligation(3, 90, 1));
        let mut progress = TurnProgress::default();
        for _ in 1..=5_u64 {
            progress.committed_receipts += 1;
            tracker.arm();
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
                "a prompt commit must never be asked for"
            );
        }
        assert_eq!(tracker.requests_issued(), 0);
    }
}
