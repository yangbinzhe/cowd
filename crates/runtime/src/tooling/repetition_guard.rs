//! Repetition and stall supervision for tool execution.
//!
//! A live one-team run spent 105 minutes producing 4 tasks that never left
//! `running`. The durable journal shows why: 150 failed tool invocations, of
//! which the same two errors repeat. `task_review` failed 30+ times with a
//! byte-identical `execution graph ... does not exist`, and `read_ref` failed
//! with `invalid MCP tool name` - and the model saw every one of them, 179
//! occurrences of the same text in its context envelopes. Knowing was not
//! enough: nothing told it that retrying the same call could not help, and
//! nothing told the Runtime to stop re-dispatching it.
//!
//! The framework already had repetition supervision, keyed on
//! `program|task|mode` (`agentic/execution/dispatch.rs`). It could not see this
//! loop for two reasons: every replan mints a new graph, so the key changes on
//! every iteration; and the mechanism governs *dispatch deferral*, not
//! *execution failure*. A content-addressed key with the right event type is
//! what was missing.
//!
//! Three questions decide whether this is safe, and each has a concrete answer:
//!
//! 1. **Does the key survive replanning?** A content hash does not, because the
//!    model rewords its call. The key is therefore the tool name plus a
//!    normalized error signature, which is invariant under rewording.
//! 2. **Does it kill legitimate retries?** It would, so only *deterministic*
//!    failures count. `Timeout` and `Panic` describe a transient condition and
//!    are excluded; a permission denial is deterministic but the model can fix
//!    it, so the breaker reports it without blocking.
//! 3. **Who lifts the breaker?** Progress does. Any accepted task transition
//!    clears the ledger, so a breaker can never outlive the state it was filed
//!    under.
//!
//! Nothing here forbids an action. A tripped breaker converts a repeat
//! invocation into a diagnosis, which is the framework stating what it already
//! knows and leaving the judgment to the model.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::tool_invocation::ToolFailureKind;

/// Consecutive identical failures tolerated before the breaker trips. The
/// second failure is already evidence that retrying changes nothing, so the
/// default is deliberately lower than the dispatch-starvation bound.
pub(crate) fn repetition_bound() -> usize {
    std::env::var("COWD_TOOL_REPETITION_LIMIT")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepetitionState {
    count: usize,
    signature: String,
    /// Whether the cause named something the model could not have put in its own
    /// input. Only these causes may refuse a call that differs in wording.
    state_derived: bool,
}

fn repetition_ledger() -> &'static std::sync::Mutex<BTreeMap<String, RepetitionState>> {
    static LEDGER: OnceLock<std::sync::Mutex<BTreeMap<String, RepetitionState>>> = OnceLock::new();
    LEDGER.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
}

/// A retryable failure is not a repetition. Timeouts and panics describe a
/// transient condition, so they never count toward the breaker; a permission
/// denial is deterministic but actionable, so it is reported rather than
/// blocked.
fn counts_toward_breaker(kind: ToolFailureKind) -> bool {
    matches!(kind, ToolFailureKind::ExecutionError | ToolFailureKind::Unknown)
}

/// Normalize an error message into a signature.
///
/// Identifiers are replaced because the message names the thing that died - the
/// graph id, the artifact id - and those change on every replan while the cause
/// does not. Masking is what makes the same failure recognizable across replans
/// and across the model rewording its call.
///
/// A hex run containing a digit is an identifier; a hex run without one is
/// prose. Masking only decimal digits is not enough (`run:abc123` vs
/// `run:9f8e7d` differ in their letters too), and masking every hex run is too
/// much (`execution` would become `#x#ution`, making unrelated causes compare
/// equal and tripping the breaker on the wrong failures). Requiring a digit is
/// what separates the two.
fn error_signature(message: &str) -> String {
    // An identifier is a run of hex characters that is NOT an English word:
    // `run:abc123` is an id, `execution` is prose. Masking every hex run would
    // turn the message into `#x#ution gr#ph` and make two different causes
    // compare equal - which would trip the breaker on unrelated failures.
    //
    // The discriminator is that identifiers contain a digit. `9f8e7d` has one,
    // `execution` does not. So: collect hex runs, and mask only those
    // containing at least one digit.
    let mut signature = String::new();
    let mut run = String::new();
    let flush = |run: &str, signature: &mut String| {
        if run.is_empty() {
            return;
        }
        if is_identifier(run) {
            signature.push('#');
        } else {
            signature.push_str(run);
        }
    };
    for ch in message.chars().take(200) {
        if ch.is_ascii_hexdigit() {
            run.push(ch);
        } else {
            flush(&run, &mut signature);
            run.clear();
            signature.push(ch.to_ascii_lowercase());
        }
    }
    flush(&run, &mut signature);
    // A longer id must not read as a different signature.
    let mut collapsed = String::with_capacity(signature.len());
    let mut prev_hash = false;
    for ch in signature.chars() {
        if ch == '#' {
            if !prev_hash {
                collapsed.push('#');
            }
            prev_hash = true;
        } else {
            collapsed.push(ch);
            prev_hash = false;
        }
    }
    collapsed.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Record a failure and decide what the next attempt should be told.
///
/// Returns `Some(diagnosis)` when the breaker has tripped: the caller should
/// refuse to re-dispatch the same call and surface the diagnosis instead. The
/// diagnosis names what did not change, because that is the fact the model was
/// missing when it retried.
pub fn record_tool_failure(
    turn_id: &str,
    tool_name: &str,
    input_hash: &str,
    input_refs: &[String],
    kind: ToolFailureKind,
    message: &str,
) -> Option<String> {
    if !counts_toward_breaker(kind) {
        return None;
    }
    // Two ledgers over one failure, answering two different questions.
    //
    // `call:` answers "the same call again". The live journal shows why the input
    // is needed: 106 failed `task_review` calls carried the same input hash but
    // distinct tool call ids, so the model re-decided the same call 106 times
    // rather than retrying one.
    //
    // `cause:` answers "the same dead end again". A key that carries the input
    // cannot answer this, because the model can reword its own prose and buy a
    // fresh attempt - one live run reworded `reason` 28 times over one cause.
    //
    // The two are not interchangeable, and the difference is what makes a refusal
    // safe: a cause the model could not have named in its own input is a
    // runtime-state condition, so no rewording can make it succeed and refusing
    // it refuses nothing that could have worked. A cause the model *did* name is
    // fixable by naming something else, and must not be refused.
    let call_key = format!("call:{turn_id}:{tool_name}:{input_hash}");
    let signature = error_signature(message);
    let cause_key = format!("cause:{turn_id}:{tool_name}:{signature}");
    let state_derived = state_derived_cause(message, input_refs);
    let mut ledger = repetition_ledger()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let bound = repetition_bound();
    let call_entry = ledger.entry(call_key).or_default();
    call_entry.count = call_entry.count.saturating_add(1);
    call_entry.signature = signature.clone();
    let call_count = call_entry.count;
    let cause_entry = ledger.entry(cause_key).or_default();
    cause_entry.count = cause_entry.count.saturating_add(1);
    cause_entry.signature = signature;
    cause_entry.state_derived = state_derived;
    let count = cause_entry.count.max(call_count);
    if count < bound {
        return None;
    }
    // Quote the failure itself. A diagnosis that only says "do not repeat" leaves
    // a model that cannot construct a valid call with nowhere to go: one live run
    // re-sent `{}` to `artifact_publish` twelve times while being told to change
    // the input, because nothing said the input was missing `path`. The schema
    // validator already names the field; repeating its words is the whole fix.
    let first_line = message
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_default()
        .trim();
    let detail = if first_line.is_empty() {
        String::new()
    } else {
        format!("\nFramework reported: {first_line}")
    };
    // The sentence has to match what Runtime will actually do. When the cause
    // names something the model could not have put in its own input, changing the
    // wording cannot change the outcome, so a different call to the same tool in
    // this turn is refused too - and saying "a different call will go through"
    // would be a promise Runtime cannot keep.
    let scope = if state_derived {
        "The cause names an object that was not in the input, so no wording of the \
         input can change it: Runtime will not re-dispatch this tool in this turn \
         while the cause stands."
    } else {
        "Runtime will keep re-dispatching a different call; it will not re-dispatch \
         this one."
    };
    Some(format!(
        "This exact failure has now occurred {count} times and the cause has not \
         changed. Re-issuing the same call cannot produce a different result. \
         Either change the input, reach the same fact through another tool, or - \
         if the objective is genuinely unreachable - call \
         objective_complete_request with blocked=true, a blocked_reason, \
         human_review_required=true and recommended_next_actions. {scope}{detail}"
    ))
}

/// Clear the breakers filed under one turn.
///
/// Scoped to a turn rather than clearing the whole map: a global clear is a
/// process-wide side effect, and in a test binary where every test shares one
/// process it wipes breakers another test is mid-way through asserting on. Keys
/// already carry the turn id, so a turn-scoped clear is both sufficient and
/// free of that interference.
/// Whether dispatching this exact call again is refused.
///
/// A diagnosis is advice; a refusal is what stops a loop. When the same call in
/// the same turn has already failed deterministically this many times, Runtime
/// will not run it again, because a deterministic failure cannot start succeeding
/// on the next attempt - one live run re-sent one review call one hundred and
/// twenty-two times for exactly that reason. Transient failures never count, so a
/// retry that can legitimately succeed is never refused.
pub fn is_dispatch_blocked(turn_id: &str, tool_name: &str, input_hash: &str) -> bool {
    // A poller legitimately re-issues identical calls while waiting on external
    // progress (`host_backend.rs:2876-2879` exempts them from the loop guard), and
    // the closure tools are the model's last exit - the diagnosis itself directs
    // the model to `objective_complete_request`. Refusing either would contradict
    // the guard's own intent or deadlock the turn.
    if is_exempt_tool(tool_name) {
        return false;
    }
    let bound = repetition_bound();
    let ledger = repetition_ledger()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if ledger
        .get(&format!("call:{turn_id}:{tool_name}:{input_hash}"))
        .is_some_and(|entry| entry.count >= bound)
    {
        return true;
    }
    let prefix = format!("cause:{turn_id}:{tool_name}:");
    ledger
        .range(prefix.clone()..)
        .take_while(|(key, _)| key.starts_with(&prefix))
        .any(|(_, entry)| entry.state_derived && entry.count >= bound)
}

/// Whether a tool is never refused: pollers (waiting on external progress) and the
/// closure tools (the model's exit path).
fn is_exempt_tool(tool_name: &str) -> bool {
    let lower = tool_name.to_ascii_lowercase();
    lower == "process"
        || lower.ends_with("_get_result")
        || lower.ends_with("_poll")
        || lower == "objective_complete_request"
        || lower == "objective_review"
}

/// Whether the cause names something the model could not have put in its own input.
///
/// An identifier in the message that is absent from the input's canonical refs was
/// resolved by Runtime, not sent by the model - so the cause is a state condition,
/// and no rewording of the input can make the call succeed. When the identifier is
/// in the input, the model can name something else, and the call must not be
/// refused.
fn is_identifier(run: &str) -> bool {
    run.len() >= 4 && run.chars().any(|ch| ch.is_ascii_digit())
}

/// Whether the cause names something the model could not have put in its own input.
///
/// An identifier in the message that is absent from the input's canonical refs was
/// resolved by Runtime, not sent by the model - so the cause is a state condition,
/// and no rewording of the input can make the call succeed. When the identifier is
/// in the input, the model can name something else, and the call must not be
/// refused. This is the same identifier rule `error_signature` uses, so the
/// classification cannot drift from the signature it is meant to explain.
fn state_derived_cause(message: &str, input_refs: &[String]) -> bool {
    let mut run = String::new();
    let names_outside_the_input = |run: &str| {
        is_identifier(run) && !input_refs.iter().any(|reference| reference.contains(run))
    };
    for ch in message.chars().take(200) {
        if ch.is_ascii_hexdigit() {
            run.push(ch);
        } else {
            if names_outside_the_input(&run) {
                return true;
            }
            run.clear();
        }
    }
    names_outside_the_input(&run)
}

pub fn clear_turn(turn_id: &str) {
    // Keys carry a type prefix (`call:` / `cause:`) before the turn, so the turn
    // is not at the start of the key.
    let needle = format!(":{turn_id}:");
    repetition_ledger()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|key, _| !key.contains(needle.as_str()));
}

/// Clear every breaker. Test-only: production clears one turn at a time.
#[cfg(test)]
pub fn clear_all() {
    repetition_ledger()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

impl Default for RepetitionState {
    fn default() -> Self {
        Self {
            count: 0,
            signature: String::new(),
            state_derived: false,
        }
    }
}

#[cfg(test)]
mod repetition_guard_tests {
    use super::*;

    /// A unique turn id per test.
    ///
    /// The ledger keys on the turn, so two tests sharing one would interfere
    /// exactly as they did before the key carried a turn - and clearing the map
    /// between tests is itself a race, because libtest runs them on parallel
    /// threads. Unique ids make the tests independent by construction, with no
    /// shared reset at all.
    ///
    /// The macro must be invoked in the test body, not inside a shared helper:
    /// `line!()`/`column!()` resolve at the macro's call site, so a helper that
    /// called it once gave every test the same turn and the tests interfered again.
    macro_rules! turn {
        () => {
            concat!("turn-", line!(), "-", column!())
        };
    }

    fn err(turn: &str, kind: ToolFailureKind, msg: &str) -> Option<String> {
        record_tool_failure(turn, "task_review", "hash-1", &[], kind, msg)
    }

    #[test]
    fn the_same_failure_trips_on_the_second_occurrence() {
        let turn = turn!();
        // First failure: advice, not a verdict. The model has not yet seen that
        // retrying changes nothing.
        assert!(err(turn, ToolFailureKind::ExecutionError, "execution graph `run:abc123` does not exist").is_none());
        // Second: the graph id differs - a replan minted a new one - but the
        // message normalizes identifiers, so the cause is recognized.
        let tripped = err(turn, ToolFailureKind::ExecutionError, "execution graph `run:9f8e7d` does not exist")
            .expect("the second identical failure must trip the breaker");
        assert!(tripped.contains("cannot produce a different result"));
        assert!(tripped.contains("objective_complete_request"));
    }

    /// The whole point: a replan changes the identifier but not the cause, so a
    /// key that includes the raw id would never trip.
    #[test]
    fn a_new_identifier_does_not_reset_the_breaker() {
        let turn = turn!();
        err(turn, ToolFailureKind::ExecutionError, "graph `run:4f9a2b` does not exist");
        assert!(
            err(turn, ToolFailureKind::ExecutionError, "graph `run:7c1e08` does not exist").is_some(),
            "identifier changes must not buy the loop a fresh budget"
        );
    }

    /// A transient failure must never be counted. A timeout that is retried
    /// successfully is the common case, and blocking it would break work.
    #[test]
    fn transient_failures_never_trip_the_breaker() {
        let turn = turn!();
        for _ in 0..8 {
            assert!(err(turn, ToolFailureKind::Timeout, "request timed out").is_none());
            assert!(err(turn, ToolFailureKind::Panic, "worker panicked").is_none());
        }
    }

    /// A denial is deterministic but the model can fix it, so it is reported
    /// rather than blocked.
    #[test]
    fn denials_are_reported_but_never_blocked() {
        let turn = turn!();
        for _ in 0..6 {
            assert!(err(turn, ToolFailureKind::PermissionDenied, "permission denied").is_none());
        }
    }

    /// A breaker must not outlive its turn: once the task transitions, a later
    /// legitimate call to the same tool must go through.
    /// A diagnosis is advice a model may ignore; a refusal is what ends a loop.
    /// One live run re-sent one review call one hundred and twenty-two times
    /// against a graph it could not name, because nothing refused it.
    #[test]
    fn a_repeatedly_failing_call_is_refused_before_it_is_dispatched() {
        let turn = turn!();
        let tool = "task_review";
        let input = "hash-1";
        assert!(
            !is_dispatch_blocked(turn, tool, input),
            "nothing has failed yet"
        );
        for _ in 0..repetition_bound() {
            record_tool_failure(
                turn,
                tool,
                input,
                &[],
                ToolFailureKind::ExecutionError,
                "execution graph `run:abc` does not exist",
            );
        }
        assert!(
            is_dispatch_blocked(turn, tool, input),
            "the same call has failed deterministically past the bound and must be refused"
        );
    }

    /// Rewording the input must not buy a fresh attempt when the cause names
    /// something the model never sent.
    ///
    /// Live run 1006c, `live_one_team_execution`, step-3.7-flash: 28 `task_review`
    /// failures over 19 turns, 6 distinct input hashes, one cause. The model
    /// reworded its own `reason` field and bought a fresh attempt every time,
    /// because both keys carried the input hash. The failing graph id was never in
    /// the input - Runtime resolved it from the actor binding - so no wording could
    /// have changed the outcome.
    #[test]
    fn rewording_the_input_does_not_buy_a_fresh_attempt_for_a_state_derived_cause() {
        let turn = turn!();
        record_tool_failure(
            turn,
            "task_review",
            "hash-a",
            &[],
            ToolFailureKind::ExecutionError,
            "execution graph `run:abc123` does not exist",
        );
        let diagnosis = record_tool_failure(
            turn,
            "task_review",
            "hash-b",
            &[],
            ToolFailureKind::ExecutionError,
            "execution graph `run:9f8e7d` does not exist",
        );
        assert!(
            diagnosis.is_some(),
            "the same cause under a reworded input must still be recognized"
        );
        assert!(
            is_dispatch_blocked(turn, "task_review", "hash-b"),
            "the cause names an object the model did not send, so a reworded call \
             could not have succeeded either"
        );
    }

    /// Characterization (same run): the same input hash repeated across turns
    /// never accumulates, because the ledger is turn-scoped and `clear_turn` runs
    /// at each turn boundary. The live data shows a single input hash failing 12
    /// times over 19 turns with a maximum of 2 per turn - exactly one short of the
    /// bound, so no refusal was possible.
    #[test]
    fn the_same_cause_across_turns_never_accumulates() {
        let tool = "task_review";
        let input = "hash-cross-turn";
        let message = "execution graph `run:abc123` does not exist";
        for turn_index in 0..19 {
            let turn = format!("turn-cross-{turn_index}");
            record_tool_failure(&turn, tool, input, &[], ToolFailureKind::ExecutionError, message);
            assert!(
                !is_dispatch_blocked(&turn, tool, input),
                "characterized: turn {turn_index} sees only its own failure"
            );
        }
    }

    /// A transient failure can succeed on retry, so it must never buy a refusal.
    #[test]
    fn a_transient_failures_never_refuse_a_call() {
        let turn = turn!();
        for _ in 0..16 {
            record_tool_failure(turn, "task_review", "hash-t", &[], ToolFailureKind::Timeout, "request timed out");
        }
        assert!(!is_dispatch_blocked(turn, "task_review", "hash-t"));
    }

    /// A refusal must be per cause, never per tool.
    ///
    /// The distinction that makes this safe: a cause the model named in its own
    /// input is fixable by naming something else, so a different input must still
    /// be dispatched. A cause the model could not have named is a state condition,
    /// and that case is covered by the state-derived test above.
    #[test]
    fn a_refusal_is_per_cause_not_per_tool() {
        let turn = turn!();
        for _ in 0..8 {
            // No identifier in the message: the cause came from the input itself,
            // so a different input is a different call.
            record_tool_failure(
                turn,
                "task_review",
                "hash-a",
                &[],
                ToolFailureKind::ExecutionError,
                "invalid tool arguments\nmissing field `decision`",
            );
        }
        assert!(
            !is_dispatch_blocked(turn, "task_review", "hash-b"),
            "a different input is a different call"
        );
        assert!(
            !is_dispatch_blocked(turn, "task_submit", "hash-a"),
            "a different tool is a different call"
        );
    }

    /// A different reference the model names in its own input is a different call
    /// and must go through: the model can change what it names, so the call could
    /// succeed. A reference the model did not name is not its input to change.
    #[test]
    fn a_different_reference_named_in_the_input_must_still_be_dispatched() {
        let turn = turn!();
        for _ in 0..6 {
            record_tool_failure(
                turn,
                "task_review",
                "ref-a",
                &["art_c6fa4a80c9334e14ad4a5a1d34ebb888".to_string()],
                ToolFailureKind::ExecutionError,
                "artifact `art_c6fa4a80c9334e14ad4a5a1d34ebb888` does not exist",
            );
        }
        assert!(
            !is_dispatch_blocked(turn, "task_review", "ref-b"),
            "the model changed what it names, so the call could succeed"
        );
    }

    /// E2.2 - pollers legitimately re-issue identical calls while waiting on
    /// external progress (`host_backend.rs:2876-2879` exempts them from the loop
    /// guard). The breaker has no matching exemption, so a poller that fails twice
    /// with an ExecutionError is refused today. RED until the exemption is added.
    #[test]
    fn a_poller_must_never_be_refused() {
        let turn = turn!();
        for _ in 0..6 {
            record_tool_failure(
                turn,
                "process_get_result",
                "poll-hash",
                &[],
                ToolFailureKind::ExecutionError,
                "worker handle `run:9f8e7d` does not exist yet",
            );
        }
        assert!(
            !is_dispatch_blocked(turn, "process_get_result", "poll-hash"),
            "waiting on external progress is not a loop"
        );
    }

    /// E2.3 - the closure tools are the model's last exit. The diagnosis already
    /// directs the model to `objective_complete_request with blocked=true`
    /// (`repetition_guard.rs:198`); if that call itself can be refused, the model
    /// has no exit and the turn deadlocks. RED until the exemption is added.
    #[test]
    fn the_closure_tools_must_never_be_refused() {
        let turn = turn!();
        for _ in 0..6 {
            record_tool_failure(
                turn,
                "objective_complete_request",
                "closure-hash",
                &[],
                ToolFailureKind::ExecutionError,
                "goal `goal:run:4f9a2b` does not exist",
            );
        }
        assert!(
            !is_dispatch_blocked(turn, "objective_complete_request", "closure-hash"),
            "blocking the exit path is a deadlock, not a guard"
        );
    }

    #[test]
    fn durable_progress_clears_the_breaker() {
        // This test owns a turn nobody else uses, because it is the one test that
        // clears - and clearing is scoped to a turn.
        const TURN: &str = "turn-clearing-case";
        let fail = |msg: &str| {
            record_tool_failure(TURN, "task_review", "h1", &[], ToolFailureKind::ExecutionError, msg)
        };
        // Real ids always contain a digit, which is what marks them as ids.
        fail("graph `run:4f9a2b` does not exist");
        assert!(fail("graph `run:7c1e08` does not exist").is_some());
        clear_turn(TURN);
        assert!(
            fail("graph `run:9b3d55` does not exist").is_none(),
            "after progress the same call must be attempted again"
        );
    }

    /// Two different turns must not share one budget: a failure observed under
    /// one plan says nothing about the next turn, which replans anyway.
    #[test]
    fn different_turns_do_not_share_a_budget() {
        record_tool_failure(turn!(), "task_review", "h1", &[], ToolFailureKind::ExecutionError, "graph `run:4f9a2b` gone");
        assert!(
            record_tool_failure("turn-other", "task_review", "h1", &[], ToolFailureKind::ExecutionError, "graph `run:7c1e08` gone").is_none(),
            "a later turn must not inherit an earlier turn's failures"
        );
    }

    /// Two different tools failing must not share one budget.
    #[test]
    fn different_tools_do_not_share_a_budget() {
        record_tool_failure(turn!(), "read_ref", "h", &[], ToolFailureKind::ExecutionError, "bad id");
        assert!(
            record_tool_failure(turn!(), "task_review", "h", &[], ToolFailureKind::ExecutionError, "bad id").is_none(),
            "one tool's failures must not block another's"
        );
    }

    /// A blocked call must be told what the framework actually rejected. The
    /// breaker can only redirect work if it names the obstacle; "change the
    /// input" is useless to a model that cannot see which input was wrong.
    #[test]
    fn the_diagnosis_quotes_the_failure_it_blocks() {
        let turn = turn!();
        let message = "invalid tool arguments\nmissing field `path` at line 1 column 3";
        assert!(err(turn, ToolFailureKind::ExecutionError, message).is_none());
        let tripped = err(turn, ToolFailureKind::ExecutionError, message)
            .expect("the second identical failure trips the breaker");
        assert!(
            tripped.contains("Framework reported: invalid tool arguments"),
            "the diagnosis must repeat the framework's own words: {tripped}"
        );
    }
}
