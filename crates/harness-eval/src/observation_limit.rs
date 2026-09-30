//! PLAN D3: the evaluator observes; it does not judge.
//!
//! # Why
//!
//! The live runner used to declare a scenario failed purely on its own absolute
//! wait, with a message shaped like:
//!
//! ```text
//! timed out after 600353ms waiting for a durable assistant message;
//! explicit absolute scenario safety wait=600000ms, phase=stalled,
//! last_active_phase=calling_tool, ...
//! ```
//!
//! That reads as "the turn produced nothing", which is wrong in both observed
//! cases:
//!
//! * **arm C** advanced queued -> preparing_context -> calling_model ->
//!   calling_tool -> calling_model over revisions 1..27 with 122 timeline events,
//!   and only ONE durable message existed. It was still working when the window
//!   expired.
//! * **EXP-003 Facet A** logged 367 progress observations and 27.9MB of
//!   activity, again with no durable terminal message.
//!
//! In both cases the verdict came from a clock, not from the turn, so the report
//! could not say WHY. PLAN I1 is explicit that a wall clock must not be the
//! authority for terminal state, and the evaluator is the last place that
//! principle should be violated.
//!
//! # What changes
//!
//! The absolute wait is still enforced - it is the operator's opt-in cap and it
//! must stop runaway work - but it is now labelled exactly as what it is, and
//! the report carries the observable state so the reader can tell the three
//! cases apart without re-deriving them:
//!
//! * `verdict: observation_limit` - the operator's window expired while the turn
//!   still showed activity. Not a claim that the work failed.
//! * `activity` - whether the turn was still making observable progress in its
//!   final poll, so "still working" is distinguishable from "quiet".
//! * the pre-existing diagnostic fields, kept verbatim.

use serde_json::{json, Value};

/// Classify the evaluator's own limit expiry.
///
/// Deliberately NOT a verdict on the turn: it says the observation window ended,
/// nothing more. Whether the work failed is for the runtime's typed terminal
/// state (or the model itself) to say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationLimit {
    /// The operator's absolute scenario window expired while the turn was still
    /// producing observable activity.
    ExpiredWithActivity,
    /// The window expired and the turn had gone quiet before it did.
    ExpiredWhileQuiet,
    /// No absolute window was configured, so this cannot happen.
    NotConfigured,
}

impl ObservationLimit {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExpiredWithActivity => "observation_limit_reached_with_activity",
            Self::ExpiredWhileQuiet => "observation_limit_reached_while_quiet",
            Self::NotConfigured => "not_configured",
        }
    }

    /// Whether the turn was still working when the window closed.
    #[must_use]
    pub const fn still_active(self) -> bool {
        matches!(self, Self::ExpiredWithActivity)
    }
}

/// Build the report payload for an absolute-wait expiry.
///
/// `last_progress_age_ms` is how long before expiry the turn last showed
/// observable activity; `activity_events` is the number of observable activity
/// records seen in the final window.
#[must_use]
pub fn observation_limit_report(
    elapsed_ms: u128,
    absolute_wait_ms: Option<u128>,
    last_progress_age_ms: u128,
    activity_events: usize,
) -> Value {
    let limit = match absolute_wait_ms {
        None => ObservationLimit::NotConfigured,
        Some(_) if activity_events > 0 && last_progress_age_ms < 120_000 => {
            ObservationLimit::ExpiredWithActivity
        }
        Some(_) => ObservationLimit::ExpiredWhileQuiet,
    };
    json!({
        // The evaluator's own limit, never a claim about the work.
        "verdict": limit.as_str(),
        "authority": "evaluator_observation_window",
        "still_active_at_expiry": limit.still_active(),
        "elapsed_ms": elapsed_ms,
        "absolute_wait_ms": absolute_wait_ms,
        "last_progress_age_ms": last_progress_age_ms,
        "activity_events": activity_events,
        "note": "the observation window ended; the turn's own terminal state is the authority on whether the work failed",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The arm-C shape: activity right up to expiry. The report must NOT read as
    /// a failure of the work.
    #[test]
    fn an_active_turn_at_expiry_is_not_reported_as_a_failure() {
        let report = observation_limit_report(600_353, Some(600_000), 4_000, 122);
        assert_eq!(report["verdict"], "observation_limit_reached_with_activity");
        assert_eq!(report["still_active_at_expiry"], true);
        assert_eq!(report["activity_events"], 122);
        assert_eq!(report["authority"], "evaluator_observation_window");
    }

    /// The genuinely quiet shape.
    #[test]
    fn a_quiet_turn_at_expiry_is_distinguishable() {
        let report = observation_limit_report(600_353, Some(600_000), 700_000, 0);
        assert_eq!(report["verdict"], "observation_limit_reached_while_quiet");
        assert_eq!(report["still_active_at_expiry"], false);
    }

    /// An operator who configured no absolute window cannot hit this path.
    #[test]
    fn no_window_means_no_verdict() {
        let report = observation_limit_report(1_000, None, 0, 0);
        assert_eq!(report["verdict"], "not_configured");
        assert_eq!(report["still_active_at_expiry"], false);
    }

    /// The boundary: activity older than the two-minute look-back counts as
    /// quiet, so a long-stalled turn is not flattered by stale activity.
    #[test]
    fn stale_activity_does_not_count_as_active() {
        let report = observation_limit_report(600_353, Some(600_000), 130_000, 5);
        assert_eq!(report["verdict"], "observation_limit_reached_while_quiet");
    }
}
