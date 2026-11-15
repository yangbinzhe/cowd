//! 034 GB-1: one registration gate for the facts the evidence record admits.
//!
//! The extent family used to be eight separate gates, each with its own
//! wording and its own idea of what a fact needed. A read that satisfied
//! one gate could be refused by the next, and a refusal named only the gate
//! that happened to speak last - so the reader learned one missing piece per
//! attempt and kept ping-ponging.
//!
//! This module is the one place that answers "may this fact enter the
//! evidence record?". It asks its proof strategies in a fixed order, records
//! which one admitted the fact, and - when none did - refuses with a
//! complete diagnosis: what each strategy was missing. The fragmented gates'
//! original wording stays verbatim at the head of every envelope, so the
//! history of a refusal remains readable and countable.

use harness_contract::refusal_codes::{FactAdmissionCode, FactAdmissionStrategy};

/// What one proof strategy could not establish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StrategyGap {
    /// The strategy that was asked.
    pub strategy: FactAdmissionStrategy,
    /// What that strategy was missing, in its own words.
    pub missing: String,
}

/// The outcome of one proof strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StrategyOutcome {
    /// The strategy that was asked.
    pub strategy: FactAdmissionStrategy,
    /// Whether this strategy admitted the fact.
    pub admitted: bool,
    /// What this strategy was missing (empty when it admitted).
    pub missing: String,
}

impl StrategyOutcome {
    /// A strategy that established the fact.
    pub(crate) fn admitted(strategy: FactAdmissionStrategy) -> Self {
        Self { strategy, admitted: true, missing: String::new() }
    }

    /// A strategy that could not.
    pub(crate) fn missing(strategy: FactAdmissionStrategy, missing: impl Into<String>) -> Self {
        Self { strategy, admitted: false, missing: missing.into() }
    }
}

/// The gate's verdict on one fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FactAdmission {
    /// The code of the judgement.
    pub code: FactAdmissionCode,
    /// The fragmented gate's original wording, verbatim.
    pub original_code: String,
    /// The strategy that admitted the fact (none on a refusal).
    pub strategy: Option<FactAdmissionStrategy>,
    /// What every strategy was missing, in the fixed order.
    pub strategy_gaps: Vec<StrategyGap>,
}

impl FactAdmission {
    /// The refusal envelope: the original wording first, then the code, then
    /// the complete diagnosis.
    ///
    /// The original wording comes first and is not rewritten, so a reader (or
    /// a counter) that greps for the old string still finds it.
    pub(crate) fn refusal_message(&self) -> String {
        let gaps = self
            .strategy_gaps
            .iter()
            .map(|gap| format!("{}(missing={})", gap.strategy.code(), gap.missing))
            .collect::<Vec<_>>()
            .join("; ");
        format!(
            "{} [code={}; strategies={}]",
            self.original_code,
            self.code.code(),
            gaps
        )
    }
}

#[cfg(test)]
thread_local! {
    static TEST_ADMISSIONS: std::cell::RefCell<Vec<FactAdmissionStrategy>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) fn take_test_admissions() -> Vec<FactAdmissionStrategy> {
    TEST_ADMISSIONS.with(|seen| std::mem::take(&mut *seen.borrow_mut()))
}

/// The one gate: ask the proof strategies in the fixed order, and say which
/// one admitted the fact - or, when none did, what each of them was missing.
///
/// The order is fixed (`nested` → `discovery` → `immutable`) and is recorded
/// in the verdict, so "which strategy should have proved it" is answerable
/// from the refusal alone.
pub(crate) fn admit(
    outcomes: &[StrategyOutcome],
    refusal_code: FactAdmissionCode,
    original_code: &str,
) -> FactAdmission {
    let mut ordered = outcomes.to_vec();
    ordered.sort_by_key(|outcome| outcome.strategy.order());
    if let Some(admitted) = ordered.iter().find(|outcome| outcome.admitted) {
        #[cfg(test)]
        TEST_ADMISSIONS.with(|seen| seen.borrow_mut().push(admitted.strategy));
        return FactAdmission {
            code: FactAdmissionCode::Admitted,
            original_code: original_code.to_string(),
            strategy: Some(admitted.strategy),
            strategy_gaps: Vec::new(),
        };
    }
    FactAdmission {
        code: refusal_code,
        original_code: original_code.to_string(),
        strategy: None,
        strategy_gaps: ordered
            .into_iter()
            .map(|outcome| StrategyGap { strategy: outcome.strategy, missing: outcome.missing })
            .collect(),
    }
}

/// The same judgment as `admit`, exposed as a control-flow gate for the
/// original fact owner. The decision is transient and does not change the
/// persisted proof representation or confer authority of its own.
pub(crate) fn require(
    outcomes: &[StrategyOutcome],
    refusal_code: FactAdmissionCode,
    original_code: &str,
) -> Result<FactAdmission, String> {
    let decision = admit(outcomes, refusal_code, original_code);
    if decision.code != FactAdmissionCode::Admitted || decision.strategy.is_none() {
        return Err(decision.refusal_message());
    }
    Ok(decision)
}

/// The refusal envelope of a gate that judged one fact without asking a
/// strategy (the strategy's own refusal, carried with its original wording).
pub(crate) fn refusal(
    code: FactAdmissionCode,
    original_code: &str,
    gaps: &[StrategyGap],
) -> String {
    FactAdmission {
        code,
        original_code: original_code.to_string(),
        strategy: None,
        strategy_gaps: gaps.to_vec(),
    }
    .refusal_message()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 034 GB-1 batch 1 acceptance: the fragmented gate's original wording
    /// stays verbatim inside the gate's envelope, and the envelope carries a
    /// complete diagnosis - what each of the three strategies was missing.
    #[test]
    fn s8_the_original_wording_stays_verbatim_and_the_diagnosis_is_complete() {
        let original = "proof_actual_input_extent_unsupported: actual read lacks an original physical extent";
        let admission = admit(
            &[
                StrategyOutcome::missing(
                    FactAdmissionStrategy::Nested,
                    "no composite child acquisitions cover this read",
                ),
                StrategyOutcome::missing(
                    FactAdmissionStrategy::Discovery,
                    "the read is not a discovery page",
                ),
                StrategyOutcome::missing(
                    FactAdmissionStrategy::Immutable,
                    "the read named a workspace path and its receipt carries no acquisition seal",
                ),
            ],
            FactAdmissionCode::Unproven,
            original,
        );
        let message = admission.refusal_message();
        assert!(
            message.starts_with(original),
            "the original wording stays verbatim at the head: {message}"
        );
        assert!(message.contains("code=fact_unproven"), "{message}");
        for strategy in ["nested", "discovery", "immutable"] {
            assert!(
                message.contains(&format!("{strategy}(missing=")),
                "the diagnosis names every strategy: {message}"
            );
        }
    }

    /// 034 GB-1: the strategy order is fixed and recorded.
    #[test]
    fn s8_the_strategy_order_is_fixed_and_recorded() {
        let admission = admit(
            &[
                StrategyOutcome::missing(FactAdmissionStrategy::Immutable, "no extent"),
                StrategyOutcome::admitted(FactAdmissionStrategy::Discovery),
                StrategyOutcome::missing(FactAdmissionStrategy::Nested, "no children"),
            ],
            FactAdmissionCode::Unproven,
            "original",
        );
        assert_eq!(admission.code, FactAdmissionCode::Admitted);
        assert_eq!(admission.strategy, Some(FactAdmissionStrategy::Discovery));

        let refused = admit(
            &[
                StrategyOutcome::missing(FactAdmissionStrategy::Immutable, "no extent"),
                StrategyOutcome::missing(FactAdmissionStrategy::Discovery, "no page"),
                StrategyOutcome::missing(FactAdmissionStrategy::Nested, "no children"),
            ],
            FactAdmissionCode::Unproven,
            "original",
        );
        let order = refused
            .strategy_gaps
            .iter()
            .map(|gap| gap.strategy.code())
            .collect::<Vec<_>>();
        assert_eq!(order, vec!["nested", "discovery", "immutable"]);
    }
}
