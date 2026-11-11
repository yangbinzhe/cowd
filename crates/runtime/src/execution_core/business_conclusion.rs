//! The single authority that derives a business terminal conclusion.
//!
//! A model can present any terminal wording it likes, including a successful
//! one.  Only durable Runtime facts may decide the business verdict of an
//! execution, and this module is the one place where that decision is made.
//!
//! The two facts it consumes are:
//!
//! * the Runtime's own committed terminal conclusion (the goal/review
//!   terminal the Runtime itself committed for this execution), and
//! * the durable obligation ledger (the acceptance evaluation and its
//!   unresolved obligation ids).
//!
//! The model's terminal wording is carried through as `reason` so a terminal
//! presentation is never lost, but it never reaches the verdict.

use harness_contract::acceptance::AcceptanceVerdict;
use harness_contract::outcome::{
    VerifiedBusinessBasis, VerifiedBusinessOutcome, VerifiedBusinessVerdict,
};
use serde::{Deserialize, Serialize};

/// The Runtime's own committed terminal conclusion for one execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GoalConclusionFact {
    /// The Runtime committed a successful terminal conclusion for this
    /// execution.  It is a Runtime decision, not a model sentence.
    Satisfied,
    /// The Runtime committed a non-successful terminal conclusion.
    NotSatisfied,
}

/// The durable obligation ledger of one execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObligationLedgerFact {
    /// No obligation contract exists for this execution, so no obligation is
    /// outstanding.  This is the same reading the canonical acceptance
    /// evaluator uses for an empty contract.
    NoContract,
    /// A durable acceptance evaluation exists for this execution.
    Evaluated {
        verdict: AcceptanceVerdict,
        unresolved_obligation_ids: Vec<String>,
    },
    /// An obligation contract exists, but its evaluation did not arrive yet.
    /// The business conclusion is Unknown: no terminal value may be claimed.
    Unresolved,
}

/// The durable facts one business conclusion is derived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessConclusionFacts {
    /// The terminal wording of the execution, preserved verbatim.
    pub model_reason: String,
    /// The Runtime's committed terminal conclusion.
    pub goal: GoalConclusionFact,
    /// Durable reference of that committed conclusion.
    pub goal_reference: String,
    /// The durable obligation ledger.
    pub ledger: ObligationLedgerFact,
}

/// Derive the verified business conclusion from durable facts.
///
/// * A successful terminal conclusion needs a successful Runtime conclusion
///   and an obligation ledger without anything outstanding.
/// * An obligation ledger with outstanding obligations is `Unsatisfied`, even
///   when the execution completed and even when the model announced success.
/// * An obligation ledger whose evaluation did not arrive is `Unknown`: the
///   conclusion is not claimed yet.
#[must_use]
pub fn derive_verified_business_conclusion(
    facts: &BusinessConclusionFacts,
) -> VerifiedBusinessOutcome {
    let mut basis = Vec::new();
    let verdict = match facts.goal {
        GoalConclusionFact::NotSatisfied => VerifiedBusinessVerdict::Unsatisfied,
        GoalConclusionFact::Satisfied => {
            basis.push(VerifiedBusinessBasis {
                kind: "goal_conclusion".to_string(),
                reference: facts.goal_reference.clone(),
            });
            match &facts.ledger {
                ObligationLedgerFact::NoContract => VerifiedBusinessVerdict::Satisfied,
                ObligationLedgerFact::Evaluated {
                    verdict,
                    unresolved_obligation_ids,
                } => {
                    basis.push(VerifiedBusinessBasis {
                        kind: "acceptance_evaluation".to_string(),
                        reference: acceptance_verdict_name(*verdict).to_string(),
                    });
                    if !unresolved_obligation_ids.is_empty() {
                        basis.push(VerifiedBusinessBasis {
                            kind: "obligation_ledger".to_string(),
                            reference: unresolved_obligation_ids.join(","),
                        });
                    }
                    if *verdict == AcceptanceVerdict::Satisfied
                        && unresolved_obligation_ids.is_empty()
                    {
                        VerifiedBusinessVerdict::Satisfied
                    } else {
                        VerifiedBusinessVerdict::Unsatisfied
                    }
                }
                ObligationLedgerFact::Unresolved => {
                    basis.push(VerifiedBusinessBasis {
                        kind: "obligation_ledger".to_string(),
                        reference: "evaluation_not_reached".to_string(),
                    });
                    VerifiedBusinessVerdict::Unknown
                }
            }
        }
    };
    VerifiedBusinessOutcome {
        verdict,
        reason: facts.model_reason.clone(),
        basis,
    }
}

/// The terminal class a verified conclusion allows.
///
/// A completion is a business success only when a durable fact verified it.
/// Every other completion is recorded as a typed `Unsatisfied` that keeps the
/// model's own wording, so a terminal presentation is never rewritten and a
/// success is never invented.
#[must_use]
pub fn terminal_class_for_completion(
    verified: &VerifiedBusinessOutcome,
    failed_tool_calls: u64,
    model_reason: &str,
) -> harness_contract::outcome::OutcomeTerminalClass {
    use harness_contract::outcome::OutcomeTerminalClass;
    match verified.verdict {
        VerifiedBusinessVerdict::Satisfied => {
            OutcomeTerminalClass::Succeeded(model_reason.to_string())
        }
        VerifiedBusinessVerdict::Unsatisfied | VerifiedBusinessVerdict::Unknown => {
            if failed_tool_calls > 0 {
                // A recovered tool failure is not a business failure, but it is
                // not a verified success either: keep the historical partial
                // failure class and the count in the reason.
                return OutcomeTerminalClass::PartialFailure(format!(
                    "{}; {} tool calls failed before terminal synthesis",
                    model_reason, failed_tool_calls
                ));
            }
            OutcomeTerminalClass::Unsatisfied(model_reason.to_string())
        }
    }
}

/// The durable name of an acceptance verdict.
#[must_use]
pub const fn acceptance_verdict_name(verdict: AcceptanceVerdict) -> &'static str {
    match verdict {
        AcceptanceVerdict::Satisfied => "satisfied",
        AcceptanceVerdict::Unsatisfied => "unsatisfied",
        AcceptanceVerdict::Unresolved => "unresolved",
        AcceptanceVerdict::FrameworkInvalid => "framework_invalid",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(goal: GoalConclusionFact, ledger: ObligationLedgerFact) -> BusinessConclusionFacts {
        BusinessConclusionFacts {
            model_reason: "the model said satisfied".to_string(),
            goal,
            goal_reference: "goal:turn-1".to_string(),
            ledger,
        }
    }

    #[test]
    fn verified_satisfied_needs_a_satisfied_ledger() {
        let satisfied = derive_verified_business_conclusion(&facts(
            GoalConclusionFact::Satisfied,
            ObligationLedgerFact::Evaluated {
                verdict: AcceptanceVerdict::Satisfied,
                unresolved_obligation_ids: Vec::new(),
            },
        ));
        assert_eq!(satisfied.verdict, VerifiedBusinessVerdict::Satisfied);
        assert_eq!(satisfied.reason, "the model said satisfied");
        assert!(satisfied
            .basis
            .iter()
            .any(|basis| basis.kind == "acceptance_evaluation"));
    }

    #[test]
    fn outstanding_obligations_refuse_a_successful_terminal() {
        let derived = derive_verified_business_conclusion(&facts(
            GoalConclusionFact::Satisfied,
            ObligationLedgerFact::Evaluated {
                verdict: AcceptanceVerdict::Satisfied,
                unresolved_obligation_ids: vec!["read:reports/q3.md".to_string()],
            },
        ));
        assert_eq!(derived.verdict, VerifiedBusinessVerdict::Unsatisfied);
        let terminal = terminal_class_for_completion(&derived, 0, "the model said satisfied");
        assert_eq!(
            terminal,
            harness_contract::outcome::OutcomeTerminalClass::Unsatisfied(
                "the model said satisfied".to_string()
            )
        );
    }

    #[test]
    fn a_missing_evaluation_is_unknown_and_never_success() {
        let derived = derive_verified_business_conclusion(&facts(
            GoalConclusionFact::Satisfied,
            ObligationLedgerFact::Unresolved,
        ));
        assert_eq!(derived.verdict, VerifiedBusinessVerdict::Unknown);
        let terminal = terminal_class_for_completion(&derived, 0, "satisfied");
        assert!(!terminal.is_success());
    }

    #[test]
    fn a_non_successful_runtime_conclusion_is_never_success() {
        let derived = derive_verified_business_conclusion(&facts(
            GoalConclusionFact::NotSatisfied,
            ObligationLedgerFact::NoContract,
        ));
        assert_eq!(derived.verdict, VerifiedBusinessVerdict::Unsatisfied);
    }
}
