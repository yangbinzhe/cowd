//! The judgment layer's data shape: a mission verdict, the per-criterion
//! judgments inside it, and the one derivation that turns them into a decision.
//!
//! A mission verdict answers, per published obligation, whether that obligation
//! is satisfied and which evidence backs the answer. The decision a reviewer
//! reports is *derived* from that verdict and never authored beside it: the old
//! three-value decision stays available as a projection so an older envelope
//! still parses, but it stops being a second source of truth.
//!
//! Three rules shape everything here:
//!   * the judged key set is exactly the published obligation key set - a
//!     reviewer may not invent, split, or paraphrase criteria into existence;
//!   * the derivation is total and deterministic - mixed verdicts resolve by a
//!     fixed precedence, and an empty criterion set is a structural error
//!     rather than a vacuous pass;
//!   * a verdict that leaves work outstanding must say what to do about it, so
//!     a decision never arrives without a way out.

use crate::agent_action::{CriterionSource, ObjectiveReviewDecision, TaskReviewDecision};
use serde::{Deserialize, Serialize};
use schemars::JsonSchema;
use std::collections::BTreeSet;

/// How one published obligation was judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Satisfaction {
    Satisfied,
    Unsatisfied,
    /// The evidence available does not decide it. This is a claim about
    /// evidence, not a verdict about the work.
    Indeterminate,
}

/// One criterion's judgment, with the evidence that backs it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CriterionVerdict {
    /// The published obligation key this judgment is about.
    pub criterion: String,
    /// Where the criterion came from. An agent-invented criterion may never
    /// stand in for an operator requirement.
    pub criterion_source: CriterionSource,
    pub satisfaction: Satisfaction,
    /// Durable references to the evidence this answer rests on.
    #[serde(default)]
    pub evidence_ptrs: Vec<String>,
    /// Reported confidence in basis points (0-10000). It is a report and never
    /// an input to the decision: a low-confidence answer is still an answer.
    #[serde(default)]
    pub confidence_bp: Option<u32>,
}

/// A named gap between what was required and what was found.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Gap {
    pub criterion: String,
    pub reason_code: String,
    /// What would close the gap.
    pub remediation: String,
}

/// An instruction for the next attempt, bound to the criterion it addresses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DirectedAction {
    pub criterion: String,
    pub action: String,
}

/// The whole judgment: per criterion, plus the gaps and instructions that
/// make an unfinished judgment actionable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MissionVerdict {
    pub per_criterion: Vec<CriterionVerdict>,
    #[serde(default)]
    pub gaps: Vec<Gap>,
    #[serde(default)]
    pub directed_actions: Vec<DirectedAction>,
}

/// Why a mission verdict cannot become a decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerdictDerivationError {
    /// The verdict judges nothing, or there are no published obligations to
    /// judge. An empty criterion set is a structural error, never a pass.
    EmptyCriteria,
    /// The verdict judged a different key set than the published obligations.
    CriterionSetMismatch {
        expected: Vec<String>,
        found: Vec<String>,
    },
    /// A criterion that is not satisfied carries neither an instruction nor a
    /// gap with a remediation, so the decision would have no way out.
    MissingRemediation { criterion: String },
}

/// The decision a mission verdict implies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DerivedDecision {
    Accept,
    Rework,
    /// The evidence does not decide it. The remedy is to collect, not to judge.
    Indeterminate,
}

/// The legacy decision an older envelope carries, and whether projecting it
/// loses anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyProjection<T> {
    pub decision: T,
    /// Named when the projection cannot carry the derived meaning. A task-level
    /// verdict with no evidence has no legacy member for "cannot decide", so the
    /// projection says so instead of pretending.
    pub lossy_note: Option<String>,
}

/// Derive the decision from the verdict and the published obligation key set.
///
/// The five rules, in order:
///   1. both sides must be non-empty (an empty set is a structural error);
///   2. the judged key set must equal the published key set;
///   3. mixed verdicts resolve by the fixed precedence
///      unsatisfied > indeterminate > satisfied;
///   4. every criterion left unfinished must carry an instruction or a gap
///      with a remediation, so a decision never arrives without a way out;
///   5. the derivation is total: it returns a decision or a named error, never
///      a default.
pub fn derive_decision(
    verdict: &MissionVerdict,
    obligation_keys: &[String],
) -> Result<DerivedDecision, VerdictDerivationError> {
    if obligation_keys.is_empty() || verdict.per_criterion.is_empty() {
        return Err(VerdictDerivationError::EmptyCriteria);
    }
    let expected: BTreeSet<&str> = obligation_keys.iter().map(String::as_str).collect();
    let found: BTreeSet<&str> = verdict
        .per_criterion
        .iter()
        .map(|criterion| criterion.criterion.as_str())
        .collect();
    if expected != found {
        return Err(VerdictDerivationError::CriterionSetMismatch {
            expected: obligation_keys.to_vec(),
            found: found.into_iter().map(str::to_string).collect(),
        });
    }
    // 032 K4c (G3b B1): a way out must carry CONTENT. A gap whose remediation
    // is blank, or an action whose instruction is blank, names the criterion
    // but says nothing to do - that is not an exit, so it does not count here
    // and the criterion falls through to MissingRemediation below.
    let instructed: BTreeSet<&str> = verdict
        .directed_actions
        .iter()
        .filter(|action| !action.action.trim().is_empty())
        .map(|action| action.criterion.as_str())
        .chain(
            verdict
                .gaps
                .iter()
                .filter(|gap| !gap.remediation.trim().is_empty())
                .map(|gap| gap.criterion.as_str()),
        )
        .collect();
    for criterion in &verdict.per_criterion {
        if criterion.satisfaction == Satisfaction::Satisfied {
            continue;
        }
        if !instructed.contains(criterion.criterion.as_str()) {
            return Err(VerdictDerivationError::MissingRemediation {
                criterion: criterion.criterion.clone(),
            });
        }
    }
    let unsatisfied = verdict
        .per_criterion
        .iter()
        .any(|criterion| criterion.satisfaction == Satisfaction::Unsatisfied);
    if unsatisfied {
        return Ok(DerivedDecision::Rework);
    }
    let indeterminate = verdict
        .per_criterion
        .iter()
        .any(|criterion| criterion.satisfaction == Satisfaction::Indeterminate);
    if indeterminate {
        return Ok(DerivedDecision::Indeterminate);
    }
    Ok(DerivedDecision::Accept)
}

/// What the root evaluator concluded about one obligation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvaluatorCriterionVerdict {
    Satisfied,
    Unsatisfied,
}

/// Fold the root evaluator's conclusions into the verdict.
///
/// The evaluator is the only signer of the final verdict, so its conclusion
/// wins where the two disagree. The reviewer's per-criterion claim stays
/// visible in `per_criterion` as written; only the satisfaction used for the
/// decision is the evaluator's. A conflict is therefore never silently
/// resolved - the two records differ and both are durable.
pub fn apply_evaluator_verdicts(
    verdict: &MissionVerdict,
    evaluator: &std::collections::BTreeMap<String, EvaluatorCriterionVerdict>,
) -> MissionVerdict {
    let mut folded = verdict.clone();
    for criterion in &mut folded.per_criterion {
        if let Some(conclusion) = evaluator.get(&criterion.criterion) {
            criterion.satisfaction = match conclusion {
                EvaluatorCriterionVerdict::Satisfied => Satisfaction::Satisfied,
                EvaluatorCriterionVerdict::Unsatisfied => Satisfaction::Unsatisfied,
            };
        }
    }
    folded
}

/// Project the derived decision onto the legacy task-level decision.
pub fn project_task_review_decision(
    decision: DerivedDecision,
) -> LegacyProjection<TaskReviewDecision> {
    match decision {
        DerivedDecision::Accept => LegacyProjection {
            decision: TaskReviewDecision::Accept,
            lossy_note: None,
        },
        DerivedDecision::Rework => LegacyProjection {
            decision: TaskReviewDecision::Rework,
            lossy_note: None,
        },
        DerivedDecision::Indeterminate => LegacyProjection {
            decision: TaskReviewDecision::Challenge,
            lossy_note: Some(
                "task review has no legacy member for an undecided criterion; Challenge carries it as a proxy"
                    .to_string(),
            ),
        },
    }
}

/// Project the derived decision onto the legacy objective-level decision.
pub fn project_objective_review_decision(
    decision: DerivedDecision,
) -> LegacyProjection<ObjectiveReviewDecision> {
    match decision {
        DerivedDecision::Accept => LegacyProjection {
            decision: ObjectiveReviewDecision::Satisfied,
            lossy_note: None,
        },
        DerivedDecision::Rework => LegacyProjection {
            decision: ObjectiveReviewDecision::Gap,
            lossy_note: None,
        },
        DerivedDecision::Indeterminate => LegacyProjection {
            decision: ObjectiveReviewDecision::Blocked,
            lossy_note: None,
        },
    }
}

/// One item of the review input pack: a reference plus the digest that fixes
/// it. The pack carries no payload, so its size is bounded by construction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReviewInputItem {
    pub item_ref: String,
    pub digest: String,
    /// The cost of rendering this item into the pack.
    pub bytes: usize,
}

/// The review input pack: the criteria under judgment and the evidence pointers
/// that back them, with the byte accounting the context budget is measured
/// against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReviewInputPack {
    pub criteria: Vec<String>,
    pub items: Vec<ReviewInputItem>,
    pub total_bytes: usize,
    pub ceiling_bytes: usize,
}

/// What the pack builder decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReviewInputPackOutcome {
    Ready(ReviewInputPack),
    /// The pack does not fit the context budget. The reviewer is told what to
    /// collect instead: a named instruction per dropped item, never a silent
    /// truncation and never a refusal of the whole review.
    OverBudget {
        over_by_bytes: usize,
        collect_instructions: Vec<String>,
    },
}

/// Build the review input pack under the byte ceiling the context budget
/// allows.
///
/// The ceiling is the caller's to source from the context budget plan (the
/// budget number is the plan's; the conversion between the plan's unit and
/// bytes belongs where the plan is read). The builder deliberately takes no
/// budget of its own: a second, private ceiling is the defect the capability
/// ledger calls an unwired capability.
///
/// An item that alone exceeds the per-item ceiling is not carried at all: it
/// becomes a named collect instruction. When the whole pack exceeds the
/// ceiling, the outcome is `OverBudget` with the instructions, because a
/// partially truncated pack would let a reviewer judge against evidence it
/// cannot see.
pub fn build_review_input_pack(
    criteria: Vec<String>,
    items: Vec<ReviewInputItem>,
    ceiling_bytes: usize,
    per_item_ceiling_bytes: usize,
) -> ReviewInputPackOutcome {
    let mut kept = Vec::new();
    let mut collect_instructions = Vec::new();
    let mut total_bytes = 0usize;
    for item in items {
        if item.bytes > per_item_ceiling_bytes {
            collect_instructions.push(format!(
                "collect evidence for {} ({} bytes exceeds the per-item ceiling {} bytes)",
                item.item_ref, item.bytes, per_item_ceiling_bytes
            ));
            continue;
        }
        if total_bytes.saturating_add(item.bytes) > ceiling_bytes {
            collect_instructions.push(format!(
                "collect evidence for {} (the review input pack exceeds its {} byte ceiling)",
                item.item_ref, ceiling_bytes
            ));
            continue;
        }
        total_bytes = total_bytes.saturating_add(item.bytes);
        kept.push(item);
    }
    let pack = ReviewInputPack {
        criteria,
        items: kept,
        total_bytes,
        ceiling_bytes,
    };
    if total_bytes > ceiling_bytes {
        return ReviewInputPackOutcome::OverBudget {
            over_by_bytes: total_bytes.saturating_sub(ceiling_bytes),
            collect_instructions,
        };
    }
    if !collect_instructions.is_empty() {
        return ReviewInputPackOutcome::OverBudget {
            over_by_bytes: 0,
            collect_instructions,
        };
    }
    ReviewInputPackOutcome::Ready(pack)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn criterion(name: &str, satisfaction: Satisfaction) -> CriterionVerdict {
        CriterionVerdict {
            criterion: name.to_string(),
            criterion_source: CriterionSource::OperatorDerived,
            satisfaction,
            evidence_ptrs: vec![format!("artifact:{name}")],
            confidence_bp: Some(9_000),
        }
    }

    fn verdict(per: Vec<CriterionVerdict>) -> MissionVerdict {
        MissionVerdict {
            per_criterion: per,
            gaps: Vec::new(),
            directed_actions: Vec::new(),
        }
    }

    /// B12 rule 2: the judged key set is exactly the published obligation set.
    /// A reviewer may not invent a criterion, and may not judge a subset.
    #[test]
    fn the_judged_key_set_must_equal_the_published_obligation_set() {
        let keys = vec!["one".to_string(), "two".to_string()];
        let ok = verdict(vec![
            criterion("one", Satisfaction::Satisfied),
            criterion("two", Satisfaction::Satisfied),
        ]);
        assert_eq!(derive_decision(&ok, &keys), Ok(DerivedDecision::Accept));
        let invented = verdict(vec![
            criterion("one", Satisfaction::Satisfied),
            criterion("three", Satisfaction::Satisfied),
        ]);
        assert!(matches!(
            derive_decision(&invented, &keys),
            Err(VerdictDerivationError::CriterionSetMismatch { .. })
        ));
    }

    /// B12 rule 1: an empty criterion set is a structural error, never a pass.
    #[test]
    fn an_empty_criterion_set_is_a_structural_error() {
        assert_eq!(
            derive_decision(&verdict(Vec::new()), &["one".to_string()]),
            Err(VerdictDerivationError::EmptyCriteria)
        );
        assert_eq!(
            derive_decision(&verdict(vec![criterion("one", Satisfaction::Satisfied)]), &[]),
            Err(VerdictDerivationError::EmptyCriteria)
        );
    }

    /// B12 rule 3: mixed verdicts resolve by a fixed precedence.
    #[test]
    fn mixed_verdicts_resolve_by_a_fixed_precedence() {
        let keys = vec!["one".to_string(), "two".to_string()];
        let mixed = MissionVerdict {
            per_criterion: vec![
                criterion("one", Satisfaction::Satisfied),
                criterion("two", Satisfaction::Indeterminate),
            ],
            gaps: vec![Gap {
                criterion: "two".to_string(),
                reason_code: "evidence_missing".to_string(),
                remediation: "collect the missing read receipt".to_string(),
            }],
            directed_actions: Vec::new(),
        };
        assert_eq!(derive_decision(&mixed, &keys), Ok(DerivedDecision::Indeterminate));
        let unsatisfied = MissionVerdict {
            per_criterion: vec![
                criterion("one", Satisfaction::Satisfied),
                criterion("two", Satisfaction::Unsatisfied),
            ],
            gaps: vec![Gap {
                criterion: "two".to_string(),
                reason_code: "not_satisfied".to_string(),
                remediation: "produce the missing artifact".to_string(),
            }],
            directed_actions: Vec::new(),
        };
        assert_eq!(
            derive_decision(&unsatisfied, &keys),
            Ok(DerivedDecision::Rework)
        );
    }

    /// B12 rule 4 at CONTENT level (032 K4c/G3b B1): an exit that only names
    /// the criterion while its remediation/action text is blank is no way out.
    #[test]
    fn a_blank_remedy_names_the_criterion_without_offering_a_way_out() {
        let keys = vec!["one".to_string()];
        let blank_gap = MissionVerdict {
            per_criterion: vec![criterion("one", Satisfaction::Unsatisfied)],
            gaps: vec![Gap {
                criterion: "one".to_string(),
                reason_code: "not_satisfied".to_string(),
                remediation: "   ".to_string(),
            }],
            directed_actions: Vec::new(),
        };
        assert_eq!(
            derive_decision(&blank_gap, &keys),
            Err(VerdictDerivationError::MissingRemediation {
                criterion: "one".to_string()
            })
        );
        let blank_action = MissionVerdict {
            per_criterion: vec![criterion("one", Satisfaction::Unsatisfied)],
            gaps: Vec::new(),
            directed_actions: vec![DirectedAction {
                criterion: "one".to_string(),
                action: "".to_string(),
            }],
        };
        assert_eq!(
            derive_decision(&blank_action, &keys),
            Err(VerdictDerivationError::MissingRemediation {
                criterion: "one".to_string()
            })
        );
    }

    /// B12 rule 4 with the exit law: a verdict that leaves work unfinished must
    /// say what to do about it, or the decision has no way out.
    #[test]
    fn an_unfinished_criterion_without_a_remedy_has_no_way_out() {
        let keys = vec!["one".to_string()];
        let unfinished = verdict(vec![criterion("one", Satisfaction::Unsatisfied)]);
        assert_eq!(
            derive_decision(&unfinished, &keys),
            Err(VerdictDerivationError::MissingRemediation {
                criterion: "one".to_string()
            })
        );
        let instructed = MissionVerdict {
            per_criterion: vec![criterion("one", Satisfaction::Unsatisfied)],
            gaps: Vec::new(),
            directed_actions: vec![DirectedAction {
                criterion: "one".to_string(),
                action: "produce the missing artifact".to_string(),
            }],
        };
        assert_eq!(derive_decision(&instructed, &keys), Ok(DerivedDecision::Rework));
    }

    /// CR1: the root evaluator signs the final verdict, so its conclusion wins
    /// where the two disagree, and the reviewer's own claim stays durable.
    #[test]
    fn the_evaluator_signs_where_the_reviewer_and_it_disagree() {
        let reviewer = verdict(vec![criterion("one", Satisfaction::Satisfied)]);
        let mut evaluator = std::collections::BTreeMap::new();
        evaluator.insert("one".to_string(), EvaluatorCriterionVerdict::Unsatisfied);
        let folded = apply_evaluator_verdicts(&reviewer, &evaluator);
        assert_eq!(
            folded.per_criterion[0].satisfaction,
            Satisfaction::Unsatisfied,
            "the evaluator's conclusion is the one the decision uses"
        );
        assert_eq!(
            reviewer.per_criterion[0].satisfaction,
            Satisfaction::Satisfied,
            "the reviewer's own claim is still what it said"
        );
    }

    /// B12 rule 5: the legacy three-value decision survives as a projection, and
    /// a projection that cannot carry the meaning says so.
    #[test]
    fn the_legacy_decision_survives_as_a_projection() {
        let task = project_task_review_decision(DerivedDecision::Accept);
        assert_eq!(task.decision, TaskReviewDecision::Accept);
        assert!(task.lossy_note.is_none());
        let task_gap = project_task_review_decision(DerivedDecision::Rework);
        assert_eq!(task_gap.decision, TaskReviewDecision::Rework);
        let task_undecided = project_task_review_decision(DerivedDecision::Indeterminate);
        assert_eq!(task_undecided.decision, TaskReviewDecision::Challenge);
        assert!(
            task_undecided
                .lossy_note
                .as_ref()
                .is_some_and(|note| note.contains("no legacy member")),
            "a lossy projection has to name what it lost"
        );
        let objective = project_objective_review_decision(DerivedDecision::Indeterminate);
        assert_eq!(objective.decision, ObjectiveReviewDecision::Blocked);
        assert!(objective.lossy_note.is_none());
    }

    /// B4: the pack is refs plus digests, and an over-budget pack produces named
    /// collect instructions rather than a silent truncation or a refusal.
    #[test]
    fn the_review_input_pack_is_bounded_and_never_silently_truncated() {
        let item = |name: &str, bytes: usize| ReviewInputItem {
            item_ref: format!("artifact:{name}"),
            digest: format!("sha256:{name}"),
            bytes,
        };
        let ready = build_review_input_pack(
            vec!["one".to_string()],
            vec![item("one", 100), item("two", 100)],
            1_000,
            500,
        );
        match ready {
            ReviewInputPackOutcome::Ready(pack) => {
                assert_eq!(pack.items.len(), 2);
                assert_eq!(pack.total_bytes, 200);
                assert_eq!(pack.ceiling_bytes, 1_000);
            }
            other => panic!("the pack fits: {other:?}"),
        }
        let oversized_item = build_review_input_pack(
            vec!["one".to_string()],
            vec![item("huge", 900)],
            1_000,
            500,
        );
        match oversized_item {
            ReviewInputPackOutcome::OverBudget {
                collect_instructions,
                ..
            } => assert!(
                collect_instructions
                    .iter()
                    .any(|instruction| instruction.contains("artifact:huge")),
                "{collect_instructions:?}"
            ),
            other => panic!("an oversized item must not ride along: {other:?}"),
        }
        let over_budget = build_review_input_pack(
            vec!["one".to_string()],
            vec![item("one", 400), item("two", 400), item("three", 400)],
            1_000,
            500,
        );
        match over_budget {
            ReviewInputPackOutcome::OverBudget {
                over_by_bytes,
                collect_instructions,
            } => {
                assert!(over_by_bytes > 0 || !collect_instructions.is_empty());
                assert!(!collect_instructions.is_empty());
            }
            other => panic!("an over-budget pack must be named: {other:?}"),
        }
    }
}
