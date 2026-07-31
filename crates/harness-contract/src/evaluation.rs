//! Immutable evaluation contracts shared by Agent and Team definitions.
//!
//! A metric contract is intentionally data, not a Gateway policy knob.  A
//! candidate is evaluated against the baseline contract that was persisted
//! before the candidate existed; changing an evaluation policy is therefore a
//! separate, human-governed change rather than a way to make a candidate pass.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::agent::definition::validate_reference;
use crate::agent::ValidationError;
use crate::policy::PermissionMode;
use crate::reality::EvidenceRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationMetricDirection {
    HigherIsBetter,
    LowerIsBetter,
}

/// The Runtime-observable quantity used for one evaluation metric. The
/// evaluator never infers a measurement from a metric name supplied by a
/// candidate; every metric maps to one of these stable observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationMetricSource {
    TaskSuccess,
    AcceptanceCoverage,
    EvidenceCoverage,
    InputTokens,
    OutputTokens,
    TotalTokens,
    ToolCalls,
    ElapsedMilliseconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationMissingValuePolicy {
    /// A missing, NaN, or non-finite observation makes the comparison
    /// ineligible. This is the default release-safe policy.
    FailClosed,
    /// The run may be retained as diagnostic evidence, but it cannot be used
    /// to satisfy a release gate.
    DiagnosticOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationMultiplicityCorrection {
    None,
    Bonferroni,
    BenjaminiHochberg,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EvaluationStoppingRule {
    FixedSamples,
    Sequential {
        max_samples: u32,
        /// Minimum additional samples between sequential decisions. This
        /// avoids repeatedly checking a noisy single new observation.
        check_interval: u32,
    },
}

/// Workspace/organization policy that an individual Definition contract may
/// strengthen but never relax.  This is deliberately a versioned contract
/// rather than evaluator configuration so Runtime can bind a candidate and
/// every subsequent approval to the exact policy that protected it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationPolicyFloor {
    pub policy_id: String,
    pub revision: u64,
    pub minimum_samples: u32,
    pub minimum_confidence_basis_points: u16,
    pub require_fail_closed_for_protected_metrics: bool,
    pub require_protected_hard_gate: bool,
    pub require_target_improvement: bool,
}

impl Default for EvaluationPolicyFloor {
    fn default() -> Self {
        Self {
            policy_id: "workspace/default-evaluation-policy".to_string(),
            revision: 1,
            minimum_samples: 10,
            minimum_confidence_basis_points: 9_000,
            require_fail_closed_for_protected_metrics: true,
            require_protected_hard_gate: true,
            require_target_improvement: true,
        }
    }
}

impl EvaluationPolicyFloor {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("evaluation.policy_floor.policy_id", &self.policy_id)?;
        if self.revision == 0 || self.minimum_samples == 0 {
            return Err(ValidationError::InvalidContract {
                message: "evaluation policy floor revision and minimum samples must be positive"
                    .to_string(),
            });
        }
        if self.minimum_confidence_basis_points == 0
            || self.minimum_confidence_basis_points > 10_000
        {
            return Err(ValidationError::InvalidContract {
                message: "evaluation policy floor confidence must be within 1..=10000 basis points"
                    .to_string(),
            });
        }
        Ok(())
    }

    /// Reject contracts that could make an evolution candidate easier to
    /// approve than the workspace policy permits. This is called by Runtime
    /// at registration, evaluation and release-review boundaries.
    pub fn validate_contract(&self, contract: &EvaluationContract) -> Result<(), ValidationError> {
        self.validate()?;
        contract.validate()?;
        if contract.metrics.iter().any(|metric| {
            metric.minimum_samples < self.minimum_samples
                || metric.minimum_confidence_basis_points < self.minimum_confidence_basis_points
        }) {
            return Err(ValidationError::InvalidContract {
                message: "evaluation contract contains a metric below the active policy floor"
                    .to_string(),
            });
        }
        if self.require_fail_closed_for_protected_metrics
            && contract
                .metrics
                .iter()
                .filter(|metric| metric.protected)
                .any(|metric| {
                    metric.missing_value_policy != EvaluationMissingValuePolicy::FailClosed
                })
        {
            return Err(ValidationError::InvalidContract {
                message:
                    "protected evaluation metrics must fail closed under the active policy floor"
                        .to_string(),
            });
        }
        if self.require_protected_hard_gate
            && !contract
                .metrics
                .iter()
                .any(|metric| metric.protected && metric.hard_gate)
        {
            return Err(ValidationError::InvalidContract {
                message: "evaluation contract requires a protected hard gate under the active policy floor"
                    .to_string(),
            });
        }
        if self.require_target_improvement
            && !contract
                .metrics
                .iter()
                .any(|metric| metric.target_improvement)
        {
            return Err(ValidationError::InvalidContract {
                message: "evaluation contract requires a target improvement under the active policy floor"
                    .to_string(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(bytes))
    }
}

/// A paired workload that can be executed against both a stable baseline and
/// an unpublished candidate. Scenario assets are intentionally declarative:
/// they describe the task and ceilings, while Runtime still creates the
/// Binding, provider session, tool boundary and durable evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationScenarioSpec {
    pub scenario_ref: String,
    pub objective: String,
    pub acceptance: Vec<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub allowed_skills: Vec<String>,
    /// Exact bounded resources shared by both sides of a paired scenario.
    /// The evaluation harness may narrow these leases but never invent a
    /// whole-workspace fallback.
    #[serde(default)]
    pub resource_scopes: Vec<String>,
    pub permission_ceiling: PermissionMode,
    pub model_lease: String,
}

/// Durable, minimized observation of a real paired scenario run. Full
/// transcript/tool evidence remains in the Runtime/session stores; this
/// contract carries only the facts required to calculate a comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationScenarioObservation {
    pub scenario_ref: String,
    pub definition_revision: u64,
    pub run_ref: String,
    pub succeeded: bool,
    pub acceptance_total: u32,
    pub acceptance_satisfied: u32,
    pub evidence_refs: Vec<EvidenceRef>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub tool_calls: u64,
    pub elapsed_ms: u64,
}

impl EvaluationScenarioSpec {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("evaluation.scenario.scenario_ref", &self.scenario_ref)?;
        validate_reference("evaluation.scenario.objective", &self.objective)?;
        validate_reference("evaluation.scenario.model_lease", &self.model_lease)?;
        if self.acceptance.is_empty() {
            return Err(ValidationError::MissingField {
                field: "evaluation.scenario.acceptance".to_string(),
            });
        }
        let mut values = BTreeSet::new();
        for acceptance in &self.acceptance {
            validate_reference("evaluation.scenario.acceptance", acceptance)?;
            if !values.insert(acceptance) {
                return Err(ValidationError::DuplicateValue {
                    field: "evaluation.scenario.acceptance".to_string(),
                    value: acceptance.clone(),
                });
            }
        }
        for (field, values) in [
            ("evaluation.scenario.allowed_tools", &self.allowed_tools),
            ("evaluation.scenario.allowed_skills", &self.allowed_skills),
        ] {
            let mut unique = BTreeSet::new();
            for value in values {
                validate_reference(field, value)?;
                if !unique.insert(value) {
                    return Err(ValidationError::DuplicateValue {
                        field: field.to_string(),
                        value: value.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

impl EvaluationStoppingRule {
    fn validate(&self, minimum_samples: u32) -> Result<(), ValidationError> {
        match self {
            Self::FixedSamples => Ok(()),
            Self::Sequential {
                max_samples,
                check_interval,
            } if *max_samples >= minimum_samples && *check_interval > 0 => Ok(()),
            Self::Sequential { .. } => Err(ValidationError::InvalidContract {
                message: "sequential evaluation rule must have a positive check interval and a max_samples no smaller than minimum_samples".to_string(),
            }),
        }
    }
}

/// One normalized metric gate.
///
/// Numeric observations use the declared unit and six-decimal fixed-point
/// margins (`non_inferiority_margin_micros`).  The fixed representation keeps
/// Definition contracts comparable, serializable, and `Eq` without using
/// floating point values as policy data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationMetricSpec {
    pub metric_id: String,
    pub source: EvaluationMetricSource,
    pub unit: String,
    pub direction: EvaluationMetricDirection,
    pub non_inferiority_margin_micros: u64,
    pub minimum_samples: u32,
    pub minimum_confidence_basis_points: u16,
    pub hard_gate: bool,
    pub protected: bool,
    pub target_improvement: bool,
    pub missing_value_policy: EvaluationMissingValuePolicy,
    pub paired_scenario_refs: Vec<String>,
    pub multiplicity_correction: EvaluationMultiplicityCorrection,
    pub stopping_rule: EvaluationStoppingRule,
}

impl EvaluationMetricSpec {
    /// A conservative normalized gate for small built-in Definitions and
    /// fixtures. Persisted manifests still contain the complete resulting
    /// structure; this constructor merely avoids duplicating the same safe
    /// baseline in bootstrap code.
    #[must_use]
    pub fn release_gate(
        scenario_ref: impl Into<String>,
        metric_id: impl Into<String>,
        hard_gate: bool,
        target_improvement: bool,
    ) -> Self {
        let metric_id = metric_id.into();
        let (source, unit, direction) = match metric_id.as_str() {
            "evidence" | "evidence_required" => (
                EvaluationMetricSource::EvidenceCoverage,
                "normalized_score",
                EvaluationMetricDirection::HigherIsBetter,
            ),
            "contract" | "acceptance" => (
                EvaluationMetricSource::AcceptanceCoverage,
                "normalized_score",
                EvaluationMetricDirection::HigherIsBetter,
            ),
            "tool_efficiency" => (
                EvaluationMetricSource::ToolCalls,
                "call_count",
                EvaluationMetricDirection::LowerIsBetter,
            ),
            _ => (
                EvaluationMetricSource::TaskSuccess,
                "normalized_score",
                EvaluationMetricDirection::HigherIsBetter,
            ),
        };
        Self {
            metric_id,
            source,
            unit: unit.to_string(),
            direction,
            non_inferiority_margin_micros: 0,
            minimum_samples: 10,
            minimum_confidence_basis_points: 9_000,
            hard_gate,
            protected: true,
            target_improvement,
            missing_value_policy: EvaluationMissingValuePolicy::FailClosed,
            paired_scenario_refs: vec![scenario_ref.into()],
            multiplicity_correction: EvaluationMultiplicityCorrection::BenjaminiHochberg,
            stopping_rule: EvaluationStoppingRule::FixedSamples,
        }
    }
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("evaluation.metrics.metric_id", &self.metric_id)?;
        validate_reference("evaluation.metrics.unit", &self.unit)?;
        if self.minimum_samples == 0 {
            return Err(ValidationError::InvalidContract {
                message: format!(
                    "evaluation metric `{}` must require at least one sample",
                    self.metric_id
                ),
            });
        }
        if self.minimum_confidence_basis_points == 0
            || self.minimum_confidence_basis_points > 10_000
        {
            return Err(ValidationError::InvalidContract {
                message: format!(
                    "evaluation metric `{}` must use confidence within 1..=10000 basis points",
                    self.metric_id
                ),
            });
        }
        if self.paired_scenario_refs.is_empty() {
            return Err(ValidationError::MissingField {
                field: format!("evaluation.metrics.{}.paired_scenario_refs", self.metric_id),
            });
        }
        let mut scenarios = BTreeSet::new();
        for scenario in &self.paired_scenario_refs {
            validate_reference("evaluation.metrics.paired_scenario_refs", scenario)?;
            if !scenarios.insert(scenario) {
                return Err(ValidationError::DuplicateValue {
                    field: format!("evaluation.metrics.{}.paired_scenario_refs", self.metric_id),
                    value: scenario.clone(),
                });
            }
        }
        if self.missing_value_policy == EvaluationMissingValuePolicy::DiagnosticOnly
            && (self.hard_gate || self.protected || self.target_improvement)
        {
            return Err(ValidationError::InvalidContract {
                message: format!(
                    "diagnostic-only evaluation metric `{}` cannot be a protected, hard-gate, or target-improvement metric",
                    self.metric_id
                ),
            });
        }
        self.stopping_rule.validate(self.minimum_samples)
    }

    #[must_use]
    pub fn non_inferiority_margin(&self) -> f64 {
        self.non_inferiority_margin_micros as f64 / 1_000_000.0
    }

    #[must_use]
    pub fn minimum_confidence(&self) -> f64 {
        self.minimum_confidence_basis_points as f64 / 10_000.0
    }
}

/// The complete immutable workload and metric policy for a Definition
/// revision. All metrics are evaluated on paired scenarios; the evaluator may
/// collect extra diagnostics but cannot replace or drop these gates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationContract {
    pub scenario_refs: Vec<String>,
    pub metrics: Vec<EvaluationMetricSpec>,
}

impl EvaluationContract {
    #[must_use]
    pub fn single_release_gate(
        scenario_ref: impl Into<String>,
        metric_id: impl Into<String>,
    ) -> Self {
        let scenario_ref = scenario_ref.into();
        Self {
            scenario_refs: vec![scenario_ref.clone()],
            metrics: vec![EvaluationMetricSpec::release_gate(
                scenario_ref,
                metric_id,
                true,
                true,
            )],
        }
    }
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.scenario_refs.is_empty() {
            return Err(ValidationError::MissingField {
                field: "evaluation.scenario_refs".to_string(),
            });
        }
        let mut scenarios = BTreeSet::<String>::new();
        for scenario in &self.scenario_refs {
            validate_reference("evaluation.scenario_refs", scenario)?;
            if !scenarios.insert(scenario.clone()) {
                return Err(ValidationError::DuplicateValue {
                    field: "evaluation.scenario_refs".to_string(),
                    value: scenario.clone(),
                });
            }
        }
        if self.metrics.is_empty() {
            return Err(ValidationError::MissingField {
                field: "evaluation.metrics".to_string(),
            });
        }
        let mut metric_ids = BTreeSet::new();
        for metric in &self.metrics {
            metric.validate()?;
            if !metric_ids.insert(metric.metric_id.as_str()) {
                return Err(ValidationError::DuplicateValue {
                    field: "evaluation.metrics.metric_id".to_string(),
                    value: metric.metric_id.clone(),
                });
            }
            if metric
                .paired_scenario_refs
                .iter()
                .any(|scenario| !scenarios.contains(scenario))
            {
                return Err(ValidationError::InvalidContract {
                    message: format!(
                        "evaluation metric `{}` references a scenario outside evaluation.scenario_refs",
                        metric.metric_id
                    ),
                });
            }
        }
        if !self.metrics.iter().any(|metric| metric.protected) {
            return Err(ValidationError::InvalidContract {
                message: "evaluation contract requires at least one protected metric".to_string(),
            });
        }
        if !self.metrics.iter().any(|metric| metric.target_improvement) {
            return Err(ValidationError::InvalidContract {
                message: "evaluation contract requires at least one target improvement metric"
                    .to_string(),
            });
        }
        Ok(())
    }

    /// Content-addressed identity for report binding and policy comparison.
    /// Callers must validate before accepting the digest as a release gate.
    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(bytes))
    }

    #[must_use]
    pub fn protected_metric_ids(&self) -> Vec<String> {
        self.metrics
            .iter()
            .filter(|metric| metric.protected)
            .map(|metric| metric.metric_id.clone())
            .collect()
    }

    /// Whether this contract can replace `baseline` without relaxing any
    /// existing release gate. New scenarios and metrics are allowed, but a
    /// baseline metric cannot be removed, weakened, or made less observable.
    #[must_use]
    pub fn is_noninferior_to(&self, baseline: &Self) -> bool {
        baseline.scenario_refs.iter().all(|scenario| {
            self.scenario_refs
                .iter()
                .any(|candidate| candidate == scenario)
        }) && baseline.metrics.iter().all(|baseline_metric| {
            let Some(candidate_metric) = self
                .metrics
                .iter()
                .find(|metric| metric.metric_id == baseline_metric.metric_id)
            else {
                return false;
            };
            candidate_metric.unit == baseline_metric.unit
                && candidate_metric.direction == baseline_metric.direction
                && candidate_metric.non_inferiority_margin_micros
                    <= baseline_metric.non_inferiority_margin_micros
                && candidate_metric.minimum_samples >= baseline_metric.minimum_samples
                && candidate_metric.minimum_confidence_basis_points
                    >= baseline_metric.minimum_confidence_basis_points
                && (!baseline_metric.hard_gate || candidate_metric.hard_gate)
                && (!baseline_metric.protected || candidate_metric.protected)
                && (!baseline_metric.target_improvement || candidate_metric.target_improvement)
                && (!matches!(
                    baseline_metric.missing_value_policy,
                    EvaluationMissingValuePolicy::FailClosed
                ) || matches!(
                    candidate_metric.missing_value_policy,
                    EvaluationMissingValuePolicy::FailClosed
                ))
                && baseline_metric.paired_scenario_refs.iter().all(|scenario| {
                    candidate_metric
                        .paired_scenario_refs
                        .iter()
                        .any(|candidate| candidate == scenario)
                })
        })
    }
}
