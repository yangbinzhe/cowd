//! Definition-revision evolution evaluation adapter.
//!
//! Runtime owns candidate state, eligibility and release authorization. This
//! crate owns evaluation execution and only returns immutable comparison
//! evidence through Runtime's dependency-inverted port.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use harness_contract::evaluation::{
    EvaluationContract, EvaluationMetricDirection, EvaluationMetricSource,
    EvaluationScenarioObservation, EvaluationScenarioSpec,
};
use serde::{Deserialize, Serialize};

use crate::report_store::now_ms;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionClosureReport {
    pub kind: String,
    pub candidate_count: usize,
    pub comparison_report_count: usize,
    pub eligible_report_count: usize,
    pub release_mutation_count: usize,
    pub runtime_port_implemented: bool,
    pub status: String,
    pub evidence_refs: Vec<String>,
}

/// Supplies paired workloads for a concrete Agent or Team definition revision.
/// The production composition root binds this to real scenario execution; it
/// is deliberately not a Gateway HTTP callback and cannot decide rollout.
#[async_trait]
pub trait DefinitionEvolutionWorkload: Send + Sync {
    async fn evaluate_definition(
        &self,
        candidate: &runtime::EvolutionGovernanceCandidate,
    ) -> Result<runtime::EvolutionComparisonReportV2, String>;
}

/// Supplies declarative paired workloads. The catalog is intentionally
/// separate from Definition assets so a candidate cannot silently edit the
/// tests that decide whether it may be released.
pub trait DefinitionEvolutionScenarioCatalog: Send + Sync {
    fn load(&self, scenario_ref: &str) -> Result<EvaluationScenarioSpec, String>;
}

/// Runtime-side executor for one paired scenario. Gateway composition binds
/// this to RuntimeServices; `harness-eval` owns scoring but never a release
/// mutation or a provider/Gateway HTTP shortcut.
#[async_trait]
pub trait DefinitionEvolutionScenarioExecutor: Send + Sync {
    async fn execute(
        &self,
        candidate_id: &str,
        scenario: &EvaluationScenarioSpec,
        sample_index: u32,
    ) -> Result<(EvaluationScenarioObservation, EvaluationScenarioObservation), String>;
}

/// File-backed workload catalog used by the production composition root.
/// References map to `<root>/<safe/ref>.json`; traversal and absolute paths
/// are rejected before filesystem access.
#[derive(Debug, Clone)]
pub struct FileDefinitionEvolutionScenarioCatalog {
    root: PathBuf,
}

impl FileDefinitionEvolutionScenarioCatalog {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, scenario_ref: &str) -> Result<PathBuf, String> {
        let relative = Path::new(scenario_ref);
        if relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir | std::path::Component::RootDir
                )
            })
        {
            return Err("evaluation_scenario_ref_must_be_a_safe_relative_path".to_string());
        }
        Ok(self.root.join(relative).with_extension("json"))
    }
}

impl DefinitionEvolutionScenarioCatalog for FileDefinitionEvolutionScenarioCatalog {
    fn load(&self, scenario_ref: &str) -> Result<EvaluationScenarioSpec, String> {
        let path = self.path_for(scenario_ref)?;
        let raw = fs::read_to_string(&path).map_err(|error| {
            format!(
                "evaluation_scenario_unavailable:{}:{}",
                path.display(),
                error
            )
        })?;
        let scenario: EvaluationScenarioSpec = serde_json::from_str(&raw)
            .map_err(|error| format!("evaluation_scenario_invalid:{}:{}", path.display(), error))?;
        scenario.validate().map_err(|error| error.to_string())?;
        if scenario.scenario_ref != scenario_ref {
            return Err("evaluation_scenario_ref_does_not_match_asset".to_string());
        }
        Ok(scenario)
    }
}

/// Production paired workload: resolve all baseline-owned scenario assets,
/// execute each baseline/candidate pair through Runtime, then calculate every
/// metric from observable run facts. Missing assets, a failed execution, or a
/// metric that cannot be measured is returned as an error so Runtime records
/// no permissive comparison report.
pub struct RuntimeDefinitionEvolutionWorkload {
    catalog: Arc<dyn DefinitionEvolutionScenarioCatalog>,
    executor: Arc<dyn DefinitionEvolutionScenarioExecutor>,
}

impl RuntimeDefinitionEvolutionWorkload {
    #[must_use]
    pub fn new(
        catalog: Arc<dyn DefinitionEvolutionScenarioCatalog>,
        executor: Arc<dyn DefinitionEvolutionScenarioExecutor>,
    ) -> Self {
        Self { catalog, executor }
    }
}

#[async_trait]
impl DefinitionEvolutionWorkload for RuntimeDefinitionEvolutionWorkload {
    async fn evaluate_definition(
        &self,
        candidate: &runtime::EvolutionGovernanceCandidate,
    ) -> Result<runtime::EvolutionComparisonReportV2, String> {
        let scenario_repetitions = scenario_repetitions(&candidate.evaluation_contract)?;
        let mut paired = Vec::new();
        for (scenario_ref, repetitions) in scenario_repetitions {
            let scenario = self.catalog.load(&scenario_ref)?;
            for sample_index in 0..repetitions {
                let (baseline, proposed) = self
                    .executor
                    .execute(&candidate.candidate_id, &scenario, sample_index)
                    .await?;
                if baseline.scenario_ref != scenario_ref
                    || proposed.scenario_ref != scenario_ref
                    || baseline.definition_revision != candidate.baseline_revision
                    || proposed.definition_revision != subject_revision(candidate)?
                {
                    return Err("evaluation_runtime_observation_binding_mismatch".to_string());
                }
                paired.push((baseline, proposed));
            }
        }
        comparison_from_observations(candidate, &paired)
    }
}

/// Determine a deterministic, complete sample schedule before any run
/// begins. A sequential metric may be observed at intermediate boundaries in
/// a future streaming evaluator, but this release-safe batch evaluator always
/// collects its declared maximum sample count before producing a promotion
/// report. It therefore cannot stop early on a lucky partial result.
fn scenario_repetitions(contract: &EvaluationContract) -> Result<Vec<(String, u32)>, String> {
    let mut repetitions = contract
        .scenario_refs
        .iter()
        .cloned()
        .map(|scenario_ref| (scenario_ref, 1_u32))
        .collect::<std::collections::BTreeMap<_, _>>();
    for metric in &contract.metrics {
        let required = match metric.stopping_rule {
            harness_contract::evaluation::EvaluationStoppingRule::FixedSamples => {
                metric.minimum_samples
            }
            harness_contract::evaluation::EvaluationStoppingRule::Sequential {
                max_samples,
                ..
            } => max_samples,
        };
        let scenario_count = u32::try_from(metric.paired_scenario_refs.len())
            .map_err(|_| "evaluation_metric_scenario_count_overflow".to_string())?;
        let per_scenario =
            required.saturating_add(scenario_count.saturating_sub(1)) / scenario_count;
        for scenario_ref in &metric.paired_scenario_refs {
            let Some(current) = repetitions.get_mut(scenario_ref) else {
                return Err("evaluation_contract_references_unknown_scenario".to_string());
            };
            *current = (*current).max(per_scenario);
        }
    }
    Ok(repetitions.into_iter().collect())
}

fn subject_revision(candidate: &runtime::EvolutionGovernanceCandidate) -> Result<u64, String> {
    match &candidate.subject {
        runtime::EvolutionCandidateSubject::AgentDefinition { revision_ref } => {
            Ok(revision_ref.revision)
        }
        runtime::EvolutionCandidateSubject::TeamTemplate { revision_ref } => {
            Ok(revision_ref.revision)
        }
    }
}

fn comparison_from_observations(
    candidate: &runtime::EvolutionGovernanceCandidate,
    paired: &[(EvaluationScenarioObservation, EvaluationScenarioObservation)],
) -> Result<runtime::EvolutionComparisonReportV2, String> {
    if paired.is_empty() {
        return Err("evaluation_contract_has_no_executed_scenarios".to_string());
    }
    let contract: &EvaluationContract = &candidate.evaluation_contract;
    let mut source_run_refs = BTreeSet::new();
    let mut evidence_refs = BTreeSet::new();
    for (baseline, proposed) in paired {
        source_run_refs.insert(baseline.run_ref.clone());
        source_run_refs.insert(proposed.run_ref.clone());
        evidence_refs.extend(baseline.evidence_refs.iter().cloned());
        evidence_refs.extend(proposed.evidence_refs.iter().cloned());
    }
    let prepared = contract
        .metrics
        .iter()
        .map(|metric| {
            let pairs = paired
                .iter()
                .filter(|(baseline, _)| {
                    metric
                        .paired_scenario_refs
                        .iter()
                        .any(|scenario_ref| scenario_ref == &baseline.scenario_ref)
                })
                .collect::<Vec<_>>();
            let baseline_values = pairs
                .iter()
                .map(|(baseline, _)| observation_value(metric.source, baseline))
                .collect::<Vec<_>>();
            let candidate_values = pairs
                .iter()
                .map(|(_, proposed)| observation_value(metric.source, proposed))
                .collect::<Vec<_>>();
            let has_missing = baseline_values
                .iter()
                .chain(&candidate_values)
                .any(|value| !value.is_finite());
            if has_missing
                && metric.missing_value_policy
                    == harness_contract::evaluation::EvaluationMissingValuePolicy::FailClosed
            {
                return Err(format!(
                    "evaluation_metric_has_missing_or_non_finite_observation:{}",
                    metric.metric_id
                ));
            }
            let valid = baseline_values
                .iter()
                .zip(&candidate_values)
                .filter_map(|(baseline, candidate)| {
                    (baseline.is_finite() && candidate.is_finite())
                        .then_some((*baseline, *candidate))
                })
                .collect::<Vec<_>>();
            let baseline_values = valid
                .iter()
                .map(|(baseline, _)| *baseline)
                .collect::<Vec<_>>();
            let candidate_values = valid
                .iter()
                .map(|(_, candidate)| *candidate)
                .collect::<Vec<_>>();
            let baseline = mean_or_zero(&baseline_values);
            let candidate = mean_or_zero(&candidate_values);
            let raw_p_value = paired_noninferiority_p_value(
                &baseline_values,
                &candidate_values,
                metric.direction,
                metric.non_inferiority_margin(),
            );
            Ok((
                metric,
                baseline,
                candidate,
                baseline_values.len(),
                raw_p_value,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let adjusted_p_values = adjust_p_values(
        &prepared
            .iter()
            .map(|(metric, _, _, _, p_value)| (*p_value, metric.multiplicity_correction))
            .collect::<Vec<_>>(),
    );
    let dimensions = prepared
        .into_iter()
        .zip(adjusted_p_values)
        .map(
            |((metric, baseline, candidate, sample_count, _), adjusted_p_value)| {
                runtime::EvolutionComparisonDimension {
                    metric_id: metric.metric_id.clone(),
                    direction: metric.direction,
                    baseline,
                    candidate,
                    non_inferiority_margin: metric.non_inferiority_margin(),
                    sample_count: sample_count.min(u32::MAX as usize) as u32,
                    minimum_samples: metric.minimum_samples,
                    // One-sided paired sign-test confidence after the contract's
                    // declared multiplicity correction. It is not an arbitrary
                    // heuristic support ratio.
                    confidence: (1.0 - adjusted_p_value).clamp(0.0, 1.0),
                    minimum_confidence: metric.minimum_confidence(),
                    hard_gate: metric.hard_gate,
                    protected: metric.protected,
                    target_improvement: metric.target_improvement,
                }
            },
        )
        .collect::<Vec<_>>();
    Ok(runtime::EvolutionComparisonReportV2 {
        report_id: format!(
            "evolution-eval:{}:{}",
            candidate.candidate_id,
            uuid::Uuid::new_v4()
        ),
        candidate_id: candidate.candidate_id.clone(),
        evaluation_contract_digest: candidate.evaluation_contract_digest(),
        dimensions,
        source_run_refs: source_run_refs.into_iter().collect(),
        evidence_refs: evidence_refs.into_iter().collect(),
        created_at_ms: now_ms().min(u128::from(u64::MAX)) as u64,
    })
}

fn observation_value(
    source: EvaluationMetricSource,
    observation: &EvaluationScenarioObservation,
) -> f64 {
    match source {
        EvaluationMetricSource::TaskSuccess => {
            if observation.succeeded {
                1.0
            } else {
                0.0
            }
        }
        EvaluationMetricSource::AcceptanceCoverage => {
            if observation.acceptance_total == 0 {
                f64::NAN
            } else {
                observation.acceptance_satisfied as f64 / observation.acceptance_total as f64
            }
        }
        EvaluationMetricSource::EvidenceCoverage => {
            if observation.acceptance_total == 0 {
                f64::NAN
            } else {
                observation
                    .evidence_refs
                    .len()
                    .min(observation.acceptance_total as usize) as f64
                    / observation.acceptance_total as f64
            }
        }
        EvaluationMetricSource::InputTokens => observation.input_tokens as f64,
        EvaluationMetricSource::OutputTokens => observation.output_tokens as f64,
        EvaluationMetricSource::TotalTokens => observation
            .input_tokens
            .saturating_add(observation.output_tokens)
            as f64,
        EvaluationMetricSource::ToolCalls => observation.tool_calls as f64,
        EvaluationMetricSource::ElapsedMilliseconds => observation.elapsed_ms as f64,
    }
}

fn mean_or_zero(values: &[f64]) -> f64 {
    if !values.is_empty() { values.iter().sum::<f64>() / values.len() as f64 } else { 0.0 }
}

/// Exact one-sided paired sign-test p-value for the null hypothesis that a
/// candidate is not more likely than chance to be non-inferior. This makes a
/// release confidence meaningful for both higher- and lower-is-better
/// metrics, without making distributional assumptions about token or tool
/// counts.
fn paired_noninferiority_p_value(
    baseline: &[f64],
    candidate: &[f64],
    direction: EvaluationMetricDirection,
    margin: f64,
) -> f64 {
    if baseline.len() != candidate.len() || baseline.is_empty() {
        return 1.0;
    }
    let successes = baseline
        .iter()
        .zip(candidate)
        .filter(|(baseline, candidate)| match direction {
            EvaluationMetricDirection::HigherIsBetter => **candidate + margin >= **baseline,
            EvaluationMetricDirection::LowerIsBetter => **candidate - margin <= **baseline,
        })
        .count();
    binomial_upper_tail(baseline.len(), successes)
}

fn binomial_upper_tail(sample_count: usize, successes: usize) -> f64 {
    if successes == 0 {
        return 1.0;
    }
    if successes > sample_count {
        return 1.0;
    }
    (successes..=sample_count)
        .map(|successes| {
            let log_coefficient = (0..successes).fold(0.0, |value, index| {
                value + ((sample_count - index) as f64).ln() - ((index + 1) as f64).ln()
            });
            (log_coefficient - (sample_count as f64) * std::f64::consts::LN_2).exp()
        })
        .sum::<f64>()
        .min(1.0)
}

fn adjust_p_values(
    metrics: &[(
        f64,
        harness_contract::evaluation::EvaluationMultiplicityCorrection,
    )],
) -> Vec<f64> {
    use harness_contract::evaluation::EvaluationMultiplicityCorrection;

    let mut adjusted = metrics
        .iter()
        .map(|(p_value, _)| p_value.clamp(0.0, 1.0))
        .collect::<Vec<_>>();
    for correction in [
        EvaluationMultiplicityCorrection::Bonferroni,
        EvaluationMultiplicityCorrection::BenjaminiHochberg,
    ] {
        let mut indexed = metrics
            .iter()
            .enumerate()
            .filter(|(_, (_, configured))| *configured == correction)
            .map(|(index, (p_value, _))| (index, p_value.clamp(0.0, 1.0)))
            .collect::<Vec<_>>();
        if indexed.is_empty() {
            continue;
        }
        match correction {
            EvaluationMultiplicityCorrection::Bonferroni => {
                let multiplier = indexed.len() as f64;
                for (index, p_value) in indexed {
                    adjusted[index] = (p_value * multiplier).min(1.0);
                }
            }
            EvaluationMultiplicityCorrection::BenjaminiHochberg => {
                indexed.sort_by(|left, right| left.1.total_cmp(&right.1));
                let total = indexed.len() as f64;
                let mut trailing_min = 1.0_f64;
                for (rank, (index, p_value)) in indexed.into_iter().enumerate().rev() {
                    let candidate = (p_value * total / (rank + 1) as f64).min(1.0);
                    trailing_min = trailing_min.min(candidate);
                    adjusted[index] = trailing_min;
                }
            }
            EvaluationMultiplicityCorrection::None => unreachable!("filtered above"),
        }
    }
    adjusted
}

/// `harness-eval` implementation of Runtime's evaluation port. The wrapped
/// workload executor can run provider-backed, deterministic, or replayed
/// paired scenarios, but the adapter itself never changes candidate lifecycle
/// or release state.
#[derive(Clone)]
pub struct DefinitionEvolutionEvalRunner {
    workload: Arc<dyn DefinitionEvolutionWorkload>,
}

impl DefinitionEvolutionEvalRunner {
    #[must_use]
    pub fn new(workload: Arc<dyn DefinitionEvolutionWorkload>) -> Self {
        Self { workload }
    }
}

#[async_trait]
impl runtime::EvolutionEvalRunner for DefinitionEvolutionEvalRunner {
    async fn evaluate(
        &self,
        candidate: &runtime::EvolutionGovernanceCandidate,
    ) -> Result<runtime::EvolutionComparisonReportV2, String> {
        let report = self.workload.evaluate_definition(candidate).await?;
        if report.candidate_id != candidate.candidate_id {
            return Err("definition_evolution_workload_returned_wrong_candidate".to_string());
        }
        if report.evaluation_contract_digest != candidate.evaluation_contract_digest() {
            return Err("definition_evolution_workload_returned_wrong_contract".to_string());
        }
        Ok(report)
    }
}

#[must_use]
pub fn evaluate_evolution_closure() -> EvolutionClosureReport {
    let candidate = closure_candidate();
    let runner = DefinitionEvolutionEvalRunner::new(Arc::new(ClosureWorkload));
    match futures::executor::block_on(runtime::EvolutionEvalRunner::evaluate(&runner, &candidate)) {
        Ok(report) => EvolutionClosureReport {
            kind: "harness_eval.definition_evolution_closure".to_string(),
            candidate_count: 1,
            comparison_report_count: 1,
            eligible_report_count: usize::from(report.is_eligible()),
            release_mutation_count: 0,
            runtime_port_implemented: true,
            status: if report.is_eligible() {
                "passed".to_string()
            } else {
                "failed".to_string()
            },
            evidence_refs: report.evidence_refs,
        },
        Err(error) => EvolutionClosureReport {
            kind: "harness_eval.definition_evolution_closure".to_string(),
            candidate_count: 1,
            comparison_report_count: 0,
            eligible_report_count: 0,
            release_mutation_count: 0,
            runtime_port_implemented: true,
            status: "failed".to_string(),
            evidence_refs: vec![format!("evaluation_error:{error}")],
        },
    }
}

struct ClosureWorkload;

#[async_trait]
impl DefinitionEvolutionWorkload for ClosureWorkload {
    async fn evaluate_definition(
        &self,
        candidate: &runtime::EvolutionGovernanceCandidate,
    ) -> Result<runtime::EvolutionComparisonReportV2, String> {
        Ok(runtime::EvolutionComparisonReportV2 {
            report_id: "harness-eval-closure-report".to_string(),
            candidate_id: candidate.candidate_id.clone(),
            evaluation_contract_digest: candidate.evaluation_contract_digest(),
            dimensions: vec![
                runtime::EvolutionComparisonDimension {
                    metric_id: "task_success".to_string(),
                    direction: runtime::EvaluationDirection::HigherIsBetter,
                    baseline: 0.80,
                    candidate: 0.90,
                    non_inferiority_margin: 0.0,
                    sample_count: 12,
                    minimum_samples: 10,
                    confidence: 0.95,
                    minimum_confidence: 0.90,
                    hard_gate: true,
                    protected: true,
                    target_improvement: true,
                },
                runtime::EvolutionComparisonDimension {
                    metric_id: "tool_efficiency".to_string(),
                    direction: runtime::EvaluationDirection::LowerIsBetter,
                    baseline: 5.0,
                    candidate: 3.0,
                    non_inferiority_margin: 0.0,
                    sample_count: 12,
                    minimum_samples: 10,
                    confidence: 0.95,
                    minimum_confidence: 0.90,
                    hard_gate: false,
                    protected: true,
                    target_improvement: false,
                },
            ],
            source_run_refs: vec!["harness-eval:closure:paired-workload".to_string()],
            evidence_refs: vec!["harness-eval:closure:report".to_string()],
            created_at_ms: 1,
        })
    }
}

fn closure_candidate() -> runtime::EvolutionGovernanceCandidate {
    let definition_id = harness_contract::agent::AgentDefinitionId::new(
        harness_contract::agent::DefinitionScope::Workspace,
        "cowd/evolution-closure",
    )
    .expect("valid fixture definition id");
    let revision_ref = harness_contract::agent::AgentDefinitionRevisionRef::new(definition_id, 2)
        .expect("valid fixture revision");
    runtime::EvolutionGovernanceCandidate {
        candidate_id: "harness-eval-closure-candidate".to_string(),
        subject: runtime::EvolutionCandidateSubject::AgentDefinition { revision_ref },
        baseline_revision: 1,
        evaluation_contract: harness_contract::evaluation::EvaluationContract {
            scenario_refs: vec!["harness-eval/closure".to_string()],
            metrics: vec![
                harness_contract::evaluation::EvaluationMetricSpec::release_gate(
                    "harness-eval/closure",
                    "task_success",
                    true,
                    true,
                ),
                harness_contract::evaluation::EvaluationMetricSpec::release_gate(
                    "harness-eval/closure",
                    "tool_efficiency",
                    false,
                    false,
                ),
            ],
        },
        evaluation_policy_floor: harness_contract::evaluation::EvaluationPolicyFloor::default(),
        source_evidence_refs: vec!["harness-eval:closure:source".to_string()],
        canary_policy: runtime::CanaryRolloutPolicy::default(),
        lifecycle: runtime::EvolutionCandidateLifecycle::Draft,
        comparison_report_ref: None,
        comparison_report_digest: None,
        canary_review_ref: None,
        stable_review_ref: None,
        canary_observation: None,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harness_eval_implements_the_runtime_port_without_release_mutation() {
        let report = evaluate_evolution_closure();
        assert_eq!(report.status, "passed");
        assert_eq!(report.candidate_count, 1);
        assert_eq!(report.comparison_report_count, 1);
        assert_eq!(report.eligible_report_count, 1);
        assert_eq!(report.release_mutation_count, 0);
        assert!(report.runtime_port_implemented);
    }

    #[test]
    fn adapter_rejects_a_workload_report_for_another_candidate() {
        struct WrongCandidate;
        #[async_trait]
        impl DefinitionEvolutionWorkload for WrongCandidate {
            async fn evaluate_definition(
                &self,
                candidate: &runtime::EvolutionGovernanceCandidate,
            ) -> Result<runtime::EvolutionComparisonReportV2, String> {
                let mut report = ClosureWorkload.evaluate_definition(candidate).await?;
                report.candidate_id = "wrong".to_string();
                Ok(report)
            }
        }
        let runner = DefinitionEvolutionEvalRunner::new(Arc::new(WrongCandidate));
        assert!(
            futures::executor::block_on(runtime::EvolutionEvalRunner::evaluate(
                &runner,
                &closure_candidate(),
            ))
            .is_err()
        );
    }

    #[test]
    fn evaluator_collects_the_contract_sample_count_before_scoring() {
        let contract = closure_candidate().evaluation_contract;
        assert_eq!(
            scenario_repetitions(&contract).expect("schedule"),
            vec![("harness-eval/closure".to_string(), 10)]
        );
    }

    #[test]
    fn paired_sign_confidence_and_multiplicity_are_conservative() {
        let p_value = paired_noninferiority_p_value(
            &[1.0; 12],
            &[1.0; 12],
            EvaluationMetricDirection::HigherIsBetter,
            0.0,
        );
        assert!(p_value < 0.01);
        let adjusted = adjust_p_values(&[
            (
                p_value,
                harness_contract::evaluation::EvaluationMultiplicityCorrection::Bonferroni,
            ),
            (
                p_value,
                harness_contract::evaluation::EvaluationMultiplicityCorrection::Bonferroni,
            ),
        ]);
        assert!(adjusted.iter().all(|value| *value >= p_value));
    }
}
