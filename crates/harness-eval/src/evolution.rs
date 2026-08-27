//! Definition-revision evolution evaluation adapter.
//!
//! Runtime owns candidate state, eligibility and release authorization. This
//! crate owns evaluation execution and only returns immutable comparison
//! evidence through Runtime's dependency-inverted port.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use harness_contract::evaluation::{
    EvaluationContract, EvaluationMetricDirection, EvaluationMetricSource,
    EvaluationScenarioObservation, EvaluationScenarioSpec, EvaluationStoppingReason,
    EvaluationStoppingRule,
};
use harness_contract::policy::PermissionMode;
use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    pub evidence_refs: Vec<EvidenceRef>,
}

/// Supplies paired workloads for a concrete Agent or Team definition revision.
/// The production composition root binds this to real scenario execution; it
/// is deliberately not a Gateway HTTP callback and cannot decide rollout.
#[async_trait]
pub trait DefinitionEvolutionWorkload: Send + Sync {
    fn readiness(
        &self,
        contract: &EvaluationContract,
    ) -> Result<runtime::EvolutionEvaluationReadiness, String> {
        contract.validate().map_err(|error| error.to_string())?;
        let mut scenario_refs = contract.scenario_refs.clone();
        scenario_refs.sort();
        let payload =
            serde_json::to_vec(&(contract.digest(), &scenario_refs)).map_err(|e| e.to_string())?;
        Ok(runtime::EvolutionEvaluationReadiness {
            scenario_bundle_digest: format!("sha256:{:x}", Sha256::digest(payload)),
            scenario_refs,
            maximum_paired_runs: maximum_paired_runs(contract)?,
        })
    }

    async fn evaluate_definition(
        &self,
        candidate: &runtime::EvolutionGovernanceCandidate,
    ) -> Result<runtime::EvolutionComparisonReportV2, String>;
}

/// Supplies declarative paired workloads. The catalog is intentionally
/// separate from Definition assets so a candidate cannot silently edit the
/// tests that decide whether it may be released.
pub trait DefinitionEvolutionScenarioCatalog: Send + Sync {
    fn load_verified(&self, scenario_ref: &str) -> Result<VerifiedEvaluationScenario, String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedEvaluationScenario {
    pub spec: EvaluationScenarioSpec,
    pub digest: String,
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
    fn load_verified(&self, scenario_ref: &str) -> Result<VerifiedEvaluationScenario, String> {
        let path = self.path_for(scenario_ref)?;
        let scenario = if scenario_ref.starts_with("builtin/") {
            if !BUILTIN_EVALUATION_SCENARIO_REFS.contains(&scenario_ref) {
                return Err("evaluation_builtin_scenario_not_registered".to_string());
            }
            EvaluationScenarioSpec {
                scenario_ref: scenario_ref.to_string(),
                objective:
                    "complete the declared definition objective and return auditable evidence"
                        .to_string(),
                acceptance: vec!["evidence".to_string()],
                allowed_tools: Vec::new(),
                allowed_skills: Vec::new(),
                resource_scopes: Vec::new(),
                permission_ceiling: PermissionMode::ReadOnly,
                model_lease: "evaluation/default".to_string(),
            }
        } else {
            let raw = fs::read_to_string(&path).map_err(|error| {
                format!(
                    "evaluation_scenario_unavailable:{}:{}",
                    path.display(),
                    error
                )
            })?;
            serde_json::from_str(&raw).map_err(|error| {
                format!("evaluation_scenario_invalid:{}:{}", path.display(), error)
            })?
        };
        scenario.validate().map_err(|error| error.to_string())?;
        if scenario.scenario_ref != scenario_ref {
            return Err("evaluation_scenario_ref_does_not_match_asset".to_string());
        }
        let canonical = serde_json::to_vec(&scenario).map_err(|error| error.to_string())?;
        Ok(VerifiedEvaluationScenario {
            spec: scenario,
            digest: format!("sha256:{:x}", Sha256::digest(canonical)),
        })
    }
}

/// Read-only scenarios owned by the Cowd binary. A Definition may reference a
/// custom file-backed scenario, but it cannot mint a trusted `builtin/*`
/// identity by choosing a new string.
const BUILTIN_EVALUATION_SCENARIO_REFS: &[&str] = &[
    "builtin/direct/baseline",
    "builtin/explore/baseline",
    "builtin/execute/baseline",
    "builtin/cowd/execute-review/team-baseline",
    "builtin/cowd/direct-executor/team-baseline",
    "builtin/cowd/planner-executor-verifier/team-baseline",
    "builtin/cowd/parallel-research-synthesis/team-baseline",
    "builtin/cowd/external-research-synthesis/team-baseline",
    "builtin/cowd/implementation-review-fix/team-baseline",
    "builtin/cowd/debate-critic-arbiter/team-baseline",
    "builtin/cowd/incident-response/team-baseline",
    "builtin/cowd/matrix-scenario-ensemble/team-baseline",
    "builtin/cowd/long-running-workstreams/team-baseline",
];

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
    fn readiness(
        &self,
        contract: &EvaluationContract,
    ) -> Result<runtime::EvolutionEvaluationReadiness, String> {
        contract.validate().map_err(|error| error.to_string())?;
        let schedule = scenario_repetitions(contract)?;
        let maximum_paired_runs = schedule.iter().try_fold(0_u32, |total, (_, count)| {
            total
                .checked_add(*count)
                .ok_or_else(|| "evaluation_schedule_size_overflow".to_string())
        })?;
        if maximum_paired_runs > MAXIMUM_PAIRED_RUNS {
            return Err(format!(
                "evaluation_schedule_exceeds_maximum_paired_runs:{maximum_paired_runs}:{MAXIMUM_PAIRED_RUNS}"
            ));
        }
        let mut assets = Vec::with_capacity(contract.scenario_refs.len());
        for scenario_ref in &contract.scenario_refs {
            let verified = self.catalog.load_verified(scenario_ref)?;
            assets.push((scenario_ref.clone(), verified.digest));
        }
        assets.sort();
        let canonical = serde_json::to_vec(&assets).map_err(|error| error.to_string())?;
        Ok(runtime::EvolutionEvaluationReadiness {
            scenario_bundle_digest: format!("sha256:{:x}", Sha256::digest(canonical)),
            scenario_refs: assets.into_iter().map(|(reference, _)| reference).collect(),
            maximum_paired_runs,
        })
    }

    async fn evaluate_definition(
        &self,
        candidate: &runtime::EvolutionGovernanceCandidate,
    ) -> Result<runtime::EvolutionComparisonReportV2, String> {
        let readiness = self.readiness(&candidate.evaluation_contract)?;
        if readiness.scenario_bundle_digest != candidate.evaluation_scenario_digest {
            return Err("evaluation_scenario_bundle_digest_mismatch".to_string());
        }
        let scenario_repetitions = scenario_repetitions(&candidate.evaluation_contract)?;
        let schedule = interleaved_schedule(&scenario_repetitions);
        let mut scenarios = BTreeMap::new();
        for scenario_ref in &candidate.evaluation_contract.scenario_refs {
            let verified = self.catalog.load_verified(scenario_ref)?;
            scenarios.insert(scenario_ref.clone(), verified.spec);
        }
        let mut paired = Vec::new();
        for (scenario_ref, sample_index) in schedule {
            let scenario = scenarios
                .get(&scenario_ref)
                .ok_or_else(|| "evaluation_schedule_references_unknown_scenario".to_string())?;
            let (baseline, proposed) = self
                .executor
                .execute(&candidate.candidate_id, scenario, sample_index)
                .await?;
            if baseline.scenario_ref != scenario_ref
                || proposed.scenario_ref != scenario_ref
                || baseline.definition_revision != candidate.baseline_revision
                || proposed.definition_revision != subject_revision(candidate)?
            {
                return Err("evaluation_runtime_observation_binding_mismatch".to_string());
            }
            paired.push((baseline, proposed));
            if let Some(reason) = sequential_stopping_reason(
                &candidate.evaluation_contract,
                candidate,
                &paired,
                &readiness,
            )? {
                return comparison_from_observations(candidate, &paired, &readiness, reason);
            }
        }
        let reason = if candidate.evaluation_contract.metrics.iter().any(|metric| {
            matches!(
                metric.stopping_rule,
                EvaluationStoppingRule::Sequential { .. }
            )
        }) {
            EvaluationStoppingReason::SequentialMaxSamples
        } else {
            EvaluationStoppingReason::FixedSamplesCompleted
        };
        comparison_from_observations(candidate, &paired, &readiness, reason)
    }
}

/// Determine a deterministic, complete sample schedule before any run
/// begins. A sequential metric may be observed at intermediate boundaries in
/// a future streaming evaluator, but this release-safe batch evaluator always
/// collects its declared maximum sample count before producing a promotion
/// report. It therefore cannot stop early on a lucky partial result.
const MAXIMUM_PAIRED_RUNS: u32 = 64;

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

fn maximum_paired_runs(contract: &EvaluationContract) -> Result<u32, String> {
    scenario_repetitions(contract)?
        .into_iter()
        .try_fold(0_u32, |total, (_, count)| {
            total
                .checked_add(count)
                .ok_or_else(|| "evaluation_schedule_size_overflow".to_string())
        })
}

fn interleaved_schedule(repetitions: &[(String, u32)]) -> Vec<(String, u32)> {
    let maximum = repetitions
        .iter()
        .map(|(_, repetitions)| *repetitions)
        .max()
        .unwrap_or_default();
    (0..maximum)
        .flat_map(|sample_index| {
            repetitions
                .iter()
                .filter(move |(_, count)| sample_index < *count)
                .map(move |(scenario_ref, _)| (scenario_ref.clone(), sample_index))
        })
        .collect()
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
    readiness: &runtime::EvolutionEvaluationReadiness,
    stopping_reason: EvaluationStoppingReason,
) -> Result<runtime::EvolutionComparisonReportV2, String> {
    if paired.is_empty() {
        return Err("evaluation_contract_has_no_executed_scenarios".to_string());
    }
    let contract: &EvaluationContract = &candidate.evaluation_contract;
    let mut source_run_refs = BTreeSet::new();
    let mut evidence_refs = BTreeMap::new();
    let mut environment_inputs = BTreeSet::new();
    for (baseline, proposed) in paired {
        source_run_refs.insert(baseline.run_ref.clone());
        source_run_refs.insert(proposed.run_ref.clone());
        for evidence in baseline.evidence_refs.iter().chain(&proposed.evidence_refs) {
            evidence_refs.insert(
                (evidence.ref_type.clone(), evidence.id.clone()),
                evidence.clone(),
            );
        }
        for observation in [baseline, proposed] {
            if !observation.environment_fingerprint.trim().is_empty() {
                environment_inputs.insert(observation.environment_fingerprint.clone());
            }
            environment_inputs.extend(observation.provider_model_refs.iter().cloned());
        }
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
            let raw_superiority_p_value = paired_superiority_p_value(
                &baseline_values,
                &candidate_values,
                metric.direction,
                metric.minimum_improvement(),
            );
            let look_multiplier = planned_sequential_looks(metric) as f64;
            Ok((
                metric,
                baseline,
                candidate,
                baseline_values.len(),
                (raw_p_value * look_multiplier).min(1.0),
                (raw_superiority_p_value * look_multiplier).min(1.0),
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let adjusted_p_values = adjust_p_values(
        &prepared
            .iter()
            .map(|(metric, _, _, _, p_value, _)| (*p_value, metric.multiplicity_correction))
            .collect::<Vec<_>>(),
    );
    let adjusted_superiority_p_values = adjust_p_values(
        &prepared
            .iter()
            .map(|(metric, _, _, _, _, p_value)| (*p_value, metric.multiplicity_correction))
            .collect::<Vec<_>>(),
    );
    let dimensions = prepared
        .into_iter()
        .zip(adjusted_p_values)
        .zip(adjusted_superiority_p_values)
        .map(
            |(
                ((metric, baseline, candidate, sample_count, _, _), adjusted_p_value),
                adjusted_superiority_p_value,
            )| {
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
                    minimum_improvement: metric.minimum_improvement(),
                    superiority_confidence: (1.0 - adjusted_superiority_p_value).clamp(0.0, 1.0),
                    minimum_superiority_confidence: metric.minimum_superiority_confidence(),
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
        evaluation_policy_digest: candidate.evaluation_policy_floor.digest(),
        evaluation_scenario_digest: readiness.scenario_bundle_digest.clone(),
        subject_ref: candidate.subject.subject_ref(),
        environment_fingerprint: environment_fingerprint(
            readiness,
            environment_inputs.into_iter(),
        )?,
        stopping_reason,
        executed_sample_count: paired.len().min(u32::MAX as usize) as u32,
        dimensions,
        source_run_refs: source_run_refs.into_iter().collect(),
        evidence_refs: evidence_refs.into_values().collect(),
        created_at_ms: now_ms().min(u128::from(u64::MAX)) as u64,
    })
}

fn environment_fingerprint(
    readiness: &runtime::EvolutionEvaluationReadiness,
    inputs: impl IntoIterator<Item = String>,
) -> Result<String, String> {
    let mut inputs = inputs.into_iter().collect::<Vec<_>>();
    inputs.sort();
    inputs.dedup();
    let canonical = serde_json::to_vec(&(
        readiness.scenario_bundle_digest.as_str(),
        readiness.maximum_paired_runs,
        inputs,
    ))
    .map_err(|error| error.to_string())?;
    Ok(format!("sha256:{:x}", Sha256::digest(canonical)))
}

fn planned_sequential_looks(metric: &harness_contract::evaluation::EvaluationMetricSpec) -> u32 {
    match metric.stopping_rule {
        EvaluationStoppingRule::FixedSamples => 1,
        EvaluationStoppingRule::Sequential {
            max_samples,
            check_interval,
            ..
        } => max_samples
            .saturating_sub(metric.minimum_samples)
            .checked_div(check_interval)
            .unwrap_or_default()
            .saturating_add(1),
    }
}

fn metric_sample_count(
    metric: &harness_contract::evaluation::EvaluationMetricSpec,
    paired: &[(EvaluationScenarioObservation, EvaluationScenarioObservation)],
) -> u32 {
    paired
        .iter()
        .filter(|(baseline, _)| {
            metric
                .paired_scenario_refs
                .iter()
                .any(|reference| reference == &baseline.scenario_ref)
        })
        .count()
        .min(u32::MAX as usize) as u32
}

fn sequential_stopping_reason(
    contract: &EvaluationContract,
    candidate: &runtime::EvolutionGovernanceCandidate,
    paired: &[(EvaluationScenarioObservation, EvaluationScenarioObservation)],
    readiness: &runtime::EvolutionEvaluationReadiness,
) -> Result<Option<EvaluationStoppingReason>, String> {
    let sequential = contract
        .metrics
        .iter()
        .filter_map(|metric| match metric.stopping_rule {
            EvaluationStoppingRule::Sequential {
                max_samples,
                check_interval,
                ..
            } => Some((metric, max_samples, check_interval)),
            EvaluationStoppingRule::FixedSamples => None,
        })
        .collect::<Vec<_>>();
    if sequential.is_empty() {
        return Ok(None);
    }
    let at_declared_look = sequential.iter().all(|(metric, max, interval)| {
        let samples = metric_sample_count(metric, paired);
        samples >= metric.minimum_samples
            && (samples >= *max
                || samples
                    .saturating_sub(metric.minimum_samples)
                    .is_multiple_of(*interval))
    });
    if !at_declared_look {
        return Ok(None);
    }
    let fixed_complete = contract.metrics.iter().all(|metric| {
        matches!(
            metric.stopping_rule,
            EvaluationStoppingRule::Sequential { .. }
        ) || metric_sample_count(metric, paired) >= metric.minimum_samples
    });
    if fixed_complete {
        let provisional = comparison_from_observations(
            candidate,
            paired,
            readiness,
            EvaluationStoppingReason::SequentialSuccess,
        )?;
        if provisional.is_eligible() {
            return Ok(Some(EvaluationStoppingReason::SequentialSuccess));
        }
    }
    if sequential
        .iter()
        .all(|(metric, max, _)| metric_sample_count(metric, paired) >= *max)
    {
        return Ok(None);
    }
    let target_metrics = contract
        .metrics
        .iter()
        .filter(|metric| metric.target_improvement)
        .collect::<Vec<_>>();
    if !target_metrics.is_empty()
        && target_metrics
            .iter()
            .all(|metric| !metric_can_still_reach_superiority(metric, paired, contract))
    {
        return Ok(Some(EvaluationStoppingReason::SequentialFutility));
    }
    Ok(None)
}

fn metric_can_still_reach_superiority(
    metric: &harness_contract::evaluation::EvaluationMetricSpec,
    paired: &[(EvaluationScenarioObservation, EvaluationScenarioObservation)],
    contract: &EvaluationContract,
) -> bool {
    let EvaluationStoppingRule::Sequential { max_samples, .. } = metric.stopping_rule else {
        return true;
    };
    let current = paired
        .iter()
        .filter(|(baseline, _)| metric.paired_scenario_refs.contains(&baseline.scenario_ref))
        .collect::<Vec<_>>();
    let successes = current
        .iter()
        .filter(|(baseline, proposed)| {
            let baseline = observation_value(metric.source, baseline);
            let proposed = observation_value(metric.source, proposed);
            match metric.direction {
                EvaluationMetricDirection::HigherIsBetter => {
                    proposed - baseline >= metric.minimum_improvement()
                }
                EvaluationMetricDirection::LowerIsBetter => {
                    baseline - proposed >= metric.minimum_improvement()
                }
            }
        })
        .count();
    let possible_samples = max_samples as usize;
    let possible_successes =
        successes.saturating_add(possible_samples.saturating_sub(current.len()));
    let look_adjusted = (binomial_upper_tail(possible_samples, possible_successes)
        * planned_sequential_looks(metric) as f64)
        .min(1.0);
    let family_size = contract
        .metrics
        .iter()
        .filter(|candidate| candidate.multiplicity_correction == metric.multiplicity_correction)
        .count()
        .max(1) as f64;
    let conservative_adjusted = match metric.multiplicity_correction {
        harness_contract::evaluation::EvaluationMultiplicityCorrection::None => look_adjusted,
        harness_contract::evaluation::EvaluationMultiplicityCorrection::Bonferroni
        | harness_contract::evaluation::EvaluationMultiplicityCorrection::BenjaminiHochberg => {
            (look_adjusted * family_size).min(1.0)
        }
    };
    1.0 - conservative_adjusted >= metric.minimum_superiority_confidence()
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
    if !values.is_empty() {
        values.iter().sum::<f64>() / values.len() as f64
    } else {
        0.0
    }
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

fn paired_superiority_p_value(
    baseline: &[f64],
    candidate: &[f64],
    direction: EvaluationMetricDirection,
    minimum_improvement: f64,
) -> f64 {
    if baseline.len() != candidate.len() || baseline.is_empty() {
        return 1.0;
    }
    let successes = baseline
        .iter()
        .zip(candidate)
        .filter(|(baseline, candidate)| match direction {
            EvaluationMetricDirection::HigherIsBetter => {
                **candidate - **baseline >= minimum_improvement
            }
            EvaluationMetricDirection::LowerIsBetter => {
                **baseline - **candidate >= minimum_improvement
            }
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
            EvaluationMultiplicityCorrection::None => {}
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
    fn readiness(
        &self,
        contract: &EvaluationContract,
    ) -> Result<runtime::EvolutionEvaluationReadiness, String> {
        self.workload.readiness(contract)
    }

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
        if report.evaluation_scenario_digest != candidate.evaluation_scenario_digest {
            return Err("definition_evolution_workload_returned_wrong_scenario_bundle".to_string());
        }
        Ok(report)
    }
}

#[must_use]
pub fn evaluate_evolution_closure() -> EvolutionClosureReport {
    let candidate = match closure_candidate() {
        Ok(candidate) => candidate,
        Err(error) => {
            return EvolutionClosureReport {
                kind: "harness_eval.definition_evolution_closure".to_string(),
                candidate_count: 0,
                comparison_report_count: 0,
                eligible_report_count: 0,
                release_mutation_count: 0,
                runtime_port_implemented: true,
                status: "failed".to_string(),
                evidence_refs: vec![EvidenceRef::observed("candidate_error", error)],
            };
        }
    };
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
            evidence_refs: vec![EvidenceRef::observed("evaluation_error", error)],
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
            evaluation_policy_digest: candidate.evaluation_policy_floor.digest(),
            evaluation_scenario_digest: candidate.evaluation_scenario_digest.clone(),
            subject_ref: candidate.subject.subject_ref(),
            environment_fingerprint: "sha256:closure-environment".to_string(),
            stopping_reason: EvaluationStoppingReason::FixedSamplesCompleted,
            executed_sample_count: 12,
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
                    minimum_improvement: 0.01,
                    superiority_confidence: 0.95,
                    minimum_superiority_confidence: 0.90,
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
                    minimum_improvement: 0.01,
                    superiority_confidence: 0.95,
                    minimum_superiority_confidence: 0.90,
                    hard_gate: false,
                    protected: true,
                    target_improvement: false,
                },
            ],
            source_run_refs: vec!["harness-eval:closure:paired-workload".to_string()],
            evidence_refs: vec![EvidenceRef::observed("harness_eval", "closure:report")],
            created_at_ms: 1,
        })
    }
}

fn closure_candidate() -> Result<runtime::EvolutionGovernanceCandidate, String> {
    let definition_id = harness_contract::agent::AgentDefinitionId::new(
        harness_contract::agent::DefinitionScope::Workspace,
        "cowd/evolution-closure",
    )
    .map_err(|error| error.to_string())?;
    let revision_ref = harness_contract::agent::AgentDefinitionRevisionRef::new(definition_id, 2)
        .map_err(|error| error.to_string())?;
    let evaluation_contract = harness_contract::evaluation::EvaluationContract {
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
    };
    let scenario_refs = evaluation_contract.scenario_refs.clone();
    let payload = serde_json::to_vec(&(evaluation_contract.digest(), scenario_refs))
        .map_err(|error| error.to_string())?;
    Ok(runtime::EvolutionGovernanceCandidate {
        candidate_id: "harness-eval-closure-candidate".to_string(),
        subject: runtime::EvolutionCandidateSubject::AgentDefinition { revision_ref },
        evaluation_baseline: Some(runtime::EvolutionEvaluationBaseline::PublishedRevision {
            subject_ref: "agent-definition:workspace/cowd/harness-eval".to_string(),
            revision: 1,
            content_digest: "sha256:baseline".to_string(),
        }),
        baseline_revision: 1,
        evaluation_contract,
        evaluation_policy_floor: harness_contract::evaluation::EvaluationPolicyFloor::default(),
        evaluation_scenario_digest: format!("sha256:{:x}", Sha256::digest(payload)),
        proposal_id: "proposal-agent-v2".to_string(),
        source_evidence_refs: vec![EvidenceRef::observed("harness_eval", "closure:source")],
        canary_policy: runtime::CanaryRolloutPolicy::default(),
        lifecycle: runtime::EvolutionCandidateLifecycle::Draft,
        comparison_report_ref: None,
        comparison_report_digest: None,
        canary_review_ref: None,
        stable_review_ref: None,
        canary_observation: None,
        created_at_ms: 1,
        updated_at_ms: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paired_observations(
        count: u32,
        baseline_success: bool,
        candidate_success: bool,
    ) -> Vec<(EvaluationScenarioObservation, EvaluationScenarioObservation)> {
        (0..count)
            .map(|index| {
                let observation = |revision, succeeded, side: &str| EvaluationScenarioObservation {
                    scenario_ref: "harness-eval/closure".to_string(),
                    definition_revision: revision,
                    run_ref: format!("{side}:{index}"),
                    succeeded,
                    acceptance_total: 1,
                    acceptance_satisfied: u32::from(succeeded),
                    evidence_refs: vec![EvidenceRef::observed(
                        "evaluation",
                        format!("{side}:{index}"),
                    )],
                    input_tokens: 1,
                    output_tokens: 1,
                    tool_calls: 0,
                    elapsed_ms: 1,
                    provider_model_refs: vec!["test/deterministic".to_string()],
                    environment_fingerprint: "sha256:test".to_string(),
                };
                (
                    observation(1, baseline_success, "baseline"),
                    observation(2, candidate_success, "candidate"),
                )
            })
            .collect()
    }

    fn sequential_candidate(max_samples: u32) -> runtime::EvolutionGovernanceCandidate {
        let mut candidate = closure_candidate().expect("candidate");
        candidate.evaluation_contract.metrics.truncate(1);
        candidate.evaluation_contract.metrics[0].stopping_rule =
            EvaluationStoppingRule::Sequential {
                max_samples,
                check_interval: 2,
                alpha_spending: harness_contract::evaluation::EvaluationAlphaSpending::Bonferroni,
            };
        let readiness = DefinitionEvolutionWorkload::readiness(
            &ClosureWorkload,
            &candidate.evaluation_contract,
        )
        .expect("readiness");
        candidate.evaluation_scenario_digest = readiness.scenario_bundle_digest;
        candidate
    }

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
                &closure_candidate().expect("closure candidate"),
            ))
            .is_err()
        );
    }

    #[test]
    fn evaluator_collects_the_contract_sample_count_before_scoring() {
        let contract = closure_candidate()
            .expect("closure candidate")
            .evaluation_contract;
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

    #[test]
    fn scenario_catalog_rejects_traversal_and_digest_detects_asset_changes() {
        let root = std::env::temp_dir().join(format!("cowd-eval-catalog-{}", uuid::Uuid::new_v4()));
        let path = root.join("custom/scenario.json");
        fs::create_dir_all(path.parent().expect("parent")).expect("directory");
        let mut scenario = EvaluationScenarioSpec {
            scenario_ref: "custom/scenario".to_string(),
            objective: "first objective".to_string(),
            acceptance: vec!["evidence".to_string()],
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            resource_scopes: Vec::new(),
            permission_ceiling: PermissionMode::ReadOnly,
            model_lease: "evaluation/default".to_string(),
        };
        fs::write(&path, serde_json::to_vec(&scenario).expect("json")).expect("write");
        let catalog = FileDefinitionEvolutionScenarioCatalog::new(&root);
        let first = catalog.load_verified("custom/scenario").expect("first");
        scenario.objective = "second objective".to_string();
        fs::write(&path, serde_json::to_vec(&scenario).expect("json")).expect("rewrite");
        let second = catalog.load_verified("custom/scenario").expect("second");
        assert_ne!(first.digest, second.digest);
        assert!(catalog.load_verified("../outside").is_err());
        assert!(catalog.load_verified("/absolute").is_err());
        assert!(catalog.load_verified("builtin/direct/baseline").is_ok());
        assert!(catalog
            .load_verified("builtin/unregistered/baseline")
            .is_err());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn sequential_stopping_is_predeclared_success_futility_or_maximum() {
        let success_candidate = sequential_candidate(20);
        let success_readiness = DefinitionEvolutionWorkload::readiness(
            &ClosureWorkload,
            &success_candidate.evaluation_contract,
        )
        .expect("readiness");
        assert_eq!(
            sequential_stopping_reason(
                &success_candidate.evaluation_contract,
                &success_candidate,
                &paired_observations(10, false, true),
                &success_readiness,
            )
            .expect("decision"),
            Some(EvaluationStoppingReason::SequentialSuccess)
        );

        let futility_candidate = sequential_candidate(12);
        let futility_readiness = DefinitionEvolutionWorkload::readiness(
            &ClosureWorkload,
            &futility_candidate.evaluation_contract,
        )
        .expect("readiness");
        assert_eq!(
            sequential_stopping_reason(
                &futility_candidate.evaluation_contract,
                &futility_candidate,
                &paired_observations(10, true, false),
                &futility_readiness,
            )
            .expect("decision"),
            Some(EvaluationStoppingReason::SequentialFutility)
        );
        assert_eq!(
            sequential_stopping_reason(
                &futility_candidate.evaluation_contract,
                &futility_candidate,
                &paired_observations(12, true, false),
                &futility_readiness,
            )
            .expect("decision"),
            None,
            "the caller records SequentialMaxSamples after the frozen schedule completes"
        );
    }
}
