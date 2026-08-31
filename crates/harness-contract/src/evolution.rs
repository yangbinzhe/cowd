//! Pure contracts for model-assisted evolution analysis.
//!
//! Model output is always a hypothesis-bearing Draft. These contracts carry
//! no release authority, executable Definition, Skill activation, or code
//! mutation capability.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const EVOLUTION_ANALYSIS_CONTRACT_VERSION: &str = "evolution-analysis-draft/v1";
pub const COLLABORATION_EXPERIENCE_SCHEMA_VERSION: u16 = 1;
pub const COLLABORATION_SIGNATURE_NORMALIZER_REVISION: u16 = 1;
pub const COLLABORATION_PATTERN_SCHEMA_VERSION: u16 = 1;
pub const MINIMUM_PATTERN_DISTINCT_TURNS: usize = 3;
pub const MAX_COLLABORATION_EPISODE_EVIDENCE_REFS: usize = 64;
pub const MAX_COLLABORATION_EPISODE_PAYLOAD_BYTES: usize = 32 * 1024;

/// Stable identity for a frozen subset of advisory evidence.  The Runtime
/// verifies this against the durable pattern projection before it admits a
/// first-revision candidate; callers cannot invent an episode baseline by
/// choosing arbitrary ids.
#[must_use]
pub fn collaboration_episode_set_digest(
    semantic_signature_digest: &str,
    episode_ids: &[String],
) -> String {
    let mut canonical_ids = episode_ids.to_vec();
    normalize_strings(&mut canonical_ids);
    let payload =
        serde_json::to_vec(&(semantic_signature_digest, canonical_ids)).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(payload))
}

/// A name-free semantic workstream shape. Display labels and prompt content
/// are deliberately absent so reusable experience cannot be keyed by prose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticWorkstreamShape {
    pub ordinal: u16,
    pub multiplicity_min: u16,
    pub multiplicity_max: u16,
    pub required_capability_ids: Vec<String>,
    pub required_skill_ids: Vec<String>,
    pub required_tool_capabilities: Vec<String>,
    pub acceptance_kinds: Vec<String>,
    pub result_field_shapes: Vec<String>,
}

/// Canonical producer-to-consumer dataflow. Ordinals refer only to the
/// normalized workstream ordering, never to Team or role display names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticDependencyShape {
    pub producer_ordinal: u16,
    pub consumer_ordinal: u16,
    pub required_artifact_kinds: Vec<String>,
    pub required_fact_kinds: Vec<String>,
    pub requires_committed_effect: bool,
    pub requires_satisfied_acceptance: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationSemanticSignature {
    pub normalizer_revision: u16,
    pub workstream_shapes: Vec<SemanticWorkstreamShape>,
    pub dependency_shapes: Vec<SemanticDependencyShape>,
    pub required_capability_ids: Vec<String>,
    pub required_skill_ids: Vec<String>,
    pub required_tool_capabilities: Vec<String>,
    pub acceptance_kinds: Vec<String>,
    pub result_field_shapes: Vec<String>,
}

impl CollaborationSemanticSignature {
    /// Normalization is intentionally total and deterministic. Callers may
    /// provide unordered sets, but never labels or payload text.
    #[must_use]
    pub fn normalized(mut self) -> Self {
        self.normalizer_revision = COLLABORATION_SIGNATURE_NORMALIZER_REVISION;
        normalize_strings(&mut self.required_capability_ids);
        normalize_strings(&mut self.required_skill_ids);
        normalize_strings(&mut self.required_tool_capabilities);
        normalize_strings(&mut self.acceptance_kinds);
        normalize_strings(&mut self.result_field_shapes);
        for workstream in &mut self.workstream_shapes {
            normalize_strings(&mut workstream.required_capability_ids);
            normalize_strings(&mut workstream.required_skill_ids);
            normalize_strings(&mut workstream.required_tool_capabilities);
            normalize_strings(&mut workstream.acceptance_kinds);
            normalize_strings(&mut workstream.result_field_shapes);
        }
        self.workstream_shapes.sort_by_key(|shape| shape.ordinal);
        for (ordinal, workstream) in self.workstream_shapes.iter_mut().enumerate() {
            workstream.ordinal = ordinal.min(u16::MAX as usize) as u16;
            workstream.multiplicity_min = workstream.multiplicity_min.max(1);
            if workstream.multiplicity_max < workstream.multiplicity_min {
                workstream.multiplicity_max = workstream.multiplicity_min;
            }
        }
        for dependency in &mut self.dependency_shapes {
            normalize_strings(&mut dependency.required_artifact_kinds);
            normalize_strings(&mut dependency.required_fact_kinds);
        }
        self.dependency_shapes.sort_by(|left, right| {
            (
                left.producer_ordinal,
                left.consumer_ordinal,
                &left.required_artifact_kinds,
                &left.required_fact_kinds,
                left.requires_committed_effect,
                left.requires_satisfied_acceptance,
            )
                .cmp(&(
                    right.producer_ordinal,
                    right.consumer_ordinal,
                    &right.required_artifact_kinds,
                    &right.required_fact_kinds,
                    right.requires_committed_effect,
                    right.requires_satisfied_acceptance,
                ))
        });
        self.dependency_shapes.dedup();
        self
    }

    #[must_use]
    pub fn digest(&self) -> String {
        let normalized = self.clone().normalized();
        let bytes = serde_json::to_vec(&normalized).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(bytes))
    }
}

fn normalize_strings(values: &mut Vec<String>) {
    values.retain(|value| !value.trim().is_empty());
    values.sort();
    values.dedup();
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationExperienceOutcome {
    Completed,
    IntentGap,
    BindingGap,
    Denied,
    Cancelled,
    Partial,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationEvidenceCoverage {
    pub required_obligation_count: u32,
    pub satisfied_obligation_count: u32,
    pub coverage_basis_points: u16,
    pub reusable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationResourceSummary {
    pub parallel_demand: u16,
    pub context_reservation_tokens: u64,
    pub output_reservation_tokens: u64,
}

/// Durable terminal-only episode. Identity inputs are opaque digests and
/// reference identifiers; this contract has no user content or model payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationExperienceEpisode {
    pub schema_version: u16,
    pub episode_id: String,
    pub session_ref_hash: String,
    pub turn_ref_hash: String,
    pub program_id: String,
    pub program_revision: u64,
    pub intent_digest: String,
    pub binding_digest: String,
    pub capacity_profile_digest: String,
    pub approval_policy_digest: String,
    pub semantic_signature: CollaborationSemanticSignature,
    pub outcome: CollaborationExperienceOutcome,
    pub evidence_refs: Vec<String>,
    pub coverage: CollaborationEvidenceCoverage,
    pub latency_ms: u64,
    pub resource_summary: CollaborationResourceSummary,
    pub completed_at_ms: u64,
}

impl CollaborationExperienceEpisode {
    #[must_use]
    pub fn deterministic_id(program_id: &str, program_revision: u64) -> String {
        let bytes = format!(
            "{}\0{}\0{}",
            COLLABORATION_EXPERIENCE_SCHEMA_VERSION, program_id, program_revision
        );
        format!("experience:{:x}", Sha256::digest(bytes))
    }

    #[must_use]
    pub fn is_pattern_eligible(&self) -> bool {
        self.schema_version == COLLABORATION_EXPERIENCE_SCHEMA_VERSION
            && self.outcome == CollaborationExperienceOutcome::Completed
            && self.coverage.reusable
            && self.coverage.required_obligation_count > 0
            && self.coverage.required_obligation_count == self.coverage.satisfied_obligation_count
            && self.coverage.coverage_basis_points == 10_000
            && !self.session_ref_hash.trim().is_empty()
            && !self.turn_ref_hash.trim().is_empty()
            && !self.intent_digest.trim().is_empty()
            && !self.binding_digest.trim().is_empty()
            && !self.capacity_profile_digest.trim().is_empty()
            && !self.approval_policy_digest.trim().is_empty()
            && self.completed_at_ms > 0
            && !self.evidence_refs.is_empty()
            && self.evidence_refs.len() <= MAX_COLLABORATION_EPISODE_EVIDENCE_REFS
            && self
                .evidence_refs
                .iter()
                .all(|reference| !reference.trim().is_empty() && reference.len() <= 512)
            && serde_json::to_vec(self)
                .is_ok_and(|payload| payload.len() <= MAX_COLLABORATION_EPISODE_PAYLOAD_BYTES)
    }
}

/// Evidence-backed reusable semantic shape. It is deliberately advisory: a
/// pattern contains neither executable topology nor release authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationSemanticPattern {
    pub schema_version: u16,
    pub pattern_id: String,
    pub pattern_revision: u64,
    pub signature_digest: String,
    pub semantic_signature: CollaborationSemanticSignature,
    pub semantic_suggestion: SemanticCollaborationSuggestion,
    pub evidence_summary: PatternEvidenceSummary,
    pub lifecycle: SemanticPatternLifecycle,
    pub qualifying_episode_ids: Vec<String>,
    pub distinct_turn_ref_hashes: Vec<String>,
    pub support_count: u32,
    pub latest_completed_at_ms: u64,
}

/// Safe, structural advice returned to the compiler after lower-precedence
/// intent sources have been evaluated. It has no label, Definition ref,
/// permission, approval, or executable graph field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticCollaborationSuggestion {
    pub required_capability_ids: Vec<String>,
    pub required_skill_ids: Vec<String>,
    pub required_tool_capabilities: Vec<String>,
    pub dependency_shapes: Vec<SemanticDependencyShape>,
    pub acceptance_kinds: Vec<String>,
    pub result_field_shapes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatternEvidenceSummary {
    pub eligible_episode_count: u32,
    pub distinct_turn_count: u32,
    pub evidence_ref_count: u32,
    pub coverage_basis_points: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticPatternLifecycle {
    Advisory,
    CandidateCreated,
    Superseded,
    Ineligible,
    Withdrawn,
}

impl CollaborationSemanticPattern {
    #[must_use]
    pub fn deterministic_id(signature_digest: &str) -> String {
        format!("pattern:{:x}", Sha256::digest(signature_digest.as_bytes()))
    }

    #[must_use]
    pub fn is_actionable(&self) -> bool {
        self.schema_version == COLLABORATION_PATTERN_SCHEMA_VERSION
            && matches!(
                self.lifecycle,
                SemanticPatternLifecycle::Advisory | SemanticPatternLifecycle::CandidateCreated
            )
            && self.qualifying_episode_ids.len() >= MINIMUM_PATTERN_DISTINCT_TURNS
            && self.distinct_turn_ref_hashes.len() >= MINIMUM_PATTERN_DISTINCT_TURNS
            && self.support_count as usize == self.qualifying_episode_ids.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionAnalysisCandidateKind {
    AgentDefinition,
    TeamTemplate,
    Strategy,
    Skill,
    Tool,
    Connector,
    Runtime,
    Surface,
    CodePatch,
    ArchitecturePlan,
    TestScenario,
    Contract,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionAnalysisHypothesis {
    pub hypothesis_id: String,
    pub statement: String,
    pub supporting_evidence_refs: Vec<String>,
    pub contradicting_evidence_refs: Vec<String>,
    pub uncertainty: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionFalsificationExperiment {
    pub target_hypothesis_id: String,
    pub objective: String,
    pub method: Vec<String>,
    pub pass_criterion: String,
    pub falsification_criterion: String,
    pub required_evidence_refs: Vec<String>,
}

/// The only JSON shape a Provider is allowed to return.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionAnalysisModelOutput {
    pub hypotheses: Vec<EvolutionAnalysisHypothesis>,
    pub falsification_experiment: EvolutionFalsificationExperiment,
    pub suggested_candidate_kind: EvolutionAnalysisCandidateKind,
    pub acceptance_scenarios: Vec<String>,
    pub expected_value: String,
    pub risks: Vec<String>,
    pub unknowns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionAnalysisUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub stop_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionAnalysisDraft {
    pub analysis_id: String,
    pub case_id: String,
    pub case_digest: String,
    pub contract_digest: String,
    pub input_digest: String,
    pub output_digest: String,
    pub provider: String,
    pub model: String,
    pub request_id: Option<String>,
    pub evidence_refs: Vec<String>,
    pub output: EvolutionAnalysisModelOutput,
    pub usage: EvolutionAnalysisUsage,
    pub created_at_ms: u64,
}

impl EvolutionAnalysisModelOutput {
    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(bytes))
    }
}

impl EvolutionAnalysisDraft {
    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        format!("sha256:{:x}", Sha256::digest(bytes))
    }
}

#[must_use]
pub fn evolution_analysis_contract_digest() -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(EVOLUTION_ANALYSIS_CONTRACT_VERSION.as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signature() -> CollaborationSemanticSignature {
        CollaborationSemanticSignature {
            normalizer_revision: 99,
            workstream_shapes: vec![SemanticWorkstreamShape {
                ordinal: 42,
                multiplicity_min: 2,
                multiplicity_max: 1,
                required_capability_ids: vec!["cap.write".to_string(), "cap.read".to_string()],
                required_skill_ids: vec!["skill.audit".to_string()],
                required_tool_capabilities: vec!["tool.fs".to_string()],
                acceptance_kinds: vec!["evidence".to_string()],
                result_field_shapes: vec!["report".to_string()],
            }],
            dependency_shapes: vec![SemanticDependencyShape {
                producer_ordinal: 0,
                consumer_ordinal: 1,
                required_artifact_kinds: vec!["report".to_string()],
                required_fact_kinds: vec!["verified".to_string()],
                requires_committed_effect: false,
                requires_satisfied_acceptance: true,
            }],
            required_capability_ids: vec!["cap.write".to_string(), "cap.read".to_string()],
            required_skill_ids: vec!["skill.audit".to_string()],
            required_tool_capabilities: vec!["tool.fs".to_string()],
            acceptance_kinds: vec!["evidence".to_string()],
            result_field_shapes: vec!["report".to_string()],
        }
    }

    #[test]
    fn collaboration_signature_is_order_independent_and_name_free() {
        let left = signature();
        let mut right = signature();
        right.required_capability_ids.reverse();
        right.workstream_shapes[0].required_capability_ids.reverse();
        right.workstream_shapes[0].multiplicity_max = 0;

        let normalized = right.normalized();
        assert_eq!(
            normalized.normalizer_revision,
            COLLABORATION_SIGNATURE_NORMALIZER_REVISION
        );
        assert_eq!(normalized.workstream_shapes[0].ordinal, 0);
        assert_eq!(normalized.workstream_shapes[0].multiplicity_max, 2);
        assert_eq!(left.digest(), normalized.digest());
        let encoded = serde_json::to_string(&left).expect("signature serializes");
        assert!(!encoded.contains("display_name"));
        assert!(!encoded.contains("prompt"));
    }

    #[test]
    fn collaboration_signature_captures_cardinality_acceptance_and_result_shape() {
        let baseline = signature().normalized();
        let baseline_digest = baseline.digest();

        let mut cardinality = baseline.clone();
        cardinality.workstream_shapes[0].multiplicity_max = 3;
        assert_ne!(baseline_digest, cardinality.digest());

        let mut acceptance = baseline.clone();
        acceptance.workstream_shapes[0]
            .acceptance_kinds
            .push("schema".to_string());
        acceptance.acceptance_kinds.push("schema".to_string());
        assert_ne!(baseline_digest, acceptance.digest());

        let mut result = baseline.clone();
        result.workstream_shapes[0]
            .result_field_shapes
            .push("confidence".to_string());
        result.result_field_shapes.push("confidence".to_string());
        assert_ne!(baseline_digest, result.digest());
    }

    #[test]
    fn terminal_episode_id_and_eligibility_are_idempotent() {
        let episode = CollaborationExperienceEpisode {
            schema_version: COLLABORATION_EXPERIENCE_SCHEMA_VERSION,
            episode_id: CollaborationExperienceEpisode::deterministic_id("program-a", 7),
            session_ref_hash: "sha256:session".to_string(),
            turn_ref_hash: "sha256:turn-a".to_string(),
            program_id: "program-a".to_string(),
            program_revision: 7,
            intent_digest: "sha256:intent".to_string(),
            binding_digest: "sha256:binding".to_string(),
            capacity_profile_digest: "sha256:capacity".to_string(),
            approval_policy_digest: "sha256:approval".to_string(),
            semantic_signature: signature(),
            outcome: CollaborationExperienceOutcome::Completed,
            evidence_refs: vec!["evidence:terminal".to_string()],
            coverage: CollaborationEvidenceCoverage {
                required_obligation_count: 2,
                satisfied_obligation_count: 2,
                coverage_basis_points: 10_000,
                reusable: true,
            },
            latency_ms: 42,
            resource_summary: CollaborationResourceSummary {
                parallel_demand: 2,
                context_reservation_tokens: 100,
                output_reservation_tokens: 20,
            },
            completed_at_ms: 99,
        };
        assert_eq!(
            episode.episode_id,
            CollaborationExperienceEpisode::deterministic_id("program-a", 7)
        );
        assert!(episode.is_pattern_eligible());
        let mut retry = episode.clone();
        retry.turn_ref_hash = "sha256:turn-b".to_string();
        assert_ne!(retry.turn_ref_hash, episode.turn_ref_hash);
        assert_eq!(
            retry.episode_id, episode.episode_id,
            "replay identity is Program-fenced"
        );
        retry.coverage.satisfied_obligation_count = 1;
        assert!(!retry.is_pattern_eligible());

        let mut missing_policy = episode.clone();
        missing_policy.approval_policy_digest.clear();
        assert!(!missing_policy.is_pattern_eligible());
        let mut missing_capacity = episode.clone();
        missing_capacity.capacity_profile_digest.clear();
        assert!(!missing_capacity.is_pattern_eligible());
        let mut missing_session = episode;
        missing_session.session_ref_hash.clear();
        assert!(!missing_session.is_pattern_eligible());
    }

    #[test]
    fn semantic_pattern_requires_three_distinct_eligible_turns() {
        let mut episode_ids = vec![
            CollaborationExperienceEpisode::deterministic_id("program-a", 1),
            CollaborationExperienceEpisode::deterministic_id("program-b", 1),
            CollaborationExperienceEpisode::deterministic_id("program-c", 1),
        ];
        episode_ids.sort();
        let signature_digest = signature().digest();
        let pattern = CollaborationSemanticPattern {
            schema_version: COLLABORATION_PATTERN_SCHEMA_VERSION,
            pattern_id: CollaborationSemanticPattern::deterministic_id(&signature_digest),
            pattern_revision: 1,
            signature_digest,
            semantic_signature: signature(),
            semantic_suggestion: SemanticCollaborationSuggestion {
                required_capability_ids: vec!["cap.read".to_string(), "cap.write".to_string()],
                required_skill_ids: vec!["skill.audit".to_string()],
                required_tool_capabilities: vec!["tool.fs".to_string()],
                dependency_shapes: signature().dependency_shapes,
                acceptance_kinds: vec!["evidence".to_string()],
                result_field_shapes: vec!["report".to_string()],
            },
            evidence_summary: PatternEvidenceSummary {
                eligible_episode_count: 3,
                distinct_turn_count: 3,
                evidence_ref_count: 3,
                coverage_basis_points: 10_000,
            },
            lifecycle: SemanticPatternLifecycle::Advisory,
            qualifying_episode_ids: episode_ids,
            distinct_turn_ref_hashes: vec![
                "sha256:turn-1".to_string(),
                "sha256:turn-2".to_string(),
                "sha256:turn-3".to_string(),
            ],
            support_count: 3,
            latest_completed_at_ms: 10,
        };
        assert!(pattern.is_actionable());
        let mut duplicate_turn = pattern.clone();
        duplicate_turn.distinct_turn_ref_hashes.pop();
        assert!(!duplicate_turn.is_actionable());
        let mut duplicate_episode = pattern;
        duplicate_episode.qualifying_episode_ids.pop();
        duplicate_episode.support_count = 2;
        assert!(!duplicate_episode.is_actionable());
        duplicate_episode.qualifying_episode_ids.push(
            CollaborationExperienceEpisode::deterministic_id("program-c", 1),
        );
        duplicate_episode.support_count = 3;
        duplicate_episode.lifecycle = SemanticPatternLifecycle::Withdrawn;
        assert!(!duplicate_episode.is_actionable());
    }

    #[test]
    fn episode_set_digest_is_order_independent_and_duplicate_free() {
        let signature_digest = signature().digest();
        let left = collaboration_episode_set_digest(
            &signature_digest,
            &[
                "experience:three".to_string(),
                "experience:one".to_string(),
                "experience:two".to_string(),
            ],
        );
        let right = collaboration_episode_set_digest(
            &signature_digest,
            &[
                "experience:two".to_string(),
                "experience:one".to_string(),
                "experience:three".to_string(),
                "experience:one".to_string(),
            ],
        );
        assert_eq!(left, right);
        assert_ne!(
            left,
            collaboration_episode_set_digest("sha256:other", &["experience:one".to_string()])
        );
    }

    #[test]
    fn model_output_contract_rejects_unknown_fields_and_has_stable_digest() {
        let value = serde_json::json!({
            "hypotheses": [{
                "hypothesis_id": "h1",
                "statement": "hypothesis",
                "supporting_evidence_refs": ["observed:test:e1"],
                "contradicting_evidence_refs": ["observed:test:e2"],
                "uncertainty": "unknown"
            }],
            "falsification_experiment": {
                "target_hypothesis_id": "h1",
                "objective": "test",
                "method": ["run"],
                "pass_criterion": "pass",
                "falsification_criterion": "fail",
                "required_evidence_refs": ["observed:test:e1"]
            },
            "suggested_candidate_kind": "architecture_plan",
            "acceptance_scenarios": ["scenario"],
            "expected_value": "value",
            "risks": ["risk"],
            "unknowns": ["unknown"]
        });
        let output: EvolutionAnalysisModelOutput =
            serde_json::from_value(value.clone()).expect("typed output");
        assert_eq!(output.digest(), output.digest());
        let mut unknown = value;
        unknown
            .as_object_mut()
            .expect("object")
            .insert("auto_publish".to_string(), serde_json::Value::Bool(true));
        assert!(serde_json::from_value::<EvolutionAnalysisModelOutput>(unknown).is_err());
    }
}
