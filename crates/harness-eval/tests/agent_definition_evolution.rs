#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{
    fs,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use harness_contract::{
    agent::{AgentDefinitionId, AgentDefinitionRevisionRef, DefinitionScope},
    evaluation::{
        EvaluationContract, EvaluationPolicyFloor, EvaluationScenarioObservation,
        EvaluationScenarioSpec,
    },
};
use harness_eval::{
    DefinitionEvolutionEvalRunner, DefinitionEvolutionScenarioExecutor,
    FileDefinitionEvolutionScenarioCatalog, RuntimeDefinitionEvolutionWorkload,
};
use runtime::{
    CanaryRolloutPolicy, EvolutionCandidateLifecycle, EvolutionCandidateSubject,
    EvolutionEvalRunner, EvolutionGovernanceCandidate,
};

#[test]
fn paired_definition_workload_executes_contract_samples_and_returns_runtime_owned_evidence() {
    let root = std::env::temp_dir().join(format!("cowd-evolution-eval-{}", uuid::Uuid::new_v4()));
    let scenario_path = root.join("evolution/agent-definition.json");
    fs::create_dir_all(scenario_path.parent().expect("scenario parent")).expect("scenario root");
    let scenario = EvaluationScenarioSpec {
        scenario_ref: "evolution/agent-definition".to_string(),
        objective: "Compare the baseline and candidate Definition under the same workload."
            .to_string(),
        acceptance: vec!["evidence".to_string()],
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        resource_scopes: vec!["read:crates/runtime".to_string()],
        permission_lease: "read_only".to_string(),
        model_lease: "default".to_string(),
    };
    fs::write(
        &scenario_path,
        serde_json::to_vec(&scenario).expect("scenario JSON"),
    )
    .expect("write scenario");

    let calls = Arc::new(AtomicU32::new(0));
    let workload = RuntimeDefinitionEvolutionWorkload::new(
        Arc::new(FileDefinitionEvolutionScenarioCatalog::new(&root)),
        Arc::new(DeterministicExecutor {
            calls: Arc::clone(&calls),
        }),
    );
    let runner = DefinitionEvolutionEvalRunner::new(Arc::new(workload));
    let candidate = candidate();
    let report = futures::executor::block_on(EvolutionEvalRunner::evaluate(&runner, &candidate))
        .expect("paired definition evaluation");

    assert_eq!(calls.load(Ordering::SeqCst), 10);
    assert_eq!(report.candidate_id, candidate.candidate_id);
    assert_eq!(
        report.evaluation_contract_digest,
        candidate.evaluation_contract_digest()
    );
    assert!(report.is_eligible());
    assert_eq!(report.dimensions[0].sample_count, 10);
    assert!(report.dimensions[0].candidate > report.dimensions[0].baseline);
    assert_eq!(report.source_run_refs.len(), 20);
    assert_eq!(report.evidence_refs.len(), 20);

    fs::remove_dir_all(root).expect("cleanup scenario root");
}

#[test]
fn evaluator_rejects_observations_not_bound_to_candidate_revision() {
    let root =
        std::env::temp_dir().join(format!("cowd-evolution-eval-bad-{}", uuid::Uuid::new_v4()));
    let scenario_path = root.join("evolution/agent-definition.json");
    fs::create_dir_all(scenario_path.parent().expect("scenario parent")).expect("scenario root");
    fs::write(
        &scenario_path,
        serde_json::to_vec(&EvaluationScenarioSpec {
            scenario_ref: "evolution/agent-definition".to_string(),
            objective: "Bind observations to exact revisions.".to_string(),
            acceptance: vec!["evidence".to_string()],
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            resource_scopes: vec!["read:crates/runtime".to_string()],
            permission_lease: "read_only".to_string(),
            model_lease: "default".to_string(),
        })
        .expect("scenario JSON"),
    )
    .expect("write scenario");
    let workload = RuntimeDefinitionEvolutionWorkload::new(
        Arc::new(FileDefinitionEvolutionScenarioCatalog::new(&root)),
        Arc::new(WrongCandidateRevisionExecutor),
    );
    let report = futures::executor::block_on(
        harness_eval::DefinitionEvolutionWorkload::evaluate_definition(&workload, &candidate()),
    );

    assert!(matches!(
        report,
        Err(error) if error == "evaluation_runtime_observation_binding_mismatch"
    ));
    fs::remove_dir_all(root).expect("cleanup scenario root");
}

struct DeterministicExecutor {
    calls: Arc<AtomicU32>,
}

#[async_trait]
impl DefinitionEvolutionScenarioExecutor for DeterministicExecutor {
    async fn execute(
        &self,
        candidate_id: &str,
        scenario: &EvaluationScenarioSpec,
        sample_index: u32,
    ) -> Result<(EvaluationScenarioObservation, EvaluationScenarioObservation), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok((
            observation(candidate_id, scenario, sample_index, 1, false),
            observation(candidate_id, scenario, sample_index, 2, true),
        ))
    }
}

struct WrongCandidateRevisionExecutor;

#[async_trait]
impl DefinitionEvolutionScenarioExecutor for WrongCandidateRevisionExecutor {
    async fn execute(
        &self,
        candidate_id: &str,
        scenario: &EvaluationScenarioSpec,
        sample_index: u32,
    ) -> Result<(EvaluationScenarioObservation, EvaluationScenarioObservation), String> {
        Ok((
            observation(candidate_id, scenario, sample_index, 1, false),
            observation(candidate_id, scenario, sample_index, 99, true),
        ))
    }
}

fn observation(
    candidate_id: &str,
    scenario: &EvaluationScenarioSpec,
    sample_index: u32,
    definition_revision: u64,
    candidate: bool,
) -> EvaluationScenarioObservation {
    EvaluationScenarioObservation {
        scenario_ref: scenario.scenario_ref.clone(),
        definition_revision,
        run_ref: format!(
            "run:{candidate_id}:{}:{sample_index}",
            if candidate { "candidate" } else { "baseline" }
        ),
        succeeded: candidate,
        acceptance_total: 1,
        acceptance_satisfied: 1,
        evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
            "evaluation",
            format!(
                "{candidate_id}:{}:{sample_index}",
                if candidate { "candidate" } else { "baseline" }
            ),
        )],
        input_tokens: 10,
        output_tokens: 10,
        tool_calls: 1,
        elapsed_ms: 1,
    }
}

fn candidate() -> EvolutionGovernanceCandidate {
    let definition_id = AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/eval-agent")
        .expect("definition id");
    EvolutionGovernanceCandidate {
        candidate_id: "workspace/cowd/eval-agent@2".to_string(),
        proposal_id: "proposal-eval-agent-v2".to_string(),
        subject: EvolutionCandidateSubject::AgentDefinition {
            revision_ref: AgentDefinitionRevisionRef::new(definition_id, 2).expect("revision"),
        },
        baseline_revision: 1,
        evaluation_contract: EvaluationContract::single_release_gate(
            "evolution/agent-definition",
            "task_success",
        ),
        evaluation_policy_floor: EvaluationPolicyFloor::default(),
        source_evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
            "source", "baseline",
        )],
        canary_policy: CanaryRolloutPolicy::default(),
        lifecycle: EvolutionCandidateLifecycle::Draft,
        comparison_report_ref: None,
        comparison_report_digest: None,
        canary_review_ref: None,
        stable_review_ref: None,
        canary_observation: None,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}
