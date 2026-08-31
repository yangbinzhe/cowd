use super::*;
use harness_contract::execution_graph::{
    ExecutionCompletionContract, ExecutionEdgeKind, ExecutionMaterializationRequest,
};
use harness_contract::policy::PermissionMode;

#[test]
fn trust_all_sessions_are_detected_for_orchestration_approval() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    assert!(!session_is_trust_all(&services, Some("session-1")));
    services.publish_session_execution_policy(
        "session-1",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Yolo,
                2,
                harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
            ),
        ),
    );
    assert!(session_is_trust_all(&services, Some("session-1")));
    assert!(!session_is_trust_all(&services, None));
}

#[test]
fn terminal_program_failure_is_fail_closed_not_fresh_id_replan() {
    let decision = RuntimeOrchestrationDecision {
        selected_pattern: ExecutionPattern::Collaborate,
        selected_template: None,
        reason: "run teams".to_string(),
        policy_gates: Vec::new(),
        validation_findings: Vec::new(),
        adjustments: Vec::new(),
        required_approval: None,
        recovery_hints: Vec::new(),
        budget: json!({}),
        permission: json!({}),
        status: "blocked".to_string(),
    };
    let outcome = OperationOutcome {
        status: "blocked".to_string(),
        disposition: RuntimeOrchestrationDisposition::Admitted,
        execution: json!({
            "collaboration_diagnostics": [{
                "code": "team_execution_not_completed",
                "failure_kind": "provider_protocol",
            }],
        }),
        evidence: json!({}),
        guidance: "inspect the original graph".to_string(),
    };

    let result = result_from_outcome("request-1", decision, outcome);
    assert_eq!(
        result.disposition,
        RuntimeOrchestrationDisposition::Admitted
    );
    assert_eq!(result.model_receipt()["disposition"], "admitted");
    assert_eq!(result.decision.recovery_hints.len(), 1);
    assert_eq!(
        result.decision.recovery_hints[0].code,
        "collaboration_terminal_program_failed"
    );
    assert!(!result.decision.recovery_hints[0].retryable);
}

#[test]
fn root_collaboration_role_selection_survives_strategy_binding() {
    let mut request = proposal(vec![node(
        "runtime-owned-team",
        CapabilityRecipeId::Team,
        Vec::new(),
    )]);
    request.selection_mode = Some(harness_contract::team::TeamSelectionMode::Explicit);

    bind_strategy(&mut request, None, None);

    assert_eq!(
        request.selection_mode,
        Some(harness_contract::team::TeamSelectionMode::Explicit)
    );
}

#[test]
fn semantic_revision_retries_only_typed_stale_errors_and_at_most_three_attempts() {
    let stale = ExecutionRunnerError::Commit(ExecutionCommitError::StaleRevision {
        graph_id: "graph-r4".to_string(),
        expected: 1,
        actual: 2,
    });
    let store_stale = ExecutionRunnerError::Commit(ExecutionCommitError::EventStore(
        crate::RuntimeEventStoreError::StaleRevision {
            stream_id: "graph:graph-r4".to_string(),
            expected: 1,
            actual: 2,
        },
    ));
    let non_stale = ExecutionRunnerError::Commit(ExecutionCommitError::InvalidReplan(
        "node collision".to_string(),
    ));

    assert!(semantic_revision_may_retry(&stale, 1));
    assert!(semantic_revision_may_retry(&store_stale, 2));
    assert!(!semantic_revision_may_retry(&stale, 3));
    assert!(!semantic_revision_may_retry(&non_stale, 1));
}

#[test]
fn committed_change_paths_accept_only_typed_runtime_receipts() {
    let change = harness_contract::agent::AgentChangeReceipt {
        path: "reports/final.html".to_string(),
        before_sha256: None,
        after_sha256: "sha256:after".to_string(),
        write_sequence: 7,
    };
    let valid = harness_contract::context::EvidenceAccessRef::unavailable(
        harness_contract::context::EvidenceRef::observed(
            "runtime_change",
            serde_json::to_string(&change).expect("change receipt"),
        ),
        "application/vnd.cowd.runtime-change+json",
        "execution-node:writer",
    );
    let invalid = harness_contract::context::EvidenceAccessRef::unavailable(
        harness_contract::context::EvidenceRef::observed("runtime_change", "not-json"),
        "application/vnd.cowd.runtime-change+json",
        "execution-node:writer",
    );

    let (paths, invalid_receipts) = committed_change_paths(&[valid, invalid]);

    assert_eq!(paths, BTreeSet::from(["reports/final.html".to_string()]));
    assert_eq!(invalid_receipts, vec!["not-json".to_string()]);
}

#[test]
fn explicit_team_requirement_rejects_collapsed_or_extra_workstreams() {
    let mut decision =
        crate::execution_core::build_runtime_execution_decision("two independent reviews", None);
    decision.strategy.understanding.required_team_count = 2;
    decision.collaboration_obligation = Some(
        harness_contract::strategy::CollaborationExecutionObligation {
            source: harness_contract::strategy::CollaborationObligationSource::ExplicitRequest,
            minimum_team_count: 2,
            exact_team_count: Some(2),
            required_focus_ids: Vec::new(),
            proposal_required: true,
        },
    );
    let one_team = proposal(vec![node(
        "runtime-review",
        CapabilityRecipeId::Team,
        Vec::new(),
    )]);
    let error = validate_collaboration_obligation_cardinality(&one_team, Some(&decision))
        .expect_err("one Team cannot satisfy an explicit two-Team obligation");
    assert!(error.contains("minimum=2:exact=Some(2):proposed=1"));

    let two_teams = proposal(vec![
        node("runtime-review", CapabilityRecipeId::Team, Vec::new()),
        node("gateway-review", CapabilityRecipeId::Team, Vec::new()),
    ]);
    assert!(validate_collaboration_obligation_cardinality(&two_teams, Some(&decision)).is_ok());
}

#[test]
fn automatic_collaboration_obligation_rejects_collapsed_workstreams() {
    let mut decision =
        crate::execution_core::build_runtime_execution_decision("three evidence domains", None);
    decision.collaboration_obligation = Some(
        harness_contract::strategy::CollaborationExecutionObligation {
            source: harness_contract::strategy::CollaborationObligationSource::AutomaticStrategy,
            minimum_team_count: 3,
            exact_team_count: None,
            required_focus_ids: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            proposal_required: true,
        },
    );
    let one_team = proposal(vec![node(
        "collapsed",
        CapabilityRecipeId::Team,
        Vec::new(),
    )]);
    assert!(validate_collaboration_obligation_cardinality(&one_team, Some(&decision)).is_err());

    let three_teams = proposal(vec![
        node("a", CapabilityRecipeId::Team, Vec::new()),
        node("b", CapabilityRecipeId::Team, Vec::new()),
        node("c", CapabilityRecipeId::Team, Vec::new()),
    ]);
    assert!(validate_collaboration_obligation_cardinality(&three_teams, Some(&decision)).is_ok());
}

#[test]
fn write_scope_failure_preserves_the_model_proposal_for_a_typed_replan() {
    let mut request = proposal(vec![GraphSemanticNode {
        node_id: "team-1".to_string(),
        recipe: CapabilityRecipeId::Team,
        objective: "review the repository".to_string(),
        depends_on: Vec::new(),
        multiplicity: 1,
        focuses: Vec::new(),
        managed_agent_escalation:
            harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
        template: Some("cowd/execute-review".to_string()),
        target_session_id: None,
        output_artifacts: vec![
            "workspace_change".to_string(),
            "terminal_synthesis".to_string(),
        ],
        evidence_contract: vec![
            "implementation".to_string(),
            "source_verification".to_string(),
            "evidence".to_string(),
            "risks".to_string(),
        ],
        required_evidence_refs: Vec::new(),
        resource_scopes: vec!["session:session-v621".to_string()],
        required: true,
        dependency: Default::default(),
        cancellation_group: None,
    }]);
    let error = "Team template resolution failed: Team acceptance criterion `implementation` has no bounded Runtime resource scope";
    let before = request.clone();
    assert!(!repair_semantic_compilation(
        &mut request,
        std::path::Path::new("/tmp"),
        error
    ));
    assert_eq!(request, before);
}

fn proposal(nodes: Vec<GraphSemanticNode>) -> RuntimeOrchestrationCommand {
    RuntimeOrchestrationCommand {
        intent: "analyze independent domains and synthesize checked evidence".to_string(),
        model_lease: Some("test-model".to_string()),
        session_id: Some("session-v621".to_string()),
        lineage: Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-v621".to_string(),
            turn_id: "turn-v621".to_string(),
            root_task_id: "task-root-v621".to_string(),
            task_id: "task-root-v621".to_string(),
            generation: 1,
        }),
        mission_id: Some("mission-v621".to_string()),
        operation: RuntimeOrchestrationOperation::Propose,
        inspect_execution_id: None,
        proposal: Some(GraphMutationProposal {
            mutation_id: "mutation-v621".to_string(),
            target_execution_id: None,
            expected_revision: None,
            nodes,
            completion: Default::default(),
            collaboration_program: None,
            collaboration_escalation: None,
            retired_collaboration_instance_ids: Vec::new(),
            reason: "parallel evidence lanes".to_string(),
        }),
        control: None,
        template_proposal: None,
        ephemeral_team_templates: Default::default(),
        collaboration_intent: None,
        collaboration_semantic_intent: None,

        input_disposition: None,
        selection_mode: None,
        strategy_binding: None,
        capabilities: Vec::new(),
        evidence_refs: Vec::new(),
        constraints: RuntimeOrchestrationConstraints {
            max_parallel_agents: Some(8),
            permission_ceiling: PermissionMode::ReadOnly,
            ..Default::default()
        },
        surface: Some("test".to_string()),
    }
}

fn node(id: &str, recipe: CapabilityRecipeId, depends_on: Vec<String>) -> GraphSemanticNode {
    GraphSemanticNode {
        node_id: id.to_string(),
        recipe,
        objective: format!("complete {id}"),
        depends_on,
        multiplicity: 1,
        focuses: Vec::new(),
        managed_agent_escalation:
            harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
        template: None,
        target_session_id: None,
        output_artifacts: Vec::new(),
        evidence_contract: Vec::new(),
        required_evidence_refs: Vec::new(),
        resource_scopes: Vec::new(),
        required: true,
        dependency: Default::default(),
        cancellation_group: None,
    }
}

fn parallel_research_team(id: &str, depends_on: Vec<String>) -> GraphSemanticNode {
    let mut team = node(id, CapabilityRecipeId::Team, depends_on);
    team.template = Some("cowd/parallel-research-synthesis".to_string());
    team.focuses = vec![
        SemanticFocus {
            focus_id: format!("{id}-research"),
            role_id: "researcher".to_string(),
            objective: "collect bounded source evidence".to_string(),
            resource_scopes: Vec::new(),
            evidence_responsibilities: vec!["source evidence".to_string()],
            output_contract: Vec::new(),
            output_acceptance: Vec::new(),
        },
        SemanticFocus {
            focus_id: format!("{id}-synthesis"),
            role_id: "synthesizer".to_string(),
            objective: "synthesize the selected evidence".to_string(),
            resource_scopes: Vec::new(),
            evidence_responsibilities: vec!["evidence synthesis".to_string()],
            output_contract: Vec::new(),
            output_acceptance: Vec::new(),
        },
    ];
    team
}

fn ensure_test_team_resource(request: &mut RuntimeOrchestrationCommand) {
    let Some(proposal) = request.proposal.as_mut() else {
        return;
    };
    if proposal
        .nodes
        .iter()
        .filter(|node| node.recipe == CapabilityRecipeId::Team)
        .all(|node| {
            !node
                .resource_scopes
                .iter()
                .any(|scope| scope.starts_with("read:") || scope.as_str() == "network:*")
        })
    {
        request.capabilities.push("resource:network:*".to_string());
        for node in proposal
            .nodes
            .iter_mut()
            .filter(|node| node.recipe == CapabilityRecipeId::Team)
        {
            node.resource_scopes.push("network:*".to_string());
        }
    }
}

fn ensure_test_mission(services: &RuntimeServices) {
    services
        .mission_runtime()
        .create_mission(
            "mission-v621",
            "test semantic orchestration",
            vec![harness_contract::reality::EvidenceRef::observed(
                "test",
                "mission-v621",
            )],
        )
        .expect("test Mission");
}

#[test]
fn propose_with_custom_template_materializes_a_turn_bound_team_snapshot() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let mut team = node(
        "independent-assessment",
        CapabilityRecipeId::Team,
        Vec::new(),
    );
    team.objective = "independently assess the bounded evidence".to_string();
    team.output_artifacts = vec!["assessment".to_string()];
    team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
    let mut request = proposal(vec![team]);
    request.template_proposal = Some(json!({
        "template_id": "cowd/turn-scoped-independent-assessment",
        "name": "Turn scoped independent assessment",
        "team_display_name": "独立评估",
        "roles": [{
            "role_id": "evidence_assessor",
            "display_name": "证据评估师",
            "responsibility": "独立检查已授权证据并报告不确定性",
            "agent_definition_ref": "builtin/cowd/explore@1",
            "grant_ceiling": ["read"],
            "fixed_count": 1,
            "acceptance": ["summary", "evidence"],
            "behavior": [{"kind": "reacquire_evidence", "required": true}]
        }],
        "result_fields": ["summary", "evidence"],
        "evidence_required": true,
        "instructions": "# 独立评估\n\n只使用已授权证据，清楚列出不确定性。"
    }));

    materialize_ephemeral_team_template(&mut request, &services)
        .expect("normal propose ingress materializes the snapshot");
    assert!(request.template_proposal.is_none());
    let snapshot = request
        .ephemeral_team_templates
        .get("independent-assessment")
        .expect("snapshot is owned by the Team node");
    assert_eq!(snapshot.session_id, "session-v621");
    assert_eq!(snapshot.turn_id, "turn-v621");
    assert!(services
        .definition_registry()
        .resolve_team(
            &snapshot.revision.revision_ref.template_id,
            harness_contract::agent::RevisionSelector::LatestApprovedStable,
        )
        .is_err());

    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    ensure_test_team_resource(&mut request);
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "turn-scoped-custom-team",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("the normal propose compiler uses the snapshot");
    let child_request: harness_contract::team::TeamInstantiationRequest =
        serde_json::from_str(&compiled.graph.nodes[0].payload_ref)
            .expect("typed Team child request");
    assert!(matches!(
        child_request.template_selector,
        harness_contract::team::TeamTemplateSelector::Ephemeral { .. }
    ));
}

#[tokio::test]
async fn v2_semantic_decision_admits_an_exact_turn_scoped_team_snapshot() {
    use harness_contract::orchestration::{
        ModelCollaborationControlDecisionV2, ModelCollaborationDependencyKind,
        ModelCollaborationWorkstreamV2, ModelRoleDependency, ModelRoleIntent,
        ModelSemanticAcceptanceCriterion, ModelTeamResultIntent, ModelTurnScopedTeamIntent,
    };

    let services = RuntimeServices::in_memory().expect("runtime services");
    let source = services
        .workspace_root()
        .join("crates/runtime/src/orchestration/mod.rs");
    std::fs::create_dir_all(source.parent().expect("source fixture parent"))
        .expect("source fixture directory");
    std::fs::write(&source, "// deterministic orchestration evidence fixture")
        .expect("source fixture");
    ensure_test_mission(&services);
    let mut request = proposal(Vec::new());
    request.collaboration_intent = Some(ModelCollaborationControlDecisionV2 {
        schema_version: 2,
        decision_id: "runtime-v2-semantic-admission".to_string(),
        intent: "perform an independent runtime evidence audit".to_string(),
        reason: "the user requested a named two-role Team".to_string(),
        workstreams: vec![ModelCollaborationWorkstreamV2 {
            workstream_id: "runtime-audit".to_string(),
            objective: "produce and synthesize bounded runtime evidence".to_string(),
            depends_on: Vec::new(),
            output_artifacts: vec!["summary".to_string()],
            evidence_contract: vec![ModelSemanticAcceptanceCriterion::Artifact {
                artifact: "summary".to_string(),
            }],
            managed_agent_escalation: Default::default(),
            team: ModelTurnScopedTeamIntent {
                team_key: "arbitrary-user-team".to_string(),
                display_name: Some("用户指定的任意团队名称".to_string()),
                instructions: "collect evidence independently and synthesize it".to_string(),
                result: ModelTeamResultIntent {
                    required_artifacts: vec!["summary".to_string()],
                    evidence_required: true,
                    synthesis_required: true,
                },
                roles: vec![
                    ModelRoleIntent {
                        role_id: "任意取证职责".to_string(),
                        display_name: Some("用户取证专家".to_string()),
                        responsibility: "collect runtime evidence".to_string(),
                        required_capabilities: vec!["read".to_string()],
                        required_skills: Vec::new(),
                        required_tools: vec!["read_file".to_string()],
                        cardinality: Default::default(),
                        acceptance: vec![
                            ModelSemanticAcceptanceCriterion::Artifact {
                                artifact: "evidence".to_string(),
                            },
                            ModelSemanticAcceptanceCriterion::EvidenceScope {
                                operation: "read".to_string(),
                                resource: "crates/runtime/src/orchestration/mod.rs".to_string(),
                            },
                        ],
                        input_artifacts: Vec::new(),
                        output_artifacts: vec!["evidence".to_string()],
                    },
                    ModelRoleIntent {
                        role_id: "任意综合职责".to_string(),
                        display_name: Some("用户结论专家".to_string()),
                        responsibility: "synthesize supplied evidence".to_string(),
                        required_capabilities: vec!["read".to_string()],
                        required_skills: Vec::new(),
                        required_tools: Vec::new(),
                        cardinality: Default::default(),
                        acceptance: vec![ModelSemanticAcceptanceCriterion::Artifact {
                            artifact: "summary".to_string(),
                        }],
                        input_artifacts: vec!["evidence".to_string()],
                        output_artifacts: vec!["summary".to_string(), "evidence".to_string()],
                    },
                ],
                dependencies: vec![ModelRoleDependency {
                    from: "任意取证职责".to_string(),
                    to: "任意综合职责".to_string(),
                    kind: ModelCollaborationDependencyKind::Handoff,
                    artifacts: vec!["evidence".to_string()],
                }],
            },
        }],
    });

    let result =
        admit_runtime_orchestration_request_background(request, None, &services, None).await;
    assert_eq!(
        result.status, "admitted",
        "{:#?}",
        result.decision.validation_findings
    );
    let graph_id = result.execution["graph_id"]
        .as_str()
        .expect("admitted graph id");
    let graph = services
        .graph_state_store()
        .load_async(graph_id)
        .await
        .expect("admitted graph");
    let program = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .expect("collaboration program");
    let intent = program
        .semantic_intent
        .as_ref()
        .expect("semantic provenance");
    assert_eq!(intent.decision_id, "runtime-v2-semantic-admission");
    assert!(intent.ai_composed);
    assert_eq!(intent.teams[0].roles.len(), 2);
}

#[test]
fn semantic_compile_rejection_does_not_invent_a_generic_graph_proposal_error() {
    use harness_contract::orchestration::{
        ModelCollaborationControlDecisionV2, ModelCollaborationWorkstreamV2, ModelRoleIntent,
        ModelTeamResultIntent, ModelTurnScopedTeamIntent,
    };

    let services = RuntimeServices::in_memory().expect("runtime services");
    let mut request = proposal(Vec::new());
    request.proposal = None;
    request.collaboration_intent = Some(ModelCollaborationControlDecisionV2 {
        schema_version: 2,
        decision_id: "missing-terminal-output".to_string(),
        intent: "perform a semantic review".to_string(),
        reason: "exercise the compiler rejection receipt".to_string(),
        workstreams: vec![ModelCollaborationWorkstreamV2 {
            workstream_id: "review".to_string(),
            objective: "produce a reviewed summary".to_string(),
            depends_on: Vec::new(),
            output_artifacts: vec!["summary".to_string()],
            evidence_contract: Vec::new(),
            managed_agent_escalation: Default::default(),
            team: ModelTurnScopedTeamIntent {
                team_key: "review".to_string(),
                display_name: None,
                instructions: String::new(),
                result: ModelTeamResultIntent {
                    required_artifacts: vec!["summary".to_string()],
                    evidence_required: false,
                    synthesis_required: true,
                },
                roles: vec![ModelRoleIntent {
                    role_id: "reviewer".to_string(),
                    display_name: None,
                    responsibility: "review evidence".to_string(),
                    required_capabilities: vec!["read".to_string()],
                    required_skills: Vec::new(),
                    required_tools: Vec::new(),
                    cardinality: Default::default(),
                    acceptance: Vec::new(),
                    input_artifacts: Vec::new(),
                    output_artifacts: vec!["evidence".to_string()],
                }],
                dependencies: Vec::new(),
            },
        }],
    });
    let error = intent_compiler::compile_turn_scoped_intent(
        &request,
        request
            .collaboration_intent
            .as_ref()
            .expect("semantic decision"),
        &services,
    )
    .expect_err("the terminal role intentionally omits the required output");

    let result = rejected_intent_compiler_result(&request, error);

    assert_eq!(result.status, "rejected");
    assert!(result
        .decision
        .validation_findings
        .iter()
        .any(|finding| { finding.contains("completion_terminal_role_missing") }));
    assert!(!result
        .decision
        .validation_findings
        .iter()
        .any(|finding| { finding.contains("propose_requires_only_graph_proposal") }));
    assert_eq!(
        result.decision.recovery_hints[0].code,
        "collaboration_compile_completion_terminal_role_missing"
    );
    assert!(result.decision.recovery_hints[0].retryable);
}

#[test]
fn multiple_custom_teams_preserve_named_bindings_without_catalog_fallback() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let mut business = node("business-team", CapabilityRecipeId::Team, Vec::new());
    business.objective = "assess the business constraints".to_string();
    let mut technical = node("technical-team", CapabilityRecipeId::Team, Vec::new());
    technical.objective = "assess the technical constraints".to_string();
    let mut request = proposal(vec![business, technical]);
    request.template_proposal = Some(json!({
        "teams": [
            {
                "node_id": "business-team",
                "template": {
                    "template_id": "cowd/turn-business-team",
                    "name": "Business team",
                    "team_display_name": "业务团队",
                    "roles": [{
                        "role_id": "signal_cartographer",
                        "display_name": "信号制图师",
                        "responsibility": "identify business constraints from authorized evidence",
                        "agent_definition_ref": "builtin/cowd/explore@1",
                        "grant_ceiling": ["read"],
                        "fixed_count": 1,
                        "acceptance": ["summary", "evidence"],
                        "behavior": [{"kind": "reacquire_evidence", "required": true}]
                    }],
                    "result_fields": ["summary", "evidence"],
                    "evidence_required": true,
                    "instructions": "# 业务团队\n\n仅依据已授权证据。"
                }
            },
            {
                "node_id": "technical-team",
                "template": {
                    "template_id": "cowd/turn-technical-team",
                    "name": "Technical team",
                    "team_display_name": "技术团队",
                    "roles": [{
                        "role_id": "constraint_weaver",
                        "display_name": "约束编织者",
                        "responsibility": "assess technical feasibility from authorized evidence",
                        "agent_definition_ref": "builtin/cowd/explore@1",
                        "grant_ceiling": ["read"],
                        "fixed_count": 1,
                        "acceptance": ["summary", "evidence"],
                        "behavior": [{"kind": "verification", "mode": "independent"}]
                    }],
                    "result_fields": ["summary", "evidence"],
                    "evidence_required": true,
                    "instructions": "# 技术团队\n\n仅依据已授权证据。"
                }
            }
        ]
    }));

    materialize_ephemeral_team_template(&mut request, &services)
        .expect("multiple custom teams materialize");
    assert!(request.template_proposal.is_none());
    assert_eq!(request.ephemeral_team_templates.len(), 2);
    assert_eq!(
        request.ephemeral_team_templates["business-team"]
            .revision
            .manifest
            .display
            .as_ref()
            .and_then(|display| display.team_display_name.as_deref()),
        Some("业务团队")
    );
    assert_eq!(
        request.ephemeral_team_templates["technical-team"]
            .revision
            .manifest
            .roles[0]
            .display_name
            .as_deref(),
        Some("约束编织者")
    );

    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    ensure_test_team_resource(&mut request);
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "multiple-turn-scoped-custom-teams",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("custom snapshots compile without a builtin template selector");
    for node in &compiled.graph.nodes {
        let child_request: harness_contract::team::TeamInstantiationRequest =
            serde_json::from_str(&node.payload_ref).expect("typed Team child request");
        assert!(matches!(
            child_request.template_selector,
            harness_contract::team::TeamTemplateSelector::Ephemeral { .. }
        ));
    }
}

#[test]
fn semantic_contract_rejects_physical_executor_injection() {
    let parsed = serde_json::from_value::<RuntimeOrchestrationCommand>(json!({
        "intent": "inject executor",
        "operation": "propose",
        "proposal": {
            "mutation_id": "bad",
            "reason": "bad",
            "nodes": [{
                "node_id": "bad",
                "recipe": "agent",
                "objective": "bad",
                "executor_kind": "shell"
            }]
        }
    }));
    assert!(parsed.is_err());
}

#[test]
fn semantic_contract_cannot_deserialize_runtime_owned_ephemeral_snapshots() {
    let mut encoded = serde_json::to_value(proposal(vec![node(
        "team",
        CapabilityRecipeId::Team,
        Vec::new(),
    )]))
    .expect("serialize semantic request");
    encoded["ephemeral_team_templates"] = json!({"team": {"forged": true}});
    let parsed: RuntimeOrchestrationCommand =
        serde_json::from_value(encoded).expect("Runtime-owned field is ignored at boundary");
    assert!(parsed.ephemeral_team_templates.is_empty());
}

#[test]
fn semantic_validator_rejects_dependency_cycle() {
    let request = proposal(vec![
        node("a", CapabilityRecipeId::Agent, vec!["b".to_string()]),
        node("b", CapabilityRecipeId::Review, vec!["a".to_string()]),
    ]);
    let plan = planner::plan_runtime_orchestration(&request);
    let decision = validator::validate_request(
        &request,
        &plan.execution_decision,
        plan.model_proposal.as_ref(),
        None,
    );
    assert_eq!(decision.status, "rejected");
    assert!(decision
        .validation_findings
        .contains(&"proposal_dependency_cycle".to_string()));
}

#[test]
fn semantic_validator_limits_concurrent_wave_not_total_graph_work() {
    let mut research = node("research", CapabilityRecipeId::Agent, Vec::new());
    research.multiplicity = 3;
    let synthesis = node(
        "synthesis",
        CapabilityRecipeId::Synthesis,
        vec!["research".to_string()],
    );
    let review = node(
        "review",
        CapabilityRecipeId::Review,
        vec!["synthesis".to_string()],
    );
    let mut request = proposal(vec![research, synthesis, review]);
    request.constraints.max_parallel_agents = Some(3);
    let plan = planner::plan_runtime_orchestration(&request);
    let decision = validator::validate_request(
        &request,
        &plan.execution_decision,
        plan.model_proposal.as_ref(),
        None,
    );

    assert_ne!(decision.status, "rejected");
    assert!(!decision
        .validation_findings
        .contains(&"proposal_exceeds_parallel_agent_ceiling".to_string()));
}

#[test]
fn semantic_validator_rejects_optional_effect_owner_before_materialization() {
    let mut team = node("team", CapabilityRecipeId::Team, Vec::new());
    team.required = false;
    let request = proposal(vec![team]);
    let plan = planner::plan_runtime_orchestration(&request);
    let decision = validator::validate_request(
        &request,
        &plan.execution_decision,
        plan.model_proposal.as_ref(),
        None,
    );
    assert_eq!(decision.status, "rejected");
    assert!(decision
        .validation_findings
        .contains(&"optional_semantic_node_owns_effect".to_string()));
}

#[test]
fn semantic_compiler_materializes_parallel_agents_and_synthesis() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let mut agents = node("research", CapabilityRecipeId::Agent, Vec::new());
    agents.multiplicity = 2;
    agents.output_artifacts = vec!["research_finding".to_string()];
    let mut synthesis = node(
        "synthesis",
        CapabilityRecipeId::Synthesis,
        vec!["research".to_string()],
    );
    synthesis.output_artifacts = vec!["report".to_string()];
    let mut request = proposal(vec![agents, synthesis]);
    request.proposal.as_mut().unwrap().completion = ExecutionCompletionContract {
        required_node_ids: vec!["synthesis".to_string()],
        required_artifact_kinds: vec!["report".to_string()],
        allow_unresolved_conflicts: false,
    };
    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    ensure_test_team_resource(&mut request);
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "compile-v621",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("semantic graph compiles");
    assert_eq!(compiled.graph.nodes.len(), 3);
    assert_eq!(compiled.graph.edges.len(), 4);
    assert_eq!(
        compiled
            .graph
            .edges
            .iter()
            .filter(|edge| {
                edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::Produces
            })
            .count(),
        2
    );
    let completion = &compiled.graph.orchestration.as_ref().unwrap().completion;
    assert_eq!(completion.required_node_ids.len(), 1);
    assert!(completion.required_node_ids[0].contains("synthesis"));
    let mut terminal = compiled.graph.clone();
    for node in &terminal.nodes {
        terminal.node_statuses.insert(
            node.id.clone(),
            if node.work.as_ref().is_some_and(|work| !work.required) {
                ExecutionNodeStatus::Cancelled
            } else {
                ExecutionNodeStatus::Completed
            },
        );
    }
    let projection = harness_contract::execution_graph::project_execution_graph(&terminal);
    assert_eq!(graph_status(&projection), "completed");
    assert_eq!(completion.required_artifact_kinds, vec!["report"]);
}

#[test]
fn semantic_compiler_inserts_required_workspace_materializer() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let mut team = parallel_research_team("report-team", Vec::new());
    team.output_artifacts = vec!["reports/final.md".to_string()];
    team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
    team.resource_scopes = vec![
        "network:*".to_string(),
        "read:crates/runtime".to_string(),
        "write:reports/final.md".to_string(),
    ];
    let mut request = proposal(vec![team]);
    request.intent =
        "research the bounded sources and write the final report to reports/final.md".to_string();
    request.constraints.permission_ceiling = PermissionMode::WorkspaceWrite;
    request.constraints.requires_write = Some(true);
    request.capabilities.extend([
        "resource:network:*".to_string(),
        "resource:read:crates/runtime".to_string(),
        "resource:write:reports/final.md".to_string(),
    ]);
    request.proposal.as_mut().unwrap().completion = ExecutionCompletionContract {
        required_node_ids: vec!["report-team".to_string()],
        required_artifact_kinds: vec!["reports/final.md".to_string()],
        allow_unresolved_conflicts: false,
    };
    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    let report_team = &mut request.proposal.as_mut().unwrap().nodes[0];
    report_team.resource_scopes = vec![
        "network:*".to_string(),
        "read:crates/runtime".to_string(),
        "session:session-v621".to_string(),
        "write:reports/final.md".to_string(),
    ];
    for focus in &mut report_team.focuses {
        focus.resource_scopes = report_team.resource_scopes.clone();
    }
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "materialize-report",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("required report graph compiles");

    let materialize = compiled
        .graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Materialize)
        .expect("materializer node");
    let payload: ExecutionMaterializationRequest =
        serde_json::from_str(&materialize.payload_ref).unwrap();
    assert_eq!(payload.target_path, "reports/final.md");
    assert_eq!(materialize.resource_scopes, vec!["write:reports/final.md"]);
    assert!(compiled
        .graph
        .orchestration
        .as_ref()
        .unwrap()
        .completion
        .required_node_ids
        .contains(&materialize.id));
    assert!(compiled
        .graph
        .edges
        .iter()
        .any(|edge| { edge.to == materialize.id && edge.kind == ExecutionEdgeKind::DependsOn }));
}

#[test]
fn semantic_compiler_exposes_quorum_and_optional_lanes_to_the_runner() {
    use harness_contract::execution_graph::ExecutionDependencyPolicy;

    let services = RuntimeServices::in_memory().expect("runtime services");
    let mut left = node("left", CapabilityRecipeId::Agent, Vec::new());
    left.required = false;
    left.cancellation_group = Some("research".to_string());
    let mut right = node("right", CapabilityRecipeId::Review, Vec::new());
    right.required = false;
    right.cancellation_group = Some("research".to_string());
    let mut synthesis = node(
        "synthesis",
        CapabilityRecipeId::Synthesis,
        vec!["left".to_string(), "right".to_string()],
    );
    synthesis.dependency = ExecutionDependencyPolicy::Quorum {
        minimum: 1,
        cancel_remaining: true,
    };
    synthesis.cancellation_group = Some("research".to_string());
    let request = proposal(vec![left, right, synthesis]);
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "quorum-v625",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("quorum graph compiles");
    let optional = compiled
        .graph
        .nodes
        .iter()
        .filter(|node| node.work.as_ref().is_some_and(|work| !work.required))
        .count();
    assert_eq!(optional, 2);
    for node in compiled
        .graph
        .nodes
        .iter()
        .filter(|node| node.work.as_ref().is_some_and(|work| !work.required))
    {
        let intent: harness_contract::agent::AgentTaskIntent =
            serde_json::from_str(&node.payload_ref).expect("optional agent intent");
        assert_eq!(
            intent.permission_ceiling,
            harness_contract::policy::PermissionMode::ReadOnly
        );
        assert!(!intent
            .granted_capabilities
            .contains(&harness_contract::agent::AgentCapability::Write));
    }
    let synthesis = compiled
        .graph
        .nodes
        .iter()
        .find(|node| node.id.contains("synthesis"))
        .and_then(|node| node.work.as_ref())
        .expect("synthesis work contract");
    assert_eq!(
        synthesis.dependency,
        ExecutionDependencyPolicy::Quorum {
            minimum: 1,
            cancel_remaining: true,
        }
    );
    assert_eq!(synthesis.cancellation_group.as_deref(), Some("research"));
    let completion = &compiled
        .graph
        .orchestration
        .as_ref()
        .expect("orchestration")
        .completion;
    assert_eq!(completion.required_node_ids.len(), 1);
    assert!(completion.required_node_ids[0].contains("synthesis"));
    let mut terminal = compiled.graph.clone();
    for node in &terminal.nodes {
        terminal.node_statuses.insert(
            node.id.clone(),
            if node.work.as_ref().is_some_and(|work| !work.required) {
                ExecutionNodeStatus::Cancelled
            } else {
                ExecutionNodeStatus::Completed
            },
        );
    }
    let projection = harness_contract::execution_graph::project_execution_graph(&terminal);
    assert_eq!(graph_status(&projection), "completed");
    assert!(completion_findings(&projection).is_empty());
}

#[test]
fn semantic_compiler_rejects_observed_negative_provider_lift() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let request = proposal(vec![
        node("left", CapabilityRecipeId::Agent, Vec::new()),
        node("right", CapabilityRecipeId::Review, Vec::new()),
    ]);
    let mut plan = planner::plan_runtime_orchestration(&request);
    let resources = &mut plan.execution_decision.strategy.resource_snapshot;
    resources.provider_effective_limit = 4;
    resources.provider_concurrency = 4;
    resources.tool_concurrency = 4;
    resources.team_slots = 4;
    resources.provider_queue_p95_ms = 300;
    resources.provider_service_p95_ms = 100;
    resources.sample_count = 4;
    let error = compiler::compile_orchestration(
        "provider-pressure-v625",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect_err("observed negative lift must reject fan-out");

    assert!(error
        .to_string()
        .contains("provider_queue_dominates_service_time"));
}

#[test]
fn semantic_compiler_materializes_three_teams_and_a_review_team() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let mut teams = ["domain-a", "domain-b", "domain-c"]
        .into_iter()
        .map(|id| {
            let mut team = parallel_research_team(id, Vec::new());
            team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
            team.output_artifacts = vec![format!("{id}-finding")];
            team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
            team
        })
        .collect::<Vec<_>>();
    let mut review = parallel_research_team(
        "review-team",
        vec![
            "domain-a".to_string(),
            "domain-b".to_string(),
            "domain-c".to_string(),
        ],
    );
    review.template = Some("cowd/parallel-research-synthesis".to_string());
    review.output_artifacts = vec!["reviewed-report".to_string()];
    review.evidence_contract = vec![
        "summary".to_string(),
        "evidence".to_string(),
        "unresolved".to_string(),
    ];
    teams.push(review);
    let mut request = proposal(teams);
    request.constraints.max_parallel_agents = Some(4);
    request.proposal.as_mut().unwrap().completion = ExecutionCompletionContract {
        required_node_ids: vec!["review-team".to_string()],
        required_artifact_kinds: vec!["reviewed-report".to_string()],
        allow_unresolved_conflicts: false,
    };
    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    ensure_test_team_resource(&mut request);
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "multi-team-v621",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("multi-Team root compiles");
    assert_eq!(compiled.graph.nodes.len(), 4);
    assert!(compiled.graph.nodes.iter().all(|node| {
        node.kind == harness_contract::execution_graph::ExecutionNodeKind::Subgraph
            && node.executor_kind == compiler::TEAM_SUBGRAPH_EXECUTOR
    }));
    assert_eq!(
        compiled
            .graph
            .edges
            .iter()
            .filter(|edge| {
                edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::CrossTeamHandoff
            })
            .count(),
        3
    );
    assert_eq!(
        compiled
            .graph
            .edges
            .iter()
            .filter(|edge| edge.kind.is_dependency())
            .count(),
        0,
        "organizational Team relations without typed input artifacts must not serialize execution"
    );
    assert_eq!(
        compiled
            .graph
            .edges
            .iter()
            .filter(|edge| {
                edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::Produces
            })
            .count(),
        3
    );
    let completion = &compiled.graph.orchestration.as_ref().unwrap().completion;
    assert_eq!(completion.required_node_ids.len(), 1);
    assert!(completion.required_node_ids[0].contains("review-team"));
}

#[test]
fn team_count_above_frozen_capacity_is_rejected() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let nodes = (0..100)
        .map(|index| {
            let mut team = parallel_research_team(&format!("team-{index:03}"), Vec::new());
            team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
            team
        })
        .collect::<Vec<_>>();
    let mut request = proposal(nodes);
    request.constraints.max_parallel_agents = Some(100);
    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    ensure_test_team_resource(&mut request);
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "hundred-team-v621",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect_err("frozen capacity rejects an oversized model hint");
    assert!(compiled
        .to_string()
        .contains("program_team_count_exceeds_capacity:100>32"));
}

#[test]
fn completion_contract_blocks_missing_artifacts_and_unresolved_conflicts() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let mut synthesis = node("synthesis", CapabilityRecipeId::Synthesis, Vec::new());
    synthesis.output_artifacts = vec!["verified-report".to_string()];
    let mut request = proposal(vec![synthesis]);
    request.proposal.as_mut().unwrap().completion = ExecutionCompletionContract {
        required_node_ids: vec!["synthesis".to_string()],
        required_artifact_kinds: vec!["verified-report".to_string()],
        allow_unresolved_conflicts: false,
    };
    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    let plan = planner::plan_runtime_orchestration(&request);
    let mut graph = compiler::compile_orchestration(
        "completion-v621",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("completion graph compiles")
    .graph;
    let node_id = graph.nodes[0].id.clone();
    graph.node_statuses.insert(
        node_id.clone(),
        harness_contract::execution_graph::ExecutionNodeStatus::Completed,
    );
    let projection = harness_contract::execution_graph::project_execution_graph(&graph);
    assert_eq!(
        completion_findings(&projection),
        vec!["required_artifact_not_materialized:verified-report"]
    );

    graph.node_results.insert(
        node_id,
        harness_contract::execution_graph::ExecutionNodeResult {
            status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
            result_ref: Some("artifact:verified-report:unresolved".to_string()),
            summary: Some("conflicting evidence retained".to_string()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        },
    );
    let findings = completion_findings(
        &harness_contract::execution_graph::project_execution_graph(&graph),
    );
    assert_eq!(findings, vec!["unresolved_conflict_rejected"]);
}

#[test]
fn failed_team_requirement_projects_a_typed_program_diagnostic() {
    use harness_contract::execution_graph::{
        CollaborationProgram, CollaborationProgramControlState, CollaborationProgramLifecycle,
        CollaborationTeamInstance, ExecutionFailure, ExecutionNodeResult, ExecutionNodeSpec,
        ExecutionOrchestrationMetadata, TeamAdmissionObligation, TeamAdmissionState,
        TeamExecutionTerminal,
    };

    let mut graph = ExecutionGraph::new("typed-team-terminal");
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::Subgraph,
        compiler::TEAM_SUBGRAPH_EXECUTOR,
        "{}",
    );
    node.id = "team-node".to_string();
    node.idempotency_key = "team-node".to_string();
    graph.nodes.push(node);
    graph
        .node_statuses
        .insert("team-node".to_string(), ExecutionNodeStatus::Failed);
    graph.node_results.insert(
        "team-node".to_string(),
        ExecutionNodeResult {
            status: ExecutionNodeStatus::Failed,
            result_ref: None,
            summary: None,
            evidence_refs: Vec::new(),
            failure: Some(ExecutionFailure {
                kind: "provider_timeout".to_string(),
                message: "provider deadline elapsed".to_string(),
                retryable: true,
                evidence_refs: Vec::new(),
            }),
            usage: Default::default(),
            finished_at_ms: 7,
        },
    );
    graph.orchestration = Some(ExecutionOrchestrationMetadata {
        mutation_id: "typed-team-terminal".to_string(),
        applied_mutation_ids: Vec::new(),
        collaboration_escalations: Vec::new(),
        semantic_revision: 1,
        source_generation: 1,
        completion: ExecutionCompletionContract {
            required_node_ids: vec!["team-node".to_string()],
            required_artifact_kinds: Vec::new(),
            allow_unresolved_conflicts: false,
        },
        collaboration_program: Some(CollaborationProgram {
            program_id: "program-terminal".to_string(),
            revision: 1,
            required_team_count: 1,
            team_instances: vec![CollaborationTeamInstance {
                instance_id: "audit:1".to_string(),
                semantic_node_id: "audit".to_string(),
                required: true,
            }],
            edges: Vec::new(),
            semantic_node_instances: BTreeMap::from([(
                "audit".to_string(),
                vec!["team-node".to_string()],
            )]),
            control: CollaborationProgramControlState {
                lifecycle: CollaborationProgramLifecycle::Failed,
                obligations: vec![TeamAdmissionObligation {
                    instance_id: "audit:1".to_string(),
                    binding_ref: "team-binding:sha256:test".to_string(),
                    state: TeamAdmissionState::Admitted,
                    child_graph_ref: Some("team-graph:audit".to_string()),
                    reason_kind: None,
                    terminal: Some(TeamExecutionTerminal {
                        node_status: ExecutionNodeStatus::Failed,
                        failure_kind: Some("provider_timeout".to_string()),
                        failure_message: Some("provider deadline elapsed".to_string()),
                        retryable: true,
                        evidence_refs: Vec::new(),
                        finished_at_ms: 7,
                    }),
                    reservation: Default::default(),
                    revision: 1,
                }],
                ..Default::default()
            },
            semantic_intent: None,
        }),
    });

    let projection = harness_contract::execution_graph::project_execution_graph(&graph);
    let findings = completion_findings(&projection);
    assert_eq!(
        findings,
        vec!["collaboration_terminal_diagnostic:audit:1:team_execution_not_completed"]
    );
    assert!(!findings
        .iter()
        .any(|finding| finding.starts_with("required_node_not_completed:")));
    let (_, diagnostics) = collaboration_program_projection(&projection);
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(
        diagnostics[0].failure_kind.as_deref(),
        Some("provider_timeout")
    );
    assert!(diagnostics[0].retryable);
}

#[tokio::test]
async fn team_board_is_revisioned_idempotent_and_binding_scoped() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let mut team = parallel_research_team("team", Vec::new());
    let synthesis_focus = team.focuses.pop().expect("synthesis focus");
    team.focuses = (0..4)
        .map(|index| SemanticFocus {
            focus_id: format!("team-research-{index}"),
            role_id: "researcher".to_string(),
            objective: format!("collect independent bounded source evidence lane {index}"),
            resource_scopes: Vec::new(),
            evidence_responsibilities: vec![format!("source evidence lane {index}")],
            output_contract: Vec::new(),
            output_acceptance: Vec::new(),
        })
        .chain(std::iter::once(synthesis_focus))
        .collect();
    let mut request = proposal(vec![team]);
    request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
        decision_id: "decision-v621".to_string(),
        decision_revision: 1,
        decision_lease: "lease-v621".to_string(),
        turn_ref: "turn-v621".to_string(),
    });
    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    ensure_test_team_resource(&mut request);
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "team-board-v621",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("team root compiles");
    let team_request: harness_contract::team::TeamInstantiationRequest =
        serde_json::from_str(&compiled.graph.nodes[0].payload_ref).expect("typed team request");
    let child = services
        .team_runtime()
        .plan(team_request)
        .expect("team child plan");
    let registered = services
        .execution_supervisor()
        .register_graph(child.graph)
        .await
        .expect("register team child");
    let agent_nodes = registered
        .nodes
        .iter()
        .filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
        .map(|node| node.id.clone())
        .collect::<Vec<_>>();
    assert!(agent_nodes.len() >= 5);
    let claim_request = |node_id: String| crate::CollaborationControlRequest {
        graph_id: registered.id.clone(),
        node_id: node_id.clone(),
        operation: crate::CollaborationControlOperation::Claim,
        expected_revision: Some(registered.revision),
        expected_work_revision: Some(0),
        work_node_id: Some(node_id),
        claim_token: None,
        lease_duration_ms: Some(60_000),
        submission_ref: None,
        finding: None,
    };
    let (claim_0, claim_1, claim_2, claim_3) = tokio::join!(
        services
            .team_runtime()
            .apply_collaboration_control(claim_request(agent_nodes[0].clone())),
        services
            .team_runtime()
            .apply_collaboration_control(claim_request(agent_nodes[1].clone())),
        services
            .team_runtime()
            .apply_collaboration_control(claim_request(agent_nodes[2].clone())),
        services
            .team_runtime()
            .apply_collaboration_control(claim_request(agent_nodes[3].clone())),
    );
    assert!(claim_0.is_ok(), "first claim: {claim_0:?}");
    assert!(claim_1.is_ok(), "second claim: {claim_1:?}");
    assert!(claim_2.is_ok(), "third claim: {claim_2:?}");
    assert!(claim_3.is_ok(), "fourth claim: {claim_3:?}");
    let claimed_graph = services
        .graph_state_store()
        .load(&registered.id)
        .expect("claimed Team graph");
    assert_eq!(
        claimed_graph
            .work_states
            .values()
            .filter(|state| {
                state.status
                    == harness_contract::execution_graph::ExecutionWorkRuntimeStatus::Claimed
            })
            .count(),
        4
    );
    let stale_work = services
        .team_runtime()
        .apply_collaboration_control(claim_request(agent_nodes[0].clone()))
        .await
        .expect_err("stale per-work revision must be rejected");
    assert!(stale_work.contains("work revision mismatch"));
    let publish = crate::TeamWorkingStatePublishRequest {
        graph_id: registered.id.clone(),
        node_id: agent_nodes[0].clone(),
        expected_revision: 0,
        kind: crate::TeamWorkingStateKind::Finding,
        summary: "checked semantic finding".to_string(),
        refs: vec!["evidence:test:v621".to_string()],
        artifact_refs: vec!["artifact:test:v621".to_string()],
        visibility: crate::TeamWorkingStateVisibility::Team,
        thread: None,
    };
    let committed = services
        .team_runtime()
        .publish_working_state(publish.clone())
        .await
        .expect("publish board entry");
    assert_eq!(committed.board_revision, 1);
    let duplicate = services
        .team_runtime()
        .publish_working_state(publish)
        .await
        .expect("idempotent retry");
    assert_eq!(duplicate.entries.len(), 1);
    let visible = services
        .team_runtime()
        .read_working_state(crate::TeamWorkingStateReadRequest {
            graph_id: registered.id.clone(),
            node_id: agent_nodes[1].clone(),
            after_revision: Some(0),
            exact_revision: None,
        })
        .expect("peer read");
    assert_eq!(visible.entries.len(), 1);
    assert_eq!(visible.entries[0].source_generation, 1);
    let exact = services
        .team_runtime()
        .read_working_state(crate::TeamWorkingStateReadRequest {
            graph_id: registered.id.clone(),
            node_id: agent_nodes[1].clone(),
            after_revision: None,
            exact_revision: Some(1),
        })
        .expect("exact revision read");
    assert_eq!(exact.entries.len(), 1);
    let after = services
        .team_runtime()
        .read_working_state(crate::TeamWorkingStateReadRequest {
            graph_id: registered.id.clone(),
            node_id: agent_nodes[1].clone(),
            after_revision: Some(1),
            exact_revision: None,
        })
        .expect("read after committed revision");
    assert!(after.entries.is_empty());

    let question = services
        .team_runtime()
        .publish_working_state(crate::TeamWorkingStatePublishRequest {
            graph_id: registered.id.clone(),
            node_id: agent_nodes[0].clone(),
            expected_revision: 1,
            kind: crate::TeamWorkingStateKind::Question,
            summary: "Can the peer independently verify source coverage?".to_string(),
            refs: Vec::new(),
            artifact_refs: Vec::new(),
            visibility: crate::TeamWorkingStateVisibility::Team,
            thread: Some(crate::TeamWorkingStateThread {
                thread_id: "coverage-review".to_string(),
                reply_to_entry_id: None,
                response_required: true,
                resolves_entry_ids: Vec::new(),
            }),
        })
        .await
        .expect("publish question");
    let question_id = question
        .entries
        .iter()
        .find(|entry| entry.kind == crate::TeamWorkingStateKind::Question)
        .expect("question entry")
        .entry_id
        .clone();
    services
        .team_runtime()
        .publish_working_state(crate::TeamWorkingStatePublishRequest {
            graph_id: registered.id.clone(),
            node_id: agent_nodes[1].clone(),
            expected_revision: 2,
            kind: crate::TeamWorkingStateKind::Response,
            summary: "Peer coverage check completed against the durable evidence.".to_string(),
            refs: vec!["evidence:test:v621".to_string()],
            artifact_refs: Vec::new(),
            visibility: crate::TeamWorkingStateVisibility::Team,
            thread: Some(crate::TeamWorkingStateThread {
                thread_id: "coverage-review".to_string(),
                reply_to_entry_id: Some(question_id),
                response_required: false,
                resolves_entry_ids: Vec::new(),
            }),
        })
        .await
        .expect("peer response");
    let unread = services
        .team_runtime()
        .read_working_state_from_cursor(registered.id.clone(), agent_nodes[1].clone())
        .expect("offline peer replays committed inbox");
    assert_eq!(unread.entries.len(), 3);
    let cursor = services
        .team_runtime()
        .acknowledge_working_state(crate::TeamWorkingStateAcknowledgeRequest {
            graph_id: registered.id.clone(),
            node_id: agent_nodes[1].clone(),
            through_revision: unread.board_revision,
            expected_cursor_revision: 0,
        })
        .expect("advance durable peer cursor");
    assert_eq!(cursor.through_revision, 3);
    assert!(services
        .team_runtime()
        .read_working_state_from_cursor(registered.id.clone(), agent_nodes[1].clone())
        .expect("cursor suppresses acknowledged messages")
        .entries
        .is_empty());

    let private_reasoning = services
        .team_runtime()
        .publish_working_state(crate::TeamWorkingStatePublishRequest {
            graph_id: registered.id,
            node_id: agent_nodes[0].clone(),
            expected_revision: 3,
            kind: crate::TeamWorkingStateKind::Finding,
            summary: "raw chain-of-thought must remain private".to_string(),
            refs: Vec::new(),
            artifact_refs: Vec::new(),
            visibility: crate::TeamWorkingStateVisibility::Private,
            thread: None,
        })
        .await
        .expect_err("private reasoning trace must be rejected");
    assert!(private_reasoning.contains("not private reasoning traces"));
}

#[tokio::test]
async fn collaboration_coordinator_persists_every_compiled_team_obligation_before_admission() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let mut request = proposal(
        (0..6)
            .map(|index| parallel_research_team(&format!("workstream-{index}"), Vec::new()))
            .collect(),
    );
    request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
        decision_id: "coordinator-obligations".to_string(),
        decision_revision: 1,
        decision_lease: "coordinator-lease".to_string(),
        turn_ref: "turn-v621".to_string(),
    });
    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    ensure_test_team_resource(&mut request);
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "coordinator-obligations",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("team root compiles");
    let mut graph = services
        .compile_graph_agent_intents(compiled.graph)
        .expect("agent intents compile");
    collaboration_coordinator::prepare_program_admission(
        &mut graph,
        services.team_runtime().as_ref(),
    )
    .expect("program admission control compiles");
    let program = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .expect("Team graph has a Program");
    assert_eq!(
        program.control.lifecycle,
        harness_contract::execution_graph::CollaborationProgramLifecycle::Admitting
    );
    assert_eq!(
        program.control.obligations.len(),
        program.team_instances.len(),
        "every requested Team must be durable before the graph is registered"
    );
    assert!(program.control.obligations.iter().all(|obligation| {
        obligation.binding_ref.starts_with("team-binding:sha256:")
            && obligation.state == harness_contract::execution_graph::TeamAdmissionState::Admitting
            && obligation.child_graph_ref.is_none()
    }));
    assert!(program.control.resource_ledger.context_reservation_tokens > 0);
    assert!(program.control.resource_ledger.output_reservation_tokens > 0);
    assert!(program.control.resource_ledger.parallel_demand >= 2);
    assert!(program.control.resource_ledger.deadline_at_ms > 0);
    program.validate().expect("active Program is complete");
    let root_node_ids = program
        .team_instances
        .iter()
        .map(|instance| {
            let (semantic, ordinal) = instance
                .instance_id
                .rsplit_once(':')
                .expect("stable semantic instance id");
            program.semantic_node_instances[semantic][ordinal
                .parse::<usize>()
                .expect("stable instance ordinal")
                .saturating_sub(1)]
            .clone()
        })
        .collect::<Vec<_>>();
    let registered = services
        .execution_supervisor()
        .register_graph(graph)
        .await
        .expect("register Program graph");
    let graph_id = &registered.id;
    let supervisor = services.execution_supervisor();
    let graphs = services.graph_state_store();
    let admission_results = futures::future::join_all(root_node_ids.iter().map(|node_id| {
        let child_graph_id = format!("team-graph:{node_id}");
        let supervisor = std::sync::Arc::clone(&supervisor);
        async move {
            collaboration_coordinator::mark_team_admitted(
                graph_id,
                node_id,
                &child_graph_id,
                supervisor.as_ref(),
                graphs,
            )
            .await
        }
    }))
    .await;
    for result in admission_results {
        result.expect("concurrent Team admission converges");
    }
    collaboration_coordinator::mark_team_admitted(
        &registered.id,
        &root_node_ids[0],
        &format!("team-graph:{}", root_node_ids[0]),
        services.execution_supervisor().as_ref(),
        services.graph_state_store(),
    )
    .await
    .expect("duplicate admission is idempotent");
    let stored = services
        .graph_state_store()
        .load_async(&registered.id)
        .await
        .expect("load registered Program");
    let control = &stored
        .orchestration
        .as_ref()
        .expect("metadata")
        .collaboration_program
        .as_ref()
        .expect("Program")
        .control;
    assert_eq!(
        control.lifecycle,
        harness_contract::execution_graph::CollaborationProgramLifecycle::Running
    );
    assert!(control.obligations.iter().all(|obligation| {
        obligation.state == harness_contract::execution_graph::TeamAdmissionState::Admitted
            && obligation.child_graph_ref.is_some()
    }));
}

#[tokio::test]
async fn one_hundred_programs_with_ten_teams_persist_all_admitted_obligations() {
    use std::time::Instant;

    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let started = Instant::now();
    let mut per_program_admission_us = Vec::with_capacity(100);

    for program_index in 0..100 {
        let program_started = Instant::now();
        let nodes = (0..10)
            .map(|team_index| {
                let mut team = node(
                    &format!("program-{program_index:03}-team-{team_index:02}"),
                    CapabilityRecipeId::Team,
                    Vec::new(),
                );
                team.template = Some("cowd/parallel-research-synthesis".to_string());
                // This is a model-assisted Program fixture. A multi-role
                // template must state its active role set explicitly so
                // the scale gate measures the same contract used in
                // production, rather than relying on the retired
                // implicit full-template expansion.
                team.focuses = vec![
                    SemanticFocus {
                        focus_id: format!(
                            "program-{program_index:03}-team-{team_index:02}-research"
                        ),
                        role_id: "researcher".to_string(),
                        objective: "collect bounded source evidence".to_string(),
                        resource_scopes: Vec::new(),
                        evidence_responsibilities: vec!["source evidence".to_string()],
                        output_contract: Vec::new(),
                        output_acceptance: Vec::new(),
                    },
                    SemanticFocus {
                        focus_id: format!(
                            "program-{program_index:03}-team-{team_index:02}-synthesis"
                        ),
                        role_id: "synthesizer".to_string(),
                        objective: "synthesize the selected evidence".to_string(),
                        resource_scopes: Vec::new(),
                        evidence_responsibilities: vec!["evidence synthesis".to_string()],
                        output_contract: Vec::new(),
                        output_acceptance: Vec::new(),
                    },
                ];
                team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
                team
            })
            .collect::<Vec<_>>();
        let mut request = proposal(nodes);
        request.proposal.as_mut().expect("proposal").mutation_id =
            format!("hundred-programs-mutation-{program_index}");
        request.constraints.max_parallel_agents = Some(10);
        request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
            decision_id: format!("hundred-programs-{program_index}"),
            decision_revision: 1,
            decision_lease: format!("hundred-programs-lease-{program_index}"),
            turn_ref: "turn-v621".to_string(),
        });
        // The stress fixture already supplies a complete semantic Team
        // contract, including exact focus roles. Do not run the
        // request-authority synthesizer here: it is responsible for
        // model-originated incomplete proposals and would overwrite this
        // deliberately frozen topology before the admission benchmark.
        ensure_test_team_resource(&mut request);
        let plan = planner::plan_runtime_orchestration(&request);
        let compiled = compiler::compile_orchestration(
            &format!("hundred-programs-{program_index}"),
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("ten-Team Program compiles");
        let mut graph = services
            .compile_graph_agent_intents(compiled.graph)
            .expect("agent intents compile");
        collaboration_coordinator::prepare_program_admission(
            &mut graph,
            services.team_runtime().as_ref(),
        )
        .expect("Program admission control compiles");
        let program = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
            .expect("Team graph has a Program");
        let root_node_ids = program
            .team_instances
            .iter()
            .map(|instance| {
                let (semantic, ordinal) = instance
                    .instance_id
                    .rsplit_once(':')
                    .expect("stable semantic instance id");
                program.semantic_node_instances[semantic][ordinal
                    .parse::<usize>()
                    .expect("stable instance ordinal")
                    .saturating_sub(1)]
                .clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(root_node_ids.len(), 10);
        let registered = services
            .execution_supervisor()
            .register_graph(graph)
            .await
            .expect("register Program graph");
        for node_id in &root_node_ids {
            collaboration_coordinator::mark_team_admitted(
                &registered.id,
                node_id,
                &format!("team-graph:{node_id}"),
                services.execution_supervisor().as_ref(),
                services.graph_state_store(),
            )
            .await
            .expect("mark Team admitted");
        }
        let stored = services
            .graph_state_store()
            .load_async(&registered.id)
            .await
            .expect("load registered Program");
        let control = &stored
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("Program")
            .control;
        assert_eq!(
            control.lifecycle,
            harness_contract::execution_graph::CollaborationProgramLifecycle::Running
        );
        assert_eq!(control.obligations.len(), 10);
        assert!(control.obligations.iter().all(|obligation| {
            obligation.state == harness_contract::execution_graph::TeamAdmissionState::Admitted
                && obligation.child_graph_ref.is_some()
        }));
        per_program_admission_us.push(program_started.elapsed().as_micros() as u64);
    }

    let indexed_programs = services
        .graph_state_store()
        .nonterminal_graph_ids_async()
        .await
        .expect("query nonterminal Program index");
    assert_eq!(indexed_programs.len(), 100);
    per_program_admission_us.sort_unstable();
    let p95_index = (per_program_admission_us.len().saturating_sub(1) * 95) / 100;
    eprintln!(
        "100 Program x 10 Team durable admission: total_us={} p95_program_us={}",
        started.elapsed().as_micros(),
        per_program_admission_us[p95_index]
    );
    assert!(
        started.elapsed().as_secs() < 60,
        "100 Program x 10 Team durable admission exceeded its bounded test window"
    );
}

#[tokio::test]
async fn startup_reconciliation_restores_live_program_approval_wait_state() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let team = parallel_research_team("research", Vec::new());
    let mut request = proposal(vec![team]);
    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    ensure_test_team_resource(&mut request);
    let plan = planner::plan_runtime_orchestration(&request);
    let graph = services
        .compile_graph_agent_intents(
            compiler::compile_orchestration(
                "startup-program-wait",
                &request,
                &plan,
                None,
                Some(services.team_runtime().as_ref()),
            )
            .expect("Team Program compiles")
            .graph,
        )
        .expect("Agent intents compile");
    // Persist the legacy Planning-shaped Program intentionally: startup
    // recovery must backfill its exact control state from frozen Team
    // requests rather than making Gateway boot depend on a prior
    // in-memory admission pass.
    let node_id = graph.nodes[0].id.clone();
    let graph = services
        .commit_service()
        .register_graph(graph)
        .expect("register Program")
        .graph;
    let graph = services
        .commit_service()
        .transition_node(
            &graph,
            &node_id,
            harness_contract::execution_graph::ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .expect("make Team root ready")
        .graph;
    let graph = services
        .commit_service()
        .transition_node(
            &graph,
            &node_id,
            harness_contract::execution_graph::ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .expect("make Team root running")
        .graph;
    let graph = services
        .commit_service()
        .transition_node(
            &graph,
            &node_id,
            harness_contract::execution_graph::ExecutionNodeStatus::WaitingApproval,
            None,
            Vec::new(),
        )
        .expect("persist approval wait")
        .graph;

    let examined =
        collaboration_coordinator::reconcile_terminal_programs_on_startup(services.as_ref(), 16)
            .await
            .expect("startup reconciliation");
    assert_eq!(examined, 1);
    let stored = services
        .graph_state_store()
        .load_async(&graph.id)
        .await
        .expect("load reconciled Program");
    let control = &stored
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .expect("Program")
        .control;
    assert_eq!(
        control.lifecycle,
        harness_contract::execution_graph::CollaborationProgramLifecycle::AwaitingApproval
    );
    assert_eq!(
        control.blocker_ref.as_deref(),
        Some(format!("execution-node:{node_id}").as_str())
    );
}

#[test]
fn add_team_patch_compiles_to_an_exact_active_program_revision() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let mut seed_team = node("research", CapabilityRecipeId::Team, Vec::new());
    seed_team.objective = "collect the bounded research evidence".to_string();
    seed_team.output_artifacts = vec!["research".to_string()];
    seed_team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
    let mut seed_request = proposal(vec![seed_team]);
    seed_request.template_proposal = Some(serde_json::json!({
        "template_id": "cowd/ephemeral-research-parent",
        "name": "临时研究父团队",
        "team_display_name": "研究",
        "roles": [{
            "role_id": "evidence_researcher",
            "display_name": "证据研究员",
            "responsibility": "收集并校验授权范围内的研究证据",
            "agent_definition_ref": "builtin/cowd/explore@1",
            "grant_ceiling": ["read"],
            "fixed_count": 1,
            "acceptance": ["summary", "evidence"],
            "behavior": [{"kind": "reacquire_evidence", "required": true}]
        }],
        "result_fields": ["summary", "evidence"],
        "evidence_required": true,
        "instructions": "# 临时研究\n\n仅收集授权范围内的证据。"
    }));
    materialize_ephemeral_team_template(&mut seed_request, &services)
        .expect("custom parent Team snapshot materializes");
    seed_request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
        decision_id: "patch-seed".to_string(),
        decision_revision: 1,
        decision_lease: "patch-seed-lease".to_string(),
        turn_ref: "turn-v621".to_string(),
    });
    team_authority::bind_semantic_resource_authority(
        &mut seed_request,
        None,
        services.workspace_root(),
    );
    ensure_test_team_resource(&mut seed_request);
    let seed_plan = planner::plan_runtime_orchestration(&seed_request);
    let mut seed_graph = services
        .compile_graph_agent_intents(
            compiler::compile_orchestration(
                "patch-seed",
                &seed_request,
                &seed_plan,
                None,
                Some(services.team_runtime().as_ref()),
            )
            .expect("seed Team program compiles")
            .graph,
        )
        .expect("seed Agent intents compile");
    collaboration_coordinator::prepare_program_admission(
        &mut seed_graph,
        services.team_runtime().as_ref(),
    )
    .expect("seed Program admission compiles");
    let registered = services
        .commit_service()
        .register_graph(seed_graph)
        .expect("register active Program")
        .graph;
    let program = registered
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .expect("registered Program");
    let source_seed = registered
        .nodes
        .iter()
        .find_map(|node| {
            serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
                &node.payload_ref,
            )
            .ok()
        })
        .expect("registered source Team request");
    let source_attempt = format!(
        "team-graph:{}:role-evidence-researcher:1:attempt:1",
        source_seed.team_id
    );
    let recovery_escalation = harness_contract::execution_graph::CollaborationEscalationRequest {
        base_revision: program.revision,
        source_attempt: source_attempt.clone(),
        request_kind: "add_team".to_string(),
        reason: "independent evidence review is required".to_string(),
        evidence_refs: Vec::new(),
        digest: "c".repeat(64),
        requested_add_team: Some(
            harness_contract::execution_graph::CollaborationEscalationAddTeam {
                semantic_node_id: "runtime-derived-review".to_string(),
                objective: "independently review the bounded research evidence".to_string(),
                depends_on: vec!["research".to_string()],
                resource_scopes: vec!["network:*".to_string()],
                output_artifacts: vec!["independent-review".to_string()],
                evidence_contract: vec!["summary".to_string(), "evidence".to_string()],
                required: true,
                parallelism_hint: 1,
            },
        ),
        template_proposal: None,
    };
    let mut recovery_patch = recovery_escalation.as_add_team_patch(program.program_id.clone());
    attach_source_ephemeral_template_for_escalation(
        &registered,
        &source_attempt,
        &mut recovery_patch,
    )
    .expect("managed escalation inherits the source immutable snapshot");
    let runtime_derived = match &recovery_patch.operation {
        harness_contract::execution_graph::CollaborationIntentPatchOperation::AddTeam { team } => {
            team
        }
        _ => unreachable!("escalation creates an AddTeam patch"),
    };
    assert_eq!(
        runtime_derived
            .ephemeral_template
            .as_ref()
            .expect("source snapshot is copied")
            .template_digest,
        match &source_seed.template_selector {
            harness_contract::team::TeamTemplateSelector::Ephemeral { snapshot } => {
                snapshot.template_digest.clone()
            }
            _ => panic!("custom source Team must retain its ephemeral snapshot"),
        }
    );
    let escalation = harness_contract::execution_graph::CollaborationEscalationRequest {
        base_revision: program.revision,
        source_attempt: "team-agent:research:attempt:1".to_string(),
        request_kind: "add_team".to_string(),
        reason: "independent evidence review is required".to_string(),
        evidence_refs: Vec::new(),
        digest: "d".repeat(64),
        requested_add_team: Some(
            harness_contract::execution_graph::CollaborationEscalationAddTeam {
                semantic_node_id: "independent-review".to_string(),
                objective: "independently review the bounded research evidence".to_string(),
                depends_on: vec!["research".to_string()],
                resource_scopes: vec!["network:*".to_string()],
                output_artifacts: vec!["independent-review".to_string()],
                evidence_contract: vec!["summary".to_string(), "evidence".to_string()],
                required: true,
                parallelism_hint: 1,
            },
        ),
        template_proposal: Some(serde_json::json!({
            "template_id": "cowd/independent-review-snapshot",
            "name": "独立审查团队",
            "team_display_name": "独立审查",
            "roles": [{
                "role_id": "evidence_reviewer",
                "display_name": "独立审查员",
                "responsibility": "独立复核授权证据并明确未解决风险",
                "agent_definition_ref": "builtin/cowd/explore@1",
                "grant_ceiling": ["read"],
                "fixed_count": 1,
                "acceptance": ["summary", "evidence"],
                "behavior": [{"kind": "verification", "mode": "independent"}]
            }],
            "result_fields": ["summary", "evidence"],
            "evidence_required": true,
            "instructions": "# 独立审查\n\n只依据授权证据复核结论，说明不确定性。"
        })),
    };
    escalation.validate().expect("typed escalation validates");
    let mut patch = escalation.as_add_team_patch(program.program_id.clone());
    attach_escalated_ephemeral_template(
        &registered,
        &mut patch,
        escalation
            .template_proposal
            .clone()
            .expect("custom template proposal"),
        &services,
    )
    .expect("Runtime binds the escalation custom template to its parent Program");
    let review = match &patch.operation {
        harness_contract::execution_graph::CollaborationIntentPatchOperation::AddTeam { team } => {
            team.clone()
        }
        _ => unreachable!("escalation creates an AddTeam patch"),
    };
    let mut split_left = review.clone();
    split_left.semantic_node_id = "research-left".to_string();
    split_left.objective = "separate the first bounded research lane".to_string();
    split_left.depends_on.clear();
    let mut split_right = split_left.clone();
    split_right.semantic_node_id = "research-right".to_string();
    split_right.objective = "separate the second bounded research lane".to_string();
    let mut split_patch = patch.clone();
    split_patch.canonical_digest = "f".repeat(64);
    split_patch.operation =
        harness_contract::execution_graph::CollaborationIntentPatchOperation::SplitWorkstream {
            source_instance_id: "research:1".to_string(),
            teams: vec![split_left, split_right],
        };
    let split_request =
        collaboration_coordinator::compile_collaboration_intent_patch(&registered, &split_patch)
            .expect("unstarted Team split compiles into one atomic replan");
    let split_proposal = split_request
        .proposal
        .expect("split has a semantic proposal");
    assert_eq!(split_proposal.nodes.len(), 2);
    assert_eq!(
        split_proposal.retired_collaboration_instance_ids,
        vec!["research:1".to_string()]
    );
    assert_eq!(
        split_proposal.completion.required_node_ids,
        vec!["research-left".to_string(), "research-right".to_string()]
    );
    let mut dispute_patch = patch.clone();
    dispute_patch.operation =
        harness_contract::execution_graph::CollaborationIntentPatchOperation::ResolveDispute {
            review: review.clone(),
            disputed_instance_ids: vec!["research:1".to_string()],
        };
    let dispute_request =
        collaboration_coordinator::compile_collaboration_intent_patch(&registered, &dispute_patch)
            .expect("fenced dispute resolution derives durable Team dependencies");
    assert_eq!(
        dispute_request
            .proposal
            .as_ref()
            .expect("dispute proposal")
            .nodes[0]
            .depends_on,
        vec!["research".to_string()]
    );
    let mut patch_request =
        collaboration_coordinator::compile_collaboration_intent_patch(&registered, &patch)
            .expect("fenced AddTeam patch compiles");
    let patch_node = patch_request
        .proposal
        .as_ref()
        .and_then(|proposal| proposal.nodes.first())
        .expect("AddTeam patch has exactly one semantic Team node");
    assert_eq!(patch_node.multiplicity, 1);
    assert_eq!(patch_request.constraints.max_parallel_agents, None);
    team_authority::bind_semantic_resource_authority(
        &mut patch_request,
        None,
        services.workspace_root(),
    );
    ensure_test_team_resource(&mut patch_request);
    let patch_plan = planner::plan_runtime_orchestration(&patch_request);
    let patch_proposal = patch_request.proposal.as_ref().expect("patch proposal");
    let existing_instances = program.semantic_node_instances.clone();
    let frozen_capacity = registered
        .nodes
        .iter()
        .find_map(|node| {
            serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
                &node.payload_ref,
            )
            .ok()
            .and_then(|request| request.execution_capacity)
        })
        .expect("existing Program Team carries a frozen capacity snapshot");
    let mut repairs = Vec::new();
    let mut mutation = compiler::compile_graph_mutation(
        "patch-revision",
        &patch_request,
        &patch_plan,
        patch_proposal,
        &registered.id,
        registered.parent_execution.as_ref(),
        services.team_runtime().as_ref(),
        Some(&frozen_capacity),
        &existing_instances,
        &mut repairs,
    )
    .expect("patch Team node compiles");
    assert!(repairs.is_empty());
    let compiled_team_request = serde_json::from_str::<
        harness_contract::team::TeamInstantiationRequest,
    >(&mutation.nodes[0].payload_ref)
    .expect("compiled custom Team request");
    assert!(matches!(
        compiled_team_request.template_selector,
        harness_contract::team::TeamTemplateSelector::Ephemeral { .. }
    ));
    services
        .compile_agent_task_nodes(&mut mutation.nodes)
        .expect("patch Agent intents compile");
    let mut delta = compiler::collaboration_program_from_proposal(
        patch_proposal,
        Some(&mutation.semantic_node_instances),
    )
    .expect("patch Program delta compiles")
    .expect("Team patch has a Program delta");
    collaboration_coordinator::prepare_program_revision_admission(
        &registered,
        &mut delta,
        mutation.nodes.clone(),
        services.team_runtime().as_ref(),
    )
    .expect("patch admission is fully prepared before commit");
    let completion = compiler::materialize_completion(
        &patch_proposal.completion,
        &mutation.semantic_node_instances,
        &patch_proposal.nodes,
        &mutation.nodes,
    );
    let committed = services
        .commit_service()
        .replan_semantic(
            &registered,
            mutation.nodes.clone(),
            mutation.edges.clone(),
            patch_proposal.reason.clone(),
            patch_proposal.mutation_id.clone(),
            completion,
            Some(delta),
            patch_proposal.collaboration_escalation.clone(),
        )
        .expect("patch revision commits atomically")
        .graph;
    let revised = committed
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .expect("revised Program");
    assert_eq!(revised.revision, program.revision + 1);
    let escalation_receipt = committed
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_escalations.first())
        .expect("applied escalation has a durable receipt");
    assert_eq!(escalation_receipt.escalation_id, escalation.digest);
    assert_eq!(escalation_receipt.source_attempt, escalation.source_attempt);
    assert_eq!(
        escalation_receipt.applied_graph_revision,
        committed.revision
    );
    assert_eq!(revised.team_instances.len(), 2);
    assert_eq!(revised.control.obligations.len(), 2);
    assert_eq!(
        revised.control.lifecycle,
        harness_contract::execution_graph::CollaborationProgramLifecycle::Admitting
    );
    assert!(revised.control.obligations.iter().all(|obligation| {
        obligation.revision == revised.revision
            && obligation.binding_ref.starts_with("team-binding:sha256:")
    }));
    assert!(revised
        .semantic_node_instances
        .contains_key("independent-review"));
    assert!(
        collaboration_coordinator::compile_collaboration_intent_patch(&committed, &patch)
            .is_err_and(|error| error == "patch_program_revision_conflict")
    );
}

#[tokio::test]
async fn collaboration_coordinator_records_rejected_team_admission_as_typed_program_truth() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    ensure_test_mission(&services);
    let team = parallel_research_team("rejected-team", Vec::new());
    let mut request = proposal(vec![team]);
    request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
        decision_id: "coordinator-rejection".to_string(),
        decision_revision: 1,
        decision_lease: "coordinator-rejection-lease".to_string(),
        turn_ref: "turn-v621".to_string(),
    });
    team_authority::bind_semantic_resource_authority(&mut request, None, services.workspace_root());
    ensure_test_team_resource(&mut request);
    let plan = planner::plan_runtime_orchestration(&request);
    let compiled = compiler::compile_orchestration(
        "coordinator-rejection",
        &request,
        &plan,
        None,
        Some(services.team_runtime().as_ref()),
    )
    .expect("Team root compiles");
    let mut graph = services
        .compile_graph_agent_intents(compiled.graph)
        .expect("agent intents compile");
    collaboration_coordinator::prepare_program_admission(
        &mut graph,
        services.team_runtime().as_ref(),
    )
    .expect("Program admission control compiles");
    let node_id = graph.nodes[0].id.clone();
    let registered = services
        .execution_supervisor()
        .register_graph(graph)
        .await
        .expect("register Program graph");
    collaboration_coordinator::mark_team_admission_rejected(
        &registered.id,
        &node_id,
        services.execution_supervisor().as_ref(),
        services.graph_state_store(),
    )
    .await
    .expect("typed rejection commits");
    let updated = services
        .graph_state_store()
        .load(&registered.id)
        .expect("load rejected Program");
    let control = &updated
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .expect("Program")
        .control;
    assert_eq!(
        control.lifecycle,
        harness_contract::execution_graph::CollaborationProgramLifecycle::Blocked
    );
    assert_eq!(
        control.blocker_ref.as_deref(),
        Some(format!("execution-node:{node_id}").as_str())
    );
    assert_eq!(
        control.next_action.as_deref(),
        Some("inspect_team_admission_failure")
    );
    assert_eq!(
        control.obligations[0].state,
        harness_contract::execution_graph::TeamAdmissionState::BlockedPolicy
    );
    assert_eq!(
        control.obligations[0].reason_kind.as_deref(),
        Some("team_admission_rejected")
    );
}
