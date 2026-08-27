//! Deterministic lowering of model-authored collaboration semantics.
//!
//! This is the only path that converts a turn-scoped Team intent into the
//! existing immutable Team snapshot request.  It deliberately has no display
//! name branches, no builtin substitution and no model-authored behavior or
//! physical identity fields.

use std::collections::{BTreeMap, BTreeSet};

use harness_contract::{
    execution_graph::{
        CollaborationIntentLifecycle, CollaborationIntentOrigin,
        CollaborationSemanticIntentSnapshot, CollaborationSemanticRoleSnapshot,
        CollaborationSemanticTeamSnapshot,
    },
    orchestration::{
        CapabilityRecipeId, ModelCollaborationControlDecisionV2, ModelCollaborationDependencyKind,
        ModelRoleIntent, ModelSemanticAcceptanceCriterion,
    },
    policy::PermissionMode,
    team::RoleBehaviorFacet,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    resolve_agent_capability,
    team_template_candidate::{ProposedDependency, ProposedRole, TeamTemplateProposal},
    AgentCapabilityRequest, AgentCatalogEntry, RuntimeServices,
};

use super::{GraphMutationProposal, GraphSemanticNode, RuntimeOrchestrationCommand};

pub const INTENT_COMPILER_REVISION: &str = "collaboration-intent/v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationCompilePhase {
    Decode,
    Validate,
    Resolve,
    Bind,
    Lower,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollaborationCompileDiagnostic {
    pub code: String,
    pub phase: CollaborationCompilePhase,
    pub field_paths: Vec<String>,
    pub semantic_ids: Vec<String>,
    pub missing_capabilities: Vec<String>,
    pub missing_skills: Vec<String>,
    pub missing_tools: Vec<String>,
    pub authorization_gap: bool,
    pub repairability: String,
    pub allowed_repairs: Vec<String>,
}

impl CollaborationCompileDiagnostic {
    fn validation(code: impl Into<String>, field_path: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            phase: CollaborationCompilePhase::Validate,
            field_paths: vec![field_path.into()],
            semantic_ids: Vec::new(),
            missing_capabilities: Vec::new(),
            missing_skills: Vec::new(),
            missing_tools: Vec::new(),
            authorization_gap: false,
            repairability: "model_revise".to_string(),
            allowed_repairs: vec!["supply_complete_semantic_intent".to_string()],
        }
    }

    fn resolver(
        role_id: &str,
        missing_capabilities: Vec<String>,
        missing_skills: Vec<String>,
        missing_tools: Vec<String>,
    ) -> Self {
        Self {
            code: "role_resolution_gap".to_string(),
            phase: CollaborationCompilePhase::Resolve,
            field_paths: vec![format!("roles[{role_id}]")],
            semantic_ids: vec![role_id.to_string()],
            missing_capabilities,
            missing_skills,
            missing_tools,
            authorization_gap: false,
            repairability: "model_revise".to_string(),
            allowed_repairs: vec!["adjust_capability_skill_or_tool_requirements".to_string()],
        }
    }
}

impl std::fmt::Display for CollaborationCompileDiagnostic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        serde_json::to_string(self)
            .map_err(|_| std::fmt::Error)
            .and_then(|value| formatter.write_str(&value))
    }
}

impl std::error::Error for CollaborationCompileDiagnostic {}

#[derive(Debug, Error)]
pub enum IntentCompilerError {
    #[error("{0}")]
    Diagnostic(#[from] CollaborationCompileDiagnostic),
    #[error("intent compiler internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Clone)]
pub struct CompiledCollaborationIntent {
    pub proposal: GraphMutationProposal,
    pub template_proposal: serde_json::Value,
    pub semantic_intent: CollaborationSemanticIntentSnapshot,
}

/// Compile an authenticated v2 decision into the legacy-free Runtime command
/// ingredients consumed by the existing graph/Team compiler.
pub fn compile_turn_scoped_intent(
    request: &RuntimeOrchestrationCommand,
    decision: &ModelCollaborationControlDecisionV2,
    services: &RuntimeServices,
) -> Result<CompiledCollaborationIntent, IntentCompilerError> {
    if decision.schema_version != 2 {
        return Err(CollaborationCompileDiagnostic::validation(
            "unsupported_collaboration_schema_version",
            "schema_version",
        )
        .into());
    }
    require_non_empty(&decision.decision_id, "decision_id")?;
    require_non_empty(&decision.intent, "intent")?;
    require_non_empty(&decision.reason, "reason")?;
    if decision.workstreams.is_empty() {
        return Err(CollaborationCompileDiagnostic::validation(
            "collaboration_workstreams_empty",
            "workstreams",
        )
        .into());
    }
    let lineage = request.lineage.as_ref().ok_or_else(|| {
        IntentCompilerError::Diagnostic(CollaborationCompileDiagnostic::validation(
            "collaboration_turn_lineage_missing",
            "runtime.lineage",
        ))
    })?;
    let catalog = services
        .definition_registry()
        .runnable_agent_catalog()
        .map_err(|error| IntentCompilerError::Internal(format!("agent catalog: {error}")))?;
    let mut workstream_ids = BTreeSet::new();
    let mut nodes = Vec::with_capacity(decision.workstreams.len());
    let mut team_entries = Vec::with_capacity(decision.workstreams.len());
    let mut semantic_teams = Vec::with_capacity(decision.workstreams.len());
    let mut binding_material = Vec::new();

    for (workstream_index, workstream) in decision.workstreams.iter().enumerate() {
        require_non_empty(
            &workstream.workstream_id,
            &format!("workstreams[{workstream_index}].workstream_id"),
        )?;
        require_non_empty(
            &workstream.objective,
            &format!("workstreams[{workstream_index}].objective"),
        )?;
        if !workstream_ids.insert(workstream.workstream_id.as_str()) {
            return Err(CollaborationCompileDiagnostic::validation(
                "duplicate_workstream_id",
                format!("workstreams[{workstream_index}].workstream_id"),
            )
            .into());
        }
        let compiled_team = compile_team(
            workstream_index,
            &workstream.workstream_id,
            &workstream.team,
            &catalog,
            request.constraints.permission_ceiling,
        )?;
        validate_workstream_artifact_contract(
            workstream_index,
            workstream,
            &compiled_team.template.result_fields,
        )?;
        let mut evidence_contract = workstream
            .evidence_contract
            .iter()
            // A Team snapshot owns its terminal artifact/structured-output
            // contract.  Copying every workstream criterion into the parent
            // Team-node acceptance created a second, conflicting contract:
            // DeepSeek could satisfy the role snapshot but the Team verifier
            // later demanded an unrelated `unresolved` field.  Only source
            // scope criteria are root-node lease requirements.
            .filter(|criterion| {
                matches!(
                    criterion,
                    ModelSemanticAcceptanceCriterion::EvidenceScope { .. }
                )
            })
            .map(criterion_key)
            .collect::<Vec<_>>();
        // Role-local scope requirements must be promoted to the Team lease so
        // instantiation can both authorize the read and crop it back to the
        // exact role. Ordinary role acceptance remains role-local.
        evidence_contract.extend(
            workstream
                .team
                .roles
                .iter()
                .flat_map(|role| role.acceptance.iter())
                .filter(|criterion| {
                    matches!(
                        criterion,
                        ModelSemanticAcceptanceCriterion::EvidenceScope { .. }
                    )
                })
                .map(criterion_key),
        );
        evidence_contract.sort();
        evidence_contract.dedup();
        let team_value = serde_json::to_value(&compiled_team.template)
            .map_err(|error| IntentCompilerError::Internal(error.to_string()))?;
        team_entries.push(serde_json::json!({
            "node_id": workstream.workstream_id,
            "template": team_value,
        }));
        binding_material.push(serde_json::json!({
            "workstream_id": workstream.workstream_id,
            "roles": compiled_team.resolved_bindings,
        }));
        nodes.push(GraphSemanticNode {
            node_id: workstream.workstream_id.clone(),
            recipe: CapabilityRecipeId::Team,
            objective: workstream.objective.clone(),
            depends_on: canonical_set(&workstream.depends_on),
            multiplicity: 1,
            focuses: Vec::new(),
            managed_agent_escalation: workstream.managed_agent_escalation,
            template: None,
            target_session_id: None,
            output_artifacts: canonical_set(&workstream.output_artifacts),
            evidence_contract,
            required_evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            required: true,
            dependency: Default::default(),
            cancellation_group: None,
        });
        semantic_teams.push(compiled_team.semantic_snapshot);
    }
    validate_workstream_dependencies(&nodes)?;
    let intent_digest = digest_json(decision)?;
    let binding_digest = digest_json(&binding_material)?;
    let semantic_intent = CollaborationSemanticIntentSnapshot {
        schema_version: decision.schema_version,
        decision_id: decision.decision_id.clone(),
        intent_digest,
        origin: CollaborationIntentOrigin::UserDirectedTurnScoped,
        lifecycle: CollaborationIntentLifecycle::TurnScoped,
        source_session_ref: lineage.session_id.clone(),
        source_turn_ref: lineage.turn_id.clone(),
        compiler_revision: INTENT_COMPILER_REVISION.to_string(),
        binding_digest,
        teams: semantic_teams,
        ai_composed: true,
        published_template_ref: None,
    };
    Ok(CompiledCollaborationIntent {
        proposal: GraphMutationProposal {
            mutation_id: format!("control-decision:{}", decision.decision_id),
            target_execution_id: None,
            expected_revision: None,
            nodes,
            completion: Default::default(),
            collaboration_program: None,
            collaboration_escalation: None,
            retired_collaboration_instance_ids: Vec::new(),
            reason: decision.reason.clone(),
        },
        template_proposal: serde_json::json!({ "teams": team_entries }),
        semantic_intent,
    })
}

fn validate_workstream_artifact_contract(
    workstream_index: usize,
    workstream: &harness_contract::orchestration::ModelCollaborationWorkstreamV2,
    team_result_fields: &[String],
) -> Result<(), IntentCompilerError> {
    for (criterion_index, criterion) in workstream.evidence_contract.iter().enumerate() {
        let ModelSemanticAcceptanceCriterion::Artifact { artifact } = criterion else {
            continue;
        };
        if team_result_fields.iter().any(|field| field == artifact) {
            continue;
        }
        let mut diagnostic = CollaborationCompileDiagnostic::validation(
            "workstream_artifact_not_in_team_result",
            format!("workstreams[{workstream_index}].evidence_contract[{criterion_index}]"),
        );
        diagnostic.semantic_ids = vec![workstream.workstream_id.clone(), artifact.clone()];
        diagnostic.allowed_repairs = vec![
            "add_artifact_to_team_result_and_terminal_role_output".to_string(),
            "remove_nonterminal_workstream_artifact_criterion".to_string(),
        ];
        return Err(diagnostic.into());
    }
    Ok(())
}

#[derive(Debug)]
struct CompiledTeam {
    template: TeamTemplateProposal,
    semantic_snapshot: CollaborationSemanticTeamSnapshot,
    resolved_bindings: Vec<serde_json::Value>,
}

fn compile_team(
    workstream_index: usize,
    workstream_id: &str,
    team: &harness_contract::orchestration::ModelTurnScopedTeamIntent,
    catalog: &[AgentCatalogEntry],
    ceiling: PermissionMode,
) -> Result<CompiledTeam, IntentCompilerError> {
    require_non_empty(
        &team.team_key,
        &format!("workstreams[{workstream_index}].team.team_key"),
    )?;
    if team.roles.is_empty() {
        return Err(CollaborationCompileDiagnostic::validation(
            "team_roles_empty",
            format!("workstreams[{workstream_index}].team.roles"),
        )
        .into());
    }
    let mut role_ids = BTreeSet::new();
    let mut canonical_ids = BTreeMap::new();
    for (index, role) in team.roles.iter().enumerate() {
        require_non_empty(
            &role.role_id,
            &format!("workstreams[{workstream_index}].team.roles[{index}].role_id"),
        )?;
        require_non_empty(
            &role.responsibility,
            &format!("workstreams[{workstream_index}].team.roles[{index}].responsibility"),
        )?;
        if !role_ids.insert(role.role_id.as_str()) {
            return Err(CollaborationCompileDiagnostic::validation(
                "duplicate_role_id",
                format!("workstreams[{workstream_index}].team.roles[{index}].role_id"),
            )
            .into());
        }
        canonical_ids.insert(role.role_id.clone(), canonical_role_id(&role.role_id));
        validate_cardinality(role, workstream_index, index)?;
    }
    let dependencies = validate_role_dependencies(team, &canonical_ids, workstream_index)?;
    let incoming = incoming_dependencies(&dependencies);
    let outgoing = outgoing_dependencies(&dependencies);
    // Runtime's durable Team contract represents required evidence as an
    // `evidence` result artifact. The model-facing V2 guidance names this
    // invariant explicitly, while the compiler preserves it even if a client
    // omitted the redundant result-field spelling.
    let mut result_fields = canonical_set(&team.result.required_artifacts);
    if team.result.evidence_required && !result_fields.iter().any(|field| field == "evidence") {
        result_fields.push("evidence".to_string());
    }
    let terminal_role = terminal_role_id(
        team,
        &canonical_ids,
        &outgoing,
        &result_fields,
        workstream_index,
    )?;
    let mut roles = Vec::with_capacity(team.roles.len());
    let mut resolved_bindings = Vec::with_capacity(team.roles.len());
    let mut semantic_roles = Vec::with_capacity(team.roles.len());
    for (index, role) in team.roles.iter().enumerate() {
        let canonical_id = canonical_ids
            .get(&role.role_id)
            .expect("canonical role id exists");
        let selected = resolve_role(role, catalog, ceiling)?;
        let behavior = derive_behavior(
            role,
            canonical_id,
            &incoming,
            &outgoing,
            terminal_role.as_deref(),
        );
        let acceptance = canonical_acceptance(role);
        roles.push(ProposedRole {
            role_id: canonical_id.clone(),
            display_name: Some(
                role.display_name
                    .clone()
                    .unwrap_or_else(|| role.role_id.clone()),
            ),
            responsibility: role.responsibility.trim().to_string(),
            agent_definition_ref: format!(
                "{}@{}",
                selected.definition_ref.definition_id.as_str(),
                selected.definition_ref.revision
            ),
            grant_ceiling: canonical_set(&role.required_capabilities),
            fixed_count: None,
            min_count: Some(u32::from(role.cardinality.min)),
            max_count: Some(u32::from(role.cardinality.max)),
            acceptance,
            input_artifacts: canonical_set(&role.input_artifacts),
            output_artifacts: canonical_set(&role.output_artifacts),
            allowed_tool_contract_refs: canonical_tool_refs(&role.required_tools),
            allowed_skill_refs: canonical_set(&role.required_skills),
            behavior,
        });
        resolved_bindings.push(serde_json::json!({
            "role_id": canonical_id,
            "definition": selected.definition_ref.definition_id.as_str(),
            "revision": selected.definition_ref.revision,
            "required_capabilities": canonical_set(&role.required_capabilities),
            "required_skills": canonical_set(&role.required_skills),
            "required_tools": canonical_set(&role.required_tools),
        }));
        semantic_roles.push(CollaborationSemanticRoleSnapshot {
            role_id: canonical_id.clone(),
            display_name: role
                .display_name
                .clone()
                .or_else(|| Some(role.role_id.clone())),
            responsibility: role.responsibility.trim().to_string(),
            required_capabilities: canonical_set(&role.required_capabilities),
            required_skills: canonical_set(&role.required_skills),
            required_tools: canonical_set(&role.required_tools),
            input_artifacts: canonical_set(&role.input_artifacts),
            output_artifacts: canonical_set(&role.output_artifacts),
        });
        let _ = index;
    }
    let template_id = format!(
        "turn-{}",
        short_digest(&format!("{workstream_id}:{}", team.team_key))
    );
    let dependencies_for_template = dependencies
        .iter()
        .map(|dependency| ProposedDependency {
            from: dependency.from.clone(),
            to: dependency.to.clone(),
        })
        .collect();
    Ok(CompiledTeam {
        template: TeamTemplateProposal {
            template_id,
            name: team
                .display_name
                .clone()
                .unwrap_or_else(|| format!("AI composed {workstream_id}")),
            team_display_name: team.display_name.clone(),
            role_display_names: Vec::new(),
            roles,
            dependencies: dependencies_for_template,
            result_fields,
            evidence_required: team.result.evidence_required,
            instructions: if team.instructions.trim().is_empty() {
                format!("# AI composed team\n\n{}\n", workstream_id)
            } else {
                team.instructions.clone()
            },
        },
        semantic_snapshot: CollaborationSemanticTeamSnapshot {
            workstream_id: workstream_id.to_string(),
            team_key: team.team_key.clone(),
            display_name: team.display_name.clone(),
            roles: semantic_roles,
            dependencies: dependencies
                .iter()
                .map(|dependency| {
                    format!(
                        "{}:{:?}:{}",
                        dependency.from, dependency.kind, dependency.to
                    )
                })
                .collect(),
        },
        resolved_bindings,
    })
}

#[derive(Debug, Clone)]
struct CanonicalDependency {
    from: String,
    to: String,
    kind: ModelCollaborationDependencyKind,
}

fn validate_role_dependencies(
    team: &harness_contract::orchestration::ModelTurnScopedTeamIntent,
    canonical_ids: &BTreeMap<String, String>,
    workstream_index: usize,
) -> Result<Vec<CanonicalDependency>, IntentCompilerError> {
    let role_by_original = team
        .roles
        .iter()
        .map(|role| (role.role_id.as_str(), role))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut edges = Vec::new();
    for (index, dependency) in team.dependencies.iter().enumerate() {
        let path = format!("workstreams[{workstream_index}].team.dependencies[{index}]");
        let Some(from) = canonical_ids.get(&dependency.from) else {
            return Err(CollaborationCompileDiagnostic::validation(
                "dependency_source_unknown",
                format!("{path}.from"),
            )
            .into());
        };
        let Some(to) = canonical_ids.get(&dependency.to) else {
            return Err(CollaborationCompileDiagnostic::validation(
                "dependency_target_unknown",
                format!("{path}.to"),
            )
            .into());
        };
        if from == to || !seen.insert((from.clone(), to.clone())) {
            return Err(CollaborationCompileDiagnostic::validation(
                "dependency_duplicate_or_self",
                path,
            )
            .into());
        }
        if dependency.artifacts.is_empty() {
            return Err(CollaborationCompileDiagnostic::validation(
                "dependency_artifacts_missing",
                format!("{path}.artifacts"),
            )
            .into());
        }
        let source = role_by_original[dependency.from.as_str()];
        let target = role_by_original[dependency.to.as_str()];
        let artifacts = canonical_set(&dependency.artifacts);
        if !artifacts
            .iter()
            .all(|artifact| source.output_artifacts.contains(artifact))
            || !artifacts
                .iter()
                .all(|artifact| target.input_artifacts.contains(artifact))
        {
            return Err(CollaborationCompileDiagnostic::validation(
                "dependency_artifact_contract_mismatch",
                path,
            )
            .into());
        }
        edges.push(CanonicalDependency {
            from: from.clone(),
            to: to.clone(),
            kind: dependency.kind.clone(),
        });
    }
    if has_cycle(canonical_ids.values().cloned().collect(), &edges) {
        return Err(CollaborationCompileDiagnostic::validation(
            "role_dependency_cycle",
            format!("workstreams[{workstream_index}].team.dependencies"),
        )
        .into());
    }
    Ok(edges)
}

fn resolve_role(
    role: &ModelRoleIntent,
    catalog: &[AgentCatalogEntry],
    ceiling: PermissionMode,
) -> Result<AgentCatalogEntry, IntentCompilerError> {
    let capabilities = canonical_set(&role.required_capabilities);
    let skills = canonical_set(&role.required_skills);
    let tools = canonical_tool_refs(&role.required_tools);
    if capabilities.is_empty() {
        return Err(CollaborationCompileDiagnostic::resolver(
            &role.role_id,
            vec!["at_least_one_capability_required".to_string()],
            Vec::new(),
            tools,
        )
        .into());
    }
    let mut invalid_or_unauthorized = Vec::new();
    for capability in &capabilities {
        if !capability_allowed_by_ceiling(ceiling, capability) {
            invalid_or_unauthorized.push(capability.clone());
        }
    }
    if !invalid_or_unauthorized.is_empty() {
        let mut diagnostic = CollaborationCompileDiagnostic::resolver(
            &role.role_id,
            invalid_or_unauthorized,
            Vec::new(),
            tools,
        );
        diagnostic.code = "authorization_gap".to_string();
        diagnostic.phase = CollaborationCompilePhase::Bind;
        diagnostic.authorization_gap = true;
        diagnostic.repairability = "user_decision".to_string();
        diagnostic.allowed_repairs = vec!["request_authorized_capabilities".to_string()];
        return Err(diagnostic.into());
    }
    let available_tools = resolve_agent_capability(AgentCapabilityRequest {
        role_id: role.role_id.clone(),
        allowed_capabilities: capabilities.clone(),
        evidence_duties: Vec::new(),
    })
    .allowed_tools;
    let missing_tools = tools
        .iter()
        .filter(|tool| !available_tools.contains(*tool))
        .cloned()
        .collect::<Vec<_>>();
    if !missing_tools.is_empty() {
        return Err(CollaborationCompileDiagnostic::resolver(
            &role.role_id,
            Vec::new(),
            Vec::new(),
            missing_tools,
        )
        .into());
    }
    let mut eligible = catalog
        .iter()
        .filter(|entry| {
            capabilities
                .iter()
                .all(|required| entry.capabilities.contains(required))
                && skills
                    .iter()
                    .all(|required| entry.skill_refs.contains(required))
        })
        .cloned()
        .collect::<Vec<_>>();
    eligible.sort_by(|left, right| {
        let left_excess = left.capabilities.len().saturating_sub(capabilities.len());
        let right_excess = right.capabilities.len().saturating_sub(capabilities.len());
        let left_skill_excess = left.skill_refs.len().saturating_sub(skills.len());
        let right_skill_excess = right.skill_refs.len().saturating_sub(skills.len());
        left_excess
            .cmp(&right_excess)
            .then_with(|| left_skill_excess.cmp(&right_skill_excess))
            .then_with(|| left.scope.as_str().cmp(right.scope.as_str()))
            .then_with(|| {
                left.definition_ref
                    .definition_id
                    .as_str()
                    .cmp(right.definition_ref.definition_id.as_str())
            })
            .then_with(|| {
                left.definition_ref
                    .revision
                    .cmp(&right.definition_ref.revision)
            })
    });
    eligible.into_iter().next().ok_or_else(|| {
        CollaborationCompileDiagnostic::resolver(&role.role_id, capabilities, skills, tools).into()
    })
}

fn derive_behavior(
    role: &ModelRoleIntent,
    canonical_id: &str,
    incoming: &BTreeMap<String, Vec<ModelCollaborationDependencyKind>>,
    outgoing: &BTreeMap<String, usize>,
    terminal_role: Option<&str>,
) -> Vec<RoleBehaviorFacet> {
    let mut behavior = Vec::new();
    let incoming_kinds = incoming.get(canonical_id).cloned().unwrap_or_default();
    if !incoming_kinds.is_empty() {
        behavior.push(RoleBehaviorFacet::UpstreamConsumption { required: true });
    }
    if incoming_kinds
        .iter()
        .any(|kind| matches!(kind, ModelCollaborationDependencyKind::ReviewOf))
        || role.acceptance.iter().any(|criterion| {
            matches!(
                criterion,
                ModelSemanticAcceptanceCriterion::IndependentReview { .. }
            )
        })
    {
        behavior.push(RoleBehaviorFacet::Verification {
            mode: "semantic_review".to_string(),
        });
    }
    if incoming_kinds
        .iter()
        .any(|kind| matches!(kind, ModelCollaborationDependencyKind::Aggregate))
        && outgoing.get(canonical_id).copied().unwrap_or_default() == 0
    {
        behavior.push(RoleBehaviorFacet::Reducer {
            mode: "semantic_aggregate".to_string(),
        });
    }
    if role.acceptance.iter().any(|criterion| {
        matches!(
            criterion,
            ModelSemanticAcceptanceCriterion::EvidenceScope { .. }
        )
    }) {
        behavior.push(RoleBehaviorFacet::ReacquireEvidence { required: true });
    }
    if terminal_role == Some(canonical_id) {
        behavior.push(RoleBehaviorFacet::TerminalCandidate { required: true });
    }
    if behavior.is_empty() {
        behavior.push(RoleBehaviorFacet::ReacquireEvidence { required: false });
    }
    behavior
}

fn terminal_role_id(
    team: &harness_contract::orchestration::ModelTurnScopedTeamIntent,
    canonical_ids: &BTreeMap<String, String>,
    outgoing: &BTreeMap<String, usize>,
    result_fields: &[String],
    workstream_index: usize,
) -> Result<Option<String>, IntentCompilerError> {
    if !team.result.synthesis_required {
        return Ok(None);
    }
    if result_fields.is_empty() {
        return Err(CollaborationCompileDiagnostic::validation(
            "synthesis_result_artifacts_missing",
            format!("workstreams[{workstream_index}].team.result.required_artifacts"),
        )
        .into());
    }
    let required = result_fields.to_vec();
    let candidates = team
        .roles
        .iter()
        .filter_map(|role| {
            let canonical_id = canonical_ids.get(&role.role_id)?;
            (outgoing.get(canonical_id).copied().unwrap_or_default() == 0
                && required
                    .iter()
                    .all(|artifact| role.output_artifacts.contains(artifact)))
            .then(|| canonical_id.clone())
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        let terminal_roles = team
            .roles
            .iter()
            .filter_map(|role| {
                let canonical_id = canonical_ids.get(&role.role_id)?;
                (outgoing.get(canonical_id).copied().unwrap_or_default() == 0)
                    .then(|| role.role_id.clone())
            })
            .collect::<Vec<_>>();
        let mut diagnostic = CollaborationCompileDiagnostic::validation(
            "completion_terminal_role_missing",
            format!("workstreams[{workstream_index}].team.roles[*].output_artifacts"),
        );
        diagnostic.semantic_ids = terminal_roles;
        diagnostic.allowed_repairs =
            vec!["assign_every_required_result_artifact_to_one_terminal_role".to_string()];
        return Err(diagnostic.into());
    }
    if candidates.len() > 1 {
        let mut diagnostic = CollaborationCompileDiagnostic::validation(
            "completion_terminal_role_ambiguous",
            format!("workstreams[{workstream_index}].team.dependencies"),
        );
        diagnostic.semantic_ids = candidates;
        diagnostic.allowed_repairs =
            vec!["retain_exactly_one_terminal_role_for_required_result_artifacts".to_string()];
        return Err(diagnostic.into());
    }
    Ok(candidates.into_iter().next())
}

fn validate_cardinality(
    role: &ModelRoleIntent,
    workstream_index: usize,
    role_index: usize,
) -> Result<(), IntentCompilerError> {
    if role.cardinality.min == 0
        || role.cardinality.preferred == 0
        || role.cardinality.max == 0
        || role.cardinality.min > role.cardinality.preferred
        || role.cardinality.preferred > role.cardinality.max
    {
        return Err(CollaborationCompileDiagnostic::validation(
            "role_cardinality_invalid",
            format!("workstreams[{workstream_index}].team.roles[{role_index}].cardinality"),
        )
        .into());
    }
    Ok(())
}

fn validate_workstream_dependencies(
    nodes: &[GraphSemanticNode],
) -> Result<(), IntentCompilerError> {
    let ids = nodes
        .iter()
        .map(|node| node.node_id.as_str())
        .collect::<BTreeSet<_>>();
    for node in nodes {
        if node
            .depends_on
            .iter()
            .any(|dependency| !ids.contains(dependency.as_str()))
        {
            return Err(CollaborationCompileDiagnostic::validation(
                "workstream_dependency_unknown",
                format!("workstreams[{}].depends_on", node.node_id),
            )
            .into());
        }
    }
    let edges = nodes
        .iter()
        .flat_map(|node| {
            node.depends_on
                .iter()
                .map(move |dependency| (dependency.clone(), node.node_id.clone()))
        })
        .collect::<Vec<_>>();
    if has_cycle(
        nodes.iter().map(|node| node.node_id.clone()).collect(),
        &edges
            .into_iter()
            .map(|(from, to)| CanonicalDependency {
                from,
                to,
                kind: ModelCollaborationDependencyKind::Handoff,
            })
            .collect::<Vec<_>>(),
    ) {
        return Err(CollaborationCompileDiagnostic::validation(
            "workstream_dependency_cycle",
            "workstreams",
        )
        .into());
    }
    Ok(())
}

fn incoming_dependencies(
    edges: &[CanonicalDependency],
) -> BTreeMap<String, Vec<ModelCollaborationDependencyKind>> {
    let mut incoming = BTreeMap::<String, Vec<ModelCollaborationDependencyKind>>::new();
    for edge in edges {
        incoming
            .entry(edge.to.clone())
            .or_default()
            .push(edge.kind.clone());
    }
    incoming
}

fn outgoing_dependencies(edges: &[CanonicalDependency]) -> BTreeMap<String, usize> {
    let mut outgoing = BTreeMap::<String, usize>::new();
    for edge in edges {
        *outgoing.entry(edge.from.clone()).or_default() += 1;
    }
    outgoing
}

fn has_cycle(nodes: Vec<String>, edges: &[CanonicalDependency]) -> bool {
    let mut incoming = nodes
        .iter()
        .cloned()
        .map(|node| (node, 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut successors = BTreeMap::<String, Vec<String>>::new();
    for edge in edges {
        *incoming.entry(edge.to.clone()).or_default() += 1;
        successors
            .entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
    }
    let mut ready = incoming
        .iter()
        .filter_map(|(node, count)| (*count == 0).then(|| node.clone()))
        .collect::<BTreeSet<_>>();
    let mut seen = 0usize;
    while let Some(node) = ready.pop_first() {
        seen += 1;
        for successor in successors.get(&node).into_iter().flatten() {
            let count = incoming
                .get_mut(successor)
                .expect("edge destination exists");
            *count -= 1;
            if *count == 0 {
                ready.insert(successor.clone());
            }
        }
    }
    seen != nodes.len()
}

fn canonical_acceptance(role: &ModelRoleIntent) -> Vec<String> {
    let mut values = role
        .acceptance
        .iter()
        .map(criterion_key)
        .collect::<Vec<_>>();
    if values.is_empty() {
        values.push("evidence".to_string());
    }
    values.sort();
    values.dedup();
    values
}

fn criterion_key(criterion: &ModelSemanticAcceptanceCriterion) -> String {
    match criterion {
        // The existing Team runtime already verifies its declared result
        // field names directly (`summary`, `evidence`, `findings`, ...). Keep
        // that compatibility at the lowering boundary; the semantic contract
        // retains the tagged `Artifact` form in provenance.
        ModelSemanticAcceptanceCriterion::Artifact { artifact } => artifact.trim().to_string(),
        ModelSemanticAcceptanceCriterion::EvidenceScope {
            operation,
            resource,
        } => {
            format!("evidence_scope:{operation}:{resource}")
        }
        ModelSemanticAcceptanceCriterion::StructuredField { path } => {
            format!("structured_field:{path}")
        }
        ModelSemanticAcceptanceCriterion::TerminalFact { fact } => format!("terminal_fact:{fact}"),
        ModelSemanticAcceptanceCriterion::CommittedEffect { effect } => {
            format!("committed_effect:{effect}")
        }
        ModelSemanticAcceptanceCriterion::IndependentReview { subject_role_id } => {
            format!("independent_review:{subject_role_id}")
        }
    }
}

fn canonical_role_id(value: &str) -> String {
    let trimmed = value.trim();
    let valid = !trimmed.is_empty()
        && trimmed.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        });
    if valid {
        trimmed.to_string()
    } else {
        format!("role-{}", short_digest(trimmed))
    }
}

fn canonical_set(values: &[String]) -> Vec<String> {
    let mut result = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    result.sort();
    result.dedup();
    result
}

fn canonical_tool_refs(values: &[String]) -> Vec<String> {
    canonical_set(values)
        .into_iter()
        .map(|value| value.strip_prefix("tool/").unwrap_or(&value).to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn capability_allowed_by_ceiling(ceiling: PermissionMode, capability: &str) -> bool {
    harness_contract::orchestration::model_collaboration_capabilities_for_permission(ceiling)
        .contains(&capability)
}

fn require_non_empty(value: &str, field_path: &str) -> Result<(), IntentCompilerError> {
    (!value.trim().is_empty()).then_some(()).ok_or_else(|| {
        CollaborationCompileDiagnostic::validation("required_field_missing", field_path).into()
    })
}

fn digest_json(value: &impl Serialize) -> Result<String, IntentCompilerError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| IntentCompilerError::Internal(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn short_digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::{
        execution_graph::ExecutionGraphLineage,
        orchestration::{
            ManagedAgentEscalationRequirement, ModelCollaborationControlDecisionV2,
            ModelCollaborationDependencyKind, ModelCollaborationWorkstreamV2, ModelRoleDependency,
            ModelRoleIntent, ModelRuntimeOrchestrationInput, ModelSemanticAcceptanceCriterion,
            ModelTeamResultIntent, ModelTurnScopedTeamIntent, RuntimeOrchestrationOperation,
        },
    };

    fn request() -> RuntimeOrchestrationCommand {
        RuntimeOrchestrationCommand::from_model(
            ModelRuntimeOrchestrationInput {
                intent: "audit the runtime".to_string(),
                operation: RuntimeOrchestrationOperation::Propose,
                inspect_execution_id: None,
                proposal: None,
                template_proposal: None,
                control: None,
                input_disposition: None,
                evidence_refs: Vec::new(),
                constraints: Default::default(),
            },
            crate::RuntimeOrchestrationBinding {
                model_lease: Some("test-model".to_string()),
                session_id: Some("session-v2".to_string()),
                lineage: Some(ExecutionGraphLineage {
                    session_id: "session-v2".to_string(),
                    turn_id: "turn-v2".to_string(),
                    root_task_id: "task-v2".to_string(),
                    task_id: "task-v2".to_string(),
                    generation: 1,
                }),
                mission_id: Some("mission-v2".to_string()),
                selection_mode: None,
                strategy_binding: None,
                capabilities: Vec::new(),
                surface: Some("test".to_string()),
                permission_ceiling: PermissionMode::ReadOnly,
            },
        )
    }

    fn decision() -> ModelCollaborationControlDecisionV2 {
        ModelCollaborationControlDecisionV2 {
            schema_version: 2,
            decision_id: "semantic-audit-v2".to_string(),
            intent: "audit runtime collaboration flow".to_string(),
            reason: "user requested an independent evidence-backed audit".to_string(),
            workstreams: vec![ModelCollaborationWorkstreamV2 {
                workstream_id: "runtime-audit".to_string(),
                objective: "inspect and synthesize the runtime flow".to_string(),
                depends_on: Vec::new(),
                output_artifacts: vec!["summary".to_string()],
                evidence_contract: vec![ModelSemanticAcceptanceCriterion::Artifact {
                    artifact: "summary".to_string(),
                }],
                managed_agent_escalation: ManagedAgentEscalationRequirement::None,
                team: ModelTurnScopedTeamIntent {
                    team_key: "runtime-audit-team".to_string(),
                    display_name: Some("任意本地化审计团队".to_string()),
                    instructions: "independently inspect, then synthesize the evidence".to_string(),
                    result: ModelTeamResultIntent {
                        required_artifacts: vec!["summary".to_string()],
                        evidence_required: true,
                        synthesis_required: true,
                    },
                    roles: vec![
                        ModelRoleIntent {
                            role_id: "任意取证角色".to_string(),
                            display_name: Some("任意名称 A".to_string()),
                            responsibility: "produce bounded evidence".to_string(),
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
                            role_id: "arbitrary-synthesizer".to_string(),
                            display_name: Some("任意名称 B".to_string()),
                            responsibility: "synthesize the supplied evidence".to_string(),
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
                        from: "任意取证角色".to_string(),
                        to: "arbitrary-synthesizer".to_string(),
                        kind: ModelCollaborationDependencyKind::Handoff,
                        artifacts: vec!["evidence".to_string()],
                    }],
                },
            }],
        }
    }

    #[test]
    fn arbitrary_localized_role_id_becomes_a_stable_machine_id() {
        assert_eq!(
            canonical_role_id("架构审查员"),
            canonical_role_id("架构审查员")
        );
        assert!(canonical_role_id("架构审查员").starts_with("role-"));
    }

    #[test]
    fn cycle_detection_is_name_independent() {
        assert!(has_cycle(
            vec!["a".to_string(), "b".to_string()],
            &[
                CanonicalDependency {
                    from: "a".to_string(),
                    to: "b".to_string(),
                    kind: ModelCollaborationDependencyKind::Handoff
                },
                CanonicalDependency {
                    from: "b".to_string(),
                    to: "a".to_string(),
                    kind: ModelCollaborationDependencyKind::Handoff
                },
            ],
        ));
    }

    #[test]
    fn compiles_arbitrary_role_names_to_exact_bound_snapshot() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let compiled = compile_turn_scoped_intent(&request(), &decision(), &services)
            .expect("semantic compilation");
        let roles = &compiled.template_proposal["teams"][0]["template"]["roles"];
        assert_eq!(roles.as_array().map(Vec::len), Some(2));
        assert!(roles[0]["agent_definition_ref"]
            .as_str()
            .is_some_and(|reference| reference.contains('@')));
        assert_eq!(
            compiled.semantic_intent.teams[0].roles[0]
                .display_name
                .as_deref(),
            Some("任意名称 A")
        );
        let proposal: TeamTemplateProposal =
            serde_json::from_value(compiled.template_proposal["teams"][0]["template"].clone())
                .expect("compiled template proposal");
        let candidate = crate::team_template_candidate::TemplateCandidateCompiler::compile(
            services.definition_registry().as_ref(),
            &proposal,
            PermissionMode::ReadOnly,
        )
        .expect("exact bound template");
        assert_eq!(
            candidate.manifest.roles[0]
                .task_contract
                .allowed_tool_contract_refs,
            vec!["read_file".to_string()]
        );
        assert!(compiled.semantic_intent.ai_composed);
        assert_eq!(
            compiled.semantic_intent.lifecycle,
            CollaborationIntentLifecycle::TurnScoped
        );
    }

    #[test]
    fn workstream_artifact_contract_must_match_the_team_terminal_result() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut invalid = decision();
        invalid.workstreams[0]
            .evidence_contract
            .push(ModelSemanticAcceptanceCriterion::Artifact {
                artifact: "unresolved".to_string(),
            });

        let error = compile_turn_scoped_intent(&request(), &invalid, &services)
            .expect_err("a workstream cannot demand an artifact the Team never emits");
        let diagnostic = match error {
            IntentCompilerError::Diagnostic(diagnostic) => diagnostic,
            other => panic!("expected semantic diagnostic, got {other}"),
        };
        assert_eq!(diagnostic.code, "workstream_artifact_not_in_team_result");
        assert_eq!(
            diagnostic.allowed_repairs,
            vec![
                "add_artifact_to_team_result_and_terminal_role_output",
                "remove_nonterminal_workstream_artifact_criterion",
            ]
        );
    }

    #[test]
    fn rejects_unknown_tool_without_role_or_builtin_fallback() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut invalid = decision();
        invalid.workstreams[0].team.roles[0].required_tools = vec!["nonexistent_tool".to_string()];
        let error = compile_turn_scoped_intent(&request(), &invalid, &services)
            .expect_err("unknown tool must fail closed");
        assert!(error.to_string().contains("role_resolution_gap"));
        assert!(error.to_string().contains("nonexistent_tool"));
    }

    #[test]
    fn rejects_missing_skill_without_substituting_an_agent() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut invalid = decision();
        invalid.workstreams[0].team.roles[0].required_skills = vec!["skill/unknown@1".to_string()];
        let error = compile_turn_scoped_intent(&request(), &invalid, &services)
            .expect_err("unknown skill must fail closed");
        assert!(error.to_string().contains("role_resolution_gap"));
        assert!(error.to_string().contains("skill/unknown@1"));
    }
}
