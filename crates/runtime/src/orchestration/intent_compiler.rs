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
    bind_agent_capability_to_host, resolve_agent_capability,
    team_template_candidate::{ProposedDependency, ProposedRole, TeamTemplateProposal},
    AgentCapabilityRequest, AgentCatalogEntry, RuntimeServices, RuntimeToolInventorySnapshot,
};

use super::{GraphMutationProposal, GraphSemanticNode, RuntimeOrchestrationCommand};

pub const INTENT_COMPILER_REVISION: &str = "collaboration-intent/v3";

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
    /// Exact capability sets currently backed by runnable immutable Agent
    /// Definitions. This is repair context, never an authorization grant.
    pub available_capability_profiles: Vec<Vec<String>>,
    /// Exact skill references currently present in the runnable catalog.
    pub available_skill_refs: Vec<String>,
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
            available_capability_profiles: Vec::new(),
            available_skill_refs: Vec::new(),
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
            available_capability_profiles: Vec::new(),
            available_skill_refs: Vec::new(),
            authorization_gap: false,
            repairability: "model_revise".to_string(),
            allowed_repairs: vec![
                "choose_one_supported_agent_capability_profile".to_string(),
                "split_incompatible_capabilities_across_roles".to_string(),
                "remove_unavailable_skill_or_tool_requirements".to_string(),
            ],
        }
    }

    fn with_agent_catalog(mut self, catalog: &[AgentCatalogEntry]) -> Self {
        self.available_capability_profiles = catalog
            .iter()
            .map(|entry| canonical_set(&entry.capabilities))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        self.available_skill_refs = catalog
            .iter()
            .flat_map(|entry| entry.skill_refs.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        self
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
    let cross_workstream_dataflow = validate_cross_workstream_artifact_dataflow(decision)?;
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
        validate_concrete_evidence_scopes(workstream_index, workstream, services)?;
        let compiled_team = compile_team(
            workstream_index,
            &workstream.workstream_id,
            &workstream.team,
            cross_workstream_dataflow
                .consumer_roles
                .get(&workstream.workstream_id)
                .cloned()
                .unwrap_or_default(),
            &catalog,
            request.tool_inventory.as_ref(),
            request.constraints.permission_ceiling,
            workstream.evidence_contract.iter().any(|criterion| {
                matches!(
                    criterion,
                    ModelSemanticAcceptanceCriterion::EvidenceScope { .. }
                )
            }),
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
            required_evidence_refs: cross_workstream_dataflow
                .input_artifacts
                .get(&workstream.workstream_id)
                .into_iter()
                .flatten()
                .map(|artifact| format!("artifact_kind:{artifact}"))
                .collect(),
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

/// Validate and classify artifact flow that crosses Team boundaries before a
/// Program identity is admitted. Local role-to-role artifacts remain owned by
/// the Team dependency compiler; only inputs without a local producer need a
/// workstream dependency and become cross-Team consumption behavior.
#[derive(Debug, Default)]
struct CrossWorkstreamArtifactDataflow {
    consumer_roles: BTreeMap<String, BTreeSet<String>>,
    input_artifacts: BTreeMap<String, BTreeSet<String>>,
}

fn validate_cross_workstream_artifact_dataflow(
    decision: &ModelCollaborationControlDecisionV2,
) -> Result<CrossWorkstreamArtifactDataflow, IntentCompilerError> {
    let mut workstreams = BTreeMap::new();
    for (index, workstream) in decision.workstreams.iter().enumerate() {
        let id = workstream.workstream_id.trim();
        if id.is_empty() {
            return Err(CollaborationCompileDiagnostic::validation(
                "collaboration_field_empty",
                format!("workstreams[{index}].workstream_id"),
            )
            .into());
        }
        if workstreams.insert(id, workstream).is_some() {
            return Err(CollaborationCompileDiagnostic::validation(
                "duplicate_workstream_id",
                format!("workstreams[{index}].workstream_id"),
            )
            .into());
        }
    }

    let mut dataflow = CrossWorkstreamArtifactDataflow::default();
    for (workstream_index, workstream) in decision.workstreams.iter().enumerate() {
        let mut predecessor_outputs = BTreeSet::new();
        for dependency in canonical_set(&workstream.depends_on) {
            let Some(predecessor) = workstreams.get(dependency.as_str()) else {
                return Err(CollaborationCompileDiagnostic::validation(
                    "workstream_dependency_unknown",
                    format!("workstreams[{workstream_index}].depends_on"),
                )
                .into());
            };
            predecessor_outputs.extend(canonical_set(&predecessor.output_artifacts));
            predecessor_outputs.extend(canonical_set(&predecessor.team.result.required_artifacts));
        }

        let local_outputs = workstream
            .team
            .roles
            .iter()
            .flat_map(|role| role.output_artifacts.iter().cloned())
            .collect::<BTreeSet<_>>();
        for (role_index, role) in workstream.team.roles.iter().enumerate() {
            let cross_inputs = canonical_set(&role.input_artifacts)
                .into_iter()
                .filter(|artifact| !local_outputs.contains(artifact))
                .collect::<Vec<_>>();
            if cross_inputs.is_empty() {
                continue;
            }
            if workstream.depends_on.is_empty() {
                let mut diagnostic = CollaborationCompileDiagnostic::validation(
                    "cross_workstream_input_without_dependency",
                    format!(
                        "workstreams[{workstream_index}].team.roles[{role_index}].input_artifacts"
                    ),
                );
                diagnostic.semantic_ids =
                    vec![workstream.workstream_id.clone(), role.role_id.clone()];
                diagnostic.allowed_repairs = vec![
                    "declare_the_producer_workstream_in_depends_on".to_string(),
                    "remove_the_unbound_input_artifact".to_string(),
                ];
                return Err(diagnostic.into());
            }
            let missing = cross_inputs
                .iter()
                .filter(|artifact| !predecessor_outputs.contains(*artifact))
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                let mut diagnostic = CollaborationCompileDiagnostic::validation(
                    "cross_workstream_input_artifact_unproduced",
                    format!(
                        "workstreams[{workstream_index}].team.roles[{role_index}].input_artifacts"
                    ),
                );
                diagnostic.semantic_ids = std::iter::once(workstream.workstream_id.clone())
                    .chain(std::iter::once(role.role_id.clone()))
                    .chain(missing)
                    .collect();
                diagnostic.allowed_repairs = vec![
                    "bind_the_input_to_a_declared_predecessor_result_artifact".to_string(),
                    "remove_the_unproduced_input_artifact".to_string(),
                ];
                return Err(diagnostic.into());
            }
            dataflow
                .consumer_roles
                .entry(workstream.workstream_id.clone())
                .or_default()
                .insert(role.role_id.clone());
            dataflow
                .input_artifacts
                .entry(workstream.workstream_id.clone())
                .or_default()
                .extend(cross_inputs);
        }
    }
    Ok(dataflow)
}

/// Evidence scopes become root-node resource leases.  Reject a malformed or
/// unavailable source here, before a Program graph and its decision identity
/// are persisted.  Otherwise a literal glob can pass semantic lowering and
/// only fail while the scheduler acquires a resource, leaving a terminal
/// Program that a corrected submission cannot replace under the same
/// decision id.
fn validate_concrete_evidence_scopes(
    workstream_index: usize,
    workstream: &harness_contract::orchestration::ModelCollaborationWorkstreamV2,
    services: &RuntimeServices,
) -> Result<(), IntentCompilerError> {
    let criteria = workstream
        .evidence_contract
        .iter()
        .enumerate()
        .map(|(index, criterion)| {
            (
                format!("workstreams[{workstream_index}].evidence_contract[{index}]"),
                criterion,
            )
        })
        .chain(workstream.team.roles.iter().enumerate().flat_map(|(role_index, role)| {
            role.acceptance.iter().enumerate().map(move |(criterion_index, criterion)| {
                (
                    format!(
                        "workstreams[{workstream_index}].team.roles[{role_index}].acceptance[{criterion_index}]"
                    ),
                    criterion,
                )
            })
        }));
    for (field_path, criterion) in criteria {
        let ModelSemanticAcceptanceCriterion::EvidenceScope {
            operation,
            resource,
        } = criterion
        else {
            continue;
        };
        let operation = operation.trim();
        let resource = resource.trim();
        if !matches!(operation, "read" | "list" | "recursive" | "glob") {
            let mut diagnostic = CollaborationCompileDiagnostic::validation(
                "evidence_scope_operation_unsupported",
                format!("{field_path}.operation"),
            );
            diagnostic.semantic_ids = vec![workstream.workstream_id.clone(), operation.to_string()];
            diagnostic.allowed_repairs = vec![
                "use_read_list_recursive_or_glob_operation".to_string(),
                "supply_concrete_existing_workspace_resource".to_string(),
            ];
            return Err(diagnostic.into());
        }
        if resource.is_empty() || resource.contains(['*', '?', '[', ']', '{', '}']) {
            let mut diagnostic = CollaborationCompileDiagnostic::validation(
                "evidence_scope_resource_must_be_concrete",
                format!("{field_path}.resource"),
            );
            diagnostic.semantic_ids = vec![workstream.workstream_id.clone(), resource.to_string()];
            diagnostic.allowed_repairs = vec![
                "replace_glob_with_one_existing_workspace_path".to_string(),
                "use_multiple_evidence_scope_criteria_for_multiple_sources".to_string(),
            ];
            return Err(diagnostic.into());
        }
        if services
            .path_identity_resolver()
            .compile_obligation(&format!("{operation}:{resource}"))
            .is_err()
        {
            let mut diagnostic = CollaborationCompileDiagnostic::validation(
                "evidence_scope_resource_unavailable",
                format!("{field_path}.resource"),
            );
            diagnostic.semantic_ids = vec![workstream.workstream_id.clone(), resource.to_string()];
            diagnostic.allowed_repairs = vec![
                "replace_with_one_existing_workspace_path".to_string(),
                "remove_the_unavailable_evidence_scope".to_string(),
            ];
            return Err(diagnostic.into());
        }
    }
    Ok(())
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
    cross_workstream_consumers: BTreeSet<String>,
    catalog: &[AgentCatalogEntry],
    tool_inventory: Option<&RuntimeToolInventorySnapshot>,
    ceiling: PermissionMode,
    terminal_owns_workstream_evidence: bool,
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
    validate_independent_review_contracts(team, &canonical_ids, &dependencies, workstream_index)?;
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
        let selected = resolve_role(role, catalog, tool_inventory, ceiling)?;
        let behavior = derive_behavior(
            role,
            canonical_id,
            &incoming,
            &outgoing,
            terminal_role.as_deref(),
            cross_workstream_consumers.contains(&role.role_id),
            terminal_owns_workstream_evidence,
        );
        // `output_artifacts` are not merely dependency-routing labels for a
        // terminal role.  They are the Team's promised terminal result
        // schema, so they must lower into that role's Runtime-verifiable
        // acceptance contract as well.  Otherwise a model can declare (for
        // example) `unresolved` in the Team result, the compiler can select
        // it as terminal producer, and the Team verifier will later demand a
        // field that no Agent was ever required to materialize.
        let acceptance = canonical_acceptance(
            role,
            (terminal_role.as_deref() == Some(canonical_id)).then_some(result_fields.as_slice()),
        );
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
                selected.entry.definition_ref.definition_id.as_str(),
                selected.entry.definition_ref.revision
            ),
            grant_ceiling: canonical_set(&role.required_capabilities),
            fixed_count: None,
            min_count: Some(u32::from(role.cardinality.min)),
            max_count: Some(u32::from(role.cardinality.max)),
            acceptance,
            input_artifacts: canonical_set(&role.input_artifacts),
            output_artifacts: canonical_set(&role.output_artifacts),
            allowed_tool_contract_refs: selected
                .executable_tools
                .clone()
                .unwrap_or_else(|| canonical_tool_refs(&role.required_tools)),
            allowed_skill_refs: canonical_set(&role.required_skills),
            behavior,
        });
        resolved_bindings.push(serde_json::json!({
            "role_id": canonical_id,
            "definition": selected.entry.definition_ref.definition_id.as_str(),
            "revision": selected.entry.definition_ref.revision,
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
            cardinality_min: role.cardinality.min,
            cardinality_max: role.cardinality.max,
            acceptance_kinds: canonical_acceptance_kinds(&role.acceptance),
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
            result_fields: result_fields.clone(),
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
            result_field_shapes: canonical_set(&result_fields),
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

fn validate_independent_review_contracts(
    team: &harness_contract::orchestration::ModelTurnScopedTeamIntent,
    canonical_ids: &BTreeMap<String, String>,
    dependencies: &[CanonicalDependency],
    workstream_index: usize,
) -> Result<(), IntentCompilerError> {
    for (role_index, role) in team.roles.iter().enumerate() {
        let reviewer_id = &canonical_ids[&role.role_id];
        for (criterion_index, criterion) in role.acceptance.iter().enumerate() {
            let ModelSemanticAcceptanceCriterion::IndependentReview { subject_role_id } = criterion
            else {
                continue;
            };
            let path = format!(
                "workstreams[{workstream_index}].team.roles[{role_index}].acceptance[{criterion_index}]"
            );
            let Some(subject_id) = canonical_ids.get(subject_role_id) else {
                return Err(CollaborationCompileDiagnostic::validation(
                    "independent_review_subject_unknown",
                    format!("{path}.subject_role_id"),
                )
                .into());
            };
            let bound_review = dependencies.iter().any(|dependency| {
                dependency.from == *subject_id
                    && dependency.to == *reviewer_id
                    && dependency.kind == ModelCollaborationDependencyKind::ReviewOf
            });
            if !bound_review {
                let mut diagnostic = CollaborationCompileDiagnostic::validation(
                    "independent_review_dependency_missing",
                    path,
                );
                diagnostic.semantic_ids = vec![subject_role_id.clone(), role.role_id.clone()];
                diagnostic.allowed_repairs = vec![
                    "add_local_review_of_dependency_from_subject_to_reviewer".to_string(),
                    "remove_independent_review_acceptance_if_only_handoff_is_required".to_string(),
                ];
                return Err(diagnostic.into());
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct HostResolvedRole {
    entry: AgentCatalogEntry,
    /// `Some` means an authenticated host inventory was present and this is
    /// the complete immutable executable allowlist for the role.
    executable_tools: Option<Vec<String>>,
}

fn resolve_role(
    role: &ModelRoleIntent,
    catalog: &[AgentCatalogEntry],
    tool_inventory: Option<&RuntimeToolInventorySnapshot>,
    ceiling: PermissionMode,
) -> Result<HostResolvedRole, IntentCompilerError> {
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
        .with_agent_catalog(catalog)
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
        return Err(diagnostic.with_agent_catalog(catalog).into());
    }
    let resolved_capability = resolve_agent_capability(AgentCapabilityRequest {
        role_id: role.role_id.clone(),
        allowed_capabilities: capabilities.clone(),
        evidence_duties: Vec::new(),
    });
    let available_tools = &resolved_capability.allowed_tools;
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
        .with_agent_catalog(catalog)
        .into());
    }
    let executable_tools = if let Some(inventory) = tool_inventory {
        let host_tools = inventory
            .available_tools
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let host_binding = bind_agent_capability_to_host(&resolved_capability, &host_tools);
        let mut host_missing_tools = tools
            .iter()
            .filter(|tool| !host_tools.contains(*tool))
            .cloned()
            .collect::<Vec<_>>();
        host_missing_tools.extend(host_binding.missing_tool_alternatives.clone());
        host_missing_tools.sort();
        host_missing_tools.dedup();
        if !host_missing_tools.is_empty() || !host_binding.missing_capabilities.is_empty() {
            let mut diagnostic = CollaborationCompileDiagnostic::resolver(
                &role.role_id,
                host_binding.missing_capabilities,
                Vec::new(),
                host_missing_tools,
            );
            diagnostic.code = "host_tool_inventory_gap".to_string();
            diagnostic.phase = CollaborationCompilePhase::Bind;
            diagnostic.repairability = "runtime_or_user_decision".to_string();
            diagnostic.allowed_repairs = vec![
                "enable_one_required_tool_in_the_active_gateway_catalog".to_string(),
                "raise_the_session_permission_ceiling_if_authorized".to_string(),
                "revise_the_role_to_remove_an_unavailable_effect_capability".to_string(),
            ];
            diagnostic.semantic_ids.push(format!(
                "tool_catalog_revision:{}",
                inventory.catalog_revision
            ));
            return Err(diagnostic.with_agent_catalog(catalog).into());
        }
        Some(host_binding.allowed_tools.into_iter().collect())
    } else {
        None
    };
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
    let entry = eligible.into_iter().next().ok_or_else(|| {
        IntentCompilerError::Diagnostic(
            CollaborationCompileDiagnostic::resolver(&role.role_id, capabilities, skills, tools)
                .with_agent_catalog(catalog),
        )
    })?;
    Ok(HostResolvedRole {
        entry,
        executable_tools,
    })
}

fn derive_behavior(
    role: &ModelRoleIntent,
    canonical_id: &str,
    incoming: &BTreeMap<String, Vec<ModelCollaborationDependencyKind>>,
    outgoing: &BTreeMap<String, usize>,
    terminal_role: Option<&str>,
    consumes_cross_workstream_input: bool,
    terminal_owns_workstream_evidence: bool,
) -> Vec<RoleBehaviorFacet> {
    let mut behavior = Vec::new();
    let incoming_kinds = incoming.get(canonical_id).cloned().unwrap_or_default();
    if !incoming_kinds.is_empty() || consumes_cross_workstream_input {
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
    // A workstream-level evidence_scope is a Team result obligation, not
    // merely an authorization hint. Its terminal carrier must independently
    // acquire that bounded evidence even when it also consumes predecessor
    // artifacts. Without this lowering, an aggregate sink is classified as
    // an upstream-only zero-tool reducer and can never satisfy the scope that
    // Runtime already admitted and leased to its Team.
    let consumes_upstream = !incoming_kinds.is_empty() || consumes_cross_workstream_input;
    let requests_independent_effect = role.required_capabilities.iter().any(|capability| {
        matches!(
            capability
                .trim()
                .replace('-', "_")
                .to_ascii_lowercase()
                .as_str(),
            "network" | "web" | "write" | "test" | "status" | "logs" | "rollback"
        )
    });
    if role.acceptance.iter().any(|criterion| {
        matches!(
            criterion,
            ModelSemanticAcceptanceCriterion::EvidenceScope { .. }
        )
    }) || (terminal_owns_workstream_evidence && terminal_role == Some(canonical_id))
        // A handoff is an input edge, not a declaration that the consumer is
        // a zero-tool reducer. Explicit effect capabilities mean this role is
        // expected to do new governed work after consuming the artifact (for
        // example design -> implementation or result -> reproduction).
        || (consumes_upstream && requests_independent_effect)
    {
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
    if result_fields.is_empty() && !team.result.synthesis_required {
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

fn canonical_acceptance(
    role: &ModelRoleIntent,
    terminal_result_fields: Option<&[String]>,
) -> Vec<String> {
    let input_artifacts = canonical_set(&role.input_artifacts);
    let output_artifacts = canonical_set(&role.output_artifacts);
    let mut values = role
        .acceptance
        .iter()
        // Providers sometimes repeat a declared input artifact in
        // `acceptance`. That is an input prerequisite, not an instruction to
        // republish the predecessor's artifact. The cross-workstream dataflow
        // validator below proves its producer and dependency; only artifacts
        // owned by this role may become output acceptance fields.
        .filter(|criterion| match criterion {
            ModelSemanticAcceptanceCriterion::Artifact { artifact } => {
                !input_artifacts.contains(artifact) || output_artifacts.contains(artifact)
            }
            _ => true,
        })
        .map(criterion_key)
        .collect::<Vec<_>>();
    // Declared outputs are owned deliverables, not merely routing labels.
    // Lower every one to a structural artifact check so non-terminal
    // producers cannot complete without materializing the payload promised
    // to their successors. Source/effect proof remains exclusively attached
    // to explicit evidence_scope / committed_effect criteria.
    values.extend(
        output_artifacts
            .iter()
            .map(|artifact| artifact_criterion_key(artifact)),
    );
    if let Some(fields) = terminal_result_fields {
        values.extend(fields.iter().map(|field| artifact_criterion_key(field)));
    }
    if values.is_empty() {
        // A role with no declared routed output still owes an inspectable
        // terminal result. Do not invent a source-evidence obligation: that
        // would require a bounded lease the semantic decision never asked
        // for and would conflate reporting with evidence acquisition.
        values.push("artifact:summary".to_string());
    }
    values.sort();
    values.dedup();
    values
}

fn criterion_key(criterion: &ModelSemanticAcceptanceCriterion) -> String {
    match criterion {
        // Preserve the tagged semantic distinction through Runtime lowering.
        // In particular, artifact `evidence` is a structured result field;
        // only EvidenceScope means fresh Runtime-observed evidence.
        ModelSemanticAcceptanceCriterion::Artifact { artifact } => artifact_criterion_key(artifact),
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

fn artifact_criterion_key(artifact: &str) -> String {
    let artifact = artifact.trim();
    format!("artifact:{artifact}")
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

fn canonical_acceptance_kinds(values: &[ModelSemanticAcceptanceCriterion]) -> Vec<String> {
    canonical_set(
        &values
            .iter()
            .map(|criterion| match criterion {
                ModelSemanticAcceptanceCriterion::Artifact { .. } => "artifact",
                ModelSemanticAcceptanceCriterion::EvidenceScope { .. } => "evidence_scope",
                ModelSemanticAcceptanceCriterion::StructuredField { .. } => "structured_field",
                ModelSemanticAcceptanceCriterion::TerminalFact { .. } => "terminal_fact",
                ModelSemanticAcceptanceCriterion::CommittedEffect { .. } => "committed_effect",
                ModelSemanticAcceptanceCriterion::IndependentReview { .. } => "independent_review",
            })
            .map(str::to_string)
            .collect::<Vec<_>>(),
    )
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

    fn install_source_fixture(services: &RuntimeServices) {
        let source = services
            .workspace_root()
            .join("crates/runtime/src/orchestration/mod.rs");
        std::fs::create_dir_all(source.parent().expect("source parent"))
            .expect("source fixture directory");
        std::fs::write(&source, "// source fixture").expect("source fixture");
    }

    fn append_cross_workstream_synthesizer(
        decision: &mut ModelCollaborationControlDecisionV2,
        input_artifact: &str,
        depends_on: Vec<String>,
    ) {
        decision.workstreams.push(ModelCollaborationWorkstreamV2 {
            workstream_id: "final-synthesis".to_string(),
            objective: "synthesize the predecessor result".to_string(),
            depends_on,
            output_artifacts: vec!["final_recommendation".to_string()],
            evidence_contract: Vec::new(),
            managed_agent_escalation: ManagedAgentEscalationRequirement::None,
            team: ModelTurnScopedTeamIntent {
                team_key: "final-synthesis-team".to_string(),
                display_name: None,
                instructions: "use only the authenticated predecessor result".to_string(),
                result: ModelTeamResultIntent {
                    required_artifacts: vec!["final_recommendation".to_string()],
                    evidence_required: false,
                    synthesis_required: true,
                },
                roles: vec![ModelRoleIntent {
                    role_id: "final-role".to_string(),
                    display_name: None,
                    responsibility: "produce the final recommendation".to_string(),
                    required_capabilities: vec!["read".to_string()],
                    required_skills: Vec::new(),
                    required_tools: Vec::new(),
                    cardinality: Default::default(),
                    // An input-only artifact repeated here is redundant
                    // provider syntax and must never become an output field.
                    acceptance: vec![
                        ModelSemanticAcceptanceCriterion::Artifact {
                            artifact: input_artifact.to_string(),
                        },
                        ModelSemanticAcceptanceCriterion::Artifact {
                            artifact: "final_recommendation".to_string(),
                        },
                    ],
                    input_artifacts: vec![input_artifact.to_string()],
                    output_artifacts: vec!["final_recommendation".to_string()],
                }],
                dependencies: Vec::new(),
            },
        });
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
    fn cross_workstream_input_derives_upstream_behavior_without_republishing_input() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
        let mut valid = decision();
        append_cross_workstream_synthesizer(
            &mut valid,
            "summary",
            vec!["runtime-audit".to_string()],
        );

        let compiled = compile_turn_scoped_intent(&request(), &valid, &services)
            .expect("typed cross-Team artifact flow compiles");
        let role = &compiled.template_proposal["teams"][1]["template"]["roles"][0];
        assert!(role["behavior"].as_array().is_some_and(|facets| facets
            .iter()
            .any(|facet| facet["kind"] == "upstream_consumption")));
        assert!(!role["behavior"].as_array().is_some_and(|facets| facets
            .iter()
            .any(|facet| facet["kind"] == "reacquire_evidence" && facet["required"] == true)));
        assert_eq!(
            role["acceptance"],
            serde_json::json!(["artifact:final_recommendation"])
        );
        assert_eq!(
            compiled.proposal.nodes[1].required_evidence_refs,
            vec!["artifact_kind:summary".to_string()]
        );
    }

    #[test]
    fn workstream_evidence_scope_reacquires_on_terminal_upstream_consumer() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
        let mut valid = decision();
        append_cross_workstream_synthesizer(
            &mut valid,
            "summary",
            vec!["runtime-audit".to_string()],
        );
        valid.workstreams[1].evidence_contract.push(
            ModelSemanticAcceptanceCriterion::EvidenceScope {
                operation: "read".to_string(),
                resource: "crates/runtime/src/orchestration/mod.rs".to_string(),
            },
        );

        let compiled = compile_turn_scoped_intent(&request(), &valid, &services)
            .expect("bounded Team evidence must remain executable at the terminal carrier");
        let role = &compiled.template_proposal["teams"][1]["template"]["roles"][0];
        let behavior = role["behavior"].as_array().expect("typed behavior");
        assert!(behavior
            .iter()
            .any(|facet| facet["kind"] == "upstream_consumption"));
        assert!(behavior
            .iter()
            .any(|facet| { facet["kind"] == "reacquire_evidence" && facet["required"] == true }));
    }

    #[test]
    fn cross_workstream_input_requires_a_declared_producer_dependency() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
        let mut missing_dependency = decision();
        append_cross_workstream_synthesizer(&mut missing_dependency, "summary", Vec::new());
        let error = compile_turn_scoped_intent(&request(), &missing_dependency, &services)
            .expect_err("cross-Team input without depends_on must fail");
        assert!(error
            .to_string()
            .contains("cross_workstream_input_without_dependency"));

        let mut missing_artifact = decision();
        append_cross_workstream_synthesizer(
            &mut missing_artifact,
            "not-produced",
            vec!["runtime-audit".to_string()],
        );
        let error = compile_turn_scoped_intent(&request(), &missing_artifact, &services)
            .expect_err("cross-Team input absent from predecessor result must fail");
        assert!(error
            .to_string()
            .contains("cross_workstream_input_artifact_unproduced"));
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
    fn independent_review_acceptance_requires_a_typed_review_of_dependency() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
        let mut invalid = decision();
        invalid.workstreams[0].team.roles[1].acceptance.push(
            ModelSemanticAcceptanceCriterion::IndependentReview {
                subject_role_id: "任意取证角色".to_string(),
            },
        );

        let error = compile_turn_scoped_intent(&request(), &invalid, &services)
            .expect_err("a handoff cannot masquerade as independent review");
        let IntentCompilerError::Diagnostic(diagnostic) = error else {
            panic!("expected semantic diagnostic");
        };
        assert_eq!(diagnostic.code, "independent_review_dependency_missing");
        assert!(diagnostic
            .allowed_repairs
            .contains(&"add_local_review_of_dependency_from_subject_to_reviewer".to_string()));
    }

    #[test]
    fn independent_review_acceptance_binds_to_review_of_without_role_name_heuristics() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
        let mut valid = decision();
        valid.workstreams[0].team.roles[1].acceptance.push(
            ModelSemanticAcceptanceCriterion::IndependentReview {
                subject_role_id: "任意取证角色".to_string(),
            },
        );
        valid.workstreams[0].team.dependencies[0].kind = ModelCollaborationDependencyKind::ReviewOf;

        let compiled = compile_turn_scoped_intent(&request(), &valid, &services)
            .expect("typed review relation compiles");
        let reviewer = &compiled.template_proposal["teams"][0]["template"]["roles"][1];
        assert!(reviewer["behavior"]
            .as_array()
            .is_some_and(|facets| facets.iter().any(|facet| facet["kind"] == "verification")));
    }

    #[test]
    fn compiles_arbitrary_role_names_to_exact_bound_snapshot() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
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
        assert_eq!(
            compiled.semantic_intent.teams[0].roles[0].cardinality_min,
            1
        );
        assert_eq!(
            compiled.semantic_intent.teams[0].roles[0].cardinality_max,
            1
        );
        assert_eq!(
            compiled.semantic_intent.teams[0].roles[0].acceptance_kinds,
            vec!["artifact", "evidence_scope"]
        );
        assert_eq!(
            compiled.semantic_intent.teams[0].result_field_shapes,
            vec!["evidence", "summary"]
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
    fn gateway_inventory_is_frozen_into_every_compiled_role_allowlist() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
        let mut bound_request = request();
        bound_request.tool_inventory = Some(RuntimeToolInventorySnapshot {
            catalog_revision: 42,
            available_tools: vec![
                "context_retrieve".to_string(),
                "read_file".to_string(),
                "team_board".to_string(),
            ],
        });

        let compiled = compile_turn_scoped_intent(&bound_request, &decision(), &services)
            .expect("host-backed semantic compilation");
        let proposal: TeamTemplateProposal =
            serde_json::from_value(compiled.template_proposal["teams"][0]["template"].clone())
                .expect("compiled template proposal");

        for role in proposal.roles {
            assert_eq!(
                role.allowed_tool_contract_refs,
                vec![
                    "context_retrieve".to_string(),
                    "read_file".to_string(),
                    "team_board".to_string(),
                ]
            );
        }
    }

    #[test]
    fn network_capability_without_a_physical_network_tool_fails_pre_admission() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
        let mut bound_request = request();
        bound_request.constraints.permission_ceiling = PermissionMode::DangerFullAccess;
        bound_request.tool_inventory = Some(RuntimeToolInventorySnapshot {
            catalog_revision: 77,
            available_tools: vec![
                "context_retrieve".to_string(),
                "read_file".to_string(),
                "tool_search".to_string(),
            ],
        });
        let mut network_decision = decision();
        network_decision.workstreams[0].team.roles[0].required_capabilities = vec![
            "read".to_string(),
            "search".to_string(),
            "network".to_string(),
        ];

        let error = compile_turn_scoped_intent(&bound_request, &network_decision, &services)
            .expect_err("network label without an executor must fail before graph admission");
        let IntentCompilerError::Diagnostic(diagnostic) = error else {
            panic!("expected host inventory diagnostic");
        };
        assert_eq!(diagnostic.code, "host_tool_inventory_gap");
        assert_eq!(diagnostic.phase, CollaborationCompilePhase::Bind);
        assert_eq!(diagnostic.missing_capabilities, vec!["network"]);
        assert!(diagnostic.missing_tools.contains(&"web_search".to_string()));
        assert!(diagnostic
            .semantic_ids
            .contains(&"tool_catalog_revision:77".to_string()));
    }

    #[test]
    fn rejects_glob_evidence_scope_before_program_admission() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut invalid = decision();
        invalid.workstreams[0].team.roles[0].acceptance =
            vec![ModelSemanticAcceptanceCriterion::EvidenceScope {
                operation: "read".to_string(),
                resource: "crates/**/*.rs".to_string(),
            }];

        let error = compile_turn_scoped_intent(&request(), &invalid, &services)
            .expect_err("wildcard source scopes must fail before graph creation");
        let IntentCompilerError::Diagnostic(diagnostic) = error else {
            panic!("expected semantic diagnostic");
        };
        assert_eq!(diagnostic.code, "evidence_scope_resource_must_be_concrete");
        assert_eq!(
            diagnostic.field_paths,
            vec!["workstreams[0].team.roles[0].acceptance[0].resource"]
        );
    }

    #[test]
    fn rejects_unavailable_evidence_scope_before_program_admission() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut invalid = decision();
        invalid.workstreams[0].team.roles[0].acceptance =
            vec![ModelSemanticAcceptanceCriterion::EvidenceScope {
                operation: "read".to_string(),
                resource: "crates/runtime/src/does-not-exist.rs".to_string(),
            }];

        let error = compile_turn_scoped_intent(&request(), &invalid, &services)
            .expect_err("missing source scopes must fail before graph creation");
        let IntentCompilerError::Diagnostic(diagnostic) = error else {
            panic!("expected semantic diagnostic");
        };
        assert_eq!(diagnostic.code, "evidence_scope_resource_unavailable");
        assert_eq!(
            diagnostic.field_paths,
            vec!["workstreams[0].team.roles[0].acceptance[0].resource"]
        );
    }

    #[test]
    fn workstream_artifact_contract_must_match_the_team_terminal_result() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
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
    fn terminal_result_artifacts_become_terminal_role_acceptance() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
        let mut valid = decision();
        valid.workstreams[0]
            .evidence_contract
            .push(ModelSemanticAcceptanceCriterion::Artifact {
                artifact: "unresolved".to_string(),
            });
        valid.workstreams[0]
            .team
            .result
            .required_artifacts
            .push("unresolved".to_string());
        valid.workstreams[0].team.roles[1]
            .output_artifacts
            .push("unresolved".to_string());

        let compiled = compile_turn_scoped_intent(&request(), &valid, &services)
            .expect("the terminal role declares every Team result artifact");
        let roles = &compiled.template_proposal["teams"][0]["template"]["roles"];
        assert!(roles[1]["acceptance"]
            .as_array()
            .is_some_and(|criteria| criteria.iter().any(|value| value == "artifact:unresolved")));
    }

    #[test]
    fn custom_result_artifact_has_a_terminal_carrier_without_source_evidence() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut valid = decision();
        let workstream = &mut valid.workstreams[0];
        workstream.output_artifacts = vec!["definitions_report".to_string()];
        workstream.evidence_contract.clear();
        workstream.team.result = ModelTeamResultIntent {
            required_artifacts: vec!["definitions_report".to_string()],
            evidence_required: false,
            synthesis_required: false,
        };
        workstream.team.roles.truncate(1);
        workstream.team.roles[0].acceptance.clear();
        workstream.team.roles[0].required_tools.clear();
        workstream.team.roles[0].output_artifacts = vec!["definitions_report".to_string()];
        workstream.team.dependencies.clear();

        let compiled = compile_turn_scoped_intent(&request(), &valid, &services)
            .expect("a declared result artifact is structural, not implicit source evidence");
        let role = &compiled.template_proposal["teams"][0]["template"]["roles"][0];
        assert_eq!(
            role["acceptance"],
            serde_json::json!(["artifact:definitions_report"])
        );
        assert!(role["behavior"].as_array().is_some_and(|facets| facets
            .iter()
            .any(|facet| { facet["kind"] == "terminal_candidate" && facet["required"] == true })));
    }

    #[test]
    fn evidence_artifact_remains_distinct_from_runtime_evidence_scope() {
        assert_eq!(artifact_criterion_key("evidence"), "artifact:evidence");
        assert_eq!(
            artifact_criterion_key("definitions_report"),
            "artifact:definitions_report"
        );
    }

    #[test]
    fn every_declared_role_output_is_structurally_verified() {
        let role = ModelRoleIntent {
            role_id: "producer".to_string(),
            display_name: None,
            responsibility: "produce a handoff".to_string(),
            required_capabilities: vec!["read".to_string()],
            required_skills: Vec::new(),
            required_tools: Vec::new(),
            cardinality: Default::default(),
            acceptance: Vec::new(),
            input_artifacts: Vec::new(),
            output_artifacts: vec!["evidence".to_string(), "handoff".to_string()],
        };
        assert_eq!(
            canonical_acceptance(&role, None),
            vec![
                "artifact:evidence".to_string(),
                "artifact:handoff".to_string()
            ]
        );
    }

    #[test]
    fn role_without_output_defaults_to_structured_summary_not_source_debt() {
        let role = ModelRoleIntent {
            role_id: "observer".to_string(),
            display_name: None,
            responsibility: "observe upstream state".to_string(),
            required_capabilities: vec!["read".to_string()],
            required_skills: Vec::new(),
            required_tools: Vec::new(),
            cardinality: Default::default(),
            acceptance: Vec::new(),
            input_artifacts: Vec::new(),
            output_artifacts: Vec::new(),
        };
        assert_eq!(
            canonical_acceptance(&role, None),
            vec!["artifact:summary".to_string()]
        );
    }

    #[test]
    fn rejects_unknown_tool_without_role_or_builtin_fallback() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        install_source_fixture(&services);
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
        install_source_fixture(&services);
        let mut invalid = decision();
        invalid.workstreams[0].team.roles[0].required_skills = vec!["skill/unknown@1".to_string()];
        let error = compile_turn_scoped_intent(&request(), &invalid, &services)
            .expect_err("unknown skill must fail closed");
        let IntentCompilerError::Diagnostic(diagnostic) = error else {
            panic!("expected typed resolution diagnostic")
        };
        assert_eq!(diagnostic.code, "role_resolution_gap");
        assert_eq!(diagnostic.missing_skills, vec!["skill/unknown@1"]);
        assert!(diagnostic.available_skill_refs.is_empty());
        assert!(diagnostic.available_capability_profiles.contains(&vec![
            "network".to_string(),
            "read".to_string(),
            "search".to_string()
        ]));
    }

    #[test]
    fn incompatible_capability_union_returns_exact_runnable_profiles() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let catalog = services
            .definition_registry()
            .runnable_agent_catalog()
            .expect("runnable Agent catalog");
        let mut role = decision().workstreams[0].team.roles.remove(0);
        role.required_capabilities = vec![
            "read".to_string(),
            "search".to_string(),
            "network".to_string(),
            "write".to_string(),
        ];
        let error = resolve_role(&role, &catalog, None, PermissionMode::DangerFullAccess)
            .expect_err("network and workspace mutation require separate Agent identities");
        let IntentCompilerError::Diagnostic(diagnostic) = error else {
            panic!("expected typed resolution diagnostic")
        };
        assert_eq!(diagnostic.code, "role_resolution_gap");
        assert!(diagnostic
            .allowed_repairs
            .iter()
            .any(|repair| { repair == "split_incompatible_capabilities_across_roles" }));
        assert!(diagnostic.available_capability_profiles.contains(&vec![
            "network".to_string(),
            "read".to_string(),
            "search".to_string()
        ]));
        assert!(diagnostic.available_capability_profiles.contains(&vec![
            "read".to_string(),
            "search".to_string(),
            "test".to_string(),
            "write".to_string(),
        ]));
    }

    #[test]
    fn upstream_effect_role_is_not_downgraded_to_zero_tool_reducer() {
        let mut role = decision().workstreams[0].team.roles.remove(0);
        role.required_capabilities = vec![
            "read".to_string(),
            "search".to_string(),
            "write".to_string(),
            "test".to_string(),
        ];
        let canonical_id = canonical_role_id(&role.role_id);
        let incoming = BTreeMap::from([(
            canonical_id.clone(),
            vec![ModelCollaborationDependencyKind::Handoff],
        )]);

        let behavior = derive_behavior(
            &role,
            &canonical_id,
            &incoming,
            &BTreeMap::new(),
            None,
            false,
            false,
        );

        assert!(behavior.iter().any(|facet| matches!(
            facet,
            RoleBehaviorFacet::UpstreamConsumption { required: true }
        )));
        assert!(behavior.iter().any(|facet| matches!(
            facet,
            RoleBehaviorFacet::ReacquireEvidence { required: true }
        )));
    }
}
