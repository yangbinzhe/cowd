//! Typed Team Binding compilation and durable admission markers.
//!
//! A `TeamBindingSnapshot` is compiled exactly once from the published
//! Template revision and the per-slot Agent Bindings, then frozen. Admission
//! writes a `team.binding.preparing` event before Task links are created and
//! a `team.binding.ready` event after the exact link set is durable. Recovery
//! reads these markers and refuses to drive an orphan graph.

use harness_contract::task::TaskCreateCommand;
use harness_contract::team::{
    TeamBindingSnapshot, TeamDisplayIdentity, TeamInstantiationRequest, TeamRoleBindingSnapshot,
    TeamStrategyBinding, TeamTemplateManifest,
};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::ResolvedRoleSlot;
use crate::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeTransactionEventInput,
};

const TEAM_BINDING_STREAM_PREFIX: &str = "team-binding:";

fn binding_stream(graph_id: &str) -> String {
    format!("{TEAM_BINDING_STREAM_PREFIX}{graph_id}")
}

fn preparing_key(graph_id: &str) -> String {
    format!("team-binding:{graph_id}:preparing")
}

fn ready_key(graph_id: &str, binding_digest: &str) -> String {
    format!("team-binding:{graph_id}:ready:{binding_digest}")
}

/// Compile the immutable Team binding. Display text (template name, role
/// responsibility) never participates in behavior; every behavior fact is a
/// typed facet derived from the published topology and task contract.
pub fn compile_team_binding(
    request: &TeamInstantiationRequest,
    manifest: &TeamTemplateManifest,
    template_digest: &str,
    team_markdown: &str,
    role_slots: &[ResolvedRoleSlot],
    strategy: Option<&TeamStrategyBinding>,
) -> Result<TeamBindingSnapshot, String> {
    if role_slots.is_empty() {
        return Err("Team Binding cannot compile with zero resolved role slots".to_string());
    }
    let roles = role_slots
        .iter()
        .map(|slot| {
            let agent_binding = slot.agent_binding.as_ref().ok_or_else(|| {
                format!(
                    "role `{}` slot {} has no compiled Agent Binding",
                    slot.role_id, slot.slot
                )
            })?;
            let role_definition = manifest
                .roles
                .iter()
                .find(|role| role.role_id == slot.role_id)
                .ok_or_else(|| {
                    format!(
                        "role `{}` is absent from the published Template manifest",
                        slot.role_id
                    )
                })?;
            Ok(TeamRoleBindingSnapshot {
                role_id: slot.role_id.clone(),
                slot: u32::try_from(slot.slot)
                    .map_err(|_| format!("role slot {} overflows u32", slot.slot))?,
                focus: Some(slot.focus_partition.focus_id.clone()),
                role_name: role_definition.responsibility.clone(),
                role_description: role_definition.responsibility.clone(),
                // The manifest is the sole author of role behavior.  Binding
                // compilation freezes that published fact; it never infers a
                // reducer, terminal candidate, or evidence duty from graph
                // topology, a role id, or result-field text.
                behavior: role_definition.behavior.clone(),
                agent_definition_ref: agent_binding
                    .definition_ref
                    .definition_id
                    .as_str()
                    .to_string(),
                agent_name: slot.agent_name.clone(),
                agent_description: slot.agent_description.clone(),
                agent_definition_digest: agent_binding.definition_digest.clone(),
                responsibility: role_definition.responsibility.clone(),
                cardinality: role_definition.cardinality.clone(),
                partition: role_definition.partition.clone(),
                task_contract_ref: role_definition.task_contract.contract_ref.clone(),
                acceptance: role_definition.task_contract.acceptance.clone(),
                team_markdown_fragment: Some(team_markdown.trim().to_string()),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let template_ref = manifest.revision_ref();
    let binding_id = format!(
        "team-binding:{}:{}@{}",
        request.team_id,
        template_ref.template_id.as_str(),
        template_ref.revision
    );
    let display_identity = compile_display_identity(request, manifest, &roles, team_markdown);
    let binding = TeamBindingSnapshot {
        binding_id: binding_id.clone(),
        template_ref: format!(
            "{}@{}",
            template_ref.template_id.as_str(),
            template_ref.revision
        ),
        template_digest: template_digest.to_string(),
        template_name: manifest.name.clone(),
        template_description: manifest.name.clone(),
        team_instructions: team_markdown.trim().to_string(),
        roles,
        strategy_decision_id: strategy
            .map(|binding| binding.decision_id.clone())
            .unwrap_or_default(),
        strategy_decision_revision: strategy
            .map(|binding| binding.decision_revision)
            .unwrap_or_default(),
        strategy_decision_lease: strategy
            .map(|binding| binding.decision_lease.clone())
            .unwrap_or_default(),
        strategy_turn_ref: strategy
            .map(|binding| binding.turn_ref.clone())
            .unwrap_or_default(),
        display_identity: display_identity.clone(),
        binding_digest: String::new(),
    };
    let digest = binding_digest(&binding)?;
    Ok(TeamBindingSnapshot {
        binding_digest: digest.clone(),
        ..binding
    })
}

fn compile_display_identity(
    request: &TeamInstantiationRequest,
    manifest: &TeamTemplateManifest,
    roles: &[TeamRoleBindingSnapshot],
    team_markdown: &str,
) -> TeamDisplayIdentity {
    let role_label = roles
        .first()
        .map(|role| role.role_name.clone())
        .unwrap_or_else(|| manifest.name.clone());
    let focus_label = roles.iter().find_map(|role| role.focus.clone());
    let team_display_name = request.display_name.clone().or_else(|| {
        manifest
            .display
            .as_ref()
            .and_then(|display| display.team_display_name.clone())
    });
    let display = TeamDisplayIdentity {
        label: manifest.name.clone(),
        team_display_name,
        role_label,
        focus_label,
        locale: "auto".to_string(),
        provenance: format!("runtime.team.compile:{}", request.request_id),
        digest: String::new(),
    };
    TeamDisplayIdentity {
        digest: format!(
            "{:x}",
            Sha256::digest(
                format!(
                    "{}|{}|{}|{}|{}|{}",
                    display.label,
                    display.team_display_name.as_deref().unwrap_or_default(),
                    display.role_label,
                    display.focus_label.as_deref().unwrap_or_default(),
                    display.locale,
                    team_markdown.len()
                )
                .as_bytes()
            )
        ),
        ..display
    }
}

fn binding_digest(binding: &TeamBindingSnapshot) -> Result<String, String> {
    let value = json!({
        "binding_id": binding.binding_id,
        "template_ref": binding.template_ref,
        "template_digest": binding.template_digest,
        "template_name": binding.template_name,
        "template_description": binding.template_description,
        "team_instructions": binding.team_instructions,
        "roles": binding.roles,
        "strategy_decision_id": binding.strategy_decision_id,
        "strategy_decision_revision": binding.strategy_decision_revision,
        "strategy_decision_lease": binding.strategy_decision_lease,
        "strategy_turn_ref": binding.strategy_turn_ref,
        "display_identity": binding.display_identity,
    });
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&value)
                .map_err(|error| format!("encode Team Binding digest: {error}"))?
        )
    ))
}

/// Persist the durable Preparing marker. Same graph retries are idempotent
/// through the stable event idempotency key.
pub fn persist_preparing(
    store: &RuntimeEventStore,
    graph_id: &str,
    binding: &TeamBindingSnapshot,
) -> Result<(), String> {
    persist_preparing_with_task_commands(store, graph_id, binding, &[])
}

/// Persist the frozen binding and the exact Task link plan that belongs to
/// this graph. The plan is data, not a second scheduler: startup recovery can
/// finish a crash between graph registration and Task linking without
/// recompiling from mutable templates or model text.
pub fn persist_preparing_with_task_commands(
    store: &RuntimeEventStore,
    graph_id: &str,
    binding: &TeamBindingSnapshot,
    task_commands: &[TaskCreateCommand],
) -> Result<(), String> {
    let stream = binding_stream(graph_id);
    let key = preparing_key(graph_id);
    if let Some(existing) = store
        .event_by_idempotency_key(&stream, &key)
        .map_err(|error| error.to_string())?
    {
        let existing_binding: TeamBindingSnapshot =
            serde_json::from_value(existing.payload.get("binding").cloned().unwrap_or_default())
                .map_err(|error| format!("decode existing Team Binding: {error}"))?;
        if existing_binding.binding_digest != binding.binding_digest {
            return Err(format!(
                "Team graph `{graph_id}` Preparing marker belongs to a different Binding"
            ));
        }
        if !task_commands.is_empty() {
            if let Some(value) = existing.payload.get("task_commands") {
                let existing_commands: Vec<TaskCreateCommand> =
                    serde_json::from_value(value.clone()).map_err(|error| {
                        format!("decode existing Team Preparing task plan: {error}")
                    })?;
                if existing_commands != task_commands {
                    return Err(format!(
                        "Team graph `{graph_id}` Preparing marker has a different Task link plan"
                    ));
                }
            }
        }
        return Ok(());
    }
    let revision = store
        .stream_revision(&stream)
        .map_err(|error| error.to_string())?;
    store
        .append_batch_if_revision(
            stream,
            revision,
            key.clone(),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: binding_stream(graph_id),
                    scope: RuntimeEventScope::Team,
                    kind: "team.binding.preparing.v1".to_string(),
                    status: Some("preparing".to_string()),
                    actor: Some("team_runtime".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "execution_graph".to_string(),
                        id: graph_id.to_string(),
                    }],
                    payload: json!({
                        "binding": binding,
                        "task_commands": task_commands,
                    }),
                },
                idempotency_key: Some(key),
                schema_version: 1,
            }],
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Load the exact durable Task plan attached to a Preparing binding marker.
/// `None` distinguishes old/incomplete markers from a valid Team with no
/// tasks, which is never a legal runtime admission.
pub fn load_prepared_task_commands(
    store: &RuntimeEventStore,
    graph_id: &str,
) -> Result<Option<Vec<TaskCreateCommand>>, String> {
    let record = store
        .event_by_idempotency_key(&binding_stream(graph_id), &preparing_key(graph_id))
        .map_err(|error| error.to_string())?;
    let Some(record) = record else {
        return Ok(None);
    };
    let Some(value) = record.payload.get("task_commands") else {
        return Ok(None);
    };
    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|error| format!("decode Team Preparing task plan: {error}"))
}

/// Mark the exact link set durable. `binding_digest` is part of the event
/// identity so a stale recovery can never falsely close a different Binding.
pub fn persist_ready(
    store: &RuntimeEventStore,
    graph_id: &str,
    binding_digest: &str,
) -> Result<(), String> {
    let stream = binding_stream(graph_id);
    let key = ready_key(graph_id, binding_digest);
    if store
        .event_by_idempotency_key(&stream, &key)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Ok(());
    }
    let revision = store
        .stream_revision(&stream)
        .map_err(|error| error.to_string())?;
    store
        .append_batch_if_revision(
            stream,
            revision,
            format!("team-binding:{graph_id}:ready-transaction:{binding_digest}"),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: binding_stream(graph_id),
                    scope: RuntimeEventScope::Team,
                    kind: "team.binding.ready.v1".to_string(),
                    status: Some("ready".to_string()),
                    actor: Some("team_runtime".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "execution_graph".to_string(),
                        id: graph_id.to_string(),
                    }],
                    payload: json!({ "binding_digest": binding_digest }),
                },
                idempotency_key: Some(key),
                schema_version: 1,
            }],
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Load the frozen Team Binding for an admitted graph. `None` means the graph
/// either is not a Team graph or its admission marker is missing (recovery
/// must not drive it).
pub fn load_binding(
    store: &RuntimeEventStore,
    graph_id: &str,
) -> Result<Option<TeamBindingSnapshot>, String> {
    let record = store
        .event_by_idempotency_key(&binding_stream(graph_id), &preparing_key(graph_id))
        .map_err(|error| error.to_string())?;
    let Some(record) = record else {
        return Ok(None);
    };
    serde_json::from_value(record.payload.get("binding").cloned().unwrap_or_default())
        .map(Some)
        .map_err(|error| format!("decode Team Binding: {error}"))
}

/// Return the ready marker digest when the exact link set was closed.
pub fn ready_digest(
    store: &RuntimeEventStore,
    graph_id: &str,
    binding_digest: &str,
) -> Result<Option<String>, String> {
    let record = store
        .event_by_idempotency_key(
            &binding_stream(graph_id),
            &ready_key(graph_id, binding_digest),
        )
        .map_err(|error| error.to_string())?;
    Ok(record.map(|record| {
        record
            .payload
            .get("binding_digest")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    }))
}

/// True when any Ready marker closed this graph's admission. Recovery uses
/// this to skip reconciliation without re-linking an already-complete set.
pub fn has_ready_marker(store: &RuntimeEventStore, graph_id: &str) -> Result<bool, String> {
    let events = store
        .list_stream(&binding_stream(graph_id))
        .map_err(|error| error.to_string())?;
    Ok(events
        .iter()
        .any(|event| event.kind == "team.binding.ready.v1"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeEventStore;
    use harness_contract::agent::{
        AgentBindingSnapshot, AgentDataLease, AgentDefinitionRevisionRef, AgentInstanceRef,
    };
    use harness_contract::team::definition::{RoleDisplayName, TeamTemplateDisplay};
    use harness_contract::team::instantiation::RoleDisplayOverride;
    use harness_contract::team::{RoleBehaviorFacet, TeamRoleDefinition};
    use harness_contract::team::{
        RoleCardinalityPolicy, RolePartitionPolicy, TeamResultContract, TeamTemplateDefinitionId,
        TeamTopologyContract,
    };
    use std::sync::Arc;

    fn request() -> TeamInstantiationRequest {
        use harness_contract::{
            context::ParentExecutionBudget,
            execution_graph::ExecutionGraphLineage,
            team::{TeamSelectionMode, TeamTemplateSelector},
        };
        TeamInstantiationRequest {
            request_id: "test-request-team-1".to_string(),
            team_id: "team-1".to_string(),
            mission_id: "mission-1".to_string(),
            lineage: ExecutionGraphLineage {
                session_id: "session-1".to_string(),
                turn_id: "turn-team-1".to_string(),
                root_task_id: "task-root-team-1".to_string(),
                task_id: "task-root-team-1".to_string(),
                generation: 1,
            },
            parent_execution: None,
            selection_mode: TeamSelectionMode::Explicit,
            strategy_binding: None,
            template_selector: TeamTemplateSelector::LatestStable {
                template_id: TeamTemplateDefinitionId::new(
                    harness_contract::agent::DefinitionScope::Builtin,
                    "cowd/test-team",
                )
                .expect("template id"),
            },
            objective: "compile team binding".to_string(),
            acceptance: vec!["summary".to_string(), "evidence".to_string()],
            risk: None,
            role_binding_overrides: Vec::new(),
            display_name: None,
            role_display_overrides: Vec::new(),
            cardinality_overrides: Vec::new(),
            focus_partition_plans: Vec::new(),
            requires_managed_collaboration_escalation: false,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "model".to_string(),
            execution_budget: ParentExecutionBudget::new(
                "service-team-budget:team-1",
                65_536,
                u64::MAX,
                32,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            resource_scopes: vec!["read:crates/runtime".to_string()],
            allow_whole_workspace_scope: false,
            upstream_evidence_refs: Vec::new(),
            upstream_artifact_refs: Vec::new(),
        }
    }

    fn manifest() -> TeamTemplateManifest {
        TeamTemplateManifest {
            api_version: "cowd.team/v1".to_string(),
            template_id: TeamTemplateDefinitionId::new(
                harness_contract::agent::DefinitionScope::Builtin,
                "cowd/test-team",
            )
            .unwrap(),
            revision: 1,
            name: "Test Team".to_string(),
            display: None,
            lifecycle: harness_contract::agent::RevisionLifecycle::Published,
            topology: TeamTopologyContract {
                protocol_ref: "review_fix@1".to_string(),
                require_synthesis: true,
                require_review: true,
            },
            role_aliases: std::collections::BTreeMap::new(),
            roles: vec![TeamRoleDefinition {
                role_id: "implementer".to_string(),
                display_name: None,
                responsibility: "Implement the bounded change.".to_string(),
                agent_definition_id: harness_contract::agent::AgentDefinitionId::new(
                    harness_contract::agent::DefinitionScope::Builtin,
                    "cowd/execute",
                )
                .unwrap(),
                agent_selector: harness_contract::agent::RevisionSelector::ExactApprovedRevision {
                    revision: 1,
                },
                cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
                partition: RolePartitionPolicy::Single,
                behavior: vec![
                    RoleBehaviorFacet::ReacquireEvidence { required: true },
                    RoleBehaviorFacet::TerminalCandidate { required: true },
                ],
                grant_ceiling: vec![harness_contract::agent::AgentCapability::Read],
                task_contract: harness_contract::team::TeamRoleTaskContract {
                    contract_ref: "task/implementer@1".to_string(),
                    acceptance: vec!["evidence".to_string()],
                    allowed_tool_contract_refs: Vec::new(),
                    allowed_skill_refs: Vec::new(),
                    dataflow: Default::default(),
                },
            }],
            dependencies: Vec::new(),
            result_contract: TeamResultContract {
                required_fields: vec!["summary".to_string(), "evidence".to_string()],
                evidence_required: true,
                synthesis_required: true,
            },
            evaluation: harness_contract::team::TeamEvaluationContract::single_release_gate(
                "team/test",
                "team_interoperability",
            ),
            instructions_digest: "digest".to_string(),
        }
    }

    fn slot() -> ResolvedRoleSlot {
        ResolvedRoleSlot {
            role_id: "implementer".to_string(),
            slot: 1,
            focus_partition: crate::team_instantiation::ResolvedFocusPartition {
                focus_id: "focus-1".to_string(),
                boundary: "workspace".to_string(),
                evidence_responsibility: "produce".to_string(),
                output_contract: vec!["summary".to_string()],
                output_acceptance: vec!["evidence".to_string()],
                shared_baseline: Vec::new(),
                capability_cropped_refs: Vec::new(),
                scope_hash: "scope".to_string(),
                overlap_budget_bp: 0,
                novelty_target_bp: 0,
            },
            definition_ref: AgentDefinitionRevisionRef::new(
                harness_contract::agent::AgentDefinitionId::new(
                    harness_contract::agent::DefinitionScope::Builtin,
                    "cowd/execute",
                )
                .unwrap(),
                1,
            )
            .unwrap(),
            agent_binding: Some(AgentBindingSnapshot {
                binding_id: "binding:execute".to_string(),
                definition_ref: AgentDefinitionRevisionRef::new(
                    harness_contract::agent::AgentDefinitionId::new(
                        harness_contract::agent::DefinitionScope::Builtin,
                        "cowd/execute",
                    )
                    .unwrap(),
                    1,
                )
                .unwrap(),
                definition_digest: "definition-digest".to_string(),
                instructions: "Execute.".to_string(),
                instance: AgentInstanceRef {
                    instance_id: "instance:1".to_string(),
                    role_slot_id: Some("implementer:1".to_string()),
                },
                executor: harness_contract::agent::AgentExecutorPolicy::CowdNative,
                model_policy: harness_contract::agent::AgentModelPolicy {
                    profile: "coding".to_string(),
                    allowed_models: vec!["test".to_string()],
                    fallback_allowed: false,
                },
                effective_capabilities: vec![harness_contract::agent::AgentCapability::Read],
                skill_refs: Vec::new(),
                tool_contract_refs: Vec::new(),
                data_lease: AgentDataLease {
                    session_id: "session-1".to_string(),
                    task_id: "task-root-team-1".to_string(),
                    team_id: Some("team-1".to_string()),
                    read_scopes: Vec::new(),
                    write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
                    team_working_state_visible: false,
                    fact_boundaries: Vec::new(),
                    fact_refs: Vec::new(),
                    matrix_snapshot_refs: Vec::new(),
                },
                release: None,
                evaluation: None,
                display: None,
                binding_digest: "binding-digest".to_string(),
            }),
            agent_name: "Execute".to_string(),
            agent_description: "Executes".to_string(),
        }
    }

    #[test]
    fn compile_team_binding_is_deterministic_and_typed() {
        let binding = compile_team_binding(
            &request(),
            &manifest(),
            "template-digest",
            "# Team\n\nReview.",
            &[slot()],
            None,
        )
        .expect("binding compiles");
        assert!(binding.binding_digest.len() == 64);
        assert_eq!(binding.template_name, "Test Team");
        assert_eq!(binding.roles.len(), 1);
        assert!(binding.roles[0].behavior.iter().any(|facet| {
            matches!(
                facet,
                RoleBehaviorFacet::ReacquireEvidence { required: true }
            )
        }));
        assert!(binding.roles[0].behavior.iter().any(|facet| {
            matches!(
                facet,
                RoleBehaviorFacet::TerminalCandidate { required: true }
            )
        }));

        let again = compile_team_binding(
            &request(),
            &manifest(),
            "template-digest",
            "# Team\n\nReview.",
            &[slot()],
            None,
        )
        .expect("binding recompiles");
        assert_eq!(binding.binding_digest, again.binding_digest);
    }

    #[test]
    fn terminal_behavior_is_frozen_from_the_published_role_contract() {
        let mut template = manifest();
        template.roles[0].role_id = "custom-convergence".to_string();
        template.roles[0].behavior = vec![
            RoleBehaviorFacet::Reducer {
                mode: "finally".to_string(),
            },
            RoleBehaviorFacet::UpstreamConsumption { required: true },
            RoleBehaviorFacet::TerminalCandidate { required: true },
        ];
        let role = &template.roles[0];
        let mut resolved_slot = slot();
        resolved_slot.role_id = "custom-convergence".to_string();
        let binding = compile_team_binding(
            &request(),
            &template,
            "template-digest",
            "# Team\n\nReview.",
            &[resolved_slot],
            None,
        )
        .expect("binding compiles");
        assert_eq!(binding.roles[0].behavior, role.behavior);
    }

    #[test]
    fn display_identity_uses_request_and_manifest_display_names() {
        let mut request = request();
        request.display_name = Some("业务团队".to_string());
        request.role_display_overrides = vec![RoleDisplayOverride {
            role_id: "implementer".to_string(),
            display_name: "供应链专家".to_string(),
        }];
        let mut manifest = manifest();
        manifest.display = Some(TeamTemplateDisplay {
            team_display_name: Some("技术团队".to_string()),
            role_display_names: vec![RoleDisplayName {
                role_id: "implementer".to_string(),
                display_name: "技术专家".to_string(),
            }],
        });
        let roles = vec![TeamRoleBindingSnapshot {
            role_id: "implementer".to_string(),
            slot: 1,
            focus: Some("supply-chain".to_string()),
            role_name: "Implement".to_string(),
            role_description: "Implement".to_string(),
            behavior: Vec::new(),
            agent_definition_ref: "builtin/cowd/execute".to_string(),
            agent_name: "Execute".to_string(),
            agent_description: "Executes".to_string(),
            agent_definition_digest: "digest".to_string(),
            responsibility: "Implement".to_string(),
            cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
            partition: RolePartitionPolicy::Single,
            task_contract_ref: "task/implementer@1".to_string(),
            acceptance: Vec::new(),
            team_markdown_fragment: None,
        }];
        let identity = compile_display_identity(&request, &manifest, &roles, "# Team");
        // Request wins over manifest for the team display name.
        assert_eq!(identity.team_display_name.as_deref(), Some("业务团队"));
        // The resolved role display override is request-first; the manifest
        // value must never replace it.
        let override_ = request
            .role_display_overrides
            .iter()
            .find(|item| item.role_id == "implementer")
            .expect("override");
        assert_eq!(override_.display_name, "供应链专家");
        assert!(!identity.digest.is_empty());
    }

    #[test]
    fn preparing_and_ready_markers_roundtrip_and_are_idempotent() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let binding = compile_team_binding(
            &request(),
            &manifest(),
            "template-digest",
            "# Team\n\nReview.",
            &[slot()],
            None,
        )
        .expect("binding compiles");
        persist_preparing(&store, "team-graph:team-1", &binding).expect("preparing");
        persist_preparing(&store, "team-graph:team-1", &binding).expect("preparing idempotent");
        let loaded = load_binding(&store, "team-graph:team-1")
            .expect("load")
            .expect("binding exists");
        assert_eq!(loaded.binding_digest, binding.binding_digest);
        assert_eq!(
            ready_digest(&store, "team-graph:team-1", &binding.binding_digest).expect("ready read"),
            None
        );
        persist_ready(&store, "team-graph:team-1", &binding.binding_digest).expect("ready");
        persist_ready(&store, "team-graph:team-1", &binding.binding_digest)
            .expect("ready idempotent");
        assert_eq!(
            ready_digest(&store, "team-graph:team-1", &binding.binding_digest)
                .expect("ready read")
                .as_deref(),
            Some(binding.binding_digest.as_str())
        );
        assert_eq!(
            load_binding(&store, "team-graph:missing").expect("load missing"),
            None
        );
    }
}
