//! Durable CollaborationProgram admission and recovery reactions.
//!
//! This module deliberately owns only short, revision-fenced Program commands.
//! ExecutionGraph Runner remains the scheduler and TeamRuntime remains the
//! immutable Team-binding compiler and child-graph admission owner.

use std::path::Path;

use harness_contract::execution_graph::{
    CollaborationProgram, CollaborationProgramControlState, CollaborationProgramLifecycle,
    ExecutionGraph, ExecutionGraphCommand, TeamAdmissionObligation, TeamAdmissionState,
};

use crate::execution_core::ExecutionStateStoreError;
use crate::{ExecutionGraphStateStore, RuntimeExecutionSupervisor, RuntimeServices, TeamRuntime};

use super::team_authority::{
    derive_team_focus_partition_plans, explicit_team_node_contract, semantic_focuses_from_plans,
};
use super::{
    CapabilityRecipeId, GraphMutationProposal, GraphSemanticNode, RuntimeOrchestrationCommand,
    RuntimeOrchestrationConstraints, RuntimeOrchestrationOperation,
};

const MAX_CAS_ATTEMPTS: usize = 3;

/// Host-facing Program intent. It carries only already-admitted Turn facts;
/// Coordinator owns the conversion to semantic graph topology and the later
/// durable Program admission.
#[derive(Debug, Clone)]
pub(crate) struct ConversationProgramIntent {
    pub objective: String,
    pub model_lease: String,
    pub session_id: String,
    pub lineage: Option<harness_contract::execution_graph::ExecutionGraphLineage>,
    pub mission_id: Option<String>,
    pub decision_id: String,
    pub decision_revision: u64,
    pub decision_lease: String,
    pub turn_ref: String,
    pub requested_team_count: usize,
    pub focus_count: usize,
    pub requests_multi_agent: bool,
    pub requires_write: bool,
    pub requires_external_facts: bool,
    pub permission_ceiling: harness_contract::policy::PermissionMode,
    pub risk: String,
}

/// Compile the conversation's selected-Team intent into a semantic Program
/// request. No Host code constructs graph nodes, dependencies, completion
/// contracts, or capability lists after this boundary.
pub(crate) fn compile_conversation_program_intent(
    intent: ConversationProgramIntent,
    workspace_root: &Path,
) -> Result<RuntimeOrchestrationCommand, String> {
    let selection_mode = if intent.requests_multi_agent {
        harness_contract::team::TeamSelectionMode::Explicit
    } else {
        harness_contract::team::TeamSelectionMode::Automatic
    };
    let team_count = if selection_mode == harness_contract::team::TeamSelectionMode::Explicit {
        intent.requested_team_count.max(1)
    } else {
        1
    };
    let team_owns_write = intent.requires_write
        && harness_contract::strategy::explicit_team_owns_persisted_artifact(&intent.objective);
    let research_team_count = if team_owns_write {
        team_count.saturating_sub(1)
    } else {
        team_count
    };
    let research_plans = (research_team_count > 0)
        .then(|| {
            derive_team_focus_partition_plans(
                &intent.objective,
                workspace_root,
                &[],
                intent.focus_count.max(research_team_count),
                false,
                selection_mode == harness_contract::team::TeamSelectionMode::Explicit,
                intent.requires_external_facts,
            )
        })
        .unwrap_or_default();
    let write_plans = team_owns_write
        .then(|| {
            derive_team_focus_partition_plans(
                &intent.objective,
                workspace_root,
                &[],
                1,
                true,
                selection_mode == harness_contract::team::TeamSelectionMode::Explicit,
                false,
            )
        })
        .unwrap_or_default();
    let focus_partition_plans = research_plans
        .iter()
        .chain(&write_plans)
        .cloned()
        .collect::<Vec<_>>();
    if focus_partition_plans.is_empty()
        || focus_partition_plans
            .iter()
            .flat_map(|plan| &plan.slots)
            .all(|slot| slot.capability_cropped_refs.is_empty())
    {
        return Err("program_intent_has_no_bounded_resource_scope".to_string());
    }
    let capabilities = focus_partition_plans
        .iter()
        .flat_map(|plan| &plan.slots)
        .flat_map(|slot| &slot.capability_cropped_refs)
        .map(|reference| format!("resource:{reference}"))
        .collect::<Vec<_>>();
    let research_focuses = semantic_focuses_from_plans(&research_plans);
    let write_focuses = semantic_focuses_from_plans(&write_plans);
    let team_node_ids = (0..team_count)
        .map(|index| format!("collaboration-{}-team-{}", intent.decision_id, index + 1))
        .collect::<Vec<_>>();
    let team_nodes = team_node_ids
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            let writer = team_owns_write && index + 1 == team_count;
            let explicit_contract = (selection_mode
                == harness_contract::team::TeamSelectionMode::Explicit)
                .then(|| {
                    explicit_team_node_contract(
                        index,
                        team_count,
                        team_owns_write,
                        intent.requires_external_facts,
                    )
                });
            let focuses = if writer {
                write_focuses.clone()
            } else {
                research_focuses.clone()
            };
            let mut resource_scopes = focuses
                .iter()
                .flat_map(|focus| focus.resource_scopes.iter().cloned())
                .collect::<Vec<_>>();
            resource_scopes.sort();
            resource_scopes.dedup();
            GraphSemanticNode {
                node_id: node_id.clone(),
                recipe: CapabilityRecipeId::Team,
                objective: intent.objective.clone(),
                depends_on: if writer && index > 0 {
                    team_node_ids[..index].to_vec()
                } else {
                    Vec::new()
                },
                multiplicity: 1,
                focuses,
                template: explicit_contract
                    .as_ref()
                    .map(|contract| contract.template.to_string()),
                target_session_id: None,
                output_artifacts: explicit_contract.as_ref().map_or_else(
                    || {
                        if writer {
                            vec![
                                "workspace_change".to_string(),
                                "terminal_synthesis".to_string(),
                            ]
                        } else {
                            vec!["terminal_synthesis".to_string()]
                        }
                    },
                    |contract| {
                        contract
                            .output_artifacts
                            .iter()
                            .map(|value| (*value).to_string())
                            .collect()
                    },
                ),
                evidence_contract: explicit_contract.as_ref().map_or_else(
                    || {
                        if writer {
                            vec![
                                "implementation".to_string(),
                                "source_verification".to_string(),
                                "evidence".to_string(),
                                "risks".to_string(),
                            ]
                        } else {
                            vec![
                                "summary".to_string(),
                                "evidence".to_string(),
                                "unresolved".to_string(),
                            ]
                        }
                    },
                    |contract| {
                        contract
                            .evidence_contract
                            .iter()
                            .map(|value| (*value).to_string())
                            .collect()
                    },
                ),
                required_evidence_refs: Vec::new(),
                resource_scopes,
                required: true,
                dependency: Default::default(),
                cancellation_group: None,
            }
        })
        .collect::<Vec<_>>();
    Ok(RuntimeOrchestrationCommand {
        intent: intent.objective,
        model_lease: Some(intent.model_lease),
        session_id: Some(intent.session_id),
        lineage: intent.lineage,
        mission_id: intent.mission_id,
        operation: RuntimeOrchestrationOperation::Propose,
        inspect_execution_id: None,
        proposal: Some(GraphMutationProposal {
            mutation_id: format!("strategy-{}", intent.decision_id),
            target_execution_id: None,
            expected_revision: None,
            nodes: team_nodes,
            completion: harness_contract::execution_graph::ExecutionCompletionContract {
                required_node_ids: team_node_ids,
                required_artifact_kinds: if team_owns_write {
                    vec!["workspace_change".to_string(), "terminal_synthesis".to_string()]
                } else {
                    vec!["terminal_synthesis".to_string()]
                },
                allow_unresolved_conflicts: false,
            },
            collaboration_program: None,
            collaboration_escalation: None,
            reason: format!(
                "admitted strategy decision selected Team at conversation admission ({selection_mode:?})"
            ),
        }),
        control: None,
        template_proposal: None,
        ephemeral_team_templates: Default::default(),
        input_disposition: None,
        selection_mode: Some(selection_mode),
        strategy_binding: Some(harness_contract::team::TeamStrategyBinding {
            decision_id: intent.decision_id,
            decision_revision: intent.decision_revision,
            decision_lease: intent.decision_lease,
            turn_ref: intent.turn_ref,
        }),
        capabilities,
        evidence_refs: Vec::new(),
        constraints: RuntimeOrchestrationConstraints {
            max_parallel_agents: Some(intent.focus_count.saturating_mul(team_count)),
            risk: Some(intent.risk),
            approval_id: None,
            requires_write: Some(team_owns_write),
            surface_latency_sensitive: Some(false),
            permission_ceiling: intent.permission_ceiling,
        },
        surface: Some("conversation_runtime_host".to_string()),
    })
}

/// Convert a fenced additive Program patch to an internal semantic revision.
/// `AddTeam` and `RequestReview` are the additive operations: the latter
/// derives its semantic dependencies from named durable Team instances rather
/// than trusting a free-form textual review target. Operations that mutate an
/// existing node or edge deliberately need the separate atomic Program+Graph
/// mutation path and are rejected here until that path exists.
pub(crate) fn compile_collaboration_intent_patch(
    graph: &ExecutionGraph,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<RuntimeOrchestrationCommand, String> {
    patch.validate()?;
    let program = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .ok_or_else(|| "patch_target_has_no_collaboration_program".to_string())?;
    if patch.program_id != program.program_id || patch.base_revision != program.revision {
        return Err("patch_program_revision_conflict".to_string());
    }
    let requested_review = match &patch.operation {
        harness_contract::execution_graph::CollaborationIntentPatchOperation::RequestReview {
            review,
            reviewed_instance_ids,
        } => Some((review, reviewed_instance_ids)),
        _ => None,
    };
    let review_team = requested_review
        .map(|(review, reviewed_instance_ids)| {
            materialize_review_patch_team(program, review, reviewed_instance_ids)
        })
        .transpose()?;
    let team = match &patch.operation {
        harness_contract::execution_graph::CollaborationIntentPatchOperation::AddTeam { team } => {
            team
        }
        harness_contract::execution_graph::CollaborationIntentPatchOperation::RequestReview {
            ..
        } => review_team
            .as_ref()
            .expect("request-review operation materializes its review Team"),
        _ => {
            return Err("patch_operation_requires_atomic_program_graph_mutation".to_string());
        }
    };
    if !team.behavior_facets.is_empty() && team.ephemeral_template.is_none() {
        return Err("add_team_behavior_facets_require_ephemeral_template_snapshot".to_string());
    }
    if program
        .semantic_node_instances
        .contains_key(&team.semantic_node_id)
    {
        return Err("patch_team_semantic_id_already_exists".to_string());
    }
    let seed = graph
        .nodes
        .iter()
        .find_map(|node| {
            serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
                &node.payload_ref,
            )
            .ok()
        })
        .ok_or_else(|| "patch_target_has_no_durable_team_binding_seed".to_string())?;
    // A custom AddTeam carries its complete immutable snapshot.  In that
    // case the parent Program's Team may itself be ephemeral, so attempting
    // to recover a reusable catalog selector from the seed would both fail
    // and reintroduce a mutable lookup that the snapshot deliberately avoids.
    let template = team
        .ephemeral_template
        .is_none()
        .then(|| template_path_from_seed(&seed))
        .transpose()?;
    let mutation_id = format!("program-patch:{}", patch.canonical_digest);
    Ok(RuntimeOrchestrationCommand {
        intent: patch.reason.clone(),
        model_lease: Some(seed.model_lease),
        session_id: Some(seed.lineage.session_id.clone()),
        lineage: Some(seed.lineage),
        mission_id: Some(seed.mission_id),
        operation: RuntimeOrchestrationOperation::Revise,
        inspect_execution_id: None,
        proposal: Some(GraphMutationProposal {
            mutation_id,
            target_execution_id: Some(graph.id.clone()),
            expected_revision: Some(graph.revision),
            nodes: vec![GraphSemanticNode {
                node_id: team.semantic_node_id.clone(),
                recipe: CapabilityRecipeId::Team,
                objective: team.objective.clone(),
                depends_on: team.depends_on.clone(),
                // A patch hint controls only the later ResourceManager
                // scheduling preference.  It is not permission to create
                // extra Team instances (or to revise a durable resource
                // ledger) while a Program is already running.
                multiplicity: 1,
                focuses: Vec::new(),
                // Preserve the selected Team definition family from the
                // already-admitted Program. A patch must not let the
                // planner's current default silently replace the source
                // Program's capability/acceptance contract.
                template,
                target_session_id: None,
                output_artifacts: team.output_artifacts.clone(),
                evidence_contract: team.evidence_contract.clone(),
                required_evidence_refs: patch
                    .evidence_refs
                    .iter()
                    .map(|reference| reference.evidence_ref.id.clone())
                    .collect(),
                resource_scopes: team.resource_scopes.clone(),
                required: team.required,
                dependency: Default::default(),
                cancellation_group: None,
            }],
            completion: harness_contract::execution_graph::ExecutionCompletionContract {
                required_node_ids: team
                    .required
                    .then(|| team.semantic_node_id.clone())
                    .into_iter()
                    .collect(),
                required_artifact_kinds: team.output_artifacts.clone(),
                allow_unresolved_conflicts: false,
            },
            collaboration_program: None,
            collaboration_escalation: patch.escalation.clone(),
            reason: patch.reason.clone(),
        }),
        control: None,
        template_proposal: None,
        ephemeral_team_templates: team
            .ephemeral_template
            .as_ref()
            .map(|snapshot| {
                std::collections::BTreeMap::from([(
                    team.semantic_node_id.clone(),
                    snapshot.clone(),
                )])
            })
            .unwrap_or_default(),
        input_disposition: None,
        selection_mode: Some(seed.selection_mode),
        strategy_binding: seed.strategy_binding,
        capabilities: team
            .resource_scopes
            .iter()
            .map(|scope| format!("resource:{scope}"))
            .collect(),
        evidence_refs: patch
            .evidence_refs
            .iter()
            .map(|reference| reference.evidence_ref.id.clone())
            .collect(),
        constraints: RuntimeOrchestrationConstraints {
            // Preserve the resolved Team topology.  `parallelism_hint` is a
            // soft scheduling signal and must not become a hidden hard
            // ceiling for the newly admitted Team's role branches.
            max_parallel_agents: None,
            risk: None,
            approval_id: None,
            requires_write: Some(
                team.resource_scopes
                    .iter()
                    .any(|scope| scope.starts_with("write:")),
            ),
            surface_latency_sensitive: Some(false),
            permission_ceiling: seed.permission_ceiling,
        },
        surface: Some("collaboration_program_patch".to_string()),
    })
}

fn materialize_review_patch_team(
    program: &CollaborationProgram,
    review: &harness_contract::execution_graph::CollaborationPatchTeam,
    reviewed_instance_ids: &[String],
) -> Result<harness_contract::execution_graph::CollaborationPatchTeam, String> {
    let mut semantic_dependencies = reviewed_instance_ids
        .iter()
        .map(|instance_id| {
            program
                .team_instances
                .iter()
                .find(|instance| instance.instance_id == *instance_id)
                .map(|instance| instance.semantic_node_id.clone())
                .ok_or_else(|| {
                    format!("review_patch_references_unknown_team_instance:{instance_id}")
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    semantic_dependencies.sort();
    semantic_dependencies.dedup();
    let mut review = review.clone();
    // The typed `reviewed_instance_ids` are authoritative. A model cannot
    // smuggle an unrelated dependency through the shape that describes the
    // new reviewer Team.
    review.depends_on = semantic_dependencies;
    Ok(review)
}

/// Compile a custom Team for exactly one session/turn without publishing it
/// to the shared Team catalog.  The snapshot contains the complete immutable
/// revision and exact Agent references, so later graph recovery uses no
/// mutable `LatestStable` lookup.
pub(crate) fn compile_ephemeral_team_template_snapshot(
    proposal_value: serde_json::Value,
    lineage: &harness_contract::execution_graph::ExecutionGraphLineage,
    permission_ceiling: harness_contract::policy::PermissionMode,
    policy_ref: String,
    expires_at_ms: u64,
    services: &RuntimeServices,
) -> Result<harness_contract::execution_graph::EphemeralTeamTemplateSnapshot, String> {
    let mut normalized = proposal_value;
    crate::team_template_candidate::normalize_template_proposal(&mut normalized)
        .map_err(|error| format!("ephemeral_template_invalid_proposal:{error}"))?;
    let proposal: crate::team_template_candidate::TeamTemplateProposal =
        serde_json::from_value(normalized)
            .map_err(|error| format!("ephemeral_template_invalid_proposal:{error}"))?;
    let candidate = crate::team_template_candidate::TemplateCandidateCompiler::compile(
        services.definition_registry(),
        &proposal,
        permission_ceiling,
    )
    .map_err(|error| format!("ephemeral_template_compile_failed:{error}"))?;
    let (revision, team_markdown) = crate::team_definition::build_revision(
        candidate.manifest,
        &crate::team_template_candidate::normalized_team_instructions(&proposal.instructions),
    )
    .map_err(|error| format!("ephemeral_template_revision_failed:{error}"))?;
    let snapshot = harness_contract::execution_graph::EphemeralTeamTemplateSnapshot {
        session_id: lineage.session_id.clone(),
        turn_id: lineage.turn_id.clone(),
        template_digest: revision.content_digest.clone(),
        role_ids: revision
            .manifest
            .roles
            .iter()
            .map(|role| role.role_id.clone())
            .collect(),
        revision,
        team_markdown,
        policy_ref,
        expires_at_ms,
        terminal_fence: format!("task:{}:turn:{}", lineage.root_task_id, lineage.turn_id),
    };
    snapshot
        .validate()
        .map_err(|error| format!("ephemeral_template_snapshot_invalid:{error}"))?;
    Ok(snapshot)
}

fn template_path_from_seed(
    seed: &harness_contract::team::TeamInstantiationRequest,
) -> Result<String, String> {
    let template_id = match &seed.template_selector {
        harness_contract::team::TeamTemplateSelector::Exact { revision_ref } => {
            &revision_ref.template_id
        }
        harness_contract::team::TeamTemplateSelector::LatestStable { template_id }
        | harness_contract::team::TeamTemplateSelector::Default { template_id } => template_id,
        harness_contract::team::TeamTemplateSelector::Automatic => {
            return Err("patch_target_seed_has_automatic_template_selector".to_string());
        }
        harness_contract::team::TeamTemplateSelector::Ephemeral { .. } => {
            return Err("patch_target_seed_has_ephemeral_template_selector".to_string());
        }
    };
    Ok(template_id.as_str().to_string())
}

/// Compile the complete Program admission truth into the root graph before it
/// is registered. This keeps `Program + obligations + frozen bindings` in the
/// same graph-registration transaction; recovery never has to select a newer
/// Team template merely to fill a missing control record.
pub(crate) fn prepare_program_admission(
    graph: &mut ExecutionGraph,
    teams: &TeamRuntime,
) -> Result<(), String> {
    let Some(program) = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .cloned()
    else {
        return Ok(());
    };
    if program.control.lifecycle != CollaborationProgramLifecycle::Planning {
        return Ok(());
    }
    let control = admission_control(graph, &program, teams)?;
    graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .ok_or_else(|| "program_control_disappeared_while_preparing_admission".to_string())?
        .control = control;
    Ok(())
}

/// Freeze only the newly compiled Team instances for an additive Program
/// revision. CommitService merges this delta in the same graph CAS that adds
/// the physical nodes, so an active Program never observes orphan Team nodes.
pub(crate) fn prepare_program_revision_admission(
    graph: &ExecutionGraph,
    delta: &mut CollaborationProgram,
    nodes: Vec<harness_contract::execution_graph::ExecutionNodeSpec>,
    teams: &TeamRuntime,
) -> Result<(), String> {
    if delta.control.lifecycle != CollaborationProgramLifecycle::Planning {
        return Err("program_revision_delta_is_not_planning".to_string());
    }
    let mut candidate = graph.clone();
    candidate.nodes = nodes;
    candidate.orchestration = Some(
        harness_contract::execution_graph::ExecutionOrchestrationMetadata {
            mutation_id: "program-revision-admission".to_string(),
            applied_mutation_ids: Vec::new(),
            collaboration_escalations: Vec::new(),
            semantic_revision: 0,
            source_generation: 0,
            completion: Default::default(),
            collaboration_program: Some(delta.clone()),
        },
    );
    delta.control = admission_control(&candidate, delta, teams)?;
    Ok(())
}

/// Record the durable child graph selected for one physical Team node. This
/// is idempotent and uses the root graph revision as the only race winner.
pub(crate) async fn mark_team_admitted(
    graph_id: &str,
    node_id: &str,
    child_graph_id: &str,
    supervisor: &RuntimeExecutionSupervisor,
    graphs: &ExecutionGraphStateStore,
) -> Result<(), String> {
    for _ in 0..MAX_CAS_ATTEMPTS {
        let graph = graphs
            .load_async(graph_id)
            .await
            .map_err(|error| format!("program_admission_load_failed:{error}"))?;
        let Some(program) = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
        else {
            return Ok(());
        };
        if program.control.lifecycle.is_terminal() {
            return Ok(());
        }
        let instance_id = instance_id_for_node(program, node_id)?;
        let mut control = program.control.clone();
        let obligation = control
            .obligations
            .iter_mut()
            .find(|obligation| obligation.instance_id == instance_id)
            .ok_or_else(|| format!("program_obligation_missing:{instance_id}"))?;
        if obligation.state == TeamAdmissionState::Admitted
            && obligation.child_graph_ref.as_deref() == Some(child_graph_id)
        {
            return Ok(());
        }
        obligation.state = TeamAdmissionState::Admitted;
        obligation.child_graph_ref = Some(child_graph_id.to_string());
        obligation.reason_kind = None;
        if control
            .obligations
            .iter()
            .all(|item| item.state == TeamAdmissionState::Admitted)
        {
            control.lifecycle = CollaborationProgramLifecycle::Running;
            control.waiting_relation = None;
            control.blocker_ref = None;
            control.next_action = Some("await_graph_transitions".to_string());
        }
        match supervisor
            .command(
                graph_id,
                ExecutionGraphCommand::UpdateCollaborationProgramControl {
                    expected_revision: graph.revision,
                    control: Box::new(control),
                },
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::execution_core::graph::ExecutionRunnerError::Commit(
                crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
            )) => continue,
            Err(error) => return Err(format!("program_admission_commit_failed:{error}")),
        }
    }
    Err("program_admission_conflict_exhausted".to_string())
}

/// Persist all incoming cross-Team deliveries before a consumer Team is
/// admitted. The commit service derives each receipt from the durable
/// producer result and validates the typed input contract; this facade only
/// selects the current consumer attempt and retries narrow graph-revision
/// conflicts. It never carries model text or arbitrary artifacts.
pub(crate) async fn record_incoming_cross_team_deliveries(
    graph_id: &str,
    consumer_node_id: &str,
    supervisor: &RuntimeExecutionSupervisor,
    graphs: &ExecutionGraphStateStore,
) -> Result<(), String> {
    let mut stale_conflicts = 0;
    loop {
        let graph = graphs
            .load_async(graph_id)
            .await
            .map_err(|error| format!("cross_team_delivery_load_failed:{error}"))?;
        let Some(program) = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
        else {
            return Ok(());
        };
        let consumer_instance = instance_id_for_node(program, consumer_node_id)?;
        let pending = program
            .edges
            .iter()
            .filter(|edge| {
                edge.to == consumer_instance
                    && matches!(
                        edge.state,
                        harness_contract::execution_graph::CrossTeamEdgeState::Pending
                            | harness_contract::execution_graph::CrossTeamEdgeState::AwaitingProducer
                    )
            })
            .map(|edge| (edge.edge_id.clone(), edge.from.clone()))
            .collect::<Vec<_>>();
        if pending.is_empty() {
            if program.edges.iter().any(|edge| {
                edge.to == consumer_instance
                    && matches!(
                        edge.state,
                        harness_contract::execution_graph::CrossTeamEdgeState::Blocked
                            | harness_contract::execution_graph::CrossTeamEdgeState::Cancelled
                    )
            }) {
                return Err("cross_team_delivery_is_terminally_blocked".to_string());
            }
            return Ok(());
        }
        let (edge_id, producer_instance) = pending[0].clone();
        let producer_node_id = node_id_for_instance(program, &producer_instance)?;
        let producer_attempt = graph
            .recovery_cursor
            .node_attempts
            .get(&producer_node_id)
            .copied()
            .unwrap_or_default();
        match supervisor
            .command(
                graph_id,
                ExecutionGraphCommand::RecordCrossTeamEdgeDelivery {
                    expected_revision: graph.revision,
                    edge_id,
                    producer_node_id,
                    producer_attempt,
                },
            )
            .await
        {
            Ok(_) => {
                // A successful command consumes exactly one pending edge. It
                // is not a CAS retry, so a fan-in larger than the retry bound
                // remains admissible.
                stale_conflicts = 0;
                continue;
            }
            Err(crate::execution_core::graph::ExecutionRunnerError::Commit(
                crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
            )) if stale_conflicts < MAX_CAS_ATTEMPTS => {
                stale_conflicts = stale_conflicts.saturating_add(1);
                continue;
            }
            Err(error) => return Err(format!("cross_team_delivery_commit_failed:{error}")),
        }
    }
}

/// Claim every delivered incoming edge before the consumer Team is admitted.
/// A duplicate call for the same node attempt is a no-op; a different attempt
/// cannot overwrite the prior claim.
pub(crate) async fn claim_incoming_cross_team_deliveries(
    graph_id: &str,
    consumer_node_id: &str,
    consumer_attempt: u32,
    supervisor: &RuntimeExecutionSupervisor,
    graphs: &ExecutionGraphStateStore,
) -> Result<(), String> {
    let mut stale_conflicts = 0;
    loop {
        let graph = graphs
            .load_async(graph_id)
            .await
            .map_err(|error| format!("cross_team_claim_load_failed:{error}"))?;
        let Some(program) = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
        else {
            return Ok(());
        };
        let consumer_instance = instance_id_for_node(program, consumer_node_id)?;
        let incoming = program
            .edges
            .iter()
            .filter(|edge| edge.to == consumer_instance)
            .collect::<Vec<_>>();
        if incoming.iter().any(|edge| {
            matches!(
                edge.state,
                harness_contract::execution_graph::CrossTeamEdgeState::Blocked
                    | harness_contract::execution_graph::CrossTeamEdgeState::Cancelled
            )
        }) {
            return Err("cross_team_claim_is_terminally_blocked".to_string());
        }
        let delivered = incoming
            .iter()
            .find(|edge| {
                edge.state == harness_contract::execution_graph::CrossTeamEdgeState::Delivered
            })
            .map(|edge| edge.edge_id.clone());
        let Some(edge_id) = delivered else {
            if incoming.is_empty() {
                return Ok(());
            }
            if incoming.iter().any(|edge| {
                matches!(
                    edge.state,
                    harness_contract::execution_graph::CrossTeamEdgeState::Pending
                        | harness_contract::execution_graph::CrossTeamEdgeState::AwaitingProducer
                )
            }) {
                return Err("cross_team_claims_not_all_delivered".to_string());
            }
            return if incoming.iter().all(|edge| {
                edge.state == harness_contract::execution_graph::CrossTeamEdgeState::Claimed
                    && edge.claim_receipt.as_ref().is_some_and(|claim| {
                        claim.consumer_node_id == consumer_node_id
                            && claim.consumer_attempt == consumer_attempt
                    })
            }) {
                Ok(())
            } else {
                Err("cross_team_claim_attempt_conflict".to_string())
            };
        };
        match supervisor
            .command(
                graph_id,
                ExecutionGraphCommand::ClaimCrossTeamEdgeDelivery {
                    expected_revision: graph.revision,
                    edge_id,
                    consumer_node_id: consumer_node_id.to_string(),
                    consumer_attempt,
                },
            )
            .await
        {
            Ok(_) => {
                stale_conflicts = 0;
                continue;
            }
            Err(crate::execution_core::graph::ExecutionRunnerError::Commit(
                crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
            )) if stale_conflicts < MAX_CAS_ATTEMPTS => {
                stale_conflicts = stale_conflicts.saturating_add(1);
                continue;
            }
            Err(error) => return Err(format!("cross_team_claim_commit_failed:{error}")),
        }
    }
}

/// A Team compiler/admitter rejection is durable Program truth, not merely a
/// transient executor string. The root node retains the detailed failure;
/// Program records the typed obligation disposition so required-N admission
/// cannot be mistaken for a partial completion.
pub(crate) async fn mark_team_admission_rejected(
    graph_id: &str,
    node_id: &str,
    supervisor: &RuntimeExecutionSupervisor,
    graphs: &ExecutionGraphStateStore,
) -> Result<(), String> {
    for _ in 0..MAX_CAS_ATTEMPTS {
        let graph = graphs
            .load_async(graph_id)
            .await
            .map_err(|error| format!("program_admission_rejection_load_failed:{error}"))?;
        let Some(program) = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
        else {
            return Ok(());
        };
        if program.control.lifecycle.is_terminal() {
            return Ok(());
        }
        let instance_id = instance_id_for_node(program, node_id)?;
        let mut control = program.control.clone();
        let obligation = control
            .obligations
            .iter_mut()
            .find(|obligation| obligation.instance_id == instance_id)
            .ok_or_else(|| format!("program_obligation_missing:{instance_id}"))?;
        if obligation.state == TeamAdmissionState::BlockedPolicy
            && obligation.reason_kind.as_deref() == Some("team_admission_rejected")
        {
            return Ok(());
        }
        // Never overwrite a successfully admitted child with a late failure
        // from an obsolete executor attempt.
        if obligation.state == TeamAdmissionState::Admitted {
            return Ok(());
        }
        obligation.state = TeamAdmissionState::BlockedPolicy;
        obligation.reason_kind = Some("team_admission_rejected".to_string());
        control.lifecycle = CollaborationProgramLifecycle::Blocked;
        control.waiting_relation = None;
        control.blocker_ref = Some(format!("execution-node:{node_id}"));
        control.next_action = Some("inspect_team_admission_failure".to_string());
        match supervisor
            .command(
                graph_id,
                ExecutionGraphCommand::UpdateCollaborationProgramControl {
                    expected_revision: graph.revision,
                    control: Box::new(control),
                },
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::execution_core::graph::ExecutionRunnerError::Commit(
                crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
            )) => continue,
            Err(error) => return Err(format!("program_admission_rejection_commit_failed:{error}")),
        }
    }
    Err("program_admission_rejection_conflict_exhausted".to_string())
}

/// Reconcile a Program after its root graph reaches a terminal state. Required
/// Team omissions are failures, never an invented `Partial` success.
pub(crate) async fn reconcile_terminal_program(
    graph_id: &str,
    services: &RuntimeServices,
) -> Result<(), String> {
    reconcile_terminal_program_with(
        graph_id,
        services.execution_supervisor().as_ref(),
        services.graph_state_store(),
    )
    .await
}

pub(crate) async fn reconcile_terminal_program_with(
    graph_id: &str,
    supervisor: &RuntimeExecutionSupervisor,
    graphs: &ExecutionGraphStateStore,
) -> Result<(), String> {
    for _ in 0..MAX_CAS_ATTEMPTS {
        let graph = match graphs.load_async(graph_id).await {
            Ok(graph) => graph,
            Err(ExecutionStateStoreError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(format!("program_terminal_load_failed:{error}")),
        };
        let Some(program) = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
        else {
            return Ok(());
        };
        if program.control.lifecycle.is_terminal()
            || graph
                .node_statuses
                .values()
                .any(|status| !status.is_terminal())
        {
            return Ok(());
        }
        let mut control = program.control.clone();
        let every_required_team_completed = program.team_instances.iter().all(|instance| {
            node_id_for_instance(program, &instance.instance_id).is_ok_and(|node_id| {
                graph.node_statuses.get(node_id.as_str())
                    == Some(&harness_contract::execution_graph::ExecutionNodeStatus::Completed)
            })
        });
        control.lifecycle = if every_required_team_completed
            && control
                .obligations
                .iter()
                .all(|obligation| obligation.state == TeamAdmissionState::Admitted)
        {
            CollaborationProgramLifecycle::Completed
        } else {
            CollaborationProgramLifecycle::Failed
        };
        control.waiting_relation = None;
        control.blocker_ref = None;
        control.next_action = None;
        match supervisor
            .command(
                graph_id,
                ExecutionGraphCommand::UpdateCollaborationProgramControl {
                    expected_revision: graph.revision,
                    control: Box::new(control),
                },
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::execution_core::graph::ExecutionRunnerError::Commit(
                crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
            )) => continue,
            Err(error) => return Err(format!("program_terminal_commit_failed:{error}")),
        }
    }
    Err("program_terminal_conflict_exhausted".to_string())
}

/// Project durable node waiting state into the Program control plane. Only
/// graph states are consumed here: no provider/resource-manager internals are
/// guessed or copied into Program truth.
pub(crate) async fn reconcile_program_wait_state_with(
    graph_id: &str,
    supervisor: &RuntimeExecutionSupervisor,
    graphs: &ExecutionGraphStateStore,
) -> Result<(), String> {
    for _ in 0..MAX_CAS_ATTEMPTS {
        let graph = match graphs.load_async(graph_id).await {
            Ok(graph) => graph,
            Err(ExecutionStateStoreError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(format!("program_wait_load_failed:{error}")),
        };
        let Some(program) = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
        else {
            return Ok(());
        };
        if program.control.lifecycle.is_terminal() {
            return Ok(());
        }
        let approval_node = graph.node_statuses.iter().find_map(|(node_id, status)| {
            (*status == harness_contract::execution_graph::ExecutionNodeStatus::WaitingApproval)
                .then_some(node_id.clone())
        });
        let all_admitted = program
            .control
            .obligations
            .iter()
            .all(|obligation| obligation.state == TeamAdmissionState::Admitted);
        let mut control = program.control.clone();
        if let Some(node_id) = approval_node {
            control.lifecycle = CollaborationProgramLifecycle::AwaitingApproval;
            control.waiting_relation = Some("approval".to_string());
            control.blocker_ref = Some(format!("execution-node:{node_id}"));
            control.next_action = Some("await_canonical_approval_decision".to_string());
        } else if all_admitted {
            control.lifecycle = CollaborationProgramLifecycle::Running;
            control.waiting_relation = None;
            control.blocker_ref = None;
            control.next_action = Some("await_graph_transitions".to_string());
        } else {
            control.lifecycle = CollaborationProgramLifecycle::Admitting;
            control.waiting_relation = Some("team_admission".to_string());
            control.blocker_ref = None;
            control.next_action = Some("admit_exact_team_bindings".to_string());
        }
        if control == program.control {
            return Ok(());
        }
        match supervisor
            .command(
                graph_id,
                ExecutionGraphCommand::UpdateCollaborationProgramControl {
                    expected_revision: graph.revision,
                    control: Box::new(control),
                },
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::execution_core::graph::ExecutionRunnerError::Commit(
                crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
            )) => continue,
            Err(error) => return Err(format!("program_wait_commit_failed:{error}")),
        }
    }
    Err("program_wait_conflict_exhausted".to_string())
}

/// Bounded startup reconciliation for Program control truth. This scans
/// durable graph cursors once; it neither starts workers nor polls Teams.
/// A restarted non-terminal Program still needs its durable approval/admission
/// wait state projected before the regular graph recovery pump resumes it.
pub(crate) async fn reconcile_terminal_programs_on_startup(
    supervisor: &RuntimeExecutionSupervisor,
    graphs: &ExecutionGraphStateStore,
    limit: usize,
) -> Result<usize, String> {
    let mut cursor = None;
    let mut examined = 0usize;
    while examined < limit {
        let page = graphs
            .graph_ids_page(cursor.take(), limit.saturating_sub(examined).max(1))
            .map_err(|error| format!("program_startup_page_failed:{error}"))?;
        if page.is_empty() {
            break;
        }
        for (graph_id, _) in &page {
            if examined >= limit {
                break;
            }
            reconcile_program_wait_state_with(graph_id, supervisor, graphs).await?;
            reconcile_terminal_program_with(graph_id, supervisor, graphs).await?;
            examined = examined.saturating_add(1);
        }
        let (graph_id, commit_cursor) = page
            .last()
            .expect("non-empty Program reconciliation page has cursor");
        cursor = Some((*commit_cursor, graph_id.clone()));
    }
    Ok(examined)
}

fn admission_control(
    graph: &ExecutionGraph,
    program: &CollaborationProgram,
    teams: &TeamRuntime,
) -> Result<CollaborationProgramControlState, String> {
    let mut obligations = Vec::with_capacity(program.team_instances.len());
    let mut deadline_at_ms = 0u64;
    // These are immutable admission requirements, not a streaming usage
    // meter. P4 owns reservations/reconciliation once ResourceManager can
    // expose a durable wait/lease receipt. Keeping the compilation-time
    // claims here makes an Admitting Program recoverable without guessing
    // from a mutable provider queue.
    let mut context_reservation_tokens = 0u64;
    let mut output_reservation_tokens = 0u64;
    let mut parallel_demand = 0u64;
    for instance in &program.team_instances {
        let node_id = node_id_for_instance(program, &instance.instance_id)?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .ok_or_else(|| format!("program_team_node_missing:{node_id}"))?;
        let request = serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
            &node.payload_ref,
        )
        .map_err(|error| format!("program_team_request_invalid:{node_id}:{error}"))?;
        let team_plan = teams.plan(request.clone())?;
        let binding = team_plan
            .binding
            .ok_or_else(|| format!("program_team_binding_missing:{node_id}"))?;
        deadline_at_ms = deadline_at_ms.max(request.deadline_at_ms);
        context_reservation_tokens =
            context_reservation_tokens.saturating_add(request.execution_budget.predicted_tokens());
        output_reservation_tokens =
            output_reservation_tokens.saturating_add(request.execution_budget.max_tokens);
        parallel_demand = parallel_demand.saturating_add(
            u64::try_from(
                team_plan
                    .graph
                    .nodes
                    .iter()
                    .filter(|node| {
                        node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                    })
                    .count(),
            )
            .unwrap_or(u64::MAX),
        );
        obligations.push(TeamAdmissionObligation {
            instance_id: instance.instance_id.clone(),
            binding_ref: format!("team-binding:sha256:{}", binding.binding_digest),
            state: TeamAdmissionState::Admitting,
            child_graph_ref: None,
            reason_kind: None,
            revision: program.revision,
        });
    }
    Ok(CollaborationProgramControlState {
        lifecycle: CollaborationProgramLifecycle::Admitting,
        obligations,
        resource_ledger: harness_contract::execution_graph::ProgramResourceLedger {
            context_reservation_tokens,
            output_reservation_tokens,
            parallel_demand: u16::try_from(parallel_demand).unwrap_or(u16::MAX).max(1),
            deadline_at_ms,
            confidence_basis_points: 10_000,
            revision: program.revision,
        },
        waiting_relation: Some("team_admission".to_string()),
        blocker_ref: None,
        next_action: Some("admit_exact_team_bindings".to_string()),
    })
}

fn node_id_for_instance(
    program: &CollaborationProgram,
    instance_id: &str,
) -> Result<String, String> {
    let (semantic_id, index) = instance_id
        .rsplit_once(':')
        .ok_or_else(|| format!("program_instance_id_invalid:{instance_id}"))?;
    let index = index
        .parse::<usize>()
        .map_err(|_| format!("program_instance_index_invalid:{instance_id}"))?;
    program
        .semantic_node_instances
        .get(semantic_id)
        .and_then(|nodes| nodes.get(index.saturating_sub(1)))
        .cloned()
        .ok_or_else(|| format!("program_instance_node_mapping_missing:{instance_id}"))
}

fn instance_id_for_node(program: &CollaborationProgram, node_id: &str) -> Result<String, String> {
    program
        .team_instances
        .iter()
        .find(|instance| {
            node_id_for_instance(program, &instance.instance_id)
                .is_ok_and(|candidate| candidate == node_id)
        })
        .map(|instance| instance.instance_id.clone())
        .ok_or_else(|| format!("program_instance_for_node_missing:{node_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_program_intent_compiles_exact_explicit_team_topology() {
        let request = compile_conversation_program_intent(
            ConversationProgramIntent {
                objective: "Research three independent public sources and compare their evidence"
                    .to_string(),
                model_lease: "test-model".to_string(),
                session_id: "session-intent".to_string(),
                lineage: None,
                mission_id: None,
                decision_id: "decision-intent".to_string(),
                decision_revision: 3,
                decision_lease: "lease-intent".to_string(),
                turn_ref: "turn-intent".to_string(),
                requested_team_count: 3,
                focus_count: 3,
                requests_multi_agent: true,
                requires_write: false,
                requires_external_facts: true,
                permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                risk: "medium".to_string(),
            },
            std::path::Path::new("."),
        )
        .expect("external-evidence intent has bounded network scopes");
        let proposal = request
            .proposal
            .expect("Program intent has a graph proposal");
        assert_eq!(proposal.nodes.len(), 3);
        assert_eq!(proposal.completion.required_node_ids.len(), 3);
        assert!(proposal.nodes.iter().all(|node| {
            node.recipe == CapabilityRecipeId::Team
                && node.template.as_deref() == Some("cowd/external-research-synthesis")
                && node
                    .resource_scopes
                    .iter()
                    .all(|scope| scope == "network:*")
        }));
        assert_eq!(request.constraints.max_parallel_agents, Some(9));
    }

    #[test]
    fn add_team_patch_cannot_target_a_graph_without_a_durable_program() {
        let patch = harness_contract::execution_graph::CollaborationIntentPatch {
            program_id: "missing-program".to_string(),
            base_revision: 1,
            source_attempt: "attempt-1".to_string(),
            reason: "add an independent reviewer".to_string(),
            evidence_refs: Vec::new(),
            canonical_digest: "c".repeat(64),
            user_confirmation_ref: None,
            escalation: None,
            operation:
                harness_contract::execution_graph::CollaborationIntentPatchOperation::AddTeam {
                    team: harness_contract::execution_graph::CollaborationPatchTeam {
                        semantic_node_id: "review".to_string(),
                        objective: "independently review evidence".to_string(),
                        depends_on: Vec::new(),
                        behavior_facets: Vec::new(),
                        ephemeral_template: None,
                        resource_scopes: vec!["network:*".to_string()],
                        output_artifacts: vec!["review".to_string()],
                        evidence_contract: vec!["evidence".to_string()],
                        required: true,
                        parallelism_hint: 1,
                    },
                },
        };
        let error =
            compile_collaboration_intent_patch(&ExecutionGraph::new("ordinary graph"), &patch)
                .expect_err("Patch must target a durable Program");
        assert_eq!(error, "patch_target_has_no_collaboration_program");
    }

    #[test]
    fn request_review_derives_only_named_durable_team_dependencies() {
        let program = CollaborationProgram {
            program_id: "program-review".to_string(),
            revision: 2,
            required_team_count: 3,
            team_instances: vec![
                harness_contract::execution_graph::CollaborationTeamInstance {
                    instance_id: "research:1".to_string(),
                    semantic_node_id: "research".to_string(),
                    required: true,
                },
                harness_contract::execution_graph::CollaborationTeamInstance {
                    instance_id: "research:2".to_string(),
                    semantic_node_id: "research".to_string(),
                    required: true,
                },
                harness_contract::execution_graph::CollaborationTeamInstance {
                    instance_id: "implementation:1".to_string(),
                    semantic_node_id: "implementation".to_string(),
                    required: true,
                },
            ],
            edges: Vec::new(),
            semantic_node_instances: std::collections::BTreeMap::new(),
            control: Default::default(),
        };
        let review = harness_contract::execution_graph::CollaborationPatchTeam {
            semantic_node_id: "independent-review".to_string(),
            objective: "review only the named Team outcomes".to_string(),
            depends_on: vec!["untrusted-model-dependency".to_string()],
            behavior_facets: Vec::new(),
            ephemeral_template: None,
            resource_scopes: vec!["read:src".to_string()],
            output_artifacts: vec!["review".to_string()],
            evidence_contract: vec!["evidence".to_string()],
            required: true,
            parallelism_hint: 1,
        };
        let materialized = materialize_review_patch_team(
            &program,
            &review,
            &[
                "implementation:1".to_string(),
                "research:1".to_string(),
                "research:2".to_string(),
            ],
        )
        .expect("named instances resolve to their durable semantic dependencies");
        assert_eq!(
            materialized.depends_on,
            vec!["implementation".to_string(), "research".to_string()]
        );
        assert!(
            materialize_review_patch_team(&program, &review, &["missing:1".to_string()]).is_err()
        );
    }

    #[tokio::test]
    async fn consumer_cannot_admit_after_only_one_of_multiple_incoming_claims() {
        use harness_contract::execution_graph::{
            CollaborationEdgeKind, CollaborationProgramEdge, CollaborationProgramLifecycle,
            CollaborationTeamInstance, CrossTeamEdgeClaimReceipt, CrossTeamEdgeDeliveryReceipt,
            CrossTeamEdgeState, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionOrchestrationMetadata,
        };

        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let mut graph = ExecutionGraph::new("fan-in receipt fence");
        graph.id = "fan-in-claim-root".to_string();
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        for node_id in ["producer-a", "producer-b", "consumer"] {
            let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "{}");
            node.id = node_id.to_string();
            node.idempotency_key = format!("{node_id}-key");
            graph.node_statuses.insert(
                node.id.clone(),
                harness_contract::execution_graph::ExecutionNodeStatus::Planned,
            );
            graph.nodes.push(node);
        }
        graph.orchestration = Some(ExecutionOrchestrationMetadata {
            mutation_id: "fan-in-claim-test".to_string(),
            applied_mutation_ids: Vec::new(),
            collaboration_escalations: Vec::new(),
            semantic_revision: 1,
            source_generation: 1,
            completion: Default::default(),
            collaboration_program: Some(CollaborationProgram {
                program_id: "program-fan-in".to_string(),
                revision: 1,
                required_team_count: 3,
                team_instances: vec![
                    CollaborationTeamInstance {
                        instance_id: "a:1".to_string(),
                        semantic_node_id: "a".to_string(),
                        required: true,
                    },
                    CollaborationTeamInstance {
                        instance_id: "b:1".to_string(),
                        semantic_node_id: "b".to_string(),
                        required: true,
                    },
                    CollaborationTeamInstance {
                        instance_id: "consumer:1".to_string(),
                        semantic_node_id: "consumer".to_string(),
                        required: true,
                    },
                ],
                edges: vec![
                    CollaborationProgramEdge {
                        edge_id: "a:1->consumer:1".to_string(),
                        from: "a:1".to_string(),
                        to: "consumer:1".to_string(),
                        kind: CollaborationEdgeKind::Handoff,
                        input_contract: Default::default(),
                        state: CrossTeamEdgeState::Claimed,
                        delivery_receipt: Some(CrossTeamEdgeDeliveryReceipt {
                            receipt_ref: "receipt-a".to_string(),
                            producer_node_id: "producer-a".to_string(),
                            producer_attempt: 1,
                            producer_result_ref: "artifact:a".to_string(),
                            evidence_refs: Vec::new(),
                        }),
                        claim_receipt: Some(CrossTeamEdgeClaimReceipt {
                            claim_ref: "claim-a".to_string(),
                            consumer_node_id: "consumer".to_string(),
                            consumer_attempt: 1,
                        }),
                    },
                    CollaborationProgramEdge {
                        edge_id: "b:1->consumer:1".to_string(),
                        from: "b:1".to_string(),
                        to: "consumer:1".to_string(),
                        kind: CollaborationEdgeKind::Handoff,
                        input_contract: Default::default(),
                        state: CrossTeamEdgeState::Pending,
                        delivery_receipt: None,
                        claim_receipt: None,
                    },
                ],
                semantic_node_instances: std::collections::BTreeMap::from([
                    ("a".to_string(), vec!["producer-a".to_string()]),
                    ("b".to_string(), vec!["producer-b".to_string()]),
                    ("consumer".to_string(), vec!["consumer".to_string()]),
                ]),
                control: harness_contract::execution_graph::CollaborationProgramControlState {
                    lifecycle: CollaborationProgramLifecycle::Planning,
                    ..Default::default()
                },
            }),
        });
        let registered = services
            .commit_service()
            .register_graph(graph)
            .expect("register fan-in graph")
            .graph;
        let error = claim_incoming_cross_team_deliveries(
            &registered.id,
            "consumer",
            1,
            services.execution_supervisor().as_ref(),
            services.graph_state_store(),
        )
        .await
        .expect_err("one claimed receipt cannot admit a two-edge consumer");
        assert_eq!(error, "cross_team_claims_not_all_delivered");
    }
}
