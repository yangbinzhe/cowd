//! Runtime-only application of model-selected running-Turn input semantics.

use harness_contract::{
    execution_graph::{
        ExecutionCompletionContract, ExecutionDependencyPolicy, ExecutionGraphLineage,
        ExecutionParentBinding,
    },
    input_disposition::{
        InputApplicationState, InputDispositionAction, ModelInputDispositionBatch,
        ModelInputDispositionDecision, RuntimeInputDispositionInput, RuntimeInputDispositionScope,
        SessionInputApplicationReceipt,
    },
    orchestration::{
        CapabilityRecipeId, ModelGraphMutationProposal, ModelGraphSemanticNode,
        ModelRuntimeOrchestrationConstraints, ModelRuntimeOrchestrationInput,
        RuntimeOrchestrationOperation,
    },
    policy::PermissionMode,
    reality::EvidenceRef,
    task::{TaskOrigin, TaskStatus},
};

use crate::{
    task::materialize_additional_session_task, RuntimeOrchestrationBinding,
    RuntimeOrchestrationCommand, RuntimeServices,
};

#[derive(Debug, Clone)]
pub(crate) struct InputDispositionRuntimeBinding {
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub execution_node_id: String,
    pub execution_revision: u64,
    pub lineage: ExecutionGraphLineage,
    pub mission_id: String,
    pub goal_id: String,
    pub model_lease: Option<String>,
    pub permission_ceiling: PermissionMode,
    pub capabilities: Vec<String>,
    pub constraints: ModelRuntimeOrchestrationConstraints,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AppliedInputDispositionBatch {
    pub receipts: Vec<SessionInputApplicationReceipt>,
    pub structural: bool,
    pub requires_fresh_model_step: bool,
    pub summaries: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct MaterializationEvidence {
    summary: String,
    task_ids: Vec<String>,
    team_ids: Vec<String>,
    agent_ids: Vec<String>,
    execution_ids: Vec<String>,
    target_session_id: Option<String>,
    target_session_created: bool,
}

pub(crate) async fn apply_input_disposition_batch(
    services: &RuntimeServices,
    binding: &InputDispositionRuntimeBinding,
    slot_input_ids: &[String],
    batch: &ModelInputDispositionBatch,
) -> Result<AppliedInputDispositionBatch, String> {
    batch.validate_slots(slot_input_ids.len())?;
    if binding.execution_revision == 0 {
        return Err("input disposition requires a committed execution graph revision".to_string());
    }
    let query = services
        .session_query_port()
        .ok_or_else(|| "Session Runtime query port is not installed".to_string())?;
    let application = services
        .session_application_port()
        .ok_or_else(|| "Session Runtime application port is not installed".to_string())?;

    let mut slot_records = Vec::with_capacity(slot_input_ids.len());
    for (slot, input_id) in slot_input_ids.iter().enumerate() {
        let record = query
            .runtime_input_by_input_id(input_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("durable Session input `{input_id}` does not exist"))?;
        if record.session_id != binding.session_id
            || record.target_turn_id.as_deref() != Some(binding.turn_id.as_str())
        {
            return Err(format!(
                "Session input `{input_id}` escaped the active Turn disposition scope"
            ));
        }
        slot_records.push((slot, record));
    }
    let session_generation = slot_records
        .first()
        .map(|(_, record)| record.session_generation)
        .ok_or_else(|| "input disposition scope has no durable records".to_string())?;
    if slot_records
        .iter()
        .any(|(_, record)| record.session_generation != session_generation)
    {
        return Err("input disposition crossed Session authority generations".to_string());
    }
    RuntimeInputDispositionScope {
        session_id: binding.session_id.clone(),
        turn_id: binding.turn_id.clone(),
        session_generation,
        execution_id: binding.execution_id.clone(),
        expected_graph_revision: binding.execution_revision,
        task_id: Some(binding.lineage.task_id.clone()),
        mission_id: (!binding.mission_id.trim().is_empty()).then(|| binding.mission_id.clone()),
        inputs: slot_records
            .iter()
            .map(|(slot, record)| {
                Ok(RuntimeInputDispositionInput {
                    slot: u16::try_from(*slot).map_err(|_| {
                        "input disposition has more slots than the contract allows".to_string()
                    })?,
                    input_id: record.input_id.clone(),
                    request_id: record.request_id.clone(),
                    sequence: record.sequence,
                    revision: record.revision,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
    }
    .validate()?;

    let mut result = AppliedInputDispositionBatch::default();
    for decision in &batch.decisions {
        let mut records = decision
            .input_slots
            .iter()
            .map(|slot| {
                slot_records
                    .get(usize::from(*slot))
                    .map(|(_, record)| record.clone())
                    .ok_or_else(|| format!("input slot {slot} disappeared before application"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        records.sort_by(|left, right| {
            left.sequence
                .cmp(&right.sequence)
                .then_with(|| left.input_id.cmp(&right.input_id))
        });
        let disposition_id = disposition_id(binding, decision, &records);
        let input_ids = records
            .iter()
            .map(|record| record.input_id.clone())
            .collect::<Vec<_>>();
        let leader_input_id = input_ids[0].clone();
        let existing = coherent_existing_receipt(&records, &disposition_id, &input_ids)?;
        if let Some(receipt) = existing
            .as_ref()
            .filter(|receipt| receipt.state == InputApplicationState::Applied)
        {
            result.structural |= receipt.action.is_structural();
            result.requires_fresh_model_step |= receipt.action.is_structural();
            result.summaries.push(receipt.summary.clone());
            result.receipts.push(receipt.clone());
            continue;
        }

        let mut current_records = records;
        let mut receipt = existing.unwrap_or_else(|| SessionInputApplicationReceipt {
            disposition_id: disposition_id.clone(),
            leader_input_id: leader_input_id.clone(),
            input_ids: input_ids.clone(),
            action: decision.action,
            relation: decision.relation,
            state: InputApplicationState::Prepared,
            objective: decision.objective.trim().to_string(),
            required: decision.required,
            attempts: 1,
            summary: "input disposition prepared".to_string(),
            task_ids: Vec::new(),
            team_ids: Vec::new(),
            agent_ids: Vec::new(),
            execution_ids: Vec::new(),
            target_session_id: None,
            target_session_created: false,
            error: None,
            revision: 0,
            updated_at_ms: now_ms(),
        });
        if current_records
            .iter()
            .all(|record| record.application_receipt.is_none())
        {
            current_records =
                commit_receipt(application.as_ref(), &current_records, &receipt).await?;
        } else if receipt.state == InputApplicationState::Failed {
            if receipt.attempts >= 2 {
                return Err(format!(
                    "input disposition exhausted two durable materialization attempts: {}",
                    receipt.error.as_deref().unwrap_or("unknown failure")
                ));
            }
            receipt.state = InputApplicationState::Prepared;
            receipt.attempts = receipt.attempts.saturating_add(1);
            receipt.summary = "retrying the same durable input disposition".to_string();
            receipt.error = None;
            receipt.revision = receipt.revision.saturating_add(1);
            receipt.updated_at_ms = now_ms();
            current_records =
                commit_receipt(application.as_ref(), &current_records, &receipt).await?;
        }

        if receipt.state == InputApplicationState::Prepared {
            receipt.state = InputApplicationState::Materializing;
            receipt.summary = "materializing typed input disposition".to_string();
            receipt.revision = receipt.revision.saturating_add(1);
            receipt.updated_at_ms = now_ms();
            current_records =
                commit_receipt(application.as_ref(), &current_records, &receipt).await?;
        }

        match materialize_decision(
            services,
            binding,
            decision,
            &disposition_id,
            &leader_input_id,
            &input_ids,
        )
        .await
        {
            Ok(evidence) => {
                receipt.state = InputApplicationState::Applied;
                receipt.summary = evidence.summary;
                receipt.task_ids = evidence.task_ids;
                receipt.team_ids = evidence.team_ids;
                receipt.agent_ids = evidence.agent_ids;
                receipt.execution_ids = evidence.execution_ids;
                receipt.target_session_id = evidence.target_session_id;
                receipt.target_session_created = evidence.target_session_created;
                receipt.error = None;
                receipt.revision = receipt.revision.saturating_add(1);
                receipt.updated_at_ms = now_ms();
                let _ = commit_receipt(application.as_ref(), &current_records, &receipt).await?;
                result.structural |= decision.action.is_structural();
                result.requires_fresh_model_step |= decision.action.is_structural();
                result.summaries.push(receipt.summary.clone());
                result.receipts.push(receipt);
            }
            Err(error) => {
                receipt.state = InputApplicationState::Failed;
                receipt.summary = "input disposition materialization failed".to_string();
                receipt.error = Some(error.clone());
                receipt.revision = receipt.revision.saturating_add(1);
                receipt.updated_at_ms = now_ms();
                let _ = commit_receipt(application.as_ref(), &current_records, &receipt).await?;
                return Err(error);
            }
        }
    }
    Ok(result)
}

async fn commit_receipt(
    application: &dyn crate::SessionRuntimeApplicationPort,
    records: &[crate::RuntimeSessionInputRecord],
    receipt: &SessionInputApplicationReceipt,
) -> Result<Vec<crate::RuntimeSessionInputRecord>, String> {
    application
        .commit_input_application_receipt(
            &receipt.input_ids,
            &records
                .iter()
                .map(|record| record.revision)
                .collect::<Vec<_>>(),
            receipt,
            now_ms(),
        )
        .await
        .map_err(|error| error.to_string())
}

fn coherent_existing_receipt(
    records: &[crate::RuntimeSessionInputRecord],
    disposition_id: &str,
    input_ids: &[String],
) -> Result<Option<SessionInputApplicationReceipt>, String> {
    let existing = records
        .iter()
        .filter_map(|record| record.application_receipt.as_ref())
        .collect::<Vec<_>>();
    if existing.is_empty() {
        return Ok(None);
    }
    if existing.len() != records.len()
        || existing.iter().any(|receipt| {
            receipt.disposition_id != disposition_id
                || receipt.input_ids != input_ids
                || *receipt != existing[0]
        })
    {
        return Err("durable input application receipt group is inconsistent".to_string());
    }
    Ok(Some(existing[0].clone()))
}

async fn materialize_decision(
    services: &RuntimeServices,
    binding: &InputDispositionRuntimeBinding,
    decision: &ModelInputDispositionDecision,
    disposition_id: &str,
    leader_input_id: &str,
    input_ids: &[String],
) -> Result<MaterializationEvidence, String> {
    let mut evidence = MaterializationEvidence::default();
    match decision.action {
        InputDispositionAction::AmendCurrentTurn | InputDispositionAction::ReplanCurrentGraph => {
            revise_goal_once(services, binding, decision, disposition_id)?;
            evidence.summary = if decision.action == InputDispositionAction::AmendCurrentTurn {
                "current Turn Goal amended from running input".to_string()
            } else {
                "current execution accepted a fresh graph replan".to_string()
            };
        }
        InputDispositionAction::ProgressOrControl => {
            evidence.summary = "progress/control input applied to the active Turn".to_string();
        }
        InputDispositionAction::Clarify => {
            evidence.summary = "input requires clarification before structural work".to_string();
        }
        InputDispositionAction::ReplaceCurrentTask => {
            let task_port = services.task_runtime_port();
            if let Some(task) = task_port.get(&binding.lineage.task_id)? {
                if !task.status.is_terminal() {
                    task_port.transition(
                        &task.task_id,
                        task.revision,
                        TaskStatus::Cancelled,
                        input_ids
                            .iter()
                            .map(|input_id| {
                                EvidenceRef::observed("session_input", input_id.clone())
                            })
                            .collect(),
                        "running input replaced the current Task",
                    )?;
                }
                evidence.task_ids.push(task.task_id);
            }
            evidence.execution_ids.push(binding.execution_id.clone());
            evidence.summary =
                "current Task cancelled; input reclassified as the successor Turn".to_string();
        }
        InputDispositionAction::AddRequiredTask
        | InputDispositionAction::AddBackgroundTask
        | InputDispositionAction::AddTaskWithTeam => {
            let task = materialize_additional_session_task(
                services,
                disposition_id,
                leader_input_id,
                &binding.session_id,
                &binding.turn_id,
                &decision.objective,
                &binding.mission_id,
                None,
                TaskOrigin::User,
            )?;
            evidence.task_ids.push(task.primary_task.task_id.clone());
            let lineage = ExecutionGraphLineage {
                session_id: binding.session_id.clone(),
                turn_id: binding.turn_id.clone(),
                root_task_id: task.root_task.task_id.clone(),
                task_id: task.primary_task.task_id,
                generation: binding.lineage.generation,
            };
            let graph = materialize_graph(
                services,
                binding,
                decision,
                disposition_id,
                input_ids,
                lineage,
                None,
            )
            .await?;
            merge_graph_evidence(&mut evidence, graph);
            evidence.summary = if disposition_runs_in_background(decision) {
                "background Task and execution graph durably admitted".to_string()
            } else {
                "additional Task and required execution graph materialized".to_string()
            };
        }
        InputDispositionAction::AddTeamLane | InputDispositionAction::DispatchSession => {
            let session_target = if decision.action == InputDispositionAction::DispatchSession {
                let target = decision.session_target.as_ref().ok_or_else(|| {
                    "dispatch_session lost its validated Session target".to_string()
                })?;
                let application = services.session_application_port().ok_or_else(|| {
                    "Session Runtime application port is not installed".to_string()
                })?;
                Some(
                    application
                        .resolve_input_disposition_session_target(
                            &crate::RuntimeSessionTargetRequest {
                                source_session_id: binding.session_id.clone(),
                                disposition_id: disposition_id.to_string(),
                                mode: target.mode,
                                target_ref: target.target_ref.clone(),
                                objective: decision.objective.clone(),
                            },
                        )
                        .await
                        .map_err(|error| error.to_string())?,
                )
            } else {
                None
            };
            let graph = materialize_graph(
                services,
                binding,
                decision,
                disposition_id,
                input_ids,
                binding.lineage.clone(),
                session_target.as_ref(),
            )
            .await?;
            merge_graph_evidence(&mut evidence, graph);
            if decision.action == InputDispositionAction::DispatchSession {
                let target = session_target.ok_or_else(|| {
                    "dispatch_session completed without a resolved Session target".to_string()
                })?;
                evidence.target_session_id = Some(target.target_session_id);
                evidence.target_session_created = target.created;
                evidence.summary = if target.created && disposition_runs_in_background(decision) {
                    "new isolated Session handoff graph durably admitted".to_string()
                } else if target.created {
                    "new isolated Session handoff graph completed".to_string()
                } else if disposition_runs_in_background(decision) {
                    "authorized Session handoff graph durably admitted".to_string()
                } else {
                    "authorized Session handoff graph completed".to_string()
                };
            } else {
                evidence.summary = if disposition_runs_in_background(decision) {
                    "background Team lane durably admitted under the current Task".to_string()
                } else {
                    "Team lane completed under the current Task".to_string()
                };
            }
        }
    }
    Ok(evidence)
}

fn revise_goal_once(
    services: &RuntimeServices,
    binding: &InputDispositionRuntimeBinding,
    decision: &ModelInputDispositionDecision,
    disposition_id: &str,
) -> Result<(), String> {
    let goal = services
        .goal_store()
        .get(&binding.goal_id)?
        .ok_or_else(|| format!("Goal `{}` disappeared", binding.goal_id))?;
    let marker = format!("input_disposition:{disposition_id}");
    if goal
        .constraints
        .iter()
        .any(|constraint| constraint == &marker)
    {
        return Ok(());
    }
    services.goal_store().revise(
        &binding.goal_id,
        goal.revision,
        goal.user_sequence.saturating_add(1),
        "typed running-Turn input disposition revised the current Goal",
        |goal| {
            goal.objective.push_str(
                if decision.action == InputDispositionAction::AmendCurrentTurn {
                    "\n\nCurrent Turn amendment:\n"
                } else {
                    "\n\nLatest user correction requiring graph replan:\n"
                },
            );
            goal.objective.push_str(decision.objective.trim());
            goal.constraints.push(marker.clone());
            vec![
                "objective".to_string(),
                "constraints".to_string(),
                "user_sequence".to_string(),
            ]
        },
    )?;
    Ok(())
}

async fn materialize_graph(
    services: &RuntimeServices,
    binding: &InputDispositionRuntimeBinding,
    decision: &ModelInputDispositionDecision,
    disposition_id: &str,
    input_ids: &[String],
    lineage: ExecutionGraphLineage,
    session_target: Option<&crate::RuntimeSessionTargetResolution>,
) -> Result<crate::RuntimeOrchestrationResult, String> {
    let proposal = bind_graph_proposal(decision, disposition_id, session_target)?;
    let command = RuntimeOrchestrationCommand::from_model(
        ModelRuntimeOrchestrationInput {
            intent: decision.objective.clone(),
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(proposal),
            template_proposal: None,
            control: None,
            input_disposition: None,
            evidence_refs: input_ids
                .iter()
                .map(|input_id| format!("session_input:{input_id}"))
                .collect(),
            constraints: binding.constraints.clone(),
        },
        RuntimeOrchestrationBinding {
            model_lease: binding.model_lease.clone(),
            session_id: Some(binding.session_id.clone()),
            lineage: Some(lineage),
            mission_id: Some(binding.mission_id.clone()),
            selection_mode: None,
            strategy_binding: None,
            capabilities: binding.capabilities.clone(),
            surface: Some("conversation_runtime_input_disposition".to_string()),
            permission_ceiling: binding.permission_ceiling,
        },
    );
    let parent = Some(ExecutionParentBinding {
        execution_id: binding.execution_id.clone(),
        node_id: binding.execution_node_id.clone(),
    });
    let background = disposition_runs_in_background(decision);
    let result = if background {
        super::admit_runtime_orchestration_request_background(command, None, services, parent).await
    } else {
        super::submit_runtime_orchestration_request(command, None, services, parent).await
    };
    let expected = if background { "admitted" } else { "completed" };
    if result.status != expected {
        return Err(format!(
            "input disposition graph {}: {}",
            result.status,
            result.decision.validation_findings.join(", ")
        ));
    }
    Ok(result)
}

fn bind_graph_proposal(
    decision: &ModelInputDispositionDecision,
    disposition_id: &str,
    session_target: Option<&crate::RuntimeSessionTargetResolution>,
) -> Result<ModelGraphMutationProposal, String> {
    decision
        .graph_plan
        .clone()
        .map(|plan| {
            let nodes = plan
                .nodes
                .into_iter()
                .map(ModelGraphSemanticNode::from)
                .map(|mut node| {
                    if node.recipe == CapabilityRecipeId::SessionDispatch {
                        node.target_session_id = Some(
                            session_target
                                .ok_or_else(|| {
                                    "Session dispatch graph requires a resolved physical target"
                                        .to_string()
                                })?
                                .target_session_id
                                .clone(),
                        );
                    }
                    Ok(node)
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(ModelGraphMutationProposal {
                mutation_id: disposition_id.to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes,
                completion: plan.completion,
                reason: plan.reason,
            })
        })
        .unwrap_or_else(|| {
            Ok(ModelGraphMutationProposal {
                mutation_id: disposition_id.to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![ModelGraphSemanticNode {
                    node_id: "execute".to_string(),
                    recipe: CapabilityRecipeId::Agent,
                    objective: decision.objective.clone(),
                    depends_on: Vec::new(),
                    multiplicity: 1,
                    focuses: Vec::new(),
                    managed_agent_escalation:
                        harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
                    template: None,
                    target_session_id: None,
                    output_artifacts: vec!["task_result".to_string()],
                    evidence_contract: vec!["verified outcome".to_string()],
                    required_evidence_refs: Vec::new(),
                    required: decision.required,
                    dependency: ExecutionDependencyPolicy::All,
                    cancellation_group: None,
                }],
                completion: ExecutionCompletionContract::default(),
                reason: decision.reason.clone(),
            })
        })
}

fn merge_graph_evidence(
    target: &mut MaterializationEvidence,
    result: crate::RuntimeOrchestrationResult,
) {
    if let Some(graph_id) = result
        .evidence
        .get("graph_id")
        .and_then(serde_json::Value::as_str)
    {
        target.execution_ids.push(graph_id.to_string());
    }
    extend_string_array(&mut target.team_ids, result.evidence.get("team_ids"));
    extend_string_array(&mut target.agent_ids, result.evidence.get("agent_ids"));
}

fn extend_string_array(target: &mut Vec<String>, value: Option<&serde_json::Value>) {
    target.extend(
        value
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(str::to_string),
    );
    target.sort();
    target.dedup();
}

fn disposition_id(
    binding: &InputDispositionRuntimeBinding,
    decision: &ModelInputDispositionDecision,
    records: &[crate::RuntimeSessionInputRecord],
) -> String {
    let input_ids = records
        .iter()
        .map(|record| record.input_id.as_str())
        .collect::<Vec<_>>();
    disposition_id_from_parts(&binding.session_id, &binding.turn_id, decision, &input_ids)
}

fn disposition_id_from_parts(
    session_id: &str,
    turn_id: &str,
    decision: &ModelInputDispositionDecision,
    input_ids: &[&str],
) -> String {
    let identity = serde_json::to_vec(&(
        session_id,
        turn_id,
        input_ids,
        decision.action,
        decision.relation,
        decision.objective.trim(),
        decision.required,
        decision.confidence_basis_points,
        decision.reason.trim(),
        &decision.graph_plan,
        &decision.session_target,
    ))
    .expect("input disposition identity is always JSON serializable");
    format!(
        "disposition-{:016x}",
        model_protocol::fingerprint::stable_hash_bytes(&identity)
    )
}

fn disposition_runs_in_background(decision: &ModelInputDispositionDecision) -> bool {
    decision.action == InputDispositionAction::AddBackgroundTask || !decision.required
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use harness_contract::{
        execution_graph::{ExecutionCompletionContract, ExecutionDependencyPolicy},
        input_disposition::{
            InputDispositionAction, InputWorkRelation, ModelInputDispositionDecision,
            ModelInputDispositionGraphNode, ModelInputDispositionGraphPlan,
        },
        orchestration::CapabilityRecipeId,
    };

    use super::{bind_graph_proposal, disposition_id_from_parts, disposition_runs_in_background};

    fn decision() -> ModelInputDispositionDecision {
        ModelInputDispositionDecision {
            input_slots: vec![0],
            action: InputDispositionAction::AddTaskWithTeam,
            relation: InputWorkRelation::NewTask,
            objective: "investigate and implement".to_string(),
            required: true,
            confidence_basis_points: 9_000,
            reason: "independent structural work".to_string(),
            graph_plan: Some(ModelInputDispositionGraphPlan {
                nodes: vec![ModelInputDispositionGraphNode {
                    node_id: "team".to_string(),
                    recipe: CapabilityRecipeId::Team,
                    objective: "investigate and implement".to_string(),
                    depends_on: Vec::new(),
                    multiplicity: 1,
                    focuses: Vec::new(),
                    template: None,
                    output_artifacts: vec!["verified_result".to_string()],
                    evidence_contract: vec!["verified evidence".to_string()],
                    required_evidence_refs: Vec::new(),
                    required: true,
                    dependency: ExecutionDependencyPolicy::All,
                    cancellation_group: None,
                }],
                completion: ExecutionCompletionContract::default(),
                reason: "parallel work is useful".to_string(),
            }),
            session_target: None,
        }
    }

    #[test]
    fn disposition_identity_covers_materialization_semantics() {
        let required = decision();
        let required_id = disposition_id_from_parts("session-a", "turn-a", &required, &["input-a"]);

        let mut background = required.clone();
        background.required = false;
        assert_ne!(
            required_id,
            disposition_id_from_parts("session-a", "turn-a", &background, &["input-a"])
        );

        let mut changed_graph = required.clone();
        changed_graph.graph_plan.as_mut().expect("graph plan").nodes[0].objective =
            "different materialized work".to_string();
        assert_ne!(
            required_id,
            disposition_id_from_parts("session-a", "turn-a", &changed_graph, &["input-a"])
        );

        let mut changed_target = required.clone();
        changed_target.action = InputDispositionAction::DispatchSession;
        changed_target.relation = InputWorkRelation::CrossSession;
        changed_target.graph_plan = None;
        changed_target.session_target = Some(
            harness_contract::input_disposition::ModelInputDispositionSessionTarget {
                mode: harness_contract::input_disposition::InputDispositionSessionTargetMode::ExistingAuthorized,
                target_ref: Some("session-b".to_string()),
            },
        );
        let target_id =
            disposition_id_from_parts("session-a", "turn-a", &changed_target, &["input-a"]);
        changed_target
            .session_target
            .as_mut()
            .expect("target")
            .target_ref = Some("session-c".to_string());
        assert_ne!(
            target_id,
            disposition_id_from_parts("session-a", "turn-a", &changed_target, &["input-a"])
        );
    }

    #[test]
    fn optional_structural_work_is_admitted_in_the_background() {
        let mut optional = decision();
        optional.required = false;
        assert!(disposition_runs_in_background(&optional));

        optional.required = true;
        assert!(!disposition_runs_in_background(&optional));
    }

    #[test]
    fn session_target_is_bound_only_after_gateway_resolution() {
        let mut dispatch = decision();
        dispatch.action = InputDispositionAction::DispatchSession;
        dispatch.relation = InputWorkRelation::CrossSession;
        dispatch.graph_plan.as_mut().expect("graph").nodes[0].recipe =
            CapabilityRecipeId::SessionDispatch;
        dispatch.session_target = Some(
            harness_contract::input_disposition::ModelInputDispositionSessionTarget {
                mode: harness_contract::input_disposition::InputDispositionSessionTargetMode::ExistingAuthorized,
                target_ref: Some("session-visible".to_string()),
            },
        );
        assert!(bind_graph_proposal(&dispatch, "disposition", None)
            .expect_err("physical resolution is mandatory")
            .contains("resolved physical target"));

        let proposal = bind_graph_proposal(
            &dispatch,
            "disposition",
            Some(&crate::RuntimeSessionTargetResolution {
                target_session_id: "session-physical".to_string(),
                created: false,
            }),
        )
        .expect("bind resolved target");
        assert_eq!(
            proposal.nodes[0].target_session_id.as_deref(),
            Some("session-physical")
        );
        assert_ne!(
            proposal.nodes[0].target_session_id.as_deref(),
            dispatch
                .session_target
                .as_ref()
                .and_then(|target| target.target_ref.as_deref())
        );
    }
}
