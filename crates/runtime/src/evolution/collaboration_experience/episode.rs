use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::agent::AgentTaskPacket;
use harness_contract::evolution::*;
use harness_contract::goal::ObjectiveTerminalKind;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{AgenticProgramProjection, AgenticTaskProjection, AgenticTaskStatus};

pub(super) fn from_verdict(
    store: &Arc<crate::RuntimeEventStore>,
    graphs: &crate::ExecutionGraphStateStore,
    workspace_key: &str,
    event: &crate::DurableRuntimeEvent,
) -> Result<CollaborationExperienceEpisode, String> {
    let program_id = event.payload["program_id"]
        .as_str()
        .ok_or("verdict missing Program id")?;
    let program = crate::AgentActionService::new(Arc::clone(store))
        .project(program_id)
        .map_err(|error| error.to_string())?;
    let verdict = program
        .objective_verdict
        .as_ref()
        .ok_or("Program lacks Objective verdict")?;
    if event.stream_id != format!("agentic-program:{program_id}")
        || event.sequence != program.revision
        || event.payload["verdict"] != serde_json::to_value(verdict).map_err(|e| e.to_string())?
    {
        return Err("experience source does not match immutable terminal Program".into());
    }
    let goal = crate::execution_core::goal::GoalStore::new(Arc::clone(store))
        .get(&verdict.goal_id)?
        .ok_or("experience Objective missing")?;
    let terminal = goal
        .terminal
        .as_ref()
        .ok_or("experience Objective is not terminal")?;
    if terminal.kind != verdict.kind
        || terminal.terminal_fence != verdict.terminal_fence
        || goal.session_id != program.session_id
        || goal.execution_binding.as_ref().is_none_or(|binding| {
            binding.objective_id != program.objective_id
                || binding.session_id != program.session_id
                || binding.turn_id != program.turn_id
                || program.root_execution_id.as_deref() != Some(&binding.root_execution_id)
                || binding.agentic_program_id != program.program_id
        })
        || terminal.authority_revision != verdict.authority_revision
    {
        return Err("experience Objective verdict mismatch".into());
    }
    let history = store.list_stream(&event.stream_id)?;
    let opened_at = history
        .iter()
        .find(|e| e.kind == "agentic.program_opened")
        .map(|e| e.created_at_ms)
        .ok_or("Program genesis missing")?;
    let mut packets = BTreeMap::new();
    for task in program
        .tasks
        .values()
        .filter(|task| !task.status.is_retired())
    {
        if let Some(graph_id) = task.claim_execution_id.as_ref() {
            let graph = graphs.load(graph_id).map_err(|error| error.to_string())?;
            let packet = graph
                .nodes
                .iter()
                .filter_map(|node| {
                    serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                        .ok()
                        .filter(|packet| packet.assignment.node_id == node.id)
                })
                .find(|packet| packet.assignment.task_id == task.task_id)
                .ok_or_else(|| format!("experience lacks compiled packet for {}", task.task_id))?;
            validate_packet(&program, task, graph_id, &packet)?;
            packets.insert(task.task_id.clone(), packet);
        } else if verdict.kind == ObjectiveTerminalKind::Satisfied {
            return Err(format!(
                "verified task has no physical claim: {}",
                task.task_id
            ));
        }
    }
    build(
        &program,
        &packets,
        workspace_key,
        opened_at,
        event.created_at_ms,
    )
}

fn validate_packet(
    program: &AgenticProgramProjection,
    task: &AgenticTaskProjection,
    graph_id: &str,
    packet: &AgentTaskPacket,
) -> Result<(), String> {
    let member = task
        .claimant
        .as_ref()
        .ok_or("experience claimant missing")?;
    let agentic = packet
        .agentic_binding
        .as_ref()
        .ok_or("experience packet has no typed Agentic binding")?;
    if packet.assignment.graph_id != graph_id
        || packet.assignment.session_id != program.session_id
        || packet.assignment.task_id != task.task_id
        || agentic.program_id != program.program_id
        || agentic.agent_id != *member
        || agentic.task_team_id != task.team_id
        || !matches!(
            &agentic.focus,
            harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
                if task_ref == &task.task_id
        )
    {
        return Err("experience physical packet binding mismatch".into());
    }
    let binding = packet
        .binding
        .as_ref()
        .ok_or("experience requires compiled binding")?;
    binding.validate().map_err(|error| error.to_string())?;
    packet
        .assignment
        .validate()
        .map_err(|error| error.to_string())?;
    if binding.definition_ref != packet.assignment.definition_ref
        || binding.data_lease.session_id != program.session_id
        || binding.data_lease.task_id != task.task_id
    {
        return Err("experience binding identity mismatch".into());
    }
    packet.validate_cohort_prompt_package()?;
    Ok(())
}

fn build(
    program: &AgenticProgramProjection,
    packets: &BTreeMap<String, AgentTaskPacket>,
    workspace_key: &str,
    opened_at: u64,
    completed_at: u64,
) -> Result<CollaborationExperienceEpisode, String> {
    let verdict = program
        .objective_verdict
        .as_ref()
        .ok_or("terminal verdict missing")?;
    let active = program
        .tasks
        .values()
        .filter(|task| !task.status.is_retired())
        .collect::<Vec<_>>();
    let satisfied = active
        .iter()
        .filter(|task| task.status == AgenticTaskStatus::Accepted)
        .count();
    let outcome = match verdict.kind {
        ObjectiveTerminalKind::Satisfied => CollaborationExperienceOutcome::Completed,
        ObjectiveTerminalKind::PartiallySatisfied => CollaborationExperienceOutcome::Partial,
        ObjectiveTerminalKind::Cancelled => CollaborationExperienceOutcome::Cancelled,
        ObjectiveTerminalKind::Blocked => CollaborationExperienceOutcome::IntentGap,
        ObjectiveTerminalKind::Failed => CollaborationExperienceOutcome::Failed,
    };
    // Name-free structural cohorts. Identical capability shapes are grouped,
    // so randomly generated task/team IDs cannot destroy cross-run reuse.
    let mut groups = BTreeMap::<String, (SemanticWorkstreamShape, Vec<String>)>::new();
    for (task_id, packet) in packets {
        let binding = packet.binding.as_ref().ok_or("missing bound packet")?;
        let mut shape = SemanticWorkstreamShape {
            ordinal: 0,
            multiplicity_min: 1,
            multiplicity_max: 1,
            required_capability_ids: binding
                .effective_capabilities
                .iter()
                .map(|cap| cap.as_str().into())
                .collect(),
            required_skill_ids: packet.allowed_skills.clone(),
            required_tool_capabilities: packet.allowed_tools.clone(),
            acceptance_kinds: vec!["independent_task_review".into()],
            result_field_shapes: vec!["artifact_refs".into(), "evidence_refs".into()],
        };
        for values in [
            &mut shape.required_capability_ids,
            &mut shape.required_skill_ids,
            &mut shape.required_tool_capabilities,
        ] {
            values.sort();
            values.dedup();
        }
        let key = digest(&shape)?;
        groups
            .entry(key)
            .or_insert_with(|| (shape, Vec::new()))
            .1
            .push(task_id.clone());
    }
    let mut task_ordinals = BTreeMap::new();
    let mut shapes = Vec::new();
    for (ordinal, (_, (mut shape, ids))) in groups.into_iter().enumerate() {
        shape.ordinal = u16::try_from(ordinal).map_err(|_| "too many experience shapes")?;
        shape.multiplicity_min =
            u16::try_from(ids.len()).map_err(|_| "too many experience tasks")?;
        shape.multiplicity_max = shape.multiplicity_min;
        for id in ids {
            task_ordinals.insert(id, shape.ordinal);
        }
        shapes.push(shape);
    }
    let mut dependencies = Vec::new();
    for task in &active {
        let Some(consumer) = task_ordinals.get(&task.task_id) else {
            continue;
        };
        for dependency in resolved_dependencies(program, &task.depends_on)? {
            let Some(producer) = task_ordinals.get(&dependency) else {
                continue;
            };
            dependencies.push(SemanticDependencyShape {
                producer_ordinal: *producer,
                consumer_ordinal: *consumer,
                required_artifact_kinds: vec!["committed_artifact".into()],
                required_fact_kinds: vec!["accepted_task".into()],
                requires_committed_effect: false,
                requires_satisfied_acceptance: true,
            });
        }
    }
    let signature = CollaborationSemanticSignature {
        normalizer_revision: COLLABORATION_SIGNATURE_NORMALIZER_REVISION,
        required_capability_ids: shapes
            .iter()
            .flat_map(|s| s.required_capability_ids.clone())
            .collect(),
        required_skill_ids: shapes
            .iter()
            .flat_map(|s| s.required_skill_ids.clone())
            .collect(),
        required_tool_capabilities: shapes
            .iter()
            .flat_map(|s| s.required_tool_capabilities.clone())
            .collect(),
        acceptance_kinds: vec!["independent_task_review".into()],
        result_field_shapes: vec!["artifact_refs".into(), "evidence_refs".into()],
        workstream_shapes: shapes,
        dependency_shapes: dependencies,
    }
    .normalized();
    let evidence = active
        .iter()
        .flat_map(|task| task.evidence_refs.iter().chain(&task.artifact_refs))
        .map(|reference| digest(&(workspace_key, "evidence", reference)))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let reusable = !active.is_empty()
        && satisfied == active.len()
        && packets.len() == active.len()
        && active.iter().all(|task| {
            task.claimant.is_some()
                && task.reviewed_by.is_some()
                && task.claimant != task.reviewed_by
                && !task.evidence_refs.is_empty()
                && !task.artifact_refs.is_empty()
        });
    let episode = CollaborationExperienceEpisode {
        schema_version: COLLABORATION_EXPERIENCE_SCHEMA_VERSION,
        episode_id: CollaborationExperienceEpisode::deterministic_id(
            &program.program_id,
            program.revision,
        ),
        session_ref_hash: digest(&(workspace_key, "session", &program.session_id))?,
        turn_ref_hash: digest(&(workspace_key, "turn", &program.session_id, &program.turn_id))?,
        program_id: program.program_id.clone(),
        program_revision: program.revision,
        intent_digest: digest(&(&program.objective_summary, program.required_team_count))?,
        binding_digest: digest(&packets.values().map(|p| &p.binding).collect::<Vec<_>>())?,
        capacity_profile_digest: digest(
            &packets
                .values()
                .map(|p| &p.budget_lease)
                .collect::<Vec<_>>(),
        )?,
        approval_policy_digest: digest(&(
            program.permission_ceiling,
            &program.resource_scopes,
            packets
                .values()
                .map(|p| (p.permission_ceiling, p.policy_revision))
                .collect::<Vec<_>>(),
        ))?,
        semantic_signature: signature,
        outcome,
        evidence_refs: evidence
            .into_iter()
            .take(MAX_COLLABORATION_EPISODE_EVIDENCE_REFS)
            .collect(),
        coverage: CollaborationEvidenceCoverage {
            required_obligation_count: u32::try_from(active.len())
                .map_err(|_| "too many obligations")?,
            satisfied_obligation_count: u32::try_from(satisfied)
                .map_err(|_| "too many satisfied obligations")?,
            coverage_basis_points: if active.is_empty() {
                0
            } else {
                ((satisfied as u128 * 10_000) / active.len() as u128) as u16
            },
            reusable,
        },
        latency_ms: completed_at.saturating_sub(opened_at),
        resource_summary: CollaborationResourceSummary {
            parallel_demand: u16::try_from(active.len()).unwrap_or(u16::MAX),
            context_reservation_tokens: None,
            output_reservation_tokens: None,
        },
        completed_at_ms: completed_at,
    };
    if serde_json::to_vec(&episode)
        .map_err(|e| e.to_string())?
        .len()
        > MAX_COLLABORATION_EPISODE_PAYLOAD_BYTES
    {
        return Err(
            "experience projection exceeds evidence payload envelope; source retained".into(),
        );
    }
    Ok(episode)
}

fn resolved_dependencies(
    program: &AgenticProgramProjection,
    roots: &[String],
) -> Result<BTreeSet<String>, String> {
    let mut pending = roots.to_vec();
    let mut visited = BTreeSet::new();
    let mut resolved = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id.clone()) {
            continue;
        }
        let task = program
            .tasks
            .get(&id)
            .ok_or("experience dependency missing")?;
        if task.status == AgenticTaskStatus::Superseded {
            pending.extend(task.replacement_task_refs.clone());
        } else {
            resolved.insert(id);
        }
    }
    Ok(resolved)
}

fn digest(value: &impl Serialize) -> Result<String, String> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(|e| e.to_string())?)
    ))
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
