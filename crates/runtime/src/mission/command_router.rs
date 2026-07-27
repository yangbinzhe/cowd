//! Durable Mission command saga boundary.
//!
//! Gateway owns cross-domain orchestration. Runtime owns the durable saga
//! record and the effects of Runtime aggregates. Session, approval and Team
//! effects are deliberately completed through their typed Gateway/Runtime
//! ports and then acknowledged here.

use harness_contract::agent::{AgentCommand, AgentCommandRequest, AgentInput};
use harness_contract::mission::{
    MissionCommand, MissionCommandAction, MissionCommandReceipt, MissionCommandSagaPhase,
    MissionCommandSagaRecord, MissionCommandTarget, MissionStatus,
};
use harness_contract::reality::EvidenceRef;

use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeServices, SessionRelationKind,
};

const SAGA_SCHEMA_VERSION: u32 = 1;

pub fn reserve_mission_command(
    services: &RuntimeServices,
    command: MissionCommand,
) -> Result<MissionCommandSagaRecord, String> {
    if command.command_id.trim().is_empty() {
        return Err("command_id is required".to_string());
    }
    let stream_id = saga_stream_id(services, &command.command_id);
    if let Some(existing) = load_saga(services, &stream_id)? {
        if existing.command != command {
            return Err(format!(
                "mission command {} is already reserved with another request",
                command.command_id
            ));
        }
        return Ok(existing);
    }
    let target_revision = target_revision(services, &command.target, command.action)?;
    if let Some(expected_revision) = command.expected_revision {
        if expected_revision != target_revision {
            return Err(format!(
                "stale target revision: expected {expected_revision}, actual {target_revision}"
            ));
        }
    }
    let record = MissionCommandSagaRecord {
        schema_version: SAGA_SCHEMA_VERSION,
        command,
        phase: MissionCommandSagaPhase::Reserved,
        revision: 1,
        reserved_target_revision: target_revision,
        effect_result: None,
        receipt: None,
        error: None,
        updated_at_ms: now_ms(),
    };
    append_saga_phase(services, &stream_id, 0, &record)?;
    Ok(record)
}

pub async fn execute_reserved_runtime_effect(
    services: &RuntimeServices,
    command_id: &str,
) -> Result<MissionCommandSagaRecord, String> {
    let stream_id = saga_stream_id(services, command_id);
    let current = load_saga(services, &stream_id)?
        .ok_or_else(|| format!("mission command {command_id} is not reserved"))?;
    if matches!(
        current.phase,
        MissionCommandSagaPhase::EffectCommitted
            | MissionCommandSagaPhase::ReceiptCommitted
            | MissionCommandSagaPhase::Finalized
    ) {
        return Ok(current);
    }
    if current.phase != MissionCommandSagaPhase::Reserved {
        return Err(format!(
            "mission command {command_id} cannot execute from {:?}",
            current.phase
        ));
    }
    let (result, evidence_refs) = execute_runtime_effect(services, &current.command).await?;
    commit_mission_effect(services, command_id, result, evidence_refs)
}

pub fn commit_mission_effect(
    services: &RuntimeServices,
    command_id: &str,
    result: serde_json::Value,
    evidence_refs: Vec<EvidenceRef>,
) -> Result<MissionCommandSagaRecord, String> {
    let stream_id = saga_stream_id(services, command_id);
    let current = load_saga(services, &stream_id)?
        .ok_or_else(|| format!("mission command {command_id} is not reserved"))?;
    if matches!(
        current.phase,
        MissionCommandSagaPhase::EffectCommitted
            | MissionCommandSagaPhase::ReceiptCommitted
            | MissionCommandSagaPhase::Finalized
    ) {
        return Ok(current);
    }
    if current.phase != MissionCommandSagaPhase::Reserved {
        return Err(format!(
            "mission command {command_id} cannot commit an effect from {:?}",
            current.phase
        ));
    }
    let mut command = current.command.clone();
    command.evidence_refs = merge_evidence(command.evidence_refs, evidence_refs);
    let next = MissionCommandSagaRecord {
        schema_version: SAGA_SCHEMA_VERSION,
        command,
        phase: MissionCommandSagaPhase::EffectCommitted,
        revision: current.revision.saturating_add(1),
        reserved_target_revision: current.reserved_target_revision,
        effect_result: Some(result),
        receipt: None,
        error: None,
        updated_at_ms: now_ms(),
    };
    append_saga_phase(services, &stream_id, current.revision, &next)?;
    Ok(next)
}

pub fn commit_mission_receipt(
    services: &RuntimeServices,
    command_id: &str,
) -> Result<MissionCommandSagaRecord, String> {
    let stream_id = saga_stream_id(services, command_id);
    let current = load_saga(services, &stream_id)?
        .ok_or_else(|| format!("mission command {command_id} is not reserved"))?;
    if matches!(
        current.phase,
        MissionCommandSagaPhase::ReceiptCommitted | MissionCommandSagaPhase::Finalized
    ) {
        return Ok(current);
    }
    if current.phase != MissionCommandSagaPhase::EffectCommitted {
        return Err(format!(
            "mission command {command_id} cannot commit a receipt from {:?}",
            current.phase
        ));
    }
    let receipt = MissionCommandReceipt {
        command_id: current.command.command_id.clone(),
        action: current.command.action,
        target: current.command.target.clone(),
        accepted_revision: current.reserved_target_revision,
        status: "accepted".to_string(),
        reason: None,
        evidence_refs: current.command.evidence_refs.clone(),
        result: current.effect_result.clone().unwrap_or_default(),
    };
    let next = MissionCommandSagaRecord {
        phase: MissionCommandSagaPhase::ReceiptCommitted,
        revision: current.revision.saturating_add(1),
        receipt: Some(receipt),
        updated_at_ms: now_ms(),
        ..current.clone()
    };
    append_saga_phase(services, &stream_id, current.revision, &next)?;
    Ok(next)
}

pub fn finalize_mission_command(
    services: &RuntimeServices,
    command_id: &str,
) -> Result<MissionCommandSagaRecord, String> {
    let stream_id = saga_stream_id(services, command_id);
    let current = load_saga(services, &stream_id)?
        .ok_or_else(|| format!("mission command {command_id} is not reserved"))?;
    if current.phase == MissionCommandSagaPhase::Finalized {
        return Ok(current);
    }
    if current.phase != MissionCommandSagaPhase::ReceiptCommitted {
        return Err(format!(
            "mission command {command_id} cannot finalize from {:?}",
            current.phase
        ));
    }
    let next = MissionCommandSagaRecord {
        phase: MissionCommandSagaPhase::Finalized,
        revision: current.revision.saturating_add(1),
        updated_at_ms: now_ms(),
        ..current.clone()
    };
    append_saga_phase(services, &stream_id, current.revision, &next)?;
    Ok(next)
}

pub fn reject_mission_command(
    services: &RuntimeServices,
    command_id: &str,
    reason: impl Into<String>,
) -> Result<MissionCommandSagaRecord, String> {
    let stream_id = saga_stream_id(services, command_id);
    let current = load_saga(services, &stream_id)?
        .ok_or_else(|| format!("mission command {command_id} is not reserved"))?;
    if current.phase.is_terminal() {
        return Ok(current);
    }
    let reason = reason.into();
    let receipt = MissionCommandReceipt {
        command_id: current.command.command_id.clone(),
        action: current.command.action,
        target: current.command.target.clone(),
        accepted_revision: current.reserved_target_revision,
        status: "rejected".to_string(),
        reason: Some(reason.clone()),
        evidence_refs: current.command.evidence_refs.clone(),
        result: serde_json::Value::Null,
    };
    let next = MissionCommandSagaRecord {
        phase: MissionCommandSagaPhase::Rejected,
        revision: current.revision.saturating_add(1),
        receipt: Some(receipt),
        error: Some(reason),
        updated_at_ms: now_ms(),
        ..current.clone()
    };
    append_saga_phase(services, &stream_id, current.revision, &next)?;
    Ok(next)
}

pub fn mission_command_saga(
    services: &RuntimeServices,
    command_id: &str,
) -> Result<Option<MissionCommandSagaRecord>, String> {
    load_saga(services, &saga_stream_id(services, command_id))
}

/// Compatibility-free convenience for Runtime-owned command effects.
///
/// Session/Team/approval cross-domain commands intentionally fail here and
/// must use Gateway MissionApplicationService.
pub async fn execute_mission_command(
    services: &RuntimeServices,
    command: MissionCommand,
) -> MissionCommandReceipt {
    let command_id = command.command_id.clone();
    let fallback_action = command.action;
    let fallback_target = command.target.clone();
    let fallback_evidence = command.evidence_refs.clone();
    let result = async {
        let reserved = reserve_mission_command(services, command)?;
        if reserved.phase.is_terminal() {
            return reserved
                .receipt
                .ok_or_else(|| "terminal mission saga has no receipt".to_string());
        }
        execute_reserved_runtime_effect(services, &command_id).await?;
        commit_mission_receipt(services, &command_id)?;
        let final_record = finalize_mission_command(services, &command_id)?;
        final_record
            .receipt
            .ok_or_else(|| "finalized mission saga has no receipt".to_string())
    }
    .await;
    match result {
        Ok(receipt) => receipt,
        Err(error) => {
            if let Ok(record) = reject_mission_command(services, &command_id, error.clone()) {
                if let Some(receipt) = record.receipt {
                    return receipt;
                }
            }
            MissionCommandReceipt {
                command_id,
                action: fallback_action,
                target: fallback_target,
                accepted_revision: 0,
                status: "failed".to_string(),
                reason: Some(error),
                evidence_refs: fallback_evidence,
                result: serde_json::Value::Null,
            }
        }
    }
}

async fn execute_runtime_effect(
    services: &RuntimeServices,
    command: &MissionCommand,
) -> Result<(serde_json::Value, Vec<EvidenceRef>), String> {
    match (&command.target, command.action) {
        (MissionCommandTarget::Mission { mission_id }, MissionCommandAction::Create) => {
            let objective = required_payload_text(&command.payload, "objective")?;
            let mission = services.mission_runtime().create_mission(
                mission_id,
                objective,
                command.evidence_refs.clone(),
            )?;
            Ok((
                serde_json::json!({"mission": mission}),
                command.evidence_refs.clone(),
            ))
        }
        (MissionCommandTarget::Mission { mission_id }, action)
            if matches!(
                action,
                MissionCommandAction::Activate
                    | MissionCommandAction::Pause
                    | MissionCommandAction::Resume
                    | MissionCommandAction::Cancel
                    | MissionCommandAction::Close
            ) =>
        {
            let current = services
                .mission_runtime()
                .aggregate(mission_id)
                .ok_or_else(|| format!("mission not found: {mission_id}"))?;
            let status = match action {
                MissionCommandAction::Activate | MissionCommandAction::Resume => {
                    MissionStatus::Active
                }
                MissionCommandAction::Pause => MissionStatus::Paused,
                MissionCommandAction::Cancel | MissionCommandAction::Close => {
                    MissionStatus::Cancelled
                }
                _ => unreachable!(),
            };
            let receipt = services.mission_runtime().transition(
                mission_id,
                command.expected_revision.unwrap_or(current.revision),
                status,
                command.evidence_refs.clone(),
            )?;
            Ok((
                serde_json::json!({"receipt": receipt}),
                command.evidence_refs.clone(),
            ))
        }
        (MissionCommandTarget::Mission { mission_id }, MissionCommandAction::Link)
        | (MissionCommandTarget::Mission { mission_id }, MissionCommandAction::Unlink) => {
            mutate_mission_link(services, command, mission_id)
        }
        (MissionCommandTarget::Agent { agent_id }, action)
            if matches!(
                action,
                MissionCommandAction::Pause
                    | MissionCommandAction::Resume
                    | MissionCommandAction::Cancel
                    | MissionCommandAction::Input
                    | MissionCommandAction::Continue
                    | MissionCommandAction::Replan
            ) =>
        {
            command_agent(services, command, agent_id).await
        }
        (MissionCommandTarget::Relation { relation_id }, MissionCommandAction::Link) => {
            let relation = parse_relation(command)?;
            let relation = services.session_relations().add_relation_with_id(
                relation_id,
                relation.from_session_id,
                relation.to_session_id,
                relation.kind,
                relation.summary,
                relation.evidence_refs,
            )?;
            let mut evidence_refs = command.evidence_refs.clone();
            evidence_refs.extend(relation.evidence_refs.iter().map(|id| {
                EvidenceRef::new("session_relation", id).with_source("runtime.mission_command")
            }));
            Ok((
                serde_json::json!({ "relation": relation }),
                merge_evidence(Vec::new(), evidence_refs),
            ))
        }
        (MissionCommandTarget::Relation { relation_id }, MissionCommandAction::Unlink) => {
            let relation = services.session_relations().remove_relation(relation_id)?;
            Ok((
                serde_json::json!({ "relation": relation, "removed": true }),
                command.evidence_refs.clone(),
            ))
        }
        (MissionCommandTarget::Session { .. }, _)
        | (MissionCommandTarget::Team { .. }, _)
        | (MissionCommandTarget::Approval { .. }, _) => Err(
            "cross-domain Mission command requires Gateway MissionApplicationService".to_string(),
        ),
        _ => Err(format!(
            "unsupported canonical MissionCommand target/action: {:?} {:?}",
            command.target, command.action
        )),
    }
}

fn mutate_mission_link(
    services: &RuntimeServices,
    command: &MissionCommand,
    mission_id: &str,
) -> Result<(serde_json::Value, Vec<EvidenceRef>), String> {
    let entity_kind = required_payload_text(&command.payload, "entity_kind")?;
    let entity_id = required_payload_text(&command.payload, "entity_id")?;
    let current = services
        .mission_runtime()
        .aggregate(mission_id)
        .ok_or_else(|| format!("mission not found: {mission_id}"))?;
    let revision = command.expected_revision.unwrap_or(current.revision);
    let evidence = command.evidence_refs.clone();
    let receipt = match (command.action, entity_kind) {
        (MissionCommandAction::Link, "session") => services.mission_runtime().link_session(
            mission_id,
            revision,
            entity_id,
            evidence.clone(),
        ),
        (MissionCommandAction::Link, "task") => {
            services
                .mission_runtime()
                .link_task(mission_id, revision, entity_id, evidence.clone())
        }
        (MissionCommandAction::Link, "graph") => {
            services
                .mission_runtime()
                .link_graph(mission_id, revision, entity_id, evidence.clone())
        }
        (MissionCommandAction::Link, "team") | (MissionCommandAction::Link, "team_run") => services
            .mission_runtime()
            .link_team_run(mission_id, revision, entity_id, evidence.clone()),
        (MissionCommandAction::Link, "agent") | (MissionCommandAction::Link, "agent_run") => {
            services.mission_runtime().link_agent_run(
                mission_id,
                revision,
                entity_id,
                evidence.clone(),
            )
        }
        (MissionCommandAction::Unlink, "session") => services.mission_runtime().unlink_session(
            mission_id,
            revision,
            entity_id,
            evidence.clone(),
        ),
        (MissionCommandAction::Unlink, "task") => services.mission_runtime().unlink_task(
            mission_id,
            revision,
            entity_id,
            evidence.clone(),
        ),
        (MissionCommandAction::Unlink, "graph") => services.mission_runtime().unlink_graph(
            mission_id,
            revision,
            entity_id,
            evidence.clone(),
        ),
        (MissionCommandAction::Unlink, "team") | (MissionCommandAction::Unlink, "team_run") => {
            services.mission_runtime().unlink_team_run(
                mission_id,
                revision,
                entity_id,
                evidence.clone(),
            )
        }
        (MissionCommandAction::Unlink, "agent") | (MissionCommandAction::Unlink, "agent_run") => {
            services.mission_runtime().unlink_agent_run(
                mission_id,
                revision,
                entity_id,
                evidence.clone(),
            )
        }
        _ => {
            return Err(format!(
                "unsupported Mission link entity kind: {entity_kind}"
            ))
        }
    }?;
    Ok((serde_json::json!({"receipt": receipt}), evidence))
}

async fn command_agent(
    services: &RuntimeServices,
    command: &MissionCommand,
    agent_id: &str,
) -> Result<(serde_json::Value, Vec<EvidenceRef>), String> {
    let snapshot = services
        .agent_runtime()
        .get(agent_id)
        .ok_or_else(|| format!("agent not found: {agent_id}"))?;
    let agent_command = match command.action {
        MissionCommandAction::Pause => AgentCommand::Pause,
        MissionCommandAction::Resume => AgentCommand::Resume,
        MissionCommandAction::Cancel => AgentCommand::Cancel,
        MissionCommandAction::Input | MissionCommandAction::Continue => AgentCommand::SendInput,
        MissionCommandAction::Replan => AgentCommand::Interrupt,
        _ => return Err("mission action is not valid for an agent target".to_string()),
    };
    let input = matches!(
        command.action,
        MissionCommandAction::Input | MissionCommandAction::Continue
    )
    .then(|| {
        required_payload_text(&command.payload, "content")
            .map(|content| AgentInput::UserSupplement(content.to_string()))
    })
    .transpose()?;
    let receipt = services
        .agent_runtime()
        .command(AgentCommandRequest {
            command_id: command.command_id.clone(),
            agent_id: agent_id.to_string(),
            expected_revision: command.expected_revision.unwrap_or(snapshot.revision),
            command: agent_command,
            input,
        })
        .await;
    if !receipt.accepted {
        return Err(receipt.message);
    }
    Ok((
        serde_json::json!({ "receipt": receipt }),
        command.evidence_refs.clone(),
    ))
}

#[derive(serde::Deserialize)]
struct RelationPayload {
    from_session_id: String,
    to_session_id: String,
    kind: SessionRelationKind,
    summary: String,
    #[serde(default)]
    evidence_refs: Vec<String>,
}

fn parse_relation(command: &MissionCommand) -> Result<RelationPayload, String> {
    serde_json::from_value(command.payload.clone())
        .map_err(|error| format!("link requires relation payload: {error}"))
}

fn target_revision(
    services: &RuntimeServices,
    target: &MissionCommandTarget,
    action: MissionCommandAction,
) -> Result<u64, String> {
    match target {
        MissionCommandTarget::Mission { mission_id } => services
            .mission_runtime()
            .aggregate(mission_id)
            .map_or(Ok(0), |mission| Ok(mission.revision)),
        MissionCommandTarget::Session { .. } => Ok(0),
        MissionCommandTarget::Task { task_id } => services
            .task_aggregate_service()
            .get(task_id)
            .map(|task| task.map_or(0, |task| task.revision))
            .map_err(|error| error.to_string()),
        MissionCommandTarget::Graph { graph_id } => services
            .graph_state_store()
            .projection(graph_id)
            .map(|projection| projection.revision)
            .map_err(|error| error.to_string()),
        MissionCommandTarget::Team { team_id } => {
            let team = services
                .team_runtime()
                .list()?
                .into_iter()
                .find(|team| team.team_id == *team_id);
            match (team, action) {
                (Some(team), _) => Ok(team.graph_revision),
                (None, MissionCommandAction::Create) => Ok(0),
                (None, _) => Err(format!("team not found: {team_id}")),
            }
        }
        MissionCommandTarget::Agent { agent_id } => services
            .agent_runtime()
            .get(agent_id)
            .map(|snapshot| snapshot.revision)
            .ok_or_else(|| format!("agent not found: {agent_id}")),
        MissionCommandTarget::Approval { approval_id } => services
            .event_store()
            .stream_revision(&format!("approval:{approval_id}"))
            .map_err(|error| error.to_string()),
        MissionCommandTarget::Relation { .. } => services.session_relations().revision(),
    }
}

fn append_saga_phase(
    services: &RuntimeServices,
    stream_id: &str,
    expected_revision: u64,
    record: &MissionCommandSagaRecord,
) -> Result<(), String> {
    services
        .event_store()
        .append_batch_if_revision(
            stream_id.to_string(),
            expected_revision,
            format!(
                "mission-command-saga:{}:{}",
                record.command.command_id, record.revision
            ),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.to_string(),
                    scope: RuntimeEventScope::Mission,
                    kind: format!("mission.command.saga.{}.v1", saga_phase_name(record.phase)),
                    status: Some(format!("{:?}", record.phase).to_lowercase()),
                    actor: Some(non_empty_actor(&record.command.actor)),
                    refs: record
                        .command
                        .evidence_refs
                        .iter()
                        .map(|reference| RuntimeEventRef {
                            kind: "evidence".to_string(),
                            id: reference.id.clone(),
                        })
                        .collect(),
                    payload: serde_json::to_value(record).map_err(|error| error.to_string())?,
                },
                idempotency_key: Some(format!("{}:{}", record.command.command_id, record.revision)),
                schema_version: SAGA_SCHEMA_VERSION,
            }],
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn saga_phase_name(phase: MissionCommandSagaPhase) -> &'static str {
    match phase {
        MissionCommandSagaPhase::Reserved => "reserved",
        MissionCommandSagaPhase::EffectCommitted => "effect_committed",
        MissionCommandSagaPhase::ReceiptCommitted => "receipt_committed",
        MissionCommandSagaPhase::Finalized => "finalized",
        MissionCommandSagaPhase::Rejected => "rejected",
        MissionCommandSagaPhase::ReconciliationRequired => "reconciliation_required",
    }
}

fn load_saga(
    services: &RuntimeServices,
    stream_id: &str,
) -> Result<Option<MissionCommandSagaRecord>, String> {
    services
        .event_store()
        .list_stream(stream_id)?
        .into_iter()
        .rev()
        .find(|event| event.kind.starts_with("mission.command.saga."))
        .map(|event| serde_json::from_value(event.payload).map_err(|error| error.to_string()))
        .transpose()
}

fn saga_stream_id(services: &RuntimeServices, command_id: &str) -> String {
    format!(
        "mission-command-saga:{}:{command_id}",
        services.workspace_key()
    )
}

fn required_payload_text<'a>(
    payload: &'a serde_json::Value,
    field: &str,
) -> Result<&'a str, String> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("payload.{field} must be a non-empty string"))
}

fn merge_evidence(mut left: Vec<EvidenceRef>, right: Vec<EvidenceRef>) -> Vec<EvidenceRef> {
    left.extend(right);
    left.sort_by(|a, b| (&a.ref_type, &a.id).cmp(&(&b.ref_type, &b.id)));
    left.dedup_by(|a, b| a.ref_type == b.ref_type && a.id == b.id);
    left
}

fn non_empty_actor(actor: &str) -> String {
    if actor.trim().is_empty() {
        "mission_command".to_string()
    } else {
        actor.to_string()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn saga_recovers_every_committed_phase_without_replaying_completed_effects() {
        let services = RuntimeServices::in_memory().expect("services");
        let mission_id = services.mission_runtime().default_mission_id().to_string();
        let command = MissionCommand {
            command_id: "mission-saga-create".to_string(),
            action: MissionCommandAction::Create,
            target: MissionCommandTarget::Mission {
                mission_id: mission_id.clone(),
            },
            actor: "test".to_string(),
            expected_revision: Some(0),
            correlation_id: "corr".to_string(),
            payload: serde_json::json!({"objective": "prove durable saga"}),
            evidence_refs: Vec::new(),
        };
        let reserved = reserve_mission_command(&services, command.clone()).expect("reserve");
        assert_eq!(reserved.phase, MissionCommandSagaPhase::Reserved);
        assert_eq!(
            reserve_mission_command(&services, command).expect("reserve replay"),
            reserved
        );

        let effect = execute_reserved_runtime_effect(&services, &reserved.command.command_id)
            .await
            .expect("effect");
        assert_eq!(effect.phase, MissionCommandSagaPhase::EffectCommitted);
        let replay = execute_reserved_runtime_effect(&services, &reserved.command.command_id)
            .await
            .expect("effect replay");
        assert_eq!(replay.revision, effect.revision);
        assert_eq!(
            services
                .mission_runtime()
                .aggregate(&mission_id)
                .unwrap()
                .revision,
            1
        );

        let receipt =
            commit_mission_receipt(&services, &reserved.command.command_id).expect("receipt");
        assert_eq!(receipt.phase, MissionCommandSagaPhase::ReceiptCommitted);
        let finalized =
            finalize_mission_command(&services, &reserved.command.command_id).expect("finalize");
        assert_eq!(finalized.phase, MissionCommandSagaPhase::Finalized);
        assert_eq!(
            finalize_mission_command(&services, &reserved.command.command_id)
                .expect("finalize replay")
                .revision,
            finalized.revision
        );
    }

    #[test]
    fn reserved_cross_domain_effect_can_be_committed_by_gateway_and_finalized() {
        let services = RuntimeServices::in_memory().expect("services");
        services
            .mission_runtime()
            .ensure_default_mission()
            .expect("mission");
        let command = MissionCommand {
            command_id: "mission-saga-session".to_string(),
            action: MissionCommandAction::Pause,
            target: MissionCommandTarget::Session {
                session_id: "session-a".to_string(),
            },
            actor: "gateway".to_string(),
            expected_revision: None,
            correlation_id: "corr".to_string(),
            payload: serde_json::Value::Null,
            evidence_refs: Vec::new(),
        };
        reserve_mission_command(&services, command).expect("reserve");
        commit_mission_effect(
            &services,
            "mission-saga-session",
            serde_json::json!({"unloaded": true}),
            Vec::new(),
        )
        .expect("effect");
        commit_mission_receipt(&services, "mission-saga-session").expect("receipt");
        let final_record =
            finalize_mission_command(&services, "mission-saga-session").expect("final");
        assert_eq!(final_record.phase, MissionCommandSagaPhase::Finalized);
        assert_eq!(
            final_record.receipt.unwrap().result["unloaded"],
            serde_json::json!(true)
        );
    }
}
