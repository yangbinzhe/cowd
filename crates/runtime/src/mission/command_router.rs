//! Canonical Mission command boundary.
//!
//! Surfaces may translate their own controls into this contract, but only this
//! module is allowed to mutate Mission-facing aggregates.  It validates a
//! target revision, delegates to the aggregate that owns the state, and
//! leaves an idempotent command receipt in the Runtime event store.

use harness_contract::agent::{AgentCommand, AgentCommandRequest, AgentInput};
use harness_contract::mission::{
    MissionCommand, MissionCommandAction, MissionCommandReceipt, MissionCommandTarget,
};
use harness_contract::reality::{EvidenceRef, RealityBoundary};
use harness_contract::turn::{SessionDispatchAction, SessionHandoff};

use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{
    ExecutionGraphHost, MissionCommandInterpreter, MissionInterpretedCommand, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope, RuntimeServices, SessionRelationKind,
};

pub async fn execute_mission_command(
    services: &RuntimeServices,
    command: MissionCommand,
) -> MissionCommandReceipt {
    let stream_id = format!("mission-commands:{}", services.workspace_key());
    if command.command_id.trim().is_empty() {
        return rejected(command, "command_id is required");
    }
    if let Ok(Some(event)) = services
        .event_store()
        .event_by_idempotency_key(&stream_id, &command.command_id)
    {
        if let Ok(receipt) = serde_json::from_value::<MissionCommandReceipt>(event.payload) {
            return receipt;
        }
        return rejected(command, "stored command receipt is corrupt");
    }

    let target_revision = match target_revision(services, &command.target) {
        Ok(revision) => revision,
        Err(error) => return rejected(command, error),
    };
    if let Some(expected_revision) = command.expected_revision {
        if expected_revision != target_revision {
            return rejected(
                command,
                format!(
                    "stale target revision: expected {expected_revision}, actual {target_revision}"
                ),
            );
        }
    }

    let outcome = execute(services, &command).await;
    let (status, reason, result, evidence_refs) = match outcome {
        Ok((result, evidence_refs)) => ("accepted".to_string(), None, result, evidence_refs),
        Err(error) => (
            "rejected".to_string(),
            Some(error),
            serde_json::Value::Null,
            command.evidence_refs.clone(),
        ),
    };
    let receipt = MissionCommandReceipt {
        command_id: command.command_id.clone(),
        action: command.action,
        target: command.target.clone(),
        accepted_revision: target_revision,
        status,
        reason,
        evidence_refs,
        result,
    };
    if let Err(error) = persist_receipt(services, &stream_id, &command, &receipt) {
        return MissionCommandReceipt {
            status: "failed".to_string(),
            reason: Some(format!(
                "command outcome could not be durably audited: {error}"
            )),
            ..receipt
        };
    }
    receipt
}

async fn execute(
    services: &RuntimeServices,
    command: &MissionCommand,
) -> Result<(serde_json::Value, Vec<EvidenceRef>), String> {
    match (&command.target, command.action) {
        (MissionCommandTarget::Session { session_id }, MissionCommandAction::Activate) => {
            let receipt = services.mission_runtime().switch_session(session_id)?;
            Ok((
                serde_json::json!({ "receipt": receipt }),
                command.evidence_refs.clone(),
            ))
        }
        (MissionCommandTarget::Session { session_id }, MissionCommandAction::Background) => {
            let receipt = services.mission_runtime().background_session(session_id)?;
            Ok((
                serde_json::json!({ "receipt": receipt }),
                command.evidence_refs.clone(),
            ))
        }
        (MissionCommandTarget::Session { session_id }, MissionCommandAction::Pause) => {
            let receipt = services.mission_runtime().pause_session(session_id)?;
            Ok((
                serde_json::json!({ "receipt": receipt }),
                command.evidence_refs.clone(),
            ))
        }
        (MissionCommandTarget::Session { session_id }, MissionCommandAction::Resume) => {
            let receipt = services.mission_runtime().switch_session(session_id)?;
            Ok((
                serde_json::json!({ "receipt": receipt }),
                command.evidence_refs.clone(),
            ))
        }
        (MissionCommandTarget::Session { session_id }, MissionCommandAction::Cancel)
        | (MissionCommandTarget::Session { session_id }, MissionCommandAction::Close) => {
            let receipt = services.mission_runtime().close_session(session_id)?;
            Ok((
                serde_json::json!({ "receipt": receipt }),
                command.evidence_refs.clone(),
            ))
        }
        (MissionCommandTarget::Session { session_id }, MissionCommandAction::Input)
        | (MissionCommandTarget::Session { session_id }, MissionCommandAction::Replan) => {
            submit_session_handoff(services, command, session_id).await
        }
        (MissionCommandTarget::Agent { agent_id }, action)
            if matches!(
                action,
                MissionCommandAction::Pause
                    | MissionCommandAction::Resume
                    | MissionCommandAction::Cancel
                    | MissionCommandAction::Input
                    | MissionCommandAction::Replan
            ) =>
        {
            command_agent(services, command, agent_id).await
        }
        (MissionCommandTarget::Approval { approval_id }, MissionCommandAction::Approve)
        | (MissionCommandTarget::Approval { approval_id }, MissionCommandAction::Reject) => {
            let _ = approval_id;
            Err("mission approval decisions require an authenticated VerifiedPrincipal command port".to_string())
        }
        (MissionCommandTarget::Relation { .. }, MissionCommandAction::Link) => {
            let relation = parse_relation(command)?;
            let relation = services.session_relations().add_relation(
                relation.from_session_id,
                relation.to_session_id,
                relation.kind,
                relation.summary,
                relation.evidence_refs,
            )?;
            let mut evidence_refs = command.evidence_refs.clone();
            evidence_refs.extend(relation.evidence_refs.iter().map(|id| {
                EvidenceRef::new("session_relation", id, RealityBoundary::Observed)
                    .with_source("runtime.mission_command")
            }));
            evidence_refs.sort_by(|left, right| left.id.cmp(&right.id));
            evidence_refs
                .dedup_by(|left, right| left.ref_type == right.ref_type && left.id == right.id);
            Ok((serde_json::json!({ "relation": relation }), evidence_refs))
        }
        _ => Err(format!(
            "unsupported canonical MissionCommand target/action: {:?} {:?}",
            command.target, command.action
        )),
    }
}

async fn submit_session_handoff(
    services: &RuntimeServices,
    command: &MissionCommand,
    target_session_id: &str,
) -> Result<(serde_json::Value, Vec<EvidenceRef>), String> {
    let mut handoff: SessionHandoff =
        serde_json::from_value(command.payload.clone()).map_err(|error| {
            format!("session input/replan requires SessionHandoff payload: {error}")
        })?;
    if handoff.target_session_id != target_session_id {
        return Err("handoff target_session_id must match command target".to_string());
    }
    if handoff.source_session_id.trim().is_empty()
        || handoff.objective.trim().is_empty()
        || handoff.correlation_id.trim().is_empty()
    {
        return Err("handoff requires source session, objective, and correlation id".to_string());
    }
    for reference in &command.evidence_refs {
        let reference = harness_contract::turn::opaque_session_evidence_ref(
            &handoff.source_session_id,
            &reference.id,
        );
        if !handoff.evidence_refs.contains(&reference) {
            handoff.evidence_refs.push(reference);
        }
    }
    let action = if matches!(command.action, MissionCommandAction::Replan) {
        SessionDispatchAction::Replan
    } else {
        SessionDispatchAction::Enqueue
    };
    let interpretation =
        MissionCommandInterpreter::interpret_session_handoff_with_action(handoff, action);
    let MissionInterpretedCommand::SubmitExecutionGraph {
        graph,
        graph_command,
    } = interpretation.command
    else {
        return Err("session handoff did not compile to an execution graph".to_string());
    };
    let receipt = services
        .execution_supervisor()
        .submit_graph(graph, graph_command)
        .await
        .map_err(|error| error.to_string())?;
    Ok((
        serde_json::json!({ "admission": receipt }),
        command.evidence_refs.clone(),
    ))
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
        MissionCommandAction::Input => AgentCommand::SendInput,
        MissionCommandAction::Replan => AgentCommand::Interrupt,
        _ => return Err("mission action is not valid for an agent target".to_string()),
    };
    let input = matches!(command.action, MissionCommandAction::Input)
        .then(|| {
            command
                .payload
                .get("content")
                .and_then(serde_json::Value::as_str)
                .filter(|content| !content.trim().is_empty())
                .map(|content| AgentInput::UserSupplement(content.to_string()))
                .ok_or_else(|| "agent input requires payload.content".to_string())
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
) -> Result<u64, String> {
    match target {
        MissionCommandTarget::Mission { .. } | MissionCommandTarget::Session { .. } => {
            services.mission_runtime().revision()
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

fn persist_receipt(
    services: &RuntimeServices,
    stream_id: &str,
    command: &MissionCommand,
    receipt: &MissionCommandReceipt,
) -> Result<(), String> {
    let revision = services
        .event_store()
        .stream_revision(stream_id)
        .map_err(|error| error.to_string())?;
    services
        .event_store()
        .append_batch_if_revision(
            stream_id.to_string(),
            revision,
            format!("mission-command:{}", command.command_id),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.to_string(),
                    scope: RuntimeEventScope::Mission,
                    kind: "mission.command.receipt.v1".to_string(),
                    status: Some(receipt.status.clone()),
                    actor: Some(non_empty_actor(&command.actor)),
                    refs: command
                        .evidence_refs
                        .iter()
                        .map(|reference| RuntimeEventRef {
                            kind: "evidence".to_string(),
                            id: reference.id.clone(),
                        })
                        .collect(),
                    payload: serde_json::to_value(receipt).map_err(|error| error.to_string())?,
                },
                idempotency_key: Some(command.command_id.clone()),
                schema_version: 1,
            }],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn rejected(command: MissionCommand, reason: impl Into<String>) -> MissionCommandReceipt {
    MissionCommandReceipt {
        command_id: command.command_id,
        action: command.action,
        target: command.target,
        accepted_revision: 0,
        status: "rejected".to_string(),
        reason: Some(reason.into()),
        evidence_refs: command.evidence_refs,
        result: serde_json::Value::Null,
    }
}

fn non_empty_actor(actor: &str) -> String {
    if !actor.trim().is_empty() {
        actor.to_string()
    } else {
        "mission_command".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StartMissionSessionRequest;

    #[tokio::test]
    async fn commands_are_revision_checked_idempotent_and_durably_audited() {
        let services = RuntimeServices::in_memory().expect("services");
        services
            .mission_runtime()
            .start_session(StartMissionSessionRequest {
                title: "command target".to_string(),
                session_id: Some("session-command-target".to_string()),
            })
            .expect("session");
        let revision = services.mission_runtime().revision().expect("revision");
        let command = MissionCommand {
            command_id: "mission-command-pause".to_string(),
            action: MissionCommandAction::Pause,
            target: MissionCommandTarget::Session {
                session_id: "session-command-target".to_string(),
            },
            actor: "test".to_string(),
            expected_revision: Some(revision),
            correlation_id: "correlation-command".to_string(),
            payload: serde_json::Value::Null,
            evidence_refs: vec![harness_contract::reality::EvidenceRef::new(
                "test_fixture",
                "test://mission-command/evidence",
                harness_contract::reality::RealityBoundary::Observed,
            )],
        };
        let first = execute_mission_command(&services, command.clone()).await;
        assert_eq!(first.status, "accepted");
        let duplicate = execute_mission_command(&services, command).await;
        assert_eq!(duplicate, first);
        assert_eq!(
            services
                .mission_runtime()
                .get_session("session-command-target")
                .expect("session")
                .status
                .as_str(),
            "paused"
        );
        let stale = execute_mission_command(
            &services,
            MissionCommand {
                command_id: "mission-command-stale".to_string(),
                action: MissionCommandAction::Resume,
                target: MissionCommandTarget::Session {
                    session_id: "session-command-target".to_string(),
                },
                actor: "test".to_string(),
                expected_revision: Some(revision.saturating_sub(1)),
                correlation_id: "correlation-stale".to_string(),
                payload: serde_json::Value::Null,
                evidence_refs: Vec::new(),
            },
        )
        .await;
        assert_eq!(stale.status, "rejected");
        assert!(stale
            .reason
            .unwrap_or_default()
            .contains("stale target revision"));
    }
}
