//! Private typed session continuation.
//!
//! "继续/上一组团队" resolves from exact Session/root history into a frozen
//! `CollaborationContinuationBinding`. Cross-session continuation only accepts
//! a typed handoff reference; same-session candidates are ordered by recency
//! and a CAS claim guarantees one new root per continuation digest+ingress.

use std::sync::Arc;

use harness_contract::turn::{
    CollaborationContinuationBinding, ContinuationAuthorization, SessionHandoff,
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeTransactionEventInput,
};

const CONTINUATION_CAS_STREAM: &str = "continuation-cas";

/// One eligible continuation source derived from durable history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuationCandidate {
    pub source_session_id: String,
    pub source_turn_id: String,
    pub source_root_id: String,
    pub team_set_ref: String,
    pub delivery_revision: u64,
    pub result_refs: Vec<String>,
    /// A durable accepted cross-session handoff authorizing this candidate.
    /// Same-session candidates intentionally leave this empty.
    pub handoff_id: Option<String>,
}

/// Compile the immutable continuation binding and compute its digest.
pub fn compile_continuation_binding(
    candidate: &ContinuationCandidate,
    current_ingress: &str,
    candidate_revision: u64,
    authorization: ContinuationAuthorization,
    authorization_revision: u64,
) -> Result<CollaborationContinuationBinding, String> {
    let mut binding = CollaborationContinuationBinding {
        source_session_id: candidate.source_session_id.clone(),
        source_turn_id: candidate.source_turn_id.clone(),
        source_root_id: candidate.source_root_id.clone(),
        team_set_ref: candidate.team_set_ref.clone(),
        delivery_revision: candidate.delivery_revision,
        result_refs: candidate.result_refs.clone(),
        current_ingress: current_ingress.to_string(),
        candidate_revision,
        binding_digest: String::new(),
        authorization,
        authorization_revision,
        handoff_id: candidate.handoff_id.clone(),
    };
    binding.binding_digest = continuation_digest(&binding)?;
    Ok(binding)
}

/// Loads the newest verified Agentic Program from the exact Session. The
/// continuation authority is the durable Program reducer and its objective
/// verdict, never a legacy strategy receipt or user/assistant prose. The
/// current turn is excluded so a retry cannot bind a root to itself.
pub fn latest_same_session_candidate(
    store: &Arc<RuntimeEventStore>,
    session_id: &str,
    current_turn_id: &str,
) -> Result<Option<(ContinuationCandidate, u64)>, String> {
    if session_id.trim().is_empty() || current_turn_id.trim().is_empty() {
        return Err("continuation lookup requires a session and turn identity".to_string());
    }
    let actions = crate::AgentActionService::new(Arc::clone(store));
    let streams = store
        .stream_ids_for_scope(RuntimeEventScope::Program)
        .map_err(|error| error.to_string())?;
    let mut candidates = Vec::new();
    for stream_id in streams {
        let Some(program_id) = stream_id.strip_prefix("agentic-program:") else {
            continue;
        };
        let Some(program) = actions
            .project_if_exists(program_id)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        if program.session_id != session_id
            || program.turn_id == current_turn_id
            || program.status != crate::AgenticProgramStatus::Verified
        {
            continue;
        }
        let Some(source_root_id) = program
            .root_execution_id
            .as_ref()
            .filter(|value| !value.trim().is_empty())
            .cloned()
        else {
            continue;
        };
        let delivery_revision = store
            .list_stream_page_desc(&stream_id, 1, 0)?
            .first()
            .map_or(program.revision, |event| event.commit_cursor);
        let mut result_refs = vec![
            format!("execution_graph:{source_root_id}"),
            format!("agentic_program:{program_id}"),
        ];
        if let Some(final_artifact_ref) = program.final_artifact_ref.as_ref() {
            result_refs.push(final_artifact_ref.clone());
        }
        for artifact in program.artifacts.values() {
            result_refs.extend([artifact.artifact_ref.clone(), artifact.content_ref.clone()]);
        }
        for task in program.tasks.values() {
            result_refs.extend(task.artifact_refs.iter().cloned());
            result_refs.extend(task.evidence_refs.iter().cloned());
        }
        for entry in program.topics.values().flatten() {
            if let Some(content_ref) = entry.content_ref.as_ref() {
                result_refs.push(content_ref.clone());
            }
            result_refs.extend(entry.refs.iter().cloned());
        }
        if let Some(completion) = program.completion_request.as_ref() {
            result_refs.extend(completion.evidence_refs.iter().cloned());
        }
        result_refs.retain(|reference| !reference.trim().is_empty());
        result_refs.sort();
        result_refs.dedup();
        candidates.push((
            ContinuationCandidate {
                source_session_id: session_id.to_string(),
                source_turn_id: program.turn_id,
                source_root_id,
                team_set_ref: format!("agentic_program:{program_id}"),
                delivery_revision,
                result_refs,
                handoff_id: None,
            },
            delivery_revision,
        ));
    }
    candidates.sort_by_key(|(_, revision)| *revision);
    Ok(candidates.pop())
}

fn continuation_digest(binding: &CollaborationContinuationBinding) -> Result<String, String> {
    let value = json!({
        "source_session_id": binding.source_session_id,
        "source_turn_id": binding.source_turn_id,
        "source_root_id": binding.source_root_id,
        "team_set_ref": binding.team_set_ref,
        "delivery_revision": binding.delivery_revision,
        "result_refs": binding.result_refs,
        "current_ingress": binding.current_ingress,
        "candidate_revision": binding.candidate_revision,
        "authorization": binding.authorization,
        "authorization_revision": binding.authorization_revision,
        "handoff_id": binding.handoff_id,
    });
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&value)
                .map_err(|error| format!("encode continuation binding: {error}"))?
        )
    ))
}

fn continuation_claim_key(binding: &CollaborationContinuationBinding) -> String {
    format!(
        "continuation:{}:{}",
        binding.current_ingress, binding.binding_digest
    )
}

/// Domain event appended atomically with `ExecutionGraph::Planned`.  The
/// graph and continuation claim therefore have one durable winner: a crash
/// cannot leave a consumed continuation claim without its root graph.
pub(crate) fn graph_continuation_claim_event(
    binding: &CollaborationContinuationBinding,
    root_graph_id: &str,
) -> Result<RuntimeTransactionEventInput, String> {
    if binding.binding_digest != continuation_digest(binding)? {
        return Err("continuation binding digest does not match its authority fields".to_string());
    }
    if root_graph_id.trim().is_empty()
        || binding.current_ingress.trim().is_empty()
        || binding.source_session_id.trim().is_empty()
        || binding.source_root_id.trim().is_empty()
        || binding.team_set_ref.trim().is_empty()
    {
        return Err("continuation graph claim has an incomplete immutable binding".to_string());
    }
    let (team_set_kind, team_set_id) =
        if let Some(program_id) = binding.team_set_ref.strip_prefix("agentic_program:") {
            ("agentic_program", program_id)
        } else if let Some(handoff_id) = binding.team_set_ref.strip_prefix("session_handoff:") {
            ("session_handoff", handoff_id)
        } else {
            return Err("continuation team_set_ref has no typed authority".to_string());
        };
    let key = continuation_claim_key(binding);
    Ok(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: CONTINUATION_CAS_STREAM.to_string(),
            scope: RuntimeEventScope::Relation,
            kind: "team.continuation.root_claimed.v2".to_string(),
            status: Some("claimed".to_string()),
            actor: Some("execution_commit_service".to_string()),
            refs: vec![
                RuntimeEventRef {
                    kind: "session".to_string(),
                    id: binding.source_session_id.clone(),
                },
                RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: binding.source_root_id.clone(),
                },
                RuntimeEventRef {
                    kind: team_set_kind.to_string(),
                    id: team_set_id.to_string(),
                },
                RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: root_graph_id.to_string(),
                },
                RuntimeEventRef {
                    kind: "session_input".to_string(),
                    id: binding.current_ingress.clone(),
                },
            ],
            payload: json!({
                "root_graph_id": root_graph_id,
                "binding": binding,
            }),
        },
        idempotency_key: Some(key),
        schema_version: 1,
    })
}

/// Reauthorization gate. Revoked revisions fail closed; cross-session only
/// proceeds through an explicit typed handoff reference.
pub fn ensure_reauthorized(
    binding: &CollaborationContinuationBinding,
    current_session_id: &str,
    cross_session_allowed: bool,
    expected_authorization_revision: u64,
) -> Result<(), String> {
    if binding.authorization != ContinuationAuthorization::Authorized {
        return Err(format!(
            "continuation binding `{}` is revoked; current execution is denied",
            binding.binding_digest
        ));
    }
    if binding.authorization_revision != expected_authorization_revision {
        return Err(format!(
            "continuation authorization revision mismatch: expected {expected_authorization_revision}, frozen {}",
            binding.authorization_revision
        ));
    }
    if binding.source_session_id != current_session_id {
        if !cross_session_allowed || binding.handoff_id.as_deref().is_none_or(str::is_empty) {
            return Err(format!(
                "cross-session continuation requires an explicit typed handoff reference (source session `{}`)",
                binding.source_session_id
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct AcceptedSessionHandoff {
    handoff: SessionHandoff,
    request_id: String,
    source_graph_id: String,
}

/// Resolve a cross-session continuation only from the immutable acceptance
/// event written by `SessionDispatchNodeExecutor`. A handoff id received at
/// ingress is merely a lookup key; this function validates target Session and
/// returns only the handoff's authorized evidence references.
pub fn accepted_cross_session_candidate(
    store: &RuntimeEventStore,
    target_session_id: &str,
    handoff_id: &str,
) -> Result<Option<(ContinuationCandidate, u64)>, String> {
    if target_session_id.trim().is_empty() || handoff_id.trim().is_empty() {
        return Err(
            "cross-session continuation requires target session and handoff id".to_string(),
        );
    }
    for stream_id in store
        .stream_ids_for_scope(RuntimeEventScope::SessionInput)
        .map_err(|error| error.to_string())?
    {
        if !stream_id.starts_with("session-handoff-target:") {
            continue;
        }
        for event in store
            .list_stream(&stream_id)
            .map_err(|error| error.to_string())?
        {
            if event.kind != "session.handoff.accepted.v1" {
                continue;
            }
            let accepted: AcceptedSessionHandoff =
                serde_json::from_value(event.payload).map_err(|error| error.to_string())?;
            let handoff = accepted.handoff;
            // Older dispatches stored correlation_id in TaskRouteHint while
            // the externally visible handoff id remained canonical. Accept
            // either durable identity, but persist the canonical handoff id.
            if handoff.target_session_id != target_session_id
                || (handoff.handoff_id != handoff_id && handoff.correlation_id != handoff_id)
            {
                continue;
            }
            let mut result_refs = handoff
                .evidence_refs
                .iter()
                .filter(|reference| reference.is_durable())
                .map(|reference| {
                    format!(
                        "evidence:{}:{}",
                        reference.evidence_ref.ref_type, reference.evidence_ref.id
                    )
                })
                .collect::<Vec<_>>();
            result_refs.push(format!("session_handoff:{}", handoff.handoff_id));
            result_refs.sort();
            result_refs.dedup();
            return Ok(Some((
                ContinuationCandidate {
                    source_session_id: handoff.source_session_id,
                    source_turn_id: accepted.request_id,
                    source_root_id: accepted.source_graph_id,
                    team_set_ref: format!("session_handoff:{}", handoff.handoff_id),
                    delivery_revision: event.sequence,
                    result_refs,
                    handoff_id: Some(handoff.handoff_id),
                },
                event.sequence,
            )));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeEventStore;

    fn candidate(session: &str, root: &str, turn: &str) -> ContinuationCandidate {
        ContinuationCandidate {
            source_session_id: session.to_string(),
            source_turn_id: turn.to_string(),
            source_root_id: root.to_string(),
            team_set_ref: format!("agentic_program:program-{root}"),
            delivery_revision: 3,
            result_refs: vec![format!("result:{root}")],
            handoff_id: None,
        }
    }

    #[test]
    fn continuation_binding_digest_is_deterministic_and_reauthorization_fails_closed() {
        let candidate = candidate("session-1", "root-1", "turn-1");
        let first = compile_continuation_binding(
            &candidate,
            "ingress-1",
            7,
            ContinuationAuthorization::Authorized,
            42,
        )
        .expect("binding");
        let second = compile_continuation_binding(
            &candidate,
            "ingress-1",
            7,
            ContinuationAuthorization::Authorized,
            42,
        )
        .expect("binding");
        assert_eq!(first.binding_digest, second.binding_digest);

        ensure_reauthorized(&first, "session-1", false, 42).expect("authorized same session");
        assert!(
            ensure_reauthorized(&first, "session-1", false, 43).is_err(),
            "stale authorization revision fails closed"
        );
        assert!(
            ensure_reauthorized(&first, "session-2", false, 42).is_err(),
            "cross-session without explicit handoff fails closed"
        );
        let mut cross_session = first.clone();
        cross_session.handoff_id = Some("handoff-1".to_string());
        ensure_reauthorized(&cross_session, "session-2", true, 42).expect("explicit handoff");

        let mut revoked = first.clone();
        revoked.authorization = ContinuationAuthorization::Revoked;
        assert!(ensure_reauthorized(&revoked, "session-1", false, 42).is_err());
    }

    #[test]
    fn accepted_cross_session_handoff_is_the_only_cross_session_candidate_source() {
        let store = RuntimeEventStore::for_test();
        let handoff = SessionHandoff {
            handoff_id: "handoff-authorized".to_string(),
            source_session_id: "session-source".to_string(),
            target_session_id: "session-target".to_string(),
            objective: "continue only the durable handoff evidence".to_string(),
            scope: vec!["read:evidence".to_string()],
            acceptance: vec!["report evidence gaps".to_string()],
            context_lens: Vec::new(),
            evidence_refs: Vec::new(),
            context_budget_lease: None,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            deadline_at_ms: None,
            priority: 1,
            correlation_id: "handoff-correlation".to_string(),
            result_contract: "evidence-backed continuation".to_string(),
            task_route_hint: None,
        };
        store
            .append(RuntimeEventInput {
                stream_id: "session-handoff-target:request-target".to_string(),
                scope: RuntimeEventScope::SessionInput,
                kind: "session.handoff.accepted.v1".to_string(),
                status: Some("queued".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: json!({
                    "handoff": handoff,
                    "request_id": "request-target",
                    "receipt": {},
                    "source_graph_id": "source-root",
                    "source_node_id": "source-dispatch",
                }),
            })
            .expect("accepted handoff event");
        let (candidate, revision) =
            accepted_cross_session_candidate(&store, "session-target", "handoff-correlation")
                .expect("lookup")
                .expect("accepted target handoff");
        assert_eq!(candidate.source_root_id, "source-root");
        assert_eq!(candidate.handoff_id.as_deref(), Some("handoff-authorized"));
        assert!(
            accepted_cross_session_candidate(&store, "other-session", "handoff-authorized")
                .expect("lookup")
                .is_none()
        );
        let binding = compile_continuation_binding(
            &candidate,
            "ingress-target",
            revision,
            ContinuationAuthorization::Authorized,
            4,
        )
        .expect("binding");
        ensure_reauthorized(&binding, "session-target", true, 4)
            .expect("accepted handoff authorizes cross-session continuation");
    }

    fn append_verified_program(
        store: &Arc<RuntimeEventStore>,
        session: &str,
        turn: &str,
        root: &str,
        program_id: &str,
    ) -> Result<(), String> {
        let actions = crate::AgentActionService::new(Arc::clone(store));
        actions
            .apply(&harness_contract::agent_action::AgentActionEnvelope {
                action_id: format!("open-{program_id}"),
                actor: harness_contract::agent_action::AgentActorBinding {
                    objective_id: format!("objective-{program_id}"),
                    program_id: program_id.to_string(),
                    session_id: session.to_string(),
                    turn_id: turn.to_string(),
                    root_execution_id: Some(root.to_string()),
                    required_team_count: 1,
                    objective_summary: format!("verified work for {program_id}"),
                    model_lease: "test".to_string(),
                    permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                    resource_scopes: Vec::new(),
                    actor_id: format!("root:{session}"),
                    kind: harness_contract::agent_action::AgentActorKind::Root,
                    execution_id: None,
                    team_id: None,
                    agent_id: None,
                },
                expected_revision: None,
                action: harness_contract::agent_action::AgentAction::TeamCreate(
                    harness_contract::agent_action::TeamCreateInput {
                        name: format!("Team {program_id}"),
                        mission: "preserve verified context".to_string(),
                        objective: None,
                    },
                ),
            })
            .map_err(|error| error.to_string())?;
        store
            .append(RuntimeEventInput {
                stream_id: format!("agentic-program:{program_id}"),
                scope: RuntimeEventScope::Program,
                kind: "agentic.objective_verdict_bound".to_string(),
                status: Some("verified".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: json!({
                    "program_id": program_id,
                    "verdict": {
                        "goal_id": format!("goal-{program_id}"),
                        "goal_revision": 2,
                        "terminal_fence": format!("fence-{program_id}"),
                        "authority_revision": 2,
                        "kind": "satisfied",
                    },
                }),
            })
            .map(|_| ())
    }

    #[test]
    fn same_session_continuation_uses_verified_agentic_program_and_excludes_current_turn() {
        let store = Arc::new(RuntimeEventStore::for_test());
        append_verified_program(&store, "session-a", "turn-old", "root-old", "program-old")
            .expect("old Program");
        append_verified_program(
            &store,
            "session-a",
            "turn-current",
            "root-current",
            "program-current",
        )
        .expect("current Program");

        let (candidate, revision) =
            latest_same_session_candidate(&store, "session-a", "turn-current")
                .expect("lookup")
                .expect("eligible candidate");
        assert_eq!(candidate.source_turn_id, "turn-old");
        assert_eq!(candidate.source_root_id, "root-old");
        assert_eq!(candidate.team_set_ref, "agentic_program:program-old");
        assert!(revision > 0);
        assert!(candidate
            .result_refs
            .contains(&"agentic_program:program-old".to_string()));
    }

    #[test]
    fn graph_claim_event_is_digest_checked_and_carries_exact_source_refs() {
        let binding = compile_continuation_binding(
            &candidate("session-1", "root-1", "turn-1"),
            "ingress-1",
            7,
            ContinuationAuthorization::Authorized,
            42,
        )
        .expect("binding");
        let event = graph_continuation_claim_event(&binding, "root-new").expect("event");
        assert_eq!(event.event.stream_id, CONTINUATION_CAS_STREAM);
        assert_eq!(event.event.kind, "team.continuation.root_claimed.v2");
        assert!(event
            .event
            .refs
            .iter()
            .any(|reference| reference.kind == "execution_graph" && reference.id == "root-new"));

        let mut tampered = binding;
        tampered.result_refs.push("unverified:ref".to_string());
        assert!(graph_continuation_claim_event(&tampered, "root-new").is_err());
    }
}
