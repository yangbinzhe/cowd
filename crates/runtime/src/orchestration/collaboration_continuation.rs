//! Typed collaboration continuation.
//!
//! "继续/上一组团队" resolves from exact Session/root history into a frozen
//! `CollaborationContinuationBinding`. Cross-session continuation only accepts
//! a typed handoff reference; same-session candidates are ordered by recency
//! and a CAS claim guarantees one new root per continuation digest+ingress.

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
const SESSION_HISTORY_PAGE: usize = 64;

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

/// Candidate ordering: current active/recoverable root (0), latest same
/// session Team set (1), explicit cross-session handoff (2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ContinuationPriority {
    CurrentRoot = 0,
    LatestSameSession = 1,
    ExplicitCrossSession = 2,
}

impl ContinuationCandidate {
    #[must_use]
    pub fn priority(
        &self,
        current_session_id: &str,
        current_root_id: Option<&str>,
    ) -> ContinuationPriority {
        if current_root_id == Some(self.source_root_id.as_str()) {
            ContinuationPriority::CurrentRoot
        } else if self.source_session_id == current_session_id {
            ContinuationPriority::LatestSameSession
        } else {
            ContinuationPriority::ExplicitCrossSession
        }
    }
}

/// Deterministically choose the best candidate. A tie at the same priority is
/// `None` (typed ambiguity) instead of a guess.
#[must_use]
pub fn resolve_candidate<'a>(
    candidates: &'a [ContinuationCandidate],
    current_session_id: &str,
    current_root_id: Option<&str>,
) -> Option<&'a ContinuationCandidate> {
    let mut best: Option<(&ContinuationCandidate, ContinuationPriority)> = None;
    let mut tied = false;
    for candidate in candidates {
        let priority = candidate.priority(current_session_id, current_root_id);
        match best {
            None => {
                best = Some((candidate, priority));
                tied = false;
            }
            Some((_, best_priority)) if priority < best_priority => {
                best = Some((candidate, priority));
                tied = false;
            }
            Some((best_candidate, best_priority)) if priority == best_priority => {
                if best_candidate.source_root_id != candidate.source_root_id {
                    tied = true;
                }
            }
            Some(_) => {}
        }
    }
    if tied {
        None
    } else {
        best.map(|(candidate, _)| candidate)
    }
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

/// Loads the newest *eligible* collaboration result from the exact Session
/// strategy stream.  This deliberately reads durable strategy facts, not
/// user/assistant text or a mutable Team projection.  The current turn is
/// excluded so a retry cannot accidentally bind a root to itself.
pub fn latest_same_session_candidate(
    store: &RuntimeEventStore,
    session_id: &str,
    current_turn_id: &str,
) -> Result<Option<(ContinuationCandidate, u64)>, String> {
    if session_id.trim().is_empty() || current_turn_id.trim().is_empty() {
        return Err("continuation lookup requires a session and turn identity".to_string());
    }
    let stream_id = format!("session:{session_id}");
    let page = store.list_stream_page_desc(&stream_id, SESSION_HISTORY_PAGE, 0)?;
    for event in page {
        if event.kind != "runtime.strategy.outcome"
            || event.status.as_deref() != Some("completed")
            || event
                .payload
                .get("session_ref")
                .and_then(serde_json::Value::as_str)
                != Some(session_id)
        {
            continue;
        }
        let Some(source_turn_id) = event
            .payload
            .get("turn_ref")
            .and_then(serde_json::Value::as_str)
            .filter(|turn| !turn.trim().is_empty())
        else {
            continue;
        };
        if source_turn_id == current_turn_id {
            continue;
        }
        let Some(source_root_id) = event
            .payload
            .get("execution_graph_ref")
            .and_then(serde_json::Value::as_str)
            .filter(|graph| !graph.trim().is_empty())
        else {
            continue;
        };
        let Some(receipt) = event
            .payload
            .get("collaboration_receipt")
            .filter(|receipt| !receipt.is_null())
        else {
            continue;
        };
        if receipt
            .get("degraded")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
            || receipt
                .get("verified_team_executions")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
                == 0
        {
            continue;
        }
        let Some(team_graph_id) = receipt
            .pointer("/evidence/graph_id")
            .or_else(|| receipt.pointer("/execution/graph_id"))
            .and_then(serde_json::Value::as_str)
            .filter(|graph| !graph.trim().is_empty())
        else {
            continue;
        };
        let mut result_refs = vec![
            format!("execution_graph:{source_root_id}"),
            format!("team_graph:{team_graph_id}"),
        ];
        if let Some(envelope_id) = receipt
            .pointer("/delivery_envelope/envelope_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            result_refs.push(format!("delivery_envelope:{envelope_id}"));
        }
        result_refs.sort();
        result_refs.dedup();
        return Ok(Some((
            ContinuationCandidate {
                source_session_id: session_id.to_string(),
                source_turn_id: source_turn_id.to_string(),
                source_root_id: source_root_id.to_string(),
                team_set_ref: format!("team_graph:{team_graph_id}"),
                // The Session event is the durable delivery revision.  A
                // mutable graph revision is intentionally not consulted.
                delivery_revision: event.sequence,
                result_refs,
                handoff_id: None,
            },
            event.sequence,
        )));
    }
    Ok(None)
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
                    kind: "team_graph".to_string(),
                    id: binding
                        .team_set_ref
                        .trim_start_matches("team_graph:")
                        .to_string(),
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

/// CAS claim for one continuation root. Exactly one concurrent caller wins;
/// every later caller sees `false` and must not create a second root.
pub fn claim_continuation_root(
    store: &RuntimeEventStore,
    current_ingress: &str,
    binding_digest: &str,
) -> Result<bool, String> {
    let key = format!("continuation:{current_ingress}:{binding_digest}");
    if store
        .event_by_idempotency_key(CONTINUATION_CAS_STREAM, &key)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Ok(false);
    }
    let revision = store
        .stream_revision(CONTINUATION_CAS_STREAM)
        .map_err(|error| error.to_string())?;
    store
        .append_batch_if_revision(
            CONTINUATION_CAS_STREAM,
            revision,
            key.clone(),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: CONTINUATION_CAS_STREAM.to_string(),
                    scope: RuntimeEventScope::Team,
                    kind: "team.continuation.root_claimed.v1".to_string(),
                    status: Some("claimed".to_string()),
                    actor: Some("collaboration_continuation".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "session_input".to_string(),
                        id: current_ingress.to_string(),
                    }],
                    payload: json!({ "binding_digest": binding_digest }),
                },
                idempotency_key: Some(key),
                schema_version: 1,
            }],
        )
        .map(|_| true)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeEventStore;
    use std::sync::Arc;

    fn candidate(session: &str, root: &str, turn: &str) -> ContinuationCandidate {
        ContinuationCandidate {
            source_session_id: session.to_string(),
            source_turn_id: turn.to_string(),
            source_root_id: root.to_string(),
            team_set_ref: format!("team-set:{root}"),
            delivery_revision: 3,
            result_refs: vec![format!("result:{root}")],
            handoff_id: None,
        }
    }

    #[test]
    fn candidate_ranking_prefers_current_root_then_same_session() {
        let candidates = vec![
            candidate("session-1", "root-old", "turn-old"),
            candidate("session-1", "root-current", "turn-current"),
            candidate("session-2", "root-other", "turn-other"),
        ];
        let resolved =
            resolve_candidate(&candidates, "session-1", Some("root-current")).expect("candidate");
        assert_eq!(resolved.source_root_id, "root-current");

        let unambiguous = vec![
            candidate("session-1", "root-old", "turn-old"),
            candidate("session-2", "root-other", "turn-other"),
        ];
        let same_session =
            resolve_candidate(&unambiguous, "session-1", None).expect("same session candidate");
        assert_eq!(same_session.source_root_id, "root-old");

        let cross_only = vec![candidate("session-2", "root-other", "turn-other")];
        let cross = resolve_candidate(&cross_only, "session-3", None).expect("cross session");
        assert_eq!(cross.source_root_id, "root-other");
    }

    #[test]
    fn ambiguous_same_priority_candidates_return_none() {
        let candidates = vec![
            candidate("session-1", "root-a", "turn-a"),
            candidate("session-1", "root-b", "turn-b"),
        ];
        assert!(resolve_candidate(&candidates, "session-2", None).is_none());
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
    fn continuation_cas_claim_is_first_winner_only() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        assert!(claim_continuation_root(&store, "ingress-1", "digest-1").expect("first claim"));
        assert!(!claim_continuation_root(&store, "ingress-1", "digest-1").expect("replay"));
        assert!(claim_continuation_root(&store, "ingress-1", "digest-2").expect("different digest"));
        assert!(
            claim_continuation_root(&store, "ingress-2", "digest-1").expect("different ingress")
        );
    }

    #[test]
    fn accepted_cross_session_handoff_is_the_only_cross_session_candidate_source() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
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

    fn append_outcome(
        store: &RuntimeEventStore,
        session: &str,
        turn: &str,
        root: &str,
        team_graph: &str,
        verified_teams: u64,
        degraded: bool,
    ) -> Result<(), String> {
        store
            .append(RuntimeEventInput {
                stream_id: format!("session:{session}"),
                scope: RuntimeEventScope::Session,
                kind: "runtime.strategy.outcome".to_string(),
                status: Some("completed".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: json!({
                    "session_ref": session,
                    "turn_ref": turn,
                    "execution_graph_ref": root,
                    "collaboration_receipt": {
                        "verified_team_executions": verified_teams,
                        "degraded": degraded,
                        "evidence": { "graph_id": team_graph },
                        "delivery_envelope": { "envelope_id": format!("envelope:{team_graph}") },
                    },
                }),
            })
            .map(|_| ())
    }

    #[test]
    fn same_session_continuation_uses_latest_verified_receipt_and_excludes_current_turn() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        append_outcome(
            &store,
            "session-a",
            "turn-old",
            "root-old",
            "team-old",
            2,
            false,
        )
        .expect("old outcome");
        append_outcome(
            &store,
            "session-a",
            "turn-degraded",
            "root-degraded",
            "team-degraded",
            2,
            true,
        )
        .expect("degraded outcome");
        append_outcome(
            &store,
            "session-a",
            "turn-current",
            "root-current",
            "team-current",
            2,
            false,
        )
        .expect("current outcome");

        let (candidate, revision) =
            latest_same_session_candidate(&store, "session-a", "turn-current")
                .expect("lookup")
                .expect("eligible candidate");
        assert_eq!(candidate.source_turn_id, "turn-old");
        assert_eq!(candidate.source_root_id, "root-old");
        assert_eq!(candidate.team_set_ref, "team_graph:team-old");
        assert!(revision > 0);
        assert_eq!(
            candidate.result_refs[0],
            "delivery_envelope:envelope:team-old"
        );
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
