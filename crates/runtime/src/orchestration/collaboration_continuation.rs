//! Typed collaboration continuation.
//!
//! "继续/上一组团队" resolves from exact Session/root history into a frozen
//! `CollaborationContinuationBinding`. Cross-session continuation only accepts
//! a typed handoff reference; same-session candidates are ordered by recency
//! and a CAS claim guarantees one new root per continuation digest+ingress.

use harness_contract::turn::{CollaborationContinuationBinding, ContinuationAuthorization};
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
    };
    binding.binding_digest = continuation_digest(&binding)?;
    Ok(binding)
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
    });
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&value)
                .map_err(|error| format!("encode continuation binding: {error}"))?
        )
    ))
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
    if binding.source_session_id != current_session_id && !cross_session_allowed {
        return Err(format!(
            "cross-session continuation requires an explicit typed handoff reference (source session `{}`)",
            binding.source_session_id
        ));
    }
    Ok(())
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
        ensure_reauthorized(&first, "session-2", true, 42).expect("explicit handoff");

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
}
