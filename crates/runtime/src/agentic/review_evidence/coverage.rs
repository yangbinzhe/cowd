use harness_contract::goal::{GoalContract, GoalResultKind, ObjectiveReviewRecord};

use super::{review_result_identity, AgenticProgramProjection, ProofInputSnapshot};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReviewCoverageError {
    #[error("review decision or Goal specification is not current")]
    Specification,
    #[error("review has no v2 proof for its exact condition")]
    Proof,
    #[error("review input snapshot does not match its authenticated sources")]
    Snapshot,
    #[error("review producer/reviewer independence is not established")]
    Independence,
    #[error("review has no complete material/result/evidence set")]
    Empty,
    #[error("review result identity is invalid: {0}")]
    Identity(String),
    #[error("review does not cover required result: {0}")]
    Result(String),
    #[error("review lacks a complete independent read of result: {0}")]
    Read(String),
    #[error("review evidence is absent from the selected evidence set: {0}")]
    Evidence(String),
}

pub(crate) fn evidence_contains(evidence: &[String], reference: &str) -> bool {
    evidence.iter().any(|candidate| {
        candidate == reference
            || candidate
                .strip_prefix("agentic_changed:")
                .is_some_and(|inner| inner == reference)
    })
}

pub(super) fn result_refs_cover(
    program: &AgenticProgramProjection,
    references: &[String],
    required: &str,
) -> bool {
    let Ok(identity) = review_result_identity(program, required) else {
        return false;
    };
    references.iter().any(|reference| {
        review_result_identity(program, reference).is_ok_and(|current| current == identity)
    })
}

pub(crate) fn validate_review_coverage(
    goal: &GoalContract,
    program: &AgenticProgramProjection,
    review: &ObjectiveReviewRecord,
    required_results: &[String],
    evidence: &[String],
    snapshot: &ProofInputSnapshot,
) -> Result<(), ReviewCoverageError> {
    if review.spec_revision != goal.spec_revision
        || review.input_manifest_digest != goal.spec_digest
        || review.decision != "satisfied"
    {
        return Err(ReviewCoverageError::Specification);
    }
    let proof = review
        .verification
        .as_ref()
        .ok_or(ReviewCoverageError::Proof)?;
    let inputs = proof.input_set.as_ref().ok_or(ReviewCoverageError::Proof)?;
    if inputs.version != 2
        || inputs.condition_ref.as_deref() != Some(review.criterion_ref.as_str())
        || proof.goal_spec_revision != Some(goal.spec_revision)
        || proof.goal_spec_digest.as_deref() != Some(goal.spec_digest.as_str())
    {
        return Err(ReviewCoverageError::Proof);
    }
    if inputs.semantic_digest.is_empty()
        || snapshot.semantic_digest != inputs.semantic_digest
        || !snapshot.reviewer_matches(program, review)
        || !proof
            .result_source_revisions
            .iter()
            .all(|(stream, revision)| {
                snapshot.sources.iter().any(|source| {
                    &source.stream_id == stream && source.expected_revision == *revision
                })
            })
    {
        return Err(ReviewCoverageError::Snapshot);
    }
    if proof.reviewer_execution_id.is_empty()
        || proof.producer_execution_ids.is_empty()
        || proof.producer_execution_ids.iter().any(|producer| {
            producer.is_empty()
                || (proof.independence_required && producer == &proof.reviewer_execution_id)
        })
        || (proof.independence_required && review.producer_refs.contains(&review.reviewer_actor))
    {
        return Err(ReviewCoverageError::Independence);
    }
    if inputs.material_events.is_empty()
        || review.evidence_refs.is_empty()
        || review.producer_refs.is_empty()
        || review.result_refs.is_empty()
        || required_results.is_empty()
    {
        return Err(ReviewCoverageError::Empty);
    }
    for reference in required_results {
        let identity =
            review_result_identity(program, reference).map_err(ReviewCoverageError::Identity)?;
        if !result_refs_cover(program, &review.result_refs, reference)
            || !result_refs_cover(program, &inputs.result_refs, reference)
        {
            return Err(ReviewCoverageError::Result(reference.clone()));
        }
        if !proof.reads.iter().any(|read| {
            review_result_identity(program, &read.result_ref)
                .is_ok_and(|candidate| candidate == identity)
                && !read.sha256.is_empty()
                && !read.receipt_refs.is_empty()
                && (read.result_kind != GoalResultKind::ToolEffect
                    || !read.effect_observation_refs.is_empty())
        }) {
            return Err(ReviewCoverageError::Read(reference.clone()));
        }
    }
    for reference in &review.evidence_refs {
        if !evidence_contains(evidence, reference) {
            return Err(ReviewCoverageError::Evidence(reference.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn shared_coverage_keeps_condition_identity_independence_and_read_boundaries() {
        let program = AgenticProgramProjection::empty("program", "objective");
        let goal: GoalContract = serde_json::from_value(json!({
            "id":"goal", "session_id":"session", "objective":"deliver", "criteria":[],
            "phase":"execution", "completion":"open", "revision":1, "user_sequence":1,
            "spec_revision":1, "spec_digest":"spec",
        }))
        .unwrap();
        let record = json!({
            "review_id":"review", "criterion_ref":"criterion", "spec_revision":1,
            "input_manifest_digest":"spec", "result_refs":["tool://result"],
            "evidence_refs":["tool://read"], "reviewer_actor":"reviewer",
            "reviewer_execution_id":"review-run", "producer_refs":["producer"],
            "decision":"satisfied", "reason_ref":"tool://read",
            "verification": {
                "goal_spec_revision":1, "goal_spec_digest":"spec", "work_manifest_digest":"work",
                "reviewer_execution_id":"review-run", "producer_execution_ids":["producer-run"],
                "reads":[{"result_kind":"tool_effect", "result_ref":"tool://result",
                    "content_ref":"artifact://body", "sha256":"digest", "bytes":4,
                    "receipt_refs":["tool://read"], "effect_observation_refs":["tool://after-write"]}],
                "input_set": {"version":2, "condition_ref":"criterion", "result_refs":["tool://result"],
                    "auxiliary_refs":[], "required_obligation_refs":[], "task_refs":[], "artifact_refs":[],
                    "packet_digests":{}, "source_revisions":{}, "required_scopes":[],
                    "required_scope_complete":true, "acceptance_policy_digest":"policy", "semantic_digest":"semantic",
                    "material_events":[{"stream_id":"source", "sequence":1, "event_id":"event", "payload_digest":"material"}]}
            }
        });
        let snapshot = ProofInputSnapshot {
            work_digest: "work".into(),
            task_refs: vec![],
            artifact_refs: vec![],
            reviewer_execution_refs: ["review-run".into()].into_iter().collect(),
            retiring_optional_task_refs: Default::default(),
            semantic_digest: "semantic".into(),
            policy_digest: "policy".into(),
            sources: vec![],
            current_review_effect_key: None,
            effects: super::super::policy::RootReviewPolicySnapshot {
                digest: "effects".into(),
                last_effect_cursor: 0,
                additional_sources: vec![],
                source: crate::ExpectedStreamRevision {
                    stream_id: "source".into(),
                    expected_revision: 1,
                },
            },
        };
        let required = vec!["tool://result".into()];
        let evidence = vec!["tool://read".into()];
        let evaluate = |value| {
            let review = serde_json::from_value(value).unwrap();
            validate_review_coverage(&goal, &program, &review, &required, &evidence, &snapshot)
        };
        assert_eq!(evaluate(record.clone()), Ok(()));
        for (path, value, expected) in [
            (
                "/spec_revision",
                json!(2),
                ReviewCoverageError::Specification,
            ),
            (
                "/verification/input_set/condition_ref",
                json!("other"),
                ReviewCoverageError::Proof,
            ),
            (
                "/verification/input_set/semantic_digest",
                json!("stale"),
                ReviewCoverageError::Snapshot,
            ),
            (
                "/reviewer_execution_id",
                json!("foreign"),
                ReviewCoverageError::Snapshot,
            ),
            (
                "/verification/producer_execution_ids",
                json!(["review-run"]),
                ReviewCoverageError::Independence,
            ),
            (
                "/verification/input_set/material_events",
                json!([]),
                ReviewCoverageError::Empty,
            ),
            (
                "/verification/input_set/result_refs",
                json!(["tool://other"]),
                ReviewCoverageError::Result("tool://result".into()),
            ),
            (
                "/verification/reads/0/receipt_refs",
                json!([]),
                ReviewCoverageError::Read("tool://result".into()),
            ),
            (
                "/verification/reads/0/effect_observation_refs",
                json!([]),
                ReviewCoverageError::Read("tool://result".into()),
            ),
            (
                "/evidence_refs",
                json!(["tool://missing"]),
                ReviewCoverageError::Evidence("tool://missing".into()),
            ),
        ] {
            let mut changed = record.clone();
            *changed.pointer_mut(path).unwrap() = value;
            assert_eq!(evaluate(changed), Err(expected), "{path}");
        }
    }
}
