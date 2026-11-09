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

/// The named failures a review's required writes left behind, in the
/// closure projection's existing entry shape.
///
/// A required write that ran and failed is carried as its own named failure:
/// it is never a satisfied requirement, and it is never an effect that did
/// not happen. The entry names the write and points at the receipt that
/// settled it, so the failure can be read plainly wherever the review's
/// conclusion is recorded.
///
/// A review whose required writes did not fail names nothing: an empty list
/// here is the ordinary case, and filling it would be noise.
pub(crate) fn required_write_failure_entries(
    snapshot: &ProofInputSnapshot,
) -> Vec<serde_json::Value> {
    snapshot
        .terminal_failures
        .iter()
        .map(|failure| {
            let effect_key = failure["effect_key"].as_str().unwrap_or_default();
            serde_json::json!({
                "name": format!("required_write_terminally_failed:{effect_key}"),
                "passed": false,
                "evidence": {
                    "grade": failure["grade"],
                    "effect_key": failure["effect_key"],
                    "tool_use_id": failure["tool_use_id"],
                    "tool_name": failure["tool_name"],
                    "failure_class": failure["failure_class"],
                    "error": failure["error"],
                    "receipt_cursor": failure["receipt_cursor"],
                },
            })
        })
        .collect()
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
                }) || snapshot.pre_authenticated_session_source.as_ref().is_some_and(|source|
                    &source.stream_id == stream && source.expected_revision == *revision)
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

    /// R-E-1: a required write that ran and failed is named in the closure
    /// entries, plainly, with the write and the receipt that settled it.
    #[test]
    fn r_e_1_a_terminally_failed_required_write_is_named_in_the_closure_entries() {
        let snapshot = snapshot_with_terminal_failures(vec![serde_json::json!({
            "grade": "required_write_terminally_failed",
            "effect_key": "f24-failed-effect-key",
            "tool_use_id": "f24-failed-call",
            "tool_name": "task_review",
            "failure_class": "terminal_failure",
            "error": "Runtime did not execute 'task_review': the objective requirement was not met",
            "receipt_cursor": 4242,
        })]);
        let entries = super::required_write_failure_entries(&snapshot);
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry["evidence"]["grade"], "required_write_terminally_failed");
        assert_eq!(entry["evidence"]["effect_key"], "f24-failed-effect-key");
        assert_eq!(entry["evidence"]["tool_use_id"], "f24-failed-call");
        assert_eq!(entry["evidence"]["receipt_cursor"], 4242);
        assert_eq!(entry["passed"], false);
        // Plain readable: the name carries the effect key as it is.
        let name = entry["name"].as_str().unwrap_or_default();
        assert!(name.contains("required_write_terminally_failed"), "{name}");
        assert!(name.contains("f24-failed-effect-key"), "{name}");
    }

    /// R-E-1: the named failure reaches the durable projection, plainly
    /// readable - and it is absent when no required write failed.
    #[test]
    fn r_e_1_the_named_failure_is_in_the_serialized_projection() {
        let mut projection = crate::agentic::program::AgenticProgramProjection::empty(
            "gc-program",
            "gc-objective",
        );
        projection.required_write_failures = super::required_write_failure_entries(
            &snapshot_with_terminal_failures(vec![serde_json::json!({
                "grade": "required_write_terminally_failed",
                "effect_key": "f24-failed-effect-key",
                "tool_use_id": "f24-failed-call",
                "tool_name": "task_review",
                "failure_class": "terminal_failure",
                "error": "Runtime did not execute 'task_review': the objective requirement was not met",
                "receipt_cursor": 4242,
            })]),
        );
        let encoded = serde_json::to_value(&projection).expect("the projection");
        let entry = &encoded["required_write_failures"][0];
        let name = entry["name"].as_str().unwrap_or_default();
        assert!(name.contains("required_write_terminally_failed"), "{name}");
        assert!(name.contains("f24-failed-effect-key"), "{name}");
        assert_eq!(entry["passed"], false);
        assert_eq!(entry["evidence"]["grade"], "required_write_terminally_failed");
        assert_eq!(entry["evidence"]["effect_key"], "f24-failed-effect-key");
        assert_eq!(entry["evidence"]["tool_use_id"], "f24-failed-call");
        assert_eq!(entry["evidence"]["receipt_cursor"], 4242);
        // The same entries again: the same durable facts give the same bytes.
        let mut again = crate::agentic::program::AgenticProgramProjection::empty(
            "gc-program",
            "gc-objective",
        );
        again.required_write_failures = super::required_write_failure_entries(
            &snapshot_with_terminal_failures(vec![serde_json::json!({
                "grade": "required_write_terminally_failed",
                "effect_key": "f24-failed-effect-key",
                "tool_use_id": "f24-failed-call",
                "tool_name": "task_review",
                "failure_class": "terminal_failure",
                "error": "Runtime did not execute 'task_review': the objective requirement was not met",
                "receipt_cursor": 4242,
            })]),
        );
        assert_eq!(
            serde_json::to_string(&again).expect("the projection"),
            serde_json::to_string(&projection).expect("the projection"),
            "the same durable facts give the same projection bytes"
        );
        // Negative control: no required write failed, so no entry appears.
        let mut quiet = crate::agentic::program::AgenticProgramProjection::empty(
            "gc-program",
            "gc-objective",
        );
        quiet.required_write_failures =
            super::required_write_failure_entries(&snapshot_with_terminal_failures(vec![]));
        let encoded = serde_json::to_value(&quiet).expect("the projection");
        assert!(
            encoded["required_write_failures"]
                .as_array()
                .is_some_and(|entries| entries.is_empty()),
            "a review without a failed required write names nothing"
        );
    }

    /// Negative control: a review whose required writes did not fail leaves
    /// no failure entry - the closure is not filled with noise.
    #[test]
    fn r_e_1_a_review_without_a_failed_required_write_names_nothing() {
        let snapshot = snapshot_with_terminal_failures(vec![]);
        assert!(super::required_write_failure_entries(&snapshot).is_empty());
    }

    /// A snapshot whose required writes did not fail, or whose failures are
    /// not the named ones, names nothing.
    fn snapshot_with_terminal_failures(
        terminal_failures: Vec<serde_json::Value>,
    ) -> ProofInputSnapshot {
        ProofInputSnapshot {
            work_digest: "work".into(),
            task_refs: vec![],
            artifact_refs: vec![],
            reviewer_execution_refs: ["review-run".into()].into_iter().collect(),
            reviewer_packet: None,
            retiring_optional_task_refs: Default::default(),
            semantic_digest: "semantic".into(),
            policy_digest: "policy".into(),
            sources: vec![],
            current_review_effect_key: None,
            terminal_failures,
            dependency_frame: super::super::policy::ConclusionMaterialDependencies::empty_for_coverage_test(),
            pre_authenticated_session_source: None,
            effects: super::super::policy::RootReviewPolicySnapshot {
                digest: "effects".into(),
                last_effect_cursor: 0,
                additional_sources: vec![],
                source: crate::ExpectedStreamRevision {
                    stream_id: "source".into(),
                    expected_revision: 1,
                },
            },
        }
    }

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
            reviewer_packet: None,
            retiring_optional_task_refs: Default::default(),
            semantic_digest: "semantic".into(),
            policy_digest: "policy".into(),
            sources: vec![],
            current_review_effect_key: None,
            // The coverage fixture has no required write that ran and failed.
            terminal_failures: vec![],
            dependency_frame: super::super::policy::ConclusionMaterialDependencies::empty_for_coverage_test(),
            pre_authenticated_session_source: None,
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
