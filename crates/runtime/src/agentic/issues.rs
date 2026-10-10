//! Source-bound issue views and adjudication; persistence remains the Topic log.
use super::program::AgenticProgramProjection;
use harness_contract::agent_action::{
    AgentActionEnvelope, AgentActorKind, IssueDispositionKind, MessagePublishInput,
};
use sha2::{Digest, Sha256};

#[derive(serde::Serialize)]
pub(crate) struct IssueView {
    pub issue_ref: String,
    pub source_ref: String,
    pub description: String,
    pub disposition: Option<harness_contract::agent_action::IssueDisposition>,
    pub adjudication_ref: Option<String>,
    pub adjudicated_by: Option<String>,
}

pub(crate) fn issues(projection: &AgenticProgramProjection) -> Vec<IssueView> {
    let mut views = Vec::new();
    for task in projection
        .tasks
        .values()
        .filter(|task| !task.status.is_retired())
    {
        for description in &task.unresolved {
            let digest = Sha256::digest(
                serde_json::to_vec(&(&task.task_id, description)).unwrap_or_default(),
            );
            let issue_ref = format!("issue:{digest:x}");
            let latest = projection
                .topics
                .values()
                .flatten()
                .filter_map(|entry| {
                    entry
                        .issue_dispositions
                        .iter()
                        .find(|item| item.issue_ref == issue_ref)
                        .map(|item| (entry, item))
                })
                .max_by_key(|(entry, _)| entry.revision);
            views.push(IssueView {
                issue_ref,
                source_ref: task.task_id.clone(),
                description: description.clone(),
                disposition: latest.map(|(_, item)| item.clone()),
                adjudication_ref: latest.map(|(entry, _)| entry.entry_id.clone()),
                adjudicated_by: latest.map(|(entry, _)| entry.actor_id.clone()),
            });
        }
    }
    views.sort_by(|a, b| a.issue_ref.cmp(&b.issue_ref));
    views
}

pub(crate) fn validate_dispositions(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &MessagePublishInput,
) -> Option<(&'static str, String)> {
    if input.issue_dispositions.is_empty() {
        return None;
    }
    if envelope.actor.kind != AgentActorKind::Root {
        return Some((
            "issue_adjudication_requires_root",
            "members may propose through Topic; final Objective classification belongs to the root"
                .into(),
        ));
    }
    let known = issues(projection);
    for disposition in &input.issue_dispositions {
        let Some(issue) = known
            .iter()
            .find(|issue| issue.issue_ref == disposition.issue_ref)
        else {
            return Some((
                "issue_source_changed_or_not_found",
                disposition.issue_ref.clone(),
            ));
        };
        // Durable evidence lives in `evidence_refs`: a tool/artifact receipt, a
        // Runtime entity anchor (the source-bound Task/Issue), or the issue's own
        // source. `reason_ref` is a human reason and may legitimately be prose,
        // so it is not required to be a reference.
        let source_anchor = issue.source_ref.as_str();
        let durable = |reference: &str| {
            reference.starts_with("artifact://")
                || reference.starts_with("tool://")
                || reference.starts_with("task:")
                || reference.starts_with("issue:")
                || reference.starts_with("program:")
                || reference == source_anchor
        };
        let all_evidence_durable = disposition
            .evidence_refs
            .iter()
            .all(|reference| durable(reference));
        let has_durable_anchor = disposition
            .evidence_refs
            .iter()
            .any(|reference| durable(reference))
            || durable(&disposition.reason_ref);
        if !all_evidence_durable || !has_durable_anchor {
            return Some((
                "issue_disposition_requires_durable_evidence",
                disposition.issue_ref.clone(),
            ));
        }
    }
    None
}

pub(crate) fn completion_gap(projection: &AgenticProgramProjection) -> Option<String> {
    for issue in issues(projection) {
        match issue.disposition.as_ref().map(|item| item.disposition) {
            None => return Some(format!("issue_requires_classification:{}", issue.issue_ref)),
            Some(IssueDispositionKind::MustResolve) => {
                return Some(format!("issue_must_resolve:{}", issue.issue_ref));
            }
            Some(IssueDispositionKind::Disclose | IssueDispositionKind::Resolved) => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agentic::program::{
        AgenticProgramProjection, AgenticTaskProjection, AgenticTaskStatus,
    };
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActorBinding, IssueDisposition,
        IssueDispositionKind, MessagePublishInput,
    };

    fn projection_with_issue() -> (AgenticProgramProjection, String) {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert(
            "task:1".to_string(),
            AgenticTaskProjection {
                task_id: "task:1".to_string(),
                team_id: "team:1".to_string(),
                title: "t".to_string(),
                objective: "o".to_string(),
                acceptance: "a".to_string(),
                acceptance_checks: Vec::new(),
                required_capabilities: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Accepted,
                claimant: None,
                claim_generation: 0,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                active_attempts: Default::default(),
                artifact_refs: Vec::new(),
                evidence_refs: Vec::new(),
                unresolved: vec!["external crate metadata is missing".to_string()],
                review_reason: None,
                reviewed_by: None,
                failed_attempts: 0,
                review_generation: 0,
                failed_review_attempts: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
        let issue_ref = issues(&projection)
            .into_iter()
            .next()
            .expect("one issue")
            .issue_ref;
        (projection, issue_ref)
    }

    fn root_envelope() -> AgentActionEnvelope {
        let actor = AgentActorBinding {
            objective_id: "objective".into(),
            program_id: "program".into(),
            session_id: "session".into(),
            turn_id: "turn".into(),
            root_execution_id: Some("graph".into()),
            required_team_count: 3,
            exact_team_count: None,
            objective_summary: "objective".into(),
            model_lease: "default".into(),
            permission_ceiling: None,
            resource_scopes: Vec::new(),
            actor_id: "root:session".into(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        };
        AgentActionEnvelope {
            action_id: "publish".into(),
            actor,
            expected_revision: None,
            action: AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: "topic:program".into(),
                summary: None,
                content_ref: None,
                refs: Vec::new(),
                recipients: Vec::new(),
                intent: None,
                issue_dispositions: Vec::new(),
            }),
        }
    }

    fn publish(dispositions: Vec<IssueDisposition>) -> MessagePublishInput {
        MessagePublishInput {
            topic_ref: "topic:program".into(),
            summary: None,
            content_ref: None,
            refs: Vec::new(),
            recipients: Vec::new(),
            intent: None,
            issue_dispositions: dispositions,
        }
    }

    #[test]
    fn prose_reason_with_runtime_entity_anchor_is_accepted() {
        let (projection, issue_ref) = projection_with_issue();
        let input = publish(vec![IssueDisposition {
            issue_ref,
            disposition: IssueDispositionKind::Disclose,
            reason_ref: "DISCLOSED, non-blocking: external crate metadata unavailable".into(),
            evidence_refs: vec!["task:1".into()],
        }]);
        assert_eq!(
            validate_dispositions(&projection, &root_envelope(), &input),
            None
        );
    }

    #[test]
    fn receipt_evidence_is_still_accepted() {
        let (projection, issue_ref) = projection_with_issue();
        let input = publish(vec![IssueDisposition {
            issue_ref,
            disposition: IssueDispositionKind::Resolved,
            reason_ref: "tool://receipt".into(),
            evidence_refs: vec!["artifact://art_1".into()],
        }]);
        assert_eq!(
            validate_dispositions(&projection, &root_envelope(), &input),
            None
        );
    }

    #[test]
    fn non_durable_evidence_is_rejected() {
        let (projection, issue_ref) = projection_with_issue();
        let input = publish(vec![IssueDisposition {
            issue_ref,
            disposition: IssueDispositionKind::Disclose,
            reason_ref: "some prose".into(),
            evidence_refs: vec!["free text, not a reference".into()],
        }]);
        assert!(validate_dispositions(&projection, &root_envelope(), &input).is_some());
    }

    #[test]
    fn unknown_issue_is_rejected() {
        let (projection, _) = projection_with_issue();
        let input = publish(vec![IssueDisposition {
            issue_ref: "issue:not-real".into(),
            disposition: IssueDispositionKind::Disclose,
            reason_ref: "tool://receipt".into(),
            evidence_refs: vec!["task:1".into()],
        }]);
        assert!(validate_dispositions(&projection, &root_envelope(), &input).is_some());
    }
}
