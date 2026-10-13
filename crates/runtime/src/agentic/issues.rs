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

/// The single classifier every layer must use to decide what a completion gap
/// asks for.
///
/// It used to be re-derived by string matching at each call site: the closure
/// view looked for `original_intent_review_required`/`criterion_review_required`
/// and otherwise assumed an issue-classification gap, while the presentation
/// layer made its own substring choice. Run one19 proved the cost of that
/// duplication - one `issue_requires_classification` gap was rendered as
/// `objective_review` by the closure view and as `message_publish` by the
/// completion-gap guidance, in the same context envelope, so the model was told
/// to do two different things and did neither. One function, one answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionGapKind {
    /// A Task-recorded issue still carries no disposition.
    IssueRequiresClassification,
    /// A Task-recorded issue is classified `must_resolve` and still blocks.
    IssueMustResolve,
    /// The Goal verifier wants an Objective review before it can certify.
    ObjectiveReview,
    /// Anything else: still an Objective review, because that is the action that
    /// produces the verdict the Goal gate reads.
    Unknown,
}

impl CompletionGapKind {
    /// The rung name the closure view advertises for this gap. A gap whose rung
    /// names an action that cannot close it is worse than no rung at all.
    pub(crate) fn closure_step(self) -> &'static str {
        match self {
            CompletionGapKind::IssueRequiresClassification => "classify_issues",
            CompletionGapKind::IssueMustResolve => "resolve_issue",
            CompletionGapKind::ObjectiveReview | CompletionGapKind::Unknown => "objective_review",
        }
    }

    /// The tool that closes this gap, named for the tool allowlist the
    /// escalation path forces once prose answers stop making progress.
    pub(crate) fn closing_tool(self) -> &'static str {
        match self {
            CompletionGapKind::IssueRequiresClassification
            | CompletionGapKind::IssueMustResolve => "message_publish",
            CompletionGapKind::ObjectiveReview | CompletionGapKind::Unknown => "objective_review",
        }
    }

    pub(crate) fn is_issue_gap(self) -> bool {
        matches!(
            self,
            CompletionGapKind::IssueRequiresClassification | CompletionGapKind::IssueMustResolve
        )
    }
}

/// Classify a gap string once, for every layer that has to act on it.
pub(crate) fn completion_gap_kind(gap: &str) -> CompletionGapKind {
    if gap.starts_with("issue_requires_classification") {
        CompletionGapKind::IssueRequiresClassification
    } else if gap.starts_with("issue_must_resolve") {
        CompletionGapKind::IssueMustResolve
    } else if gap.starts_with("original_intent_review_required")
        || gap.starts_with("criterion_review_required")
    {
        CompletionGapKind::ObjectiveReview
    } else {
        CompletionGapKind::Unknown
    }
}

/// Every Task-recorded issue that still blocks completion, with the reason it
/// blocks. This is the one source for "are there unclassified issues", and it
/// reads the same per-Task descriptions `completion_gap` reads - never
/// `AgenticProgramProjection::unresolved`, which is derived from Goal-level
/// gaps and disagrees with it. Run one19 carried both at once: `satisfied`
/// claimed `classify_issues` was done while the same object reported
/// `completion_gap: issue_requires_classification`.
pub(crate) fn open_issue_blockers(
    projection: &AgenticProgramProjection,
) -> Vec<(String, CompletionGapKind)> {
    issues(projection)
        .into_iter()
        .filter_map(|issue| {
            let kind = match issue.disposition.as_ref().map(|item| item.disposition) {
                None => CompletionGapKind::IssueRequiresClassification,
                Some(IssueDispositionKind::MustResolve) => CompletionGapKind::IssueMustResolve,
                Some(IssueDispositionKind::Disclose | IssueDispositionKind::Resolved) => return None,
            };
            Some((issue.issue_ref, kind))
        })
        .collect()
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
                criterion_source: Default::default(),
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
                claim_renewal_streak: 0,
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

    /// The one19 counterfactual, asserted directly. `AgenticProgramProjection::
    /// unresolved` is Goal-derived and was empty, while a Task carried two
    /// unclassified issue descriptions. The closure view used to read the Goal
    /// field, so it advertised `classify_issues` as satisfied while the same
    /// object reported `completion_gap: issue_requires_classification`. One
    /// source has to answer both, and it is the Task-recorded one.
    #[test]
    fn open_issue_blockers_reads_the_task_recorded_issues_not_the_goal_field() {
        let (mut projection, _) = projection_with_issue();
        // Goal-level gaps: empty. This is exactly the one19 state.
        projection.unresolved = Vec::new();

        let blockers = open_issue_blockers(&projection);
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].1, CompletionGapKind::IssueRequiresClassification);
        assert_eq!(
            completion_gap(&projection).as_deref(),
            Some(format!("issue_requires_classification:{}", blockers[0].0).as_str())
        );
    }

    #[test]
    fn a_disclosed_or_resolved_issue_is_not_a_blocker() {
        let (projection, issue_ref) = projection_with_issue();
        let mut projection = projection;
        projection.topics.insert(
            "topic".to_string(),
            vec![crate::agentic::program::AgenticTopicEntryProjection {
                entry_id: "entry".into(),
                revision: 1,
                actor_id: "root".into(),
                source_execution_id: None,
                intent_generation: None,
                coordination: None,
                summary: None,
                content_ref: None,
                refs: Vec::new(),
                recipients: Vec::new(),
                intent: None,
                issue_dispositions: vec![IssueDisposition {
                    issue_ref,
                    disposition: IssueDispositionKind::Disclose,
                    reason_ref: "tool://receipt".into(),
                    evidence_refs: vec!["task:1".into()],
                }],
            }],
        );
        assert!(open_issue_blockers(&projection).is_empty());
        assert!(completion_gap(&projection).is_none());
    }

    #[test]
    fn a_must_resolve_issue_still_blocks_and_names_its_own_step() {
        let (projection, issue_ref) = projection_with_issue();
        let mut projection = projection;
        projection.topics.insert(
            "topic".to_string(),
            vec![crate::agentic::program::AgenticTopicEntryProjection {
                entry_id: "entry".into(),
                revision: 1,
                actor_id: "root".into(),
                source_execution_id: None,
                intent_generation: None,
                coordination: None,
                summary: None,
                content_ref: None,
                refs: Vec::new(),
                recipients: Vec::new(),
                intent: None,
                issue_dispositions: vec![IssueDisposition {
                    issue_ref,
                    disposition: IssueDispositionKind::MustResolve,
                    reason_ref: "tool://receipt".into(),
                    evidence_refs: vec!["task:1".into()],
                }],
            }],
        );
        let blockers = open_issue_blockers(&projection);
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].1, CompletionGapKind::IssueMustResolve);
        assert_eq!(blockers[0].1.closure_step(), "resolve_issue");
        assert_eq!(blockers[0].1.closing_tool(), "message_publish");
    }

    #[test]
    fn every_gap_kind_names_the_step_and_tool_that_actually_close_it() {
        // A gap whose rung names an action that cannot close it is worse than no
        // rung: one19 was told to run `objective_review` and `message_publish`
        // for the same `issue_requires_classification` gap.
        for (gap, step, tool) in [
            (
                "issue_requires_classification:issue:abc",
                "classify_issues",
                "message_publish",
            ),
            ("issue_must_resolve:issue:abc", "resolve_issue", "message_publish"),
            (
                "criterion_review_required:user_intent",
                "objective_review",
                "objective_review",
            ),
            (
                "original_intent_review_required:user_intent",
                "objective_review",
                "objective_review",
            ),
            ("some_unknown_gap", "objective_review", "objective_review"),
        ] {
            let kind = completion_gap_kind(gap);
            assert_eq!(kind.closure_step(), step, "step for {gap}");
            assert_eq!(kind.closing_tool(), tool, "tool for {gap}");
        }
    }

    #[test]
    fn an_issue_gap_is_distinguished_from_an_objective_review_gap() {
        assert!(completion_gap_kind("issue_requires_classification:x").is_issue_gap());
        assert!(completion_gap_kind("issue_must_resolve:x").is_issue_gap());
        assert!(!completion_gap_kind("criterion_review_required:user_intent").is_issue_gap());
        assert!(!completion_gap_kind("anything_else").is_issue_gap());
    }
}
