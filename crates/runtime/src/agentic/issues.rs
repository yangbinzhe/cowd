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
                return Some(format!("issue_must_resolve:{}", issue.issue_ref))
            }
            Some(IssueDispositionKind::Disclose | IssueDispositionKind::Resolved) => {}
        }
    }
    None
}
