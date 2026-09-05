use harness_contract::agent_action::{
    AgentActionEnvelope, ArtifactCommitInput, MessagePublishInput,
};

use super::program::{
    AgenticArtifactProjection, AgenticProgramProjection, AgenticTopicEntryProjection,
};

pub(crate) fn apply_message_publish(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &MessagePublishInput,
    entity_ref: Option<&str>,
    revision: u64,
) {
    let Some(entry_id) = entity_ref else {
        return;
    };
    projection
        .topics
        .entry(input.topic_ref.clone())
        .or_default()
        .push(AgenticTopicEntryProjection {
            entry_id: entry_id.to_string(),
            revision,
            actor_id: envelope.actor.actor_id.clone(),
            summary: input.summary.clone(),
            content_ref: input.content_ref.clone(),
            refs: input.refs.clone(),
        });
}

pub(crate) fn apply_artifact_commit(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &ArtifactCommitInput,
    entity_ref: Option<&str>,
) {
    let Some(artifact_ref) = entity_ref else {
        return;
    };
    let mut relates_to = input.relates_to.clone();
    // An executing Agent is already fenced to exactly one claimed Task by
    // actor identity + physical execution. That relation is Runtime fact, not
    // model-authored metadata. Persist it automatically so a correct artifact
    // cannot become orphaned merely because the model omitted a redundant ID
    // and only discover that mistake later at task_submit.
    if let Some(task_ref) = projection
        .tasks
        .values()
        .find(|task| {
            task.status == super::program::AgenticTaskStatus::Claimed
                && task.claimant.as_deref() == envelope.actor.agent_id.as_deref()
                && task.claim_execution_id == envelope.actor.execution_id
        })
        .map(|task| task.task_id.clone())
    {
        relates_to.push(task_ref);
        relates_to.sort();
        relates_to.dedup();
    }
    projection.artifacts.insert(
        artifact_ref.to_string(),
        AgenticArtifactProjection {
            artifact_ref: artifact_ref.to_string(),
            content_ref: input.content_ref.clone(),
            kind: input.kind.clone(),
            title: input.title.clone(),
            relates_to,
            committed_by: envelope.actor.actor_id.clone(),
        },
    );
}
