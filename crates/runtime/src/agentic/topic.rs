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
    let intent_generation = input.intent.as_ref().and_then(|intent| {
        let task = projection.tasks.get_mut(&intent.task_ref)?;
        if intent.kind == harness_contract::agent_action::TaskIntentKind::Decline {
            // Historical prose/unbound typed messages are not retroactively
            // promoted to a settled opportunity during journal replay.
            let execution_id = envelope.actor.execution_id.as_ref()?;
            let attempt = task.active_attempts.get(execution_id)?;
            if Some(attempt.agent_id.as_str()) != envelope.actor.agent_id.as_deref()
                || attempt.agent_id != envelope.actor.actor_id
                || attempt.mode != harness_contract::agent_action::AgentAttemptMode::Execute
                || attempt.generation != task.claim_generation
                || !matches!(
                    task.status,
                    super::program::AgenticTaskStatus::Published
                        | super::program::AgenticTaskStatus::Rework
                )
            {
                return None;
            }
            // Validation bound this message to the publisher's unclaimed
            // opportunity. Settle in the SAME journal transition as the
            // reason, before followup dispatch, with no self-cancellation wait.
            task.active_attempts.remove(execution_id);
        }
        Some(task.claim_generation)
    });
    projection
        .topics
        .entry(input.topic_ref.clone())
        .or_default()
        .push(AgenticTopicEntryProjection {
            entry_id: entry_id.to_string(),
            revision,
            actor_id: envelope.actor.actor_id.clone(),
            source_execution_id: envelope.actor.execution_id.clone(),
            intent_generation,
            coordination: None,
            summary: input.summary.clone(),
            content_ref: input.content_ref.clone(),
            refs: input.refs.clone(),
            recipients: input.recipients.clone(),
            intent: input.intent.clone(),
            issue_dispositions: input.issue_dispositions.clone(),
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
    let active_claim = projection
        .tasks
        .values()
        .find(|task| {
            task.status == super::program::AgenticTaskStatus::Claimed
                && task.claimant.as_deref() == envelope.actor.agent_id.as_deref()
                && task.claim_execution_id == envelope.actor.execution_id
        })
        .map(|task| {
            (
                task.task_id.clone(),
                task.claim_execution_id.clone(),
                task.claim_generation,
            )
        });
    if let Some((task_ref, _, _)) = active_claim.as_ref() {
        // The physical claim relation is Runtime-authored. Model-provided
        // `relates_to` only augments it and can never replace the fence.
        relates_to.push(task_ref.clone());
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
            claim_execution_id: active_claim
                .as_ref()
                .and_then(|(_, execution_id, _)| execution_id.clone()),
            claim_generation: active_claim.map(|(_, _, generation)| generation),
        },
    );
}
