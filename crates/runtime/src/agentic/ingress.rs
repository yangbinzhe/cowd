//! Trusted Agent-action ingress.
//!
//! Gateway, native agents and Process bridges may parse model-facing action
//! DTOs, but only this Runtime boundary verifies that references are readable
//! durable evidence before the Program journal is mutated.

use harness_contract::agent_action::{AgentAction, AgentActionEnvelope, AgentActionObservation};

use crate::RuntimeServices;

impl RuntimeServices {
    /// Validate evidence selectors in the authenticated actor scope. This is
    /// intentionally an async preflight: storage reads happen before the
    /// ActionService acquires the Program stream lock.
    pub async fn validate_agent_action_evidence(
        &self,
        actor: &harness_contract::agent_action::AgentActorBinding,
        action: &AgentAction,
    ) -> Result<(), String> {
        // All content selectors that enter a durable action use the same
        // authenticated readability check.  Unknown logical IDs (Task/Team
        // IDs, capability IDs, etc.) deliberately remain the reducer's
        // responsibility; only content-shaped refs can reach ArtifactStore.
        let mut evidence_refs = Vec::<&String>::new();
        match action {
            AgentAction::TaskSubmit(input) => {
                evidence_refs.extend(&input.artifact_refs);
                evidence_refs.extend(&input.evidence_refs);
            }
            AgentAction::TaskReview(input) => evidence_refs.extend(&input.evidence_refs),
            AgentAction::TaskSupersede(input) => evidence_refs.extend(&input.evidence_refs),
            AgentAction::TaskWithdraw(input) => {
                evidence_refs.push(&input.reason_ref);
                evidence_refs.extend(&input.evidence_refs);
            }
            AgentAction::ObjectiveUpdate(input) => {
                if let Some(statement_ref) = &input.statement_ref {
                    evidence_refs.push(statement_ref);
                }
                evidence_refs.extend(&input.source_refs);
                if let Some(reason_ref) = &input.reason_ref {
                    evidence_refs.push(reason_ref);
                }
            }
            AgentAction::ObjectiveReview(input) => {
                evidence_refs.extend(&input.result_refs);
                evidence_refs.extend(&input.evidence_refs);
                evidence_refs.push(&input.reason_ref);
            }
            AgentAction::MembershipUpdate(input) => {
                if let Some(reason_ref) = &input.reason_ref {
                    evidence_refs.push(reason_ref);
                }
            }
            AgentAction::TeamUpdate(input) => {
                if let Some(mission_ref) = &input.mission_ref {
                    evidence_refs.push(mission_ref);
                }
                if let Some(reason_ref) = &input.reason_ref {
                    evidence_refs.push(reason_ref);
                }
            }
            AgentAction::MessagePublish(input) => {
                for disposition in &input.issue_dispositions {
                    evidence_refs.push(&disposition.reason_ref);
                    evidence_refs.extend(&disposition.evidence_refs);
                }
                if let Some(content_ref) = &input.content_ref {
                    evidence_refs.push(content_ref);
                }
                evidence_refs.extend(&input.refs);
                if let Some(intent) = &input.intent {
                    if let Some(reason_ref) = &intent.reason_ref {
                        evidence_refs.push(reason_ref);
                    }
                }
            }
            AgentAction::ObjectiveCompleteRequest(input) => {
                evidence_refs.extend(&input.result_refs);
                evidence_refs.extend(&input.evidence_refs);
            }
            _ => {}
        }
        for evidence_ref in evidence_refs {
            let artifact = if let Some(evidence_id) = evidence_ref.strip_prefix("tool://") {
                let access = self
                    .session_evidence_access(&actor.session_id, evidence_id)
                    .await
                    .map_err(|error| format!(
                        "{} could not resolve evidence {evidence_ref} through the authenticated Session journal: {error}",
                        action.kind()
                    ))?
                    .ok_or_else(|| format!(
                        "{} evidence {evidence_ref} has no canonical durable receipt in Session {}; use a tool:// reference returned by a completed tool call in this Session",
                        action.kind(), actor.session_id
                    ))?;
                let artifact = self
                    .artifact_store()
                    .resolve(&access.retrieval_selector)
                    .map_err(|error| {
                        format!(
                            "{} evidence {evidence_ref} points to missing durable content: {error}",
                            action.kind()
                        )
                    })?;
                if artifact.sha256 != access.sha256
                    || artifact.bytes != access.bytes
                    || artifact.media_type != access.media_type
                    || artifact.visibility_scope != access.visibility_scope
                {
                    return Err(format!(
                        "{} evidence {evidence_ref} failed durable receipt integrity validation",
                        action.kind()
                    ));
                }
                artifact
            } else if evidence_ref.starts_with("artifact://") {
                self.artifact_store()
                    .resolve(evidence_ref)
                    .map_err(|error| {
                        format!(
                            "{} evidence {evidence_ref} points to missing durable content: {error}",
                            action.kind()
                        )
                    })?
            } else {
                // Non-content references are validated by their individual
                // action reducers; do not invent a second registry here.
                continue;
            };
            let session_scope = format!("session:{}", actor.session_id);
            if artifact.visibility_scope != "public"
                && artifact.visibility_scope != session_scope
                && !actor
                    .resource_scopes
                    .iter()
                    .any(|scope| scope == &artifact.visibility_scope)
            {
                return Err(format!(
                    "{} evidence {evidence_ref} is not readable in Session {}",
                    action.kind(),
                    actor.session_id
                ));
            }
            self.artifact_store()
                .read(
                    &artifact,
                    &artifact.visibility_scope,
                    Some(0..artifact.bytes.min(1)),
                )
                .await
                .map_err(|error| {
                    format!(
                        "{} evidence {evidence_ref} is not readable: {error}",
                        action.kind()
                    )
                })?;
        }
        Ok(())
    }

    /// The sole public Program mutation ingress. actor is a trusted Runtime
    /// binding constructed from Session/graph/run lineage, never a model JSON
    /// object. Gateway only transports its DTO to this boundary.
    pub async fn submit_agent_action(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, String> {
        envelope.validate().map_err(|error| error.to_string())?;
        let actions = self.agent_action_service();
        if let Some(replay) = actions
            .replay_if_applied(envelope)
            .map_err(|error| error.to_string())?
        {
            return Ok(replay);
        }
        self.validate_agent_action_evidence(&envelope.actor, &envelope.action)
            .await?;
        let positive_review = matches!(&envelope.action,
            AgentAction::ObjectiveReview(input) if input.decision == harness_contract::agent_action::ObjectiveReviewDecision::Satisfied)
            || matches!(&envelope.action, AgentAction::TaskReview(input)
                if input.decision == harness_contract::agent_action::TaskReviewDecision::Accept);
        let review = if positive_review {
            // Resolve both the result set and its manifest from one projection.
            let program = actions
                .project(&envelope.actor.program_id)
                .map_err(|error| error.to_string())?;
            let results = match &envelope.action {
                AgentAction::ObjectiveReview(input) => &input.result_refs,
                AgentAction::TaskReview(input) => {
                    &program
                        .tasks
                        .get(&input.task_ref)
                        .ok_or("review Task not found")?
                        .artifact_refs
                }
                _ => unreachable!("positive review classification"),
            };
            Some(
                self.independent_result_review(
                    &envelope.actor,
                    &program,
                    results,
                    match &envelope.action {
                        AgentAction::ObjectiveReview(input) => Some(input.criterion_ref.as_str()),
                        _ => None,
                    },
                )
                .await?,
            )
        } else {
            None
        };
        let verification = review.as_ref().map(|review| &review.verification);
        if matches!(
            envelope.action,
            AgentAction::ObjectiveUpdate(_) | AgentAction::ObjectiveReview(_)
        ) {
            let producers = review
                .as_ref()
                .map_or(&[][..], |review| review.producers.as_slice());
            let prepared = self.goal_store().prepare_agentic_objective_action(
                envelope,
                producers,
                verification,
            )?;
            actions
                .apply_with_goal_event(
                    envelope,
                    prepared.stream_id,
                    prepared.expected_stream_revision,
                    prepared.event,
                    verification,
                )
                .map_err(|error| error.to_string())
        } else if let Some(verification) = verification {
            actions
                .apply_verified_review(envelope, verification)
                .map_err(|error| error.to_string())
        } else {
            actions.apply(envelope).map_err(|error| error.to_string())
        }
    }
}
