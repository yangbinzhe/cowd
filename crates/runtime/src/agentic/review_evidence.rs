mod policy;
mod results;
pub(crate) use policy::{
    RootReviewPolicySnapshot, effect_review_snapshot, root_self_review_policy,
};
// Independent review reads existing Runtime receipts; it never mints effects.
use std::collections::{BTreeMap, BTreeSet};

use harness_contract::agent::AgentTaskPacket;
use harness_contract::agent_action::{AgentActorBinding, AgentActorKind};
use harness_contract::goal::{ObjectiveResultReadProof, ObjectiveReviewVerification};
use sha2::{Digest, Sha256};

use super::program::AgenticProgramProjection;
use crate::RuntimeServices;

pub(crate) fn work_manifest_digest(program: &AgenticProgramProjection) -> String {
    // Hash semantic work, not lease renewal or retirement of its workers.
    let teams = program.teams.iter().map(|(id, team)| (id, serde_json::json!({
        "team_id":team.team_id,"name":team.name,"mission":team.mission,"objective":team.objective,
        "topic_ref":team.topic_ref,"created_by":team.created_by,"member_ids":team.member_ids,"task_ids":team.task_ids,
    }))).collect::<BTreeMap<_, _>>();
    let memberships = program.memberships.iter().map(|(id, member)| (id, serde_json::json!({
        "membership_id":member.membership_id,"agent_id":member.agent_id,"team_id":member.team_id,
        "delegation_ref":member.delegation_ref,"created_by":member.created_by,
    }))).collect::<BTreeMap<_, _>>();
    let tasks = program
        .tasks
        .iter()
        .map(|(id, task)| {
            let mut value = serde_json::to_value(task).expect("serializable Task projection");
            let fields = value.as_object_mut().expect("Task object");
            for field in ["claimed_at_ms", "lease_expires_at_ms", "active_attempts"] {
                fields.remove(field);
            }
            if task.purpose == harness_contract::agent_action::TaskPurpose::Exploration {
                for field in [
                    "status",
                    "claimant",
                    "claim_generation",
                    "claim_execution_id",
                    "failed_attempts",
                    "review_generation",
                    "failed_review_attempts",
                    "last_failure",
                    "cancel_requested_by",
                    "cancel_reason_ref",
                    "cancel_evidence_refs",
                    "pending_retirement",
                ] {
                    fields.remove(field);
                }
            }
            (id, value)
        })
        .collect::<BTreeMap<_, _>>();
    let value = serde_json::json!({
        "program": program.program_id, "objective": program.objective_id,
        "continuation": program.continuation, "teams": teams,
        "memberships": memberships, "agents": program.agents,
        "tasks": tasks, "artifacts": program.artifacts,
        "topics": program.topics, "required_team_count": program.required_team_count,
    });
    format!("{:x}", Sha256::digest(value.to_string().as_bytes()))
}

/// The refusal the model sees when an obligation's producer requirement is not
/// met. Pure so the acceptance can assert the model is told WHICH requirement
/// failed: the old text listed all four, so a Task with the right capability
/// and the wrong definition was told both were wrong.
pub(crate) fn unmet_obligation_error(
    obligation_id: &str,
    required: &harness_contract::goal::ObjectiveProducerContract,
) -> String {
    let mut unmet = Vec::new();
    if let Some(capability) = &required.capability_id {
        unmet.push(format!("capability `{capability}`"));
    }
    if let Some(definition) = &required.agent_definition_ref {
        unmet.push(format!("agent definition `{definition}`"));
    }
    if let Some(skill) = &required.skill_ref {
        unmet.push(format!("skill `{skill}`"));
    }
    if let Some(tool) = &required.tool_ref {
        unmet.push(format!("tool `{tool}`"));
    }
    let detail = if unmet.is_empty() {
        "a Runtime-bound producer with an actual tool receipt".to_string()
    } else {
        unmet.join(", ")
    };
    format!(
        "obligation {obligation_id} requires a producer with {detail}, and no delivered result was \
         produced by one. Publish a Task whose claimant satisfies that requirement, or record the \
         gap explicitly rather than asserting it is covered."
    )
}

/// The refusal the model sees when a review cites no current result. Pure so
/// the acceptance can assert the model is told which refs Runtime holds.
pub(crate) fn no_current_results_error(current: &[String]) -> String {
    let detail = if current.is_empty() {
        "no delivered result is accepted yet, so there is nothing to review; publish and accept a \
         Task first"
            .to_string()
    } else {
        format!(
            "the delivered result refs Runtime currently holds are: [{}]. Cite those (or the \
             `task:<id>` that produced them) in result_refs, then read each one with \
             evidence_retrieve before submitting the review",
            current.join(", ")
        )
    };
    format!("independent review has no current results: {detail}")
}

/// Compact only trusted ToolHost output, before the existing receipt owner
/// applies its size bound. The caller still receives the complete original page.
pub(crate) fn compact_read_receipt(output: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(output) else {
        return output.into();
    };
    if value["kind"] != "evidence_retrieve" || value["available"] != true {
        return output.into();
    }
    let Some(chunks) = value["chunks"]
        .as_array()
        .filter(|chunks| chunks.len() <= 16)
    else {
        return output.into();
    };
    let Some(chunks) = chunks
        .iter()
        .map(|chunk| {
            let content = chunk["content"].as_str()?;
            Some(
                serde_json::json!({"index":chunk["index"], "characters":content.chars().count(),
            "content_sha256":format!("{:x}", Sha256::digest(content.as_bytes()))}),
            )
        })
        .collect::<Option<Vec<_>>>()
    else {
        return output.into();
    };
    serde_json::json!({"kind":value["kind"],"available":true,"evidence_ref":value["evidence_ref"],
        "sha256":value["sha256"],"bytes":value["bytes"],"total_chunks":value["total_chunks"],
        "receipt_format":"runtime_chunk_digests_v1", "chunks":chunks})
    .to_string()
}

/// Expand `task:<id>` entries in a review's result refs into that Task's
/// delivered artifacts, and pass everything else through unchanged.
///
/// A model that has just accepted three Tasks naturally reaches for the three
/// refs it was handed. Refusing that costs a review cycle and tells the model
/// nothing about what a "result" is, so the boundary resolves it instead.
/// Artifact refs are already the right shape and are returned as-is, and a task
/// with no artifacts contributes nothing rather than an invented ref.
fn resolve_result_refs(program: &AgenticProgramProjection, result_refs: &[String]) -> Vec<String> {
    let mut resolved = Vec::with_capacity(result_refs.len());
    for reference in result_refs {
        match program.tasks.get(reference) {
            Some(task) => resolved.extend(task.artifact_refs.iter().cloned()),
            None => resolved.push(reference.clone()),
        }
    }
    resolved.sort();
    resolved.dedup();
    resolved
}

pub(crate) struct IndependentReview {
    pub producers: Vec<String>,
    pub verification: ObjectiveReviewVerification,
}

struct ReadCoverage {
    proof: ObjectiveResultReadProof,
    total: Option<usize>,
    chunks: BTreeSet<usize>,
    effect_scopes_remaining: BTreeSet<usize>,
    unscoped_effect: bool,
}

/// Whether a tool result counts as a physical read of a delivered result.
///
/// `read_ref` is the unified facade the primer names as the canonical reader
/// ("Read Runtime facts through one canonical reference with `read_ref`"), and
/// it resolves the same durable receipt as `evidence_retrieve` with the same
/// payload shape, so both count. `"read"` used to sit in this list and is not a
/// tool name at all, so that arm could never match - which left the facade the
/// primer advertises as the one reader that does not count.
fn counts_as_physical_read(tool_name: &str) -> bool {
    matches!(tool_name, "evidence_retrieve" | "read_ref")
}

/// Whether two references name the same object under either spelling.
///
/// Runtime records a one-shot Task deliverable as `artifact:<24 hex>` - the
/// Program-logical key it repeats in the completion gap and in the citable-refs
/// projection - while `read_ref` resolves and echoes it back as
/// `artifact://<24 hex>`. The two are the same object, but a byte comparison of
/// the strings never matches, so a reviewer that read exactly the results it was
/// told to read was refused with `review_requires_current_physical_reads`.
/// `resolve_review_result` already normalises this spelling when it maps the
/// logical key to its selector; the coverage check must use the same rule.
fn same_reference_spelling(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let normalize = |value: &str| {
        value
            .replacen("://", ":", 1)
            .trim_end_matches('/')
            .to_string()
    };
    normalize(left) == normalize(right)
}

impl ReadCoverage {
    fn observe(&mut self, output: &str, receipt_ref: &str) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(output) else {
            return;
        };
        if value["kind"] != "evidence_retrieve"
            || value["available"] != true
            || value["sha256"].as_str() != Some(self.proof.sha256.as_str())
            || value["bytes"].as_u64() != Some(self.proof.bytes)
            || !value["evidence_ref"].as_str().is_some_and(|reference| {
                reference == self.proof.content_ref
                    || (same_reference_spelling(reference, &self.proof.result_ref)
                        && (reference.starts_with("tool://")
                            || reference.starts_with("artifact://")
                            || reference.starts_with("approval:v1:")))
            })
            || receipt_ref.is_empty()
        {
            return;
        }
        let Some(total) = value["total_chunks"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
        else {
            return;
        };
        let Some(chunks) = value["chunks"].as_array() else {
            return;
        };
        if self.total.is_some_and(|expected| expected != total) {
            return;
        }
        let indices = chunks
            .iter()
            .map(|chunk| {
                let index = usize::try_from(chunk["index"].as_u64()?).ok()?;
                let has_content = chunk["content"]
                    .as_str()
                    .is_some_and(|text| !text.is_empty())
                    || (value["receipt_format"] == "runtime_chunk_digests_v1"
                        && chunk["characters"].as_u64().is_some_and(|count| count > 0)
                        && chunk["content_sha256"].as_str().is_some_and(|hash| {
                            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                        }));
                (index < total && has_content).then_some(index)
            })
            .collect::<Option<Vec<_>>>();
        let Some(indices) = indices else {
            return;
        };
        if indices.is_empty() && self.proof.bytes != 0 {
            return;
        }
        self.total = Some(total);
        self.chunks.extend(indices);
        self.proof.receipt_refs.push(receipt_ref.into());
    }

    fn complete(&self) -> bool {
        self.total.is_some_and(|total| self.chunks.len() == total)
            && !self.proof.receipt_refs.is_empty()
    }
}

fn record_effect_observation(
    results: &[results::ResolvedReviewResult],
    coverage: &mut BTreeMap<String, ReadCoverage>,
    cursor: u64,
    scopes: &[harness_contract::policy::PermissionScope],
    reference: &str,
) {
    for result in results {
        if !result.effect_cursor.is_some_and(|effect| cursor > effect) {
            continue;
        }
        if let Some(state) = coverage.get_mut(&result.reference) {
            let before = state.effect_scopes_remaining.len();
            state.effect_scopes_remaining.retain(|index| {
                let effect = &result.effect_scopes[*index];
                !scopes.iter().any(|read| {
                    read.resource == effect.resource
                        && effect.target.is_some()
                        && read.target == effect.target
                })
            });
            if state.effect_scopes_remaining.len() != before {
                state.proof.effect_observation_refs.push(reference.into());
            }
        }
    }
}

/// The review path reads through the durable-verified store, not the hot cache.
/// Event notification precedes the writer's cache publish, so a hot read can miss
/// a graph that exists; a failure produced by that window is timing, not a dead
/// end, and must not be counted as one. This is the read the store's own comment
/// prescribes for a consumer acknowledging a notification.
async fn physical_packet(
    services: &RuntimeServices,
    graph_id: &str,
) -> Result<AgentTaskPacket, String> {
    let graph = services
        .graph_state_store()
        .load_current_async(graph_id.to_string())
        .await
        .map_err(|error| error.to_string())?;
    graph
        .nodes
        .iter()
        .filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
        .filter_map(|node| {
            serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                .ok()
                .filter(|packet| {
                    packet_identity_matches(packet.graph_id(), packet.run_id(), graph_id)
                        && packet.node_id() == node.id
                })
        })
        .next()
        .ok_or_else(|| format!("review physical Agent packet missing:{graph_id}"))
}

/// The identity check in the caller's namespace.
///
/// The caller names an execution id, and there are two binding paths:
/// `agent/in_process/model_loop.rs:196/367` bind the graph id, `:470` binds the physical run
/// id. Comparing a run id against `packet.graph_id()` can never hold, so a packet that was
/// present was reported missing for a run-bound actor - that was the origin of the 158-refusal
/// run. This normalises the namespace; it does not relax the identity check, and a packet from
/// a different graph still never matches.
fn packet_identity_matches(
    packet_graph_id: &str,
    packet_run_id: &str,
    caller_execution_id: &str,
) -> bool {
    if caller_execution_id.starts_with("run:") {
        packet_run_id == caller_execution_id
    } else {
        packet_graph_id == caller_execution_id
    }
}

/// Why a TaskReview-scoped physical attempt cannot validate this review.
///
/// Returns the specific reason, or `Ok(())` when every condition holds. The
/// three conditions used to collapse into one sentence - "review physical
/// attempt is stale or owns different results" - which named neither the
/// generation, the attempt, nor the refs, so a reviewer that had done everything
/// right could not act on it and cycled until Runtime settled the attempt.
///
/// The third condition is Task-review-only. An Objective review is
/// criterion-scoped and legitimately cites the delivered results of several
/// Tasks at once: with two or more accepted Tasks the criterion's result set
/// necessarily spans them, so applying the single-Task rule to an objective
/// review made every objective review of real multi-Task work impossible.
fn task_review_attempt_rejection(
    task: &crate::agentic::program::AgenticTaskProjection,
    graph_id: &str,
    attempt: u32,
    agent_id: &str,
    membership_id: &str,
    result_refs: &[String],
    condition_ref: Option<&str>,
) -> Result<(), String> {
    if task.review_generation.saturating_add(1) != u64::from(attempt) {
        return Err(format!(
            "review attempt generation is stale: Task {} is at review generation {} and this \
             physical run is attempt {}; inspect the Task and continue from its current revision",
            task.task_id, task.review_generation, attempt
        ));
    }
    let active = task.active_attempts.get(graph_id).is_some_and(|candidate| {
        candidate.mode == harness_contract::agent_action::AgentAttemptMode::Review
            && candidate.generation.saturating_add(1) == u64::from(attempt)
            && candidate.agent_id == agent_id
            && candidate.membership_id == membership_id
    });
    if !active {
        return Err(format!(
            "review attempt is not the active Review attempt for Task {}: Runtime has no Review \
             attempt at generation {} bound to this graph, agent and membership; re-read the \
             Task and let Runtime dispatch the current attempt",
            task.task_id, attempt
        ));
    }
    if condition_ref.is_none()
        && !result_refs
            .iter()
            .all(|reference| task.artifact_refs.contains(reference))
    {
        // Name the legal refs, not just the rule. Runtime reads `task.artifact_refs`
        // in this very condition, so it already knows the answer; the old message
        // named the Task and the rule and made the model guess the refs. Run one28
        // guessed eighteen times, each one refused with `state_derived: true` - a
        // cause the model could not have put in its own input, which is precisely
        // the case where the framework has to supply it.
        let legal = if task.artifact_refs.is_empty() {
            "this Task has no delivered artifact yet; submit or inspect it first".to_string()
        } else {
            task.artifact_refs.join(", ")
        };
        let shape = if task.artifact_refs.is_empty() {
            String::new()
        } else {
            format!(
                " A conforming call is {{\"task_ref\": \"{}\", \"decision\": \"accept\", \
                 \"reason\": \"...\", \"evidence_refs\": [{}]}}.",
                task.task_id,
                task.artifact_refs
                    .iter()
                    .map(|reference| format!("\"{reference}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        return Err(format!(
            "task review cites results outside Task {}: a Task review covers only that Task's \
             delivered artifacts. This Task's legal result refs are: {legal}.{shape} Cite those, \
             or settle the criterion with objective_review.",
            task.task_id
        ));
    }
    Ok(())
}

/// Whether a tool read belongs to the root actor itself.
///
/// The root's own reads are owned by the root execution and Runtime carries them
/// under whichever agentic-graph run happens to be executing the root's step at
/// that moment - one live run's 125 root tool calls were attributed to eight
/// different graphs, interleaved in time. Ownership, not the presence of a run
/// id, is therefore what identifies the root's own read: a delegated Agent's read
/// is not owned by the root execution and must stay invisible to it.
fn binding_is_owned_by_root(
    binding: &harness_contract::projection::RuntimeActivityBinding,
    root_execution_id: Option<&str>,
) -> bool {
    root_execution_id.is_some_and(|root| {
        binding
            .activity_id
            .starts_with(&format!("activity:execution:{root}:owner:"))
    })
}

impl RuntimeServices {
    pub(crate) async fn independent_result_review(
        &self,
        actor: &AgentActorBinding,
        program: &AgenticProgramProjection,
        result_refs: &[String],
        condition_ref: Option<&str>,
    ) -> Result<IndependentReview, String> {
        // A review names the delivered results it covers, and a model routinely
        // names the Task that produced them instead: `task:<id>` is work, not a
        // result, so the coverage map came back empty and the review was refused
        // with "independent review has no current results" while the work was in
        // fact delivered and accepted. The mapping is unambiguous - a Task's
        // `artifact_refs` are exactly its delivered results - so resolve it here
        // rather than making the model guess the spelling.
        let result_refs = resolve_result_refs(program, result_refs);
        let result_refs = result_refs.as_slice();
        let root = actor
            .root_execution_id
            .as_deref()
            .ok_or("review has no root execution")?;
        let goal = self.goal_store().get(&format!("goal:{root}"))?;
        let packet = if matches!(actor.kind, AgentActorKind::Agent | AgentActorKind::TeamLead) {
            let packet = physical_packet(
                self,
                actor
                    .execution_id
                    .as_deref()
                    .ok_or("review has no physical run")?,
            )
            .await?;
            let scope = packet
                .agentic_binding
                .as_ref()
                .ok_or("review packet has no Program binding")?;
            if scope.program_id != actor.program_id
                || Some(scope.agent_id.as_str()) != actor.agent_id.as_deref()
                || Some(scope.team_id.as_str()) != actor.team_id.as_deref()
                || packet.session_id() != actor.session_id
            {
                return Err("review physical run does not match actor scope".into());
            }
            Some(packet)
        } else {
            None
        };
        if let Some(packet) = &packet {
            let scope = packet.agentic_binding.as_ref().unwrap();
            let binding = packet
                .binding
                .as_ref()
                .ok_or("review execution has no compiled binding")?;
            if let harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } =
                &scope.focus
            {
                let task = program
                    .tasks
                    .get(task_ref)
                    .ok_or("review Task disappeared")?;
                task_review_attempt_rejection(
                    task,
                    packet.graph_id(),
                    packet.attempt,
                    &scope.agent_id,
                    &scope.membership_id,
                    result_refs,
                    condition_ref,
                )?;
            }

            if actor.agent_id.as_deref() != Some(actor.actor_id.as_str())
                || !program.agent_is_active_in(&scope.agent_id, &scope.team_id)
                || !program
                    .membership_for(&scope.agent_id, &scope.team_id)
                    .is_some_and(|member| member.membership_id == scope.membership_id)
                || !crate::agent::binding::recompute_binding_digest(binding)
                    .is_ok_and(|digest| digest == binding.binding_digest)
            {
                return Err("review execution binding or membership is stale".into());
            }
        }
        let reviewer_execution_id = packet.as_ref().map_or_else(
            || format!("root-execution:{root}"),
            |packet| format!("agent-run:{}", packet.assignment.run_id),
        );
        let mut producers = BTreeSet::new();
        let mut producer_execution_ids = BTreeSet::new();
        let mut coverage = BTreeMap::new();
        let mut producer_packets = Vec::new();
        let mut result_kinds = BTreeSet::new();
        let mut resolved = Vec::new();
        for reference in result_refs {
            resolved.push(results::resolve_result(self, actor, program, reference).await?);
        }
        let policy = if actor.kind == AgentActorKind::Root && condition_ref.is_some() {
            match goal.as_ref() {
                Some(goal) => root_self_review_policy(self.event_store(), program, goal)?,
                None => None,
            }
        } else {
            None
        };
        let independence_required = policy.is_none();
        let effect_source_refs = resolved
            .iter()
            .flat_map(|result| &result.producers)
            .filter_map(|producer| producer.packet.as_ref())
            .map(|packet| {
                format!(
                    "execution-agent-receipts:{}:{}:{}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                )
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let effects = if condition_ref.is_some()
            || !effect_source_refs.is_empty()
            || resolved
                .iter()
                .any(|result| result.kind == harness_contract::goal::GoalResultKind::ToolEffect)
        {
            Some(effect_review_snapshot(
                self.event_store(),
                program,
                goal.as_ref(),
                &effect_source_refs,
            )?)
        } else {
            None
        };

        if let Some(effects) = &effects {
            if resolved
                .iter()
                .flat_map(|result| &result.effect_sources)
                .any(|source| {
                    !std::iter::once(&effects.source)
                        .chain(&effects.additional_sources)
                        .any(|current| {
                            current.stream_id == source.stream_id
                                && current.expected_revision == source.expected_revision
                        })
                })
            {
                return Err(
                    "producer effects changed during result resolution; retry current results"
                        .into(),
                );
            }
            for result in &mut resolved {
                if let Some(cursor) = result.effect_cursor.as_mut() {
                    *cursor = (*cursor).max(effects.last_effect_cursor);
                }
            }
        }
        for result in &resolved {
            for producer in &result.producers {
                if independence_required
                    && (producer.execution == reviewer_execution_id
                        || producer.actor == actor.actor_id)
                {
                    return Err(
                        "review producer and reviewer must be different physical executions".into(),
                    );
                }
                producers.insert(producer.actor.clone());
                producer_execution_ids.insert(producer.execution.clone());
                if let Some(packet) = &producer.packet {
                    producer_packets.push(packet.clone());
                }
            }
            result_kinds.extend(result.artifact_kinds.iter().cloned());
            coverage.insert(
                result.reference.clone(),
                ReadCoverage {
                    proof: ObjectiveResultReadProof {
                        result_kind: result.kind,
                        effect_observation_refs: vec![],
                        result_ref: result.reference.clone(),
                        content_ref: result.content.selector.clone(),
                        sha256: result.content.sha256.clone(),
                        bytes: result.content.bytes,
                        receipt_refs: vec![],
                    },
                    total: None,
                    chunks: BTreeSet::new(),
                    effect_scopes_remaining: (0..result.effect_scopes.len()).collect(),
                    unscoped_effect: result.unscoped_effect,
                },
            );
        }
        if let Some(obligation) = goal.as_ref().and_then(|goal| {
            goal.obligations
                .iter()
                .find(|obligation| Some(obligation.obligation_id.as_str()) == condition_ref)
        }) {
            if obligation
                .evidence_requirement
                .required_artifact_kinds
                .iter()
                .any(|kind| !result_kinds.contains(kind.as_str()))
            {
                return Err(
                    "obligation result kinds do not cover the original evidence requirement".into(),
                );
            }
            let required = &obligation.producer;
            let requires_bound_producer = required.capability_id.is_some()
                || required.agent_definition_ref.is_some()
                || required.skill_ref.is_some()
                || required.tool_ref.is_some();
            let mut matched = !requires_bound_producer;
            if required.agent_definition_ref.is_none() && required.skill_ref.is_none() {
                matched |= resolved
                    .iter()
                    .flat_map(|result| &result.producers)
                    .any(|producer| {
                        producer.packet.is_none()
                            && producer.tool.as_ref().is_some_and(|(name, effect)| {
                                required
                                    .tool_ref
                                    .as_ref()
                                    .is_none_or(|required| required == name)
                                    && required.capability_id.as_ref().is_none_or(|required| {
                                        match effect {
                                            harness_contract::tool::ToolEffectKind::Read => {
                                                required == "read"
                                            }
                                            harness_contract::tool::ToolEffectKind::Write => {
                                                required == "write"
                                            }
                                            _ => false,
                                        }
                                    })
                            })
                    });
            }

            for producer in &producer_packets {
                let Some(binding) = &producer.binding else {
                    continue;
                };
                if !crate::agent::binding::recompute_binding_digest(binding)
                    .is_ok_and(|digest| digest == binding.binding_digest)
                {
                    continue;
                }
                if required.capability_id.as_ref().is_some_and(|required| {
                    !binding
                        .effective_capabilities
                        .iter()
                        .any(|capability| capability.as_str() == required)
                }) || required
                    .agent_definition_ref
                    .as_ref()
                    .is_some_and(|required| {
                        required != binding.definition_ref.definition_id.as_str()
                            && required
                                != &format!(
                                    "{}@{}",
                                    binding.definition_ref.definition_id.as_str(),
                                    binding.definition_ref.revision
                                )
                    })
                    || required
                        .skill_ref
                        .as_ref()
                        .is_some_and(|required| !binding.skill_refs.contains(required))
                {
                    continue;
                }
                if let Some(tool) = &required.tool_ref {
                    if !binding.tool_contract_refs.contains(tool) {
                        continue;
                    }
                    let receipts = self
                        .commit_service()
                        .load_delegated_agent_tool_receipts(
                            producer.graph_id(),
                            producer.node_id(),
                            producer.attempt,
                        )
                        .map_err(|error| error.to_string())?;
                    if !receipts.iter().any(|receipt| {
                        receipt.outcome.tool_name == *tool
                            && receipt.outcome.status == crate::RuntimeToolExecutionStatus::Executed
                    }) {
                        continue;
                    }
                }
                matched = true;
                break;
            }
            if !matched {
                // Name the requirement that was not met. The old message listed
                // all four requirements, so a model whose Task had the right
                // capability but the wrong definition was told both were wrong
                // and had no way to tell which.
                return Err(unmet_obligation_error(&obligation.obligation_id, required));
            }
        }
        if coverage.is_empty() {
            // Name what IS current. The old message said the review had no
            // current results and stopped; a model that had just named three
            // real receipts was told its evidence did not exist, with no list of
            // what Runtime considered current and no spelling to try.
            let current =
                crate::agentic::supervision::completion_ready_result_artifact_refs(program);
            return Err(no_current_results_error(&current));
        }
        if let Some(packet) = packet {
            // A read is a fact about this Agent node, not about one dispatch of it.
            // The per-attempt stream made a re-dispatched reviewer start from zero
            // reads, so it was refused for "not having read" bodies it had already
            // read, and each refusal burned the retry budget until the attempt
            // settled. Union the node attempts; observe still verifies every payload
            // sha256 and bytes, so a stale body cannot pass, and receipts from any
            // other Agent node are still excluded.
            for receipt in self
                .commit_service()
                .load_delegated_agent_tool_receipts_all_attempts(
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt,
                )
                .map_err(|error| error.to_string())?
            {
                if receipt.outcome.status == crate::RuntimeToolExecutionStatus::Executed
                    && receipt.effect_kind == harness_contract::tool::ToolEffectKind::Read
                    && receipt.outcome.tool_name != "evidence_retrieve"
                {
                    if let Some(scope) = receipt.effect_scope.as_ref() {
                        record_effect_observation(
                            &resolved,
                            &mut coverage,
                            receipt.committed_cursor,
                            std::slice::from_ref(scope),
                            &format!(
                                "execution-agent-receipts:{}:{}:{}#{}",
                                packet.graph_id(),
                                packet.node_id(),
                                packet.attempt,
                                receipt.sequence
                            ),
                        );
                    }
                }
                // `read_ref` is the unified facade the primer names as THE
                // canonical reader, and it resolves the same durable receipt as
                // `evidence_retrieve` with the same payload shape. Refusing it
                // here told a reviewer to read the delivered results and then
                // refused the review that followed those reads: one live run
                // cycled `read_ref` -> `objective_review` ->
                // `review_requires_current_physical_reads` until the window
                // expired, while the identical read through `evidence_retrieve`
                // would have closed the coverage.
                if !counts_as_physical_read(&receipt.outcome.tool_name)
                    || receipt.outcome.status != crate::RuntimeToolExecutionStatus::Executed
                {
                    continue;
                }
                if let Some(output) = receipt.outcome.output.as_deref() {
                    let reference = format!(
                        "execution-agent-receipts:{}:{}:{}#{}",
                        packet.graph_id(),
                        packet.node_id(),
                        packet.attempt,
                        receipt.sequence
                    );
                    for state in coverage.values_mut() {
                        state.observe(output, &reference);
                    }
                }
            }
        } else {
            let mut position = None;
            loop {
                let events = self.event_store().events_for_root_execution_kind(
                    root,
                    "tool.invocation.completed",
                    position,
                    64,
                )?;
                if events.is_empty() {
                    break;
                }
                for event in events {
                    position = Some((event.commit_cursor, event.transaction_index));
                    let Some(binding) = event.activity_binding() else {
                        continue;
                    };
                    if binding.session_id != actor.session_id
                        || binding.turn_id != actor.turn_id
                        || event.payload["status"] != "completed"
                    {
                        continue;
                    }
                    // The root's own reads are owned by the root execution, and
                    // Runtime carries them under whichever agentic-graph run is
                    // executing the root's step at that moment. One live run's
                    // 125 root tool calls were attributed to eight different
                    // graphs, interleaved in time, so the old blanket skip for
                    // "any agent_run_id" discarded every one of them and the
                    // root could never satisfy its own read coverage.
                    //
                    // What must still be excluded is another Agent's read. Those
                    // are not owned by the root execution, so ownership is the
                    // discriminator - not the presence of a run id.
                    if !binding_is_owned_by_root(&binding, actor.root_execution_id.as_deref()) {
                        continue;
                    }
                    // `read_ref` is the unified facade; a tool:// or artifact:// read
                    // through it resolves the same durable receipt as
                    // `evidence_retrieve`, so it must count as a physical read.
                    // Non-evidence `read_ref` calls simply have no full_output_ref
                    // and are skipped below.
                    // `read_ref` is the unified facade; a tool:// or artifact:// read
                    // through it resolves the same durable receipt as
                    // `evidence_retrieve`, so it must count as a physical read.
                    // Non-evidence `read_ref` calls simply have no full_output_ref
                    // and are skipped below. (`"read"` was never a tool name; the
                    // facade is `read_ref`, so that arm could not match anything.)
                    if !event.payload["tool_name"]
                        .as_str()
                        .is_some_and(counts_as_physical_read)
                    {
                        if let Some(effect) = results::tool_effect(self, &event)? {
                            if effect.effect_kind == harness_contract::tool::ToolEffectKind::Read {
                                record_effect_observation(
                                    &resolved,
                                    &mut coverage,
                                    event.commit_cursor,
                                    &effect.scopes,
                                    &format!("runtime-event:{}", event.event_id),
                                );
                            }
                        }
                        continue;
                    }
                    let Some(reference) = event.payload["full_output_ref"]
                        .as_str()
                        .and_then(|reference| reference.strip_prefix("tool://"))
                    else {
                        continue;
                    };
                    let Some(access) = self
                        .session_evidence_access(&actor.session_id, reference)
                        .await
                        .map_err(|error| error.to_string())?
                    else {
                        continue;
                    };
                    if access.bytes > 256 * 1024 {
                        continue;
                    }
                    let raw = self
                        .artifact_store()
                        .resolve(&access.retrieval_selector)
                        .map_err(|error| {
                            // Name the receipt and the selector. This is the exact
                            // shape that cost run one30 its whole window: the model
                            // cited a real receipt, the selector did not resolve,
                            // and it was told "artifact does not exist" with nothing
                            // it could act on.
                            results::unresolvable_receipt_error(
                                "review evidence receipt tool://",
                                reference,
                                &access.retrieval_selector,
                                &error.to_string(),
                                "re-read it with evidence_retrieve and cite the reference that call \
                                 returns",
                            )
                        })?;
                    if raw.sha256 != access.sha256 || raw.bytes != access.bytes {
                        return Err("review raw receipt integrity mismatch".into());
                    }
                    let bytes = self
                        .artifact_store()
                        .read(&raw, &raw.visibility_scope, None)
                        .await
                        .map_err(|error| {
                            results::unreadable_receipt_error(
                                "review evidence receipt tool://",
                                reference,
                                &raw.selector,
                                &error.to_string(),
                                "re-read it with evidence_retrieve and cite the reference that call \
                                 returns",
                            )
                        })?;
                    let Ok(output) = std::str::from_utf8(&bytes) else {
                        continue;
                    };
                    for state in coverage.values_mut() {
                        state.observe(output, &format!("runtime-event:{}", event.event_id));
                    }
                }
                if coverage.values().all(|state| {
                    state.complete()
                        && (state.proof.result_kind
                            != harness_contract::goal::GoalResultKind::ToolEffect
                            || (!state.proof.effect_observation_refs.is_empty()
                                && state.effect_scopes_remaining.is_empty()
                                && !state.unscoped_effect))
                }) {
                    break;
                }
            }
        }
        // If the runtime already handed this reviewer the submitted bodies (a
        // durable `agentic.review_body_prefetched` attestation for the same
        // reviewer execution), the physical-read coverage is satisfied without a
        // separate tool read.
        let body_prefetched = self.review_body_prefetched(&actor, program);
        let missing = if body_prefetched {
            Vec::new()
        } else {
            coverage
                .values()
                .filter(|state| !state.complete())
                .map(|state| state.proof.result_ref.clone())
                .collect::<Vec<_>>()
        };
        if !missing.is_empty() {
            // Name WHO must read. The old text said only "use evidence_retrieve",
            // which a model satisfied by having some other Agent - or itself in an
            // earlier attempt - read the results, then submitted the review from a
            // different execution and was refused again. One live run cycled this
            // five times across three Agents. Coverage is scoped to the reviewing
            // execution, so the instruction has to say so.
            return Err(format!(
                "review_requires_current_physical_reads:{}; the Agent that submits this \
                 objective_review must itself read each of these results in THIS execution \
                 (evidence_retrieve or read_ref, following every next_request to the end). \
                 A read by another Agent, by the root, or by an earlier attempt of this \
                 Agent does not count.",
                missing.join(",")
            ));
        }
        let unobserved = coverage
            .values()
            .filter(|state| {
                state.proof.result_kind == harness_contract::goal::GoalResultKind::ToolEffect
                    && (state.proof.effect_observation_refs.is_empty()
                        || !state.effect_scopes_remaining.is_empty()
                        || state.unscoped_effect)
            })
            .map(|state| state.proof.result_ref.as_str())
            .collect::<Vec<_>>();
        if !unobserved.is_empty() {
            return Err(format!(
                "review_requires_effect_observation:{}; independently read the actual affected target after the effect",
                unobserved.join(",")
            ));
        }
        Ok(IndependentReview {
            producers: producers.into_iter().collect(),
            verification: ObjectiveReviewVerification {
                result_source_revisions: resolved
                    .iter()
                    .flat_map(|result| &result.result_sources)
                    .map(|source| (source.stream_id.clone(), source.expected_revision))
                    .collect(),
                independence_required,
                review_policy_digest: policy.map(|policy| policy.digest),
                effect_manifest_digest: effects.map(|effects| effects.digest),
                effect_source_refs,
                goal_spec_revision: goal.as_ref().map(|goal| goal.spec_revision),
                goal_spec_digest: goal.as_ref().map(|goal| goal.spec_digest.clone()),
                work_manifest_digest: work_manifest_digest(program),
                reviewer_execution_id,
                producer_execution_ids: producer_execution_ids.into_iter().collect(),
                reads: coverage
                    .into_values()
                    .map(|mut state| {
                        state.proof.receipt_refs.sort();
                        state.proof.receipt_refs.dedup();
                        state.proof
                    })
                    .collect(),
            },
        })
    }

    /// True when the runtime handed this reviewer the submitted bodies for its
    /// exact review execution (a durable prefetch attestation), which satisfies
    /// the physical-read requirement without a separate tool read.
    fn review_body_prefetched(
        &self,
        actor: &harness_contract::agent_action::AgentActorBinding,
        program: &crate::AgenticProgramProjection,
    ) -> bool {
        let stream_id = format!("agentic-program:{}", program.program_id);
        let Ok(events) = self.event_store().list_stream(&stream_id) else {
            return false;
        };
        events.iter().any(|event| {
            event.kind == "agentic.review_body_prefetched"
                && event
                    .payload
                    .get("program_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(program.program_id.as_str())
                && event
                    .payload
                    .get("execution_id")
                    .and_then(serde_json::Value::as_str)
                    == actor.execution_id.as_deref()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_names_which_producer_requirement_was_not_met() {
        // V27-B2: the old text listed all four requirements, so a Task with
        // the right capability and the wrong definition was told both were
        // wrong. The model must be able to tell which one to fix.
        let required = harness_contract::goal::ObjectiveProducerContract {
            capability_id: Some("capability:reader".into()),
            agent_definition_ref: Some("definition:reviewer".into()),
            skill_ref: None,
            tool_ref: None,
        };
        let refusal = unmet_obligation_error("obligation:1", &required);
        assert!(
            refusal.contains("capability `capability:reader`"),
            "{refusal}"
        );
        assert!(
            refusal.contains("agent definition `definition:reviewer`"),
            "{refusal}"
        );
        // A requirement that was not set is not named as unmet.
        assert!(!refusal.contains("skill `"), "{refusal}");
        assert!(!refusal.contains("tool `"), "{refusal}");
        // And the way out is stated.
        assert!(refusal.contains("Publish a Task"), "{refusal}");
    }

    #[test]
    fn a_refusal_lists_the_result_refs_runtime_currently_holds() {
        // V27-B1: a model that had just named three real receipts was told its
        // evidence did not exist. It must be told what IS current.
        let with_refs = no_current_results_error(&[
            "artifact://1111".to_string(),
            "artifact://2222".to_string(),
        ]);
        assert!(with_refs.contains("artifact://1111"), "{with_refs}");
        assert!(with_refs.contains("artifact://2222"), "{with_refs}");
        assert!(with_refs.contains("evidence_retrieve"), "{with_refs}");
        let empty = no_current_results_error(&[]);
        assert!(empty.contains("publish and accept a Task first"), "{empty}");
        assert!(!empty.contains("artifact://"), "{empty}");
    }

    fn coverage() -> ReadCoverage {
        ReadCoverage {
            proof: ObjectiveResultReadProof {
                result_kind: harness_contract::goal::GoalResultKind::Artifact,
                effect_observation_refs: vec![],
                result_ref: "result:1".into(),
                content_ref: "artifact://current".into(),
                sha256: "current-hash".into(),
                bytes: 3001,
                receipt_refs: vec![],
            },
            total: None,
            chunks: BTreeSet::new(),
            effect_scopes_remaining: BTreeSet::new(),
            unscoped_effect: false,
        }
    }

    fn page(index: usize) -> serde_json::Value {
        serde_json::json!({"kind":"evidence_retrieve", "available":true,
            "evidence_ref":"artifact://current", "sha256":"current-hash", "bytes":3001,
            "total_chunks":3, "chunks":[{"index":index,"content":"actual page"}]})
    }

    /// A Task with two delivered artifacts and one active Review attempt at
    /// generation 1 bound to `graph` / `agent:1` / `membership:1`.
    fn reviewed_task() -> crate::agentic::program::AgenticTaskProjection {
        let mut task = crate::agentic::program::AgenticTaskProjection {

                task_id: "task".to_string(),
                team_id: "team".to_string(),
                title: "long task".to_string(),
                objective: "finish without an arbitrary wall timeout".to_string(),
                acceptance: "durable evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                required_capabilities: Vec::new(),
                depends_on: Vec::new(),
                status: crate::agentic::program::AgenticTaskStatus::Published,
                claimant: None,
                claim_generation: 0,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                active_attempts: Default::default(),
                artifact_refs: Vec::new(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
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
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
        };
        task.review_generation = 1;
        task.artifact_refs = vec!["artifact:a".to_string(), "artifact:b".to_string()];
        task.active_attempts.insert(
            "graph".to_string(),
            crate::agentic::program::AgenticTaskAttemptProjection {
                execution_id: "execution:1".to_string(),
                agent_id: "agent:1".to_string(),
                membership_id: "membership:1".to_string(),
                mode: harness_contract::agent_action::AgentAttemptMode::Review,
                generation: 1,
            },
        );
        task
    }

    #[test]
    fn an_objective_review_may_cite_results_across_tasks() {
        let task = reviewed_task();
        // The fix this pins: the single-Task artifact rule used to apply to every
        // review, so an objective review of real multi-Task work was refused with
        // "stale or owns different results" while generation, attempt and reads
        // were all correct. A criterion-scoped review legitimately cites the
        // delivered results of several Tasks.
        assert!(
            task_review_attempt_rejection(
                &task,
                "graph",
                2,
                "agent:1",
                "membership:1",
                &["artifact:a".to_string(), "artifact:z".to_string()],
                Some("user_intent"),
            )
            .is_ok(),
            "an Objective review is criterion-scoped, not Task-scoped"
        );
        // The same refs under a Task review are still refused, with a reason that
        // names the rule instead of collapsing three conditions into one sentence.
        let refusal = task_review_attempt_rejection(
            &task,
            "graph",
            2,
            "agent:1",
            "membership:1",
            &["artifact:a".to_string(), "artifact:z".to_string()],
            None,
        )
        .expect_err("a Task review covers only its own Task's artifacts");
        assert!(
            refusal.contains("covers only that Task's"),
            "the refusal must name the rule: {refusal}"
        );
        // The rule is not enough. Runtime reads `task.artifact_refs` in the same
        // condition that produced this refusal, so it already knows the legal set;
        // run one28 was told the rule eighteen times and guessed eighteen times.
        // The refusal has to hand over the answer.
        assert!(
            refusal.contains("artifact:a"),
            "the refusal must name this Task's legal artifact refs: {refusal}"
        );
        assert!(
            refusal.contains("\"evidence_refs\""),
            "the refusal must show a conforming input shape: {refusal}"
        );
    }

    #[test]
    fn a_stale_review_attempt_is_refused_with_its_own_reason() {
        let task = reviewed_task();
        let stale_generation = task_review_attempt_rejection(
            &task,
            "graph",
            5,
            "agent:1",
            "membership:1",
            &["artifact:a".to_string()],
            None,
        )
        .expect_err("generation 5 cannot review a Task at review generation 1");
        assert!(
            stale_generation.contains("generation is stale"),
            "the refusal must name the generation: {stale_generation}"
        );

        let wrong_agent = task_review_attempt_rejection(
            &task,
            "graph",
            2,
            "agent:9",
            "membership:1",
            &["artifact:a".to_string()],
            None,
        )
        .expect_err("another agent's attempt is not this attempt");
        assert!(
            wrong_agent.contains("not the active Review attempt"),
            "the refusal must name the attempt: {wrong_agent}"
        );

        assert!(task_review_attempt_rejection(
            &task,
            "graph",
            2,
            "agent:1",
            "membership:1",
            &["artifact:a".to_string()],
            None,
        )
        .is_ok());
    }

    #[test]
    fn a_read_of_the_logical_key_counts_as_a_read_of_the_result() {
        // Runtime records a one-shot Task deliverable as `artifact:<24 hex>` and
        // repeats that spelling in the completion gap and the citable-refs
        // projection, while `read_ref` resolves and echoes it back as
        // `artifact://<24 hex>`. A byte comparison never matched, so a reviewer
        // that read exactly the results it was told to read was refused.
        assert!(same_reference_spelling(
            "artifact://4541b02b47dc2bca34a7f0f7",
            "artifact:4541b02b47dc2bca34a7f0f7"
        ));
        assert!(same_reference_spelling(
            "artifact:4541b02b47dc2bca34a7f0f7",
            "artifact://4541b02b47dc2bca34a7f0f7"
        ));
        assert!(same_reference_spelling(
            "tool://tool-raw-chatcmpl-tool-abc",
            "tool:tool-raw-chatcmpl-tool-abc"
        ));
        // Different objects stay different: the normalisation must not become a
        // wildcard that lets any read satisfy any result.
        assert!(!same_reference_spelling(
            "artifact://4541b02b47dc2bca34a7f0f7",
            "artifact:64506e38bc17f2a2ae97c06e"
        ));
        assert!(!same_reference_spelling(
            "tool://tool-raw-chatcmpl-tool-abc",
            "artifact://art_2387b74c32b348d9b3d2b17e69f8718a"
        ));
        // An exact match still passes, and a trailing slash is not a difference.
        assert!(same_reference_spelling("artifact://x", "artifact://x"));
        assert!(same_reference_spelling("artifact://x/", "artifact://x"));
    }

    #[test]
    fn the_roots_own_reads_count_whatever_graph_carried_them() {
        // Runtime attributes the root's tool calls to whichever agentic-graph run
        // is executing the root's step. One live run's 125 root tool calls were
        // spread over eight different graphs, interleaved in time, so a filter
        // keyed on the run id discarded every one of them and the root could
        // never satisfy its own read coverage.
        let root = "session-ingress-graph:root-1";
        let binding = |activity_id: &str| harness_contract::projection::RuntimeActivityBinding {
            activity_id: activity_id.to_string(),
            parent_activity_id: None,
            initiator_activity_id: None,
            agent_run_id: Some("run:agentic-graph:whatever".to_string()),
            agent_instance_id: None,
            root_execution_id: root.to_string(),
            root_task_id: "task:root:1".to_string(),
            task_id: "task:1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            node_id: None,
            team_run_id: None,
            skill_id: None,
            skill_revision: None,
            skill_activation_id: None,
            tool_contract_id: None,
            tool_call_id: None,
            approval_id: None,
            parallel_group_id: None,
            revision: 1,
            fence: 1,
            generation: 1,
        };
        for graph in ["aaa", "bbb", "ccc"] {
            assert!(
                binding_is_owned_by_root(
                    &binding(&format!("activity:execution:{root}:owner:run:agentic-graph:{graph}:tool:x")),
                    Some(root)
                ),
                "the root's own read counts under any graph: {graph}"
            );
        }
        // A delegated Agent's read is not owned by the root execution and must
        // stay invisible: independence is about who read it.
        assert!(
            !binding_is_owned_by_root(
                &binding("activity:execution:agentic-graph:delegated:node:agent"),
                Some(root)
            ),
            "another Agent's read must not count for the root"
        );
        // An actor with no root execution has nothing to match against.
        assert!(!binding_is_owned_by_root(
            &binding(&format!("activity:execution:{root}:owner:run:agentic-graph:aaa")),
            None
        ));
    }

    #[test]
    fn the_facade_the_primer_advertises_counts_as_a_physical_read() {
        // The primer names `read_ref` as THE canonical reader. It used to be
        // absent from this predicate (the list held `"read"`, which is not a
        // tool name), so a reviewer that followed the primer and then reviewed
        // was told its reads did not count. One live run cycled
        // `read_ref` -> `objective_review` -> `review_requires_current_physical_reads`
        // until the window expired; the identical read through
        // `evidence_retrieve` closed the coverage.
        assert!(counts_as_physical_read("read_ref"));
        assert!(counts_as_physical_read("evidence_retrieve"));
        // A capability label is not a tool name and must never satisfy the gate.
        assert!(!counts_as_physical_read("read"));
        // Ordinary workspace reads are not reads of a delivered result.
        assert!(!counts_as_physical_read("read_file"));
        assert!(!counts_as_physical_read("grep_search"));
        assert!(!counts_as_physical_read("state_inspect"));
    }

    #[test]
    fn historical_review_proofs_default_to_independent_verification() {
        let value = serde_json::json!({"goal_spec_revision":1,"goal_spec_digest":"spec",
            "work_manifest_digest":"work","reviewer_execution_id":"reviewer",
            "producer_execution_ids":["producer"],"reads":[{"result_ref":"result",
            "content_ref":"artifact://content","sha256":"hash","bytes":1,"receipt_refs":["read"]}]});
        let proof: ObjectiveReviewVerification = serde_json::from_value(value).unwrap();
        assert!(proof.independence_required);
        assert!(proof.review_policy_digest.is_none());
        assert_eq!(
            proof.reads[0].result_kind,
            harness_contract::goal::GoalResultKind::Artifact
        );
    }

    #[test]
    fn effect_observations_require_later_reads_of_every_actual_target() {
        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        let scope = |target: &str| {
            let mut scope =
                PermissionScope::new(PermissionResource::File, PermissionOperation::Read);
            scope.target = Some(target.into());
            scope
        };
        let mut state = coverage();
        state.effect_scopes_remaining = BTreeSet::from([0, 1]);
        let mut coverage = BTreeMap::from([("result:1".into(), state)]);
        let results = vec![results::ResolvedReviewResult {
            result_sources: vec![],
            reference: "result:1".into(),
            content: harness_contract::context::ArtifactRef::durable(
                "artifact://content",
                "hash",
                1,
                "text/plain",
                "public",
            ),
            kind: harness_contract::goal::GoalResultKind::ToolEffect,
            artifact_kinds: BTreeSet::new(),
            producers: vec![],
            effect_cursor: Some(10),
            effect_sources: Vec::new(),
            effect_scopes: vec![scope("a"), scope("b")],
            unscoped_effect: false,
        }];
        record_effect_observation(&results, &mut coverage, 10, &[scope("a")], "same-commit");
        record_effect_observation(
            &results,
            &mut coverage,
            11,
            &[scope("other")],
            "wrong-target",
        );
        assert!(
            coverage["result:1"]
                .proof
                .effect_observation_refs
                .is_empty()
        );
        record_effect_observation(&results, &mut coverage, 12, &[scope("a")], "read-a");
        assert_eq!(
            coverage["result:1"].effect_scopes_remaining,
            BTreeSet::from([1])
        );
        record_effect_observation(&results, &mut coverage, 13, &[scope("b")], "read-b");
        assert!(coverage["result:1"].effect_scopes_remaining.is_empty());
        assert_eq!(
            coverage["result:1"].proof.effect_observation_refs,
            ["read-a", "read-b"]
        );
    }

    #[test]
    fn current_hash_pages_must_cover_every_chunk() {
        let mut read = coverage();
        read.observe(&page(2).to_string(), "receipt:2");
        read.observe(&page(0).to_string(), "receipt:0");
        read.observe(&page(0).to_string(), "receipt:duplicate");
        assert!(!read.complete());
        read.observe(&page(1).to_string(), "receipt:1");
        assert!(read.complete());
    }

    #[test]
    fn full_size_read_page_has_bounded_durable_coverage_without_truncation() {
        let mut page = page(0);
        page["total_chunks"] = 16.into();
        page["chunks"] = (0..16)
            .map(|index| serde_json::json!({"index":index,"content":"文".repeat(1500)}))
            .collect();
        let original = page.to_string();
        assert!(original.chars().count() > 16 * 1024);
        let compact = compact_read_receipt(&original);
        assert!(compact.len() < 4096);
        let mut read = coverage();
        read.observe(&compact, "receipt:large-page");
        assert!(read.complete());
        assert!(!compact.contains(&"文".repeat(100)));
    }

    #[test]
    fn metadata_denials_old_hashes_and_inconsistent_pages_are_not_reads() {
        for field in [
            "sha256",
            "evidence_ref",
            "bytes",
            "available",
            "kind",
            "chunks",
        ] {
            let mut read = coverage();
            let mut output = page(0);
            output[field] = serde_json::Value::Null;
            read.observe(&output.to_string(), "receipt:bad");
            assert!(read.chunks.is_empty(), "{field}");
            assert!(!read.complete());
        }
        let mut read = coverage();
        read.observe(&page(0).to_string(), "");
        assert!(read.chunks.is_empty());
        read.observe(&page(0).to_string(), "receipt:0");
        let mut inconsistent = page(1);
        inconsistent["total_chunks"] = 2.into();
        read.observe(&inconsistent.to_string(), "receipt:bad-total");
        assert_eq!(read.chunks, BTreeSet::from([0]));
        let mut invalid = page(3);
        invalid["chunks"][0]["content"] = "".into();
        read.observe(&invalid.to_string(), "receipt:empty");
        assert!(!read.complete());
    }

    /// The origin of the 158-refusal run: the review reported "packet missing" for a packet
    /// that was present, because the caller named the physical run id while the packet stored
    /// the bare graph id.
    #[test]
    fn the_identity_check_uses_the_namespace_the_caller_named() {
        let graph = "agentic-graph:bf5d8374a4c7";
        let run = "run:agentic-graph:bf5d8374a4c7";

        // The run-bound binding (`model_loop.rs:470`) names the run id.
        assert!(packet_identity_matches(graph, run, run));
        // The graph-bound binding (`model_loop.rs:196/367`) names the graph id.
        assert!(packet_identity_matches(graph, run, graph));

        // Normalising the namespace must not relax the identity check: a packet belonging to a
        // different graph still never matches, in either namespace. A packet's run id and graph
        // id name the same graph, so a different graph means both differ.
        assert!(!packet_identity_matches(
            "agentic-graph:other",
            "run:agentic-graph:other",
            run
        ));
        assert!(!packet_identity_matches(
            "agentic-graph:other",
            "run:agentic-graph:other",
            graph
        ));
        assert!(!packet_identity_matches(
            graph,
            run,
            "run:agentic-graph:bf5d8374a4c8"
        ));
        assert!(!packet_identity_matches(
            graph,
            run,
            "agentic-graph:bf5d8374a4c8"
        ));
    }

    /// A model that has just accepted three Tasks naturally names the three
    /// refs it was handed. Those are Tasks, not results, so the coverage map came
    /// back empty and the review was refused with "independent review has no
    /// current results" while the work was delivered and accepted. The Task's
    /// artifacts are exactly its delivered results, so the boundary resolves it.
    #[test]
    fn a_task_ref_resolves_to_the_tasks_delivered_artifacts() {
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        for (task_id, artifact) in [("task:a", "artifact:a"), ("task:b", "artifact:b")] {
            program.tasks.insert(
                task_id.to_string(),
                crate::AgenticTaskProjection {
                    task_id: task_id.to_string(),
                    team_id: "team:1".to_string(),
                    title: "work".to_string(),
                    objective: "objective".to_string(),
                    acceptance: "evidence".to_string(),
                    acceptance_checks: Vec::new(),
                    criterion_source: Default::default(),
                    required_capabilities: Vec::new(),
                    depends_on: Vec::new(),
                    status: crate::AgenticTaskStatus::Accepted,
                    claimant: Some("agent:worker".to_string()),
                    claim_generation: 1,
                    claim_execution_id: None,
                    claimed_at_ms: None,
                    lease_expires_at_ms: None,
                    active_attempts: Default::default(),
                    artifact_refs: vec![artifact.to_string()],
                    evidence_refs: Vec::new(),
                    unresolved: Vec::new(),
                    review_reason: None,
                    reviewed_by: None,
                    failed_attempts: 0,
                    review_generation: 1,
                    failed_review_attempts: 0,
                    last_failure: None,
                    replacement_task_refs: Vec::new(),
                    supersede_evidence_refs: Vec::new(),
                    superseded_reason: None,
                    superseded_by: None,
                    obligation_refs: Vec::new(),
                    purpose: Default::default(),
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                    cancel_requested_by: None,
                    cancel_reason_ref: None,
                    cancel_evidence_refs: Vec::new(),
                    pending_retirement: None,
                },
            );
        }
        assert_eq!(
            resolve_result_refs(&program, &["task:a".to_string(), "task:b".to_string()]),
            vec!["artifact:a".to_string(), "artifact:b".to_string()]
        );
    }

    /// Artifact refs are already the right shape and pass through untouched, so
    /// the coercion cannot invent or drop a result.
    #[test]
    fn artifact_refs_pass_through_unchanged() {
        let program = crate::AgenticProgramProjection::empty("program", "objective");
        assert_eq!(
            resolve_result_refs(
                &program,
                &["artifact://art_1".to_string(), "tool://receipt".to_string()]
            ),
            vec!["artifact://art_1".to_string(), "tool://receipt".to_string()]
        );
    }

    /// A Task with no artifacts contributes nothing rather than an invented ref.
    #[test]
    fn a_task_with_no_artifacts_contributes_nothing() {
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        program.tasks.insert(
            "task:empty".to_string(),
            crate::AgenticTaskProjection {
                task_id: "task:empty".to_string(),
                team_id: "team:1".to_string(),
                title: "work".to_string(),
                objective: "objective".to_string(),
                acceptance: "evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                required_capabilities: Vec::new(),
                depends_on: Vec::new(),
                status: crate::AgenticTaskStatus::Published,
                claimant: None,
                claim_generation: 0,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                active_attempts: Default::default(),
                artifact_refs: Vec::new(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                review_reason: None,
                reviewed_by: None,
                failed_attempts: 0,
                review_generation: 1,
                failed_review_attempts: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
        assert!(resolve_result_refs(&program, &["task:empty".to_string()]).is_empty());
    }
}
