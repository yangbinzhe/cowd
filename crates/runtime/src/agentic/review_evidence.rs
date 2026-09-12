mod policy;
mod results;
pub(crate) use policy::{
    effect_review_snapshot, root_self_review_policy, RootReviewPolicySnapshot,
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
                    || (reference == self.proof.result_ref
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

fn physical_packet(services: &RuntimeServices, graph_id: &str) -> Result<AgentTaskPacket, String> {
    let graph = services
        .graph_state_store()
        .load(graph_id)
        .map_err(|error| error.to_string())?;
    graph
        .nodes
        .iter()
        .filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
        .filter_map(|node| {
            serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                .ok()
                .filter(|packet| packet.graph_id() == graph_id && packet.node_id() == node.id)
        })
        .next()
        .ok_or_else(|| format!("review physical Agent packet missing:{graph_id}"))
}

impl RuntimeServices {
    pub(crate) async fn independent_result_review(
        &self,
        actor: &AgentActorBinding,
        program: &AgenticProgramProjection,
        result_refs: &[String],
        condition_ref: Option<&str>,
    ) -> Result<IndependentReview, String> {
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
            )?;
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
                if task.review_generation.saturating_add(1) != u64::from(packet.attempt)
                    || !task
                        .active_attempts
                        .get(packet.graph_id())
                        .is_some_and(|attempt| {
                            attempt.mode == harness_contract::agent_action::AgentAttemptMode::Review
                                && attempt.generation.saturating_add(1) == u64::from(packet.attempt)
                                && attempt.agent_id == scope.agent_id
                                && attempt.membership_id == scope.membership_id
                        })
                    || !result_refs
                        .iter()
                        .all(|reference| task.artifact_refs.contains(reference))
                {
                    return Err("review physical attempt is stale or owns different results".into());
                }
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
                return Err("obligation requires a Runtime-bound producer with the declared capability, definition, skill and actual tool receipt".into());
            }
        }
        if coverage.is_empty() {
            return Err("independent review has no current results".into());
        }
        if let Some(packet) = packet {
            for receipt in self
                .commit_service()
                .load_delegated_agent_tool_receipts(
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
                if receipt.outcome.tool_name != "evidence_retrieve"
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
                        || binding.agent_run_id.is_some()
                        || event.payload["status"] != "completed"
                    {
                        continue;
                    }
                    if event.payload["tool_name"] != "evidence_retrieve" {
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
                        .map_err(|error| error.to_string())?;
                    if raw.sha256 != access.sha256 || raw.bytes != access.bytes {
                        return Err("review raw receipt integrity mismatch".into());
                    }
                    let bytes = self
                        .artifact_store()
                        .read(&raw, &raw.visibility_scope, None)
                        .await
                        .map_err(|error| error.to_string())?;
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
        let missing = coverage
            .values()
            .filter(|state| !state.complete())
            .map(|state| state.proof.result_ref.clone())
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(format!("review_requires_current_physical_reads:{}; use evidence_retrieve and follow every next_request for the current result", missing.join(",")));
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
            return Err(format!("review_requires_effect_observation:{}; independently read the actual affected target after the effect", unobserved.join(",")));
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(coverage["result:1"]
            .proof
            .effect_observation_refs
            .is_empty());
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
}
