use super::*;

pub(super) fn attach_agentic_display(
    graph: &mut ExecutionGraph,
    member: &AgentMemberProjection,
    task: &AgenticTaskProjection,
    program_id: &str,
) -> Result<(), String> {
    let node = graph
        .nodes
        .first_mut()
        .ok_or_else(|| "compiled Agent graph has no node".to_string())?;
    let mut packet: AgentTaskPacket =
        serde_json::from_str(&node.payload_ref).map_err(|error| error.to_string())?;
    let binding = packet
        .binding
        .as_mut()
        .ok_or_else(|| "compiled Agent graph has no binding".to_string())?;
    let label = member.role.trim().to_string();
    let role_label = format!("{} · {}", member.role.trim(), task.title.trim());
    let provenance = format!("runtime.agentic:{program_id}");
    let display_digest = format!(
        "{:x}",
        Sha256::digest(
            format!(
                "{}|{}|{}|{}|{}",
                member.agent_id, member.role, label, role_label, provenance
            )
            .as_bytes()
        )
    );
    binding.display = Some(harness_contract::agent::AgentDisplayIdentity {
        agent_id: member.agent_id.clone(),
        role_id: member.role.clone(),
        role_display_name: Some(member.role.clone()),
        label,
        role_label,
        focus_label: Some(task.title.clone()),
        locale: "auto".to_string(),
        provenance,
        digest: display_digest,
    });
    binding.binding_digest = crate::agent::binding::recompute_binding_digest(binding)
        .map_err(|error| error.to_string())?;
    node.payload_ref = serde_json::to_string(&packet).map_err(|error| error.to_string())?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum DispatchMode {
    Execute,
    Review,
    Coordination(u64),
}

impl DispatchMode {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Execute => "execute",
            Self::Review => "review",
            Self::Coordination(_) => "coordination",
        }
    }

    pub(super) const fn attempt_mode(self) -> harness_contract::agent_action::AgentAttemptMode {
        use harness_contract::agent_action::AgentAttemptMode;
        match self {
            Self::Execute => AgentAttemptMode::Execute,
            Self::Review => AgentAttemptMode::Review,
            Self::Coordination(_) => AgentAttemptMode::Coordination,
        }
    }

    pub(super) fn team_id<'a>(
        self,
        projection: &'a AgenticProgramProjection,
        agent_id: &str,
        task: &AgenticTaskProjection,
    ) -> Option<&'a str> {
        match self {
            Self::Coordination(revision) => projection.coordination_team_id_for(agent_id, revision),
            _ => projection.dispatch_team_id_for(agent_id, task, self == Self::Review),
        }
    }
}

pub(super) fn task_is_ready(projection: &AgenticProgramProjection, task_ref: &str) -> bool {
    projection.tasks.get(task_ref).is_some_and(|task| {
        task_ready_for_execution(task, now_ms()) && dependencies_accepted(projection, task)
    })
}

pub(super) fn coordination_requests(
    projection: &AgenticProgramProjection,
) -> Vec<(String, DispatchMode)> {
    projection
        .coordination_requests()
        .into_iter()
        .filter(|(_, entry)| {
            projection.coordination_request_current(entry)
                && entry
                    .coordination
                    .as_ref()
                    .is_none_or(|consumption| !consumption.settled)
        })
        .filter_map(|(_, entry)| {
            entry.intent.as_ref().map(|intent| {
                (
                    intent.task_ref.clone(),
                    DispatchMode::Coordination(entry.revision),
                )
            })
        })
        .collect()
}

pub(super) fn task_ready_for_execution(task: &AgenticTaskProjection, observed_at_ms: u64) -> bool {
    matches!(
        task.status,
        AgenticTaskStatus::Published | AgenticTaskStatus::Rework
    ) || (task.status == AgenticTaskStatus::Claimed
        && task
            .lease_expires_at_ms
            .is_some_and(|expires| expires <= observed_at_ms))
}

pub(super) fn dependencies_accepted(
    projection: &AgenticProgramProjection,
    task: &AgenticTaskProjection,
) -> bool {
    task.depends_on.iter().all(|dependency| {
        super::super::work_market::task_dependency_satisfied(projection, dependency)
    })
}

pub(super) fn eligible_members<'a>(
    projection: &'a AgenticProgramProjection,
    task: &AgenticTaskProjection,
    mode: DispatchMode,
) -> Vec<&'a AgentMemberProjection> {
    projection
        .agents
        .values()
        .filter(|member| match mode {
            DispatchMode::Coordination(revision) => projection
                .coordination_team_id_for(&member.agent_id, revision)
                .is_some(),
            // Team membership is the semantic eligibility boundary. Model
            // capability labels express expertise and ranking hints, not a
            // trusted physical grant or an exact-string scheduling fence.
            // Concrete effect capabilities and ToolHost availability are
            // resolved later by Runtime admission for the selected member.
            DispatchMode::Execute => {
                projection.agent_is_active_in(&member.agent_id, &task.team_id)
                    && !projection.declined_task_opportunity(
                        &task.task_id,
                        &member.agent_id,
                        Some(task.claim_generation),
                        None,
                    )
            }
            DispatchMode::Review => {
                task.claimant.as_deref() != Some(member.agent_id.as_str())
                    && projection
                        .dispatch_team_id_for(&member.agent_id, task, true)
                        .is_some()
            }
        })
        .collect()
}

pub(super) fn member_dispatch_rank(
    projection: &AgenticProgramProjection,
    member: &AgentMemberProjection,
    task: &AgenticTaskProjection,
    mode: DispatchMode,
) -> (u8, u8, usize, Reverse<usize>, String) {
    // Independence is a semantic topology fact; role labels are presentation
    // authored by the model and must never act as a hidden scheduler policy.
    let preferred_reviewer = mode == DispatchMode::Review
        && projection
            .dispatch_team_id_for(&member.agent_id, task, true)
            .is_some_and(|team_id| team_id != task.team_id);
    let active_execution_load = projection
        .tasks
        .values()
        .filter(|candidate| {
            candidate
                .active_attempts
                .values()
                .any(|attempt| attempt.agent_id == member.agent_id)
                || (candidate.status == AgenticTaskStatus::Claimed
                    && candidate.claimant.as_deref() == Some(member.agent_id.as_str()))
        })
        .count();
    let member_terms = semantic_terms(&format!(
        "{} {} {}",
        member.role,
        member.mission,
        member.expertise_hints.join(" "),
    ));
    let task_terms = semantic_terms(&format!(
        "{} {} {} {}",
        task.title,
        task.objective,
        task.acceptance,
        task.expertise_hints.join(" "),
    ));
    let semantic_relevance = member_terms.intersection(&task_terms).count();
    let digest = Sha256::digest(
        format!("{}|{}|{}", task.task_id, member.agent_id, mode.as_str()).as_bytes(),
    );
    (
        (!preferred_reviewer) as u8,
        (!(mode == DispatchMode::Execute
            && projection.offered_task_opportunity(
                &task.task_id,
                &member.agent_id,
                task.claim_generation,
            ))) as u8,
        active_execution_load,
        Reverse(semantic_relevance),
        format!("{digest:x}"),
    )
}

pub(super) fn semantic_terms(value: &str) -> BTreeSet<String> {
    let normalized = value.to_lowercase();
    let mut terms = normalized
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| term.chars().count() >= 2)
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let compact_non_ascii = normalized
        .chars()
        .filter(|character| character.is_alphanumeric() && !character.is_ascii())
        .collect::<Vec<_>>();
    for window in compact_non_ascii.windows(2) {
        terms.insert(window.iter().collect());
    }
    terms
}

pub(super) fn task_objective(
    projection: &AgenticProgramProjection,
    task: &AgenticTaskProjection,
    member: &AgentMemberProjection,
    mode: DispatchMode,
) -> String {
    if let DispatchMode::Coordination(revision) = mode {
        let Some((topic, entry)) = projection.coordination_wake(revision) else {
            return String::new();
        };
        return format!("You are coordination Agent `{}`. A member requested help on Task `{}`. Inspect the authorized request below and its referenced evidence, then contribute a useful answer, challenge, or actionable proposal through message_publish on `{}`. Include `{}` in refs so the requester can trace your response. Respect the original execution owner: do not task_claim or task_review from this coordination run. You may propose work, publish an Offer for another task, or invite collaborators within your authorized scope; actual execution requires a new bound run. Do not invent replies or evidence. Your durable response, not final prose, closes this opportunity. No fixed debate rounds are required.\n\nRequest: {}", member.agent_id, task.task_id, topic, entry.entry_id, serde_json::json!(entry));
    }
    let topic_ref = projection.teams.get(&task.team_id).map_or_else(
        || format!("topic:{}", task.team_id),
        |team| team.topic_ref.clone(),
    );
    let mut objective = match mode {
        DispatchMode::Coordination(_) => unreachable!("coordination objective handled above"),
        DispatchMode::Execute => format!(
            "You are Agent `{}` in Team `{}`. Role: {}. Mission: {}.\n\nWork item `{}`: {}\nObjective: {}\nAcceptance: {}\n\nFirst call state_inspect and inspect the current Program truth. If this work fits your role and capabilities, actively call task_claim for this exact task before doing substantive work; Runtime binds that claim to your immutable Agent identity and physical execution. If it is unsuitable or already owned, do not claim or submit it: explain the mismatch concisely and let the Team reassign or replan. After a successful claim, use workspace_snapshot to identify actual repository roots and document entries before searching files; follow returned continuation requests and never infer absence from a partial scan. Then act autonomously: choose and use the most effective available tools, and publish useful findings to topic:{} when collaboration benefits. Produce long-form work in a workspace file, use read_file to obtain its exact sha256 and artifact_publish to publish it, then call artifact_commit with the returned content_ref. For ordinary response text, explicitly select the intended zero-based Text block using content_ref=current_message_block:<index>; Runtime automatically binds the artifact to this claimed Task, while relates_to is only for additional semantic relations. Finally call task_submit: pass the collaboration `artifact:...` value returned in artifact_commit.changed_refs as artifact_refs, and pass real durable source/test/tool receipts as evidence_refs. Runtime automatically binds the artifact's content; do not copy its internal artifact:// content_ref into evidence_refs. Retain material uncertainty in task_submit.unresolved; distinguish measured facts from assumptions and estimates, and use Topic references for challenges and responses. Never submit before claiming, and never claim completion only in prose.",
            member.agent_id,
            task.team_id,
            member.role,
            member.mission,
            task.task_id,
            task.title,
            task.objective,
            task.acceptance,
            topic_ref,
        ),
        DispatchMode::Review => format!(
            "You are independent reviewer `{}`. Review submitted work item `{}` against: {}. First call state_inspect for the Task, retrieve and inspect its submitted artifact content, and check the supporting evidence. Do not redo the author’s work and do not self-review. Use task_review with accept only when the evidence supports the acceptance criterion; otherwise challenge or request rework with a concrete reason. Cite the real durable inspection/source/test receipts in evidence_refs; Runtime already binds the Task's artifact set, so do not echo internal artifact storage selectors merely to satisfy a join. Your durable review action, not prose, is the verdict. Program: {}.",
            member.agent_id, task.task_id, task.acceptance, projection.program_id,
        ),
    };
    if matches!(mode, DispatchMode::Execute) {
        if let Some(reason) = task.review_reason.as_deref().filter(|reason| !reason.trim().is_empty())
        {
            objective.push_str(&format!(
                "\n\nA previous review requested rework for this exact reason, which you must resolve before resubmitting: {reason}"
            ));
        }
    }
    objective.push_str(&format!(
        "\n\nCurrent work directory: {}. This directory describes existing work, not evidence of its correctness. Inspect the exact Task and follow its artifact/evidence read requests before repeating prior work. Add only the contribution required by this Task objective and acceptance; reuse cited common background instead of restating or regenerating it. There is one result owner for each Task. Other members may contribute evidence, challenges and responses through authorized Topics without taking a second claim. You may propose narrower tasks or invite collaborators within your active memberships; preserve existing accepted results and source references when replanning.",
        serde_json::json!({
            "task_ref":task.task_id,
            "artifact_count":task.artifact_refs.len(),
            "evidence_count":task.evidence_refs.len(),
            "unresolved_count":task.unresolved.len(),
            "dependency_count":task.depends_on.len(),
            "read_request":{"name":"state_inspect","input":{"scope_ref":task.task_id}}
        })
    ));
    objective
}

pub(super) fn entity_ref_for_action(envelope: &AgentActionEnvelope) -> Option<String> {
    let prefix = match envelope.action {
        AgentAction::TaskPublish(_) => "task",
        _ => return None,
    };
    let digest = Sha256::digest(
        format!(
            "{}|{}|{}|{}",
            envelope.actor.program_id,
            envelope.actor.actor_id,
            envelope.action_id,
            envelope.action.kind()
        )
        .as_bytes(),
    );
    Some(format!("{prefix}:{}", &format!("{digest:x}")[..24]))
}

pub(super) fn deterministic_graph_id(
    program_id: &str,
    task_ref: &str,
    agent_ref: &str,
    mode: DispatchMode,
    claim_generation: u64,
) -> String {
    let digest = Sha256::digest(
        format!(
            "{program_id}|{task_ref}|{agent_ref}|{}|{claim_generation}",
            mode.as_str()
        )
        .as_bytes(),
    );
    format!("agentic-graph:{digest:x}")
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
