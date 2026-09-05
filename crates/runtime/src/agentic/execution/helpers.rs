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
}

impl DispatchMode {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Execute => "execute",
            Self::Review => "review",
        }
    }
}

pub(super) fn task_is_ready(projection: &AgenticProgramProjection, task_ref: &str) -> bool {
    projection.tasks.get(task_ref).is_some_and(|task| {
        task_ready_for_execution(task, now_ms()) && dependencies_accepted(projection, task)
    })
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
            DispatchMode::Execute => {
                member.team_id == task.team_id
                    && task
                        .required_capabilities
                        .iter()
                        .all(|capability| member.required_capabilities.contains(capability))
            }
            DispatchMode::Review => task.claimant.as_deref() != Some(member.agent_id.as_str()),
        })
        .collect()
}

pub(super) fn member_dispatch_rank(
    projection: &AgenticProgramProjection,
    member: &AgentMemberProjection,
    task: &AgenticTaskProjection,
    mode: DispatchMode,
) -> (u8, usize, Reverse<usize>, String) {
    // Independence is a semantic topology fact; role labels are presentation
    // authored by the model and must never act as a hidden scheduler policy.
    let preferred_reviewer = mode == DispatchMode::Review && member.team_id != task.team_id;
    let active_execution_load = projection
        .tasks
        .values()
        .filter(|candidate| {
            candidate.status == AgenticTaskStatus::Claimed
                && candidate.claimant.as_deref() == Some(member.agent_id.as_str())
        })
        .count();
    let member_terms = semantic_terms(&format!("{} {}", member.role, member.mission));
    let task_terms = semantic_terms(&format!(
        "{} {} {}",
        task.title, task.objective, task.acceptance
    ));
    let semantic_relevance = member_terms.intersection(&task_terms).count();
    let digest = Sha256::digest(
        format!("{}|{}|{}", task.task_id, member.agent_id, mode.as_str()).as_bytes(),
    );
    (
        (!preferred_reviewer) as u8,
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
    let topic_ref = projection.teams.get(&task.team_id).map_or_else(
        || format!("topic:{}", task.team_id),
        |team| team.topic_ref.clone(),
    );
    match mode {
        DispatchMode::Execute => format!(
            "You are Agent `{}` in Team `{}`. Role: {}. Mission: {}.\n\nWork item `{}`: {}\nObjective: {}\nAcceptance: {}\n\nFirst call state_inspect and inspect the current Program truth. If this work fits your role and capabilities, actively call task_claim for this exact task before doing substantive work; Runtime binds that claim to your immutable Agent identity and physical execution. If it is unsuitable or already owned, do not claim or submit it: explain the mismatch concisely and let the Team reassign or replan. After a successful claim, act autonomously: choose and use the most effective available tools, and publish useful findings to topic:{} when collaboration benefits. Produce the substantive long-form work in ordinary content or a workspace artifact, call artifact_commit with this exact task ref in relates_to, then call task_submit with the returned collaboration artifact ref and real durable evidence references. Never submit before claiming, and never claim completion only in prose.",
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
            "You are independent reviewer `{}`. Review submitted work item `{}` against: {}. First call state_inspect and inspect its artifact/evidence references. Do not redo the author’s work and do not self-review. Use task_review with accept only when the evidence supports the acceptance criterion; otherwise challenge or request rework with a concrete reason and evidence references. Your durable review action, not prose, is the verdict. Program: {}.",
            member.agent_id, task.task_id, task.acceptance, projection.program_id,
        ),
    }
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
