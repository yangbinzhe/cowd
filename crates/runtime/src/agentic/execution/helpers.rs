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
pub(in crate::agentic) enum DispatchMode {
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
    // A Task with an unsettled coordination attempt already owes that work. A
    // second request for the same Task would admit a parallel graph for the
    // same opportunity, which is exactly the duplicate model work this path
    // exists to prevent.
    let in_flight = projection
        .coordination_requests()
        .into_iter()
        .filter(|(_, entry)| {
            entry
                .coordination
                .as_ref()
                .is_some_and(|consumption| !consumption.settled)
        })
        .filter_map(|(_, entry)| entry.intent.as_ref().map(|intent| intent.task_ref.clone()))
        .collect::<std::collections::BTreeSet<_>>();
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
        .filter(|(task_ref, _)| !in_flight.contains(task_ref))
        .collect()
}

pub(super) fn task_ready_for_execution(task: &AgenticTaskProjection, _observed_at_ms: u64) -> bool {
    // Expiry is a liveness signal, not evidence that the old physical owner
    // closed. Recovery must first settle and release that exact generation.
    matches!(task.status, AgenticTaskStatus::Published | AgenticTaskStatus::Rework)
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
        member.agent_id.clone(),
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
        return format!(
            "You are coordination Agent `{}`. A member requested help on Task `{}`. Inspect the authorized request below and its referenced evidence, then contribute a useful answer, challenge, or actionable proposal through message_publish on `{}`. Include `{}` in refs so the requester can trace your response. Respect the original execution owner: do not task_claim or task_review from this coordination run. You may propose work, publish an Offer for another task, or invite collaborators within your authorized scope; actual execution requires a new bound run. Do not invent replies or evidence. Your durable response, not final prose, closes this opportunity. No fixed debate rounds are required.\n\nRequest: {}",
            member.agent_id,
            task.task_id,
            topic,
            entry.entry_id,
            serde_json::json!(entry)
        );
    }
    let topic_ref = projection.teams.get(&task.team_id).map_or_else(
        || format!("topic:{}", task.team_id),
        |team| team.topic_ref.clone(),
    );
    let purpose_contract = match task.purpose {
        harness_contract::agent_action::TaskPurpose::Delivery =>
            crate::agentic::order_contract::TASK_REPORT_SUBMISSION_CONTRACT,
        harness_contract::agent_action::TaskPurpose::Exploration =>
            crate::agentic::order_contract::EXPLORATION_CLOSEOUT_CONTRACT,
    };
    let mut objective = match mode {
        DispatchMode::Coordination(_) => unreachable!("coordination objective handled above"),
        DispatchMode::Execute => format!(
            "你是 Agent `{}`（Team `{}`，角色 {}）。\n任务 `{}`：{}\n目标：{}\n验收：{}\n\n在真实授权和资源边界内自主选择有效行动，不添加操作者没有要求的交付或报告格式。state_inspect 可查看 Task 真相；执行和提交前须 task_claim 领取该确切 Task。只有真实工具动作和持久回执改变协作状态，正文不是动作；不确定的保留在 unresolved，不得编造。当实际验收要求原文、精确字节或 file:line 时，用 read_ref 读取所需范围，预览摘要不等于完整证据。\n{purpose_contract}\n\n资源范围只说明能力，不代表必须写文件。普通正文、已有文件或多段产物按实际任务需要选择，不越过当前权限。需要协作时可在 topic:{} 发布有用的新事实；不要为了收口重复制造报告或消息。",
            member.agent_id,
            task.team_id,
            member.role,
            task.task_id,
            task.title,
            task.objective,
            task.acceptance,
            topic_ref,
        ),
        DispatchMode::Review => format!(
            "你是独立复核者 `{}`（不得复核自己）。任务 `{}`，验收判据：{}\n按实际证据自主复核，不新增操作者未要求的步骤或报告格式。state_inspect 可读取 Task 与已提交 artifact；逐条检查实际验收判据，引用你亲自读取的证据。通过 task_review 明确裁决：证据确实支持原要求时 accept，不因风格或篇幅拒绝；缺口已披露不等于要求自动满足，只有原要求允许这种披露且本轮授权确实无法取得证据时才按原边界判断并写明原因；其余 rework/challenge 必须点名具体操作者条款和亲读证据缺口，不笼统拒绝。\n{scope_discipline}不要重做作者的工作；Runtime 已绑定 artifact，不要把内部存储选择器抄进 evidence_refs。真实 durable review 才是裁决。Program: {}。",
            member.agent_id,
            task.task_id,
            task.acceptance,
            projection.program_id,
            scope_discipline = crate::agentic::order_contract::REVIEWER_SCOPE_DISCIPLINE,
        ),
    };
    if !task.acceptance_checks.is_empty() {
        objective.push_str(&format!(
            "\n\nAcceptance checks (advisory; the prose acceptance criterion stays authoritative -- use these to self-check and to cite concrete evidence): {}",
            task.acceptance_checks.join("; ")
        ));
    }
    if matches!(mode, DispatchMode::Execute) && task.status == AgenticTaskStatus::Rework {
        if let Some(reason) = task
            .review_reason
            .as_deref()
            .filter(|reason| !reason.trim().is_empty())
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

pub(in crate::agentic) fn deterministic_graph_id(
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

#[cfg(test)]
mod delivery_lease_tests {
    use super::*;

    fn card(purpose: harness_contract::agent_action::TaskPurpose, resource_scopes: Vec<String>) -> String {
        let mut program = AgenticProgramProjection::empty("program:card", "objective:card");
        program.resource_scopes = resource_scopes;
        let task = serde_json::from_value(serde_json::json!({
            "task_id":"task:card","team_id":"team:card","title":"Review-labelled delivery",
            "objective":"Perform exactly the operator-requested work","acceptance":"Actual requested outcome",
            "purpose":purpose,"required_capabilities":[],"depends_on":[],"status":"published",
            "claim_generation":0,"artifact_refs":[],"evidence_refs":[],"unresolved":[],
            "failed_attempts":0,"review_generation":0,"failed_review_attempts":0,
        })).unwrap();
        let member = serde_json::from_value(serde_json::json!({
            "agent_id":"agent:card","role":"independent-looking name","mission":"real task",
            "required_capabilities":[],"invited_by":"root",
        })).unwrap();
        task_objective(&program, &task, &member, DispatchMode::Execute)
    }

    #[test]
    fn workspace_capability_does_not_turn_an_ordinary_task_into_a_file_report_pipeline() {
        let read_only = card(harness_contract::agent_action::TaskPurpose::Delivery, vec!["read:.".into()]);
        let writable = card(harness_contract::agent_action::TaskPurpose::Delivery, vec!["workspace:.".into()]);
        assert_eq!(read_only, writable);
        assert!(writable.contains(crate::agentic::order_contract::TASK_REPORT_SUBMISSION_CONTRACT));
        assert!(!writable.contains("不要增加其它步骤"));
        assert!(!writable.contains("撰写审查报告"));
        assert!(!writable.contains("把正文写入 workspace 文件"));
    }

    #[test]
    fn actual_typed_purpose_not_title_or_workspace_selects_the_control_exit_contract() {
        let exploration = card(harness_contract::agent_action::TaskPurpose::Exploration, vec!["workspace:.".into()]);
        let delivery = card(harness_contract::agent_action::TaskPurpose::Delivery, vec!["workspace:.".into()]);
        assert!(exploration.contains(crate::agentic::order_contract::EXPLORATION_CLOSEOUT_CONTRACT));
        assert!(!exploration.contains(crate::agentic::order_contract::TASK_REPORT_SUBMISSION_CONTRACT));
        assert!(!delivery.contains(crate::agentic::order_contract::EXPLORATION_CLOSEOUT_CONTRACT));
        assert!(exploration.contains("task_withdraw"));
        assert!(exploration.contains("not acceptance"));
    }
}

#[cfg(test)]
mod dispatch_mode_contract_tests {
    use crate::agentic::order_contract::worker_terminal_action_ids;
    use harness_contract::agent_action::TaskPurpose;

    #[test]
    fn delivery_review_and_coordination_keep_their_actual_terminal_actions() {
        assert_eq!(worker_terminal_action_ids("execute", TaskPurpose::Delivery), &["task_submit"]);
        assert_eq!(worker_terminal_action_ids("review", TaskPurpose::Delivery), &["task_review"]);
        assert_eq!(worker_terminal_action_ids("coordination", TaskPurpose::Delivery), &["message_publish"]);
    }

    #[test]
    fn exploration_allows_explicit_retirement_without_inventing_acceptance() {
        let actions = worker_terminal_action_ids("execute", TaskPurpose::Exploration);
        assert!(actions.contains(&"task_withdraw"));
        assert!(actions.contains(&"task_submit"));
        assert!(!actions.contains(&"task_review"));
        assert!(!actions.contains(&"objective_complete_request"));
    }
}
