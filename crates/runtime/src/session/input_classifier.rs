use harness_contract::turn::{
    InputPayloadKind, InputRelationKind, InputRelationProposal, InputRoutingDecision,
    InputRoutingReason, SessionInputEnvelope, TurnId,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeInputState {
    pub active_turn_id: Option<TurnId>,
    pub waiting_for_approval: bool,
    pub waiting_for_clarification: bool,
}

impl RuntimeInputState {
    #[must_use]
    pub fn active(active_turn_id: TurnId) -> Self {
        Self {
            active_turn_id: Some(active_turn_id),
            waiting_for_approval: false,
            waiting_for_clarification: false,
        }
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active_turn_id.is_some()
    }
}

#[must_use]
pub fn classify_session_input(
    envelope: &SessionInputEnvelope,
    state: &RuntimeInputState,
) -> (InputRoutingDecision, InputRoutingReason) {
    if envelope.content.trim().is_empty() {
        return (
            InputRoutingDecision::RejectPolicy,
            InputRoutingReason::new("empty_input", "empty input is not actionable", 10_000),
        );
    }

    let normalized = envelope.content.trim().to_ascii_lowercase();
    if matches!(envelope.payload_kind, InputPayloadKind::Approval)
        || (state.waiting_for_approval && is_explicit_approval_control(&normalized))
    {
        return (
            InputRoutingDecision::ControlOrApproval,
            InputRoutingReason::new(
                "approval_or_control",
                "runtime is waiting for an approval/control answer",
                9_500,
            ),
        );
    }

    if matches!(envelope.payload_kind, InputPayloadKind::Clarification)
        || state.waiting_for_clarification
    {
        return (
            InputRoutingDecision::SupplementCurrentTurn,
            InputRoutingReason::new(
                "clarification",
                "input answers an active clarification need",
                9_000,
            ),
        );
    }

    if normalized.starts_with("/stop")
        || normalized.starts_with("/cancel")
        || normalized.starts_with("/resume")
        || normalized.starts_with("/approve")
        || normalized.starts_with("/deny")
    {
        return (
            InputRoutingDecision::ControlOrApproval,
            InputRoutingReason::new(
                "slash_control",
                "slash command targets the running turn control plane",
                9_200,
            ),
        );
    }

    // Natural-language mentions of teams, sessions, follow-up work, and
    // replanning are observations, not authority to mutate lifecycle state.
    // The proposal is carried separately and evaluated by Strategy/Goal policy
    // inside the canonical graph. Only typed slash/control input is allowed to
    // take a deterministic control-plane path here.

    if state.is_active() {
        return (
            InputRoutingDecision::SupplementCurrentTurn,
            InputRoutingReason::new(
                "active_turn_supplement",
                "active turn exists and no explicit new-task signal was detected",
                7_500,
            ),
        );
    }

    (
        InputRoutingDecision::StartNewTurn,
        InputRoutingReason::new(
            "idle_session_start",
            "session has no active turn, so input starts a new turn",
            8_000,
        ),
    )
}

fn is_explicit_approval_control(normalized: &str) -> bool {
    let first = normalized.split_whitespace().next().unwrap_or_default();
    matches!(
        first,
        "/approve" | "/deny" | "批准" | "同意" | "拒绝" | "approve" | "deny"
    )
}

/// Extract a non-authoritative relationship hint from user input. The caller
/// persists this alongside the inbox record, then lets Runtime policy decide
/// whether the hint becomes a supplement, replan, team graph, or new session.
#[must_use]
pub fn propose_input_relation(envelope: &SessionInputEnvelope) -> Option<InputRelationProposal> {
    let normalized = envelope.content.trim().to_ascii_lowercase();
    let (candidate, confidence, reason) = if contains_any(
        &normalized,
        &[
            "/progress",
            "progress?",
            "what is the progress",
            "what's the progress",
            "当前进度",
            "现在进度",
            "进展如何",
            "进度怎么样",
            "做到哪了",
            "做到哪里",
        ],
    ) {
        (InputRelationKind::Progress, 9_000, "progress_query")
    } else if contains_any(
        &normalized,
        &[
            "/background",
            "continue in background",
            "run in background",
            "后台继续",
            "转到后台",
            "后台运行",
            "后台执行",
        ],
    ) {
        (
            InputRelationKind::Background,
            8_900,
            "background_execution_request",
        )
    } else if contains_any(
        &normalized,
        &[
            "@session",
            "cross-session",
            "跨session",
            "跨 session",
            "跨会话",
        ],
    ) {
        (
            InputRelationKind::CrossSession,
            8_700,
            "cross_session_reference",
        )
    } else if contains_any(
        &normalized,
        &[
            "new session",
            "start a new session",
            "new conversation",
            "新建session",
            "新建 session",
            "新建一个 session",
            "新建会话",
            "新会话",
            "另开会话",
            "另开一个会话",
            "单独会话",
            "独立 session",
            "独立session",
            "独立会话",
            "新起一个会话",
            "重新开一个会话",
        ],
    ) {
        (InputRelationKind::NewSession, 8_700, "new_session_request")
    } else if contains_any(
        &normalized,
        &[
            "@agent",
            "subagent",
            "delegate",
            "parallel",
            "子agent",
            "子 agent",
            "多agent",
            "多 agent",
            "委派",
            "分派",
            "并行",
            "团队",
        ],
    ) {
        (
            InputRelationKind::Subtask,
            8_400,
            "subtask_or_agent_reference",
        )
    } else if contains_any(
        &normalized,
        &[
            "interrupt",
            "change direction",
            "stop current",
            "instead",
            "打断",
            "停止当前",
            "改方向",
            "重新规划",
            "不要继续",
        ],
    ) {
        (InputRelationKind::Replan, 8_300, "replan_signal")
    } else if contains_any(
        &normalized,
        &[
            "next",
            "after this",
            "later",
            "todo",
            "下一步",
            "后续",
            "稍后",
            "待办",
            "排队",
        ],
    ) {
        (InputRelationKind::NewTask, 7_700, "future_work_hint")
    } else {
        return None;
    };
    Some(InputRelationProposal {
        candidate,
        confidence_basis_points: confidence,
        reasons: vec![reason.to_string()],
        target_ref: None,
    })
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};

    #[test]
    fn active_turn_plain_text_is_supplement() {
        let envelope = SessionInputEnvelope::text("s1", InputSourceKind::Webui, "add this");
        let state = RuntimeInputState::active(TurnId::from_string("turn-1"));

        let (decision, reason) = classify_session_input(&envelope, &state);

        assert_eq!(decision, InputRoutingDecision::SupplementCurrentTurn);
        assert_eq!(reason.code, "active_turn_supplement");
    }

    #[test]
    fn explicit_parallel_input_only_proposes_a_subtask_relation() {
        let envelope =
            SessionInputEnvelope::text("s1", InputSourceKind::Surface, "parallel @agent check");
        let state = RuntimeInputState::active(TurnId::from_string("turn-1"));

        let (decision, _) = classify_session_input(&envelope, &state);
        let proposal = propose_input_relation(&envelope).expect("relation proposal");

        assert_eq!(decision, InputRoutingDecision::SupplementCurrentTurn);
        assert_eq!(proposal.candidate, InputRelationKind::Subtask);
    }

    #[test]
    fn chinese_explicit_new_session_input_only_proposes_new_session() {
        let envelope = SessionInputEnvelope::text(
            "s1",
            InputSourceKind::Surface,
            "请为这个补充信息新建一个独立 session，用于后续并行跟进。",
        );
        let state = RuntimeInputState::active(TurnId::from_string("turn-1"));

        let (decision, reason) = classify_session_input(&envelope, &state);
        let proposal = propose_input_relation(&envelope).expect("relation proposal");

        assert_eq!(decision, InputRoutingDecision::SupplementCurrentTurn);
        assert_eq!(reason.code, "active_turn_supplement");
        assert_eq!(proposal.candidate, InputRelationKind::NewSession);
    }

    #[test]
    fn progress_and_background_are_typed_relation_proposals() {
        let progress =
            SessionInputEnvelope::text("s1", InputSourceKind::Surface, "现在进度怎么样？");
        let background =
            SessionInputEnvelope::text("s1", InputSourceKind::Surface, "转到后台继续执行");

        assert_eq!(
            propose_input_relation(&progress)
                .expect("progress proposal")
                .candidate,
            InputRelationKind::Progress
        );
        assert_eq!(
            propose_input_relation(&background)
                .expect("background proposal")
                .candidate,
            InputRelationKind::Background
        );
    }
}
