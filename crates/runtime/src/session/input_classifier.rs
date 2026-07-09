use harness_contract::turn::{
    InputPayloadKind, InputRoutingDecision, InputRoutingReason, SessionInputEnvelope, TurnId,
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

    if matches!(envelope.payload_kind, InputPayloadKind::Approval) || state.waiting_for_approval {
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

    let normalized = envelope.content.trim().to_ascii_lowercase();
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

    if contains_any(
        &normalized,
        &[
            "@session",
            "cross-session",
            "跨session",
            "跨 session",
            "跨会话",
        ],
    ) {
        return (
            InputRoutingDecision::RouteCrossSession,
            InputRoutingReason::new(
                "cross_session_reference",
                "input explicitly references another session",
                8_700,
            ),
        );
    }

    if contains_any(
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
        return (
            InputRoutingDecision::CreateNewSession,
            InputRoutingReason::new(
                "new_session_request",
                "input explicitly requests a separate session",
                8_700,
            ),
        );
    }

    if contains_any(
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
        return (
            InputRoutingDecision::SpawnSubtask,
            InputRoutingReason::new(
                "subtask_or_agent_reference",
                "input asks for delegated or parallel execution",
                8_400,
            ),
        );
    }

    if contains_any(
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
        return (
            InputRoutingDecision::EnqueueNextStep,
            InputRoutingReason::new(
                "future_work_hint",
                "input appears to add follow-up work after the active turn",
                7_700,
            ),
        );
    }

    if contains_any(
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
        return (
            InputRoutingDecision::InterruptAndReplan,
            InputRoutingReason::new(
                "replan_signal",
                "input asks to alter the active execution path",
                8_300,
            ),
        );
    }

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
    fn explicit_parallel_input_spawns_subtask() {
        let envelope =
            SessionInputEnvelope::text("s1", InputSourceKind::Surface, "parallel @agent check");
        let state = RuntimeInputState::active(TurnId::from_string("turn-1"));

        let (decision, _) = classify_session_input(&envelope, &state);

        assert_eq!(decision, InputRoutingDecision::SpawnSubtask);
    }

    #[test]
    fn chinese_explicit_new_session_input_creates_new_session() {
        let envelope = SessionInputEnvelope::text(
            "s1",
            InputSourceKind::Surface,
            "请为这个补充信息新建一个独立 session，用于后续并行跟进。",
        );
        let state = RuntimeInputState::active(TurnId::from_string("turn-1"));

        let (decision, reason) = classify_session_input(&envelope, &state);

        assert_eq!(decision, InputRoutingDecision::CreateNewSession);
        assert_eq!(reason.code, "new_session_request");
    }
}
