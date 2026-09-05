pub(super) struct SessionApprovalControl {
    pub(super) approval_id: Option<String>,
    pub(super) approved: bool,
    pub(super) skip: bool,
    pub(super) scope: runtime::ApprovalGrantScope,
}

pub(super) fn parse_session_approval_control(content: &str) -> Option<SessionApprovalControl> {
    let tokens = content.split_whitespace().collect::<Vec<_>>();
    let command = tokens.first()?.to_ascii_lowercase();
    let (approved, skip) = match command.as_str() {
        "/approve" | "approve" | "批准" | "同意" => (true, false),
        "/deny" | "deny" | "拒绝" => (false, false),
        "/skip" | "skip" | "跳过" => (false, true),
        _ => return None,
    };
    let mut approval_id = None;
    let mut scope = runtime::ApprovalGrantScope::Once;
    for token in tokens.iter().skip(1) {
        let normalized = token.to_ascii_lowercase();
        let parsed_scope = match normalized.as_str() {
            "once" | "本次" => Some(runtime::ApprovalGrantScope::Once),
            "turn" | "本轮" | "回合" => Some(runtime::ApprovalGrantScope::Turn),
            "task" | "任务" => Some(runtime::ApprovalGrantScope::Task),
            "session" | "会话" => Some(runtime::ApprovalGrantScope::Session),
            "global" | "全局" => Some(runtime::ApprovalGrantScope::Global),
            _ => None,
        };
        if let Some(parsed_scope) = parsed_scope {
            scope = parsed_scope;
        } else if approval_id.is_none() {
            approval_id = Some((*token).to_string());
        }
    }
    Some(SessionApprovalControl {
        approval_id,
        approved,
        skip,
        scope,
    })
}

pub(super) fn surface_actor_from_classification(
    classification_json: Option<&str>,
) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(classification_json?).ok()?;
    let surface = value
        .pointer("/metadata/surface")
        .and_then(serde_json::Value::as_str)?;
    let user = value
        .pointer("/metadata/user_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("bound-user");
    Some(format!("surface:{surface}:{user}"))
}
