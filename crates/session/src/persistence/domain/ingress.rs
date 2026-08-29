//! Canonical input-disposition encoding and applied-receipt projection.

use harness_contract::input_disposition::{
    InputApplicationState, InputDispositionAction, SessionInputApplicationReceipt,
};
use harness_contract::turn::InputRoutingDecision;

use super::super::SessionRuntimeInputStatus;

#[must_use]
pub const fn input_decision_as_str(decision: InputRoutingDecision) -> &'static str {
    match decision {
        InputRoutingDecision::StartNewTurn => "start_new_turn",
        InputRoutingDecision::SupplementCurrentTurn => "supplement_current_turn",
        InputRoutingDecision::InterruptAndReplan => "interrupt_and_replan",
        InputRoutingDecision::EnqueueNextStep => "enqueue_next_step",
        InputRoutingDecision::SpawnSubtask => "spawn_subtask",
        InputRoutingDecision::RouteCrossSession => "route_cross_session",
        InputRoutingDecision::CreateNewSession => "create_new_session",
        InputRoutingDecision::ControlOrApproval => "control_or_approval",
        InputRoutingDecision::RejectDuplicate => "reject_duplicate",
        InputRoutingDecision::RejectPolicy => "reject_policy",
    }
}

#[must_use]
pub fn parse_input_decision(value: &str) -> Option<InputRoutingDecision> {
    Some(match value {
        "start_new_turn" => InputRoutingDecision::StartNewTurn,
        "supplement_current_turn" => InputRoutingDecision::SupplementCurrentTurn,
        "interrupt_and_replan" => InputRoutingDecision::InterruptAndReplan,
        "enqueue_next_step" => InputRoutingDecision::EnqueueNextStep,
        "spawn_subtask" => InputRoutingDecision::SpawnSubtask,
        "route_cross_session" => InputRoutingDecision::RouteCrossSession,
        "create_new_session" => InputRoutingDecision::CreateNewSession,
        "control_or_approval" => InputRoutingDecision::ControlOrApproval,
        "reject_duplicate" => InputRoutingDecision::RejectDuplicate,
        "reject_policy" => InputRoutingDecision::RejectPolicy,
        _ => return None,
    })
}

#[must_use]
pub const fn decision_requires_target_turn(decision: InputRoutingDecision) -> bool {
    matches!(
        decision,
        InputRoutingDecision::SupplementCurrentTurn
            | InputRoutingDecision::InterruptAndReplan
            | InputRoutingDecision::ControlOrApproval
    )
}

#[must_use]
pub fn applied_input_projection(
    receipt: &SessionInputApplicationReceipt,
    current_target_turn_id: Option<&str>,
    now_ms: u64,
) -> Option<(
    InputRoutingDecision,
    SessionRuntimeInputStatus,
    Option<String>,
    Option<u64>,
)> {
    if receipt.state != InputApplicationState::Applied {
        return None;
    }
    Some(match receipt.action {
        InputDispositionAction::AmendCurrentTurn
        | InputDispositionAction::ReplanCurrentGraph
        | InputDispositionAction::Clarify => (
            InputRoutingDecision::SupplementCurrentTurn,
            SessionRuntimeInputStatus::Attached,
            current_target_turn_id.map(str::to_string),
            None,
        ),
        InputDispositionAction::ProgressOrControl => (
            InputRoutingDecision::ControlOrApproval,
            SessionRuntimeInputStatus::Completed,
            current_target_turn_id.map(str::to_string),
            Some(now_ms),
        ),
        InputDispositionAction::ReplaceCurrentTask => (
            InputRoutingDecision::StartNewTurn,
            SessionRuntimeInputStatus::Reclassified,
            None,
            None,
        ),
        InputDispositionAction::AddRequiredTask
        | InputDispositionAction::AddBackgroundTask
        | InputDispositionAction::AddTeamLane
        | InputDispositionAction::AddTaskWithTeam => (
            InputRoutingDecision::SpawnSubtask,
            SessionRuntimeInputStatus::Completed,
            None,
            Some(now_ms),
        ),
        InputDispositionAction::DispatchSession => (
            if receipt.target_session_created {
                InputRoutingDecision::CreateNewSession
            } else {
                InputRoutingDecision::RouteCrossSession
            },
            SessionRuntimeInputStatus::Completed,
            None,
            Some(now_ms),
        ),
    })
}
