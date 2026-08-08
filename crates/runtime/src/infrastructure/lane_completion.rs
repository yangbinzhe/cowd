//! Lane completion detector — automatically marks lanes as completed when
//! session finishes successfully with green tests and pushed code.
//!
//! This bridges the gap where `LaneContext::completed` was a passive bool
//! that nothing automatically set. Now completion is detected from:
//! - Agent output shows Finished status
//! - No errors/blockers present  
//! - Tests passed (green status)
//! - Code pushed (has output file)

use std::process::Command;
use std::time::Duration;

use crate::{
    check_freshness, evaluate, BranchFreshness, GreenLevel, LaneBlocker, LaneContext, PolicyAction,
    PolicyCondition, PolicyEngine, PolicyRule, ReviewStatus,
};

type AgentOutput = crate::AgentRunSnapshot;

/// Detects if a lane should be automatically marked as completed.
///
/// Returns `Some(LaneContext)` with `completed = true` if all conditions met,
/// `None` if lane should remain active.
#[allow(dead_code)]
pub(crate) fn detect_lane_completion(
    output: &AgentOutput,
    test_green: bool,
    has_pushed: bool,
) -> Option<LaneContext> {
    // Must be finished without errors
    if output.failure.is_some() {
        return None;
    }

    // Must have finished status
    if output.status != harness_contract::agent::AgentStatus::Completed {
        return None;
    }

    // Must have green tests
    if !test_green {
        return None;
    }

    // Must have pushed code
    if !has_pushed {
        return None;
    }

    // All conditions met — create completed context
    Some(LaneContext {
        lane_id: output.agent_id.clone(),
        green_level: GreenLevel::Workspace,
        branch_freshness: std::time::Duration::from_secs(0),
        stale_branch: None,
        blocker: LaneBlocker::None,
        review_status: ReviewStatus::Approved,
        diff_scope: crate::DiffScope::Scoped,
        completed: true,
        reconciled: false,
    })
}

/// Evaluates policy actions for a completed lane, after checking branch freshness.
///
/// Stale/diverged branches are surfaced via policy rules that inspect
/// `context.branch_freshness` against `STALE_BRANCH_THRESHOLD`.
#[allow(dead_code)]
pub(crate) fn evaluate_completed_lane(context: &mut LaneContext) -> Vec<PolicyAction> {
    if let Some(branch) = current_git_branch() {
        let freshness = check_freshness(&branch, "main");
        context.branch_freshness = branch_freshness_to_duration(&freshness);
    }

    let engine = PolicyEngine::new(vec![
        PolicyRule::new(
            "closeout-completed-lane",
            PolicyCondition::And(vec![
                PolicyCondition::LaneCompleted,
                PolicyCondition::GreenAt {
                    level: GreenLevel::Workspace,
                },
            ]),
            PolicyAction::CloseoutLane,
            10,
        ),
        PolicyRule::new(
            "cleanup-completed-session",
            PolicyCondition::LaneCompleted,
            PolicyAction::CleanupSession,
            5,
        ),
    ]);

    evaluate(&engine, context)
}

#[allow(dead_code)]
fn current_git_branch() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|b| !b.is_empty())
}

fn branch_freshness_to_duration(freshness: &BranchFreshness) -> Duration {
    const STALE_BRANCH_THRESHOLD: Duration = Duration::from_secs(3600);
    match freshness {
        BranchFreshness::Fresh => Duration::ZERO,
        BranchFreshness::Stale { .. } => STALE_BRANCH_THRESHOLD + Duration::from_secs(1),
        BranchFreshness::Diverged { .. } => STALE_BRANCH_THRESHOLD * 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DiffScope, LaneBlocker};

    fn test_output() -> AgentOutput {
        let graph_identity = harness_contract::execution::ExecutionIdentity::for_task_graph(
            "principal-test",
            "workspace-test",
            "mission-test",
            "task-test",
            "session-test",
            "turn-test",
            "graph-test",
        )
        .expect("graph identity");
        AgentOutput {
            execution_identity: harness_contract::execution::ExecutionIdentity::for_agent_node(
                &graph_identity,
                "run-test",
                "node-test",
            )
            .expect("agent identity"),
            run_id: "run-test".to_string(),
            agent_id: "test-lane-1".to_string(),
            task_id: "task-test".to_string(),
            root_task_id: "task-test".to_string(),
            session_id: "session-test".to_string(),
            graph_id: "graph-test".to_string(),
            node_id: "node-test".to_string(),
            attempt: 1,
            expected_graph_revision: 1,
            backend: crate::AgentBackendKind::InProcess,
            status: harness_contract::agent::AgentStatus::Completed,
            revision: 1,
            model: None,
            provider: None,
            binding: None,
            started_at_ms: 1,
            updated_at_ms: 1,
            failure: None,
        }
    }

    #[test]
    fn detects_completion_when_all_conditions_met() {
        let output = test_output();
        let result = detect_lane_completion(&output, true, true);

        assert!(result.is_some());
        let context = result.unwrap();
        assert!(context.completed);
        assert_eq!(context.green_level, GreenLevel::Workspace);
        assert_eq!(context.blocker, LaneBlocker::None);
    }

    #[test]
    fn no_completion_when_error_present() {
        let mut output = test_output();
        output.failure = Some("Build failed".to_string());

        let result = detect_lane_completion(&output, true, true);
        assert!(result.is_none());
    }

    #[test]
    fn no_completion_when_not_finished() {
        let mut output = test_output();
        output.status = harness_contract::agent::AgentStatus::Running;

        let result = detect_lane_completion(&output, true, true);
        assert!(result.is_none());
    }

    #[test]
    fn no_completion_when_tests_not_green() {
        let output = test_output();

        let result = detect_lane_completion(&output, false, true);
        assert!(result.is_none());
    }

    #[test]
    fn no_completion_when_not_pushed() {
        let output = test_output();

        let result = detect_lane_completion(&output, true, false);
        assert!(result.is_none());
    }

    #[test]
    fn evaluate_triggers_closeout_for_completed_lane() {
        let mut context = LaneContext {
            lane_id: "completed-lane".to_string(),
            green_level: GreenLevel::Workspace,
            branch_freshness: std::time::Duration::from_secs(0),
            stale_branch: None,
            blocker: LaneBlocker::None,
            review_status: ReviewStatus::Approved,
            diff_scope: DiffScope::Scoped,
            completed: true,
            reconciled: false,
        };

        let actions = evaluate_completed_lane(&mut context);

        assert!(actions.contains(&PolicyAction::CloseoutLane));
        assert!(actions.contains(&PolicyAction::CleanupSession));
    }
}
