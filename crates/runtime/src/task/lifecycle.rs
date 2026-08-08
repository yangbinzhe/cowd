//! Root/delegated lifecycle helpers.

use harness_contract::task::{TaskAggregate, TaskKind, TaskStatus};

#[must_use]
pub fn root_can_close(root: &TaskAggregate, children: &[TaskAggregate]) -> bool {
    root.kind == TaskKind::Root
        && !root.status.is_terminal()
        && children.iter().all(|child| {
            child.root_task_id == root.task_id
                && child.kind == TaskKind::Delegated
                && child.status.is_terminal()
        })
}

pub fn require_continuable(task: &TaskAggregate) -> Result<(), String> {
    if !status_is_continuable(task.status) {
        Err(format!(
            "terminal task `{}` requires a successor instead of continuation",
            task.task_id
        ))
    } else {
        Ok(())
    }
}

const fn status_is_continuable(status: TaskStatus) -> bool {
    !status.is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blocked_task_remains_continuable_for_a_replan_turn() {
        assert!(status_is_continuable(TaskStatus::Blocked));
        assert!(!status_is_continuable(TaskStatus::Completed));
    }
}
