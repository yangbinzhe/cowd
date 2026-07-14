#![allow(clippy::expect_used, clippy::unwrap_used)]

use runtime::{RuntimeEventScope, RuntimeServices, TaskLifecycleEvent, TaskLifecycleKind};

/// A tool-facing consumer can read durable evidence through the projection
/// port, while all writes remain narrow domain commands.  This test uses the
/// same service object that a Runtime tool host receives and proves the
/// resulting durable event is a task-domain event, not a caller-chosen
/// control-plane mutation.
#[test]
fn tool_facing_projection_port_is_read_only_while_domain_command_owns_the_write_shape() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let reader = services.event_reader();
    assert!(reader
        .list_scope(RuntimeEventScope::Evolution, 10)
        .expect("read-only empty projection")
        .is_empty());

    services
        .record_task_lifecycle(TaskLifecycleEvent {
            task_id: "task:v0-tool-boundary".to_string(),
            kind: TaskLifecycleKind::Started,
            payload: serde_json::json!({"origin": "tool-facing workflow"}),
        })
        .expect("typed task command");

    let task_events = reader
        .list_scope(RuntimeEventScope::Task, 10)
        .expect("task projection");
    assert_eq!(task_events.len(), 1);
    assert_eq!(task_events[0].kind, "task.started");
    assert!(reader
        .list_scope(RuntimeEventScope::Evolution, 10)
        .expect("control-plane projection")
        .is_empty());
}
