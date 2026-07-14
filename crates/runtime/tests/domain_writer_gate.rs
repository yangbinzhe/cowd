#![allow(clippy::expect_used, clippy::unwrap_used)]

use runtime::{RuntimeEventScope, RuntimeServices, TaskLifecycleEvent, TaskLifecycleKind};

/// Gateway-facing lifecycle writes have a typed Runtime command.  The caller
/// supplies business payload only; it cannot select a ledger scope, event
/// family, or trusted actor.
#[test]
fn typed_task_writer_emits_only_the_canonical_task_lifecycle_event() {
    let services = RuntimeServices::in_memory().expect("runtime services");

    let event = services
        .record_task_lifecycle(TaskLifecycleEvent {
            task_id: "task:v0-domain-writer".to_string(),
            kind: TaskLifecycleKind::PhaseReviewed,
            payload: serde_json::json!({"summary": "review completed"}),
        })
        .expect("typed task lifecycle command");

    assert_eq!(event.scope, RuntimeEventScope::Task);
    assert_eq!(event.kind, "task.phase.reviewed");
    assert_eq!(event.actor.as_deref(), Some("gateway-task-command"));
    assert_eq!(event.stream_id, "task:v0-domain-writer");

    let projected = services
        .event_reader()
        .list_stream("task:v0-domain-writer")
        .expect("read-only event projection");
    assert_eq!(projected, vec![event]);
}
