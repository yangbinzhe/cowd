#![cfg(feature = "test-fixtures")]

use std::sync::Arc;

use runtime::{RuntimeEventInput, RuntimeEventScope, RuntimeEventStore};
use runtime::runtime_event_store::EphemeralRuntimeEventStore;

#[test]
fn a_shared_backend_commit_is_visible_even_when_the_reader_has_no_local_doorbell() {
    let backend = Arc::new(EphemeralRuntimeEventStore::new());
    let reader = RuntimeEventStore::from_backend(backend.clone());
    let writer = RuntimeEventStore::from_backend(backend);
    let hint = reader.current_commit_cursor();
    let signal = reader.subscribe_commits();
    let event = writer.append(RuntimeEventInput {
        stream_id: "kernel:external-write".into(), scope: RuntimeEventScope::Recovery,
        kind: "kernel.external_committed".into(), status: Some("committed".into()),
        actor: Some("separate-adapter".into()), refs: vec![], payload: serde_json::json!({}),
    }).unwrap();
    assert!(!signal.has_changed().unwrap());
    assert_eq!(reader.current_commit_cursor(), hint);
    assert_eq!(reader.durable_commit_cursor().unwrap(), event.commit_cursor);
    let recovered = reader.events_after_cursor(hint, 10).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].events[0].event_id, event.event_id);
}

#[test]
fn reconstructing_a_reader_keeps_the_committed_boundary_without_reexecuting_the_write() {
    let backend = Arc::new(EphemeralRuntimeEventStore::new());
    let writer = RuntimeEventStore::from_backend(backend.clone());
    let event = writer.append(RuntimeEventInput {
        stream_id: "kernel:reconstructed-reader".into(), scope: RuntimeEventScope::Recovery,
        kind: "kernel.external_committed".into(), status: None, actor: None, refs: vec![],
        payload: serde_json::json!({}),
    }).unwrap();
    drop(writer);
    let reader = RuntimeEventStore::from_backend(backend);
    assert_eq!(reader.durable_commit_cursor().unwrap(), event.commit_cursor);
    assert_eq!(reader.list_stream("kernel:reconstructed-reader").unwrap().len(), 1);
}
