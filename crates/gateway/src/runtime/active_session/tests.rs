use std::sync::Arc;

use super::*;

fn fixture() -> crate::runtime_entry::GatewayRuntimeEntry {
    crate::runtime_entry::GatewayRuntimeEntry::test_runtime_entry()
}

fn complete_fixture(session_id: &str) -> PreparedActiveSession {
    let policy = runtime::permissions::SessionExecutionPolicyControl::from_policy(
        runtime::SessionExecutionPolicy::from_defaults(
            runtime::PermissionMode::WorkspaceWrite,
            runtime::ApprovalProfile::Balanced,
        ),
    );
    PreparedActiveSession::complete(
        fixture(),
        runtime::SessionInputStream::new(session_id),
        Some(runtime::CowdEventBus::new()),
        Some("test-model".to_string()),
        policy,
        Some(SessionRelayLease::new(7)),
    )
}

#[test]
fn directory_preserves_sorted_lookup_and_removal_contract() {
    let sessions = ActiveSessionDirectory::new();
    sessions.register("b".into(), fixture()).unwrap();
    sessions.register("a".into(), fixture()).unwrap();
    assert_eq!(sessions.list(), vec!["a", "b"]);
    assert!(sessions.get("a").is_some());
    assert!(sessions.remove("a").is_some());
    assert!(sessions.get("a").is_none());
    assert_eq!(
        sessions.observations(),
        ActiveSessionObservations {
            registered: 2,
            unregistered: 1,
        }
    );
}

#[test]
fn replacement_advances_generation_even_at_capacity() {
    let sessions = ActiveSessionDirectory::with_max_sessions(1);
    sessions.register("same".into(), fixture()).unwrap();
    let first = sessions.session("same").unwrap().generation();
    let replaced = sessions.register("same".into(), fixture()).unwrap();
    let second = sessions.session("same").unwrap().generation();
    assert!(replaced.is_some());
    assert!(second > first);
    assert!(sessions.register("other".into(), fixture()).is_err());
}

#[test]
fn directory_contract_performance_smoke() {
    directory_preserves_sorted_lookup_and_removal_contract();
    replacement_advances_generation_even_at_capacity();
    let sessions = ActiveSessionDirectory::new();
    assert!(sessions.get("missing").is_none());
    assert!(sessions.remove("missing").is_none());
}

#[test]
fn concurrent_publications_never_expose_partial_or_duplicate_generation() {
    let sessions = Arc::new(ActiveSessionDirectory::new());
    let mut threads = Vec::new();
    for index in 0..256 {
        let sessions = Arc::clone(&sessions);
        threads.push(std::thread::spawn(move || {
            let id = format!("session-{index:03}");
            sessions.register(id.clone(), fixture()).unwrap();
            let aggregate = sessions.session(&id).unwrap();
            assert_eq!(aggregate.session_id(), id);
            assert!(aggregate.generation() > 0);
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(sessions.list().len(), 256);
    assert_eq!(sessions.observations().registered, 256);
}

#[test]
fn one_sixteen_and_two_hundred_fifty_six_parallel_lifecycles_are_atomic() {
    for concurrency in [1_usize, 16, 256] {
        let sessions = Arc::new(ActiveSessionDirectory::new());
        let barrier = Arc::new(std::sync::Barrier::new(concurrency));
        let mut threads = Vec::with_capacity(concurrency);
        for index in 0..concurrency {
            let sessions = Arc::clone(&sessions);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                let session_id = format!("lifecycle-{concurrency}-{index}");
                barrier.wait();
                sessions
                    .publish(session_id.clone(), complete_fixture(&session_id))
                    .unwrap();
                let aggregate = sessions.session(&session_id).expect("published aggregate");
                assert!(aggregate.input().is_some());
                assert!(aggregate.event_bus().is_some());
                assert_eq!(aggregate.model().as_deref(), Some("test-model"));
                assert!(aggregate.policy_control().is_some());
                assert_eq!(aggregate.relay().map(SessionRelayLease::task_id), Some(7));
                let removed = sessions
                    .remove_aggregate(&session_id)
                    .expect("published aggregate removes as one unit");
                assert_eq!(removed.generation(), aggregate.generation());
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(sessions.list().is_empty());
        assert_eq!(sessions.observations().registered, concurrency as u64);
        assert_eq!(sessions.observations().unregistered, concurrency as u64);
    }
}

#[test]
fn lifecycle_locks_serialize_same_key_without_serializing_other_keys() {
    let sessions = ActiveSessionDirectory::new();
    let same_a = sessions.transition_lock("same");
    let same_b = sessions.transition_lock("same");
    let other = sessions.transition_lock("other");
    assert!(Arc::ptr_eq(&same_a, &same_b));
    assert!(!Arc::ptr_eq(&same_a, &other));
}

#[test]
#[ignore = "phase performance gate; run explicitly with --ignored --nocapture"]
fn active_session_register_remove_microbench_gate() {
    use std::collections::HashMap;
    use std::time::Instant;

    const OPERATIONS: usize = 20_000;
    let mut legacy_samples = Vec::new();
    let mut aggregate_samples = Vec::new();
    for round in 0..7 {
        let legacy_carriers = std::sync::RwLock::new(HashMap::new());
        let legacy_inputs = std::sync::Mutex::new(HashMap::new());
        let legacy_events = std::sync::Mutex::new(HashMap::new());
        let legacy_models = std::sync::Mutex::new(HashMap::new());
        let legacy_policies = std::sync::Mutex::new(HashMap::new());
        let legacy_policy_locks = std::sync::Mutex::new(HashMap::new());
        let started = Instant::now();
        for index in 0..OPERATIONS {
            let id = format!("legacy-{round}-{index}");
            legacy_carriers.write().unwrap().insert(id.clone(), index);
            legacy_inputs.lock().unwrap().insert(id.clone(), index);
            legacy_events.lock().unwrap().insert(id.clone(), index);
            legacy_models.lock().unwrap().insert(id.clone(), index);
            legacy_policies.lock().unwrap().insert(id.clone(), index);
            legacy_policy_locks
                .lock()
                .unwrap()
                .insert(id.clone(), index);
            legacy_inputs.lock().unwrap().remove(&id);
            legacy_events.lock().unwrap().remove(&id);
            legacy_models.lock().unwrap().remove(&id);
            legacy_policies.lock().unwrap().remove(&id);
            legacy_policy_locks.lock().unwrap().remove(&id);
            legacy_carriers.write().unwrap().remove(&id);
        }
        legacy_samples.push(started.elapsed());

        let sessions = ActiveSessionDirectory::new();
        let started = Instant::now();
        for index in 0..OPERATIONS {
            let id = format!("aggregate-{round}-{index}");
            sessions
                .publish(id.clone(), PreparedActiveSession::carrier_only(fixture()))
                .unwrap();
            sessions.remove_aggregate(&id).unwrap();
        }
        aggregate_samples.push(started.elapsed());
    }
    legacy_samples.sort_unstable();
    aggregate_samples.sort_unstable();
    let legacy = legacy_samples[legacy_samples.len() / 2];
    let aggregate = aggregate_samples[aggregate_samples.len() / 2];
    let improvement = 1.0 - aggregate.as_secs_f64() / legacy.as_secs_f64();
    let legacy_p95 = legacy_samples[legacy_samples.len() - 1];
    let aggregate_p95 = aggregate_samples[aggregate_samples.len() - 1];
    let activation_p95_improvement = 1.0 - aggregate_p95.as_secs_f64() / legacy_p95.as_secs_f64();
    eprintln!(
        "active-session register/remove: legacy={legacy:?} aggregate={aggregate:?} improvement={:.2}%; activation-p95 legacy={legacy_p95:?} aggregate={aggregate_p95:?} improvement={:.2}%",
        improvement * 100.0,
        activation_p95_improvement * 100.0,
    );
    assert!(
        improvement >= 0.25,
        "aggregate register/remove throughput must improve by at least 25%; observed {:.2}%",
        improvement * 100.0
    );
    assert!(
        activation_p95_improvement >= 0.15,
        "aggregate activation p95 must improve by at least 15%; observed {:.2}%",
        activation_p95_improvement * 100.0
    );
}
