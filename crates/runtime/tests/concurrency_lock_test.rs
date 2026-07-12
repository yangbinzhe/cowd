#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! Concurrency safety tests for tokio::sync::RwLock-based Session lock.
//!
//! Verifies that switching from std::sync::RwLock to tokio::sync::RwLock
//! does not introduce deadlocks under concurrent read/write pressure.

use std::sync::Arc;
use std::time::Duration;

use runtime::Session;
use tokio::sync::RwLock;

/// Spawns 100 concurrent readers + 10 periodic writers against a shared
/// Session. The entire test must complete under 5 seconds — a timeout
/// signals a deadlock or starvation bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_no_deadlock_under_concurrent_reads_and_writes() {
    let session = Arc::new(RwLock::new(Session::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(111)); // 100 readers + 10 writers + main
    let start = tokio::time::Instant::now();

    // Spawn read tasks: each reads session fields 10 times
    let mut read_handles = Vec::with_capacity(100);
    for i in 0..100 {
        let session = Arc::clone(&session);
        let barrier = Arc::clone(&barrier);
        read_handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..10 {
                let guard = session.read().await;
                let _session_id_len = guard.session_id.len();
                let _msg_count = guard.messages.len();
                let _version = guard.version;
                drop(guard);
            }
            i // return reader index
        }));
    }

    // Spawn write tasks: each modifies session messages 5 times
    let mut write_handles = Vec::with_capacity(10);
    for j in 0..10 {
        let session = Arc::clone(&session);
        let barrier = Arc::clone(&barrier);
        write_handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for k in 0..5 {
                let mut guard = session.write().await;
                // Push a minimal message to exercise the write path
                let msg = runtime::ConversationMessage {
                    role: runtime::MessageRole::User,
                    blocks: vec![runtime::ContentBlock::Text {
                        text: format!("writer-{j}-msg-{k}"),
                    }],
                    usage: None,
                };
                guard.messages.push(msg);
                guard.updated_at_ms = 0;
                drop(guard);
            }
            j // return writer index
        }));
    }

    // Signal all tasks to start simultaneously
    barrier.wait().await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        // Await all readers
        for h in read_handles {
            let _ = h.await.expect("reader should not panic");
        }
        // Await all writers
        for h in write_handles {
            let _ = h.await.expect("writer should not panic");
        }
    })
    .await;

    let elapsed = start.elapsed();
    assert!(
        outcome.is_ok(),
        "deadlock or starvation detected: concurrent reads + writes did not complete within 5s (elapsed: {elapsed:.2?})"
    );

    // Verify the session has the expected number of messages (10 writers × 5 msgs each)
    let final_session = session.read().await;
    assert_eq!(
        final_session.messages.len(),
        50,
        "expected 50 messages from 10 writers × 5 messages each"
    );
}

/// Heavier stress test: 1000 concurrent operations mixed reads and writes
/// must complete under 10 seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_lock_contention_under_heavy_load() {
    let session = Arc::new(RwLock::new(Session::new()));
    let total_ops = 1000;
    let write_ratio = 0.1; // 10% writes, 90% reads
    let write_count = (total_ops as f64 * write_ratio) as usize;
    let read_count = total_ops - write_count;

    let mut handles = Vec::with_capacity(total_ops);

    // Spawn readers
    for i in 0..read_count {
        let session = Arc::clone(&session);
        handles.push(tokio::spawn(async move {
            let guard = session.read().await;
            let _sid = guard.session_id.clone();
            let _msgs = guard.messages.len();
            drop(guard);
            i
        }));
    }

    // Spawn writers
    for j in 0..write_count {
        let session = Arc::clone(&session);
        handles.push(tokio::spawn(async move {
            let mut guard = session.write().await;
            let msg = runtime::ConversationMessage {
                role: runtime::MessageRole::User,
                blocks: vec![runtime::ContentBlock::Text {
                    text: format!("heavy-{j}"),
                }],
                usage: None,
            };
            guard.messages.push(msg);
            guard.updated_at_ms = j as u64;
            drop(guard);
            j
        }));
    }

    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        for h in handles {
            let _ = h.await.expect("task should not panic");
        }
    })
    .await;

    assert!(
        outcome.is_ok(),
        "heavy-load test timed out: 1000 concurrent operations did not complete within 10s"
    );

    let final_session = session.read().await;
    assert_eq!(
        final_session.messages.len(),
        write_count,
        "expected {write_count} messages from writers"
    );
}
