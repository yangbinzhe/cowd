#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! Tests for `SessionMessage` CRUD and FTS5 search.
//!
//! Covers:
//! - Schema existence (messages table, indexes, FTS virtual table)
//! - Insert and retrieve single message
//! - Bulk insert in a transaction
//! - Pagination (offset/limit)
//! - Delete messages from a given sequence
//! - FTS5 search (English + Chinese)
//! - Multi-block content extraction for FTS indexing

use session::{SessionMessage, SqliteSessionStore};

// Keep all SQLite integration contracts in one process. This preserves every
// contract case while avoiding repeated crate startup for filtered page and
// batch workloads in local and CI performance gates.
#[path = "support/shared_backend_contract.rs"]
mod shared_backend_contract;
#[path = "support/sqlite_backend_contract.rs"]
mod sqlite_backend_contract;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_store() -> (SqliteSessionStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sessions.db");
    let store = SqliteSessionStore::open(&path).expect("open session store");
    (store, dir)
}

fn make_session(store: &SqliteSessionStore, id: &str) {
    use session::SessionRecord;
    store
        .create_session(&SessionRecord {
            session_id: id.to_string(),
            platform: "test".to_string(),
            chat_id: format!("chat-{id}"),
            user_id: Some("user-1".to_string()),
            model: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            last_activity: "2024-01-01T00:01:00Z".to_string(),
            message_count: 0,
            reset_policy: "None".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .expect("create session");
}

fn make_message(
    session_id: &str,
    sequence: usize,
    role: &str,
    content_text: &str,
) -> SessionMessage {
    SessionMessage {
        stable_message_id: format!("test:{session_id}:{sequence}"),
        session_id: session_id.to_string(),
        sequence,
        role: role.to_string(),
        content_json: format!(r#"[{{"type":"text","text":"{}"}}]"#, content_text),
        blocks_count: 1,
        tool_use_id: None,
        tool_name: None,
        token_usage_json: None,
        created_at_ms: 1700000000000 + sequence as u64,
    }
}

fn make_multi_block_message(session_id: &str, sequence: usize) -> SessionMessage {
    SessionMessage {
        stable_message_id: format!("test:{session_id}:{sequence}"),
        session_id: session_id.to_string(),
        sequence,
        role: "assistant".to_string(),
        content_json: r#"[
            {"type":"text","text":"I will help with that."},
            {"type":"tool_use","id":"tool_abc","name":"read_file","input":{"path":"/foo.rs"}},
            {"type":"tool_result","tool_use_id":"tool_abc","content":[{"type":"text","text":"fn main() {}"}]},
            {"type":"text","text":"Here is the result."}
        ]"#
        .to_string(),
        blocks_count: 4,
        tool_use_id: Some("tool_abc".to_string()),
        tool_name: Some("read_file".to_string()),
        token_usage_json: Some(r#"{"input":100,"output":50}"#.to_string()),
        created_at_ms: 1700000000000 + sequence as u64,
    }
}

#[test]
fn message_persistence_insert_and_get() {
    let (store, _dir) = make_store();
    make_session(&store, "sess-1");

    let msg = make_message("sess-1", 0, "user", "Hello world");
    store.insert_message(&msg).expect("insert message");

    let msgs = store.get_messages("sess-1", 0, 10).expect("get messages");
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].session_id, "sess-1");
    assert_eq!(msgs[0].sequence, 0);
    assert_eq!(msgs[0].role, "user");
    assert!(msgs[0].content_json.contains("Hello world"));
}

// ---------------------------------------------------------------------------
// Test 3: Bulk insert
// ---------------------------------------------------------------------------

#[test]
fn message_persistence_batch_insert() {
    let (store, _dir) = make_store();
    make_session(&store, "sess-2");

    let batch: Vec<SessionMessage> = (0..5)
        .map(|i| make_message("sess-2", i, "user", &format!("msg {i}")))
        .collect();

    store.insert_messages_batch(&batch).expect("batch insert");

    let msgs = store.get_messages("sess-2", 0, 20).expect("get messages");
    assert_eq!(msgs.len(), 5);
    assert_eq!(msgs[0].sequence, 0);
    assert_eq!(msgs[4].sequence, 4);

    let count = store.get_message_count("sess-2").expect("count");
    assert_eq!(count, 5);
}

// ---------------------------------------------------------------------------
// Test 4: Pagination
// ---------------------------------------------------------------------------

#[test]
fn message_persistence_pagination() {
    let (store, _dir) = make_store();
    make_session(&store, "sess-3");

    let batch: Vec<SessionMessage> = (0..10)
        .map(|i| make_message("sess-3", i, "user", &format!("p{i}")))
        .collect();
    store.insert_messages_batch(&batch).expect("batch insert");

    // First page (offset 0, limit 3)
    let page1 = store.get_messages("sess-3", 0, 3).expect("page 1");
    assert_eq!(page1.len(), 3);
    assert_eq!(page1[0].sequence, 0);
    assert_eq!(page1[2].sequence, 2);

    // Second page (offset 3, limit 3)
    let page2 = store.get_messages("sess-3", 3, 3).expect("page 2");
    assert_eq!(page2.len(), 3);
    assert_eq!(page2[0].sequence, 3);
    assert_eq!(page2[2].sequence, 5);

    // Last page (offset 9, limit 5)
    let page3 = store.get_messages("sess-3", 9, 5).expect("page 3");
    assert_eq!(page3.len(), 1);
    assert_eq!(page3[0].sequence, 9);
}

// ---------------------------------------------------------------------------
// Test 5: Delete messages from sequence
// ---------------------------------------------------------------------------

#[test]
fn message_persistence_delete_from() {
    let (store, _dir) = make_store();
    make_session(&store, "sess-4");

    let batch: Vec<SessionMessage> = (0..10)
        .map(|i| make_message("sess-4", i, "user", &format!("d{i}")))
        .collect();
    store.insert_messages_batch(&batch).expect("batch insert");

    // Delete from sequence 5 onwards
    let removed = store
        .delete_messages_from("sess-4", 5)
        .expect("delete from");
    assert_eq!(removed, 5);

    let remaining = store.get_messages("sess-4", 0, 20).expect("get remaining");
    assert_eq!(remaining.len(), 5);
    assert_eq!(remaining[0].sequence, 0);
    assert_eq!(remaining[4].sequence, 4);
}

// ---------------------------------------------------------------------------
// Test 6: FTS5 search (English)
// ---------------------------------------------------------------------------

#[test]
fn message_persistence_fts_english() {
    let (store, _dir) = make_store();
    make_session(&store, "sess-en");

    store
        .insert_message(&make_message(
            "sess-en",
            0,
            "user",
            "How do I write a Rust async function?",
        ))
        .expect("insert 1");
    store
        .insert_message(&make_message(
            "sess-en",
            1,
            "assistant",
            "You can use the async keyword before fn to declare an async function in Rust.",
        ))
        .expect("insert 2");
    store
        .insert_message(&make_message(
            "sess-en",
            2,
            "user",
            "Thanks, that is helpful.",
        ))
        .expect("insert 3");

    // Search for "async"
    let results = store
        .search_messages("async", Some("sess-en"), 10)
        .expect("search messages");
    assert!(!results.is_empty(), "should find messages about async");
    // The assistant message should match
    assert!(
        results
            .iter()
            .any(|m| m.content_json.contains("async keyword")),
        "should find the async explanation"
    );

    // Search for "Rust"
    let results_rust = store
        .search_messages("Rust", None, 10)
        .expect("search all sessions");
    assert!(!results_rust.is_empty());
}

// ---------------------------------------------------------------------------
// Test 7: FTS5 search (Chinese)
// ---------------------------------------------------------------------------

#[test]
fn message_persistence_fts_chinese() {
    let (store, _dir) = make_store();
    make_session(&store, "sess-zh");

    store
        .insert_message(&make_message(
            "sess-zh",
            0,
            "user",
            "你能解释一下什么是异步编程吗？",
        ))
        .expect("insert zh 1");
    store
        .insert_message(&make_message(
            "sess-zh",
            1,
            "assistant",
            "异步编程是一种不阻塞线程的编程方式。",
        ))
        .expect("insert zh 2");

    // FTS5 default tokenizer treats CJK characters as single tokens; MATCH works
    // but requires the exact token form. We use a simple prefix query.
    let results = store
        .search_messages("异步*", Some("sess-zh"), 10)
        .expect("search zh messages");
    assert!(
        !results.is_empty(),
        "should find Chinese messages about async"
    );
}

// ---------------------------------------------------------------------------
// Test 8: Multi-block content extraction for FTS
// ---------------------------------------------------------------------------

#[test]
fn message_persistence_multi_block() {
    let (store, _dir) = make_store();
    make_session(&store, "sess-mb");

    let msg = make_multi_block_message("sess-mb", 0);
    store.insert_message(&msg).expect("insert multi-block");

    // Verify the message was stored correctly
    let msgs = store.get_messages("sess-mb", 0, 10).expect("get messages");
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].blocks_count, 4);
    assert_eq!(msgs[0].tool_name.as_deref(), Some("read_file"));

    // FTS should index text blocks: "I will help with that." and "Here is the result."
    // but NOT the tool_use/tool_result JSON structures
    let results = store
        .search_messages("help", Some("sess-mb"), 10)
        .expect("search for help");
    assert!(!results.is_empty(), "should find message with 'help' text");

    let results2 = store
        .search_messages("result", Some("sess-mb"), 10)
        .expect("search for result");
    assert!(
        !results2.is_empty(),
        "should find message with 'result' text"
    );

    // Tool name search via the tool_name column
    let results3 = store
        .search_messages("read_file", Some("sess-mb"), 10)
        .expect("search for tool name");
    assert!(!results3.is_empty(), "should find message by tool_name");
}
