//! Public MemoryStore behavior retained from the original backend contract.
//! Real PostgreSQL namespaces; no legacy database migration or fallback.
use memory::{
    code_indexer::{CodeSymbol, SymbolEdge, SymbolEdgeType, SymbolKind},
    project_scope::MemoryScope,
    store::{MemoryScanCursor, MemoryStore, TaggedLookup},
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Priority,
};
use memory_postgres::PostgresMemoryStore;
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

fn open_store() -> (PostgresTestScope, PostgresMemoryStore) {
    let fixture = PostgresTestScope::new();
    let store = PostgresMemoryStore::new(fixture.reconnect()).expect("owned PG memory namespace");
    (fixture, store)
}
use uuid::Uuid;

fn uid(s: &str) -> Uuid {
    // Use deterministic UUIDs from known strings for stable test IDs
    let bytes = s.as_bytes();
    let mut buf = [0u8; 16];
    for (i, &b) in bytes.iter().take(16).enumerate() {
        buf[i] = b;
    }
    Uuid::from_bytes(buf)
}

fn entry(id_suffix: &str, title: &str, content: &str, layer: MemoryLayer) -> MemoryEntry {
    use chrono::Utc;
    MemoryEntry {
        id: uid(id_suffix),
        layer,
        category: MemoryCategory::Decision,
        priority: Priority::Normal,
        source: MemorySource::AutoExtracted,
        title: title.into(),
        content: content.into(),
        embedding: None,
        tags: vec![],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::default(),
        session_id: None,
        source_agent: None,
        visibility: AgentVisibility::default(),
    }
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn insert_and_get_roundtrip() {
    let (_fixture, store) = open_store();
    let id = uid("roundtrip");
    let e = entry("roundtrip", "Test", "Some content", MemoryLayer::L1);
    store.insert(&e).await.unwrap();

    let got = store.get(&id).await.unwrap().unwrap();
    assert_eq!(got.title, "Test");
    assert_eq!(got.content, "Some content");
    assert_eq!(got.layer, MemoryLayer::L1);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn insert_or_replace() {
    let (_fixture, store) = open_store();
    let id = uid("replace");
    let e1 = entry("replace", "V1", "C1", MemoryLayer::L1);
    store.insert(&e1).await.unwrap();

    let mut e2 = entry("replace", "V2", "C2", MemoryLayer::L1);
    e2.id = id;
    store.insert(&e2).await.unwrap();

    let got = store.get(&id).await.unwrap().unwrap();
    assert_eq!(got.title, "V2");
    assert_eq!(got.content, "C2");
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn get_returns_none_for_missing() {
    let (_fixture, store) = open_store();
    let fake = Uuid::new_v4();
    assert!(store.get(&fake).await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn update_modifies_existing() {
    let (_fixture, store) = open_store();
    let e = entry("update", "Original", "Old", MemoryLayer::L1);
    let id = e.id;
    store.insert(&e).await.unwrap();

    let mut updated = e.clone();
    updated.content = "New content".into();
    updated.staleness = 0.5;
    store.update(&updated).await.unwrap();

    let got = store.get(&id).await.unwrap().unwrap();
    assert_eq!(got.content, "New content");
    assert_eq!(got.staleness, 0.5);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn delete_removes_entry() {
    let (_fixture, store) = open_store();
    let e = entry("delete", "T", "C", MemoryLayer::L1);
    let id = e.id;
    store.insert(&e).await.unwrap();
    assert!(store.get(&id).await.unwrap().is_some());

    store.delete(&id).await.unwrap();
    assert!(store.get(&id).await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn delete_idempotent() {
    let (_fixture, store) = open_store();
    store.delete(&Uuid::new_v4()).await.unwrap();
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn search_by_layer_filters_correctly() {
    let (_fixture, store) = open_store();
    store
        .insert(&entry("a", "A", "aa", MemoryLayer::L1))
        .await
        .unwrap();
    store
        .insert(&entry("b", "B", "bb", MemoryLayer::L2))
        .await
        .unwrap();
    store
        .insert(&entry("c", "C", "cc", MemoryLayer::L1))
        .await
        .unwrap();

    let l1 = store.search_by_layer(MemoryLayer::L1).await.unwrap();
    assert_eq!(l1.len(), 2);

    let l2 = store.search_by_layer(MemoryLayer::L2).await.unwrap();
    assert_eq!(l2.len(), 1);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn aggregate_counts_layers_health_and_inactive_lifecycle_without_loading_bodies() {
    let (_fixture, store) = open_store();
    let mut active = entry("aggregate-active", "Active", "body", MemoryLayer::L1);
    active.tags.push("linked".to_string());
    active.confidence = 0.2;
    active.staleness = 0.9;
    let archived = entry("aggregate-archived", "Archived", "body", MemoryLayer::L3);
    store.insert(&active).await.unwrap();
    store.insert(&archived).await.unwrap();
    store
        .kv_put(
            &format!("memory_lifecycle:{}", archived.id),
            &serde_json::json!([{"to": "Archived"}]).to_string(),
        )
        .await
        .unwrap();

    let aggregate = store.aggregate(0.85).await.unwrap();
    assert_eq!(aggregate.total_entries, 2);
    assert_eq!(aggregate.active_entries, 1);
    assert_eq!(aggregate.orientation_like, 1);
    assert_eq!(aggregate.conflicted, 1);
    assert_eq!(aggregate.stale, 1);
    assert_eq!(aggregate.linked, 1);
    assert_eq!(aggregate.evidence_backed, 2);
    let l3 = aggregate
        .layers
        .iter()
        .find(|layer| layer.layer == MemoryLayer::L3)
        .unwrap();
    assert_eq!(l3.retained_count, 1);
    assert_eq!(l3.active_count, 0);
    assert_eq!(l3.archived_count, 1);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn search_by_category_returns_matching() {
    let (_fixture, store) = open_store();
    let mut e1 = entry("cat_a", "A", "aa", MemoryLayer::L1);
    e1.category = MemoryCategory::Decision;
    let e1_id = e1.id;
    let mut e2 = entry("cat_b", "B", "bb", MemoryLayer::L1);
    e2.category = MemoryCategory::Reference;
    store.insert(&e1).await.unwrap();
    store.insert(&e2).await.unwrap();

    let decisions = store
        .search_by_category(MemoryCategory::Decision)
        .await
        .unwrap();
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].id, e1_id);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn search_fts_finds_by_content() {
    let (_fixture, store) = open_store();
    let e1 = entry(
        "fts1",
        "Rust Guide",
        "Learn Rust programming language",
        MemoryLayer::L1,
    );
    let e1_id = e1.id;
    store.insert(&e1).await.unwrap();
    store
        .insert(&entry(
            "fts2",
            "Python Notes",
            "Data science with Python",
            MemoryLayer::L1,
        ))
        .await
        .unwrap();

    let results = store.search_fts("Rust", 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, e1_id);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn search_fts_returns_empty_for_no_match() {
    let (_fixture, store) = open_store();
    store
        .insert(&entry("fts3", "Rust", "content", MemoryLayer::L1))
        .await
        .unwrap();

    assert!(store
        .search_fts("zzzzzzzzzzzz", 10)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn list_metas_returns_summaries() {
    let (_fixture, store) = open_store();
    let e = entry("meta1", "A", "aa", MemoryLayer::L1);
    let id = e.id;
    store.insert(&e).await.unwrap();

    let metas = store.list_metas(Some(MemoryLayer::L1)).await.unwrap();
    assert!(!metas.is_empty());
    assert_eq!(metas[0].id, id);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn list_metas_all_layers() {
    let (_fixture, store) = open_store();
    store
        .insert(&entry("meta2", "A", "aa", MemoryLayer::L1))
        .await
        .unwrap();
    store
        .insert(&entry("meta3", "B", "bb", MemoryLayer::L2))
        .await
        .unwrap();

    let metas = store.list_metas(None).await.unwrap();
    assert_eq!(metas.len(), 2);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn list_all_returns_all_entries() {
    let (_fixture, store) = open_store();
    store
        .insert(&entry("all1", "A", "aa", MemoryLayer::L1))
        .await
        .unwrap();
    store
        .insert(&entry("all2", "B", "bb", MemoryLayer::L2))
        .await
        .unwrap();

    let all = store.list_all().await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn tagged_lookup_is_scoped_bounded_and_source_aware() {
    let (_fixture, store) = open_store();
    for (name, scope, source_agent, tags) in [
        (
            "selected",
            MemoryScope::Project("project-a".into()),
            Some("connector".to_string()),
            vec!["connector-ref:one".to_string()],
        ),
        (
            "wrong-source",
            MemoryScope::Project("project-a".into()),
            Some("growth".to_string()),
            vec!["connector-ref:one".to_string()],
        ),
        (
            "wrong-scope",
            MemoryScope::Project("project-b".into()),
            Some("connector".to_string()),
            vec!["connector-ref:one".to_string()],
        ),
    ] {
        let mut item = entry(name, name, name, MemoryLayer::L2);
        item.scope = scope;
        item.source_agent = source_agent;
        item.tags = tags;
        store.insert(&item).await.unwrap();
    }

    let candidates = store
        .lookup_tagged_candidates(TaggedLookup {
            scope: MemoryScope::Project("project-a".into()),
            tags_any: vec!["connector-ref:one".to_string()],
            source_agent: Some("connector".to_string()),
            limit: 1,
        })
        .await
        .unwrap();

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].title, "selected");
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn maintenance_scan_uses_stable_keyset_pages() {
    let (_fixture, store) = open_store();
    for index in 0..5 {
        store
            .insert(&entry(
                &format!("scan-{index}"),
                &format!("Scan {index}"),
                "bounded maintenance",
                MemoryLayer::L2,
            ))
            .await
            .unwrap();
    }

    let first = store
        .scan_entries_page(MemoryScanCursor::default(), 2)
        .await
        .unwrap();
    let second = store
        .scan_entries_page(first.next.clone().expect("next cursor"), 2)
        .await
        .unwrap();
    let third = store
        .scan_entries_page(second.next.clone().expect("next cursor"), 2)
        .await
        .unwrap();
    let ids = first
        .entries
        .iter()
        .chain(&second.entries)
        .chain(&third.entries)
        .map(|entry| entry.id)
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(first.entries.len(), 2);
    assert_eq!(second.entries.len(), 2);
    assert_eq!(third.entries.len(), 1);
    assert_eq!(ids.len(), 5);
    assert!(third.next.is_none());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn get_meta_returns_metadata() {
    let (_fixture, store) = open_store();
    let e = entry("getmeta", "A", "aa", MemoryLayer::L1);
    let id = e.id;
    store.insert(&e).await.unwrap();

    let meta = store.get_meta(&id).await.unwrap().unwrap();
    assert_eq!(meta.id, id);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn get_meta_returns_none_for_missing() {
    let (_fixture, store) = open_store();
    assert!(store.get_meta(&Uuid::new_v4()).await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn insert_preserves_all_fields() {
    let (_fixture, store) = open_store();
    let now = chrono::Utc::now();
    let id = Uuid::new_v4();
    let e = MemoryEntry {
        id,
        layer: MemoryLayer::L3,
        category: MemoryCategory::CompressedSummary,
        priority: Priority::High,
        source: MemorySource::Compression,
        title: "Full Entry".into(),
        content: "All fields present".into(),
        embedding: Some(vec![1.0, 2.0, 3.0]),
        tags: vec!["rust".into(), "async".into()],
        relations: vec![],
        confidence: 0.85,
        access_count: 5,
        staleness: 0.1,
        created_at: now,
        updated_at: now,
        last_accessed_at: Some(now),
        scope: MemoryScope::Project("project-1".into()),
        session_id: Some("session-1".into()),
        source_agent: None,
        visibility: AgentVisibility::default(),
    };
    store.insert(&e).await.unwrap();

    let got = store.get(&id).await.unwrap().unwrap();
    assert_eq!(got.id, id);
    assert_eq!(got.layer, MemoryLayer::L3);
    assert_eq!(got.category, MemoryCategory::CompressedSummary);
    assert_eq!(got.priority, Priority::High);
    assert_eq!(got.source, MemorySource::Compression);
    assert_eq!(got.title, "Full Entry");
    assert_eq!(got.content, "All fields present");
    assert_eq!(got.confidence, 0.85);
    assert_eq!(got.access_count, 5);
    assert_eq!(got.staleness, 0.1);
    assert_eq!(got.tags, vec!["rust", "async"]);
    assert_eq!(got.scope, MemoryScope::Project("project-1".into()));
    assert_eq!(got.session_id.as_deref(), Some("session-1"));
    assert!(got.embedding.is_some());
}

// -------------------------------------------------------------------
// Code symbol persistence tests (T2)
// -------------------------------------------------------------------

fn make_symbol(id: &str, name: &str, kind: SymbolKind, file_path: &str, line: usize) -> CodeSymbol {
    CodeSymbol {
        id: id.to_string(),
        name: name.to_string(),
        kind,
        file_path: file_path.to_string(),
        line,
        signature: format!("fn {name}()"),
        doc: None,
    }
}

fn make_edge(source: &str, target: &str, edge_type: SymbolEdgeType, file: &str) -> SymbolEdge {
    SymbolEdge {
        source_id: source.to_string(),
        target_id: target.to_string(),
        edge_type,
        file_path: file.to_string(),
    }
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn test_insert_and_query_symbol() {
    let (_fixture, store) = open_store();
    let sym = make_symbol(
        "src/main.rs:hello:10",
        "hello",
        SymbolKind::Function,
        "src/main.rs",
        10,
    );

    store
        .insert_symbol(&sym)
        .await
        .expect("insert symbol should succeed");

    let results = store
        .search_symbols("hello", 10)
        .await
        .expect("search should succeed");
    assert!(
        !results.is_empty(),
        "should find 'hello' via PostgreSQL full-text search"
    );
    assert_eq!(results[0].name, "hello");
    assert_eq!(results[0].kind, SymbolKind::Function);
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn test_symbol_full_text_search() {
    let (_fixture, store) = open_store();

    store
        .insert_symbol(&make_symbol(
            "a:alpha_func:1",
            "alpha_func",
            SymbolKind::Function,
            "a.rs",
            1,
        ))
        .await
        .unwrap();
    store
        .insert_symbol(&make_symbol(
            "b:bravo:2",
            "bravoClass",
            SymbolKind::Class,
            "b.rs",
            2,
        ))
        .await
        .unwrap();
    store
        .insert_symbol(&make_symbol(
            "c:setup:3",
            "setupServer",
            SymbolKind::Function,
            "c.rs",
            3,
        ))
        .await
        .unwrap();

    let results = store.search_symbols("alpha_func", 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "alpha_func");
    let results = store.search_symbols("bravoClass", 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "bravoClass");

    // Verify no match returns empty
    let empty = store.search_symbols("zzznonexistent", 1).await.unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn test_get_callers() {
    let (_fixture, store) = open_store();

    let caller = make_symbol("a:caller:1", "caller_fn", SymbolKind::Function, "a.rs", 1);
    let callee = make_symbol("b:callee:1", "callee_fn", SymbolKind::Function, "b.rs", 1);

    store.insert_symbol(&caller).await.unwrap();
    store.insert_symbol(&callee).await.unwrap();

    let edge = make_edge("a:caller:1", "b:callee:1", SymbolEdgeType::Calls, "a.rs");

    store.insert_edge(&edge).await.unwrap();

    let callers = store.get_callers("b:callee:1").await.unwrap();
    assert_eq!(callers.len(), 1, "should find one caller");
    assert_eq!(callers[0].name, "caller_fn");
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn test_get_callees() {
    let (_fixture, store) = open_store();

    let caller = make_symbol("a:call_main:1", "main", SymbolKind::Function, "a.rs", 1);
    let callee1 = make_symbol("a:foo:5", "foo", SymbolKind::Function, "a.rs", 5);
    let callee2 = make_symbol("a:bar:9", "bar", SymbolKind::Function, "a.rs", 9);

    store.insert_symbol(&caller).await.unwrap();
    store.insert_symbol(&callee1).await.unwrap();
    store.insert_symbol(&callee2).await.unwrap();

    let edges = vec![
        make_edge("a:call_main:1", "a:foo:5", SymbolEdgeType::Calls, "a.rs"),
        make_edge("a:call_main:1", "a:bar:9", SymbolEdgeType::Calls, "a.rs"),
    ];

    for edge in edges {
        store.insert_edge(&edge).await.unwrap();
    }

    let callees = store.get_callees("a:call_main:1").await.unwrap();
    assert_eq!(callees.len(), 2, "main should call foo and bar");
    assert!(callees.iter().any(|s| s.name == "foo"));
    assert!(callees.iter().any(|s| s.name == "bar"));
}

// -------------------------------------------------------------------
// T5: Symbol ↔ memory conversation linking
// -------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn test_symbol_conversation_link() {
    let (_fixture, store) = open_store();

    let memory_id = Uuid::new_v4();
    let symbol_id = "src/auth.rs:authenticate_user:10";
    let timestamp = chrono::Utc::now().timestamp();

    // Link a symbol to a memory entry
    let result = store
        .link_symbol_to_memory(symbol_id, &memory_id, Some(1), "tool_call", timestamp)
        .await;
    assert!(result.is_ok(), "linking symbol to memory should succeed");

    // Link another reference of the same symbol
    store
        .link_symbol_to_memory(symbol_id, &memory_id, Some(3), "response", timestamp + 10)
        .await
        .unwrap();

    // Find memories by symbol
    let mem_ids = store
        .find_memories_by_symbol("authenticate_user")
        .await
        .unwrap();
    assert!(!mem_ids.is_empty(), "should find the linked memory");
    assert!(mem_ids.contains(&memory_id));
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn test_find_conversations_by_symbol() {
    let (_fixture, store) = open_store();

    let mem1 = Uuid::new_v4();
    let mem2 = Uuid::new_v4();
    let now = chrono::Utc::now().timestamp();

    // Link symbol A to two different memories
    store
        .link_symbol_to_memory(
            "src/auth.rs:authenticate_user:10",
            &mem1,
            Some(1),
            "tool_call",
            now,
        )
        .await
        .unwrap();
    store
        .link_symbol_to_memory(
            "src/auth.rs:authenticate_user:10",
            &mem2,
            Some(2),
            "reference",
            now + 1,
        )
        .await
        .unwrap();

    // Link a different symbol to mem1
    store
        .link_symbol_to_memory(
            "src/auth.rs:TokenManager:25",
            &mem1,
            Some(2),
            "tool_call",
            now + 2,
        )
        .await
        .unwrap();

    // Find memories by authenticate_user
    let auth_mems = store
        .find_memories_by_symbol("authenticate_user")
        .await
        .unwrap();
    assert_eq!(
        auth_mems.len(),
        2,
        "authenticate_user should be linked to two memories"
    );
    assert!(auth_mems.contains(&mem1));
    assert!(auth_mems.contains(&mem2));

    // Find memories by TokenManager
    let token_mems = store.find_memories_by_symbol("TokenManager").await.unwrap();
    assert_eq!(
        token_mems.len(),
        1,
        "TokenManager should be linked to one memory"
    );
    assert_eq!(token_mems[0], mem1);

    // Find by non-existent symbol
    let none = store.find_memories_by_symbol("nonexistent").await.unwrap();
    assert!(none.is_empty());
}
