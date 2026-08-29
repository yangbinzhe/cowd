//! Real dual-backend digest conformance for durable Memory classes.

use memory::{
    project_scope::MemoryScope,
    store::{sqlite::SqliteStore, MemoryStore},
    types::AgentVisibility,
    MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Priority,
};
use memory_postgres::{copy_quiesced_memory_store, PostgresMemoryStore};
use storage::{PostgresConnectionConfig, StaticSecretRefResolver};

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn sqlite_and_postgres_memory_snapshots_have_equal_digests() {
    let sqlite = SqliteStore::open_in_memory().expect("SQLite Memory store");
    let now = chrono::Utc::now();
    sqlite
        .insert(&MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::ProjectKnowledge,
            priority: Priority::High,
            source: MemorySource::Import,
            title: "backend conformance".to_string(),
            content: "SQLite and PostgreSQL preserve the same memory truth".to_string(),
            embedding: Some(vec![0.25, 0.75]),
            tags: vec!["conformance".to_string()],
            relations: Vec::new(),
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: MemoryScope::Project("cowd".to_string()),
            session_id: Some("backend-conformance".to_string()),
            source_agent: Some("backend-conformance".to_string()),
            visibility: AgentVisibility::Private,
        })
        .await
        .expect("seed SQLite Memory entry");

    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let resolver = StaticSecretRefResolver::new([("memory.conformance.pg".to_string(), url)]);
    let postgres = PostgresMemoryStore::connect(
        PostgresConnectionConfig::new(
            "memory-backend-conformance",
            "memory.conformance.pg",
            "cowd-memory-backend-conformance",
        ),
        &resolver,
    )
    .expect("PostgreSQL Memory store");
    let manifest = copy_quiesced_memory_store(&sqlite, &postgres)
        .await
        .expect("quiesced Memory copy remains digest exact");

    assert_eq!(manifest.source_digest, manifest.target_digest);
    assert_eq!(manifest.entry_count, 1);
}
