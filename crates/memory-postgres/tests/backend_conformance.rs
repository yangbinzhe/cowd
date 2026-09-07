//! PostgreSQL-only persistence conformance for durable Memory.

use memory::{
    project_scope::MemoryScope, store::MemoryStore, AgentVisibility, MemoryCategory, MemoryEntry,
    MemoryLayer, MemorySource, Priority,
};
use memory_postgres::PostgresMemoryStore;
use storage::{PostgresConnectionConfig, PostgresExecutor, StaticSecretRefResolver};

fn isolated_executor() -> (PostgresExecutor, PostgresExecutor, String) {
    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let resolver = StaticSecretRefResolver::new([("memory.conformance.pg".to_string(), url)]);
    let base = PostgresExecutor::connect(
        PostgresConnectionConfig::new(
            "memory-backend-conformance",
            "memory.conformance.pg",
            "cowd-memory-backend-conformance",
        ),
        &resolver,
    )
    .expect("PostgreSQL executor");
    let schema = format!("cowdmemory_{}", uuid::Uuid::new_v4().simple());
    base.checkout_critical()
        .expect("connection")
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .expect("create schema");
    let scoped = base.scoped_namespace(&schema).expect("scoped executor");
    (base, scoped, schema)
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn postgres_memory_survives_adapter_reconstruction() {
    let (base, scoped, schema) = isolated_executor();
    let id = uuid::Uuid::new_v4();
    let now = chrono::Utc::now();
    let first = PostgresMemoryStore::new(scoped.clone()).expect("Memory PostgreSQL store");
    first
        .insert(&MemoryEntry {
            id,
            layer: MemoryLayer::L3,
            category: MemoryCategory::ProjectKnowledge,
            priority: Priority::High,
            source: MemorySource::Import,
            title: "PostgreSQL conformance".to_string(),
            content: "canonical memory survives adapter reconstruction".to_string(),
            embedding: None,
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
        .expect("insert memory");
    drop(first);
    let reopened = PostgresMemoryStore::new(scoped).expect("reopen Memory PostgreSQL store");
    assert_eq!(
        reopened.get(&id).await.expect("read memory").unwrap().title,
        "PostgreSQL conformance"
    );
    drop(reopened);
    base.checkout_critical()
        .expect("cleanup connection")
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .expect("drop schema");
}
