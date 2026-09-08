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

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn postgres_memory_discovery_versions_content_and_lifecycle_atomically() {
    use memory::store::MemoryDiscoveryQuery;
    let (base, scoped, schema) = isolated_executor();
    let store = PostgresMemoryStore::new(scoped.clone()).unwrap();
    let now = chrono::Utc::now();
    let mut sample = MemoryEntry {
        id: uuid::Uuid::nil(),
        layer: MemoryLayer::L3,
        category: MemoryCategory::Reference,
        priority: Priority::Normal,
        source: MemorySource::Import,
        title: "needle".into(),
        content: "needle 中文末尾".into(),
        embedding: None,
        tags: vec![],
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: now,
        updated_at: now,
        last_accessed_at: None,
        scope: MemoryScope::Project("authorized".into()),
        session_id: None,
        source_agent: None,
        visibility: AgentVisibility::Private,
    };
    for index in 1..=140 {
        sample.id = uuid::Uuid::from_u128(index);
        store.insert(&sample).await.unwrap();
    }
    let query = MemoryDiscoveryQuery {
        scopes: vec![sample.scope.clone()],
        query: "needle".into(),
        after_id: None,
        skip: 0,
        expected_revisions: None,
        limit: 13,
    };
    let first = store.discover_page(query.clone()).await.unwrap();
    let resume = MemoryDiscoveryQuery {
        after_id: first.next_id.clone(),
        expected_revisions: Some(first.revisions.clone()),
        ..query.clone()
    };
    let mut current = query.clone();
    let mut ids = std::collections::BTreeSet::new();
    loop {
        let page = store.discover_page(current.clone()).await.unwrap();
        for entry in page.entries {
            assert!(ids.insert(entry.id));
        }
        let Some(after) = page.next_id else {
            break;
        };
        current.after_id = Some(after);
        current.expected_revisions = Some(page.revisions);
    }
    assert_eq!(ids.len(), 140);
    // get increments persisted usage metadata; it must not invalidate discovery.
    let mut saved = store.get(&sample.id).await.unwrap().unwrap();
    store.discover_page(resume.clone()).await.unwrap();
    sample.id = uuid::Uuid::new_v4();
    sample.scope = MemoryScope::Project("unrelated".into());
    store.insert(&sample).await.unwrap();
    store.discover_page(resume.clone()).await.unwrap();
    saved.content.push_str(" changed");
    store.update(&saved).await.unwrap();
    assert!(store
        .discover_page(resume)
        .await
        .unwrap_err()
        .to_string()
        .contains("source changed"));
    let fresh = store.discover_page(query.clone()).await.unwrap();
    let resume = MemoryDiscoveryQuery {
        after_id: fresh.next_id,
        expected_revisions: Some(fresh.revisions),
        ..query.clone()
    };
    let key = format!("memory_lifecycle:{}", saved.id);
    store.kv_put(&key, "[{\"to\":\"Archived\"}]").await.unwrap();
    assert!(store.discover_page(resume).await.is_err());
    let all = store
        .discover_page(MemoryDiscoveryQuery {
            after_id: Some(uuid::Uuid::from_u128(139).to_string()),
            ..query.clone()
        })
        .await
        .unwrap();
    assert_eq!(all.lifecycle[0].key, key);
    let before = all.revisions.clone();
    store.kv_put(&key, "[{\"to\":\"Archived\"}]").await.unwrap();
    let reopened = PostgresMemoryStore::new(scoped).unwrap();
    assert_eq!(
        reopened
            .discover_page(query.clone())
            .await
            .unwrap()
            .revisions,
        before
    );
    store.delete(&saved.id).await.unwrap();
    assert!(reopened
        .discover_page(MemoryDiscoveryQuery {
            expected_revisions: Some(before),
            ..query
        })
        .await
        .is_err());
    drop(reopened);
    drop(store);
    base.checkout_critical()
        .unwrap()
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .unwrap();
}
