#![allow(clippy::expect_used, clippy::unwrap_used)]

//! Zero-loss raw storage contract.

use memory::config::{BudgetConfig, StoreConfig};
use memory::{CognitiveContextManager, MemoryConfig, VerbatimEntry, VerbatimSink};

fn test_config(sqlite_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
            sqlite_path: sqlite_path.to_path_buf(),
            blob_dir: sqlite_path.parent().unwrap().join("blobs"),
            enable_vector_index: false,
            cache_capacity: 128,
            ..Default::default()
        },
        budget: BudgetConfig {
            context_window: 8000,
            reserved_system: 2000,
            reserved_response: 1000,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn verbatim_entries_remain_exact_searchable_and_durable_after_reopen() {
    let temporary = tempfile::TempDir::new().unwrap();
    let database = temporary.path().join("verbatim.db");

    // The memory store owns schema creation; the raw sink owns raw operations.
    drop(
        CognitiveContextManager::new(test_config(&database))
            .await
            .expect("initialize memory schema"),
    );

    let entry = VerbatimEntry {
        id: uuid::Uuid::new_v4().to_string(),
        content: "Line one.\n\n原始内容与空白必须保持不变。\n".to_string(),
        source: "UserExplicit".to_string(),
        layer: 3,
        timestamp: chrono::Utc::now().to_rfc3339(),
    };
    {
        let sink = VerbatimSink::new(database.to_str().expect("UTF-8 test path"))
            .expect("open verbatim sink");
        sink.store_raw(&entry).expect("store raw entry");
        assert_eq!(
            sink.search_by_content("%原始内容%")
                .expect("search raw content")
                .len(),
            1
        );
        assert_eq!(
            sink.search_by_entity("UserExplicit")
                .expect("search raw source")
                .len(),
            1
        );
    }

    let reopened = VerbatimSink::new(database.to_str().expect("UTF-8 test path"))
        .expect("reopen verbatim sink");
    let restored = reopened
        .retrieve_by_id(&entry.id)
        .expect("retrieve raw entry")
        .expect("raw entry survives reopen");
    assert_eq!(restored.id, entry.id);
    assert_eq!(restored.content, entry.content);
    assert_eq!(restored.source, entry.source);
    assert_eq!(restored.layer, entry.layer);
    assert_eq!(restored.timestamp, entry.timestamp);
}
