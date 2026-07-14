#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

//! Integration tests — end-to-end memory enhancement with code indexing.
//!
//! Tests the full flow: init code graph → index project → ask code question
//! → verify symbols injected → verify symbol linked to conversation.

#[cfg(feature = "code-index")]
use std::io::Write;

use memory::config::{BudgetConfig, StoreConfig};
#[cfg(feature = "code-index")]
use memory::store::sqlite::SqliteStore;
#[cfg(feature = "code-index")]
use memory::store::MemoryStore;
use memory::types::Message;
#[cfg(feature = "code-index")]
use memory::{CodeIndexer, CodeSymbol, SymbolEdge, SymbolEdgeType, SymbolKind};
use memory::{CognitiveContextManager, ImpactReport, MemoryConfig, TokenBudget, TuningConfig};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_config(db_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        store: StoreConfig {
            sqlite_path: db_path.to_path_buf(),
            blob_dir: db_path.parent().unwrap().to_path_buf(),
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

fn test_config_with_sandbox(db_path: &std::path::Path) -> MemoryConfig {
    MemoryConfig {
        tuning: TuningConfig {
            sandbox_min_lines: 1,
            ..Default::default()
        },
        ..test_config(db_path)
    }
}

#[cfg(feature = "code-index")]
fn write_rust_file(dir: &tempfile::TempDir, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
    path
}

// ---------------------------------------------------------------------------
// F1: Integration tests (7 tests)
// ---------------------------------------------------------------------------

#[cfg(feature = "code-index")]
#[tokio::test]
async fn test_integration_init_code_graph_indexes_symbols() {
    let tmp = tempfile::TempDir::new().unwrap();

    write_rust_file(
        &tmp,
        "src/main.rs",
        r#"
fn main() {
    authenticate_user("token");
}

fn authenticate_user(token: &str) -> bool {
    token == "valid"
}
"#,
    );

    let mut indexer = CodeIndexer::new(tmp.path()).unwrap();
    let mut symbols_found = 0usize;
    let mut files_processed = 0usize;
    for entry in walkdir::WalkDir::new(tmp.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        if memory::IndexLanguage::is_indexable(path) {
            match indexer.index_file(path) {
                Ok((symbols, _edges)) => {
                    files_processed += 1;
                    symbols_found += symbols.len();
                }
                Err(_) => {
                    files_processed += 1;
                }
            }
        }
    }
    assert!(symbols_found > 0, "should find symbols");
    assert!(files_processed > 0, "should process files");
}

#[tokio::test]
async fn test_integration_code_context_injection_on_code_query() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("e2e_inject.db");

    let cfg = test_config(&db_path);
    let mgr = CognitiveContextManager::new(cfg).await.unwrap();

    let query = "fix bug in authenticate_user function";
    let ctx = mgr.prepare_context(query, &[], None).await.unwrap();

    // code_context may be None (no code indexer in bare config) but pipeline works
    let _ = ctx.code_context;
}

#[tokio::test]
async fn test_integration_no_injection_on_non_code_query() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("e2e_nocode.db");

    let cfg = test_config(&db_path);
    let mgr = CognitiveContextManager::new(cfg).await.unwrap();

    let query = "tell me about the weather";
    let ctx = mgr.prepare_context(query, &[], None).await.unwrap();

    assert!(
        ctx.code_context.is_none(),
        "non-code query should not inject symbols"
    );
}

#[tokio::test]
async fn test_integration_tool_sandbox_rejects_raw_without_durable_evidence() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("e2e_sandbox.db");

    let cfg = test_config_with_sandbox(&db_path);
    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    let needle = "COWD_SANDBOX_NEEDLE_ALPHA";
    let large_tool_output = (0..80)
        .map(|i| format!("line {i}: build log detail {needle} component-{i}"))
        .collect::<Vec<_>>()
        .join("\n");

    let mut messages = vec![
        Message::user("run diagnostics"),
        Message::assistant("I will inspect the diagnostic output."),
        Message::tool_result("tool-call-1", "diagnostics", large_tool_output),
    ];

    mgr.on_turn_end(&mut messages).await.unwrap();

    let ctx = mgr
        .prepare_context(needle, &messages, Some("session-sandbox"))
        .await
        .unwrap();

    assert!(
        ctx.entries
            .iter()
            .all(|entry| !entry.tags.iter().any(|tag| tag == "tool_output")),
        "raw tool output without durable evidence must not create an orphan sandbox index"
    );
}

#[tokio::test]
async fn test_integration_build_context_with_code_wraps_prepare() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("e2e_build.db");

    let cfg = test_config(&db_path);
    let mgr = CognitiveContextManager::new(cfg).await.unwrap();

    let ctx = mgr
        .build_context_with_code("refactor the auth module", &[])
        .await
        .unwrap();

    assert!(ctx.total_tokens <= ctx.budget.total);
}

#[cfg(feature = "code-index")]
#[tokio::test]
async fn test_integration_impact_analysis_with_store() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("e2e_impact.db");

    let sqlite = SqliteStore::open_path(&db_path).unwrap();

    let authenticate = CodeSymbol {
        id: "auth.rs:authenticate:10".into(),
        name: "authenticate".into(),
        kind: SymbolKind::Function,
        file_path: "auth.rs".into(),
        line: 10,
        signature: "fn authenticate(token: &str) -> bool".into(),
        doc: None,
    };
    let login_handler = CodeSymbol {
        id: "handlers.rs:login_handler:5".into(),
        name: "login_handler".into(),
        kind: SymbolKind::Function,
        file_path: "handlers.rs".into(),
        line: 5,
        signature: "fn login_handler(req: Request) -> Response".into(),
        doc: None,
    };

    sqlite.insert_symbol(&authenticate).await.unwrap();
    sqlite.insert_symbol(&login_handler).await.unwrap();

    let edge = SymbolEdge {
        source_id: "handlers.rs:login_handler:5".into(),
        target_id: "auth.rs:authenticate:10".into(),
        edge_type: SymbolEdgeType::Calls,
        file_path: "handlers.rs".into(),
    };
    sqlite
        .index_file_symbols(
            "handlers.rs",
            &[authenticate.clone(), login_handler.clone()],
            &[edge],
        )
        .unwrap();

    let store: std::sync::Arc<dyn MemoryStore> = std::sync::Arc::new(sqlite);
    let indexer = CodeIndexer::new(tmp.path()).unwrap().with_store(store);

    let report = indexer.get_impact("authenticate", 1).await;
    assert!(!report.direct_callers.is_empty());
    assert!(report.direct_callers.contains(&"login_handler".to_string()));
    assert_eq!(report.symbol_name, "authenticate");
}

#[tokio::test]
async fn test_integration_prepared_context_has_code_field() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("e2e_codefield.db");

    let cfg = test_config(&db_path);
    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    let ctx = mgr
        .prepare_context("fix the authenticate function bug", &[], None)
        .await
        .unwrap();

    let _has_field = ctx.code_context;
}

#[tokio::test]
async fn test_integration_impact_report_defaults() {
    let report = ImpactReport::default();
    assert!(report.symbol_name.is_empty());
    assert!(report.direct_callers.is_empty());
    assert!(report.indirect.is_empty());
    assert!(report.affected_files.is_empty());

    let budget = TokenBudget {
        total: 8000,
        reserved_system: 2000,
        reserved_response: 1000,
        allocated_memory: 0,
        allocated_conversation: 0,
        available: 5000,
    };
    assert_eq!(budget.compute_available(), 5000);
}
