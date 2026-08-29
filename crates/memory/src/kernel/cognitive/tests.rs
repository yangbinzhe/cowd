use super::*;
use crate::config::{BudgetConfig, MemoryConfig};
use crate::types::MemoryLayer;
use crate::write_guard::WriteSource;

fn test_config() -> MemoryConfig {
    MemoryConfig {
        budget: BudgetConfig {
            context_window: 8000,
            reserved_system: 2000,
            reserved_response: 1000,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn user_message(turn_index: usize, content: &str) -> Message {
    Message {
        turn_index,
        role: MessageRole::User,
        content: content.to_string(),
        tool_use_id: None,
        tool_name: None,
        pinned: false,
    }
}

#[test]
fn background_extraction_coalesces_retries_but_keeps_distinct_turns() {
    let mut batches = HashMap::new();
    let first_turn = MemoryTurnContext::new("session-a", "agent-a");
    let first = BackgroundExtractionRequest {
        turn: first_turn.clone(),
        messages: vec![user_message(0, "first")],
        heuristic_entries: Vec::new(),
    };
    assert!(!coalesce_background_request(&mut batches, first.clone(),));
    assert!(coalesce_background_request(&mut batches, first.clone(),));
    assert!(!coalesce_background_request(
        &mut batches,
        BackgroundExtractionRequest {
            turn: first_turn,
            messages: vec![user_message(1, "latest")],
            heuristic_entries: Vec::new(),
        },
    ));
    assert!(!coalesce_background_request(
        &mut batches,
        BackgroundExtractionRequest {
            turn: MemoryTurnContext::new("session-b", "agent-a"),
            messages: vec![user_message(0, "other session")],
            heuristic_entries: Vec::new(),
        },
    ));

    assert_eq!(batches.len(), 3);
    let first = batches
        .get(&background_extraction_key(&first))
        .expect("coalesced first turn");
    assert_eq!(first.1, 2);
    assert_eq!(first.0.messages[0].content, "first");
}

#[test]
fn automatic_extraction_identity_is_stable_within_scope_and_isolated_across_projects() {
    let extractor = MemoryExtractor::new(Default::default());
    let messages = vec![
        Message::user("I prefer using tabs for indentation, please always use tabs."),
        Message::assistant(
            "Understood. I've decided we'll use tabs for all Rust files in this project.",
        ),
    ];
    let seed = extractor.finalize_entries(extractor.extract_heuristic(&messages));
    assert!(!seed.is_empty());
    let mut first = seed.clone();
    let mut retry = seed.clone();
    let mut other_project = seed;
    let project_a = MemoryTurnContext::new("session-a", "agent-a")
        .with_project_id(Some("project-a".to_string()));
    let project_b = MemoryTurnContext::new("session-b", "agent-a")
        .with_project_id(Some("project-b".to_string()));

    let batch_a = extraction_batch_tag(&project_a, &messages);
    let batch_b = extraction_batch_tag(&project_b, &messages);
    canonicalize_automatic_entries(&project_a, &batch_a, &mut first);
    canonicalize_automatic_entries(&project_a, &batch_a, &mut retry);
    canonicalize_automatic_entries(&project_b, &batch_b, &mut other_project);

    let preference_index = first
        .iter()
        .position(|entry| entry.category == MemoryCategory::UserPreference)
        .expect("preference entry");
    let decision_index = first
        .iter()
        .position(|entry| entry.category == MemoryCategory::Decision)
        .expect("decision entry");

    assert_eq!(first[preference_index].id, retry[preference_index].id);
    assert_ne!(
        first[preference_index].id, other_project[preference_index].id,
        "automatically inferred preferences must remain project-scoped"
    );
    assert_eq!(
        first[preference_index].scope,
        MemoryScope::Project("project-a".into())
    );
    assert!(
        !first[preference_index]
            .tags
            .iter()
            .any(|tag| tag == "memory-policy:always"),
        "heuristic extraction cannot grant unconditional injection authority"
    );

    assert_eq!(first[decision_index].id, retry[decision_index].id);
    assert_ne!(
        first[decision_index].id, other_project[decision_index].id,
        "project decisions remain isolated"
    );
    assert_eq!(
        first[decision_index].scope,
        MemoryScope::Project("project-a".into())
    );
    assert!(first[decision_index].tags.iter().any(|tag| tag == &batch_a));
}

#[test]
fn semantic_extraction_refines_same_turn_heuristic_without_hiding_other_atoms() {
    let extractor = MemoryExtractor::new(Default::default());
    let messages = vec![
        Message::user("请记住：今后代码审查先列风险与证据，再给结论。"),
        Message::assistant("决定采用 Gateway 统一托管 Runtime 生命周期。"),
    ];
    let turn = MemoryTurnContext::new("session-a", "agent-a")
        .with_project_id(Some("project-a".to_string()));
    let batch = extraction_batch_tag(&turn, &messages);
    let mut heuristic = extractor.finalize_entries(extractor.extract_heuristic(&messages));
    canonicalize_automatic_entries(&turn, &batch, &mut heuristic);

    let mut semantic_preference = heuristic
        .iter()
        .find(|entry| entry.category == MemoryCategory::UserPreference)
        .expect("heuristic preference")
        .clone();
    semantic_preference.id = uuid::Uuid::new_v4();
    semantic_preference.title = "Code review order".to_string();
    semantic_preference.content =
        "List risks and evidence before the conclusion in every code review.".to_string();
    let mut semantic_reference = semantic_preference.clone();
    semantic_reference.id = uuid::Uuid::new_v4();
    semantic_reference.layer = MemoryLayer::L3;
    semantic_reference.category = MemoryCategory::Reference;
    semantic_reference.title = "Gateway mediator pattern".to_string();

    let (refinements, inserts) = partition_semantic_refinements(
        vec![semantic_preference, semantic_reference.clone()],
        &heuristic,
    );

    assert_eq!(refinements.len(), 1);
    assert_eq!(refinements[0].0.id, heuristic[0].id);
    assert_eq!(inserts.len(), 1);
    assert_eq!(inserts[0].id, semantic_reference.id);
}

fn semantic_entry(
    layer: MemoryLayer,
    category: MemoryCategory,
    scope: MemoryScope,
    content: &str,
    tags: &[&str],
) -> MemoryEntry {
    let now = Utc::now();
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer,
        category,
        priority: Priority::High,
        source: MemorySource::AutoExtracted,
        title: content.chars().take(40).collect(),
        content: content.to_string(),
        embedding: None,
        tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
        relations: Vec::new(),
        confidence: 0.9,
        access_count: 0,
        staleness: 0.0,
        created_at: now,
        updated_at: now,
        last_accessed_at: None,
        scope,
        session_id: Some("semantic-dedup-test".to_string()),
        source_agent: Some("root-agent".to_string()),
        visibility: crate::types::AgentVisibility::Private,
    }
}

#[test]
fn cross_turn_semantic_dedup_accepts_paraphrases_but_preserves_scope_and_conflicts() {
    let preference = semantic_entry(
        MemoryLayer::L1,
        MemoryCategory::UserPreference,
        MemoryScope::Global,
        "All architecture audits must verify production evidence before conclusions.",
        &["preference", "architecture audit"],
    );
    let translated_preference = semantic_entry(
        MemoryLayer::L1,
        MemoryCategory::UserPreference,
        MemoryScope::Global,
        "所有架构审计必须先核验真实生产证据，再陈述结论。",
        &["preference", "架构审计"],
    );
    assert!(semantic_duplicate_compatible(
        &preference,
        &translated_preference,
        0.862
    ));

    let decision = semantic_entry(
        MemoryLayer::L2,
        MemoryCategory::Decision,
        MemoryScope::Project("cowd".to_string()),
        "Fact Kernel reviews structural facts before Matrix deduction.",
        &["Reality Core", "Fact Kernel"],
    );
    let project_knowledge = semantic_entry(
        MemoryLayer::L2,
        MemoryCategory::ProjectKnowledge,
        MemoryScope::Project("cowd".to_string()),
        "Matrix uses structural facts only after Fact Kernel review.",
        &["Reality Core", "Fact Kernel"],
    );
    assert!(semantic_duplicate_compatible(
        &decision,
        &project_knowledge,
        0.862
    ));

    let project_restates_global_preference = semantic_entry(
        MemoryLayer::L2,
        MemoryCategory::ProjectConvention,
        MemoryScope::Project("cowd".to_string()),
        "架构审计必须先核验真实生产证据，再陈述结论。",
        &["architecture-audit", "evidence-first"],
    );
    assert!(semantic_duplicate_compatible(
        &preference,
        &project_restates_global_preference,
        0.837
    ));

    let mut other_project = project_knowledge.clone();
    other_project.scope = MemoryScope::Project("other".to_string());
    assert!(!semantic_duplicate_compatible(
        &decision,
        &other_project,
        0.99
    ));

    let mut contradictory = project_knowledge;
    contradictory.content =
        "Matrix must not wait for Fact Kernel review before deduction.".to_string();
    assert!(!semantic_duplicate_compatible(
        &decision,
        &contradictory,
        0.99
    ));
}

#[test]
fn vector_reconciliation_excludes_archived_and_superseded_lifecycle_states() {
    assert!(lifecycle_state_is_active(None));
    assert!(lifecycle_state_is_active(Some(MemoryState::Active)));
    assert!(!lifecycle_state_is_active(Some(MemoryState::Archived)));
    assert!(!lifecycle_state_is_active(Some(MemoryState::Superseded)));

    let event = MemoryLifecycleEvent {
        memory_id: uuid::Uuid::new_v4(),
        from: Some(MemoryState::Active),
        to: MemoryState::Archived,
        reason: "test archive".to_string(),
        session_id: "session-a".to_string(),
        agent_id: "agent-a".to_string(),
        occurred_at: Utc::now(),
    };
    let raw = serde_json::to_string(&vec![event]).expect("lifecycle JSON");
    assert_eq!(latest_lifecycle_state(&raw), Some(MemoryState::Archived));
}

#[test]
fn truncate_summary_short_content_unchanged() {
    assert_eq!(truncate_summary("hello", 100), "hello");
}

#[test]
fn truncate_summary_long_content_cut() {
    assert_eq!(truncate_summary(&"a".repeat(200), 10), "aaaaaaaaaa...");
}

#[test]
fn truncate_summary_unicode_boundary_safe() {
    let content = "项目概述：这是中文内容，用于验证 UTF-8 边界截断不会 panic";
    let truncated = truncate_summary(content, 12);
    assert!(truncated.ends_with("..."));
    assert!(content.starts_with(truncated.trim_end_matches("...")));
}

#[test]
fn truncate_summary_emoji_boundary_safe() {
    let content = "状态正常 ✅ 继续处理后续任务";
    let truncated = truncate_summary(content, 17);
    assert!(truncated.ends_with("..."));
    assert!(content.starts_with(truncated.trim_end_matches("...")));
}

#[test]
fn truncate_summary_exact_length() {
    assert_eq!(truncate_summary("hello", 5), "hello");
}

#[tokio::test]
async fn new_constructs_with_default_config() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");
    cfg.store.blob_dir = tmp.path().join("blobs");

    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    assert_eq!(mgr.search_mode_label(), "keyword");
    assert_eq!(mgr.vector_index_count(), 0);
}

#[tokio::test]
async fn corrupt_vector_artifact_degrades_to_fts_without_false_empty() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");
    cfg.store.blob_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&cfg.store.blob_dir).unwrap();
    std::fs::write(
        cfg.store.blob_dir.join("vector_index.json"),
        b"{not-valid-json",
    )
    .unwrap();

    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    let entry = semantic_entry(
        MemoryLayer::L2,
        MemoryCategory::ProjectKnowledge,
        MemoryScope::Global,
        "quartz-harbor-needle remains searchable through FTS",
        &["fallback"],
    );
    mgr.remember(entry).await.unwrap();
    let result = mgr
        .search_memories(SearchMemoriesRequest {
            query: "quartz harbor needle".to_string(),
            limit: 8,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.entries.len(), 1);
    let health = mgr.background_extraction_health();
    assert!(health.degraded_to_fts);
    assert!(health.last_index_error.is_some());
}

#[tokio::test]
async fn usage_signals_are_visible_in_memory_before_coalesced_persistence() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");
    cfg.store.blob_dir = tmp.path().join("blobs");
    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    let memory_id = uuid::Uuid::new_v4();

    for index in 0..8 {
        mgr.record_memory_usage_signal(MemoryUsageSignal {
            memory_id,
            session_id: "session-a".to_string(),
            agent_id: "agent-a".to_string(),
            selected_count: 1,
            last_reason: format!("selection-{index}"),
        });
    }

    let summary = mgr.memory_usage_summary();
    assert_eq!(summary.total_selected, 8);
    assert_eq!(summary.per_memory_selected.get(&memory_id), Some(&8));
    assert_eq!(mgr.memory_usage_writer_health().keys, 1);
    let shutdown = mgr.shutdown_background_tasks().await;
    assert!(
        shutdown.errors.is_empty(),
        "usage writer must drain cleanly: {:?}",
        shutdown.errors
    );
    assert!(
        mgr.memory_usage_writer_health().persisted_batches >= 1,
        "shutdown must persist the latest coalesced usage state"
    );
}

#[tokio::test]
async fn with_write_source_configures_guard() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");
    cfg.store.blob_dir = tmp.path().join("blobs");

    let mgr = CognitiveContextManager::new(cfg)
        .await
        .unwrap()
        .with_write_source(WriteSource::System);
    let policy = mgr.check_write_access(MemoryLayer::L1);
    assert!(policy.is_allowed());
}

#[tokio::test]
async fn list_layers_returns_info() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");
    cfg.store.blob_dir = tmp.path().join("blobs");

    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    let layers = mgr.list_layers().await;
    assert!(!layers.is_empty());
}

#[tokio::test]
async fn embedding_capability_defaults_fts5_only() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");
    cfg.store.blob_dir = tmp.path().join("blobs");

    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    assert!(!mgr.embedding_capability().supports_semantic());
}

#[tokio::test]
async fn vector_index_stats_empty() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");
    cfg.store.blob_dir = tmp.path().join("blobs");

    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    assert_eq!(mgr.vector_index_stats().count, 0);
}

// -----------------------------------------------------------------------
// T7: Code context injection tests
// -----------------------------------------------------------------------

#[test]
fn test_is_code_query_rust_file() {
    assert!(is_code_query("fix bug in src/main.rs"));
    assert!(is_code_query("how does this function work?"));
    assert!(is_code_query("refactor the auth class"));
    assert!(is_code_query("add a new struct for user"));
    assert!(is_code_query("cargo build error"));
}

#[test]
fn test_is_code_query_non_code() {
    assert!(!is_code_query("hello world"));
    assert!(!is_code_query("what is the weather today?"));
    assert!(!is_code_query("tell me a joke"));
    assert!(!is_code_query("create a summary of the meeting"));
    assert!(!is_code_query(""));
}

#[test]
fn test_format_code_context() {
    let symbols = vec![
        CodeSymbol {
            id: "src/auth.rs:authenticate_user:42".into(),
            name: "authenticate_user".into(),
            kind: crate::code_indexer::SymbolKind::Function,
            file_path: "src/auth.rs".into(),
            line: 42,
            signature: "pub fn authenticate_user(token: &str) -> Result<User>".into(),
            doc: Some("validates JWT token and returns user".into()),
        },
        CodeSymbol {
            id: "src/service.rs:MyService:15".into(),
            name: "MyService".into(),
            kind: crate::code_indexer::SymbolKind::Class,
            file_path: "src/service.rs".into(),
            line: 15,
            signature: "class MyService { ... }".into(),
            doc: None,
        },
    ];

    let context = format_code_context(&symbols);
    assert!(context.contains("## Relevant Code Symbols"));
    assert!(context.contains("authenticate_user"));
    assert!(context.contains("src/auth.rs:42"));
    assert!(context.contains("validates JWT token"));
    assert!(context.contains("Kind: Function"));
    assert!(context.contains("MyService"));
    assert!(context.contains("Kind: Class"));
}

#[test]
fn test_format_code_context_empty() {
    let context = format_code_context(&[]);
    assert_eq!(context, "## Relevant Code Symbols");
}

#[tokio::test]
async fn test_auto_inject_on_code_query() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");

    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    let query = "fix bug in src/auth.rs";
    let ctx = mgr.prepare_context(query, &[], None).await.unwrap();

    // code_context may be None (no code indexer in test config) or Some
    // This test primarily validates the pipeline doesn't crash
    assert_eq!(ctx.entries.len(), 0); // empty project has no entries
}

#[tokio::test]
async fn test_no_inject_on_non_code_query() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");

    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    let query = "tell me a joke";
    let ctx = mgr.prepare_context(query, &[], None).await.unwrap();

    // code_context should be None for non-code queries
    assert!(ctx.code_context.is_none());
}

#[tokio::test]
async fn test_build_context_with_code_delegates() {
    let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");

    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    let ctx = mgr.build_context_with_code("hello", &[]).await.unwrap();

    // build_context_with_code wraps prepare_context
    assert!(ctx.code_context.is_none()); // non-code query
}

#[tokio::test]
async fn background_tasks_are_joined_and_shutdown_is_idempotent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");

    let mgr = CognitiveContextManager::new(cfg).await.unwrap();
    let first = mgr.shutdown_background_tasks().await;
    assert_eq!(first.forced_aborts, 0);
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    assert!(first.watcher_joined);
    assert_eq!(
        first.joined_tasks, 3,
        "extraction, knowledge-graph rebuild and usage persistence must all join"
    );

    let second = mgr.shutdown_background_tasks().await;
    assert_eq!(second.forced_aborts, 0);
    assert!(second.errors.is_empty());
    assert!(second.watcher_joined);
    assert_eq!(second.joined_tasks, 0);
}

#[tokio::test]
async fn automatic_governance_admission_is_single_owner_until_completion() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = test_config();
    cfg.store.sqlite_path = tmp.path().join("test.db");
    let mgr = CognitiveContextManager::new(cfg).await.unwrap();

    let nightly = mgr
        .try_begin_automatic_governance("nightly")
        .expect("first governance run should acquire admission");
    assert_eq!(
        mgr.automatic_governance_run_status()
            .as_ref()
            .map(|run| run.run_id.as_str()),
        Some(nightly.run_id.as_str())
    );
    assert!(mgr.try_begin_automatic_governance("manual").is_none());

    mgr.finish_automatic_governance(&nightly.run_id);
    let manual = mgr
        .try_begin_automatic_governance("manual")
        .expect("manual run should acquire admission after nightly completion");
    assert_eq!(manual.mode, "manual");
    mgr.finish_automatic_governance(&manual.run_id);
    assert!(mgr.automatic_governance_run_status().is_none());
}
