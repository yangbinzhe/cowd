    #[tokio::test]
    async fn context_retrieve_is_runtime_bound_and_degrades_explicitly() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "context_retrieve".to_string(),
                description: Some("bounded context retrieval".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "source": { "type": "string" },
                        "query": { "type": "string" }
                    },
                    "required": ["source", "query"]
                }),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor =
            GatewayToolExecutor::new(None, false, registry).with_runtime_session_id("s1");
        executor
            .bind_runtime_services(runtime::RuntimeServices::in_memory().unwrap())
            .expect("bind services");

        let output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"memory","query":"session decisions"}"#,
            )
            .await
            .expect("unconfigured memory returns a degraded receipt");
        let value: serde_json::Value = serde_json::from_str(&output).expect("context receipt");

        assert_eq!(value["kind"], "runtime.context_retrieval");
        assert_eq!(value["status"], "degraded");
        assert_eq!(value["source"], "memory");
    }

    #[tokio::test]
    async fn context_evidence_references_do_not_fall_through_to_mcp() {
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());

        let error = executor
            .execute_runtime_tool(
                "read_mcp_resource_tool",
                json!({
                    "server": "runtime",
                    "uri": "session://session-a/messages/4",
                }),
            )
            .await
            .expect_err("Session evidence is not an MCP resource");

        assert!(error.to_string().contains("context_retrieve"));
        assert!(error.to_string().contains("not MCP resources"));
    }

    #[tokio::test]
    async fn context_retrieve_reads_the_exact_runtime_memory_binding() {
        let temp = tempfile::tempdir().expect("context retrieval root");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let manager = Arc::new(
            crate::pg_test_support::memory_manager(memory::MemoryConfig {
                store: memory::config::StoreConfig {
                    blob_dir: temp.path().join("blobs"),
                    enable_vector_index: false,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("memory manager"),
        );
        let context = memory::MemoryTurnContext::new("session-exact", "agent-exact")
            .with_definition_lineage_id(Some("definition-exact".to_string()))
            .with_project_id(Some(runtime::memory_project_id_for_workspace(&workspace)))
            .with_task_id(Some("task-exact".to_string()))
            .with_cognitive_read_scopes(vec![
                harness_contract::agent::CognitiveReadScope::Session,
                harness_contract::agent::CognitiveReadScope::Project,
            ]);
        let now = chrono::Utc::now();
        let exact_memory_id = uuid::Uuid::new_v4();
        manager
            .remember_for_turn(
                &context,
                memory::MemoryEntry {
                    id: exact_memory_id,
                    layer: memory::MemoryLayer::L2,
                    category: memory::MemoryCategory::Decision,
                    priority: memory::Priority::High,
                    source: memory::MemorySource::UserExplicit,
                    title: "Context isolation decision".to_string(),
                    content: "Use exact runtime binding for active memory retrieval.".to_string(),
                    embedding: None,
                    tags: vec!["context-isolation".to_string()],
                    relations: Vec::new(),
                    confidence: 0.98,
                    access_count: 0,
                    staleness: 0.0,
                    created_at: now,
                    updated_at: now,
                    last_accessed_at: None,
                    scope: memory::MemoryScope::default(),
                    session_id: None,
                    source_agent: None,
                    visibility: memory::AgentVisibility::Private,
                },
            )
            .await
            .expect("remember exact binding");
        let other_context = memory::MemoryTurnContext::new("session-other", "agent-other")
            .with_project_id(Some("other-project".to_string()))
            .with_task_id(Some("task-other".to_string()))
            .with_cognitive_read_scopes(vec![
                harness_contract::agent::CognitiveReadScope::Session,
                harness_contract::agent::CognitiveReadScope::Project,
            ]);
        manager
            .remember_for_turn(
                &other_context,
                memory::MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: memory::MemoryLayer::L2,
                    category: memory::MemoryCategory::Decision,
                    priority: memory::Priority::Critical,
                    source: memory::MemorySource::UserExplicit,
                    title: "Other project context isolation decision".to_string(),
                    content:
                        "Other project also mentions exact runtime binding and must remain hidden."
                            .to_string(),
                    embedding: None,
                    tags: vec!["context-isolation".to_string()],
                    relations: Vec::new(),
                    confidence: 1.0,
                    access_count: 0,
                    staleness: 0.0,
                    created_at: now,
                    updated_at: now,
                    last_accessed_at: None,
                    scope: memory::MemoryScope::default(),
                    session_id: None,
                    source_agent: None,
                    visibility: memory::AgentVisibility::Private,
                },
            )
            .await
            .expect("remember out-of-binding memory");
        assert_eq!(
            manager
                .list_all_entries()
                .await
                .expect("list remembered entries")
                .len(),
            2
        );
        assert_eq!(
            manager
                .search_memories(memory::SearchMemoriesRequest {
                    query: "exact runtime binding".to_string(),
                    limit: 8,
                    ..Default::default()
                })
                .await
                .expect("search remembered entries")
                .entries
                .len(),
            2
        );
        let services = runtime::RuntimeServices::builder(temp.path().join("home"), &workspace)
            .memory_manager(Arc::clone(&manager))
            .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
            .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
            .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(
                temp.path().join("artifacts"),
            )))
            .build()
            .expect("runtime services");
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "context_retrieve".to_string(),
                description: Some("bounded context retrieval".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "source": { "type": "string" },
                        "query": { "type": "string" }
                    },
                    "required": ["source", "query"]
                }),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry)
            .with_runtime_session_id("session-exact")
            .with_runtime_memory_context(context);
        executor
            .bind_runtime_services(services)
            .expect("bind services");

        let output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"memory","query":"exact runtime binding","limit":8}"#,
            )
            .await
            .expect("active memory retrieval");
        let value: serde_json::Value = serde_json::from_str(&output).expect("context receipt");

        assert_eq!(value["status"], "completed");
        assert_eq!(value["selected_count"], 1, "{output}");
        assert_eq!(value["selected"][0]["title"], "Context isolation decision");
        assert_eq!(
            value["selected"][0]["read_request"]["memory_id"],
            exact_memory_id.to_string()
        );
        assert!(value["reference_contract"]["instruction"]
            .as_str()
            .is_some_and(|instruction| instruction.contains("not MCP resources")));
        assert!(!output.contains("Other project"));

        let exact_output = executor
            .execute(
                "context_retrieve",
                &serde_json::json!({
                    "source": "memory",
                    "memory_id": exact_memory_id,
                })
                .to_string(),
            )
            .await
            .expect("exact authorized memory retrieval");
        let exact: serde_json::Value =
            serde_json::from_str(&exact_output).expect("exact memory receipt");
        assert_eq!(exact["selected_count"], 1);
        assert_eq!(
            exact["selected"][0]["content"],
            "Use exact runtime binding for active memory retrieval."
        );
    }

    #[tokio::test]
    async fn context_retrieve_follows_only_durable_session_relations() {
        let temp = tempfile::tempdir().expect("session retrieval root");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let store =
            Arc::new(crate::pg_test_support::session_store());
        let now = chrono::Utc::now().to_rfc3339();
        for session_id in [
            "session-current",
            "session-related",
            "session-workspace-peer",
            "session-unrelated",
        ] {
            let owner = if matches!(session_id, "session-current" | "session-workspace-peer") {
                "local-human"
            } else {
                "other-human"
            };
            store
                .create_session(&session::SessionRecord {
                    session_id: session_id.to_string(),
                    platform: "test".to_string(),
                    chat_id: session_id.to_string(),
                    user_id: None,
                    model: None,
                    created_at: now.clone(),
                    last_activity: now.clone(),
                    message_count: 0,
                    reset_policy: "manual".to_string(),
                    metadata_json: Some(
                        serde_json::json!({
                            "title": format!("Context {session_id}"),
                            "workspace_root": workspace.display().to_string(),
                            "owner_principal_id": owner,
                        })
                        .to_string(),
                    ),
                    input_tokens: 0,
                    output_tokens: 0,
                    status: "active".to_string(),
                })
                .await
                .expect("create session");
            store
                .insert_message(&session::SessionMessage {
                    stable_message_id: format!("{session_id}-message"),
                    session_id: session_id.to_string(),
                    sequence: 0,
                    role: "user".to_string(),
                    content_json: serde_json::json!([{
                        "type": "text",
                        "text": format!("shared gateway relation marker from {session_id}")
                    }])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 1,
                })
                .await
                .expect("insert session message");
        }
        let event_bus = crate::event_bus::SessionProjectionHub::new();
        let repository = Arc::new(
            crate::services::session_service::repository::SessionRepository::new(
                Arc::new(crate::active_session::ActiveSessionDirectory::new()),
                Some(Arc::clone(&store)),
                event_bus,
            ),
        );
        let presence = Arc::new(
            crate::services::session_service::presence::SessionPresenceLedger::with_store(
                Arc::clone(&store),
            ),
        );
        let session_port =
            crate::session_runtime_data_port::GatewaySessionRuntimePort::new_for_test(
                repository, presence,
            );
        let services = runtime::RuntimeServices::builder(temp.path().join("home"), &workspace)
            .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
            .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
            .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(
                temp.path().join("artifacts"),
            )))
            .build()
            .expect("runtime services");
        services
            .install_session_ports(
                session_port.clone(),
                session_port.clone(),
                session_port.clone(),
                session_port,
            )
            .expect("install session ports");
        services
            .session_relations()
            .add_relation(
                "session-current",
                "session-related",
                runtime::SessionRelationKind::References,
                "current session explicitly references the related session",
                Vec::new(),
            )
            .expect("durable session relation");
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "context_retrieve".to_string(),
                description: Some("bounded context retrieval".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "source": { "type": "string" },
                        "query": { "type": "string" },
                        "scope": { "type": "string" }
                    },
                    "required": ["source", "query"]
                }),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry)
            .with_runtime_session_id("session-current");
        executor
            .bind_runtime_services(services)
            .expect("bind services");

        let output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_history","scope":"related_sessions","query":"gateway relation marker","limit":8}"#,
            )
            .await
            .expect("active related-session retrieval");
        let value: serde_json::Value = serde_json::from_str(&output).expect("context receipt");

        assert_eq!(value["status"], "completed");
        assert_eq!(value["scope"], "related_sessions");
        assert_eq!(value["authorized_session_count"], 2);
        assert_eq!(value["selected_count"], 2, "{output}");
        assert!(output.contains("session-current"));
        assert!(output.contains("session-related"));
        assert!(!output.contains("session-unrelated"));

        let catalog_output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_catalog","query":"gateway relation marker","limit":8}"#,
            )
            .await
            .expect("discover same-actor workspace Sessions");
        let catalog: serde_json::Value =
            serde_json::from_str(&catalog_output).expect("session catalog receipt");
        assert_eq!(catalog["scope"], "workspace_sessions");
        assert_eq!(catalog["selected_count"], 2, "{catalog_output}");
        assert!(catalog_output.contains("session-current"));
        assert!(catalog_output.contains("session-workspace-peer"));
        assert!(!catalog_output.contains("session-related"));
        assert_eq!(
            catalog["selected"][0]["read_request"]["source"],
            "session_history"
        );
        assert!(catalog["reference_contract"]["instruction"]
            .as_str()
            .is_some_and(|instruction| instruction.contains("not MCP resources")));
        assert!(!catalog_output.contains("session-unrelated"));

        let workspace_output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_history","scope":"workspace_sessions","query":"gateway relation marker","limit":8}"#,
            )
            .await
            .expect("one-hop workspace Session search");
        let workspace: serde_json::Value =
            serde_json::from_str(&workspace_output).expect("workspace search receipt");
        assert_eq!(workspace["scope"], "workspace_sessions");
        assert!(workspace_output.contains("session-workspace-peer"));
        assert!(!workspace_output.contains("session-unrelated"));

        let explicit_output = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_history","scope":"explicit_session","session_id":"session-workspace-peer","limit":8}"#,
            )
            .await
            .expect("read explicit same-actor Session");
        let explicit: serde_json::Value =
            serde_json::from_str(&explicit_output).expect("explicit session receipt");
        assert_eq!(explicit["scope"], "explicit_session");
        assert_eq!(explicit["selected_count"], 1, "{explicit_output}");
        assert!(explicit_output.contains("session-workspace-peer"));

        let denied = executor
            .execute(
                "context_retrieve",
                r#"{"source":"session_history","scope":"explicit_session","session_id":"session-unrelated","limit":8}"#,
            )
            .await
            .expect_err("other actor Session must remain hidden");
        assert!(denied.to_string().contains("outside"));
    }
