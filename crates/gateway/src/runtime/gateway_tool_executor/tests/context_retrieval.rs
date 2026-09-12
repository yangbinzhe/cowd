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
            effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver("runtime.readonly"),
        }])
        .expect("runtime tool registry");
    let executor = GatewayToolExecutor::new(None, false, registry).with_runtime_session_id("s1");
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
    let exact_content = format!(
        "Use exact runtime binding for active memory retrieval.{}记忆末尾证据",
        "甲🙂乙".repeat(10_000)
    );
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
                content: exact_content.clone(),
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
        .with_runtime_tools(
            crate::runtime_bootstrap::runtime_capability_tool_definitions()
                .into_iter()
                .filter(|definition| {
                    matches!(
                        definition.name.as_str(),
                        "context_retrieve" | "working_context" | "private_note"
                    )
                })
                .collect(),
        )
        .expect("runtime tool registry");
    let executor = GatewayToolExecutor::new(None, false, registry)
        .with_runtime_session_id("session-exact")
        .with_runtime_memory_context(context.clone());
    executor
        .bind_runtime_services(Arc::clone(&services))
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
    assert_eq!(exact["selected"][0]["content_truncated"], true);
    let mut restored = exact["selected"][0]["content"].as_str().unwrap().to_owned();
    let mut next = exact["selected"][0]["next_request"].clone();
    while !next.is_null() {
        let output = executor
            .execute("context_retrieve", &next.to_string())
            .await
            .expect("memory continuation");
        let page: serde_json::Value = serde_json::from_str(&output).unwrap();
        restored.push_str(page["selected"][0]["content"].as_str().unwrap());
        next = page["selected"][0]["next_request"].clone();
    }
    assert_eq!(restored, exact_content);
    let binding = RuntimeToolExecutionBinding {
        action_id: Some("working-memory-pin"),
        session_id: Some("session-exact"),
        authorized_scopes: &[],
        memory_context: Some(&context),
        reality_context: None,
        model_lease: None,
        parent_execution: None,
        execution_decision: None,
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
    };
    let note_input = json!({"kind":"hypothesis","title":"private source hypothesis","content":"Possible cause only: this is not a verified observation."});
    let note_binding = RuntimeToolExecutionBinding {
        action_id: Some("private-note-1"),
        ..binding
    };
    let note = executor
        .execute_runtime_tool_with_binding("private_note", note_input.clone(), note_binding)
        .await
        .unwrap();
    let note: serde_json::Value = serde_json::from_str(&note).unwrap();
    assert_eq!(note["epistemic_status"], "agent_reported");
    assert_eq!(note["independently_verified"], false);
    let repeated = executor
        .execute_runtime_tool_with_binding("private_note", note_input.clone(), note_binding)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&repeated).unwrap(),
        note
    );
    let mut changed = note_input.clone();
    changed["content"] = json!("changed content");
    assert!(executor
        .execute_runtime_tool_with_binding("private_note", changed, note_binding)
        .await
        .is_err());
    let mut forged = note_input.clone();
    forged["source"] = json!("UserExplicit");
    assert!(executor
        .execute_runtime_tool_with_binding("private_note", forged, note_binding)
        .await
        .is_err());
    assert!(executor
        .execute_runtime_tool_with_binding(
            "private_note",
            note_input.clone(),
            RuntimeToolExecutionBinding {
                session_id: Some("other-session"),
                ..note_binding
            }
        )
        .await
        .is_err());
    let read = executor
        .execute_runtime_tool_with_binding(
            "context_retrieve",
            note["read_request"]["input"].clone(),
            binding,
        )
        .await
        .unwrap();
    assert!(read.contains("not independently verified"));
    let id = uuid::Uuid::parse_str(note["memory_id"].as_str().unwrap()).unwrap();
    let kernel = memory::MemoryKernel::new(Arc::clone(&manager));
    let stored = kernel
        .retrieve_visible_entry(&context, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.source, memory::MemorySource::AutoExtracted);
    assert_eq!(stored.visibility, memory::AgentVisibility::Private);
    assert_eq!(
        stored.scope,
        memory::MemoryScope::AgentInstance(context.agent_id.clone())
    );
    let mut foreign = context.clone();
    foreign.agent_id = "another-note-agent".into();
    assert!(kernel
        .retrieve_visible_entry(&foreign, id)
        .await
        .unwrap()
        .is_none());
    let pin = executor
        .execute_runtime_tool_with_binding(
            "working_context",
            note["pin_request"]["input"].clone(),
            RuntimeToolExecutionBinding {
                action_id: Some("pin-private-note"),
                ..binding
            },
        )
        .await
        .unwrap();
    assert!(pin.contains("not independently verified"));
    executor
        .execute_runtime_tool_with_binding(
            "working_context",
            json!({"operation":"unpin","source":{"kind":"memory","memory_id":id}}),
            RuntimeToolExecutionBinding {
                action_id: Some("unpin-private-note"),
                ..binding
            },
        )
        .await
        .unwrap();
    manager.orchestrator().forget(&id).await.unwrap();
    assert!(executor
        .execute_runtime_tool_with_binding("private_note", note_input, note_binding)
        .await
        .unwrap_err()
        .to_string()
        .contains("cannot recreate"));
    assert!(kernel
        .retrieve_visible_entry(&context, id)
        .await
        .unwrap()
        .is_none());
    let source = json!({"kind":"memory","memory_id":exact_memory_id});
    let pinned = executor
        .execute_runtime_tool_with_binding(
            "working_context",
            json!({"operation":"pin","source":source}),
            binding,
        )
        .await
        .unwrap();
    let mut page: serde_json::Value = serde_json::from_str(&pinned).unwrap();
    assert_eq!(
        page["entries"][0]["source_hash"],
        exact["selected"][0]["revision_digest"]
    );
    let mut bytes = Vec::new();
    let mut position = 0;
    loop {
        let entry = &page["entries"][0];
        let text = entry["content"].as_str().unwrap();
        if entry["encoding"] == "base64" {
            use base64::Engine;
            bytes.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(text)
                    .unwrap(),
            );
        } else {
            bytes.extend_from_slice(text.as_bytes());
        }
        if entry["next_request"].is_null() {
            break;
        }
        position += 1;
        let action = format!("working-memory-expand-{position}");
        let output = executor
            .execute_runtime_tool_with_binding(
                "working_context",
                entry["next_request"]["input"].clone(),
                RuntimeToolExecutionBinding {
                    action_id: Some(&action),
                    ..binding
                },
            )
            .await
            .unwrap();
        page = serde_json::from_str(&output).unwrap();
    }
    assert_eq!(bytes, exact_content.as_bytes());
    let mut other_agent = context.clone();
    other_agent.agent_id = "different-agent".into();
    assert!(executor
        .execute_runtime_tool_with_binding(
            "working_context",
            json!({"operation":"pin","source":source}),
            RuntimeToolExecutionBinding {
                memory_context: Some(&other_agent),
                ..binding
            }
        )
        .await
        .is_err());
    assert!(executor
        .execute_runtime_tool_with_binding(
            "working_context",
            json!({"operation":"list"}),
            RuntimeToolExecutionBinding {
                session_id: Some("wrong-session"),
                ..binding
            }
        )
        .await
        .is_err());
    let original_store = manager.orchestrator().store().clone();
    let mut changed = original_store.get(&exact_memory_id).await.unwrap().unwrap();
    changed.content.push_str(" revised original source");
    changed.updated_at += chrono::Duration::milliseconds(1);
    original_store.update(&changed).await.unwrap();
    let stale = executor
        .execute_runtime_tool_with_binding("working_context", json!({"operation":"list"}), binding)
        .await
        .unwrap();
    let stale: serde_json::Value = serde_json::from_str(&stale).unwrap();
    assert!(stale["entries"][0]["content"].is_null());
    assert!(stale["entries"][0]["omission"]
        .as_str()
        .unwrap()
        .contains("source changed"));
    let unpinned = executor
        .execute_runtime_tool_with_binding(
            "working_context",
            json!({"operation":"unpin","source":source}),
            RuntimeToolExecutionBinding {
                action_id: Some("working-memory-unpin"),
                ..binding
            },
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&unpinned).unwrap()["coverage"]
            ["active_references"],
        0
    );

    let orchestrator = manager.orchestrator();
    let store = orchestrator.store();
    let mut sample = store.get(&exact_memory_id).await.unwrap().unwrap();
    let mut expected = std::collections::BTreeSet::new();
    for index in 0..35 {
        sample.id = uuid::Uuid::new_v4();
        sample.title = format!("paginationneedle {index}");
        sample.content = "paginationneedle source".into();
        store.insert(&sample).await.unwrap();
        expected.insert(sample.id.to_string());
    }
    let mut request = json!({"source":"memory","query":"paginationneedle","limit":3});
    let mut actual = std::collections::BTreeSet::new();
    let mut first_next = None;
    let mut pages = 0;
    loop {
        let output = executor
            .execute("context_retrieve", &request.to_string())
            .await
            .unwrap();
        let page: serde_json::Value = serde_json::from_str(&output).unwrap();
        pages += 1;
        assert!(pages < 20);
        assert_eq!(page["coverage"]["kind"], "authorized_lexical_catalog");
        assert_eq!(page["recommendation_coverage"]["computed"], pages == 1);
        for item in page["selected"].as_array().unwrap() {
            assert!(actual.insert(item["memory_id"].as_str().unwrap().to_owned()));
        }
        if page["next_request"].is_null() {
            assert_eq!(page["coverage"]["complete"], true);
            break;
        }
        if first_next.is_none() {
            first_next = Some(page["next_request"].clone());
        }
        request = page["next_request"].clone();
    }
    assert_eq!(actual, expected);
    sample.content.push_str(" changed");
    store.update(&sample).await.unwrap();
    assert!(executor
        .execute("context_retrieve", &first_next.unwrap().to_string())
        .await
        .unwrap_err()
        .to_string()
        .contains("source changed"));
}

#[tokio::test]
async fn context_retrieve_follows_only_durable_session_relations() {
    let temp = tempfile::tempdir().expect("session retrieval root");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let store = Arc::new(crate::pg_test_support::session_store());
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
        crate::services::session_service::presence::SessionPresenceLedger::with_store(Arc::clone(
            &store,
        )),
    );
    let session_port = crate::session_runtime_data_port::GatewaySessionRuntimePort::new_for_test(
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
            effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver("runtime.readonly"),
        }])
        .expect("runtime tool registry");
    let executor =
        GatewayToolExecutor::new(None, false, registry).with_runtime_session_id("session-current");
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
    assert_eq!(value["explicitly_authorized_session_count"], 2);
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

#[tokio::test]
async fn context_fact_directory_and_exact_evidence_use_runtime_data_lease_and_real_schema() {
    use fact_kernel::{EvidencePacket, FactId, FactLedger, FactRecord, FactSource, SourceKind};
    let root = tempfile::tempdir().unwrap();
    let resolver = storage::StaticSecretRefResolver::new([(
        "fact-gateway".into(),
        std::env::var("COWD_TEST_POSTGRES_URL").unwrap(),
    )]);
    let database = storage::PostgresExecutor::connect(
        storage::PostgresConnectionConfig::new("test", "fact-gateway", "fact-gateway-test"),
        &resolver,
    )
    .unwrap();
    let schema = format!("gateway_fact_{}", uuid::Uuid::new_v4().simple());
    database
        .checkout_critical()
        .unwrap()
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .unwrap();
    let ledger = Arc::new(
        fact_postgres::PostgresFactLedger::new(database.scoped_namespace(&schema).unwrap())
            .unwrap(),
    );
    let long = format!("needle {}末尾", "甲🙂乙".repeat(10_000));
    let evidence = ledger
        .upsert_evidence(EvidencePacket::new(
            FactSource {
                kind: SourceKind::Growth,
                id: "source".into(),
                label: None,
            },
            json!({"original":long}),
        ))
        .unwrap();
    let uncited = ledger
        .upsert_evidence(EvidencePacket::new(
            FactSource {
                kind: SourceKind::Growth,
                id: "private".into(),
                label: None,
            },
            json!({"private":"not cited"}),
        ))
        .unwrap();
    for i in 0..35 {
        let mut fact = FactRecord::new(
            "test",
            if i == 0 {
                long.clone()
            } else {
                format!("needle {i}")
            },
        );
        fact.id = FactId::from_string(format!("fact-{i:03}"));
        fact.scope_key = Some("session:session-facts".into());
        fact.boundary = harness_contract::reality::RealityBoundary::Observed;
        if i == 0 {
            fact.evidence.push(evidence.id.clone());
        }
        ledger.upsert_fact(fact).unwrap();
    }
    let mut hidden = FactRecord::new("test", "needle private");
    hidden.id = FactId::from_string("private");
    hidden.scope_key = Some("session:other".into());
    hidden.boundary = harness_contract::reality::RealityBoundary::Observed;
    ledger.upsert_fact(hidden).unwrap();
    let port = runtime::RealityRecallPort::with_fact_ledger(root.path(), ledger.clone())
        .with_workspace_scope(root.path());
    let services = runtime::RuntimeServices::builder(root.path().join("home"), root.path())
        .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
        .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
        .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(
            root.path().join("artifacts"),
        )))
        .reality_recall_port(Arc::new(port))
        .build()
        .unwrap();
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .unwrap();
    let executor = GatewayToolExecutor::new(None, false, registry);
    executor.bind_runtime_services(services).unwrap();
    let lease = harness_contract::agent::AgentDataLease {
        session_id: "session-facts".into(),
        task_id: "task-facts".into(),
        team_id: None,
        read_scopes: vec![harness_contract::agent::CognitiveReadScope::Session],
        write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
        fact_boundaries: vec!["observed".into()],
        fact_refs: vec![],
        matrix_snapshot_refs: vec![],
    };
    let binding = RuntimeToolExecutionBinding {
        action_id: Some("fact-discovery"),
        session_id: Some("session-facts"),
        authorized_scopes: &[],
        memory_context: None,
        reality_context: Some(&lease),
        model_lease: None,
        parent_execution: None,
        execution_decision: None,
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
    };
    let mut request = json!({"source":"fact","query":"needle","limit":3});
    let mut seen = std::collections::BTreeSet::new();
    let mut first_cursor = None;
    loop {
        executor
            .tool_host
            .pin_snapshot()
            .validate_input("context_retrieve", &request)
            .unwrap();
        let raw = executor
            .execute_runtime_tool_with_binding("context_retrieve", request, binding)
            .await
            .unwrap();
        assert!(raw.find("\"selected\"").unwrap() < raw.find("\"coverage\"").unwrap());
        let page: serde_json::Value = serde_json::from_str(&raw).unwrap();
        if first_cursor.is_none() {
            first_cursor = page["next_cursor"].as_str().map(str::to_owned);
        }
        for item in page["selected"].as_array().unwrap() {
            assert!(seen.insert(item["entry_ref"].as_str().unwrap().to_owned()));
            let mut read_request = item["read_request"].clone();
            let mut content = String::new();
            let mut reads = 0;
            loop {
                executor
                    .tool_host
                    .pin_snapshot()
                    .validate_input("context_retrieve", &read_request)
                    .unwrap();
                let exact: serde_json::Value = serde_json::from_str(
                    &executor
                        .execute_runtime_tool_with_binding(
                            "context_retrieve",
                            read_request,
                            binding,
                        )
                        .await
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(exact["sha256"], item["sha256"]);
                for chunk in exact["chunks"].as_array().unwrap() {
                    content.push_str(chunk["content"].as_str().unwrap());
                }
                reads += 1;
                assert!(reads < 20);
                if exact["next_request"].is_null() {
                    break;
                }
                read_request = exact["next_request"].clone();
            }
            let fact: FactRecord = serde_json::from_str(&content).unwrap();
            if fact.id.as_str() == "fact-000" {
                assert_eq!(fact.statement, long);
                assert!(reads > 1);
            }
        }
        if page["next_request"].is_null() {
            break;
        }
        request = page["next_request"].clone();
    }
    assert_eq!(seen.len(), 35);
    assert!(!seen.contains("fact:private"));
    for (request, binding) in [
        (json!({"source":"fact","entry_ref":"fact:private"}), binding),
        (
            json!({"source":"fact","query":"needle"}),
            RuntimeToolExecutionBinding {
                reality_context: None,
                ..binding
            },
        ),
        (
            json!({"source":"fact","query":"needle"}),
            RuntimeToolExecutionBinding {
                session_id: Some("different"),
                ..binding
            },
        ),
        (
            json!({"source":"fact","entry_ref":format!("fact:evidence:{}",uncited.id.as_str()),"parent_ref":"fact:fact-000"}),
            binding,
        ),
        (
            json!({"source":"fact","entry_ref":format!("fact:evidence:{}",evidence.id.as_str())}),
            binding,
        ),
        (
            json!({"source":"fact","query":"changed","cursor":first_cursor}),
            binding,
        ),
    ] {
        assert!(executor
            .execute_runtime_tool_with_binding("context_retrieve", request, binding)
            .await
            .is_err());
    }
    let first: serde_json::Value = serde_json::from_str(
        &executor
            .execute_runtime_tool_with_binding(
                "context_retrieve",
                json!({"source":"fact","entry_ref":"fact:fact-000","limit":1}),
                binding,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    let mut request = first["evidence_reads"][0]["read_request"].clone();
    request["limit"] = json!(1);
    let mut content = String::new();
    let mut pages = 0;
    loop {
        executor
            .tool_host
            .pin_snapshot()
            .validate_input("context_retrieve", &request)
            .unwrap();
        let read: serde_json::Value = serde_json::from_str(
            &executor
                .execute_runtime_tool_with_binding("context_retrieve", request, binding)
                .await
                .unwrap(),
        )
        .unwrap();
        for chunk in read["chunks"].as_array().unwrap() {
            content.push_str(chunk["content"].as_str().unwrap());
        }
        pages += 1;
        assert!(pages < 40);
        if read["next_request"].is_null() {
            break;
        }
        request = read["next_request"].clone();
    }
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&content).unwrap()["payload"]["original"],
        long
    );
    assert!(pages > 16);
    let mut changed = ledger.get_fact("fact-000").unwrap().unwrap();
    changed.statement = "edited".into();
    ledger.upsert_fact(changed).unwrap();
    assert!(executor
        .execute_runtime_tool_with_binding(
            "context_retrieve",
            first["next_request"].clone(),
            binding
        )
        .await
        .is_err());
    assert!(executor
        .execute_runtime_tool_with_binding(
            "context_retrieve",
            json!({"source":"fact","query":"needle","cursor":first_cursor}),
            binding
        )
        .await
        .is_err());
    drop(executor);
    drop(ledger);
    database
        .checkout_critical()
        .unwrap()
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .unwrap();
}

#[tokio::test]
async fn context_matrix_directory_reads_complete_scoped_json_and_rejects_changed_grants() {
    use matrix_repository::MatrixStore;
    let root = tempfile::tempdir().unwrap();
    let resolver = storage::StaticSecretRefResolver::new([(
        "matrix-gateway".into(),
        std::env::var("COWD_TEST_POSTGRES_URL").unwrap(),
    )]);
    let database = storage::PostgresExecutor::connect(
        storage::PostgresConnectionConfig::new("test", "matrix-gateway", "matrix-gateway-test"),
        &resolver,
    )
    .unwrap();
    let schema = format!("gateway_matrix_{}", uuid::Uuid::new_v4().simple());
    database
        .checkout_critical()
        .unwrap()
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .unwrap();
    let store = Arc::new(
        matrix_repository::PostgresMatrixRepository::new(
            database.scoped_namespace(&schema).unwrap(),
        )
        .unwrap(),
    );
    let mut snapshot = matrix_core::MatrixSourceSnapshot::new(
        "needle source",
        matrix_core::MatrixSourceKind::Manual,
        "1",
    );
    snapshot.snapshot_id = "allowed".into();
    store.upsert_source_snapshot(snapshot).unwrap();
    let long = format!("{}末尾", "甲🙂乙".repeat(10_000));
    let make = |id: &str, snapshot_id: &str| matrix_core::MatrixFact {
        fact_id: id.into(),
        snapshot_id: snapshot_id.into(),
        fact_type: "needle".into(),
        entity_refs: vec![],
        metric_key: None,
        dimensions: json!({"region":"east"}),
        measures: json!({"original":if id=="fact-000"{long.as_str()}else{"bounded"}}),
        event_time: chrono::Utc::now(),
        valid_from: None,
        valid_to: None,
        source_ref: None,
        confidence: 0.9,
        raw_hash: id.into(),
    };
    for i in 0..35 {
        store
            .ingest_fact(&make(&format!("fact-{i:03}"), "allowed"))
            .unwrap();
    }
    store.ingest_fact(&make("hidden", "private")).unwrap();
    let port = runtime::RealityRecallPort::with_fact_and_matrix_store(
        root.path(),
        Arc::new(fact_kernel::UnavailableFactLedger::new("not used")),
        store.clone(),
    );
    let services = runtime::RuntimeServices::builder(root.path().join("home"), root.path())
        .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
        .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
        .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(
            root.path().join("artifacts"),
        )))
        .reality_recall_port(Arc::new(port))
        .build()
        .unwrap();
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .unwrap();
    let executor = GatewayToolExecutor::new(None, false, registry);
    executor.bind_runtime_services(services).unwrap();
    let lease = harness_contract::agent::AgentDataLease {
        session_id: "session-matrix".into(),
        task_id: "task-matrix".into(),
        team_id: None,
        read_scopes: vec![],
        write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
        fact_boundaries: vec![],
        fact_refs: vec![],
        matrix_snapshot_refs: vec!["matrix:source_snapshot:allowed".into()],
    };
    let binding = RuntimeToolExecutionBinding {
        action_id: Some("matrix-discovery"),
        session_id: Some("session-matrix"),
        authorized_scopes: &[],
        memory_context: None,
        reality_context: Some(&lease),
        model_lease: None,
        parent_execution: None,
        execution_decision: None,
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
    };
    let mut request = json!({"source":"matrix","query":"needle","limit":3});
    let mut seen = std::collections::BTreeSet::new();
    let mut first_cursor = None;
    loop {
        executor
            .tool_host
            .pin_snapshot()
            .validate_input("context_retrieve", &request)
            .unwrap();
        let page: serde_json::Value = serde_json::from_str(
            &executor
                .execute_runtime_tool_with_binding("context_retrieve", request, binding)
                .await
                .unwrap(),
        )
        .unwrap();
        if first_cursor.is_none() {
            first_cursor = page["next_cursor"].as_str().map(str::to_owned);
        }
        for item in page["selected"].as_array().unwrap() {
            assert!(seen.insert(item["entry_ref"].as_str().unwrap().to_owned()));
            let mut read_request = item["read_request"].clone();
            read_request["limit"] = json!(1);
            let mut content = String::new();
            let mut pages = 0;
            loop {
                executor
                    .tool_host
                    .pin_snapshot()
                    .validate_input("context_retrieve", &read_request)
                    .unwrap();
                let exact: serde_json::Value = serde_json::from_str(
                    &executor
                        .execute_runtime_tool_with_binding(
                            "context_retrieve",
                            read_request,
                            binding,
                        )
                        .await
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(exact["sha256"], item["sha256"]);
                for chunk in exact["chunks"].as_array().unwrap() {
                    content.push_str(chunk["content"].as_str().unwrap());
                }
                for related in exact["evidence_reads"].as_array().unwrap() {
                    executor
                        .tool_host
                        .pin_snapshot()
                        .validate_input("context_retrieve", &related["read_request"])
                        .unwrap();
                }
                pages += 1;
                assert!(pages < 40);
                if exact["next_request"].is_null() {
                    break;
                }
                read_request = exact["next_request"].clone();
            }
            let value: serde_json::Value = serde_json::from_str(&content).unwrap();
            if value["fact_id"] == "fact-000" {
                assert_eq!(value["measures"]["original"], long);
                assert!(pages > 16);
            }
        }
        if page["next_request"].is_null() {
            break;
        }
        request = page["next_request"].clone();
    }
    assert_eq!(seen.len(), 36);
    assert!(seen.contains("matrix:source_snapshot:allowed"));
    assert!(!seen.contains("matrix:fact:hidden"));
    let mut changed_grant = lease.clone();
    changed_grant.matrix_snapshot_refs = vec!["matrix:source_snapshot:private".into()];
    for (request, binding) in [
        (
            json!({"source":"matrix","entry_ref":"matrix:fact:hidden"}),
            binding,
        ),
        (
            json!({"source":"matrix","entry_ref":"matrix:source_snapshot:private"}),
            binding,
        ),
        (
            json!({"source":"matrix","query":"needle"}),
            RuntimeToolExecutionBinding {
                session_id: Some("other"),
                ..binding
            },
        ),
        (
            json!({"source":"matrix","query":"changed","cursor":first_cursor}),
            binding,
        ),
        (
            json!({"source":"matrix","query":"needle","cursor":first_cursor}),
            RuntimeToolExecutionBinding {
                reality_context: Some(&changed_grant),
                ..binding
            },
        ),
    ] {
        assert!(executor
            .execute_runtime_tool_with_binding("context_retrieve", request, binding)
            .await
            .is_err());
    }
    let first: serde_json::Value = serde_json::from_str(
        &executor
            .execute_runtime_tool_with_binding(
                "context_retrieve",
                json!({"source":"matrix","entry_ref":"matrix:fact:fact-000","limit":1}),
                binding,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    let mut changed = make("fact-000", "allowed");
    changed.measures = json!({"changed":true});
    store.ingest_fact(&changed).unwrap();
    assert!(executor
        .execute_runtime_tool_with_binding(
            "context_retrieve",
            first["next_request"].clone(),
            binding
        )
        .await
        .is_err());
    assert!(executor
        .execute_runtime_tool_with_binding(
            "context_retrieve",
            json!({"source":"matrix","query":"needle","cursor":first_cursor}),
            binding
        )
        .await
        .is_err());
    let mut no_grants = lease.clone();
    no_grants.matrix_snapshot_refs.clear();
    let disabled: serde_json::Value = serde_json::from_str(
        &executor
            .execute_runtime_tool_with_binding(
                "context_retrieve",
                json!({"source":"matrix"}),
                RuntimeToolExecutionBinding {
                    reality_context: Some(&no_grants),
                    ..binding
                },
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(disabled["status"], "disabled");
    drop(executor);
    drop(store);
    database
        .checkout_critical()
        .unwrap()
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .unwrap();
}

#[tokio::test]
async fn context_session_discovery_pages_beyond_512_and_binds_exact_message_revision() {
    let root = tempfile::tempdir().unwrap();
    let resolver = storage::StaticSecretRefResolver::new([(
        "session-gateway".into(),
        std::env::var("COWD_TEST_POSTGRES_URL").unwrap(),
    )]);
    let database = storage::PostgresExecutor::connect(
        storage::PostgresConnectionConfig::new("test", "session-gateway", "session-gateway-test"),
        &resolver,
    )
    .unwrap();
    let schema = format!(
        "gateway_session_discovery_{}",
        uuid::Uuid::new_v4().simple()
    );
    database
        .checkout_critical()
        .unwrap()
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .unwrap();
    let backend = Arc::new(
        session_postgres::PostgresSessionStore::new(database.scoped_namespace(&schema).unwrap())
            .unwrap(),
    );
    let store = Arc::new(session::UnifiedSessionStore::from_backend(backend.clone()));
    let record = |id: &str, owner: &str| session::SessionRecord {
        session_id: id.into(),
        platform: "test".into(),
        chat_id: id.into(),
        user_id: None,
        model: None,
        created_at: "2026-09-08T00:00:00Z".into(),
        last_activity: "2026-09-08T00:00:00Z".into(),
        message_count: 0,
        reset_policy: "manual".into(),
        metadata_json: Some(
            json!({"workspace_root":"/test","owner_principal_id":owner,"title":"needle"})
                .to_string(),
        ),
        input_tokens: 0,
        output_tokens: 0,
        status: "active".into(),
    };
    let message = |id: &str, sequence: usize, text: &str| session::SessionMessage {
        stable_message_id: format!("{id}-m{sequence}"),
        session_id: id.into(),
        sequence,
        role: "user".into(),
        content_json: json!([{"type":"text","text":text}]).to_string(),
        blocks_count: 1,
        tool_use_id: None,
        tool_name: None,
        token_usage_json: None,
        created_at_ms: 1,
    };
    for i in 0..600 {
        store
            .create_session(&record(&format!("session-{i:03}"), "owner"))
            .await
            .unwrap();
    }
    store
        .create_session(&record("private", "other"))
        .await
        .unwrap();
    store
        .insert_message(&message("private", 0, "needle tail_marker"))
        .await
        .unwrap();
    store
        .insert_message(&message("session-599", 0, "needle tail_marker"))
        .await
        .unwrap();
    for sequence in 0..130 {
        store
            .insert_message(&message("session-000", sequence, "current_marker"))
            .await
            .unwrap();
    }
    let repository = Arc::new(
        crate::services::session_service::repository::SessionRepository::new(
            Arc::new(crate::active_session::ActiveSessionDirectory::new()),
            Some(store.clone()),
            crate::event_bus::SessionProjectionHub::new(),
        ),
    );
    let presence = Arc::new(
        crate::services::session_service::presence::SessionPresenceLedger::with_store(
            store.clone(),
        ),
    );
    let port = crate::session_runtime_data_port::GatewaySessionRuntimePort::new_for_test(
        repository, presence,
    );
    let services = runtime::RuntimeServices::builder(root.path().join("home"), root.path())
        .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
        .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
        .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(
            root.path().join("artifacts"),
        )))
        .build()
        .unwrap();
    services
        .install_session_ports(port.clone(), port.clone(), port.clone(), port)
        .unwrap();
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .unwrap();
    let executor = GatewayToolExecutor::new(None, false, registry);
    executor.bind_runtime_services(services).unwrap();
    let binding = RuntimeToolExecutionBinding {
        action_id: Some("session-discovery"),
        session_id: Some("session-000"),
        authorized_scopes: &[],
        memory_context: None,
        reality_context: None,
        model_lease: None,
        parent_execution: None,
        execution_decision: None,
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
    };
    let mut request = json!({"source":"session_catalog","query":"needle","limit":16});
    let mut first_cursor = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut pages = 0;
    loop {
        executor
            .tool_host
            .pin_snapshot()
            .validate_input("context_retrieve", &request)
            .unwrap();
        let page: serde_json::Value = serde_json::from_str(
            &executor
                .execute_runtime_tool_with_binding("context_retrieve", request, binding)
                .await
                .unwrap(),
        )
        .unwrap();
        if first_cursor.is_none() {
            first_cursor = page["next_cursor"].as_str().map(str::to_owned);
            store
                .create_session(&record("future", "owner"))
                .await
                .unwrap();
        }
        for item in page["selected"].as_array().unwrap() {
            assert!(seen.insert(item["session_id"].as_str().unwrap().to_owned()));
            for key in [
                "source_kind",
                "ref",
                "revision",
                "scope",
                "source_time",
                "information_status",
                "preview",
                "read_request",
            ] {
                assert!(item.get(key).is_some(), "missing {key}");
            }
            executor
                .tool_host
                .pin_snapshot()
                .validate_input("context_retrieve", &item["read_request"])
                .unwrap();
        }
        pages += 1;
        assert!(pages < 50);
        if page["next_request"].is_null() {
            break;
        }
        request = page["next_request"].clone();
    }
    assert_eq!(seen.len(), 600);
    assert!(seen.contains("session-599"));
    assert!(!seen.contains("private"));
    assert!(!seen.contains("future"));
    let tail:serde_json::Value=serde_json::from_str(&executor.execute_runtime_tool_with_binding("context_retrieve",json!({"source":"session_history","scope":"workspace_sessions","query":"tail_marker"}),binding).await.unwrap()).unwrap();
    assert_eq!(tail["selected_count"], 1);
    assert_eq!(tail["selected"][0]["session_id"], "session-599");
    let mut request = json!({"source":"session_history","query":"current_marker","limit":7});
    let mut ids = std::collections::BTreeSet::new();
    let mut history_cursor = None;
    loop {
        executor
            .tool_host
            .pin_snapshot()
            .validate_input("context_retrieve", &request)
            .unwrap();
        let page: serde_json::Value = serde_json::from_str(
            &executor
                .execute_runtime_tool_with_binding("context_retrieve", request, binding)
                .await
                .unwrap(),
        )
        .unwrap();
        if history_cursor.is_none() {
            history_cursor = page["next_cursor"].as_str().map(str::to_owned);
            store
                .insert_message(&message("session-000", 200, "current_marker new"))
                .await
                .unwrap();
        }
        for item in page["selected"].as_array().unwrap() {
            assert!(ids.insert(item["message_id"].as_str().unwrap().to_owned()));
            executor
                .tool_host
                .pin_snapshot()
                .validate_input("context_retrieve", &item["read_request"])
                .unwrap();
        }
        if page["next_request"].is_null() {
            break;
        }
        request = page["next_request"].clone();
    }
    assert_eq!(ids.len(), 130);
    assert!(!ids.contains("session-000-m200"));
    for (request, binding) in [
        (
            json!({"source":"session_history","query":"changed","cursor":history_cursor}),
            binding,
        ),
        (
            json!({"source":"session_catalog","query":"needle","cursor":first_cursor}),
            RuntimeToolExecutionBinding {
                session_id: Some("session-001"),
                ..binding
            },
        ),
        (
            json!({"source":"session_history","scope":"explicit_session","session_id":"private"}),
            binding,
        ),
    ] {
        assert!(executor
            .execute_runtime_tool_with_binding("context_retrieve", request, binding)
            .await
            .is_err());
    }
    let original = (0..35)
        .map(|i| json!({"type":"text","text":format!("block {i} 甲🙂乙")}))
        .collect::<Vec<_>>();
    let mut exact_message = message("session-000", 300, "unused");
    exact_message.content_json = serde_json::to_string(&original).unwrap();
    exact_message.blocks_count = original.len();
    store.insert_message(&exact_message).await.unwrap();
    let mut request = json!({"source":"session_history","message_id":exact_message.stable_message_id,"block_limit":2});
    let mut blocks = Vec::new();
    let mut first_next = None;
    loop {
        executor
            .tool_host
            .pin_snapshot()
            .validate_input("context_retrieve", &request)
            .unwrap();
        let page: serde_json::Value = serde_json::from_str(
            &executor
                .execute_runtime_tool_with_binding("context_retrieve", request, binding)
                .await
                .unwrap(),
        )
        .unwrap();
        blocks.extend(
            page["selected"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["content"].clone()),
        );
        if page["next_request"].is_null() {
            break;
        }
        if first_next.is_none() {
            first_next = Some(page["next_request"].clone());
        }
        request = page["next_request"].clone();
    }
    assert_eq!(blocks, original);
    assert!(executor.execute_runtime_tool_with_binding("context_retrieve",json!({"source":"session_history","message_id":exact_message.stable_message_id,"block_cursor":2}),binding).await.is_err());
    let mut outside = first_next.clone().unwrap();
    outside["block_cursor"] = json!(99);
    assert!(executor
        .execute_runtime_tool_with_binding("context_retrieve", outside, binding)
        .await
        .is_err());
    exact_message.content_json = json!([{"type":"text","text":"changed"}]).to_string();
    exact_message.blocks_count = 1;
    store.insert_message(&exact_message).await.unwrap();
    assert!(executor
        .execute_runtime_tool_with_binding("context_retrieve", first_next.unwrap(), binding)
        .await
        .unwrap_err()
        .to_string()
        .contains("source changed"));
    assert!(executor
        .execute_runtime_tool_with_binding(
            "context_retrieve",
            json!({"source":"session_history","query":"current_marker","cursor":history_cursor}),
            binding
        )
        .await
        .is_err());
    drop(executor);
    drop(store);
    drop(backend);
    database
        .checkout_critical()
        .unwrap()
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .unwrap();
}
