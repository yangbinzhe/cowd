#[test]
fn evidence_retrieve_request_matches_published_query_schema() {
    let request: EvidenceRetrieveToolRequest = serde_json::from_value(serde_json::json!({
        "evidence_ref": "artifact://sha256:abc",
        "query": "acceptance evidence",
        "cursor": "opaque-page-cursor",
        "limit": 4
    }))
    .expect("published evidence_retrieve schema must deserialize");
    assert_eq!(request.evidence_ref, "artifact://sha256:abc");
    assert_eq!(request.query.as_deref(), Some("acceptance evidence"));
    assert_eq!(request.limit, Some(4));
    assert_eq!(request.cursor.as_deref(), Some("opaque-page-cursor"));
    assert!(
        serde_json::from_value::<EvidenceRetrieveToolRequest>(serde_json::json!({
            "evidence_ref": "artifact://sha256:abc",
            "selector": "legacy-hidden-field"
        }))
        .is_err()
    );
}

#[tokio::test]
async fn external_decision_evidence_uses_runtime_source_and_authenticated_pagination() {
    use harness_contract::execution_graph::*;
    use harness_contract::policy::*;
    let services = runtime::RuntimeServices::in_memory().unwrap();
    let mut graph = ExecutionGraph::new("external decision Gateway");
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: "decision-session".into(),
        turn_id: "decision-turn".into(),
        root_task_id: "decision-task".into(),
        task_id: "decision-task".into(),
        generation: 1,
    });
    let node = ExecutionNodeSpec::new(ExecutionNodeKind::Approval, "approval", "decision source");
    let reference =
        runtime::execution_core::graph::executors::graph_approval_id(&graph.id, &node.id);
    graph.nodes.push(node);
    services.commit_service().register_graph(graph).unwrap();
    let source = ApprovalSource {
        kind: ApprovalSourceKind::Session,
        session_id: Some("decision-session".into()),
        agent_id: None,
        team_id: None,
        mission_id: None,
        resource_ref: None,
        review_ref: None,
        application: None,
    };
    services
        .approval_queue()
        .submit_scoped(
            &reference,
            SubmitApprovalRequest {
                context: ApprovalContext::owned(&source, "decide work", "test-workspace"),
                source,
                action: "decide work".into(),
                summary: "Review requested work".into(),
                risk: harness_contract::core::TaskRisk::Medium,
                domain: ApprovalDomain::Execution,
                blocks_execution: true,
                evidence_refs: vec![],
                timeout_policy: ApprovalTimeoutPolicy::AutoDeny,
            },
        )
        .unwrap();
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor.bind_runtime_services(&services).unwrap();
    let request = || {
        serde_json::from_value::<EvidenceRetrieveToolRequest>(
            serde_json::json!({"evidence_ref":reference, "limit":1}),
        )
        .unwrap()
    };
    assert!(executor
        .execute_evidence_retrieve(request(), Some("decision-session"), &[])
        .await
        .is_err());
    let reason = "Operator checked the proposed work and declined. ".repeat(1200);
    services
        .approval_queue()
        .decide_surface_human(
            "operator:gateway",
            ApprovalDecisionCommand {
                approval_id: reference.clone(),
                approved: false,
                skip: false,
                reason: reason.clone(),
                scope: ApprovalGrantScope::Once,
                actor: ApprovalDecisionActor {
                    kind: ApprovalDecisionActorKind::Human,
                    actor_id: "surface-replaces-this".into(),
                },
                evidence_refs: vec![],
            },
        )
        .unwrap();
    assert!(executor
        .execute_evidence_retrieve(request(), None, &[])
        .await
        .is_err());
    assert!(executor
        .execute_evidence_retrieve(
            request(),
            Some("other-session"),
            &["session:decision-session".into()]
        )
        .await
        .is_err());
    let mut next = request();
    let mut text = String::new();
    let mut pages = 0;
    let mut expected_hash = None;
    loop {
        let value: serde_json::Value = serde_json::from_str(
            &executor
                .execute_evidence_retrieve(next, Some("decision-session"), &[])
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(value["available"], true);
        if let Some(hash) = &expected_hash {
            assert_eq!(&value["sha256"], hash);
        }
        expected_hash = Some(value["sha256"].clone());
        for chunk in value["chunks"].as_array().unwrap() {
            text.push_str(chunk["content"].as_str().unwrap());
        }
        pages += 1;
        assert!(pages < 200, "pagination must terminate");
        if value["next_request"].is_null() {
            break;
        }
        next = serde_json::from_value(value["next_request"].clone()).unwrap();
    }
    assert!(pages > 1);
    let content: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(content["approval"]["decision"]["reason"], reason);
    assert_eq!(content["approval"]["decision"]["approved"], false);
    assert_eq!(
        content["approval"]["decision"]["actor"]["actor_id"],
        "operator:gateway"
    );
    assert_eq!(content["scope"], "approval_decision_only");
}

#[tokio::test]
async fn r1_publication_gateway_mint_and_passthrough_keep_creation_identity() {
    publication_creation_identity_gateway_case().await;
}

async fn publication_creation_identity_gateway_case() {
    let temporary = tempfile::tempdir().expect("temporary Runtime root");
    let workspace = temporary.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let store = Arc::new(crate::pg_test_support::session_store());
    let session_id = "session-logical-evidence";
    store
        .create_session(&session::SessionRecord {
            session_id: session_id.to_string(),
            platform: "test".to_string(),
            chat_id: "logical-evidence".to_string(),
            user_id: None,
            model: None,
            created_at: "2026-09-05T00:00:00Z".to_string(),
            last_activity: "2026-09-05T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .expect("create Session");
    let repository = Arc::new(
        crate::services::session_service::repository::SessionRepository::new(
            Arc::new(crate::active_session::ActiveSessionDirectory::new()),
            Some(Arc::clone(&store)),
            crate::event_bus::SessionProjectionHub::new(),
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
    let runtime_journal = Arc::new(runtime::RuntimeEventStore::for_test());
    let services = runtime::RuntimeServices::builder(temporary.path().join("home"), &workspace)
        .runtime_event_store(runtime_journal.clone())
        .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
        .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(
            temporary.path().join("artifacts"),
        )))
        .build()
        .expect("Runtime services");
    services
        .install_session_ports(
            session_port.clone(),
            session_port.clone(),
            session_port.clone(),
            session_port,
        )
        .expect("Session ports");
    let artifact = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".to_string(),
                visibility_scope: format!("session:{session_id}"),
                expected_bytes: Some(23),
                original_name: Some("logical-evidence.raw".to_string()),
            },
            b"durable mapped evidence",
        )
        .await
        .expect("artifact");
    let evidence_id = "tool-raw-logical-evidence";
    for index in 0..129 {
        store
            .append_session_domain_event_allocating_sequence(&session::SessionDomainEvent::new(
                session_id,
                0,
                session::SessionDomainScope::Tool,
                runtime::RuntimeSessionEventKind::EvidenceRawPersisted.as_str(),
                serde_json::json!({"evidence_id": format!("unrelated-{index}")}),
                index,
            ))
            .await
            .expect("unrelated evidence receipt");
    }
    store
        .append_session_domain_event_allocating_sequence(&session::SessionDomainEvent::new(
            session_id,
            0,
            session::SessionDomainScope::Tool,
            runtime::RuntimeSessionEventKind::EvidenceRawPersisted.as_str(),
            serde_json::json!({
                "evidence_id": evidence_id,
                "content_hash": artifact.sha256,
                "byte_count": artifact.bytes,
                "media_type": artifact.media_type,
                "artifact_selector": artifact.selector,
                "visibility_scope": artifact.visibility_scope,
            }),
            1,
        ))
        .await
        .expect("raw evidence receipt");
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor
        .bind_runtime_services(&services)
        .expect("bind Runtime services");

    // A durable Session selector publishes exactly the chosen block, never
    // the assistant preamble or a neighbouring message.
    let blocks = serde_json::json!([
        {"type": "text", "text": "publication preamble"},
        {"type": "text", "text": "<html>正文\r\n</html>\n"},
        {"type": "tool_use", "id": "tool-block", "name": "read_file", "input": {}}
    ]);
    store
        .insert_message(&session::SessionMessage {
            stable_message_id: "publication-message".to_string(),
            session_id: session_id.to_string(),
            sequence: 0,
            role: "assistant".to_string(),
            content_json: blocks.to_string(),
            blocks_count: 3,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 2,
        })
        .await
        .expect("durable source blocks");
    let binding = RuntimeToolExecutionBinding {
        action_invocation: None,
        tool_lease: None,
        action_id: None,
        session_id: Some(session_id),
        authorized_scopes: &[],
        memory_context: None,
        reality_context: None,
        model_lease: Some("test"),
        parent_execution: None,
        parent_execution_attempt: None,
        execution_decision: None,
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
    };
    use sha2::{Digest, Sha256};
    let block_hash = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&blocks[1]).unwrap())
    );
    let request = serde_json::json!({
        "source": "message_block", "message_id": "publication-message",
        "block_index": 1, "sha256": block_hash, "media_type": "text/html"
    });
    let published: serde_json::Value = serde_json::from_str(
        &executor
            .execute_content_publication("artifact_publish", request.clone(), binding)
            .await
            .expect("publish selected durable block"),
    )
    .unwrap();
    let (_, body) = services
        .read_authorized_publication(
            published["content_ref"].as_str().unwrap(),
            published["sha256"].as_str().unwrap(),
            session_id,
            &[],
        )
        .await
        .expect("published body");
    assert_eq!(&*body, blocks[1]["text"].as_str().unwrap().as_bytes());
    assert_ne!(published["sha256"], request["sha256"]);
    let stream = format!("session:{session_id}");
    let facts = || {
        runtime_journal
            .list_stream(&stream)
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == "artifact.fresh_object_created")
            .collect::<Vec<_>>()
    };
    let message_facts = facts();
    let [message_fact] = message_facts.as_slice() else { panic!("expected the original artifact fact exactly once") };
    assert_eq!(
        message_fact.payload["artifact"]["selector"],
        published["content_ref"]
    );
    assert_eq!(
        message_fact.payload["precreation_head"]
            .as_u64()
            .unwrap()
            + 1,
        message_fact.sequence
    );
    let file_host = Arc::new(ToolHost::new(
        "fresh-file",
        &workspace,
        ToolHostSnapshot::new(
            Arc::new(GatewayToolRegistry::builtin()),
            Arc::new(tools::lsp_client::LspRegistry::new()),
            None,
        ),
    ));
    let file_executor = GatewayToolExecutor::from_tool_host(None, false, file_host);
    file_executor.bind_runtime_services(&services).unwrap();
    std::fs::write(workspace.join("fresh.txt"), &body).unwrap();
    let file_request = serde_json::json!({"source":"file", "path":"fresh.txt",
        "sha256":published["sha256"], "media_type":"text/html"});
    let mut selectors =
        std::collections::BTreeSet::from([published["content_ref"].as_str().unwrap().to_owned()]);
    for _ in 0..2 {
        let before = runtime_journal.stream_revision(&stream).unwrap();
        let reply: serde_json::Value = serde_json::from_str(
            &file_executor
                .execute_content_publication("artifact_publish", file_request.clone(), binding)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(selectors.insert(reply["content_ref"].as_str().unwrap().to_owned()));
        assert_eq!(reply["sha256"], published["sha256"]);
        let current = facts();
        let fact = current.last().unwrap();
        assert_eq!(fact.payload["artifact"]["selector"], reply["content_ref"]);
        assert_eq!(fact.payload["precreation_head"], before);
        assert_eq!(fact.sequence, before + 1);
    }
    let before = runtime_journal.stream_revision(&stream).unwrap();
    let passthrough: serde_json::Value = serde_json::from_str(
        &executor
            .execute_content_publication(
                "artifact_publish",
                serde_json::json!({
                    "source":"artifact", "content_ref":published["content_ref"],
                    "sha256":published["sha256"]
                }),
                binding,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(passthrough["content_ref"], published["content_ref"]);
    assert_eq!(runtime_journal.stream_revision(&stream).unwrap(), before);
    let mut actual_selectors = facts().iter().map(|fact| fact.payload["artifact"]["selector"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
    actual_selectors.sort_unstable();
    assert_eq!(actual_selectors, selectors.iter().cloned().collect::<Vec<_>>());
    assert_eq!(facts()[0], message_facts[0]);
    let legacy_passthrough: serde_json::Value = serde_json::from_str(
        &executor
            .execute_content_publication(
                "artifact_publish",
                serde_json::json!({
                    "source":"artifact", "content_ref":artifact.selector, "sha256":artifact.sha256
                }),
                binding,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(legacy_passthrough["content_ref"], artifact.selector);
    assert_eq!(runtime_journal.stream_revision(&stream).unwrap(), before);
    let mut actual_selectors = facts().iter().map(|fact| fact.payload["artifact"]["selector"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
    actual_selectors.sort_unstable();
    assert_eq!(actual_selectors, selectors.iter().cloned().collect::<Vec<_>>());
    let mut other_session = binding;
    other_session.session_id = Some("unrelated-session");
    assert!(executor
        .execute_content_publication("artifact_publish", request.clone(), other_session)
        .await
        .is_err());
    for index in [0, 2, 3] {
        let mut wrong = request.clone();
        wrong["block_index"] = serde_json::json!(index);
        if index == 2 {
            wrong["sha256"] = serde_json::json!(format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&blocks[2]).unwrap())
            ));
        }
        assert!(executor
            .execute_content_publication("artifact_publish", wrong, binding)
            .await
            .is_err());
    }

    // An explicitly selected but empty/whitespace-only text block must be
    // refused: authored content must never become an empty Task artifact
    // that only surfaces as a reviewer rework after burning the budget.
    let empty_blocks = serde_json::json!([{"type": "text", "text": "   \n  "}]);
    store
        .insert_message(&session::SessionMessage {
            stable_message_id: "empty-publication-message".to_string(),
            session_id: session_id.to_string(),
            sequence: 1,
            role: "assistant".to_string(),
            content_json: empty_blocks.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 3,
        })
        .await
        .expect("empty source block");
    let empty_request = serde_json::json!({
        "source": "message_block", "message_id": "empty-publication-message",
        "block_index": 0,
        "sha256": format!("{:x}", Sha256::digest(serde_json::to_vec(&empty_blocks[0]).unwrap())),
        "media_type": "text/plain"
    });
    let empty_error = executor
        .execute_content_publication("artifact_publish", empty_request, binding)
        .await
        .expect_err("empty text block must not publish");
    assert!(
        empty_error.to_string().contains("empty text block"),
        "unexpected error: {empty_error}"
    );

    let actor = root_agent_action_actor(RuntimeToolExecutionBinding {
        action_invocation: None,
        tool_lease: None,
        action_id: None,
        session_id: Some(session_id),
        authorized_scopes: &[],
        memory_context: None,
        reality_context: None,
        model_lease: Some("deepseek-v4-flash"),
        parent_execution: None,
        parent_execution_attempt: None,
        execution_decision: None,
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
    })
    .unwrap();
    let mut adjudication = harness_contract::agent_action::MessagePublishInput {
        topic_ref: "topic:program".into(),
        summary: Some("disclose the sourced limitation".into()),
        content_ref: None,
        refs: Vec::new(),
        recipients: Vec::new(),
        intent: None,
        issue_dispositions: vec![harness_contract::agent_action::IssueDisposition {
            issue_ref: "issue:source-validated-by-program".into(),
            disposition: harness_contract::agent_action::IssueDispositionKind::Disclose,
            reason_ref: artifact.selector.clone(),
            evidence_refs: vec![format!("tool://{evidence_id}")],
        }],
    };
    services
        .validate_agent_action_evidence(
            &actor,
            &harness_contract::agent_action::AgentAction::MessagePublish(adjudication.clone()),
        )
        .await
        .expect("adjudication reasons and evidence use real Session authority");
    adjudication.issue_dispositions[0].reason_ref = "artifact://missing-disposition-reason".into();
    assert!(services
        .validate_agent_action_evidence(
            &actor,
            &harness_contract::agent_action::AgentAction::MessagePublish(adjudication)
        )
        .await
        .is_err());

    let action = harness_contract::agent_action::AgentAction::TaskSubmit(
        harness_contract::agent_action::TaskSubmitInput {
            deliverable: None,
            task_ref: "task:any".to_string(),
            artifact_refs: vec!["artifact:any".to_string()],
            evidence_refs: vec![format!("tool://{evidence_id}")],
            unresolved: Vec::new(),
        },
    );
    services
        .validate_agent_action_evidence(&actor, &action)
        .await
        .expect("logical evidence is authenticated before Program mutation");

    let output = executor
        .execute_evidence_retrieve(
            EvidenceRetrieveToolRequest {
                evidence_ref: format!("tool://{evidence_id}"),
                query: Some("mapped".to_string()),
                limit: Some(2),
                cursor: None,
            },
            Some(session_id),
            &["workspace:.".to_string()],
        )
        .await
        .expect("resolve logical tool evidence");

    assert!(output.contains("durable mapped evidence"), "{output}");
    assert!(output.contains("\"available\": true"), "{output}");
}

#[test]
fn successful_gateway_file_fact_has_typed_identity_and_digest() {
    let services = runtime::RuntimeServices::in_memory().expect("runtime services");
    std::fs::write(services.workspace_root().join("identity.txt"), "checked")
        .expect("workspace fixture");
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor
        .bind_runtime_services(&services)
        .expect("bind runtime services");
    let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
        &runtime::tool_dispatch::ToolRequest {
            tool_use_id: "typed-receipt".to_string(),
            tool_name: "read_file".to_string(),
            input: r#"{"path":"identity.txt"}"#.to_string(),
            depends_on: Vec::new(),
        },
    );
    request.category = runtime::ToolSafetyCategory::ReadOnly;
    request.governed_plan_revision = 7;
    request.observation_wave_sequence = 7;
    let output = serde_json::json!({
        "type": "text",
        "truncated": false,
        "file": {
            "filePath": services.workspace_root().join("identity.txt"),
            "content": "checked",
            "numLines": 1,
            "startLine": 1,
            "totalLines": 1,
            "sha256": format!("{:x}", Sha256::digest(b"checked")),
            "byteLength": 7
        }
    })
    .to_string();
    let observed = gateway_observed_evidence(
        &executor,
        &request,
        &output,
        "gateway-tool:test:7:typed-receipt",
    );
    assert!(matches!(observed.as_slice(), [fact] if fact.observed_at_sequence == 7));
    assert_eq!(observed[0].observed_at_sequence, 7);
    assert!(matches!(
        &observed[0].target,
        harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
            if scope.path.workspace_relative_path == "identity.txt"
                && scope.path.observed_revision_or_digest.is_some()
    ));
    request.tool_name = "unrelated_tool".to_string();
    assert!(gateway_observed_evidence(
        &executor,
        &request,
        &output,
        "gateway-tool:test:7:unrelated",
    )
    .is_empty());
}

#[test]
fn gateway_windowed_read_never_mints_exact_content_evidence() {
    let services = runtime::RuntimeServices::in_memory().expect("runtime services");
    std::fs::write(
        services.workspace_root().join("windowed.txt"),
        "one\ntwo\nthree",
    )
    .expect("workspace fixture");
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor
        .bind_runtime_services(&services)
        .expect("bind runtime services");
    let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
        &runtime::tool_dispatch::ToolRequest {
            tool_use_id: "windowed-read".to_string(),
            tool_name: "read_file".to_string(),
            input: r#"{"path":"windowed.txt","offset":1,"limit":1}"#.to_string(),
            depends_on: Vec::new(),
        },
    );
    request.category = runtime::ToolSafetyCategory::ReadOnly;
    let base = serde_json::json!({
        "type": "text",
        "file": {
            "filePath": services.workspace_root().join("windowed.txt"),
            "content": "two",
            "numLines": 1,
            "startLine": 2,
            "totalLines": 3,
            "sha256": format!("{:x}", Sha256::digest(b"one\ntwo\nthree")),
            "byteLength": 13
        }
    });
    let mut truncated = base.clone();
    truncated["truncated"] = serde_json::Value::Bool(true);
    assert!(gateway_observed_evidence(
        &executor,
        &request,
        &truncated.to_string(),
        "gateway-tool:test:windowed-truncated",
    )
    .is_empty());

    let mut offset = base;
    offset["truncated"] = serde_json::Value::Bool(false);
    assert!(gateway_observed_evidence(
        &executor,
        &request,
        &offset.to_string(),
        "gateway-tool:test:windowed-offset",
    )
    .is_empty());
}

#[test]
fn successful_gateway_edit_fact_hashes_exact_result_without_rereading_disk() {
    let services = runtime::RuntimeServices::in_memory().expect("runtime services");
    std::fs::write(
        services.workspace_root().join("edit.txt"),
        "alpha beta alpha",
    )
    .expect("workspace fixture");
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor
        .bind_runtime_services(&services)
        .expect("bind runtime services");
    let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
        &runtime::tool_dispatch::ToolRequest {
            tool_use_id: "typed-edit-receipt".to_string(),
            tool_name: "edit".to_string(),
            input: r#"{"action":"replace","path":"edit.txt"}"#.to_string(),
            depends_on: Vec::new(),
        },
    );
    request.category = runtime::ToolSafetyCategory::WriteLocal;
    request.governed_plan_revision = 9;
    request.observation_wave_sequence = 9;
    let output = serde_json::json!({
        "filePath": services.workspace_root().join("edit.txt"),
        "oldString": "alpha",
        "newString": "omega",
        "originalFile": "alpha beta alpha",
        "structuredPatch": [],
        "userModified": false,
        "replaceAll": false,
        "gitDiff": null
    })
    .to_string();
    std::fs::write(services.workspace_root().join("edit.txt"), "different now")
        .expect("concurrent fixture");

    let observed = gateway_observed_evidence(
        &executor,
        &request,
        &output,
        "gateway-tool:test:9:typed-edit-receipt",
    );
    let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
        &observed[0].target
    else {
        panic!("workspace evidence expected");
    };
    let expected = format!("{:x}", Sha256::digest(b"omega beta alpha"));
    assert_eq!(
        scope.path.observed_revision_or_digest.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(
        observed[0].workspace_prior_state,
        Some(harness_contract::context::WorkspacePriorState::Existing {
            sha256: format!("{:x}", Sha256::digest(b"alpha beta alpha")),
        })
    );
}

#[test]
fn gateway_write_receipt_distinguishes_attested_absence_from_existing_preimage() {
    let services = runtime::RuntimeServices::in_memory().expect("runtime services");
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor
        .bind_runtime_services(&services)
        .expect("bind runtime services");
    let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
        &runtime::tool_dispatch::ToolRequest {
            tool_use_id: "typed-write-receipt".to_string(),
            tool_name: "edit".to_string(),
            input: "{\"action\":\"write\"}".to_string(),
            depends_on: Vec::new(),
        },
    );
    request.observation_wave_sequence = 10;

    let created = serde_json::json!({
        "type": "create",
        "filePath": services.workspace_root().join("created.txt"),
        "content": "created",
        "originalFile": null
    });
    let created = gateway_observed_evidence(&executor, &request, &created.to_string(), "created");
    assert_eq!(
        created[0].workspace_prior_state,
        Some(harness_contract::context::WorkspacePriorState::Absent)
    );

    let updated = serde_json::json!({
        "type": "update",
        "filePath": services.workspace_root().join("updated.txt"),
        "content": "after",
        "originalFile": "before"
    });
    let updated = gateway_observed_evidence(&executor, &request, &updated.to_string(), "updated");
    assert_eq!(
        updated[0].workspace_prior_state,
        Some(harness_contract::context::WorkspacePriorState::Existing {
            sha256: format!("{:x}", Sha256::digest(b"before")),
        })
    );

    let ambiguous = serde_json::json!({
        "type": "create",
        "filePath": services.workspace_root().join("ambiguous.txt"),
        "content": "after",
        "originalFile": "unreadable-is-not-absent"
    });
    assert!(
        gateway_observed_evidence(&executor, &request, &ambiguous.to_string(), "ambiguous",)
            .is_empty()
    );
}

#[test]
fn compact_write_receipt_keeps_typed_workspace_evidence_after_body_elision() {
    let services = runtime::RuntimeServices::in_memory().expect("runtime services");
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor
        .bind_runtime_services(&services)
        .expect("bind runtime services");
    let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
        &runtime::tool_dispatch::ToolRequest {
            tool_use_id: "compact-write-receipt".to_string(),
            tool_name: "edit".to_string(),
            input: "{\"action\":\"write\"}".to_string(),
            depends_on: Vec::new(),
        },
    );
    request.observation_wave_sequence = 11;

    // This is the canonical model-facing receipt emitted after the full
    // adapter response is intentionally elided from the conversation.
    let content = "created through compact receipt";
    let output = serde_json::json!({
        "operation": "create",
        "path": services.workspace_root().join("compact-created.txt"),
        "content_bytes": content.len(),
        "content_sha256": format!("{:x}", Sha256::digest(content.as_bytes())),
        "prior_bytes": 0,
        "prior_sha256": null,
        "replacement_count": 0,
        "replace_all": true,
    });
    let observed =
        gateway_observed_evidence(&executor, &request, &output.to_string(), "compact-create");

    assert!(matches!(observed.as_slice(), [fact]
        if matches!(&fact.target, harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
            if scope.access_mode == harness_contract::context::WorkspaceAccessMode::Write
                && scope.path.workspace_relative_path == "compact-created.txt")));
    assert_eq!(
        observed[0].workspace_prior_state,
        Some(harness_contract::context::WorkspacePriorState::Absent)
    );

    let mut malformed = output;
    malformed["content_sha256"] = serde_json::json!("not-a-digest");
    assert!(gateway_observed_evidence(
        &executor,
        &request,
        &malformed.to_string(),
        "compact-malformed",
    )
    .is_empty());
}

use serde_json::json;
use tools::permissions::PermissionMode as ToolPermissionMode;
use tools::RuntimeToolDefinition;

#[tokio::test]
async fn explicit_publication_roundtrip_and_scope_checks() {
    use sha2::{Digest, Sha256};
    let temp = tempfile::tempdir().unwrap();
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .unwrap();
    let host = Arc::new(ToolHost::new(
        "publication-test",
        temp.path(),
        ToolHostSnapshot::new(
            Arc::new(registry),
            Arc::new(tools::lsp_client::LspRegistry::new()),
            None,
        ),
    ));
    let executor = GatewayToolExecutor::from_tool_host(None, false, host.clone())
        .with_runtime_session_id("publication-session");
    let services = runtime::RuntimeServices::builder(temp.path().join("home"), temp.path())
        .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
        .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
        .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(
            temp.path().join("artifacts"),
        )))
        .build()
        .unwrap();
    executor.bind_runtime_services(&services).unwrap();
    let body = "<html>中文\r\n完整正文</html>\r\n";
    std::fs::write(temp.path().join("source.html"), body).unwrap();
    let hash = format!("{:x}", Sha256::digest(body.as_bytes()));
    let input = serde_json::json!({"source":"file","path":"source.html","sha256":hash,"media_type":"text/html"});
    host.pin_snapshot()
        .validate_input("artifact_publish", &input)
        .unwrap();
    let binding = RuntimeToolExecutionBinding {
        action_invocation: None,
        tool_lease: None,
        action_id: Some("publish"),
        session_id: Some("publication-session"),
        authorized_scopes: &[],
        memory_context: None,
        reality_context: None,
        model_lease: None,
        parent_execution: None,
        parent_execution_attempt: None,
        execution_decision: None,
        permission_ceiling: harness_contract::policy::PermissionMode::WorkspaceWrite,
    };
    let published: serde_json::Value = serde_json::from_str(
        &executor
            .execute_runtime_tool_with_binding("artifact_publish", input, binding)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(published["bytes"], body.len());
    assert_eq!(published["sha256"], format!("sha256:{hash}"));
    let export = serde_json::json!({"content_ref": published["content_ref"], "sha256": published["sha256"], "path":"result.html"});
    host.pin_snapshot()
        .validate_input("artifact_materialize", &export)
        .unwrap();
    for index in 0..2 {
        let result: serde_json::Value = serde_json::from_str(
            &executor
                .execute_runtime_tool_with_binding("artifact_materialize", export.clone(), binding)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result["created"], index == 0);
        let evidence =
            materialized_artifact_evidence(services.path_identity_resolver(), &result, index + 1)
                .unwrap();
        assert_eq!(evidence.workspace_prior_state.is_some(), index == 0);
    }
    assert_eq!(
        std::fs::read(temp.path().join("result.html")).unwrap(),
        body.as_bytes()
    );
    let denied = RuntimeToolExecutionBinding {
        session_id: Some("other-session"),
        ..binding
    };
    assert!(executor
        .execute_runtime_tool_with_binding("artifact_materialize", export.clone(), denied)
        .await
        .is_err());
    let parent = harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: "parent".into(),
        node_id: "agent".into(),
    };
    let scopes = vec![
        "write:unrelated".to_string(),
        "session:publication-session".to_string(),
    ];
    let scoped = RuntimeToolExecutionBinding {
        parent_execution: Some(&parent),
        authorized_scopes: &scopes,
        ..binding
    };
    assert!(executor
        .execute_runtime_tool_with_binding("artifact_materialize", export, scoped)
        .await
        .is_err());
    std::fs::write(temp.path().join("source.html"), "changed").unwrap();
    assert!(executor
        .execute_runtime_tool_with_binding(
            "artifact_publish",
            serde_json::json!({
                "source":"file","path":"source.html","sha256":hash,"media_type":"text/html"
            }),
            binding
        )
        .await
        .is_err());
}

#[tokio::test]
async fn context_artifact_catalog_returns_readable_scoped_content_and_lossless_binary() {
    use base64::Engine;
    let services = runtime::RuntimeServices::in_memory().unwrap();
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .unwrap();
    let executor = GatewayToolExecutor::new(None, false, registry);
    executor.bind_runtime_services(&services).unwrap();
    let binding = RuntimeToolExecutionBinding {
        action_invocation: None,
        tool_lease: None,
        action_id: Some("artifact-directory"),
        session_id: Some("artifact-directory-session"),
        authorized_scopes: &[],
        memory_context: None,
        reality_context: None,
        model_lease: None,
        parent_execution: None,
        parent_execution_attempt: None,
        execution_decision: None,
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
    };
    let descriptor = harness_contract::context::ArtifactWriteDescriptor {
        media_type: "text/plain".into(),
        visibility_scope: "session:artifact-directory-session".into(),
        expected_bytes: None,
        original_name: None,
    };
    let mut expected = std::collections::BTreeSet::new();
    for index in 0..35 {
        expected.insert(
            services
                .artifact_store()
                .write_bytes(descriptor.clone(), format!("published {index}").as_bytes())
                .await
                .unwrap()
                .selector,
        );
    }
    let hidden = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                visibility_scope: "session:private".into(),
                ..descriptor.clone()
            },
            b"private",
        )
        .await
        .unwrap();
    let mut request = serde_json::json!({"source":"artifact","query":"text/plain","limit":3});
    let mut actual = std::collections::BTreeSet::new();
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
        pages += 1;
        assert!(pages < 20);
        for item in page["selected"].as_array().unwrap() {
            assert!(actual.insert(item["ref"].as_str().unwrap().to_string()));
            executor
                .tool_host
                .pin_snapshot()
                .validate_input("evidence_retrieve", &item["read_request"])
                .unwrap();
            let read: serde_json::Value = serde_json::from_str(
                &executor
                    .execute_runtime_tool_with_binding(
                        "evidence_retrieve",
                        item["read_request"].clone(),
                        binding,
                    )
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(read["available"], true);
            assert_eq!(read["sha256"], item["sha256"]);
        }
        if page["next_request"].is_null() {
            break;
        }
        services
            .artifact_store()
            .write_bytes(descriptor.clone(), b"own tool receipt")
            .await
            .unwrap();
        request = page["next_request"].clone();
    }
    assert_eq!(actual, expected);
    assert!(!actual.contains(&hidden.selector));
    let bytes = (0..50_000)
        .map(|index| (index % 256) as u8)
        .collect::<Vec<_>>();
    let binary = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/octet-stream".into(),
                ..descriptor
            },
            &bytes,
        )
        .await
        .unwrap();
    let mut request = serde_json::json!({"evidence_ref":binary.selector,"limit":1});
    let mut encoded = String::new();
    loop {
        let read: serde_json::Value = serde_json::from_str(
            &executor
                .execute_runtime_tool_with_binding("evidence_retrieve", request, binding)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(read["encoding"], "base64");
        assert_eq!(read["sha256"], binary.sha256);
        for chunk in read["chunks"].as_array().unwrap() {
            encoded.push_str(chunk["content"].as_str().unwrap());
        }
        if read["next_request"].is_null() {
            break;
        }
        request = read["next_request"].clone();
    }
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap(),
        bytes
    );
    assert!(executor
        .execute_runtime_tool_with_binding(
            "evidence_retrieve",
            serde_json::json!({"evidence_ref":binary.selector,"query":"needle"}),
            binding
        )
        .await
        .is_err());
}

/// PLAN M2: several Agent actions in ONE provider round, each on exactly the
/// same validation/authorization/commit path a direct call takes.

#[tokio::test]
async fn native_agent_actions_reject_unadmitted_root_execution() {
    let services = runtime::RuntimeServices::in_memory().expect("runtime services");
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("Agent action tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    executor
        .bind_runtime_services(&services)
        .expect("bind runtime services");
    let error = executor
        .execute_runtime_tool(
            "team_create",
            serde_json::json!({
                "name": "Unbound", "mission": "must not open a Program without an admitted graph"
            }),
        )
        .await
        .expect_err("unadmitted root must be rejected");
    assert!(error.to_string().contains("authenticated parent binding"));
}

#[test]
fn a_pure_one_of_schema_still_coerces_its_variant_fields() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let mut value = serde_json::json!({
        "source": "message_block",
        "message_id": "session-1",
        "block_index": "0",
        "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "media_type": "text/plain"
    });
    let repaired = executor.coerce_string_typed_structured_fields("artifact_publish", &mut value);
    assert_eq!(
        GatewayToolExecutor::coerced_field_names(&repaired),
        vec!["block_index".to_string()],
        "{repaired:?}"
    );
    assert_eq!(
        value["block_index"],
        serde_json::json!(0),
        "a quoted integer must be coerced, not left for the schema to refuse"
    );
}

/// The schema is the authority: a field it types as `string` is never coerced,
/// so a code fragment that happens to be valid JSON text survives untouched.
#[test]
fn a_schema_string_field_is_never_coerced() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let mut value = serde_json::json!({
        "source": "message_block",
        "message_id": "session-1",
        "block_index": 0,
        "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "media_type": "text/plain"
    });
    let repaired = executor.coerce_string_typed_structured_fields("artifact_publish", &mut value);
    assert!(
        repaired.is_empty(),
        "a correctly typed request must not be rewritten: {repaired:?}"
    );
    assert_eq!(value["media_type"], serde_json::json!("text/plain"));
}

/// Coercion only fixes the container shape; a value that is not a number is
/// still left for the schema to refuse, so a malformed request is not laundered
/// into a valid one.
#[test]
fn an_invalid_number_string_is_not_coerced() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let mut value = serde_json::json!({
        "source": "message_block",
        "message_id": "session-1",
        "block_index": "not-a-number",
        "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "media_type": "text/plain"
    });
    let repaired = executor.coerce_string_typed_structured_fields("artifact_publish", &mut value);
    assert!(repaired.is_empty(), "{repaired:?}");
    assert_eq!(value["block_index"], serde_json::json!("not-a-number"));
}

/// The restructure must not break the original container coercion that
/// `agent_action_batch` depends on.
#[test]
fn the_container_coercion_still_works_after_the_restructure() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let mut value = serde_json::json!({
        "actions": serde_json::json!([
            {"tool": "team_create", "input": {"name": "Gamma", "mission": "coerced lane"}}
        ])
        .to_string()
    });
    let repaired = executor.coerce_string_typed_structured_fields("agent_action_batch", &mut value);
    assert_eq!(
        GatewayToolExecutor::coerced_field_names(&repaired),
        vec!["actions".to_string()],
        "{repaired:?}"
    );
    assert!(
        value["actions"].is_array(),
        "the JSON-encoded array must be coerced back into an array"
    );
}

/// The contract boundary has to repair, not just refuse.
///
/// The coercion above lives in the executor, but it used to run only inside
/// `execute_runtime_tool_with_binding` - downstream of
/// `validate_tool_input`, which is where Runtime actually decides whether a
/// frame may run at all. So the one corruption the coercion exists to fix was
/// refused before the repair could reach it: one live run sent two identical
/// `agent_action_batch` frames with `actions` as a JSON-encoded string, was
/// refused twice, spent its single frame-repair budget, and ended with three
/// Teams, zero Tasks and verdict `partial`. This test drives the boundary the
/// runtime really calls.
#[tokio::test]
async fn the_contract_boundary_repairs_a_string_typed_array_and_hands_back_dispatch_bytes() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let malformed = serde_json::json!({
        "actions": serde_json::json!([
            {"tool": "team_create", "input": {"name": "Delta", "mission": "boundary repair"}}
        ])
        .to_string()
    })
    .to_string();

    let dispatched = executor
        .validate_tool_input("agent_action_batch", &malformed)
        .expect("a JSON-encoded array in an array field is repairable at the boundary");
    let value: serde_json::Value = serde_json::from_str(&dispatched).expect("dispatched JSON");
    assert!(
        value["actions"].is_array(),
        "the bytes Runtime dispatches must carry the array, not the string: {dispatched}"
    );
    assert_eq!(value["actions"][0]["tool"], "team_create");
}

/// Repair is not a licence to wave a frame through. A string that does not
/// parse as the declared type is still a contract violation, and the refusal
/// now says what to send.
#[tokio::test]
async fn the_contract_boundary_still_refuses_a_string_that_is_not_the_declared_type() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);

    let error = executor
        .validate_tool_input("agent_action_batch", r#"{"actions": "[{]"}"#)
        .expect_err("a string that is not a JSON array must still be refused");
    let message = error.to_string();
    assert!(message.contains("$.actions"), "{message}");
    assert!(message.contains("declared type: array"), "{message}");
    assert!(message.contains("received: string"), "{message}");
}

/// A field the schema types as `string` keeps its JSON text untouched: the
/// repair never rewrites a value the contract already accepts.
#[tokio::test]
async fn the_contract_boundary_leaves_a_string_field_alone() {
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    let input = r#"{"command": "echo [1,2,3]"}"#;

    let dispatched = executor
        .validate_tool_input("bash", input)
        .expect("valid bash input");
    let value: serde_json::Value = serde_json::from_str(&dispatched).expect("dispatched JSON");
    assert_eq!(value["command"], "echo [1,2,3]");
}

/// A frame that does not parse as a whole must be salvaged entry by entry.
///
/// Three live frames were refused whole with "value has the wrong JSON type".
/// None of them was a clean cut-off tail: the delimiter-closing repair refused
/// all three, because the model had dropped closing braces in the middle and
/// omitted the `tool` field on later entries. The individual entries that were
/// well formed were still perfectly good JSON, and Runtime threw them away too.
#[tokio::test]
async fn a_frame_that_does_not_parse_is_salvaged_entry_by_entry() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let first = r#"{"tool":"team_create","input":{"name":"Alpha","mission":"first"}}"#;
    let second = r#"{"tool":"agent_invite","input":{"team_ref":"team:a","mission":"second"}}"#;
    // The third entry is cut off inside its own object, exactly as the provider
    // ceiling leaves it.
    let inner = format!(r#"[{first},{second},{{"tool":"task_publish","input":"#);
    let cut_off = serde_json::json!({"actions": inner}).to_string();

    let dispatched = executor
        .validate_tool_input("agent_action_batch", &cut_off)
        .expect("the valid entries of an unparseable frame must survive");
    let value: serde_json::Value = serde_json::from_str(&dispatched).expect("dispatched JSON");
    let actions = value["actions"].as_array().expect("salvaged array");
    assert_eq!(
        actions.len(),
        2,
        "only the two well-formed entries may survive: {dispatched}"
    );
    assert_eq!(actions[0]["tool"], "team_create");
    assert_eq!(actions[1]["tool"], "agent_invite");
}

/// The salvage must be reported, not performed silently: the model has to know
/// an entry was dropped, or it will believe it sent everything it wrote.
#[tokio::test]
async fn a_salvaged_frame_reports_how_many_entries_were_dropped() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let first = r#"{"tool":"team_create","input":{"name":"Alpha","mission":"first"}}"#;
    let inner = format!(r#"[{first},{{"tool":"task_publish","input":"#);
    let cut_off = serde_json::json!({"actions": inner}).to_string();

    let coercions = executor.tool_input_coercions("agent_action_batch", &cut_off);
    assert!(matches!(coercions.as_slice(), [coercion] if coercion.field == "actions" && coercion.truncated), "{coercions:?}");
    assert_eq!(coercions[0].field, "actions");
    assert!(
        coercions[0].truncated,
        "a salvaged frame must be reported as repaired: {coercions:?}"
    );
    assert_eq!(coercions[0].salvaged_items, 1, "{coercions:?}");
    assert_eq!(
        coercions[0].dropped_items, 1,
        "the entry that did not parse must be counted, not hidden: {coercions:?}"
    );
}

/// The shape the live runs actually produced: later entries that omit `tool`
/// and drop their closing brace. The first entry is well formed and must still
/// run; the rest must be reported as dropped rather than silently discarded.
#[tokio::test]
async fn entries_that_omit_the_tool_field_do_not_discard_the_valid_ones() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let inner = concat!(
        r#"[{"tool":"team_create","input":{"name":"Alpha","mission":"first"}}"#,
        r#", "input": {"mission":"second"},"#,
        r#" "input": {"mission":"third"}]"#
    );
    let frame = serde_json::json!({"actions": inner}).to_string();

    let dispatched = executor
        .validate_tool_input("agent_action_batch", &frame)
        .expect("the one well-formed entry must survive");
    let value: serde_json::Value = serde_json::from_str(&dispatched).expect("dispatched JSON");
    let actions = value["actions"].as_array().expect("salvaged array");
    assert!(matches!(actions.as_slice(), [action] if action["tool"] == "team_create"), "{dispatched}");

    let coercions = executor.tool_input_coercions("agent_action_batch", &frame);
    assert_eq!(coercions[0].dropped_items, 2, "{coercions:?}");
}

/// Salvage is not a licence to guess. A string that is neither valid JSON nor a
/// cut-off container is still a contract violation.
#[tokio::test]
async fn a_string_that_is_neither_valid_nor_cut_off_is_still_refused() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);

    let error = executor
        .validate_tool_input("agent_action_batch", r#"{"actions": "team_create please"}"#)
        .expect_err("prose is not a cut-off container");
    assert!(error.to_string().contains("$.actions"), "{error}");
    let coercions =
        executor.tool_input_coercions("agent_action_batch", r#"{"actions": "team_create please"}"#);
    assert!(
        coercions.is_empty(),
        "nothing was repaired, so nothing may be reported: {coercions:?}"
    );
}

/// The refusal has to name the defect the model actually has.
///
/// This is the exact shape a live run produced: the model quoted the array and
/// repeated the `input` key inside one object instead of emitting one
/// `{tool, input}` entry per action. Runtime answered "send an array value as
/// raw JSON, not a JSON-encoded string" - accurate about the type and useless
/// about the problem, so the model's single repair attempt reproduced the same
/// frame. Nothing here is salvaged, and nothing is guessed; the refusal only
/// reports the shape the model used.
#[tokio::test]
async fn a_refusal_names_the_missing_tool_field_rather_than_the_quoting() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    // One object holding three `input` keys, never closed, then a `]` that
    // already closes the array. Neither salvage nor delimiter-closing can help:
    // the splitter sees no top-level comma, and closing the delimiters appends a
    // second `]`.
    let inner = concat!(
        r#"[{"input": {"expertise_hints": ["formal_methods"], "mission": "Formalize definitions."}"#,
        r#", "input": {"mission": "Survey applications."}"#,
        r#", "input": {"mission": "Design the experiment."}]"#
    );
    let frame = serde_json::json!({"actions": inner}).to_string();

    let error = executor
        .validate_tool_input("agent_action_batch", &frame)
        .expect_err("entries without a `tool` are not a dispatchable batch");
    let message = error.to_string();
    assert!(message.contains("$.actions"), "{message}");
    assert!(
        message.contains("carry `input` but no `tool`"),
        "the refusal must name the real defect: {message}"
    );
    assert!(
        message.contains("one action per entry"),
        "the refusal must say what to send instead: {message}"
    );
}

/// A logical artifact key that no Program maps must still fail with the store's
/// own error. The mapping is a fallback for a real reference, not a way to make
/// an unknown id resolve: laundering "this does not exist" into "read failed"
/// would hide the model's mistake instead of naming it.
#[tokio::test]
async fn evidence_retrieve_still_fails_when_no_program_maps_the_key() {
    let services = runtime::RuntimeServices::in_memory().expect("runtime services");
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor.bind_runtime_services(&services).unwrap();
    // The spelling a model produces when it copies Runtime's `artifact:<24 hex>`
    // and adds the separator itself. No Program holds this key.
    let request = serde_json::from_value::<EvidenceRetrieveToolRequest>(
        serde_json::json!({"evidence_ref": "artifact://0123456789abcdef01234567"}),
    )
    .unwrap();
    let error = executor
        .execute_evidence_retrieve(request, Some("session-none"), &[])
        .await
        .expect_err("an unmapped logical key must not resolve");
    assert!(
        error.to_string().contains("artifact does not exist"),
        "the store's own error must survive: {error}"
    );
}

/// A real store selector must keep resolving. The logical-key fallback is only
/// consulted after the store refuses, so it cannot disturb the normal path.
#[tokio::test]
async fn a_valid_artifact_selector_still_resolves() {
    let services = runtime::RuntimeServices::in_memory().expect("runtime services");
    let artifact = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:session-direct".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"the reviewed body",
        )
        .await
        .unwrap();
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor.bind_runtime_services(&services).unwrap();
    let request = serde_json::from_value::<EvidenceRetrieveToolRequest>(
        serde_json::json!({"evidence_ref": artifact.selector}),
    )
    .unwrap();
    let output = executor
        .execute_evidence_retrieve(
            request,
            Some("session-direct"),
            &["session:session-direct".to_string()],
        )
        .await
        .expect("a real selector must still resolve");
    let value: serde_json::Value = serde_json::from_str(&output).expect("json");
    assert_eq!(value["available"], true);
    assert!(value["bytes"].as_u64().unwrap() > 0, "{value}");
}

/// The fields a schema requires, across the variants of a tagged schema.
fn required_field_names(schema: &serde_json::Value) -> std::collections::BTreeSet<String> {
    let mut names = std::collections::BTreeSet::new();
    let mut collect = |node: &serde_json::Value| {
        if let Some(required) = node.get("required").and_then(serde_json::Value::as_array) {
            for name in required.iter().filter_map(serde_json::Value::as_str) {
                names.insert(name.to_string());
            }
        }
    };
    collect(schema);
    for keyword in ["oneOf", "anyOf"] {
        if let Some(variants) = schema.get(keyword).and_then(serde_json::Value::as_array) {
            for variant in variants {
                collect(variant);
            }
        }
    }
    names
}

/// Every structured-input tool must advertise an example that passes its own
/// schema.
///
/// The example exists so a model can copy a correct shape instead of
/// synthesizing one. An example that fails the tool's own `validate_input` is
/// worse than no example - the model copies it and is refused, which is the
/// exact loop this was built to end. Checking for the literal string
/// `Example input:` would only catch a missing injection, not a drifted one.
#[tokio::test]
async fn every_structured_input_tool_advertises_an_example_that_validates() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let mut checked = 0_usize;
    let mut degraded = 0_usize;
    let mut skipped = 0_usize;
    for definition in crate::runtime_bootstrap::runtime_capability_tool_definitions() {
        let Some(example) =
            harness_contract::tool_input_contract::input_example(&definition.input_schema)
        else {
            // Nothing to fill in: an example of `{}` would be noise.
            skipped += 1;
            continue;
        };
        let description = definition.description.unwrap_or_default();
        // A degraded example is not an instance: it names the fields instead of
        // showing one. It is accepted only when it names every required field
        // and the description says so - a degradation passed off as an input
        // would be the failure this whole contract exists to prevent.
        if let Some(degradation) =
            harness_contract::tool_input_contract::degraded_example(&example)
        {
            let named = degradation
                .fields
                .iter()
                .filter(|field| field.required)
                .map(|field| field.name.clone())
                .collect::<std::collections::BTreeSet<_>>();
            let missing = required_field_names(&definition.input_schema)
                .into_iter()
                .filter(|name| !named.contains(name))
                .collect::<Vec<_>>();
            assert!(
                missing.is_empty(),
                "`{}` has a degraded example that does not name {missing:?}",
                definition.name
            );
            assert!(
                description.contains("Example degraded:"),
                "`{}` has a degraded example but its description does not say so",
                definition.name
            );
            degraded += 1;
            continue;
        }
        let rendered = serde_json::to_string(&example).expect("example renders");
        executor
            .validate_tool_input(&definition.name, &rendered)
            .unwrap_or_else(|error| {
                panic!(
                    "example for `{}` fails its own schema: {error}\n{rendered}",
                    definition.name
                )
            });
        assert!(
            description.contains("Example input:"),
            "`{}` has an example but its description does not carry it",
            definition.name
        );
        assert!(
            description.contains(&rendered),
            "`{}` description carries a different example than the schema derives",
            definition.name
        );
        checked += 1;
    }
    // The window counts instances only: a tool whose example degraded is not
    // covered by an instance, so it never widens the window. The degraded
    // branch has its own bound.
    let coverage = harness_contract::tool_input_contract::ExampleCoverage {
        instances: checked,
        degraded,
    };
    // The counts are printed so the relaxation window is measurable from the
    // gate itself, not from a reading of the code.
    println!("example coverage: {coverage:?}");
    assert!(
        coverage.instance_window_holds(),
        "expected the runtime tool surface to be covered by real instances: {coverage:?}"
    );
    assert!(
        coverage.degraded_branch_holds(),
        "the degraded branch is a bounded side branch: {coverage:?}"
    );
    assert!(
        skipped > 0,
        "some tools legitimately have nothing to fill in"
    );
}

/// The case this whole patch exists for: the batch entry wrapper.
#[tokio::test]
async fn the_batch_tool_advertises_the_entry_wrapper() {
    let definition = crate::runtime_bootstrap::runtime_capability_tool_definitions()
        .into_iter()
        .find(|definition| definition.name == "agent_action_batch")
        .expect("agent_action_batch is registered");
    let description = definition.description.unwrap_or_default();
    assert!(description.contains("Example input:"), "{description}");
    let rendered = description
        .split("Example input:")
        .nth(1)
        .expect("the example marker")
        .trim();
    let example: serde_json::Value = serde_json::from_str(rendered).expect("example parses");
    let entry = example["actions"]
        .as_array()
        .expect("actions array")
        .first()
        .expect("one entry")
        .as_object()
        .expect("entry object");
    assert!(
        entry.contains_key("tool"),
        "the wrapper must be shown: {rendered}"
    );
    assert!(
        entry.contains_key("input"),
        "the payload must be shown: {rendered}"
    );
}

/// Cross-channel contract invariants.
///
/// A tool call's contract is decided at five places, and each used to derive its
/// own answer from the raw bytes. That is how a rule written at one point failed to
/// reach the others: V1-V4 drifted from V8-V14, and the 1014 plan drifted from
/// P42/U1-U2 twice more. Both times a human read the code and missed it, because
/// nothing machine-checked that the channels still agree.
///
/// These assertions are about *external behaviour* - accepted, repaired, refused,
/// and what the refusal says - never about internal structure. Two channels may
/// legitimately be built differently; they may not answer differently.
mod cross_channel_contract_invariants {
    use super::*;

    /// The corpus every invariant below runs over.
    ///
    /// Four shapes, because those are the four decisions a contract boundary can
    /// make: accept as-is, repair, refuse, and count a repeat.
    fn corpus() -> Vec<(&'static str, String)> {
        vec![
            (
                "legal",
                serde_json::json!({"actions": [
                    {"tool": "team_create", "input": {"name": "Alpha", "mission": "invariants"}}
                ]})
                .to_string(),
            ),
            (
                "string-typed",
                serde_json::json!({"actions":
                    serde_json::json!([{"tool": "team_create", "input": {"name": "Beta", "mission": "invariants"}}]).to_string()
                })
                .to_string(),
            ),
            ("missing-required", r#"{"actions": [{"input": {"name": "Gamma"}}]}"#.to_string()),
            (
                "unknown-field",
                serde_json::json!({"actions": [
                    {"tool": "team_create", "input": {"name": "Delta", "mission": "m"}, "invented": 1}
                ]})
                .to_string(),
            ),
        ]
    }

    fn executor() -> GatewayToolExecutor {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
            .expect("runtime tools");
        GatewayToolExecutor::new(None, false, registry)
    }

    /// Repair must be a fixed point.
    ///
    /// The boundary hands back the bytes Runtime will actually dispatch, and the
    /// transcript, the governed plan and the execution all read those bytes. If
    /// repairing an already-repaired input produced something different, those
    /// three could disagree about what ran - which is the exact hazard the
    /// `provider_plane` frame-boundary comment pays an extra schema walk to avoid.
    #[tokio::test]
    async fn repair_is_a_fixed_point() {
        let executor = executor();
        let cases = corpus().into_iter().chain(std::iter::once((
            "root-unknown-field",
            serde_json::json!({"actions": [
                {"tool": "team_create", "input": {"name": "Alpha", "mission": "invariants"}}
            ], "invented": 1})
            .to_string(),
        )));
        for (label, input) in cases {
            // Only the root object forbids extensions; actions.items is open.
            let expected_acceptance = match label {
                "legal" | "string-typed" | "unknown-field" => true,
                "missing-required" | "root-unknown-field" => false,
                _ => panic!("{label}: corpus case has no independent contract expectation"),
            };
            let result = executor.validate_tool_input("agent_action_batch", &input);
            assert_eq!(
                result.is_ok(),
                expected_acceptance,
                "{label}: schema-boundary acceptance violates its declared contract: {result:?}"
            );
            if let Ok(first) = result {
                let second = executor
                    .validate_tool_input("agent_action_batch", &first)
                    .unwrap_or_else(|error| {
                        panic!("{label}: the repaired bytes must still be legal: {error}")
                    });
                assert_eq!(
                    first, second,
                    "{label}: repairing an already-repaired frame changed it, so the transcript \\
                     and the dispatched bytes could disagree"
                );
            }
        }
    }

    /// The bytes the boundary hands back are the bytes that get dispatched.
    ///
    /// `validate_tool_input` returns the repaired form precisely so the transcript,
    /// the governed plan and the execution cannot disagree. A model that re-sends the
    /// same malformed frame must therefore hash the same and be seen as a repeat by
    /// the repetition guard - the precondition for every bound in the framework.
    #[tokio::test]
    async fn the_same_frame_hashes_identically_after_repair() {
        let executor = executor();
        let cases = corpus().into_iter().chain(std::iter::once((
            "root-unknown-field",
            serde_json::json!({"actions": [
                {"tool": "team_create", "input": {"name": "Alpha", "mission": "invariants"}}
            ], "invented": 1})
            .to_string(),
        )));
        for (label, input) in cases {
            // A legal entry extension is distinct from a forbidden root field.
            let expected_acceptance = match label {
                "legal" | "string-typed" | "unknown-field" => true,
                "missing-required" | "root-unknown-field" => false,
                _ => panic!("{label}: corpus case has no independent contract expectation"),
            };
            let result = executor.validate_tool_input("agent_action_batch", &input);
            assert_eq!(
                result.is_ok(),
                expected_acceptance,
                "{label}: schema-boundary acceptance violates its declared contract: {result:?}"
            );
            if let Ok(repaired) = result {
                let again = executor
                    .validate_tool_input("agent_action_batch", &input)
                    .expect("the same input must decide the same way twice");
                // Byte equality is the whole invariant: the repetition guard keys on a
                // hash of these bytes, so identical bytes are necessarily an identical
                // key and the guard sees a repeat.
                assert_eq!(
                    repaired, again,
                    "{label}: the same frame produced different bytes, so the repetition guard \
                     would not recognise it as a repeat"
                );
            }
        }
    }

    /// A refusal for a missing field must name that field.
    ///
    /// One live run re-sent `{}` to `artifact_publish` twelve times while being told
    /// to change the input, because nothing said the input was missing `path`. A
    /// refusal that does not name the field is not a bound; it is a wall the model
    /// pays a provider round-trip to discover.
    #[tokio::test]
    async fn a_refusal_for_a_missing_field_names_that_field() {
        let executor = executor();
        let error = executor
            .validate_tool_input(
                "agent_action_batch",
                r#"{"actions": [{"input": {"name": "Gamma"}}]}"#,
            )
            .expect_err("an entry without `tool` violates the declared contract");
        let message = error.to_string();
        assert!(
            message.contains("tool"),
            "the refusal must name the field the model must add: {message}"
        );
    }
}

/// An unknown tool name is one character from working, so say which character.
///
/// One live run re-issued the same invented tool name sixteen times against a
/// refusal that only said "not registered". codex and opencode both write the
/// correction into the error itself; the ranked suggestion engine already existed
/// here and was wired only to the CLI and slash commands.
#[tokio::test]
async fn an_unknown_tool_name_names_the_closest_registered_tool() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);

    let error = executor
        .validate_tool_input("read_files", "{}")
        .expect_err("an unregistered name must be refused");
    let message = error.to_string();
    assert!(
        message.contains("Did you mean"),
        "the refusal must offer the correction: {message}"
    );
    assert!(
        message.contains("read_file"),
        "and it must be the right one: {message}"
    );
    assert!(
        message.contains("Registered tools:"),
        "the full set stays visible so the model is not funnelled: {message}"
    );
}

#[tokio::test]
async fn publication_reader_restores_all_binary_and_unicode_bytes_with_bounded_actual_pages() {
    use base64::Engine;
    use sha2::{Digest, Sha256};
    let services = runtime::RuntimeServices::in_memory().unwrap();
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor.bind_runtime_services(&services).unwrap();
    for bytes in [
        "中文 evidence 🙂".repeat(2000).into_bytes(),
        (0..10007).map(|i| (i % 256) as u8).collect(),
    ] {
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let artifact = services
            .publish_authorized_content(
                &bytes,
                &digest,
                "application/octet-stream",
                "bounded-reader",
            )
            .await
            .unwrap();
        let mut request: EvidenceRetrieveToolRequest =
            serde_json::from_value(serde_json::json!({"evidence_ref":artifact.selector,"limit":1}))
                .unwrap();
        let mut restored = Vec::new();
        let mut pages = 0;
        loop {
            let output: serde_json::Value = serde_json::from_str(
                &executor
                    .execute_evidence_retrieve(request, Some("bounded-reader"), &[])
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(output["bytes"], bytes.len());
            assert!(output["read_bytes"].as_u64().unwrap() <= 1500);
            assert_eq!(output["coverage"]["byte_start"], restored.len());
            for chunk in output["chunks"].as_array().unwrap() {
                assert_eq!(chunk["byte_start"], restored.len());
                let content = chunk["content"].as_str().unwrap();
                if chunk["encoding"] == "base64" {
                    restored.extend(
                        base64::engine::general_purpose::STANDARD
                            .decode(content)
                            .unwrap(),
                    );
                } else {
                    restored.extend_from_slice(content.as_bytes());
                }
            }
            assert_eq!(output["coverage"]["byte_end"], restored.len());
            pages += 1;
            assert!(pages < 100);
            if output["next_request"].is_null() {
                break;
            }
            assert_eq!(output["coverage"]["complete"], false);
            request = serde_json::from_value(output["next_request"].clone()).unwrap();
        }
        assert!(pages > 1);
        assert_eq!(restored, bytes);
    }
}

#[tokio::test]
async fn publication_query_without_match_keeps_unread_source_as_continuation() {
    use sha2::{Digest, Sha256};
    let services = runtime::RuntimeServices::in_memory().unwrap();
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    executor.bind_runtime_services(&services).unwrap();
    let bytes = format!("{}needle", "x".repeat(1500)).into_bytes();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let artifact = services
        .publish_authorized_content(&bytes, &digest, "text/plain", "query-reader")
        .await
        .unwrap();
    let request = serde_json::from_value(
        serde_json::json!({"evidence_ref":artifact.selector,"query":"needle","limit":1}),
    )
    .unwrap();
    let first: serde_json::Value = serde_json::from_str(
        &executor
            .execute_evidence_retrieve(request, Some("query-reader"), &[])
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first["chunks"], serde_json::json!([]));
    assert_eq!(first["truncated"], true);
    assert_eq!(first["coverage"]["complete"], false);
    let next = serde_json::from_value(first["next_request"].clone()).unwrap();
    let last: serde_json::Value = serde_json::from_str(
        &executor
            .execute_evidence_retrieve(next, Some("query-reader"), &[])
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(last["chunks"][0]["content"], "needle");
    assert!(last["next_request"].is_null());
    assert_eq!(last["coverage"]["complete"], false);
}

#[test]
fn bound_step_decodes_only_complete_strict_batch_arrays() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let inner = r#"[{"tool":"team_create","input":{}}]"#;
    let frame = json!({"actions": inner}).to_string();
    let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
        &runtime::tool_dispatch::ToolRequest {
            tool_use_id: "bound-encoded-batch".into(),
            tool_name: "agent_action_batch".into(),
            input: frame.clone(),
            depends_on: vec![],
        },
    );
    request.session_id = Some("bound-encoded-session".into());
    attach_gateway_step(&executor, &mut request);
    let binding = request.step_binding.as_ref().unwrap();
    let repaired = executor.validate_step_tool_input(Some(binding), "agent_action_batch", &frame)
        .expect("the original pinned Step accepts a complete encoded array");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&repaired).unwrap(),
        json!({"actions":[{"tool":"team_create","input":{}}]}));
    for bad in [
        r#"[{"tool":"team_create","input":{}}, {"tool":"team_create""#,
        r#"[{"tool":"team_create","tool":"task_publish","input":{}}]"#,
        r#"{"tool":"team_create","input":{}}"#,
        r#"[{"input":{}}]"#,
    ] {
        let invalid = json!({"actions": bad}).to_string();
        assert!(executor.validate_step_tool_input(Some(binding), "agent_action_batch", &invalid).is_err());
    }
    let mut foreign = binding.clone();
    foreign.origin.source = harness_contract::step::CallSource::Provider { attempt_id: "foreign".into() };
    assert!(executor.validate_step_tool_input(Some(&foreign), "agent_action_batch", &frame)
        .unwrap_err().to_string().contains("step_origin_source_rejected"));
}

/// The live task_submit refusal on 0.10.49 (live_one_team_execution): the model sent a
/// `deliverable` that failed the pinned `anyOf` and the way_out was the whole-call
/// fallback `{"task_ref":"<task_ref>"}` - a shape with no deliverable at all - followed
/// by "Send the same intended action again in the exact shape above". The model cannot
/// repair against that. The way_out must show the violated field's shape.
#[test]
fn task_submit_deliverable_violation_shows_the_deliverable_shape_not_a_bare_task_ref() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let frame = json!({
        "task_ref": "task:5f1e44e20353348db43c142b",
        "deliverable": {"block_index": "0", "kind": "source_fact",
            "title": "crates/runtime/src/lib.rs — runtime_event_store 在互斥 cfg 门控下双重声明"},
        "evidence_refs": ["tool://tool-raw-call_cb29b09df5554931929356b7-ee6a87"]
    })
    .to_string();
    let refused = executor
        .validate_tool_input("task_submit", &frame)
        .expect_err("the live shape violates the pinned deliverable anyOf");
    let envelope: serde_json::Value =
        serde_json::from_str(&refused.to_string()).expect("the unbound refusal is a JSON envelope");
    let message = envelope["error"]["message"].as_str().expect("error message").to_string();
    assert!(message.contains("$.deliverable"), "{message}");
    let example_text = message
        .split_once("Send it shaped like this: ")
        .expect("the refusal carries a way_out")
        .1;
    // The path form shows the violated field's own shape (the established
    // `violated_path` contract), so the example IS the deliverable object.
    let example: serde_json::Value =
        serde_json::from_str(example_text).expect("complete JSON example");
    assert!(example.is_object(), "the way_out must be the deliverable's shape: {example_text}");
    assert!(
        example["block_index"].is_u64(),
        "block_index is a usize, the live model sent `\"0\"`: {example_text}"
    );
    assert!(example["kind"].is_string() && example["title"].is_string(), "{example_text}");
    assert!(example.get("task_ref").is_none(), "a bare task_ref is not the deliverable's shape: {example_text}");
    let repaired = json!({
        "task_ref": "task:5f1e44e20353348db43c142b",
        "deliverable": example,
        "evidence_refs": ["tool://tool-raw-call_cb29b09df5554931929356b7-ee6a87"]
    })
    .to_string();
    executor
        .validate_tool_input("task_submit", &repaired)
        .expect("the shown deliverable shape must satisfy the same pinned schema");
}

/// live_one_team_execution (0.10.49 + six fixes, 2026-11-24): task_review was refused
/// three times for `unexpected field `reason_ref`` and the refusal carried no hint that
/// `reason_ref` belongs to other actions and task_review takes a text `reason`.
#[test]
fn task_review_reason_ref_refusal_carries_the_shape_correction_hint() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let frame = json!({
        "task_ref": "task:5f1e44e20353348db43c142b",
        "decision": "accept",
        "reason": "the diff matches the Task statement",
        "reason_ref": "tool://tool-raw-call_cb29b09df5554931929356b7-ee6a87"
    })
    .to_string();
    let refused = executor
        .validate_tool_input("task_review", &frame)
        .expect_err("reason_ref is not a task_review field");
    let message = refused.to_string();
    assert!(message.contains("unexpected field `reason_ref`"), "{message}");
    assert!(
        message.contains("task_review requires a string `reason`"),
        "the refusal must name the replacement field: {message}"
    );

    let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
        &runtime::tool_dispatch::ToolRequest {
            tool_use_id: "bound-refused-review".into(),
            tool_name: "task_review".into(),
            input: frame.clone(),
            depends_on: vec![],
        },
    );
    request.session_id = Some("bound-feedback-session".into());
    attach_gateway_step(&executor, &mut request);
    let binding = request.step_binding.as_ref().unwrap();
    let bound = executor
        .validate_step_tool_input(Some(binding), "task_review", &frame)
        .expect_err("the bound Step validator refuses the extra field too");
    let bound_message = bound.to_string();
    assert!(
        bound_message.contains("unexpected field `reason_ref`")
            && bound_message.contains("task_review requires a string `reason`"),
        "the bound Step refusal (production path) must carry the same hint: {bound_message}"
    );
}

/// Production uses the bound Step validator, not the unbound CLI validator.
/// Reproduce the observed actions:string / malformed inner entries; this is a
/// controlled shape counterexample, not a claim of replaying an entire HTTP frame.
#[test]
fn bound_step_refusal_carries_the_existing_pinned_schema_example() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let inner = concat!(
        r#"[{"input":{"mission":"first independent audit"}"#,
        r#", "input":{"mission":"second independent audit"}]"#,
    );
    let frame = json!({"actions":inner,"tool":"agent_invite"}).to_string();
    let unbound = executor
        .validate_tool_input("agent_action_batch", &frame)
        .unwrap_err();
    assert!(
        unbound.to_string().contains("Send it shaped like this:"),
        "{unbound}"
    );
    let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
        &runtime::tool_dispatch::ToolRequest {
            tool_use_id: "bound-refused-batch".into(),
            tool_name: "agent_action_batch".into(),
            input: frame.clone(),
            depends_on: vec![],
        },
    );
    request.session_id = Some("bound-feedback-session".into());
    attach_gateway_step(&executor, &mut request);
    let binding = request.step_binding.as_ref().unwrap();
    let refused = executor
        .validate_step_tool_input(Some(binding), "agent_action_batch", &frame)
        .unwrap_err();
    let message = refused.to_string();
    let example_text = message
        .split_once("Send it shaped like this: ")
        .expect("the actual bound production caller must carry the existing example")
        .1;
    let example: serde_json::Value =
        serde_json::from_str(example_text).expect("complete JSON example");
    assert!(example["actions"].is_array());
    assert!(example["actions"][0]["tool"].is_string());
    assert!(example["actions"][0]["input"].is_object());
    let accepted = executor
        .validate_step_tool_input(Some(binding), "agent_action_batch", example_text)
        .expect("the same original pinned Step accepts the example's shape");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&accepted).unwrap(),
        example
    );
    for invalid in [
        frame.as_str(),
        r#"{"actions":[{"input":{"mission":"missing tool"}}]}"#,
        r#"{"actions":[],"actions":[{"tool":"team_create","input":{}}]}"#,
        r#"{"actions":[{"tool":"team_create","input":{}}"#,
    ] {
        assert!(
            executor
                .validate_step_tool_input(Some(binding), "agent_action_batch", invalid)
                .is_err(),
            "invalid frame must not become a dispatchable input: {invalid}"
        );
    }
    let mut foreign = binding.clone();
    foreign.origin.source = harness_contract::step::CallSource::Provider {
        attempt_id: "foreign-attempt".into(),
    };
    let foreign_error = executor
        .validate_step_tool_input(Some(&foreign), "agent_action_batch", &frame)
        .unwrap_err()
        .to_string();
    assert!(
        foreign_error.contains("step_origin_source_rejected"),
        "{foreign_error}"
    );
    assert!(
        !foreign_error.contains("Send it shaped like this:"),
        "authentication failures must not be converted into schema guidance"
    );
    assert_eq!(
        request.input, frame,
        "the refused original input is not rewritten or broadcast"
    );
}

/// A one-shot deliverable that arrived quoted is put back into shape at the
/// boundary.
///
/// The repair the boundary already had never fired on this field: the field's
/// schema node is a `$ref` into the definitions, so the repair read no type
/// there and left the quote in place - and the shape gate then refused the
/// call for a shape it could have repaired. One live run lost an objective to
/// exactly this.
#[test]
fn a_quoted_one_shot_deliverable_is_repaired_at_the_shape_gate() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let frame = json!({
        "task_ref": "task-1",
        "deliverable": "{\"block_index\": 2, \"kind\": \"report\", \"title\": \"Result\"}",
    });
    let accepted = executor
        .validate_tool_input("task_submit", &frame.to_string())
        .unwrap_or_else(|error| panic!("a quoted deliverable must be repaired: {error}"));
    let value: serde_json::Value = serde_json::from_str(&accepted).expect("json");
    assert!(
        value["deliverable"].is_object(),
        "the quoted block became an object: {value}"
    );
}

/// A literal the contract closed the field to, written in another case, is put
/// back into the contract's spelling instead of being refused.
///
/// The shape gate used to refuse the call for a letter, and the refusal cost
/// the model an interaction for a spelling it could not have known. The schema
/// is the authority on the member, so the boundary repairs the spelling.
#[test]
fn a_literal_written_in_another_case_is_repaired_at_the_shape_gate() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let frame = json!({
        "task_ref": "task-1",
        "decision": "Accept",
        "reason": "the deliverable is there",
    });
    let accepted = executor
        .validate_tool_input("task_review", &frame.to_string())
        .unwrap_or_else(|error| panic!("a case-drifted literal must be repaired: {error}"));
    let value: serde_json::Value = serde_json::from_str(&accepted).expect("json");
    assert_eq!(
        value["decision"], "accept",
        "the contract's spelling is restored: {value}"
    );
}

/// A value that names no legal member is not repaired: the shape face does not
/// choose a member for the model.
///
/// This is the mirror of the repair: the repair only ever restores a spelling
/// the schema already closed the field to, and never invents one.
#[test]
fn a_literal_that_names_no_member_is_still_refused() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let frame = json!({
        "task_ref": "task-1",
        "decision": "nonsense",
        "reason": "the deliverable is there",
    });
    let error = executor
        .validate_tool_input("task_review", &frame.to_string())
        .expect_err("a value that names no legal member must still be refused");
    assert!(
        error.to_string().contains("enum"),
        "the refusal must name the closed field: {error}"
    );
}

#[tokio::test]
async fn k02_orchestration_partial_receipt_recovers_same_session_bounded_pages() {
    use base64::Engine;
    use sha2::{Digest, Sha256};
    let temporary = tempfile::tempdir().unwrap();
    let workspace = temporary.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let store = Arc::new(crate::pg_test_support::session_store());
    let session_id = "k02-recovery-owner";
    for id in [session_id, "k02-recovery-foreign"] {
        store.create_session(&session::SessionRecord {
            session_id: id.into(), platform: "test".into(), chat_id: id.into(), user_id: None,
            model: None, created_at: "2026-10-29T00:00:00Z".into(), last_activity: "2026-10-29T00:00:00Z".into(),
            message_count: 0, reset_policy: "manual".into(), metadata_json: None,
            input_tokens: 0, output_tokens: 0, status: "active".into(),
        }).await.unwrap();
    }
    let repository = Arc::new(crate::services::session_service::repository::SessionRepository::new(
        Arc::new(crate::active_session::ActiveSessionDirectory::new()), Some(Arc::clone(&store)),
        crate::event_bus::SessionProjectionHub::new()));
    let presence = Arc::new(crate::services::session_service::presence::SessionPresenceLedger::with_store(Arc::clone(&store)));
    let port = crate::session_runtime_data_port::GatewaySessionRuntimePort::new_for_test(repository, presence);
    let services = runtime::RuntimeServices::builder(temporary.path().join("home"), &workspace)
        .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
        .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
        .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(temporary.path().join("artifacts"))))
        .build().unwrap();
    services.install_session_ports(port.clone(), port.clone(), port.clone(), port).unwrap();
    let synthetic_raw = serde_json::json!({"calls":[{"stepId":"batch","op":"batch","status":"ok","calls":2}],"result":{
        "cargo":{"ok":true,"status":"success","output":{"truncated":false,"file":{"filePath":"Cargo.toml","content":"[workspace]\\nmembers = []\\n","startLine":1,"numLines":2,"totalLines":2}}},
        "runtime":{"ok":true,"status":"success","output":{"truncated":true,"scanComplete":false,"numLines":100,"next_request":{"cursor":"next-search-page"},"content":"x".repeat(20000)}}}}).to_string();
    let fixture_path = std::env::var_os("K02_BATCH_RAW_FIXTURE");
    let raw = fixture_path.as_ref().map_or_else(|| synthetic_raw.clone(), |path| std::fs::read_to_string(path).expect("original private raw"));
    let real_original = fixture_path.is_some();
    if real_original { assert_eq!(format!("{:x}", Sha256::digest(raw.as_bytes())), "19b54274f071f26f941633685c8d8e5597abda57254e95722e49842add78c91a"); }
    let evidence_id = "tool-raw-k02-recovery";
    let artifact = services.artifact_store().write_bytes(harness_contract::context::ArtifactWriteDescriptor {
        media_type: "text/plain".into(), visibility_scope: format!("session:{session_id}"),
        expected_bytes: Some(raw.len() as u64), original_name: Some("raw".into()),
    }, raw.as_bytes()).await.unwrap();
    store.append_session_domain_event_allocating_sequence(&session::SessionDomainEvent::new(
        session_id, 0, session::SessionDomainScope::Tool, runtime::RuntimeSessionEventKind::EvidenceRawPersisted.as_str(),
        serde_json::json!({"evidence_id":evidence_id,"content_hash":artifact.sha256,"byte_count":artifact.bytes,
            "media_type":artifact.media_type,"artifact_selector":artifact.selector,"visibility_scope":artifact.visibility_scope}), 1)).await.unwrap();
    let fixture_input = if real_original {
        let path = std::env::var_os("K02_BATCH_SCENARIO_FIXTURE").expect("private scenario path");
        let report: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        report["failure_diagnostics"]["timeline"]["events"].as_array().unwrap().iter()
            .find(|event| event["kind"] == "model.item_completed" && event["payload"]["tool_call_id"] == "chatcmpl-tool-90aac7f46052e856")
            .unwrap()["payload"]["content"].as_str().unwrap().to_owned()
    } else {
        serde_json::json!({"steps":[{"id":"batch","op":"batch","calls":[{"as":"cargo","tool":"read_file","input":{"path":"Cargo.toml"}},{"as":"runtime","tool":"grep_search","input":{"path":"src"}}]}],"return":"$batch"}).to_string()
    };
    let runtime_key = if real_original { "runtime_mods" } else { "runtime" };
    let receipt = runtime::context_evidence::k02_fixture_orchestration_receipt(&raw, evidence_id, 4000, true, Some(&fixture_input));
    assert!(receipt.truncated && receipt.omitted_tokens > 0 && receipt.receipt_tokens <= 4000);
    let summary: serde_json::Value = serde_json::from_str(&receipt.summary[receipt.summary.find('{').unwrap()..]).unwrap();
    assert_eq!(summary["complete"], false);
    assert_eq!(summary["result"][runtime_key]["source_tool"], "grep_search");
    assert_eq!(summary["result"][runtime_key]["output"]["scanComplete"], false);
    assert!(summary["result"][runtime_key]["output"]["next_request"]["cursor"].is_string());
    // The production OpenAI-compatible wire compiler retains the exact
    // bounded receipt under the same tool call identity.
    let provider_request = provider::MessageRequest {
        model: "fixture-model".into(), max_tokens: 32,
        messages: vec![provider::InputMessage { role: "assistant".into(), content: vec![provider::InputContentBlock::ToolUse { id: "fixture-call".into(), name: "tool_orchestrate".into(), input: serde_json::json!({"steps":[]}) }] },
            provider::InputMessage::user_tool_result("fixture-call", receipt.summary.clone(), false)],
        ..Default::default()
    };
    let wire = provider::OpenAiCompatClient::new("fixture-key", provider::OpenAiCompatConfig::openai()).wire_request(&provider_request).unwrap();
    let delivered = wire.body["messages"].as_array().unwrap().iter().find(|m| m["tool_call_id"] == "fixture-call").unwrap();
    assert!(delivered["content"].as_str() == Some(receipt.summary.as_str()), "provider wire did not preserve the bounded receipt");
    let request = summary["recovery"]["when_available_and_authorized"]["input"].clone();
    assert_eq!(request["reference"], format!("tool://{evidence_id}"));
    let registry = GatewayToolRegistry::builtin().with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions()).unwrap();
    let executor = GatewayToolExecutor::new(None, false, registry);
    executor.bind_runtime_services(&services).unwrap();
    let binding = RuntimeToolExecutionBinding { action_invocation: None, tool_lease: None, action_id: None, session_id: Some(session_id), authorized_scopes: &[], memory_context: None, reality_context: None, model_lease: Some("test"), parent_execution: None, parent_execution_attempt: None, execution_decision: None, permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly };
    let foreign = RuntimeToolExecutionBinding { session_id: Some("k02-recovery-foreign"), ..binding.clone() };
    let foreign_out = executor.execute_runtime_tool_with_binding("read_ref", request.clone(), foreign).await;
    assert!(foreign_out.is_err() || serde_json::from_str::<serde_json::Value>(&foreign_out.unwrap()).unwrap()["available"] != true);
    let invalid = executor.execute_runtime_tool_with_binding("read_ref", serde_json::json!({"reference":format!("tool://{evidence_id}"),"cursor":"not-a-valid-cursor"}), binding.clone()).await;
    assert!(invalid.is_err() || serde_json::from_str::<serde_json::Value>(&invalid.unwrap()).unwrap()["available"] != true);
    let mut request = request; request["limit"] = serde_json::json!(16); let mut restored = Vec::new(); let mut pages = 0;
    loop {
        let page: serde_json::Value = serde_json::from_str(&executor.execute_runtime_tool_with_binding("read_ref", request.clone(), binding.clone()).await.unwrap()).unwrap();
        assert_eq!(page["available"], true);
        assert_eq!(page["sha256"], artifact.sha256);
        for chunk in page["chunks"].as_array().unwrap() {
            let content = chunk["content"].as_str().unwrap();
            if chunk["encoding"] == "base64" {
                restored.extend(base64::engine::general_purpose::STANDARD.decode(content).unwrap());
            } else {
                assert_eq!(chunk["encoding"], "utf8");
                restored.extend_from_slice(content.as_bytes());
            }
        }
        pages += 1; assert!(pages < 128);
        if pages == 1 {
            assert_eq!(page["coverage"]["complete"], false);
            assert!(restored.len() < raw.len(), "a one-page budget cannot claim complete evidence");
            assert!(!page["next_request"].is_null());
        }
        if page["next_request"].is_null() { break; }
        let cursor = page["next_request"]["cursor"].as_str().unwrap();
        request = serde_json::json!({"reference":format!("tool://{evidence_id}"),"cursor":cursor,"limit":16});
    }
    assert!(pages > 1);
    assert!(restored == raw.as_bytes(), "restored content mismatch: recovered_bytes={} expected_bytes={} recovered_sha256={:x} expected_sha256={:x}", restored.len(), raw.len(), Sha256::digest(&restored), Sha256::digest(raw.as_bytes()));
    assert_eq!(format!("sha256:{:x}", Sha256::digest(&restored)), artifact.sha256);
    let wrong_input = serde_json::json!({"steps":[{"id":"batch","op":"batch","calls":[{"as":"different","tool":"read_file","input":{}}]}],"return":"$batch"}).to_string();
    let wrong=runtime::context_evidence::k02_fixture_orchestration_receipt(&raw,evidence_id,4000,true,Some(&wrong_input));
    let wrong_summary: serde_json::Value = serde_json::from_str(&wrong.summary[wrong.summary.find('{').unwrap()..]).unwrap();
    assert!(wrong_summary["result"][runtime_key].get("source_tool").is_none());
    let tiny=runtime::context_evidence::k02_fixture_orchestration_receipt(&raw,evidence_id,40,true,None);
    assert!(tiny.truncated && tiny.summary.contains("read_ref("));
    let transient=runtime::context_evidence::k02_fixture_orchestration_receipt(&raw,evidence_id,40,false,None);
    assert!(transient.truncated && !transient.summary.contains("read_ref("));
    // A consumer must not treat a partially scanned search as a complete fact.
    let recovered: serde_json::Value = serde_json::from_slice(&restored).unwrap();
    assert_eq!(recovered["result"][runtime_key]["output"]["scanComplete"], false);
}

#[test]
fn k02_default_batch_original_preview_survives_provider_wire() {
    use sha2::{Digest, Sha256};
    let raw_path = std::env::var_os("K02_DEFAULT_BATCH_RAW_FIXTURE").expect("private original raw fixture");
    let input_path = std::env::var_os("K02_DEFAULT_BATCH_INPUT_FIXTURE").expect("private original input fixture");
    let raw = std::fs::read_to_string(raw_path).unwrap();
    let input = std::fs::read_to_string(input_path).unwrap();
    assert_eq!(format!("{:x}", Sha256::digest(raw.as_bytes())), "9b269e0d11d4c35b80c17ad0199aa4190e0a4f6317463abdb2791d98edc085ce");
    let original: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let receipt = runtime::context_evidence::k02_fixture_orchestration_receipt(&raw, "tool-raw-k02-default", 12000, true, Some(&input));
    assert!(receipt.truncated && receipt.omitted_tokens > 0 && receipt.receipt_tokens <= 12000);
    let body: serde_json::Value = serde_json::from_str(&receipt.summary[receipt.summary.find('{').unwrap()..]).unwrap();
    assert_eq!(body["kind"], "orchestration_partial_receipt");
    assert_eq!(body["complete"], false);
    assert_eq!(body["result"].as_object().unwrap().keys().cloned().collect::<Vec<_>>(), (0..6).map(|i| format!("read_file[{i}]")).collect::<Vec<_>>());
    for i in 0..6 {
        let key = format!("read_file[{i}]");
        let child = &body["result"][&key];
        let file = &child["output"]["file"];
        let source = &original["result"]["read_all"][&key]["file"];
        assert_eq!(child["source_tool"], "read_file");
        let shown = file["content"].as_str().expect("actual sample has bounded content or preview");
        let original_text = source["content"].as_str().unwrap();
        assert!(original_text.starts_with(shown), "child {i} must be an original content prefix");
        if file["preview"] == true {
            assert!(shown.len() <= 4096);
            assert_eq!(original_text.as_bytes().get(shown.len()), Some(&b'\n'));
            assert_eq!(file["numLines"].as_u64(), Some(shown.split('\n').count() as u64));
            assert_eq!(file["sourceNumLines"], source["numLines"]);
            assert_eq!(file["source_omitted"], true);
            assert_eq!(file["content_omitted"], true);
            assert_eq!(child["output"]["truncated"], true);
        } else {
            assert!(shown == original_text, "child {i} must be complete if not marked preview");
        }
        assert_eq!(file["sha256"], source["sha256"]);
        assert_eq!(file["byteLength"], source["byteLength"]);
    }
    let provider_request = provider::MessageRequest {
        model: "fixture-model".into(), max_tokens: 32,
        messages: vec![provider::InputMessage { role: "assistant".into(), content: vec![provider::InputContentBlock::ToolUse { id: "fixture-default-call".into(), name: "tool_orchestrate".into(), input: serde_json::json!({"steps":[]}) }] },
            provider::InputMessage::user_tool_result("fixture-default-call", receipt.summary.clone(), false)],
        ..Default::default()
    };
    let wire = provider::OpenAiCompatClient::new("fixture-key", provider::OpenAiCompatConfig::openai()).wire_request(&provider_request).unwrap();
    let delivered = wire.body["messages"].as_array().unwrap().iter().find(|m| m["tool_call_id"] == "fixture-default-call").unwrap();
    assert!(delivered["content"].as_str() == Some(receipt.summary.as_str()), "provider wire must preserve the exact partial receipt");
    let wrong = input.replace("read_file", "grep_search");
    let rejected = runtime::context_evidence::k02_fixture_orchestration_receipt(&raw, "tool-raw-k02-default", 12000, true, Some(&wrong));
    assert!(rejected.truncated && rejected.summary.contains("Partial orchestration output omitted"));
    assert!(!rejected.summary.contains("orchestration_partial_receipt"));
}

#[test]
fn gb2_reason_ref_refusal_teaches_the_real_required_field() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .expect("runtime tools");
    let executor = GatewayToolExecutor::new(None, false, registry);
    let invalid = json!({"task_ref":"task-1", "decision":"accept", "reason":"done", "reason_ref":"artifact://other"});
    let lease = executor.tool_host.pin_snapshot();
    let original = lease.validate_input("task_review", &invalid).expect_err("unknown field must refuse");
    let old = executor.input_contract_error("task_review", original.to_string());
    let old_failure = old.failure().expect("old structured failure");
    let error = executor.validate_tool_input("task_review", &invalid.to_string()).expect_err("no field may be silently dropped");
    let failure = error.failure().expect("structured failure");
    assert_eq!(failure.class, old_failure.class);
    assert_eq!(failure.schema_hash, old_failure.schema_hash);
    assert_eq!(failure.allowed_fields, old_failure.allowed_fields);
    assert!(failure.message.starts_with(&old_failure.message), "{}", failure.message);
    assert!(failure.message.contains("task_review requires a string `reason`"), "{}", failure.message);
    assert!(failure.message.contains("`reason_ref` is not a task_review field"), "{}", failure.message);
    let fixed = json!({"task_ref":"task-1", "decision":"accept", "reason":"done"});
    assert!(executor.validate_tool_input("task_review", &fixed.to_string()).is_ok());
    let missing = json!({"task_ref":"task-1", "decision":"accept", "reason_ref":"artifact://other"});
    let missing_error = executor.validate_tool_input("task_review", &missing.to_string()).expect_err("missing reason must be refused").to_string();
    assert!(missing_error.contains("missing required field `reason`"), "{missing_error}");
    assert!(missing_error.contains("task_review requires a string `reason`"), "{missing_error}");
    let repeated = executor.validate_tool_input("task_review", &invalid.to_string()).expect_err("repeat remains refused");
    assert_eq!(error.failure().unwrap().message, repeated.failure().unwrap().message);
}

#[test]
fn gb2_orchestrate_top_level_id_refuses_and_names_the_step_path() {
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
    let invalid = json!({"id":"wrong", "steps":[{"id":"read", "op":"call", "tool":"read_file", "input":{"path":"a"}}]});
    let error = executor.validate_tool_input("tool_orchestrate", &invalid.to_string()).expect_err("extra field must be refused");
    let message = &error.failure().unwrap().message;
    assert!(message.contains("unexpected field `id`"), "{message}");
    assert!(message.contains("each step requires its own string `id`"), "{message}");
    assert!(message.contains("Schema-valid structural example"), "{message}");
    for extra in [json!({"id":"wrong", "op":"batch", "steps":[{"id":"read", "op":"call", "tool":"read_file", "input":{"path":"a"}}]}),
                  json!({"id":"wrong", "op":"call", "item":{}, "steps":[{"id":"read", "op":"call", "tool":"read_file", "input":{"path":"a"}}]})] {
        let err = executor.validate_tool_input("tool_orchestrate", &extra.to_string()).unwrap_err();
        let msg = &err.failure().unwrap().message;
        assert!(msg.contains("unexpected field `id`"), "{msg}");
        assert!(msg.contains("`op`"), "{msg}");
        if extra.get("item").is_some() { assert!(msg.contains("`item`"), "{msg}"); }
    }
    let corrected = json!({"steps":[{"id":"read", "op":"call", "tool":"read_file", "input":{"path":"a"}}]});
    assert!(executor.validate_tool_input("tool_orchestrate", &corrected.to_string()).is_ok());
}

#[test]
fn gb2_membership_reason_refusal_preserves_the_original_field() {
    let registry = GatewayToolRegistry::builtin()
        .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions()).unwrap();
    let executor = GatewayToolExecutor::new(None, false, registry);
    let invalid = json!({"agent_ref":"agent:test", "team_ref":"team:test", "operation":"join", "reason":"why"});
    let error = executor.validate_tool_input("membership_update", &invalid.to_string()).unwrap_err();
    let message = &error.failure().unwrap().message;
    assert!(message.contains("unexpected field `reason`"), "{message}");
    assert!(message.contains("Do not automatically convert reason text"), "{message}");
    let corrected = json!({"agent_ref":"agent:test", "team_ref":"team:test", "operation":"join"});
    assert!(executor.validate_tool_input("membership_update", &corrected.to_string()).is_ok());
}

/// A repeated read must still carry the persistent envelope.
///
/// The second ask of the same immutable reference was answered with a receipt
/// that carried no bytes at all: kind, available, chunks, bytes and chunk_bytes
/// were all missing. read_body_seal cannot derive a seal from such an answer,
/// so every review that needed that evidence was refused - and a refusal with
/// no readable source is not a verdict on the work.
///
/// Nothing is withheld by design: the receipt names how to get the content
/// again. So the repeat has to carry the same persistent envelope fields the
/// first answer carried.
#[tokio::test]
async fn a_repeated_read_still_carries_the_persistent_envelope() {
    let services = runtime::RuntimeServices::in_memory().unwrap();
    let session_id = format!(
        "repeat-read-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let artifact = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: format!("session:{session_id}"),
                expected_bytes: None,
                original_name: None,
            },
            b"durable bytes that a repeated read must still carry",
        )
        .await
        .unwrap();
    let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin())
        .with_runtime_session_id(session_id.clone());
    executor.bind_runtime_services(&services).unwrap();
    let input = UnifiedReadRequest {
        reference: artifact.selector.clone(),
        cursor: None,
        limit: Some(64),
    };
    let binding = || RuntimeToolExecutionBinding {
        action_invocation: None,
        tool_lease: None,
        action_id: None,
        session_id: Some(session_id.as_str()),
        authorized_scopes: &[],
        memory_context: None,
        reality_context: None,
        model_lease: None,
        parent_execution: None,
        parent_execution_attempt: None,
        execution_decision: None,
        permission_ceiling: harness_contract::policy::PermissionMode::WorkspaceWrite,
    };
    let first: serde_json::Value = serde_json::from_str(
        &executor
            .execute_unified_read(input.clone(), binding())
            .await
            .unwrap(),
    )
    .unwrap();
    let second: serde_json::Value = serde_json::from_str(
        &executor.execute_unified_read(input.clone(), binding()).await.unwrap(),
    )
    .unwrap();
    assert_eq!(second["kind"], "evidence_retrieve", "second answer: {second}");
    assert_eq!(second["available"], true, "second answer: {second}");
    assert_eq!(second["chunks"], first["chunks"], "second answer: {second}");
    assert_eq!(second["bytes"], first["bytes"], "second answer: {second}");
    assert_eq!(second["chunk_bytes"], first["chunk_bytes"], "second answer: {second}");
}
