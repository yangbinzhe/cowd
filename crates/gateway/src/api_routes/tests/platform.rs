// Legacy API behavior shard; included into one shared test scope.
    #[tokio::test]
    async fn cross_plane_execute_persists_surface_dispatch_target_snapshot() {
        let app = api_router(test_state_with_config_and_runtime(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("message.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-receipt-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "dry_run",
            "idempotency_key": format!("idem-dispatch-receipt-{suffix}"),
            "action": {
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/demo-chat",
                "resource_ref": "text://receipt payload",
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["ready"],
            true
        );
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["session_key"],
            "feishu:demo-chat"
        );

        let executions = app
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/action/executions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let executions_body = to_bytes(executions.into_body(), usize::MAX).await.unwrap();
        let executions_json: serde_json::Value = serde_json::from_slice(&executions_body).unwrap();
        assert!(executions_json["executions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|receipt| receipt["dispatch_target"]["session_key"] == "feishu:demo-chat"));
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_reports_surface_unavailable_without_sidecar() {
        let app = api_router(test_state_with_config_and_runtime(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("message.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-live-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "commit",
            "idempotency_key": format!("idem-dispatch-live-{suffix}"),
            "action": {
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/live-chat",
                "resource_ref": "text://live payload",
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();

        assert_eq!(executed_json["status"], "blocked");
        assert_eq!(executed_json["dispatch_status"], "adapter_unavailable");
        assert_eq!(executed_json["dispatched"], false);
        assert!(executed_json["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| {
                value
                    .as_str()
                    .is_some_and(|value| value.starts_with("adapter:feishu:send_text:not_bound"))
            }));
        assert!(executed_json["execution_graph"].is_null());
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_resolves_image_target_but_requires_surface_sidecar() {
        let app = api_router(test_state_with_config_and_runtime(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("message.feishu.send_image.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-image-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "commit",
            "idempotency_key": format!("idem-dispatch-image-{suffix}"),
            "action": {
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/live-chat",
                "resource_ref": "image://https://example.test/panel.png",
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();

        assert_eq!(executed_json["status"], "blocked");
        assert_eq!(executed_json["dispatch_status"], "adapter_unavailable");
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["outbound_message"]
                ["payload_kind"],
            "image"
        );
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_resolves_workspace_file_target_but_requires_surface_sidecar(
    ) {
        let root = test_temp_dir("cross-plane-file-dispatch");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(workspace.join("reports")).unwrap();
        let report_path = workspace.join("reports").join("panel.txt");
        std::fs::write(&report_path, "dispatchable report").unwrap();
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
            workspace.clone(),
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("message.feishu.send_file.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-file-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "commit",
            "idempotency_key": format!("idem-dispatch-file-{suffix}"),
            "action": {
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/live-chat",
                "resource_ref": "file://reports/panel.txt",
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();

        assert_eq!(executed_json["status"], "blocked");
        assert_eq!(executed_json["dispatch_status"], "adapter_unavailable");
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["outbound_message"]
                ["payload_kind"],
            "file"
        );
        assert_eq!(
            executed_json["execution_receipt"]["dispatch_target"]["outbound_message"]
                ["payload_ref"],
            "reports/panel.txt"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_blocks_file_outside_workspace() {
        let root = test_temp_dir("cross-plane-file-block");
        let workspace = root.join("workspace");
        let outside = root.join("outside.txt");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(&outside, "must not send").unwrap();
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
            workspace,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("message.feishu.send_file.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-file-block-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "commit",
            "idempotency_key": format!("idem-dispatch-file-block-{suffix}"),
            "action": {
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/source",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": "channel://feishu/chat/live-chat",
                "resource_ref": format!("file://{}", outside.display()),
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });
        let executed = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let executed_body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let executed_json: serde_json::Value = serde_json::from_slice(&executed_body).unwrap();

        assert_eq!(executed_json["status"], "blocked");
        assert_eq!(executed_json["dispatch_status"], "payload_rejected");
        assert!(executed_json["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| blocker
                .as_str()
                .unwrap_or_default()
                .contains("payload_blocked")));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn context_current_returns_degraded_envelope_without_memory() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=ship&session_id=session-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["envelope"]["identity"]["session_id"], "session-1");
        assert_eq!(json["envelope"]["intent"], "ship");
        assert_eq!(
            json["envelope"]["assembled"]["stable_head"][0],
            "cowd-context-runtime:v0.8.13"
        );
        assert_eq!(
            json["envelope"]["diagnostics"]["degraded_sources"][0],
            "Memory"
        );
        assert_eq!(json["lean_probe"]["envelope_id"], json["envelope"]["id"]);
        assert_eq!(json["lean_probe"]["pressure_level"], "Nominal");
        assert_eq!(json["lean_probe"]["degradation_path"], "SourceFallback");
        assert_eq!(json["policy_decision"]["action"], "PreferOrientationPacket");
        assert_eq!(
            json["policy_decision"]["stable_head_hash"],
            json["lean_probe"]["stable_head_hash"]
        );
        assert_eq!(json["cache_stability"]["stable_head_reusable"], true);
        assert_eq!(
            json["snapshot"]["stable_head_hash"],
            json["lean_probe"]["stable_head_hash"]
        );
        assert_eq!(
            json["budget_explanation"]["total_tokens"],
            json["envelope"]["budget"]["total_tokens"]
        );
        assert_eq!(json["mode_coverage"]["all_profiles_covered"], true);
        assert_eq!(json["mode_coverage"]["all_stable_heads_reusable"], true);
        assert_eq!(
            json["mode_coverage"]["entries"].as_array().unwrap().len(),
            11
        );
        let profiles = json["mode_coverage"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|entry| entry["profile"].as_str())
            .collect::<Vec<_>>();
        assert!(profiles.contains(&"SurfaceQuickReply"));
        assert!(profiles.contains(&"SurfaceTaskIntake"));
        assert!(profiles.contains(&"DeepInvestigation"));
    }

    #[tokio::test]
    async fn context_current_accepts_profile_query_for_synthetic_envelope() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=ship&session_id=session-1&profile=yolo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["envelope"]["profile"], "YoloGoal");
        assert_eq!(json["envelope"]["identity"]["mode"], "YoloGoal");
        assert_eq!(json["envelope"]["selected"][0]["source"], "Task");
        assert_eq!(json["envelope"]["selected"][0]["role"], "TaskState");
        assert!(json["envelope"]["assembled"]["runtime_header"][0]
            .as_str()
            .unwrap()
            .contains("profile:YoloGoal"));
        assert!(json["envelope"]["assembled"]["runtime_header"][0]
            .as_str()
            .unwrap()
            .contains("mode:YoloGoal"));
        assert!(json["mode_coverage"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["profile"] == "SubAgent" && entry["mode"] == "SubAgent"));
    }

    #[tokio::test]
    async fn context_current_can_project_agent_view() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=review&session_id=session-1&agent_id=reviewer&agent_task=review%20the%20plan")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["agent_view"]["child_agent_id"], "reviewer");
        assert_eq!(json["agent_view"]["parent_agent_id"], "primary");
        assert_eq!(json["agent_view"]["envelope"]["profile"], "SubAgent");
        assert_eq!(
            json["agent_view"]["envelope"]["diagnostics"]["stable_head_hash"],
            json["envelope"]["diagnostics"]["stable_head_hash"]
        );
    }

    #[tokio::test]
    async fn context_current_injects_connector_resource_refs_without_resource_body() {
        let workspace = unique_test_workspace("context-resource");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let request = serde_json::json!({
            "tool_id": "service.local.docs.read",
            "resource_id": "context-doc",
            "title": "Context Resource Plan",
            "mode": "dry_run",
            "idempotency_key": format!("context-resource-{}", uuid::Uuid::new_v4())
        });
        let execute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=Context&session_id=session-resource")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let selected = json["envelope"]["selected"].as_array().unwrap();
        let resource_item = selected
            .iter()
            .find(|item| item["id"] == "service://local.docs/document/context-doc")
            .expect("resource context item should be selected");
        assert_eq!(resource_item["source"], "Workspace");
        assert_eq!(resource_item["role"], "Evidence");
        assert!(resource_item["content"]
            .as_str()
            .unwrap()
            .contains("indexed_state: unknown"));
        assert!(!resource_item["content"]
            .as_str()
            .unwrap()
            .contains("Mock document"));
        assert_eq!(
            resource_item["evidence"][0],
            "service://local.docs/document/context-doc"
        );
    }

    #[tokio::test]
    async fn evidence_resolver_returns_connector_resource_metadata_only() {
        let workspace = unique_test_workspace("resource-evidence");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let request = serde_json::json!({
            "tool_id": "service.local.docs.read",
            "resource_id": "evidence-doc",
            "title": "Evidence Resource",
            "mode": "dry_run",
            "idempotency_key": format!("resource-evidence-{}", uuid::Uuid::new_v4())
        });
        let execute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/evidence/resolve?ref=service%3A%2F%2Flocal.docs%2Fdocument%2Fevidence-doc")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "resource");
        assert_eq!(json["available"], true);
        assert_eq!(json["resource"]["title"], "Evidence Resource");
        assert_eq!(json["body"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn runtime_timeline_preserves_runtime_run_context_refs() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-context-ref-timeline";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_session_domain_event(&session::SessionDomainEvent::new(
                session_id,
                0,
                session::SessionDomainScope::Message,
                "message_appended",
                serde_json::json!({
                    "type": "message_appended",
                    "sequence": 0,
                    "role": "user"
                }),
                10,
            ))
            .await
            .unwrap();
        store
            .append_event(&session::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(
                    session_id,
                    "ctx-runtime-timeline",
                    "timeline linked context",
                )
                .to_string(),
                sequence: 1,
                created_at_ms: 11,
            })
            .await
            .unwrap();
        let mut runtime_run_event = session::SessionDomainEvent::new(
            session_id,
            2,
            session::SessionDomainScope::Turn,
            "RuntimeRun",
            message_routes::runtime_run_completed_payload(
                session_id,
                "run-runtime-timeline",
                None,
                ContextProfile::MainTurn,
                "completed",
                Some(1),
                Some("ctx-runtime-timeline".to_string()),
                None,
                20,
                30,
            ),
            12,
        );
        runtime_run_event.status = Some("completed".to_string());
        runtime_run_event.refs = vec![session::SessionDomainRef {
            ref_type: "context_envelope".to_string(),
            id: "ctx-runtime-timeline".to_string(),
            label: None,
        }];
        store
            .append_session_domain_event(&runtime_run_event)
            .await
            .unwrap();
        store
            .append_session_domain_event(&session::SessionDomainEvent::new(
                session_id,
                3,
                session::SessionDomainScope::Policy,
                "runtime.policy.decided",
                serde_json::json!({
                    "agent_mode": "Solo",
                    "requires_review": false,
                    "complexity": {"level": "Simple", "score": 30}
                }),
                13,
            ))
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let timeline_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(timeline_response.status(), StatusCode::OK);
        let timeline_body = to_bytes(timeline_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let timeline: serde_json::Value = serde_json::from_slice(&timeline_body).unwrap();
        assert_eq!(timeline["total"], 3);
        assert_eq!(timeline["events"][0]["kind"], "message_appended");
        let runtime_run = timeline["events"]
            .as_array()
            .expect("timeline events")
            .iter()
            .find(|event| event["kind"] == "RuntimeRun")
            .expect("runtime run projection");
        assert_eq!(runtime_run["status"], "completed");
        assert_eq!(runtime_run["refs"][0]["type"], "context_envelope");
        assert_eq!(runtime_run["refs"][0]["id"], "ctx-runtime-timeline");
        assert_eq!(
            timeline["health_summary"]["latest_policy"]["agent_mode"],
            "Solo"
        );
        assert_eq!(timeline["health_summary"]["scope_counts"]["turn"], 1);
        assert_eq!(timeline["health_summary"]["scope_counts"]["message"], 1);
        assert_eq!(timeline["health_summary"]["scope_counts"]["policy"], 1);

        let detail_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/ctx-runtime-timeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail_response.status(), StatusCode::OK);
        let detail_body = to_bytes(detail_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let detail_json: serde_json::Value = serde_json::from_slice(&detail_body).unwrap();
        assert_eq!(
            detail_json["context"]["envelope"]["intent"],
            "timeline linked context"
        );
    }

    #[tokio::test]
    async fn evidence_resolver_reads_tool_events_by_ref() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "evidence-tool-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(Arc::clone(&store));
        let raw = "tests passed";
        let artifacts = state
            .services
            .artifact_store()
            .expect("test artifact store");
        let artifact = artifacts
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".to_string(),
                    visibility_scope: format!("session:{session_id}"),
                    expected_bytes: Some(raw.len() as u64),
                    original_name: Some("tool-1.raw".to_string()),
                },
                raw.as_bytes(),
            )
            .await
            .expect("persist test artifact");
        store
            .append_session_domain_event_allocating_sequence(&session::SessionDomainEvent::new(
                session_id,
                0,
                session::SessionDomainScope::Tool,
                "evidence.raw.persisted",
                serde_json::json!({
                    "type": "RawEvidence",
                    "evidence_id": "tool-1",
                    "tool_name": "bash",
                    "artifact_selector": artifact.selector.clone(),
                    "content_hash": artifact.sha256.clone(),
                    "byte_count": artifact.bytes,
                    "media_type": "text/plain",
                    "visibility_scope": format!("session:{session_id}"),
                }),
                1,
            ))
            .await
            .unwrap();
        let evidence_ref = harness_contract::reality::EvidenceRef::observed("tool", "tool-1");
        let projection = harness_contract::context::EvidenceAuditProjection {
            evidence_ref: evidence_ref.clone(),
            content_kind: harness_contract::context::EvidenceContentKind::Text,
            raw_tokens: 3,
            receipt_tokens: 1,
            omitted_tokens: 2,
            raw_available: true,
            access: Some(harness_contract::context::EvidenceAccessRef::durable(
                evidence_ref,
                artifact.sha256,
                artifact.bytes,
                "text/plain",
                artifact.selector,
                format!("session:{session_id}"),
            )),
        };
        store
            .append_session_domain_event_allocating_sequence(&session::SessionDomainEvent::new(
                session_id,
                0,
                session::SessionDomainScope::Context,
                "context.turn_report",
                serde_json::json!({
                    "type": "ContextTurnReport",
                    "report": {
                        "turn_id": "tool-evidence-turn",
                        "audit_projections": [projection],
                    }
                }),
                2,
            ))
            .await
            .unwrap();

        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/evidence/resolve?session_id={session_id}&ref=tool%3A%2F%2Ftool-1%2Fevidence%2Fevent-1"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["available"], true);
        assert_eq!(json["kind"], "tool");
        assert_eq!(json["verified"], true);
        assert_eq!(json["artifact"]["snippet"], "tests passed");
    }

    #[tokio::test]
    async fn evidence_resolver_rejects_unsupported_refs() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/evidence/resolve?ref=unknown%3A%2F%2Fvalue")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn task_api_starts_reports_and_blocks_after_repeated_failures() {
        let state = test_state();
        let source_session_id = "session-task-failure";
        publish_test_session_policy(&state.services, source_session_id);
        let mission_id = state
            .services
            .task
            .workspace_default_mission_id()
            .expect("Runtime-backed TaskService");
        let app = api_router(state);
        let start_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "task_id": "task-failure",
                            "mission_id": mission_id,
                            "origin_session_id": source_session_id,
                            "origin_turn_id": "turn-task-failure",
                            "objective": "finish v0.8.10"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(start_response.status(), StatusCode::CREATED);
        let start_body = to_bytes(start_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let started: serde_json::Value = serde_json::from_slice(&start_body).unwrap();
        let task_id = started["task_id"].as_str().expect("task id").to_string();
        let mut expected_revision = started["revision"].as_u64().expect("task revision");
        assert_eq!(started["status"], "running");
        assert!(started["execution_policy"]["binding"].is_object());
        assert_eq!(started["execution_policy"]["continuation"], "standard");

        for reason in ["first", "second", "external input required"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/tasks/{task_id}/failure"))
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({
                                "expected_revision": expected_revision,
                                "reason": reason
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let task: serde_json::Value = serde_json::from_slice(&body).unwrap();
            expected_revision = task["revision"].as_u64().expect("updated task revision");
        }

        let status_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tasks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
        assert_eq!(status_json["tasks"][0]["status"], "blocked");
        assert_eq!(
            status_json["tasks"][0]["blocker_reason"],
            "external input required"
        );
    }

    #[tokio::test]
    async fn task_api_records_phase_artifacts_and_review() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let state = test_state_with_store(store);
        let source_session_id = "session-task-phase";
        publish_test_session_policy(&state.services, source_session_id);
        let mission_id = state
            .services
            .task
            .workspace_default_mission_id()
            .expect("Runtime-backed TaskService");
        let app = api_router(state);
        let start_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "task_id": "task-phase",
                            "mission_id": mission_id,
                            "origin_session_id": source_session_id,
                            "origin_turn_id": "turn-task-phase",
                            "objective": "ship task phase"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(start_response.status(), StatusCode::CREATED);
        let start_body = to_bytes(start_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let started: serde_json::Value = serde_json::from_slice(&start_body).unwrap();
        let task_id = started["task_id"].as_str().unwrap().to_string();
        let start_revision = started["revision"].as_u64().expect("task revision");
        assert_eq!(
            started["command_receipt"]["accepted_revision"],
            start_revision
        );
        assert_eq!(started["command_receipt"]["task_id"], task_id);
        assert!(started["command_receipt"]["outbox_id"]
            .as_str()
            .is_some_and(|outbox_id| !outbox_id.is_empty()));

        let phase_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/phases"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "expected_revision": start_revision,
                            "name": "browser-e2e",
                            "objective": "cover WebUI task panel",
                            "plan": ["add playwright spec"],
                            "acceptance": ["2 e2e tests pass"],
                            "test_commands": ["cargo test -p gateway runtime_task -- --nocapture"],
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(phase_response.status(), StatusCode::CREATED);
        let phase_body = to_bytes(phase_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let phase_json: serde_json::Value = serde_json::from_slice(&phase_body).unwrap();
        let phase_id = phase_json["phases"].as_array().unwrap().last().unwrap()["phase_id"]
            .as_str()
            .unwrap()
            .to_string();
        let phase_revision = phase_json["revision"]
            .as_u64()
            .expect("phase task revision");
        assert_eq!(phase_json["current_phase_id"], phase_id);

        let artifact_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/phases/{phase_id}/artifacts"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "expected_revision": phase_revision,
                            "kind": "test",
                            "label": "playwright",
                            "value": "2 passed",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(artifact_response.status(), StatusCode::OK);
        let artifact_body = to_bytes(artifact_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let artifact_json: serde_json::Value = serde_json::from_slice(&artifact_body).unwrap();
        let artifact_revision = artifact_json["revision"]
            .as_u64()
            .expect("artifact task revision");

        let review_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/phases/{phase_id}/review"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "expected_revision": artifact_revision,
                            "result": "accepted",
                            "completed": true,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(review_response.status(), StatusCode::OK);
        let review_body = to_bytes(review_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let review_json: serde_json::Value = serde_json::from_slice(&review_body).unwrap();
        let reviewed_phase = review_json["phases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|phase| phase["phase_id"] == phase_id)
            .unwrap();
        assert_eq!(reviewed_phase["status"], "completed");
        assert_eq!(reviewed_phase["review_result"], "accepted");
        assert_eq!(reviewed_phase["artifacts"][0]["label"], "playwright");

        let timeline_response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={source_session_id}&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(timeline_response.status(), StatusCode::OK);
        let timeline_body = to_bytes(timeline_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let timeline_json: serde_json::Value = serde_json::from_slice(&timeline_body).unwrap();
        let kinds = timeline_json["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["scope"] == "task")
            .map(|event| event["kind"].as_str().unwrap_or_default())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                "task.created",
                "task.phase.started",
                "task.phase.artifact.recorded",
                "task.phase.reviewed",
            ]
        );
        let reviewed = timeline_json["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["kind"] == "task.phase.reviewed")
            .expect("reviewed task event");
        assert_eq!(reviewed["payload"]["status"], "reviewing");
    }

    #[tokio::test]
    async fn memory_without_config_returns_disabled() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["status"], "disabled");
        assert_eq!(json["context_health"]["level"], "unavailable");
        assert_eq!(json["kernel_health"]["degraded"], true);
        assert_eq!(
            json["kernel_health"]["degraded_reasons"][0],
            "memory not configured"
        );
    }

    #[tokio::test]
    async fn memory_maintenance_without_config_degrades() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/maintenance")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["degraded_reason"], "memory not configured");
        assert!(json["candidates"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn memory_maintenance_scan_and_transition() {
        let dir =
            std::env::temp_dir().join(format!("cowd-api-maintenance-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&dir.join("memory.db")))
                .await
                .unwrap(),
        );
        let id = MemoryId::new_v4();
        manager
            .remember(MemoryEntry {
                id,
                layer: MemoryLayer::L2,
                category: MemoryCategory::Reference,
                priority: Priority::Normal,
                source: MemorySource::UserExplicit,
                title: "Old context rule".to_string(),
                content: "Prefer bounded context packets".to_string(),
                embedding: None,
                tags: vec![],
                relations: vec![],
                confidence: 0.7,
                access_count: 0,
                staleness: 0.95,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                last_accessed_at: None,
                scope: MemoryScope::Session("maintenance-test".to_string()),
                session_id: None,
                source_agent: None,
                visibility: AgentVisibility::Shared,
            })
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let scan_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/maintenance")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"stale_threshold":0.9}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scan_response.status(), StatusCode::OK);
        let scan_body = to_bytes(scan_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let scan_json: serde_json::Value = serde_json::from_slice(&scan_body).unwrap();
        let candidate_id = scan_json["candidates"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(scan_json["candidates"][0]["kind"], "stale");

        let ack_response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/memory/maintenance/{candidate_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"status":"acknowledged"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ack_response.status(), StatusCode::OK);
        let ack_body = to_bytes(ack_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let ack_json: serde_json::Value = serde_json::from_slice(&ack_body).unwrap();
        assert_eq!(ack_json["candidate"]["status"], "acknowledged");
    }

    #[tokio::test]
    async fn memory_maintenance_rejects_invalid_status_filter() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/maintenance?status=unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], false);
        assert_eq!(json["degraded_reason"], "memory not configured");

        let dir = std::env::temp_dir().join(format!(
            "cowd-api-maintenance-invalid-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&dir.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/maintenance?status=unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn memory_recall_explain_reports_source_mode_and_score() {
        let tmp = std::env::temp_dir().join(format!("cowd-api-memory-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::ProjectKnowledge,
                "SessionRepository migration",
                "SessionRepository owns durable sessions and task phase review evidence.",
                Priority::High,
                vec!["session".into(), "task".into()],
                MemoryScope::Project("api-test".to_string()),
            )
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/recall/explain?q=SessionRepository&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["query"], "SessionRepository");
        assert_eq!(json["degraded"], false);
        assert_eq!(json["results"][0]["source_layer"], "L3");
        assert_eq!(json["results"][0]["category"], "ProjectKnowledge");
        assert!(json["results"][0]["score"].as_f64().is_some());
        assert!(json["results"][0]["mode"].as_str().is_some());
        assert!(json["results"][0]["snippet"]
            .as_str()
            .unwrap_or_default()
            .contains("SessionRepository"));

        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[tokio::test]
    async fn memory_packet_returns_explainable_packet() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-memory-packet-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let entry = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L2,
            category: MemoryCategory::ProjectKnowledge,
            priority: Priority::High,
            source: MemorySource::UserExplicit,
            title: "PACKET_API_ALPHA".to_string(),
            content: "PACKET_API_ALPHA should appear in an explainable packet.".to_string(),
            embedding: None,
            tags: vec!["packet".to_string()],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Session("api-memory-packet".to_string()),
            session_id: Some("api-memory-packet".to_string()),
            source_agent: Some("api".to_string()),
            visibility: AgentVisibility::Shared,
        };
        manager.remember(entry).await.unwrap();

        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/packet?q=PACKET_API_ALPHA&max_items=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert!(json["packet"]["selected"].as_array().unwrap().len() <= 5);

        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[tokio::test]
    async fn memory_links_returns_kernel_links() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-memory-links-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let target_id = manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::Reference,
                "Link Target",
                "target",
                Priority::Normal,
                vec!["api-link".to_string()],
                MemoryScope::Global,
            )
            .await
            .unwrap();
        let source = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::UserExplicit,
            title: "Link Source".to_string(),
            content: "source".to_string(),
            embedding: None,
            tags: vec![],
            relations: vec![memory::Relation {
                target_id,
                kind: memory::RelationKind::DependsOn,
                strength: 0.8,
                temporal: None,
                entity: None,
            }],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Global,
            session_id: None,
            source_agent: None,
            visibility: AgentVisibility::Shared,
        };
        manager.remember(source).await.unwrap();

        let app = api_router(test_state_with_memory(manager));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/links")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["total"].as_u64().unwrap() >= 1);

        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[tokio::test]
    async fn memory_layers_and_entries_read_real_store() {
        let tmp = std::env::temp_dir().join(format!("cowd-api-memory-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::Shared,
                "Durable Decision Candidate",
                "Use SessionRepository as the source of truth for v0.8.10.",
                Priority::High,
                vec!["team_relevant".into()],
                MemoryScope::Global,
            )
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let status_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
        assert_eq!(status_json["enabled"], true);
        assert_eq!(status_json["status"], "ready");
        assert_eq!(status_json["context_health"]["level"], "healthy");
        assert_eq!(status_json["kernel_health"]["degraded"], false);
        assert_eq!(status_json["kernel_health"]["stale_pressure"], 0.0);
        assert!(status_json["kernel_health"]["evidence_coverage"]
            .as_f64()
            .is_some());

        let layers_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/layers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(layers_response.status(), StatusCode::OK);
        let layers_body = to_bytes(layers_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let layers_json: serde_json::Value = serde_json::from_slice(&layers_body).unwrap();
        let l3_count = layers_json["layers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|layer| layer["layer"] == "L3")
            .and_then(|layer| layer["entry_count"].as_u64())
            .unwrap_or_default();
        assert_eq!(l3_count, 1);

        let entries_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(entries_response.status(), StatusCode::OK);
        let entries_body = to_bytes(entries_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let entries_json: serde_json::Value = serde_json::from_slice(&entries_body).unwrap();
        assert_eq!(entries_json["entries"].as_array().unwrap().len(), 1);
        assert_eq!(
            entries_json["entries"][0]["title"],
            "Durable Decision Candidate"
        );
        let entry_id = entries_json["entries"][0]["id"].as_str().unwrap();
        let archive_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/memory/L3/{entry_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(archive_response.status(), StatusCode::NO_CONTENT);

        let active_entries_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let active_entries_body = to_bytes(active_entries_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let active_entries_json: serde_json::Value =
            serde_json::from_slice(&active_entries_body).unwrap();
        assert!(active_entries_json["entries"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(active_entries_json["archived_count"], 1);

        let retained_entries_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L3?include_archived=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let retained_entries_body = to_bytes(retained_entries_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let retained_entries_json: serde_json::Value =
            serde_json::from_slice(&retained_entries_body).unwrap();
        assert_eq!(
            retained_entries_json["entries"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            retained_entries_json["entries"][0]["lifecycle_state"],
            "archived"
        );

        let layers_after_archive = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/layers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let layers_after_archive = to_bytes(layers_after_archive.into_body(), usize::MAX)
            .await
            .unwrap();
        let layers_after_archive: serde_json::Value =
            serde_json::from_slice(&layers_after_archive).unwrap();
        let l3 = layers_after_archive["layers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|layer| layer["layer"] == "L3")
            .unwrap();
        assert_eq!(l3["entry_count"], 0);
        assert_eq!(l3["retained_count"], 1);
        assert_eq!(l3["archived_count"], 1);
        assert_eq!(l3["state"], "ready_empty");

        let l4_read = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L4")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(l4_read.status(), StatusCode::OK);

        let l4_write = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/L4")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"title":"bad","content":"bypass"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(l4_write.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn memory_entry_update_route_updates_real_store() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-memory-update-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory(manager));

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/L3")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "Update target",
                            "content": "original memory content",
                            "category": "Reference",
                            "priority": "Normal",
                            "tags": ["before"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let create_body = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let create_json: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
        let id = create_json["id"].as_str().unwrap();

        let update_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/memory/entry/{id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "content": "updated memory content",
                            "priority": "High",
                            "tags": ["after", "webui"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(update_response.status(), StatusCode::OK);

        let entries_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(entries_response.status(), StatusCode::OK);
        let entries_body = to_bytes(entries_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let entries_json: serde_json::Value = serde_json::from_slice(&entries_body).unwrap();
        let entry = entries_json["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == id)
            .unwrap();
        assert_eq!(entry["content"], "updated memory content");
        assert_eq!(entry["priority"], "High");
        assert_eq!(entry["tags"][0], "after");
    }

    #[tokio::test]
    async fn audit_export_includes_memory_write_audit() {
        let tmp = std::env::temp_dir().join(format!("cowd-api-audit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory(manager));

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/L3")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "audit-export-memory",
                            "content": "COWD_AUDIT_EXPORT_MEMORY_WRITE",
                            "category": "Reference",
                            "priority": "High",
                            "tags": ["audit", "e2e"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);

        let export_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/audit/export?source=memory&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(export_response.status(), StatusCode::OK);
        let body = to_bytes(export_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "audit_export");
        assert_eq!(json["source"], "memory");
        assert_eq!(json["totals"]["memory"], 1);
        assert_eq!(json["records"][0]["source"], "memory");
        assert_eq!(
            json["records"][0]["record"]["summary"],
            "COWD_AUDIT_EXPORT_MEMORY_WRITE"
        );
        assert_eq!(json["memory"][0]["operation"], "Create");
        assert_eq!(json["memory"][0]["layer"], "L3");

        let _ = std::fs::remove_dir_all(tmp);
    }

    #[tokio::test]
    async fn memory_symbol_links_roundtrip_real_store() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-symbol-links-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let memory_id = manager
            .create_entry(
                MemoryLayer::L3,
                MemoryCategory::Reference,
                "Auth impact note",
                "authenticate_user controls login policy and API auth behavior.",
                Priority::High,
                vec!["symbol".into(), "auth".into()],
                MemoryScope::Global,
            )
            .await
            .unwrap();

        let app = api_router(test_state_with_memory(manager));
        let link_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/memory/symbol-links")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "symbol_id": "src/auth.rs:authenticate_user:42",
                            "memory_id": memory_id.to_string(),
                            "turn_index": 7,
                            "reference_type": "impact"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(link_response.status(), StatusCode::CREATED);

        let lookup_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/symbol-links?symbol=authenticate_user")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lookup_response.status(), StatusCode::OK);
        let body = to_bytes(lookup_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 1);
        assert_eq!(json["entries"][0]["id"], memory_id.to_string());
        assert_eq!(json["entries"][0]["title"], "Auth impact note");
    }

    #[tokio::test]
    async fn config_returns_version() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn verify_auth_allows_no_auth_configuration() {
        let state = test_state();
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/verify")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn auth_required_when_token_set() {
        let sessions = Arc::new(ActiveSessionDirectory::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let session_repository = test_session_repository(sessions.clone(), None, event_bus.clone());
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_repository, None),
            session_lease_registry: None,
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn system_routes_stay_protected_when_auth_token_set() {
        let sessions = Arc::new(ActiveSessionDirectory::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let session_repository = test_session_repository(sessions.clone(), None, event_bus.clone());
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_repository, None),
            session_lease_registry: None,
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn same_origin_headers_do_not_bypass_bearer_authentication() {
        let sessions = Arc::new(ActiveSessionDirectory::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let session_repository = test_session_repository(sessions.clone(), None, event_bus.clone());
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_repository, None),
            session_lease_registry: None,
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools")
                    .header("sec-fetch-site", "same-origin")
                    .header("sec-fetch-dest", "empty")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cross_site_requests_still_require_bearer_auth() {
        let sessions = Arc::new(ActiveSessionDirectory::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let session_repository = test_session_repository(sessions.clone(), None, event_bus.clone());
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_repository, None),
            session_lease_registry: None,
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools")
                    .header("sec-fetch-site", "cross-site")
                    .header("sec-fetch-dest", "empty")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn profile_and_workspace_routes_stay_protected_when_auth_token_set() {
        let sessions = Arc::new(ActiveSessionDirectory::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let session_repository = test_session_repository(sessions.clone(), None, event_bus.clone());
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_repository, None),
            session_lease_registry: None,
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        });
        let app = api_router(state);

        for uri in [
            "/api/profiles",
            "/api/workspace",
            "/api/approval/pending",
            "/api/cross-plane/summary",
            "/api/message-connectors/wechat-ilink/accounts",
            "/api/memory/status",
            "/api/tasks",
            "/api/runtime/control-plane",
            "/api/context/current",
            "/api/evidence/resolve?ref=session%3A%2F%2Ftest",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn auth_passes_with_valid_token() {
        let sessions = Arc::new(ActiveSessionDirectory::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let session_repository = test_session_repository(sessions.clone(), None, event_bus.clone());
        let state = Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: Some("test-token".into()),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_repository, None),
            session_lease_registry: None,
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        });
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .header("Authorization", "Bearer test-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
