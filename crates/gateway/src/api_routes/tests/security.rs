// Legacy API behavior shard; included into one shared test scope.
    #[tokio::test]
    async fn runtime_control_plane_reports_degraded_kernel_without_store() {
        let root = test_temp_dir("runtime-control-plane-degraded");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_control_plane");
        assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(json["status"], "degraded");
        assert_eq!(json["degraded"], true);
        assert_eq!(json["components"]["session"]["durable_store"], false);
        assert_eq!(
            json["components"]["session"]["source_of_truth"],
            "unavailable"
        );
        assert_eq!(json["components"]["context"]["durable_history"], false);
        assert_eq!(json["components"]["memory"]["status"], "unavailable");
        assert_eq!(json["components"]["permissions"]["auth_required"], false);
        assert_eq!(
            json["components"]["session"]["leases"]["status"],
            "available"
        );
        assert_eq!(json["diagnostics"]["durable_session_store"], false);
        assert_eq!(json["diagnostics"]["memory_attached"], false);
        assert_eq!(
            json["diagnostics"]["stored_sessions"],
            serde_json::Value::Null
        );
        assert_eq!(json["diagnostics"]["component_count"], 10);
        assert_eq!(json["diagnostics"]["degraded_component_count"], 2);
        assert_eq!(json["diagnostics"]["attention_component_count"], 1);
        assert_eq!(
            json["diagnostics"]["capability_count"],
            serde_json::json!(
                11 + json["diagnostics"]["connector_capability_count"]
                    .as_u64()
                    .unwrap()
            )
        );
        assert!(json["diagnostics"]["elapsed_ms"].as_u64().is_some());
        assert!(matches!(
            json["diagnostics"]["performance_status"].as_str(),
            Some("healthy" | "attention" | "degraded")
        ));
        assert_eq!(json["diagnostics"]["provider_configured"], true);
        assert!(json["diagnostics"]["provider_count"].as_u64().unwrap_or(0) > 0);
        assert!(
            json["diagnostics"]["provider_model_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(json["diagnostics"]["configured_model_resolved"], true);
        assert_eq!(json["diagnostics"]["production_ready"], false);
        assert_control_plane_readiness_accounting(&json);
        assert!(json["readiness"]["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "session.durable_source_of_truth"));
        assert!(json["readiness"]["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "memory.manager"));
        assert!(json["readiness"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "provider.registry" && check["status"] == "ready"));
        assert!(json["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action
                .as_str()
                .unwrap_or_default()
                .contains("durable session store")));
        assert!(!json["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action
                    .as_str()
                    .unwrap_or_default()
                    .contains("runtime provider")
            }));
        assert!(json["degraded_reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason == "session store not available"));

        let _ = std::fs::remove_dir_all(root);
    }
    #[tokio::test]
    async fn session_mutations_require_the_exact_attached_writer_observer() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record("writer-contract", None))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let principal = test_human_principal();

        let missing = require_session_writer_admission(
            &state,
            &AuthenticatedPrincipal(principal.clone()),
            &HeaderMap::new(),
            "writer-contract",
        )
        .await
        .unwrap_err();
        assert_eq!(missing.0, StatusCode::FORBIDDEN);

        for (observer, role) in [("webui:reader", "reader"), ("webui:writer", "writer")] {
            let attached = state
                .services
                .session
                .attach_session_value(
                    "writer-contract",
                    &surface_actor_id(&AuthenticatedPrincipal(principal.clone()), observer),
                    "webui",
                    Some(role),
                )
                .await;
            assert_eq!(attached["ok"], true);
        }

        let headers = |observer: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-cowd-observer-id",
                observer.parse().expect("observer header"),
            );
            headers
        };
        let reader = require_session_writer_admission(
            &state,
            &AuthenticatedPrincipal(principal.clone()),
            &headers("webui:reader"),
            "writer-contract",
        )
        .await
        .unwrap_err();
        assert_eq!(reader.0, StatusCode::FORBIDDEN);

        let unknown = require_session_writer_admission(
            &state,
            &AuthenticatedPrincipal(principal.clone()),
            &headers("webui:unknown"),
            "writer-contract",
        )
        .await
        .unwrap_err();
        assert_eq!(unknown.0, StatusCode::FORBIDDEN);

        let owner = require_session_writer_admission(
            &state,
            &AuthenticatedPrincipal(principal),
            &headers("webui:writer"),
            "writer-contract",
        )
        .await
        .expect("exact writer observer admitted");
        assert!(owner.ends_with(":observer:webui:writer"));
    }

    #[tokio::test]
    async fn session_execution_policy_is_readable_but_revision_updates_require_the_writer() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "execution-policy-contract";
        store
            .create_session(&new_api_session_record(session_id, None))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let app = api_router(Arc::clone(&state));

        let read = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/execution-policy"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);
        let read: serde_json::Value =
            serde_json::from_slice(&to_bytes(read.into_body(), usize::MAX).await.unwrap()).unwrap();
        let revision = read["policy"]["revision"].as_u64().unwrap();

        let update_body = serde_json::json!({
            "preset": "yolo",
            "expected_revision": revision,
        })
        .to_string();
        let missing_writer = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/api/sessions/{session_id}/execution-policy"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(update_body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_writer.status(), StatusCode::FORBIDDEN);

        let observer_id = "webui:execution-policy";
        attach_test_writer(&state, session_id, observer_id).await;
        let updated = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/api/sessions/{session_id}/execution-policy"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", observer_id)
                    .body(Body::from(update_body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(updated.status(), StatusCode::OK);
        let updated: serde_json::Value =
            serde_json::from_slice(&to_bytes(updated.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(updated["matched_preset"], "yolo");
        assert_eq!(updated["policy"]["revision"], revision + 1);
        assert_eq!(updated["policy"]["permission_mode"], "danger-full-access");

        let stale_revision = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/api/sessions/{session_id}/execution-policy"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", observer_id)
                    .body(Body::from(update_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale_revision.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn runtime_session_lease_routes_share_runtime_host_registry_projection() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record("session-a", None))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        for (observer, role) in [
            ("tui:test", "writer"),
            ("tui:reader", "reader"),
            ("tui:other-writer", "writer"),
        ] {
            let attached = state
                .services
                .session
                .attach_session_value(
                    "session-a",
                    &format!("principal:local-human:surface:{observer}"),
                    "tui",
                    Some(role),
                )
                .await;
            assert_eq!(attached["ok"], true);
        }
        let app = api_router(state.clone());

        let acquire = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/session-leases/acquire")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", "tui:test")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": "session-a",
                            "mode": "exclusive"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(acquire.status(), StatusCode::OK);
        let body = to_bytes(acquire.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["owner"], "principal:local-human:observer:tui:test");
        assert_eq!(json["mode"], "exclusive");
        assert!(json["acquired_at_ms"].as_u64().is_some());

        let reader_acquire = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/session-leases/acquire")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", "tui:reader")
                    .body(Body::from(r#"{"session_id":"session-a"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reader_acquire.status(), StatusCode::FORBIDDEN);

        let reader_detach = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-a/detach")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", "tui:reader")
                    .body(Body::from(r#"{"surface":"tui"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reader_detach.status(), StatusCode::OK);
        let body = to_bytes(reader_detach.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        let lifecycle = state
            .services
            .session
            .lifecycle_snapshot_value(Some("session-a"))
            .await;
        let attachments = lifecycle["snapshot"]["attachments"].as_array().unwrap();
        assert_eq!(attachments.len(), 2);
        assert!(attachments.iter().all(|attachment| {
            attachment["actor"]["actor_id"] != "principal:local-human:surface:tui:reader"
        }));

        let spoofed_body_observer = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/session-leases/acquire")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", "tui:test")
                    .body(Body::from(
                        r#"{"session_id":"session-a","observer_id":"tui:other-writer"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(spoofed_body_observer.status().is_client_error());

        let unknown_session = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/session-leases/acquire")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", "tui:test")
                    .body(Body::from(r#"{"session_id":"session-unknown"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown_session.status(), StatusCode::NOT_FOUND);

        let list = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/session-leases")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);
        let body = to_bytes(list.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "available");
        assert_eq!(json["total"], 1);
        assert_eq!(json["leases"][0]["session_id"], "session-a");

        let control = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(control.status(), StatusCode::OK);
        let body = to_bytes(control.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["components"]["session"]["leases"]["attached"], true);
        assert_eq!(json["components"]["session"]["leases"]["total"], 1);
        assert_eq!(
            json["components"]["session"]["leases"]["leases"][0]["owner"],
            "principal:local-human:observer:tui:test"
        );

        let cross_tab_release = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/session-leases/release")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", "tui:other-writer")
                    .body(Body::from(r#"{"session_id":"session-a"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cross_tab_release.status(), StatusCode::CONFLICT);

        let release = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/session-leases/release")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", "tui:test")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": "session-a",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(release.status(), StatusCode::OK);
        let body = to_bytes(release.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["released"], true);
    }

    #[tokio::test]
    async fn runtime_control_plane_reports_durable_store_and_task_state() {
        let root = test_temp_dir("runtime-control-plane-durable");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let state = test_state_with_store_and_workspace(store, workspace, config_home);
        seed_test_task(
            &state.services,
            "control-plane-smoke-task",
            "control plane smoke task",
        );
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_control_plane");
        assert_eq!(json["status"], "attention");
        assert_eq!(json["degraded"], false);
        assert_eq!(json["components"]["session"]["durable_store"], true);
        assert_eq!(json["components"]["session"]["source_of_truth"], "attached");
        assert!(json["config_reload"]["status"].is_string());
        assert!(json["health"]["runtime"].is_object());
        assert_eq!(json["health"]["storage"]["backend"], "attached");
        assert_eq!(json["components"]["context"]["durable_history"], true);
        assert_eq!(json["components"]["task"]["total"], 1);
        assert_eq!(json["components"]["task"]["open"], 1);
        assert_eq!(json["components"]["task"]["status_counts"]["running"], 1);
        assert_eq!(json["diagnostics"]["durable_session_store"], true);
        assert_eq!(json["diagnostics"]["memory_attached"], false);
        assert_eq!(json["diagnostics"]["active_sessions"], 0);
        assert_eq!(json["diagnostics"]["stored_sessions"], 0);
        assert_eq!(json["diagnostics"]["open_tasks"], 1);
        assert_eq!(json["diagnostics"]["component_count"], 10);
        assert_eq!(json["diagnostics"]["degraded_component_count"], 0);
        assert_eq!(json["diagnostics"]["attention_component_count"], 1);
        assert!(json["diagnostics"]["elapsed_ms"].as_u64().is_some());
        assert!(matches!(
            json["diagnostics"]["performance_status"].as_str(),
            Some("healthy" | "attention" | "degraded")
        ));
        assert_eq!(json["diagnostics"]["provider_configured"], true);
        assert_eq!(json["components"]["provider"]["status"], "available");
        assert_eq!(json["diagnostics"]["production_ready"], false);
        assert_control_plane_readiness_accounting(&json);
        assert!(json["readiness"]["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "memory.manager"));
        assert!(json["readiness"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "provider.registry" && check["status"] == "ready"));
        assert!(json["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action
                .as_str()
                .unwrap_or_default()
                .contains("memory manager")));
        assert_eq!(
            json["components"]["channels"]["adapters"][0]["id"],
            "wechat-ilink"
        );
        assert!(json["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| capability == "permission.cross_plane"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn runtime_control_plane_counts_file_backed_sqlite_sessions_after_reopen() {
        let dir = test_temp_dir("runtime-control-plane-db");
        let db_path = dir.join("sessions.db");
        {
            let store = UnifiedSessionStore::open(&db_path).unwrap();
            store
                .create_session(&new_api_session_record(
                    "control-db-session-a",
                    Some("model-a".into()),
                ))
                .await
                .unwrap();
            store
                .create_session(&new_api_session_record(
                    "control-db-session-b",
                    Some("model-b".into()),
                ))
                .await
                .unwrap();
        }
        assert!(
            db_path.exists(),
            "file-backed session database should exist"
        );

        let workspace = dir.join("workspace");
        let config_home = dir.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let reopened = Arc::new(UnifiedSessionStore::open(&db_path).unwrap());
        let app = api_router(test_state_with_store_and_workspace(
            reopened,
            workspace,
            config_home,
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_control_plane");
        assert_eq!(json["components"]["session"]["durable_store"], true);
        assert_eq!(json["components"]["session"]["source_of_truth"], "attached");
        assert_eq!(json["diagnostics"]["durable_session_store"], true);
        assert_eq!(json["diagnostics"]["stored_sessions"], 2);
        assert_eq!(json["diagnostics"]["active_sessions"], 0);
        assert_eq!(json["diagnostics"]["open_tasks"], 0);
        assert!(json["diagnostics"]["elapsed_ms"].as_u64().is_some());
        assert!(matches!(
            json["diagnostics"]["performance_status"].as_str(),
            Some("healthy" | "attention" | "degraded")
        ));
        assert_eq!(json["diagnostics"]["production_ready"], false);
        assert_control_plane_readiness_accounting(&json);
        assert!(json["readiness"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "context.durable_history" && check["status"] == "ready"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn runtime_control_plane_reports_provider_config_without_secrets() {
        let root = test_temp_dir("runtime-control-provider-config");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
model: "sonnet-enterprise"
providers:
  anthropic:
    base_url: "https://api.anthropic.example/v1"
    api_key: "secret-provider-key"
    models: ["sonnet-enterprise", "haiku-enterprise"]
    protocol: "anthropic"
"#,
        )
        .unwrap();

        let state = test_state_with_workspace(workspace, config_home);
        activate_test_provider_config(&state);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/control-plane")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["components"]["provider"]["status"], "available");
        assert_eq!(json["components"]["provider"]["provider_count"], 1);
        assert_eq!(json["components"]["provider"]["model_count"], 2);
        assert_eq!(
            json["components"]["provider"]["configured_model"],
            "sonnet-enterprise"
        );
        assert_eq!(
            json["components"]["provider"]["configured_model_provider"],
            "anthropic"
        );
        assert_eq!(
            json["components"]["provider"]["configured_model_resolved"],
            true
        );
        assert!(json["components"]["provider"]["catalog_generation"]
            .as_str()
            .unwrap_or_default()
            .starts_with("provider-catalog-v2-"));
        assert!(
            json["components"]["provider"]["configured_catalog_generation"]
                .as_str()
                .unwrap_or_default()
                .starts_with("provider-catalog-v2-")
        );
        assert_eq!(
            json["components"]["provider"]["active_matches_configured"],
            true
        );
        assert_eq!(
            json["components"]["provider"]["catalog"]["models"][0]["effective_protocol"],
            "anthropic"
        );
        assert_eq!(
            json["components"]["provider"]["provider_names"]
                .as_array()
                .unwrap(),
            &vec![serde_json::Value::from("anthropic")]
        );
        assert_eq!(json["diagnostics"]["provider_configured"], true);
        assert_eq!(json["diagnostics"]["provider_count"], 1);
        assert_eq!(json["diagnostics"]["provider_model_count"], 2);
        assert_eq!(json["diagnostics"]["configured_model_resolved"], true);
        assert!(json["readiness"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "provider.registry" && check["status"] == "ready"));
        assert!(json["readiness"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["id"] == "provider.model_routing" && check["status"] == "ready"));
        assert!(!json.to_string().contains("secret-provider-key"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn config_providers_and_update_config_are_real_and_redacted() {
        let root = test_temp_dir("system-config-providers");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
model: "model-a"
providers:
  local:
    base_url: "https://local.example/v1"
    api_key: "secret-local-key"
    models: ["model-a", "model-b"]
    protocol: "completions"
"#,
        )
        .unwrap();

        let state = test_state_with_workspace(workspace, config_home.clone());
        activate_test_provider_config(&state);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/config/providers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["provider_count"], 1);
        assert_eq!(json["provider_model_count"], 2);
        assert_eq!(json["configured_model"], "model-a");
        assert_eq!(json["models"][1]["id"], "model-b");
        assert_eq!(json["models"][1]["effective_protocol"], "completions");
        assert_eq!(json["models"][1]["protocol_configured"], true);
        assert!(json["catalog_generation"]
            .as_str()
            .unwrap_or_default()
            .starts_with("provider-catalog-v2-"));
        assert_ne!(
            json["catalog_generation"],
            json["configured_catalog_generation"]
        );
        assert!(json["active_provider_revision"].as_u64().is_some());
        assert_eq!(json["active_matches_configured"], true);
        assert_eq!(json["catalog"]["providers"][0]["id"], "local");
        assert_eq!(json["catalog"]["models"][1]["id"], "model-b");
        assert_eq!(json["catalog"]["profiles"][0]["id"], "default");
        assert_eq!(json["providers"][0]["effective_protocol"], "completions");
        assert_eq!(json["providers"][0]["protocol_configured"], true);
        assert_eq!(json["providers"][0]["credential_present"], true);
        assert!(!json.to_string().contains("secret-local-key"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/config/provider-catalog")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let catalog_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            catalog_json["catalog"]["generation"],
            json["catalog_generation"]
        );
        assert_eq!(catalog_json["catalog"]["models"][0]["provider"], "local");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["providers"]["local"]["api_key"], "[redacted]");
        assert!(!json.to_string().contains("secret-local-key"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/config")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"model-b"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let written = std::fs::read_to_string(config_home.join("config.yaml")).unwrap();
        assert!(written.contains("model-b"));

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/config")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"missing-model"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["error"]
            .as_str()
            .unwrap_or_default()
            .contains("model `missing-model` is not declared"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn slash_catalog_dispatch_and_history_are_available() {
        let root = test_temp_dir("slash-gateway");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();

        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record(
                "s1",
                Some("test-model".to_string()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store_and_workspace(store, workspace, config_home);
        let attached = state
            .services
            .session
            .attach_session_value(
                "s1",
                "principal:local-human:surface:webui:slash",
                "webui",
                Some("writer"),
            )
            .await;
        assert_eq!(attached["ok"], true);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/slash?surface=webui")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["commands"]
            .as_array()
            .unwrap()
            .iter()
            .any(|command| command["name"] == "/status"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/slash/slash.status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["slash"]["id"], "slash.status");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/slash/resolve")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"input":"/status","surface":"webui"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["resolution"]["command"]["name"], "/status");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/slash/dispatch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"command":"/status","args":{"session_id":"s1"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
        assert_eq!(json["slash"], "/status");
        assert!(matches!(
            json["status"].as_str(),
            Some("complete" | "degraded")
        ));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/slash/dispatch")
                    .header("x-cowd-observer-id", "webui:slash")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"command":"/compact","args":{"session_id":"s1"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["error"]
            .as_str()
            .is_some_and(|error| error.contains("owned by the requesting Surface")));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/slash/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 1);
    }

    #[tokio::test]
    async fn runtime_provider_reload_replaces_runtime_registry_from_config() {
        let root = test_temp_dir("runtime-provider-reload");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
model: "reload-model"
providers:
  reload:
    base_url: "https://reload.example/v1"
    api_key: "reload-secret-key"
    models: ["reload-model", "reload-fast"]
    protocol: "completions"
"#,
        )
        .unwrap();

        let state = test_state_with_workspace(workspace, config_home);
        let provider_registry = state.services.runtime.as_ref().unwrap().provider_registry();
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/providers/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_provider_reload");
        assert_eq!(json["status"], "applied");
        assert_eq!(json["applied"], true);
        assert_eq!(json["provider_count"], 1);
        assert_eq!(json["provider_model_count"], 2);
        assert_eq!(json["configured_model"], "reload-model");
        assert_eq!(json["configured_model_provider"], "reload");
        assert_eq!(json["configured_model_resolved"], true);
        assert!(!json.to_string().contains("reload-secret-key"));
        let provider_snapshot = provider_registry.pin();
        let provider = provider_snapshot
            .resolve("reload-model")
            .expect("reloaded provider should resolve model");
        assert_eq!(provider.name, "reload");
        assert_eq!(provider.models, vec!["reload-model", "reload-fast"]);

        let invalid_root = test_temp_dir("runtime-provider-reload-invalid");
        let invalid_workspace = invalid_root.join("workspace");
        let invalid_config_home = invalid_root.join("home");
        std::fs::create_dir_all(&invalid_workspace).unwrap();
        std::fs::create_dir_all(&invalid_config_home).unwrap();
        std::fs::write(
            invalid_config_home.join("config.yaml"),
            r#"
model: "broken-model"
providers:
  broken:
    base_url: "https://broken.example/v1"
    api_key: "broken-secret-key"
    models: ["broken-model"]
    protocol: "unsupported-protocol"
"#,
        )
        .unwrap();

        let invalid_state = test_state_with_workspace(invalid_workspace, invalid_config_home);
        let invalid_registry = invalid_state
            .services
            .runtime
            .as_ref()
            .unwrap()
            .provider_registry();
        let app = api_router(invalid_state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/providers/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "runtime_provider_reload");
        assert_eq!(json["status"], "failed");
        assert_eq!(json["applied"], false);
        assert_eq!(json["configured_model_resolved"], false);
        assert!(json["warnings"]
            .to_string()
            .contains("unsupported-protocol"));
        assert!(!json.to_string().contains("broken-secret-key"));
        assert!(invalid_registry.pin().resolve("broken-model").is_none());
        let retained_snapshot = provider_registry.pin();
        assert_eq!(
            retained_snapshot
                .resolve("reload-model")
                .expect("existing provider should remain after failed reload")
                .name,
            "reload"
        );

        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(invalid_root);
    }

    #[tokio::test]
    async fn runtime_config_reload_applies_gateway_runtime_dependencies() {
        let root = test_temp_dir("runtime-config-reload");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        let webui_dir = root.join("webui");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::create_dir_all(&webui_dir).unwrap();
        std::fs::write(webui_dir.join("index.html"), "<!doctype html>reload").unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            format!(
                r#"
model: "reload-model"
providers:
  reload:
    base_url: "https://reload.example/v1"
    api_key: "reload-secret-key"
    models: ["reload-model", "reload-fallback"]
    protocol: "completions"
fallbacks: ["reload-fallback"]
gateway:
  enabled: true
  webui_dir: "{}"
  platforms:
    - platform_type: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: 8642
    - platform_type: "feishu"
      enabled: true
      app_id: "app-id"
      app_secret: "app-secret"
"#,
                webui_dir.display()
            ),
        )
        .unwrap();

        let state = test_state_with_workspace(workspace, config_home);
        let app = api_router(Arc::clone(&state));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/config/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "gateway.config.reload");
        assert_eq!(json["applied"], true);
        assert_eq!(json["applied_sections"]["providers"]["provider_count"], 1);
        assert_eq!(
            json["applied_sections"]["provider_fallbacks"]["activation_scope"],
            "next_provider_request_in_existing_and_new_session_runtimes"
        );
        assert_eq!(
            state
                .services
                .runtime
                .as_ref()
                .unwrap()
                .runtime_services()
                .provider_fallbacks(),
            vec!["reload-fallback".to_string()]
        );
        assert_eq!(
            json["applied_sections"]["surface_runtime_configs"]["count"],
            1
        );
        assert_eq!(json["applied_sections"]["static_webui"]["status"], "ready");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/message-connectors")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let connectors: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let feishu = connectors["connectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|connector| connector["connector"] == "feishu")
            .expect("feishu message connector should be projected from reloaded config");
        assert_eq!(feishu["configured"], true);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_config_reload_rejects_invalid_config_without_replacing_running_state() {
        let root = test_temp_dir("runtime-config-reload-invalid-preserve");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let config_path = config_home.join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
model: "stable-model"
providers:
  stable:
    base_url: "https://stable.example/v1"
    api_key: "stable-secret-key"
    models: ["stable-model"]
    protocol: "completions"
"#,
        )
        .unwrap();

        let state = test_state_with_workspace(workspace, config_home);
        let provider_registry = state.services.runtime.as_ref().unwrap().provider_registry();
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/config/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["applied"], true);
        assert!(provider_registry.pin().resolve("stable-model").is_some());

        std::fs::write(&config_path, "model: [\n").unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/config/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "invalid");
        assert_eq!(json["applied"], false);
        assert!(provider_registry.pin().resolve("stable-model").is_some());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/config/reload/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["kind"], "gateway.config.reload.status");
        assert_eq!(status["status"], "invalid");
        assert_eq!(status["applied"], false);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_control_plane_emits_structured_trace_event() {
        let root = test_temp_dir("runtime-control-plane-trace");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let state = test_state_with_store_and_workspace(store, workspace, config_home);
        seed_test_task(
            &state.services,
            "control-plane-trace-task",
            "trace control plane",
        );
        let Json(json) = runtime_routes::get_runtime_control_plane(AxumState(state)).await;
        assert_eq!(json["kind"], "runtime_control_plane");
        assert_eq!(json["status"], "attention");
        assert_eq!(json["degraded"], false);
        assert_eq!(json["diagnostics"]["durable_session_store"], true);
        assert_eq!(json["diagnostics"]["memory_attached"], false);
        assert_eq!(json["diagnostics"]["provider_configured"], true);
        assert!(json["diagnostics"]["provider_count"].as_u64().unwrap_or(0) > 0);
        assert!(
            json["diagnostics"]["provider_model_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(json["diagnostics"]["configured_model_resolved"], true);
        assert_eq!(json["diagnostics"]["stored_sessions"], 0);
        assert_eq!(json["diagnostics"]["open_tasks"], 1);
        assert_eq!(json["diagnostics"]["component_count"], 10);
        assert!(json["diagnostics"]["capability_count"].as_u64().is_some());
        assert!(json["diagnostics"]["elapsed_ms"].as_u64().is_some());
        assert!(json["readiness"]["production_ready"].is_boolean());
        assert!(json["readiness"]["required_blocked"].as_u64().is_some());
        assert!(json["readiness"]["score"].as_u64().is_some());

        let _ = std::fs::remove_dir_all(root);
    }

    fn test_context_envelope(
        session_id: &str,
        envelope_id: &str,
        intent: &str,
    ) -> serde_json::Value {
        let mut envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            profile: ContextProfile::MainTurn,
            identity: ContextIdentity::main(session_id),
            intent: intent.to_string(),
            stable_head: vec!["stable".to_string()],
            runtime_header: vec!["runtime".to_string()],
            dynamic_items: vec![ContextItem::new(
                format!("{envelope_id}-item"),
                ContextSourceKind::Memory,
                ContextRole::Orientation,
                "orientation",
            )],
            omitted: Vec::new(),
            total_budget_tokens: 4_000,
        });
        envelope.id = envelope_id.to_string();
        serde_json::json!({
            "type": "ContextEnvelope",
            "envelope_id": envelope.id,
            "run_id": format!("run-{envelope_id}"),
            "session_id": session_id,
            "envelope": envelope,
        })
    }

    #[tokio::test]
    async fn session_context_history_reads_context_events_only() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-history-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, event_type, payload) in [
            (
                0,
                "TextDelta",
                serde_json::json!({"type":"TextDelta","content":"skip"}),
            ),
            (
                1,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-1", "first"),
            ),
            (
                2,
                "ToolStart",
                serde_json::json!({"type":"ToolStart","name":"skip"}),
            ),
            (
                3,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-2", "second"),
            ),
        ] {
            store
                .append_event(&session::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: event_type.to_string(),
                    event_json: payload.to_string(),
                    sequence: sequence as usize,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?from_seq=0&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["total"], 2);
        assert_eq!(json["envelopes"].as_array().unwrap().len(), 2);
        assert_eq!(json["summaries"].as_array().unwrap().len(), 2);
        assert_eq!(json["envelopes"][0]["sequence"], 1);
        assert_eq!(json["envelopes"][0]["envelope_id"], "env-1");
        assert_eq!(json["envelopes"][0]["run_id"], "run-env-1");
        assert_eq!(json["envelopes"][1]["envelope"]["intent"], "second");
        assert_eq!(json["summaries"][0]["envelope_id"], "env-1");
        assert_eq!(json["summaries"][0]["profile"], "MainTurn");
        assert_eq!(json["summaries"][0]["intent"], "first");
        assert_eq!(json["summaries"][0]["selected_count"], 1);
        assert_eq!(json["summaries"][0]["omitted_count"], 0);
    }

    #[tokio::test]
    async fn session_context_history_can_return_summaries_without_full_envelopes() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-summary-only-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&session::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(session_id, "env-summary", "summary").to_string(),
                sequence: 5,
                created_at_ms: 5,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?include_envelopes=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["include_envelopes"], false);
        assert_eq!(json["envelopes"].as_array().unwrap().len(), 0);
        assert_eq!(json["summaries"].as_array().unwrap().len(), 1);
        assert_eq!(json["summaries"][0]["envelope_id"], "env-summary");
        assert_eq!(json["summaries"][0]["intent"], "summary");
    }

    #[tokio::test]
    async fn session_context_history_paginates_summary_timeline() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-summary-page-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, envelope_id, intent) in [
            (1, "env-page-1", "first"),
            (3, "env-page-3", "second"),
            (5, "env-page-5", "third"),
        ] {
            store
                .append_event(&session::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: "ContextEnvelope".to_string(),
                    event_json: test_context_envelope(session_id, envelope_id, intent).to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?limit=2&include_envelopes=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 3);
        assert_eq!(json["has_more"], true);
        assert_eq!(json["next_seq"], 4);
        assert_eq!(json["envelopes"].as_array().unwrap().len(), 0);
        assert_eq!(json["summaries"].as_array().unwrap().len(), 2);
        assert_eq!(json["summaries"][0]["envelope_id"], "env-page-1");
        assert_eq!(json["summaries"][1]["envelope_id"], "env-page-3");
    }

    #[tokio::test]
    async fn session_context_history_matches_sqlite_event_log() {
        let dir = test_temp_dir("context-db-timeline");
        let db_path = dir.join("sessions.sqlite");
        let store = Arc::new(UnifiedSessionStore::open(&db_path).unwrap());
        let session_id = "context-db-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, event_type, payload) in [
            (
                0,
                "TextDelta",
                serde_json::json!({"type":"TextDelta","content":"not context"}),
            ),
            (
                1,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-db-1", "first db context"),
            ),
            (
                2,
                "ToolComplete",
                serde_json::json!({"type":"ToolComplete","summary":"not context"}),
            ),
            (
                3,
                "ContextEnvelope",
                test_context_envelope(session_id, "env-db-3", "second db context"),
            ),
        ] {
            store
                .append_event(&session::SessionEvent {
                    session_id: session_id.to_string(),
                    event_type: event_type.to_string(),
                    event_json: payload.to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .await
                .unwrap();
        }

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let db_context_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1 AND event_type = 'ContextEnvelope'",
                [session_id],
                |row| row.get(0),
            )
            .unwrap();
        let db_all_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .unwrap();
        drop(conn);

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?limit=1&include_envelopes=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(db_all_count, 4);
        assert_eq!(db_context_count, 2);
        assert_eq!(json["total"], db_context_count);
        assert_eq!(json["has_more"], true);
        assert_eq!(json["next_seq"], 2);
        assert_eq!(json["envelopes"].as_array().unwrap().len(), 0);
        assert_eq!(json["summaries"].as_array().unwrap().len(), 1);
        assert_eq!(json["summaries"][0]["sequence"], 1);
        assert_eq!(json["summaries"][0]["envelope_id"], "env-db-1");

        let detail_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/env-db-3")
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
        assert_eq!(detail_json["context"]["sequence"], 3);
        assert_eq!(
            detail_json["context"]["envelope"]["intent"],
            "second db context"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(trace_capture)]
    async fn session_context_history_emits_structured_trace_events() {
        use tracing_subscriber::prelude::*;

        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-log-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&session::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(session_id, "env-log-1", "logged").to_string(),
                sequence: 7,
                created_at_ms: 77,
            })
            .await
            .unwrap();

        let _trace_guard = trace_capture_lock().lock().await;
        let capture = CapturedTraceEvents::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());

        let _default_trace_subscriber = tracing::subscriber::set_default(subscriber);
        tracing::callsite::rebuild_interest_cache();
        let state = test_state_with_store(store);
        let app = api_router(state);
        let history_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context?include_envelopes=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(history_response.status(), StatusCode::OK);
        let history_body = to_bytes(history_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let history_json: serde_json::Value = serde_json::from_slice(&history_body).unwrap();
        assert_eq!(history_json["session_id"], session_id);
        assert_eq!(history_json["include_envelopes"], false);
        assert_eq!(history_json["total"], 1);
        assert_eq!(history_json["summaries"].as_array().unwrap().len(), 1);

        let detail_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/env-log-1")
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
        assert_eq!(detail_json["context"]["envelope_id"], "env-log-1");

        let lines = capture.lines();
        let joined = lines
            .into_iter()
            .filter(|line| {
                line.contains("context history loaded") || line.contains("context envelope loaded")
            })
            .collect::<Vec<_>>()
            .join("\n");
        if !joined.is_empty() {
            assert!(joined.contains("context-log-session"));
            if joined.contains("context history loaded") {
                assert!(joined.contains("include_envelopes=false"));
                assert!(joined.contains("total=1"));
            } else {
                assert!(
                    joined.contains("envelope_id=env-log-1")
                        || joined.contains("envelope_id=\"env-log-1\"")
                );
                assert!(joined.contains("sequence=7"));
            }
}
    }

    #[tokio::test]
    async fn context_envelope_route_reads_by_envelope_id() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-id-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        store
            .append_event(&session::SessionEvent {
                session_id: session_id.to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: test_context_envelope(session_id, "env-target", "inspect").to_string(),
                sequence: 4,
                created_at_ms: 4,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/env-target")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["enabled"], true);
        assert_eq!(json["source"], "history");
        assert_eq!(json["context"]["session_id"], session_id);
        assert_eq!(json["context"]["sequence"], 4);
        assert_eq!(json["context"]["envelope"]["id"], "env-target");
    }

    #[tokio::test]
    async fn context_recommendation_action_records_session_event() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-recommendation-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();

        let state = test_state_with_store(store.clone());
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/sessions/{session_id}/context/recommendations"
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "envelope_id": "env-1",
                            "recommendation": "Start a handoff",
                            "action": "acknowledged",
                            "note": "handled"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let events = store.get_events(session_id, 0).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, session::SESSION_DOMAIN_EVENT_TYPE);
        let event = session::SessionDomainEvent::from_session_event(&events[0]).unwrap();
        assert_eq!(event.kind, "context.recommendation_action");
        let payload = event.payload;
        assert_eq!(payload["envelope_id"], "env-1");
        assert_eq!(payload["recommendation"], "Start a handoff");
        assert_eq!(payload["note"], "handled");
    }

    #[tokio::test]
    async fn context_recommendation_stats_groups_actions() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "context-recommendation-stats-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for (sequence, action) in [(0, "acknowledged"), (1, "applied")] {
            let event = session::SessionDomainEvent::new(
                session_id,
                sequence,
                session::SessionDomainScope::Context,
                "context.recommendation_action",
                serde_json::json!({
                    "type": "ContextRecommendationAction",
                    "session_id": session_id,
                    "envelope_id": format!("env-{sequence}"),
                    "recommendation": "Start a handoff",
                    "action": action,
                }),
                sequence as u64,
            );
            store.append_session_domain_event(&event).await.unwrap();
        }

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/context/recommendations?limit=20"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["total"], 2);
        assert_eq!(
            json["recommendations"][0]["recommendation"],
            "Start a handoff"
        );
        assert_eq!(json["recommendations"][0]["count"], 2);
        assert_eq!(json["recommendations"][0]["actions"]["acknowledged"], 1);
        assert_eq!(json["recommendations"][0]["actions"]["applied"], 1);
        assert_eq!(json["recommendations"][0]["latest_envelope_id"], "env-1");
    }

    #[test]
    fn task_resume_context_packet_summarizes_current_task() {
        let runtime_services = runtime::RuntimeServices::in_memory().expect("test task runtime");
        let policy = harness_contract::policy::SessionExecutionPolicy::from_profile(
            harness_contract::policy::AutonomyProfileId::Supervised,
            1,
            harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
        );
        runtime_services.publish_session_execution_policy(
            "test-session".to_string(),
            runtime::permissions::SessionExecutionPolicyControl::from_policy(policy),
        );
        let service = crate::services::TaskService::with_runtime(runtime_services);
        let task = service
            .create(
                "task-resume-context".to_string(),
                service
                    .workspace_default_mission_id()
                    .expect("Runtime-backed TaskService"),
                "test-session".to_string(),
                "test-turn-task-resume-context".to_string(),
                "ship context runtime".to_string(),
                vec![harness_contract::reality::EvidenceRef::observed(
                    "test_fixture",
                    "test://tasks/task-resume-context",
                )],
            )
            .expect("seed canonical Runtime task")
            .aggregate;
        let phase_id = task.phases[0].phase_id.clone();
        let task = service
            .record_phase_artifact(
                &task.task_id,
                task.revision,
                &phase_id,
                "evidence".to_string(),
                "test".to_string(),
                "cargo test -p runtime context_runtime".to_string(),
                vec![harness_contract::reality::EvidenceRef::observed(
                    "test_fixture",
                    "test://tasks/task-resume-context/artifacts/1",
                )],
            )
            .unwrap();

        let packet = message_routes::task_resume_context_packet("session-task", &task);

        assert_eq!(packet.session_id, "session-task");
        assert_eq!(packet.source, ResumeContextSource::ExecutionGraph);
        assert!(packet
            .active_task
            .as_deref()
            .is_some_and(|task| task.contains("ship context runtime")));
        assert!(packet
            .recent_decisions
            .iter()
            .any(|event| event.contains("phase=implementation")));
    }

    #[tokio::test]
    async fn tools_returns_list() {
        let state = test_state();
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
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn approval_routes_resolve_global_queue_request() {
        let state = test_state();
        let runtime_services = state
            .services
            .runtime
            .as_ref()
            .expect("test runtime service")
            .runtime_services();
        let app = api_router(state);
        let source = runtime::ApprovalSource {
            kind: runtime::ApprovalSourceKind::Session,
            session_id: Some(format!("approval-route-{}", uuid::Uuid::new_v4())),
            agent_id: None,
            team_id: None,
            mission_id: Some("mission-approval-route".to_string()),
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        let approval = runtime_services
            .approval_queue()
            .submit(runtime::SubmitGlobalApprovalRequest {
                context: harness_contract::policy::ApprovalContext::owned(
                    &source,
                    "apply_patch",
                    "workspace:approval-route",
                ),
                source,
                action: "apply_patch".to_string(),
                summary: "modify runtime file".to_string(),
                risk: harness_contract::core::TaskRisk::High,
                domain: harness_contract::policy::ApprovalDomain::Execution,
                blocks_execution: true,
                evidence_refs: vec!["approval-route:test".to_string()],
                timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
            })
            .expect("approval submitted");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/pending")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let pending_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(pending_json["kind"], "gateway.unified_approval_pending");
        assert!(pending_json["pending"]
            .as_array()
            .expect("pending approvals")
            .iter()
            .any(|item| item["approval_id"].as_str() == Some(approval.approval_id.as_str())));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": approval.approval_id,
                            "approved": true,
                            "scope": "once"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            runtime_services
                .approval_queue()
                .get(&approval.approval_id)
                .expect("approval exists")
                .status,
            runtime::GlobalApprovalStatus::Approved
        );
    }

    #[tokio::test]
    async fn approval_routes_isolate_session_owners_and_hide_foreign_exact_records() {
        use crate::services::session_service::{EnsureSessionRequest, SessionSource};

        let state = test_state();
        let runtime_services = state
            .services
            .runtime
            .as_ref()
            .expect("test runtime service")
            .runtime_services();
        for (session_id, owner) in [
            ("approval-owner-session", "approval-owner"),
            ("approval-foreign-session", "approval-foreign"),
        ] {
            let mut request = EnsureSessionRequest::new(
                session_id,
                Some("test-model".to_string()),
                SessionSource::WebUi,
            );
            request.owner_principal_id = Some(owner.to_string());
            state
                .services
                .session
                .ensure_surface_session(request)
                .await
                .expect("durable owner-bound Session fixture");
        }
        let submit = |approval_id: &str, session_id: &str| {
            let source = runtime::ApprovalSource {
                kind: runtime::ApprovalSourceKind::Session,
                session_id: Some(session_id.to_string()),
                agent_id: None,
                team_id: None,
                mission_id: None,
                resource_ref: Some(format!("session:{session_id}:workspace-file")),
                review_ref: None,
                application: None,
            };
            runtime_services
                .approval_queue()
                .submit_scoped(
                    approval_id,
                    runtime::SubmitGlobalApprovalRequest {
                        context: harness_contract::policy::ApprovalContext::owned(
                            &source,
                            "apply_patch",
                            runtime_services.workspace_key(),
                        ),
                        source,
                        action: "apply_patch".to_string(),
                        summary: format!("review {session_id}"),
                        risk: harness_contract::core::TaskRisk::High,
                        domain: harness_contract::policy::ApprovalDomain::Execution,
                        blocks_execution: true,
                        evidence_refs: vec![format!("private:{session_id}")],
                        timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
                    },
                )
                .expect("owner-bound approval")
        };
        let own = submit("approval-owner-record", "approval-owner-session");
        let foreign = submit("approval-foreign-record", "approval-foreign-session");
        let owner = runtime::VerifiedPrincipal::from_test_claims(
            harness_contract::security::PrincipalClaims {
                principal_id: "approval-owner".to_string(),
                tenant_id: "tenant:test".to_string(),
                grant_id: "grant:approval-owner".to_string(),
                kind: harness_contract::security::PrincipalKind::Human,
                scopes: vec!["gateway".to_string()],
                capabilities: vec!["approval.respond".to_string()],
                assurance: harness_contract::security::PrincipalAssurance::HumanInteractive,
                issuer: "approval-route-test".to_string(),
                issued_at_ms: 1,
                expires_at_ms: None,
                credential_fingerprint: "approval-owner-test".to_string(),
                credential_epoch: 1,
                profile_revision: 1,
                app_profiles: std::collections::BTreeMap::new(),
            },
        );
        let app = approval_routes::router()
            .layer(Extension(AuthenticatedPrincipal(owner)))
            .with_state(state);

        let pending = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/pending")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(pending.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(pending.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let ids = body["pending"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|request| request["approval_id"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![own.approval_id.as_str()]);
        assert!(body["pending"][0]["evidence_refs"]
            .as_array()
            .is_some_and(Vec::is_empty));

        let foreign_exact = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/approval/{}", foreign.approval_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(foreign_exact.status(), StatusCode::NOT_FOUND);
    }
