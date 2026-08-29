// Legacy API behavior shard; included into one shared test scope.

    #[tokio::test]
    async fn runtime_agent_routes_preserve_rejection_for_unrecoverable_process_handles() {
        let state = test_state();
        let agent_id = format!("agent-command-{}", uuid::Uuid::new_v4());
        let services = state
            .services
            .runtime
            .as_ref()
            .expect("runtime service")
            .runtime_services();
        let graph_identity = harness_contract::execution::ExecutionIdentity::for_task_graph(
            "principal-command",
            services.workspace_key(),
            "mission-command",
            "task-command",
            "session-command",
            "turn-command",
            "graph-command",
        )
        .expect("graph identity");
        services
            .agent_runtime()
            .restore_verified_run(runtime::AgentRunSnapshot {
                execution_identity: harness_contract::execution::ExecutionIdentity::for_agent_node(
                    &graph_identity,
                    format!("run-{agent_id}"),
                    "node-command",
                )
                .expect("agent identity"),
                run_id: format!("run-{agent_id}"),
                agent_id: agent_id.clone(),
                task_id: "task-command".to_string(),
                root_task_id: "task-command".to_string(),
                session_id: "session-command".to_string(),
                graph_id: "graph-command".to_string(),
                node_id: "node-command".to_string(),
                attempt: 1,
                expected_graph_revision: 1,
                backend: runtime::AgentBackendKind::ProcessJsonl,
                status: harness_contract::agent::AgentStatus::Running,
                revision: 0,
                model: None,
                provider: None,
                binding: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: None,
            })
            .expect("restore agent");
        let app = api_router(state);

        for path in ["input", "interrupt", "shutdown"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/runtime/agents/{agent_id}/{path}"))
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({"payload": {"text": path}}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["receipt"]["accepted"], false);
            assert_eq!(json["receipt"]["reject_reason"], "unsupported_by_backend");
        }
    }
    #[tokio::test]
    async fn tool_cache_api_reports_stats() {
        let workspace = test_temp_dir("tool-cache-api");
        let config_home = test_temp_dir("tool-cache-api-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tools/cache")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["tool_name"], "tool_cache_stats");
        assert_eq!(json["status"], "ok");
        assert!(json["data"]["entries"].is_number());

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_execute_rejects_write_tools_and_path_escape() {
        let workspace = test_temp_dir("tool-execute-safety");
        let config_home = test_temp_dir("tool-execute-safety-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let rejected_write = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "name": "write_file",
                            "input": { "path": "owned.txt", "content": "no" }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected_write.status(), StatusCode::FORBIDDEN);

        let rejected_escape = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "name": "read_file",
                            "input": { "path": "../outside.txt" }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected_escape.status(), StatusCode::BAD_REQUEST);

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_mutation_api_previews_and_applies_transaction() {
        let workspace = test_temp_dir("tool-mutation-api");
        let config_home = test_temp_dir("tool-mutation-api-config");
        std::fs::write(workspace.join("a.txt"), "alpha\n").unwrap();
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let preview = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/mutations/preview")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "edits": [{
                                "path": "a.txt",
                                "old_string": "alpha",
                                "new_string": "beta"
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preview.status(), StatusCode::OK);
        let body = to_bytes(preview.into_body(), usize::MAX).await.unwrap();
        let preview_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(preview_json["data"]["type"], "mutation_preview");
        let expected_hash = preview_json["data"]["files"][0]["expectedHash"]
            .as_str()
            .unwrap();

        let apply = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/mutations/apply")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "edits": [{
                                "path": "a.txt",
                                "old_string": "alpha",
                                "new_string": "beta"
                            }],
                            "expected_hashes": {
                                "a.txt": expected_hash
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(apply.status(), StatusCode::OK);
        assert_eq!(
            std::fs::read_to_string(workspace.join("a.txt")).unwrap(),
            "beta\n"
        );

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_checkpoint_api_returns_receipts() {
        let workspace = test_temp_dir("tool-checkpoint-api");
        let config_home = test_temp_dir("tool-checkpoint-api-config");
        std::fs::write(workspace.join("a.txt"), "before\n").unwrap();
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/checkpoints")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "label": "before edit" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::OK);
        let body = to_bytes(create.into_body(), usize::MAX).await.unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let checkpoint_id = created["data"]["id"].as_str().unwrap().to_string();
        assert_eq!(created["tool_name"], "checkpoint_create");
        assert_eq!(
            created["changed_refs"][0],
            format!("checkpoint:{checkpoint_id}")
        );

        std::fs::write(workspace.join("a.txt"), "after\n").unwrap();
        let diff = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/tools/checkpoints/{checkpoint_id}/diff"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let diff_status = diff.status();
        let body = to_bytes(diff.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            diff_status,
            StatusCode::OK,
            "checkpoint diff failed: {}",
            String::from_utf8_lossy(&body)
        );
        let diff_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(diff_json["data"]["changedFiles"][0], "a.txt");

        let restore = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tools/checkpoints/{checkpoint_id}/restore"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(restore.status(), StatusCode::OK);
        assert_eq!(
            std::fs::read_to_string(workspace.join("a.txt")).unwrap(),
            "before\n"
        );

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_batch_readonly_api_rejects_write_tools() {
        let workspace = test_temp_dir("tool-batch-api");
        let config_home = test_temp_dir("tool-batch-api-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let rejected = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/batch-readonly")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "calls": [{
                                "name": "write_file",
                                "input": { "path": "a.txt", "content": "no" }
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn tool_intent_and_fanout_plan_are_readonly() {
        let app = api_router(test_state());

        let intent = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/intent-plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "prompt": "review this WebUI change" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(intent.status(), StatusCode::OK);
        let body = to_bytes(intent.into_body(), usize::MAX).await.unwrap();
        let intent_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(intent_json["kind"], "tool.intent_plan");
        assert!(intent_json["recommended_tools"].as_array().unwrap().len() > 1);

        let fanout = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tools/context-fanout/plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "prompt": "发布前验收" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fanout.status(), StatusCode::OK);
        let body = to_bytes(fanout.into_body(), usize::MAX).await.unwrap();
        let fanout_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(fanout_json["kind"], "tool.context_fanout_plan");
        assert_eq!(fanout_json["batch_ready"], true);
    }

    #[tokio::test]
    async fn workspace_api_reports_profile_and_lists_files() {
        let workspace = test_temp_dir("workspace-list");
        let config_home = test_temp_dir("workspace-config");
        std::fs::create_dir_all(workspace.join("src")).unwrap();
        std::fs::write(workspace.join("src").join("main.rs"), "fn main() {}\n").unwrap();
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let workspace_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(workspace_response.status(), StatusCode::OK);
        let body = to_bytes(workspace_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["profile_id"], "enterprise");
        assert_eq!(json["workspace_root"], workspace.display().to_string());

        let files_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/files?dir=src")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(files_response.status(), StatusCode::OK);
        let body = to_bytes(files_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["dir"], "src");
        assert_eq!(json["files"][0]["name"], "main.rs");
        assert_eq!(json["files"][0]["path"], "src/main.rs");
        assert_eq!(json["files"][0]["type"], "file");

        std::fs::create_dir_all(workspace.join("src/bin")).unwrap();
        std::fs::write(workspace.join("src/bin").join("tool.rs"), "fn tool() {}\n").unwrap();
        std::fs::create_dir_all(workspace.join("target/debug")).unwrap();
        std::fs::write(workspace.join("target/debug/ignored.rs"), "ignored").unwrap();
        let recursive_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/files?recursive=true&limit=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recursive_response.status(), StatusCode::OK);
        let body = to_bytes(recursive_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let paths = json["files"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|item| item["path"].as_str())
            .collect::<Vec<_>>();
        assert!(paths.contains(&"src/main.rs"), "{paths:?}");
        assert!(paths.contains(&"src/bin/tool.rs"), "{paths:?}");
        assert!(!paths.iter().any(|path| path.starts_with("target/")));
        assert_eq!(json["recursive"], true);
        assert_eq!(json["truncated"], false);
    }

    #[tokio::test]
    async fn workspace_api_creates_reads_and_rejects_escape_paths() {
        let workspace = test_temp_dir("workspace-create");
        let config_home = test_temp_dir("workspace-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/workspace/files")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "path": "notes/audit.txt",
                            "content": "workspace isolation verified"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        assert_eq!(
            std::fs::read_to_string(workspace.join("notes/audit.txt")).unwrap(),
            "workspace isolation verified"
        );

        let raw_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/file/raw?path=notes/audit.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(raw_response.status(), StatusCode::OK);
        let body = to_bytes(raw_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"workspace isolation verified");

        let escape_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/files?dir=..")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(escape_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn workspace_download_returns_files_and_directory_zip() {
        let workspace = test_temp_dir("workspace-download");
        let config_home = test_temp_dir("workspace-download-config");
        std::fs::create_dir_all(workspace.join("docs/nested")).unwrap();
        std::fs::write(workspace.join("docs/readme.md"), "# readme").unwrap();
        std::fs::write(workspace.join("docs/nested/a.txt"), "nested").unwrap();
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let file_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/download?path=docs%2Freadme.md")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(file_response.status(), StatusCode::OK);
        assert_eq!(
            file_response.headers()[header::CONTENT_DISPOSITION],
            "attachment; filename=\"readme.md\""
        );
        let body = to_bytes(file_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"# readme");

        let dir_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/download?path=docs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(dir_response.status(), StatusCode::OK);
        assert_eq!(
            dir_response.headers()[header::CONTENT_TYPE],
            "application/zip"
        );
        assert_eq!(
            dir_response.headers()[header::CONTENT_DISPOSITION],
            "attachment; filename=\"docs.zip\""
        );
        let body = to_bytes(dir_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(body)).unwrap();
        let names = (0..archive.len())
            .map(|index| archive.by_index(index).unwrap().name().to_string())
            .collect::<Vec<_>>();
        assert!(names.iter().any(|name| name == "docs/readme.md"));
        assert!(names.iter().any(|name| name == "docs/nested/a.txt"));
        let mut readme = String::new();
        std::io::Read::read_to_string(&mut archive.by_name("docs/readme.md").unwrap(), &mut readme)
            .unwrap();
        assert_eq!(readme, "# readme");
    }

    #[tokio::test]
    async fn workspace_upload_meta_delete_and_attachments_are_real() {
        let workspace = test_temp_dir("workspace-upload");
        let config_home = test_temp_dir("workspace-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let mkdir_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/workspace/dirs")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"path":"uploads"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mkdir_response.status(), StatusCode::CREATED);

        let boundary = "cowd-test-boundary";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"dir\"\r\n\r\nuploads\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"sample.md\"\r\nContent-Type: text/markdown\r\n\r\n# uploaded\r\n\r\n--{boundary}--\r\n"
        );
        let upload_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/upload")
                    .header(
                        header::CONTENT_TYPE,
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(upload_response.status(), StatusCode::CREATED);
        let body = to_bytes(upload_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["path"], "uploads/sample.md");
        assert!(json["sha256"].as_str().unwrap().starts_with("sha256:"));
        assert_eq!(
            std::fs::read_to_string(workspace.join("uploads/sample.md")).unwrap(),
            "# uploaded\r\n"
        );

        let meta_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace/meta?path=uploads%2Fsample.md")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(meta_response.status(), StatusCode::OK);
        let body = to_bytes(meta_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["item"]["path"], "uploads/sample.md");

        let add_attachment = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/sessions/session-1/attachments")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"path":"uploads/sample.md","label":"Uploaded markdown"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(add_attachment.status(), StatusCode::CREATED);
        let body = to_bytes(add_attachment.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let ref_id = json["attachment"]["ref_id"].as_str().unwrap().to_string();
        assert_eq!(json["attachment"]["path"], "uploads/sample.md");

        let list_attachment = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/sessions/session-1/attachments")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_attachment.status(), StatusCode::OK);
        let body = to_bytes(list_attachment.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["count"], 1);

        let delete_attachment = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/sessions/session-1/attachments/{ref_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_attachment.status(), StatusCode::OK);

        let delete_file = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/workspace/files?path=uploads%2Fsample.md")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_file.status(), StatusCode::OK);
        assert!(!workspace.join("uploads/sample.md").exists());
    }

    #[tokio::test]
    async fn resource_upload_query_and_evidence_do_not_touch_workspace() {
        let workspace = test_temp_dir("resource-upload-workspace");
        let config_home = test_temp_dir("resource-upload-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let boundary = "cowd-resource-boundary";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"source\"\r\n\r\nwebui\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"voice.mp3\"\r\nContent-Type: application/octet-stream\r\n\r\nfake mp3 data\r\n--{boundary}--\r\n"
        );
        let upload_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/resources")
                    .header(
                        header::CONTENT_TYPE,
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(upload_response.status(), StatusCode::CREATED);
        let body = to_bytes(upload_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let resource_id = json["resource"]["id"].as_str().unwrap().to_string();
        assert!(json["resource"]["uri"]
            .as_str()
            .unwrap()
            .starts_with("resource://"));
        assert_eq!(json["resource"]["kind"], "audio");
        assert_eq!(json["resource"]["detected_mime"], "audio/mpeg");
        assert!(json["resource"]["artifact"]["selector"]
            .as_str()
            .unwrap()
            .starts_with("artifact://"));
        assert!(json["resource"].get("storage_path").is_none());
        assert!(!serde_json::to_string(&json)
            .unwrap()
            .contains(config_home.to_string_lossy().as_ref()));
        assert!(json["hint"]["guardrails"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or("")
                .contains("Do not claim audio content")));
        assert!(!workspace.join("voice.mp3").exists());

        let metadata_path = config_home
            .join("storage")
            .join("resources")
            .join("metadata")
            .join(format!("{resource_id}.json"));
        assert!(metadata_path.exists());

        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/resources/{resource_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);

        let content_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/resources/{resource_id}/content"))
                    .header(header::RANGE, "bytes=5-7")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(content_response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            to_bytes(content_response.into_body(), usize::MAX)
                .await
                .unwrap(),
            "mp3"
        );

        let evidence_response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/resources/{resource_id}/evidence"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence_response.status(), StatusCode::OK);
        let evidence_body = to_bytes(evidence_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&evidence_body).unwrap();
        assert!(evidence_json["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["action"] == "register_resource_from_path"));
    }

    #[tokio::test]
    async fn profile_api_creates_switches_and_deletes_profiles() {
        let app = api_router(test_state());

        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/profiles")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);
        let body = to_bytes(list_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["active_profile"], "default");
        assert_eq!(json["runtime_profile"], "default");

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/profiles")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "name": "Enterprise Ops" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let body = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["profile"]["id"], "enterprise_ops");
        assert_eq!(json["restart_required"], false);

        let switch_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/profiles/switch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "profile": "enterprise_ops" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(switch_response.status(), StatusCode::OK);
        let body = to_bytes(switch_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["active_profile"], "enterprise_ops");
        assert_eq!(json["runtime_profile"], "default");
        assert_eq!(json["restart_required"], true);

        let delete_active_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/profiles/enterprise_ops")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_active_response.status(), StatusCode::BAD_REQUEST);

        let switch_back_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/profiles/switch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "profile": "default" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(switch_back_response.status(), StatusCode::OK);

        let delete_response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/profiles/enterprise_ops")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_sessions_returns_empty() {
        let state = test_state();
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
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_sessions_reads_unified_store_metadata() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record(
                "stored-session",
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
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

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_sessions_filters_and_paginates_unified_store() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());

        let mut auth_a = new_api_session_record("auth-a", Some("claude-sonnet-4-6".into()));
        auth_a.metadata_json = Some(serde_json::json!({"title":"Auth Audit A"}).to_string());
        auth_a.message_count = 3;
        auth_a.last_activity = "2026-06-04T00:03:00Z".to_string();
        store.create_session(&auth_a).await.unwrap();

        let mut auth_b = new_api_session_record("auth-b", Some("claude-sonnet-4-6".into()));
        auth_b.metadata_json = Some(serde_json::json!({"title":"Auth Audit B"}).to_string());
        auth_b.message_count = 8;
        auth_b.last_activity = "2026-06-04T00:08:00Z".to_string();
        store.create_session(&auth_b).await.unwrap();

        let mut closed = new_api_session_record("auth-closed", Some("claude-sonnet-4-6".into()));
        closed.metadata_json = Some(serde_json::json!({"title":"Auth Closed"}).to_string());
        closed.status = "closed".to_string();
        closed.message_count = 99;
        store.create_session(&closed).await.unwrap();

        let mut other_model =
            new_api_session_record("auth-other-model", Some("claude-haiku-4-5".into()));
        other_model.metadata_json =
            Some(serde_json::json!({"title":"Auth Other Model"}).to_string());
        store.create_session(&other_model).await.unwrap();

        let mut deleted = new_api_session_record("auth-deleted", Some("claude-sonnet-4-6".into()));
        deleted.status = "deleted".to_string();
        store.create_session(&deleted).await.unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);

        let default_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let default_body = to_bytes(default_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let default_json: serde_json::Value = serde_json::from_slice(&default_body).unwrap();
        assert!(default_json["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|session| session["status"] != "deleted"));

        let deleted_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/sessions?status=deleted")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let deleted_body = to_bytes(deleted_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let deleted_json: serde_json::Value = serde_json::from_slice(&deleted_body).unwrap();
        assert_eq!(deleted_json["sessions"][0]["id"], "auth-deleted");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sessions?q=auth&model=claude-sonnet-4-6&status=active&sort=message_count&order=desc&limit=1&offset=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 2);
        assert_eq!(json["limit"], 1);
        assert_eq!(json["sessions"][0]["id"], "auth-b");
        assert_eq!(json["sessions"][0]["status"], "active");
        assert_eq!(json["sessions"][0]["model"], "claude-sonnet-4-6");
    }

    #[tokio::test]
    async fn get_session_prefers_unified_store_metadata() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "metadata-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("stored-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], session_id);
        assert_eq!(json["model"], "stored-model");
        assert!(json["created_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn patch_session_updates_cold_store_metadata() {
        let session_id = "patch-session";
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
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
                    .method("PATCH")
                    .uri(format!("/api/sessions/{session_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"title":"Patch Session Title","model":"patched-model"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let record = store
            .get_session(session_id)
            .await
            .unwrap()
            .expect("stored session");
        assert_eq!(record.model.as_deref(), Some("patched-model"));
        assert!(record
            .metadata_json
            .as_deref()
            .unwrap_or("")
            .contains("Patch Session Title"));
    }

    #[tokio::test]
    async fn session_messages_support_sequence_paging_and_limit_cap() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "message-page-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let messages: Vec<session::SessionMessage> = (0..1000)
            .map(|i| session::SessionMessage {
                stable_message_id: format!("page:{session_id}:{i}"),
                session_id: session_id.to_string(),
                sequence: i,
                role: if i % 2 == 0 { "user" } else { "assistant" }.to_string(),
                content_json: serde_json::json!([{"type":"text","text":format!("message {i}")}])
                    .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: i as u64,
            })
            .collect();
        store.insert_messages_batch(&messages).await.unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/messages?from_seq=990&limit=999"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total"], 1000);
        assert_eq!(json["limit"], 500);
        assert_eq!(json["from_seq"], 990);
        assert_eq!(json["next_seq"], 1000);
        assert_eq!(json["has_more"], false);
        assert_eq!(json["messages"].as_array().unwrap().len(), 10);
        assert_eq!(json["messages"][0]["id"], "page:message-page-session:990");
        assert_eq!(json["messages"][0]["sequence"], 990);
        assert_eq!(json["messages"][9]["sequence"], 999);
    }

    #[tokio::test]
    async fn delete_session_commits_tombstone_closes_admission_and_rejects_execution() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "cold-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
        let session_service = Arc::clone(&state.services.session);
        let app = api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let tombstone = store
            .get_session(session_id)
            .await
            .unwrap()
            .expect("logical deletion retains the durable Session tombstone");
        assert_eq!(tombstone.status, "deleted");
        let metadata: serde_json::Value =
            serde_json::from_str(tombstone.metadata_json.as_deref().unwrap_or("{}")).unwrap();
        assert_eq!(metadata["tombstone"]["kind"], "deleted");
        assert_eq!(metadata["tombstone"]["physical_delete"], false);

        let admission = store
            .get_session_input_admission(session_id)
            .await
            .unwrap()
            .expect("deleted Session retains its fenced admission generation");
        assert!(!admission.open);

        let execution_error = session_service
            .admit_input(harness_contract::turn::SessionInputEnvelope::text(
                session_id,
                harness_contract::turn::InputSourceKind::Webui,
                "must not execute after logical deletion",
            ))
            .await
            .expect_err("deleted Session must reject new execution input");
        assert!(
            execution_error.contains("no longer accepts input"),
            "unexpected admission rejection: {execution_error}"
        );
    }

    #[tokio::test]
    async fn delete_running_session_cancels_and_drains_before_committing_tombstone() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "running-delete-session";
        let execution_id = "running-delete-execution";
        store
            .create_session(&new_api_session_record(session_id, None))
            .await
            .unwrap();
        let state = test_state_with_store(Arc::clone(&store));
        let runtime = state.services.runtime.as_ref().expect("runtime service");
        let active = runtime.spawn_test_active_session_execution(
            session_id,
            "running-delete-turn",
            execution_id,
        );
        assert!(runtime
            .running_session_execution_indices()
            .iter()
            .any(|entry| entry.session_id == session_id
                && entry
                    .active_execution_ids
                    .contains(&execution_id.to_string())));

        let response = api_router(Arc::clone(&state))
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        tokio::time::timeout(std::time::Duration::from_secs(1), active)
            .await
            .expect("active turn should observe lifecycle cancellation")
            .expect("active turn test task");
        assert!(!runtime
            .running_session_execution_indices()
            .iter()
            .any(|entry| entry.session_id == session_id && !entry.active_execution_ids.is_empty()));
        assert_eq!(
            store
                .get_session(session_id)
                .await
                .unwrap()
                .expect("durable tombstone")
                .status,
            "deleted"
        );
    }

    #[tokio::test]
    async fn session_events_reads_unified_store_event_log() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "event-session";
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
                event_type: "message_appended".to_string(),
                event_json: serde_json::json!({
                    "type": "message_appended",
                    "sequence": 0,
                    "role": "user",
                })
                .to_string(),
                sequence: 0,
                created_at_ms: 1_234,
            })
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/events?from_seq=0&limit=10"
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
        assert_eq!(json["events"][0]["type"], "message_appended");
        assert_eq!(json["events"][0]["sequence"], 0);
        assert_eq!(json["events"][0]["payload"]["role"], "user");
        assert_eq!(json["has_more"], false);
    }

    #[tokio::test]
    async fn session_cancel_records_gateway_control_event() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "cancel-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let observer_id = "test.session-cancel";
        attach_test_writer(&state, session_id, observer_id).await;
        let mut projected = state.event_bus().subscribe(session_id, 4).await;
        let app = api_router(Arc::clone(&state));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/sessions/{session_id}/cancel"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", observer_id)
                    .body(Body::from(
                        serde_json::json!({
                            "reason": "test_cancel",
                            "cancellation_id": "cancel-test-request-1",
                            "requested_at_ms": 424242,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["cancellation_id"], "cancel-test-request-1");
        assert_eq!(json["session_id"], session_id);
        assert_eq!(json["status"], "already_terminal");
        assert_eq!(json["cause"], "user_requested");
        assert_eq!(json["actor_id"], "principal:local-human");
        assert!(json["requested_at_ms"].as_u64().is_some());
        assert!(json["effective_at_ms"].as_u64().is_some());
        assert!(json["journal_sequence"].as_u64().unwrap_or_default() > 0);
        assert!(json["projection_revision"].as_u64().unwrap_or_default() > 0);
        let projected = projected
            .recv()
            .await
            .expect("typed cancellation reaches the Session projection bus")
            .to_transport_value();
        assert_eq!(projected["type"], "TerminalDelivery");
        assert_eq!(
            projected["delivery"]["receipt"]["cancellation_id"],
            json["cancellation_id"]
        );
        assert_eq!(
            projected["delivery"]["receipt"]["journal_sequence"],
            json["journal_sequence"]
        );

        // A lost HTTP response is retried with the same cancellation id. The
        // durable final receipt is returned byte-for-byte instead of trying to
        // cancel again and changing the winner/effective timestamp.
        let retry = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/sessions/{session_id}/cancel"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", observer_id)
                    .body(Body::from(
                        serde_json::json!({
                            "reason": "test_cancel",
                            "cancellation_id": "cancel-test-request-1",
                            "requested_at_ms": 424242,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry.status(), StatusCode::OK);
        let retry_body = to_bytes(retry.into_body(), usize::MAX).await.unwrap();
        assert_eq!(retry_body, body);
        let retry_json: serde_json::Value = serde_json::from_slice(&retry_body).unwrap();
        assert_eq!(retry_json, json);
    }

    #[tokio::test]
    async fn session_baseline_replays_durable_cancellation_receipt() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "cancel-replay-session";
        store
            .create_session(&new_api_session_record(session_id, None))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let services = state.services.runtime.as_ref().unwrap().runtime_services();
        let receipt = services
            .commit_cancellation_receipt(harness_contract::turn::CancellationReceipt {
                cancellation_id: "cancel-replay-1".to_string(),
                session_id: session_id.to_string(),
                turn_id: "turn-replay-1".to_string(),
                execution_id: "execution-replay-1".to_string(),
                actor_id: "principal:local-human".to_string(),
                cause: harness_contract::turn::CancellationCause::UserRequested,
                reason: Some("user_requested".to_string()),
                requested_at_ms: 100,
                effective_at_ms: Some(101),
                status: harness_contract::turn::CancellationStatus::Cancelled,
                journal_sequence: 0,
                projection_revision: 0,
            })
            .unwrap();

        let page =
            message_routes::replay_materialized_terminal_events(state.as_ref(), session_id, 0, 20)
                .await;
        assert!(!page.requires_resync);
        assert_eq!(page.events.len(), 1);
        let replayed: serde_json::Value = serde_json::from_str(&page.events[0]).unwrap();
        assert_eq!(replayed["type"], "TerminalDelivery");
        assert_eq!(replayed["delivery"]["event"], "cancellation_committed");
        assert_eq!(
            replayed["delivery"]["receipt"]["cancellation_id"],
            receipt.cancellation_id
        );
        assert_eq!(replayed["runtime_commit_cursor"], receipt.journal_sequence);
        assert_eq!(page.last_cursor, Some(receipt.journal_sequence));
    }

    #[tokio::test]
    async fn requested_cancellation_recovers_without_process_local_turn_control() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "cancel-crash-session";
        let execution_id = "cancel-crash-execution";
        let turn_id = "cancel-crash-turn";
        store
            .create_session(&new_api_session_record(session_id, None))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let observer_id = "test.cancel-crash";
        attach_test_writer(&state, session_id, observer_id).await;
        let services = state.services.runtime.as_ref().unwrap().runtime_services();
        services.record_live_execution(session_id, execution_id.to_string(), turn_id.to_string());
        services
            .commit_cancellation_receipt(harness_contract::turn::CancellationReceipt {
                cancellation_id: "cancel-crash-id".to_string(),
                session_id: session_id.to_string(),
                turn_id: turn_id.to_string(),
                execution_id: execution_id.to_string(),
                actor_id: "principal:local-human".to_string(),
                cause: harness_contract::turn::CancellationCause::UserRequested,
                reason: Some("crash_recovery".to_string()),
                requested_at_ms: 700,
                effective_at_ms: None,
                status: harness_contract::turn::CancellationStatus::Requested,
                journal_sequence: 0,
                projection_revision: 0,
            })
            .unwrap();

        let response = api_router(Arc::clone(&state))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/sessions/{session_id}/cancel"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", observer_id)
                    .body(Body::from(
                        serde_json::json!({
                            "reason": "crash_recovery",
                            "cancellation_id": "cancel-crash-id",
                            "requested_at_ms": 700,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let receipt: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(receipt["status"], "cancelled");
        assert_eq!(
            services.execution_live(execution_id).unwrap().status,
            harness_contract::projection::ExecutionLiveStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn runtime_timeline_projection_is_paged() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-timeline-session";
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
                session::SessionDomainScope::Tool,
                "tool.started",
                serde_json::json!({"tool": "bash"}),
                10,
            ))
            .await
            .unwrap();
        store
            .append_session_domain_event(&session::SessionDomainEvent::new(
                session_id,
                1,
                session::SessionDomainScope::Memory,
                "memory.pulse.created",
                serde_json::json!({"candidates": 2}),
                11,
            ))
            .await
            .unwrap();

        let state = test_state_with_store(store);
        let app = api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&limit=1"
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
        assert_eq!(json["events"].as_array().unwrap().len(), 1);
        assert_eq!(json["events"][0]["kind"], "tool.started");
        assert_eq!(json["events"][0]["scope"], "tool");
        assert_eq!(json["next_cursor"], "v2:1:-:-");
        assert_eq!(json["has_more"], true);
        assert_eq!(json["degraded"], false);
    }

    #[tokio::test]
    async fn runtime_timeline_composite_cursor_does_not_skip_interleaved_sources() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-composite-cursor-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let future_ms = chrono::Utc::now()
            .timestamp_millis()
            .saturating_add(60_000)
            .max(0) as u64;
        for (sequence, created_at_ms) in [(0, 1), (1, future_ms)] {
            store
                .append_session_domain_event(&session::SessionDomainEvent::new(
                    session_id,
                    sequence,
                    session::SessionDomainScope::ApplicationTask,
                    format!("task.phase.{sequence}"),
                    serde_json::json!({"phase": sequence}),
                    created_at_ms,
                ))
                .await
                .unwrap();
        }

        let state = test_state_with_store(store);
        state
            .services
            .runtime_events
            .append_fixture(runtime::RuntimeEventInput {
                stream_id: session_id.to_string(),
                scope: runtime::RuntimeEventScope::SessionInput,
                kind: "runtime.session.observed".to_string(),
                status: Some("completed".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"session_id": session_id}),
            })
            .unwrap();
        let app = api_router(state);

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&limit=2"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first: serde_json::Value =
            serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(first["events"].as_array().unwrap().len(), 2);
        assert_eq!(first["events"][0]["kind"], "task.phase.0");
        assert_eq!(first["events"][1]["kind"], "runtime.session.observed");
        let cursor = first["next_cursor"].as_str().unwrap();

        let second = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&limit=2&cursor={cursor}"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let second: serde_json::Value =
            serde_json::from_slice(&to_bytes(second.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let events = second["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["kind"], "task.phase.1");
    }

    #[tokio::test]
    async fn runtime_timeline_projects_execution_graph_summary() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-execution_graph-summary-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let event = runtime::RuntimeEventInput {
            stream_id: session_id.to_string(),
            scope: runtime::RuntimeEventScope::ExecutionGraph,
            kind: "agent.execution_graph.reviewed".to_string(),
            status: Some("completed".to_string()),
            actor: Some("gateway-test".to_string()),
            refs: vec![
                runtime::RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: "graph-summary".to_string(),
                },
                runtime::RuntimeEventRef {
                    kind: "collaboration_board".to_string(),
                    id: "board-summary".to_string(),
                },
            ],
            payload: serde_json::json!({
                "board_id": "board-summary",
                "graph": {
                    "graph_id": "graph-summary",
                    "status": "completed",
                    "nodes": [
                        {"kind": "AgentTask", "node_id": "task-1"},
                        {"kind": "Synthesis", "node_id": "synthesis-board-summary"}
                    ]
                },
                "scorecard": {
                    "completion_rate": 1.0,
                    "synthesis_lift": 1.2,
                    "complementarity_score": 0.75,
                    "conflict_count": 1
                },
                "value_verdict": {
                    "positive_lift": true,
                    "continue_multi_agent": true,
                    "value_score": 70,
                    "reasons": ["positive_multi_agent_lift"]
                },
                "maintenance_candidates": [{"id": "candidate-summary"}]
            }),
        };

        let state = test_state_with_store(store);
        state.services.runtime_events.append_fixture(event).unwrap();
        let app = api_router(state);
        let response = app
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

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["execution_graph_summary"]["count"], 1);
        assert_eq!(
            json["execution_graph_summary"]["latest"]["graph_id"],
            "graph-summary"
        );
        assert_eq!(
            json["execution_graph_summary"]["latest"]["board_id"],
            "board-summary"
        );
        assert_eq!(
            json["execution_graph_summary"]["latest"]["completion_rate"],
            1.0
        );
        assert_eq!(
            json["execution_graph_summary"]["latest"]["value_verdict"]["positive_lift"],
            true
        );
        assert_eq!(json["execution_graph_summary"]["agent_tasks"], 1);
        assert_eq!(json["execution_graph_summary"]["memory_candidates"], 1);
        assert_eq!(json["execution_graph_summary"]["conflicts"], 1);
        assert_eq!(json["agent_value"]["status"], "review_required");
        assert_eq!(json["agent_value"]["recommendation"], "review_conflicts");
        assert_eq!(json["agent_value"]["policy_passed"], false);
        assert_eq!(json["agent_value"]["latest"]["agent_tasks"], 1);
        assert_eq!(json["agent_value"]["latest"]["value_score"], 70);
    }

    #[tokio::test]
    async fn runtime_timeline_resolves_session_terminal_to_canonical_graph_events() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-timeline-terminal-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let graph_id = "graph:terminal-session";
        let child_graph_id = "graph:terminal-session:team";
        state
            .services
            .runtime_events
            .append_fixture(runtime::RuntimeEventInput {
                stream_id: graph_id.to_string(),
                scope: runtime::RuntimeEventScope::ExecutionGraph,
                kind: "execution_graph.planned".to_string(),
                status: Some("running".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"graph": {"graph_id": graph_id, "status": "running"}}),
            })
            .unwrap();
        state
            .services
            .runtime_events
            .append_fixture(runtime::RuntimeEventInput {
                stream_id: child_graph_id.to_string(),
                scope: runtime::RuntimeEventScope::ExecutionGraph,
                kind: "execution_graph.planned".to_string(),
                status: Some("running".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "event": "planned",
                    "graph": {
                        "id": child_graph_id,
                        "node_statuses": {"researcher": "planned"},
                        "nodes": [{"kind": "agent_task", "id": "researcher"}]
                    }
                }),
            })
            .unwrap();
        state
            .services
            .runtime_events
            .append_fixture(runtime::RuntimeEventInput {
                stream_id: format!("execution-lineage:{graph_id}"),
                scope: runtime::RuntimeEventScope::Relation,
                kind: "execution.lineage.child_registered.v1".to_string(),
                status: Some("registered".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "parent_execution_id": graph_id,
                    "parent_node_id": "model",
                    "child_execution_id": child_graph_id,
                    "child_objective": "parallel review"
                }),
            })
            .unwrap();
        state
            .services
            .runtime_events
            .append_fixture(runtime::RuntimeEventInput {
                stream_id: "session-terminal:timeline-terminal".to_string(),
                scope: runtime::RuntimeEventScope::SessionInput,
                kind: "runtime.session.terminal_requested".to_string(),
                status: Some("pending_delivery".to_string()),
                actor: Some("test".to_string()),
                refs: vec![
                    runtime::RuntimeEventRef {
                        kind: "execution_graph".to_string(),
                        id: graph_id.to_string(),
                    },
                    runtime::RuntimeEventRef {
                        kind: "session".to_string(),
                        id: session_id.to_string(),
                    },
                ],
                payload: serde_json::json!({"session_id": session_id}),
            })
            .unwrap();

        let app = api_router(state);
        let response = app
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
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["kind"] == "execution_graph.planned"));
        assert_eq!(json["execution_graph_summary"]["count"], 1);
        assert_eq!(json["execution_graph_summary"]["agent_tasks"], 1);
        assert_eq!(
            json["execution_graph_summary"]["latest"]["graph_id"],
            child_graph_id
        );
        assert_eq!(
            json["agent_value"]["status"], "unproven",
            "operational graph visibility must not fabricate collaboration lift"
        );
    }

    #[tokio::test]
    async fn runtime_timeline_projects_health_summary() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "runtime-health-summary-session";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store.clone());
        store
            .append_session_domain_event(&session::SessionDomainEvent::new(
                session_id,
                0,
                session::SessionDomainScope::ApplicationTask,
                "task.started",
                serde_json::json!({"task_id": "task-health"}),
                10,
            ))
            .await
            .unwrap();
        store
            .append_session_domain_event(&session::SessionDomainEvent::new(
                session_id,
                1,
                session::SessionDomainScope::Policy,
                "runtime.policy.decided",
                serde_json::json!({
                    "agent_mode": "Parallel",
                    "requires_review": false,
                    "complexity": {
                        "level": "Complex",
                        "score": 72,
                        "signals": [{"name": "verification_required"}]
                    }
                }),
                11,
            ))
            .await
            .unwrap();
        state
            .services
            .runtime_events
            .append_fixture(runtime::RuntimeEventInput {
                stream_id: session_id.to_string(),
                scope: runtime::RuntimeEventScope::ExecutionGraph,
                kind: "agent.execution_graph.reviewed".to_string(),
                status: None,
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "value_verdict": {
                        "positive_lift": true,
                        "continue_multi_agent": true,
                        "value_score": 73,
                        "reasons": ["positive_multi_agent_lift"]
                    }
                }),
            })
            .unwrap();
        store
            .append_session_domain_event(&session::SessionDomainEvent::new(
                session_id,
                3,
                session::SessionDomainScope::ApplicationTask,
                "task.completed",
                serde_json::json!({"task_id": "task-health"}),
                13,
            ))
            .await
            .unwrap();

        let app = api_router(state);
        let response = app
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

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["health_summary"]["status"], "healthy");
        assert_eq!(json["health_summary"]["event_count"], 4);
        assert_eq!(json["health_summary"]["failed_events"], 0);
        assert_eq!(json["health_summary"]["degraded_events"], 0);
        assert_eq!(json["health_summary"]["open_tasks"], 0);
        assert_eq!(json["health_summary"]["positive_agent_lift"], true);
        assert_eq!(json["health_summary"]["latest_value_score"], 73);
        assert_eq!(
            json["health_summary"]["latest_policy"]["agent_mode"],
            "Parallel"
        );
        assert_eq!(json["health_summary"]["scope_counts"]["task"], 2);
        assert_eq!(json["health_summary"]["scope_counts"]["policy"], 1);
        assert_eq!(json["health_summary"]["scope_counts"]["execution_graph"], 1);
        assert_eq!(json["value_loop"]["status"], "incomplete");
        assert_eq!(json["value_loop"]["required_observed"], 3);
        assert_eq!(json["value_loop"]["missing_required_count"], 4);
        assert_eq!(json["value_loop"]["positive_agent_lift"], true);
    }

    #[tokio::test]
    async fn runtime_projection_degrades_missing_sources() {
        let app = api_router(test_state_with_config(serde_json::json!({})));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/timeline?session_id=missing-store")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["degraded"], true);
        assert_eq!(json["events"].as_array().unwrap().len(), 0);
        assert_eq!(json["execution_graph_summary"]["count"], 0);
        assert_eq!(json["health_summary"]["status"], "degraded");
        assert_eq!(json["health_summary"]["score"], 35);
        assert_eq!(json["health_summary"]["degraded_events"], 0);
        assert_eq!(
            json["health_summary"]["reasons"][0],
            "session store not available"
        );
        assert_eq!(json["value_loop"]["status"], "degraded");
        assert_eq!(json["value_loop"]["missing_required_count"], 7);
        assert_eq!(
            json["value_loop"]["reasons"][0],
            "session store not available"
        );
        assert_eq!(json["agent_value"]["status"], "degraded");
        assert_eq!(
            json["agent_value"]["recommendation"],
            "collect_execution_graph_review"
        );
    }

    #[tokio::test]
    async fn runtime_effective_config_exposes_default_control_policy() {
        let root = test_temp_dir("runtime-control-default");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        let app = api_router(test_state_with_workspace(workspace, config_home));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/config/effective")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["source"], "default");
        assert_eq!(json["scenario"], "coding");
        assert_eq!(json["control_policy"]["enabled"], true);
        assert_eq!(json["control_policy"]["agent"]["max_parallel_agents"], 42);
        assert_eq!(
            json["control_policy"]["task"]["max_failures_before_review"],
            2
        );
        assert!(json["control_policy"]["task"].get("thresholds").is_none());
        assert!(json["warnings"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn runtime_effective_config_exposes_configured_control_policy() {
        let root = test_temp_dir("runtime-control-config");
        let workspace = root.join("workspace");
        let config_home = root.join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::write(
            config_home.join("config.yaml"),
            r#"
runtime:
  scenario: office
  control:
    enabled: false
    agent:
      max_parallel_agents: 2
      min_collaboration_score: 77
    context:
      yolo_budget_tokens: 7000
"#,
        )
        .unwrap();

        let app = api_router(test_state_with_workspace(workspace, config_home));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/config/effective")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["source"], "config");
        assert_eq!(json["scenario"], "office");
        assert_eq!(json["control_policy"]["enabled"], false);
        assert_eq!(json["control_policy"]["agent"]["max_parallel_agents"], 2);
        assert_eq!(
            json["control_policy"]["agent"]["min_collaboration_score"],
            77
        );
        assert_eq!(
            json["control_policy"]["context"]["yolo_budget_tokens"],
            7000
        );

        let _ = std::fs::remove_dir_all(root);
    }
