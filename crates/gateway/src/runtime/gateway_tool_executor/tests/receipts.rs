    #[test]
    fn session_message_preview_is_bounded_and_structured() {
        let preview = session_message_preview(
            r#"[{"type":"text","text":"hello context"},{"type":"tool_use","name":"read_file"}]"#,
            20,
        );

        assert!(preview.starts_with("hello context"));
        assert!(preview.contains("[tool"));
        assert!(preview.chars().count() <= 23);
    }

    #[test]
    fn exact_session_message_pages_restore_every_block_with_stable_digest() {
        let message = session::SessionMessage {
            stable_message_id: "message-stable".to_string(),
            session_id: "session-current".to_string(),
            sequence: 42,
            role: "assistant".to_string(),
            content_json: serde_json::json!([
                {"type":"text","text":"first"},
                {"type":"tool_use","name":"read_file","input":{"path":"README.md"}},
                {"type":"text","text":"last"}
            ])
            .to_string(),
            blocks_count: 3,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 7,
        };
        let first = exact_session_message_page(&message, 0, 2, ContextRetrieveScope::Current)
            .expect("first page");
        let second = exact_session_message_page(
            &message,
            first["next_request"]["block_cursor"]
                .as_u64()
                .expect("cursor") as usize,
            2,
            ContextRetrieveScope::Current,
        )
        .expect("second page");

        assert_eq!(first["selected_count"], 2);
        assert_eq!(second["selected_count"], 1);
        assert_eq!(first["message_digest"], second["message_digest"]);
        assert!(second["next_request"].is_null());
        assert_eq!(second["selected"][0]["content"]["text"], "last");
    }

    #[tokio::test]
    async fn runtime_config_view_is_read_only_and_never_returns_credentials() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_config_view".to_string(),
                description: Some("safe config view".to_string()),
                input_schema: json!({"type":"object","additionalProperties":false}),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);

        let output = executor
            .execute("runtime_config_view", r#"{"detail":"summary"}"#)
            .await
            .expect("safe configuration view");
        let value: serde_json::Value = serde_json::from_str(&output).expect("config view json");
        assert_eq!(value["kind"], "runtime.config_view");
        assert!(value.get("config_path").is_none());
        assert!(value.get("headers").is_none());
        assert!(value.get("env").is_none());
    }

    #[tokio::test]
    async fn resource_capability_query_is_explicit_and_bounded() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_resource_capabilities".to_string(),
                description: Some("resource capability query".to_string()),
                input_schema: json!({"type":"object","additionalProperties":true}),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);

        let output = executor
            .execute(
                "runtime_resource_capabilities",
                r#"{"resource_kind":"pdf","mime":"application/pdf","intent":"extract document text"}"#,
            )
            .await
            .expect("resource capability query");
        let value: serde_json::Value = serde_json::from_str(&output).expect("resource json");
        assert_eq!(value["kind"], "runtime.resource_capabilities");
        assert!(value["candidate_tools"]
            .as_array()
            .is_some_and(|tools| tools.len() <= 3));
        assert!(value["installed_skills"]
            .as_array()
            .is_some_and(|items| items.len() <= 4));
    }

    #[test]
    fn resource_capability_keywords_include_kind_specific_parsers() {
        let pdf = resource_capability_keywords("pdf", Some("application/pdf"), "extract text");
        assert!(pdf.contains(&"pdftotext".to_string()));
        assert!(pdf.contains(&"pdfinfo".to_string()));

        let audio = resource_capability_keywords("audio", Some("audio/mpeg"), "inspect");
        assert!(audio.contains(&"ffprobe".to_string()));
    }

    #[test]
    fn runtime_tool_permission_metadata_drives_safety_classification() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "company_catalog_lookup".to_string(),
                description: Some("read company catalog".to_string()),
                input_schema: json!({"type":"object"}),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);

        assert_eq!(
            executor.classify_tool_safety("company_catalog_lookup", "{}"),
            Some(runtime::ToolSafetyCategory::ReadOnly)
        );
    }

    #[test]
    fn read_many_only_mints_complete_success_children() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: "read-many".to_string(),
                tool_name: "read_many".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
        );
        request.observation_wave_sequence = 12;
        let complete = |name: &str, content: &str| {
            serde_json::json!({
                "type": "text",
                "truncated": false,
                "file": {
                    "filePath": services.workspace_root().join(name),
                    "content": content,
                    "numLines": 1,
                    "startLine": 1,
                    "totalLines": 1,
                    "sha256": format!("{:x}", Sha256::digest(content.as_bytes()))
                }
            })
        };
        let mut windowed = complete("partial.txt", "partial");
        windowed["truncated"] = serde_json::Value::Bool(true);
        let output = serde_json::json!({
            "results": [
                {"status": "success", "output": complete("complete.txt", "complete")},
                {"status": "success", "output": windowed},
                {"status": "error", "output": complete("failed.txt", "failed")}
            ]
        });
        let facts = gateway_observed_evidence(
            &executor,
            &request,
            &output.to_string(),
            "gateway-tool:read-many",
        );
        assert_eq!(facts.len(), 1);
        assert!(matches!(
            &facts[0].target,
            harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                if scope.path.workspace_relative_path == "complete.txt"
        ));
    }

    #[test]
    fn discovery_and_network_receipts_require_actual_complete_outputs() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let request_for = |tool_name: &str| {
            let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
                &runtime::tool_dispatch::ToolRequest {
                    tool_use_id: tool_name.to_string(),
                    tool_name: tool_name.to_string(),
                    input: r#"{"url":"https://request.invalid"}"#.to_string(),
                    depends_on: Vec::new(),
                },
            );
            request.observation_wave_sequence = 3;
            request
        };
        let discovery_root = services.workspace_root().join("discovery");
        std::fs::create_dir_all(&discovery_root).expect("discovery fixture");
        let glob = serde_json::json!({
            "basePath": discovery_root,
            "pattern": "**/*.rs",
            "truncated": false,
            "scanComplete": true,
            "filenames": []
        });
        assert_eq!(
            gateway_observed_evidence(
                &executor,
                &request_for("glob_search"),
                &glob.to_string(),
                "glob"
            )
            .len(),
            1
        );
        let mut truncated = glob;
        truncated["scanComplete"] = serde_json::Value::Bool(false);
        assert!(gateway_observed_evidence(
            &executor,
            &request_for("glob_search"),
            &truncated.to_string(),
            "glob-truncated"
        )
        .is_empty());
        let truncated_complete = serde_json::json!({
            "basePath": discovery_root,
            "pattern": "**/*.rs",
            "truncated": true,
            "scanComplete": true,
            "filenames": []
        });
        assert!(gateway_observed_evidence(
            &executor,
            &request_for("glob_search"),
            &truncated_complete.to_string(),
            "glob-truncated-complete",
        )
        .is_empty());

        let fetch = serde_json::json!({
            "url": "https://actual.example/final",
            "code": 200,
            "bytes": 4,
            "result": "done",
            "networkPolicy": {"denied": false, "requires_approval": false}
        });
        let network = gateway_observed_evidence(
            &executor,
            &request_for("web_fetch"),
            &fetch.to_string(),
            "fetch",
        );
        assert!(matches!(
            &network[0].target,
            harness_contract::context::EvidenceTargetIdentity::Network { endpoint }
                if endpoint == "https://actual.example/final"
        ));
        let mut denied_fetch = fetch;
        denied_fetch["networkPolicy"]["denied"] = serde_json::Value::Bool(true);
        assert!(gateway_observed_evidence(
            &executor,
            &request_for("web_fetch"),
            &denied_fetch.to_string(),
            "fetch-denied",
        )
        .is_empty());

        for (tool_name, unsafe_output) in [
            (
                "grep_search",
                serde_json::json!({"basePath": services.workspace_root(), "scanComplete": true}),
            ),
            (
                "workspace_snapshot",
                serde_json::json!({"resolvedRoots": [services.workspace_root()], "scanComplete": true}),
            ),
            (
                "web_search",
                serde_json::json!({"results": [{"content": [{"url": "https://hit.example"}]}]}),
            ),
        ] {
            assert!(gateway_observed_evidence(
                &executor,
                &request_for(tool_name),
                &unsafe_output.to_string(),
                tool_name,
            )
            .is_empty());
        }
    }

    #[test]
    fn patch_transaction_uses_each_committed_digest_and_unknown_tools_fail_closed() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: "patch".to_string(),
                tool_name: "apply_patch_transaction".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
        );
        request.observation_wave_sequence = 4;
        let output = serde_json::json!({
            "appliedCount": 2,
            "applied": [
                {"path": "a.rs", "resolvedPath": services.workspace_root().join("a.rs"), "previousSha256": format!("{:x}", Sha256::digest(b"before-a")), "sha256": format!("{:x}", Sha256::digest(b"a"))},
                {"path": "b.rs", "resolvedPath": services.workspace_root().join("b.rs"), "previousSha256": format!("{:x}", Sha256::digest(b"before-b")), "sha256": format!("{:x}", Sha256::digest(b"b"))}
            ]
        });
        assert_eq!(
            gateway_observed_evidence(&executor, &request, &output.to_string(), "patch").len(),
            2
        );
        request.tool_name = "unproven_writer".to_string();
        assert!(
            gateway_observed_evidence(&executor, &request, &output.to_string(), "unknown")
                .is_empty()
        );
    }

    #[test]
    fn actual_patch_transaction_output_carries_preimage_into_gateway_receipt() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        std::fs::create_dir_all(services.workspace_root().join("src")).expect("source directory");
        std::fs::write(services.workspace_root().join("src/lib.rs"), "before\n")
            .expect("source preimage");
        let output = tools::mutation_plan::apply_mutations(
            &tools::path_policy::WorkspacePathPolicy::new(services.workspace_root()),
            tools::mutation_plan::MutationApplyInput {
                edits: vec![tools::mutation_plan::MutationEdit {
                    path: "src/lib.rs".to_string(),
                    old_string: "before".to_string(),
                    new_string: "after".to_string(),
                    replace_all: Some(false),
                }],
                expected_hashes: Default::default(),
            },
        )
        .expect("canonical patch transaction");
        let output = serde_json::to_string(&output).expect("canonical output JSON");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: "actual-patch".to_string(),
                tool_name: "apply_patch_transaction".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
        );
        request.observation_wave_sequence = 11;

        let facts = gateway_observed_evidence(&executor, &request, &output, "actual-patch-receipt");
        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].workspace_prior_state,
            Some(harness_contract::context::WorkspacePriorState::Existing {
                sha256: format!("{:x}", Sha256::digest(b"before\n")),
            })
        );
        let expected_after = format!("{:x}", Sha256::digest(b"after\n"));
        assert!(matches!(
            &facts[0].target,
            harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                if scope.path.observed_revision_or_digest.as_deref()
                    == Some(expected_after.as_str())
        ));
    }

    #[test]
    fn notebook_edit_mints_digest_from_the_exact_committed_document() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: "notebook".to_string(),
                tool_name: "notebook_edit".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
        );
        request.observation_wave_sequence = 5;
        let content = r#"{"cells":[]}"#;
        let output = serde_json::json!({
            "notebook_path": services.workspace_root().join("analysis.ipynb"),
            "updated_file": content,
            "original_file": r#"{"cells":[{"cell_type":"code"}]}"#,
            "error": null
        });
        let facts = gateway_observed_evidence(&executor, &request, &output.to_string(), "notebook");
        assert_eq!(facts.len(), 1);
        let expected = format!("{:x}", Sha256::digest(content.as_bytes()));
        assert!(matches!(
            &facts[0].target,
            harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                if scope.path.observed_revision_or_digest.as_deref() == Some(expected.as_str())
        ));
    }
