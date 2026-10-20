#[tokio::test]
async fn c4_root_file_discovery_and_publication_need_no_program_or_team() {
    use sha2::{Digest, Sha256};
    let temporary = tempfile::tempdir().unwrap();
    let workspace = temporary.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let body = b"nonempty_lines=3\n";
    std::fs::write(workspace.join("result.txt"), body).unwrap();
    let services = runtime::RuntimeServices::builder(temporary.path().join("home"), &workspace)
        .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
        .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
        .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(temporary.path().join("artifacts"))))
        .build()
        .unwrap();
    let catalog = GatewayToolRegistry::builtin()
        .extend_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
        .unwrap();
    let host = Arc::new(ToolHost::new(
        "c4-publication", workspace.clone(), ToolHostSnapshot::new(
            Arc::new(catalog), Arc::new(tools::lsp_client::LspRegistry::new()), None,
        ),
    ));
    let executor = GatewayToolExecutor::from_tool_host(None, false, host);
    executor.bind_runtime_services(&services).unwrap();
    let discovery: serde_json::Value = serde_json::from_str(
        &executor.execute_search_tool(json!({"query":"artifact_publish","max_results":5})).unwrap(),
    ).unwrap();
    assert!(discovery["matches"].as_array().unwrap().iter().any(|entry| entry.as_str() == Some("artifact_publish")));
    let binding = RuntimeToolExecutionBinding {
        tool_lease: None, action_id: None, session_id: Some("c4-file-session"), authorized_scopes: &[],
        memory_context: None, reality_context: None, model_lease: Some("fixture"), parent_execution: None,
        execution_decision: None, permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
    };
    let input = json!({"source":"file","path":workspace.join("result.txt").to_str().unwrap(),
        "sha256":format!("{:x}",Sha256::digest(body)),"media_type":"text/plain"});
    let publication: serde_json::Value = serde_json::from_str(
        &executor.execute_content_publication("artifact_publish", input.clone(), binding).await.unwrap(),
    ).unwrap();
    let artifact = services.artifact_store().resolve(publication["content_ref"].as_str().unwrap()).unwrap();
    assert_eq!(services.artifact_store().read(&artifact, "session:c4-file-session", None).await.unwrap(), body);
    assert!(publication.get("kind").is_none() && publication.get("title").is_none());
    let mut stale = input;
    stale["sha256"] = json!("0".repeat(64));
    assert!(executor.execute_content_publication("artifact_publish", stale, binding).await.is_err());
    let mut anonymous = binding;
    anonymous.session_id = None;
    assert!(executor.execute_content_publication("artifact_publish", json!({"source":"artifact",
        "content_ref":artifact.selector,"sha256":artifact.sha256}), anonymous).await.is_err());
    assert_eq!(std::fs::read(workspace.join("result.txt")).unwrap(), body);
}

#[tokio::test]
async fn c4_stored_body_publication_keeps_original_scope_and_exact_block_negatives() {
    publication_creation_identity_gateway_case().await;
}
