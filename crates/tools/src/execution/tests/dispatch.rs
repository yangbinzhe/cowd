// Canonical dispatch, filesystem, search, cache, and mutation coverage.
#[test]
fn vision_analyze_prepares_png_payload_end_to_end() {
    let _guard = env_lock();
    let root = temp_path("vision");
    fs::create_dir_all(&root).unwrap();
    // Minimal valid PNG signature + IHDR chunk; run_vision_analyze only
    // needs the file to exist and the extension to classify the media
    // type, so the bytes are a real image container.
    let png = root.join("sample.png");
    fs::write(
        &png,
        [
            0x89u8, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, // signature
            0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52, // IHDR length + tag
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, // 1x1
            0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, // bit depth etc.
            0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82, // IEND
        ],
    )
    .expect("write sample png");
    let result = execute_in_workspace(
        &root,
        "vision_analyze",
        &json!({"image_path": "sample.png", "prompt": "describe this image"}),
    )
    .expect("vision_analyze succeeds");
    let value: serde_json::Value =
        serde_json::from_str(&result).expect("vision_analyze returns JSON");
    assert_eq!(value["tool"], "vision_analyze");
    assert_eq!(value["status"], "prepared");
    assert_eq!(value["media_type"], "image/png");
    assert_eq!(value["size_bytes"], 45);
    assert!(value["image_base64"]
        .as_str()
        .is_some_and(|encoded| !encoded.is_empty()));
    let _ = fs::remove_dir_all(&root);
}
#[test]
fn ast_grep_search_filters_by_language_extension() {
    let _guard = env_lock();
    let root = temp_path("ast-grep");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("a.rs"), "fn foo() {}\n").unwrap();
    fs::write(root.join("b.py"), "def foo():\n    pass\n").unwrap();
    let result = execute_in_workspace(
        &root,
        "ast_grep_search",
        &json!({"pattern": "fn foo", "language": "rust"}),
    )
    .expect("ast_grep_search succeeds");
    let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(parsed["match_count"], 1);
    assert!(parsed["matches"][0]["path"]
        .as_str()
        .unwrap()
        .ends_with("a.rs"));
}

fn run_git(cwd: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .status()
        .unwrap_or_else(|error| panic!("git {} failed: {error}", args.join(" ")));
    assert!(
        status.success(),
        "git {} exited with {status}",
        args.join(" ")
    );
}

fn init_git_repo(path: &Path) {
    std::fs::create_dir_all(path).expect("create repo");
    run_git(path, &["init", "--quiet", "-b", "main"]);
    run_git(path, &["config", "user.email", "tests@example.com"]);
    run_git(path, &["config", "user.name", "Tools Tests"]);
    std::fs::write(path.join("README.md"), "initial\n").expect("write readme");
    run_git(path, &["add", "README.md"]);
    run_git(path, &["commit", "-m", "initial commit", "--quiet"]);
}

fn commit_file(path: &Path, file: &str, contents: &str, message: &str) {
    std::fs::write(path.join(file), contents).expect("write file");
    run_git(path, &["add", file]);
    run_git(path, &["commit", "-m", message, "--quiet"]);
}

struct HttpResponse {
    status: u16,
    reason: &'static str,
    content_type: &'static str,
    body: String,
}

impl HttpResponse {
    fn html(status: u16, reason: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            reason,
            content_type: "text/html; charset=utf-8",
            body: body.into(),
        }
    }

    fn text(status: u16, reason: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            reason,
            content_type: "text/plain; charset=utf-8",
            body: body.into(),
        }
    }
}

struct TestServer {
    addr: SocketAddr,
}

impl TestServer {
    fn spawn(handler: Arc<dyn Fn(&str) -> HttpResponse + Send + Sync + 'static>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        thread::spawn(move || {
            for stream in listener.incoming().take(8) {
                let Ok(mut stream) = stream else {
                    continue;
                };
                let mut buffer = [0_u8; 4096];
                let Ok(read) = stream.read(&mut buffer) else {
                    continue;
                };
                let request = String::from_utf8_lossy(&buffer[..read]);
                let request_line = request.lines().next().unwrap_or_default();
                let response = handler(request_line);
                let payload = format!(
                        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response.status,
                        response.reason,
                        response.content_type,
                        response.body.len(),
                        response.body
                    );
                let _ = stream.write_all(payload.as_bytes());
            }
        });
        Self { addr }
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }
}

#[test]
fn exposes_mvp_tools() {
    let names = mvp_tool_specs()
        .into_iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>();
    assert!(names.contains(&"bash"));
    assert!(names.contains(&"read_file"));
    assert!(names.contains(&"web_fetch"));
    assert!(names.contains(&"web_search"));
    assert!(names.contains(&"todo_write"));
    assert!(names.contains(&"tool_search"));
    assert!(names.contains(&"notebook_edit"));
    assert!(names.contains(&"sleep"));
    assert!(names.contains(&"send_user_message"));
    assert!(names.contains(&"config"));
    assert!(names.contains(&"enter_plan_mode"));
    assert!(names.contains(&"exit_plan_mode"));
    assert!(names.contains(&"structured_output"));
    assert!(names.contains(&"repl"));
    assert!(names.contains(&"power_shell"));
    for removed in [
        "Agent",
        "TaskCreate",
        "RunTaskPacket",
        "TaskGet",
        "TaskList",
        "TaskStop",
        "TaskUpdate",
        "TaskOutput",
        "WorkerCreate",
        "WorkerObserve",
        "WorkerAwaitReady",
        "WorkerSendPrompt",
        "WorkerRestart",
        "WorkerTerminate",
        "TeamCreate",
        "TeamDelete",
        "CronCreate",
        "CronDelete",
        "CronList",
    ] {
        assert!(
            !names.contains(&removed),
            "control-plane tool {removed} must not be exposed by tools"
        );
    }
}

#[test]
fn rejects_unknown_tool_names() {
    let error = execute_tool("nope", &json!({})).expect_err("tool should be rejected");
    assert!(error.contains("unsupported tool"));
}

#[test]
fn permission_mode_from_plugin_rejects_invalid_inputs() {
    let unknown_permission =
        permission_mode_from_plugin("admin").expect_err("unknown plugin permission should fail");
    assert!(unknown_permission.contains("unsupported plugin permission: admin"));

    let empty_permission =
        permission_mode_from_plugin("").expect_err("empty plugin permission should fail");
    assert!(empty_permission.contains("unsupported plugin permission: "));
}

#[test]
fn runtime_tools_extend_registry_definitions_permissions_and_search() {
    let registry = Arc::new(
        ToolCatalog::builtin()
            .with_runtime_tools(vec![crate::RuntimeToolDefinition {
                name: "mcp__demo__echo".to_string(),
                description: Some("Echo text from the demo MCP server".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": { "text": { "type": "string" } },
                    "additionalProperties": false
                }),
                required_permission: PermissionMode::ReadOnly,
                effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
                    resolver_id: "runtime.external_read".to_string(),
                    resolver_version: 1,
                },
            }])
            .expect("runtime tools should register"),
    );

    let allowed = registry
        .normalize_allowed_tools(&["mcp__demo__echo".to_string()])
        .expect("runtime tool should be allow-listable")
        .expect("allow-list should be populated");
    assert!(allowed.contains("mcp__demo__echo"));

    let definitions = registry.definitions(Some(&allowed));
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name, "mcp__demo__echo");

    let permissions = registry
        .permission_specs(Some(&allowed))
        .expect("runtime tool permissions should resolve");
    assert_eq!(
        permissions,
        vec![("mcp__demo__echo".to_string(), PermissionMode::ReadOnly)]
    );

    let host = crate::ToolHost::new(
        "test-workspace",
        std::env::current_dir().unwrap(),
        crate::ToolHostSnapshot::new(
            Arc::clone(&registry),
            Arc::new(crate::lsp_client::LspRegistry::new()),
            None,
        ),
    );
    let search = host.pin_snapshot().search("demo echo", 5);
    let output = serde_json::to_value(search).expect("search output should serialize");
    assert_eq!(output["activation_candidates"][0], "mcp__demo__echo");
    assert_eq!(output["descriptors"][0]["source"], "runtime");
    assert_eq!(output["catalog_revision"], 1);
}

#[test]
fn web_fetch_returns_prompt_aware_summary() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous_private_network = std::env::var_os("COWD_ALLOW_PRIVATE_NETWORK");
    std::env::set_var("COWD_ALLOW_PRIVATE_NETWORK", "1");
    let server = TestServer::spawn(Arc::new(|request_line: &str| {
        assert!(request_line.starts_with("GET /page "));
        HttpResponse::html(
                200,
                "OK",
                "<html><head><title>Ignored</title></head><body><h1>Test Page</h1><p>Hello <b>world</b> from local server.</p></body></html>",
            )
    }));

    let result = execute_tool(
        "web_fetch",
        &json!({
            "url": format!("http://{}/page", server.addr()),
            "prompt": "Summarize this page"
        }),
    )
    .expect("WebFetch should succeed");

    let output: serde_json::Value = serde_json::from_str(&result).expect("valid json");
    assert_eq!(output["code"], 200);
    let summary = output["result"].as_str().expect("result string");
    assert!(summary.contains("Fetched"));
    assert!(summary.contains("Test Page"));
    assert!(summary.contains("Hello world from local server"));

    let titled = execute_tool(
        "web_fetch",
        &json!({
            "url": format!("http://{}/page", server.addr()),
            "prompt": "What is the page title?"
        }),
    )
    .expect("WebFetch title query should succeed");
    let titled_output: serde_json::Value = serde_json::from_str(&titled).expect("valid json");
    let titled_summary = titled_output["result"].as_str().expect("result string");
    assert!(titled_summary.contains("Title: Ignored"));
    match previous_private_network {
        Some(value) => std::env::set_var("COWD_ALLOW_PRIVATE_NETWORK", value),
        None => std::env::remove_var("COWD_ALLOW_PRIVATE_NETWORK"),
    }
}

#[test]
fn web_fetch_supports_plain_text_and_rejects_invalid_url() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous_private_network = std::env::var_os("COWD_ALLOW_PRIVATE_NETWORK");
    std::env::set_var("COWD_ALLOW_PRIVATE_NETWORK", "1");
    let server = TestServer::spawn(Arc::new(|request_line: &str| {
        assert!(request_line.starts_with("GET /plain "));
        HttpResponse::text(200, "OK", "plain text response")
    }));

    let result = execute_tool(
        "web_fetch",
        &json!({
            "url": format!("http://{}/plain", server.addr()),
            "prompt": "Show me the content"
        }),
    )
    .expect("WebFetch should succeed for text content");

    let output: serde_json::Value = serde_json::from_str(&result).expect("valid json");
    assert_eq!(output["url"], format!("http://{}/plain", server.addr()));
    assert!(output["result"]
        .as_str()
        .expect("result")
        .contains("plain text response"));

    let error = execute_tool(
        "web_fetch",
        &json!({
            "url": "not a url",
            "prompt": "Summarize"
        }),
    )
    .expect_err("invalid URL should fail");
    assert!(error.contains("relative URL without a base") || error.contains("invalid"));
    match previous_private_network {
        Some(value) => std::env::set_var("COWD_ALLOW_PRIVATE_NETWORK", value),
        None => std::env::remove_var("COWD_ALLOW_PRIVATE_NETWORK"),
    }
}

#[test]
fn web_search_extracts_and_filters_results() {
    // Serialize env-var mutation so this test cannot race with the sibling
    // web_search_handles_generic_links_and_invalid_base_url test that also
    // sets COWD_WEB_SEARCH_BASE_URL. Without the lock, parallel test
    // runners can interleave the set/remove calls and cause assertion
    // failures on the wrong port.
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let server = TestServer::spawn(Arc::new(|request_line: &str| {
        assert!(request_line.contains("GET /search?q=rust+web+search "));
        HttpResponse::html(
            200,
            "OK",
            r#"
                <html><body>
                  <a class="result__a" href="https://docs.rs/reqwest">Reqwest docs</a>
                  <a class="result__a" href="https://example.com/blocked">Blocked result</a>
                </body></html>
                "#,
        )
    }));

    std::env::set_var(
        "COWD_WEB_SEARCH_BASE_URL",
        format!("http://{}/search", server.addr()),
    );
    let result = execute_tool(
        "web_search",
        &json!({
            "query": "rust web search",
            "allowed_domains": ["https://DOCS.rs/"],
            "blocked_domains": ["HTTPS://EXAMPLE.COM"]
        }),
    )
    .expect("WebSearch should succeed");
    std::env::remove_var("COWD_WEB_SEARCH_BASE_URL");

    let output: serde_json::Value = serde_json::from_str(&result).expect("valid json");
    assert_eq!(output["query"], "rust web search");
    let results = output["results"].as_array().expect("results array");
    let search_result = results
        .iter()
        .find(|item| item.get("content").is_some())
        .expect("search result block present");
    let content = search_result["content"].as_array().expect("content array");
    assert_eq!(content.len(), 1);
    assert_eq!(content[0]["title"], "Reqwest docs");
    assert_eq!(content[0]["url"], "https://docs.rs/reqwest");
}

#[test]
fn web_search_handles_generic_links_and_invalid_base_url() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let server = TestServer::spawn(Arc::new(|request_line: &str| {
        assert!(request_line.contains("GET /fallback?q=generic+links "));
        HttpResponse::html(
            200,
            "OK",
            r#"
                <html><body>
                  <a href="https://example.com/one">Example One</a>
                  <a href="https://example.com/one">Duplicate Example One</a>
                  <a href="https://docs.rs/tokio">Tokio Docs</a>
                  <a href="https://r.search.yahoo.com/route/RU=https%3A%2F%2Fopenai.com%2Fcodex%2F/RK=2/">OpenAI Codex</a>
                </body></html>
                "#,
        )
    }));

    std::env::set_var(
        "COWD_WEB_SEARCH_BASE_URL",
        format!("http://{}/fallback", server.addr()),
    );
    let result = execute_tool(
        "web_search",
        &json!({
            "query": "generic links"
        }),
    )
    .expect("WebSearch fallback parsing should succeed");
    std::env::remove_var("COWD_WEB_SEARCH_BASE_URL");

    let output: serde_json::Value = serde_json::from_str(&result).expect("valid json");
    let results = output["results"].as_array().expect("results array");
    let search_result = results
        .iter()
        .find(|item| item.get("content").is_some())
        .expect("search result block present");
    let content = search_result["content"].as_array().expect("content array");
    assert_eq!(content.len(), 3);
    assert_eq!(content[0]["url"], "https://example.com/one");
    assert_eq!(content[1]["url"], "https://docs.rs/tokio");
    assert_eq!(content[2]["url"], "https://openai.com/codex");

    std::env::set_var("COWD_WEB_SEARCH_BASE_URL", "://bad-base-url");
    let error = execute_tool("web_search", &json!({ "query": "generic links" }))
        .expect_err("invalid base URL should fail");
    std::env::remove_var("COWD_WEB_SEARCH_BASE_URL");
    assert!(error.contains("relative URL without a base") || error.contains("empty host"));
}

#[test]
fn web_search_rejects_search_backend_navigation_as_false_evidence() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let server = TestServer::spawn(Arc::new(|request_line: &str| {
        assert!(request_line.contains("GET /self?q=no+evidence "));
        HttpResponse::html(
            200,
            "OK",
            r#"
                <html><body>
                  <a href="https://duckduckgo.com/about">About DuckDuckGo</a>
                  <a href="https://html.duckduckgo.com/settings">Settings</a>
                  <a href="https://search.brave.com/settings">Brave settings</a>
                  <a href="https://search.yahoo.com/preferences">Yahoo settings</a>
                </body></html>
                "#,
        )
    }));

    std::env::set_var(
        "COWD_WEB_SEARCH_BASE_URL",
        format!("http://{}/self", server.addr()),
    );
    let error = execute_tool("web_search", &json!({ "query": "no evidence" }))
        .expect_err("search backend navigation is not external evidence");
    std::env::remove_var("COWD_WEB_SEARCH_BASE_URL");
    assert!(error.contains("no usable external results"));
}

#[test]
fn todo_write_persists_and_returns_previous_state() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let path = temp_path("todos.json");
    std::env::set_var("COWD_TODO_STORE", &path);

    let first = execute_tool(
        "todo_write",
        &json!({
            "todos": [
                {"content": "Add tool", "activeForm": "Adding tool", "status": "in_progress"},
                {"content": "Run tests", "activeForm": "Running tests", "status": "pending"}
            ]
        }),
    )
    .expect("TodoWrite should succeed");
    let first_output: serde_json::Value = serde_json::from_str(&first).expect("valid json");
    assert_eq!(first_output["oldTodos"].as_array().expect("array").len(), 0);

    let second = execute_tool(
        "todo_write",
        &json!({
            "todos": [
                {"content": "Add tool", "activeForm": "Adding tool", "status": "completed"},
                {"content": "Run tests", "activeForm": "Running tests", "status": "completed"},
                {"content": "Verify", "activeForm": "Verifying", "status": "completed"}
            ]
        }),
    )
    .expect("TodoWrite should succeed");
    std::env::remove_var("COWD_TODO_STORE");
    let _ = std::fs::remove_file(path);

    let second_output: serde_json::Value = serde_json::from_str(&second).expect("valid json");
    assert_eq!(
        second_output["oldTodos"].as_array().expect("array").len(),
        2
    );
    assert_eq!(
        second_output["newTodos"].as_array().expect("array").len(),
        3
    );
    assert!(second_output["verificationNudgeNeeded"].is_null());
}

#[test]
fn todo_write_rejects_invalid_payloads_and_sets_verification_nudge() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let path = temp_path("todos-errors.json");
    std::env::set_var("COWD_TODO_STORE", &path);

    let empty =
        execute_tool("todo_write", &json!({ "todos": [] })).expect_err("empty todos should fail");
    assert!(empty.contains("todos must not be empty"));

    // Multiple in_progress items are now allowed for parallel workflows
    let _multi_active = execute_tool(
        "todo_write",
        &json!({
            "todos": [
                {"content": "One", "activeForm": "Doing one", "status": "in_progress"},
                {"content": "Two", "activeForm": "Doing two", "status": "in_progress"}
            ]
        }),
    )
    .expect("multiple in-progress todos should succeed");

    let blank_content = execute_tool(
        "todo_write",
        &json!({
            "todos": [
                {"content": "   ", "activeForm": "Doing it", "status": "pending"}
            ]
        }),
    )
    .expect_err("blank content should fail");
    assert!(blank_content.contains("todo content must not be empty"));

    let nudge = execute_tool(
        "todo_write",
        &json!({
            "todos": [
                {"content": "Write tests", "activeForm": "Writing tests", "status": "completed"},
                {"content": "Fix errors", "activeForm": "Fixing errors", "status": "completed"},
                {"content": "Ship branch", "activeForm": "Shipping branch", "status": "completed"}
            ]
        }),
    )
    .expect("completed todos should succeed");
    std::env::remove_var("COWD_TODO_STORE");
    let _ = fs::remove_file(path);

    let output: serde_json::Value = serde_json::from_str(&nudge).expect("valid json");
    assert_eq!(output["verificationNudgeNeeded"], true);
}

#[test]
fn tool_search_supports_keyword_and_select_queries() {
    let keyword = execute_tool(
        "tool_search",
        &json!({"query": "web current", "max_results": 3}),
    )
    .expect("ToolSearch should succeed");
    let keyword_output: serde_json::Value = serde_json::from_str(&keyword).expect("valid json");
    let matches = keyword_output["activation_candidates"]
        .as_array()
        .expect("activation candidates");
    assert!(matches.iter().any(|value| value == "web_search"));

    let selected = execute_tool(
        "tool_search",
        &json!({"query": "select:WebSearch,ToolSearch"}),
    )
    .expect("ToolSearch should succeed");
    let selected_output: serde_json::Value = serde_json::from_str(&selected).expect("valid json");
    let selected_matches = selected_output["activation_candidates"]
        .as_array()
        .expect("activation candidates");
    assert_eq!(selected_matches.len(), 2);
    assert!(selected_matches.iter().any(|value| value == "web_search"));
    assert!(selected_matches.iter().any(|value| value == "tool_search"));

    let source_search = execute_tool(
        "tool_search",
        &json!({"query": "select:grep_search,grep_many,read_file"}),
    )
    .expect("ToolSearch should expose executable source tools");
    let source_output: serde_json::Value =
        serde_json::from_str(&source_search).expect("valid json");
    assert_eq!(
        source_output["activation_candidates"],
        json!(["grep_search", "grep_many", "read_file"])
    );

    let exact_grep = execute_tool(
        "tool_search",
        &json!({"query": "grep_search", "max_results": 1}),
    )
    .expect("focused grep discovery should succeed");
    let exact_grep_output: serde_json::Value =
        serde_json::from_str(&exact_grep).expect("valid json");
    assert_eq!(
        exact_grep_output["activation_candidates"],
        json!(["grep_search"])
    );

    let removed = execute_tool(
        "tool_search",
        &json!({"query": "select:Agent,WorkerCreate"}),
    )
    .expect("ToolSearch should ignore removed control-plane tools");
    let removed_output: serde_json::Value = serde_json::from_str(&removed).expect("valid json");
    assert!(
        removed_output["activation_candidates"]
            .as_array()
            .expect("activation candidates")
            .is_empty(),
        "removed control-plane tools must not be searchable"
    );
}

#[test]
fn lane_event_schema_serializes_to_canonical_names() {
    let cases = [
        (LaneEventName::Started, "lane.started"),
        (LaneEventName::Ready, "lane.ready"),
        (LaneEventName::PromptMisdelivery, "lane.prompt_misdelivery"),
        (LaneEventName::Blocked, "lane.blocked"),
        (LaneEventName::Red, "lane.red"),
        (LaneEventName::Green, "lane.green"),
        (LaneEventName::CommitCreated, "lane.commit.created"),
        (LaneEventName::PrOpened, "lane.pr.opened"),
        (LaneEventName::MergeReady, "lane.merge.ready"),
        (LaneEventName::Finished, "lane.finished"),
        (LaneEventName::Failed, "lane.failed"),
        (
            LaneEventName::BranchStaleAgainstMain,
            "branch.stale_against_main",
        ),
        (
            LaneEventName::BranchWorkspaceMismatch,
            "branch.workspace_mismatch",
        ),
    ];

    for (event, expected) in cases {
        assert_eq!(
            serde_json::to_value(event).expect("serialize lane event"),
            json!(expected)
        );
    }
}

#[test]
fn agent_control_plane_tool_is_not_executable_from_tools() {
    let error = execute_tool(
        "Agent",
        &json!({
            "description": "Inspect branch",
            "prompt": "Inspect"
        }),
    )
    .expect_err("control-plane Agent tool should not be executable from tools");
    assert!(error.contains("unsupported tool"));
}

#[test]
fn notebook_edit_replaces_inserts_and_deletes_cells() {
    let path = temp_path("notebook.ipynb");
    let root = path.parent().expect("notebook parent");
    std::fs::write(
            &path,
            r#"{
  "cells": [
    {"cell_type": "code", "id": "cell-a", "metadata": {}, "source": ["print(1)\n"], "outputs": [], "execution_count": null}
  ],
  "metadata": {"kernelspec": {"language": "python"}},
  "nbformat": 4,
  "nbformat_minor": 5
}"#,
        )
        .expect("write notebook");

    let replaced = execute_in_workspace(
        root,
        "notebook_edit",
        &json!({
            "notebook_path": path.display().to_string(),
            "cell_id": "cell-a",
            "new_source": "print(2)\n",
            "edit_mode": "replace"
        }),
    )
    .expect("NotebookEdit replace should succeed");
    let replaced_output: serde_json::Value = serde_json::from_str(&replaced).expect("json");
    assert_eq!(replaced_output["cell_id"], "cell-a");
    assert_eq!(replaced_output["cell_type"], "code");

    let inserted = execute_in_workspace(
        root,
        "notebook_edit",
        &json!({
            "notebook_path": path.display().to_string(),
            "cell_id": "cell-a",
            "new_source": "# heading\n",
            "cell_type": "markdown",
            "edit_mode": "insert"
        }),
    )
    .expect("NotebookEdit insert should succeed");
    let inserted_output: serde_json::Value = serde_json::from_str(&inserted).expect("json");
    assert_eq!(inserted_output["cell_type"], "markdown");
    let appended = execute_in_workspace(
        root,
        "notebook_edit",
        &json!({
            "notebook_path": path.display().to_string(),
            "new_source": "print(3)\n",
            "edit_mode": "insert"
        }),
    )
    .expect("NotebookEdit append should succeed");
    let appended_output: serde_json::Value = serde_json::from_str(&appended).expect("json");
    assert_eq!(appended_output["cell_type"], "code");

    let deleted = execute_in_workspace(
        root,
        "notebook_edit",
        &json!({
            "notebook_path": path.display().to_string(),
            "cell_id": "cell-a",
            "edit_mode": "delete"
        }),
    )
    .expect("NotebookEdit delete should succeed without new_source");
    let deleted_output: serde_json::Value = serde_json::from_str(&deleted).expect("json");
    assert!(deleted_output["cell_type"].is_null());
    assert_eq!(deleted_output["new_source"], "");

    let final_notebook: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("read notebook"))
            .expect("valid notebook json");
    let cells = final_notebook["cells"].as_array().expect("cells array");
    assert_eq!(cells.len(), 2);
    assert_eq!(cells[0]["cell_type"], "markdown");
    assert!(cells[0].get("outputs").is_none());
    assert_eq!(cells[1]["cell_type"], "code");
    assert_eq!(cells[1]["source"][0], "print(3)\n");
    let _ = std::fs::remove_file(path);
}

#[test]
fn notebook_edit_rejects_invalid_inputs() {
    let text_path = temp_path("notebook.txt");
    let root = text_path.parent().expect("notebook parent");
    fs::write(&text_path, "not a notebook").expect("write text file");
    let wrong_extension = execute_in_workspace(
        root,
        "notebook_edit",
        &json!({
            "notebook_path": text_path.display().to_string(),
            "new_source": "print(1)\n"
        }),
    )
    .expect_err("non-ipynb file should fail");
    assert!(wrong_extension.contains("Jupyter notebook"));
    let _ = fs::remove_file(&text_path);

    let empty_notebook = temp_path("empty.ipynb");
    fs::write(
            &empty_notebook,
            r#"{"cells":[],"metadata":{"kernelspec":{"language":"python"}},"nbformat":4,"nbformat_minor":5}"#,
        )
        .expect("write empty notebook");

    let missing_source = execute_in_workspace(
        root,
        "notebook_edit",
        &json!({
            "notebook_path": empty_notebook.display().to_string(),
            "edit_mode": "insert"
        }),
    )
    .expect_err("insert without source should fail");
    assert!(missing_source.contains("new_source is required"));

    let missing_cell = execute_in_workspace(
        root,
        "notebook_edit",
        &json!({
            "notebook_path": empty_notebook.display().to_string(),
            "edit_mode": "delete"
        }),
    )
    .expect_err("delete on empty notebook should fail");
    assert!(missing_cell.contains("Notebook has no cells to edit"));
    let _ = fs::remove_file(empty_notebook);
}

#[test]
fn skill_install_tools_bind_reviewed_digest_and_workspace_source() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("skill-lifecycle");
    let source = root.join("reviewed-skill");
    let config = root.join("config");
    fs::create_dir_all(&source).expect("skill source");
    fs::write(
            source.join("SKILL.md"),
            "---\nname: reviewed-skill\ndescription: reviewed model tool fixture\nlicense: MIT\n---\nUse typed evidence.\n",
        )
        .expect("skill prompt");
    std::env::set_var("COWD_CONFIG_HOME", &config);

    let plan = execute_in_workspace(
        &root,
        "skill_install_plan",
        &json!({"source": "reviewed-skill"}),
    )
    .expect("plan");
    let plan: serde_json::Value = serde_json::from_str(&plan).expect("plan json");
    let digest = plan["plan"]["package_digest"]
        .as_str()
        .expect("package digest");
    assert!(plan["plan"]["installable"].as_bool().unwrap_or(false));

    let mismatch = execute_in_workspace(
            &root,
            "skill_install_commit",
            &json!({
                "source": "reviewed-skill",
                "expected_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            }),
        )
        .expect_err("unreviewed digest must fail");
    assert!(mismatch.contains("changed after review"));

    let receipt = execute_in_workspace(
        &root,
        "skill_install_commit",
        &json!({"source": "reviewed-skill", "expected_digest": digest}),
    )
    .expect("commit");
    let receipt: serde_json::Value = serde_json::from_str(&receipt).expect("receipt json");
    assert_eq!(receipt["capabilities_granted"], json!([]));
    assert_eq!(receipt["execution"], "none");
    assert_eq!(receipt["receipt"]["package_digest"], digest);

    let status = execute_in_workspace(
        &root,
        "skill_status",
        &json!({"skill_id": "reviewed-skill"}),
    )
    .expect("status");
    let status: serde_json::Value = serde_json::from_str(&status).expect("status json");
    assert_eq!(status["active"]["revision"], digest);

    let outside = temp_path("outside-skill");
    fs::create_dir_all(&outside).expect("outside source");
    fs::write(
        outside.join("SKILL.md"),
        "---\nname: outside\ndescription: outside fixture\n---\n",
    )
    .expect("outside prompt");
    let rejected = execute_in_workspace(
        &root,
        "skill_install_plan",
        &json!({"source": outside.display().to_string()}),
    )
    .expect_err("model local sources outside the workspace must fail");
    assert!(rejected.contains("limited to the current workspace"));

    std::env::remove_var("COWD_CONFIG_HOME");
    fs::remove_dir_all(&outside).expect("outside cleanup");
    let store = config.join("skill-store/v1");
    make_tree_writable_for_test(&store);
    fs::remove_dir_all(&root).expect("workspace cleanup");
}

#[test]
fn bash_tool_reports_success_exit_failure_timeout_and_background() {
    let root = temp_path("bash-tool-cwd");
    fs::create_dir_all(&root).expect("bash cwd should exist");
    let cwd = root.to_string_lossy().to_string();

    let success = execute_in_workspace(
            &root,
            "bash",
            &json!({ "command": "printf 'hello'", "cwd": cwd, "dangerouslyDisableSandbox": true, "workspaceAccess": "read_write" }),
        )
        .expect("bash should succeed");
    let success_output: serde_json::Value = serde_json::from_str(&success).expect("json");
    assert_eq!(success_output["stdout"], "hello");
    assert_eq!(success_output["interrupted"], false);

    let failure = execute_in_workspace(
            &root,
            "bash",
            &json!({ "command": "printf 'oops' >&2; exit 7", "cwd": cwd, "dangerouslyDisableSandbox": true, "workspaceAccess": "read_write" }),
        )
        .expect("bash failure should still return structured output");
    let failure_output: serde_json::Value = serde_json::from_str(&failure).expect("json");
    assert_eq!(failure_output["returnCodeInterpretation"], "exit_code:7");
    assert!(failure_output["stderr"]
        .as_str()
        .expect("stderr")
        .contains("oops"));

    let timeout = execute_in_workspace(
            &root,
            "bash",
            &json!({ "command": "sleep 1", "cwd": cwd, "timeout_ms": 10, "dangerouslyDisableSandbox": true, "workspaceAccess": "read_write" }),
        )
        .expect("bash timeout should return output");
    let timeout_output: serde_json::Value = serde_json::from_str(&timeout).expect("json");
    assert_eq!(timeout_output["interrupted"], true);
    assert_eq!(timeout_output["returnCodeInterpretation"], "timeout");
    assert!(timeout_output["stderr"]
        .as_str()
        .expect("stderr")
        .contains("Command exceeded timeout"));

    let background = execute_in_workspace(
            &root,
            "bash",
            &json!({ "command": "sleep 1", "cwd": cwd, "run_in_background": true, "dangerouslyDisableSandbox": true, "workspaceAccess": "read_write" }),
        )
        .expect_err("PID-only background execution is not a model capability (S-03)");
    assert!(background.contains("S-03"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn bash_workspace_tests_are_blocked_when_branch_is_behind_main() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("workspace-test-preflight");
    let original_dir = std::env::current_dir().expect("cwd");
    init_git_repo(&root);
    run_git(&root, &["checkout", "-b", "feature/stale-tests"]);
    run_git(&root, &["checkout", "main"]);
    commit_file(
        &root,
        "hotfix.txt",
        "fix from main\n",
        "fix: unblock workspace tests",
    );
    run_git(&root, &["checkout", "feature/stale-tests"]);
    std::env::set_current_dir(&root).expect("set cwd");

    let output = execute_tool(
        "bash",
        &json!({ "command": "cargo test --workspace --all-targets" }),
    )
    .expect("preflight should return structured output");
    let output_json: serde_json::Value = serde_json::from_str(&output).expect("json");
    assert_eq!(
        output_json["returnCodeInterpretation"],
        "preflight_blocked:branch_divergence"
    );
    assert!(output_json["stderr"]
        .as_str()
        .expect("stderr")
        .contains("branch divergence detected before workspace tests"));
    assert_eq!(
        output_json["structuredContent"][0]["event"],
        "branch.stale_against_main"
    );
    assert_eq!(
        output_json["structuredContent"][0]["failureClass"],
        "branch_divergence"
    );
    assert_eq!(
        output_json["structuredContent"][0]["data"]["missingCommits"][0],
        "fix: unblock workspace tests"
    );

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn bash_targeted_tests_skip_branch_preflight() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("targeted-test-no-preflight");
    let original_dir = std::env::current_dir().expect("cwd");
    init_git_repo(&root);
    run_git(&root, &["checkout", "-b", "feature/targeted-tests"]);
    run_git(&root, &["checkout", "main"]);
    commit_file(
        &root,
        "hotfix.txt",
        "fix from main\n",
        "fix: only broad tests should block",
    );
    run_git(&root, &["checkout", "feature/targeted-tests"]);
    std::env::set_current_dir(&root).expect("set cwd");

    let output = execute_tool(
        "bash",
        &json!({ "command": "printf 'targeted ok'; cargo test -p runtime stale_branch" }),
    )
    .expect("targeted commands should still execute");
    let output_json: serde_json::Value = serde_json::from_str(&output).expect("json");
    assert_ne!(
        output_json["returnCodeInterpretation"],
        "preflight_blocked:branch_divergence"
    );

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn file_tools_cover_read_write_and_edit_behaviors() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("fs-suite");
    fs::create_dir_all(&root).expect("create root");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&root).expect("set cwd");

    let write_create = execute_tool(
        "write_file",
        &json!({ "path": "nested/demo.txt", "content": "alpha\nbeta\nalpha\n" }),
    )
    .expect("write create should succeed");
    let write_create_output: serde_json::Value = serde_json::from_str(&write_create).expect("json");
    assert_eq!(write_create_output["type"], "create");
    assert!(root.join("nested/demo.txt").exists());

    let write_update = execute_tool(
        "write_file",
        &json!({ "path": "nested/demo.txt", "content": "alpha\nbeta\ngamma\n" }),
    )
    .expect("write update should succeed");
    let write_update_output: serde_json::Value = serde_json::from_str(&write_update).expect("json");
    assert_eq!(write_update_output["type"], "update");
    assert_eq!(write_update_output["originalFile"], "alpha\nbeta\nalpha\n");

    let read_full = execute_tool("read_file", &json!({ "path": "nested/demo.txt" }))
        .expect("read full should succeed");
    let read_full_output: serde_json::Value = serde_json::from_str(&read_full).expect("json");
    assert_eq!(read_full_output["file"]["content"], "alpha\nbeta\ngamma");
    assert_eq!(read_full_output["file"]["startLine"], 1);
    assert_eq!(read_full_output["file"]["byteLength"], 17);
    assert_eq!(read_full_output["file"]["endsWithNewline"], true);
    assert_eq!(
        read_full_output["file"]["sha256"],
        "4fdbc441ea7b546100e086ac1e4fc5ae6749b7314311c99db05be450eca12996"
    );

    let read_slice = execute_tool(
        "read_file",
        &json!({ "path": "nested/demo.txt", "offset": 1, "limit": 1 }),
    )
    .expect("read slice should succeed");
    let read_slice_output: serde_json::Value = serde_json::from_str(&read_slice).expect("json");
    assert_eq!(read_slice_output["file"]["content"], "beta");
    assert_eq!(read_slice_output["file"]["startLine"], 2);

    let read_past_end = execute_tool(
        "read_file",
        &json!({ "path": "nested/demo.txt", "offset": 50 }),
    )
    .expect("read past EOF should succeed");
    let read_past_end_output: serde_json::Value =
        serde_json::from_str(&read_past_end).expect("json");
    assert_eq!(read_past_end_output["file"]["content"], "");
    assert_eq!(read_past_end_output["file"]["startLine"], 4);

    let read_error = execute_tool("read_file", &json!({ "path": "missing.txt" }))
        .expect_err("missing file should fail");
    assert!(!read_error.is_empty());

    let edit_once = execute_tool(
        "edit_file",
        &json!({ "path": "nested/demo.txt", "old_string": "alpha", "new_string": "omega" }),
    )
    .expect("single edit should succeed");
    let edit_once_output: serde_json::Value = serde_json::from_str(&edit_once).expect("json");
    assert_eq!(edit_once_output["replaceAll"], false);
    assert_eq!(
        fs::read_to_string(root.join("nested/demo.txt")).expect("read file"),
        "omega\nbeta\ngamma\n"
    );

    execute_tool(
        "write_file",
        &json!({ "path": "nested/demo.txt", "content": "alpha\nbeta\nalpha\n" }),
    )
    .expect("reset file");
    let edit_all = execute_tool(
        "edit_file",
        &json!({
            "path": "nested/demo.txt",
            "old_string": "alpha",
            "new_string": "omega",
            "replace_all": true
        }),
    )
    .expect("replace all should succeed");
    let edit_all_output: serde_json::Value = serde_json::from_str(&edit_all).expect("json");
    assert_eq!(edit_all_output["replaceAll"], true);
    assert_eq!(
        fs::read_to_string(root.join("nested/demo.txt")).expect("read file"),
        "omega\nbeta\nomega\n"
    );

    let edit_same = execute_tool(
        "edit_file",
        &json!({ "path": "nested/demo.txt", "old_string": "omega", "new_string": "omega" }),
    )
    .expect_err("identical old/new should fail");
    assert!(edit_same.contains("must differ"));

    let edit_missing = execute_tool(
        "edit_file",
        &json!({ "path": "nested/demo.txt", "old_string": "missing", "new_string": "omega" }),
    )
    .expect_err("missing substring should fail");
    assert!(edit_missing.contains("old_string not found"));

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn glob_and_grep_tools_cover_success_and_errors() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("search-suite");
    fs::create_dir_all(root.join("nested")).expect("create root");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&root).expect("set cwd");

    fs::write(
        root.join("nested/lib.rs"),
        "fn main() {}\nlet alpha = 1;\nlet alpha = 2;\n",
    )
    .expect("write rust file");
    fs::write(root.join("nested/notes.txt"), "alpha\nbeta\n").expect("write txt file");

    let globbed = execute_tool("glob_search", &json!({ "pattern": "nested/*.rs" }))
        .expect("glob should succeed");
    let globbed_output: serde_json::Value = serde_json::from_str(&globbed).expect("json");
    assert_eq!(globbed_output["numFiles"], 1);
    assert!(globbed_output["filenames"][0]
        .as_str()
        .expect("filename")
        .ends_with("nested/lib.rs"));

    let glob_error = execute_tool("glob_search", &json!({ "pattern": "[" }))
        .expect_err("invalid glob should fail");
    assert!(!glob_error.is_empty());

    let grep_content = execute_tool(
        "grep_search",
        &json!({
            "pattern": "alpha",
            "path": "nested",
            "glob": "*.rs",
            "output_mode": "content",
            "-n": true,
            "head_limit": 1,
            "offset": 1
        }),
    )
    .expect("grep content should succeed");
    let grep_content_output: serde_json::Value = serde_json::from_str(&grep_content).expect("json");
    assert_eq!(grep_content_output["numFiles"], 0);
    assert!(grep_content_output["appliedLimit"].is_null());
    assert_eq!(grep_content_output["appliedOffset"], 1);
    assert!(grep_content_output["content"]
        .as_str()
        .expect("content")
        .contains("let alpha = 2;"));

    let grep_count = execute_tool(
        "grep_search",
        &json!({ "pattern": "alpha", "path": "nested", "output_mode": "count" }),
    )
    .expect("grep count should succeed");
    let grep_count_output: serde_json::Value = serde_json::from_str(&grep_count).expect("json");
    assert_eq!(grep_count_output["numMatches"], 3);

    let grep_error = execute_tool(
        "grep_search",
        &json!({ "pattern": "(alpha", "path": "nested" }),
    )
    .expect_err("invalid regex should fail");
    assert!(!grep_error.is_empty());

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn read_many_preserves_order_and_reports_partial_failures() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("read-many-suite");
    fs::create_dir_all(root.join("nested")).expect("create root");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&root).expect("set cwd");

    fs::write(root.join("nested/a.txt"), "alpha\nbeta\n").expect("write a");
    fs::write(root.join("nested/b.txt"), "gamma\n").expect("write b");

    let output = execute_tool(
        "read_many",
        &json!({
            "files": [
                { "path": "nested/a.txt", "offset": 1, "limit": 1 },
                { "path": "missing.txt" },
                { "path": "nested/b.txt" }
            ],
            "max_concurrency": 2
        }),
    )
    .expect("read_many should return structured batch output");
    let value: serde_json::Value = serde_json::from_str(&output).expect("json");

    assert_eq!(value["type"], "read_many");
    assert_eq!(value["count"], 3);
    assert_eq!(value["successCount"], 2);
    assert_eq!(value["errorCount"], 1);
    assert_eq!(value["partialSuccess"], true);
    assert_eq!(value["results"][0]["index"], 0);
    assert_eq!(value["results"][0]["status"], "success");
    assert_eq!(value["results"][0]["output"]["file"]["content"], "beta");
    assert_eq!(value["results"][1]["index"], 1);
    assert_eq!(value["results"][1]["status"], "error");
    assert_eq!(value["results"][2]["index"], 2);
    assert_eq!(value["results"][2]["output"]["file"]["content"], "gamma");

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn read_tool_cache_hits_and_invalidates_after_write() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("tool-cache-suite");
    fs::create_dir_all(root.join("src")).expect("create root");
    let file = root.join("src/lib.rs");
    fs::write(&file, "alpha\n").expect("write file");
    let host = crate::ToolHost::builtin("tool-cache-suite", &root);
    let lease = host.pin_snapshot();

    super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
        .expect("first read");
    super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
        .expect("second read");
    let stats = super::execute_with_lease(&lease, "tool_cache_stats", &json!({})).expect("stats");
    let stats_value: serde_json::Value = serde_json::from_str(&stats).expect("json");
    assert_eq!(stats_value["hits"], 1);
    assert_eq!(stats_value["entries"], 1);

    super::execute_with_lease(
        &lease,
        "write_file",
        &json!({ "path": "src/lib.rs", "content": "omega\n" }),
    )
    .expect("write invalidates cache");
    let stats = super::execute_with_lease(&lease, "tool_cache_stats", &json!({}))
        .expect("stats after write");
    let stats_value: serde_json::Value = serde_json::from_str(&stats).expect("json");
    assert_eq!(stats_value["invalidations"], 1);
    assert_eq!(stats_value["scopeEpochs"], 1);
    let reread = super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
        .expect("reread should not use stale cache");
    assert!(reread.contains("omega"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn read_tool_cache_misses_after_external_file_change() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("tool-cache-external-suite");
    fs::create_dir_all(root.join("src")).expect("create root");
    let file = root.join("src/lib.rs");
    fs::write(&file, "alpha\n").expect("write file");
    let host = crate::ToolHost::builtin("tool-cache-external-suite", &root);
    let lease = host.pin_snapshot();

    let first = super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
        .expect("first");
    assert!(first.contains("alpha"));
    fs::write(&file, "omega\n").expect("external write");
    let second = super::execute_with_lease(&lease, "read_file", &json!({ "path": "src/lib.rs" }))
        .expect("second");
    assert!(second.contains("omega"));
    let stats = super::execute_with_lease(&lease, "tool_cache_stats", &json!({})).expect("stats");
    let stats_value: serde_json::Value = serde_json::from_str(&stats).expect("json");
    assert_eq!(stats_value["hits"], 0);
    assert_eq!(stats_value["misses"], 2);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn auto_checkpoint_can_guard_mutations_when_enabled() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("auto-checkpoint-suite");
    fs::create_dir_all(root.join("src")).expect("create root");
    fs::write(root.join("src/lib.rs"), "alpha\n").expect("write file");
    let original_dir = std::env::current_dir().expect("cwd");
    let original_auto_checkpoint = std::env::var("COWD_AUTO_CHECKPOINT").ok();
    std::env::set_current_dir(&root).expect("set cwd");
    std::env::set_var("COWD_AUTO_CHECKPOINT", "1");

    execute_tool(
        "write_file",
        &json!({ "path": "src/lib.rs", "content": "omega\n" }),
    )
    .expect("write should create checkpoint first");
    let checkpoints = execute_tool("checkpoint_list", &json!({})).expect("list checkpoints");
    let value: serde_json::Value = serde_json::from_str(&checkpoints).expect("json");
    let labels = value["checkpoints"]
        .as_array()
        .expect("checkpoints")
        .iter()
        .filter_map(|checkpoint| checkpoint["label"].as_str())
        .collect::<Vec<_>>();
    assert!(labels.contains(&"auto-before-write_file"));

    match original_auto_checkpoint {
        Some(value) => std::env::set_var("COWD_AUTO_CHECKPOINT", value),
        None => std::env::remove_var("COWD_AUTO_CHECKPOINT"),
    }
    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn mutation_preview_and_apply_patch_transaction_cover_conflict_and_success() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("mutation-transaction-suite");
    fs::create_dir_all(root.join("src")).expect("create root");
    let file = root.join("src/lib.rs");
    fs::write(&file, "alpha\nbeta\n").expect("write file");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&root).expect("set cwd");

    let preview = execute_tool(
        "mutation_preview",
        &json!({
            "edits": [
                { "path": "src/lib.rs", "old_string": "alpha", "new_string": "omega" }
            ]
        }),
    )
    .expect("mutation preview should succeed");
    let preview_value: serde_json::Value = serde_json::from_str(&preview).expect("json");
    assert_eq!(preview_value["type"], "mutation_preview");
    assert_eq!(preview_value["conflictCount"], 0);
    let expected_hash = preview_value["files"][0]["expectedHash"]
        .as_str()
        .expect("expected hash")
        .to_string();

    let applied = execute_tool(
        "apply_patch_transaction",
        &json!({
            "edits": [
                { "path": "src/lib.rs", "old_string": "alpha", "new_string": "omega" }
            ],
            "expected_hashes": {
                "src/lib.rs": expected_hash
            }
        }),
    )
    .expect("apply should succeed");
    let applied_value: serde_json::Value = serde_json::from_str(&applied).expect("json");
    assert_eq!(applied_value["type"], "mutation_apply");
    assert_eq!(
        fs::read_to_string(&file).expect("read file"),
        "omega\nbeta\n"
    );

    fs::write(&file, "alpha\nalpha\n").expect("reset file");
    let conflict = execute_tool(
        "patch_plan",
        &json!({
            "edits": [
                { "path": "src/lib.rs", "old_string": "alpha", "new_string": "omega" }
            ]
        }),
    )
    .expect("patch plan should return conflict report");
    let conflict_value: serde_json::Value = serde_json::from_str(&conflict).expect("json");
    assert_eq!(conflict_value["conflictCount"], 1);

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn apply_patch_transaction_rejects_stale_expected_hash() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("mutation-stale-suite");
    fs::create_dir_all(root.join("src")).expect("create root");
    let file = root.join("src/lib.rs");
    fs::write(&file, "alpha\n").expect("write file");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&root).expect("set cwd");

    let err = execute_tool(
        "apply_patch_transaction",
        &json!({
            "edits": [
                { "path": "src/lib.rs", "old_string": "alpha", "new_string": "omega" }
            ],
            "expected_hashes": {
                "src/lib.rs": "stale"
            }
        }),
    )
    .expect_err("stale hash should fail");
    assert!(err.contains("changed before apply"));
    assert_eq!(fs::read_to_string(&file).expect("read file"), "alpha\n");

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn checkpoint_tools_create_diff_and_restore_workspace_files() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("checkpoint-suite");
    let unrelated_cwd = temp_path("checkpoint-unrelated-cwd");
    fs::create_dir_all(root.join("src")).expect("create root");
    fs::create_dir_all(&unrelated_cwd).expect("create unrelated cwd");
    let file = root.join("src/lib.rs");
    fs::write(&file, "alpha\n").expect("write file");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&unrelated_cwd).expect("set unrelated cwd");

    let created = execute_in_workspace(
        &root,
        "checkpoint_create",
        &json!({ "label": "before edit" }),
    )
    .expect("checkpoint create should succeed");
    let created_value: serde_json::Value = serde_json::from_str(&created).expect("json");
    let checkpoint_id = created_value["id"]
        .as_str()
        .expect("checkpoint id")
        .to_string();
    assert!(root.join(".cowd/checkpoints").is_dir());
    assert!(
        !unrelated_cwd.join(".cowd/checkpoints").exists(),
        "checkpoint state must remain in the leased workspace rather than process cwd"
    );

    fs::write(&file, "omega\n").expect("mutate file");
    fs::write(root.join("src/new.rs"), "new\n").expect("add file");
    fs::remove_file(&file).expect("delete file");
    let diff = execute_in_workspace(&root, "checkpoint_diff", &json!({ "id": checkpoint_id }))
        .expect("checkpoint diff should succeed");
    let diff_value: serde_json::Value = serde_json::from_str(&diff).expect("json");
    assert_eq!(diff_value["type"], "checkpoint_diff");
    assert!(diff_value["deletedFiles"]
        .as_array()
        .expect("deleted files")
        .iter()
        .any(|file| file.as_str() == Some("src/lib.rs")));
    assert!(diff_value["addedFiles"]
        .as_array()
        .expect("added files")
        .iter()
        .any(|file| file.as_str() == Some("src/new.rs")));

    let checkpoint_id = created_value["id"].as_str().expect("checkpoint id");
    execute_in_workspace(&root, "checkpoint_restore", &json!({ "id": checkpoint_id }))
        .expect("checkpoint restore should succeed");
    assert_eq!(fs::read_to_string(&file).expect("read restored"), "alpha\n");
    assert!(!root.join("src/new.rs").exists());

    let listed =
        execute_in_workspace(&root, "checkpoint_list", &json!({})).expect("checkpoint list");
    let listed_value: serde_json::Value = serde_json::from_str(&listed).expect("json");
    assert_eq!(listed_value["type"], "checkpoint_list");
    assert!(!listed_value["checkpoints"]
        .as_array()
        .expect("checkpoints")
        .is_empty());

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(unrelated_cwd);
}

#[test]
fn glob_many_and_grep_many_preserve_order() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("search-many-suite");
    fs::create_dir_all(root.join("nested")).expect("create root");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&root).expect("set cwd");

    fs::write(root.join("nested/lib.rs"), "let alpha = 1;\n").expect("write rs");
    fs::write(root.join("nested/notes.md"), "alpha\nbeta\n").expect("write md");

    let globbed = execute_tool(
        "glob_many",
        &json!({
            "patterns": [
                { "pattern": "nested/*.rs" },
                { "pattern": "[" },
                { "pattern": "nested/*.md" }
            ],
            "max_concurrency": 2
        }),
    )
    .expect("glob_many should return structured batch output");
    let globbed_value: serde_json::Value = serde_json::from_str(&globbed).expect("json");
    assert_eq!(globbed_value["successCount"], 2);
    assert_eq!(globbed_value["errorCount"], 1);
    assert_eq!(globbed_value["results"][0]["index"], 0);
    assert_eq!(globbed_value["results"][1]["status"], "error");
    assert_eq!(globbed_value["results"][2]["index"], 2);

    let grepped = execute_tool(
        "grep_many",
        &json!({
            "searches": [
                { "pattern": "alpha", "path": "nested", "glob": "*.rs" },
                { "pattern": "(alpha", "path": "nested" },
                { "pattern": "beta", "path": "nested", "output_mode": "content" }
            ],
            "max_concurrency": 2
        }),
    )
    .expect("grep_many should return structured batch output");
    let grepped_value: serde_json::Value = serde_json::from_str(&grepped).expect("json");
    assert_eq!(grepped_value["successCount"], 2);
    assert_eq!(grepped_value["errorCount"], 1);
    assert_eq!(grepped_value["results"][0]["index"], 0);
    assert_eq!(grepped_value["results"][1]["status"], "error");
    assert_eq!(grepped_value["results"][2]["index"], 2);
    assert!(grepped_value["results"][2]["output"]["content"]
        .as_str()
        .expect("content")
        .contains("beta"));

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn workspace_snapshot_reports_compact_read_only_state() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("workspace-snapshot-suite");
    fs::create_dir_all(root.join("src")).expect("create root");
    fs::write(root.join("src/main.rs"), "fn main() {}\n").expect("write file");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&root).expect("set cwd");

    let output = execute_tool(
        "workspace_snapshot",
        &json!({
            "include_git": false,
            "include_files": true,
            "roots": ["src"],
            "max_files": 10
        }),
    )
    .expect("workspace_snapshot should succeed");
    let value: serde_json::Value = serde_json::from_str(&output).expect("json");
    assert_eq!(value["type"], "workspace_snapshot");
    assert!(value["git"].is_null());
    assert!(value["files"]
        .as_array()
        .expect("files")
        .iter()
        .any(|file| file
            .as_str()
            .is_some_and(|path| path.ends_with("src/main.rs"))));

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn tool_batch_readonly_runs_allowed_calls_in_order() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("tool-batch-readonly-suite");
    fs::create_dir_all(root.join("src")).expect("create root");
    fs::write(root.join("src/lib.rs"), "pub fn alpha() {}\n").expect("write rs");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&root).expect("set cwd");

    let output = execute_tool(
        "tool_batch_readonly",
        &json!({
            "calls": [
                { "name": "read_file", "input": { "path": "src/lib.rs" } },
                { "name": "grep_search", "input": { "pattern": "alpha", "path": "src" } },
                { "name": "glob_search", "input": { "pattern": "src/*.rs" } }
            ],
            "max_concurrency": 3
        }),
    )
    .expect("tool_batch_readonly should succeed");
    let value: serde_json::Value = serde_json::from_str(&output).expect("json");
    assert_eq!(value["type"], "tool_batch_readonly");
    assert_eq!(value["executionMode"], "prepared_readonly");
    assert_eq!(value["successCount"], 3);
    assert_eq!(value["errorCount"], 0);
    assert_eq!(value["results"][0]["index"], 0);
    assert_eq!(
        value["results"][0]["output"]["file"]["content"],
        "pub fn alpha() {}"
    );
    assert_eq!(value["results"][1]["index"], 1);
    assert_eq!(value["results"][2]["index"], 2);

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn tool_hosts_execute_concurrently_without_process_cwd_switching() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root_a = temp_path("explicit-workspace-a");
    let root_b = temp_path("explicit-workspace-b");
    fs::create_dir_all(&root_a).expect("workspace a");
    fs::create_dir_all(&root_b).expect("workspace b");
    fs::write(root_a.join("identity.txt"), "workspace-a").expect("identity a");
    fs::write(root_b.join("identity.txt"), "workspace-b").expect("identity b");
    let process_cwd = std::env::current_dir().expect("process cwd");

    let read_a = {
        let root = root_a.clone();
        thread::spawn(move || {
            execute_in_workspace(&root, "read_file", &json!({"path": "identity.txt"}))
        })
    };
    let read_b = {
        let root = root_b.clone();
        thread::spawn(move || {
            execute_in_workspace(
                &root,
                "tool_batch_readonly",
                &json!({
                    "calls": [{"name": "read_file", "input": {"path": "identity.txt"}}],
                    "max_concurrency": 2
                }),
            )
        })
    };

    assert!(read_a
        .join()
        .expect("workspace a thread")
        .expect("workspace a read")
        .contains("workspace-a"));
    assert!(read_b
        .join()
        .expect("workspace b thread")
        .expect("workspace b read")
        .contains("workspace-b"));
    assert_eq!(
        std::env::current_dir().expect("process cwd after tools"),
        process_cwd
    );

    fs::remove_dir_all(root_a).expect("cleanup workspace a");
    fs::remove_dir_all(root_b).expect("cleanup workspace b");
}

#[test]
fn tool_batch_readonly_falls_back_for_readonly_aggregate_tools() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = temp_path("tool-batch-readonly-compat-suite");
    fs::create_dir_all(root.join("src")).expect("create root");
    fs::write(root.join("src/a.rs"), "alpha\n").expect("write a");
    fs::write(root.join("src/b.rs"), "beta\n").expect("write b");
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(&root).expect("set cwd");

    let output = execute_tool(
        "tool_batch_readonly",
        &json!({
            "calls": [
                {
                    "name": "read_many",
                    "input": {
                        "files": [
                            { "path": "src/a.rs" },
                            { "path": "src/b.rs" }
                        ],
                        "max_concurrency": 2
                    }
                }
            ]
        }),
    )
    .expect("tool_batch_readonly should keep aggregate compatibility");
    let value: serde_json::Value = serde_json::from_str(&output).expect("json");
    assert_eq!(value["executionMode"], "compat_recursive");
    assert_eq!(value["successCount"], 1);
    assert_eq!(value["results"][0]["output"]["type"], "read_many");

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn tool_batch_readonly_rejects_non_readonly_tools_before_execution() {
    let output = execute_tool(
            "tool_batch_readonly",
            &json!({
                "calls": [
                    { "name": "read_file", "input": { "path": "Cargo.toml" } },
                    { "name": "write_file", "input": { "path": "should-not-exist.txt", "content": "no" } }
                ]
            }),
        )
        .expect_err("write_file must be rejected");
    assert!(output.contains("write_file"));
    assert!(output.contains("not allowed"));
}
