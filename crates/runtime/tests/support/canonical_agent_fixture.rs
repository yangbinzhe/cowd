use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::policy::{
    EffectAssessment, PermissionOperation, PermissionResource, PermissionScope,
};
use harness_contract::tool::{
    ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency, ToolPermissionMode,
};
use runtime::{
    RuntimeExecutionHost, RuntimeServices, RuntimeToolExecutionOutcome,
    RuntimeToolExecutionRequest, RuntimeToolExecutionStatus,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub struct CanonicalProviderServer {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CanonicalProviderServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct CanonicalReadHost {
    resolver: runtime::path_identity::WorkspacePathIdentityResolver,
}

#[async_trait]
impl RuntimeExecutionHost for CanonicalReadHost {
    async fn execute_runtime_tool(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> RuntimeToolExecutionOutcome {
        let input = serde_json::from_str::<serde_json::Value>(&request.input)
            .expect("canonical read_file input");
        let path = input
            .get("path")
            .and_then(serde_json::Value::as_str)
            .expect("read_file path");
        let mut observed = self
            .resolver
            .observe_tool_scope(
                "read_file",
                &format!("read:{path}"),
                Some(&"a".repeat(64)),
                1,
            )
            .expect("canonical observed read receipt");
        observed.evidence_ref = Some(harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed(
                "tool",
                format!("canonical-read:{}", request.tool_use_id),
            ),
            "a".repeat(64),
            32,
            "text/plain",
            format!("runtime-tool:{}", request.tool_use_id),
            format!(
                "session:{}",
                request.session_id.as_deref().unwrap_or("canonical-fixture")
            ),
        ));
        RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some("canonical source evidence".to_string()),
            error: None,
            evidence_ref: format!("runtime-tool:{}", request.tool_use_id),
            observed_evidence: vec![observed],
        }
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<ToolEffectDescriptor> {
        (tool_name == "read_file").then(|| ToolEffectDescriptor {
            tool_id: tool_name.to_string(),
            descriptor_hash: "canonical-fixture:read_file:v1".to_string(),
            effect_kind: ToolEffectKind::Read,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![PermissionScope {
                resource: PermissionResource::File,
                operation: PermissionOperation::Read,
                target: input
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            }],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: EffectAssessment::default(),
        })
    }
}

pub async fn services_with_canonical_agent(
    session_id: &str,
) -> (Arc<RuntimeServices>, CanonicalProviderServer) {
    let root = tempfile::tempdir().expect("temporary runtime root").keep();
    let workspace = root.join("workspace");
    let fixture_source = workspace.join("crates/runtime/src/lib.rs");
    std::fs::create_dir_all(fixture_source.parent().expect("fixture parent"))
        .expect("fixture source parent");
    std::fs::write(
        &fixture_source,
        "pub const CANONICAL_FIXTURE: bool = true;\n",
    )
    .expect("fixture source");

    let server = spawn_provider().await;
    let providers = model_protocol::provider_config::ProvidersConfig {
        providers: HashMap::from([(
            "test".to_string(),
            model_protocol::provider_config::ProviderConfig {
                name: "test".to_string(),
                base_url: format!("{}/v1", server.base_url),
                api_key: "test".to_string(),
                models: vec!["deepseek-v4-flash".to_string()],
                protocol: Some("responses".to_string()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    };
    let host = Arc::new(CanonicalReadHost {
        resolver: runtime::path_identity::WorkspacePathIdentityResolver::discover(&workspace)
            .expect("workspace identity resolver"),
    });
    let services = RuntimeServices::builder(&root, &workspace)
        .provider_registry(Arc::new(
            runtime::ProviderRegistry::new(providers).expect("provider registry"),
        ))
        .tool_execution_host(host)
        .build()
        .expect("runtime services");
    services.publish_session_execution_policy(
        session_id,
        runtime::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Cautious,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
            ),
        ),
    );
    (services, server.into())
}

struct ProviderServer {
    base_url: String,
    task: tokio::task::JoinHandle<()>,
}

async fn spawn_provider() -> ProviderServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener");
    let address = listener.local_addr().expect("provider address");
    let task = tokio::spawn(async move {
        let mut first_request = true;
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let _request = read_request(&mut socket).await;
            let body = if first_request {
                first_request = false;
                tool_call_response()
            } else {
                final_response()
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("provider response");
        }
    });
    ProviderServer {
        base_url: format!("http://{address}"),
        task,
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 2048];
        let read = socket.read(&mut chunk).await.expect("provider request");
        assert!(read > 0, "provider request ended before headers");
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
    };
    let headers = String::from_utf8(buffer[..header_end].to_vec()).expect("request headers");
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().expect("content length"))
        })
        .unwrap_or(0);
    let body_start = header_end + 4;
    while buffer.len() - body_start < content_length {
        let mut chunk = vec![0_u8; content_length - (buffer.len() - body_start)];
        let read = socket
            .read(&mut chunk)
            .await
            .expect("provider request body");
        assert!(read > 0, "provider request body ended early");
        buffer.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8(buffer[body_start..body_start + content_length].to_vec())
        .expect("request body")
}

fn tool_call_response() -> String {
    concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_tool\",\"model\":\"default\"}}\n\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_read\",\"name\":\"read_file\"}}\n\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{\\\"path\\\":\\\"crates/runtime/src/lib.rs\\\"}\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_tool\",\"model\":\"default\",\"output\":[{\"type\":\"function_call\",\"call_id\":\"call_read\",\"name\":\"read_file\",\"arguments\":\"{\\\"path\\\":\\\"crates/runtime/src/lib.rs\\\"}\"}],\"usage\":{\"input_tokens\":8,\"output_tokens\":5}}}\n\n",
        "data: [DONE]\n\n"
    )
    .to_string()
}

fn final_response() -> String {
    let answer = "Summary: completed with evidence reference from canonical provider\n\nEvidence: canonical read_file receipt";
    let delta = serde_json::json!({
        "type": "response.output_text.delta",
        "delta": answer,
    });
    format!(
        "data: {{\"type\":\"response.created\",\"response\":{{\"id\":\"resp_final\",\"model\":\"default\"}}}}\n\ndata: {delta}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp_final\",\"model\":\"default\",\"output\":[],\"usage\":{{\"input_tokens\":8,\"output_tokens\":12}}}}}}\n\ndata: [DONE]\n\n"
    )
}

impl From<ProviderServer> for CanonicalProviderServer {
    fn from(server: ProviderServer) -> Self {
        Self { task: server.task }
    }
}
