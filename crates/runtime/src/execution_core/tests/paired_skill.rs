//! Local wire fixture: only Provider responses are scripted. Native, Skill
//! activation, graph scheduling, filesystem effects and receipts are real.
use super::*;
use harness_contract::{policy::*, skill::*, tool::*};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ASSET: &str = "PAIRED_SKILL_ASSET_V1: produce proof.txt under your exact evaluation output lease. Pass that exact path to paired-verification; do not claim verification before its real read.";
const VERIFY_ASSET: &str = "PAIRED_VERIFICATION_ASSET_V2: consume the proof.txt produced by paired-evidence using read_file. Verify its actual content before claiming completion; never substitute the producer's prose for a file read.";

pub(super) struct Fixture {
    pub url: String,
    pub host: Arc<FileHost>,
    server: tokio::task::JoinHandle<()>,
    requests: Arc<Mutex<Vec<String>>>,
    sessions: Arc<session::UnifiedSessionStore>,
}

pub(super) struct FileHost {
    root: std::path::PathBuf,
    calls: Mutex<Vec<crate::RuntimeToolExecutionRequest>>,
}

impl Fixture {
    pub fn diagnostics(&self) -> String {
        format!(
            "physical={:?}; wire={:?}",
            self.host.calls.lock().unwrap(),
            self.requests.lock().unwrap()
        )
    }
    pub async fn new(root: std::path::PathBuf) -> Self {
        let package = root.join("skills/paired-evidence");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::write(package.join("SKILL.md"), ASSET).unwrap();
        let verification = root.join("skills/paired-verification");
        std::fs::create_dir_all(&verification).unwrap();
        std::fs::write(verification.join("SKILL.md"), VERIFY_ASSET).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let server = tokio::spawn(async move {
            let mut rounds = std::collections::BTreeMap::<String, usize>::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (offset, size) = loop {
                    let mut buf = [0; 8192];
                    let count = socket.read(&mut buf).await.unwrap();
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&buf[..count]);
                    if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let size = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (end + 4, size);
                    }
                };
                while bytes.len() < offset + size {
                    let mut buf = [0; 8192];
                    let count = socket.read(&mut buf).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buf[..count]);
                }
                let request = String::from_utf8(bytes[offset..offset + size].to_vec()).unwrap();
                captured.lock().unwrap().push(request.clone());
                let begin = request
                    .find(".cowd/evaluation/")
                    .expect("Runtime output lease in prompt");
                let scope = request[begin..begin + ".cowd/evaluation/".len() + 64].to_string();
                let round = rounds.entry(scope.clone()).or_default();
                if *round == 0 {
                    assert!(
                        request.contains(ASSET),
                        "first real Native request must activate the Skill: {request}"
                    );
                    assert!(
                        request.contains(VERIFY_ASSET),
                        "second requested Skill must actually enter Native context"
                    );
                    assert!(
                        request.contains("version: 1.0.0") && request.contains("version: 2.0.0")
                    );
                }
                if *round == 3 {
                    let wire: serde_json::Value = serde_json::from_str(&request).unwrap();
                    assert!(wire["input"].as_array().unwrap().iter().any(|item| {
                        item["type"] == "function_call_output" && item["call_id"] == "paired-call-2"
                            && item["output"].to_string().contains(&format!("verified Skill output {scope}"))
                    }), "verification must consume the physical predecessor read, not a fabricated Skill success: {request}");
                }
                let events = if *round < 4 {
                    let name = if *round < 2 {
                        "write_file"
                    } else {
                        "read_file"
                    };
                    let baseline_scope = format!(
                        ".cowd/evaluation/{:x}",
                        Sha256::digest(
                            b"candidate-episode-executable\0episode/replay\0baseline\x000"
                        )
                    );
                    let other = if scope == baseline_scope {
                        "candidate"
                    } else {
                        "baseline"
                    };
                    let foreign_scope = format!(
                        ".cowd/evaluation/{:x}",
                        Sha256::digest(format!(
                            "candidate-episode-executable\0episode/replay\0{other}\0{}",
                            0
                        ))
                    );
                    let arguments = match *round {
                        0 => serde_json::json!({"path":format!("{scope}/proof.txt"), "content":format!("verified Skill output {scope}")}),
                        1 => serde_json::json!({"path":format!("{foreign_scope}/proof.txt"), "content":"CROSS_CANDIDATE_MUST_NOT_APPEAR"}),
                        2 => serde_json::json!({"path":format!("{scope}/proof.txt")}),
                        _ => serde_json::json!({"path":format!("{foreign_scope}/proof.txt")}),
                    }.to_string();
                    assert_ne!(
                        foreign_scope, scope,
                        "negative probe must target the other side"
                    );
                    let call = format!("paired-call-{round}");
                    vec![
                        serde_json::json!({"type":"response.created","response":{"id":"pair","model":"fast"}}),
                        serde_json::json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":call,"name":name}}),
                        serde_json::json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":arguments}),
                        serde_json::json!({"type":"response.completed","response":{"id":"pair","model":"fast","output":[{"type":"function_call","call_id":call,"name":name,"arguments":arguments}],"usage":{"input_tokens":32,"output_tokens":16}}}),
                    ]
                } else {
                    assert!(*round < 5, "paired fixture did not converge: {request}");
                    let text = format!("Summary: completed. Created and independently reread {scope}/proof.txt.\nImplementation: created isolated proof file and reread it.\nValidation: actual write_file and read_file receipts.\nRisks: none observed in this local fixture.\nUnresolved: none.");
                    vec![
                        serde_json::json!({"type":"response.created","response":{"id":"pair-final","model":"fast"}}),
                        serde_json::json!({"type":"response.output_text.delta","delta":text}),
                        serde_json::json!({"type":"response.completed","response":{"id":"pair-final","model":"fast","output":[],"usage":{"input_tokens":32,"output_tokens":16}}}),
                    ]
                };
                *round += 1;
                let body = events
                    .into_iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect::<String>()
                    + "data: [DONE]\n\n";
                socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        Self {
            url,
            host: Arc::new(FileHost {
                root,
                calls: Mutex::new(Vec::new()),
            }),
            server,
            requests,
            sessions: Arc::new(crate::test_support::session_store()),
        }
    }

    pub fn catalog(&self) -> crate::RuntimeSkillCatalog {
        let producer = crate::RuntimeSkillCatalog::new(
            vec![SkillCapabilityProfile {
                skill_id: "paired-evidence".into(),
                name: "Paired evidence".into(),
                version: Some("1.0.0".into()),
                source_root: self
                    .host
                    .root
                    .join("skills/paired-evidence")
                    .display()
                    .to_string(),
                package_fingerprint: format!("{:x}", Sha256::digest(ASSET.as_bytes())),
                kind: SkillKind::Workflow,
                lifecycle_status: SkillLifecycleStatus::UsablePrompt,
                adapters: vec![SkillAdapterKind::PromptOnly],
                risk_level: SkillRiskLevel::Low,
                entrypoints: vec![SkillEntrypoint {
                    runtime: SkillDetectedRuntime::Markdown,
                    path: "SKILL.md".into(),
                    adapter: SkillAdapterKind::PromptOnly,
                    command_hint: None,
                }],
                inspection_summary: vec!["write and independently verify isolated evidence".into()],
                structured_dependencies: vec![],
            }],
            vec![crate::RuntimeSkillPromptAsset {
                skill_id: "paired-evidence".into(),
                version: Some("1.0.0".into()),
                content: std::fs::read_to_string(
                    self.host.root.join("skills/paired-evidence/SKILL.md"),
                )
                .unwrap(),
                source_ref: "skill://paired-evidence/SKILL.md".into(),
                tool_refs: vec!["write_file".into(), "read_file".into()],
            }],
        );
        let mut profiles = producer.profiles();
        let mut verification = profiles[0].clone();
        verification.skill_id = "paired-verification".into();
        verification.name = "Paired verification".into();
        verification.version = Some("2.0.0".into());
        verification.source_root = self
            .host
            .root
            .join("skills/paired-verification")
            .display()
            .to_string();
        verification.package_fingerprint = format!("{:x}", Sha256::digest(VERIFY_ASSET.as_bytes()));
        profiles.push(verification);
        let mut assets = producer.prompt_assets();
        assets.push(crate::RuntimeSkillPromptAsset {
            skill_id: "paired-verification".into(),
            version: Some("2.0.0".into()),
            content: std::fs::read_to_string(
                self.host.root.join("skills/paired-verification/SKILL.md"),
            )
            .unwrap(),
            source_ref: "skill://paired-verification/SKILL.md".into(),
            tool_refs: vec!["read_file".into()],
        });
        crate::RuntimeSkillCatalog::new(profiles, assets)
    }

    pub async fn install_sessions(&self, services: &Arc<RuntimeServices>) {
        let ports = crate::session_runtime_port::TestSessionPortAdapter::new(self.sessions.clone());
        services
            .install_session_ports(ports.clone(), ports.clone(), ports.clone(), ports)
            .unwrap();
        for side in ["baseline", "candidate"] {
            self.sessions
                .create_session(&SessionRecord {
                    session_id: format!("evolution-eval:candidate-episode-executable:{side}:0"),
                    platform: "test".into(),
                    chat_id: side.into(),
                    user_id: None,
                    model: Some("fast".into()),
                    created_at: "2026-09-09T00:00:00Z".into(),
                    last_activity: "2026-09-09T00:00:00Z".into(),
                    message_count: 0,
                    reset_policy: "None".into(),
                    metadata_json: None,
                    input_tokens: 0,
                    output_tokens: 0,
                    status: "active".into(),
                })
                .await
                .unwrap();
        }
    }

    pub async fn verify(self, services: &Arc<RuntimeServices>) {
        let calls = self.host.calls.lock().unwrap().clone();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.tool_name == "write_file")
                .count(),
            2,
            "{calls:?}"
        );
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.tool_name == "read_file")
                .count(),
            2
        );
        let mut paths = BTreeSet::new();
        for call in calls.iter().filter(|call| call.tool_name == "write_file") {
            assert!(call.evaluation_isolated);
            let input: serde_json::Value = serde_json::from_str(&call.input).unwrap();
            let path = input["path"].as_str().unwrap();
            paths.insert(path.to_string());
            assert_eq!(
                std::fs::read_to_string(self.host.root.join(path)).unwrap(),
                input["content"].as_str().unwrap()
            );
            let parent = call.parent_execution.as_ref().unwrap();
            let graph = services
                .graph_state_store()
                .load(&parent.execution_id)
                .unwrap();
            assert!(graph
                .node_results
                .values()
                .any(|result| result.status == ExecutionNodeStatus::Completed));
            let receipts = services
                .event_store()
                .list_stream(&format!(
                    "execution-agent-receipts:{}:{}:1",
                    parent.execution_id, parent.node_id
                ))
                .unwrap();
            assert!(
                receipts
                    .iter()
                    .any(|event| event.payload["effect_kind"] == "write"),
                "actual write must have a durable receipt"
            );
            assert!(
                receipts
                    .iter()
                    .any(|event| event.payload["effect_kind"] == "read"),
                "actual reread must have a durable receipt"
            );
        }
        assert_eq!(paths.len(), 2);
        for side in ["baseline", "candidate"] {
            let events = self
                .sessions
                .session_domain_events_page(
                    &format!("evolution-eval:candidate-episode-executable:{side}:0"),
                    0,
                    256,
                )
                .await
                .unwrap();
            for (skill, version) in [
                ("paired-evidence", "1.0.0"),
                ("paired-verification", "2.0.0"),
            ] {
                assert!(events
                    .events
                    .iter()
                    .any(|event| event.kind == "skill_candidates"
                        && event.payload["selected"] == skill
                        && event.payload["invocation_evidence"]["skill_version"] == version));
            }
        }
        assert_eq!(
            self.requests.lock().unwrap().len(),
            10,
            "repeated sample must not invoke the Provider again"
        );
        assert!(
            self.requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| request.contains("outside the Agent focus/resource lease")),
            "cross-candidate effect rejection must return to the actual model loop"
        );
        let first = &calls[0];
        let parent = first.parent_execution.as_ref().unwrap();
        let graph = services
            .graph_state_store()
            .load(&parent.execution_id)
            .unwrap();
        let packet: AgentTaskPacket = serde_json::from_str(
            &graph
                .nodes
                .iter()
                .find(|node| node.id == parent.node_id)
                .unwrap()
                .payload_ref,
        )
        .unwrap();
        let dispatcher =
            crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(
                services, &packet,
            );
        let mut late = first.clone();
        late.tool_use_id = "after-evaluation-terminal".into();
        late.idempotency_key = "after-evaluation-terminal".into();
        let outcome = dispatcher.execute(late).await;
        assert!(
            !matches!(outcome, Ok(outcome) if outcome.status == crate::RuntimeToolExecutionStatus::Executed),
            "terminal evaluation must revoke new effects"
        );
        assert_eq!(self.host.calls.lock().unwrap().len(), calls.len());
        self.server.abort();
        let _ = self.server.await;
    }
}

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for FileHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        assert!(request.authorization.is_some());
        assert!(request.evaluation_isolated);
        let input: serde_json::Value = serde_json::from_str(&request.input).unwrap();
        let path = self.root.join(input["path"].as_str().unwrap());
        let prior = match std::fs::read(&path) {
            Ok(bytes) => harness_contract::context::WorkspacePriorState::Existing {
                sha256: format!("{:x}", Sha256::digest(bytes)),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                harness_contract::context::WorkspacePriorState::Absent
            }
            Err(error) => panic!("read actual prior state: {error}"),
        };
        let output = match request.tool_name.as_str() {
            "write_file" => {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, input["content"].as_str().unwrap()).unwrap();
                format!("wrote {}", path.display())
            }
            "read_file" => {
                let content = std::fs::read_to_string(&path).unwrap();
                let lines = content.lines().count();
                serde_json::json!({"type":"text", "truncated":false, "file":{
                    "filePath":input["path"], "content":content, "startLine":1,
                    "numLines":lines, "totalLines":lines, "sha256":format!("{:x}", Sha256::digest(content.as_bytes()))
                }}).to_string()
            }
            name => panic!("unexpected physical tool: {name}"),
        };
        self.calls.lock().unwrap().push(request.clone());
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(&self.root).unwrap();
        let mut observed = if request.tool_name == "write_file" {
            let digest = format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()));
            let mut evidence = resolver
                .observe_trusted_tool_output_file(
                    "write_file",
                    harness_contract::context::WorkspaceAccessMode::Write,
                    input["path"].as_str().unwrap(),
                    &digest,
                    request.observation_wave_sequence,
                )
                .unwrap();
            evidence.workspace_prior_state = Some(prior);
            evidence
        } else {
            resolver
                .observe_complete_read_tool_output(
                    "read_file",
                    &serde_json::from_str(&output).unwrap(),
                    request.observation_wave_sequence,
                )
                .unwrap()
        };
        // This adapter attests bytes physically read/written above; original
        // Runtime effect commit is still the only durable receipt owner.
        observed.evidence_ref = None;
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(output),
            error: None,
            evidence_ref: format!("file:{}", request.tool_use_id),
            observed_evidence: vec![observed],
        }
    }

    fn delegated_tool_effect_descriptor(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<ToolEffectDescriptor> {
        if !matches!(name, "write_file" | "read_file") {
            return None;
        }
        let write = name == "write_file";
        let mut scope = PermissionScope::new(
            PermissionResource::File,
            if write {
                PermissionOperation::Write
            } else {
                PermissionOperation::Read
            },
        );
        scope.target = input["path"].as_str().map(str::to_owned);
        Some(ToolEffectDescriptor {
            tool_id: name.into(),
            descriptor_hash: format!("paired:{name}"),
            effect_kind: if write {
                ToolEffectKind::Write
            } else {
                ToolEffectKind::Read
            },
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![scope],
            required_permission: if write {
                ToolPermissionMode::WorkspaceWrite
            } else {
                ToolPermissionMode::ReadOnly
            },
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: if write {
                EffectAssessment {
                    reversibility: EffectReversibility::Compensatable,
                    externality: EffectExternality::Workspace,
                    data_sensitivity: DataClassification::Internal,
                    novelty: EffectNovelty::Routine,
                    blast_radius: EffectBlastRadius::Workspace,
                }
            } else {
                EffectAssessment::default()
            },
        })
    }
}
