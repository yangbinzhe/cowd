use super::*;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::io::Read;

const OUTPUT_BYTES: usize = 512 * 1024;
const STREAM_BYTES: usize = 64 * 1024;
const MAX_GRAPHS: usize = 16;

fn raw_anchor_is_bound(payload: &Value, scope: &str) -> bool {
    let anchor = &payload["runtime_raw_content_v1"];
    anchor["version"] == 1
        && anchor["visibility_scope"].as_str() == Some(scope)
        && anchor["ref_id"] == payload["full_output_ref"]
        && anchor["tool_call_id"] == payload["tool_call_id"]
        && anchor["selector"].as_str().is_some_and(|selector| !selector.is_empty())
        && anchor["byte_count"].as_u64().is_some()
        && payload["tool_call_id"].as_str().is_some_and(|call| !call.is_empty())
        && payload["full_output_ref"].as_str().is_some_and(|reference| !reference.is_empty())
}

fn bounded(value: Value, remaining: &mut usize, depth: usize) -> Value {
    if *remaining == 0 || depth > 16 {
        return json!({"omitted":"diagnostic tree budget"});
    }
    *remaining -= 1;
    match value {
        Value::String(text) if text.chars().count() > 256 => {
            let chunks = text.chars().take(2048).collect::<Vec<_>>()
                .chunks(256).map(|chunk| chunk.iter().collect::<String>()).collect::<Vec<_>>();
            json!({"text_chunks":chunks,"original_bytes":text.len(),
                "sha256":crate::context_evidence::read_sha256(text.as_bytes()),
                "truncated":text.chars().count()>2048})
        }
        Value::Array(values) => {
            let count = values.len();
            let mut kept = values.into_iter().take(32)
                .map(|value| bounded(value, remaining, depth + 1)).collect::<Vec<_>>();
            if count > kept.len() {
                kept.push(json!({"omitted_array_items":count-kept.len()}));
            }
            Value::Array(kept)
        }
        Value::Object(values) => {
            let count = values.len();
            let mut kept = serde_json::Map::new();
            for (key, value) in values.into_iter().take(64) {
                let key = if key.len() > 128 {
                    format!("key-sha256:{}", crate::context_evidence::read_sha256(key.as_bytes()))
                } else {
                    key
                };
                kept.insert(key, bounded(value, remaining, depth + 1));
            }
            if count > kept.len() {
                kept.insert("omitted_object_fields".into(), json!(count-kept.len()));
            }
            Value::Object(kept)
        }
        value => value,
    }
}

struct Snapshot {
    remaining: usize,
    records: usize,
    omitted: usize,
}

impl Snapshot {
    fn raw_tool_return(
        &mut self,
        services: &RuntimeServices,
        packet: &AgentTaskPacket,
        event: &crate::DurableRuntimeEvent,
    ) {
        let anchor = &event.payload["runtime_raw_content_v1"];
        let scope = format!("session:{}", packet.session_id());
        let selector = anchor["selector"].as_str();
        let expected_bytes = anchor["byte_count"].as_u64();
        if !raw_anchor_is_bound(&event.payload, &scope) {
            self.emit("raw_tool_return_omitted", json!({"event_id":event.event_id,
                "reason":"missing, legacy or mismatched original same-Session raw anchor",
                "full_output_ref":event.payload["full_output_ref"],"anchor":anchor}));
            return;
        }
        if expected_bytes.is_some_and(|bytes| bytes > STREAM_BYTES as u64) {
            self.emit("raw_tool_return_omitted", json!({"event_id":event.event_id,
                "reason":"original raw tool body exceeds diagnostic read bound",
                "max_bytes":STREAM_BYTES,"anchor":anchor}));
            return;
        }
        let raw = match services.artifact_store().resolve(selector.unwrap()) {
            Ok(raw) => raw,
            Err(error) => {
                self.emit("raw_tool_return_omitted", json!({"event_id":event.event_id,
                    "reason":"original artifact metadata unavailable","error":error.to_string(),"anchor":anchor}));
                return;
            }
        };
        if raw.visibility_scope != scope || raw.bytes != expected_bytes.unwrap()
            || anchor["sha256"].as_str() != Some(raw.sha256.as_str())
        {
            self.emit("raw_tool_return_omitted", json!({"event_id":event.event_id,
                "reason":"resolved original raw artifact does not match the pinned anchor","anchor":anchor}));
            return;
        }
        match services.artifact_store().read_blocking(&raw, &scope, None) {
            Ok(bytes) => self.emit("original_raw_tool_return", json!({
                "event_id":event.event_id,"tool_call_id":event.payload["tool_call_id"],
                "tool_name":event.payload["tool_name"],"anchor":anchor,
                "observed_bytes":bytes.len(),"sha256":crate::context_evidence::read_sha256(&bytes),
                "json":serde_json::from_slice::<Value>(&bytes).ok(),
                "text":std::str::from_utf8(&bytes).ok(),
                "binary_prefix_hex":if std::str::from_utf8(&bytes).is_err() {
                    Some(bytes.iter().take(128).map(|byte| format!("{byte:02x}")).collect::<String>())
                } else { None },
                "binary_prefix_truncated":std::str::from_utf8(&bytes).is_err() && bytes.len()>128,
                "output_rendering_bounded":true,
                "source":"original verified artifact, never reconstructed from preview"})),
            Err(error) => self.emit("raw_tool_return_omitted", json!({"event_id":event.event_id,
                "reason":"original artifact body read failed","error":error.to_string(),"anchor":anchor})),
        }
    }

    fn tool_lifecycle(&mut self, services: &RuntimeServices, packet: &AgentTaskPacket, raw_remaining: &mut usize) {
        let stream = format!("session:{}", packet.session_id());
        let revision = match services.event_store().stream_revision(&stream) {
            Ok(revision) => revision,
            Err(error) => {
                self.emit("tool_lifecycle_omitted", json!({"stream":stream,"error":error.to_string()}));
                return;
            }
        };
        match services.event_store().list_stream_before(&stream, revision, 32, STREAM_BYTES) {
            Ok(events) => {
                let scanned = events.len();
                let mut matched = 0;
                let mut raw_reads = 0;
                for event in events {
                    if !matches!(event.kind.as_str(), "tool.invocation.completed" | "tool.invocation.failed")
                        || !event.activity_binding().is_some_and(|owner|
                            owner.session_id == packet.session_id()
                                && owner.agent_run_id.as_deref() == Some(packet.run_id()))
                    {
                        continue;
                    }
                    matched += 1;
                    self.emit("original_tool_lifecycle", json!({
                        "packet_task_matches":event.activity_binding().is_some_and(|owner|
                            owner.task_id == packet.assignment.task_id),"event":event}));
                    if *raw_remaining > 0 {
                        *raw_remaining -= 1;
                        raw_reads += 1;
                        self.raw_tool_return(services, packet, &event);
                    }
                }
                self.emit("tool_lifecycle_extent", json!({"stream":stream,"revision":revision,
                    "graph":packet.graph_id(),"run":packet.run_id(),"scanned_tail_events":scanned,
                    "matched_exact_run":matched,"raw_outputs_attempted":raw_reads,
                    "raw_outputs_omitted_by_count":matched-raw_reads,"max_events":32,"max_bytes":STREAM_BYTES,
                    "full_session_history_scanned":revision<=scanned as u64,
                    "session_history_omitted_before_sequence":revision.saturating_sub(scanned as u64),
                    "no_match_is_not_proof_of_no_tool_execution":true}));
            }
            Err(error) => self.emit("tool_lifecycle_omitted", json!({"stream":stream,"error":error})),
        }
    }

    fn emit(&mut self, kind: &str, data: Value) {
        let value = json!({"kind":kind,"data":bounded(data, &mut 2048, 0)});
        if let Ok(encoded) = serde_json::to_string_pretty(&value) {
            if encoded.len() + 64 > self.remaining {
                self.omitted += 1;
                return;
            }
            self.remaining -= encoded.len() + 64;
            self.records += 1;
            eprintln!("OWN832_RECORD_BEGIN\n{encoded}\nOWN832_RECORD_END");
        } else {
            self.omitted += 1;
        }
    }

    fn stream(&mut self, services: &RuntimeServices, stream: &str) {
        match services.event_store().stream_revision(stream) {
            Ok(revision) => match services.event_store()
                .list_stream_before(stream, revision, 16, STREAM_BYTES) {
                Ok(events) => self.emit("durable_stream_tail", json!({
                    "stream":stream,"through_revision":revision,"max_events":16,
                    "max_bytes":STREAM_BYTES,"events":events})),
                Err(error) => self.emit("stream_read_error", json!({"stream":stream,"error":error})),
            },
            Err(error) => self.emit("stream_revision_error", json!({"stream":stream,"error":error.to_string()})),
        }
    }

    fn graph(&mut self, services: &RuntimeServices, graph_id: &str) {
        let graph = match services.graph_state_store().load(graph_id) {
            Ok(graph) => graph,
            Err(error) => {
                self.emit("graph_missing", json!({"graph_id":graph_id,"error":error.to_string()}));
                return;
            }
        };
        self.emit("actual_graph", json!({"id":graph.id,"revision":graph.revision,
            "parent_execution":graph.parent_execution,"lineage":graph.lineage,
            "node_statuses":graph.node_statuses,"node_results":graph.node_results,
            "recovery_cursor":graph.recovery_cursor}));
        self.stream(services, graph_id);
        let mut raw_remaining = 4;
        for node in graph.nodes.iter().filter(|node| node.kind == ExecutionNodeKind::AgentTask).take(4) {
            let packet: AgentTaskPacket = match serde_json::from_str(&node.payload_ref) {
                Ok(packet) => packet,
                Err(error) => {
                    self.emit("original_packet_decode_error", json!({"graph":graph_id,
                        "node":node.id,"payload_bytes":node.payload_ref.len(),"error":error.to_string()}));
                    continue;
                }
            };
            self.emit("original_packet", json!({"graph":graph_id,"node":node.id,
                "payload_sha256":crate::context_evidence::read_sha256(node.payload_ref.as_bytes()),
                "packet":packet}));
            let run = services.agent_runtime().get(packet.agent_id());
            self.emit("actual_backend", json!({"graph":graph_id,
                "exact_run":run.as_ref().is_some_and(|run| run.graph_id == packet.graph_id()
                    && run.node_id == packet.node_id() && run.run_id == packet.run_id()
                    && run.attempt == packet.attempt),"snapshot":run}));
            self.emit("authenticated_exit", json!({"graph":graph_id,
                "sources":crate::agent_runtime::confirmed_run_exit(services.event_store(), &packet)
                    .map(|exit| exit.map(|(_, sources)| sources))}));
            let lifecycle = crate::agent_runtime::authenticated_run_lifecycle(services.event_store(), &packet);
            match lifecycle {
                Ok(state) => self.emit("authenticated_lifecycle", json!({"graph":graph_id,
                    "semantic_fact":state.semantic_fact(),"sources":state.sources})),
                Err(error) => self.emit("lifecycle_error", json!({"graph":graph_id,"error":error})),
            }
            self.stream(services, &format!("agent:{}", packet.agent_id()));
            self.tool_lifecycle(services, &packet, &mut raw_remaining);
            self.stream(services, &format!("execution-agent-receipts:{}:{}:{}",
                graph_id, packet.node_id(), packet.attempt));
            let index = format!("execution-agent-receipts:{}:{}:{}",
                graph_id, packet.node_id(), packet.attempt);
            if let Ok(revision) = services.event_store().stream_revision(&index) {
                if let Ok(events) = services.event_store().list_stream_before(&index, revision, 8, STREAM_BYTES) {
                    for event in events {
                        if event.kind == "execution.agent_tool.receipt" {
                            let output = event.payload["outcome"]["output"].as_str();
                            self.emit("original_index_tool_return", json!({
                                "event_id":event.event_id,"stream":event.stream_id,"sequence":event.sequence,
                                "transaction":event.transaction_id,"transaction_index":event.transaction_index,
                                "idempotency_key":event.payload["idempotency_key"],
                                "outcome":event.payload["outcome"],
                                "parsed_output":output.and_then(|output| serde_json::from_str::<Value>(output).ok()),
                                "runtime_truncated":output.is_some_and(|output|
                                    output.contains("[effect receipt truncated; full output requires its artifact receipt]")),
                                "original_output_absent":output.is_none(),
                                "source":"original persisted bounded tool outcome, not synthesized response"}));
                        }
                        if let Some(key) = event.payload["idempotency_key"].as_str() {
                            self.stream(services, &format!("execution-effect:{key}"));
                        }
                    }
                }
            }
            if let Ok(scratch) = services.agent_runtime().process_fixture_scratch(&packet) {
                self.emit("original_scratch", json!({"graph":graph_id,"path":scratch,
                    "exists":scratch.exists(),"directory_scan_limit":64,"request_response_limit":8}));
                let mut paths = vec![scratch.join("started.json"), scratch.join("receive-held.json"),
                    scratch.join("stdin-closed.json")];
                if let Ok(entries) = std::fs::read_dir(&scratch) {
                    paths.extend(entries.take(64).filter_map(Result::ok).map(|entry| entry.path())
                        .filter(|path| path.extension().is_some_and(|extension|
                            extension == "request" || extension == "response")).take(8));
                }
                for path in paths {
                    match std::fs::symlink_metadata(&path) {
                        Ok(metadata) if metadata.file_type().is_file() => {}
                        Ok(_) => {
                            self.emit("original_backend_checkpoint_omitted", json!({"path":path,
                                "reason":"original checkpoint is not a regular file; not followed"}));
                            continue;
                        }
                        Err(error) => {
                            self.emit("original_backend_checkpoint_omitted", json!({"path":path,
                                "reason":"original checkpoint unavailable","error":error.to_string()}));
                            continue;
                        }
                    }
                    match std::fs::File::open(&path) {
                        Ok(file) => {
                        let mut bytes = Vec::new();
                        let read = file.take(STREAM_BYTES as u64 + 1).read_to_end(&mut bytes);
                        let parsed = if bytes.len() <= STREAM_BYTES {
                            serde_json::from_slice::<Value>(&bytes).ok()
                        } else {
                            None
                        };
                        self.emit("original_backend_checkpoint", json!({"path":path,
                            "read_error":read.err().map(|error| error.to_string()),
                            "read_bytes":bytes.len(),"truncated":bytes.len()>STREAM_BYTES,"value":parsed}));
                        }
                        Err(error) => self.emit("original_backend_checkpoint_omitted", json!({
                            "path":path,"reason":"original checkpoint open failed","error":error.to_string()})),
                    }
                }
            }
            if let Err(error) = services.agent_runtime().process_fixture_scratch(&packet) {
                self.emit("original_scratch_omitted", json!({"graph":graph_id,"error":error,
                    "reason":"original Process scratch mapping unavailable; no replacement directory is created"}));
            }
        }
        match services.graph_state_store().child_links(graph_id) {
            Ok(links) => {
                self.emit("actual_children", json!({"graph":graph_id,
                    "total":links.len(),"links":links.iter().take(8).collect::<Vec<_>>()}));
                for link in links.iter().take(8) {
                    match services.graph_state_store().load(&link.child_execution_id) {
                        Ok(child) => {
                            self.emit("actual_child", json!({"id":child.id,
                                "revision":child.revision,"parent":child.parent_execution,
                                "lineage":child.lineage,"statuses":child.node_statuses,
                                "results":child.node_results,"cursor":child.recovery_cursor}));
                            if crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(&child) {
                                match crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool_graph(&child) {
                                    Ok(bound) => self.emit("original_bound_tool_request", json!({
                                        "graph":child.id,"agent_id":bound.agent_id,"run_id":bound.run_id,
                                        "tool_use_id":bound.request.tool_use_id,"tool_name":bound.request.tool_name,
                                        "input":bound.request.input,"idempotency_key":bound.request.idempotency_key,
                                        "parent":bound.request.parent_execution,
                                        "parent_attempt":bound.request.parent_execution_attempt})),
                                    Err(error) => self.emit("original_tool_request_omitted", json!({
                                        "graph":child.id,"error":error.to_string()})),
                                }
                            }
                        }
                        Err(error) => self.emit("child_read_error", json!({
                            "graph":link.child_execution_id,"error":error.to_string()})),
                    }
                    self.stream(services, &link.child_execution_id);
                }
            }
            Err(error) => self.emit("children_read_error", json!({"graph":graph_id,"error":error.to_string()})),
        }
    }
}

pub(super) fn capture(
    services: &Arc<RuntimeServices>,
    program_id: &str,
    task_ref: &str,
    stage: &str,
    original: Option<&AgentTaskPacket>,
) {
    let mut snapshot = Snapshot { remaining: OUTPUT_BYTES - 1024, records: 0, omitted: 0 };
    snapshot.emit("begin", json!({"schema":1,"stage":stage,"program":program_id,
        "task":task_ref,"workspace":services.workspace_root(),
        "cursor_before":services.event_store().durable_commit_cursor().map_err(|error| error.to_string()),
        "output_byte_limit":OUTPUT_BYTES,"graph_limit":MAX_GRAPHS,
        "observation_only":true,"not_atomic_acceptance_evidence":true}));
    let mut graphs = BTreeSet::new();
    if let Some(packet) = original {
        graphs.insert(packet.graph_id().to_string());
        snapshot.emit("caller_original_packet", json!({"packet":packet}));
    }
    snapshot.stream(services, &format!("agentic-dispatch:{program_id}"));
    match services.agent_action_service().project_snapshot(program_id) {
        Ok(projection) => {
            let task = projection.tasks.get(task_ref);
            snapshot.emit("canonical_task", json!({"revision":projection.revision,
                "task":task,"task_missing":task.is_none()}));
            let running_members = services.agent_runtime().running_agentic_members(program_id);
            snapshot.emit("review_roster_extent", json!({"members":projection.agents.len(),
                "selected":projection.agents.len().min(MAX_GRAPHS),
                "limit":MAX_GRAPHS,"complete":projection.agents.len()<=MAX_GRAPHS}));
            for member in projection.agents.values().take(MAX_GRAPHS) {
                let review_team = task.and_then(|task|
                    projection.dispatch_team_id_for(&member.agent_id, task, true));
                let busy_tasks = projection.tasks.values().filter(|candidate|
                    candidate.active_attempts.values().any(|attempt|
                        attempt.agent_id == member.agent_id)
                    || (candidate.status == crate::AgenticTaskStatus::Claimed
                        && candidate.claimant.as_deref() == Some(member.agent_id.as_str())))
                    .map(|candidate| candidate.task_id.as_str()).collect::<Vec<_>>();
                snapshot.emit("review_roster_member", json!({"member":member,
                    "logical_agent_id":member.agent_id,
                    "active_team_ids":projection.active_team_ids_for(&member.agent_id),
                    "dispatch_review_team":review_team,
                    "independently_eligible":task.is_some_and(|task|
                        task.claimant.as_deref()!=Some(member.agent_id.as_str())
                            && review_team.is_some()),
                    "running_member":running_members.contains(&member.agent_id),
                    "busy_task_count":busy_tasks.len(),
                    "busy_tasks":busy_tasks.into_iter().take(MAX_GRAPHS).collect::<Vec<_>>()}));
            }
            if let Some(task) = task {
                graphs.extend(task.active_attempts.keys().take(MAX_GRAPHS).cloned());
                graphs.extend(task.claim_execution_id.iter().cloned());
            }
        }
        Err(error) => snapshot.emit("projection_error", json!({"error":error.to_string()})),
    }
    let stream = format!("agentic-program:{program_id}");
    if let Ok(revision) = services.event_store().stream_revision(&stream) {
        let mut after = 0;
        for _ in 0..8 {
            match services.event_store().list_stream_after(&stream, after, revision, 32, STREAM_BYTES) {
                Ok(events) => {
                    if events.is_empty() {
                        break;
                    }
                    for event in events {
                        after = event.sequence;
                        let action = &event.payload["envelope"]["action"];
                        if action["input"]["task_ref"].as_str() != Some(task_ref) {
                            continue;
                        }
                        if let Some(execution) = action["input"]["execution_id"].as_str() {
                            if graphs.len() < MAX_GRAPHS {
                                graphs.insert(execution.to_string());
                            }
                        }
                        snapshot.emit("original_task_action_event", json!({"event":event}));
                    }
                    if after >= revision {
                        break;
                    }
                }
                Err(error) => {
                    snapshot.emit("program_history_read_error", json!({"error":error,"after":after}));
                    break;
                }
            }
        }
        snapshot.emit("program_history_extent", json!({"through_revision":revision,
            "scanned_through":after,"complete":after>=revision,"max_pages":8}));
        match services.event_store().list_stream_before(&stream, revision, 32, STREAM_BYTES) {
            Ok(events) => {
                for event in events {
                    let action = &event.payload["envelope"]["action"];
                    if action["input"]["task_ref"].as_str() != Some(task_ref) {
                        continue;
                    }
                    if let Some(execution) = action["input"]["execution_id"].as_str() {
                        if graphs.len() < MAX_GRAPHS {
                            graphs.insert(execution.to_string());
                        }
                    }
                    snapshot.emit("original_task_action_tail", json!({"event":event}));
                }
            }
            Err(error) => snapshot.emit("program_tail_read_error", json!({"error":error})),
        }
    }
    snapshot.emit("graph_discovery_extent", json!({"selected":graphs.len(),
        "limit":MAX_GRAPHS,"limit_reached":graphs.len()>=MAX_GRAPHS,
        "discovery_may_omit_older_executions":graphs.len()>=MAX_GRAPHS}));
    for graph in graphs.iter().take(MAX_GRAPHS) {
        snapshot.graph(services, graph);
    }
    snapshot.emit("producer_lanes", json!({"health":services.event_reactor_health().map_err(|error| error.to_string()),
        "scope_leases":services.scope_locks().active_lease_count().map_err(|error| error.to_string())}));
    eprintln!("OWN832_SNAPSHOT_END {}", json!({"records":snapshot.records,
        "omitted_records":snapshot.omitted,"cursor_after":services.event_store().durable_commit_cursor().map_err(|error| error.to_string())}));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn followup_packet(scopes: Vec<String>) -> AgentTaskPacket {
        AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(None, "own832-instance", "own832-run",
                "own832-task", "session-dispatch", "own832-mission", None, "own832-graph", "own832-node"),
            attempt: 1,
            expected_graph_revision: 0,
            objective: "pure original followup grant test".into(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            cohort_prompt_package: None,
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            agentic_binding: Some(harness_contract::agent::AgenticExecutionBinding {
                program_id: "program-dispatch".into(), agent_id: "own832-agent".into(),
                membership_id: "own832-membership".into(), team_id: "own832-team".into(),
                task_team_id: "own832-team".into(), source_spec_revision: 1, task_purpose: None,
                focus: harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref: "own832-task".into() },
            }),
            evidence_refs: Vec::new(),
            resource_scopes: scopes,
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: PermissionMode::ReadOnly,
            policy_revision: 1,
            model_lease: "original-packet-model".into(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                "own832-budget", "own832-instance", "agent", 1, u64::MAX, 1),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "own832-pure-packet".into(),
        }
    }

    fn followup_actor(packet: &AgentTaskPacket) -> AgentActorBinding {
        let mut actor = agent("own832-action", "own832-team", "own832-agent", packet.graph_id(),
            AgentAction::TaskClaim(TaskClaimInput { task_ref: "own832-task".into(), reason: None })).actor;
        actor.turn_id = packet.assignment.execution_identity.turn_id().unwrap().into();
        actor
    }

    #[test]
    fn own832_followup_context_preserves_packet_grants_without_actor_defaults() {
        let workspace = tempfile::tempdir().unwrap();
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path()).unwrap();
        for (packet_scopes, actor_scopes, whole_read) in [
            (vec!["workspace:.".to_string()], Vec::new(), true),
            (vec!["read:out/a".to_string()], vec!["workspace:.".to_string()], false),
            (Vec::new(), vec!["workspace:.".to_string()], false),
        ] {
            let packet = followup_packet(packet_scopes);
            let mut actor = followup_actor(&packet);
            actor.resource_scopes = actor_scopes;
            actor.model_lease = "actor-is-not-the-execution-model".into();
            actor.permission_ceiling = Some(PermissionMode::DangerFullAccess);
            let parent = ExecutionParentBinding { execution_id: packet.graph_id().into(), node_id: packet.node_id().into() };
            let context = original_packet_followup_context(&packet, &parent, &actor).unwrap();
            assert_eq!(context.session_id, packet.session_id());
            assert_eq!(context.turn_id, packet.assignment.execution_identity.turn_id().unwrap());
            assert_eq!(context.model_lease, packet.model_lease);
            assert_eq!(context.permission_ceiling, PermissionMode::ReadOnly);
            assert!(!context.permission_ceiling.permits(PermissionMode::WorkspaceWrite));
            assert_eq!(context.resource_scopes, packet.resource_scopes);
            assert_eq!(crate::agent_in_process_worker::resource_path_is_authorized(
                &resolver, ".", &context.resource_scopes, false), whole_read);
        }
    }

    #[test]
    fn own832_followup_context_rejects_foreign_actor_and_parent() {
        let packet = followup_packet(vec!["workspace:.".into()]);
        let actor = followup_actor(&packet);
        let parent = ExecutionParentBinding { execution_id: packet.graph_id().into(), node_id: packet.node_id().into() };
        for fault in ["session", "turn", "program", "actor", "agent", "team", "execution", "kind"] {
            let mut foreign = actor.clone();
            match fault {
                "session" => foreign.session_id = "foreign-session".into(),
                "turn" => foreign.turn_id = "foreign-turn".into(),
                "program" => foreign.program_id = "foreign-program".into(),
                "actor" => foreign.actor_id = "foreign-actor".into(),
                "agent" => foreign.agent_id = Some("foreign-agent".into()),
                "team" => foreign.team_id = Some("foreign-team".into()),
                "execution" => foreign.execution_id = Some("foreign-graph".into()),
                "kind" => foreign.kind = AgentActorKind::Root,
                _ => unreachable!(),
            }
            assert!(original_packet_followup_context(&packet, &parent, &foreign).is_err(), "{fault}");
        }
        for foreign in [
            ExecutionParentBinding { execution_id: "foreign-graph".into(), node_id: packet.node_id().into() },
            ExecutionParentBinding { execution_id: packet.graph_id().into(), node_id: "foreign-node".into() },
        ] {
            assert!(original_packet_followup_context(&packet, &foreign, &actor).is_err());
        }
        let mut missing = packet.clone();
        missing.agentic_binding = None;
        assert!(original_packet_followup_context(&missing, &parent, &actor).is_err());
        let mut conflicting = packet.clone();
        conflicting.assignment.session_id = "conflicting-session".into();
        assert!(original_packet_followup_context(&conflicting, &parent, &actor).is_err());
    }

    #[test]
    fn own832_raw_return_anchor_never_borrows_foreign_or_missing_carriers() {
        let payload = json!({"tool_call_id":"original-call","full_output_ref":"tool://original",
            "runtime_raw_content_v1":{"version":1,"visibility_scope":"session:owned",
                "ref_id":"tool://original","tool_call_id":"original-call",
                "selector":"artifact://original","byte_count":8}});
        assert!(raw_anchor_is_bound(&payload, "session:owned"));
        assert!(!raw_anchor_is_bound(&payload, "session:foreign"));
        for (field, value) in [
            ("version", json!(2)), ("visibility_scope", json!("session:foreign")),
            ("ref_id", json!("tool://foreign")), ("tool_call_id", json!("foreign-call")),
            ("selector", json!("")), ("byte_count", Value::Null),
        ] {
            let mut invalid = payload.clone();
            invalid["runtime_raw_content_v1"][field] = value;
            assert!(!raw_anchor_is_bound(&invalid, "session:owned"));
        }
        assert!(!raw_anchor_is_bound(&json!({}), "session:owned"));
    }

    #[test]
    fn own832_bounded_snapshot_retains_typed_failure_and_marks_truncation() {
        let source = json!({"task":{"status":"blocked","last_failure":"actual backend failure"},
            "packet":{"attempt":2},"events":[{"kind":"agentic.action_applied"}],
            "long":"界".repeat(3000),"many":(0..40).collect::<Vec<_>>()});
        let value = bounded(source, &mut 2048, 0);
        assert_eq!(value["task"]["status"], "blocked");
        assert_eq!(value["packet"]["attempt"], 2);
        assert_eq!(value["long"]["truncated"], true);
        assert_eq!(value["many"][32]["omitted_array_items"], 8);
        assert!(serde_json::to_string_pretty(&value).unwrap().lines().all(|line| line.len()<2048));
        assert!(bounded(json!({"never":"accepted"}), &mut 0, 0).get("omitted").is_some());
    }
}
