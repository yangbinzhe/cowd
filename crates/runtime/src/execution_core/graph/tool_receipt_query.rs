use super::{
    receipt_payload_matches, receipt_step_digest, ExecutionCommitError, ExecutionCommitService,
    ToolReceiptReplay,
};
use crate::step_snapshot::StepCallBinding;
use harness_contract::execution_graph::ExecutionParentBinding;
use serde_json::json;

/// Bounded number of observations one stored tool receipt may carry. The
/// Runtime-minted host process closure fact occupies one of these slots.
const MAX_STORED_TOOL_OBSERVATIONS: usize = 9;

fn bounded_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn bounded_archive_bytes<'archive>(
    bytes: &'archive [u8],
    position: &mut usize,
    count: usize,
) -> Result<&'archive [u8], ExecutionCommitError> {
    let end = position
        .checked_add(count)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand("bounded source archive extent invalid".into())
        })?;
    let selected = &bytes[*position..end];
    *position = end;
    Ok(selected)
}

fn validate_bounded_source_archive(
    bytes: &[u8],
    admission: &harness_contract::context::ToolExecutionAdmission,
) -> Result<(), ExecutionCommitError> {
    use sha2::{Digest, Sha256};
    let invalid = ExecutionCommitError::InvalidCommand;
    let mut position = 0;
    if bytes.len() > 17 * 1024 * 1024
        || bounded_archive_bytes(bytes, &mut position, 8)? != b"COWD-BP1"
    {
        return Err(invalid("bounded source archive version invalid".into()));
    }
    let manifest_length = u32::from_be_bytes(
        bounded_archive_bytes(bytes, &mut position, 4)?
            .try_into()
            .map_err(|_| invalid("bounded manifest length invalid".into()))?,
    ) as usize;
    if manifest_length > 128 * 1024 {
        return Err(invalid("bounded source manifest exceeds budget".into()));
    }
    let manifest = harness_contract::step::parse_strict_json(
        std::str::from_utf8(bounded_archive_bytes(
            bytes,
            &mut position,
            manifest_length,
        )?)
        .map_err(|error| invalid(error.to_string()))?,
    )
    .map_err(|error| invalid(error.to_string()))?;
    if manifest.as_object().is_none_or(|object| object.len() != 2)
        || manifest["code_sha256"] != admission.code_sha256
    {
        return Err(invalid("bounded source manifest identity invalid".into()));
    }
    let code_length = u64::from_be_bytes(
        bounded_archive_bytes(bytes, &mut position, 8)?
            .try_into()
            .map_err(|_| invalid("bounded code length invalid".into()))?,
    );
    if code_length > 65536 {
        return Err(invalid("bounded source code exceeds budget".into()));
    }
    let code = bounded_archive_bytes(bytes, &mut position, code_length as usize)?;
    if format!("{:x}", Sha256::digest(code)) != admission.code_sha256 {
        return Err(invalid("bounded source code digest invalid".into()));
    }
    let inputs = manifest["inputs"]
        .as_array()
        .ok_or_else(|| invalid("bounded source inputs missing".into()))?;
    if inputs.len() != admission.inputs.len() || inputs.len() > 64 {
        return Err(invalid("bounded source input set mismatch".into()));
    }
    let mut total = 0u64;
    let mut previous: Option<&str> = None;
    for (entry, input) in inputs.iter().zip(&admission.inputs) {
        if entry != &json!({"path":input.relative_path,"sha256":input.sha256,"bytes":input.bytes})
            || input.bytes > 2 * 1024 * 1024
            || !bounded_hex_digest(&input.sha256)
            || input.inode == 0
            || input.relative_path.is_empty()
            || input.relative_path.len() > 1024
            || input.relative_path.contains(['\\', '\0'])
            || input
                .relative_path
                .split('/')
                .any(|component| component.is_empty() || component == "." || component == "..")
            || previous.is_some_and(|path| path >= input.relative_path.as_str())
        {
            return Err(invalid("bounded source input manifest invalid".into()));
        }
        previous = Some(input.relative_path.as_str());
        total = total
            .checked_add(input.bytes)
            .filter(|total| *total <= 16 * 1024 * 1024)
            .ok_or_else(|| invalid("bounded source total exceeds budget".into()))?;
        let length = u64::from_be_bytes(
            bounded_archive_bytes(bytes, &mut position, 8)?
                .try_into()
                .map_err(|_| invalid("bounded source length invalid".into()))?,
        );
        if length != input.bytes {
            return Err(invalid("bounded source file extent mismatch".into()));
        }
        let contents = bounded_archive_bytes(bytes, &mut position, length as usize)?;
        if format!("{:x}", Sha256::digest(contents)) != input.sha256 {
            return Err(invalid("bounded source file digest mismatch".into()));
        }
    }
    if position != bytes.len() {
        return Err(invalid("bounded source archive trailing bytes".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bound_tool_batch::scope_admission_tests::bound_request;

    #[test]
    fn both_query_fast_paths_replay_legacy_but_reject_claimed_v2_without_purposes() {
        for corrupt in [false, true] {
            let (store, service, graph, _root, _owner) =
                super::super::scope_commit_tests::root_fixture();
            let workspace = tempfile::tempdir().unwrap();
            let host = tools::ToolHost::builtin("scope2-query-classifier", workspace.path());
            let mut request = bound_request(&host, "read_file", json!({"path":"proof.txt"}));
            let parent = ExecutionParentBinding {
                execution_id: graph.id,
                node_id: "scope2-node".into(),
            };
            request.parent_execution = Some(parent.clone());
            request.parent_execution_attempt = Some(1);
            let effect = &request.compiled_invocation.as_ref().unwrap().effect;
            let scope = json!({
                "version":if corrupt { 2 } else { 1 },
                "descriptor_hash":effect.descriptor_hash,"complete":false,"targets":[],
            });
            let stream = format!("execution-effect:{}", request.idempotency_key);
            let outcome = crate::RuntimeToolExecutionOutcome {
                pending_agent_batch: None,
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some("original raw result".into()),
                error: None,
                evidence_ref: "query-original".into(),
                observed_evidence: vec![],
            };
            store.append_transaction(crate::runtime_event_store::AppendTransactionRequest {
                transaction_id:"query-terminal".into(),
                expected_streams:vec![crate::runtime_event_store::ExpectedStreamRevision {
                    stream_id:stream.clone(), expected_revision:0,
                }],
                events:vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                    event:crate::runtime_event_store::RuntimeEventInput {
                        stream_id:stream.clone(), scope:crate::RuntimeEventScope::ExecutionNode,
                        kind:"execution.read.receipt".into(), status:Some("completed".into()),
                        actor:Some("governed_tool".into()), refs:vec![],
                        payload:json!({
                            "idempotency_key":request.idempotency_key,"tool_name":request.tool_name,
                            "replay":super::super::receipt_replay_contract(&request).unwrap(),
                            "input_sha256":crate::context_evidence::read_sha256(request.input.as_bytes()),
                            "resolved_effect_scope":scope,"outcome":outcome,
                        }),
                    },
                    idempotency_key:Some(format!("{}:read-receipt", request.idempotency_key)),
                    schema_version:1,
                }],
            }).unwrap();
            let query = ToolReceiptQuery {
                step_binding: request.step_binding.as_ref().unwrap(),
                parent_execution: &parent,
                session_id: "scope2-session",
                policy_revision: 1,
                idempotency_key: &request.idempotency_key,
                tool_use_id: &request.tool_use_id,
                tool_name: &request.tool_name,
                input: &request.input,
            };
            for result in [
                service.query_completed_tool_effect(&query),
                service.query_completed_agent_tool_effect(&query),
            ] {
                if corrupt {
                    assert!(result.is_err());
                } else {
                    assert_eq!(
                        result.unwrap().unwrap().outcome.output.as_deref(),
                        Some("original raw result")
                    );
                }
            }
            assert_eq!(store.stream_revision(&stream).unwrap(), 1);
            assert!(!workspace.path().join("proof.txt").exists());
        }
    }
}

pub(crate) struct ToolReceiptQuery<'a> {
    pub(crate) step_binding: &'a StepCallBinding,
    pub(crate) parent_execution: &'a ExecutionParentBinding,
    pub(crate) session_id: &'a str,
    pub(crate) policy_revision: u64,
    pub(crate) idempotency_key: &'a str,
    pub(crate) tool_use_id: &'a str,
    pub(crate) tool_name: &'a str,
    pub(crate) input: &'a str,
}

impl ExecutionCommitService {
    fn validate_bounded_parent_history(
        &self,
        primary: &crate::DurableRuntimeEvent,
        intent: &crate::DurableRuntimeEvent,
    ) -> Result<(), ExecutionCommitError> {
        use super::ExecutionGraphEvent;
        use harness_contract::execution_graph::ExecutionNodeStatus;
        let invalid = ExecutionCommitError::InvalidCommand;
        let parent = &primary.payload["replay"]["parent_execution"];
        if parent.is_null() {
            return Ok(());
        }
        let graph = parent["execution_id"]
            .as_str()
            .ok_or_else(|| invalid("process historical parent graph missing".into()))?;
        let node = parent["node_id"]
            .as_str()
            .ok_or_else(|| invalid("process historical parent node missing".into()))?;
        let attempt = primary
            .refs
            .iter()
            .find(|reference| reference.kind == "agent_attempt")
            .and_then(|reference| reference.id.parse::<u32>().ok())
            .filter(|attempt| *attempt > 0)
            .ok_or_else(|| invalid("process historical parent attempt missing".into()))?;
        let stream = format!("{graph}:node:{node}");
        let mut sequence = self.event_store.stream_revision(&stream)?;
        let mut budget = 0usize;
        while sequence > 0 {
            let records = self
                .event_store
                .list_stream_before(&stream, sequence, 100, 1024 * 1024)
                .map_err(invalid)?;
            if records.is_empty() {
                return Err(invalid("process historical parent history gap".into()));
            }
            for running in records {
                if running.sequence != sequence {
                    return Err(invalid(
                        "process historical parent sequence mismatch".into(),
                    ));
                }
                sequence = sequence.checked_sub(1).ok_or_else(|| {
                    invalid("process historical parent sequence underflow".into())
                })?;
                budget = budget.saturating_add(serde_json::to_vec(&running)?.len());
                if budget > 4 * 1024 * 1024 {
                    return Err(invalid(
                        "HistoryBudgetExceeded: process original parent".into(),
                    ));
                }
                if (running.commit_cursor, running.transaction_index)
                    > (intent.commit_cursor, intent.transaction_index)
                {
                    continue;
                }
                if running.kind != "execution_node.transitioned"
                    || running.schema_version != 1
                    || running.actor.as_deref() != Some("execution_commit_service")
                    || running.status.as_deref() != Some("running")
                    || running.scope != crate::RuntimeEventScope::ExecutionNode
                    || running.payload["to"] != "running"
                    || running.payload["graph_id"] != graph
                    || running.payload["node_id"] != node
                {
                    return Err(invalid(
                        "process parent was not Running at original intent cursor".into(),
                    ));
                }
                let revision = running.payload["graph_revision"]
                    .as_u64()
                    .filter(|revision| *revision > 0)
                    .ok_or_else(|| invalid("process historical graph revision missing".into()))?;
                let start = self
                    .event_store
                    .list_stream_after(graph, revision - 1, revision, 1, 4 * 1024 * 1024)
                    .map_err(invalid)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| invalid("process historical graph start missing".into()))?;
                if start.schema_version != 2
                    || start.actor.as_deref() != Some("execution_commit_service")
                    || start.scope != crate::RuntimeEventScope::ExecutionGraph
                    || start.sequence != revision
                    || start.stream_id != graph
                    || start.transaction_id != running.transaction_id
                    || start.commit_cursor != running.commit_cursor
                    || start.payload["bytes"]
                        .as_u64()
                        .is_none_or(|bytes| bytes > 1024 * 1024)
                {
                    return Err(invalid("process original NodeStart source mismatch".into()));
                }
                let source_bytes = start.payload["bytes"].as_u64().ok_or_else(|| {
                    invalid("process original NodeStart body extent missing".into())
                })? as usize;
                budget = budget
                    .saturating_add(serde_json::to_vec(&start)?.len())
                    .saturating_add(source_bytes);
                if budget > 4 * 1024 * 1024 {
                    return Err(invalid(
                        "HistoryBudgetExceeded: process original NodeStart content".into(),
                    ));
                }
                let activity = running
                    .activity_binding()
                    .ok_or_else(|| invalid("process historical activity binding missing".into()))?;
                activity
                    .validate()
                    .map_err(|error| invalid(error.to_string()))?;
                if activity.node_id.as_deref() != Some(node)
                    || primary.payload["replay"]["session_id"] != activity.session_id
                {
                    return Err(invalid(
                        "process historical NodeStart session/owner conflict".into(),
                    ));
                }
                let event: ExecutionGraphEvent = serde_json::from_value(
                    self.event_store
                        .resolve_graph_record(&start)
                        .map_err(invalid)?,
                )?;
                if event.kind() != start.kind {
                    return Err(invalid("process original NodeStart kind mismatch".into()));
                }
                let binding = match &event {
                    ExecutionGraphEvent::NodesStarted { bindings, .. } => bindings.get(node),
                    ExecutionGraphEvent::NodeTransitioned {
                        node_id,
                        to: ExecutionNodeStatus::Running,
                        binding,
                        ..
                    } if node_id == node => binding.as_ref(),
                    ExecutionGraphEvent::Checkpoint {
                        cause, bindings, ..
                    } if matches!(
                        cause.as_str(),
                        "execution_graph.nodes_started" | "execution_graph.node_transitioned"
                    ) =>
                    {
                        bindings.get(node)
                    }
                    _ => None,
                };
                if binding.is_none_or(|binding| {
                    binding.attempt != attempt || binding.ticket_idempotency_key.is_empty()
                }) {
                    return Err(invalid(
                        "process historical parent NodeStart attempt mismatch".into(),
                    ));
                }
                return Ok(());
            }
        }
        Err(invalid("process historical NodeStart not found".into()))
    }

    pub(super) fn validate_stored_bounded_process(
        &self,
        primary: &crate::DurableRuntimeEvent,
    ) -> Result<(), ExecutionCommitError> {
        use base64::Engine;
        use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionAdmission};
        use sha2::{Digest, Sha256};
        let invalid = ExecutionCommitError::InvalidCommand;
        let key = primary.payload["idempotency_key"]
            .as_str()
            .ok_or_else(|| invalid("terminal call key missing".into()))?;
        let observations = primary.payload["outcome"]["observed_evidence"].as_array();
        if observations.is_some_and(|items| items.len() > MAX_STORED_TOOL_OBSERVATIONS) {
            return Err(invalid(
                "stored tool observations exceed metadata bounds".into(),
            ));
        }
        let claimed = observations.is_some_and(|items| {
            items
                .iter()
                .any(|observed| observed["target"]["kind"] == "tool_execution")
        });
        let stored = self
            .event_store
            .event_by_idempotency_key(&primary.stream_id, &format!("{key}:process-admission"))?;
        if !claimed && stored.is_none() && primary.payload["process_admission_ref"].is_null() {
            return Ok(());
        }
        if !claimed {
            return Err(invalid(
                "admitted process terminal lost its typed execution fact".into(),
            ));
        }
        let outcome: crate::RuntimeToolExecutionOutcome =
            serde_json::from_value(primary.payload["outcome"].clone())?;
        if serde_json::to_vec(&outcome.observed_evidence)?.len() > 128 * 1024 {
            return Err(invalid("stored process metadata exceeds bounds".into()));
        }
        let facts = outcome
            .observed_evidence
            .iter()
            .filter_map(|observed| match &observed.target {
                EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
                _ => None,
            })
            .collect::<Vec<_>>();
        if facts.len() != 1 {
            return Err(invalid(
                "one process leaf must have exactly one private terminal fact".into(),
            ));
        }
        let fact = facts[0];
        if stored.is_none() && primary.payload["process_admission_ref"].is_null() {
            // No private process admission was ever published for this call,
            // so this terminal can only be the Runtime-minted host process
            // fact: a host shell proves quiescence, not a private capability.
            crate::host_process_closure::validate_host_process_fact(fact).map_err(invalid)?;
            return Ok(());
        }
        let admission =
            stored.ok_or_else(|| invalid("process terminal admission missing".into()))?;
        let intent = self
            .event_store
            .event_by_idempotency_key(&primary.stream_id, &format!("{key}:intent"))?
            .ok_or_else(|| invalid("process terminal intent missing".into()))?;
        self.validate_bounded_parent_history(primary, &intent)?;
        let original: ToolExecutionAdmission =
            serde_json::from_value(admission.payload["admission"].clone())?;
        let original_input = admission.payload["original_input"]
            .as_str()
            .ok_or_else(|| invalid("process admission original input missing".into()))?;
        if original_input.len() > 131072
            || intent.payload["input_sha256"]
                != format!("sha256:{:x}", Sha256::digest(original_input.as_bytes()))
            || receipt_payload_matches(original_input, &intent.payload)? != Some(true)
        {
            return Err(invalid(
                "process admission original raw input disagrees with intent".into(),
            ));
        }
        let input = harness_contract::step::parse_strict_json(original_input)
            .map_err(|error| invalid(error.to_string()))?;
        let effect: harness_contract::tool::ToolEffectDescriptor =
            serde_json::from_value(intent.payload["effect"].clone())?;
        let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&original)?));
        if fact.version != 1
            || original.version != 1
            || fact.admission != original
            || admission.payload["version"] != 1
            || admission.payload["admission_sha256"] != digest
            || admission.kind != "execution.process.admitted"
            || admission.actor != intent.actor
            || admission.refs != intent.refs
            || (intent.commit_cursor, intent.transaction_index)
                >= (admission.commit_cursor, admission.transaction_index)
            || (admission.commit_cursor, admission.transaction_index)
                >= (primary.commit_cursor, primary.transaction_index)
            || original.binding.call_key != key
            || original.binding.tool_use_id != outcome.tool_use_id
            || original.binding.input_sha256 != Self::bounded_process_input_sha256(&input)?
            || original.binding.descriptor_sha256
                != Self::bounded_process_descriptor_sha256(&effect)?
            || primary.payload["process_admission_ref"] != format!("{key}:process-admission")
            || original.inputs.len() > 64
            || fact.body.bytes > 6 * 1024 * 1024
            || original.source_bundle.bytes > 17 * 1024 * 1024
            || fact.body.media_type != "application/json"
            || original.source_bundle.media_type != "application/octet-stream"
            || !bounded_hex_digest(&original.binding.input_sha256)
            || !bounded_hex_digest(&original.binding.authority_sha256)
            || !bounded_hex_digest(&original.binding.descriptor_sha256)
            || !bounded_hex_digest(&original.code_sha256)
            || !bounded_hex_digest(&fact.stdout_sha256)
            || !bounded_hex_digest(&fact.stderr_sha256)
        {
            return Err(invalid(
                "stored process terminal provenance conflict".into(),
            ));
        }
        let artifacts = self.bounded_process_artifacts()?;
        let source_bytes = artifacts
            .read_blocking(
                &original.source_bundle,
                &original.source_bundle.visibility_scope,
                None,
            )
            .map_err(|error| invalid(format!("original process source archive: {error}")))?;
        validate_bounded_source_archive(&source_bytes, &original)?;
        let bytes = artifacts
            .read_blocking(&fact.body, &fact.body.visibility_scope, None)
            .map_err(|error| invalid(format!("original process body archive: {error}")))?;
        let body = harness_contract::step::parse_strict_json(
            std::str::from_utf8(&bytes).map_err(|error| invalid(error.to_string()))?,
        )
        .map_err(|error| invalid(error.to_string()))?;
        if body.as_object().is_none_or(|object| {
            object.len() < 10
                || object.len() > 12
                || object.keys().any(|key| {
                    ![
                        "version",
                        "admission_sha256",
                        "terminal",
                        "stdout_base64",
                        "stderr_base64",
                        "stdout_sha256",
                        "stderr_sha256",
                        "stdout_bytes",
                        "stderr_bytes",
                        "capture_complete",
                        "stdout_utf8",
                        "stderr_utf8",
                    ]
                    .contains(&key.as_str())
                })
        }) || body["version"] != 1
            || body["admission_sha256"] != digest
            || body["terminal"] != serde_json::to_value(&fact.terminal)?
            || body["stdout_sha256"] != fact.stdout_sha256
            || body["stderr_sha256"] != fact.stderr_sha256
            || body["stdout_bytes"] != fact.stdout_bytes
            || body["stderr_bytes"] != fact.stderr_bytes
            || body["capture_complete"] != fact.capture_complete
        {
            return Err(invalid("process body/fact provenance conflict".into()));
        }
        for (field, length, hash) in [
            ("stdout_base64", fact.stdout_bytes, &fact.stdout_sha256),
            ("stderr_base64", fact.stderr_bytes, &fact.stderr_sha256),
        ] {
            if length > 2 * 1024 * 1024 {
                return Err(invalid("process capture exceeds bounds".into()));
            }
            let encoded = body[field]
                .as_str()
                .ok_or_else(|| invalid("process captured bytes missing".into()))?;
            if encoded.len() as u64 != length.div_ceil(3) * 4 {
                return Err(invalid(
                    "process captured encoding exceeds original extent".into(),
                ));
            }
            let raw = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|error| invalid(error.to_string()))?;
            if raw.len() as u64 != length || format!("{:x}", Sha256::digest(&raw)) != *hash {
                return Err(invalid("process captured bytes hash mismatch".into()));
            }
            let text_field = if field == "stdout_base64" {
                "stdout_utf8"
            } else {
                "stderr_utf8"
            };
            if let Some(text) = body.get(text_field) {
                if raw.len() > 131072
                    || text
                        .as_str()
                        .is_none_or(|text| text.as_bytes() != raw.as_slice())
                    || std::str::from_utf8(&raw).is_err()
                {
                    return Err(invalid(
                        "process optional UTF8 text is not the exact entire captured stream".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn query_completed_tool_effect(
        &self,
        query: &ToolReceiptQuery<'_>,
    ) -> Result<Option<ToolReceiptReplay>, ExecutionCommitError> {
        self.query_completed_tool_effect_scoped(query, false)
    }

    pub(crate) fn query_completed_agent_tool_effect(
        &self,
        query: &ToolReceiptQuery<'_>,
    ) -> Result<Option<ToolReceiptReplay>, ExecutionCommitError> {
        self.query_completed_tool_effect_scoped(query, true)
    }

    fn query_completed_tool_effect_scoped(
        &self,
        query: &ToolReceiptQuery<'_>,
        exact_parent_node: bool,
    ) -> Result<Option<ToolReceiptReplay>, ExecutionCommitError> {
        let binding = query.step_binding;
        let parent = query.parent_execution;
        let stream = format!("execution-effect:{}", query.idempotency_key);
        for suffix in ["receipt", "read-receipt"] {
            let Some(receipt) = self.event_store.event_by_idempotency_key(
                &stream,
                &format!("{}:{suffix}", query.idempotency_key),
            )?
            else {
                continue;
            };
            let Some(replay) = receipt
                .payload
                .get("replay")
                .filter(|value| !value.is_null())
            else {
                return Err(ExecutionCommitError::InvalidCommand(
                    "existing terminal receipt has no replay identity; refusing fresh execution"
                        .into(),
                ));
            };
            let graph = self
                .effect_authority_graphs
                .load(&parent.execution_id)
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
            if graph.lineage.as_ref().is_none_or(|lineage| {
                lineage.session_id != query.session_id
                    || lineage.turn_id != binding.snapshot.identity.turn_id
            }) || !graph.nodes.iter().any(|node| node.id == parent.node_id)
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "receipt reader parent lineage mismatch".into(),
                ));
            }
            let root_record = &receipt.payload["root_effect_owner"];
            let root_replay = replay["parent_execution"].is_null();
            let original_parent_matches = if root_replay {
                root_record["binding"]["root_execution_id"] == json!(parent.execution_id)
                    && (!exact_parent_node
                        || root_record["binding"]["node_id"] == json!(parent.node_id))
                    && root_record["binding"]["session_id"] == json!(query.session_id)
                    && root_record["binding"]["turn_id"] == json!(binding.snapshot.identity.turn_id)
                    && root_record["attempt"]
                        .as_u64()
                        .is_some_and(|attempt| attempt > 0)
                    && root_record["graph_revision"]
                        .as_u64()
                        .is_some_and(|revision| revision > 0)
                    && root_record
                        .as_object()
                        .is_some_and(|record| record.len() == 3)
            } else {
                replay["parent_execution"]["execution_id"] == json!(parent.execution_id)
                    && (!exact_parent_node
                        || replay["parent_execution"]["node_id"] == json!(parent.node_id))
            };
            if query.policy_revision == 0
                || query.session_id != binding.snapshot.identity.session_id
                || replay["session_id"] != json!(query.session_id)
                || !original_parent_matches
                || replay["call_id"] != query.tool_use_id
                || replay["origin"] != json!(binding.origin)
                || (receipt.payload["resolved_effect_scope"]["version"] == 2
                    && replay["step_snapshot_digest"] != json!(receipt_step_digest(binding)?))
                || receipt.payload["tool_name"] != query.tool_name
                || receipt_payload_matches(query.input, &receipt.payload)? != Some(true)
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "tool receipt read identity or canonical payload conflict".into(),
                ));
            }
            let mut invocation: harness_contract::tool::GovernedToolInvocation =
                serde_json::from_value(replay["invocation"].clone())?;
            if invocation.origin.as_ref() != Some(&binding.origin)
                || invocation.invocation_id != query.tool_use_id
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "stored receipt invocation identity conflict".into(),
                ));
            }
            self.validate_stored_terminal_scope(
                &invocation.effect,
                &receipt,
                suffix == "read-receipt",
            )?;
            invocation.intent.normalized_input =
                harness_contract::step::parse_strict_json(query.input)
                    .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
            let outcome: crate::RuntimeToolExecutionOutcome =
                serde_json::from_value(receipt.payload["outcome"].clone())?;
            let lifecycle = if let Some(value) = receipt.payload.get("lifecycle_anchor").filter(|value| !value.is_null()) {
                use sha2::{Digest, Sha256};
                let anchor: crate::tool_host::ToolLifecycleAnchor = serde_json::from_value(value.clone())?;
                let producer = &anchor.producer;
                if anchor.invocation_id.is_empty()
                    || anchor.session_id != query.session_id
                    || anchor.tool_call_id != query.tool_use_id
                    || anchor.tool_name != query.tool_name
                    || producer.validate().is_err()
                    || producer.session_id != query.session_id
                    || producer.turn_id != binding.snapshot.identity.turn_id
                    || graph.lineage.as_ref().is_none_or(|lineage|
                        producer.root_task_id != lineage.root_task_id
                            || producer.task_id != lineage.task_id
                            || producer.generation != lineage.generation)
                    || (graph.parent_execution.is_none() && producer.root_execution_id != graph.id)
                    || producer.tool_call_id.as_deref() != Some(query.tool_use_id)
                    || producer.tool_contract_id.as_deref() != Some(query.tool_name)
                {
                    return Err(ExecutionCommitError::InvalidCommand("stored lifecycle anchor identity conflict".into()));
                }
                let stream = format!("session:{}", query.session_id);
                let key = format!("tool-lifecycle:{}:tool.invocation.completed", anchor.invocation_id);
                match self.event_store.event_by_idempotency_key(&stream, &key)? {
                    Some(event) if !outcome.model_feedback_is_error()
                        && receipt.payload["outcome_truncated"] != true
                        && outcome.output.is_some() => {
                        let raw = &event.payload["runtime_raw_content_v1"];
                        let body = outcome.output.as_deref().unwrap_or_default();
                        let body_hash = format!("{:x}", Sha256::digest(body.as_bytes()));
                        if event.kind != "tool.invocation.completed"
                            || event.scope != crate::RuntimeEventScope::Tool
                            || event.status.as_deref() != Some("completed")
                            || event.actor.as_deref() != Some("conversation_runtime")
                            || event.activity_binding().as_ref() != Some(producer)
                            || event.payload["invocation_id"] != anchor.invocation_id
                            || event.payload["tool_call_id"] != query.tool_use_id
                            || event.payload["tool_name"] != query.tool_name
                            || event.payload["input_hash"] != anchor.input_hash
                            || raw["tool_call_id"] != query.tool_use_id
                            || raw["visibility_scope"] != format!("session:{}", query.session_id)
                            || raw["byte_count"] != body.len() as u64
                            || raw["sha256"].as_str().map(|hash| hash.strip_prefix("sha256:").unwrap_or(hash)) != Some(body_hash.as_str())
                        {
                            return Err(ExecutionCommitError::InvalidCommand("original tool lifecycle conflicts with its receipt anchor".into()));
                        }
                        Some(event)
                    }
                    _ => None,
                }
            } else { None };
            return Ok(Some(ToolReceiptReplay {
                invocation,
                outcome,
                completed_at_ms: receipt.created_at_ms,
                lifecycle,
            }));
        }
        if self
            .event_store
            .event_by_idempotency_key(
                &stream,
                &format!("{}:process-admission", query.idempotency_key),
            )?
            .is_some()
        {
            return Err(ExecutionCommitError::InvalidCommand("bounded process admitted without terminal; private handle unavailable, refusing fresh execution".into()));
        }
        Ok(None)
    }
}
