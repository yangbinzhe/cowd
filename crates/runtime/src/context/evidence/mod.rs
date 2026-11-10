pub mod projection;
pub mod raw;
mod grep;
pub(crate) use grep::{grep_original_input_matches, grep_read_target, grep_read_target_for_parent, grep_read_target_for_composite, original_read_file_domain};

use harness_contract::context::EvidenceContentKind;
use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::context_ledger::estimate_text_tokens;

pub use harness_contract::context::EvidenceAuditProjection as AuditProjection;
pub use projection::audit_projection;

pub(crate) const READ_ACQUISITION_FIELD: &str = "read_acquisition_v1";
pub(crate) const CHILD_READ_ACQUISITIONS_FIELD: &str = "child_read_acquisitions_v1";
pub(crate) fn native_read_candidate_tool(name: &str) -> bool {
    matches!(name, "read_file" | "read_ref" | "evidence_retrieve" | "tool_orchestrate" | "grep_search")
}
pub(crate) const NATIVE_DELIVERY_KIND: &str = "context.model_input_observed";
pub(crate) const PROCESS_DELIVERY_KIND: &str = "context.agent_channel_sent";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadBodySeal {
    pub schema_version: u32,
    pub source_ref: String,
    pub source_sha256: String,
    pub source_bytes: u64,
    pub output_sha256: String,
    pub output_bytes: u64,
    pub extent: ReadExtent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ReadExtent {
    FileLines {
        start_line: u64,
        line_count: u64,
        total_lines: u64,
        ends_with_newline: bool,
        body_sha256: String,
        body_bytes: u64,
        source_omitted: bool,
    },
    ArtifactChunks {
        total_chunks: u64,
        chunks: Vec<ReadChunkSeal>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadChunkSeal {
    pub index: u64,
    pub body_sha256: String,
    pub body_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadAcquisitionSeal {
    pub schema_version: u32,
    pub primary_stream: String,
    pub primary_key: String,
    pub index_stream: String,
    pub index_key: String,
    pub session_id: String,
    pub invocation_id: String,
    pub tool_name: String,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub step: harness_contract::step::StepIdentity,
    pub origin: serde_json::Value,
    pub input_sha256: String,
    pub body: ReadBodySeal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReadDeliveryGrade {
    AgentChannelSend,
    ProviderModelObserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RootReadOwner {
    pub stream_id: String,
    pub sequence: u64,
    pub event_id: String,
    pub payload_digest: String,
    pub binding: harness_contract::projection::RuntimeActivityBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ReadDeliveryRecipient {
    AgentTask {
        identity: harness_contract::execution::ExecutionIdentity,
    },
    Root {
        owner: RootReadOwner,
        identity: Option<harness_contract::execution::ExecutionIdentity>,
    },
}

pub(crate) fn read_payload_digest(payload: &serde_json::Value) -> String {
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(payload).expect("JSON payload is serializable"))
    )
}

pub(crate) fn root_read_owner_matches(
    owner: &RootReadOwner,
    event: &crate::runtime_event_store::DurableRuntimeEvent,
    acquisition: &ReadAcquisitionSeal,
) -> bool {
    owner.stream_id == event.stream_id
        && owner.sequence == event.sequence
        && owner.event_id == event.event_id
        && owner.payload_digest == read_payload_digest(&event.payload)
        && event.kind == "tool.invocation.completed"
        && event.scope == crate::runtime_event_store::RuntimeEventScope::Tool
        && event.actor.as_deref() == Some("conversation_runtime")
        && event.status.as_deref() == Some("completed")
        && event.activity_binding().as_ref() == Some(&owner.binding)
        && owner.binding.validate().is_ok()
        && owner.stream_id == format!("session:{}", acquisition.session_id)
        && owner.binding.session_id == acquisition.session_id
        && owner.binding.turn_id == acquisition.step.turn_id
        && Some(owner.binding.root_execution_id.as_str())
            == acquisition.step.execution_id.as_deref()
        && owner.binding.activity_id.starts_with(&format!(
            "activity:execution:{}:owner:",
            owner.binding.root_execution_id
        ))
        && owner.binding.tool_call_id.as_deref() == Some(acquisition.invocation_id.as_str())
        && owner.binding.tool_contract_id.as_deref() == Some(acquisition.tool_name.as_str())
        && event.payload["status"] == "completed"
        && event.payload["session_id"] == acquisition.session_id
        && event.payload["tool_call_id"] == acquisition.invocation_id
        && event.payload["tool_name"] == acquisition.tool_name
        && event.payload["is_error"].as_bool() != Some(true)
        && event.refs.iter().any(|reference| {
            reference.kind == "tool_call" && reference.id == acquisition.invocation_id
        })
        && event.refs.iter().any(|reference| {
            reference.kind == "tool_invocation" && event.payload["invocation_id"] == reference.id
        })
        && event.refs.iter().any(|reference| {
            reference.kind == "execution"
                && Some(reference.id.as_str()) == acquisition.step.execution_id.as_deref()
        })
        && event
            .refs
            .iter()
            .any(|reference| reference.kind == "session" && reference.id == acquisition.session_id)
        && event
            .refs
            .iter()
            .any(|reference| reference.kind == "turn" && reference.id == acquisition.step.turn_id)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadDeliveryMaterial {
    pub schema_version: u32,
    pub grade: ReadDeliveryGrade,
    pub acquisition: ReadAcquisitionSeal,
    pub acquisition_event_id: String,
    pub acquisition_cursor: u64,
    pub index_event_id: String,
    pub session_id: String,
    pub execution_id: String,
    pub turn_id: String,
    pub tool_invocation_id: String,
    pub recipient: ReadDeliveryRecipient,
    pub presentation_sha256: String,
    pub presentation_bytes: u64,
    pub body_omitted: bool,
    pub delivered_extent: Option<ReadExtent>,
    pub transport: ReadDeliveryTransport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ReadDeliveryTransport {
    NativeProviderContinuation {
        request_id: String,
        request_sequence: usize,
        provider_attempt: u32,
        step_render: harness_contract::step::StepRenderRef,
        wire_body_sha256: String,
        wire_artifact: harness_contract::context::ArtifactRef,
        packed_sequence: usize,
        outcome_sequence: usize,
        model: String,
        raw_ref: EvidenceRef,
    },
    ProcessChannelSend {
        worker_id: String,
        request_id: String,
        sequence: u64,
        step_id: String,
        request_fingerprint: String,
        response_sha256: String,
        response_bytes: u64,
    },
}

pub(crate) fn decode_read_delivery(
    value: &serde_json::Value,
) -> Result<ReadDeliveryMaterial, &'static str> {
    let material: ReadDeliveryMaterial =
        serde_json::from_value(value.clone()).map_err(|_| "invalid read delivery material")?;
    decode_read_acquisition(
        &serde_json::to_value(&material.acquisition).map_err(|_| "invalid acquisition")?,
    )?;
    let grade_matches = matches!(
        (&material.grade, &material.transport),
        (
            ReadDeliveryGrade::ProviderModelObserved,
            ReadDeliveryTransport::NativeProviderContinuation { .. }
        ) | (
            ReadDeliveryGrade::AgentChannelSend,
            ReadDeliveryTransport::ProcessChannelSend { .. }
        )
    );
    let transport_valid = match &material.transport {
        ReadDeliveryTransport::NativeProviderContinuation {
            request_id,
            step_render,
            wire_body_sha256,
            wire_artifact,
            packed_sequence,
            outcome_sequence,
            model,
            ..
        } => {
            !request_id.is_empty()
                && !step_render.step_id.is_empty()
                && !wire_body_sha256.is_empty()
                && wire_artifact.is_durable()
                && !wire_artifact.selector.is_empty()
                && outcome_sequence > packed_sequence
                && !model.is_empty()
        }
        ReadDeliveryTransport::ProcessChannelSend {
            worker_id,
            request_id,
            step_id,
            request_fingerprint,
            response_sha256,
            response_bytes,
            ..
        } => {
            !worker_id.is_empty()
                && !request_id.is_empty()
                && !step_id.is_empty()
                && !request_fingerprint.is_empty()
                && !response_sha256.is_empty()
                && *response_bytes > 0
        }
    };
    let recipient_valid = match &material.recipient {
        ReadDeliveryRecipient::AgentTask { identity } => {
            identity.kind() == harness_contract::execution::ExecutionIdentityKind::AgentNode
                && identity.graph_id() == Some(material.acquisition.graph_id.as_str())
                && identity.node_id() == Some(material.acquisition.node_id.as_str())
                && identity.agent_run_id().is_some()
                && identity.agent_run_id() == material.acquisition.step.run_ref.as_deref()
                && identity.session_id() == Some(material.session_id.as_str())
                && identity.turn_id() == Some(material.turn_id.as_str())
        }
        ReadDeliveryRecipient::Root { owner, identity } => {
            owner.binding.validate().is_ok()
                && owner.sequence > 0
                && !owner.event_id.is_empty()
                && owner.payload_digest.len() == 64
                && owner
                    .payload_digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
                && owner.stream_id == format!("session:{}", material.session_id)
                && owner.binding.root_execution_id == material.execution_id
                && owner.binding.session_id == material.session_id
                && owner.binding.turn_id == material.turn_id
                && material.acquisition.step.execution_id.as_deref()
                    == Some(material.execution_id.as_str())
                && owner.binding.tool_call_id.as_deref()
                    == Some(material.tool_invocation_id.as_str())
                && owner.binding.tool_contract_id.as_deref()
                    == Some(material.acquisition.tool_name.as_str())
                && owner.binding.activity_id.starts_with(&format!(
                    "activity:execution:{}:owner:",
                    material.execution_id
                ))
                && identity.as_ref().is_none_or(|identity| {
                    matches!(
                        identity.kind(),
                        harness_contract::execution::ExecutionIdentityKind::TaskGraph
                            | harness_contract::execution::ExecutionIdentityKind::SessionTurn
                    ) && identity.session_id() == Some(material.session_id.as_str())
                        && identity.turn_id() == Some(material.turn_id.as_str())
                        && identity
                            .task_id()
                            .is_none_or(|task| task == owner.binding.task_id)
                        && material.acquisition.step.actor_ref.as_deref()
                            == Some(identity.principal_id())
                        && material.acquisition.step.task_ref.as_deref() == identity.task_id()
                })
        }
    };
    if material.schema_version != 1
        || !grade_matches
        || !transport_valid
        || !recipient_valid
        || material.acquisition_cursor == 0
        || material.acquisition_event_id.is_empty()
        || material.index_event_id.is_empty()
        || material.execution_id.is_empty()
        || material.turn_id.is_empty()
        || material.session_id != material.acquisition.session_id
        || material.tool_invocation_id != material.acquisition.invocation_id
        || (material.body_omitted && material.delivered_extent.is_some())
        || (!material.body_omitted
            && material.delivered_extent.as_ref() != Some(&material.acquisition.body.extent))
    {
        return Err("unknown, mismatched or unbound read delivery material");
    }
    Ok(material)
}

pub(crate) fn read_sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

/// One decoder for original read acquisition and its compact receipt writer.
/// An encoding label cannot silently change which bytes the receipt authenticates.
pub(crate) fn read_chunk_bytes<'a>(
    chunk: &'a serde_json::Value,
    page: &serde_json::Value,
) -> Option<std::borrow::Cow<'a, [u8]>> {
    let content = chunk["content"].as_str()?;
    match chunk["encoding"]
        .as_str()
        .or_else(|| page["encoding"].as_str())
    {
        None | Some("utf8") => Some(std::borrow::Cow::Borrowed(content.as_bytes())),
        Some("base64") => {
            use base64::Engine;
            Some(std::borrow::Cow::Owned(
                base64::engine::general_purpose::STANDARD
                    .decode(content)
                    .ok()?,
            ))
        }
        Some(_) => None,
    }
}

pub(crate) fn read_body_seal(tool_name: &str, output: &str) -> Option<ReadBodySeal> {
    let value: serde_json::Value = serde_json::from_str(output).ok()?;
    let hash = |value: &serde_json::Value| {
        let digest = value.as_str()?.trim_start_matches("sha256:");
        (digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| format!("sha256:{}", digest.to_ascii_lowercase()))
    };
    let (source_ref, source_sha256, source_bytes, extent) = if tool_name == "read_file" {
        if value["type"] != "text" {
            return None;
        }
        let file = &value["file"];
        let body = file["content"].as_str()?;
        let start_line = file["startLine"].as_u64()?;
        let line_count = file["numLines"].as_u64()?;
        let total_lines = file["totalLines"].as_u64()?;
        if start_line == 0 || start_line.checked_sub(1)?.checked_add(line_count)? > total_lines {
            return None;
        }
        let expected_lines = if line_count == 0 {
            0
        } else {
            body.split('\n').count() as u64
        };
        if expected_lines != line_count {
            return None;
        }
        (
            file["filePath"].as_str()?.to_string(),
            hash(&file["sha256"])?,
            file["byteLength"].as_u64()?,
            ReadExtent::FileLines {
                start_line,
                line_count,
                total_lines,
                ends_with_newline: file["endsWithNewline"].as_bool()?,
                body_sha256: read_sha256(body.as_bytes()),
                body_bytes: body.len() as u64,
                source_omitted: start_line != 1
                    || line_count != total_lines
                    || value["truncated"].as_bool()?,
            },
        )
    } else if matches!(tool_name, "read_ref" | "evidence_retrieve") {
        if value["kind"] != "evidence_retrieve" || value["available"] != true {
            return None;
        }
        let total_chunks = value["total_chunks"].as_u64()?;
        let values = value["chunks"].as_array()?;
        if values.len() > 16 {
            return None;
        }
        if total_chunks == 0 {
            if !values.is_empty()
                || value["bytes"].as_u64()? != 0
                || hash(&value["sha256"])? != read_sha256(b"")
            {
                return None;
            }
        } else if values.is_empty() {
            return None;
        }
        let mut chunks = Vec::with_capacity(values.len());
        for chunk in values {
            let index = chunk["index"].as_u64()?;
            if index >= total_chunks
                || chunks
                    .iter()
                    .any(|existing: &ReadChunkSeal| existing.index == index)
            {
                return None;
            }
            let (body_sha256, body_bytes) = if chunk["content"].is_string() {
                let bytes = read_chunk_bytes(chunk, &value)?;
                if let Some(width) = value["chunk_bytes"].as_u64() {
                    let source_bytes = value["bytes"].as_u64()?;
                    let start = index.checked_mul(width)?;
                    if width != 1500
                        || total_chunks != source_bytes.div_ceil(width)
                        || chunk["byte_start"].as_u64()? != start
                        || chunk["byte_end"].as_u64()? != start.checked_add(bytes.len() as u64)?
                        || bytes.len() as u64 != (source_bytes.checked_sub(start)?).min(width)
                    {
                        return None;
                    }
                }
                (read_sha256(&bytes), bytes.len() as u64)
            } else {
                return None;
            };
            chunks.push(ReadChunkSeal {
                index,
                body_sha256,
                body_bytes,
            });
        }
        chunks.sort_by_key(|chunk| chunk.index);
        (
            value["evidence_ref"].as_str()?.to_string(),
            hash(&value["sha256"])?,
            value["bytes"].as_u64()?,
            ReadExtent::ArtifactChunks {
                total_chunks,
                chunks,
            },
        )
    } else {
        return None;
    };
    Some(ReadBodySeal {
        schema_version: 1,
        source_ref,
        source_sha256,
        source_bytes,
        output_sha256: read_sha256(output.as_bytes()),
        output_bytes: output.len() as u64,
        extent,
    })
}

/// Candidate child acquisitions are accepted only from the in-process host
/// capability, under the parent grant and its already-resolved scope. These
/// records are not delivery evidence.
pub(crate) fn child_read_acquisitions(
    request: &crate::RuntimeToolExecutionRequest,
    scope: &harness_contract::goal::ProofEffectScope,
    children: &[tools::OrchestratedRead],
) -> Option<Vec<serde_json::Value>> {
    let auth = request.authorization.as_ref()?;
    let parent = request.compiled_invocation.as_ref()?;
    if !matches!(request.tool_name.as_str(), "tool_orchestrate" | "grep_search")
        || parent.effect.tool_id != request.tool_name
        || parent.effect.descriptor_hash != auth.descriptor_hash || auth.request_id.is_empty()
        || parent.invocation_id != request.tool_use_id
        || request.step_binding.as_ref().is_none_or(|binding| binding.origin.call_id != request.tool_use_id
            || parent.origin.as_ref() != Some(&binding.origin)
            || Some(binding.snapshot.identity.session_id.as_str()) != request.session_id.as_deref())
    { return None; }
    let mut positions = std::collections::BTreeSet::new();
    let mut records = Vec::new();
    let mut prefix_count = 0usize;
    for child in children {
        let effect = child.effect();
        let direct_batch = if request.tool_name == "grep_search" {
            serde_json::from_str::<serde_json::Value>(&request.input).ok().is_some_and(|input| {
                let child_value = serde_json::json!({"parent_tool_name":"grep_search", "step_index":child.step_index(),
                    "step_id":child.step_id(), "position":child.position(), "batch_index":child.grep_batch_index(),
                    "input":child.input(), "input_sha256":read_sha256(&serde_json::to_vec(child.input()).unwrap_or_default()),
                    "effect":effect});
                grep::direct_grep_file_domain(&input, &parent.effect, scope, &child_value)
                    && parent.effect.scopes.iter().all(|s| auth.authorization_lease.scopes.contains(s))
            })
        } else { false };
        let composite_file_domain = request.tool_name == "tool_orchestrate"
            && original_read_file_domain(&parent.effect, scope)
            && parent.effect.scopes.iter().all(|item| auth.authorization_lease.scopes.contains(item));
        if (!scope.complete && !direct_batch && !composite_file_domain) || child.parent_request_id() != auth.request_id
            || child.parent_descriptor_hash() != auth.descriptor_hash
            || child.parent_authorization() != auth
            || !positions.insert((child.step_index(), child.position(), child.grep_batch_index()))
            || !matches!(effect.tool_id.as_str(), "read_file" | "grep_search")
            || effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
            || effect.uses_network || effect.spawns_process || effect.mutates_packages || effect.mutates_system
            || effect.scopes.is_empty()
            || (!direct_batch && !effect.scopes.iter().all(|s| auth.authorization_lease.scopes.contains(s)
                && parent.effect.scopes.contains(s)
                && (scope.targets.iter().filter(|t| &t.declared_scope == s).count() == 1
                    || composite_file_domain
                        && s.resource == harness_contract::policy::PermissionResource::Tool
                        && s.operation == harness_contract::policy::PermissionOperation::Read)))
        { return None; }
        if effect.tool_id == "grep_search" {
            records.extend(grep::acquisitions(request, scope, child)?);
            if records.len() > 400 { return None; }
            continue;
        }
        let body = read_body_seal("read_file", child.output())?;
        let output: serde_json::Value = serde_json::from_str(child.output()).ok()?;
        let content = output["file"]["content"].as_str()?;
        let mut prefixes = Vec::new();
        let mut hash = Sha256::new();
        let mut bytes = 0usize;
        for (index, line) in content.split('\n').enumerate() {
            if index >= 16 { break; }
            if output["file"]["numLines"].as_u64()? == 0 { break; }
            let next = bytes.checked_add(line.len())?.checked_add(usize::from(index != 0))?;
            if next > 4096 { break; }
            if index != 0 { hash.update(b"\n"); }
            hash.update(line.as_bytes());
            bytes = next;
            prefixes.push(serde_json::json!({"line_count":index + 1,"bytes":bytes,"ends_with_newline":output["file"]["endsWithNewline"],
                "sha256":format!("sha256:{:x}",hash.clone().finalize())}));
        }
        prefix_count = prefix_count.checked_add(prefixes.len())?;
        if prefix_count > 16384 { return None; }
        let input = serde_json::to_vec(child.input()).ok()?;
        records.push(serde_json::json!({
            "schema_version": 1,
            "parent_tool_use_id": request.tool_use_id,
            "parent_idempotency_key": request.idempotency_key,
            "parent_descriptor_hash": auth.descriptor_hash,
            "step_index": child.step_index(), "step_id": child.step_id(), "position": child.position(),
            "tool_name": "read_file", "input": child.input(), "input_sha256": read_sha256(&input),
            "effect": effect, "body": body, "prefixes": prefixes,
        }));
    }
    Some(records)
}

/// Compare the committed records with every position/window in the original
/// child trace. Equal counts alone cannot prove a one-to-one correspondence.
pub(crate) fn original_child_coverage_complete(
    children: &[tools::OrchestratedRead], records: &[serde_json::Value],
) -> bool {
    let mut expected = std::collections::BTreeSet::new();
    for child in children {
        match child.effect().tool_id.as_str() {
            "read_file" => {
                if !expected.insert((child.step_index() as u64, child.position() as u64,
                    None, None)) { return false; }
            }
            "grep_search" => {
                let Some(windows) = child.grep_windows() else { return false; };
                if windows.is_empty() { return false; }
                for index in 0..windows.len() {
                    if !expected.insert((child.step_index() as u64, child.position() as u64,
                        child.grep_batch_index().map(|value| value as u64), Some(index as u64))) {
                        return false;
                    }
                }
            }
            _ => return false,
        }
    }
    child_record_coverage_matches(&expected, records)
}

fn child_record_coverage_matches(
    expected: &std::collections::BTreeSet<(u64, u64, Option<u64>, Option<u64>)>,
    records: &[serde_json::Value],
) -> bool {
    let mut actual = std::collections::BTreeSet::new();
    for record in records {
        let Some(step) = record["step_index"].as_u64() else { return false; };
        let Some(position) = record["position"].as_u64() else { return false; };
        let (batch, window) = match record["tool_name"].as_str() {
            Some("read_file") if record["batch_index"].is_null() && record["window_index"].is_null() => (None, None),
            Some("grep_search") => {
                let Some(window) = record["window_index"].as_u64() else { return false; };
                (record["batch_index"].as_u64(), Some(window))
            }
            _ => return false,
        };
        if !actual.insert((step, position, batch, window)) { return false; }
    }
    &actual == expected
}

/// Match only a literal batch member at its original program location. This
/// function proves bytes in the supplied presentation, not its wire delivery;
/// the caller must first bind the whole presentation to the accepted wire.
pub(crate) fn child_read_presented_extent(
    parent_input: &str,
    presentation: &str,
    child: &serde_json::Value,
) -> Option<serde_json::Value> {
    if child["tool_name"] == "grep_search" {
        return grep::presented_extent(parent_input, presentation, child);
    }
    let input: serde_json::Value = serde_json::from_str(parent_input).ok()?;
    let steps = input["steps"].as_array()?;
    let index = usize::try_from(child["step_index"].as_u64()?).ok()?;
    let position = usize::try_from(child["position"].as_u64()?).ok()?;
    let step = steps.get(index)?;
    let id = step["id"].as_str()?;
    if child["step_id"] != id || steps.iter().filter(|step| step["id"] == id).count() != 1 { return None; }
    let (call, alias) = match step["op"].as_str()? {
        "call" if position == 0 => (step, None),
        "batch" => {
            let calls = step["calls"].as_array()?;
            let call = calls.get(position)?;
            let alias = call["as"].as_str().map(str::to_string)
                .unwrap_or_else(|| format!("read_file[{position}]"));
            if calls.iter().enumerate().filter(|(i, call)| {
                call["as"].as_str().map(str::to_string)
                    .unwrap_or_else(|| format!("{}[{i}]", call["tool"].as_str().unwrap_or(""))) == alias
            }).count() != 1 { return None; }
            (call, Some(alias))
        }
        _ => return None,
    };
    if call["tool"] != "read_file" { return None; }
    let prefix = format!("Tool `tool_orchestrate` completed. Evidence: tool://");
    let tail = presentation.strip_prefix(&prefix)?;
    let (_, mut body) = tail.split_once(". ")?;
    if body.starts_with("JSON object with ") { body = body.split_once(". ")?.1; }
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let partial = value["kind"] == "orchestration_partial_receipt";
    let selected = input.get("return").and_then(serde_json::Value::as_str);
    if selected.is_some_and(|selected| selected != format!("${id}")) { return None; }
    let result = &value["result"];
    let batch = if selected.is_some() || (partial && steps.len() == 1) { result } else { &result[id] };
    let member = alias.as_ref().map_or(batch, |alias| &batch[alias]);
    let output = if partial {
        if member["source_tool"] != "read_file" || member["ok"] != true { return None; }
        &member["output"]
    } else if step["on_error"] == "collect" {
        if member["ok"] != true { return None; }
        &member["output"]
    } else { member };
    let file = &output["file"];
    let content = file["content"].as_str()?;
    if file["content_omitted"] == true
        && !(file["preview"] == true && file["source_omitted"] == true)
    { return None; }
    let extent = &child["body"]["extent"];
    let lines = file["numLines"].as_u64()?;
    if extent["kind"] != "file_lines" || file["startLine"] != extent["start_line"]
        || file["totalLines"] != extent["total_lines"]
        || (lines == 0 && !(content.is_empty() && extent["line_count"] == 0))
        || (lines > 0 && content.split('\n').count() as u64 != lines)
        || (file["preview"] == true && (lines > 16 || content.len() > 4096
            || file["sourceNumLines"] != extent["line_count"]
            || file["byteLength"] != child["body"]["source_bytes"]
            || file["sha256"].as_str()?.trim_start_matches("sha256:")
                != child["body"]["source_sha256"].as_str()?.trim_start_matches("sha256:")))
        || file["filePath"] != child["body"]["source_ref"] { return None; }
    let hash = read_sha256(content.as_bytes());
    let full = lines == extent["line_count"].as_u64()? && content.len() as u64 == extent["body_bytes"].as_u64()?
        && hash == extent["body_sha256"].as_str()?;
    let matched = child["prefixes"].as_array()?.iter().find(|prefix| {
        prefix["line_count"] == lines && prefix["bytes"] == content.len() as u64 && prefix["sha256"] == hash
    });
    if !full && matched.is_none() { return None; }
    Some(serde_json::json!({"kind":"file_lines", "start_line":extent["start_line"],
        "line_count":lines, "total_lines":extent["total_lines"],
        "body_sha256":hash,"body_bytes":content.len(),"ends_with_newline":extent["ends_with_newline"].as_bool()?,
        "source_omitted":file["preview"] == true || extent["source_omitted"] == true
            || lines != extent["line_count"].as_u64()?}))
}

pub(crate) fn read_acquisition_seal(
    request: &crate::RuntimeToolExecutionRequest,
    output: &str,
) -> Option<ReadAcquisitionSeal> {
    let parent = request.parent_execution.as_ref()?;
    let attempt = request.parent_execution_attempt?;
    read_acquisition_seal_at(request, output, parent, attempt)
}

pub(crate) fn read_acquisition_seal_at(
    request: &crate::RuntimeToolExecutionRequest,
    output: &str,
    parent: &harness_contract::execution_graph::ExecutionParentBinding,
    attempt: u32,
) -> Option<ReadAcquisitionSeal> {
    if parent.execution_id.is_empty() || parent.node_id.is_empty() || attempt == 0 {
        return None;
    }
    let body = read_body_seal(&request.tool_name, output)?;
    let binding = request.step_binding.as_ref()?;
    let session_id = request.session_id.as_ref()?;
    if session_id != &binding.snapshot.identity.session_id
        || binding.origin.call_id != request.tool_use_id
    {
        return None;
    }
    Some(ReadAcquisitionSeal {
        schema_version: 1,
        primary_stream: format!("execution-effect:{}", request.idempotency_key),
        primary_key: format!("{}:read-receipt", request.idempotency_key),
        index_stream: format!(
            "execution-agent-receipts:{}:{}:{attempt}",
            parent.execution_id, parent.node_id
        ),
        index_key: format!("agent-tool-receipt:{}", request.idempotency_key),
        session_id: session_id.clone(),
        invocation_id: request.tool_use_id.clone(),
        tool_name: request.tool_name.clone(),
        graph_id: parent.execution_id.clone(),
        node_id: parent.node_id.clone(),
        attempt,
        step: binding.snapshot.identity.clone(),
        origin: serde_json::to_value(&binding.origin).ok()?,
        input_sha256: read_sha256(request.input.as_bytes()),
        body,
    })
}

pub(crate) fn authenticate_root_read_intent(
    intent: &crate::DurableRuntimeEvent,
    primary: &crate::DurableRuntimeEvent,
    index: &crate::DurableRuntimeEvent,
    seal: &ReadAcquisitionSeal,
) -> Result<harness_contract::projection::RuntimeActivityBinding, &'static str> {
    let record = &intent.payload["root_effect_owner"];
    let owner: harness_contract::projection::RuntimeActivityBinding =
        serde_json::from_value(record["binding"].clone())
            .map_err(|_| "invalid original root read owner")?;
    owner.validate()?;
    let root: crate::CowdExecutionContext =
        serde_json::from_value(intent.payload["root_effect_context"].clone())
            .map_err(|_| "invalid original root read context")?;
    let key = seal
        .primary_key
        .strip_suffix(":read-receipt")
        .ok_or("invalid root primary key")?;
    if record.as_object().is_none_or(|record| record.len() != 3)
        || record["attempt"].as_u64() != Some(u64::from(seal.attempt))
        || seal.attempt == 0
        || record["graph_revision"]
            .as_u64()
            .is_none_or(|revision| revision == 0)
        || owner.agent_run_id.is_some()
        || owner.node_id.as_deref() != Some(seal.node_id.as_str())
        || owner.root_execution_id != seal.graph_id
        || root.execution_id != seal.graph_id
        || owner.session_id != seal.session_id
        || root.session_id != seal.session_id
        || owner.turn_id != seal.step.turn_id
        || root.turn_id != seal.step.turn_id
        || seal.step.execution_id.as_deref() != Some(root.execution_id.as_str())
        || intent.kind != "execution.read.intent"
        || intent.actor.as_deref() != Some("governed_tool")
        || intent.scope != crate::runtime_event_store::RuntimeEventScope::ExecutionNode
        || intent.status.as_deref() != Some("inflight")
        || intent.stream_id != seal.primary_stream
        || intent.idempotency_key.as_deref() != Some(format!("{key}:read-intent").as_str())
        || intent.commit_cursor >= primary.commit_cursor
        || intent.transaction_id == primary.transaction_id
        || primary.transaction_id != index.transaction_id
        || primary.commit_cursor != index.commit_cursor
        || index.transaction_index <= primary.transaction_index
        || intent.payload["idempotency_key"] != key
        || primary.payload["idempotency_key"] != key
        || index.payload["idempotency_key"] != key
        || intent.payload["tool_use_id"] != seal.invocation_id
        || intent.payload["tool_name"] != seal.tool_name
        || intent.payload["input_sha256"] != seal.input_sha256
        || !intent.payload["replay"]["parent_execution"].is_null()
        || !intent.payload["replay"]["parent_execution_attempt"].is_null()
        || intent.payload["replay"] != primary.payload["replay"]
    {
        return Err("root read acquisition is not bound to its original admission");
    }
    let effect: harness_contract::tool::ToolEffectDescriptor =
        serde_json::from_value(intent.payload["effect"].clone())
            .map_err(|_| "invalid root read descriptor")?;
    let scope: harness_contract::goal::ProofEffectScope =
        serde_json::from_value(intent.payload["resolved_effect_scope"].clone())
            .map_err(|_| "invalid root read scope2")?;
    scope
        .validate_purpose_mapping(&effect)
        .map_err(|_| "invalid root read purpose mapping")?;
    if effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
        || effect.tool_id != seal.tool_name
    {
        return Err("root read admission has a non-read descriptor");
    }
    if intent.payload["replay"]["session_id"] != seal.session_id
        || intent.payload["replay"]["call_id"] != seal.invocation_id
        || intent.payload["replay"]["origin"] != seal.origin
        || intent.payload["replay"]["invocation"]["effect"] != intent.payload["effect"]
    {
        return Err("root read admission compiled invocation mismatch");
    }
    for event in [intent, primary, index] {
        if [
            "effect",
            "resolved_effect_scope",
            "root_effect_context",
            "root_effect_owner",
        ]
        .iter()
        .any(|field| event.payload[*field] != intent.payload[*field])
            || !event.refs.iter().any(|reference| {
                reference.kind == "execution_graph" && reference.id == seal.graph_id
            })
            || !event
                .refs
                .iter()
                .any(|reference| reference.kind == "execution_node" && reference.id == seal.node_id)
            || !event.refs.iter().any(|reference| {
                reference.kind == "agent_attempt" && reference.id == seal.attempt.to_string()
            })
            || !event
                .refs
                .iter()
                .any(|reference| reference.kind == "turn" && reference.id == seal.step.turn_id)
        {
            return Err("root read receipt/index replaced the original admission");
        }
    }
    if primary.payload["outcome"] != index.payload["outcome"] {
        return Err("root read receipt/index outcome mismatch");
    }
    Ok(owner)
}

pub(crate) fn native_acquisition_descriptor(
    primary: &crate::DurableRuntimeEvent,
    seal: &ReadAcquisitionSeal,
) -> Result<harness_contract::tool::ToolEffectDescriptor, &'static str> {
    let compiled: harness_contract::tool::ToolEffectDescriptor =
        serde_json::from_value(primary.payload["replay"]["invocation"]["effect"].clone())
            .map_err(|_| "Native acquisition has no original compiled descriptor")?;
    if compiled.effect_kind != harness_contract::tool::ToolEffectKind::Read
        || compiled.tool_id != seal.tool_name
        || (!primary.payload["effect"].is_null()
            && primary.payload["effect"]
                != serde_json::to_value(&compiled)
                    .map_err(|_| "invalid Native acquisition descriptor")?)
    {
        return Err("Native acquisition descriptor differs from original compiled read");
    }
    let scope_value = &primary.payload["resolved_effect_scope"];
    if scope_value.is_null() {
        let immutable_body = !primary.payload["replay"]["parent_execution"].is_null()
            && matches!(seal.tool_name.as_str(), "read_ref" | "evidence_retrieve")
            && matches!(&seal.body.extent, ReadExtent::ArtifactChunks { .. })
            && !compiled
                .scopes
                .iter()
                .any(|scope| scope.resource == harness_contract::policy::PermissionResource::File);
        if !immutable_body {
            return Err("Native File/root acquisition requires its original scope2");
        }
    } else {
        let scope: harness_contract::goal::ProofEffectScope =
            serde_json::from_value(scope_value.clone())
                .map_err(|_| "invalid Native acquisition scope2")?;
        scope
            .validate_purpose_mapping(&compiled)
            .map_err(|_| "invalid Native acquisition purpose mapping")?;
        let mapped = scope.targets.iter().all(|target| {
            compiled.scopes.contains(&target.declared_scope)
                && target.declared_scope.resource
                    == harness_contract::policy::PermissionResource::File
                && !target.path.workspace_id.is_empty()
                && !target.path.repository_id.is_empty()
                && !target.path.workspace_relative_path.is_empty()
                && (!target.exact
                    || target.path.object_kind
                        == harness_contract::context::WorkspaceObjectKind::File)
                && match target.declared_scope.operation {
                    harness_contract::policy::PermissionOperation::Read => {
                        target.access_mode == harness_contract::context::WorkspaceAccessMode::Read
                            && target.coverage
                                == harness_contract::context::EvidenceCoverageKind::ScopedContent
                    }
                    harness_contract::policy::PermissionOperation::Write => {
                        target.access_mode == harness_contract::context::WorkspaceAccessMode::Write
                            && target.coverage
                                == harness_contract::context::EvidenceCoverageKind::WriteEffect
                    }
                    _ => false,
                }
        });
        let complete = !compiled.scopes.is_empty()
            && compiled.scopes.len() == scope.targets.len()
            && compiled.scopes.iter().all(|declared| {
                scope
                    .targets
                    .iter()
                    .filter(|target| &target.declared_scope == declared)
                    .count()
                    == 1
            })
            && !compiled.uses_network
            && !compiled.spawns_process
            && !compiled.mutates_packages
            && !compiled.mutates_system;
        if !mapped || (scope.complete && !complete) {
            return Err("Native acquisition target mapping is invalid");
        }
    }
    Ok(compiled)
}

pub(crate) fn decode_read_acquisition(
    value: &serde_json::Value,
) -> Result<ReadAcquisitionSeal, &'static str> {
    let seal: ReadAcquisitionSeal =
        serde_json::from_value(value.clone()).map_err(|_| "invalid read acquisition seal")?;
    let origin: harness_contract::step::CallOrigin = serde_json::from_value(seal.origin.clone())
        .map_err(|_| "invalid original acquisition origin")?;
    if seal.schema_version != 1
        || seal.body.schema_version != 1
        || seal.invocation_id.is_empty()
        || seal.session_id != seal.step.session_id
        || seal.primary_key.is_empty()
        || seal.index_key.is_empty()
        || seal.graph_id.is_empty()
        || seal.node_id.is_empty()
        || seal.step.step_id != origin.origin_step_id
        || seal.invocation_id != origin.call_id
    {
        return Err("unknown or unbound read acquisition seal");
    }
    Ok(seal)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WireToolResultSeal {
    pub invocation_id: String,
    pub presentation_sha256: String,
    pub presentation_bytes: u64,
    pub is_error: bool,
}

pub(crate) fn wire_tool_result_seals(
    wire: &provider::ProviderWireRequest,
) -> Option<Vec<WireToolResultSeal>> {
    let bytes = serde_json::to_vec(&wire.body).ok()?;
    if read_sha256(&bytes).trim_start_matches("sha256:") != wire.body_sha256 {
        return None;
    }
    let mut seals = Vec::new();
    let mut insert = |id: &str, text: &str, is_error: bool| -> Option<()> {
        if id.is_empty()
            || seals
                .iter()
                .any(|seal: &WireToolResultSeal| seal.invocation_id == id)
        {
            return None;
        }
        seals.push(WireToolResultSeal {
            invocation_id: id.to_string(),
            presentation_sha256: read_sha256(text.as_bytes()),
            presentation_bytes: text.len() as u64,
            is_error,
        });
        Some(())
    };
    match wire.protocol.as_str() {
        "anthropic_messages" => {
            for message in wire.body["messages"].as_array()? {
                if message["role"] != "user" {
                    continue;
                }
                let Some(blocks) = message["content"].as_array() else {
                    continue;
                };
                for block in blocks {
                    if block["type"] != "tool_result" {
                        continue;
                    }
                    let text = if let Some(text) = block["content"].as_str() {
                        text.to_string()
                    } else {
                        let blocks = block["content"].as_array()?;
                        if blocks.len() != 1 || blocks[0]["type"] != "text" {
                            return None;
                        }
                        blocks[0]["text"].as_str()?.to_string()
                    };
                    insert(
                        block["tool_use_id"].as_str()?,
                        &text,
                        block["is_error"].as_bool().unwrap_or(false),
                    )?;
                }
            }
        }
        "openai_chat_completions" => {
            for message in wire.body["messages"].as_array()? {
                if message["role"] != "tool" {
                    continue;
                }
                insert(
                    message["tool_call_id"].as_str()?,
                    message["content"].as_str()?,
                    message["is_error"].as_bool().unwrap_or(false),
                )?;
            }
        }
        "openai_responses" => {
            for item in wire.body["input"].as_array()? {
                if item["type"] != "function_call_output" {
                    continue;
                }
                insert(item["call_id"].as_str()?, item["output"].as_str()?, false)?;
            }
        }
        _ => return None,
    }
    Some(seals)
}

pub(crate) fn wire_tool_result_text<'a>(wire: &'a provider::ProviderWireRequest, id: &str) -> Option<&'a str> {
    let seals = wire_tool_result_seals(wire)?;
    if seals.iter().filter(|seal| seal.invocation_id == id && !seal.is_error).count() != 1 { return None; }
    match wire.protocol.as_str() {
        "openai_chat_completions" => wire.body["messages"].as_array()?.iter()
            .find(|item| item["role"] == "tool" && item["tool_call_id"] == id)?["content"].as_str(),
        "openai_responses" => wire.body["input"].as_array()?.iter()
            .find(|item| item["type"] == "function_call_output" && item["call_id"] == id)?["output"].as_str(),
        "anthropic_messages" => {
            let item = wire.body["messages"].as_array()?.iter().filter(|message| message["role"] == "user")
                .flat_map(|message| message["content"].as_array().into_iter().flatten())
                .find(|item| item["type"] == "tool_result" && item["tool_use_id"] == id)?;
            item["content"].as_str().or_else(|| item["content"].as_array().and_then(|items| items.first()).and_then(|item| item["text"].as_str()))
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelReceipt {
    pub evidence_ref: EvidenceRef,
    pub content_kind: EvidenceContentKind,
    pub summary: String,
    pub raw_tokens: u64,
    pub receipt_tokens: u64,
    pub omitted_tokens: u64,
    pub truncated: bool,
}

/// Opt-in fixture exercises the same receipt builder as the production tool path.
#[cfg(feature = "test-fixtures")]
pub fn k02_fixture_orchestration_receipt(output: &str, raw_id: &str, token_budget: u64, durable: bool, input: Option<&str>) -> ModelReceipt {
    build_tool_receipt_for_delivery("tool_orchestrate", output, false, EvidenceRef::observed("tool", raw_id), token_budget, false, durable, input)
}

#[must_use]
pub fn build_tool_receipt(
    tool_name: &str,
    output: &str,
    is_error: bool,
    evidence_ref: EvidenceRef,
    token_budget: u64,
) -> ModelReceipt {
    build_tool_receipt_for_delivery(tool_name, output, is_error, evidence_ref, token_budget, false, false, None)
}

pub(crate) fn build_tool_receipt_for_delivery(
    tool_name: &str,
    output: &str,
    is_error: bool,
    evidence_ref: EvidenceRef,
    token_budget: u64,
    preserve_complete_read: bool,
    durable_recovery: bool,
    original_input: Option<&str>,
) -> ModelReceipt {
    let content_kind = classify_content(output, is_error);
    let raw_tokens = estimate_text_tokens(output);
    let fixed_prefix = format!(
        "Tool `{tool_name}` {}. Evidence: tool://{}. ",
        if is_error { "failed" } else { "completed" },
        evidence_ref.id()
    );
    let prefix_tokens = estimate_text_tokens(&fixed_prefix);
    let body_budget = token_budget.saturating_sub(prefix_tokens).max(1);
    let raw_lines = output.lines().count();
    if tool_name == "grep_search" && !is_error
        && estimate_text_tokens(&format!("{fixed_prefix}{}", summarize_json(tool_name, output))) > token_budget
    {
        if let Some((summary, truncated)) = compact_grep_batch(output, original_input, &fixed_prefix, token_budget) {
            return ModelReceipt {
                evidence_ref, content_kind, receipt_tokens: estimate_text_tokens(&summary),
                raw_tokens, omitted_tokens: if truncated { raw_tokens.saturating_sub(estimate_text_tokens(&summary)).max(1) } else { 0 },
                summary, truncated,
            };
        }
    }
    let (body, body_omitted) = if preserve_complete_read && tool_name == "read_file" && !is_error {
        // A complete acquisition must not lose its middle to a second generic
        // receipt summary. The original request packing/capacity and wire proof
        // still decide whether these actual bytes reach the model.
        (output.to_string(), false)
    } else {
        let recover_ref = (durable_recovery && tool_name == "tool_orchestrate")
            .then(|| format!("tool://{}", evidence_ref.id()));
        let compact = recover_ref.as_deref().and_then(|reference| {
            if tool_name == "tool_orchestrate" && !is_error && raw_tokens > body_budget {
                compact_orchestrated_batch(output, reference, body_budget, original_input)
            } else {
                None
            }
        });
        compact.map_or_else(
            || {
                if let Some(reference) = recover_ref.as_deref().filter(|_| !is_error && raw_tokens > body_budget) {
                    // Unknown or oversized shapes remain explicitly incomplete;
                    // never silently splice a JSON head and tail into a result.
                    if let Some(fallback) = bounded_orchestration_fallback(reference, body_budget) { return (fallback, true); }
                }
                summarize_body(tool_name, output, content_kind, body_budget, raw_tokens, raw_lines, recover_ref.as_deref())
            },
            |body| (body, true),
        )
    };
    let summary = format!("{fixed_prefix}{body}");
    let receipt_tokens = estimate_text_tokens(&summary);
    ModelReceipt {
        evidence_ref,
        content_kind,
        summary,
        raw_tokens,
        receipt_tokens,
        omitted_tokens: if body_omitted {
            raw_tokens
                .saturating_sub(estimate_text_tokens(&body))
                .max(1)
        } else {
            0
        },
        truncated: body_omitted,
    }
}

// Preserve only complete original batch members. A missing member is never a
// successful read, and the proof consumer still authenticates each member.
fn compact_grep_batch(output: &str, original_input: Option<&str>, prefix: &str, budget: u64) -> Option<(String, bool)> {
    let source: serde_json::Value = serde_json::from_str(output).ok()?;
    if source.get("type")?.as_str()? != "grep_search" || !source.get("results")?.is_array() { return None; }
    let minimal = || {
        let body = serde_json::json!({"complete":false}).to_string();
        let with_prefix = format!("{prefix}{body}");
        if estimate_text_tokens(&with_prefix) <= budget { Some((with_prefix, true)) }
        else { Some((String::new(), true)) }
    };
    let Some(input) = original_input.and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()) else { return minimal(); };
    let members = source.get("results")?.as_array()?;
    let Some(searches) = input.get("searches").and_then(serde_json::Value::as_array) else { return minimal(); };
    if members.len() != searches.len() || source.get("count").and_then(serde_json::Value::as_u64) != Some(members.len() as u64)
        || members.iter().enumerate().any(|(i, member)| member.get("index").and_then(serde_json::Value::as_u64) != Some(i as u64)) {
        return minimal();
    }
    let compact_original = format!("{prefix}{source}");
    if estimate_text_tokens(&compact_original) <= budget { return Some((compact_original, false)); }
    let omitted = |i: usize| serde_json::json!({"index":i,"status":"omitted"});
    let mut result = serde_json::json!({
        "type":"grep_search", "complete":false,
        "continuation":"Repeat omitted searches with a narrower request; omitted members were not delivered.",
        "results":(0..members.len()).map(omitted).collect::<Vec<_>>()
    });
    let render = |value: &serde_json::Value| format!("{prefix}{value}");
    if estimate_text_tokens(&render(&result)) > budget {
        result = serde_json::json!({"type":"grep_search","complete":false,"results":[],
            "omitted_count":members.len(),"continuation":"Repeat omitted searches with a narrower request."});
        if estimate_text_tokens(&render(&result)) > budget {
            return minimal();
        }
        return Some((render(&result), true));
    }
    for (i, member) in members.iter().enumerate() {
        result["results"][i] = member.clone();
        if estimate_text_tokens(&render(&result)) > budget {
            result["results"][i] = omitted(i);
        }
    }
    Some((render(&result), true))
}

fn classify_content(output: &str, is_error: bool) -> EvidenceContentKind {
    if is_error {
        EvidenceContentKind::Error
    } else if serde_json::from_str::<serde_json::Value>(output).is_ok() {
        EvidenceContentKind::Json
    } else if output.lines().any(|line| {
        line.starts_with("diff --git") || line.starts_with("@@") || line.starts_with("+++")
    }) {
        EvidenceContentKind::Diff
    } else {
        EvidenceContentKind::Text
    }
}

fn bounded_orchestration_fallback(reference: &str, token_budget: u64) -> Option<String> {
    let fallback = format!("Partial orchestration output omitted; read_ref({}) and follow its cursor, or repeat a narrower original request if unavailable.", serde_json::json!({"reference":reference}));
    (estimate_text_tokens(&fallback) <= token_budget).then_some(fallback)
}

/// Preserve small, independently meaningful batch metadata instead of cutting
/// through a large JSON document. The raw artifact remains authoritative;
/// omitted bodies and incomplete child scans are explicitly identified.
fn compact_orchestrated_batch(output: &str, reference: &str, token_budget: u64, original_input: Option<&str>) -> Option<String> {
    let raw: serde_json::Value = serde_json::from_str(output).ok()?;
    let calls = raw.get("calls")?.as_array()?;
    let mut items = raw.get("result")?.as_object()?;
    // The default return is the complete step map, not the selected batch.
    // Unwrap only one literal batch step with a matching successful trace and
    // identical result/step objects; ambiguous multi-step programs fail closed.
    let request = original_input.and_then(|input| serde_json::from_str::<serde_json::Value>(input).ok());
    let mut default_fail_batch = false;
    if let Some(input) = request.as_ref() {
        if input.get("return").is_none() {
            let steps = input.get("steps")?.as_array()?;
            if steps.len() > 1 {
                // Preserve the default result's step hierarchy. Each batch is
                // compacted through the same validated path, rather than
                // guessing flattened aliases across independent steps.
                if steps.len() > 32 || calls.len() != steps.len()
                    || items.len() != steps.len() || raw.get("steps")?.as_object()?.len() != steps.len()
                    || raw.get("result")? != raw.get("steps")? { return None; }
                let mut tree = serde_json::Map::new();
                for step in steps {
                    let id = step.get("id")?.as_str()?;
                    if tree.contains_key(id) || step.get("op")?.as_str()? != "batch" { return None; }
                    let mut traces = calls.iter().filter(|trace| trace.get("stepId").and_then(serde_json::Value::as_str) == Some(id));
                    let trace = traces.next()?;
                    if traces.next().is_some() { return None; }
                    let value = items.get(id)?;
                    let child_raw = serde_json::json!({"calls":[trace],"steps":{id:value},"result":{id:value}});
                    let child_input = serde_json::json!({"steps":[step]});
                    let compact = compact_orchestrated_batch(&child_raw.to_string(), reference, token_budget, Some(&child_input.to_string()))?;
                    let child: serde_json::Value = serde_json::from_str(&compact).ok()?;
                    tree.insert(id.to_owned(), child.get("result")?.clone());
                }
                let value = serde_json::json!({"kind":"orchestration_partial_receipt", "complete":false,
                    "raw_reference":reference, "recovery":{"when_available_and_authorized":{"name":"read_ref","input":{"reference":reference}},
                    "continuation":"Use the returned cursor with the same reference until complete; otherwise repeat a narrower original request."},
                    "calls":calls,"result":tree});
                let body = serde_json::to_string(&value).ok()?;
                return (estimate_text_tokens(&body) <= token_budget).then_some(body);
            }
            if steps.len() != 1 { return None; }
            let step = &steps[0];
            let id = step.get("id")?.as_str()?;
            let entries = step.get("calls")?.as_array()?;
            if step.get("op")?.as_str()? != "batch" || calls.len() != 1
                || calls[0].get("stepId")?.as_str()? != id
                || calls[0].get("op")?.as_str()? != "batch"
                || calls[0].get("status")?.as_str()? != "ok"
                || calls[0].get("calls")?.as_u64()? != entries.len() as u64
                || items.len() != 1 || raw.get("steps")?.as_object()?.len() != 1
                || items.get(id)? != raw.get("steps")?.get(id)? { return None; }
            default_fail_batch = step.get("on_error").is_none()
                || step.get("on_error").and_then(serde_json::Value::as_str) == Some("fail");
            items = items.get(id)?.as_object()?;
        } else if let Some(selected) = input.get("return").and_then(serde_json::Value::as_str).and_then(|s| s.strip_prefix('$')) {
            let steps = input.get("steps")?.as_array()?;
            let mut matching = steps.iter().filter(|step| step.get("id").and_then(serde_json::Value::as_str) == Some(selected));
            if let Some(step) = matching.next() {
                let mut traces = calls.iter().filter(|trace| trace.get("stepId").and_then(serde_json::Value::as_str) == Some(selected));
                let trace = traces.next();
                if matching.next().is_none() && step.get("op").and_then(serde_json::Value::as_str) == Some("batch")
                    && (step.get("on_error").is_none() || step.get("on_error").and_then(serde_json::Value::as_str) == Some("fail"))
                    && traces.next().is_none()
                    && trace.is_some_and(|trace| trace.get("status").and_then(serde_json::Value::as_str) == Some("ok")
                        && trace.get("op").and_then(serde_json::Value::as_str) == Some("batch")
                        && trace.get("calls").and_then(serde_json::Value::as_u64) == step.get("calls").and_then(serde_json::Value::as_array).map(|v| v.len() as u64))
                    && raw.get("steps").and_then(|steps| steps.get(selected)) == raw.get("result") {
                    default_fail_batch = true;
                }
            }
        }
    }
    if calls.len() > 32 || items.is_empty() || items.len() > 32 { return None; }
    // Bind aliases only to the exact literal batch and complete result key set.
    let source_tools = request.as_ref().and_then(|input| {
        let steps = input.get("steps")?.as_array()?;
        let selected = match input.get("return") {
            Some(value) => value.as_str()?.strip_prefix('$')?,
            None if steps.len() == 1 => steps[0].get("id")?.as_str()?,
            None => return None,
        };
        let mut matching = steps.iter().filter(|step| step.get("id").and_then(serde_json::Value::as_str) == Some(selected));
        let step = matching.next()?;
        if matching.next().is_some() || step.get("op")?.as_str()? != "batch" { return None; }
        let mut map = std::collections::BTreeMap::new();
        for (index, call) in step.get("calls")?.as_array()?.iter().enumerate() {
            let tool = call.get("tool")?.as_str()?;
            if tool.is_empty() || tool.len() > 128 || !tool.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.')) { return None; }
            let alias = call.get("as").and_then(serde_json::Value::as_str).map(str::to_owned).unwrap_or_else(|| format!("{tool}[{index}]"));
            if alias.len() > 128 || map.insert(alias, tool.to_owned()).is_some() { return None; }
        }
        (map.len() == items.len() && items.keys().all(|key| map.contains_key(key))).then_some(map)
    });
    let mut results = serde_json::Map::new();
    for (name, item) in items {
        if name.len() > 128 { return None; }
        let item = item.as_object()?;
        // Fail-mode batch items are direct tool outputs; collect-mode items
        // contain explicit per-item status. Never infer a direct success unless
        // the unique default batch was authenticated by its successful trace.
        let direct = default_fail_batch && !item.contains_key("status") && !item.contains_key("ok");
        if direct && source_tools.is_none() { return None; }
        let status = if direct { "success" } else { item.get("status")?.as_str()? };
        if status.len() > 64 { return None; }
        let ok = if direct { true } else { item.get("ok")?.as_bool()? };
        let output = if direct { Some(item) } else { item.get("output").and_then(serde_json::Value::as_object) };
        let mut entry = serde_json::Map::new();
        if let Some(tool) = source_tools.as_ref().and_then(|map| map.get(name)) {
            entry.insert("source_tool".into(), serde_json::json!(tool));
        }
        entry.insert("ok".into(), serde_json::json!(ok));
        entry.insert("status".into(), serde_json::json!(status));
        if let Some(error) = item.get("error").and_then(serde_json::Value::as_str) {
            if error.len() > 512 { return None; }
            entry.insert("error".into(), serde_json::json!(error));
        }
        if let Some(object) = output {
            let mut result = serde_json::Map::new();
            for key in ["type", "truncated", "scanComplete", "numFiles", "numLines", "numMatches", "appliedLimit", "appliedOffset", "coverage", "next_request", "continuationCursor", "omissions", "filenames"] {
                if let Some(value) = object.get(key) {
                    if serde_json::to_vec(value).ok()?.len() > 4096 { return None; }
                    result.insert(key.into(), value.clone());
                }
            }
            if object.contains_key("content") { result.insert("content_omitted".into(), serde_json::json!(true)); }
            if let Some(file) = object.get("file").and_then(serde_json::Value::as_object) {
                let mut compact_file = serde_json::Map::new();
                for key in ["filePath", "byteLength", "sha256", "endsWithNewline", "startLine", "numLines", "totalLines"] {
                    if let Some(value) = file.get(key) { compact_file.insert(key.into(), value.clone()); }
                }
                if let Some(content) = file.get("content").and_then(serde_json::Value::as_str) {
                    if content.len() <= 4096 { compact_file.insert("content".into(), serde_json::json!(content)); }
                    else {
                        // A preview contains only real complete leading lines.
                        // It is data, not proof of authenticated delivery: the
                        // receipt/consumer must verify its exact extent and hash.
                        let original_lines = file.get("numLines").and_then(serde_json::Value::as_u64);
                        let start = file.get("startLine").and_then(serde_json::Value::as_u64);
                        let total = file.get("totalLines").and_then(serde_json::Value::as_u64);
                        let valid = original_lines.zip(start).zip(total).is_some_and(|((lines, start), total)|
                            start > 0 && start.checked_sub(1).and_then(|v| v.checked_add(lines)).is_some_and(|end| end <= total)
                                && content.split('\n').count() as u64 == lines);
                        let mut preview = String::new();
                        let mut lines = 0u64;
                        if valid {
                            for line in content.split('\n').take(16) {
                                let extra = line.len() + usize::from(lines > 0);
                                if preview.len().saturating_add(extra) > 4096 { break; }
                                if lines > 0 { preview.push('\n'); }
                                preview.push_str(line);
                                lines += 1;
                            }
                        }
                        compact_file.insert("content_omitted".into(), serde_json::json!(true));
                        if lines > 0 && !preview.is_empty() && original_lines.is_some_and(|count| lines < count) {
                            compact_file.insert("content".into(), serde_json::json!(preview));
                            compact_file.insert("preview".into(), serde_json::json!(true));
                            compact_file.insert("source_omitted".into(), serde_json::json!(true));
                            compact_file.insert("sourceNumLines".into(), serde_json::json!(original_lines));
                            compact_file.insert("numLines".into(), serde_json::json!(lines));
                            result.insert("source_truncated".into(), object.get("truncated").cloned().unwrap_or(serde_json::Value::Null));
                            result.insert("truncated".into(), serde_json::json!(true));
                        }
                    }
                } else if file.contains_key("content") { return None; }
                result.insert("file".into(), serde_json::Value::Object(compact_file));
            }
            let omitted_fields = object.keys().filter(|key| !result.contains_key(*key) && key.as_str() != "content" && key.as_str() != "file").cloned().collect::<Vec<_>>();
            if omitted_fields.len() > 32 || omitted_fields.iter().any(|key| key.len() > 128) { return None; }
            if !omitted_fields.is_empty() { result.insert("omitted_fields".into(), serde_json::json!(omitted_fields)); }
            entry.insert("output".into(), serde_json::Value::Object(result));
        } else if item.get("output").is_some_and(|v| !v.is_null()) {
            entry.insert("output_omitted".into(), serde_json::json!(true));
        }
        results.insert(name.clone(), serde_json::Value::Object(entry));
    }
    let value = serde_json::json!({"kind":"orchestration_partial_receipt", "complete":false, "raw_reference":reference,
        "recovery":{"when_available_and_authorized":{"name":"read_ref","input":{"reference":reference}},
            "continuation":"Use the returned cursor with the same reference until complete; otherwise repeat a narrower original request."},
        "calls":calls, "result":results});
    let body = serde_json::to_string(&value).ok()?;
    (estimate_text_tokens(&body) <= token_budget).then_some(body)
}

fn summarize_body(
    tool_name: &str,
    output: &str,
    kind: EvidenceContentKind,
    token_budget: u64,
    raw_tokens: u64,
    raw_lines: usize,
    recover_ref: Option<&str>,
) -> (String, bool) {
    if output.is_empty() {
        return ("No output.".to_string(), false);
    }
    let normalized_omits = match kind {
        EvidenceContentKind::Json => {
            tool_name == "edit"
                && serde_json::from_str::<serde_json::Value>(output)
                    .ok()
                    .is_some_and(|value| mutation_receipt(tool_name, &value).is_some())
        }
        EvidenceContentKind::Error => output.lines().count() > 40,
        _ => false,
    };
    let normalized = match kind {
        EvidenceContentKind::Json => summarize_json(tool_name, output),
        EvidenceContentKind::Diff => summarize_diff(output),
        EvidenceContentKind::Error => summarize_error(output),
        EvidenceContentKind::Text | EvidenceContentKind::Media => output.to_string(),
    };
    let (body, clipped) =
        truncate_head_tail(tool_name, &normalized, token_budget, raw_tokens, raw_lines, recover_ref);
    (body, normalized_omits || clipped)
}

fn summarize_json(tool_name: &str, output: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(output) else {
        return output.to_string();
    };
    if matches!(tool_name, "edit" | "edit") {
        if let Some(receipt) = mutation_receipt(tool_name, &value) {
            return receipt;
        }
    }
    match value {
        serde_json::Value::Object(map) => {
            let keys = map.keys().take(32).cloned().collect::<Vec<_>>().join(", ");
            format!("JSON object with {} keys: {keys}. {output}", map.len())
        }
        serde_json::Value::Array(items) => {
            format!("JSON array with {} items. {output}", items.len())
        }
        _ => output.to_string(),
    }
}

fn mutation_receipt(tool_name: &str, value: &serde_json::Value) -> Option<String> {
    let object = value.as_object()?;
    let operation = object
        .get("type")
        .or_else(|| object.get("operation"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("mutation");
    let path = ["filePath", "path", "target"]
        .into_iter()
        .find_map(|key| object.get(key).and_then(serde_json::Value::as_str))
        .unwrap_or("unknown");
    let original = object
        .get("originalFile")
        .and_then(serde_json::Value::as_str);
    let (content, replacement_count, replace_all) = if tool_name == "edit" {
        let original = original?;
        let old = object.get("oldString")?.as_str()?;
        let new = object.get("newString")?.as_str()?;
        let replace_all = object
            .get("replaceAll")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let replacement_count = if replace_all {
            original.matches(old).count()
        } else {
            usize::from(original.contains(old))
        };
        let updated = if replace_all {
            original.replace(old, new)
        } else {
            original.replacen(old, new, 1)
        };
        (updated, replacement_count, replace_all)
    } else {
        (
            object.get("content")?.as_str()?.to_string(),
            usize::from(original.is_some()),
            true,
        )
    };
    let prior_bytes = original.map_or(0, str::len);
    let prior_sha256 = original.map(|value| format!("{:x}", Sha256::digest(value.as_bytes())));
    Some(
        serde_json::json!({
            "operation": operation,
            "path": path,
            "content_bytes": content.len(),
            "content_sha256": format!("{:x}", Sha256::digest(content.as_bytes())),
            "prior_bytes": prior_bytes,
            "prior_sha256": prior_sha256,
            "replacement_count": replacement_count,
            "replace_all": replace_all,
            "detail": "full mutation output is available through the evidence URI",
        })
        .to_string(),
    )
}

fn summarize_diff(output: &str) -> String {
    let added = output
        .lines()
        .filter(|line| line.starts_with('+') && !line.starts_with("+++"))
        .count();
    let removed = output
        .lines()
        .filter(|line| line.starts_with('-') && !line.starts_with("---"))
        .count();
    format!("Diff summary: +{added} -{removed}. {output}")
}

fn summarize_error(output: &str) -> String {
    let lines = output.lines().collect::<Vec<_>>();
    if lines.len() <= 40 {
        return format!("Error tail: {output}");
    }
    let start = lines.len().saturating_sub(40);
    format!("Error tail: {}", lines[start..].join("\n"))
}

fn truncate_head_tail(
    tool_name: &str,
    value: &str,
    max_tokens: u64,
    raw_tokens: u64,
    raw_lines: usize,
    recover_ref: Option<&str>,
) -> (String, bool) {
    if estimate_text_tokens(value) <= max_tokens {
        return (value.to_string(), false);
    }
    let char_budget = usize::try_from(max_tokens.saturating_mul(3)).unwrap_or(usize::MAX);
    let head = char_budget.saturating_mul(2) / 3;
    let tail = char_budget.saturating_sub(head);
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= char_budget {
        return (value.to_string(), false);
    }
    let head_text = chars.iter().take(head).collect::<String>();
    let tail_text = chars
        .iter()
        .skip(chars.len().saturating_sub(tail))
        .collect::<String>();
    // Announce the original size and the omission so the model can decide to
    // retrieve the full body by evidence ref instead of guessing (codex parity:
    // "truncated output (original token count: N) / Total output lines: M").
    let kept_tokens = estimate_text_tokens(&format!("{head_text}\n{tail_text}"));
    let omitted_tokens = raw_tokens.saturating_sub(kept_tokens);
    // A composite result may contain multiple paths and search pages, so a
    // single read_file nextOffset cannot recover it. Only offer the scoped
    // durable reference when the producer supplied an authenticated access ref.
    let recovery = if tool_name == "tool_orchestrate" && recover_ref.is_some() {
        let reference = serde_json::to_string(recover_ref.unwrap()).unwrap_or_default();
        format!("if available and authorized, call read_ref({{\"reference\":{reference}}}) and follow its cursor; otherwise repeat a narrower original request")
    } else if tool_name == "tool_orchestrate" {
        "repeat a narrower original request; do not treat this partial result as complete".to_string()
    } else if tool_name == "read_file" {
        "re-read the needed window with read_file(path, offset=nextOffset, limit=...) instead of re-reading the same range".to_string()
    } else {
        "narrow the query (concrete path/pattern, smaller head_limit) or read the persisted output by range instead of repeating this call".to_string()
    };
    (format!(
        "{head_text}\n...[omitted {omitted_tokens} of {raw_tokens} tokens; {raw_lines} total lines; {recovery}]...\n{tail_text}"
    ), true)
}

#[cfg(test)]
mod tests {

    #[test]
    fn original_child_coverage_rejects_duplicate_in_place_of_missing_window() {
        let expected = [(0, 0, Some(0), Some(0)), (0, 0, Some(0), Some(1))]
            .into_iter().collect();
        let first = serde_json::json!({"tool_name":"grep_search", "step_index":0,
            "position":0, "batch_index":0, "window_index":0});
        let second = serde_json::json!({"tool_name":"grep_search", "step_index":0,
            "position":0, "batch_index":0, "window_index":1});
        assert!(super::child_record_coverage_matches(&expected, &[first.clone(), second]));
        assert!(!super::child_record_coverage_matches(&expected, &[first.clone(), first]));
    }

    #[test]
    fn grep_batch_receipt_keeps_whole_members_within_original_budget() {
        let input = serde_json::json!({"searches":[{"pattern":"a"},{"pattern":"b"},{"pattern":"c"}]});
        let member = |index, text: String| serde_json::json!({"index":index,"status":"success",
            "output":{"mode":"content","content":text,"scanComplete":true,"truncated":false,"omissions":[]}});
        let first = member(0, "small first".into());
        let middle = member(1, "x".repeat(20000));
        let last = member(2, "small last".into());
        let output = serde_json::json!({"type":"grep_search","count":3,"results":[first.clone(),middle.clone(),last.clone()]}).to_string();
        let reference = super::EvidenceRef::observed("tool", "batch-test");
        let prefix = format!("Tool `grep_search` completed. Evidence: tool://{}. ", reference.id());
        let receipt = super::build_tool_receipt_for_delivery("grep_search", &output, false,
            reference, 400, false, false, Some(&input.to_string()));
        assert!(receipt.truncated);
        assert!(receipt.receipt_tokens <= 400);
        let body = receipt.summary.strip_prefix(&prefix).expect("original tool receipt prefix");
        let actual: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(actual["results"][0], first);
        assert_eq!(actual["results"][1]["status"], "omitted");
        assert_eq!(actual["results"][2], last);
        assert_eq!(actual["complete"], false);
        let child = |index: usize, presentation: &str| serde_json::json!({
            "parent_tool_name":"grep_search", "tool_name":"grep_search", "step_index":0, "step_id":"", "position":0,
            "batch_index":index, "input":input["searches"][index],
            "input_sha256":super::read_sha256(&serde_json::to_vec(&input["searches"][index]).unwrap()),
            "output_row":0, "presentation":presentation,
            "body":{"extent":{"kind":"file_lines","start_line":1,"line_count":1}}
        });
        assert!(super::child_read_presented_extent(&input.to_string(), &receipt.summary, &child(0, "small first")).is_some());
        assert!(super::child_read_presented_extent(&input.to_string(), &receipt.summary, &child(1, &"x".repeat(20000))).is_none());
        assert!(super::child_read_presented_extent(&input.to_string(), &receipt.summary, &child(2, "small last")).is_some());
        let mut foreign = child(0, "small first");
        foreign["input"]["pattern"] = serde_json::json!("foreign");
        assert!(super::child_read_presented_extent(&input.to_string(), &receipt.summary, &foreign).is_none());
        let zero = super::build_tool_receipt_for_delivery("grep_search", &output, false,
            super::EvidenceRef::observed("tool", "batch-test"), 0, false, false, Some(&input.to_string()));
        assert!(zero.truncated && zero.summary.is_empty());
        assert!(super::child_read_presented_extent(&input.to_string(), &zero.summary, &child(0, "small first")).is_none());
        let reference = super::EvidenceRef::observed("tool", "batch-test");
        let minimal = format!("Tool `grep_search` completed. Evidence: tool://{}. {{\"complete\":false}}", reference.id());
        let minimum_budget = super::estimate_text_tokens(&minimal);
        let too_small = super::build_tool_receipt_for_delivery("grep_search", &output, false,
            reference, minimum_budget, false, false, Some(&input.to_string()));
        assert!(too_small.truncated);
        assert_eq!(too_small.summary, minimal);
        assert!(too_small.receipt_tokens <= minimum_budget);
        assert!(super::child_read_presented_extent(&input.to_string(), &too_small.summary, &child(0, "small first")).is_none());
        let count_input = serde_json::json!({"searches":[{"pattern":"a"}]}).to_string();
        let count_receipt = format!("Tool `grep_search` completed. Evidence: tool://batch-test. {}",
            serde_json::json!({"results":[{"index":0,"status":"success","output":{"mode":"count","content":null}}]}));
        let mut count_child = child(0, "small first");
        count_child["input"] = serde_json::json!({"pattern":"a"});
        assert!(super::child_read_presented_extent(&count_input, &count_receipt, &count_child).is_none());
        let full = super::build_tool_receipt_for_delivery("grep_search", &output, false,
            super::EvidenceRef::observed("tool", "batch-test"), 30000, false, false, Some(&input.to_string()));
        assert!(!full.truncated);
        assert!(full.summary.contains(&middle.to_string()));
    }

    #[test]
    fn grep_batch_receipt_rejects_unbound_members_and_honours_tiny_budget() {
        let input = serde_json::json!({"searches":[{"pattern":"x"},{"pattern":"y"}]}).to_string();
        let duplicate = serde_json::json!({"type":"grep_search","count":2,"results":[
            {"index":0,"status":"success"},{"index":0,"status":"success"}]}).to_string();
        for source in [None, Some(input.as_str())] {
            let (shown, incomplete) = super::compact_grep_batch(&duplicate, source, "", 100).unwrap();
            assert!(incomplete);
            let body: serde_json::Value = serde_json::from_str(&shown).unwrap();
            assert_eq!(body["complete"], false);
            assert!(body["results"].is_null());
        }
        let unknown = serde_json::json!({"type":"grep_search","count":2,"results":[
            {"index":0,"status":"success"},{"index":9,"status":"success"}]}).to_string();
        let (shown, _) = super::compact_grep_batch(&unknown, Some(&input), "", 100).unwrap();
        assert!(serde_json::from_str::<serde_json::Value>(&shown).unwrap()["results"].is_null());
        let single = serde_json::json!({"mode":"content","content":"x"}).to_string();
        assert!(super::compact_grep_batch(&single, Some(&input), "", 100).is_none());
        let (tiny, incomplete) = super::compact_grep_batch(&duplicate, Some(&input), "prefix", 0).unwrap();
        assert!(incomplete);
        assert!(tiny.is_empty());
        assert_eq!(super::estimate_text_tokens(&tiny), 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn grep_samecall_target_requires_original_file_identity() {
        use harness_contract::context::{EvidenceCoverageKind, WorkspaceAccessMode};
        use harness_contract::goal::{ProofEffectScope, ProofEffectTarget};
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("a.txt"), "original\n").unwrap();
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let path = resolver.resolve_existing("a.txt").unwrap();
        let admitted = resolver.physical_file_identity_at_admission(&path).unwrap().unwrap();
        let lease = tools::ToolHost::builtin("grep-identity", root.path()).pin_snapshot();
        let resolution = lease.describe_effect_resolution("grep_search", &serde_json::json!({"path":"a.txt","pattern":".*"}));
        let scope = ProofEffectScope {
            version:2, descriptor_hash:resolution.descriptor.descriptor_hash.clone(), complete:true,
            scope_purposes:resolution.scope_purposes,
            targets:vec![ProofEffectTarget { path:path.clone(), declared_scope:resolution.descriptor.scopes[0].clone(),
                access_mode:WorkspaceAccessMode::Read, coverage:EvidenceCoverageKind::ScopedContent,
                exact:true, alias_checked:true, physical_identity:Some(admitted.clone()) }],
        };
        // Candidate validation only; these values are not appended as a receipt.
        let mut child = serde_json::json!({"relative_path":"a.txt","physical_identity":admitted,
            "effect":resolution.descriptor,"body":{"source_sha256":super::read_sha256(b"original\n")}});
        assert!(super::grep_read_target(&scope, &child).is_some());
        let mut missing = scope.clone(); missing.targets[0].physical_identity = None;
        assert!(super::grep_read_target(&missing, &child).is_none());
        std::fs::rename(root.path().join("a.txt"), root.path().join("old.txt")).unwrap();
        std::fs::write(root.path().join("a.txt"), "replacement\n").unwrap();
        child["physical_identity"] = serde_json::json!(resolver.physical_file_identity_at_admission(&path).unwrap().unwrap());
        assert!(super::grep_read_target(&scope, &child).is_none());
        let directory = resolver.resolve_existing(".").unwrap();
        assert!(resolver.physical_file_identity_at_admission(&directory).unwrap().is_none());
        let mut directory_scope = scope; directory_scope.targets[0].path = directory;
        directory_scope.targets[0].physical_identity = None;
        assert!(super::grep_read_target(&directory_scope, &child).is_none(), "lexical prefix is not ancestry");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn direct_grep_file_domain_keeps_parent_and_physical_scope_bound() {
        use harness_contract::context::{EvidenceCoverageKind, WorkspaceAccessMode};
        use harness_contract::goal::{ProofEffectScope, ProofEffectTarget};
        use harness_contract::policy::PermissionResource;
        let root = tempfile::tempdir().unwrap();
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let path = resolver.resolve_existing(".").unwrap();
        let lease = tools::ToolHost::builtin("grep-parent-scope", root.path()).pin_snapshot();
        let member = serde_json::json!({"pattern":"hit"});
        let input = serde_json::json!({"path":root.path().to_str().unwrap(),"searches":[member]});
        let resolution = lease.describe_effect_resolution("grep_search", &input);
        let file = resolution.descriptor.scopes.iter().find(|s| s.resource == PermissionResource::File).unwrap().clone();
        let scope = ProofEffectScope { version:2, descriptor_hash:resolution.descriptor.descriptor_hash.clone(),
            scope_purposes:resolution.scope_purposes, complete:false, targets:vec![ProofEffectTarget {
                path, declared_scope:file, access_mode:WorkspaceAccessMode::Read,
                coverage:EvidenceCoverageKind::ScopedContent, exact:false, alias_checked:false, physical_identity:None,
            }] };
        let effect = lease.describe_effect("grep_search", &input["searches"][0]);
        let mut child = serde_json::json!({"parent_tool_name":"grep_search","step_index":0,"step_id":"","position":0,
            "batch_index":0,"input":input["searches"][0],"input_sha256":super::read_sha256(&serde_json::to_vec(&input["searches"][0]).unwrap()),"effect":effect});
        assert!(super::grep::direct_grep_file_domain(&input, &resolution.descriptor, &scope, &child));
        child["batch_index"] = serde_json::json!(1);
        assert!(!super::grep::direct_grep_file_domain(&input, &resolution.descriptor, &scope, &child));
        child["batch_index"] = serde_json::json!(0);
        child["input"]["pattern"] = serde_json::json!("foreign");
        assert!(!super::grep::direct_grep_file_domain(&input, &resolution.descriptor, &scope, &child));
        child["input"] = input["searches"][0].clone();
        assert!(super::grep::direct_grep_file_domain(&input, &resolution.descriptor, &scope, &child));
        let mut missing = scope.clone(); missing.targets.clear();
        assert!(!super::grep::direct_grep_file_domain(&input, &resolution.descriptor, &missing, &child));
    }

    #[test]
    fn grep_samecall_delivery_rejects_summary_wrong_window_and_child_input() {
        let input = serde_json::json!({"pattern":"hit","path":"a.txt"});
        let mut child = serde_json::json!({"parent_tool_name":"grep_search","step_index":0,"step_id":"","position":0,
            "input":input,"input_sha256":super::read_sha256(&serde_json::to_vec(&input).unwrap()),
            "tool_name":"grep_search","output_row":0,"presentation":"a.txt:1:hit",
            "body":{"extent":{"kind":"file_lines","start_line":1,"line_count":1}}});
        let presented = "Tool `grep_search` completed. Evidence: tool://original. {\"mode\":\"content\",\"content\":\"a.txt:1:hit\"}";
        assert!(super::child_read_presented_extent(&input.to_string(), presented, &child).is_some());
        assert!(super::child_read_presented_extent(&input.to_string(), "summary only", &child).is_none());
        assert!(super::child_read_presented_extent(&input.to_string(), &presented[..presented.len()-1], &child).is_none());
        child["output_row"] = serde_json::json!(1);
        assert!(super::child_read_presented_extent(&input.to_string(), presented, &child).is_none());
        child["output_row"] = serde_json::json!(0);
        child["input"]["path"] = serde_json::json!("foreign.txt");
        assert!(super::child_read_presented_extent(&input.to_string(), presented, &child).is_none());
    }

    #[test]
    fn child_preview_requires_original_position_and_exact_content() {
        let input = serde_json::json!({"steps":[{"id":"r","op":"batch","calls":[{"tool":"read_file","input":{"path":"a"}}]}]}).to_string();
        let child = serde_json::json!({"step_index":0,"step_id":"r","position":0,
            "body":{"source_ref":"a","source_bytes":2,"source_sha256":"sha256:abc","extent":{"kind":"file_lines","start_line":1,"line_count":2,"total_lines":2,"ends_with_newline":true}},
            "prefixes":[{"line_count":1,"bytes":1,"sha256":super::read_sha256(b"a")}]});
        let mut value = serde_json::json!({"kind":"orchestration_partial_receipt","result":{
            "read_file[0]":{"source_tool":"read_file","ok":true,"output":{"file":{
                "filePath":"a","content":"a","startLine":1,"numLines":1,"totalLines":2
            }}}}});
        let show = |value: &serde_json::Value| format!("Tool `tool_orchestrate` completed. Evidence: tool://original. {value}");
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_some());
        let generic = format!("Tool `tool_orchestrate` completed. Evidence: tool://original. JSON object with 2 keys: kind, result. {value}");
        assert!(super::child_read_presented_extent(&input, &generic, &child).is_some());
        value["result"]["read_file[0]"]["output"]["file"]["content"] = serde_json::json!("b");
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_none());
        value["result"]["read_file[0]"]["output"]["file"]["content"] = serde_json::json!("a");
        value["result"]["read_file[0]"]["output"]["file"]["content_omitted"] = serde_json::json!(true);
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_none());
        value["result"]["read_file[0]"]["output"]["file"]["preview"] = serde_json::json!(true);
        value["result"]["read_file[0]"]["output"]["file"]["source_omitted"] = serde_json::json!(true);
        value["result"]["read_file[0]"]["output"]["file"]["sourceNumLines"] = serde_json::json!(2);
        value["result"]["read_file[0]"]["output"]["file"]["byteLength"] = serde_json::json!(2);
        value["result"]["read_file[0]"]["output"]["file"]["sha256"] = serde_json::json!("abc");
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_some());
        value["result"]["read_file[0]"]["output"]["file"]["sourceNumLines"] = serde_json::json!(3);
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_none());
        value["result"]["read_file[0]"]["output"]["file"]["sourceNumLines"] = serde_json::json!(2);
        value["result"]["read_file[0]"]["output"]["file"]["sha256"] = serde_json::json!("foreign");
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_none());
        value["result"]["read_file[0]"]["output"]["file"]["sha256"] = serde_json::json!("abc");
        let duplicate = serde_json::json!({"steps":[{"id":"r","op":"batch","calls":[{"tool":"read_file"}]},{"id":"r","op":"batch","calls":[]}]}).to_string();
        assert!(super::child_read_presented_extent(&duplicate, &show(&value), &child).is_none());
    }

    #[test]
    fn original_single_child_delivery_keeps_original_position_and_body() {
        let input = serde_json::json!({"steps":[{"id":"r","op":"call","tool":"read_file","input":{"path":"a"}}]}).to_string();
        let child = serde_json::json!({"step_index":0,"step_id":"r","position":0,"body":{"source_ref":"a",
            "extent":{"kind":"file_lines","start_line":1,"line_count":1,"total_lines":1,
                "body_bytes":1,"body_sha256":super::read_sha256(b"x"),
                "ends_with_newline":false,"source_omitted":false}},"prefixes":[]});
        let result = serde_json::json!({"result":{"r":{"file":{"filePath":"a","content":"x",
            "startLine":1,"numLines":1,"totalLines":1}}}});
        let shown = format!("Tool `tool_orchestrate` completed. Evidence: tool://original. {result}");
        assert!(super::child_read_presented_extent(&input, &shown, &child).is_some());
        let mut foreign = child.clone(); foreign["position"] = serde_json::json!(1);
        assert!(super::child_read_presented_extent(&input, &shown, &foreign).is_none());
        let mut changed = result.clone(); changed["result"]["r"]["file"]["content"] = serde_json::json!("y");
        let shown = format!("Tool `tool_orchestrate` completed. Evidence: tool://original. {changed}");
        assert!(super::child_read_presented_extent(&input, &shown, &child).is_none());
    }

    #[test]
    fn child_small_complete_body_can_exceed_prefix_line_limit() {
        let body = (0..58).map(|i| format!("line {i}")).collect::<Vec<_>>().join("\n");
        let input = serde_json::json!({"steps":[{"id":"r","op":"batch","calls":[{"tool":"read_file","input":{"path":"a"}}]}]}).to_string();
        let child = serde_json::json!({"step_index":0,"step_id":"r","position":0,"body":{"source_ref":"a",
            "extent":{"kind":"file_lines","start_line":1,"line_count":58,"total_lines":58,
                "body_bytes":body.len(),"body_sha256":super::read_sha256(body.as_bytes()),
                "ends_with_newline":false,"source_omitted":false}},"prefixes":[]});
        let mut value = serde_json::json!({"kind":"orchestration_partial_receipt","result":{
            "read_file[0]":{"source_tool":"read_file","ok":true,"output":{"file":{
                "filePath":"a","content":body,"startLine":1,"numLines":58,"totalLines":58}}}}});
        let show = |value: &serde_json::Value| format!("Tool `tool_orchestrate` completed. Evidence: tool://original. {value}");
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_some());
        value["result"]["read_file[0]"]["output"]["file"]["content"] = serde_json::json!("forged\n".repeat(57) + "forged");
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_none());
    }

    #[test]
    fn child_empty_complete_body_is_distinct_from_omission() {
        let input = serde_json::json!({"steps":[{"id":"r","op":"batch","calls":[{"tool":"read_file","input":{"path":"empty"}}]}]}).to_string();
        let child = serde_json::json!({"step_index":0,"step_id":"r","position":0,"body":{"source_ref":"empty",
            "extent":{"kind":"file_lines","start_line":1,"line_count":0,"total_lines":0,"body_bytes":0,
                "body_sha256":super::read_sha256(b""),"ends_with_newline":false,"source_omitted":false}},"prefixes":[]});
        let mut value = serde_json::json!({"kind":"orchestration_partial_receipt","result":{
            "read_file[0]":{"source_tool":"read_file","ok":true,"output":{"file":{
                "filePath":"empty","content":"","startLine":1,"numLines":0,"totalLines":0}}}}});
        let show = |value: &serde_json::Value| format!("Tool `tool_orchestrate` completed. Evidence: tool://original. {value}");
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_some());
        value["result"]["read_file[0]"]["output"]["file"]["content_omitted"] = serde_json::json!(true);
        assert!(super::child_read_presented_extent(&input, &show(&value), &child).is_none());
    }
    use super::*;

    fn root_delivery_fixture() -> (
        ReadDeliveryMaterial,
        crate::runtime_event_store::DurableRuntimeEvent,
    ) {
        let binding: harness_contract::projection::RuntimeActivityBinding = serde_json::from_value(serde_json::json!({
            "root_execution_id":"root", "session_id":"session", "turn_id":"turn",
            "root_task_id":"root-task", "task_id":"root-task",
            "activity_id":"activity:execution:root:owner:root:tool:causal-step:read",
            "parent_activity_id":"activity:execution:root", "initiator_activity_id":"activity:execution:root",
            "tool_call_id":"read", "tool_contract_id":"read_ref", "revision":1,"fence":1,"generation":1,
        })).unwrap();
        let payload = serde_json::json!({"session_id":"session","tool_call_id":"read","tool_name":"read_ref","invocation_id":"tool-inv",
            "status":"completed","is_error":false,"full_output_ref":"tool://raw","_runtime_activity_binding":binding});
        let event = crate::runtime_event_store::DurableRuntimeEvent {
            event_id: "owner-event".into(),
            stream_id: "session:session".into(),
            sequence: 5,
            scope: crate::runtime_event_store::RuntimeEventScope::Tool,
            kind: "tool.invocation.completed".into(),
            status: Some("completed".into()),
            actor: Some("conversation_runtime".into()),
            refs: [
                ("tool_call", "read"),
                ("tool_invocation", "tool-inv"),
                ("execution", "root"),
                ("session", "session"),
                ("turn", "turn"),
            ]
            .into_iter()
            .map(|(kind, id)| crate::runtime_event_store::RuntimeEventRef {
                kind: kind.into(),
                id: id.into(),
            })
            .collect(),
            payload,
            created_at_ms: 1,
            commit_cursor: 20,
            transaction_id: "owner-tx".into(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        };
        let owner = RootReadOwner {
            stream_id: event.stream_id.clone(),
            sequence: event.sequence,
            event_id: event.event_id.clone(),
            payload_digest: read_payload_digest(&event.payload),
            binding,
        };
        let step = harness_contract::step::StepIdentity {
            step_id: "source-step".into(),
            session_id: "session".into(),
            turn_id: "turn".into(),
            execution_id: Some("root".into()),
            actor_ref: None,
            run_ref: None,
            attempt: None,
            epoch: Some("turn".into()),
            goal_ref: None,
            program_ref: None,
            task_ref: None,
            build_ref: "fixture".into(),
        };
        let acquisition = ReadAcquisitionSeal {
            schema_version: 1,
            primary_stream: "execution-effect:key".into(),
            primary_key: "key:read-receipt".into(),
            index_stream: "execution-agent-receipts:tool-graph:tool-node:2".into(),
            index_key: "agent-tool-receipt:key".into(),
            session_id: "session".into(),
            invocation_id: "read".into(),
            tool_name: "read_ref".into(),
            graph_id: "tool-graph".into(),
            node_id: "tool-node".into(),
            attempt: 2,
            step,
            origin: serde_json::to_value(harness_contract::step::CallOrigin {
                origin_step_id: "source-step".into(),
                call_id: "read".into(),
                catalog_digest: "catalog".into(),
                source: harness_contract::step::CallSource::Provider {
                    attempt_id: "attempt".into(),
                },
            })
            .unwrap(),
            input_sha256: read_sha256(b"{}"),
            body: ReadBodySeal {
                schema_version: 1,
                source_ref: "artifact://empty".into(),
                source_sha256: read_sha256(b""),
                source_bytes: 0,
                output_sha256: read_sha256(b"output"),
                output_bytes: 6,
                extent: ReadExtent::ArtifactChunks {
                    total_chunks: 0,
                    chunks: vec![],
                },
            },
        };
        let material = ReadDeliveryMaterial {
            schema_version: 1,
            grade: ReadDeliveryGrade::ProviderModelObserved,
            delivered_extent: Some(acquisition.body.extent.clone()),
            acquisition,
            acquisition_event_id: "primary".into(),
            acquisition_cursor: 10,
            index_event_id: "index".into(),
            session_id: "session".into(),
            execution_id: "root".into(),
            turn_id: "turn".into(),
            tool_invocation_id: "read".into(),
            recipient: ReadDeliveryRecipient::Root {
                owner,
                identity: None,
            },
            presentation_sha256: read_sha256(b"presentation"),
            presentation_bytes: 12,
            body_omitted: false,
            transport: ReadDeliveryTransport::NativeProviderContinuation {
                request_id: "request".into(),
                request_sequence: 3,
                provider_attempt: 1,
                step_render: harness_contract::step::StepRenderRef {
                    step_id: "continuation".into(),
                    attempt_id: "wire-attempt".into(),
                    catalog_digest: "catalog".into(),
                    render_digest: "render".into(),
                },
                wire_body_sha256: "wire".into(),
                wire_artifact: harness_contract::context::ArtifactRef::durable(
                    "artifact://wire",
                    read_sha256(b"wire"),
                    4,
                    "application/json",
                    "session:session",
                ),
                packed_sequence: 8,
                outcome_sequence: 9,
                model: "model".into(),
                raw_ref: EvidenceRef::observed("tool", "raw"),
            },
        };
        (material, event)
    }

    #[test]
    fn root_read_intent_authenticates_original_owner_scope_and_atomic_index() {
        let (material, template) = root_delivery_fixture();
        let mut seal = material.acquisition;
        seal.graph_id = "root".into();
        seal.tool_name = "read_file".into();
        seal.index_stream = "execution-agent-receipts:root:tool-node:2".into();
        let output = serde_json::json!({"type":"text","truncated":false,"file":{
            "filePath":"input.txt","content":"original body","startLine":1,"numLines":1,"totalLines":1,
            "sha256":read_sha256(b"original body"),"byteLength":13,"endsWithNewline":false,
        }}).to_string();
        seal.body = read_body_seal("read_file", &output).unwrap();
        let ReadDeliveryRecipient::Root { owner, .. } = material.recipient else {
            panic!("Root");
        };
        let mut binding = owner.binding;
        binding.node_id = Some(seal.node_id.clone());
        binding.activity_id = "activity:execution:root:node:tool-node".into();
        binding.tool_call_id = None;
        binding.tool_contract_id = None;
        let host = tools::ToolHost::builtin("root-admission", std::env::temp_dir()).pin_snapshot();
        let resolution =
            host.describe_effect_resolution("read_file", &serde_json::json!({"path":"input.txt"}));
        let scope = harness_contract::goal::ProofEffectScope {
            version: 2,
            descriptor_hash: resolution.descriptor.descriptor_hash.clone(),
            complete: false,
            targets: Vec::new(),
            scope_purposes: resolution.scope_purposes,
        };
        let effect = serde_json::to_value(&resolution.descriptor).unwrap();
        let replay = serde_json::json!({"session_id":seal.session_id,"call_id":seal.invocation_id,
            "origin":seal.origin,"parent_execution":null,"invocation":{"effect":effect}});
        let mut intent = template.clone();
        intent.stream_id = seal.primary_stream.clone();
        intent.kind = "execution.read.intent".into();
        intent.scope = crate::RuntimeEventScope::ExecutionNode;
        intent.actor = Some("governed_tool".into());
        intent.status = Some("inflight".into());
        intent.idempotency_key = Some("key:read-intent".into());
        intent.commit_cursor = 8;
        intent.transaction_id = "admission-tx".into();
        intent.payload = serde_json::json!({"idempotency_key":"key", "tool_use_id":seal.invocation_id,
            "tool_name":seal.tool_name,"input_sha256":seal.input_sha256,"effect":effect,
            "resolved_effect_scope":scope,"replay":replay,
            "root_effect_context":{"execution_id":"root","session_id":"session","turn_id":"turn"},
            "root_effect_owner":{"binding":binding,"attempt":2,"graph_revision":7}});
        let mut primary = intent.clone();
        primary.kind = "execution.read.receipt".into();
        primary.status = Some("completed".into());
        primary.commit_cursor = 9;
        primary.transaction_id = "receipt-tx".into();
        primary.transaction_index = 0;
        primary.payload["outcome"] = serde_json::json!({"output":output});
        primary.refs = [
            ("execution_graph", "root"),
            ("execution_node", "tool-node"),
            ("agent_attempt", "2"),
            ("turn", "turn"),
        ]
        .into_iter()
        .map(|(kind, id)| crate::RuntimeEventRef {
            kind: kind.into(),
            id: id.into(),
        })
        .collect();
        intent.refs = primary.refs.clone();
        let mut index = primary.clone();
        index.kind = "execution.agent_tool.receipt".into();
        index.stream_id = seal.index_stream.clone();
        index.transaction_index = 1;
        assert_eq!(
            authenticate_root_read_intent(&intent, &primary, &index, &seal).unwrap(),
            binding
        );
        for field in [
            "root_effect_owner",
            "root_effect_context",
            "resolved_effect_scope",
            "effect",
        ] {
            let mut forged = primary.clone();
            forged.payload[field] = serde_json::Value::Null;
            assert!(
                authenticate_root_read_intent(&intent, &forged, &index, &seal).is_err(),
                "{field}"
            );
        }
        for mutation in [
            "attempt",
            "node",
            "extra-key",
            "parent",
            "scope1",
            "missing-purpose",
            "input",
            "origin",
            "compiled",
        ] {
            let mut forged_intent = intent.clone();
            match mutation {
                "attempt" => forged_intent.payload["root_effect_owner"]["attempt"] = 3.into(),
                "node" => {
                    forged_intent.payload["root_effect_owner"]["binding"]["node_id"] =
                        "parallel-node".into()
                }
                "extra-key" => {
                    forged_intent.payload["root_effect_owner"]["node_id"] = "tool-node".into()
                }
                "parent" => {
                    forged_intent.payload["replay"]["parent_execution"] =
                        serde_json::json!({"execution_id":"root","node_id":"tool-node"})
                }
                "scope1" => forged_intent.payload["resolved_effect_scope"]["version"] = 1.into(),
                "missing-purpose" => {
                    forged_intent.payload["resolved_effect_scope"]
                        .as_object_mut()
                        .unwrap()
                        .remove("scope_purposes");
                }
                "input" => forged_intent.payload["input_sha256"] = read_sha256(b"foreign").into(),
                "origin" => {
                    forged_intent.payload["replay"]["origin"]["call_id"] = "foreign-call".into()
                }
                "compiled" => {
                    forged_intent.payload["replay"]["invocation"]["effect"] =
                        serde_json::Value::Null
                }
                _ => unreachable!(),
            }
            let mut forged_primary = primary.clone();
            let mut forged_index = index.clone();
            for field in [
                "root_effect_owner",
                "root_effect_context",
                "resolved_effect_scope",
                "effect",
                "replay",
                "input_sha256",
            ] {
                forged_primary.payload[field] = forged_intent.payload[field].clone();
                forged_index.payload[field] = forged_intent.payload[field].clone();
            }
            assert!(
                authenticate_root_read_intent(
                    &forged_intent,
                    &forged_primary,
                    &forged_index,
                    &seal
                )
                .is_err(),
                "{mutation}"
            );
        }
        let mut foreign = index.clone();
        foreign.transaction_id = "other-tx".into();
        assert!(authenticate_root_read_intent(&intent, &primary, &foreign, &seal).is_err());
        foreign = index.clone();
        foreign
            .refs
            .retain(|reference| reference.kind != "execution_node");
        assert!(authenticate_root_read_intent(&intent, &primary, &foreign, &seal).is_err());
        foreign = index.clone();
        foreign.payload["outcome"]["output"] = "foreign-body".into();
        assert!(authenticate_root_read_intent(&intent, &primary, &foreign, &seal).is_err());
    }

    #[test]
    fn root_recipient_structural_binding_preserves_none_and_task_graph() {
        let (mut material, event) = root_delivery_fixture();
        assert!(decode_read_delivery(&serde_json::to_value(&material).unwrap()).is_ok());
        let ReadDeliveryRecipient::Root { owner, identity } = &mut material.recipient else {
            panic!("Root");
        };
        assert!(root_read_owner_matches(
            owner,
            &event,
            &material.acquisition
        ));
        assert_eq!(owner.payload_digest.len(), 64);
        *identity = Some(
            harness_contract::execution::ExecutionIdentity::for_task_graph(
                "principal",
                "workspace",
                "mission",
                "root-task",
                "session",
                "turn",
                "receiver-graph",
            )
            .unwrap(),
        );
        material.acquisition.step.actor_ref = Some("principal".into());
        material.acquisition.step.task_ref = Some("root-task".into());
        assert!(decode_read_delivery(&serde_json::to_value(&material).unwrap()).is_ok());
    }

    #[test]
    fn root_owner_requires_exact_event_payload_origin_and_live_scope() {
        let (material, event) = root_delivery_fixture();
        let ReadDeliveryRecipient::Root { owner, .. } = &material.recipient else {
            panic!("Root");
        };
        for field in ["session_id", "tool_call_id", "tool_name", "status"] {
            let mut foreign = event.clone();
            foreign.payload[field] = "foreign".into();
            assert!(!root_read_owner_matches(
                owner,
                &foreign,
                &material.acquisition
            ));
        }
        let mut foreign_owner = owner.clone();
        foreign_owner.binding.root_execution_id = "foreign-root".into();
        assert!(!root_read_owner_matches(
            &foreign_owner,
            &event,
            &material.acquisition
        ));
        foreign_owner = owner.clone();
        foreign_owner.sequence += 1;
        assert!(!root_read_owner_matches(
            &foreign_owner,
            &event,
            &material.acquisition
        ));
        let mut forged = serde_json::to_value(&material).unwrap();
        forged["recipient"]["owner"]["binding"]["turn_id"] = "foreign-turn".into();
        assert!(decode_read_delivery(&forged).is_err());
        forged = serde_json::to_value(&material).unwrap();
        forged["recipient_identity"] = serde_json::Value::Null;
        assert!(decode_read_delivery(&forged).is_err());
    }

    #[test]
    fn body_omission_survives_larger_receipt_prefix() {
        let output = "middle-body-to-omit";
        let receipt = build_tool_receipt(
            &"long-tool-name".repeat(40),
            output,
            false,
            EvidenceRef::observed("tool", "prefix"),
            1,
        );
        assert!(receipt.receipt_tokens > receipt.raw_tokens);
        assert!(receipt.truncated);
        assert!(receipt.omitted_tokens > 0);
    }

    #[test]
    fn full_body_with_prefix_has_no_omission() {
        for output in ["", "body", "body\n", "first\r\nsecond\n", "中文正文"] {
            let receipt = build_tool_receipt(
                "read_file",
                output,
                false,
                EvidenceRef::observed("tool", "complete"),
                1_000,
            );
            assert!(!receipt.truncated);
            assert_eq!(receipt.omitted_tokens, 0);
        }
    }

    #[test]
    fn file_seal_separates_raw_revision_from_normalized_presentation() {
        let raw = "中文\r\nlast\r\n";
        let body = "中文\nlast";
        let output = serde_json::json!({"type":"text","truncated":false,"file":{
            "filePath":"/workspace/source.txt","content":body,"startLine":1,"numLines":2,
            "totalLines":2,"endsWithNewline":true,"sha256":read_sha256(raw.as_bytes()),
            "byteLength":raw.len()}})
        .to_string();
        let seal = read_body_seal("read_file", &output).unwrap();
        assert_eq!(seal.source_sha256, read_sha256(raw.as_bytes()));
        assert_eq!(seal.source_bytes, raw.len() as u64);
        let ReadExtent::FileLines {
            body_sha256,
            body_bytes,
            source_omitted,
            ..
        } = seal.extent
        else {
            panic!("file extent");
        };
        assert_eq!(body_sha256, read_sha256(body.as_bytes()));
        assert_eq!(body_bytes, body.len() as u64);
        assert!(!source_omitted);
    }

    #[test]
    fn empty_file_and_partial_window_keep_actual_extent() {
        let output = serde_json::json!({"type":"text","truncated":false,"file":{
            "filePath":"/workspace/empty","content":"","startLine":1,"numLines":0,
            "totalLines":0,"endsWithNewline":false,"sha256":read_sha256(b""),"byteLength":0}});
        assert!(read_body_seal("read_file", &output.to_string()).is_some());
        let mut partial = output;
        partial["file"]["startLine"] = 2.into();
        partial["file"]["numLines"] = 1.into();
        partial["file"]["totalLines"] = 3.into();
        partial["file"]["content"] = "middle".into();
        partial["truncated"] = true.into();
        let seal = read_body_seal("read_file", &partial.to_string()).unwrap();
        assert!(matches!(
            seal.extent,
            ReadExtent::FileLines {
                source_omitted: true,
                start_line: 2,
                line_count: 1,
                ..
            }
        ));
    }

    #[test]
    fn artifact_seal_requires_real_unique_chunk_bodies_not_digest_summary() {
        let mut output = serde_json::json!({"kind":"evidence_retrieve","available":true,
            "evidence_ref":"artifact://immutable","sha256":read_sha256(b"source"),"bytes":6,
            "total_chunks":3,"chunks":[{"index":1,"content":"page"}]});
        assert!(read_body_seal("read_ref", &output.to_string()).is_some());
        output["chunks"] =
            serde_json::json!([{"index":1,"characters":4,"content_sha256":read_sha256(b"page")}]);
        assert!(read_body_seal("read_ref", &output.to_string()).is_none());
        output["chunks"] =
            serde_json::json!([{"index":1,"content":"page"},{"index":1,"content":"page"}]);
        assert!(read_body_seal("read_ref", &output.to_string()).is_none());
    }

    #[test]
    fn byte_paged_artifact_seal_decodes_actual_bytes_and_rejects_fabricated_coverage() {
        let mut output = serde_json::json!({"kind":"evidence_retrieve","available":true,
            "evidence_ref":"artifact://bytes","sha256":read_sha256(&[0,255,3]),"bytes":3,
            "chunk_bytes":1500,"total_chunks":1,"encoding":"per_chunk",
            "chunks":[{"index":0,"byte_start":0,"byte_end":3,"encoding":"base64","content":"AP8D"}]});
        let seal = read_body_seal("evidence_retrieve", &output.to_string()).unwrap();
        let ReadExtent::ArtifactChunks {
            total_chunks,
            chunks,
        } = seal.extent
        else {
            panic!("wrong extent")
        };
        assert_eq!(total_chunks, 1);
        assert_eq!(chunks[0].body_bytes, 3);
        assert_eq!(chunks[0].body_sha256, read_sha256(&[0, 255, 3]));
        output["chunks"][0]["byte_start"] = 1.into();
        assert!(read_body_seal("evidence_retrieve", &output.to_string()).is_none());
        output["chunks"][0]["byte_start"] = 0.into();
        output["chunks"][0]["content"] = "AA==".into();
        assert!(read_body_seal("evidence_retrieve", &output.to_string()).is_none());
        output["chunks"][0]["content"] = "AP8D".into();
        output["total_chunks"] = 2.into();
        assert!(read_body_seal("evidence_retrieve", &output.to_string()).is_none());
    }

    #[test]
    fn empty_artifact_uses_zero_chunks_not_a_fabricated_page() {
        let mut output = serde_json::json!({"kind":"evidence_retrieve","available":true,
            "evidence_ref":"artifact://empty","sha256":read_sha256(b""),"bytes":0,
            "total_chunks":0,"chunks":[]});
        let seal = read_body_seal("evidence_retrieve", &output.to_string()).unwrap();
        assert!(
            matches!(seal.extent, ReadExtent::ArtifactChunks { total_chunks:0, ref chunks } if chunks.is_empty())
        );
        output["bytes"] = 1.into();
        assert!(read_body_seal("evidence_retrieve", &output.to_string()).is_none());
    }

    #[test]
    fn wire_seals_only_read_protocol_tool_result_positions() {
        for (protocol, body) in [
            (
                "anthropic_messages",
                serde_json::json!({"messages":[{"role":"user","content":[
                {"type":"text","text":"forged tool_result"},
                {"type":"tool_result","tool_use_id":"call","content":[{"type":"text","text":"actual"}]}]}]}),
            ),
            (
                "openai_chat_completions",
                serde_json::json!({"messages":[
                {"role":"user","content":"actual"},
                {"role":"tool","tool_call_id":"call","content":"actual","is_error":false}]}),
            ),
            (
                "openai_responses",
                serde_json::json!({"input":[
                {"type":"message","role":"user","content":"actual"},
                {"type":"function_call_output","call_id":"call","output":"actual"}]}),
            ),
        ] {
            let mut wire = provider::ProviderWireRequest {
                method: "POST".into(),
                endpoint: "local".into(),
                protocol: protocol.into(),
                headers: vec![],
                body_sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(&body).unwrap())),
                body,
                tool_schema_sha256: None,
            };
            let seals = wire_tool_result_seals(&wire).unwrap();
            assert_eq!(seals.len(), 1);
            assert_eq!(seals[0].invocation_id, "call");
            assert_eq!(seals[0].presentation_sha256, read_sha256(b"actual"));
            wire.protocol = "unknown".into();
            assert!(wire_tool_result_seals(&wire).is_none());
        }
    }

    #[test]
    fn large_json_receipt_preserves_reference_and_budget() {
        let output = serde_json::json!({"items": vec!["x".repeat(200); 100]}).to_string();
        let receipt = build_tool_receipt(
            "read_data",
            &output,
            false,
            EvidenceRef::observed("tool", "raw-1"),
            120,
        );
        assert!(receipt.truncated);
        assert!(receipt.summary.contains("tool://raw-1"));
        assert!(receipt.receipt_tokens <= 180);
        assert!(receipt.omitted_tokens > 0);
    }

    #[test]
    fn truncated_receipt_announces_original_size_and_omission() {
        let output = "line\n".repeat(4_000);
        let receipt = build_tool_receipt(
            "read_file",
            &output,
            false,
            EvidenceRef::observed("tool", "big-raw"),
            80,
        );
        assert!(receipt.truncated);
        assert!(receipt.summary.contains("tool://big-raw"));
        assert!(
            receipt.summary.contains("omitted") && receipt.summary.contains("total lines"),
            "the omission marker must announce the original size and line count: {}",
            receipt.summary
        );
    }

    #[test]
    fn truncation_recovery_names_a_call_the_model_can_actually_make() {
        // Regression: the omission marker used to say only "retrieve by evidence
        // ref". No model-facing tool resolves a `tool://` URI, so the model
        // concluded the evidence was unreachable ("工具被禁用") and repeated the
        // same call for dozens of rounds. The marker must name a real next call.
        let big = "fn evidence() {}\n".repeat(2_000);

        let read = build_tool_receipt(
            "read_file",
            &big,
            false,
            EvidenceRef::observed("tool", "raw-read"),
            40,
        );
        assert!(
            read.summary.contains("read_file") && read.summary.contains("offset=nextOffset"),
            "a read truncation must name the windowed re-read: {}",
            read.summary
        );
        assert!(
            !read.summary.contains("retrieve by evidence ref"),
            "an unresolvable evidence URI must not be offered as the recovery: {}",
            read.summary
        );

        let search = build_tool_receipt(
            "grep_search",
            &big,
            false,
            EvidenceRef::observed("tool", "raw-grep"),
            40,
        );
        assert!(
            search.summary.contains("narrow") || search.summary.contains("persisted"),
            "a non-read truncation must name a narrower query or a ranged read: {}",
            search.summary
        );
    }

    #[test]
    fn composite_durable_omission_names_exact_scoped_reader_and_ephemeral_does_not() {
        let big = format!("{{\"calls\":[{{\"status\":\"ok\"}}],\"result\":\"{}\"}}", "a".repeat(20000));
        let durable = build_tool_receipt_for_delivery("tool_orchestrate", &big, false, EvidenceRef::observed("tool", "raw-composite"), 80, false, true, None);
        assert!(durable.truncated);
        assert!(durable.summary.contains(r#"read_ref({"reference":"tool://raw-composite"})"#));
        assert!(durable.summary.contains("follow its cursor"));
        let transient = build_tool_receipt_for_delivery("tool_orchestrate", &big, false, EvidenceRef::observed("tool", "raw-composite"), 80, false, false, None);
        assert!(transient.truncated);
        assert!(!transient.summary.contains("read_ref("));
    }

    #[test]
    fn bounded_composite_receipt_preserves_results_and_never_hides_incomplete_children() {
        let raw = serde_json::json!({"calls":[{"stepId":"b","op":"batch","status":"ok","calls":2}],"result":{
            "complete":{"ok":true,"status":"success","output":{"type":"read_file","truncated":false,"file":{"filePath":"Cargo.toml","content":"[workspace]\nmembers = []\n","startLine":1,"numLines":2,"totalLines":2}}},
            "partial":{"ok":true,"status":"success","output":{"truncated":true,"scanComplete":false,"numLines":100,"next_request":{"cursor":"continuation"},"content":"large".repeat(20000)}}}}).to_string();
        let receipt = build_tool_receipt_for_delivery("tool_orchestrate", &raw, false, EvidenceRef::observed("tool", "raw-composite"), 10000, false, true, None);
        assert!(receipt.truncated);
        assert!(receipt.summary.contains("orchestration_partial_receipt"));
        assert!(receipt.summary.contains("\"partial\""));
        assert!(receipt.summary.contains("\"scanComplete\":false"));
        assert!(receipt.summary.contains("continuation"));
        assert!(receipt.summary.contains("tool://raw-composite"));
        assert!(receipt.summary.contains("members = []"));
        assert!(!receipt.summary.contains(&"large".repeat(100)));
        let tiny = build_tool_receipt_for_delivery("tool_orchestrate", &raw, false, EvidenceRef::observed("tool", "raw-composite"), 40, false, true, None);
        assert!(tiny.truncated && tiny.summary.contains("read_ref("));
    }

    #[test]
    fn default_batch_receipt_preserves_direct_children_and_rejects_ambiguous_sources() {
        let input = serde_json::json!({"steps":[{"id":"read_all","op":"batch","calls":[
            {"tool":"read_file","input":{"path":"a.rs"}},
            {"tool":"read_file","input":{"path":"b.rs"}}
        ]}]}).to_string();
        let items = serde_json::json!({
            "read_file[0]":{"type":"text","truncated":false,"file":{"filePath":"a.rs","content":"small","startLine":1,"numLines":1,"totalLines":1}},
            "read_file[1]":{"type":"text","truncated":false,"file":{"filePath":"b.rs","content":"large".repeat(10000),"startLine":1,"numLines":1,"totalLines":1}}
        });
        let mut raw = serde_json::json!({"calls":[{"stepId":"read_all","op":"batch","status":"ok","calls":2}],
            "steps":{"read_all":items},"result":{"read_all":items}});
        let compact = compact_orchestrated_batch(&raw.to_string(), "tool://raw", 10000, Some(&input)).unwrap();
        let body: serde_json::Value = serde_json::from_str(&compact).unwrap();
        assert_eq!(body["complete"], false);
        assert_eq!(body["result"]["read_file[0]"]["source_tool"], "read_file");
        assert_eq!(body["result"]["read_file[0]"]["output"]["file"]["content"], "small");
        assert_eq!(body["result"]["read_file[1]"]["output"]["file"]["content_omitted"], true);
        assert_eq!(body["recovery"]["when_available_and_authorized"]["input"]["reference"], "tool://raw");
        let mut selected_input: serde_json::Value = serde_json::from_str(&input).unwrap();
        selected_input["return"] = "$read_all".into();
        let mut selected_raw = raw.clone();
        selected_raw["result"] = selected_raw["steps"]["read_all"].clone();
        let selected = compact_orchestrated_batch(&selected_raw.to_string(), "tool://raw", 10000, Some(&selected_input.to_string())).unwrap();
        let selected: serde_json::Value = serde_json::from_str(&selected).unwrap();
        assert_eq!(selected["result"]["read_file[0]"]["source_tool"], "read_file");
        raw["calls"][0]["status"] = "error".into();
        assert!(compact_orchestrated_batch(&raw.to_string(), "tool://raw", 10000, Some(&input)).is_none());
        raw["calls"][0]["status"] = "ok".into();
        raw["steps"]["read_all"]["read_file[0]"]["truncated"] = true.into();
        assert!(compact_orchestrated_batch(&raw.to_string(), "tool://raw", 10000, Some(&input)).is_none());
        raw["steps"]["read_all"]["read_file[0]"]["truncated"] = false.into();
        let wrong = input.replace("read_file", "grep_search");
        assert!(compact_orchestrated_batch(&raw.to_string(), "tool://raw", 10000, Some(&wrong)).is_none());
    }

    #[test]
    fn default_multiple_batches_remain_bounded_and_keep_step_identity() {
        let input = serde_json::json!({"steps":[
            {"id":"a","op":"batch","calls":[{"tool":"read_file","input":{"path":"a.rs"}}]},
            {"id":"b","op":"batch","calls":[{"tool":"read_file","input":{"path":"b.rs"}}]}
        ]});
        let a = serde_json::json!({"read_file[0]":{"type":"text","truncated":false,"file":{"filePath":"a.rs","content":"a"}}});
        let b = serde_json::json!({"read_file[0]":{"type":"text","truncated":false,"file":{"filePath":"b.rs","content":"b"}}});
        let mut raw = serde_json::json!({"calls":[{"stepId":"a","op":"batch","status":"ok","calls":1},
            {"stepId":"b","op":"batch","status":"ok","calls":1}],
            "steps":{"a":a,"b":b},"result":{"a":a,"b":b}});
        let compact = compact_orchestrated_batch(&raw.to_string(), "tool://raw", 10000, Some(&input.to_string())).unwrap();
        let v: serde_json::Value = serde_json::from_str(&compact).unwrap();
        assert_eq!(v["complete"], false);
        assert_eq!(v["result"]["a"]["read_file[0]"]["output"]["file"]["content"], "a");
        assert_eq!(v["result"]["b"]["read_file[0]"]["output"]["file"]["content"], "b");
        raw["calls"][1]["stepId"] = "a".into();
        assert!(compact_orchestrated_batch(&raw.to_string(), "tool://raw", 10000, Some(&input.to_string())).is_none());
    }

    #[test]
    fn unknown_or_oversized_orchestration_is_explicit_and_reference_scoped() {
        let fallback = bounded_orchestration_fallback("tool://raw", 1000).unwrap();
        assert!(fallback.contains("Partial orchestration output omitted"));
        assert!(fallback.contains(r#"read_ref({"reference":"tool://raw"})"#));
        assert!(bounded_orchestration_fallback("tool://raw", 1).is_none());
    }

    #[test]
    fn batch_file_preview_is_exact_complete_lines_and_preserves_source_extent() {
        let content = (0..1000).map(|i| format!("line-{i}-数据")).collect::<Vec<_>>().join("\n");
        let input = serde_json::json!({"steps":[{"id":"read_all","op":"batch","calls":[{"tool":"read_file","input":{"path":"a.rs"}}]}]}).to_string();
        let value = serde_json::json!({"read_file[0]":{"type":"text","truncated":false,"file":{"filePath":"a.rs","content":content,"startLine":1,"numLines":1000,"totalLines":1000,"sha256":"original","byteLength":12345}}});
        let mut raw = serde_json::json!({"calls":[{"stepId":"read_all","op":"batch","status":"ok","calls":1}],"steps":{"read_all":value},"result":{"read_all":value}});
        let output = compact_orchestrated_batch(&raw.to_string(), "tool://raw", 12000, Some(&input)).unwrap();
        let v:serde_json::Value = serde_json::from_str(&output).unwrap();
        let file = &v["result"]["read_file[0]"]["output"]["file"];
        let preview = file["content"].as_str().unwrap();
        assert!(content.starts_with(preview));
        assert_eq!(content.as_bytes()[preview.len()], b'\n');
        assert!(preview.len() <= 4096);
        assert_eq!(file["numLines"], 16);
        assert_eq!(file["sourceNumLines"], 1000);
        assert_eq!(file["totalLines"], 1000);
        assert_eq!(file["sha256"], "original");
        assert_eq!(file["preview"], true);
        assert_eq!(file["source_omitted"], true);
        assert_eq!(file["content_omitted"], true);
        assert_eq!(v["result"]["read_file[0]"]["output"]["source_truncated"], false);
        assert_eq!(v["result"]["read_file[0]"]["output"]["truncated"], true);
        raw["steps"]["read_all"]["read_file[0]"]["file"]["numLines"] = 999.into();
        raw["result"] = raw["steps"].clone();
        let malformed = compact_orchestrated_batch(&raw.to_string(), "tool://raw", 12000, Some(&input)).unwrap();
        let malformed:serde_json::Value = serde_json::from_str(&malformed).unwrap();
        assert!(malformed["result"]["read_file[0]"]["output"]["file"]["preview"].is_null());
        assert!(malformed["result"]["read_file[0]"]["output"]["file"]["content"].is_null());
    }

    #[test]
    fn mutation_receipt_does_not_echo_file_or_patch_bodies() {
        let output = serde_json::json!({
            "type": "update",
            "filePath": "src/large.rs",
            "oldString": "old-secret-body",
            "newString": "new-important-body".repeat(1_000),
            "originalFile": format!("prefix\n{}\nsuffix\n", "old-secret-body"),
            "structuredPatch": [{
                "oldLines": 40,
                "newLines": 55,
                "lines": vec!["+duplicated-line"; 5_000],
            }],
        })
        .to_string();
        let receipt = build_tool_receipt(
            "edit",
            &output,
            false,
            EvidenceRef::observed("tool", "mutation-raw"),
            100_000,
        );
        assert!(receipt.summary.contains("src/large.rs"));
        assert!(receipt.summary.contains("tool://mutation-raw"));
        assert!(!receipt.summary.contains("old-secret-body"));
        assert!(!receipt.summary.contains("duplicated-line"));
        assert!(receipt.receipt_tokens < 256);
        assert!(receipt.truncated);
    }

    #[test]
    fn already_compact_transaction_receipt_keeps_per_file_evidence() {
        let output = serde_json::json!({
            "type": "mutation_apply",
            "appliedCount": 2,
            "applied": [
                {"path": "src/a.rs", "sha256": "aaa", "replacementCount": 1},
                {"path": "src/b.rs", "sha256": "bbb", "replacementCount": 2},
            ],
        })
        .to_string();
        let receipt = build_tool_receipt(
            "edit",
            &output,
            false,
            EvidenceRef::observed("tool", "transaction-raw"),
            1_000,
        );
        assert!(receipt.summary.contains("src/a.rs"));
        assert!(receipt.summary.contains("src/b.rs"));
        assert!(receipt.summary.contains("aaa"));
        assert!(receipt.summary.contains("bbb"));
    }
}

/// The model-facing receipt for a JSON tool output is prefixed with a prose
/// summary, so the output is not deserializable any more. For a read tool that is
/// fine - the model only needs the reference and a hint. An agentic action
/// receipt is a protocol object (`status`, `duplicate`, `changed_refs`,
/// `actionable`, `revision`), and Runtime's own progress measure parses the very
/// text it hands the model, so the prefix makes a delegated Agent's applied
/// actions invisible to the measure. This test pins the mechanism.
#[test]
fn a_json_receipt_is_no_longer_deserializable_after_the_prose_prefix() {
    let receipt = serde_json::json!({
        "receipt_id": "receipt:program:test:claim",
        "action_id": "action:claim",
        "action": "task_claim",
        "program_id": "program:test",
        "revision": 4,
        "status": "applied",
        "duplicate": false,
        "changed_refs": ["task:t1"],
        "actionable": ["submit the deliverable"],
        "projection": serde_json::json!({"padding": "x".repeat(512)}),
        "error": null
    })
    .to_string();
    assert!(
        serde_json::from_str::<serde_json::Value>(&receipt).is_ok(),
        "the durable receipt is JSON"
    );

    let model_receipt = build_tool_receipt(
        "task_claim",
        &receipt,
        false,
        EvidenceRef::observed("tool", "raw-claim"),
        12_000,
    );
    assert!(
        serde_json::from_str::<serde_json::Value>(&model_receipt.summary).is_err(),
        "the model-facing summary is not JSON: {}",
        &model_receipt.summary[..60.min(model_receipt.summary.len())]
    );
}
