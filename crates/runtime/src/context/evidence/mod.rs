pub mod projection;
pub mod raw;

use harness_contract::context::EvidenceContentKind;
use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::context_ledger::estimate_text_tokens;

pub use harness_contract::context::EvidenceAuditProjection as AuditProjection;
pub use projection::audit_projection;

pub(crate) const READ_ACQUISITION_FIELD: &str = "read_acquisition_v1";
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
            let (body_sha256, body_bytes) = if let Some(content) = chunk["content"].as_str() {
                (read_sha256(content.as_bytes()), content.len() as u64)
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

#[must_use]
pub fn build_tool_receipt(
    tool_name: &str,
    output: &str,
    is_error: bool,
    evidence_ref: EvidenceRef,
    token_budget: u64,
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
    let (body, body_omitted) = summarize_body(
        tool_name,
        output,
        content_kind,
        body_budget,
        raw_tokens,
        raw_lines,
    );
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

fn summarize_body(
    tool_name: &str,
    output: &str,
    kind: EvidenceContentKind,
    token_budget: u64,
    raw_tokens: u64,
    raw_lines: usize,
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
        truncate_head_tail(tool_name, &normalized, token_budget, raw_tokens, raw_lines);
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
    // The recovery instruction must be an action the model can actually take.
    // A bare "retrieve by evidence ref" is not: there is no model-facing tool
    // that resolves a `tool://` URI, so the model concludes the evidence is
    // unreachable and repeats the same call. Name the concrete call instead.
    let recovery = if matches!(tool_name, "read_file" | "tool_orchestrate") {
        "re-read the needed window with read_file(path, offset=nextOffset, limit=...) instead of re-reading the same range"
    } else {
        "narrow the query (concrete path/pattern, smaller head_limit) or read the persisted output by range instead of repeating this call"
    };
    (format!(
        "{head_text}\n...[omitted {omitted_tokens} of {raw_tokens} tokens; {raw_lines} total lines; {recovery}]...\n{tail_text}"
    ), true)
}

#[cfg(test)]
mod tests {
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
