use super::*;
use crate::PreparedArtifactOrigin;
use harness_contract::context::ArtifactRef;

const CONTENT_SCHEMA: u32 = 2;
const INLINE_BYTES: usize = 16 * 1024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContentEnvelope {
    content_schema: u32,
    sha256: String,
    bytes: u64,
    body: ContentBody,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ContentBody {
    Inline { value: serde_json::Value },
    ArtifactRef { artifact: ArtifactRef },
}

pub(crate) fn history_checkpoint_metadata_reserve(encoded: &serde_json::Value) -> Result<(usize, usize), String> {
    let bytes = serde_json::to_vec(encoded).map_err(|error| error.to_string())?.len();
    Ok(if bytes > INLINE_BYTES { (8, 32 * 1024) } else { (0, 0) })
}

pub(crate) struct PreparedHistoryCheckpoint {
    backend: Arc<dyn RuntimeEventStoreBackend>,
    projection_id: String,
    source_cursor: u64,
    expected_revision: u64,
    payload: serde_json::Value,
    source_fence: RuntimeSourceMutationFence,
    guard: Option<crate::artifact::PreparedCheckpointGuard>,
}

impl PreparedHistoryCheckpoint {
    pub(crate) fn payload(&self) -> &serde_json::Value { &self.payload }
}

impl RuntimeEventStore {
    /// Exact versions of the derived Program cache. Durable Program events
    /// retain their original schema; unknown cache owners never authenticate.
    pub fn program_read_model_checkpoint_schema(projection_id: &str) -> Option<u64> {
        if projection_id.starts_with("runtime:agentic-read-model:v1:") {
            Some(2)
        } else if projection_id.starts_with("runtime:agentic-read-model:v2:") {
            Some(3)
        } else {
            None
        }
    }

    pub(crate) fn prepare_history_checkpoint(
        &self, projection_id: &str, source_cursor: u64, expected_revision: u64,
        identity: &str, mut payload: serde_json::Value, encoded: &serde_json::Value,
    ) -> RuntimeEventStoreResult<PreparedHistoryCheckpoint> {
        if !projection_id.starts_with("projection-history:") || payload["schema"] != 6 {
            return Err(RuntimeEventStoreError::InvalidTransaction("unsupported prepared history owner".into()));
        }
        let scope = payload["scope"].as_str().ok_or_else(|| RuntimeEventStoreError::InvalidTransaction(
            "prepared history scope missing".into()))?.to_owned();
        let source_fence: RuntimeSourceMutationFence = serde_json::from_value(payload["source_fence"].clone())?;
        let origin = PreparedArtifactOrigin::CheckpointBody { projection_id: projection_id.into(), source_cursor };
        payload["body"] = self.prepare_content(&scope, identity, encoded, &origin)
            .map_err(RuntimeEventStoreError::InvalidTransaction)?;
        self.validate_checkpoint_content_with_body(projection_id, source_cursor, &payload, Some(encoded))?;
        let envelope: ContentEnvelope = serde_json::from_value(payload["body"].clone())?;
        let guard = match envelope.body {
            ContentBody::Inline { .. } => None,
            ContentBody::ArtifactRef { artifact } => Some(self.artifacts.get()
                .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("MissingContentStore".into()))?
                .checkpoint_guard_after_write(&artifact, &scope, &origin)
                .map_err(|error| {
                    RuntimeEventStoreError::InvalidTransaction(format!(
                        "{}: prepared history checkpoint artifact {} (projection {})",
                        error,
                        artifact.selector,
                        projection_id
                    ))
                })?),
        };
        Ok(PreparedHistoryCheckpoint {
            backend: self.backend.clone(), projection_id: projection_id.into(), source_cursor,
            expected_revision, payload, source_fence, guard,
        })
    }

    pub(crate) fn publish_prepared_history_checkpoint(
        &self, proof: PreparedHistoryCheckpoint, source_fence: &RuntimeSourceMutationFence,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        if !Arc::ptr_eq(&self.backend, &proof.backend) || &proof.source_fence != source_fence {
            return Err(RuntimeEventStoreError::InvalidTransaction("prepared history store or source mismatch".into()));
        }
        let scope = proof.payload["scope"].as_str().ok_or_else(|| RuntimeEventStoreError::InvalidTransaction(
            "prepared history scope missing".into()))?;
        let origin = PreparedArtifactOrigin::CheckpointBody {
            projection_id: proof.projection_id.clone(), source_cursor: proof.source_cursor,
        };
        let envelope: ContentEnvelope = serde_json::from_value(proof.payload["body"].clone())?;
        match (proof.guard, envelope.body) {
            (Some(guard), ContentBody::ArtifactRef { artifact }) => self.artifacts.get()
                .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction("MissingContentStore".into()))?
                .validate_checkpoint_guard(guard, &artifact, scope, &origin)
                .map_err(|error| {
                    RuntimeEventStoreError::InvalidTransaction(format!(
                        "{}: prepared history checkpoint artifact {} (projection {})",
                        error,
                        artifact.selector,
                        proof.projection_id
                    ))
                })?,
            (None, ContentBody::Inline { .. }) => (),
            _ => return Err(RuntimeEventStoreError::InvalidTransaction("prepared history witness mismatch".into())),
        }
        self.backend.compare_and_put_projection_checkpoint_fenced(
            &proof.projection_id, proof.source_cursor, proof.expected_revision, &proof.payload, 0, source_fence,
        )
    }

    pub(crate) fn prepared_content_wire_bytes(
        &self,
        scope: &str,
        value: &serde_json::Value,
    ) -> Result<usize, String> {
        let bytes = serde_json::to_vec(value).map_err(|_| "content encoding failed")?;
        let sha256 = format!("sha256:{:x}", Sha256::digest(&bytes));
        let body = if bytes.len() <= INLINE_BYTES {
            ContentBody::Inline { value: value.clone() }
        } else {
            ContentBody::ArtifactRef { artifact: ArtifactRef {
                selector: format!("artifact://art_{}", "0".repeat(64)),
                sha256: sha256.clone(),
                bytes: bytes.len() as u64,
                media_type: "application/json".into(),
                durability: harness_contract::context::EvidenceDurability::Durable,
                visibility_scope: scope.into(),
            }}
        };
        serde_json::to_vec(&ContentEnvelope {
            content_schema: CONTENT_SCHEMA, sha256, bytes: bytes.len() as u64, body,
        }).map(|encoded| encoded.len()).map_err(|_| "content extent encoding failed".into())
    }

    pub(crate) fn validate_terminal_publication(
        &self,
        terminal: &SessionTerminalInput,
        request: &AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<()> {
        self.validate_terminal_publication_metadata(terminal, request, None)
    }

    fn validate_terminal_publication_metadata(
        &self,
        terminal: &SessionTerminalInput,
        request: &AppendTransactionRequest,
        authored_metadata: Option<&serde_json::Value>,
    ) -> RuntimeEventStoreResult<()> {
        let mut canonical = request
            .events
            .iter()
            .filter(|input| input.event.kind == "runtime.session.terminal_requested");
        if let Some(input) = canonical.next() {
            if canonical.next().is_some()
                || input.event.scope != RuntimeEventScope::SessionInput
                || input.schema_version != 1
                || input
                    .idempotency_key
                    .as_deref()
                    .is_none_or(|key| key.trim().is_empty())
                || terminal.execution_id.as_deref() == Some(input.event.stream_id.as_str())
            {
                return Err(RuntimeEventStoreError::InvalidTransaction(
                    "invalid canonical terminal identity".into(),
                ));
            }
            let mut payload = input.event.payload.clone();
            if let Some(expected) = authored_metadata {
                if !expected.is_object()
                    || payload
                        .as_object_mut()
                        .and_then(|object| object.remove("objective_verdict"))
                        .as_ref()
                        != Some(expected)
                {
                    return Err(RuntimeEventStoreError::TransactionConflict {
                        transaction_id: request.transaction_id.clone(),
                    });
                }
            }
            if serde_json::to_value(terminal)? != payload {
                return Err(RuntimeEventStoreError::TransactionConflict {
                    transaction_id: request.transaction_id.clone(),
                });
            }
            return Ok(());
        }
        if authored_metadata.is_some() {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "authored terminal metadata requires canonical event".into(),
            ));
        }
        let artifact = decode_session_terminal_artifact_ref(&terminal.payload_ref)
            .map_err(RuntimeEventStoreError::InvalidTransaction)?;
        let store = self.artifacts.get().ok_or_else(|| {
            RuntimeEventStoreError::InvalidTransaction("MissingContentStore".into())
        })?;
        let resolved = store
            .resolve(&artifact.selector)
            .map_err(|error| {
                RuntimeEventStoreError::InvalidTransaction(format!(
                    "{}: session terminal artifact {} (session {})",
                    error,
                    artifact.selector,
                    terminal.session_id
                ))
            })?;
        if resolved != artifact
            || artifact.visibility_scope != format!("session:{}", terminal.session_id)
            || artifact.media_type != "application/vnd.cowd.session-terminal+json"
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "terminal requires a matching live body or canonical prepared origin".into(),
            ));
        }
        let bytes = store
            .read_blocking(&artifact, &artifact.visibility_scope, None)
            .map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?;
        let payload: serde_json::Value = serde_json::from_slice(&bytes)?;
        if payload["schema_version"] != crate::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION
            || !payload["text"].is_string()
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "terminal payload schema mismatch".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn append_transaction_with_authored_terminal_metadata(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
        authored_metadata: Option<&serde_json::Value>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let mut request = request;
        self.validate_content_publication(&mut request)?;
        self.validate_terminal_publication_metadata(&terminal, &request, authored_metadata)?;
        let stream_ids = request
            .expected_streams
            .iter()
            .map(|stream| stream.stream_id.clone())
            .collect::<Vec<_>>();
        let receipt = self.with_stream_locks(&stream_ids, || {
            self.backend
                .append_transaction_with_terminal(request, terminal)
        })?;
        self.publish_commit(receipt.commit_cursor);
        Ok(receipt)
    }
    pub fn quiesced_content_roots(&self) -> QuiescedContentRoots {
        QuiescedContentRoots {
            backend: Arc::clone(&self.backend),
        }
    }

    pub fn transaction_stream_revision_exact(
        &self,
        transaction_id: &str,
        stream_id: &str,
    ) -> RuntimeEventStoreResult<Option<CommittedStreamRevision>> {
        self.backend
            .transaction_stream_revision_exact(transaction_id, stream_id)
    }

    pub fn prepared_slot_root(
        slot: &serde_json::Value,
        artifact: &ArtifactRef,
    ) -> PreparedContentRoot {
        if slot["content_schema"] != CONTENT_SCHEMA
            || !slot["sha256"]
                .as_str()
                .and_then(|digest| digest.strip_prefix("sha256:"))
                .is_some_and(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
            || !slot["bytes"].is_u64()
        {
            return PreparedContentRoot::Blocked;
        }
        match slot["body"]["kind"].as_str() {
            Some("inline")
                if slot["bytes"]
                    .as_u64()
                    .is_some_and(|bytes| bytes <= INLINE_BYTES as u64) =>
            {
                PreparedContentRoot::Absent
            }
            Some("artifact_ref") => {
                let Ok(reference) =
                    serde_json::from_value::<ArtifactRef>(slot["body"]["artifact"].clone())
                else {
                    return PreparedContentRoot::Blocked;
                };
                if reference.sha256 != slot["sha256"]
                    || reference.bytes != slot["bytes"]
                    || !reference.is_durable()
                {
                    return PreparedContentRoot::Blocked;
                }
                if &reference == artifact {
                    PreparedContentRoot::Canonical
                } else {
                    PreparedContentRoot::Absent
                }
            }
            _ => PreparedContentRoot::Blocked,
        }
    }

    pub fn process_closure_artifact(slot: &serde_json::Value) -> Result<ArtifactRef, String> {
        let envelope: ContentEnvelope = serde_json::from_value(slot.clone())
            .map_err(|_| "invalid process closure envelope".to_string())?;
        let ContentBody::ArtifactRef { artifact } = envelope.body else {
            return Err("process closure must be an artifact reference".into());
        };
        if envelope.content_schema != CONTENT_SCHEMA
            || envelope.bytes > 32768
            || envelope.sha256 != artifact.sha256
            || envelope.bytes != artifact.bytes
            || !artifact.is_durable()
            || artifact.media_type != "application/vnd.cowd.process-closure+json"
            || !artifact.visibility_scope.starts_with("session:")
            || artifact.visibility_scope == "session:"
            || !artifact
                .sha256
                .strip_prefix("sha256:")
                .is_some_and(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
        {
            return Err("process closure envelope identity mismatch".into());
        }
        Ok(artifact)
    }

    pub fn process_closure_index_identity(
        payload: &serde_json::Value,
        refs: &[RuntimeEventRef],
    ) -> Result<(String, String), String> {
        let call = payload["idempotency_key"]
            .as_str()
            .filter(|key| !key.is_empty() && key.len() <= 1024)
            .ok_or("process closure call key missing")?;
        let root = &payload["root_effect_owner"];
        let (graph, node, attempt) = if root.is_null() {
            let mut attempts = refs
                .iter()
                .filter(|reference| reference.kind == "agent_attempt");
            let attempt = attempts
                .next()
                .and_then(|reference| reference.id.parse::<u32>().ok());
            if attempts.next().is_some() {
                return Err("ambiguous process closure attempt".into());
            }
            (
                payload["replay"]["parent_execution"]["execution_id"].as_str(),
                payload["replay"]["parent_execution"]["node_id"].as_str(),
                attempt,
            )
        } else {
            (
                root["binding"]["root_execution_id"].as_str(),
                root["binding"]["node_id"].as_str(),
                root["attempt"]
                    .as_u64()
                    .and_then(|attempt| u32::try_from(attempt).ok()),
            )
        };
        let (Some(graph), Some(node), Some(attempt)) = (graph, node, attempt) else {
            return Err("process closure original owner missing".into());
        };
        if graph.is_empty()
            || node.is_empty()
            || graph.len() > 1024
            || node.len() > 1024
            || attempt == 0
        {
            return Err("invalid process closure original owner".into());
        }
        Ok((
            format!("execution-agent-receipts:{graph}:{node}:{attempt}"),
            format!("agent-tool-receipt:{call}"),
        ))
    }

    pub fn validate_process_closure_pair(
        primary: &RuntimeEventInput,
        primary_key: &str,
        primary_schema: u32,
        index: &RuntimeEventInput,
        index_key: &str,
        index_schema: u32,
    ) -> Result<ArtifactRef, String> {
        let artifact = Self::process_closure_artifact(&primary.payload["process_closure"])?;
        let (index_stream, expected_index_key) =
            Self::process_closure_index_identity(&primary.payload, &primary.refs)?;
        let call = primary.payload["idempotency_key"]
            .as_str()
            .ok_or("process closure key missing")?;
        let session = primary.payload["replay"]["session_id"]
            .as_str()
            .ok_or("process closure session missing")?;
        // The primary is the receipt of the effect's own key family, and its
        // key names that family exactly.
        let read_family = primary.kind == "execution.read.receipt";
        let expected_primary_kind = if read_family {
            "execution.read.receipt"
        } else {
            "execution.effect.receipt"
        };
        let expected_primary_key = if read_family {
            format!("{call}:read-receipt")
        } else {
            format!("{call}:receipt")
        };
        if primary.scope != RuntimeEventScope::ExecutionNode
            || primary_schema != 1
            || primary.kind != expected_primary_kind
            || primary.actor.as_deref() != Some("governed_tool")
            || primary.status.as_deref() != Some("completed")
            || primary.stream_id != format!("execution-effect:{call}")
            || primary_key != expected_primary_key
            || artifact.visibility_scope != format!("session:{session}")
            || primary.payload["process_admission_ref"] != format!("{call}:process-admission")
            || index.scope != RuntimeEventScope::ExecutionNode
            || (index_schema != 1 && (index_schema != 2
                || crate::ExecutionCommitService::receipt_payload_observation_sequence(
                    index_schema, &index.payload,
                ).is_none()))
            || index.kind != "execution.agent_tool.receipt"
            || index.actor.as_deref() != Some("governed_tool")
            || index.status.as_deref() != Some("completed")
            || index.stream_id != index_stream
            || index_key != expected_index_key
        {
            return Err("process closure primary/index source mismatch".into());
        }
        for field in [
            "idempotency_key",
            "process_closure",
            "process_admission_ref",
            "root_effect_owner",
            "root_effect_context",
            "resolved_effect_scope",
            "effect",
            "outcome",
        ] {
            if primary.payload[field] != index.payload[field] {
                return Err(format!("process closure primary/index {field} mismatch"));
            }
        }
        let source_refs = |event: &RuntimeEventInput| {
            event
                .refs
                .iter()
                .filter(|reference| reference.kind != "artifact")
                .map(|reference| (reference.kind.clone(), reference.id.clone()))
                .collect::<std::collections::BTreeSet<_>>()
        };
        if source_refs(primary) != source_refs(index) {
            return Err("process closure primary/index references mismatch".into());
        }
        Ok(artifact)
    }

    pub fn process_closure_root(
        primary: Option<&DurableRuntimeEvent>,
        index: Option<&DurableRuntimeEvent>,
        artifact: &ArtifactRef,
    ) -> PreparedContentRoot {
        let Some(primary) = primary else {
            return PreparedContentRoot::Absent;
        };
        let Some(slot) = primary
            .payload
            .get("process_closure")
            .filter(|slot| !slot.is_null())
        else {
            return PreparedContentRoot::Absent;
        };
        let Ok(reference) = Self::process_closure_artifact(slot) else {
            return PreparedContentRoot::Blocked;
        };
        if &reference != artifact {
            return PreparedContentRoot::Absent;
        }
        let Some(index) = index else {
            return PreparedContentRoot::Blocked;
        };
        let as_input = |event: &DurableRuntimeEvent| RuntimeEventInput {
            stream_id: event.stream_id.clone(),
            scope: event.scope,
            kind: event.kind.clone(),
            status: event.status.clone(),
            actor: event.actor.clone(),
            refs: event.refs.clone(),
            payload: event.payload.clone(),
        };
        if primary.transaction_id.is_empty()
            || primary.commit_cursor == 0
            || primary.transaction_id != index.transaction_id
            || primary.commit_cursor != index.commit_cursor
            || primary.transaction_index >= index.transaction_index
            || Self::validate_process_closure_pair(
                &as_input(primary),
                primary.idempotency_key.as_deref().unwrap_or(""),
                primary.schema_version,
                &as_input(index),
                index.idempotency_key.as_deref().unwrap_or(""),
                index.schema_version,
            )
            .is_err()
        {
            return PreparedContentRoot::Blocked;
        }
        PreparedContentRoot::Canonical
    }

    fn process_closure_publications(
        &self,
        request: &AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<std::collections::BTreeMap<usize, ArtifactRef>> {
        let mut own = std::collections::BTreeMap::new();
        for (position, input) in request.events.iter().enumerate() {
            // The closure of a process-class effect belongs to the primary
            // receipt of its own key family: the mutation receipt or the read
            // receipt. Both are published by the commit service in the same
            // transaction as their per-attempt index.
            if (input.event.kind != "execution.effect.receipt"
                && input.event.kind != "execution.read.receipt")
                || !input
                    .event
                    .payload
                    .get("process_closure")
                    .is_some_and(|slot| !slot.is_null())
            {
                continue;
            }
            let key = input.idempotency_key.as_deref().ok_or_else(|| {
                RuntimeEventStoreError::InvalidTransaction(
                    "process closure primary key missing".into(),
                )
            })?;
            let (stream, index_key) =
                Self::process_closure_index_identity(&input.event.payload, &input.event.refs)
                    .map_err(RuntimeEventStoreError::InvalidTransaction)?;
            let mut indexes = request.events.iter().enumerate().filter(|(_, candidate)| {
                candidate.event.stream_id == stream
                    && candidate.idempotency_key.as_deref() == Some(index_key.as_str())
            });
            let (index_position, index) = indexes.next().ok_or_else(|| {
                RuntimeEventStoreError::InvalidTransaction(
                    "process closure same-transaction index missing".into(),
                )
            })?;
            if indexes.next().is_some()
                || index_position <= position
                || own.contains_key(&index_position)
            {
                return Err(RuntimeEventStoreError::InvalidTransaction(
                    "ambiguous process closure index".into(),
                ));
            }
            let artifact = Self::validate_process_closure_pair(
                &input.event,
                key,
                input.schema_version,
                &index.event,
                &index_key,
                index.schema_version,
            )
            .map_err(RuntimeEventStoreError::InvalidTransaction)?;
            let origin = PreparedArtifactOrigin::ToolEffectClosure {
                stream_id: input.event.stream_id.clone(),
                idempotency_key: key.into(),
            };
            let store = self.artifacts.get().ok_or_else(|| {
                RuntimeEventStoreError::InvalidTransaction("MissingContentStore".into())
            })?;
            store
                .read_process_closure_blocking(&artifact, &artifact.visibility_scope, &origin)
                .map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?;
            own.insert(position, artifact.clone());
            own.insert(index_position, artifact);
        }
        for (position, input) in request.events.iter().enumerate() {
            if input
                .event
                .payload
                .get("process_closure")
                .is_some_and(|slot| !slot.is_null())
                && !own.contains_key(&position)
            {
                return Err(RuntimeEventStoreError::InvalidTransaction(
                    "unowned process closure slot".into(),
                ));
            }
        }
        Ok(own)
    }

    pub(crate) fn validate_content_publication(
        &self,
        request: &mut AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<()> {
        let host_transcripts = request
            .events
            .iter()
            .enumerate()
            .filter(|(_, input)| input.event.kind == "execution.host.transcript")
            .map(|(position, _)| {
                crate::ExecutionCommitService::validate_host_transcript_publication(
                    self, request, position,
                )
                .map(|artifact| (position, artifact))
            })
            .collect::<RuntimeEventStoreResult<std::collections::BTreeMap<_, _>>>()?;
        Self::validate_current_content_inputs(request)?;
        let process_closures = self.process_closure_publications(request)?;
        let expected = request
            .expected_streams
            .iter()
            .map(|stream| (stream.stream_id.as_str(), stream.expected_revision))
            .collect::<std::collections::BTreeMap<_, _>>();
        for (position, input) in request.events.iter_mut().enumerate() {
            let own = if input.event.kind == "execution.host.transcript" {
                host_transcripts.get(&position).cloned()
            } else if input.event.scope == RuntimeEventScope::ExecutionGraph
                || input.event.kind == "goal.observation"
            {
                let origin = PreparedArtifactOrigin::EventBody {
                    stream_id: input.event.stream_id.clone(),
                    idempotency_key: input.idempotency_key.clone().ok_or_else(|| {
                        RuntimeEventStoreError::InvalidTransaction(
                            "content idempotency missing".into(),
                        )
                    })?,
                    kind: input.event.kind.clone(),
                };
                let (scope, envelope) = if input.event.kind == "goal.observation" {
                    (
                        format!(
                            "session:{}",
                            input.event.payload["session_id"].as_str().ok_or_else(|| {
                                RuntimeEventStoreError::InvalidTransaction(
                                    "observation session missing".into(),
                                )
                            })?
                        ),
                        &input.event.payload["observation_body"],
                    )
                } else {
                    (
                        format!(
                            "session:{}",
                            input
                                .event
                                .activity_binding()
                                .ok_or_else(|| RuntimeEventStoreError::InvalidTransaction(
                                    "graph binding missing".into()
                                ))?
                                .session_id
                        ),
                        &input.event.payload,
                    )
                };
                let value = self
                    .resolve_content(&scope, envelope, &origin)
                    .map_err(RuntimeEventStoreError::InvalidTransaction)?;
                if input.event.kind == "goal.observation" {
                    if input.event.payload["goal_source"]["sequence"].as_u64()
                        != expected.get(input.event.stream_id.as_str()).copied()
                    {
                        return Err(RuntimeEventStoreError::InvalidTransaction(
                            "observation source does not match transaction read-set".into(),
                        ));
                    }
                    if value
                        .pointer("/identity/session_id")
                        .and_then(serde_json::Value::as_str)
                        != scope.strip_prefix("session:")
                        || observation_classifier(&value)
                            .map_err(RuntimeEventStoreError::InvalidTransaction)?
                            != input.event.payload["classifier"]
                    {
                        return Err(RuntimeEventStoreError::InvalidTransaction(
                            "observation body classifier or identity mismatch".into(),
                        ));
                    }
                    let observation: harness_contract::goal::RuntimeObservation =
                        serde_json::from_value(value.clone())?;
                    crate::execution_core::goal::validate_observation(&observation)
                        .map_err(RuntimeEventStoreError::InvalidTransaction)?;
                    if input.event.scope != RuntimeEventScope::Goal
                        || input.event.stream_id != format!("goal:{}", observation.goal_id())
                    {
                        return Err(RuntimeEventStoreError::InvalidTransaction(
                            "observation body crosses Goal target".into(),
                        ));
                    }
                    self.validate_observation_typed_artifacts(&observation)?;
                } else {
                    let graph: crate::execution_core::graph::ExecutionGraphEvent =
                        serde_json::from_value(value)?;
                    if graph.kind() != input.event.kind {
                        return Err(RuntimeEventStoreError::InvalidTransaction(
                            "graph body kind mismatch".into(),
                        ));
                    }
                    self.validate_graph_typed_artifacts(&graph)?;
                }
                envelope_artifact(envelope).map_err(RuntimeEventStoreError::InvalidTransaction)?
            } else if input.event.kind == "runtime.session.terminal_requested" {
                let terminal: SessionTerminalInput =
                    serde_json::from_value(input.event.payload.clone())?;
                let artifact = decode_session_terminal_artifact_ref(&terminal.payload_ref)
                    .map_err(RuntimeEventStoreError::InvalidTransaction)?;
                let origin = PreparedArtifactOrigin::TerminalBody {
                    terminal_id: terminal.terminal_id.clone(),
                    session_id: terminal.session_id.clone(),
                    execution_id: terminal.execution_id.clone().ok_or_else(|| {
                        RuntimeEventStoreError::InvalidTransaction(
                            "terminal execution missing".into(),
                        )
                    })?,
                    turn_id: terminal.turn_id.clone().ok_or_else(|| {
                        RuntimeEventStoreError::InvalidTransaction("terminal turn missing".into())
                    })?,
                    event_stream: input.event.stream_id.clone(),
                    event_key: input.idempotency_key.clone().ok_or_else(|| {
                        RuntimeEventStoreError::InvalidTransaction("terminal key missing".into())
                    })?,
                };
                if artifact.media_type != "application/vnd.cowd.session-terminal+json"
                    || artifact.visibility_scope != format!("session:{}", terminal.session_id)
                    || !artifact.is_durable()
                {
                    return Err(RuntimeEventStoreError::InvalidTransaction(
                        "terminal artifact identity mismatch".into(),
                    ));
                }
                let store = self.artifacts.get().ok_or_else(|| {
                    RuntimeEventStoreError::InvalidTransaction("MissingContentStore".into())
                })?;
                let bytes = store
                    .read_prepared_blocking(&artifact, &artifact.visibility_scope, &origin)
                    .map_err(|error| {
                        RuntimeEventStoreError::InvalidTransaction(error.to_string())
                    })?;
                let payload: serde_json::Value = serde_json::from_slice(&bytes)?;
                if payload["schema_version"] != crate::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION
                    || !payload["text"].is_string()
                {
                    return Err(RuntimeEventStoreError::InvalidTransaction(
                        "terminal payload schema mismatch".into(),
                    ));
                }
                Some(artifact)
            } else {
                process_closures.get(&position).cloned()
            };
            for reference in &input.event.refs {
                if !reference.id.starts_with("artifact://")
                    || own.as_ref().is_some_and(|artifact| {
                        artifact.selector == reference.id && reference.kind == "artifact"
                    })
                {
                    continue;
                }
                self.artifacts
                    .get()
                    .ok_or_else(|| {
                        RuntimeEventStoreError::InvalidTransaction("MissingContentStore".into())
                    })?
                    .resolve(&reference.id)
                    .map_err(|error| {
                        RuntimeEventStoreError::InvalidTransaction(format!(
                            "{}: event {} on stream {} references artifact {}",
                            error,
                            input.event.kind,
                            input.event.stream_id,
                            reference.id
                        ))
                    })?;
            }
            if let Some(artifact) = own {
                let reference = RuntimeEventRef {
                    kind: "artifact".into(),
                    id: artifact.selector,
                };
                if !input.event.refs.contains(&reference) {
                    input.event.refs.push(reference);
                }
            }
        }
        Ok(())
    }

    fn validate_live_typed_selector(
        &self,
        selector: &str,
        field: &str,
    ) -> RuntimeEventStoreResult<Option<ArtifactRef>> {
        if !selector.starts_with("artifact://") {
            return Ok(None);
        }
        self.artifacts
            .get()
            .ok_or_else(|| {
                RuntimeEventStoreError::InvalidTransaction("MissingContentStore".into())
            })?
            .resolve(selector)
            .map(Some)
            .map_err(|error| {
                RuntimeEventStoreError::InvalidTransaction(format!(
                    "{}: typed selector {} in {}",
                    error, selector, field
                ))
            })
    }

    fn validate_observation_typed_artifacts(
        &self,
        observation: &harness_contract::goal::RuntimeObservation,
    ) -> RuntimeEventStoreResult<()> {
        for selector in observation
            .evidence_refs
            .iter()
            .chain(&observation.evidence_delta.added)
            .chain(&observation.evidence_delta.invalidated)
            .chain(&observation.information_gain.distinguishing_evidence_refs)
        {
            self.validate_live_typed_selector(selector, "observation.evidence_refs")?;
        }
        for delta in &observation.criterion_deltas {
            for selector in &delta.evidence_refs {
                self.validate_live_typed_selector(selector, "observation.criterion_deltas.evidence_refs")?;
            }
        }
        for delta in &observation.conflict_deltas {
            for selector in &delta.evidence_refs {
                self.validate_live_typed_selector(
                    selector,
                    "observation.conflict_deltas.evidence_refs",
                )?;
            }
        }
        for delta in &observation.unknown_deltas {
            for selector in &delta.evidence_refs {
                self.validate_live_typed_selector(
                    selector,
                    "observation.unknown_deltas.evidence_refs",
                )?;
            }
        }
        Ok(())
    }

    fn validate_graph_typed_artifacts(
        &self,
        event: &crate::execution_core::graph::ExecutionGraphEvent,
    ) -> RuntimeEventStoreResult<()> {
        use crate::execution_core::graph::ExecutionGraphEvent;
        let validate_result = |result: &harness_contract::execution_graph::ExecutionNodeResult| -> RuntimeEventStoreResult<()> {
            if let Some(selector) = &result.result_ref {
                self.validate_live_typed_selector(selector, "observation.result_refs")?;
            }
            for evidence in result.evidence_refs.iter().chain(result.failure.iter().flat_map(|failure| &failure.evidence_refs)) {
                if let Some(artifact) =
                    self.validate_live_typed_selector(&evidence.retrieval_selector, "observation.observed_evidence.retrieval_selector")?
                {
                    if !evidence.is_durable() || artifact.sha256 != evidence.sha256 || artifact.bytes != evidence.bytes
                        || artifact.media_type != evidence.media_type || artifact.visibility_scope != evidence.visibility_scope {
                        return Err(RuntimeEventStoreError::InvalidTransaction("typed Graph evidence identity mismatch".into()));
                    }
                }
            }
            Ok(())
        };
        let delta = match event {
            ExecutionGraphEvent::Planned { graph }
            | ExecutionGraphEvent::Checkpoint { graph, .. } => {
                for node in &graph.nodes {
                    self.validate_live_typed_selector(&node.payload_ref, "observation.node.payload_ref")?;
                }
                for result in graph.node_results.values() {
                    validate_result(result)?;
                }
                return Ok(());
            }
            ExecutionGraphEvent::NodeTransitioned { result, delta, .. } => {
                if let Some(result) = result {
                    validate_result(result)?;
                }
                delta
            }
            ExecutionGraphEvent::NodeTransitionedAndReplanned { result, delta, .. } => {
                validate_result(result)?;
                delta
            }
            ExecutionGraphEvent::NodesStarted { delta, .. }
            | ExecutionGraphEvent::NodesTransitioned { delta, .. }
            | ExecutionGraphEvent::CommandApplied { delta, .. }
            | ExecutionGraphEvent::Replanned { delta, .. }
            | ExecutionGraphEvent::Recovered { delta, .. } => delta,
        };
        for node in &delta.added_nodes {
            self.validate_live_typed_selector(&node.payload_ref, "observation.node.payload_ref")?;
        }
        for result in delta.node_result_updates.values() {
            validate_result(result)?;
        }
        Ok(())
    }

    pub(crate) fn validate_checkpoint_content(
        &self,
        projection_id: &str,
        source_cursor: u64,
        payload: &serde_json::Value,
    ) -> RuntimeEventStoreResult<()> {
        self.validate_checkpoint_content_with_body(projection_id, source_cursor, payload, None)
    }

    fn validate_checkpoint_content_with_body(
        &self, projection_id: &str, source_cursor: u64,
        payload: &serde_json::Value, prepared_body: Option<&serde_json::Value>,
    ) -> RuntimeEventStoreResult<()> {
        let goal = projection_id.strip_prefix("goal-projection:");
        let program_schema = Self::program_read_model_checkpoint_schema(projection_id);
        let program = program_schema.is_some();
        let history = projection_id.starts_with("projection-history:");
        if goal.is_none() && !program && !history {
            if payload["body"]["content_schema"] == CONTENT_SCHEMA {
                return Err(RuntimeEventStoreError::InvalidTransaction(
                    "unsupported content checkpoint owner".into(),
                ));
            }
            return Ok(());
        }
        let scope = if program {
            payload["session_id"]
                .as_str()
                .map(|session| format!("session:{session}"))
        } else {
            payload["scope"].as_str().map(str::to_string)
        }
        .ok_or_else(|| {
            RuntimeEventStoreError::InvalidTransaction("checkpoint scope missing".into())
        })?;
        let schema = if program {
            payload["schema_version"].as_u64()
        } else {
            payload["schema"].as_u64()
        };
        let valid_schema = if let Some(expected) = program_schema {
            schema == Some(expected)
        } else if history {
            matches!(schema, Some(3 | 4 | 5 | 6))
        } else {
            schema == Some(u64::from(CONTENT_SCHEMA))
        };
        if !scope.starts_with("session:") || !valid_schema {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "checkpoint schema mismatch".into(),
            ));
        }
        let origin = PreparedArtifactOrigin::CheckpointBody {
            projection_id: projection_id.into(),
            source_cursor,
        };
        let body = match prepared_body {
            Some(body) => body.clone(),
            None => self.resolve_content(&scope, &payload["body"], &origin)
                .map_err(RuntimeEventStoreError::InvalidTransaction)?,
        };
        let valid = if let Some(goal) = goal {
            body.pointer("/goal/id").and_then(serde_json::Value::as_str) == Some(goal)
                && body["stream_revision"] == source_cursor
                && body
                    .pointer("/goal/session_id")
                    .and_then(serde_json::Value::as_str)
                    == scope.strip_prefix("session:")
        } else if program {
            body["revision"] == source_cursor
                && body["session_id"] == payload["session_id"]
                && body["program_id"].as_str().is_some_and(|identity| {
                    projection_id
                        == format!(
                            "runtime:agentic-read-model:v{}:{:x}",
                            if program_schema == Some(3) { 2 } else { 1 },
                            Sha256::digest(identity.as_bytes())
                        )
                })
        } else {
            let state = if matches!(payload["schema"].as_u64(), Some(4 | 5 | 6)) {
                &body["state"]
            } else {
                &body
            };
            if payload["schema"] == 6 {
                payload["target_cursor"] == source_cursor
                    && payload["verified_through"].as_u64().is_some_and(|verified| verified <= source_cursor)
                    && state["through"] == payload["verified_through"]
                    && payload["incomplete"].as_bool() == Some(state["incomplete"].as_bool().unwrap_or(false))
                    && state["identity"].is_string()
                    && payload["source_fence"].is_object()
                    && state["source_fence"] == payload["source_fence"]
            } else {
                state["through"] == source_cursor && state["identity"].is_string()
            }
        };
        if !valid {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "checkpoint body owner or source mismatch".into(),
            ));
        }
        if goal.is_some() {
            let projection: crate::execution_core::GoalProjection = serde_json::from_value(body)?;
            for selector in &projection.goal.evidence_refs {
                self.validate_live_typed_selector(selector, "goal.evidence_refs")?;
            }
            for criterion in &projection.goal.criteria {
                for selector in criterion
                    .source_refs
                    .iter()
                    .chain(&criterion.required_evidence)
                    .chain(criterion.statement_ref.iter())
                {
                    self.validate_live_typed_selector(selector, "goal.criterion.evidence")?;
                }
            }
            for observation in &projection.observations {
                self.validate_observation_typed_artifacts(observation)?;
            }
        } else if program {
            let projection: crate::AgenticProgramProjection = serde_json::from_value(body)?;
            for artifact in projection.artifacts.values() {
                self.validate_live_typed_selector(&artifact.content_ref, "artifact.content_ref")?;
            }
            if let Some(selector) = &projection.final_artifact_ref {
                self.validate_live_typed_selector(selector, "program.final_artifact_ref")?;
            }
        }
        Ok(())
    }
    pub fn validate_current_content_inputs(
        request: &AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<()> {
        for input in &request.events {
            if input.event.scope != RuntimeEventScope::ExecutionGraph
                && input.event.kind != "goal.observation"
            {
                continue;
            }
            let payload = &input.event.payload;
            let envelope = if input.event.kind == "goal.observation" {
                if payload["observation_schema"] != 2
                    || payload.get("goal").is_some()
                    || payload["goal_source"]["stream_id"] != input.event.stream_id
                {
                    return Err(RuntimeEventStoreError::InvalidTransaction(
                        "unsupported observation schema".into(),
                    ));
                }
                &payload["observation_body"]
            } else {
                payload
            };
            let mut envelope = envelope.clone();
            if let Some(object) = envelope.as_object_mut() {
                object.remove("_runtime_activity_binding");
            }
            let envelope: ContentEnvelope = serde_json::from_value(envelope).map_err(|_| {
                RuntimeEventStoreError::InvalidTransaction("unsupported content envelope".into())
            })?;
            if input.schema_version != CONTENT_SCHEMA || envelope.content_schema != CONTENT_SCHEMA {
                return Err(RuntimeEventStoreError::InvalidTransaction(
                    "unsupported content schema".into(),
                ));
            }
            if !envelope
                .sha256
                .strip_prefix("sha256:")
                .is_some_and(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
            {
                return Err(RuntimeEventStoreError::InvalidTransaction(
                    "invalid current content digest".into(),
                ));
            }
            let session = if input.event.kind == "goal.observation" {
                payload["session_id"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            } else {
                input
                    .event
                    .activity_binding()
                    .filter(|binding| binding.validate().is_ok())
                    .map(|binding| binding.session_id)
            }
            .ok_or_else(|| {
                RuntimeEventStoreError::InvalidTransaction("current content scope missing".into())
            })?;
            match &envelope.body {
                ContentBody::Inline { value } => {
                    let encoded = serde_json::to_vec(value)?;
                    if encoded.len() > INLINE_BYTES
                        || encoded.len() as u64 != envelope.bytes
                        || format!("sha256:{:x}", Sha256::digest(&encoded)) != envelope.sha256
                    {
                        return Err(RuntimeEventStoreError::InvalidTransaction(
                            "invalid inline current content bytes".into(),
                        ));
                    }
                    if input.event.kind == "goal.observation"
                        && (value
                            .pointer("/identity/session_id")
                            .and_then(serde_json::Value::as_str)
                            != Some(session.as_str())
                            || observation_classifier(value)
                                .map_err(RuntimeEventStoreError::InvalidTransaction)?
                                != payload["classifier"])
                    {
                        return Err(RuntimeEventStoreError::InvalidTransaction(
                            "invalid inline observation classifier or scope".into(),
                        ));
                    }
                }
                ContentBody::ArtifactRef { artifact } => {
                    if !artifact.is_durable()
                        || artifact.media_type != "application/json"
                        || artifact.visibility_scope != format!("session:{session}")
                        || artifact.sha256 != envelope.sha256
                        || artifact.bytes != envelope.bytes
                    {
                        return Err(RuntimeEventStoreError::InvalidTransaction(
                            "invalid current content reference identity".into(),
                        ));
                    }
                }
            }
            let bytes = serde_json::to_vec(&input.event)?
                .len()
                .checked_add(request.transaction_id.len())
                .and_then(|bytes| {
                    bytes.checked_add(input.idempotency_key.as_ref().map_or(0, String::len))
                })
                .and_then(|bytes| bytes.checked_add(512))
                .ok_or_else(|| {
                    RuntimeEventStoreError::HistoryBudgetExceeded(
                        "content metadata overflow".into(),
                    )
                })?;
            if bytes > 1024 * 1024 {
                return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                    "content event metadata".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn classify_observation(
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        observation_classifier(value)
    }
    pub(crate) fn resolve_read_record(
        &self,
        mut record: DurableRuntimeEvent,
    ) -> Result<DurableRuntimeEvent, String> {
        let body_bytes = if record.scope == RuntimeEventScope::ExecutionGraph {
            record.payload["bytes"].as_u64()
        } else if record.kind == "goal.observation" {
            record.payload["observation_body"]["bytes"].as_u64()
        } else {
            None
        };
        if body_bytes.is_some_and(|bytes| bytes > 1024 * 1024) {
            return Err(
                RuntimeEventStoreError::HistoryBudgetExceeded(record.event_id.clone()).to_string(),
            );
        }
        if record.scope == RuntimeEventScope::ExecutionGraph {
            let raw = self.resolve_graph_record(&record)?;
            return self.hydrate_graph_record(record, raw);
        } else if record.kind == "goal.observation" {
            let observation = self.resolve_observation_record(&record)?;
            let payload = record
                .payload
                .as_object_mut()
                .ok_or("observation is not an object")?;
            payload.remove("observation_body");
            payload.insert("observation".into(), observation);
        }
        Ok(record)
    }
    pub fn bind_artifact_store(&self, artifacts: Arc<crate::ArtifactStore>) -> Result<(), String> {
        if artifacts.is_verified_observation() {
            return Err("Cannot use an observation view as a tracked content binding".into());
        }
        let bound = self.artifacts.get_or_init(|| Arc::clone(&artifacts));
        if !Arc::ptr_eq(bound, &artifacts) {
            return Err("Runtime content store already bound to another ArtifactStore".into());
        }
        Ok(())
    }

    /// Bind an immutable observation policy at the original binding boundary.
    /// The original resolver and metadata/pin checks are shared; only prepared
    /// read access-time updates are omitted. A live binding cannot be switched.
    pub fn bind_artifact_store_for_verified_observation(
        &self,
        artifacts: Arc<crate::ArtifactStore>,
    ) -> Result<(), String> {
        self.artifacts
            .set(Arc::new(artifacts.verified_observation_view()))
            .map_err(|_| "Runtime content observation must be the first and only binding".into())
    }

    pub(crate) fn prepare_content(
        &self,
        scope: &str,
        identity: &str,
        value: &serde_json::Value,
        origin: &PreparedArtifactOrigin,
    ) -> Result<serde_json::Value, String> {
        origin.validate()?;
        let bytes = serde_json::to_vec(value).map_err(|_| "content encoding failed".to_string())?;
        let sha256 = format!("sha256:{:x}", Sha256::digest(&bytes));
        let body = if bytes.len() <= INLINE_BYTES {
            ContentBody::Inline {
                value: value.clone(),
            }
        } else {
            let store = self.artifacts.get().ok_or("MissingContentStore")?;
            let artifact = store
                .write_pre_pinned_blocking(scope, identity, &bytes, origin)
                .map_err(|error| error.to_string())?;
            ContentBody::ArtifactRef { artifact }
        };
        serde_json::to_value(ContentEnvelope {
            content_schema: CONTENT_SCHEMA,
            sha256,
            bytes: bytes.len() as u64,
            body,
        })
        .map_err(|_| "content encoding failed".to_string())
    }

    pub(crate) fn resolve_content(
        &self,
        scope: &str,
        envelope: &serde_json::Value,
        origin: &PreparedArtifactOrigin,
    ) -> Result<serde_json::Value, String> {
        origin.validate()?;
        let mut value = envelope.clone();
        if let Some(object) = value.as_object_mut() {
            object.remove("_runtime_activity_binding");
        }
        let envelope: ContentEnvelope = serde_json::from_value(value)
            .map_err(|_| "unsupported content envelope".to_string())?;
        if envelope.content_schema != CONTENT_SCHEMA {
            return Err("unsupported content schema".into());
        }
        let (bytes, value) = match envelope.body {
            ContentBody::Inline { value } => {
                let bytes =
                    serde_json::to_vec(&value).map_err(|_| "invalid inline content".to_string())?;
                if bytes.len() > INLINE_BYTES {
                    return Err("oversize inline content".into());
                }
                (bytes, Some(value))
            }
            ContentBody::ArtifactRef { artifact } => {
                if artifact.visibility_scope != scope
                    || artifact.sha256 != envelope.sha256
                    || artifact.bytes != envelope.bytes
                {
                    return Err("content reference identity mismatch".into());
                }
                let store = self.artifacts.get().ok_or("MissingContentStore")?;
                let read = if store.is_verified_observation() {
                    store.read_prepared_observation_blocking(&artifact, scope, origin)
                } else {
                    store.read_prepared_blocking(&artifact, scope, origin)
                };
                (read.map_err(|error| error.to_string())?, None)
            }
        };
        if bytes.len() as u64 != envelope.bytes
            || format!("sha256:{:x}", Sha256::digest(&bytes)) != envelope.sha256
        {
            return Err("content bytes/digest mismatch".into());
        }
        match value {
            Some(value) => Ok(value),
            None => serde_json::from_slice(&bytes).map_err(|_| "invalid content JSON".to_string()),
        }
    }

    /// Pure hydration of an already authenticated graph body; no second IO.
    pub(crate) fn hydrate_graph_record(
        &self,
        mut record: DurableRuntimeEvent,
        mut body: serde_json::Value,
    ) -> Result<DurableRuntimeEvent, String> {
        if record.schema_version != CONTENT_SCHEMA
            || record.scope != RuntimeEventScope::ExecutionGraph
        {
            return Err("unsupported graph event schema".into());
        }
        let binding = record
            .payload
            .get("_runtime_activity_binding")
            .cloned()
            .ok_or("graph binding missing")?;
        body.as_object_mut()
            .ok_or("graph content is not an object")?
            .insert("_runtime_activity_binding".into(), binding);
        record.payload = body;
        Ok(record)
    }
    pub(crate) fn resolve_graph_record(
        &self,
        record: &DurableRuntimeEvent,
    ) -> Result<serde_json::Value, String> {
        if record.schema_version != CONTENT_SCHEMA
            || record.scope != RuntimeEventScope::ExecutionGraph
        {
            return Err("unsupported graph event schema".into());
        }
        let binding = record
            .activity_binding()
            .ok_or("graph content has no trusted activity binding")?;
        self.resolve_content(
            &format!("session:{}", binding.session_id),
            &record.payload,
            &PreparedArtifactOrigin::EventBody {
                stream_id: record.stream_id.clone(),
                idempotency_key: record
                    .idempotency_key
                    .clone()
                    .ok_or("graph content idempotency missing")?,
                kind: record.kind.clone(),
            },
        )
    }

    pub(crate) fn resolve_observation_record(
        &self,
        record: &DurableRuntimeEvent,
    ) -> Result<serde_json::Value, String> {
        if record.schema_version != CONTENT_SCHEMA
            || record.payload["observation_schema"] != CONTENT_SCHEMA
        {
            return Err("unsupported observation schema".into());
        }
        let session = record.payload["session_id"]
            .as_str()
            .filter(|session| !session.is_empty())
            .ok_or("observation session missing")?;
        let observation = self.resolve_content(
            &format!("session:{session}"),
            &record.payload["observation_body"],
            &PreparedArtifactOrigin::EventBody {
                stream_id: record.stream_id.clone(),
                idempotency_key: record
                    .idempotency_key
                    .clone()
                    .ok_or("observation idempotency missing")?,
                kind: record.kind.clone(),
            },
        )?;
        if observation
            .pointer("/identity/session_id")
            .and_then(serde_json::Value::as_str)
            != Some(session)
        {
            return Err("observation scope mismatch".into());
        }
        if observation_classifier(&observation)? != record.payload["classifier"] {
            return Err("observation classifier mismatch".into());
        }
        Ok(observation)
    }
}

pub(crate) fn observation_classifier(
    observation: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let observation: harness_contract::goal::RuntimeObservation =
        serde_json::from_value(observation.clone())
            .map_err(|_| "invalid typed current observation".to_string())?;
    Ok(serde_json::json!({
        "result_class":observation.result_class,"failure_present":observation.failure_class.is_some(),
        "tool_calls":observation.cost_delta.tool_calls,
        "distinguishing_evidence_count":observation.information_gain.distinguishing_evidence_refs.len(),
        "resolved_unknown_count":observation.information_gain.resolved_unknown_refs.len(),
        "provenance":observation.information_gain.provenance,"verified_gain":observation.has_verified_gain()
    }))
}

fn envelope_artifact(value: &serde_json::Value) -> Result<Option<ArtifactRef>, String> {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("_runtime_activity_binding");
    }
    let envelope: ContentEnvelope =
        serde_json::from_value(value).map_err(|error| error.to_string())?;
    Ok(match envelope.body {
        ContentBody::Inline { .. } => None,
        ContentBody::ArtifactRef { artifact } => Some(artifact),
    })
}

pub struct QuiescedContentRoots {
    backend: Arc<dyn RuntimeEventStoreBackend>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedMaintenanceProgress {
    schema: u32,
    repository: String,
    run: String,
    seal: u32,
    high_water: Option<String>,
    after: Option<String>,
    complete: bool,
    attempt_after: Option<String>,
    attempts_complete: bool,
    blocked: bool,
}

impl QuiescedContentRoots {
    pub fn reconcile_prepared_batch(
        &self,
        artifacts: &mut crate::ArtifactStore,
        all_publishers_quiesced: bool,
        repository: &str,
        run: &str,
    ) -> Result<PreparedReconciliationReport, String> {
        artifacts
            .require_quiesced_exclusive(all_publishers_quiesced)
            .map_err(|error| error.to_string())?;
        if repository.is_empty() || repository.len() > 4096 || run.is_empty() || run.len() > 128 {
            return Err("invalid prepared maintenance binding".into());
        }
        let projection_id = format!(
            "runtime:prepared-maintenance:v1:{:x}",
            Sha256::digest(repository.as_bytes())
        );
        let checkpoint = self
            .backend
            .projection_checkpoint(&projection_id)
            .map_err(|error| error.to_string())?;
        let saved = checkpoint
            .as_ref()
            .map(|saved| {
                serde_json::from_value::<PreparedMaintenanceProgress>(saved.payload.clone())
                    .map_err(|_| "invalid prepared maintenance checkpoint".to_string())
            })
            .transpose()?;
        let mut progress = match saved {
            Some(progress)
                if progress.schema == 1
                    && progress.seal == 1
                    && progress.repository == repository
                    && progress.run == run =>
            {
                progress
            }
            _ => PreparedMaintenanceProgress {
                schema: 1,
                seal: 1,
                repository: repository.into(),
                run: run.into(),
                high_water: artifacts.repository().prepared_high_water()?,
                after: None,
                complete: false,
                attempt_after: None,
                attempts_complete: false,
                blocked: false,
            },
        };
        if progress
            .after
            .as_ref()
            .is_some_and(|after| progress.high_water.as_ref().is_none_or(|high| after > high))
        {
            return Err("invalid prepared maintenance keyset".into());
        }
        let mut report = PreparedReconciliationReport::default();
        let mut remaining = 100usize;
        if !progress.attempts_complete {
            let attempts = artifacts
                .repository()
                .active_attempts_page(progress.attempt_after.as_deref(), remaining)?;
            progress.attempts_complete = attempts.len() < remaining;
            remaining -= attempts.len();
            for attempt in attempts {
                if artifacts.repository().release_attempt(
                    &attempt.token,
                    &attempt.sha256,
                    &attempt.generation,
                )? {
                    report.released_attempts += 1;
                }
                progress.attempt_after = Some(attempt.token);
            }
        }
        if !progress.complete && remaining > 0 {
            if let Some(high) = &progress.high_water {
                let page = artifacts.repository().prepared_page(
                    progress.after.as_deref(),
                    high,
                    remaining,
                )?;
                for candidate in &page.candidates {
                    report.examined += 1;
                    let Some(origin) = candidate
                        .record
                        .prepared_origin
                        .as_ref()
                        .filter(|origin| origin.validate().is_ok())
                    else {
                        report.blocked += 1;
                        progress.blocked = true;
                        continue;
                    };
                    let root = self
                        .backend
                        .prepared_content_root(origin, &candidate.record.content_reference())
                        .map_err(|error| error.to_string())?;
                    match root {
                        PreparedContentRoot::Canonical => report.referenced_canonical += 1,
                        PreparedContentRoot::Checkpoint => report.referenced_checkpoint += 1,
                        PreparedContentRoot::Terminal => report.referenced_terminal += 1,
                        PreparedContentRoot::Blocked => {
                            report.blocked += 1;
                            progress.blocked = true;
                        }
                        PreparedContentRoot::Absent => {
                            match artifacts.repository().retire_prepared(candidate)? {
                                crate::PreparedArtifactRetirement::Removed => {
                                    report.retired_records += 1
                                }
                                crate::PreparedArtifactRetirement::Missing => {}
                                crate::PreparedArtifactRetirement::Retained => {
                                    report.retained_other_pin_or_changed += 1
                                }
                            }
                        }
                    }
                }
                if !page.complete
                    && page.last_examined_id.as_ref().is_none_or(|last| {
                        progress.after.as_ref().is_some_and(|after| last <= after)
                    })
                {
                    return Err("prepared inventory did not advance".into());
                }
                if page.last_examined_id.is_some() {
                    progress.after = page.last_examined_id;
                }
                progress.complete = page.complete;
            } else {
                progress.complete = true;
            }
        }
        let payload = serde_json::to_value(&progress).map_err(|error| error.to_string())?;
        self.backend
            .compare_and_put_projection_checkpoint(
                &projection_id,
                0,
                checkpoint.as_ref().map_or(0, |saved| saved.revision),
                &payload,
                0,
            )
            .map_err(|error| error.to_string())?;
        report.inventory_pass_complete = progress.complete;
        report.attempt_pass_complete = progress.attempts_complete;
        report.eligible_prepared_reconciled = progress.complete && !progress.blocked;
        Ok(report)
    }
}

impl RuntimeEventStore {
    pub(crate) fn resolve_host_transcript_content(
        &self,
        scope: &str,
        envelope: &serde_json::Value,
        origin: &PreparedArtifactOrigin,
    ) -> Result<serde_json::Value, String> {
        origin.validate()?;
        if !matches!(origin, PreparedArtifactOrigin::EventBody { kind, .. } if kind == "execution.host.transcript")
            || !scope.starts_with("session:")
            || scope == "session:"
        {
            return Err("invalid Host transcript prepared origin".into());
        }
        let supplied = envelope;
        let envelope: ContentEnvelope = serde_json::from_value(envelope.clone())
            .map_err(|_| "invalid Host transcript content envelope".to_string())?;
        let ContentBody::ArtifactRef { artifact } = envelope.body else {
            return Err("Host transcript requires private artifact body".into());
        };
        if envelope.content_schema != 2
            || envelope.bytes == 0
            || envelope.bytes > 524288
            || artifact.bytes != envelope.bytes
            || artifact.sha256 != envelope.sha256
            || !artifact.is_durable()
            || artifact.visibility_scope != scope
            || artifact.media_type != "application/vnd.cowd.host-transcript+json"
        {
            return Err("Host transcript content identity mismatch".into());
        }
        let store = self.artifacts.get().ok_or("MissingContentStore")?;
        let identity = artifact
            .selector
            .strip_prefix("artifact://art_")
            .filter(|identity| {
                identity.len() == 64
                    && identity
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            .ok_or("invalid Host transcript artifact selector")?;
        let record = store
            .repository()
            .record(&format!("art_{identity}"))?
            .ok_or("Host transcript private artifact missing")?;
        if record.record_class != crate::ArtifactRecordClass::PrivatePrepared
            || record.prepared_origin.as_ref() != Some(origin)
            || record.content_reference() != artifact
        {
            return Err("Host transcript requires exact private prepared record".into());
        }
        self.resolve_content(scope, supplied, origin)
    }

    pub fn host_transcript_root_identity(
        event: &DurableRuntimeEvent,
    ) -> Result<(String, String, ArtifactRef), String> {
        let invalid = || "invalid Host transcript root identity".to_string();
        let fields = [
            "host_transcript_schema",
            "graph_id",
            "node_id",
            "attempt",
            "session_id",
            "turn_id",
            "ticket_digest",
            "body",
            "_runtime_activity_binding",
        ];
        let object = event.payload.as_object().ok_or_else(invalid)?;
        if object.len() != fields.len() || object.keys().any(|key| !fields.contains(&key.as_str()))
        {
            return Err(invalid());
        }
        let text = |field: &str| {
            event.payload[field]
                .as_str()
                .filter(|value| !value.trim().is_empty() && value.len() <= 1024)
                .ok_or_else(invalid)
        };
        let graph = text("graph_id")?;
        let node = text("node_id")?;
        let session = text("session_id")?;
        let turn = text("turn_id")?;
        let digest = text("ticket_digest")?;
        let binding = event.activity_binding().ok_or_else(invalid)?;
        if event.kind != "execution.host.transcript"
            || event.scope != RuntimeEventScope::ExecutionNode
            || event.schema_version != 2
            || event.status.as_deref() != Some("prepared")
            || !matches!(
                event.actor.as_deref(),
                Some("inline_model" | "tool_batch" | "synthesize")
            )
            || event.payload["host_transcript_schema"] != 1
            || event.payload["attempt"]
                .as_u64()
                .and_then(|attempt| u32::try_from(attempt).ok())
                .is_none_or(|attempt| attempt == 0)
            || digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || event.stream_id != format!("{graph}:node:{node}")
            || event.idempotency_key.as_deref()
                != Some(format!("host-transcript:{digest}").as_str())
            || binding.validate().is_err()
            || binding.root_execution_id != graph
            || binding.node_id.as_deref() != Some(node)
            || binding.session_id != session
            || binding.turn_id != turn
        {
            return Err(invalid());
        }
        let envelope: ContentEnvelope =
            serde_json::from_value(event.payload["body"].clone()).map_err(|_| invalid())?;
        let ContentBody::ArtifactRef { artifact } = envelope.body else {
            return Err(invalid());
        };
        if envelope.content_schema != 2
            || envelope.bytes == 0
            || envelope.bytes > 524288
            || artifact.bytes != envelope.bytes
            || artifact.sha256 != envelope.sha256
            || !artifact.is_durable()
            || artifact.media_type != "application/vnd.cowd.host-transcript+json"
            || artifact.visibility_scope != format!("session:{session}")
            || !artifact
                .sha256
                .strip_prefix("sha256:")
                .is_some_and(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
            || !artifact
                .selector
                .strip_prefix("artifact://art_")
                .is_some_and(|identity| {
                    identity.len() == 64
                        && identity
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
        {
            return Err(invalid());
        }
        Ok((graph.into(), node.into(), artifact))
    }

    pub fn host_transcript_root(
        primary: Option<&DurableRuntimeEvent>,
        node: Option<&DurableRuntimeEvent>,
        graph: Option<&DurableRuntimeEvent>,
        artifact: &ArtifactRef,
    ) -> PreparedContentRoot {
        let Some(primary) = primary else {
            return PreparedContentRoot::Absent;
        };
        let Ok((graph_id, node_id, reference)) = Self::host_transcript_root_identity(primary)
        else {
            return PreparedContentRoot::Blocked;
        };
        if &reference != artifact {
            return PreparedContentRoot::Absent;
        }
        let (Some(node), Some(graph)) = (node, graph) else {
            return PreparedContentRoot::Blocked;
        };
        let (Some(mut original), Some(node_binding), Some(graph_binding)) = (
            primary.activity_binding(),
            node.activity_binding(),
            graph.activity_binding(),
        ) else {
            return PreparedContentRoot::Blocked;
        };
        let start_revision = original.revision;
        let start_fence = original.fence;
        original.revision = node_binding.revision;
        original.fence = node_binding.fence;
        let key = primary.idempotency_key.as_deref().unwrap_or("");
        let references = node.payload["result"]["evidence_refs"].as_array();
        let revision = node.payload["graph_revision"].as_u64().unwrap_or(0);
        let expected_reference = harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::reality::EvidenceRef::new("host_transcript", key),
            reference.sha256.clone(),
            reference.bytes,
            reference.media_type.clone(),
            key,
            reference.visibility_scope.clone(),
        );
        let matching_reference = references.is_some_and(|references| {
            let matches = references
                .iter()
                .filter(|reference| reference["retrieval_selector"] == key)
                .collect::<Vec<_>>();
            matches.len() == 1
                && serde_json::from_value::<harness_contract::context::EvidenceAccessRef>(
                    (*matches[0]).clone(),
                )
                .is_ok_and(|reference| reference == expected_reference)
        });
        if primary.transaction_id.is_empty()
            || primary.commit_cursor == 0
            || node.transaction_id != primary.transaction_id
            || graph.transaction_id != primary.transaction_id
            || node.commit_cursor != primary.commit_cursor
            || graph.commit_cursor != primary.commit_cursor
            || primary.transaction_index == node.transaction_index
            || primary.transaction_index == graph.transaction_index
            || node.transaction_index == graph.transaction_index
            || graph.transaction_index != 0
            || node.scope != RuntimeEventScope::ExecutionNode
            || node.kind != "execution_node.transitioned"
            || node.schema_version != 1
            || node.actor.as_deref() != Some("execution_commit_service")
            || node.stream_id != primary.stream_id
            || node.payload["graph_id"] != graph_id
            || node.payload["node_id"] != node_id
            || node.payload["from"] != "running"
            || node.payload["to"] != node.payload["result"]["status"]
            || !matches!(
                node.payload["to"].as_str(),
                Some(
                    "completed"
                        | "failed"
                        | "blocked"
                        | "cancelled"
                        | "waiting_input"
                        | "waiting_approval"
                        | "waiting_external"
                        | "paused"
                )
            )
            || node.status.as_deref() != node.payload["to"].as_str()
            || !matching_reference
            || revision == 0
            || node.idempotency_key.as_deref()
                != Some(format!("{graph_id}:{node_id}:{revision}").as_str())
            || original != node_binding
            || node_binding.validate().is_err()
            || graph.scope != RuntimeEventScope::ExecutionGraph
            || graph.schema_version != 2
            || graph.stream_id != graph_id
            || graph.actor.as_deref() != Some("execution_commit_service")
            || !matches!(
                graph.kind.as_str(),
                "execution_graph.node_transitioned"
                    | "execution_graph.nodes_transitioned"
                    | "execution_graph.node_transitioned_and_replanned"
                    | "execution_graph.checkpoint"
            )
            || graph.idempotency_key.as_deref()
                != Some(format!("{graph_id}:revision:{revision}").as_str())
            || graph.sequence != revision
            || graph_binding.validate().is_err()
            || graph_binding.root_execution_id != graph_id
            || graph_binding.node_id.is_some()
            || graph_binding.session_id != original.session_id
            || graph_binding.turn_id != original.turn_id
            || graph_binding.root_task_id != original.root_task_id
            || graph_binding.task_id != original.task_id
            || graph_binding.generation != original.generation
            || graph_binding.revision != revision
            || node_binding.revision != revision
            || graph_binding.fence != revision
            || node_binding.fence != revision
            || start_revision >= revision
            || start_fence >= revision
        {
            return PreparedContentRoot::Blocked;
        }
        PreparedContentRoot::Canonical
    }
}

#[cfg(test)]
mod host_transcript_roots_460_tests {
    use super::*;
    use crate::execution_core::graph::{
        ExecutionNodeBinding, NodeExecutionOutcome, NodeExecutionTicket,
    };
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeResult,
        ExecutionNodeSpec, ExecutionNodeStatus,
    };

    struct Fixture {
        directory: tempfile::TempDir,
        artifacts: Arc<crate::ArtifactStore>,
        events: Arc<RuntimeEventStore>,
        commits: crate::ExecutionCommitService,
        graph: ExecutionGraph,
        ticket: NodeExecutionTicket,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let artifacts = Arc::new(
                crate::ArtifactStore::for_test(
                    directory.path().join("artifacts"),
                    crate::ArtifactStoreConfig {
                        orphan_grace_ms: 0,
                        ..Default::default()
                    },
                )
                .unwrap(),
            );
            let events = Arc::new(RuntimeEventStore::for_test());
            events.bind_artifact_store(artifacts.clone()).unwrap();
            let commits = crate::ExecutionCommitService::new(events.clone())
                .with_artifact_store(artifacts.clone());
            let mut graph = ExecutionGraph::new("Host transcript canonical publication fixture")
                .with_lineage(ExecutionGraphLineage {
                    session_id: "session-460".into(),
                    turn_id: "turn-460".into(),
                    root_task_id: "task-460".into(),
                    task_id: "task-460".into(),
                    generation: 1,
                });
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "fixture-original-payload",
            );
            node.id = "host-node-460".into();
            graph.nodes.push(node.clone());
            let graph = commits.register_graph(graph).unwrap().graph;
            let graph = commits
                .transition_node(
                    &graph,
                    &node.id,
                    ExecutionNodeStatus::Ready,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            let ticket = NodeExecutionTicket {
                graph_id: graph.id.clone(),
                node_id: node.id.clone(),
                executor_kind: node.executor_kind.clone(),
                service_class: graph.service_class,
                attempt: 1,
                idempotency_key: format!("{}:1", node.idempotency_key),
                payload_ref: node.payload_ref,
            };
            let binding = ExecutionNodeBinding {
                executor_kind: ticket.executor_kind.clone(),
                ticket_idempotency_key: ticket.idempotency_key.clone(),
                attempt: ticket.attempt,
                resource_lease_refs: Vec::new(),
                scope_lease_ref: None,
                worktree_lease_ref: None,
            };
            let graph = commits
                .bind_and_start_nodes(
                    &graph,
                    BTreeMap::from([(node.id, binding)]),
                    vec![ticket.clone()],
                )
                .unwrap()
                .graph;
            Self {
                directory,
                artifacts,
                events,
                commits,
                graph,
                ticket,
            }
        }

        fn stage(&self) -> RuntimeTransactionEventInput {
            self.commits
                .prepare_host_transcript(
                    &self.ticket,
                    "session-460",
                    "turn-460",
                    0,
                    format!("sha256:{:x}", Sha256::digest([])),
                    Vec::new(),
                    None,
                )
                .unwrap()
        }

        fn origin(event: &RuntimeTransactionEventInput) -> PreparedArtifactOrigin {
            PreparedArtifactOrigin::EventBody {
                stream_id: event.event.stream_id.clone(),
                idempotency_key: event.idempotency_key.clone().unwrap(),
                kind: event.event.kind.clone(),
            }
        }

        fn artifact(event: &RuntimeTransactionEventInput) -> ArtifactRef {
            serde_json::from_value(event.event.payload["body"]["body"]["artifact"].clone()).unwrap()
        }

        fn commit(
            &self,
            event: RuntimeTransactionEventInput,
        ) -> crate::execution_core::graph::ExecutionCommitReceipt {
            let reference = harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::reality::EvidenceRef::new(
                    "host_transcript",
                    event.idempotency_key.as_ref().unwrap(),
                ),
                event.event.payload["body"]["sha256"].as_str().unwrap(),
                event.event.payload["body"]["bytes"].as_u64().unwrap(),
                "application/vnd.cowd.host-transcript+json",
                event.idempotency_key.as_ref().unwrap(),
                "session:session-460",
            );
            let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                wait_class: None,
                summary: None,
                evidence_refs: vec![reference],
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            });
            outcome.domain_events.push(event);
            self.commits
                .transition_execution_outcome(&self.graph, &self.ticket, &outcome, &BTreeMap::new())
                .unwrap()
        }
    }

    #[test]
    fn r1_460_host_private_reader_rejects_live_inline_wrong_origin_and_slot() {
        let fixture = Fixture::new();
        let stage = fixture.stage();
        let origin = Fixture::origin(&stage);
        let envelope = &stage.event.payload["body"];
        let document = fixture
            .events
            .resolve_host_transcript_content("session:session-460", envelope, &origin)
            .unwrap();
        assert_eq!(
            document["ticket"],
            serde_json::to_value(&fixture.ticket).unwrap()
        );
        assert_eq!(document["messages"], serde_json::json!([]));
        let inline = serde_json::json!({"content_schema":2,"sha256":envelope["sha256"],"bytes":envelope["bytes"],
            "body":{"kind":"inline","value":document}});
        assert_eq!(
            fixture
                .events
                .resolve_host_transcript_content("session:session-460", &inline, &origin)
                .unwrap_err(),
            "Host transcript requires private artifact body"
        );
        assert_eq!(
            fixture
                .events
                .resolve_host_transcript_content(
                    "session:session-460",
                    &stage.event.payload,
                    &origin
                )
                .unwrap_err(),
            "invalid Host transcript content envelope"
        );
        let mut foreign = origin.clone();
        if let PreparedArtifactOrigin::EventBody {
            idempotency_key, ..
        } = &mut foreign
        {
            idempotency_key.push_str("-foreign");
        }
        assert_eq!(
            fixture
                .events
                .resolve_host_transcript_content("session:session-460", envelope, &foreign)
                .unwrap_err(),
            "Host transcript requires exact private prepared record"
        );
        let artifact = Fixture::artifact(&stage);
        let record = fixture
            .artifacts
            .repository()
            .record(artifact.selector.strip_prefix("artifact://").unwrap())
            .unwrap()
            .unwrap();
        fixture.artifacts.repository().adopt_live(&record).unwrap();
        assert_eq!(
            fixture
                .events
                .resolve_host_transcript_content("session:session-460", envelope, &origin)
                .unwrap_err(),
            "Host transcript requires exact private prepared record"
        );
    }

    #[test]
    fn r1_460_reserved_publication_checks_original_ticket_and_atomic_pair_before_append() {
        let fixture = Fixture::new();
        let stage = fixture.stage();
        let before = fixture.events.durable_commit_cursor().unwrap();
        let request = |event: RuntimeTransactionEventInput| AppendTransactionRequest {
            transaction_id: "host-460-invalid-publication".into(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: event.event.stream_id.clone(),
                expected_revision: fixture
                    .events
                    .stream_revision(&event.event.stream_id)
                    .unwrap(),
            }],
            events: vec![event],
        };
        let error = fixture
            .events
            .append_transaction(request(stage.clone()))
            .unwrap_err();
        assert!(
            matches!(error, RuntimeEventStoreError::InvalidTransaction(ref reason)
            if reason == "Host transcript requires one atomic original Graph result")
        );
        let mut foreign = stage.clone();
        foreign.event.payload["attempt"] = serde_json::json!(2);
        let error = fixture
            .events
            .append_transaction(request(foreign))
            .unwrap_err();
        assert!(
            matches!(error, RuntimeEventStoreError::InvalidTransaction(ref reason)
            if reason == "Host transcript original identity mismatch")
        );
        assert_eq!(fixture.events.durable_commit_cursor().unwrap(), before);
        assert!(fixture
            .events
            .event_by_idempotency_key(
                &stage.event.stream_id,
                stage.idempotency_key.as_ref().unwrap()
            )
            .unwrap()
            .is_none());
        let receipt = fixture.commit(stage.clone());
        assert_eq!(
            receipt.graph.node_statuses[&fixture.ticket.node_id],
            ExecutionNodeStatus::Completed
        );
        assert_eq!(
            fixture
                .events
                .backend
                .prepared_content_root(&Fixture::origin(&stage), &Fixture::artifact(&stage))
                .unwrap(),
            PreparedContentRoot::Canonical
        );
    }

    #[test]
    fn r1_460_host_root_exact_same_transaction_pair_and_identity_mutants() {
        let fixture = Fixture::new();
        let stage = fixture.stage();
        let artifact = Fixture::artifact(&stage);
        let receipt = fixture.commit(stage.clone());
        let batch = fixture
            .events
            .backend
            .events_after_cursor(receipt.transaction.commit_cursor - 1, 1)
            .unwrap()
            .remove(0);
        let primary = batch
            .events
            .iter()
            .find(|event| event.kind == "execution.host.transcript")
            .unwrap();
        let node = batch
            .events
            .iter()
            .find(|event| event.kind == "execution_node.transitioned")
            .unwrap();
        let graph = batch
            .events
            .iter()
            .find(|event| event.scope == RuntimeEventScope::ExecutionGraph)
            .unwrap();
        assert_eq!(
            RuntimeEventStore::host_transcript_root(
                Some(primary),
                Some(node),
                Some(graph),
                &artifact
            ),
            PreparedContentRoot::Canonical
        );
        assert_eq!(
            RuntimeEventStore::host_transcript_root(None, None, None, &artifact),
            PreparedContentRoot::Absent
        );
        assert_eq!(
            RuntimeEventStore::host_transcript_root(Some(primary), None, Some(graph), &artifact),
            PreparedContentRoot::Blocked
        );
        for change in [
            "transaction",
            "cursor",
            "reference",
            "reference_hash",
            "reference_media",
            "reference_scope",
            "source",
            "generation",
            "revision",
        ] {
            let mut foreign = node.clone();
            match change {
                "transaction" => foreign.transaction_id.push_str("-foreign"),
                "cursor" => foreign.commit_cursor += 1,
                "reference" => foreign.payload["result"]["evidence_refs"] = serde_json::json!([]),
                "reference_hash" => {
                    foreign.payload["result"]["evidence_refs"][0]["sha256"] =
                        serde_json::json!(format!("sha256:{}", "c".repeat(64)))
                }
                "reference_media" => {
                    foreign.payload["result"]["evidence_refs"][0]["media_type"] =
                        serde_json::json!("application/json")
                }
                "reference_scope" => {
                    foreign.payload["result"]["evidence_refs"][0]["visibility_scope"] =
                        serde_json::json!("session:foreign")
                }
                "source" => foreign.payload["node_id"] = serde_json::json!("foreign"),
                "generation" => {
                    foreign.payload["_runtime_activity_binding"]["generation"] =
                        serde_json::json!(2)
                }
                _ => foreign.payload["graph_revision"] = serde_json::json!(0),
            }
            assert_eq!(
                RuntimeEventStore::host_transcript_root(
                    Some(primary),
                    Some(&foreign),
                    Some(graph),
                    &artifact
                ),
                PreparedContentRoot::Blocked,
                "{change}"
            );
        }
        for change in [
            "transaction",
            "cursor",
            "schema",
            "actor",
            "kind",
            "revision",
        ] {
            let mut foreign = graph.clone();
            match change {
                "transaction" => foreign.transaction_id.push_str("-foreign"),
                "cursor" => foreign.commit_cursor += 1,
                "schema" => foreign.schema_version = 1,
                "actor" => foreign.actor = Some("foreign".into()),
                "kind" => foreign.kind = "execution_graph.planned".into(),
                _ => foreign.sequence += 1,
            }
            assert_eq!(
                RuntimeEventStore::host_transcript_root(
                    Some(primary),
                    Some(node),
                    Some(&foreign),
                    &artifact
                ),
                PreparedContentRoot::Blocked,
                "{change}"
            );
        }
        for change in [
            "extra", "kind", "scope", "actor", "slot", "session", "turn", "digest", "attempt",
            "inline",
        ] {
            let mut foreign = primary.clone();
            match change {
                "extra" => foreign.payload["extra_outcome"] = serde_json::json!({}),
                "kind" => foreign.kind = "execution.effect.receipt".into(),
                "scope" => foreign.scope = RuntimeEventScope::Session,
                "actor" => foreign.actor = Some("foreign".into()),
                "slot" => {
                    let body = foreign
                        .payload
                        .as_object_mut()
                        .unwrap()
                        .remove("body")
                        .unwrap();
                    foreign.payload["wrong_slot"] = body;
                }
                "session" => foreign.payload["session_id"] = serde_json::json!("foreign"),
                "turn" => foreign.payload["turn_id"] = serde_json::json!("foreign"),
                "digest" => foreign.payload["ticket_digest"] = serde_json::json!("wrong"),
                "attempt" => foreign.payload["attempt"] = serde_json::json!(0),
                _ => {
                    foreign.payload["body"]["body"] =
                        serde_json::json!({"kind":"inline","value":{}})
                }
            }
            assert_eq!(
                RuntimeEventStore::host_transcript_root(
                    Some(&foreign),
                    Some(node),
                    Some(graph),
                    &artifact
                ),
                PreparedContentRoot::Blocked,
                "{change}"
            );
        }
    }

    #[test]
    fn r1_460_host_staged_orphan_gc_and_canonical_private_retention() {
        for committed in [false, true] {
            let fixture = Fixture::new();
            let stage = fixture.stage();
            let artifact = Fixture::artifact(&stage);
            if committed {
                fixture.commit(stage.clone());
            }
            let roots = fixture.events.quiesced_content_roots();
            let Fixture {
                directory,
                artifacts,
                events,
                commits,
                ..
            } = fixture;
            drop(commits);
            drop(events);
            let mut artifacts = Arc::try_unwrap(artifacts).ok().unwrap();
            let report = roots
                .reconcile_prepared_batch(&mut artifacts, true, "host-460", "host-460-pass")
                .unwrap();
            assert!(report.eligible_prepared_reconciled);
            assert_eq!(report.blocked, 0);
            assert_eq!(report.referenced_canonical, u64::from(committed));
            assert_eq!(report.retired_records, u64::from(!committed));
            let record = artifacts
                .repository()
                .record(artifact.selector.strip_prefix("artifact://").unwrap())
                .unwrap();
            if committed {
                assert_eq!(
                    record.unwrap().record_class,
                    crate::ArtifactRecordClass::PrivatePrepared
                );
                assert!(artifacts.resolve(&artifact.selector).is_err());
            } else {
                assert!(record.is_none());
                assert_eq!(
                    artifacts
                        .reconcile_quiesced_orphans(true, 100)
                        .unwrap()
                        .removed_objects,
                    1
                );
                assert_eq!(artifacts.stats().unwrap().physical_bytes, 0);
            }
            drop(artifacts);
            drop(directory);
        }
    }
}

#[cfg(test)]
mod process_closure_roots_428_tests {
    use super::*;

    fn pair() -> (DurableRuntimeEvent, DurableRuntimeEvent, ArtifactRef) {
        let artifact = ArtifactRef {
            selector: "artifact://art_closure_428".into(),
            sha256: format!("sha256:{}", "a".repeat(64)),
            bytes: 32,
            media_type: "application/vnd.cowd.process-closure+json".into(),
            visibility_scope: "session:session-428".into(),
            durability: harness_contract::context::EvidenceDurability::Durable,
        };
        let slot = serde_json::json!({
            "content_schema":2,"sha256":artifact.sha256,"bytes":artifact.bytes,
            "body":{"kind":"artifact_ref","artifact":artifact},
        });
        let mut primary = DurableRuntimeEvent {
            event_id: "primary-428".into(),
            stream_id: "execution-effect:call-428".into(),
            sequence: 2,
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution.effect.receipt".into(),
            status: Some("completed".into()),
            actor: Some("governed_tool".into()),
            refs: vec![RuntimeEventRef {
                kind: "agent_attempt".into(),
                id: "1".into(),
            }],
            payload: serde_json::json!({
                "idempotency_key":"call-428","process_admission_ref":"call-428:process-admission",
                "process_closure":slot,"replay":{"session_id":"session-428",
                    "parent_execution":{"execution_id":"graph-428","node_id":"node-428"}},
                "root_effect_owner":null,"root_effect_context":null,"effect":{"spawns_process":true},
                "resolved_effect_scope":{"version":2},"outcome":{"status":"executed"},
            }),
            created_at_ms: 1,
            commit_cursor: 5,
            transaction_id: "tool-effect-receipt:call-428".into(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: Some("call-428:receipt".into()),
        };
        let mut index = primary.clone();
        index.event_id = "index-428".into();
        index.stream_id = "execution-agent-receipts:graph-428:node-428:1".into();
        index.kind = "execution.agent_tool.receipt".into();
        index.idempotency_key = Some("agent-tool-receipt:call-428".into());
        index.transaction_index = 1;
        index.payload.as_object_mut().unwrap().remove("replay");
        primary.sequence = 3;
        (primary, index, artifact)
    }

    #[test]
    fn r1_428_closure_root_exact_pair_and_orphan() {
        let (primary, index, artifact) = pair();
        assert!(matches!(
            RuntimeEventStore::process_closure_root(Some(&primary), Some(&index), &artifact),
            PreparedContentRoot::Canonical
        ));
        assert!(matches!(
            RuntimeEventStore::process_closure_root(None, None, &artifact),
            PreparedContentRoot::Absent
        ));
        let mut legacy = primary.clone();
        legacy.payload["process_closure"] = serde_json::Value::Null;
        assert!(matches!(
            RuntimeEventStore::process_closure_root(Some(&legacy), None, &artifact),
            PreparedContentRoot::Absent
        ));
        let mut unrelated = artifact.clone();
        unrelated.selector = "artifact://other".into();
        assert!(matches!(
            RuntimeEventStore::process_closure_root(Some(&primary), Some(&index), &unrelated),
            PreparedContentRoot::Absent
        ));
        for change in [
            "transaction",
            "cursor",
            "order",
            "attempt",
            "outcome",
            "actor",
            "scope",
            "schema",
            "reference",
        ] {
            let mut changed = index.clone();
            match change {
                "transaction" => changed.transaction_id = "other".into(),
                "cursor" => changed.commit_cursor += 1,
                "order" => changed.transaction_index = 0,
                "attempt" => {
                    changed.stream_id = "execution-agent-receipts:graph-428:node-428:2".into()
                }
                "outcome" => changed.payload["outcome"] = serde_json::json!({"status":"failed"}),
                "actor" => changed.actor = Some("model".into()),
                "scope" => changed.scope = RuntimeEventScope::ExecutionGraph,
                "schema" => changed.schema_version = 2,
                "reference" => {
                    changed.payload["process_closure"]["body"]["artifact"]["selector"] =
                        serde_json::json!("artifact://other")
                }
                _ => unreachable!(),
            }
            assert!(
                matches!(
                    RuntimeEventStore::process_closure_root(
                        Some(&primary),
                        Some(&changed),
                        &artifact
                    ),
                    PreparedContentRoot::Blocked
                ),
                "{change}"
            );
        }
        assert!(matches!(
            RuntimeEventStore::process_closure_root(Some(&primary), None, &artifact),
            PreparedContentRoot::Blocked
        ));
    }

    #[test]
    fn r1_428_closure_envelope_rejects_inline_extent_and_extra_fields() {
        let (primary, _, _) = pair();
        let original = primary.payload["process_closure"].clone();
        for change in ["inline", "extent", "media", "unknown", "digest"] {
            let mut slot = original.clone();
            match change {
                "inline" => slot["body"] = serde_json::json!({"kind":"inline","value":{}}),
                "extent" => slot["bytes"] = serde_json::json!(32769),
                "media" => {
                    slot["body"]["artifact"]["media_type"] = serde_json::json!("application/json")
                }
                "unknown" => slot["extra"] = serde_json::json!(true),
                "digest" => slot["sha256"] = serde_json::json!("sha256:bad"),
                _ => unreachable!(),
            }
            assert!(
                RuntimeEventStore::process_closure_artifact(&slot).is_err(),
                "{change}"
            );
        }
    }

    #[tokio::test]
    async fn r1_428_closure_publication_rejects_live_artifact_and_lone_index() {
        let (mut primary, mut index, _) = pair();
        let temporary = tempfile::tempdir().unwrap();
        let artifacts = std::sync::Arc::new(
            crate::ArtifactStore::for_test(temporary.path(), crate::ArtifactStoreConfig::default())
                .unwrap(),
        );
        let live = artifacts
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/vnd.cowd.process-closure+json".into(),
                    visibility_scope: "session:session-428".into(),
                    expected_bytes: None,
                    original_name: None,
                },
                b"{\"version\":1}",
            )
            .await
            .unwrap();
        let slot = serde_json::json!({
            "content_schema":2,"sha256":live.sha256,"bytes":live.bytes,
            "body":{"kind":"artifact_ref","artifact":live},
        });
        primary.payload["process_closure"] = slot.clone();
        index.payload["process_closure"] = slot;
        let as_input = |event: &DurableRuntimeEvent| RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: event.stream_id.clone(),
                scope: event.scope,
                kind: event.kind.clone(),
                status: event.status.clone(),
                actor: event.actor.clone(),
                refs: event.refs.clone(),
                payload: event.payload.clone(),
            },
            idempotency_key: event.idempotency_key.clone(),
            schema_version: event.schema_version,
        };
        let store = RuntimeEventStore::for_test();
        store.bind_artifact_store(artifacts).unwrap();
        let request = AppendTransactionRequest {
            transaction_id: "closure-publication-428".into(),
            expected_streams: vec![],
            events: vec![as_input(&primary), as_input(&index)],
        };
        RuntimeEventStore::validate_process_closure_pair(
            &request.events[0].event,
            request.events[0].idempotency_key.as_deref().unwrap(),
            request.events[0].schema_version,
            &request.events[1].event,
            request.events[1].idempotency_key.as_deref().unwrap(),
            request.events[1].schema_version,
        )
        .expect("complete primary/index identity baseline is valid");
        assert!(
            matches!(
                store.process_closure_publications(&request),
                Err(RuntimeEventStoreError::InvalidTransaction(reason))
                    if reason == "artifact metadata failed: process closure metadata bounds exceeded"
            ),
            "a structurally valid pair still rejects a Live closure artifact"
        );
        let lone = AppendTransactionRequest {
            events: vec![as_input(&index)],
            ..request
        };
        assert!(
            matches!(
                store.process_closure_publications(&lone),
                Err(RuntimeEventStoreError::InvalidTransaction(reason))
                    if reason == "unowned process closure slot"
            ),
            "lone index must fail ownership before artifact lookup"
        );
    }
}

#[cfg(test)]
mod own865_prepared_checkpoint_tests {
    use super::*;
    use crate::ArtifactMetadataRepository;

    struct Fixture {
        _directory: tempfile::TempDir,
        artifacts: Arc<crate::ArtifactStore>,
        repository: Arc<crate::EphemeralArtifactRepository>,
        events: RuntimeEventStore,
        payload: serde_json::Value,
        encoded: serde_json::Value,
        fence: RuntimeSourceMutationFence,
    }

    impl Fixture {
        fn new(compact: bool) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let repository = Arc::new(crate::EphemeralArtifactRepository::default());
            let artifacts = Arc::new(crate::ArtifactStore::new(directory.path().join("artifacts"), repository.clone(),
                crate::ArtifactStoreConfig { compact_threshold_bytes: if compact { 1024 * 1024 } else { 1 },
                    ..Default::default() }).unwrap());
            let events = RuntimeEventStore::for_test();
            events.bind_artifact_store(artifacts.clone()).unwrap();
            let fence = events.source_mutation_fence().unwrap();
            let mut encoded = serde_json::json!({"state":{"through":0,"identity":"original-owner",
                "incomplete":true,"source_fence":fence},"padding":""});
            let overhead = serde_json::to_vec(&encoded).unwrap().len();
            encoded["padding"] = serde_json::json!("x".repeat(242788 - overhead));
            assert_eq!(serde_json::to_vec(&encoded).unwrap().len(), 242788);
            let payload = serde_json::json!({"schema":6,"scope":"session:prepared-reuse",
                "source_fence":fence,"target_cursor":0,"verified_through":0,"incomplete":true});
            Self { _directory: directory, artifacts, repository, events, payload, encoded, fence }
        }

        fn prepare(&self) -> PreparedHistoryCheckpoint {
            self.events.prepare_history_checkpoint("projection-history:prepared-reuse", 0, 0,
                "same-request", self.payload.clone(), &self.encoded).unwrap()
        }
    }

    #[test]
    fn same_request_checkpoint_reuse_eliminates_validation_body_io_compact_and_blob() {
        for compact in [true, false] {
            let fixture = Fixture::new(compact);
            let proof = fixture.prepare();
            let payload = proof.payload().clone();
            let before = fixture.artifacts.checkpoint_body_read_count();
            let saved = fixture.events.publish_prepared_history_checkpoint(proof, &fixture.fence).unwrap();
            assert_eq!(fixture.artifacts.checkpoint_body_read_count(), before);
            fixture.events.compare_and_put_projection_checkpoint_fenced(
                "projection-history:prepared-reuse", 0, saved.revision, &payload, 0, &fixture.fence,
            ).unwrap();
            assert_eq!(fixture.artifacts.checkpoint_body_read_count(), before + 1);
        }
    }

    #[test]
    fn prepared_checkpoint_rejects_foreign_store_source_and_stale_cas() {
        let fixture = Fixture::new(true);
        let proof = fixture.prepare();
        let foreign = RuntimeEventStore::for_test();
        foreign.bind_artifact_store(fixture.artifacts.clone()).unwrap();
        assert!(foreign.publish_prepared_history_checkpoint(proof, &fixture.fence).is_err());
        let proof = fixture.prepare();
        let mut changed = fixture.fence.clone();
        changed.mutation_epoch += 1;
        assert!(fixture.events.publish_prepared_history_checkpoint(proof, &changed).is_err());
        let proof = fixture.prepare();
        let payload = proof.payload().clone();
        fixture.events.compare_and_put_projection_checkpoint_fenced(
            "projection-history:prepared-reuse", 0, 0, &payload, 0, &fixture.fence,
        ).unwrap();
        assert!(fixture.events.publish_prepared_history_checkpoint(proof, &fixture.fence).is_err());
    }

    #[test]
    fn prepared_checkpoint_rejects_missing_content_and_bad_body_owner() {
        let fixture = Fixture::new(true);
        let proof = fixture.prepare();
        let selector = proof.payload()["body"]["body"]["artifact"]["selector"].as_str().unwrap();
        let id = selector.strip_prefix("artifact://").unwrap();
        assert_eq!(fixture.repository.remove_record(id).unwrap_err(),
            "private prepared retirement requires exact roots");
        let record = fixture.repository.record(id).unwrap().unwrap();
        let (object, _) = fixture.repository.checkpoint_object_metadata(&record.sha256).unwrap().unwrap();
        assert_eq!(fixture.repository.retire_prepared(&crate::PreparedArtifactCandidate {
            record, generation: object.generation,
        }).unwrap(), crate::PreparedArtifactRetirement::Removed);
        assert!(fixture.repository.record(id).unwrap().is_none());
        assert!(fixture.events.publish_prepared_history_checkpoint(proof, &fixture.fence).is_err());
        let fixture = Fixture::new(true);
        let mut bad = fixture.encoded.clone();
        bad["state"]["source_fence"]["mutation_epoch"] = serde_json::json!(1);
        assert!(fixture.events.prepare_history_checkpoint("projection-history:prepared-reuse", 0, 0,
            "bad-source", fixture.payload.clone(), &bad).is_err());
        assert!(fixture.events.projection_checkpoint("projection-history:prepared-reuse").unwrap().is_none());
    }
}

#[cfg(test)]
mod checkpoint_pool_root_551_tests {
    use super::*;

    #[test]
    fn history_pool_checkpoint_keeps_private_root_scope_source_and_absent_gc() {
        let directory = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::for_test(directory.path().join("artifacts"), Default::default())
                .unwrap(),
        );
        let events = RuntimeEventStore::for_test();
        events.bind_artifact_store(artifacts).unwrap();
        let projection_id = "projection-history:checkpoint-root-551";
        let origin = PreparedArtifactOrigin::CheckpointBody {
            projection_id: projection_id.into(),
            source_cursor: 0,
        };
        let body = serde_json::json!({
            "payloads": ["checkpoint-root-551".repeat(2_000)],
            "state": {"through":0,"identity":"root-classification-only"}
        });
        let envelope = events
            .prepare_content(
                "session:checkpoint-root-551",
                "checkpoint-root-551",
                &body,
                &origin,
            )
            .unwrap();
        assert_eq!(envelope["body"]["kind"], "artifact_ref");
        let artifact: ArtifactRef =
            serde_json::from_value(envelope["body"]["artifact"].clone()).unwrap();
        let payload = serde_json::json!({
            "schema":4,"scope":"session:checkpoint-root-551","body":envelope
        });
        let saved = events
            .compare_and_put_projection_checkpoint(projection_id, 0, 0, &payload, 0)
            .unwrap();
        assert_eq!(
            events
                .backend
                .prepared_content_root(&origin, &artifact)
                .unwrap(),
            PreparedContentRoot::Checkpoint
        );
        let foreign_source = PreparedArtifactOrigin::CheckpointBody {
            projection_id: projection_id.into(),
            source_cursor: 1,
        };
        assert_eq!(
            events
                .backend
                .prepared_content_root(&foreign_source, &artifact)
                .unwrap(),
            PreparedContentRoot::Blocked
        );
        let mut foreign_scope = artifact.clone();
        foreign_scope.visibility_scope = "session:foreign".into();
        assert_eq!(
            events
                .backend
                .prepared_content_root(&origin, &foreign_scope)
                .unwrap(),
            PreparedContentRoot::Blocked
        );
        let mut corrupt = payload;
        corrupt["schema"] = serde_json::json!(6);
        assert!(events
            .validate_checkpoint_content(projection_id, 0, &corrupt)
            .is_err());
        events
            .backend
            .compare_and_put_projection_checkpoint(projection_id, 0, saved.revision, &corrupt, 0)
            .unwrap();
        assert_eq!(
            events
                .backend
                .prepared_content_root(&origin, &artifact)
                .unwrap(),
            PreparedContentRoot::Blocked
        );
        events.delete_projection_checkpoint(projection_id).unwrap();
        assert_eq!(
            events
                .backend
                .prepared_content_root(&origin, &artifact)
                .unwrap(),
            PreparedContentRoot::Absent
        );
    }
}

#[cfg(test)]
mod verified_observation_tests {
    use super::*;

    #[test]
    fn observation_binding_is_fixed_and_resolves_original_inline_and_artifact_content() {
        let root = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(root.path()));
        let writer = RuntimeEventStore::for_test();
        writer.bind_artifact_store(artifacts.clone()).unwrap();
        writer.bind_artifact_store(artifacts.clone()).unwrap();
        let origin = PreparedArtifactOrigin::EventBody {
            stream_id: "graph:original".into(),
            idempotency_key: "original".into(),
            kind: "execution_graph.planned".into(),
        };
        let value = serde_json::json!({"body": "x".repeat(300_000)});
        let envelope = writer
            .prepare_content("session:owner", "original", &value, &origin)
            .unwrap();
        assert_eq!(envelope["body"]["kind"], "artifact_ref");
        let observer = RuntimeEventStore::for_test();
        assert_eq!(
            observer
                .resolve_content("session:owner", &envelope, &origin)
                .unwrap_err(),
            "MissingContentStore"
        );
        observer
            .bind_artifact_store_for_verified_observation(artifacts.clone())
            .unwrap();
        assert_eq!(
            observer
                .resolve_content("session:owner", &envelope, &origin)
                .unwrap(),
            value
        );
        assert!(observer.bind_artifact_store(artifacts.clone()).is_err());
        assert!(
            observer
                .bind_artifact_store_for_verified_observation(artifacts.clone())
                .is_err()
        );
        assert!(
            writer
                .bind_artifact_store_for_verified_observation(artifacts.clone())
                .is_err()
        );
        assert_eq!(
            writer
                .resolve_content("session:owner", &envelope, &origin)
                .unwrap(),
            value
        );
        let small = serde_json::json!({"inline": "literal"});
        let inline = writer
            .prepare_content("session:owner", "small", &small, &origin)
            .unwrap();
        assert_eq!(
            RuntimeEventStore::for_test()
                .resolve_content("session:owner", &inline, &origin)
                .unwrap(),
            small
        );
        assert!(
            observer
                .resolve_content("session:foreign", &envelope, &origin)
                .is_err()
        );
        let foreign = PreparedArtifactOrigin::EventBody {
            stream_id: "graph:foreign".into(),
            idempotency_key: "original".into(),
            kind: "execution_graph.planned".into(),
        };
        assert!(
            observer
                .resolve_content("session:owner", &envelope, &foreign)
                .is_err()
        );
        let mut tampered = envelope.clone();
        tampered["bytes"] = serde_json::json!(1);
        assert!(
            observer
                .resolve_content("session:owner", &tampered, &origin)
                .is_err()
        );
        let mut missing = envelope;
        missing["body"]["artifact"]["selector"] =
            serde_json::json!(format!("artifact://art_{}", "0".repeat(64)));
        assert!(
            observer
                .resolve_content("session:owner", &missing, &origin)
                .is_err()
        );
    }
}
