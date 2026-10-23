//! Original Memory lifecycle transaction inputs and durable derived-cleanup facts.
//! These records belong to MemoryStore, never to a Gateway retry or a projection.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::Result;
use crate::{MemoryEntry, MemoryError, MemoryId, MemoryLifecycleEvent, MemoryState};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySourceFence {
    pub source_ref: String,
    pub scope_key: String,
    pub delete_epoch: u64,
    pub retired: bool,
}

impl MemorySourceFence {
    /// Reserve borrowed metadata before cloning it into a source snapshot.
    pub fn read_bytes(&self, budget_bytes: usize) -> Result<usize> {
        reserved_source_bytes(self, budget_bytes)
    }

    /// A committed typed retirement never becomes live again. Later epochs
    /// preserve all earlier retirement proofs within the exact immutable scope.
    pub fn verifies_retirement(&self, expected: &Self) -> Result<()> {
        let id = expected
            .source_ref
            .strip_prefix("memory:")
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .ok_or_else(|| MemoryError::Store("invalid Memory retirement source".into()))?;
        if expected.source_ref != format!("memory:{id}")
            || !expected.retired
            || expected.delete_epoch == 0
            || expected.scope_key.is_empty()
            || self.source_ref != expected.source_ref
            || self.scope_key != expected.scope_key
            || !self.retired
            || self.delete_epoch < expected.delete_epoch
        {
            return Err(MemoryError::Store(
                "foreign or stale Memory retirement proof".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryLifecycleMutation {
    pub mutation_id: String,
    pub scope_key: String,
    pub expected_entry_digest: String,
    pub expected_history: Option<String>,
    pub event: MemoryLifecycleEvent,
    #[serde(default = "origin_revocation_default", skip_deserializing)]
    revoke_origin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryCleanupPending {
    pub mutation_id: String,
    pub memory_id: MemoryId,
    pub source: MemorySourceFence,
    #[serde(default)]
    pub knowledge_receipt_digest: Option<String>,
    pub auxiliary_confirmed: bool,
}

impl MemoryCleanupPending {
    pub fn validate(&self) -> Result<()> {
        self.source.verifies_retirement(&self.source)?;
        if self.mutation_id.is_empty()
            || self.source.source_ref != format!("memory:{}", self.memory_id)
            || self
                .knowledge_receipt_digest
                .as_ref()
                .is_some_and(|digest| {
                    digest.len() != 64
                        || !digest
                            .bytes()
                            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                })
        {
            return Err(MemoryError::Store("invalid Memory cleanup binding".into()));
        }
        Ok(())
    }

    pub fn confirm_knowledge(
        &mut self,
        receipt: Option<&crate::knowledge::MemorySourceQuarantineReceipt>,
    ) -> Result<()> {
        self.validate()?;
        if let Some(receipt) = receipt {
            let digest = receipt
                .digest()
                .map_err(|_| MemoryError::Store("invalid Knowledge retirement receipt".into()))?;
            if receipt.source != self.source
                || self
                    .knowledge_receipt_digest
                    .as_ref()
                    .is_some_and(|old| old != &digest)
            {
                return Err(MemoryError::Store(
                    "foreign Knowledge retirement receipt".into(),
                ));
            }
            self.knowledge_receipt_digest = Some(digest);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryLifecycleReceipt {
    pub mutation_id: String,
    pub command_digest: String,
    pub event: MemoryLifecycleEvent,
    pub source: MemorySourceFence,
}

/// A synchronous, narrow read port for the synchronous Knowledge owner. Both
/// adapters read their original Memory state; no writable mirror is permitted.
pub trait MemorySourceFenceReader: Send + Sync {
    fn read_source_fence(&self, source_ref: &str) -> Result<Option<MemorySourceFence>>;

    /// Original live body cut, reserved before materialization. A missing or
    /// retired source cannot authorize a newly prepared derived write.
    fn read_source_revision(
        &self,
        source_ref: &str,
        budget_bytes: usize,
    ) -> Result<Option<MemorySourceRevisionRead>>;

    /// Typed retirement is irreversible in the original owner. An older epoch
    /// remains retired after a later retirement; it must not strand its own
    /// durable cleanup obligation. Legacy epoch-zero observations are not proofs.
    fn verify_retirement(
        &self,
        expected: &MemorySourceFence,
    ) -> std::result::Result<VerifiedMemoryRetirement, MemoryRetirementVerificationError> {
        expected
            .verifies_retirement(expected)
            .map_err(|_| MemoryRetirementVerificationError::Stale)?;
        let current = self
            .read_source_fence(&expected.source_ref)
            .map_err(MemoryRetirementVerificationError::Read)?
            .ok_or(MemoryRetirementVerificationError::Unknown)?;
        current
            .verifies_retirement(expected)
            .map_err(|_| MemoryRetirementVerificationError::Stale)?;
        Ok(VerifiedMemoryRetirement {
            source: expected.clone(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryRetirementVerificationError {
    #[error("Memory retirement source read failed")]
    Read(#[source] MemoryError),
    #[error("Memory retirement source is unknown")]
    Unknown,
    #[error("Memory retirement source does not match the proof")]
    Stale,
}

/// Transient proof from the original Memory reader, never deserializable input
/// or a writable mirror. Its constructor stays inside this owner module.
#[derive(Debug, Clone)]
pub struct VerifiedMemoryRetirement {
    source: MemorySourceFence,
}
impl VerifiedMemoryRetirement {
    pub fn source(&self) -> &MemorySourceFence {
        &self.source
    }
}

/// Read-only source facts, never an authorization grant. The current caller's
/// scope and cognitive read leases are checked at the actual use boundary.
#[derive(Debug, Clone)]
pub struct MemorySourceAccess {
    pub scope: crate::MemoryScope,
    pub visibility: crate::AgentVisibility,
    pub source_agent: Option<String>,
    pub layer: crate::MemoryLayer,
    pub source: crate::MemorySource,
    pub always_active: bool,
}
impl MemorySourceAccess {
    pub fn of(entry: &MemoryEntry) -> Self {
        Self {
            scope: entry.scope.clone(),
            visibility: entry.visibility.clone(),
            source_agent: entry.source_agent.clone(),
            layer: entry.layer,
            source: entry.source,
            always_active: entry
                .tags
                .iter()
                .any(|tag| matches!(tag.as_str(), "memory-policy:always" | "always-active")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemorySourceRevisionRead {
    pub origin_retired: bool,
    /// Facts read from the original body in the same cut as its revision.
    pub access: Option<MemorySourceAccess>,
    pub packet_content_digest: Option<String>,
    pub packet_atom: Option<crate::MemoryAtomView>,
    pub revision: Option<MemoryEntryRevision>,
    pub fence: MemorySourceFence,
    pub materialized_bytes: usize,
}

pub(crate) fn reserved_source_bytes(value: &impl Serialize, budget_bytes: usize) -> Result<usize> {
    struct Reserved {
        used: usize,
        limit: usize,
    }
    impl std::io::Write for Reserved {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next = self
                .used
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| std::io::Error::other("Memory source read budget exceeded"))?;
            self.used = next;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut reserved = Reserved {
        used: 0,
        limit: budget_bytes,
    };
    serde_json::to_writer(&mut reserved, value).map_err(|error| {
        if error.is_io() {
            MemoryError::SourceReadLimit
        } else {
            MemoryError::Serialisation(error)
        }
    })?;
    Ok(reserved.used)
}

/// Shared native predicate; callers must hold the original body/history/fence
/// read cut until their dependent commit, rather than reloading a later head.
pub fn live_source_revision(
    source_ref: &str,
    entry: &MemoryEntry,
    history: Option<&str>,
    fence: Option<&MemorySourceFence>,
    budget_bytes: usize,
) -> Result<MemorySourceRevisionRead> {
    let materialized_bytes = reserved_source_bytes(&(entry, history, fence), budget_bytes)?;
    if source_ref != format!("memory:{}", entry.id)
        || fence.is_some_and(|fence| {
            fence.source_ref != source_ref || fence.scope_key != entry.scope.scope_key()
        })
    {
        return Err(MemoryError::Store(
            "Memory source/body identity mismatch".into(),
        ));
    }
    let mut source = fence.cloned().unwrap_or(MemorySourceFence {
        source_ref: source_ref.into(),
        scope_key: entry.scope.scope_key(),
        delete_epoch: 0,
        retired: false,
    });
    source.retired |= lifecycle_retired(entry.id, history)?;
    let mut atom =
        crate::MemoryAtomView::from_entry(entry, crate::MemoryInformationState::Orientation);
    if let Some(history) = history {
        let events: Vec<MemoryLifecycleEvent> = serde_json::from_str(history)?;
        if let Some(event) = events.last() {
            atom.state = event.to;
        }
    }
    Ok(MemorySourceRevisionRead {
        origin_retired: false,
        packet_atom: Some(atom),
        access: (!source.retired).then(|| MemorySourceAccess::of(entry)),
        packet_content_digest: (!source.retired).then(|| {
            crate::kernel::packet_content_digest(
                &entry.title,
                &crate::kernel::truncate_memory_content_preview(&entry.content),
            )
        }),
        revision: if source.retired {
            None
        } else {
            Some(MemoryEntryRevision::of(entry)?)
        },
        fence: source,
        materialized_bytes,
    })
}

/// Immutable identity of one automatic extraction input in the original owner.
/// Retirement is recorded against this origin, so a late paraphrase/new UUID
/// cannot turn the same deleted input into a fresh source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryExtractionOrigin {
    scope_key: String,
    session_id: String,
    agent_id: String,
    batch_tag: String,
}
impl MemoryExtractionOrigin {
    pub fn for_entry(
        turn: &crate::MemoryTurnContext,
        entry: &MemoryEntry,
        batch_tag: &str,
    ) -> Result<Self> {
        let origin = Self {
            scope_key: entry.scope.scope_key(),
            session_id: turn.session_id.clone(),
            agent_id: turn.agent_id.clone(),
            batch_tag: batch_tag.into(),
        };
        origin.validate_entry(entry)?;
        Ok(origin)
    }
    pub fn validate_entry(&self, entry: &MemoryEntry) -> Result<()> {
        let batch = self
            .batch_tag
            .strip_prefix("extraction-batch:")
            .and_then(|id| uuid::Uuid::parse_str(id).ok());
        if batch.is_none_or(|id| self.batch_tag != format!("extraction-batch:{id}"))
            || entry.source != crate::MemorySource::AutoExtracted
            || self.scope_key != entry.scope.scope_key()
            || entry.session_id.as_deref() != Some(self.session_id.as_str())
            || entry.source_agent.as_deref() != Some(self.agent_id.as_str())
            || !entry.tags.contains(&self.batch_tag)
        {
            return Err(MemoryError::StaleSource);
        }
        Ok(())
    }
    pub fn binding_key(id: MemoryId) -> String {
        format!("memory_extraction_origin:{id}")
    }
    pub fn retirement_fence(&self, original: &MemorySourceFence) -> Result<MemorySourceFence> {
        Ok(MemorySourceFence {
            source_ref: format!("memory-origin:{}", self.key()?),
            scope_key: self.scope_key.clone(),
            delete_epoch: original.delete_epoch,
            retired: true,
        })
    }
    pub fn retirement_ref(&self) -> Result<String> {
        Ok(format!("memory-origin:{}", self.key()?))
    }
    pub fn key(&self) -> Result<String> {
        let mut hash = Sha256::new();
        hash.update(b"cowd:memory-extraction-origin:v1\0");
        hash.update(serde_json::to_vec(self)?);
        Ok(format!("sha256:{:x}", hash.finalize()))
    }
}

/// A compare-and-swap witness for the original body, not an authorization grant.
/// Accepted-use observations do not change the body's semantic revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryEntryRevision {
    pub memory_id: MemoryId,
    pub scope_key: String,
    pub body_digest: String,
}

impl MemoryEntryRevision {
    pub fn of(entry: &MemoryEntry) -> Result<Self> {
        let mut body = serde_json::to_value(entry).map_err(MemoryError::Serialisation)?;
        if let Some(body) = body.as_object_mut() {
            body.remove("access_count");
            body.remove("last_accessed_at");
        }
        Ok(Self {
            memory_id: entry.id,
            scope_key: entry.scope.scope_key(),
            body_digest: typed_digest("cowd.memory.body-revision.v1", &body)?,
        })
    }

    pub fn validate_replace(
        &self,
        current: &MemoryEntry,
        next: &MemoryEntry,
    ) -> Result<MemoryEntry> {
        if self != &Self::of(current)?
            || next.id != self.memory_id
            || next.scope.scope_key() != self.scope_key
        {
            return Err(MemoryError::Store(
                "stale or foreign Memory target revision".into(),
            ));
        }
        let mut next = next.clone();
        next.access_count = next.access_count.max(current.access_count);
        next.last_accessed_at = next.last_accessed_at.max(current.last_accessed_at);
        Ok(next)
    }
}

/// Legacy/imported lifecycle bytes are still source evidence. Missing new
/// receipt tables cannot turn an archived old snapshot into an active source.
pub fn lifecycle_retired(memory_id: MemoryId, raw: Option<&str>) -> Result<bool> {
    let events: Vec<MemoryLifecycleEvent> = raw
        .map(serde_json::from_str)
        .transpose()
        .map_err(MemoryError::Serialisation)?
        .unwrap_or_default();
    if events.iter().any(|event| event.memory_id != memory_id) {
        return Err(MemoryError::Store(
            "foreign Memory lifecycle history".into(),
        ));
    }
    Ok(events
        .last()
        .is_some_and(|event| matches!(event.to, MemoryState::Archived | MemoryState::Superseded)))
}

fn typed_digest(domain: &str, value: &impl Serialize) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(serde_json::to_vec(value).map_err(MemoryError::Serialisation)?);
    Ok(format!("{:x}", digest.finalize()))
}

fn origin_revocation_default() -> bool {
    true
}

impl MemoryLifecycleMutation {
    /// A retry carries the original source cut. Observation time is assigned by
    /// the first commit, while a later transition over a different history is
    /// a different command even when its actor, reason and target state match.
    pub fn new(
        entry: &MemoryEntry,
        expected_history: Option<String>,
        event: MemoryLifecycleEvent,
    ) -> Result<Self> {
        let mut command = Self {
            mutation_id: String::new(),
            scope_key: entry.scope.scope_key(),
            expected_entry_digest: MemoryEntryRevision::of(entry)?.body_digest,
            expected_history,
            event,
            revoke_origin: true,
        };
        let mutation_id = if matches!(
            command.event.to,
            MemoryState::Archived | MemoryState::Superseded
        ) {
            // Irreversible retirement is a stable user operation. Confirming it
            // later uses the original receipt, never a newly rebound read cut.
            typed_digest(
                "cowd.memory.retirement.id.v2",
                &(
                    &command.scope_key,
                    command.event.memory_id,
                    &command.event.session_id,
                    &command.event.agent_id,
                    command.event.to,
                    &command.event.reason,
                ),
            )?
        } else {
            typed_digest("cowd.memory.lifecycle.id.v2", &command.input_cut())?
        };
        command.mutation_id = mutation_id;
        Ok(command)
    }

    /// Retiring a redundant projection is not revoking its original input.
    pub(crate) fn redundant_projection(
        entry: &MemoryEntry,
        history: Option<String>,
        event: MemoryLifecycleEvent,
    ) -> Result<Self> {
        let mut command = Self::new(entry, history, event)?;
        command.revoke_origin = false;
        let mutation_id = typed_digest(
            "cowd.memory.redundant-projection.id.v1",
            &command.input_cut(),
        )?;
        command.mutation_id = mutation_id;
        Ok(command)
    }
    pub fn revokes_origin(&self) -> bool {
        self.revoke_origin
    }

    fn input_cut(&self) -> impl Serialize + '_ {
        (
            &self.scope_key,
            &self.expected_entry_digest,
            &self.expected_history,
            self.event.memory_id,
            self.event.from,
            self.event.to,
            &self.event.session_id,
            &self.event.agent_id,
            &self.event.reason,
        )
    }

    pub fn command_digest(&self) -> Result<String> {
        typed_digest(
            if self.revoke_origin {
                "cowd.memory.lifecycle.command.v2"
            } else {
                "cowd.memory.redundant-projection.command.v1"
            },
            &(&self.mutation_id, self.input_cut()),
        )
    }

    pub fn validate_replay(&self, receipt: &MemoryLifecycleReceipt) -> Result<()> {
        if receipt.mutation_id != self.mutation_id
            || receipt.command_digest != self.command_digest()?
            || receipt.event.memory_id != self.event.memory_id
            || receipt.source.scope_key != self.scope_key
        {
            return Err(MemoryError::Store(
                "Memory lifecycle mutation identity collision".into(),
            ));
        }
        Ok(())
    }

    /// A read-only confirmation of an irreversible original operation is not
    /// admission of this newly assembled command or its current read cut.
    pub fn validate_retirement_confirmation(&self, receipt: &MemoryLifecycleReceipt) -> Result<()> {
        receipt.source.verifies_retirement(&receipt.source)?;
        if !matches!(
            self.event.to,
            MemoryState::Archived | MemoryState::Superseded
        ) || receipt.mutation_id != self.mutation_id
            || receipt.source.source_ref != format!("memory:{}", self.event.memory_id)
            || receipt.source.scope_key != self.scope_key
            || receipt.event.memory_id != self.event.memory_id
            || receipt.event.to != self.event.to
            || receipt.event.session_id != self.event.session_id
            || receipt.event.agent_id != self.event.agent_id
            || receipt.event.reason != self.event.reason
        {
            return Err(MemoryError::Store(
                "foreign Memory retirement confirmation".into(),
            ));
        }
        Ok(())
    }

    /// Called inside the native store transaction/guard, over its actual body,
    /// history and source epoch. All validation precedes native writes.
    pub fn validate_cut(
        &self,
        entry: &MemoryEntry,
        history: Option<&str>,
        source: Option<&MemorySourceFence>,
    ) -> Result<(String, MemoryLifecycleReceipt, Option<MemoryCleanupPending>)> {
        if self.mutation_id.is_empty()
            || self.event.session_id.is_empty()
            || self.event.agent_id.is_empty()
            || entry.id != self.event.memory_id
            || entry.scope.scope_key() != self.scope_key
            || MemoryEntryRevision::of(entry)?.body_digest != self.expected_entry_digest
            || history != self.expected_history.as_deref()
        {
            return Err(MemoryError::Store(
                "stale or foreign Memory lifecycle read set".into(),
            ));
        }
        let mut events: Vec<MemoryLifecycleEvent> = history
            .map(serde_json::from_str)
            .transpose()
            .map_err(MemoryError::Serialisation)?
            .unwrap_or_default();
        if events.last().map(|e| e.to) != self.event.from
            || events.iter().any(|e| e.memory_id != entry.id)
        {
            return Err(MemoryError::Store(
                "Memory lifecycle history identity/state mismatch".into(),
            ));
        }
        let mut fence = source.cloned().unwrap_or(MemorySourceFence {
            source_ref: format!("memory:{}", entry.id),
            scope_key: self.scope_key.clone(),
            delete_epoch: 0,
            retired: false,
        });
        if fence.scope_key != self.scope_key || fence.source_ref != format!("memory:{}", entry.id) {
            return Err(MemoryError::Store("foreign Memory source fence".into()));
        }
        fence.retired |= lifecycle_retired(entry.id, history)?;
        let retiring = matches!(
            self.event.to,
            MemoryState::Archived | MemoryState::Superseded
        );
        if fence.retired && !retiring {
            return Err(MemoryError::Store(
                "retired Memory source cannot be reactivated by lifecycle append".into(),
            ));
        }
        let pending = if retiring {
            fence.delete_epoch = fence
                .delete_epoch
                .checked_add(1)
                .ok_or_else(|| MemoryError::Store("Memory delete epoch overflow".into()))?;
            fence.retired = true;
            Some(MemoryCleanupPending {
                mutation_id: self.mutation_id.clone(),
                memory_id: entry.id,
                source: fence.clone(),
                knowledge_receipt_digest: None,
                auxiliary_confirmed: false,
            })
        } else {
            None
        };
        events.push(self.event.clone());
        let raw = serde_json::to_string(&events).map_err(MemoryError::Serialisation)?;
        let receipt = MemoryLifecycleReceipt {
            mutation_id: self.mutation_id.clone(),
            command_digest: self.command_digest()?,
            event: self.event.clone(),
            source: fence,
        };
        Ok((raw, receipt, pending))
    }
}
