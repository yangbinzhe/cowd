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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryLifecycleMutation {
    pub mutation_id: String,
    pub scope_key: String,
    pub expected_entry_digest: String,
    pub expected_history: Option<String>,
    pub event: MemoryLifecycleEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryCleanupPending {
    pub mutation_id: String,
    pub memory_id: MemoryId,
    pub source: MemorySourceFence,
    pub knowledge_confirmed: bool,
    pub auxiliary_confirmed: bool,
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
}

pub fn memory_entry_digest(entry: &MemoryEntry) -> Result<String> {
    typed_digest("cowd.memory.entry.v1", entry)
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

impl MemoryLifecycleMutation {
    /// Stable command identity excludes observation time and the mutable read
    /// cut. Retrying an acknowledged or response-lost command returns its own
    /// original receipt; a different command cannot borrow that receipt.
    pub fn new(
        entry: &MemoryEntry,
        expected_history: Option<String>,
        event: MemoryLifecycleEvent,
    ) -> Result<Self> {
        let scope_key = entry.scope.scope_key();
        let mutation_id = typed_digest(
            "cowd.memory.lifecycle.id.v1",
            &(
                &scope_key,
                event.memory_id,
                &event.session_id,
                &event.agent_id,
                event.to,
                &event.reason,
            ),
        )?;
        Ok(Self {
            mutation_id,
            scope_key,
            expected_entry_digest: memory_entry_digest(entry)?,
            expected_history,
            event,
        })
    }

    pub fn command_digest(&self) -> Result<String> {
        typed_digest(
            "cowd.memory.lifecycle.command.v1",
            &(
                &self.mutation_id,
                &self.scope_key,
                self.event.memory_id,
                &self.event.session_id,
                &self.event.agent_id,
                self.event.to,
                &self.event.reason,
            ),
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
            || memory_entry_digest(entry)? != self.expected_entry_digest
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
                knowledge_confirmed: false,
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
