//! Lossless micro-compaction for in-flight conversation messages.
//!
//! Canonical tool output may only leave the context after a durable evidence
//! receipt exists. This stage therefore never applies size- or age-based
//! truncation. It only folds duplicate durable receipts, whose raw payload can
//! still be retrieved from the session evidence ledger.

use std::collections::HashMap;

use crate::{
    compression::Result,
    config::CompressionConfig,
    types::{MemoryEntry, Message},
};

/// Marker configuration for the lossless micro-compaction stage.
#[derive(Debug, Clone, Default)]
pub struct MicroCompactConfig;

impl MicroCompactConfig {
    #[must_use]
    pub fn from_config(_config: &CompressionConfig) -> Self {
        Self
    }
}

/// Stage-1 compactor. Raw content without a durable reference is immutable.
pub struct MicroCompactor {
    _config: MicroCompactConfig,
}

impl MicroCompactor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            _config: MicroCompactConfig,
        }
    }

    #[must_use]
    pub fn from_config(config: &CompressionConfig) -> Self {
        Self {
            _config: MicroCompactConfig::from_config(config),
        }
    }

    /// Fold older duplicate receipts. Legacy/raw messages remain byte-for-byte
    /// unchanged because they have no independently retrievable canonical raw.
    pub fn compact(&self, messages: &mut Vec<Message>) {
        let mut last_seen: HashMap<(String, String, String), usize> = HashMap::new();
        for (index, message) in messages.iter().enumerate() {
            let Some(receipt) = message.canonical_raw_evidence() else {
                continue;
            };
            let tool_name = message.tool_name.clone().unwrap_or_default();
            last_seen.insert(
                (
                    tool_name,
                    receipt.access.evidence_ref.0.id,
                    receipt.access.sha256,
                ),
                index,
            );
        }

        for (index, message) in messages.iter_mut().enumerate() {
            if message.pinned {
                continue;
            }
            let Some(receipt) = message.canonical_raw_evidence() else {
                continue;
            };
            let key = (
                message.tool_name.clone().unwrap_or_default(),
                receipt.access.evidence_ref.0.id.clone(),
                receipt.access.sha256.clone(),
            );
            if last_seen.get(&key).is_some_and(|last| *last > index) {
                let mut duplicate_receipt = receipt;
                duplicate_receipt.preview = format!(
                    "[duplicate durable evidence: {}:{}; retrieve canonical raw by ref]",
                    duplicate_receipt.access.evidence_ref.0.ref_type,
                    duplicate_receipt.access.evidence_ref.0.id
                );
                let replaced = message.replace_with_canonical_raw_receipt(&duplicate_receipt);
                debug_assert!(replaced);
            }
        }
    }
}

impl Default for MicroCompactor {
    fn default() -> Self {
        Self::new()
    }
}

/// Legacy entry-count compactor. This API does not handle tool-result bodies.
pub struct MicroCompressor {
    pub threshold: usize,
}

impl MicroCompressor {
    #[must_use]
    pub fn new(threshold: usize) -> Self {
        Self { threshold }
    }

    pub async fn compress(&self, entries: Vec<MemoryEntry>) -> Result<Vec<MemoryEntry>> {
        if entries.len() < self.threshold {
            return Ok(entries);
        }
        let mut entries = entries;
        entries.sort_by(|a, b| {
            a.staleness
                .partial_cmp(&b.staleness)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let keep = (entries.len() / 2).max(1);
        entries.truncate(keep);
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CanonicalRawEvidence, Message};
    use harness_contract::{context::EvidenceAccessRef, core::EvidenceRef};

    fn durable_message(id: &str, hash: &str, preview: &str) -> Message {
        let mut message = Message::tool_result(id, "read_file", preview);
        let receipt = CanonicalRawEvidence::new(
            EvidenceAccessRef::durable(
                EvidenceRef::durable(id),
                hash,
                preview.len() as u64,
                "text/plain",
                format!("retrieve {id}"),
                "session:test",
            ),
            preview,
        );
        assert!(message.replace_with_canonical_raw_receipt(&receipt));
        message
    }

    #[test]
    fn raw_tool_output_without_durable_ref_is_never_changed() {
        let raw = "x".repeat(100_000);
        let mut messages = vec![Message::tool_result("call-1", "bash", raw.clone())];
        MicroCompactor::new().compact(&mut messages);
        assert_eq!(messages[0].content, raw);
    }

    #[test]
    fn duplicate_durable_receipt_can_be_folded_without_losing_raw() {
        let mut messages = vec![
            durable_message("raw-1", "sha256:abc", "first"),
            durable_message("raw-1", "sha256:abc", "second"),
        ];
        MicroCompactor::new().compact(&mut messages);
        assert!(messages[0]
            .canonical_raw_evidence()
            .is_some_and(|receipt| receipt.preview.contains("duplicate durable evidence")));
        assert!(messages[1].canonical_raw_evidence().is_some());
    }
}
