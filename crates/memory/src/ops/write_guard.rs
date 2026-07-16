//! Memory write guard — controls which sources may write to which memory layers.
//!
//! This module implements the anti-corruption layer for the memory system.
//! It prevents untrusted sources (sub-agents, tools) from modifying
//! protected memory layers (L0 identity, L1 working memory).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

use crate::types::MemoryLayer;

// ---------------------------------------------------------------------------
// WriteSource
// ---------------------------------------------------------------------------

/// Origin of a memory write request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WriteSource {
    /// User direct input — full access.
    User,
    /// AI assistant reply — may write L1-L3.
    Assistant,
    /// Tool output — may write L2-L3.
    Tool,
    /// Sub-agent — session-level only.
    SubAgent,
    /// Scheduled / cron task — may write L3 only.
    Cron,
    /// System internal — full access.
    System,
}

// ---------------------------------------------------------------------------
// WritePolicy
// ---------------------------------------------------------------------------

/// Decision returned by the write guard for a given request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WritePolicy {
    /// Write is allowed.
    Allow,
    /// Write is allowed but must be recorded in the audit log.
    AllowWithAudit,
    /// Source may only read; write is denied.
    ReadOnly,
    /// Write is completely denied.
    Deny,
}

impl WritePolicy {
    /// Returns `true` if the policy permits the write.
    pub fn is_allowed(self) -> bool {
        matches!(self, WritePolicy::Allow | WritePolicy::AllowWithAudit)
    }

    /// Returns `true` if the policy requires audit logging.
    pub fn requires_audit(self) -> bool {
        matches!(self, WritePolicy::AllowWithAudit)
    }
}

// ---------------------------------------------------------------------------
// MemoryWriteGuard
// ---------------------------------------------------------------------------

/// Controls write access to memory layers based on the source of the request.
///
/// # Default layer permissions
///
/// | Source     | L0 | L1 | L2 | L3 | L4 | Session |
/// |-----------|----|----|----|----|----|---------|
/// | User      | ✓  | ✓  | ✓  | ✓  | ✓  | ✓       |
/// | Assistant |    | ✓  | ✓  | ✓  | ✓  | ✓       |
/// | Tool      |    |    | ✓  | ✓  |    | ✓       |
/// | SubAgent  |    |    | ✓  |    |    | ✓       |
/// | Cron      |    |    |    | ✓  |    |          |
/// | System    | ✓  | ✓  | ✓  | ✓  | ✓  | ✓       |
#[derive(Debug, Clone)]
pub struct MemoryWriteGuard {
    source: WriteSource,
    allowed_layers: HashSet<MemoryLayer>,
    /// Whether audit logging is mandatory for all writes from this source.
    audit_all: bool,
}

impl MemoryWriteGuard {
    /// Create a guard with default permissions for the given source.
    pub fn new(source: WriteSource) -> Self {
        let allowed_layers = default_allowed_layers(source);
        let audit_all = matches!(source, WriteSource::SubAgent | WriteSource::Tool);
        Self {
            source,
            allowed_layers,
            audit_all,
        }
    }

    /// Create a guard for a sub-agent with custom allowed layers.
    pub fn for_sub_agent(extra_layers: impl IntoIterator<Item = MemoryLayer>) -> Self {
        let mut layers: HashSet<MemoryLayer> = [
            MemoryLayer::L2,
            MemoryLayer::L3, // session-scoped layer
        ]
        .into_iter()
        .collect();
        for l in extra_layers {
            layers.insert(l);
        }
        Self {
            source: WriteSource::SubAgent,
            allowed_layers: layers,
            audit_all: true,
        }
    }

    /// Check whether a write to `layer` is permitted, returning the policy.
    pub fn check_write(&self, layer: MemoryLayer) -> WritePolicy {
        if self.allowed_layers.contains(&layer) {
            if self.audit_all {
                WritePolicy::AllowWithAudit
            } else {
                WritePolicy::Allow
            }
        } else {
            WritePolicy::Deny
        }
    }

    /// Convenience: returns `true` if the write is allowed (with or without audit).
    pub fn is_write_allowed(&self, layer: MemoryLayer) -> bool {
        self.check_write(layer).is_allowed()
    }

    /// The source this guard represents.
    pub fn source(&self) -> WriteSource {
        self.source
    }

    /// Whether all writes from this source require audit logging.
    pub fn audit_all(&self) -> bool {
        self.audit_all
    }
}

/// Default layer permissions per source.
fn default_allowed_layers(source: WriteSource) -> HashSet<MemoryLayer> {
    match source {
        WriteSource::User | WriteSource::System => [
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
        ]
        .into_iter()
        .collect(),
        WriteSource::Assistant => [MemoryLayer::L1, MemoryLayer::L2, MemoryLayer::L3]
            .into_iter()
            .collect(),
        WriteSource::Tool => [MemoryLayer::L2, MemoryLayer::L3].into_iter().collect(),
        WriteSource::SubAgent => [MemoryLayer::L2, MemoryLayer::L3].into_iter().collect(),
        WriteSource::Cron => [MemoryLayer::L3].into_iter().collect(),
    }
}

// ---------------------------------------------------------------------------
// AuditEntry & AuditLog
// ---------------------------------------------------------------------------

/// Operation type for an audit entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AuditOperation {
    Create,
    Update,
    Delete,
}

/// A single audit record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: DateTime<Utc>,
    pub operation: AuditOperation,
    pub entry_id: String,
    pub layer: String,
    pub source: WriteSource,
    /// Short content summary (not full content, for privacy).
    pub summary: String,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
}

/// Append-only audit log backed by a JSONL file.
#[derive(Debug)]
pub struct AuditLog {
    path: PathBuf,
}

impl AuditLog {
    /// Open (or create) the audit log at `path`.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Create file if it doesn't exist
        if !path.exists() {
            std::fs::File::create(&path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o600);
                std::fs::set_permissions(&path, perms)?;
            }
        }
        Ok(Self { path })
    }

    /// Append an audit entry.
    pub fn log(&self, entry: &AuditEntry) -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new().append(true).open(&self.path)?;
        let json = serde_json::to_string(entry)?;
        writeln!(file, "{}", json)?;
        Ok(())
    }

    /// Query the most recent `n` entries.
    pub fn query_recent(&self, n: usize) -> std::io::Result<Vec<AuditEntry>> {
        let content = std::fs::read_to_string(&self.path)?;
        let entries: Vec<AuditEntry> = content
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        let start = entries.len().saturating_sub(n);
        Ok(entries[start..].to_vec())
    }

    /// Query entries by source type.
    pub fn query_by_source(&self, source: WriteSource) -> std::io::Result<Vec<AuditEntry>> {
        let content = std::fs::read_to_string(&self.path)?;
        Ok(content
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .filter(|e: &AuditEntry| e.source == source)
            .collect())
    }

    /// Query entries within a time range.
    pub fn query_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> std::io::Result<Vec<AuditEntry>> {
        let content = std::fs::read_to_string(&self.path)?;
        Ok(content
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .filter(|e: &AuditEntry| e.timestamp >= start && e.timestamp <= end)
            .collect())
    }

    /// Return the file path.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

// ---------------------------------------------------------------------------
// IntegrityChecker
// ---------------------------------------------------------------------------

/// Result of an anomaly scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyReport {
    pub timestamp: DateTime<Utc>,
    pub anomalies: Vec<Anomaly>,
}

/// A single detected anomaly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Anomaly {
    /// Too many deletes in a short window.
    RapidDeletion {
        count: usize,
        window_secs: u64,
        source: WriteSource,
    },
    /// A protected layer was modified by a non-system source.
    ProtectedLayerModified {
        layer: String,
        source: WriteSource,
        entry_id: String,
    },
    /// An entity relation flip-flopped (changed back and forth).
    RelationOscillation {
        subject: String,
        predicate: String,
        flip_count: usize,
    },
}

/// Checks for anomalous write patterns in the audit log.
#[derive(Debug)]
pub struct IntegrityChecker {
    audit_log: AuditLog,
    /// Threshold for "rapid deletion" detection: max deletes per window.
    pub rapid_deletion_threshold: usize,
    /// Window in seconds for rapid deletion detection.
    pub rapid_deletion_window_secs: u64,
}

impl IntegrityChecker {
    /// Create a checker backed by the given audit log.
    pub fn new(audit_log: AuditLog) -> Self {
        Self {
            audit_log,
            rapid_deletion_threshold: 10,
            rapid_deletion_window_secs: 60,
        }
    }

    /// Scan the audit log for anomalies.
    pub fn check_anomalies(&self) -> std::io::Result<AnomalyReport> {
        let entries = self.audit_log.query_recent(1000)?;
        let mut anomalies = Vec::new();

        // 1. Rapid deletion detection
        let deletes: Vec<&AuditEntry> = entries
            .iter()
            .filter(|e| e.operation == AuditOperation::Delete)
            .collect();

        if deletes.len() >= self.rapid_deletion_threshold {
            // Check if all deletes are within the window
            if let (Some(first), Some(last)) = (deletes.first(), deletes.last()) {
                let window = chrono::Duration::seconds(self.rapid_deletion_window_secs as i64);
                if last.timestamp - first.timestamp <= window {
                    anomalies.push(Anomaly::RapidDeletion {
                        count: deletes.len(),
                        window_secs: self.rapid_deletion_window_secs,
                        source: deletes[0].source,
                    });
                }
            }
        }

        // 2. Protected layer modification by non-system source
        for entry in &entries {
            if entry.source != WriteSource::System && entry.source != WriteSource::User
                && (entry.layer == "L0" || entry.layer == "L1") {
                    anomalies.push(Anomaly::ProtectedLayerModified {
                        layer: entry.layer.clone(),
                        source: entry.source,
                        entry_id: entry.entry_id.clone(),
                    });
                }
        }

        // 3. Relation oscillation is detected at a higher level (TemporalGraph)
        // We just report the anomaly framework here.

        Ok(AnomalyReport {
            timestamp: Utc::now(),
            anomalies,
        })
    }

    /// Mark an entry as suspended (append a special audit entry).
    pub fn suspend_entry(&self, entry_id: &str, reason: &str) -> std::io::Result<()> {
        self.audit_log.log(&AuditEntry {
            timestamp: Utc::now(),
            operation: AuditOperation::Update,
            entry_id: entry_id.to_string(),
            layer: "suspended".to_string(),
            source: WriteSource::System,
            summary: format!("SUSPENDED: {}", reason),
            agent_id: None,
            session_id: None,
        })
    }

    /// Access the underlying audit log.
    pub fn audit_log(&self) -> &AuditLog {
        &self.audit_log
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_cannot_bypass_governed_l4_promotion_boundary() {
        let guard = MemoryWriteGuard::new(WriteSource::User);
        for layer in [
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
        ] {
            assert!(
                guard.is_write_allowed(layer),
                "User should write {:?}",
                layer
            );
        }
        assert!(
            !guard.is_write_allowed(MemoryLayer::L4),
            "L4 must only be written through the governed promotion service"
        );
    }

    #[test]
    fn sub_agent_cannot_write_l0_l1() {
        let guard = MemoryWriteGuard::new(WriteSource::SubAgent);
        assert!(!guard.is_write_allowed(MemoryLayer::L0));
        assert!(!guard.is_write_allowed(MemoryLayer::L1));
        assert!(guard.is_write_allowed(MemoryLayer::L2));
        assert!(guard.is_write_allowed(MemoryLayer::L3));
    }

    #[test]
    fn tool_cannot_write_l0_l1_l4() {
        let guard = MemoryWriteGuard::new(WriteSource::Tool);
        assert!(!guard.is_write_allowed(MemoryLayer::L0));
        assert!(!guard.is_write_allowed(MemoryLayer::L1));
        assert!(guard.is_write_allowed(MemoryLayer::L2));
        assert!(!guard.is_write_allowed(MemoryLayer::L4));
    }

    #[test]
    fn cron_only_writes_l3() {
        let guard = MemoryWriteGuard::new(WriteSource::Cron);
        assert!(!guard.is_write_allowed(MemoryLayer::L0));
        assert!(!guard.is_write_allowed(MemoryLayer::L1));
        assert!(!guard.is_write_allowed(MemoryLayer::L2));
        assert!(guard.is_write_allowed(MemoryLayer::L3));
        assert!(!guard.is_write_allowed(MemoryLayer::L4));
    }

    #[test]
    fn sub_agent_requires_audit() {
        let guard = MemoryWriteGuard::new(WriteSource::SubAgent);
        assert!(guard.audit_all());
        assert_eq!(
            guard.check_write(MemoryLayer::L2),
            WritePolicy::AllowWithAudit
        );
    }

    #[test]
    fn denied_returns_deny_policy() {
        let guard = MemoryWriteGuard::new(WriteSource::Cron);
        assert_eq!(guard.check_write(MemoryLayer::L0), WritePolicy::Deny);
        assert!(!guard.check_write(MemoryLayer::L0).is_allowed());
    }

    #[test]
    fn custom_sub_agent_layers() {
        let guard = MemoryWriteGuard::for_sub_agent([MemoryLayer::L1]);
        assert!(guard.is_write_allowed(MemoryLayer::L2));
        assert!(guard.is_write_allowed(MemoryLayer::L1)); // extra layer
        assert!(!guard.is_write_allowed(MemoryLayer::L0));
    }

    #[test]
    fn audit_log_roundtrip() {
        let dir = std::env::temp_dir().join("cc_memory_test_audit");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("write_log.jsonl");

        let log = AuditLog::open(path.clone()).unwrap();
        log.log(&AuditEntry {
            timestamp: Utc::now(),
            operation: AuditOperation::Create,
            entry_id: "test-1".to_string(),
            layer: "L2".to_string(),
            source: WriteSource::Assistant,
            summary: "test entry".to_string(),
            agent_id: None,
            session_id: None,
        })
        .unwrap();

        let entries = log.query_recent(10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_id, "test-1");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn audit_log_query_by_source() {
        let dir = std::env::temp_dir().join("cc_memory_test_source");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("write_log.jsonl");

        let log = AuditLog::open(path.clone()).unwrap();
        log.log(&AuditEntry {
            timestamp: Utc::now(),
            operation: AuditOperation::Create,
            entry_id: "u1".to_string(),
            layer: "L0".to_string(),
            source: WriteSource::User,
            summary: "user entry".to_string(),
            agent_id: None,
            session_id: None,
        })
        .unwrap();
        log.log(&AuditEntry {
            timestamp: Utc::now(),
            operation: AuditOperation::Create,
            entry_id: "s1".to_string(),
            layer: "L2".to_string(),
            source: WriteSource::SubAgent,
            summary: "sub entry".to_string(),
            agent_id: None,
            session_id: None,
        })
        .unwrap();

        let sub_entries = log.query_by_source(WriteSource::SubAgent).unwrap();
        assert_eq!(sub_entries.len(), 1);
        assert_eq!(sub_entries[0].entry_id, "s1");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
