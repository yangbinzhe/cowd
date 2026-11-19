use crate::runtime_event_store::{FixtureEvidencePageRequest, RuntimeEventStore};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub struct FailureEvidenceSource<'a> {
    pub stream_id: &'a str,
    pub after_sequence: u64,
    pub through_sequence: u64,
}

pub fn collect_failure_evidence(
    store: &RuntimeEventStore,
    sources: &[FailureEvidenceSource<'_>],
    original_failure: &str,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    let Some(directory) = std::env::var_os("COWD_FIXTURE_EVIDENCE_DIR") else {
        return json!({"status":"Unavailable", "reason":"COWD_FIXTURE_EVIDENCE_DIR_unset",
            "first_predicate":"Unknown", "source_count":sources.len(), "reads_started":0});
    };
    collect_failure_evidence_into(
        std::path::Path::new(&directory),
        store,
        sources,
        original_failure,
        deadline,
    )
}

/// Archive to a directory the caller already owns. The environment form exists
/// for the operator-supplied fixture directory; the direct form lets a test
/// prove the archive without racing a process-wide variable.
pub fn collect_failure_evidence_into(
    directory: &std::path::Path,
    store: &RuntimeEventStore,
    sources: &[FailureEvidenceSource<'_>],
    original_failure: &str,
    deadline: Instant,
) -> Value {
    let (path, file) = match prepare_archive(directory) {
        Ok(prepared) => prepared,
        Err(_) => return json!({"status":"Unavailable", "reason":"restricted_directory_preflight_failed",
            "first_predicate":"Unknown", "source_count":sources.len(), "reads_started":0}),
    };
    let report = collect_until(store, sources, original_failure, deadline);
    match archive_report(&report, deadline, path, file) {
        Ok(receipt) => receipt,
        Err(_) => json!({"status":"Unavailable", "reason":"restricted_evidence_archive_failed", "first_predicate":"Unknown"}),
    }
}

fn prepare_archive(directory: &std::path::Path) -> std::io::Result<(std::path::PathBuf, std::fs::File)> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let metadata = std::fs::symlink_metadata(directory)?;
    if !directory.is_absolute() || directory.canonicalize()? != directory
        || !metadata.is_dir() || metadata.file_type().is_symlink() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(std::io::Error::other("evidence directory must be private"));
    }
    let path = directory.join(format!("fixture-{}-{}.json", std::process::id(), uuid::Uuid::new_v4()));
    let file = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path)?;
    let actual = file.metadata()?;
    if !actual.is_file() || actual.permissions().mode() & 0o777 != 0o600 || actual.uid() != metadata.uid() {
        drop(file);
        let _ = std::fs::remove_file(&path);
        return Err(std::io::Error::other("evidence file permission preflight"));
    }
    Ok((path, file))
}

fn archive_report(
    report: &Value, deadline: Instant, path: std::path::PathBuf, file: std::fs::File,
) -> std::io::Result<Value> {
    use std::io::Write;
    struct ArchiveWriter {
        file: std::fs::File,
        hash: Sha256,
        remaining: usize,
        deadline: Instant,
    }
    impl Write for ArchiveWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if Instant::now() >= self.deadline || bytes.len() > self.remaining {
                return Err(std::io::Error::other("evidence archive budget"));
            }
            let written = self.file.write(bytes)?;
            self.hash.update(&bytes[..written]);
            self.remaining -= written;
            Ok(written)
        }
        fn flush(&mut self) -> std::io::Result<()> { self.file.flush() }
    }
    let mut writer = ArchiveWriter { file, hash:Sha256::new(), remaining:512 * 1024, deadline };
    if serde_json::to_writer(&mut writer, report).is_err() || Instant::now() >= deadline {
        drop(writer);
        let _ = std::fs::remove_file(&path);
        return Err(std::io::Error::other("evidence archive incomplete"));
    }
    Ok(json!({
        "status":"ArchivedDiagnostic", "ref":path, "sha256":format!("{:x}", writer.hash.finalize()),
        "bytes":512 * 1024 - writer.remaining, "owner_pid":std::process::id(),
        "first_predicate":"Unknown", "cleanup_owner":"candidate evidence owner; retained plan evidence",
    }))
}

fn collect_until(
    store: &RuntimeEventStore,
    sources: &[FailureEvidenceSource<'_>],
    original_failure: &str,
    deadline: Instant,
) -> Value {
    let mut remaining_bytes: usize = 2 * 1024 * 1024 - 512 * 1024;
    let mut remaining_events = 200usize;
    let mut remaining_output: usize = 512 * 1024 - 32 * 4096 - 8192;
    let mut classes = BTreeMap::<&str, (usize, usize)>::new();
    let mut observations = Vec::new();
    let mut stopped = None;
    for (source_index, source) in sources.iter().enumerate() {
        if Instant::now() >= deadline || source_index >= 32 || remaining_events == 0 || remaining_bytes == 0 {
            stopped = Some("total_budget");
            break;
        }
        if source.stream_id.len() > 512 || source.through_sequence < source.after_sequence {
            observations.push(json!({"status":"Unavailable", "reason":"invalid_or_oversized_source"}));
            continue;
        }
        let class = source.stream_id.split(':').next().unwrap_or("");
        let class = match class {
            "agentic-program" => "program",
            "goal" => "goal",
            "agent" => "agent",
            _ => "graph_and_related",
        };
        let usage = classes.entry(class).or_default();
        let mut after = source.after_sequence;
        let mut before_head = None;
        let mut records = Vec::new();
        let mut reason = None;
        loop {
            let count = remaining_events.min(100usize.saturating_sub(usage.0));
            let bytes = remaining_bytes.min((1024 * 1024usize).saturating_sub(usage.1));
            if count == 0 || bytes == 0 || Instant::now() >= deadline {
                reason = Some("budget_before_read");
                break;
            }
            let page = match store.fixture_evidence_page(&FixtureEvidencePageRequest {
                stream_id: source.stream_id, after_sequence: after,
                through_sequence: source.through_sequence, max_events: count,
                max_bytes: bytes, deadline,
            }) {
                Ok(page) => page,
                Err(error) => { reason = Some(error); break; }
            };
            before_head.get_or_insert(page.head);
            remaining_events -= page.records.len();
            remaining_bytes -= page.encoded_bytes;
            usage.0 += page.records.len();
            usage.1 += page.encoded_bytes;
            for event in &page.records {
                if Instant::now() >= deadline {
                    reason = Some("deadline_before_encoding");
                    break;
                }
                if [&event.event_id, &event.kind, &event.stream_id].iter().any(|value| value.len() > 512) {
                    reason = Some("oversized_event_identity");
                    break;
                }
                if event.status.iter().chain(event.actor.iter()).any(|value| value.len() > 512) {
                    reason = Some("oversized_event_status_or_actor");
                    break;
                }
                let mut writer = DigestWriter { hash: Sha256::new(), remaining: remaining_output.saturating_sub(128), deadline };
                if serde_json::to_writer(&mut writer, event).is_err() {
                    reason = Some("output_budget");
                    break;
                }
                let mut record = serde_json::to_value(event).expect("preflight serialized event");
                record["original_record_sha256"] = json!(format!("{:x}", writer.hash.finalize()));
                let mut output = DigestWriter { hash: Sha256::new(), remaining: remaining_output, deadline };
                if serde_json::to_writer(&mut output, &record).is_err() {
                    reason = Some("output_budget");
                    break;
                }
                remaining_output = output.remaining;
                records.push(record);
                after = event.sequence;
            }
            if reason.is_some() { break; }
            if let Some(stop) = page.stop { reason = Some(stop); break; }
            if after >= source.through_sequence.min(page.head) || page.records.is_empty() { break; }
        }
        let after_read = if before_head.is_some() && Instant::now() < deadline {
            store.fixture_evidence_page(&FixtureEvidencePageRequest {
                stream_id: source.stream_id, after_sequence: u64::MAX,
                through_sequence: u64::MAX, max_events: 1, max_bytes: 1, deadline,
            }).map(|page| page.head)
        } else { Err("Unavailable: source unread or deadline exhausted") };
        let after_head = after_read.as_ref().ok().copied();
        observations.push(json!({
            "stream_id":source.stream_id, "requested_after":source.after_sequence,
            "requested_through":source.through_sequence, "before_head":before_head,
            "after_head":after_head, "head_changed":before_head.zip(after_head).map(|(before, after)| before != after),
            "after_head_error":after_read.err(),
            "next_sequence":after, "status":if before_head.is_none() {"Unavailable"} else if reason.is_some() {"Truncated"} else {"Captured"},
            "reason":reason, "records":records,
        }));
    }
    json!({
        "original_failure":if original_failure.len() <= 512 {original_failure} else {"Unavailable: oversized original failure; see original caller"},
        "first_predicate":"Unknown", "transaction_consistent_snapshot":false,
        "source_limit":32, "node_limit":256, "nodes_expanded":0,
        "events_read":200 - remaining_events, "original_bytes_read":2 * 1024 * 1024 - 512 * 1024 - remaining_bytes,
        "output_reserved_bytes":512 * 1024, "stop":stopped,
        "remaining_sources":sources.len().saturating_sub(observations.len()),
        "sources":observations,
    })
}

struct DigestWriter {
    hash: Sha256,
    remaining: usize,
    deadline: Instant,
}

impl std::io::Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if Instant::now() >= self.deadline || bytes.len() > self.remaining {
            return Err(std::io::Error::other("fixture evidence encoding budget"));
        }
        self.remaining -= bytes.len();
        self.hash.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_event_store::{EphemeralRuntimeEventStore, RuntimeEventInput, RuntimeEventScope};
    use std::sync::Arc;

    fn collect_failure_evidence(store: &RuntimeEventStore, sources: &[FailureEvidenceSource<'_>], failure: &str) -> Value {
        collect_until(store, sources, failure, Instant::now() + Duration::from_secs(5))
    }

    #[test]
    fn fixture_evidence_archives_full_original_and_returns_only_reference() {
        let store = RuntimeEventStore::from_backend(Arc::new(EphemeralRuntimeEventStore::new()));
        store.append(RuntimeEventInput {
            stream_id:"agent:archive".into(), scope:RuntimeEventScope::Agent,
            kind:"agent.local_exit".into(), status:Some("completed".into()), actor:Some("fixture".into()), refs:vec![],
            payload:json!({"phase":"local_returned", "private_body":"private_archive_only"}),
        }).unwrap();
        let archive = tempfile::tempdir().unwrap();
        let archive_directory = archive.path().canonicalize().unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                &archive_directory,
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
        let receipt = super::collect_failure_evidence_into(
            &archive_directory,
            &store,
            &[FailureEvidenceSource {
                stream_id: "agent:archive",
                after_sequence: 0,
                through_sequence: u64::MAX,
            }],
            "controlled failure",
            Instant::now() + Duration::from_secs(5),
        );
        assert_eq!(receipt["status"], "ArchivedDiagnostic", "{receipt}");
        assert!(receipt.get("sources").is_none());
        assert!(!receipt.to_string().contains("private_archive_only"));
        drop(store);
        let path = std::path::Path::new(receipt["ref"].as_str().unwrap());
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(receipt["sha256"], format!("{:x}", Sha256::digest(&bytes)));
        let report: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(report["sources"][0]["records"][0]["payload"]["phase"], "local_returned");
        assert_eq!(report["sources"][0]["records"][0]["payload"]["private_body"], "private_archive_only");
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(path).unwrap().permissions().mode() & 0o777, 0o600);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn fixture_evidence_captures_complete_real_original() {
        let store = RuntimeEventStore::from_backend(Arc::new(EphemeralRuntimeEventStore::new()));
        let original = store.append(RuntimeEventInput {
            stream_id:"agent:owned".into(), scope:RuntimeEventScope::Agent,
            kind:"agent.local_exit".into(), status:Some("completed".into()), actor:Some("agent_runtime".into()), refs:vec![],
            payload:json!({"phase":"local_returned", "attempt":1, "private_body":"never_print_this"}),
        }).unwrap();
        let report = collect_failure_evidence(&store, &[FailureEvidenceSource {
            stream_id:"agent:owned", after_sequence:0, through_sequence:u64::MAX,
        }], "original oracle failure");
        assert_eq!(report["sources"][0]["records"][0]["event_id"], original.event_id);
        assert_eq!(report["sources"][0]["records"][0]["original_record_sha256"],
            format!("{:x}", Sha256::digest(serde_json::to_vec(&original).unwrap())));
        assert_eq!(report["sources"][0]["head_changed"], false);
        assert_eq!(report["sources"][0]["records"][0]["actor"], "agent_runtime");
        assert_eq!(report["sources"][0]["records"][0]["status"], "completed");
        let mut archived = report["sources"][0]["records"][0].clone();
        archived.as_object_mut().unwrap().remove("original_record_sha256");
        assert_eq!(archived, serde_json::to_value(&original).unwrap());
        let expired = collect_until(&store, &[FailureEvidenceSource {
            stream_id:"agent:owned", after_sequence:0, through_sequence:u64::MAX,
        }], "original", Instant::now());
        assert_eq!(expired["events_read"], 0);
        assert_eq!(expired["stop"], "total_budget");
    }

    #[test]
    fn fixture_evidence_enforces_class_and_global_event_budgets() {
        let store = RuntimeEventStore::from_backend(Arc::new(EphemeralRuntimeEventStore::new()));
        let mut original_first_stream = Vec::new();
        for stream in ["agent:owned", "goal:owned", "agentic-program:owned"] {
            for _ in 0..101 {
                let event = store.append(RuntimeEventInput {
                    stream_id:stream.into(), scope:RuntimeEventScope::Agent,
                    kind:"fixture.record".into(), status:None, actor:None, refs:vec![], payload:json!({}),
                }).unwrap();
                if stream == "agent:owned" { original_first_stream.push(event); }
            }
        }
        let sources = ["agent:owned", "goal:owned", "agentic-program:owned"].map(|stream_id| FailureEvidenceSource {
            stream_id, after_sequence:0, through_sequence:u64::MAX,
        });
        let report = collect_failure_evidence(&store, &sources, "original");
        assert_eq!(report["events_read"], 200);
        assert_eq!(
            report["sources"][0]["records"].as_array().unwrap().iter()
                .map(|record| record["event_id"].as_str().unwrap()).collect::<Vec<_>>(),
            original_first_stream.iter().take(100).map(|event| event.event_id.as_str()).collect::<Vec<_>>()
        );
        assert_eq!(report["sources"][0]["status"], "Truncated");
        assert_eq!(report["sources"][0]["reason"], "events");
        assert_eq!(report["remaining_sources"], 1);
        assert!(report.to_string().len() <= 512 * 1024);
    }

    #[test]
    fn fixture_evidence_enforces_total_bytes_and_source_limits() {
        let store = RuntimeEventStore::from_backend(Arc::new(EphemeralRuntimeEventStore::new()));
        for stream in ["agent:owned", "goal:owned", "agentic-program:owned"] {
            store.append(RuntimeEventInput {
                stream_id:stream.into(), scope:RuntimeEventScope::Agent,
                kind:"fixture.record".into(), status:None, actor:None, refs:vec![],
                payload:json!({"body":"x".repeat(800 * 1024)}),
            }).unwrap();
        }
        let sources = ["agent:owned", "goal:owned", "agentic-program:owned"].map(|stream_id| FailureEvidenceSource {
            stream_id, after_sequence:0, through_sequence:u64::MAX,
        });
        let report = collect_failure_evidence(&store, &sources, "original");
        assert_eq!(report["events_read"], 1);
        assert_eq!(report["sources"][0]["status"], "Truncated");
        assert_eq!(report["sources"][0]["reason"], "output_budget");
        assert!(report["sources"][0]["records"].as_array().unwrap().is_empty());
        assert_eq!(report["sources"][1]["reason"], "bytes");
        assert!(report["original_bytes_read"].as_u64().unwrap() + report.to_string().len() as u64 <= 2 * 1024 * 1024);
        let sources = (0..33).map(|_| FailureEvidenceSource {
            stream_id:"empty", after_sequence:0, through_sequence:u64::MAX,
        }).collect::<Vec<_>>();
        let report = collect_failure_evidence(&store, &sources, "original");
        assert_eq!(
            report["sources"].as_array().unwrap().iter()
                .map(|source| (source["stream_id"].as_str().unwrap(), source["requested_after"].as_u64().unwrap(), source["requested_through"].as_u64().unwrap()))
                .collect::<Vec<_>>(),
            sources.iter().take(32).map(|source| (source.stream_id, source.after_sequence, source.through_sequence)).collect::<Vec<_>>()
        );
        assert_eq!(report["remaining_sources"], 1);
        assert_eq!(report["stop"], "total_budget");
    }
}
