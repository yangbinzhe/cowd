//! Outer cleanup observation for the focused U5 scenarios.
//!
//! The harness cannot observe the outer cleanup by itself: the launcher stops
//! the Gateway and the owner returns the Postgres container only AFTER the
//! harness process has exited. Those facts are durable and they are bound to
//! this run, but they are produced outside the harness, so the harness
//! publishes `outer_cleanup_observed=false` with a `pending_outer_cleanup`
//! disposition, and the scenario can never be judged.
//!
//! This module is the projection that reads those durable facts back and binds
//! them to the harness report that produced them. It never invents a fact and
//! it never accepts a claim:
//!
//!   * a missing receipt leaves the observation unobserved;
//!   * a receipt that predates the harness report cannot be this run's outer
//!     cleanup;
//!   * a receipt from another run, another owner or another report digest is
//!     rejected;
//!   * an owner that merely exited 0 is not a cleanup receipt - the owner's
//!     own durable cleanup facts must be present and complete;
//!   * a report that claims the observation without the receipts stays
//!     unobserved, because the gate only accepts a bound projection.

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const U5_FAIRNESS_SCENARIO: &str = "live_u5_shared_session_fairness";
pub const U5_CANCEL_SCENARIO: &str = "live_u5_active_cancel_isolation";

/// Why an outer cleanup could not be observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OuterCleanupGap {
    Missing(&'static str),
    Malformed(String),
    WrongRun(String),
    WrongOwner(String),
    DigestMismatch(String),
    OutOfOrder(String),
    CleanupNotReturned(String),
    OwnerCleanupIncomplete(String),
}

impl OuterCleanupGap {
    /// The typed reason a reader can branch on.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Missing(_) => "missing_outer_fact",
            Self::Malformed(_) => "malformed_outer_fact",
            Self::WrongRun(_) => "wrong_run",
            Self::WrongOwner(_) => "wrong_owner",
            Self::DigestMismatch(_) => "digest_mismatch",
            Self::OutOfOrder(_) => "outer_fact_out_of_order",
            Self::CleanupNotReturned(_) => "cleanup_not_returned",
            Self::OwnerCleanupIncomplete(_) => "owner_cleanup_incomplete",
        }
    }

    /// The human readable detail of the gap.
    #[must_use]
    pub fn detail(&self) -> String {
        match self {
            Self::Missing(what) => format!("missing outer fact: {what}"),
            Self::Malformed(what) => format!("malformed outer fact: {what}"),
            Self::WrongRun(what) => format!("outer fact belongs to another run: {what}"),
            Self::WrongOwner(what) => format!("outer fact belongs to another owner: {what}"),
            Self::DigestMismatch(what) => format!("outer fact digest mismatch: {what}"),
            Self::OutOfOrder(what) => format!("outer fact out of order: {what}"),
            Self::CleanupNotReturned(what) => format!("outer resources were not returned: {what}"),
            Self::OwnerCleanupIncomplete(what) => {
                format!("owner cleanup is incomplete: {what}")
            }
        }
    }

    /// The projection record for an unobserved outer cleanup.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({"status":"unobserved","reason":self.reason(),"detail":self.detail()})
    }
}

/// Where the durable receipts live, so a persisted projection can be
/// re-verified by an independent reader instead of trusting its own record.
#[derive(Debug, Clone, Default)]
pub struct ReceiptSources {
    pub harness_report: String,
    pub inner_receipt: String,
    pub final_receipt: String,
    pub cleanup_result: Option<String>,
    pub shutdown_validation: Option<String>,
    pub owner_report: Option<String>,
    pub manifest: Option<String>,
    pub sealed_key: Option<String>,
}

/// The durable receipts one projection reads.
#[derive(Debug, Clone)]
pub struct OuterCleanupReceipts {
    /// The harness report that produced the inner facts.
    pub harness_report: Value,
    /// The digest of the harness report file, as the receipts state it.
    pub harness_report_sha256: String,
    /// The harness inner receipt (`u5-inner-receipt.json`).
    pub inner_receipt: Value,
    /// The digest of the inner receipt file.
    pub inner_receipt_sha256: String,
    /// The launcher final receipt (`u5-final-acceptance.v1.json`).
    pub final_receipt: Value,
    /// The digest of the final receipt file.
    pub final_receipt_sha256: String,
    /// The launcher cleanup result (`cleanup-result.json`).
    pub cleanup_result: Option<Value>,
    /// The digest of the cleanup result file.
    pub cleanup_result_sha256: Option<String>,
    /// The launcher shutdown validation (`shutdown-validation.json`).
    pub shutdown_validation: Option<Value>,
    /// The digest of the shutdown validation file.
    pub shutdown_validation_sha256: Option<String>,
    /// The shutdown receipt digest the final receipt names.
    pub shutdown_receipt_sha256: Option<String>,
    /// The owner report (`pg-owned-run.py`).
    pub owner_report: Option<Value>,
    /// The launcher manifest: it binds the harness run to the owner run and
    /// to the owner nonce. The harness run id and the owner run id are
    /// different namespaces, so the relation is declared, never assumed.
    pub manifest: Option<Value>,
    /// The sealed evidence key the owner report names.
    pub sealed_key_path: Option<String>,
    /// The digest of the owner report file.
    pub owner_report_sha256: Option<String>,
    /// The digest of the launcher manifest file.
    pub manifest_sha256: Option<String>,
    /// Where each receipt lives, so the persisted projection can be re-read.
    pub sources: ReceiptSources,
    /// File modification times, used for the ordering guard.
    pub harness_report_mtime_ms: Option<u64>,
    pub final_receipt_mtime_ms: Option<u64>,
    pub owner_report_mtime_ms: Option<u64>,
}

/// The bound outer cleanup facts.
#[derive(Debug, Clone)]
pub struct OuterCleanupFacts {
    pub run_id: String,
    pub scenario: String,
    pub owner: Value,
    pub report_sha256: String,
    pub inner_sha256: String,
    pub final_receipt_sha256: String,
    pub cleanup_sha256: Option<String>,
    pub shutdown_validation_sha256: Option<String>,
    pub owner_report_sha256: Option<String>,
    pub manifest_sha256: Option<String>,
    pub remaining_owned: usize,
    pub gateway_wait_status: String,
    pub business_exit: i64,
    pub owner_layer: Option<Value>,
    /// The receipts the observation was derived from.
    pub sources: ReceiptSources,
}

impl OuterCleanupFacts {
    /// The projection record written into the report.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "status":"observed",
            "run_id":self.run_id,
            "scenario":self.scenario,
            "owner":self.owner,
            "report_sha256":self.report_sha256,
            "inner_sha256":self.inner_sha256,
            "final_receipt_sha256":self.final_receipt_sha256,
            "cleanup_sha256":self.cleanup_sha256,
            "shutdown_validation_sha256":self.shutdown_validation_sha256,
            "remaining_owned":self.remaining_owned,
            "gateway_wait_status":self.gateway_wait_status,
            "business_exit":self.business_exit,
            "owner_layer":self.owner_layer,
            "receipts":{
                "harness_report":{"path":self.sources.harness_report,"sha256":self.report_sha256},
                "inner_receipt":{"path":self.sources.inner_receipt,"sha256":self.inner_sha256},
                "final_receipt":{"path":self.sources.final_receipt,"sha256":self.final_receipt_sha256},
                "cleanup_result":{"path":self.sources.cleanup_result,"sha256":self.cleanup_sha256},
                "shutdown_validation":{"path":self.sources.shutdown_validation,"sha256":self.shutdown_validation_sha256},
                "owner_report":{"path":self.sources.owner_report,"sha256":self.owner_report_sha256},
                "manifest":{"path":self.sources.manifest,"sha256":self.manifest_sha256},
                "sealed_key":{"path":self.sources.sealed_key},
            },
        })
    }
}

fn text(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| value.as_u64().map(|number| number.to_string()))
}

fn digest_of(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Observe the outer cleanup from the durable receipts.
///
/// Every check is a binding check: the receipts must describe *this* harness
/// report, produced by *this* owner, and they must postdate it.
pub fn observe_outer_cleanup(
    receipts: &OuterCleanupReceipts,
) -> Result<OuterCleanupFacts, OuterCleanupGap> {
    let report = &receipts.harness_report;
    if receipts.inner_receipt.is_null() {
        return Err(OuterCleanupGap::Missing("inner receipt"));
    }
    if receipts.final_receipt.is_null() {
        return Err(OuterCleanupGap::Missing("outer final receipt"));
    }
    // The outer cleanup has two layers and both are required: the launcher
    // layer (the final receipt and the cleanup result) and the owner layer
    // (the Postgres owner's own cleanup facts). A missing layer is not a
    // weaker observation, it is an absent fact.
    if receipts.cleanup_result.is_none() {
        return Err(OuterCleanupGap::Missing("launcher cleanup result"));
    }
    if receipts.owner_report.is_none() {
        return Err(OuterCleanupGap::Missing("owner cleanup report"));
    }
    if report["kind"] != json!("harness_eval.u5_internal") {
        return Err(OuterCleanupGap::Malformed("harness report kind".into()));
    }
    if report["status"] != json!("pending_outer_cleanup") {
        return Err(OuterCleanupGap::Malformed("harness report status".into()));
    }
    let run_id = report["run_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| OuterCleanupGap::Malformed("harness report run id".into()))?
        .to_string();
    let host = report["u5_host"].clone();
    if host["nonce"].as_str().is_none() {
        return Err(OuterCleanupGap::Malformed("harness report host binding".into()));
    }

    // The inner receipt is the harness' own statement about this report.
    let inner = &receipts.inner_receipt;
    if inner["schema_version"] != json!(1) {
        return Err(OuterCleanupGap::Malformed("inner receipt schema".into()));
    }
    if inner["disposition"] != json!("pending_outer_cleanup") {
        return Err(OuterCleanupGap::Malformed("inner receipt disposition".into()));
    }
    let scenario = inner["scenario"]
        .as_str()
        .ok_or_else(|| OuterCleanupGap::Malformed("inner receipt scenario".into()))?
        .to_string();
    if !matches!(scenario.as_str(), U5_FAIRNESS_SCENARIO | U5_CANCEL_SCENARIO) {
        return Err(OuterCleanupGap::Malformed("inner receipt scenario".into()));
    }
    if inner["run_id"] != json!(run_id) {
        return Err(OuterCleanupGap::WrongRun("inner receipt run id".into()));
    }
    if inner["owner"] != host {
        return Err(OuterCleanupGap::WrongOwner("inner receipt owner".into()));
    }
    if inner["report_sha256"] != json!(receipts.harness_report_sha256) {
        return Err(OuterCleanupGap::DigestMismatch(
            "inner receipt report digest".into(),
        ));
    }

    // The report must describe exactly the scenario the receipts bind, and the
    // two leaves must carry distinct session / turn / execution identities.
    let scenarios = report["live_gateway_scenarios"]["scenarios"]
        .as_array()
        .ok_or_else(|| OuterCleanupGap::Malformed("report scenarios".into()))?;
    if scenarios.len() != 1 {
        return Err(OuterCleanupGap::WrongRun("report scenario count".into()));
    }
    let scenario_id = scenarios[0]["scenario_id"]
        .as_str()
        .ok_or_else(|| OuterCleanupGap::Malformed("report scenario id".into()))?;
    if scenario_id != scenario {
        return Err(OuterCleanupGap::WrongRun("report scenario id".into()));
    }
    if report["live_gateway_scenarios"]["selected_scenario_ids"] != json!([scenario]) {
        return Err(OuterCleanupGap::WrongRun("report selected scenario".into()));
    }
    let leaves = scenarios[0]["sessions"]
        .as_array()
        .ok_or_else(|| OuterCleanupGap::Malformed("report sessions".into()))?;
    if leaves.len() != 2 {
        return Err(OuterCleanupGap::Malformed("report leaf count".into()));
    }
    for key in ["session_id", "turn_id", "execution_id"] {
        let first = leaves[0][key].as_str().unwrap_or_default();
        let second = leaves[1][key].as_str().unwrap_or_default();
        if first.is_empty() || second.is_empty() || first == second {
            return Err(OuterCleanupGap::WrongRun(format!("leaf identity {key}")));
        }
    }


    // The launcher final receipt binds the outer cleanup to both inner facts.
    let final_receipt = &receipts.final_receipt;
    if final_receipt["schema_version"] != json!(1) {
        return Err(OuterCleanupGap::Malformed("final receipt schema".into()));
    }
    if final_receipt["status"] != json!("passed") {
        return Err(OuterCleanupGap::CleanupNotReturned(
            "final receipt status".into(),
        ));
    }
    let expected_scope = if scenario == U5_CANCEL_SCENARIO {
        "focused_U5_C_local_only"
    } else {
        "focused_U5_F_only"
    };
    if final_receipt["scope"] != json!(expected_scope) {
        return Err(OuterCleanupGap::WrongRun("final receipt scope".into()));
    }
    if final_receipt["original_harness_exit"] != json!(75) {
        return Err(OuterCleanupGap::Malformed(
            "final receipt original harness exit".into(),
        ));
    }
    if final_receipt["run_id"] != json!(run_id) {
        return Err(OuterCleanupGap::WrongRun("final receipt run id".into()));
    }
    if final_receipt["owner"] != host {
        return Err(OuterCleanupGap::WrongOwner("final receipt owner".into()));
    }
    if final_receipt["report_sha256"] != json!(receipts.harness_report_sha256) {
        return Err(OuterCleanupGap::DigestMismatch(
            "final receipt report digest".into(),
        ));
    }
    if final_receipt["inner_sha256"] != json!(receipts.inner_receipt_sha256) {
        return Err(OuterCleanupGap::DigestMismatch(
            "final receipt inner digest".into(),
        ));
    }

    // The launcher cleanup result: the owned processes came back and the
    // Gateway stopped cleanly.
    let mut remaining_owned = 0usize;
    let mut gateway_wait_status = String::new();
    let mut business_exit = 75i64;
    {
        let cleanup = receipts.cleanup_result.as_ref().expect("cleanup result is required");
        business_exit = cleanup["business_exit"]
            .as_i64()
            .ok_or_else(|| OuterCleanupGap::Malformed("cleanup business exit".into()))?;
        if business_exit != 75 {
            return Err(OuterCleanupGap::CleanupNotReturned(
                "cleanup business exit".into(),
            ));
        }
        if cleanup["cleanup_failed"] != json!(false) {
            return Err(OuterCleanupGap::CleanupNotReturned(
                "cleanup reported a failure".into(),
            ));
        }
        gateway_wait_status = cleanup["gateway_wait_status"]
            .as_str()
            .ok_or_else(|| OuterCleanupGap::Malformed("cleanup gateway wait".into()))?
            .to_string();
        if gateway_wait_status != "0" {
            return Err(OuterCleanupGap::CleanupNotReturned(
                "gateway did not stop cleanly".into(),
            ));
        }
        let remaining = cleanup["remaining_owned"]
            .as_array()
            .ok_or_else(|| OuterCleanupGap::Malformed("cleanup remaining owned".into()))?;
        remaining_owned = remaining.len();
        if remaining_owned != 0 {
            return Err(OuterCleanupGap::CleanupNotReturned(
                "owned processes remain".into(),
            ));
        }
        if let Some(digest) = &receipts.cleanup_result_sha256 {
            if final_receipt["cleanup_sha256"] != json!(digest) {
                return Err(OuterCleanupGap::DigestMismatch(
                    "cleanup result digest".into(),
                ));
            }
        }
    }

    // The launcher shutdown validation must describe the same host.
    if let Some(validation) = &receipts.shutdown_validation {
        if validation["status"] != json!("passed") {
            return Err(OuterCleanupGap::CleanupNotReturned(
                "shutdown validation status".into(),
            ));
        }
        for key in ["nonce", "pid", "starttime", "image_sha256"] {
            if validation["identity"][key] != host[key] {
                return Err(OuterCleanupGap::WrongOwner(format!(
                    "shutdown validation {key}"
                )));
            }
        }
        // The final receipt names the shutdown validation and its receipt by
        // digest; the bytes on disk must be exactly those.
        if let Some(digest) = &receipts.shutdown_validation_sha256 {
            if final_receipt["shutdown_validation_sha256"] != json!(digest) {
                return Err(OuterCleanupGap::DigestMismatch(
                    "shutdown validation digest".into(),
                ));
            }
        }
        if let Some(receipt_digest) = &receipts.shutdown_receipt_sha256 {
            if final_receipt["shutdown_receipt_sha256"] != json!(receipt_digest) {
                return Err(OuterCleanupGap::DigestMismatch(
                    "shutdown receipt digest".into(),
                ));
            }
            if validation["receipt_sha256"] != json!(receipt_digest) {
                return Err(OuterCleanupGap::DigestMismatch(
                    "shutdown receipt content".into(),
                ));
            }
        }
    }

    // The owner layer: the Postgres owner must have owned the harness process
    // and must have returned its own resources. An owner that merely exited 0
    // proves nothing; these are the owner's own durable cleanup facts.
    let owner_layer = match &receipts.owner_report {
        Some(owner) => {
            // The manifest binds this harness run to the owner run and to the
            // owner nonce. The two ids live in different namespaces, so the
            // relation has to be declared by the launcher and verified here.
            let manifest = receipts
                .manifest
                .as_ref()
                .ok_or(OuterCleanupGap::Missing("launcher manifest"))?;
            if manifest["harness_run_id"] != json!(run_id) {
                return Err(OuterCleanupGap::WrongRun(
                    "manifest harness run id".into(),
                ));
            }
            if manifest["scenario"] != json!(scenario) {
                return Err(OuterCleanupGap::WrongRun("manifest scenario".into()));
            }
            if manifest["owner_run_id"] != owner["run_id"] {
                return Err(OuterCleanupGap::WrongRun("manifest owner run id".into()));
            }
            // A same-UID child can create a 0600 manifest. Only the owner's
            // credentialed launcher handoff may bind these exact bytes.
            let declaration = owner.get("outer_launcher_declaration")
                .filter(|value| value["status"] == json!("received"))
                .ok_or(OuterCleanupGap::Missing("owner launcher declaration"))?;
            if owner["outer_record_reserved"] != json!(true) {
                return Err(OuterCleanupGap::Missing("owner reserved outer record"));
            }
            if receipts.manifest_sha256.as_deref() != declaration["sha256"].as_str() {
                return Err(OuterCleanupGap::DigestMismatch("owner launcher declaration".into()));
            }
            let sender_pid = text(&declaration["pid"])
                .ok_or_else(|| OuterCleanupGap::Malformed("launcher sender pid".into()))?;
            let sender_start = text(&declaration["starttime"])
                .ok_or_else(|| OuterCleanupGap::Malformed("launcher sender starttime".into()))?;
            let child = &owner["child_identity"];
            let direct = text(&child["pid"]).as_deref() == Some(sender_pid.as_str())
                && text(&child["starttime"]).as_deref() == Some(sender_start.as_str());
            let timed_child = owner["owned_processes"].as_array().is_some_and(|entries| entries.iter().any(|entry| {
                text(&entry["pid"]).as_deref() == Some(sender_pid.as_str())
                    && text(&entry["starttime"]).as_deref() == Some(sender_start.as_str())
                    && text(&entry["ppid"]) == text(&child["pid"])
                    && text(&entry["pgid"]) == text(&child["pgid"])
                    && text(&child["pgid"]).is_some()
            }));
            if !direct && !timed_child {
                return Err(OuterCleanupGap::WrongOwner("launcher declaration sender".into()));
            }
            let owner_nonce = manifest["owner_nonce"].as_str().ok_or_else(|| {
                OuterCleanupGap::Malformed("manifest owner nonce".into())
            })?;
            if owner["labels"]["cowd.test.owner"] != json!(owner_nonce) {
                return Err(OuterCleanupGap::WrongOwner("manifest owner nonce".into()));
            }
            if !owner["container"]
                .as_str()
                .is_some_and(|name| name.contains(owner_nonce))
                || !owner["volume"]
                    .as_str()
                    .is_some_and(|name| name.contains(owner_nonce))
            {
                return Err(OuterCleanupGap::WrongOwner(
                    "owner resource identity".into(),
                ));
            }
            // The owner must have reaped its child and reached a final state.
            // The owner is still running while it projects, so its own exit is
            // not the evidence; its durable child and cleanup facts are.
            if owner["child_reaped"] != json!(true) {
                return Err(OuterCleanupGap::OwnerCleanupIncomplete(
                    "owner did not reap the child".into(),
                ));
            }
            if owner["execution_status"] != json!("passed") {
                return Err(OuterCleanupGap::OwnerCleanupIncomplete(
                    "owner execution status".into(),
                ));
            }
            if !matches!(
                owner["status"].as_str(),
                Some("passed") | Some("acceptance_pending")
            ) {
                return Err(OuterCleanupGap::OwnerCleanupIncomplete(
                    "owner status is not final".into(),
                ));
            }
            let owned = owner["owned_processes"]
                .as_array()
                .ok_or_else(|| OuterCleanupGap::Malformed("owner owned processes".into()))?;
            let harness_pid = text(&host["pid"])
                .ok_or_else(|| OuterCleanupGap::Malformed("host pid".into()))?;
            let harness_start = text(&host["starttime"])
                .ok_or_else(|| OuterCleanupGap::Malformed("host start time".into()))?;
            let owns_harness = owned.iter().any(|entry| {
                text(&entry["pid"]).as_deref() == Some(harness_pid.as_str())
                    && text(&entry["starttime"]).as_deref() == Some(harness_start.as_str())
            });
            if !owns_harness {
                return Err(OuterCleanupGap::WrongOwner(
                    "owner did not own the harness process".into(),
                ));
            }
            // The harness process must belong to the owner's own child
            // generation, not merely appear in the owner's process list.
            let child = owner["child_identity"].clone();
            let child_pgid = text(&child["pgid"]);
            let harness_pgid = owned
                .iter()
                .find(|entry| {
                    text(&entry["pid"]).as_deref() == Some(harness_pid.as_str())
                        && text(&entry["starttime"]).as_deref() == Some(harness_start.as_str())
                })
                .and_then(|entry| text(&entry["pgid"]));
            if child_pgid.is_none() || harness_pgid.is_none() || child_pgid != harness_pgid {
                return Err(OuterCleanupGap::WrongOwner(
                    "harness process is not in the owner child generation".into(),
                ));
            }
            let cleanup_errors = owner["cleanup_errors"]
                .as_array()
                .ok_or_else(|| OuterCleanupGap::Malformed("owner cleanup errors".into()))?;
            if !cleanup_errors.is_empty() {
                return Err(OuterCleanupGap::OwnerCleanupIncomplete(
                    "owner reported cleanup errors".into(),
                ));
            }
            let remaining = owner["remaining_processes"]
                .as_array()
                .ok_or_else(|| OuterCleanupGap::Malformed("owner remaining processes".into()))?;
            if !remaining.is_empty() {
                return Err(OuterCleanupGap::OwnerCleanupIncomplete(
                    "owner left processes behind".into(),
                ));
            }
            if owner["remaining_db_clients"] != json!(0) {
                return Err(OuterCleanupGap::OwnerCleanupIncomplete(
                    "owner left database clients behind".into(),
                ));
            }
            if owner["container_stopped"] != json!(true) {
                return Err(OuterCleanupGap::OwnerCleanupIncomplete(
                    "owner did not stop the container".into(),
                ));
            }
            if owner["PG_process_returned"] != json!(true) {
                return Err(OuterCleanupGap::OwnerCleanupIncomplete(
                    "owner did not return the Postgres process".into(),
                ));
            }
            if owner["port_bindable"] != json!(true) {
                return Err(OuterCleanupGap::OwnerCleanupIncomplete(
                    "owner left the port bound".into(),
                ));
            }
            let sealed = owner["encrypted_evidence"]["key_reference"]
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    OuterCleanupGap::OwnerCleanupIncomplete(
                        "owner did not seal the retained evidence".into(),
                    )
                })?;
            // The sealed key must exist and be the key the owner recorded: a
            // named path that does not hold the key is not a custody fact.
            let sealed_path = receipts
                .sealed_key_path
                .as_deref()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    OuterCleanupGap::OwnerCleanupIncomplete(
                        "owner did not record the sealed evidence key".into(),
                    )
                })?;
            let sealed_digest = std::fs::read(sealed_path)
                .map(|bytes| digest_of(&bytes))
                .map_err(|error| {
                    OuterCleanupGap::OwnerCleanupIncomplete(format!(
                        "sealed evidence key unreadable: {error}"
                    ))
                })?;
            if owner["encrypted_evidence"]["key_sha256"] != json!(sealed_digest) {
                return Err(OuterCleanupGap::DigestMismatch(
                    "sealed evidence key digest".into(),
                ));
            }
            Some(json!({
                "owner_run_id":owner["run_id"],
                "container":owner["container"],
                "volume":owner["volume"],
                "port":owner["port"],
                "container_stopped":owner["container_stopped"],
                "container_retained":owner["container_retained"],
                "volume_retained":owner["volume_retained"],
                "PG_process_returned":owner["PG_process_returned"],
                "private_root_retained":owner["private_root_retained"],
                "port_bindable":owner["port_bindable"],
                "remaining_processes":remaining.len(),
                "remaining_db_clients":owner["remaining_db_clients"],
                "cleanup_errors":cleanup_errors.len(),
                "evidence_key_reference":sealed,
                "evidence_sha256":owner["encrypted_evidence"]["sha256"],
                "finished_at":owner["finished_at"],
            }))
        }
        None => None,
    };

    // Ordering: the outer facts cannot predate the report they belong to, and
    // the ordering evidence must exist - a missing timestamp is unknown, not
    // an accepted order. A late arrival is fine: the outer facts may land
    // after the run, they may not land before the inner facts.
    let report_ms = receipts
        .harness_report_mtime_ms
        .ok_or(OuterCleanupGap::Missing("harness report timestamp"))?;
    let final_ms = receipts
        .final_receipt_mtime_ms
        .ok_or(OuterCleanupGap::Missing("final receipt timestamp"))?;
    if final_ms < report_ms {
        return Err(OuterCleanupGap::OutOfOrder(
            "final receipt predates the harness report".into(),
        ));
    }
    if let Some(owner_ms) = receipts.owner_report_mtime_ms {
        if owner_ms < report_ms {
            return Err(OuterCleanupGap::OutOfOrder(
                "owner report predates the harness report".into(),
            ));
        }
    }

    Ok(OuterCleanupFacts {
        run_id,
        scenario,
        owner: host,
        report_sha256: receipts.harness_report_sha256.clone(),
        inner_sha256: receipts.inner_receipt_sha256.clone(),
        final_receipt_sha256: receipts.final_receipt_sha256.clone(),
        cleanup_sha256: receipts.cleanup_result_sha256.clone(),
        shutdown_validation_sha256: receipts.shutdown_validation_sha256.clone(),
        owner_report_sha256: receipts.owner_report_sha256.clone(),
        manifest_sha256: receipts.manifest_sha256.clone(),
        remaining_owned,
        gateway_wait_status,
        business_exit,
        owner_layer,
        sources: receipts.sources.clone(),
    })
}

/// Project the observed outer cleanup into one scenario object.
///
/// The scenario is the same fact in two places (the live summary and the
/// stored detail artifact); both copies are projected the same way.
fn project_scenario(scenario: &mut Value, record: &Value) {
    if let Some(checks) = scenario.pointer_mut("/acceptance/checks") {
        if let Some(map) = checks.as_object_mut() {
            map.insert("outer_cleanup_observed".into(), json!(true));
        }
    }
    if let Some(map) = scenario.as_object_mut() {
        map.insert("outer_cleanup".into(), json!("observed"));
        map.insert("outer_cleanup_facts".into(), record.clone());
    }
    // The business state stays as the original run left it.
}

/// Project the observed outer cleanup into a stored scenario artifact.
///
/// The stored artifact is the scenario the report points at; the original
/// bytes stay untouched, the projection is a separate document. The business
/// state is not rewritten: the outer observation is one fact and the gate
/// decides the verdict.
pub fn project_scenario_artifact(artifact: &mut Value, facts: &OuterCleanupFacts) {
    let record = facts.to_json();
    project_scenario(artifact, &record);
    if let Some(map) = artifact.as_object_mut() {
        map.insert("outer_cleanup_projection".into(), record);
    }
}

/// Project the observed outer cleanup into the harness report.
///
/// The projection never decides the business outcome: it records the outer
/// observation only. The gate decides the verdict from the same receipts, so a
/// scenario that failed a business predicate can never be turned into a pass
/// by the outer cleanup.
pub fn project_outer_cleanup(report: &mut Value, facts: &OuterCleanupFacts) {
    let record = facts.to_json();
    for path in [
        "/live_gateway_scenario_details/scenarios/0",
        "/live_gateway_scenarios/scenarios/0",
    ] {
        let Some(scenario) = report.pointer_mut(path) else {
            continue;
        };
        project_scenario(scenario, &record);
    }
    if let Some(object) = report.as_object_mut() {
        object.insert("outer_cleanup_projection".into(), record);
    }
}


/// Re-verify a persisted outer cleanup projection from the receipts it names.
///
/// A persisted report that claims the observation is only credible when the
/// receipts it names are still there and still say the same thing. A deleted,
/// replaced or changed receipt leaves the observation unobserved: the
/// canonical read never trusts the persisted claim alone.
pub fn verify_persisted_projection(report: &Value) -> Option<OuterCleanupFacts> {
    let record = report.get("outer_cleanup_projection")?;
    if record.get("status").and_then(Value::as_str) != Some("observed") {
        return None;
    }
    let receipts = record.get("receipts")?;
    let path_of = |key: &str| -> Option<String> {
        receipts
            .get(key)
            .and_then(|value| value.get("path"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let read = |value: &Option<String>| -> Option<Vec<u8>> {
        value.as_ref().and_then(|path| std::fs::read(path).ok())
    };
    let digest_of_path = |value: &Option<String>| -> Option<String> {
        value
            .as_ref()
            .and_then(|path| std::fs::read(path).ok())
            .map(|bytes| digest_of(&bytes))
    };
    let mtime_of = |value: &Option<String>| -> Option<u64> {
        value.as_ref().and_then(|path| {
            std::fs::metadata(path)
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|value| value.as_millis() as u64)
        })
    };
    let harness_report_path = path_of("harness_report")?;
    let inner_receipt_path = path_of("inner_receipt")?;
    let final_receipt_path = path_of("final_receipt")?;
    let cleanup_result_path = path_of("cleanup_result");
    let shutdown_validation_path = path_of("shutdown_validation");
    let owner_report_path = path_of("owner_report");
    let manifest_path = path_of("manifest");
    let sealed_key_path = path_of("sealed_key");
    let harness_bytes = read(&Some(harness_report_path.clone()))?;
    let inner_bytes = read(&Some(inner_receipt_path.clone()))?;
    let final_bytes = read(&Some(final_receipt_path.clone()))?;
    let cleanup_bytes = read(&cleanup_result_path);
    let shutdown_bytes = read(&shutdown_validation_path);
    let owner_bytes = read(&owner_report_path);
    let manifest_bytes = read(&manifest_path);
    let matches = |key: &str, bytes: &[u8]| -> bool {
        receipts
            .get(key)
            .and_then(|value| value.get("sha256"))
            .and_then(Value::as_str)
            == Some(digest_of(bytes).as_str())
    };
    if !matches("harness_report", &harness_bytes)
        || !matches("inner_receipt", &inner_bytes)
        || !matches("final_receipt", &final_bytes)
    {
        return None;
    }
    if let Some(bytes) = &cleanup_bytes {
        if !matches("cleanup_result", bytes) {
            return None;
        }
    }
    if let Some(bytes) = &shutdown_bytes {
        if !matches("shutdown_validation", bytes) {
            return None;
        }
    }
    if let Some(bytes) = &owner_bytes {
        if !matches("owner_report", bytes) {
            return None;
        }
    }
    if let Some(bytes) = &manifest_bytes {
        if !matches("manifest", bytes) {
            return None;
        }
    }
    let parse = |bytes: Option<Vec<u8>>| -> Option<Value> {
        bytes.and_then(|bytes| serde_json::from_slice(&bytes).ok())
    };
    let shutdown_receipt_sha256 = parse(shutdown_bytes.clone())
        .and_then(|value| value.get("receipt_sha256").and_then(Value::as_str).map(str::to_string));
    let owner_report_mtime = mtime_of(&owner_report_path);
    let receipts = OuterCleanupReceipts {
        harness_report: parse(Some(harness_bytes))?,
        harness_report_sha256: digest_of(&std::fs::read(&harness_report_path).ok()?),
        inner_receipt: parse(Some(inner_bytes))?,
        inner_receipt_sha256: digest_of(&std::fs::read(&inner_receipt_path).ok()?),
        final_receipt: parse(Some(final_bytes))?,
        final_receipt_sha256: digest_of(&std::fs::read(&final_receipt_path).ok()?),
        cleanup_result: parse(cleanup_bytes),
        cleanup_result_sha256: digest_of_path(&cleanup_result_path),
        shutdown_validation: parse(shutdown_bytes),
        shutdown_validation_sha256: digest_of_path(&shutdown_validation_path),
        shutdown_receipt_sha256,
        owner_report: parse(owner_bytes),
        owner_report_sha256: digest_of_path(&owner_report_path),
        manifest: parse(manifest_bytes),
        manifest_sha256: digest_of_path(&manifest_path),
        sealed_key_path: sealed_key_path.clone(),
        sources: ReceiptSources {
            harness_report: harness_report_path,
            inner_receipt: inner_receipt_path,
            final_receipt: final_receipt_path,
            cleanup_result: cleanup_result_path,
            shutdown_validation: shutdown_validation_path,
            owner_report: owner_report_path,
            manifest: manifest_path,
            sealed_key: sealed_key_path,
        },
        harness_report_mtime_ms: mtime_of(&Some(path_of("harness_report")?)),
        final_receipt_mtime_ms: mtime_of(&Some(path_of("final_receipt")?)),
        owner_report_mtime_ms: owner_report_mtime,
    };
    observe_outer_cleanup(&receipts).ok()
}

/// Apply the gate's verdict to a projected report.
///
/// The outer observation never decides the business; the gate does. Only a
/// gate that passed turns the scenario's disposition into `passed`.
pub fn apply_gate_verdict(report: &mut Value, gate_passed: bool) {
    if !gate_passed {
        return;
    }
    for path in [
        "/live_gateway_scenario_details/scenarios/0",
        "/live_gateway_scenarios/scenarios/0",
    ] {
        let Some(scenario) = report.pointer_mut(path) else {
            continue;
        };
        if let Some(acceptance) = scenario.pointer_mut("/acceptance") {
            if let Some(map) = acceptance.as_object_mut() {
                map.insert("passed".into(), json!(true));
            }
        }
        if let Some(map) = scenario.as_object_mut() {
            map.insert("status".into(), json!("passed"));
        }
    }
    if let Some(live) = report
        .get_mut("live_gateway_scenarios")
        .and_then(Value::as_object_mut)
    {
        live.insert("status".into(), json!("passed"));
        live.insert("claim_status".into(), json!("component_passed"));
        live.insert("passed".into(), json!(1));
        live.insert("failed".into(), json!(0));
    }
    if let Some(scenarios) = report.get_mut("scenarios").and_then(Value::as_array_mut) {
        if let Some(first) = scenarios.first_mut().and_then(Value::as_object_mut) {
            first.insert("status".into(), json!("passed"));
        }
    }
}

/// Apply the gate's verdict to a projected scenario artifact.
pub fn apply_gate_verdict_to_scenario(artifact: &mut Value, gate_passed: bool) {
    if !gate_passed {
        return;
    }
    if let Some(acceptance) = artifact.pointer_mut("/acceptance") {
        if let Some(map) = acceptance.as_object_mut() {
            map.insert("passed".into(), json!(true));
        }
    }
    if let Some(map) = artifact.as_object_mut() {
        map.insert("status".into(), json!("passed"));
    }
}

/// The digest of a receipt file, as the receipts state it.
#[must_use]
pub fn receipt_digest(bytes: &[u8]) -> String {
    digest_of(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sealed evidence key the owner names: a real file, because a named
    /// path that does not hold the key is not a custody fact.
    fn sealed_key() -> (String, String) {
        let path = std::env::temp_dir().join(format!(
            "u5-outer-cleanup-sealed-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|value| value.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::write(&path, b"sealed-key-bytes").expect("sealed key fixture");
        let digest = digest_of(b"sealed-key-bytes");
        (path.display().to_string(), digest)
    }

    fn owner_binding() -> Value {
        json!({
            "candidate":"1a2918059affc324393cdb02acab50da3fbeec6b",
            "harness_sha256":"fb38e8ba119a7a051194e9c646ed9e6015bb00841ae2a4c242e1989431ce8826",
            "image_sha256":"sha256:8028d954b08a2b970cd398651e61500a98ff180c04fb050766577be7b0979b07",
            "namespace":"663254371ef4c0cca4a25fc12e47fddf8b15fb6cc7783120a777e71619f7dffa:cowd-real-pilot-irw9zy9w",
            "nonce":"b582282e4d19dca1c00605d69570261c9ae5b782d807c31875d4484eebb129f1",
            "pid":981141,
            "source_sha256":"685e5add74097ec9c1f8387698f46209a5c66a6c694338cba0a0ed7f0d73da33",
            "starttime":101130574
        })
    }

    fn harness_report() -> Value {
        json!({
            "kind":"harness_eval.u5_internal",
            "status":"pending_outer_cleanup",
            "run_id":"harness-eval-deep-66b0721b",
            "u5_host":owner_binding(),
            "live_gateway_scenarios":{
                "status":"pending_outer_cleanup",
                "claim_status":"pending_outer_cleanup",
                "selected_scenario_ids":["live_u5_active_cancel_isolation"],
                "passed":0,
                "failed":0,
                "scenarios":[{
                    "scenario_id":"live_u5_active_cancel_isolation",
                    "status":"pending_outer_cleanup",
                    "acceptance":{"passed":false,"checks":{"internal_passed":true,"outer_cleanup_observed":false}},
                    "outer_cleanup":"not_observed",
                    "sessions":[
                        {"session_id":"session-left","turn_id":"turn-left","execution_id":"execution-left"},
                        {"session_id":"session-right","turn_id":"turn-right","execution_id":"execution-right"}
                    ]
                }]
            },
            "live_gateway_scenario_details":{
                "scenarios":[{
                    "scenario_id":"live_u5_active_cancel_isolation",
                    "status":"pending_outer_cleanup",
                    "acceptance":{"passed":false,"checks":{"internal_passed":true,"outer_cleanup_observed":false}},
                    "outer_cleanup":"not_observed",
                    "sessions":[
                        {"session_id":"session-left","turn_id":"turn-left","execution_id":"execution-left"},
                        {"session_id":"session-right","turn_id":"turn-right","execution_id":"execution-right"}
                    ]
                }]
            },
            "scenarios":[{"capability":"live_u5_active_cancel_isolation","status":"pending_outer_cleanup"}]
        })
    }

    fn receipts() -> OuterCleanupReceipts {
        let (sealed_key_path, sealed_key_sha) = sealed_key();
        let report = harness_report();
        let inner = json!({
            "schema_version":1,
            "disposition":"pending_outer_cleanup",
            "run_id":"harness-eval-deep-66b0721b",
            "owner":owner_binding(),
            "report_path":"/evidence/report.json",
            "report_sha256":"report-digest",
            "scenario":"live_u5_active_cancel_isolation",
            "usage":{"physical_calls":3}
        });
        let final_receipt = json!({
            "schema_version":1,
            "status":"passed",
            "scope":"focused_U5_C_local_only",
            "original_harness_exit":75,
            "run_id":"harness-eval-deep-66b0721b",
            "owner":owner_binding(),
            "inner_sha256":"inner-digest",
            "report_sha256":"report-digest",
            "cleanup_sha256":"cleanup-digest",
            "shutdown_validation_sha256":"validation-digest",
            "shutdown_receipt_sha256":"shutdown-receipt"
        });
        let cleanup = json!({
            "business_exit":75,
            "cleanup_failed":false,
            "gateway_wait_status":"0",
            "remaining_owned":[]
        });
        let validation = json!({
            "status":"passed",
            "receipt_sha256":"shutdown-receipt",
            "identity":{
                "nonce":owner_binding()["nonce"].clone(),
                "pid":owner_binding()["pid"].clone(),
                "starttime":owner_binding()["starttime"].clone(),
                "image_sha256":owner_binding()["image_sha256"].clone()
            }
        });
        let owner_report = json!({
            "run_id":"owner-run",
            "outer_record_reserved":true,
            "outer_launcher_declaration":{"status":"received","pid":980636,"starttime":"101130308","sha256":"manifest-digest"},
            "labels":{"cowd.test.owner":"a644c83586812997"},
            "child_identity":{"pid":980636,"ppid":980011,"pgid":980636,"starttime":"101130308"},
            "container":"cowd-test-x-a644c83586812997",
            "volume":"cowd-test-x-a644c83586812997-data",
            "port":40379,
            "owned_processes":[{"pid":981141,"ppid":980639,"pgid":980636,"starttime":"101130574"}],
            "cleanup_errors":[],
            "remaining_processes":[],
            "remaining_db_clients":0,
            "child_reaped":true,
            "execution_status":"passed",
            "status":"acceptance_pending",
            "container_stopped":true,
            "container_retained":true,
            "volume_retained":true,
            "PG_process_returned":true,
            "private_root_retained":true,
            "port_bindable":true,
            "encrypted_evidence":{"path":"/evidence/pg.json.evidence.enc","sha256":"enc","key_reference":sealed_key_path,"key_sha256":sealed_key_sha},
            "finished_at":1793430794.2777748
        });
        let manifest = json!({
            "harness_run_id":"harness-eval-deep-66b0721b",
            "scenario":"live_u5_active_cancel_isolation",
            "owner_run_id":"owner-run",
            "owner_nonce":"a644c83586812997",
        });
        OuterCleanupReceipts {
            manifest: Some(manifest),
            manifest_sha256: Some("manifest-digest".into()),
            sealed_key_path: Some(sealed_key_path.clone()),
            owner_report_sha256: Some("owner-digest".into()),
            shutdown_validation_sha256: Some("validation-digest".into()),
            shutdown_receipt_sha256: Some("shutdown-receipt".into()),
            sources: ReceiptSources::default(),
            harness_report: report,
            harness_report_sha256: "report-digest".into(),
            inner_receipt: inner,
            inner_receipt_sha256: "inner-digest".into(),
            final_receipt,
            final_receipt_sha256: "final-digest".into(),
            cleanup_result: Some(cleanup),
            cleanup_result_sha256: Some("cleanup-digest".into()),
            shutdown_validation: Some(validation),
            owner_report: Some(owner_report),
            harness_report_mtime_ms: Some(1_000),
            final_receipt_mtime_ms: Some(2_000),
            owner_report_mtime_ms: Some(2_100),
        }
    }

    #[test]
    fn a_bound_outer_cleanup_is_observed() {
        let receipts = receipts();
        let facts = observe_outer_cleanup(&receipts).expect("bound outer cleanup is observed");
        assert_eq!(facts.run_id, "harness-eval-deep-66b0721b");
        assert_eq!(facts.scenario, "live_u5_active_cancel_isolation");
        assert_eq!(facts.remaining_owned, 0);
        assert_eq!(facts.gateway_wait_status, "0");
        assert!(facts.owner_layer.is_some());
    }

    #[test]
    fn the_projection_records_the_observation_and_leaves_the_business_alone() {
        let receipts = receipts();
        let facts = observe_outer_cleanup(&receipts).expect("bound outer cleanup is observed");
        let mut report = harness_report();
        project_outer_cleanup(&mut report, &facts);
        assert_eq!(
            report.pointer("/live_gateway_scenario_details/scenarios/0/acceptance/checks/outer_cleanup_observed"),
            Some(&Value::Bool(true))
        );
        // The business state is not rewritten by the outer observation.
        assert_eq!(
            report.pointer("/live_gateway_scenario_details/scenarios/0/acceptance/passed"),
            Some(&Value::Bool(false))
        );
        assert_eq!(
            report.pointer("/live_gateway_scenario_details/scenarios/0/status"),
            Some(&json!("pending_outer_cleanup"))
        );
    }

    #[test]
    fn a_failing_business_predicate_is_never_turned_into_a_pass() {
        let mut report = harness_report();
        report["live_gateway_scenarios"]["scenarios"][0]["acceptance"]["checks"]["identity"] =
            json!(false);
        let receipts = receipts();
        let facts = observe_outer_cleanup(&receipts).expect("bound outer cleanup is observed");
        let mut projected = report.clone();
        project_outer_cleanup(&mut projected, &facts);
        let gate = crate::report::u5_stage_gate_with_outer(&projected, Some(&facts));
        assert_ne!(
            gate.status, "passed",
            "a scenario with a failing business predicate must not pass"
        );
    }

    #[test]
    fn a_foreign_scenario_is_refused() {
        let mut report = harness_report();
        report["live_gateway_scenarios"]["scenarios"][0]["scenario_id"] = json!("foreign");
        report["live_gateway_scenarios"]["scenarios"][0]["status"] = json!("failed");
        report["live_gateway_scenarios"]["scenarios"][0]["acceptance"]["checks"]["identity"] =
            json!(false);
        let mut receipts = receipts();
        receipts.harness_report = report;
        let gap = observe_outer_cleanup(&receipts)
            .expect_err("a foreign scenario is not the bound scenario");
        assert_eq!(gap.reason(), "wrong_run");
    }

    #[test]
    fn missing_owner_authenticated_launcher_declaration_is_unobserved() {
        let mut receipts = receipts();
        receipts.owner_report.as_mut().unwrap().as_object_mut().unwrap().remove("outer_launcher_declaration");
        let gap = observe_outer_cleanup(&receipts).expect_err("child-authored manifest lacks owner declaration");
        assert!(matches!(gap, OuterCleanupGap::Missing(_)), "{gap:?}");
    }

    #[test]
    fn forged_launcher_declaration_digest_and_sender_are_refused() {
        let mut changed = receipts();
        changed.owner_report.as_mut().unwrap()["outer_launcher_declaration"]["sha256"] = json!("other");
        assert!(matches!(observe_outer_cleanup(&changed), Err(OuterCleanupGap::DigestMismatch(_))));
        let mut changed = receipts();
        changed.owner_report.as_mut().unwrap()["outer_launcher_declaration"]["pid"] = json!(999999);
        assert!(matches!(observe_outer_cleanup(&changed), Err(OuterCleanupGap::WrongOwner(_))));
        let mut changed = receipts();
        changed.owner_report.as_mut().unwrap()["outer_record_reserved"] = json!(false);
        assert!(matches!(observe_outer_cleanup(&changed), Err(OuterCleanupGap::Missing(_))));
    }

    #[test]
    fn an_owner_run_that_is_not_the_declared_run_is_refused() {
        let mut receipts = receipts();
        receipts.manifest.as_mut().expect("manifest")["owner_run_id"] = json!("another-owner-run");
        let gap = observe_outer_cleanup(&receipts).expect_err("another owner run is refused");
        assert_eq!(gap.reason(), "wrong_run");
    }

    #[test]
    fn an_owner_that_did_not_reap_its_child_is_refused() {
        let mut receipts = receipts();
        receipts.owner_report.as_mut().expect("owner report")["child_reaped"] = json!(false);
        let gap = observe_outer_cleanup(&receipts).expect_err("an unreaped child is refused");
        assert_eq!(gap.reason(), "owner_cleanup_incomplete");
    }

    #[test]
    fn an_owner_that_is_still_running_is_refused() {
        let mut receipts = receipts();
        receipts.owner_report.as_mut().expect("owner report")["status"] = json!("running");
        let gap = observe_outer_cleanup(&receipts).expect_err("a running owner is not final");
        assert_eq!(gap.reason(), "owner_cleanup_incomplete");
    }

    #[test]
    fn a_sealed_key_that_does_not_exist_is_refused() {
        let mut receipts = receipts();
        receipts.sealed_key_path = Some("/nonexistent/sealed.key".to_string());
        let gap = observe_outer_cleanup(&receipts).expect_err("a missing sealed key is refused");
        assert_eq!(gap.reason(), "owner_cleanup_incomplete");
    }

    #[test]
    fn a_mutated_shutdown_validation_is_refused() {
        let mut receipts = receipts();
        receipts.shutdown_validation_sha256 = Some("another-digest".into());
        let gap = observe_outer_cleanup(&receipts).expect_err("a mutated shutdown receipt is refused");
        assert_eq!(gap.reason(), "digest_mismatch");
    }

    #[test]
    fn a_missing_outer_receipt_leaves_the_observation_unobserved() {
        let mut receipts = receipts();
        receipts.final_receipt = Value::Null;
        let gap = observe_outer_cleanup(&receipts).expect_err("a missing receipt is not observed");
        assert_eq!(gap.reason(), "missing_outer_fact");
    }

    #[test]
    fn another_run_is_rejected() {
        let mut receipts = receipts();
        receipts.final_receipt["run_id"] = json!("another-run");
        let gap = observe_outer_cleanup(&receipts).expect_err("another run is rejected");
        assert_eq!(gap.reason(), "wrong_run");
    }

    #[test]
    fn another_owner_is_rejected() {
        let mut receipts = receipts();
        receipts.final_receipt["owner"]["nonce"] = json!("0000000000000000000000000000000000000000000000000000000000000000");
        let gap = observe_outer_cleanup(&receipts).expect_err("another owner is rejected");
        assert_eq!(gap.reason(), "wrong_owner");
    }

    #[test]
    fn another_report_digest_is_rejected() {
        let mut receipts = receipts();
        receipts.final_receipt["report_sha256"] = json!("other-report");
        let gap = observe_outer_cleanup(&receipts).expect_err("another report digest is rejected");
        assert_eq!(gap.reason(), "digest_mismatch");
    }

    #[test]
    fn an_outer_receipt_that_predates_the_report_is_rejected() {
        let mut receipts = receipts();
        receipts.final_receipt_mtime_ms = Some(999);
        let gap = observe_outer_cleanup(&receipts).expect_err("an out of order receipt is rejected");
        assert_eq!(gap.reason(), "outer_fact_out_of_order");
    }

    #[test]
    fn resources_that_were_not_returned_are_rejected() {
        let mut receipts = receipts();
        receipts.cleanup_result.as_mut().expect("cleanup result")["remaining_owned"] =
            json!([{"pid":42}]);
        let gap = observe_outer_cleanup(&receipts).expect_err("remaining resources are rejected");
        assert_eq!(gap.reason(), "cleanup_not_returned");
    }

    #[test]
    fn an_owner_that_left_resources_behind_is_rejected() {
        let mut receipts = receipts();
        receipts.owner_report.as_mut().expect("owner report")["remaining_processes"] =
            json!([{"pid":7}]);
        let gap = observe_outer_cleanup(&receipts).expect_err("an incomplete owner is rejected");
        assert_eq!(gap.reason(), "owner_cleanup_incomplete");
    }

    #[test]
    fn a_missing_owner_report_leaves_the_observation_unobserved() {
        let mut receipts = receipts();
        receipts.owner_report = None;
        let gap = observe_outer_cleanup(&receipts)
            .expect_err("a missing owner report is not observed");
        assert_eq!(gap.reason(), "missing_outer_fact");
        assert!(gap.detail().contains("owner cleanup report"));
    }

    #[test]
    fn a_missing_cleanup_result_leaves_the_observation_unobserved() {
        let mut receipts = receipts();
        receipts.cleanup_result = None;
        receipts.cleanup_result_sha256 = None;
        let gap = observe_outer_cleanup(&receipts)
            .expect_err("a missing cleanup result is not observed");
        assert_eq!(gap.reason(), "missing_outer_fact");
        assert!(gap.detail().contains("launcher cleanup result"));
    }

    #[test]
    fn a_report_that_claims_the_observation_without_the_receipts_is_refused() {
        let mut claimed = harness_report();
        claimed["live_gateway_scenarios"]["scenarios"][0]["status"] = json!("passed");
        claimed["live_gateway_scenarios"]["scenarios"][0]["acceptance"]["passed"] = json!(true);
        claimed["live_gateway_scenarios"]["scenarios"][0]["acceptance"]["checks"]
            ["outer_cleanup_observed"] = json!(true);
        claimed["live_gateway_scenarios"]["status"] = json!("passed");
        // The gate refuses the claim: without the receipts the outer facts are
        // not observed, so the report keeps its pending disposition.
        let gate = crate::report::evaluate_report_gate(&claimed);
        assert_ne!(gate.status, "passed");
    }

    #[test]
    fn an_owner_that_did_not_own_the_harness_process_is_rejected() {
        let mut receipts = receipts();
        receipts.owner_report.as_mut().expect("owner report")["owned_processes"] = json!([]);
        let gap = observe_outer_cleanup(&receipts).expect_err("an unrelated owner is rejected");
        assert_eq!(gap.reason(), "wrong_owner");
    }
}
