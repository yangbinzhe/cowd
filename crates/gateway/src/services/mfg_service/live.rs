use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

use app_mfg::{MfgLiveDeltaRead, MfgLiveEpoch, MfgLiveProjectionEvent};
use app_mfg_contract::{
    MfgContractVersion, MfgLiveDeltaV1, MfgLiveEnvelopeV1, MfgLiveEventV1, MfgLiveHeartbeatV1,
    MfgLiveResyncV1, MfgLiveSnapshotStateV1, MfgLiveSnapshotV1,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::MfgService;

const CURSOR_KEY_FILE: &str = "mfg-live-cursor.key";
const CURSOR_KEY_BYTES: usize = 32;
const CURSOR_NONCE_BYTES: usize = 16;
const MAX_HEARTBEAT_SCAN_EVENTS: usize = 10_000;

#[derive(Debug, Clone)]
pub(crate) struct MfgLivePrincipalContext {
    pub(crate) principal_id: String,
    pub(crate) profile_revision: u64,
    pub(crate) scopes: Vec<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) credential_epoch: u64,
    pub(crate) expires_at_ms: Option<u64>,
}

impl MfgLivePrincipalContext {
    fn scope_hash(&self) -> String {
        let mut scopes = self.scopes.clone();
        scopes.sort();
        scopes.dedup();
        format!("{:x}", Sha256::digest(scopes.join("\n").as_bytes()))
    }

    fn capability_hash(&self) -> String {
        let mut capabilities = self.capabilities.clone();
        capabilities.sort();
        capabilities.dedup();
        format!("{:x}", Sha256::digest(capabilities.join("\n").as_bytes()))
    }

    fn actor_ref(&self) -> String {
        format!("principal:{}", self.principal_id)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MfgLiveServiceError {
    #[error("MFG live cursor key is invalid: {0}")]
    InvalidCursorKey(String),
    #[error("MFG live cursor key I/O failed: {0}")]
    CursorKeyIo(String),
    #[error(transparent)]
    Repository(#[from] app_mfg::MfgRepositoryError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MfgLiveCursorPayload {
    epoch_id: String,
    internal_cursor: u64,
    principal_id: String,
    profile_revision: u64,
    scope_hash: String,
    capability_hash: String,
    contract_version: String,
}

impl MfgService {
    pub(crate) async fn live_authorization_error_async(
        &self,
        config_home: PathBuf,
        principal: MfgLivePrincipalContext,
    ) -> Option<app_mfg_contract::MfgApiErrorV1> {
        tokio::task::spawn_blocking(move || live_authorization_error(&config_home, &principal))
            .await
            .unwrap_or_else(|error| {
                Some(app_mfg_contract::MfgApiErrorV1::authentication_required(
                    format!("MFG live authorization check failed: {error}"),
                ))
            })
    }

    pub(crate) async fn live_snapshot_envelope_async(
        &self,
        config_home: PathBuf,
        principal: MfgLivePrincipalContext,
    ) -> Result<MfgLiveEnvelopeV1, MfgLiveServiceError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.live_snapshot_envelope(config_home, &principal))
            .await
            .map_err(|error| MfgLiveServiceError::CursorKeyIo(error.to_string()))?
    }

    pub(crate) async fn live_delta_envelope_async(
        &self,
        config_home: PathBuf,
        principal: MfgLivePrincipalContext,
        previous_view_epoch: String,
        cursor: String,
        limit: usize,
    ) -> Result<Option<MfgLiveEnvelopeV1>, MfgLiveServiceError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.live_delta_envelope(
                config_home,
                &principal,
                &previous_view_epoch,
                &cursor,
                limit,
            )
        })
        .await
        .map_err(|error| MfgLiveServiceError::CursorKeyIo(error.to_string()))?
    }

    pub(crate) async fn live_heartbeat_envelope_async(
        &self,
        config_home: PathBuf,
        principal: MfgLivePrincipalContext,
        cursor: String,
    ) -> Result<MfgLiveEnvelopeV1, MfgLiveServiceError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.live_heartbeat_envelope(config_home, &principal, &cursor)
        })
        .await
        .map_err(|error| MfgLiveServiceError::CursorKeyIo(error.to_string()))?
    }

    pub(crate) async fn live_resync_envelope_async(
        &self,
        config_home: PathBuf,
        principal: MfgLivePrincipalContext,
        previous_view_epoch: String,
        reason: String,
    ) -> Result<MfgLiveEnvelopeV1, MfgLiveServiceError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let key = service.load_or_create_live_cursor_key(&config_home)?;
            let epoch = service.live_epoch(&config_home)?;
            resync_envelope(&key, &epoch, &principal, &previous_view_epoch, &reason)
        })
        .await
        .map_err(|error| MfgLiveServiceError::CursorKeyIo(error.to_string()))?
    }

    pub(crate) fn live_snapshot_envelope(
        &self,
        config_home: impl AsRef<Path>,
        principal: &MfgLivePrincipalContext,
    ) -> Result<MfgLiveEnvelopeV1, MfgLiveServiceError> {
        let config_home = config_home.as_ref();
        let key = self.load_or_create_live_cursor_key(config_home)?;
        let snapshot = self.live_snapshot_read(config_home)?;
        let cursor = encode_cursor(&key, &snapshot.epoch, snapshot.high_cursor, principal)?;
        Ok(MfgLiveEnvelopeV1::Snapshot(MfgLiveSnapshotV1 {
            view_epoch: public_view_epoch(&key, &snapshot.epoch, principal),
            cursor,
            generated_at: Utc::now(),
            contract_version: MfgContractVersion::default(),
            state: crop_snapshot_state(snapshot.state, principal),
        }))
    }

    pub(crate) fn live_delta_envelope(
        &self,
        config_home: impl AsRef<Path>,
        principal: &MfgLivePrincipalContext,
        previous_view_epoch: &str,
        cursor: &str,
        limit: usize,
    ) -> Result<Option<MfgLiveEnvelopeV1>, MfgLiveServiceError> {
        let config_home = config_home.as_ref();
        let key = self.load_or_create_live_cursor_key(config_home)?;
        let epoch = self.live_epoch(config_home)?;
        let current_view_epoch = public_view_epoch(&key, &epoch, principal);
        let payload = match decode_cursor(&key, cursor) {
            Ok(payload)
                if cursor_payload_matches(&payload, &epoch, principal)
                    && previous_view_epoch == current_view_epoch =>
            {
                payload
            }
            Ok(_) => {
                return Ok(Some(resync_envelope(
                    &key,
                    &epoch,
                    principal,
                    previous_view_epoch,
                    "view_scope_changed",
                )?))
            }
            Err(_) => {
                return Ok(Some(resync_envelope(
                    &key,
                    &epoch,
                    principal,
                    previous_view_epoch,
                    "cursor_invalid",
                )?))
            }
        };
        let mut scan_cursor = payload.internal_cursor;
        loop {
            let delta = self.live_delta_read(config_home, scan_cursor, limit)?;
            if let Some(reason) = &delta.resync_reason {
                return Ok(Some(resync_envelope(
                    &key,
                    &delta.epoch,
                    principal,
                    previous_view_epoch,
                    reason,
                )?));
            }
            let high_cursor = delta.high_cursor;
            let next_scan_cursor = delta
                .events
                .last()
                .map_or(delta.base_cursor, |event| event.cursor);
            let events = visible_coalesced_events(delta, principal);
            if !events.is_empty() {
                let target_cursor = encode_cursor(&key, &epoch, next_scan_cursor, principal)?;
                return Ok(Some(MfgLiveEnvelopeV1::Delta(MfgLiveDeltaV1 {
                    view_epoch: current_view_epoch,
                    base_cursor: cursor.to_string(),
                    target_cursor,
                    events,
                })));
            }
            if next_scan_cursor <= scan_cursor || next_scan_cursor >= high_cursor {
                return Ok(None);
            }
            scan_cursor = next_scan_cursor;
        }
    }

    pub(crate) fn live_heartbeat_envelope(
        &self,
        config_home: impl AsRef<Path>,
        principal: &MfgLivePrincipalContext,
        cursor: &str,
    ) -> Result<MfgLiveEnvelopeV1, MfgLiveServiceError> {
        let config_home = config_home.as_ref();
        let key = self.load_or_create_live_cursor_key(config_home)?;
        let epoch = self.live_epoch(config_home)?;
        let payload = decode_cursor(&key, cursor).ok();
        let base = payload
            .filter(|payload| cursor_payload_matches(payload, &epoch, principal))
            .map_or(epoch.retention_low_cursor, |payload| {
                payload.internal_cursor
            });
        let mut scan_cursor = base;
        let mut scanned_events = 0_usize;
        'scan: loop {
            let delta = self.live_delta_read(config_home, scan_cursor, 500)?;
            let high_cursor = delta.high_cursor;
            let mut next_scan_cursor = scan_cursor;
            for event in delta.events {
                if live_event_visible(&event, principal) {
                    scan_cursor = next_scan_cursor;
                    break 'scan;
                }
                next_scan_cursor = event.cursor;
                scanned_events = scanned_events.saturating_add(1);
                if scanned_events >= MAX_HEARTBEAT_SCAN_EVENTS {
                    scan_cursor = next_scan_cursor;
                    break 'scan;
                }
            }
            if next_scan_cursor <= scan_cursor || next_scan_cursor >= high_cursor {
                scan_cursor = next_scan_cursor;
                break;
            }
            scan_cursor = next_scan_cursor;
        }
        Ok(MfgLiveEnvelopeV1::Heartbeat(MfgLiveHeartbeatV1 {
            view_epoch: public_view_epoch(&key, &epoch, principal),
            cursor: encode_cursor(&key, &epoch, scan_cursor, principal)?,
            generated_at: Utc::now(),
        }))
    }

    fn load_or_create_live_cursor_key(
        &self,
        config_home: &Path,
    ) -> Result<[u8; CURSOR_KEY_BYTES], MfgLiveServiceError> {
        let path = config_home.join(CURSOR_KEY_FILE);
        match read_cursor_key(&path) {
            Ok(key) => Ok(key),
            Err(CursorKeyReadError::Missing) => {
                let _guard = self
                    .live_key_lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match read_cursor_key(&path) {
                    Ok(key) => return Ok(key),
                    Err(CursorKeyReadError::Missing) => {}
                    Err(error) => return Err(cursor_key_read_to_service_error(error)),
                }
                fs::create_dir_all(config_home)
                    .map_err(|error| MfgLiveServiceError::CursorKeyIo(error.to_string()))?;
                self.live_epoch(config_home)?;
                let (key, created) = create_cursor_key_atomically(&path)?;
                if created {
                    self.rotate_live_epoch(config_home, "cursor_key_recreated")?;
                }
                Ok(key)
            }
            Err(CursorKeyReadError::Invalid(message)) => {
                Err(MfgLiveServiceError::InvalidCursorKey(message))
            }
            Err(CursorKeyReadError::Io(message)) => Err(MfgLiveServiceError::CursorKeyIo(message)),
        }
    }
}

fn live_authorization_error(
    config_home: &Path,
    principal: &MfgLivePrincipalContext,
) -> Option<app_mfg_contract::MfgApiErrorV1> {
    if !principal
        .capabilities
        .iter()
        .any(|capability| capability == "mfg.read")
    {
        return Some(app_mfg_contract::MfgApiErrorV1::capability_denied(
            "mfg.read",
        ));
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(u64::MAX, |duration| duration.as_millis() as u64);
    if principal
        .expires_at_ms
        .is_some_and(|expires_at| expires_at <= now_ms)
    {
        return Some(live_authentication_required(
            "MFG live principal expired",
            "principal_expired",
        ));
    }
    let client = auth_broker::BrokerClient::new(auth_broker::BrokerClient::default_socket(
        config_home.join("auth-broker"),
    ));
    let lifecycle = match client.credential_lifecycle() {
        Ok(lifecycle) => lifecycle,
        Err(error) => {
            return Some(live_authentication_required(
                format!("MFG live authorization authority is unavailable: {error}"),
                "authority_unavailable",
            ))
        }
    };
    if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
        return Some(live_authentication_required(
            "MFG live credential is no longer active",
            "credential_inactive",
        ));
    }
    if lifecycle.credential_epoch != principal.credential_epoch {
        return Some(live_authentication_required(
            "MFG live credential epoch changed; authenticate again",
            "credential_epoch_changed",
        ));
    }
    if lifecycle.profile_revision != principal.profile_revision {
        return Some(live_authentication_required(
            "MFG live authorization changed; authenticate again",
            "profile_revision_changed",
        ));
    }
    None
}

fn live_authentication_required(
    message: impl Into<String>,
    reason: &'static str,
) -> app_mfg_contract::MfgApiErrorV1 {
    let mut error = app_mfg_contract::MfgApiErrorV1::authentication_required(message);
    error.details = serde_json::json!({"reason": reason});
    error
}

fn public_view_epoch(
    key: &[u8; CURSOR_KEY_BYTES],
    epoch: &MfgLiveEpoch,
    principal: &MfgLivePrincipalContext,
) -> String {
    let material = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        epoch.epoch_id,
        principal.principal_id,
        principal.profile_revision,
        principal.scope_hash(),
        principal.capability_hash(),
        app_mfg_contract::MFG_CONTRACT_VERSION,
    );
    URL_SAFE_NO_PAD.encode(hmac_sha256(key, material.as_bytes()))
}

fn encode_cursor(
    key: &[u8; CURSOR_KEY_BYTES],
    epoch: &MfgLiveEpoch,
    internal_cursor: u64,
    principal: &MfgLivePrincipalContext,
) -> Result<String, MfgLiveServiceError> {
    let payload = MfgLiveCursorPayload {
        epoch_id: epoch.epoch_id.clone(),
        internal_cursor,
        principal_id: principal.principal_id.clone(),
        profile_revision: principal.profile_revision,
        scope_hash: principal.scope_hash(),
        capability_hash: principal.capability_hash(),
        contract_version: app_mfg_contract::MFG_CONTRACT_VERSION.to_string(),
    };
    let plaintext = serde_json::to_vec(&payload)
        .map_err(|error| MfgLiveServiceError::CursorKeyIo(error.to_string()))?;
    let nonce_digest = hmac_sha256_with_domain(key, b"cursor-nonce", &plaintext);
    let nonce = &nonce_digest[..CURSOR_NONCE_BYTES];
    let ciphertext = xor_cursor_payload(key, nonce, &plaintext);
    let mut sealed = Vec::with_capacity(CURSOR_NONCE_BYTES + ciphertext.len());
    sealed.extend_from_slice(nonce);
    sealed.extend_from_slice(&ciphertext);
    let signature = hmac_sha256_with_domain(key, b"cursor-auth", &sealed);
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(sealed),
        URL_SAFE_NO_PAD.encode(signature)
    ))
}

fn decode_cursor(key: &[u8; CURSOR_KEY_BYTES], token: &str) -> Result<MfgLiveCursorPayload, ()> {
    let (sealed, signature) = token.split_once('.').ok_or(())?;
    let sealed = URL_SAFE_NO_PAD.decode(sealed).map_err(|_| ())?;
    let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| ())?;
    if sealed.len() <= CURSOR_NONCE_BYTES
        || !constant_time_eq(
            &hmac_sha256_with_domain(key, b"cursor-auth", &sealed),
            &signature,
        )
    {
        return Err(());
    }
    let (nonce, ciphertext) = sealed.split_at(CURSOR_NONCE_BYTES);
    let plaintext = xor_cursor_payload(key, nonce, ciphertext);
    let payload = serde_json::from_slice::<MfgLiveCursorPayload>(&plaintext).map_err(|_| ())?;
    let expected_nonce = hmac_sha256_with_domain(key, b"cursor-nonce", &plaintext);
    if !constant_time_eq(nonce, &expected_nonce[..CURSOR_NONCE_BYTES]) {
        return Err(());
    }
    Ok(payload)
}

fn cursor_payload_matches(
    payload: &MfgLiveCursorPayload,
    epoch: &MfgLiveEpoch,
    principal: &MfgLivePrincipalContext,
) -> bool {
    payload.epoch_id == epoch.epoch_id
        && payload.principal_id == principal.principal_id
        && payload.profile_revision == principal.profile_revision
        && payload.scope_hash == principal.scope_hash()
        && payload.capability_hash == principal.capability_hash()
        && payload.contract_version == app_mfg_contract::MFG_CONTRACT_VERSION
}

fn resync_envelope(
    key: &[u8; CURSOR_KEY_BYTES],
    epoch: &MfgLiveEpoch,
    principal: &MfgLivePrincipalContext,
    previous_view_epoch: &str,
    reason: &str,
) -> Result<MfgLiveEnvelopeV1, MfgLiveServiceError> {
    Ok(MfgLiveEnvelopeV1::Resync(MfgLiveResyncV1 {
        previous_view_epoch: previous_view_epoch.to_string(),
        reason: reason.to_string(),
        snapshot_url: "/api/apps/mfg/live/snapshot".to_string(),
        latest_cursor: encode_cursor(key, epoch, epoch.retention_high_cursor, principal)?,
    }))
}

fn visible_coalesced_events(
    delta: MfgLiveDeltaRead,
    principal: &MfgLivePrincipalContext,
) -> Vec<MfgLiveEventV1> {
    let mut durable = Vec::new();
    let mut coalesced = BTreeMap::new();
    for event in delta
        .events
        .into_iter()
        .filter(|event| live_event_visible(event, principal))
    {
        if app_mfg_contract::mfg_live_event_priority(&event.event_type, &event.payload) <= 1 {
            durable.push(event);
        } else {
            coalesced.insert((event.event_type.clone(), event.subject_ref.clone()), event);
        }
    }
    durable.extend(coalesced.into_values());
    durable.sort_by_key(|event| event.cursor);
    durable
        .into_iter()
        .map(|event| contract_event(event, principal))
        .collect()
}

fn contract_event(
    mut event: MfgLiveProjectionEvent,
    principal: &MfgLivePrincipalContext,
) -> MfgLiveEventV1 {
    if let Some(profile) = event.payload.get_mut("profile") {
        crop_profile_widgets(profile, principal);
    }
    MfgLiveEventV1 {
        event_type: event.event_type,
        subject_ref: event.subject_ref,
        revision: highest_revision(&event.payload),
        occurred_at: event.created_at,
        payload: event.payload,
    }
}

fn highest_revision(value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Object(object) => {
            let direct = ["revision", "current_revision", "result_revision"]
                .into_iter()
                .filter_map(|field| object.get(field).and_then(serde_json::Value::as_u64))
                .max()
                .unwrap_or_default();
            object.values().map(highest_revision).fold(direct, u64::max)
        }
        serde_json::Value::Array(values) => values
            .iter()
            .map(highest_revision)
            .max()
            .unwrap_or_default(),
        _ => 0,
    }
}

fn crop_snapshot_state(
    mut state: MfgLiveSnapshotStateV1,
    principal: &MfgLivePrincipalContext,
) -> MfgLiveSnapshotStateV1 {
    if !principal
        .capabilities
        .iter()
        .any(|capability| capability == "mfg.read")
    {
        return MfgLiveSnapshotStateV1::default();
    }
    let mut report_profile_ids = BTreeSet::new();
    if let Some(profiles) = state
        .cockpit
        .get_mut("profiles")
        .and_then(serde_json::Value::as_array_mut)
    {
        profiles.retain(|profile| {
            let visible = cockpit_profile_visible(profile, principal);
            if visible && cockpit_profile_report_allowed(profile, principal) {
                if let Some(profile_id) = profile
                    .get("profile_id")
                    .and_then(serde_json::Value::as_str)
                {
                    report_profile_ids.insert(profile_id.to_string());
                }
            }
            visible
        });
        for profile in profiles {
            crop_profile_widgets(profile, principal);
        }
    }
    let visible_rule_ids = state
        .alerts
        .get("rules")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|rule| record_visible(rule, principal))
        .filter_map(|rule| rule.get("rule_id").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    filter_array_field(&mut state.alerts, "rules", |rule| {
        record_visible(rule, principal)
    });
    filter_array_field(&mut state.alerts, "subscriptions", |subscription| {
        record_visible(subscription, principal)
            && subscription
                .get("rule_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|rule_id| visible_rule_ids.contains(rule_id))
    });
    filter_array_field(&mut state.alerts, "occurrences", |occurrence| {
        occurrence
            .get("rule_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|rule_id| visible_rule_ids.contains(rule_id))
    });
    filter_array_field(&mut state.assignments, "items", |item| {
        assignment_visible(item, principal)
    });
    filter_array_field(&mut state.reports, "items", |item| {
        item.get("profile_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|profile_id| report_profile_ids.contains(profile_id))
    });
    let visible_report_ids = state
        .reports
        .get("items")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|report| report.get("report_id").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    filter_array_field(&mut state.reviews, "items", |review| {
        review
            .get("report_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|report_id| visible_report_ids.contains(report_id))
    });
    filter_array_field(&mut state.receipts, "commands", |receipt| {
        record_visible(receipt, principal)
    });
    filter_array_field(&mut state.receipts, "mutations", |receipt| {
        record_visible(receipt, principal)
    });
    state
}

fn filter_array_field(
    value: &mut serde_json::Value,
    field: &str,
    visible: impl Fn(&serde_json::Value) -> bool,
) {
    if let Some(items) = value
        .get_mut(field)
        .and_then(serde_json::Value::as_array_mut)
    {
        items.retain(visible);
    }
}

fn live_event_visible(event: &MfgLiveProjectionEvent, principal: &MfgLivePrincipalContext) -> bool {
    if !principal
        .capabilities
        .iter()
        .any(|capability| capability == "mfg.read")
    {
        return false;
    }
    let event_type = event.event_type.as_str();
    if event_type.starts_with("profile.") {
        return event
            .payload
            .get("profile")
            .is_some_and(|profile| cockpit_profile_visible(profile, principal));
    }
    if event_type.starts_with("assignment.") {
        return event
            .payload
            .get("assignment")
            .is_some_and(|assignment| assignment_visible(assignment, principal));
    }
    if event_type.starts_with("report.") || event_type.starts_with("report_review.") {
        return event.payload.get("profile").map_or_else(
            || record_visible(&event.payload, principal),
            |profile| cockpit_profile_report_allowed(profile, principal),
        );
    }
    if event_type.starts_with("alert_rule.")
        || event_type.starts_with("alert_subscription.")
        || event_type.starts_with("receipt.")
        || event_type.starts_with("notification.")
    {
        return record_visible(&event.payload, principal);
    }
    if event_type.starts_with("alert.") {
        return record_visible(&event.payload, principal);
    }
    matches!(
        event_type.split('.').next().unwrap_or_default(),
        "incident"
            | "workflow"
            | "analysis"
            | "execution"
            | "skill_run"
            | "compute_job"
            | "metric_state"
            | "metric_change"
            | "data_watermark"
            | "entity"
            | "relation"
            | "fact"
            | "attention"
            | "evidence"
            | "quality_gate"
            | "metric_definition"
            | "metric_dependency"
            | "metric_snapshot"
            | "source_pack"
            | "connector_run"
            | "ontology"
            | "memory_case"
            | "playbook"
    )
}

fn cockpit_profile_visible(
    profile: &serde_json::Value,
    principal: &MfgLivePrincipalContext,
) -> bool {
    if record_visible(profile, principal) {
        return true;
    }
    if profile
        .pointer("/sharing_policy/visibility")
        .and_then(serde_json::Value::as_str)
        != Some("team")
    {
        return false;
    }
    let Some(scope_ref) = profile
        .pointer("/scope/scope_ref")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
    else {
        return false;
    };
    let scope_kind = profile
        .pointer("/scope/kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let qualified = format!("{scope_kind}:{scope_ref}");
    principal
        .scopes
        .iter()
        .any(|scope| scope == scope_ref || scope == &qualified)
}

fn cockpit_profile_report_allowed(
    profile: &serde_json::Value,
    principal: &MfgLivePrincipalContext,
) -> bool {
    if !cockpit_profile_visible(profile, principal) {
        return false;
    }
    let definitions = app_mfg::mfg_widget_catalog()
        .into_iter()
        .map(|definition| (definition.definition_id, definition.required_capability))
        .collect::<BTreeMap<_, _>>();
    profile
        .get("widget_instances")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .all(|instance| {
            instance
                .get("definition_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|definition_id| definitions.get(definition_id))
                .is_some_and(|required| {
                    required.trim().is_empty()
                        || principal
                            .capabilities
                            .iter()
                            .any(|capability| capability == required)
                })
        })
}

fn crop_profile_widgets(profile: &mut serde_json::Value, principal: &MfgLivePrincipalContext) {
    let definitions = app_mfg::mfg_widget_catalog()
        .into_iter()
        .map(|definition| (definition.definition_id, definition.required_capability))
        .collect::<BTreeMap<_, _>>();
    if let Some(instances) = profile
        .get_mut("widget_instances")
        .and_then(serde_json::Value::as_array_mut)
    {
        instances.retain(|instance| {
            instance
                .get("definition_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|definition_id| definitions.get(definition_id))
                .is_some_and(|required| {
                    required.trim().is_empty()
                        || principal
                            .capabilities
                            .iter()
                            .any(|capability| capability == required)
                })
        });
    }
}

fn assignment_visible(assignment: &serde_json::Value, principal: &MfgLivePrincipalContext) -> bool {
    let actor = principal.actor_ref();
    if assignment
        .get("created_by")
        .and_then(serde_json::Value::as_str)
        == Some(actor.as_str())
        || assignment
            .get("assignee_ref")
            .and_then(serde_json::Value::as_str)
            == Some(actor.as_str())
        || assignment
            .get("watcher_refs")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .any(|watcher| watcher == actor.as_str())
    {
        return true;
    }
    if assignment
        .get("visibility")
        .and_then(serde_json::Value::as_str)
        == Some("public")
    {
        return true;
    }
    let Some(kind) = assignment
        .get("assignee_kind")
        .and_then(serde_json::Value::as_str)
        .filter(|kind| matches!(*kind, "team" | "role" | "organization"))
    else {
        return false;
    };
    if assignment
        .get("visibility")
        .and_then(serde_json::Value::as_str)
        != Some("team")
    {
        return false;
    }
    let assignee = assignment
        .get("assignee_ref")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let qualified = format!("{kind}:{assignee}");
    principal
        .scopes
        .iter()
        .any(|scope| scope == assignee || scope == &qualified)
}

fn record_visible(value: &serde_json::Value, principal: &MfgLivePrincipalContext) -> bool {
    let actor = principal.actor_ref();
    let scopes = principal.scopes.iter().cloned().collect::<BTreeSet<_>>();
    let mut protected_refs = Vec::new();
    let mut shared_refs = Vec::new();
    collect_visibility_refs(value, &mut protected_refs, &mut shared_refs);
    if has_public_visibility(value) {
        return true;
    }
    if protected_refs.is_empty() && shared_refs.is_empty() {
        return false;
    }
    protected_refs
        .iter()
        .any(|reference| *reference == actor || scopes.contains(*reference))
        || shared_refs
            .iter()
            .any(|reference| *reference == actor || scopes.contains(*reference))
}

fn collect_visibility_refs<'a>(
    value: &'a serde_json::Value,
    protected_refs: &mut Vec<&'a str>,
    shared_refs: &mut Vec<&'a str>,
) {
    match value {
        serde_json::Value::Object(object) => {
            for field in [
                "owner_ref",
                "assignee_ref",
                "requester_principal",
                "actor_principal",
                "actor_ref",
                "created_by",
                "subscriber_ref",
                "reviewer_principal",
                "operator_id",
            ] {
                if let Some(reference) = object.get(field).and_then(serde_json::Value::as_str) {
                    protected_refs.push(reference);
                }
            }
            shared_refs.extend(
                object
                    .get("share_with")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str),
            );
            for field in ["watcher_refs", "viewer_refs", "editor_refs"] {
                shared_refs.extend(
                    object
                        .get(field)
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(serde_json::Value::as_str),
                );
            }
            if let Some(reference) = object.get("scope_ref").and_then(serde_json::Value::as_str) {
                shared_refs.push(reference);
            }
            for nested in object.values() {
                collect_visibility_refs(nested, protected_refs, shared_refs);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                collect_visibility_refs(nested, protected_refs, shared_refs);
            }
        }
        _ => {}
    }
}

fn has_public_visibility(value: &serde_json::Value) -> bool {
    value
        .get("visibility")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|visibility| visibility == "public")
        || value
            .get("sharing_policy")
            .and_then(|policy| policy.get("visibility"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|visibility| visibility == "public")
}

enum CursorKeyReadError {
    Missing,
    Invalid(String),
    Io(String),
}

fn read_cursor_key(path: &Path) -> Result<[u8; CURSOR_KEY_BYTES], CursorKeyReadError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(CursorKeyReadError::Missing)
        }
        Err(error) => return Err(CursorKeyReadError::Io(error.to_string())),
    };
    if metadata.mode() & 0o777 != 0o600 {
        return Err(CursorKeyReadError::Invalid(format!(
            "{} must have mode 0600",
            path.display()
        )));
    }
    let bytes = fs::read(path).map_err(|error| CursorKeyReadError::Io(error.to_string()))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        CursorKeyReadError::Invalid(format!(
            "{} must contain exactly {CURSOR_KEY_BYTES} bytes, found {}",
            path.display(),
            bytes.len()
        ))
    })
}

fn create_cursor_key_atomically(
    path: &Path,
) -> Result<([u8; CURSOR_KEY_BYTES], bool), MfgLiveServiceError> {
    let mut key = [0_u8; CURSOR_KEY_BYTES];
    OpenOptions::new()
        .read(true)
        .open("/dev/urandom")
        .and_then(|mut random| random.read_exact(&mut key))
        .map_err(|error| MfgLiveServiceError::CursorKeyIo(error.to_string()))?;
    let temporary = temporary_key_path(path);
    let result = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&key)?;
        file.sync_all()?;
        fs::hard_link(&temporary, path)?;
        fs::remove_file(&temporary)?;
        Ok(())
    })();
    match result {
        Ok(()) => Ok((key, true)),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists || path.exists() => {
            let _ = fs::remove_file(&temporary);
            read_cursor_key(path)
                .map(|key| (key, false))
                .map_err(cursor_key_read_to_service_error)
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(MfgLiveServiceError::CursorKeyIo(error.to_string()))
        }
    }
}

fn cursor_key_read_to_service_error(error: CursorKeyReadError) -> MfgLiveServiceError {
    match error {
        CursorKeyReadError::Invalid(message) => MfgLiveServiceError::InvalidCursorKey(message),
        CursorKeyReadError::Io(message) => MfgLiveServiceError::CursorKeyIo(message),
        CursorKeyReadError::Missing => {
            MfgLiveServiceError::CursorKeyIo("cursor key disappeared".to_string())
        }
    }
}

fn temporary_key_path(path: &Path) -> PathBuf {
    path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ))
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut normalized = [0_u8; 64];
    if key.len() > 64 {
        normalized[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        normalized[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = [0x36_u8; 64];
    let mut outer_pad = [0x5c_u8; 64];
    for index in 0..64 {
        inner_pad[index] ^= normalized[index];
        outer_pad[index] ^= normalized[index];
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner);
    outer.finalize().into()
}

fn hmac_sha256_with_domain(key: &[u8], domain: &[u8], message: &[u8]) -> [u8; 32] {
    let mut material = Vec::with_capacity(domain.len() + 1 + message.len());
    material.extend_from_slice(domain);
    material.push(0);
    material.extend_from_slice(message);
    hmac_sha256(key, &material)
}

fn xor_cursor_payload(key: &[u8], nonce: &[u8], input: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    for (block_index, block) in input.chunks(32).enumerate() {
        let mut material = Vec::with_capacity(nonce.len() + std::mem::size_of::<u64>());
        material.extend_from_slice(nonce);
        material.extend_from_slice(&(block_index as u64).to_be_bytes());
        let stream = hmac_sha256_with_domain(key, b"cursor-stream", &material);
        output.extend(block.iter().zip(stream).map(|(plain, mask)| *plain ^ mask));
    }
    output
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, Barrier};

    use super::*;

    fn principal(id: &str, revision: u64, scopes: &[&str]) -> MfgLivePrincipalContext {
        MfgLivePrincipalContext {
            principal_id: id.to_string(),
            profile_revision: revision,
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            capabilities: vec!["mfg.read".to_string()],
            credential_epoch: 1,
            expires_at_ms: None,
        }
    }

    #[test]
    fn public_epoch_and_cursor_are_stable_per_view_and_diverge_across_scope() {
        let config_home = tempfile::tempdir().unwrap();
        let service = MfgService::new();
        let first = service
            .live_snapshot_envelope(config_home.path(), &principal("operator", 3, &["plant:a"]))
            .unwrap();
        let second = service
            .live_snapshot_envelope(config_home.path(), &principal("operator", 3, &["plant:a"]))
            .unwrap();
        let cropped = service
            .live_snapshot_envelope(config_home.path(), &principal("operator", 4, &["plant:b"]))
            .unwrap();
        let mut elevated_principal = principal("operator", 3, &["plant:a"]);
        elevated_principal
            .capabilities
            .push("mfg.data.manage".to_string());
        let elevated = service
            .live_snapshot_envelope(config_home.path(), &elevated_principal)
            .unwrap();
        let MfgLiveEnvelopeV1::Snapshot(first) = first else {
            panic!("snapshot")
        };
        let MfgLiveEnvelopeV1::Snapshot(second) = second else {
            panic!("snapshot")
        };
        let MfgLiveEnvelopeV1::Snapshot(cropped) = cropped else {
            panic!("snapshot")
        };
        let MfgLiveEnvelopeV1::Snapshot(elevated) = elevated else {
            panic!("snapshot")
        };
        assert_eq!(first.view_epoch, second.view_epoch);
        assert_eq!(first.cursor, second.cursor);
        assert_ne!(first.view_epoch, cropped.view_epoch);
        assert_ne!(first.cursor, cropped.cursor);
        assert_ne!(first.view_epoch, elevated.view_epoch);
        assert_ne!(first.cursor, elevated.cursor);
        assert!(!first.cursor.contains("operator"));
        assert!(!first.cursor.contains("plant:a"));
        let (sealed, _) = first.cursor.split_once('.').unwrap();
        let sealed = URL_SAFE_NO_PAD.decode(sealed).unwrap();
        let sealed = String::from_utf8_lossy(&sealed);
        assert!(!sealed.contains("operator"));
        assert!(!sealed.contains("internal_cursor"));
        assert!(!sealed.contains("plant:a"));
    }

    #[test]
    fn missing_key_rotates_epoch_but_corrupt_or_insecure_key_fails_closed() {
        let config_home = tempfile::tempdir().unwrap();
        let service = MfgService::new();
        let principal = principal("operator", 1, &["gateway"]);
        service
            .live_snapshot_envelope(config_home.path(), &principal)
            .unwrap();
        let first_epoch = service.live_epoch(config_home.path()).unwrap();
        let key_path = config_home.path().join(CURSOR_KEY_FILE);
        std::fs::remove_file(&key_path).unwrap();
        service
            .live_snapshot_envelope(config_home.path(), &principal)
            .unwrap();
        let rotated = service.live_epoch(config_home.path()).unwrap();
        assert_ne!(rotated.epoch_id, first_epoch.epoch_id);
        assert_eq!(rotated.rotation_reason, "cursor_key_recreated");

        std::fs::write(&key_path, b"short").unwrap();
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(matches!(
            service.live_snapshot_envelope(config_home.path(), &principal),
            Err(MfgLiveServiceError::InvalidCursorKey(_))
        ));
        std::fs::write(&key_path, [7_u8; CURSOR_KEY_BYTES]).unwrap();
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            service.live_snapshot_envelope(config_home.path(), &principal),
            Err(MfgLiveServiceError::InvalidCursorKey(_))
        ));
    }

    #[test]
    fn hidden_events_are_cropped_and_low_priority_updates_are_coalesced() {
        let principal = principal("operator", 1, &["plant:a"]);
        let events = vec![
            MfgLiveProjectionEvent {
                cursor: 1,
                event_type: "assignment.receipted".to_string(),
                subject_ref: "mfg:assignment:hidden".to_string(),
                payload: serde_json::json!({
                    "assignment": {
                        "assignment_id": "hidden",
                        "owner_ref": "principal:other",
                        "revision": 1
                    }
                }),
                created_at: Utc::now(),
            },
            MfgLiveProjectionEvent {
                cursor: 2,
                event_type: "metric_state.updated".to_string(),
                subject_ref: "matrix:metric:visible".to_string(),
                payload: serde_json::json!({"revision": 1}),
                created_at: Utc::now(),
            },
            MfgLiveProjectionEvent {
                cursor: 3,
                event_type: "metric_state.updated".to_string(),
                subject_ref: "matrix:metric:visible".to_string(),
                payload: serde_json::json!({"revision": 2}),
                created_at: Utc::now(),
            },
        ];
        let projected = visible_coalesced_events(
            MfgLiveDeltaRead {
                epoch: MfgLiveEpoch {
                    epoch_id: "epoch".to_string(),
                    contract_version: app_mfg_contract::MFG_CONTRACT_VERSION.to_string(),
                    schema_version: 1,
                    created_at: Utc::now(),
                    rotation_reason: "test".to_string(),
                    retention_low_cursor: 0,
                    retention_high_cursor: 3,
                    updated_at: Utc::now(),
                },
                base_cursor: 0,
                high_cursor: 3,
                events,
                resync_reason: None,
            },
            &principal,
        );
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].subject_ref, "matrix:metric:visible");
        assert_eq!(projected[0].revision, 2);
    }

    #[test]
    fn snapshot_crop_enforces_owner_subscriber_actor_sharing_and_linked_review_visibility() {
        let state = MfgLiveSnapshotStateV1 {
            cockpit: serde_json::json!({"profiles": [
                {"profile_id": "mine", "owner_ref": "principal:operator", "widget_instances": []},
                {"profile_id": "shared", "owner_ref": "principal:other", "viewer_refs": ["principal:operator"], "widget_instances": []},
                {
                    "profile_id": "team-scope",
                    "owner_ref": "principal:other",
                    "scope": {"kind": "team", "scope_ref": "plant-a"},
                    "sharing_policy": {"visibility": "team"},
                    "widget_instances": []
                },
                {"profile_id": "hidden", "owner_ref": "principal:other", "widget_instances": []}
            ]}),
            alerts: serde_json::json!({
                "rules": [
                    {"rule_id": "mine", "owner_ref": "principal:operator"},
                    {"rule_id": "hidden", "owner_ref": "principal:other"}
                ],
                "subscriptions": [
                    {"subscription_id": "mine", "rule_id": "mine", "subscriber_ref": "principal:operator"},
                    {"subscription_id": "other", "rule_id": "mine", "subscriber_ref": "principal:other"},
                    {"subscription_id": "hidden-rule", "rule_id": "hidden", "subscriber_ref": "principal:operator"}
                ],
                "occurrences": [
                    {"occurrence_id": "mine", "rule_id": "mine"},
                    {"occurrence_id": "hidden", "rule_id": "hidden"}
                ]
            }),
            assignments: serde_json::json!({"items": [
                {"assignment_id": "mine", "assignee_ref": "principal:operator"},
                {
                    "assignment_id": "scope",
                    "assignee_ref": "plant-a",
                    "assignee_kind": "team",
                    "visibility": "team"
                },
                {"assignment_id": "hidden", "assignee_ref": "principal:other"}
            ]}),
            incidents: serde_json::json!({"items": [{"incident_id": "global"}]}),
            executions: serde_json::json!({"actions": [{"execution_id": "global"}]}),
            reports: serde_json::json!({"items": [
                {"report_id": "mine", "profile_id": "mine", "owner_ref": "principal:operator"},
                {"report_id": "shared-report", "profile_id": "shared", "owner_ref": "principal:other"},
                {"report_id": "team-report", "profile_id": "team-scope", "owner_ref": "principal:other"},
                {"report_id": "hidden", "profile_id": "hidden", "owner_ref": "principal:other"}
            ]}),
            reviews: serde_json::json!({"items": [
                {"review_id": "mine", "report_id": "mine"},
                {"review_id": "shared-review", "report_id": "shared-report"},
                {"review_id": "team-review", "report_id": "team-report"},
                {"review_id": "hidden", "report_id": "hidden"}
            ]}),
            receipts: serde_json::json!({
                "commands": [
                    {"receipt_id": "mine", "actor_ref": "principal:operator"},
                    {"receipt_id": "hidden", "actor_ref": "principal:other"}
                ],
                "mutations": [
                    {"receipt_id": "mine", "actor_principal": "principal:operator"},
                    {"receipt_id": "hidden", "actor_principal": "principal:other"}
                ]
            }),
            data_compute: serde_json::json!({"entities": [{"entity_id": "global"}]}),
        };
        let cropped = crop_snapshot_state(state, &principal("operator", 1, &["team:plant-a"]));
        let cropped = serde_json::to_value(cropped).unwrap();
        for (domain, field, visible, hidden) in [
            (
                "cockpit",
                "profiles",
                &["mine", "shared", "team-scope"][..],
                &["hidden"][..],
            ),
            ("alerts", "rules", &["mine"][..], &["hidden"][..]),
            (
                "alerts",
                "subscriptions",
                &["mine"][..],
                &["other", "hidden-rule"][..],
            ),
            ("alerts", "occurrences", &["mine"][..], &["hidden"][..]),
            (
                "assignments",
                "items",
                &["mine", "scope"][..],
                &["hidden"][..],
            ),
            (
                "reports",
                "items",
                &["mine", "shared-report", "team-report"][..],
                &["hidden"][..],
            ),
            (
                "reviews",
                "items",
                &["mine", "shared-review", "team-review"][..],
                &["hidden"][..],
            ),
            ("receipts", "commands", &["mine"][..], &["hidden"][..]),
            ("receipts", "mutations", &["mine"][..], &["hidden"][..]),
        ] {
            let encoded = cropped[domain][field].to_string();
            for id in visible {
                assert!(encoded.contains(id), "{domain}.{field} omitted {id}");
            }
            for id in hidden {
                assert!(!encoded.contains(id), "{domain}.{field} leaked {id}");
            }
        }
        assert_eq!(cropped["incidents"]["items"][0]["incident_id"], "global");
        assert_eq!(
            cropped["data_compute"]["entities"][0]["entity_id"],
            "global"
        );
    }

    #[test]
    fn hidden_only_changes_advance_only_the_payload_free_heartbeat_cursor() {
        let config_home = tempfile::tempdir().unwrap();
        let service = MfgService::new();
        let observer = principal("operator", 1, &["gateway"]);
        let MfgLiveEnvelopeV1::Snapshot(snapshot) = service
            .live_snapshot_envelope(config_home.path(), &observer)
            .unwrap()
        else {
            panic!("snapshot")
        };
        service
            .claim_mutation_receipt(
                config_home.path(),
                "hidden-live-key",
                "principal:other",
                "mfg.incident.create",
                "mfg:incident:hidden",
                None,
                "sha256:hidden",
                "correlation:hidden",
            )
            .unwrap();
        service
            .record_mutation_receipt(
                config_home.path(),
                "hidden-live-key",
                "principal:other",
                "mfg.incident.create",
                "mfg:incident:hidden",
                None,
                Some(1),
                "sha256:hidden",
                &serde_json::json!({"revision": 1}),
            )
            .unwrap();
        assert!(service
            .live_delta_envelope(
                config_home.path(),
                &observer,
                &snapshot.view_epoch,
                &snapshot.cursor,
                100,
            )
            .unwrap()
            .is_none());
        let MfgLiveEnvelopeV1::Heartbeat(heartbeat) = service
            .live_heartbeat_envelope(config_home.path(), &observer, &snapshot.cursor)
            .unwrap()
        else {
            panic!("heartbeat")
        };
        assert_ne!(heartbeat.cursor, snapshot.cursor);
        let encoded = serde_json::to_value(heartbeat).unwrap();
        assert_eq!(encoded.as_object().unwrap().len(), 3);
        assert!(encoded.get("event_count").is_none());
        assert!(encoded.get("internal_cursor").is_none());
        assert!(encoded.get("subjects").is_none());
    }

    #[test]
    fn hidden_backlog_is_scanned_past_one_page_without_delaying_the_next_visible_event() {
        let config_home = tempfile::tempdir().unwrap();
        let service = MfgService::new();
        let observer = principal("operator", 1, &["gateway"]);
        let MfgLiveEnvelopeV1::Snapshot(snapshot) = service
            .live_snapshot_envelope(config_home.path(), &observer)
            .unwrap()
        else {
            panic!("snapshot")
        };
        for index in 0..501 {
            let key = format!("hidden-page-{index}");
            service
                .claim_mutation_receipt(
                    config_home.path(),
                    &key,
                    "principal:other",
                    "mfg.incident.create",
                    &format!("mfg:incident:hidden-{index}"),
                    None,
                    &format!("sha256:hidden-{index}"),
                    &format!("correlation:hidden-{index}"),
                )
                .unwrap();
            service
                .record_mutation_receipt(
                    config_home.path(),
                    &key,
                    "principal:other",
                    "mfg.incident.create",
                    &format!("mfg:incident:hidden-{index}"),
                    None,
                    Some(1),
                    &format!("sha256:hidden-{index}"),
                    &serde_json::json!({"revision": 1}),
                )
                .unwrap();
        }
        let MfgLiveEnvelopeV1::Heartbeat(hidden_heartbeat) = service
            .live_heartbeat_envelope(config_home.path(), &observer, &snapshot.cursor)
            .unwrap()
        else {
            panic!("heartbeat")
        };
        let key = service
            .load_or_create_live_cursor_key(config_home.path())
            .unwrap();
        let heartbeat_position = decode_cursor(&key, &hidden_heartbeat.cursor).unwrap();
        assert_eq!(
            heartbeat_position.internal_cursor,
            service
                .live_epoch(config_home.path())
                .unwrap()
                .retention_high_cursor
        );
        service
            .claim_mutation_receipt(
                config_home.path(),
                "visible-after-hidden",
                "principal:operator",
                "mfg.incident.create",
                "mfg:incident:visible",
                None,
                "sha256:visible",
                "correlation:visible",
            )
            .unwrap();
        service
            .record_mutation_receipt(
                config_home.path(),
                "visible-after-hidden",
                "principal:operator",
                "mfg.incident.create",
                "mfg:incident:visible",
                None,
                Some(1),
                "sha256:visible",
                &serde_json::json!({"revision": 1}),
            )
            .unwrap();

        let MfgLiveEnvelopeV1::Delta(delta) = service
            .live_delta_envelope(
                config_home.path(),
                &observer,
                &snapshot.view_epoch,
                &snapshot.cursor,
                100,
            )
            .unwrap()
            .expect("visible event after hidden backlog")
        else {
            panic!("delta")
        };
        assert_eq!(delta.events.len(), 1);
        assert_eq!(
            delta.events[0].payload["receipt"]["actor_principal"],
            "principal:operator"
        );
    }

    #[test]
    fn shared_and_public_records_are_visible_without_exposing_private_neighbors() {
        let principal = principal("operator", 1, &["gateway"]);
        assert!(record_visible(
            &serde_json::json!({
                "owner_ref": "principal:other",
                "watcher_refs": ["principal:operator"],
            }),
            &principal,
        ));
        assert!(record_visible(
            &serde_json::json!({
                "owner_ref": "principal:other",
                "sharing_policy": {
                    "visibility": "public",
                    "viewer_refs": [],
                },
            }),
            &principal,
        ));
        assert!(!record_visible(
            &serde_json::json!({
                "owner_ref": "principal:other",
                "watcher_refs": ["principal:third"],
            }),
            &principal,
        ));
        assert!(!record_visible(
            &serde_json::json!({"summary": "record without an access policy"}),
            &principal,
        ));
        assert!(record_visible(
            &serde_json::json!({"subscriber_ref": "principal:operator"}),
            &principal,
        ));
    }

    #[test]
    fn report_and_assignment_events_use_the_same_linked_scope_rules_as_rest_reads() {
        let observer = principal("operator", 1, &["team:plant-a"]);
        let shared_report = MfgLiveProjectionEvent {
            cursor: 1,
            event_type: "report.updated".to_string(),
            subject_ref: "mfg:cockpit-report:shared".to_string(),
            payload: serde_json::json!({
                "report": {
                    "report_id": "shared",
                    "profile_id": "shared-profile",
                    "owner_ref": "principal:other",
                },
                "profile": {
                    "profile_id": "shared-profile",
                    "owner_ref": "principal:other",
                    "sharing_policy": {
                        "visibility": "private",
                        "viewer_refs": ["principal:operator"],
                    },
                    "widget_instances": [],
                },
            }),
            created_at: Utc::now(),
        };
        let team_assignment = MfgLiveProjectionEvent {
            cursor: 2,
            event_type: "assignment.receipted".to_string(),
            subject_ref: "mfg:assignment:team".to_string(),
            payload: serde_json::json!({
                "assignment": {
                    "assignment_id": "team",
                    "assignee_ref": "plant-a",
                    "assignee_kind": "team",
                    "visibility": "team",
                },
            }),
            created_at: Utc::now(),
        };
        assert!(live_event_visible(&shared_report, &observer));
        assert!(live_event_visible(&team_assignment, &observer));

        let unrelated = principal("operator", 1, &["team:plant-b"]);
        assert!(!live_event_visible(&team_assignment, &unrelated));
    }

    #[test]
    fn nested_business_revision_is_projected_into_the_contract_event() {
        let event = contract_event(MfgLiveProjectionEvent {
            cursor: 9,
            event_type: "assignment.receipted".to_string(),
            subject_ref: "mfg:assignment:revision-7".to_string(),
            payload: serde_json::json!({
                "assignment": {"assignment_id": "revision-7", "revision": 7},
                "receipt": {"current_revision": 7, "result_revision": 6}
            }),
            created_at: Utc::now(),
        });
        assert_eq!(event.revision, 7);
    }

    #[test]
    fn atomic_cursor_key_creation_reports_exactly_one_creator() {
        let config_home = tempfile::tempdir().unwrap();
        let path = config_home.path().join(CURSOR_KEY_FILE);
        let (first, created) = create_cursor_key_atomically(&path).unwrap();
        let (second, reused) = create_cursor_key_atomically(&path).unwrap();
        assert!(created);
        assert!(!reused);
        assert_eq!(first, second);
    }

    #[test]
    fn concurrent_observers_share_the_single_initialized_epoch() {
        let config_home = tempfile::tempdir().unwrap();
        let service = Arc::new(MfgService::new());
        let barrier = Arc::new(Barrier::new(8));
        let mut observers = Vec::new();
        for _ in 0..8 {
            let service = Arc::clone(&service);
            let barrier = Arc::clone(&barrier);
            let config_home = config_home.path().to_path_buf();
            observers.push(std::thread::spawn(move || {
                barrier.wait();
                service
                    .live_snapshot_envelope(config_home, &principal("operator", 1, &["gateway"]))
                    .unwrap()
            }));
        }
        let epochs = observers
            .into_iter()
            .map(|observer| {
                let MfgLiveEnvelopeV1::Snapshot(snapshot) = observer.join().unwrap() else {
                    panic!("snapshot")
                };
                snapshot.view_epoch
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(epochs.len(), 1);
        assert_eq!(
            service
                .live_epoch(config_home.path())
                .unwrap()
                .rotation_reason,
            "cursor_key_recreated"
        );
    }

    #[test]
    fn hmac_sha256_matches_the_standard_vector() {
        let actual = hmac_sha256(b"key", b"The quick brown fox jumps over the lazy dog")
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            actual,
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }
}
