use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use harness_contract::live::{
    SessionMessageAck, SessionMessageAckRequest, SessionMessageReceiptContext,
    SessionPublicPayloadKind, SessionReceiptOutboxObservation,
};
use reqwest::blocking::{Client, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const RECEIPT_PROTOCOL: &str = "session-public-v1";
const CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct ConfirmedTerminalDelivery {
    pub blocks: Value,
    pub observation: Value,
}

pub(crate) struct SessionActor<'a> {
    client: &'a Client,
    base_url: String,
    surface_id: String,
    observer_id: String,
    session_id: String,
    attached: bool,
    lease_cleanup_pending: bool,
    receipt_selector: Option<String>,
    receipt_expires_at_ms: Option<u64>,
    receipt_refresh_at_ms: Option<u64>,
    receipt_authority_error: Option<String>,
    observation_deadline: Instant,
    cleanup_deadline: Option<Instant>,
    trace: Vec<Value>,
}

impl<'a> SessionActor<'a> {
    pub(crate) fn create(
        client: &'a Client,
        base_url: &str,
        model: Option<&str>,
        surface_id: &str,
        deadline: Instant,
    ) -> Result<Self, String> {
        let base_url = base_url.trim_end_matches('/').to_string();
        let body = model
            .filter(|value| !value.trim().is_empty())
            .map_or_else(|| json!({}), |model| json!({"model": model}));
        let session = send_json(
            client
                .post(format!("{base_url}/api/sessions"))
                .header("x-cowd-surface-id", surface_id)
                .header("x-cowd-observer-id", surface_id)
                .header("x-cowd-requested-capabilities", "mission.observe")
                .json(&body),
            deadline,
        )
        .map_err(|error| format!("create_session:{error}"))?;
        let session_id = session
            .get("id")
            .or_else(|| session.get("session_id"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or("create_session:missing_session_id")?;
        let mut actor = Self::new(client, &base_url, session_id, surface_id, deadline);
        actor.trace.push(trace_entry("create_session", true));
        actor.initialize()?;
        Ok(actor)
    }

    pub(crate) fn resume(
        client: &'a Client,
        base_url: &str,
        session_id: &str,
        surface_id: &str,
        deadline: Instant,
    ) -> Result<Self, String> {
        let mut actor = Self::new(client, base_url, session_id, surface_id, deadline);
        actor.trace.push(trace_entry("resume_session", true));
        actor.initialize()?;
        Ok(actor)
    }

    fn new(
        client: &'a Client,
        base_url: &str,
        session_id: &str,
        surface_id: &str,
        deadline: Instant,
    ) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            surface_id: surface_id.to_string(),
            observer_id: surface_id.to_string(),
            session_id: session_id.to_string(),
            attached: false,
            lease_cleanup_pending: false,
            receipt_selector: None,
            receipt_expires_at_ms: None,
            receipt_refresh_at_ms: None,
            receipt_authority_error: None,
            observation_deadline: deadline,
            cleanup_deadline: None,
            trace: Vec::new(),
        }
    }

    fn initialize(&mut self) -> Result<(), String> {
        if let Err(error) = self.attach_and_acquire() {
            return match self.finish() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(format!("{error};{cleanup}")),
            };
        }
        Ok(())
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn set_observation_deadline(&mut self, deadline: Instant) {
        self.observation_deadline = deadline;
    }

    pub(crate) fn post_mutation(&mut self, path: &str, body: Value) -> Result<Value, String> {
        let response = send_json(
            self.writer_request(self.client.post(self.url(path)))
                .json(&body),
            self.observation_deadline,
        );
        self.record("post_mutation", &response);
        response
    }

    pub(crate) fn post_cleanup_mutation(
        &mut self,
        path: &str,
        body: Value,
    ) -> Result<Value, String> {
        let deadline = *self
            .cleanup_deadline
            .get_or_insert_with(|| Instant::now() + CLEANUP_TIMEOUT);
        let response = send_json(
            self.writer_request(self.client.post(self.url(path)))
                .json(&body),
            deadline,
        );
        self.record("post_cleanup_mutation", &response);
        response
    }

    pub(crate) fn terminal_receipt_context(
        &mut self,
        message_id: &str,
    ) -> Result<SessionMessageReceiptContext, String> {
        let result = (|| {
            let url = self.message_url(message_id, "receipt-context")?;
            let context: SessionMessageReceiptContext = send_typed(
                self.receipt_request(self.client.get(url))?,
                self.observation_deadline,
            )?;
            if matches!(&context, SessionMessageReceiptContext::Available { .. }) {
                context_request(&context, &self.session_id, message_id, self.selector()?)?;
            }
            Ok(context)
        })();
        self.record("terminal_receipt_context", &result);
        result
    }

    pub(crate) fn ack_terminal_message(
        &mut self,
        message_id: &str,
        context: &SessionMessageReceiptContext,
    ) -> Result<SessionMessageAck, String> {
        let result = (|| {
            let request = context_request(context, &self.session_id, message_id, self.selector()?)?;
            request.validate().map_err(|_| "receipt_request_invalid")?;
            let url = self.message_url(message_id, "ack")?;
            let ack: SessionMessageAck = send_typed(
                self.receipt_request(self.client.post(url))?.json(&request),
                self.observation_deadline,
            )?;
            validate_ack(
                &ack,
                &self.session_id,
                message_id,
                self.selector()?,
                &request,
            )?;
            Ok(ack)
        })();
        self.record("ack_terminal_message", &result);
        result
    }

    pub(crate) fn confirm_terminal_delivery(
        &mut self,
        message_id: &str,
        expected_turn_id: &str,
        expected_execution_id: &str,
    ) -> Result<ConfirmedTerminalDelivery, String> {
        let result = (|| {
            self.maintain_receipt_authority()?;
            let before = self.terminal_receipt_context(message_id)?;
            let blocks = delivery_blocks(&before, expected_turn_id, expected_execution_id)?;
            let ack = self.ack_terminal_message(message_id, &before)?;
            let current = self.terminal_receipt_context(message_id)?;
            let observation = confirmed_observation(
                &before,
                &ack,
                &current,
                expected_turn_id,
                expected_execution_id,
            )?;
            remaining_until(self.observation_deadline)?;
            Ok(ConfirmedTerminalDelivery {
                blocks,
                observation,
            })
        })();
        self.record("confirm_terminal_delivery", &result);
        result
    }

    pub(crate) fn select_terminal_message(
        &mut self,
        message_ids: &[String],
        expected_turn_id: &str,
        expected_execution_id: &str,
    ) -> Result<Option<String>, String> {
        self.maintain_receipt_authority()?;
        let session_id = self.session_id.clone();
        let selected = select_terminal_source(
            message_ids,
            &session_id,
            expected_turn_id,
            expected_execution_id,
            |message_id| self.terminal_receipt_context(message_id),
        )?;
        remaining_until(self.observation_deadline)?;
        Ok(selected)
    }

    pub(crate) fn maintain_receipt_authority(&mut self) -> Result<(), String> {
        let mut attempted = false;
        let result = (|| {
            remaining_until(self.observation_deadline)?;
            let selector = self.selector()?.to_string();
            let now = now_ms()?;
            let refresh_at = self
                .receipt_refresh_at_ms
                .ok_or("receipt_refresh_schedule_unavailable")?;
            if now < refresh_at {
                return Ok(());
            }
            let deadline = self
                .receipt_expires_at_ms
                .ok_or("receipt_attachment_expired_or_unavailable")?;
            if now >= deadline {
                return Err("receipt_attachment_expired_or_unavailable".into());
            }
            let url = self.session_url("attach")?;
            attempted = true;
            let grant = send_json(
                self.writer_request(self.client.post(url))
                    .header("x-cowd-client-id", &selector)
                    .json(&session_attach_body(&self.surface_id)),
                self.observation_deadline,
            )?;
            self.adopt_reaffirmed_grant(&grant, now_ms()?)
        })();
        if attempted || result.is_err() {
            self.record("reaffirm_session", &result);
        }
        if let Err(error) = &result {
            self.receipt_authority_error = Some(error.clone());
        }
        result
    }

    fn adopt_reaffirmed_grant(&mut self, grant: &Value, now: u64) -> Result<(), String> {
        let previous = self
            .receipt_expires_at_ms
            .filter(|deadline| now < *deadline)
            .ok_or("receipt_attachment_expired_or_unavailable")?;
        let selector = self
            .receipt_selector
            .as_deref()
            .ok_or("receipt_selector_unavailable")?;
        let deadline = validate_grant(grant, &self.session_id, now)?;
        if grant_selector(grant).as_deref() != Some(selector) {
            return Err("receipt_reaffirm_client_mismatch".into());
        }
        let refresh_at = receipt_refresh_at(now, deadline, Some(previous))?;
        self.receipt_expires_at_ms = Some(deadline);
        self.receipt_refresh_at_ms = Some(refresh_at);
        Ok(())
    }

    pub(crate) fn drain_trace(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.trace)
    }

    pub(crate) fn finish(&mut self) -> Result<(), String> {
        if !self.lease_cleanup_pending && !self.attached {
            return Ok(());
        }
        let deadline = *self
            .cleanup_deadline
            .get_or_insert_with(|| Instant::now() + CLEANUP_TIMEOUT);
        let release = if self.lease_cleanup_pending {
            let result = send_json(
                self.writer_request(
                    self.client
                        .post(self.url("/api/runtime/session-leases/release")),
                )
                .json(&session_lease_release_body(&self.session_id)),
                deadline,
            );
            self.record("release_session_lease", &result);
            if result.is_ok() {
                self.lease_cleanup_pending = false;
            }
            result.map(|_| ())
        } else {
            Ok(())
        };
        let detach = if self.attached {
            let result = (|| {
                let selector = self
                    .receipt_selector
                    .as_deref()
                    .ok_or("detach_selector_unavailable")?;
                let url = self.session_url("detach")?;
                send_json(
                    self.writer_request(self.client.post(url))
                        .header("x-cowd-client-id", selector)
                        .json(&session_detach_body(&self.surface_id)),
                    deadline,
                )
                .map(|_| ())
            })();
            self.record("detach_session", &result);
            if result.is_ok() {
                self.attached = false;
                self.receipt_selector = None;
                self.receipt_expires_at_ms = None;
                self.receipt_refresh_at_ms = None;
            }
            result
        } else {
            Ok(())
        };
        match (release, detach) {
            (Ok(()), Ok(())) => Ok(()),
            (release, detach) => Err(format!(
                "session_actor_cleanup_failed:release={};detach={}",
                result_summary(&release),
                result_summary(&detach)
            )),
        }
    }

    fn attach_and_acquire(&mut self) -> Result<(), String> {
        let attach = (|| {
            let url = self.session_url("attach")?;
            let response = send_response(
                self.writer_request(self.client.post(url))
                    .json(&session_attach_body(&self.surface_id)),
                self.observation_deadline,
            )?;
            self.attached = true;
            let grant: Value = decode_response(response)?;
            self.receipt_selector = grant_selector(&grant);
            let now = now_ms()?;
            let deadline = validate_grant(&grant, &self.session_id, now)?;
            self.receipt_expires_at_ms = Some(deadline);
            self.receipt_refresh_at_ms = Some(receipt_refresh_at(now, deadline, None)?);
            Ok(())
        })();
        self.record("attach_session", &attach);
        attach.map_err(|error: String| format!("attach_writer:{error}"))?;
        self.lease_cleanup_pending = true;
        let acquire = send_json(
            self.writer_request(
                self.client
                    .post(self.url("/api/runtime/session-leases/acquire")),
            )
            .json(&json!({"session_id": self.session_id, "mode": "collaborative"})),
            self.observation_deadline,
        );
        self.record("acquire_session_lease", &acquire);
        acquire.map_err(|error| format!("acquire_writer_lease:{error}"))?;
        Ok(())
    }

    fn selector(&self) -> Result<&str, String> {
        if let Some(error) = &self.receipt_authority_error {
            return Err(error.clone());
        }
        if !self.attached {
            return Err("receipt_attachment_inactive".into());
        }
        let now = now_ms()?;
        if self
            .receipt_expires_at_ms
            .is_none_or(|deadline| now >= deadline)
        {
            return Err("receipt_attachment_expired_or_unavailable".into());
        }
        self.receipt_selector
            .as_deref()
            .ok_or_else(|| "receipt_selector_unavailable".into())
    }

    fn receipt_request(&self, request: RequestBuilder) -> Result<RequestBuilder, String> {
        Ok(self
            .writer_request(request)
            .header("x-cowd-client-id", self.selector()?))
    }

    fn writer_request(&self, request: RequestBuilder) -> RequestBuilder {
        request
            .header("x-cowd-surface-id", &self.surface_id)
            .header("x-cowd-observer-id", &self.observer_id)
            .header("x-cowd-requested-capabilities", "mission.observe")
    }

    fn session_url(&self, operation: &str) -> Result<reqwest::Url, String> {
        if self.session_id.is_empty() || matches!(self.session_id.as_str(), "." | "..") {
            return Err("session_id_invalid".into());
        }
        let mut url =
            reqwest::Url::parse(&self.url("/api/sessions/")).map_err(|_| "session_url_invalid")?;
        url.path_segments_mut()
            .map_err(|_| "session_url_invalid")?
            .pop_if_empty()
            .push(&self.session_id)
            .push(operation);
        Ok(url)
    }

    fn message_url(&self, message_id: &str, operation: &str) -> Result<reqwest::Url, String> {
        if message_id.is_empty() || matches!(message_id, "." | "..") {
            return Err("receipt_message_id_invalid".into());
        }
        let mut url = self.session_url("messages")?;
        url.path_segments_mut()
            .map_err(|_| "session_url_invalid")?
            .push(message_id)
            .push(operation);
        Ok(url)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn record<Output>(&mut self, operation: &'static str, result: &Result<Output, String>) {
        self.trace.push(trace_entry(operation, result.is_ok()));
    }
}

impl Drop for SessionActor<'_> {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            eprintln!("{error}");
        }
    }
}

fn context_request(
    context: &SessionMessageReceiptContext,
    expected_session: &str,
    expected_message: &str,
    expected_client: &str,
) -> Result<SessionMessageAckRequest, String> {
    let SessionMessageReceiptContext::Available {
        payload_kind: SessionPublicPayloadKind::SessionPublicV1,
        payload_json,
        session_id,
        message_id,
        client_id,
        runtime_commit_cursor,
        content_digest,
        receipt,
        ..
    } = context
    else {
        return Err("receipt_context_unavailable".into());
    };
    if session_id != expected_session
        || message_id != expected_message
        || client_id != expected_client
    {
        return Err("receipt_context_identity_mismatch".into());
    }
    let request = SessionMessageAckRequest {
        runtime_commit_cursor: runtime_commit_cursor.clone(),
        content_digest: content_digest.clone(),
    };
    request.validate().map_err(|_| "receipt_request_invalid")?;
    if content_digest != &public_payload_digest(payload_json) {
        return Err("receipt_payload_digest_mismatch".into());
    }
    if let Some(receipt) = receipt {
        validate_ack(receipt, session_id, message_id, client_id, &request)?;
    }
    Ok(request)
}

fn validate_ack(
    ack: &SessionMessageAck,
    session_id: &str,
    message_id: &str,
    client_id: &str,
    request: &SessionMessageAckRequest,
) -> Result<(), String> {
    if ack.session_id != session_id
        || ack.message_id != message_id
        || ack.client_id != client_id
        || ack.runtime_commit_cursor != request.runtime_commit_cursor
        || ack.content_digest != request.content_digest
    {
        return Err("receipt_ack_tuple_mismatch".into());
    }
    Ok(())
}

fn terminal_source_matches(
    context: &SessionMessageReceiptContext,
    session_id: &str,
    turn_id: &str,
    execution_id: &str,
) -> bool {
    let SessionMessageReceiptContext::Available {
        session_id: observed_session,
        session_source,
        runtime_commit_cursor,
        outbox_observation,
        ..
    } = context
    else {
        return false;
    };
    if session_id.is_empty()
        || turn_id.is_empty()
        || execution_id.is_empty()
        || observed_session != session_id
        || session_source.turn_id != turn_id
        || session_source.request_id.is_empty()
    {
        return false;
    }
    match outbox_observation {
        SessionReceiptOutboxObservation::RuntimeOutboxUnknown => true,
        SessionReceiptOutboxObservation::RuntimeOutbox {
            terminal_id,
            runtime_commit_cursor: outbox_cursor,
            execution_id: observed_execution,
            turn_id: observed_turn,
            request_id,
            ..
        } => {
            observed_execution
                .as_deref()
                .is_none_or(|observed| observed == execution_id)
                && observed_turn == turn_id
                && request_id == &session_source.request_id
                && terminal_id == &format!("turn-terminal:{}", session_source.request_id)
                && outbox_cursor == runtime_commit_cursor
        }
    }
}

fn select_terminal_source(
    message_ids: &[String],
    session_id: &str,
    turn_id: &str,
    execution_id: &str,
    mut lookup: impl FnMut(&str) -> Result<SessionMessageReceiptContext, String>,
) -> Result<Option<String>, String> {
    let mut selected = None;
    let mut seen = std::collections::BTreeSet::new();
    for message_id in message_ids {
        if !seen.insert(message_id) {
            continue;
        }
        let context = lookup(message_id)?;
        if matches!(&context, SessionMessageReceiptContext::Available { message_id: observed, .. } if observed == message_id)
            && terminal_source_matches(&context, session_id, turn_id, execution_id)
        {
            if selected.replace(message_id.clone()).is_some() {
                return Err("receipt_terminal_source_ambiguous".into());
            }
        }
    }
    Ok(selected)
}

fn delivery_blocks(
    context: &SessionMessageReceiptContext,
    expected_turn_id: &str,
    expected_execution_id: &str,
) -> Result<Value, String> {
    let SessionMessageReceiptContext::Available {
        payload_json,
        session_id,
        message_id,
        client_id,
        ..
    } = context
    else {
        return Err("receipt_context_unavailable".into());
    };
    context_request(context, session_id, message_id, client_id)?;
    if expected_turn_id.is_empty()
        || expected_execution_id.is_empty()
        || !terminal_source_matches(context, session_id, expected_turn_id, expected_execution_id)
    {
        return Err("receipt_source_mismatch".into());
    }
    let blocks: Value =
        serde_json::from_str(payload_json).map_err(|_| "receipt_payload_invalid")?;
    if !blocks.is_array() {
        return Err("receipt_payload_not_blocks".into());
    }
    Ok(blocks)
}

fn confirmed_observation(
    before: &SessionMessageReceiptContext,
    ack: &SessionMessageAck,
    current: &SessionMessageReceiptContext,
    expected_turn_id: &str,
    expected_execution_id: &str,
) -> Result<Value, String> {
    delivery_blocks(before, expected_turn_id, expected_execution_id)?;
    delivery_blocks(current, expected_turn_id, expected_execution_id)?;
    let (
        SessionMessageReceiptContext::Available {
            session_id,
            message_id,
            client_id,
            message_sequence,
            runtime_commit_cursor,
            content_digest,
            payload_json,
            session_source,
            ..
        },
        SessionMessageReceiptContext::Available {
            session_id: current_session,
            message_id: current_message,
            client_id: current_client,
            message_sequence: current_sequence,
            runtime_commit_cursor: current_cursor,
            content_digest: current_digest,
            payload_json: current_payload,
            session_source: current_source,
            receipt: Some(current_receipt),
            outbox_observation,
            ..
        },
    ) = (before, current)
    else {
        return Err("receipt_current_confirmation_missing".into());
    };
    let request = context_request(before, session_id, message_id, client_id)?;
    validate_ack(ack, session_id, message_id, client_id, &request)?;
    if current_session != session_id
        || current_message != message_id
        || current_client != client_id
        || current_sequence != message_sequence
        || current_cursor != runtime_commit_cursor
        || current_digest != content_digest
        || current_payload != payload_json
        || current_source.request_id != session_source.request_id
        || current_source.turn_id != session_source.turn_id
        || current_source.session_generation != session_source.session_generation
        || current_source.input_sequence != session_source.input_sequence
        || current_receipt != ack
    {
        return Err("receipt_current_confirmation_mismatch".into());
    }
    let execution_id = match outbox_observation {
        SessionReceiptOutboxObservation::RuntimeOutbox { execution_id, .. } => {
            execution_id.as_deref()
        }
        SessionReceiptOutboxObservation::RuntimeOutboxUnknown => None,
    };
    Ok(json!({
        "status": "confirmed", "session_id": session_id, "message_id": message_id,
        "turn_id": session_source.turn_id,
        "execution_id": execution_id,
        "runtime_commit_cursor": runtime_commit_cursor, "content_digest": content_digest,
        "client_binding_sha256": sha256_hex(client_id), "received_at_ms": ack.received_at_ms,
        "fresh_context_confirmed": true,
        "ack": receipt_observation(ack),
        "current_context": {
            "session_id":current_session,"message_id":current_message,
            "turn_id":current_source.turn_id,"execution_id":execution_id,
            "runtime_commit_cursor":current_cursor,"content_digest":current_digest,
            "client_binding_sha256":sha256_hex(current_client),
            "receipt":receipt_observation(current_receipt),
        },
    }))
}

fn receipt_observation(receipt: &SessionMessageAck) -> Value {
    json!({
        "session_id":receipt.session_id,"message_id":receipt.message_id,
        "runtime_commit_cursor":receipt.runtime_commit_cursor,
        "content_digest":receipt.content_digest,
        "client_binding_sha256":sha256_hex(&receipt.client_id),
        "received_at_ms":receipt.received_at_ms,
    })
}

fn grant_selector(grant: &Value) -> Option<String> {
    grant
        .get("receipt_attachment")?
        .get("client_id")?
        .as_str()
        .filter(|selector| uuid::Uuid::parse_str(selector).is_ok())
        .map(str::to_string)
}

fn validate_grant(grant: &Value, session_id: &str, now: u64) -> Result<u64, String> {
    if grant.get("ok").and_then(Value::as_bool) != Some(true)
        || grant.get("session_id").and_then(Value::as_str) != Some(session_id)
        || grant_selector(grant).is_none()
    {
        return Err("receipt_grant_invalid".into());
    }
    let attachment = &grant["receipt_attachment"];
    if attachment.get("receipt_protocol").and_then(Value::as_str) != Some(RECEIPT_PROTOCOL) {
        return Err("receipt_grant_protocol_invalid".into());
    }
    attachment
        .get("client_expires_at_ms")
        .and_then(Value::as_u64)
        .filter(|deadline| *deadline > now)
        .ok_or_else(|| "receipt_grant_expired_or_invalid".into())
}

fn receipt_refresh_at(now: u64, deadline: u64, previous: Option<u64>) -> Result<u64, String> {
    let remaining = deadline
        .checked_sub(now)
        .filter(|remaining| *remaining > 0)
        .ok_or("receipt_grant_expired_or_invalid")?;
    if previous.is_some_and(|previous| deadline <= previous) {
        return Ok(deadline);
    }
    Ok(now + (remaining / 2).max(1))
}

fn now_ms() -> Result<u64, String> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "receipt_clock_unavailable")?;
    u64::try_from(elapsed.as_millis()).map_err(|_| "receipt_clock_unavailable".into())
}

fn sha256_hex(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn public_payload_digest(payload_json: &str) -> String {
    format!("{RECEIPT_PROTOCOL}:sha256:{}", sha256_hex(payload_json))
}

fn remaining_until(deadline: Instant) -> Result<Duration, String> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| "receipt_observation_deadline_elapsed".into())
}

fn send_response(request: RequestBuilder, deadline: Instant) -> Result<Response, String> {
    let response = request
        .timeout(remaining_until(deadline)?.min(CONTROL_REQUEST_TIMEOUT))
        .send()
        .map_err(|error| {
            if error.is_timeout() {
                "http_timeout_or_deadline_elapsed"
            } else {
                "http_transport_failed"
            }
        })?;
    remaining_until(deadline)?;
    if !response.status().is_success() {
        return Err(format!("http_status_{}", response.status().as_u16()));
    }
    Ok(response)
}

fn decode_response<Output: DeserializeOwned>(response: Response) -> Result<Output, String> {
    response
        .json()
        .map_err(|_| "http_response_decode_failed".into())
}

fn send_typed<Output: DeserializeOwned>(
    request: RequestBuilder,
    deadline: Instant,
) -> Result<Output, String> {
    let output = decode_response(send_response(request, deadline)?)?;
    remaining_until(deadline)?;
    Ok(output)
}

fn send_json(request: RequestBuilder, deadline: Instant) -> Result<Value, String> {
    send_typed(request, deadline)
}

fn trace_entry(operation: &str, success: bool) -> Value {
    json!({"operation": operation, "status": if success { "ok" } else { "failed" }})
}

fn result_summary(result: &Result<(), String>) -> &str {
    match result {
        Ok(()) => "ok",
        Err(error) => error,
    }
}

fn session_attach_body(surface_id: &str) -> Value {
    json!({"surface": surface_id, "role": "writer", "receipt_protocol": RECEIPT_PROTOCOL})
}

fn session_detach_body(surface_id: &str) -> Value {
    json!({"surface": surface_id, "receipt_protocol": RECEIPT_PROTOCOL})
}

fn session_lease_release_body(session_id: &str) -> Value {
    json!({"session_id": session_id})
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::live::{
        ReceiptCursor, SessionMessageReceiptUnavailableReason, SessionReceiptSessionObservation,
    };

    const CLIENT_ID: &str = "a1111111-1111-4111-8111-111111111111";

    #[test]
    fn terminal_selection_ignores_stale_latest_text_and_rejects_real_ambiguity() {
        let lookup = |message_id: &str| {
            let mut current = context();
            if let SessionMessageReceiptContext::Available {
                message_id: observed,
                session_source,
                ..
            } = &mut current
            {
                *observed = message_id.to_string();
                if message_id != "terminal" {
                    session_source.turn_id = "old-turn".into();
                }
            }
            Ok(current)
        };
        assert_eq!(
            select_terminal_source(
                &["old".into(), "later".into()],
                "session-1",
                "turn-1",
                "execution-1",
                lookup
            )
            .unwrap(),
            None
        );
        assert_eq!(
            select_terminal_source(
                &[
                    "old".into(),
                    "terminal".into(),
                    "later".into(),
                    "terminal".into()
                ],
                "session-1",
                "turn-1",
                "execution-1",
                lookup
            )
            .unwrap(),
            Some("terminal".into())
        );
        assert!(select_terminal_source(
            &["terminal".into()],
            "session-1",
            "turn-1",
            "execution-1",
            |_| Err("receipt_transport_failed".into())
        )
        .is_err());
        assert!(select_terminal_source(
            &["first".into(), "second".into()],
            "session-1",
            "turn-1",
            "execution-1",
            |id| {
                let mut current = context();
                if let SessionMessageReceiptContext::Available { message_id, .. } = &mut current {
                    *message_id = id.to_string();
                }
                Ok(current)
            }
        )
        .is_err());
    }

    fn context() -> SessionMessageReceiptContext {
        let payload_json = r#"[{"type":"text","text":"56"}]"#.to_string();
        SessionMessageReceiptContext::Available {
            payload_kind: SessionPublicPayloadKind::SessionPublicV1,
            content_digest: public_payload_digest(&payload_json),
            payload_json,
            session_id: "session-1".into(),
            message_id: "assistant:input-1".into(),
            client_id: CLIENT_ID.into(),
            message_sequence: "2".into(),
            runtime_commit_cursor: ReceiptCursor::parse("9007199254740993").unwrap(),
            receipt: None,
            session_source: SessionReceiptSessionObservation {
                request_id: "request-1".into(),
                turn_id: "turn-1".into(),
                session_generation: "1".into(),
                input_sequence: "1".into(),
                presence_revision: "3".into(),
                observed_at_ms: 100,
            },
            outbox_observation: SessionReceiptOutboxObservation::RuntimeOutboxUnknown,
        }
    }

    fn ack_for(context: &SessionMessageReceiptContext) -> SessionMessageAck {
        let request =
            context_request(context, "session-1", "assistant:input-1", CLIENT_ID).unwrap();
        SessionMessageAck {
            session_id: "session-1".into(),
            message_id: "assistant:input-1".into(),
            client_id: CLIENT_ID.into(),
            runtime_commit_cursor: request.runtime_commit_cursor,
            content_digest: request.content_digest,
            received_at_ms: 110,
        }
    }

    fn with_receipt(
        mut context: SessionMessageReceiptContext,
        ack: SessionMessageAck,
    ) -> SessionMessageReceiptContext {
        if let SessionMessageReceiptContext::Available {
            receipt,
            session_source,
            ..
        } = &mut context
        {
            *receipt = Some(ack);
            session_source.observed_at_ms = 120;
        }
        context
    }

    fn known_outbox(execution: Option<&str>) -> SessionReceiptOutboxObservation {
        SessionReceiptOutboxObservation::RuntimeOutbox {
            terminal_id: "turn-terminal:request-1".into(),
            revision: "1".into(),
            status: "pending".into(),
            materialized_at_ms: None,
            runtime_commit_cursor: ReceiptCursor::parse("9007199254740993").unwrap(),
            execution_id: execution.map(str::to_string),
            turn_id: "turn-1".into(),
            request_id: "request-1".into(),
        }
    }

    #[test]
    fn lease_release_body_matches_the_gateway_contract() {
        assert_eq!(
            session_lease_release_body("session-1"),
            json!({"session_id":"session-1"})
        );
        assert_eq!(
            session_attach_body("surface"),
            json!({"surface":"surface","role":"writer","receipt_protocol":"session-public-v1"})
        );
        assert_eq!(
            session_detach_body("surface"),
            json!({"surface":"surface","receipt_protocol":"session-public-v1"})
        );
    }

    #[test]
    fn receipt_request_has_only_original_lossless_tuple() {
        let request =
            context_request(&context(), "session-1", "assistant:input-1", CLIENT_ID).unwrap();
        assert_eq!(request.validate().unwrap(), 9007199254740993);
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            json!({
                "runtime_commit_cursor":"9007199254740993",
                "content_digest":public_payload_digest(r#"[{"type":"text","text":"56"}]"#)
            })
        );
        assert!(serde_json::from_value::<SessionMessageAckRequest>(json!({
            "runtime_commit_cursor":"1","content_digest":request.content_digest,"client_id":CLIENT_ID
        })).is_err());
    }

    #[test]
    fn foreign_context_and_unavailable_cannot_supply_ack_request() {
        for (session, message, client) in [
            ("foreign", "assistant:input-1", CLIENT_ID),
            ("session-1", "foreign", CLIENT_ID),
            ("session-1", "assistant:input-1", "foreign"),
        ] {
            assert!(context_request(&context(), session, message, client).is_err());
        }
        let unavailable = SessionMessageReceiptContext::Unavailable {
            reason: SessionMessageReceiptUnavailableReason::UnboundOrInvalidated,
        };
        assert!(
            context_request(&unavailable, "session-1", "assistant:input-1", CLIENT_ID).is_err()
        );
        assert!(delivery_blocks(&unavailable, "turn-1", "execution-1").is_err());
        assert!(
            serde_json::from_value::<SessionMessageReceiptContext>(json!({"status":"future"}))
                .is_err()
        );
    }

    #[test]
    fn digest_is_over_original_public_payload_bytes_not_rendered_text() {
        assert_eq!(
            public_payload_digest("[]"),
            "session-public-v1:sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
        );
        let mut changed = context();
        if let SessionMessageReceiptContext::Available { payload_json, .. } = &mut changed {
            payload_json.push(' ');
        }
        assert!(context_request(&changed, "session-1", "assistant:input-1", CLIENT_ID).is_err());
        let mut invalid = context();
        if let SessionMessageReceiptContext::Available { content_digest, .. } = &mut invalid {
            *content_digest = "invalid".into();
        }
        assert!(context_request(&invalid, "session-1", "assistant:input-1", CLIENT_ID).is_err());
    }

    #[test]
    fn ack_response_requires_every_identity_and_binding_field() {
        let before = context();
        let request =
            context_request(&before, "session-1", "assistant:input-1", CLIENT_ID).unwrap();
        let ack = ack_for(&before);
        assert!(validate_ack(&ack, "session-1", "assistant:input-1", CLIENT_ID, &request).is_ok());
        for field in [
            "session_id",
            "message_id",
            "client_id",
            "runtime_commit_cursor",
            "content_digest",
        ] {
            let mut value = serde_json::to_value(&ack).unwrap();
            value[field] = if field == "runtime_commit_cursor" {
                json!("1")
            } else {
                json!("foreign")
            };
            let foreign: SessionMessageAck = serde_json::from_value(value).unwrap();
            assert!(validate_ack(
                &foreign,
                "session-1",
                "assistant:input-1",
                CLIENT_ID,
                &request
            )
            .is_err());
        }
    }

    #[test]
    fn confirmed_current_receipt_is_sanitized_and_unknown_execution_stays_null() {
        let before = context();
        let ack = ack_for(&before);
        let current = with_receipt(before.clone(), ack.clone());
        let mut presence_advanced = current.clone();
        if let SessionMessageReceiptContext::Available { session_source, .. } =
            &mut presence_advanced
        {
            session_source.presence_revision = "8".into();
            session_source.observed_at_ms += 1;
        }
        assert!(
            confirmed_observation(&before, &ack, &presence_advanced, "turn-1", "execution-1")
                .is_ok()
        );
        let observation =
            confirmed_observation(&before, &ack, &current, "turn-1", "execution-1").unwrap();
        assert_eq!(
            delivery_blocks(&before, "turn-1", "execution-1").unwrap(),
            json!([{"type":"text","text":"56"}])
        );
        let expected = json!({
            "status":"confirmed","session_id":"session-1","message_id":"assistant:input-1",
            "turn_id":"turn-1","execution_id":null,"runtime_commit_cursor":"9007199254740993",
            "content_digest":ack.content_digest,"client_binding_sha256":sha256_hex(CLIENT_ID),
            "received_at_ms":110,"fresh_context_confirmed":true
        });
        for (field, value) in expected.as_object().unwrap() {
            assert_eq!(&observation[field], value);
        }
        assert_eq!(
            observation["ack"],
            observation["current_context"]["receipt"]
        );
        assert_eq!(
            observation["ack"]["runtime_commit_cursor"],
            "9007199254740993"
        );
        assert_eq!(
            observation["current_context"]["message_id"],
            "assistant:input-1"
        );
        let encoded = observation.to_string();
        assert!(!encoded.contains(CLIENT_ID));
        assert!(!encoded.contains("payload"));
        assert!(!encoded.contains("materialized"));
    }

    #[test]
    fn known_outbox_binds_execution_but_not_physical_readiness() {
        let mut before = context();
        if let SessionMessageReceiptContext::Available {
            outbox_observation, ..
        } = &mut before
        {
            *outbox_observation = known_outbox(Some("execution-1"));
        }
        let ack = ack_for(&before);
        let current = with_receipt(before.clone(), ack.clone());
        let observation =
            confirmed_observation(&before, &ack, &current, "turn-1", "execution-1").unwrap();
        assert_eq!(observation["execution_id"], "execution-1");
        assert!(delivery_blocks(&before, "turn-1", "foreign").is_err());
        assert!(delivery_blocks(&before, "foreign", "execution-1").is_err());
        if let SessionMessageReceiptContext::Available {
            outbox_observation, ..
        } = &mut before
        {
            *outbox_observation = known_outbox(None);
        }
        assert!(delivery_blocks(&before, "turn-1", "execution-1").is_ok());
        if let SessionMessageReceiptContext::Available {
            outbox_observation:
                SessionReceiptOutboxObservation::RuntimeOutbox {
                    runtime_commit_cursor,
                    ..
                },
            ..
        } = &mut before
        {
            *runtime_commit_cursor = ReceiptCursor::parse("1").unwrap();
        }
        assert!(delivery_blocks(&before, "turn-1", "execution-1").is_err());
    }

    #[test]
    fn missing_stale_foreign_or_historical_current_receipt_is_not_confirmation() {
        let before = context();
        let ack = ack_for(&before);
        assert!(confirmed_observation(&before, &ack, &before, "turn-1", "execution-1").is_err());
        let current = with_receipt(before.clone(), ack.clone());
        for field in [
            "session_id",
            "message_id",
            "client_id",
            "message_sequence",
            "runtime_commit_cursor",
            "content_digest",
        ] {
            let mut value = serde_json::to_value(&current).unwrap();
            value[field] = if matches!(field, "runtime_commit_cursor" | "message_sequence") {
                json!("1")
            } else {
                json!("foreign")
            };
            let changed: SessionMessageReceiptContext = serde_json::from_value(value).unwrap();
            assert!(
                confirmed_observation(&before, &ack, &changed, "turn-1", "execution-1").is_err()
            );
        }
        let mut changed = current.clone();
        if let SessionMessageReceiptContext::Available {
            receipt: Some(receipt),
            ..
        } = &mut changed
        {
            receipt.received_at_ms += 1;
        }
        assert!(confirmed_observation(&before, &ack, &changed, "turn-1", "execution-1").is_err());
        for field in [
            "request_id",
            "turn_id",
            "session_generation",
            "input_sequence",
        ] {
            let mut value = serde_json::to_value(&current).unwrap();
            value["session_source"][field] = json!("foreign");
            let changed: SessionMessageReceiptContext = serde_json::from_value(value).unwrap();
            assert!(
                confirmed_observation(&before, &ack, &changed, "turn-1", "execution-1").is_err()
            );
        }
        let unavailable = SessionMessageReceiptContext::Unavailable {
            reason: SessionMessageReceiptUnavailableReason::BindingMismatch,
        };
        assert!(
            confirmed_observation(&before, &ack, &unavailable, "turn-1", "execution-1").is_err()
        );
    }

    #[test]
    fn payload_must_be_consumable_block_array_before_ack() {
        for payload in ["{}", "null", "not-json"] {
            let mut before = context();
            if let SessionMessageReceiptContext::Available {
                payload_json,
                content_digest,
                ..
            } = &mut before
            {
                *payload_json = payload.into();
                *content_digest = public_payload_digest(payload);
            }
            assert!(delivery_blocks(&before, "turn-1", "execution-1").is_err());
        }
    }

    #[test]
    fn malformed_grant_keeps_original_selector_for_cleanup_not_authority() {
        let mut grant = json!({"ok":true,"session_id":"session-1","receipt_attachment":{
            "client_id":CLIENT_ID,"client_expires_at_ms":200,"receipt_protocol":"session-public-v1"
        }});
        assert_eq!(validate_grant(&grant, "session-1", 100).unwrap(), 200);
        assert!(validate_grant(&grant, "foreign", 100).is_err());
        assert!(validate_grant(&grant, "session-1", 200).is_err());
        grant["receipt_attachment"]["receipt_protocol"] = json!("foreign");
        assert!(validate_grant(&grant, "session-1", 100).is_err());
        assert_eq!(grant_selector(&grant).as_deref(), Some(CLIENT_ID));
        grant["receipt_attachment"]["client_expires_at_ms"] = Value::Null;
        assert_eq!(grant_selector(&grant).as_deref(), Some(CLIENT_ID));
        grant["receipt_attachment"]["client_id"] = json!("invalid\nheader");
        assert!(grant_selector(&grant).is_none());
    }

    #[test]
    fn original_grant_schedule_requires_valid_ttl_and_avoids_capped_retry_loops() {
        assert_eq!(receipt_refresh_at(100, 200, None).unwrap(), 150);
        assert_eq!(receipt_refresh_at(150, 250, Some(200)).unwrap(), 200);
        assert_eq!(receipt_refresh_at(150, 200, Some(200)).unwrap(), 200);
        assert_eq!(receipt_refresh_at(150, 180, Some(200)).unwrap(), 180);
        assert_eq!(receipt_refresh_at(100, 101, None).unwrap(), 101);
        assert_eq!(
            receipt_refresh_at(u64::MAX - 2, u64::MAX, None).unwrap(),
            u64::MAX - 1
        );
        assert!(receipt_refresh_at(200, 200, None).is_err());
        assert!(receipt_refresh_at(201, 200, Some(200)).is_err());
    }

    #[test]
    fn reaffirm_grant_mutations_cannot_change_original_authority() {
        let client = Client::new();
        let grant = json!({"ok":true,"session_id":"session-1","receipt_attachment":{
            "client_id":CLIENT_ID,"client_expires_at_ms":300,"receipt_protocol":RECEIPT_PROTOCOL
        }});
        let mut actor = SessionActor::new(
            &client,
            "http://unused.invalid",
            "session-1",
            "surface",
            Instant::now() + Duration::from_secs(1),
        );
        actor.receipt_selector = Some(CLIENT_ID.into());
        actor.receipt_expires_at_ms = Some(200);
        actor.receipt_refresh_at_ms = Some(150);
        for (pointer, value) in [
            ("/ok", json!(false)),
            ("/session_id", json!("foreign")),
            (
                "/receipt_attachment/client_id",
                json!("b1111111-1111-4111-8111-111111111111"),
            ),
            ("/receipt_attachment/client_id", json!("invalid\nheader")),
            ("/receipt_attachment/receipt_protocol", json!("foreign")),
            ("/receipt_attachment/client_expires_at_ms", json!(150)),
            ("/receipt_attachment/client_expires_at_ms", json!(149)),
            ("/receipt_attachment/client_expires_at_ms", Value::Null),
            ("/receipt_attachment/client_expires_at_ms", json!("300")),
        ] {
            let mut changed = grant.clone();
            *changed.pointer_mut(pointer).unwrap() = value;
            assert!(actor.adopt_reaffirmed_grant(&changed, 150).is_err());
            assert_eq!(actor.receipt_selector.as_deref(), Some(CLIENT_ID));
            assert_eq!(actor.receipt_expires_at_ms, Some(200));
            assert_eq!(actor.receipt_refresh_at_ms, Some(150));
            assert!(!actor.attached);
            assert!(!actor.lease_cleanup_pending);
            assert!(actor.selector().is_err());
        }
        assert!(actor.adopt_reaffirmed_grant(&grant, 200).is_err());
        assert_eq!(actor.receipt_expires_at_ms, Some(200));
        actor.adopt_reaffirmed_grant(&grant, 150).unwrap();
        assert_eq!(actor.receipt_selector.as_deref(), Some(CLIENT_ID));
        assert_eq!(actor.receipt_expires_at_ms, Some(300));
        assert_eq!(actor.receipt_refresh_at_ms, Some(225));
        actor.adopt_reaffirmed_grant(&grant, 225).unwrap();
        assert_eq!(actor.receipt_refresh_at_ms, Some(300));
        assert!(!actor.attached);
        assert!(actor.selector().is_err());
    }

    #[test]
    fn maintenance_failure_is_latched_without_retrying_attach() {
        let client = Client::new();
        let mut actor = SessionActor::new(
            &client,
            "http://unused.invalid",
            "session-1",
            "surface",
            Instant::now() + Duration::from_secs(1),
        );
        assert_eq!(
            actor.maintain_receipt_authority().unwrap_err(),
            "receipt_attachment_inactive"
        );
        assert_eq!(
            actor.receipt_authority_error.as_deref(),
            Some("receipt_attachment_inactive")
        );
        assert_eq!(
            actor.maintain_receipt_authority().unwrap_err(),
            "receipt_attachment_inactive"
        );
        assert_eq!(actor.receipt_selector, None);
        assert!(!actor.attached);
        assert!(!actor.lease_cleanup_pending);
        assert!(actor
            .trace
            .iter()
            .all(|entry| entry == &trace_entry("reaffirm_session", false)));
    }

    #[test]
    fn history_reads_share_one_deadline_instead_of_one_timeout_per_message() {
        let mut stale = context();
        if let SessionMessageReceiptContext::Available {
            session_source,
            message_id,
            ..
        } = &mut stale
        {
            session_source.turn_id = "old-turn".into();
            *message_id = "old".into();
        }
        let (url, server) = delayed_responses(vec![
            (Duration::ZERO, serde_json::to_value(&stale).unwrap()),
            (
                Duration::from_millis(700),
                serde_json::to_value(context()).unwrap(),
            ),
        ]);
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let started = Instant::now();
        let mut actor = scripted_actor(&client, &url, started + Duration::from_millis(350));
        let result = actor.select_terminal_message(
            &["old".into(), "assistant:input-1".into(), "later".into()],
            "turn-1",
            "execution-1",
        );
        let elapsed = started.elapsed();
        actor.attached = false;
        assert!(result.is_err());
        assert!(elapsed < Duration::from_millis(650), "{elapsed:?}");
        assert_eq!(server.join().unwrap(), 2);
    }

    #[test]
    fn late_ack_cannot_confirm_or_start_another_context_read() {
        let original = context();
        let (url, server) = delayed_responses(vec![
            (Duration::ZERO, serde_json::to_value(&original).unwrap()),
            (
                Duration::from_millis(700),
                serde_json::to_value(ack_for(&original)).unwrap(),
            ),
        ]);
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let started = Instant::now();
        let mut actor = scripted_actor(&client, &url, started + Duration::from_millis(350));
        let result = actor.confirm_terminal_delivery("assistant:input-1", "turn-1", "execution-1");
        let elapsed = started.elapsed();
        actor.attached = false;
        assert!(result.is_err());
        assert!(actor
            .trace
            .iter()
            .any(|entry| entry == &trace_entry("confirm_terminal_delivery", false)));
        assert!(elapsed < Duration::from_millis(650), "{elapsed:?}");
        assert_eq!(server.join().unwrap(), 2);
    }

    #[test]
    fn current_receipt_confirms_before_deadline_but_expired_authority_sends_nothing() {
        let original = context();
        let ack = ack_for(&original);
        let mut fresh = original.clone();
        if let SessionMessageReceiptContext::Available { receipt, .. } = &mut fresh {
            *receipt = Some(ack.clone());
        }
        let (url, server) = delayed_responses(vec![
            (Duration::ZERO, serde_json::to_value(&original).unwrap()),
            (Duration::ZERO, serde_json::to_value(&ack).unwrap()),
            (Duration::ZERO, serde_json::to_value(fresh).unwrap()),
        ]);
        let client = Client::builder().no_proxy().build().unwrap();
        let mut actor = scripted_actor(&client, &url, Instant::now() + Duration::from_secs(1));
        let delivered = actor
            .confirm_terminal_delivery("assistant:input-1", "turn-1", "execution-1")
            .unwrap();
        assert_eq!(delivered.observation["fresh_context_confirmed"], true);
        actor.set_observation_deadline(Instant::now() - Duration::from_millis(1));
        assert_eq!(
            actor
                .confirm_terminal_delivery("assistant:input-1", "turn-1", "execution-1")
                .err()
                .as_deref(),
            Some("receipt_observation_deadline_elapsed")
        );
        actor.attached = false;
        assert_eq!(server.join().unwrap(), 3);
    }

    #[test]
    fn cleanup_budget_is_independent_and_never_renewed_by_finish_or_drop() {
        let (url, server) =
            delayed_responses(vec![(Duration::from_millis(500), json!({"ok":true}))]);
        let client = Client::builder().no_proxy().build().unwrap();
        let mut actor = scripted_actor(&client, &url, Instant::now() - Duration::from_secs(1));
        actor.lease_cleanup_pending = true;
        actor.cleanup_deadline = Some(Instant::now() + Duration::from_millis(200));
        let deadline = actor.cleanup_deadline;
        let started = Instant::now();
        assert!(actor.finish().is_err());
        assert!(actor.attached && actor.lease_cleanup_pending);
        assert_eq!(actor.cleanup_deadline, deadline);
        assert!(actor.finish().is_err());
        assert_eq!(actor.cleanup_deadline, deadline);
        drop(actor);
        assert!(started.elapsed() < Duration::from_millis(450));
        assert_eq!(server.join().unwrap(), 1);
    }

    fn scripted_actor<'client>(
        client: &'client Client,
        url: &str,
        deadline: Instant,
    ) -> SessionActor<'client> {
        let mut actor = SessionActor::new(client, url, "session-1", "surface", deadline);
        actor.attached = true;
        actor.receipt_selector = Some(CLIENT_ID.into());
        actor.receipt_expires_at_ms = Some(now_ms().unwrap() + 60_000);
        actor.receipt_refresh_at_ms = Some(now_ms().unwrap() + 30_000);
        actor
    }

    fn delayed_responses(
        responses: Vec<(Duration, Value)>,
    ) -> (String, std::thread::JoinHandle<usize>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(3);
            let mut accepted = 0;
            for (delay, body) in responses {
                let mut connection = loop {
                    match listener.accept() {
                        Ok((connection, _)) => break connection,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(1))
                        }
                        Err(_) => return accepted,
                    }
                };
                accepted += 1;
                connection
                    .set_read_timeout(Some(Duration::from_millis(200)))
                    .unwrap();
                let mut buffer = [0; 8192];
                let _ = connection.read(&mut buffer);
                std::thread::sleep(delay);
                let body = body.to_string();
                let _ = write!(connection, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
            }
            accepted
        });
        (url, server)
    }

    #[test]
    fn trace_has_no_request_response_identity_payload_or_error_echo() {
        assert_eq!(
            trace_entry("attach_session", true),
            json!({"operation":"attach_session","status":"ok"})
        );
        assert_eq!(
            trace_entry("post_mutation", false),
            json!({"operation":"post_mutation","status":"failed"})
        );
    }
}
