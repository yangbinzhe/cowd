use std::sync::Arc;
use std::time::Instant;

use crate::gateway::ActiveSessions;
use crate::runtime_boundary::{
    RuntimeBoundaryClock, RuntimeBoundarySnapshot, RuntimeBoundaryStatus,
};
use crate::runtime_host::SessionLeaseRegistry;
use crate::runtime_protocol::{RuntimeErrorKind, RuntimeRequest, RuntimeResponse};
use crate::session_kernel::SessionKernel;
use crate::session_lifecycle_kernel::{SessionActor, SessionLifecycleKernel};

#[derive(Clone)]
pub(crate) struct RuntimeService {
    sessions: Arc<ActiveSessions>,
    lease_registry: Arc<SessionLeaseRegistry>,
    session_kernel: Arc<SessionKernel>,
    lifecycle_kernel: Arc<SessionLifecycleKernel>,
    started_at: Instant,
}

impl RuntimeService {
    #[must_use]
    pub(crate) fn new(
        sessions: Arc<ActiveSessions>,
        lease_registry: Arc<SessionLeaseRegistry>,
        session_kernel: Arc<SessionKernel>,
        lifecycle_kernel: Arc<SessionLifecycleKernel>,
        started_at: Instant,
    ) -> Self {
        Self {
            sessions,
            lease_registry,
            session_kernel,
            lifecycle_kernel,
            started_at,
        }
    }

    #[must_use]
    pub(crate) fn status_value(&self) -> serde_json::Value {
        let status = self.status();
        serde_json::json!({
            "ok": true,
            "protocol_version": status.protocol_version,
            "runtime_host": status.runtime_host,
            "daemon": status.runtime_host,
            "compat": {
                "daemon": {
                    "delete_by": "0.9.293",
                    "replacement": "runtime_host",
                }
            },
            "active_sessions": status.active_sessions,
            "uptime_secs": status.uptime_secs,
        })
    }

    #[must_use]
    pub(crate) fn status(&self) -> RuntimeBoundaryStatus {
        RuntimeBoundaryStatus {
            protocol_version: crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            runtime_host: "gateway-runtime-host",
            active_sessions: self.sessions.list().len(),
            uptime_secs: self.clock().uptime_secs(),
        }
    }

    pub(crate) async fn snapshot_value(&self) -> serde_json::Value {
        let snapshot = self.snapshot().await;
        let leases = self.lease_registry.list().await;
        serde_json::json!({
            "ok": true,
            "kind": "gateway_runtime_snapshot",
            "legacy_kind": "daemon_runtime_snapshot",
            "protocol_version": snapshot.protocol_version,
            "runtime_host": snapshot.runtime_host,
            "daemon": snapshot.runtime_host,
            "compat": {
                "legacy_fields": {
                    "legacy_kind": {
                        "delete_by": "0.9.293",
                        "replacement": "kind",
                        "consumer": "tui/control_client",
                    },
                    "daemon": {
                        "delete_by": "0.9.293",
                        "replacement": "runtime_host",
                        "consumer": "tui/control_client",
                    }
                }
            },
            "active_sessions": snapshot.active_sessions,
            "uptime_secs": snapshot.uptime_secs,
            "sessions": snapshot.sessions,
            "leases": {
                "total": leases.len(),
                "items": leases,
            },
            "lifecycle": self.lifecycle_kernel.snapshots().await,
            "transport": {
                "control": "gateway_http",
                "socket_transition": {
                    "enabled": true,
                    "delete_by": "0.9.293",
                    "replacement": "gateway_http_sse",
                },
                "projection": "http_optional",
            },
        })
    }

    pub(crate) async fn snapshot(&self) -> RuntimeBoundarySnapshot {
        let mut session_ids = self.sessions.list();
        session_ids.sort();
        RuntimeBoundarySnapshot {
            protocol_version: crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            runtime_host: "gateway-runtime-host",
            active_sessions: session_ids.len(),
            uptime_secs: self.clock().uptime_secs(),
            sessions: session_ids,
        }
    }

    #[must_use]
    pub(crate) fn list_sessions_value(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "sessions": self.sessions.list(),
        })
    }

    pub(crate) async fn acquire_session_lease_value(
        &self,
        session_id: &str,
        owner: &str,
        mode: &str,
    ) -> serde_json::Value {
        self.lease_registry.acquire(session_id, owner, mode).await
    }

    pub(crate) async fn release_session_lease_value(
        &self,
        session_id: &str,
        owner: &str,
    ) -> serde_json::Value {
        self.lease_registry.release(session_id, owner).await
    }

    pub(crate) async fn attach_session_value(
        &self,
        session_id: &str,
        actor_id: &str,
        surface: &str,
        role: Option<&str>,
    ) -> serde_json::Value {
        let mut actor = SessionActor::new(actor_id, surface);
        actor.role = role.map(ToOwned::to_owned);
        match self.lifecycle_kernel.attach(session_id, actor).await {
            Ok(event) => {
                let snapshot = self.lifecycle_kernel.snapshot(session_id).await;
                serde_json::json!({
                    "ok": true,
                    "event": event,
                    "snapshot": snapshot,
                })
            }
            Err(error) => serde_json::json!({
                "ok": false,
                "error": error,
            }),
        }
    }

    pub(crate) async fn detach_session_value(
        &self,
        session_id: &str,
        actor_id: &str,
    ) -> serde_json::Value {
        match self.lifecycle_kernel.detach(session_id, actor_id).await {
            Ok(event) => {
                let snapshot = self.lifecycle_kernel.snapshot(session_id).await;
                serde_json::json!({
                    "ok": true,
                    "event": event,
                    "snapshot": snapshot,
                })
            }
            Err(error) => serde_json::json!({
                "ok": false,
                "error": error,
            }),
        }
    }

    pub(crate) async fn lifecycle_snapshot_value(
        &self,
        session_id: Option<&str>,
    ) -> serde_json::Value {
        match session_id {
            Some(session_id) => serde_json::json!({
                "ok": true,
                "session_id": session_id,
                "snapshot": self.lifecycle_kernel.snapshot(session_id).await,
            }),
            None => serde_json::json!({
                "ok": true,
                "sessions": self.lifecycle_kernel.snapshots().await,
            }),
        }
    }

    pub(crate) async fn replay_session_value(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> serde_json::Value {
        if session_id.trim().is_empty() {
            return serde_json::json!({
                "ok": false,
                "error": "session_id is required",
            });
        }
        let capped_limit = limit.clamp(1, 500);
        match self
            .session_kernel
            .stored_events_page(session_id, from_sequence, capped_limit)
            .await
        {
            Ok(Some((total, events))) => {
                let next_sequence = events
                    .last()
                    .map(|event| event.sequence + 1)
                    .unwrap_or(from_sequence);
                let projected_events: Vec<_> = events
                    .into_iter()
                    .map(|event| {
                        serde_json::json!({
                            "session_id": event.session_id,
                            "event_type": event.event_type,
                            "event_json": event.event_json,
                            "sequence": event.sequence,
                            "created_at_ms": event.created_at_ms,
                        })
                    })
                    .collect();
                serde_json::json!({
                    "ok": true,
                    "session_id": session_id,
                    "from_sequence": from_sequence,
                    "limit": capped_limit,
                    "total": total,
                    "next_sequence": next_sequence,
                    "events": projected_events,
                })
            }
            Ok(None) => serde_json::json!({
                "ok": true,
                "session_id": session_id,
                "from_sequence": from_sequence,
                "limit": capped_limit,
                "total": 0,
                "next_sequence": from_sequence,
                "events": [],
                "degraded": "unified session store unavailable",
            }),
            Err(error) => serde_json::json!({
                "ok": false,
                "error": error.to_string(),
            }),
        }
    }

    #[must_use]
    pub(crate) fn unsupported_protocol_value(request: &RuntimeRequest) -> serde_json::Value {
        let response = RuntimeResponse::unsupported_protocol(request);
        let message = response
            .error
            .as_ref()
            .map(|error| error.message.clone())
            .unwrap_or_else(|| "unsupported runtime protocol version".to_string());
        serde_json::json!({
            "ok": false,
            "protocol_version": crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            "request_id": response.request_id,
            "error": message,
            "error_kind": RuntimeErrorKind::UnsupportedProtocol,
            "retryable": false,
        })
    }

    fn clock(&self) -> RuntimeBoundaryClock {
        RuntimeBoundaryClock::from_uptime(self.started_at.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runtime_service_status_does_not_initialize_model_provider() {
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                Arc::new(ActiveSessions::default()),
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let value = service.status_value();
        assert_eq!(value["ok"], true);
        assert_eq!(value["runtime_host"], "gateway-runtime-host");
        assert_eq!(value["daemon"], "gateway-runtime-host");
        assert_eq!(value["compat"]["daemon"]["delete_by"], "0.9.293");
        assert_eq!(value["active_sessions"], 0);
    }

    #[tokio::test]
    async fn runtime_service_snapshot_reports_lease_projection() {
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                Arc::new(ActiveSessions::default()),
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let lease = service
            .acquire_session_lease_value("session-1", "tui:test", "collaborative")
            .await;
        assert_eq!(lease["ok"], true);

        let snapshot = service.snapshot_value().await;
        assert_eq!(snapshot["kind"], "gateway_runtime_snapshot");
        assert_eq!(snapshot["legacy_kind"], "daemon_runtime_snapshot");
        assert_eq!(
            snapshot["compat"]["legacy_fields"]["legacy_kind"]["delete_by"],
            "0.9.293"
        );
        assert_eq!(
            snapshot["compat"]["legacy_fields"]["daemon"]["replacement"],
            "runtime_host"
        );
        assert_eq!(snapshot["leases"]["total"], 1);
        assert_eq!(snapshot["transport"]["control"], "gateway_http");
        assert_eq!(
            snapshot["transport"]["socket_transition"]["delete_by"],
            "0.9.293"
        );
    }

    #[test]
    fn runtime_service_rejects_unsupported_protocol_as_legacy_socket_error() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "protocol_version": 999,
            "request_id": "req-old",
            "cmd": "status",
        }))
        .expect("request parses");

        let value = RuntimeService::unsupported_protocol_value(&request);
        assert_eq!(value["ok"], false);
        assert_eq!(value["request_id"], "req-old");
        assert_eq!(value["error_kind"], "unsupported_protocol");
        assert_eq!(value["retryable"], false);
        assert!(value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("unsupported runtime protocol version"));
    }

    #[tokio::test]
    async fn runtime_service_attach_detach_projects_lifecycle_snapshot() {
        let sessions = Arc::new(ActiveSessions::default());
        let service = RuntimeService::new(
            sessions.clone(),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                sessions,
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let attached = service
            .attach_session_value("session-1", "tui-1", "tui", Some("reader"))
            .await;
        assert_eq!(attached["ok"], true);
        assert_eq!(attached["event"]["sequence"], 0);
        assert_eq!(attached["snapshot"]["state"], "attached");

        let detached = service.detach_session_value("session-1", "tui-1").await;
        assert_eq!(detached["ok"], true);
        assert_eq!(detached["snapshot"]["state"], "detached");
    }
}
