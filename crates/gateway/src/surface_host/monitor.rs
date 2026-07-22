use std::time::Duration;

use chrono::Utc;
use surface::{SurfaceLifecycle, SurfaceRuntimeStatus};

use super::SurfaceHost;

impl SurfaceHost {
    pub(crate) fn start_monitor(&self) {
        let mut started = self
            .monitor_started
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *started {
            return;
        }
        *started = true;
        drop(started);

        let host = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                host.monitor_tick().await;
            }
        });
    }

    async fn monitor_tick(&self) {
        self.retry_due_deliveries().await;
        let surfaces = self.snapshot().surfaces;
        for surface in surfaces {
            if surface.lifecycle != SurfaceLifecycle::Managed {
                continue;
            }
            let Some(runtime) = self.runtime_snapshot(&surface.id) else {
                continue;
            };
            if !surface.default_enabled
                && self.config_for(&surface.id).is_none()
                && runtime.status == SurfaceRuntimeStatus::Discovered
            {
                continue;
            }
            if matches!(
                runtime.status,
                SurfaceRuntimeStatus::Disabled | SurfaceRuntimeStatus::CircuitOpen
            ) {
                continue;
            }
            if let Some(next_retry_at) = runtime.next_retry_at {
                if next_retry_at > Utc::now() {
                    continue;
                }
            }
            let due = runtime
                .last_health_at
                .map(|last| {
                    let elapsed = Utc::now().signed_duration_since(last);
                    elapsed.num_milliseconds() >= surface.health.interval_ms as i64
                })
                .unwrap_or(surface.is_executable());
            if due {
                let _ = self.check_surface_health(&surface.id).await;
            }
        }
    }

    async fn retry_due_deliveries(&self) {
        let deliveries = match self.messages.due_retry_deliveries() {
            Ok(deliveries) => deliveries,
            Err(error) => {
                tracing::warn!(error = %error, "surface outbox retry scan failed");
                return;
            }
        };
        for delivery in deliveries {
            let delivery_id = delivery.delivery_id.clone();
            if let Err(error) = self.retry_outbox_delivery(&delivery_id).await {
                tracing::warn!(
                    surface = %delivery.surface,
                    delivery_id = %delivery_id,
                    error = %error,
                    "surface outbox retry failed"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, fs};
    use surface::SURFACE_MANIFEST_FILE;

    #[tokio::test]
    async fn monitor_does_not_start_unconfigured_default_disabled_sidecar() {
        let root = std::env::temp_dir().join(format!(
            "cowd-disabled-edge-monitor-{}",
            uuid::Uuid::new_v4()
        ));
        let surface_dir = root.join("disabled");
        fs::create_dir_all(&surface_dir).expect("create test manifest directory");
        fs::write(
            surface_dir.join(SURFACE_MANIFEST_FILE),
            r#"{
                "schema": "cowd.surface.v1",
                "id": "disabled",
                "name": "Disabled Edge",
                "version": "1.0.0",
                "kind": "external-integration",
                "runtime": {
                    "kind": "managed",
                    "artifact": "missing-disabled-edge",
                    "driver_profile": "test",
                    "transport": "uds-http2"
                },
                "health": { "interval_ms": 1, "timeout_ms": 1 },
                "default_enabled": false
            }"#,
        )
        .expect("write test manifest");

        let host = SurfaceHost::with_configs(vec![root.clone()], BTreeMap::new());
        host.discover();
        host.monitor_tick().await;

        let runtime = host.runtime_snapshot("disabled").expect("runtime snapshot");
        assert_eq!(runtime.status, SurfaceRuntimeStatus::Discovered);
        assert!(!runtime.active);
        assert_eq!(runtime.consecutive_failures, 0);
        let _ = fs::remove_dir_all(root);
    }
}
