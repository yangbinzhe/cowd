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
        for delivery in self.messages.due_retry_deliveries() {
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
