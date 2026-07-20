//! Generic application catalogue and contribution mount point.
//!
//! Product bundles supply registered applications. This module intentionally
//! contains no application id, route contract, DTO, or feature-specific
//! authorization rule.

use std::sync::Arc;

use axum::{routing::get, Extension, Json, Router};
use cowd_app_host::{AppRegistry, RegisteredApp};

pub(super) fn router(app_registry: Arc<AppRegistry>) -> Router<Arc<super::AppState>> {
    catalogue_router(app_registry).with_state::<Arc<super::AppState>>(())
}

fn catalogue_router(app_registry: Arc<AppRegistry>) -> Router {
    app_registry
        .http_router()
        .merge(Router::new().route("/api/apps", get(list_apps)))
        .layer(Extension(app_registry))
}

async fn list_apps(
    Extension(app_registry): Extension<Arc<AppRegistry>>,
) -> Json<Vec<RegisteredApp>> {
    Json(app_registry.apps())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[tokio::test]
    async fn core_catalogue_is_a_real_empty_registry_consumer() {
        let response = catalogue_router(Arc::new(AppRegistry::default()))
            .oneshot(
                Request::get("/api/apps")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }
}
