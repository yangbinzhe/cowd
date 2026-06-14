use std::sync::Arc;

use axum::{extract::Query, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use runtime::capability::{CowdCapabilityRegistry, CowdSurface};
use runtime::projection::CowdProjection;
use serde::Deserialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/cowd/capabilities", get(capabilities_handler))
        .route("/api/cowd/projection", get(projection_handler))
}

#[derive(Debug, Deserialize)]
struct ProjectionQuery {
    #[serde(default)]
    surface: Option<String>,
}

async fn capabilities_handler() -> impl IntoResponse {
    Json(CowdCapabilityRegistry::core())
}

async fn projection_handler(
    Query(query): Query<ProjectionQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let surface = parse_surface(query.surface.as_deref())?;
    let registry = CowdCapabilityRegistry::core();
    Ok(Json(CowdProjection::for_surface(&registry, surface)))
}

fn parse_surface(surface: Option<&str>) -> Result<CowdSurface, (StatusCode, Json<ErrorResponse>)> {
    match surface
        .unwrap_or("webui")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "" | "webui" => Ok(CowdSurface::Webui),
        "tui" => Ok(CowdSurface::Tui),
        "cli" => Ok(CowdSurface::Cli),
        other => Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("unsupported cowd projection surface: {other}"),
        )),
    }
}
