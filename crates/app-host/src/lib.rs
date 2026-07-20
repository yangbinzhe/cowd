//! Generic host-side application composition.
//!
//! This crate has no product application imports. Product bundles construct
//! contributions; Gateway, TUI and WebUI consume the registry projection.

use std::collections::{BTreeMap, BTreeSet};

use axum::Router;
use cowd_app_sdk::{AppContractError, AppDescriptor, AppHealth, AppId, CapabilityApp};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone)]
pub struct HttpAppContribution {
    pub router: Router,
}

impl std::fmt::Debug for HttpAppContribution {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpAppContribution")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiAppContribution {
    pub panel_id: String,
    pub title: String,
    pub actions: Vec<String>,
}

pub struct AppContribution {
    pub app: Box<dyn CapabilityApp>,
    pub http: Option<HttpAppContribution>,
    pub tui: Option<TuiAppContribution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredApp {
    pub descriptor: AppDescriptor,
    pub health: AppHealth,
    pub http_registered: bool,
    pub tui_registered: bool,
}

/// Startup-only registry. It is intentionally immutable after construction:
/// source changes require a product build/release, never a runtime code load.
#[derive(Default)]
pub struct AppRegistry {
    apps: BTreeMap<AppId, RegisteredApp>,
    http: Vec<HttpAppContribution>,
    tui: BTreeMap<AppId, TuiAppContribution>,
}

impl AppRegistry {
    pub fn register(&mut self, contribution: AppContribution) -> Result<(), AppRegistryError> {
        let descriptor = contribution.app.descriptor();
        descriptor.validate()?;
        let app_id = descriptor.id.clone();
        if self.apps.contains_key(&app_id) {
            return Err(AppRegistryError::DuplicateApp(app_id));
        }

        let existing_routes: BTreeSet<(String, String)> = self
            .apps
            .values()
            .flat_map(|app| app.descriptor.routes.iter())
            .map(|route| (route.method.clone(), route.path.clone()))
            .collect();
        for route in &descriptor.routes {
            if existing_routes.contains(&(route.method.clone(), route.path.clone())) {
                return Err(AppRegistryError::RouteCollision {
                    app_id,
                    method: route.method.clone(),
                    path: route.path.clone(),
                });
            }
        }

        let existing_capabilities: BTreeSet<String> = self
            .apps
            .values()
            .flat_map(|app| app.descriptor.capabilities.iter().cloned())
            .collect();
        if let Some(capability) = descriptor
            .capabilities
            .iter()
            .find(|capability| existing_capabilities.contains(*capability))
        {
            return Err(AppRegistryError::CapabilityCollision {
                app_id,
                capability: capability.clone(),
            });
        }

        let health = contribution.app.health();
        let http_registered = contribution.http.is_some();
        let tui_registered = contribution.tui.is_some();
        if let Some(http) = contribution.http {
            self.http.push(http);
        }
        if let Some(tui) = contribution.tui {
            self.tui.insert(app_id.clone(), tui);
        }
        self.apps.insert(
            app_id,
            RegisteredApp {
                descriptor,
                health,
                http_registered,
                tui_registered,
            },
        );
        Ok(())
    }

    #[must_use]
    pub fn apps(&self) -> Vec<RegisteredApp> {
        self.apps.values().cloned().collect()
    }

    #[must_use]
    pub fn app(&self, id: &AppId) -> Option<&RegisteredApp> {
        self.apps.get(id)
    }

    #[must_use]
    pub fn tui(&self) -> Vec<(AppId, TuiAppContribution)> {
        self.tui
            .iter()
            .map(|(id, item)| (id.clone(), item.clone()))
            .collect()
    }

    #[must_use]
    pub fn into_http_router(self) -> Router {
        self.http
            .into_iter()
            .fold(Router::new(), |router, app| router.merge(app.router))
    }

    /// Build the product's application router from already-registered
    /// contributions. This is read-only and may be called by Gateway while it
    /// assembles its outer authentication/capacity middleware.
    #[must_use]
    pub fn http_router(&self) -> Router {
        self.http.iter().fold(Router::new(), |router, app| {
            router.merge(app.router.clone())
        })
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AppRegistryError {
    #[error(transparent)]
    Contract(#[from] AppContractError),
    #[error("duplicate app id: {0}")]
    DuplicateApp(AppId),
    #[error("route collision for app {app_id}: {method} {path}")]
    RouteCollision {
        app_id: AppId,
        method: String,
        path: String,
    },
    #[error("capability collision for app {app_id}: {capability}")]
    CapabilityCollision { app_id: AppId, capability: String },
}

#[cfg(test)]
mod tests {
    use axum::{routing::get, Router};
    use cowd_app_sdk::{
        AppActionDescriptor, AppProfileDescriptor, AppProfileVariant, SDK_API_VERSION,
    };

    use super::*;

    struct FixtureApp {
        id: &'static str,
        path: &'static str,
        capability: &'static str,
    }

    impl CapabilityApp for FixtureApp {
        fn descriptor(&self) -> AppDescriptor {
            AppDescriptor {
                id: AppId::parse(self.id).expect("fixture id"),
                display_name: self.id.to_string(),
                sdk_api: SDK_API_VERSION,
                version: "0.1.0".to_string(),
                capabilities: vec![self.capability.to_string()],
                routes: vec![cowd_app_sdk::AppRouteDescriptor {
                    method: "GET".to_string(),
                    path: self.path.to_string(),
                    operation_id: format!("{}.ping", self.id),
                    streaming: false,
                }],
                actions: vec![AppActionDescriptor {
                    id: "read".to_string(),
                    title: "Read".to_string(),
                    requires_confirmation: false,
                }],
                profile: Some(AppProfileDescriptor {
                    catalog_revision: 1,
                    capability_digest: "fixture".to_string(),
                    default_profile_id: "operator".to_string(),
                    profiles: vec![AppProfileVariant {
                        id: "operator".to_string(),
                        capabilities: vec!["fixture.read".to_string()],
                    }],
                    surface_capabilities: std::collections::BTreeMap::from([(
                        "tui".to_string(),
                        vec!["fixture.read".to_string()],
                    )]),
                }),
            }
        }
        fn health(&self) -> AppHealth {
            AppHealth::Ready
        }
    }

    fn fixture(id: &'static str, path: &'static str, capability: &'static str) -> AppContribution {
        AppContribution {
            app: Box::new(FixtureApp {
                id,
                path,
                capability,
            }),
            http: Some(HttpAppContribution {
                router: Router::new().route(path, get(|| async { "ok" })),
            }),
            tui: Some(TuiAppContribution {
                panel_id: format!("{id}.panel"),
                title: id.to_string(),
                actions: vec!["read".to_string()],
            }),
        }
    }

    #[test]
    fn registry_has_a_real_generic_http_and_tui_consumer() {
        let mut registry = AppRegistry::default();
        registry
            .register(fixture("fixture", "/api/apps/fixture/ping", "fixture.read"))
            .expect("register fixture");
        assert_eq!(registry.apps().len(), 1);
        assert_eq!(registry.tui().len(), 1);
        let _router = registry.into_http_router();
    }

    #[test]
    fn registry_rejects_route_and_capability_collisions_before_startup() {
        let mut registry = AppRegistry::default();
        registry
            .register(fixture("fixture", "/api/apps/fixture/ping", "fixture.read"))
            .expect("first app");
        assert!(matches!(
            registry.register(fixture("second", "/api/apps/fixture/ping", "second.read")),
            Err(AppRegistryError::RouteCollision { .. })
        ));
        assert!(matches!(
            registry.register(fixture("third", "/api/apps/third/ping", "fixture.read")),
            Err(AppRegistryError::CapabilityCollision { .. })
        ));
    }
}
