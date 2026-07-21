//! Generic host-side application composition.
//!
//! This crate has no product application imports. Product bundles construct
//! contributions; Gateway, TUI and WebUI consume the registry projection.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use axum::Router;
use cowd_app_sdk::{
    AppContractError, AppDescriptor, AppHealth, AppId, AppSkillDescriptor, CapabilityApp,
};
use crossterm::event::KeyEvent;
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    Frame,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

/// Stable, product-neutral visual tokens for an APP-owned terminal panel.
///
/// The Cowd TUI owns theme selection and converts its skin to this compact
/// palette before rendering an external APP.  APP code deliberately never
/// receives `SkinConfig`, `ThemeEngine`, or another private TUI type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TuiAppTheme {
    pub foreground: Color,
    pub muted: Color,
    pub accent: Color,
    pub success: Color,
    pub warning: Color,
    pub danger: Color,
    pub border: Color,
}

impl TuiAppTheme {
    #[must_use]
    pub const fn dark() -> Self {
        Self {
            foreground: Color::White,
            muted: Color::DarkGray,
            accent: Color::Cyan,
            success: Color::Green,
            warning: Color::Yellow,
            danger: Color::Red,
            border: Color::DarkGray,
        }
    }

    #[must_use]
    pub const fn foreground_style(self) -> Style {
        Style::new().fg(self.foreground)
    }

    #[must_use]
    pub const fn muted_style(self) -> Style {
        Style::new().fg(self.muted)
    }

    #[must_use]
    pub const fn accent_style(self) -> Style {
        Style::new().fg(self.accent)
    }
}

/// Read-only context supplied to an APP panel while Cowd renders it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TuiAppRenderContext {
    pub theme: TuiAppTheme,
    pub focused: bool,
}

/// A generic, app-owned action displayed by the terminal command palette.
/// The host does not interpret its risk policy: it asks the owning panel to
/// dispatch the action and only executes the resulting generic effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiAppAction {
    pub id: String,
    pub label: String,
    pub description: String,
    pub domain: String,
    pub risk: String,
    pub requires_confirmation: bool,
    pub enabled: bool,
    pub unavailable_reason: Option<String>,
}

/// Host-to-APP envelope. Payloads are intentionally JSON values: the APP
/// owns its canonical typed contract and deserializes only its own messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TuiAppEvent {
    Response {
        request_id: String,
        status: u16,
        body: Value,
    },
    RequestFailed {
        request_id: String,
        error: String,
    },
    LiveEnvelope {
        subscription_id: String,
        body: Value,
    },
    LiveFailed {
        subscription_id: String,
        error: String,
    },
    LiveStopped {
        subscription_id: String,
    },
}

/// APP-to-host command. Cowd performs network authentication, task lifetime,
/// cross-panel navigation and composer insertion; the APP owns endpoint
/// selection, request body, live semantics and state reduction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TuiAppEffect {
    Request {
        request_id: String,
        method: String,
        path: String,
        body: Option<Value>,
    },
    Subscribe {
        subscription_id: String,
        path: String,
    },
    Unsubscribe {
        subscription_id: String,
    },
    Navigate {
        route: String,
    },
    Composer {
        text: String,
    },
    Notice {
        level: TuiAppNoticeLevel,
        title: Option<String>,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TuiAppNoticeLevel {
    Info,
    Warning,
    Error,
}

/// Mutable effect sink passed to an APP panel. It has no network client,
/// credential, Gateway service, Tokio handle or Cowd state access.
#[derive(Debug, Default)]
pub struct TuiAppEffects {
    effects: Vec<TuiAppEffect>,
}

impl TuiAppEffects {
    pub fn push(&mut self, effect: TuiAppEffect) {
        self.effects.push(effect);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }

    pub fn take(&mut self) -> Vec<TuiAppEffect> {
        std::mem::take(&mut self.effects)
    }
}

/// Complete APP-owned TUI behavior. The trait intentionally contains the
/// whole user-facing seam (rendering, input, app event reduction, palette and
/// slash dispatch), so the Cowd TUI host cannot grow an APP-specific branch.
pub trait TuiAppPanel: Send {
    fn panel_id(&self) -> &str;

    fn render(&mut self, frame: &mut Frame<'_>, area: Rect, context: TuiAppRenderContext);

    fn handle_key(&mut self, key: KeyEvent, effects: &mut TuiAppEffects) -> bool;

    fn apply_event(&mut self, event: TuiAppEvent, effects: &mut TuiAppEffects);

    fn actions(&self) -> Vec<TuiAppAction>;

    fn dispatch_action(&mut self, action_id: &str, effects: &mut TuiAppEffects) -> bool;

    fn handle_command(&mut self, command: &str, effects: &mut TuiAppEffects) -> bool;
}

/// A factory is registered at product startup; each TUI process creates its
/// own panel and state.  It prevents any APP state from leaking through the
/// global Gateway registry.
pub trait TuiAppPanelFactory: Send + Sync {
    fn create(&self) -> Box<dyn TuiAppPanel>;
}

impl<F> TuiAppPanelFactory for F
where
    F: Fn() -> Box<dyn TuiAppPanel> + Send + Sync,
{
    fn create(&self) -> Box<dyn TuiAppPanel> {
        self()
    }
}

/// Product composition of a descriptor already visible to `AppRegistry` and
/// the concrete external panel factory consumed by the generic TUI host.
#[derive(Clone)]
pub struct TuiAppSurfaceContribution {
    pub app_id: AppId,
    pub descriptor: TuiAppContribution,
    factory: Arc<dyn TuiAppPanelFactory>,
}

impl std::fmt::Debug for TuiAppSurfaceContribution {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TuiAppSurfaceContribution")
            .field("app_id", &self.app_id)
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl TuiAppSurfaceContribution {
    #[must_use]
    pub fn new(
        app_id: AppId,
        descriptor: TuiAppContribution,
        factory: Arc<dyn TuiAppPanelFactory>,
    ) -> Self {
        Self {
            app_id,
            descriptor,
            factory,
        }
    }

    #[must_use]
    pub fn create_panel(&self) -> Box<dyn TuiAppPanel> {
        self.factory.create()
    }

    pub fn validate(&self) -> Result<(), AppRegistryError> {
        if self.descriptor.panel_id.trim().is_empty() {
            return Err(AppRegistryError::InvalidTuiContribution {
                app_id: self.app_id.clone(),
                reason: "panel_id is empty".to_string(),
            });
        }
        if self.descriptor.title.trim().is_empty() {
            return Err(AppRegistryError::InvalidTuiContribution {
                app_id: self.app_id.clone(),
                reason: "title is empty".to_string(),
            });
        }
        let mut actions = BTreeSet::new();
        if self
            .descriptor
            .actions
            .iter()
            .any(|action| action.trim().is_empty() || !actions.insert(action.as_str()))
        {
            return Err(AppRegistryError::InvalidTuiContribution {
                app_id: self.app_id.clone(),
                reason: "actions contain an empty or duplicate id".to_string(),
            });
        }
        Ok(())
    }
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
    skills: BTreeMap<AppId, Vec<AppSkillDescriptor>>,
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

    /// Attach APP-owned skill descriptors after the APP's transport
    /// contribution has been validated. Product composition is the sole
    /// caller: Gateway, CLI and TUI only read the flattened generic view.
    pub fn register_app_skills(
        &mut self,
        app_id: &AppId,
        skills: Vec<AppSkillDescriptor>,
    ) -> Result<(), AppRegistryError> {
        if !self.apps.contains_key(app_id) {
            return Err(AppRegistryError::UnknownApp(app_id.clone()));
        }
        let existing_ids = self
            .skills
            .values()
            .flat_map(|items| items.iter().map(|skill| skill.id.as_str()))
            .collect::<BTreeSet<_>>();
        let mut local_ids = BTreeSet::new();
        for skill in &skills {
            skill.validate(app_id)?;
            if !local_ids.insert(skill.id.as_str()) || existing_ids.contains(skill.id.as_str()) {
                return Err(AppRegistryError::DuplicateSkill {
                    app_id: app_id.clone(),
                    skill_id: skill.id.clone(),
                });
            }
        }
        self.skills.insert(app_id.clone(), skills);
        Ok(())
    }

    /// Deterministic, app-agnostic application skill catalogue.
    #[must_use]
    pub fn skills(&self) -> Vec<AppSkillDescriptor> {
        self.skills
            .values()
            .flat_map(|items| items.iter().cloned())
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
    #[error("application not registered: {0}")]
    UnknownApp(AppId),
    #[error("duplicate skill for app {app_id}: {skill_id}")]
    DuplicateSkill { app_id: AppId, skill_id: String },
    #[error("invalid TUI contribution for app {app_id}: {reason}")]
    InvalidTuiContribution { app_id: AppId, reason: String },
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

    struct FixturePanel;

    impl TuiAppPanel for FixturePanel {
        fn panel_id(&self) -> &str {
            "fixture.panel"
        }

        fn render(&mut self, frame: &mut Frame<'_>, area: Rect, context: TuiAppRenderContext) {
            use ratatui::widgets::Paragraph;

            frame.render_widget(
                Paragraph::new(if context.focused { "focused" } else { "idle" })
                    .style(context.theme.accent_style()),
                area,
            );
        }

        fn handle_key(&mut self, key: KeyEvent, effects: &mut TuiAppEffects) -> bool {
            if key.code == crossterm::event::KeyCode::Char('r') {
                effects.push(TuiAppEffect::Request {
                    request_id: "fixture.read".to_string(),
                    method: "GET".to_string(),
                    path: "/api/apps/fixture/ping".to_string(),
                    body: None,
                });
                return true;
            }
            false
        }

        fn apply_event(&mut self, event: TuiAppEvent, effects: &mut TuiAppEffects) {
            if let TuiAppEvent::RequestFailed { error, .. } = event {
                effects.push(TuiAppEffect::Notice {
                    level: TuiAppNoticeLevel::Error,
                    title: Some("Fixture".to_string()),
                    message: error,
                });
            }
        }

        fn actions(&self) -> Vec<TuiAppAction> {
            vec![TuiAppAction {
                id: "fixture.read".to_string(),
                label: "Read fixture".to_string(),
                description: "Issue a generic app request".to_string(),
                domain: "fixture".to_string(),
                risk: "low".to_string(),
                requires_confirmation: false,
                enabled: true,
                unavailable_reason: None,
            }]
        }

        fn dispatch_action(&mut self, action_id: &str, effects: &mut TuiAppEffects) -> bool {
            if action_id == "fixture.read" {
                effects.push(TuiAppEffect::Request {
                    request_id: action_id.to_string(),
                    method: "GET".to_string(),
                    path: "/api/apps/fixture/ping".to_string(),
                    body: None,
                });
                return true;
            }
            false
        }

        fn handle_command(&mut self, command: &str, effects: &mut TuiAppEffects) -> bool {
            if command == "/fixture read" {
                return self.dispatch_action("fixture.read", effects);
            }
            false
        }
    }

    #[test]
    fn external_tui_panel_factory_exercises_the_complete_generic_host_seam() {
        let contribution = TuiAppSurfaceContribution::new(
            AppId::parse("fixture").expect("fixture app id"),
            TuiAppContribution {
                panel_id: "fixture.panel".to_string(),
                title: "Fixture".to_string(),
                actions: vec!["fixture.read".to_string()],
            },
            Arc::new(|| Box::new(FixturePanel) as Box<dyn TuiAppPanel>),
        );
        contribution.validate().expect("valid contribution");

        let mut panel = contribution.create_panel();
        assert_eq!(panel.panel_id(), "fixture.panel");
        assert_eq!(panel.actions().len(), 1);
        let mut effects = TuiAppEffects::default();
        assert!(panel.dispatch_action("fixture.read", &mut effects));
        assert!(matches!(
            effects.take().as_slice(),
            [TuiAppEffect::Request { .. }]
        ));
        assert!(panel.handle_command("/fixture read", &mut effects));
        assert!(matches!(
            effects.take().as_slice(),
            [TuiAppEffect::Request { .. }]
        ));
        panel.apply_event(
            TuiAppEvent::RequestFailed {
                request_id: "fixture.read".to_string(),
                error: "offline".to_string(),
            },
            &mut effects,
        );
        assert!(matches!(
            effects.take().as_slice(),
            [TuiAppEffect::Notice { .. }]
        ));

        use ratatui::{backend::TestBackend, Terminal};
        let backend = TestBackend::new(24, 4);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| {
                let area = frame.area();
                panel.render(
                    frame,
                    area,
                    TuiAppRenderContext {
                        theme: TuiAppTheme::dark(),
                        focused: true,
                    },
                );
            })
            .expect("render external panel");
    }
}
