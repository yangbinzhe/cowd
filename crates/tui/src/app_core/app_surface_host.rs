//! Gateway-catalogued declarative APP surfaces.
//!
//! The terminal links no APP implementation. It projects the Gateway catalog,
//! opens versioned view documents, reduces actions and stream frames, and
//! renders every APP through the shared safe renderer.

use std::collections::{BTreeMap, BTreeSet};

use cowd_app_protocol::{
    app_operation_catalog_digest_v1, AppActionV1, AppActivationPolicyV1, AppCatalogEntryV1,
    AppCatalogV1, AppCompatibilityStatusV1, AppLifecycleStateV1, AppManifestV1, AppStreamFrameV1,
    AppTuiViewActionResponseV1, AppTuiViewDescriptorV1, AppTuiViewOpenRequestV1,
    AppTuiViewOpenResponseV1, AppTuiViewStreamRequestV1, AppTuiViewUpdateV1, AppViewDocumentV1,
    AppViewPatchV1, AppViewSubscriptionV1, OperationDescriptorV1, ProtocolValidate,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::app_core::app_view::{
    render_app_view, AppSubscriptionStatus, AppViewInputResult, AppViewState, AppViewStreamState,
};

pub const DEFAULT_APP_VIEW_ID: &str = "main";
const MAXIMUM_NAVIGATION_DEPTH: usize = 64;
const MAXIMUM_STREAM_RECONNECTS: u8 = 5;

#[derive(Debug, Clone, PartialEq)]
pub enum AppSurfaceCommand {
    LoadDetail {
        request_id: u64,
        app_id: String,
    },
    Open {
        request_id: u64,
        app_id: String,
        view_id: String,
        operation_id: String,
        request: AppTuiViewOpenRequestV1,
    },
    Action {
        request_id: u64,
        app_id: String,
        view_id: String,
        operation_id: String,
        action: AppActionV1,
    },
    StreamStart {
        app_id: String,
        view_id: String,
        operation_id: String,
        request: AppTuiViewStreamRequestV1,
    },
    StreamCancel {
        app_id: String,
        view_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AppSurfaceRequestKind {
    Open,
    Action,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AppSurfaceEvent {
    DetailLoaded {
        request_id: u64,
        entry: AppCatalogEntryV1,
        manifest: AppManifestV1,
        operations: Vec<OperationDescriptorV1>,
    },
    DetailFailed {
        request_id: u64,
        app_id: String,
        error: String,
    },
    Response {
        request_id: u64,
        app_id: String,
        view_id: String,
        kind: AppSurfaceRequestKind,
        status: u16,
        body: Value,
    },
    RequestFailed {
        request_id: u64,
        app_id: String,
        view_id: String,
        kind: AppSurfaceRequestKind,
        status: Option<u16>,
        body: Option<Value>,
        error: String,
    },
    StreamFrame {
        app_id: String,
        view_id: String,
        frame: AppStreamFrameV1,
    },
    StreamDisconnected {
        app_id: String,
        view_id: String,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostedAppAction {
    pub app_id: String,
    pub view_id: String,
    pub action_id: String,
    pub label: String,
    pub enabled: bool,
    pub requires_confirmation: bool,
}

#[derive(Debug, Clone)]
struct AppViewSession {
    state: AppViewState,
    streams: AppViewStreamState,
    subscriptions: BTreeMap<String, AppViewSubscriptionV1>,
    active_subscriptions: BTreeSet<String>,
    stream_active: bool,
    stream_reconnects: u8,
    stream_cursor: Option<String>,
}

impl AppViewSession {
    fn new(document: AppViewDocumentV1) -> Result<Self, String> {
        let state = AppViewState::new(document).map_err(|error| error.to_string())?;
        let streams = AppViewStreamState::from_document(state.document())
            .map_err(|error| error.to_string())?;
        let subscriptions = subscription_map(state.document());
        let stream_cursor = common_subscription_cursor(subscriptions.values())?;
        Ok(Self {
            state,
            streams,
            subscriptions,
            active_subscriptions: BTreeSet::new(),
            stream_active: false,
            stream_reconnects: 0,
            stream_cursor,
        })
    }

    fn replace_document(&mut self, document: AppViewDocumentV1) -> Result<bool, String> {
        self.state
            .replace_document(document)
            .map_err(|error| error.to_string())?;
        self.reconcile_subscription_contract()
    }

    fn apply_patch(&mut self, patch: &AppViewPatchV1) -> Result<bool, String> {
        self.state
            .apply_patch(patch)
            .map_err(|error| error.to_string())?;
        self.reconcile_subscription_contract()
    }

    fn reconcile_subscription_contract(&mut self) -> Result<bool, String> {
        let next = subscription_map(self.state.document());
        if next == self.subscriptions {
            return Ok(false);
        }
        self.streams = AppViewStreamState::from_document(self.state.document())
            .map_err(|error| error.to_string())?;
        self.subscriptions = next;
        self.stream_reconnects = 0;
        self.stream_cursor = common_subscription_cursor(self.subscriptions.values())?;
        Ok(true)
    }
}

#[derive(Debug, Clone)]
struct CataloguedApp {
    entry: AppCatalogEntryV1,
    signed_views: BTreeMap<String, AppTuiViewDescriptorV1>,
    detail_loading: Option<u64>,
    views: BTreeMap<String, AppViewSession>,
    navigation: Vec<String>,
    loading: Option<(String, u64)>,
    actions: BTreeMap<u64, (String, String)>,
    error: Option<String>,
}

impl CataloguedApp {
    fn available(&self) -> bool {
        self.entry.compatibility.status == AppCompatibilityStatusV1::Compatible
            && match self.entry.activation {
                AppActivationPolicyV1::Lazy => matches!(
                    self.entry.lifecycle.state,
                    AppLifecycleStateV1::Mounted
                        | AppLifecycleStateV1::Ready
                        | AppLifecycleStateV1::Idle
                        | AppLifecycleStateV1::Stopped
                ),
                AppActivationPolicyV1::Resident => matches!(
                    self.entry.lifecycle.state,
                    AppLifecycleStateV1::Ready | AppLifecycleStateV1::Idle
                ),
            }
    }

    fn active_view_id(&self) -> Option<&str> {
        self.navigation.last().map(String::as_str)
    }
}

/// Pure, application-neutral reducer for dynamic terminal surfaces.
#[derive(Debug, Clone, Default)]
pub struct DeclarativeAppHost {
    apps: BTreeMap<String, CataloguedApp>,
    active_app_id: Option<String>,
    pending: Vec<AppSurfaceCommand>,
    notices: Vec<String>,
    next_request_id: u64,
}

impl DeclarativeAppHost {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn install_catalog(&mut self, catalog: AppCatalogV1) -> Result<(), String> {
        catalog.validate().map_err(|error| error.to_string())?;
        self.cancel_all_streams();
        let cancellations = std::mem::take(&mut self.pending)
            .into_iter()
            .filter(|command| matches!(command, AppSurfaceCommand::StreamCancel { .. }))
            .collect();
        self.apps.clear();
        self.pending = cancellations;
        for entry in catalog.apps {
            let app_id = entry.app_id.0.clone();
            self.apps.insert(
                app_id,
                CataloguedApp {
                    entry,
                    signed_views: BTreeMap::new(),
                    detail_loading: None,
                    views: BTreeMap::new(),
                    navigation: Vec::new(),
                    loading: None,
                    actions: BTreeMap::new(),
                    error: None,
                },
            );
        }
        self.active_app_id = self
            .apps
            .iter()
            .find_map(|(id, app)| app.available().then(|| id.clone()))
            .or_else(|| self.apps.keys().next().cloned());
        Ok(())
    }

    /// Admit one live APP surface only after its worker catalog is bound to the
    /// signed manifest and to the matching Gateway catalog generation.
    pub fn install_contract(
        &mut self,
        entry: &AppCatalogEntryV1,
        manifest: &AppManifestV1,
        operations: &[OperationDescriptorV1],
    ) -> Result<(), String> {
        entry.validate().map_err(|error| error.to_string())?;
        manifest.validate().map_err(|error| error.to_string())?;
        let operation_catalog_digest =
            app_operation_catalog_digest_v1(&manifest.app_id, operations)
                .map_err(|error| error.to_string())?;
        if entry.app_id != manifest.app_id
            || entry.artifact_version != manifest.artifact_version
            || operation_catalog_digest != manifest.operation_catalog_digest
        {
            return Err("APP detail identity, version, or operation catalog mismatch".to_owned());
        }
        let app_id = entry.app_id.0.clone();
        let app = self
            .apps
            .get_mut(&app_id)
            .ok_or_else(|| "APP detail is absent from the installed catalog".to_owned())?;
        if &app.entry != entry {
            return Err("APP detail catalog entry does not match the installed catalog".to_owned());
        }
        let presentation = manifest
            .presentation
            .as_ref()
            .ok_or_else(|| "APP has no signed presentation contract".to_owned())?;
        app.signed_views = presentation
            .tui_views
            .iter()
            .cloned()
            .map(|descriptor| (descriptor.view_id.clone(), descriptor))
            .collect();
        app.detail_loading = None;
        app.error = None;
        if self.active_app_id.as_deref() == Some(&app_id) && app.available() {
            let first_view = app
                .signed_views
                .contains_key(DEFAULT_APP_VIEW_ID)
                .then(|| DEFAULT_APP_VIEW_ID.to_owned())
                .or_else(|| app.signed_views.keys().next().cloned());
            if let Some(view_id) = first_view {
                self.open_view(&app_id, &view_id, false);
            }
        }
        Ok(())
    }

    pub fn reject_contract(&mut self, app_id: &str, error: String) {
        if self.apps.contains_key(app_id) {
            self.isolate(app_id, error);
        }
    }

    /// Request admission for the selected APP. Catalog installation remains a
    /// static projection and never activates a lazy worker.
    pub fn activate_selected_contract(&mut self) -> bool {
        let Some(app_id) = self.active_app_id.clone() else {
            return false;
        };
        self.ensure_contract(&app_id)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.apps.is_empty()
    }

    #[must_use]
    pub fn app_ids(&self) -> Vec<String> {
        self.apps.keys().cloned().collect()
    }

    #[must_use]
    pub fn catalog_entry(&self, app_id: &str) -> Option<AppCatalogEntryV1> {
        self.apps.get(app_id).map(|app| app.entry.clone())
    }

    #[must_use]
    pub fn active_app_id(&self) -> Option<&str> {
        self.active_app_id.as_deref()
    }

    #[must_use]
    pub fn active_view_id(&self) -> Option<&str> {
        self.active_app().and_then(CataloguedApp::active_view_id)
    }

    pub fn render(&self, frame: &mut Frame<'_>, area: Rect, focused: bool) {
        let Some(app) = self.active_app() else {
            render_message(
                frame,
                area,
                "Applications",
                "No applications are admitted by the connected Gateway.",
                Color::DarkGray,
            );
            return;
        };
        if !app.available() {
            let reason = app
                .entry
                .lifecycle
                .reason_code
                .as_deref()
                .unwrap_or("no diagnostic supplied");
            render_message(
                frame,
                area,
                &app.entry.display_name,
                &format!(
                    "APP unavailable\nstate: {:?}\ncompatibility: {:?}\nreason: {reason}\nCore TUI remains fully operational.",
                    app.entry.lifecycle.state, app.entry.compatibility.status
                ),
                Color::Red,
            );
            return;
        }
        if let Some(error) = &app.error {
            render_message(
                frame,
                area,
                &app.entry.display_name,
                &format!(
                    "APP surface isolated after an invalid response:\n{error}\n\nPress r to retry."
                ),
                Color::Red,
            );
            return;
        }
        let Some(view_id) = app.active_view_id() else {
            render_message(
                frame,
                area,
                &app.entry.display_name,
                "Opening declarative terminal surface…",
                Color::Yellow,
            );
            return;
        };
        if let Some(view) = app.views.get(view_id) {
            render_app_view(frame, area, &view.state);
        } else {
            render_message(
                frame,
                area,
                &format!("{} · {view_id}", app.entry.display_name),
                if focused {
                    "Loading view from Gateway…"
                } else {
                    "APP view is loading."
                },
                Color::Yellow,
            );
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Tab | KeyCode::BackTab)
        {
            return false;
        }
        if key.code == KeyCode::Char('r')
            && self.active_app().is_some_and(|app| app.error.is_some())
        {
            let Some(app_id) = self.active_app_id.clone() else {
                return false;
            };
            let view_id = self
                .active_view_id()
                .unwrap_or(DEFAULT_APP_VIEW_ID)
                .to_owned();
            self.open_view(&app_id, &view_id, false);
            return true;
        }
        let Some((app_id, view_id)) = self.active_identity() else {
            return false;
        };
        let result = {
            let Some(view) = self
                .apps
                .get_mut(&app_id)
                .and_then(|app| app.views.get_mut(&view_id))
            else {
                return false;
            };
            view.state.handle_key(key)
        };
        match result {
            Ok(AppViewInputResult::Ignored) if key.code == KeyCode::Esc => {
                self.navigate_back(&app_id)
            }
            Ok(AppViewInputResult::Ignored) => false,
            Ok(AppViewInputResult::StateChanged) => true,
            Ok(AppViewInputResult::ConfirmationRequired { action_id }) => {
                self.notices.push(format!(
                    "Press Enter again to confirm APP action {action_id}; Esc cancels"
                ));
                true
            }
            Ok(AppViewInputResult::Action(action)) => {
                self.queue_action(action);
                true
            }
            Err(error) => {
                self.isolate(&app_id, error.to_string());
                true
            }
        }
    }

    pub fn cycle_app(&mut self, reverse: bool) -> bool {
        let ids = self.app_ids();
        if ids.is_empty() {
            return false;
        }
        let current = self
            .active_app_id
            .as_ref()
            .and_then(|id| ids.iter().position(|candidate| candidate == id))
            .unwrap_or(0);
        let next = if reverse {
            current.checked_sub(1).unwrap_or(ids.len() - 1)
        } else {
            (current + 1) % ids.len()
        };
        self.select_app(&ids[next])
    }

    pub fn select_app(&mut self, app_id: &str) -> bool {
        if !self.apps.contains_key(app_id) {
            return false;
        }
        if self.active_app_id.as_deref() == Some(app_id) {
            return self.ensure_contract(app_id);
        }
        if let Some(previous) = self.active_app_id.clone() {
            self.cancel_app_streams(&previous);
        }
        self.active_app_id = Some(app_id.to_owned());
        let (available, target) = self
            .apps
            .get(app_id)
            .map(|app| {
                (
                    app.available(),
                    app.active_view_id()
                        .map(str::to_owned)
                        .or_else(|| {
                            app.signed_views
                                .contains_key(DEFAULT_APP_VIEW_ID)
                                .then(|| DEFAULT_APP_VIEW_ID.to_owned())
                        })
                        .or_else(|| app.signed_views.keys().next().cloned())
                        .unwrap_or_else(|| DEFAULT_APP_VIEW_ID.to_owned()),
                )
            })
            .unwrap_or((false, DEFAULT_APP_VIEW_ID.to_owned()));
        if available {
            if self
                .apps
                .get(app_id)
                .is_some_and(|app| app.signed_views.is_empty())
            {
                return self.ensure_contract(app_id);
            }
            if self
                .apps
                .get(app_id)
                .is_some_and(|app| app.views.contains_key(&target))
            {
                self.start_view_streams(app_id, &target);
            } else {
                self.open_view(app_id, &target, false);
            }
        }
        true
    }

    pub fn open_view(&mut self, app_id: &str, view_id: &str, push_history: bool) -> bool {
        if !valid_identifier(app_id) || !valid_view_id(view_id) {
            return false;
        }
        let Some(app) = self.apps.get(app_id) else {
            return false;
        };
        if !app.available() {
            return false;
        }
        if app.signed_views.is_empty() {
            return self.ensure_contract(app_id);
        }
        if !app.signed_views.contains_key(view_id) {
            return false;
        }
        if self.active_app_id.as_deref() != Some(app_id) {
            if let Some(previous) = self.active_app_id.clone() {
                self.cancel_app_streams(&previous);
            }
            self.active_app_id = Some(app_id.to_owned());
        }
        self.cancel_app_streams(app_id);
        let request_id = self.allocate_request_id();
        let Some(app) = self.apps.get_mut(app_id) else {
            return false;
        };
        if push_history && app.navigation.last().map(String::as_str) != Some(view_id) {
            if app.navigation.len() >= MAXIMUM_NAVIGATION_DEPTH {
                app.navigation.remove(0);
            }
            app.navigation.push(view_id.to_owned());
        } else if app.navigation.is_empty() {
            app.navigation.push(view_id.to_owned());
        } else if !push_history {
            if let Some(current) = app.navigation.last_mut() {
                *current = view_id.to_owned();
            }
        }
        app.loading = Some((view_id.to_owned(), request_id));
        app.error = None;
        self.pending.push(AppSurfaceCommand::Open {
            request_id,
            app_id: app_id.to_owned(),
            view_id: view_id.to_owned(),
            operation_id: app.signed_views[view_id].open_operation_id.clone(),
            request: AppTuiViewOpenRequestV1 {
                schema_version: 1,
                view_id: view_id.to_owned(),
            },
        });
        true
    }

    pub fn dispatch_action(&mut self, app_id: &str, view_id: &str, action_id: &str) -> bool {
        if !self.select_app(app_id) {
            return false;
        }
        if self.active_view_id() != Some(view_id) {
            if !self.open_view(app_id, view_id, true) {
                return false;
            }
            self.notices.push(format!(
                "Opened {app_id}/{view_id}; invoke {action_id} after the view is ready"
            ));
            return true;
        }
        let action = self
            .apps
            .get(app_id)
            .and_then(|app| app.views.get(view_id))
            .and_then(|view| {
                view.state
                    .document()
                    .actions
                    .iter()
                    .find(|action| action.action_id == action_id && action.enabled)
            });
        let Some(action) = action else {
            return false;
        };
        if action.requires_confirmation {
            self.notices.push(format!(
                "Focus {} and press Enter twice to confirm {}",
                action.component_id, action.label
            ));
            return true;
        }
        let action = AppActionV1 {
            schema_version: 1,
            app_id: self.apps[app_id].entry.app_id.clone(),
            view_id: view_id.to_owned(),
            document_revision: self.apps[app_id].views[view_id]
                .state
                .document()
                .revision
                .clone(),
            component_id: action.component_id.clone(),
            action_id: action.action_id.clone(),
            selection: Value::Null,
            form: Value::Null,
            confirmed: false,
        };
        self.queue_action(action);
        true
    }

    pub fn apply_event(&mut self, event: AppSurfaceEvent) {
        match event {
            AppSurfaceEvent::DetailLoaded {
                request_id,
                entry,
                manifest,
                operations,
            } => {
                let app_id = entry.app_id.0.clone();
                if !self
                    .apps
                    .get(&app_id)
                    .is_some_and(|app| app.detail_loading == Some(request_id))
                {
                    return;
                }
                if let Err(error) = self.install_contract(&entry, &manifest, &operations) {
                    self.isolate(&app_id, error);
                }
            }
            AppSurfaceEvent::DetailFailed {
                request_id,
                app_id,
                error,
            } => {
                if self
                    .apps
                    .get(&app_id)
                    .is_some_and(|app| app.detail_loading == Some(request_id))
                {
                    self.isolate(&app_id, error);
                }
            }
            AppSurfaceEvent::Response {
                request_id,
                app_id,
                view_id,
                kind,
                status,
                body,
            } => {
                if !(200..300).contains(&status) {
                    self.apply_failure(
                        request_id,
                        &app_id,
                        &view_id,
                        kind,
                        format!("Gateway returned HTTP {status}"),
                    );
                    return;
                }
                match kind {
                    AppSurfaceRequestKind::Open => {
                        if !self.open_response_is_current(&app_id, &view_id, request_id) {
                            return;
                        }
                        match decode_open_response(&body, &app_id, &view_id)
                            .and_then(AppViewSession::new)
                        {
                            Ok(view) => {
                                if let Some(app) = self.apps.get_mut(&app_id) {
                                    app.views.insert(view_id.clone(), view);
                                    app.loading = None;
                                    app.error = None;
                                }
                                self.start_view_streams(&app_id, &view_id);
                            }
                            Err(error) => self.isolate(&app_id, error),
                        }
                    }
                    AppSurfaceRequestKind::Action => {
                        let pending = self
                            .apps
                            .get_mut(&app_id)
                            .and_then(|app| app.actions.remove(&request_id));
                        let Some((pending_view, base_revision)) = pending else {
                            return;
                        };
                        if pending_view != view_id
                            || self
                                .apps
                                .get(&app_id)
                                .and_then(|app| app.views.get(&view_id))
                                .is_none_or(|view| view.state.document().revision != base_revision)
                        {
                            return;
                        }
                        if let Err(error) = self.reduce_action_response(&app_id, &view_id, &body) {
                            self.isolate(&app_id, error);
                        }
                    }
                }
            }
            AppSurfaceEvent::RequestFailed {
                request_id,
                app_id,
                view_id,
                kind,
                status,
                body,
                error,
            } => {
                let detail = body
                    .as_ref()
                    .and_then(|value| value.pointer("/error/message"))
                    .and_then(Value::as_str)
                    .unwrap_or(&error);
                self.apply_failure(
                    request_id,
                    &app_id,
                    &view_id,
                    kind,
                    status.map_or_else(
                        || detail.to_owned(),
                        |code| format!("HTTP {code}: {detail}"),
                    ),
                );
            }
            AppSurfaceEvent::StreamFrame {
                app_id,
                view_id,
                frame,
            } => self.apply_stream_frame(&app_id, &view_id, frame),
            AppSurfaceEvent::StreamDisconnected {
                app_id,
                view_id,
                error,
            } => self.reconnect_stream(&app_id, &view_id, &error),
        }
    }

    #[must_use]
    pub fn actions(&self) -> Vec<HostedAppAction> {
        self.apps
            .iter()
            .flat_map(|(app_id, app)| {
                app.views.iter().flat_map(move |(view_id, view)| {
                    view.state
                        .document()
                        .actions
                        .iter()
                        .map(move |action| HostedAppAction {
                            app_id: app_id.clone(),
                            view_id: view_id.clone(),
                            action_id: action.action_id.clone(),
                            label: action.label.clone(),
                            enabled: action.enabled,
                            requires_confirmation: action.requires_confirmation,
                        })
                })
            })
            .collect()
    }

    #[must_use]
    pub fn take_commands(&mut self) -> Vec<AppSurfaceCommand> {
        std::mem::take(&mut self.pending)
    }

    #[must_use]
    pub fn take_notices(&mut self) -> Vec<String> {
        std::mem::take(&mut self.notices)
    }

    fn active_app(&self) -> Option<&CataloguedApp> {
        self.active_app_id.as_ref().and_then(|id| self.apps.get(id))
    }

    fn active_identity(&self) -> Option<(String, String)> {
        let app_id = self.active_app_id.clone()?;
        let view_id = self.apps.get(&app_id)?.active_view_id()?.to_owned();
        Some((app_id, view_id))
    }

    fn allocate_request_id(&mut self) -> u64 {
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        self.next_request_id
    }

    fn ensure_contract(&mut self, app_id: &str) -> bool {
        let Some(app) = self.apps.get(app_id) else {
            return false;
        };
        if !app.available() {
            return false;
        }
        if !app.signed_views.is_empty() || app.detail_loading.is_some() {
            return true;
        }
        let request_id = self.allocate_request_id();
        if let Some(app) = self.apps.get_mut(app_id) {
            app.detail_loading = Some(request_id);
        }
        self.pending.push(AppSurfaceCommand::LoadDetail {
            request_id,
            app_id: app_id.to_owned(),
        });
        true
    }

    fn queue_action(&mut self, action: AppActionV1) {
        let request_id = self.allocate_request_id();
        let Some(app) = self.apps.get_mut(&action.app_id.0) else {
            return;
        };
        let Some(descriptor) = app.signed_views.get(&action.view_id) else {
            return;
        };
        app.actions.insert(
            request_id,
            (action.view_id.clone(), action.document_revision.clone()),
        );
        self.pending.push(AppSurfaceCommand::Action {
            request_id,
            app_id: action.app_id.0.clone(),
            view_id: action.view_id.clone(),
            operation_id: descriptor.action_operation_id.clone(),
            action,
        });
    }

    fn open_response_is_current(&self, app_id: &str, view_id: &str, request_id: u64) -> bool {
        self.apps
            .get(app_id)
            .and_then(|app| app.loading.as_ref())
            .is_some_and(|(loading_view, loading_request)| {
                loading_view == view_id && *loading_request == request_id
            })
    }

    fn apply_failure(
        &mut self,
        request_id: u64,
        app_id: &str,
        view_id: &str,
        kind: AppSurfaceRequestKind,
        error: String,
    ) {
        if kind == AppSurfaceRequestKind::Open
            && !self.open_response_is_current(app_id, view_id, request_id)
        {
            return;
        }
        if kind == AppSurfaceRequestKind::Action
            && self
                .apps
                .get_mut(app_id)
                .and_then(|app| app.actions.remove(&request_id))
                .is_none()
        {
            return;
        }
        self.isolate(app_id, error);
    }

    fn reduce_action_response(
        &mut self,
        app_id: &str,
        view_id: &str,
        body: &Value,
    ) -> Result<(), String> {
        let response: AppTuiViewActionResponseV1 = serde_json::from_value(body.clone())
            .map_err(|error| format!("invalid APP action response: {error}"))?;
        response.validate().map_err(|error| error.to_string())?;
        if response.view_id != view_id {
            return Err("APP action response targets a different signed view".to_owned());
        }
        let changed = {
            let view = self
                .apps
                .get_mut(app_id)
                .and_then(|app| app.views.get_mut(view_id))
                .ok_or_else(|| "APP response targets an unopened view".to_owned())?;
            match response.update {
                AppTuiViewUpdateV1::Document { document } => {
                    if document.app_id.0 != app_id {
                        return Err("APP action document identity mismatch".to_owned());
                    }
                    view.replace_document(*document)?
                }
                AppTuiViewUpdateV1::Patch { patch } => {
                    if patch.app_id.0 != app_id {
                        return Err("APP action patch identity mismatch".to_owned());
                    }
                    view.apply_patch(&patch)?
                }
                AppTuiViewUpdateV1::NoChange => {
                    if response.revision != view.state.document().revision {
                        return Err(
                            "APP no-change response changed the document revision".to_owned()
                        );
                    }
                    false
                }
            }
        };
        if changed {
            self.restart_view_streams(app_id, view_id);
        }
        Ok(())
    }

    fn apply_stream_frame(&mut self, app_id: &str, view_id: &str, frame: AppStreamFrameV1) {
        let subscription_id = frame.subscription_id().to_owned();
        let payload = match &frame {
            AppStreamFrameV1::Data { payload, .. } => Some(payload.clone()),
            _ => None,
        };
        let result = self
            .apps
            .get_mut(app_id)
            .and_then(|app| app.views.get_mut(view_id))
            .ok_or_else(|| "stream targets an unopened APP view".to_owned())
            .and_then(|view| {
                view.streams
                    .apply_frame(&frame)
                    .map_err(|error| error.to_string())?;
                if matches!(frame, AppStreamFrameV1::Checkpoint { .. }) {
                    view.stream_cursor = view
                        .streams
                        .subscription(&subscription_id)
                        .and_then(|state| state.cursor.clone());
                }
                if matches!(
                    view.streams
                        .subscription(&subscription_id)
                        .map(|state| &state.status),
                    Some(AppSubscriptionStatus::Live)
                ) {
                    view.stream_reconnects = 0;
                }
                Ok(())
            });
        if let Err(error) = result {
            self.reconnect_stream(app_id, view_id, &error);
            return;
        }
        if let AppStreamFrameV1::Error { error, .. } = &frame {
            self.isolate(
                app_id,
                format!("stream {subscription_id} failed: {}", error.message),
            );
            return;
        }
        if let Some(payload) = payload {
            let patch: AppViewPatchV1 = match serde_json::from_value(payload) {
                Ok(patch) => patch,
                Err(error) => {
                    self.isolate(app_id, format!("invalid APP stream patch: {error}"));
                    return;
                }
            };
            if let Err(error) = patch.validate().map_err(|error| error.to_string()) {
                self.isolate(app_id, error);
                return;
            }
            let current_revision = self
                .apps
                .get(app_id)
                .and_then(|app| app.views.get(view_id))
                .map(|view| view.state.document().revision.clone());
            if patch.app_id.0 != app_id || patch.view_id != view_id {
                self.isolate(app_id, "APP stream patch identity mismatch".to_owned());
            } else if current_revision.as_deref() != Some(&patch.base_revision) {
                self.notices.push(format!(
                    "APP {app_id}/{view_id} stream revision gap; reopening the signed view"
                ));
                self.open_view(app_id, view_id, false);
            } else if let Some(view) = self
                .apps
                .get_mut(app_id)
                .and_then(|app| app.views.get_mut(view_id))
            {
                if let Err(error) = view.apply_patch(&patch) {
                    self.isolate(app_id, error);
                }
            }
        }
    }

    fn reconnect_stream(&mut self, app_id: &str, view_id: &str, error: &str) {
        let reconnect = self
            .apps
            .get_mut(app_id)
            .and_then(|app| app.views.get_mut(view_id))
            .and_then(|view| {
                view.stream_active = false;
                view.stream_reconnects = view.stream_reconnects.saturating_add(1);
                if view.stream_reconnects > MAXIMUM_STREAM_RECONNECTS {
                    return None;
                }
                for subscription_id in view.subscriptions.keys() {
                    let _ = view.streams.reconnect(subscription_id);
                }
                Some((
                    view.state.document().revision.clone(),
                    view.stream_cursor.clone(),
                ))
            });
        if let Some((document_revision, cursor)) = reconnect {
            let Some(operation_id) = self
                .apps
                .get(app_id)
                .and_then(|app| app.signed_views.get(view_id))
                .map(|descriptor| descriptor.stream_operation_id.clone())
            else {
                self.isolate(app_id, "signed stream descriptor is unavailable".to_owned());
                return;
            };
            self.pending.push(AppSurfaceCommand::StreamStart {
                app_id: app_id.to_owned(),
                view_id: view_id.to_owned(),
                operation_id,
                request: AppTuiViewStreamRequestV1 {
                    schema_version: 1,
                    view_id: view_id.to_owned(),
                    document_revision,
                    cursor,
                },
            });
        } else if self.apps.contains_key(app_id) {
            self.isolate(app_id, format!("stream exceeded reconnect budget: {error}"));
        }
    }

    fn start_view_streams(&mut self, app_id: &str, view_id: &str) {
        let operation_id = self
            .apps
            .get(app_id)
            .and_then(|app| app.signed_views.get(view_id))
            .map(|descriptor| descriptor.stream_operation_id.clone());
        let start = self
            .apps
            .get_mut(app_id)
            .and_then(|app| app.views.get_mut(view_id))
            .and_then(|view| {
                if view.subscriptions.is_empty() || view.stream_active {
                    return None;
                }
                view.stream_active = true;
                view.active_subscriptions = view.subscriptions.keys().cloned().collect();
                Some((
                    view.state.document().revision.clone(),
                    view.stream_cursor.clone(),
                ))
            });
        if let (Some(operation_id), Some((document_revision, cursor))) = (operation_id, start) {
            self.pending.push(AppSurfaceCommand::StreamStart {
                app_id: app_id.to_owned(),
                view_id: view_id.to_owned(),
                operation_id,
                request: AppTuiViewStreamRequestV1 {
                    schema_version: 1,
                    view_id: view_id.to_owned(),
                    document_revision,
                    cursor,
                },
            });
        }
    }

    fn restart_view_streams(&mut self, app_id: &str, view_id: &str) {
        self.cancel_view_streams(app_id, view_id);
        self.start_view_streams(app_id, view_id);
    }

    fn cancel_view_streams(&mut self, app_id: &str, view_id: &str) {
        let cancel = self
            .apps
            .get_mut(app_id)
            .and_then(|app| app.views.get_mut(view_id))
            .and_then(|view| {
                view.active_subscriptions.clear();
                std::mem::replace(&mut view.stream_active, false).then(|| {
                    AppSurfaceCommand::StreamCancel {
                        app_id: app_id.to_owned(),
                        view_id: view_id.to_owned(),
                    }
                })
            });
        if let Some(cancel) = cancel {
            self.pending.push(cancel);
        }
    }

    fn cancel_app_streams(&mut self, app_id: &str) {
        let view_ids = self
            .apps
            .get(app_id)
            .map(|app| app.views.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for view_id in view_ids {
            self.cancel_view_streams(app_id, &view_id);
        }
    }

    fn cancel_all_streams(&mut self) {
        let app_ids = self.app_ids();
        for app_id in app_ids {
            self.cancel_app_streams(&app_id);
        }
    }

    fn navigate_back(&mut self, app_id: &str) -> bool {
        let previous = self.apps.get_mut(app_id).and_then(|app| {
            (app.navigation.len() > 1).then(|| {
                app.navigation.pop();
                app.navigation.last().cloned()
            })?
        });
        let Some(previous) = previous else {
            return false;
        };
        self.cancel_app_streams(app_id);
        if self
            .apps
            .get(app_id)
            .is_some_and(|app| app.views.contains_key(&previous))
        {
            self.start_view_streams(app_id, &previous);
        } else {
            self.open_view(app_id, &previous, false);
        }
        true
    }

    fn isolate(&mut self, app_id: &str, error: String) {
        self.cancel_app_streams(app_id);
        if let Some(app) = self.apps.get_mut(app_id) {
            app.loading = None;
            app.error = Some(error.clone());
        }
        self.notices
            .push(format!("APP {app_id} surface isolated: {error}"));
    }
}

fn subscription_map(document: &AppViewDocumentV1) -> BTreeMap<String, AppViewSubscriptionV1> {
    document
        .subscriptions
        .iter()
        .map(|descriptor| (descriptor.subscription_id.clone(), descriptor.clone()))
        .collect()
}

fn decode_open_response(
    body: &Value,
    app_id: &str,
    view_id: &str,
) -> Result<AppViewDocumentV1, String> {
    let response: AppTuiViewOpenResponseV1 = serde_json::from_value(body.clone())
        .map_err(|error| format!("invalid APP open response: {error}"))?;
    response.validate().map_err(|error| error.to_string())?;
    if response.document.app_id.0 != app_id || response.document.view_id != view_id {
        return Err("APP open response identity does not match the signed view".to_owned());
    }
    Ok(response.document)
}

fn common_subscription_cursor<'a>(
    subscriptions: impl Iterator<Item = &'a AppViewSubscriptionV1>,
) -> Result<Option<String>, String> {
    let mut cursor: Option<Option<&str>> = None;
    for subscription in subscriptions {
        let candidate = subscription.cursor.as_deref();
        if cursor.is_some_and(|current| current != candidate) {
            return Err(
                "APP view subscriptions require different replay cursors; the v1 stream contract has one connection cursor"
                    .to_owned(),
            );
        }
        cursor = Some(candidate);
    }
    Ok(cursor.flatten().map(str::to_owned))
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_view_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn render_message(frame: &mut Frame<'_>, area: Rect, title: &str, message: &str, color: Color) {
    frame.render_widget(
        Paragraph::new(message.to_owned())
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title.to_owned())
                    .border_style(Style::default().fg(color).add_modifier(Modifier::BOLD)),
            )
            .wrap(Wrap { trim: false }),
        area,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const APP_ID: &str = "reference-app";

    fn catalog(entries: Value) -> AppCatalogV1 {
        serde_json::from_value(json!({
            "schema_version": 1,
            "protocol_revision": 1,
            "protocol_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "catalog_generation": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "apps": entries,
        }))
        .expect("catalog fixture")
    }

    fn entry(app_id: &str, state: &str, compatibility: &str) -> Value {
        json!({
            "app_id": app_id,
            "display_name": format!("{app_id} APP"),
            "artifact_version": "1.0.0",
            "generation": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "required": false,
            "activation": "resident",
            "lifecycle": {"state": state, "retryable": false},
            "compatibility": {
                "status": compatibility,
                "gateway_supported_minimum": 1,
                "gateway_supported_maximum": 1,
                "app_required_minimum": 1,
                "app_required_maximum": 1
            },
            "web_surface": {"available": false, "bridge_revision": 1},
            "effective_capabilities": [],
            "effective_authorization_profile": "default"
        })
    }

    fn document(app_id: &str, view_id: &str, revision: &str) -> Value {
        json!({
            "schema_version": 1,
            "app_id": app_id,
            "view_id": view_id,
            "revision": revision,
            "title": "Reference",
            "root": {
                "component_id": "actions",
                "kind": "action_bar",
                "accessibility_label": "Actions",
                "properties": {},
                "children": []
            },
            "actions": [{
                "action_id": "deploy",
                "component_id": "actions",
                "label": "Deploy",
                "enabled": true,
                "requires_confirmation": true
            }],
            "subscriptions": [{
                "subscription_id": "updates",
                "cursor": "cursor-1"
            }],
            "refresh_policy": {"mode": "subscription"}
        })
    }

    fn open_response(app_id: &str, view_id: &str, revision: &str) -> Value {
        json!({
            "schema_version": 1,
            "document": document(app_id, view_id, revision),
        })
    }

    fn signed_contract() -> (AppManifestV1, Vec<OperationDescriptorV1>) {
        let manifest = serde_json::from_str(include_str!(
            "../../../app-protocol/contracts/v1/golden/app-manifest.json"
        ))
        .expect("signed manifest fixture");
        let handshake: cowd_app_protocol::AppHandshakeV1 = serde_json::from_str(include_str!(
            "../../../app-protocol/contracts/v1/golden/handshake-success.json"
        ))
        .expect("live handshake fixture");
        (manifest, handshake.operations)
    }

    fn admitted_host() -> DeclarativeAppHost {
        let mut host = DeclarativeAppHost::empty();
        host.install_catalog(catalog(json!([entry(APP_ID, "ready", "compatible")])))
            .expect("catalog");
        assert!(
            host.take_commands().is_empty(),
            "catalog alone cannot open a view"
        );
        assert!(host.select_app(APP_ID));
        assert!(
            matches!(host.take_commands().as_slice(), [AppSurfaceCommand::LoadDetail { app_id, .. }] if app_id == APP_ID)
        );
        let (manifest, operations) = signed_contract();
        let catalog_entry = host.apps[APP_ID].entry.clone();
        host.install_contract(&catalog_entry, &manifest, &operations)
            .expect("signed live contract");
        host
    }

    #[test]
    fn catalog_zero_one_many_projects_without_linked_application_code() {
        let mut host = DeclarativeAppHost::empty();
        host.install_catalog(catalog(json!([])))
            .expect("empty catalog");
        assert!(host.is_empty());

        host.install_catalog(catalog(json!([entry("alpha", "ready", "compatible")])))
            .expect("one app");
        assert_eq!(host.app_ids(), vec!["alpha"]);
        assert!(host.take_commands().is_empty());
        assert!(host.select_app("alpha"));
        assert!(
            matches!(host.take_commands().as_slice(), [AppSurfaceCommand::LoadDetail { app_id, .. }] if app_id == "alpha")
        );

        host.install_catalog(catalog(json!([
            entry("beta", "ready", "compatible"),
            entry("alpha", "ready", "compatible")
        ])))
        .expect("many apps");
        assert_eq!(host.app_ids(), vec!["alpha", "beta"]);
        assert_eq!(host.active_app_id(), Some("alpha"));
        assert!(host.cycle_app(false));
        assert_eq!(host.active_app_id(), Some("beta"));
    }

    #[test]
    fn failed_circuit_and_incompatible_apps_are_visible_but_never_opened() {
        let mut host = DeclarativeAppHost::empty();
        host.install_catalog(catalog(json!([
            entry("failed", "failed", "compatible"),
            entry("circuit", "circuit_open", "compatible"),
            entry("old", "protocol_incompatible", "protocol_incompatible")
        ])))
        .expect("degraded catalog");
        assert_eq!(host.app_ids().len(), 3);
        assert!(host.take_commands().is_empty());
    }

    #[test]
    fn open_action_patch_stream_navigation_and_cancellation_share_one_reducer() {
        let mut host = admitted_host();
        let request_id = match host.take_commands().pop().expect("open") {
            AppSurfaceCommand::Open {
                request_id,
                operation_id,
                request,
                ..
            } => {
                assert_eq!(operation_id, "reference-app.tui.main.open");
                assert_eq!(request.view_id, "main");
                request_id
            }
            other => panic!("unexpected command: {other:?}"),
        };
        host.apply_event(AppSurfaceEvent::Response {
            request_id,
            app_id: APP_ID.into(),
            view_id: "main".into(),
            kind: AppSurfaceRequestKind::Open,
            status: 200,
            body: open_response(APP_ID, "main", "1"),
        });
        assert!(host.take_commands().iter().any(|command| matches!(command,
            AppSurfaceCommand::StreamStart { operation_id, request, .. }
                if operation_id == "reference-app.tui.main.stream"
                    && request.cursor.as_deref() == Some("cursor-1")
                    && request.document_revision == "1"
        )));

        assert!(host.handle_key(KeyEvent::new(
            KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE
        )));
        assert!(host.take_commands().is_empty(), "first Enter only confirms");
        assert!(host.handle_key(KeyEvent::new(
            KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE
        )));
        let action_request = match host.take_commands().pop().expect("confirmed action") {
            AppSurfaceCommand::Action {
                request_id,
                operation_id,
                action,
                ..
            } => {
                assert!(action.confirmed);
                assert_eq!(operation_id, "reference-app.tui.main.action");
                request_id
            }
            other => panic!("unexpected command: {other:?}"),
        };
        host.apply_event(AppSurfaceEvent::Response {
            request_id: action_request,
            app_id: APP_ID.into(),
            view_id: "main".into(),
            kind: AppSurfaceRequestKind::Action,
            status: 200,
            body: json!({
                "schema_version": 1,
                "view_id": "main",
                "revision": "2",
                "update": {
                    "kind": "patch",
                    "patch": {
                    "schema_version": 1,
                    "app_id": APP_ID,
                    "view_id": "main",
                    "base_revision": "1",
                    "revision": "2",
                    "operations": [{"op": "replace", "path": "/title", "value": "Updated"}]
                    }
                }
            }),
        });
        assert_eq!(
            host.apps[APP_ID].views["main"].state.document().revision,
            "2"
        );
        assert!(!host.open_view(APP_ID, "unsigned", true));
    }

    #[test]
    fn stale_open_and_stream_revision_faults_are_isolated_per_app() {
        let mut host = admitted_host();
        let stale = match host.take_commands().pop().expect("open") {
            AppSurfaceCommand::Open { request_id, .. } => request_id,
            other => panic!("unexpected: {other:?}"),
        };
        assert!(host.open_view(APP_ID, "main", false));
        host.apply_event(AppSurfaceEvent::Response {
            request_id: stale,
            app_id: APP_ID.into(),
            view_id: "main".into(),
            kind: AppSurfaceRequestKind::Open,
            status: 200,
            body: open_response(APP_ID, "main", "1"),
        });
        assert!(host.apps[APP_ID].views.is_empty(), "stale response ignored");
    }

    #[test]
    fn stream_frames_advance_revision_checkpoint_and_reconnect_from_cursor() {
        let mut host = admitted_host();
        let request_id = match host.take_commands().pop().expect("open") {
            AppSurfaceCommand::Open { request_id, .. } => request_id,
            other => panic!("unexpected: {other:?}"),
        };
        host.apply_event(AppSurfaceEvent::Response {
            request_id,
            app_id: APP_ID.into(),
            view_id: "main".into(),
            kind: AppSurfaceRequestKind::Open,
            status: 200,
            body: open_response(APP_ID, "main", "1"),
        });
        let _ = host.take_commands();
        host.apply_event(AppSurfaceEvent::StreamFrame {
            app_id: APP_ID.into(),
            view_id: "main".into(),
            frame: AppStreamFrameV1::Open {
                schema_version: 1,
                subscription_id: "updates".into(),
                sequence: 0,
                schema_digest: cowd_app_protocol::app_tui_view_patch_schema_digest_v1()
                    .expect("patch digest"),
            },
        });
        host.apply_event(AppSurfaceEvent::StreamFrame {
            app_id: APP_ID.into(),
            view_id: "main".into(),
            frame: AppStreamFrameV1::Data {
                schema_version: 1,
                subscription_id: "updates".into(),
                sequence: 1,
                payload: json!({
                        "schema_version": 1,
                        "app_id": APP_ID,
                        "view_id": "main",
                        "base_revision": "1",
                        "revision": "2",
                        "operations": [{"op": "replace", "path": "/title", "value": "Live"}]
                }),
            },
        });
        host.apply_event(AppSurfaceEvent::StreamFrame {
            app_id: APP_ID.into(),
            view_id: "main".into(),
            frame: AppStreamFrameV1::Checkpoint {
                schema_version: 1,
                subscription_id: "updates".into(),
                sequence: 2,
                cursor: "cursor-2".into(),
            },
        });
        assert_eq!(
            host.apps[APP_ID].views["main"].state.document().revision,
            "2"
        );

        host.apply_event(AppSurfaceEvent::StreamDisconnected {
            app_id: APP_ID.into(),
            view_id: "main".into(),
            error: "transport reset".into(),
        });
        assert!(host.take_commands().iter().any(|command| matches!(
            command,
            AppSurfaceCommand::StreamStart {
                request,
                ..
            } if request.cursor.as_deref() == Some("cursor-2")
        )));
        assert!(host.apps[APP_ID].error.is_none());
    }

    #[test]
    fn malformed_or_stale_action_updates_are_fail_closed_without_overwriting_state() {
        let mut host = admitted_host();
        let open = match host.take_commands().pop().expect("open") {
            AppSurfaceCommand::Open { request_id, .. } => request_id,
            other => panic!("unexpected: {other:?}"),
        };
        host.apply_event(AppSurfaceEvent::Response {
            request_id: open,
            app_id: APP_ID.into(),
            view_id: "main".into(),
            kind: AppSurfaceRequestKind::Open,
            status: 200,
            body: open_response(APP_ID, "main", "1"),
        });
        let _ = host.take_commands();
        assert!(host.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)));
        assert!(host.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)));
        let action_request = match host.take_commands().pop().expect("action") {
            AppSurfaceCommand::Action { request_id, .. } => request_id,
            other => panic!("unexpected: {other:?}"),
        };
        host.apply_event(AppSurfaceEvent::Response {
            request_id: action_request,
            app_id: APP_ID.into(),
            view_id: "main".into(),
            kind: AppSurfaceRequestKind::Action,
            status: 200,
            body: json!({
                "schema_version": 1,
                "view_id": "main",
                "revision": "1",
                "update": {"kind": "no_change"},
                "legacy_result": {}
            }),
        });
        assert!(host.apps[APP_ID].error.is_some());
        assert_eq!(
            host.apps[APP_ID].views["main"].state.document().revision,
            "1"
        );
    }
}
