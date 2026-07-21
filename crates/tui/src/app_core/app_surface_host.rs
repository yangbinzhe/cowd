//! Product-composed, application-neutral terminal surface host.
//!
//! This is the only TUI layer allowed to hold external APP panel objects.
//! It owns lifecycle, duplicate validation and effect collection, while every
//! application owns rendering, input reduction, contracts and state in its
//! own repository.

use std::collections::BTreeMap;

use cowd_app_host::{
    TuiAppAction, TuiAppEffect, TuiAppEffects, TuiAppEvent, TuiAppPanel, TuiAppRenderContext,
    TuiAppSurfaceContribution, TuiAppTheme,
};
use crossterm::event::KeyEvent;
use ratatui::{layout::Rect, Frame};

/// One effect accompanied by the source panel identity needed to route the
/// asynchronous host response back to the same external application.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingAppEffect {
    pub app_id: String,
    pub panel_id: String,
    pub effect: TuiAppEffect,
}

/// An action advertised by an external application panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostedAppAction {
    pub app_id: String,
    pub panel_id: String,
    pub action: TuiAppAction,
}

struct HostedPanel {
    app_id: String,
    panel: Box<dyn TuiAppPanel>,
}

/// Generic host for statically-linked APP panels.
///
/// There is intentionally no product-specific branch here. Product composition supplies
/// zero or more contributions, so a core-only binary has an empty host while
/// a full product mounts all linked APP surfaces through the same path.
#[derive(Default)]
pub struct TuiAppHost {
    panels: BTreeMap<String, HostedPanel>,
    pending_effects: Vec<PendingAppEffect>,
    startup_notices: Vec<String>,
}

impl TuiAppHost {
    #[must_use]
    pub fn product() -> Self {
        let contributions = {
            #[cfg(feature = "app-mfg")]
            {
                vec![app_bundle_mfg::mfg_tui_surface_contribution()]
            }
            #[cfg(not(feature = "app-mfg"))]
            {
                Vec::new()
            }
        };

        match Self::from_contributions(contributions) {
            Ok(host) => host,
            Err(error) => Self {
                startup_notices: vec![format!(
                    "Application terminal surface is unavailable: {error}"
                )],
                ..Self::default()
            },
        }
    }

    pub fn from_contributions(
        contributions: impl IntoIterator<Item = TuiAppSurfaceContribution>,
    ) -> Result<Self, String> {
        let mut host = Self::default();
        let mut app_ids = BTreeMap::new();
        for contribution in contributions {
            contribution
                .validate()
                .map_err(|error| format!("invalid application TUI contribution: {error}"))?;
            let app_id = contribution.app_id.to_string();
            if app_ids.insert(app_id.clone(), ()).is_some() {
                return Err(format!(
                    "duplicate application id registered for TUI: {app_id}"
                ));
            }
            let panel_id = contribution.descriptor.panel_id.clone();
            if host.panels.contains_key(&panel_id) {
                return Err(format!("duplicate application panel id: {panel_id}"));
            }

            let mut panel = contribution.create_panel();
            if panel.panel_id() != panel_id {
                return Err(format!(
                    "application {app_id} factory returned panel {} instead of {panel_id}",
                    panel.panel_id()
                ));
            }
            let mut effects = TuiAppEffects::default();
            panel.on_mount(&mut effects);
            host.pending_effects
                .extend(effects.take().into_iter().map(|effect| PendingAppEffect {
                    app_id: app_id.clone(),
                    panel_id: panel_id.clone(),
                    effect,
                }));
            host.panels.insert(panel_id, HostedPanel { app_id, panel });
        }
        Ok(host)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.panels.is_empty()
    }

    #[must_use]
    pub fn panel_ids(&self) -> Vec<String> {
        self.panels.keys().cloned().collect()
    }

    pub fn render(&mut self, panel_id: &str, frame: &mut Frame<'_>, area: Rect, focused: bool) {
        if let Some(hosted) = self.panels.get_mut(panel_id) {
            hosted.panel.render(
                frame,
                area,
                TuiAppRenderContext {
                    theme: TuiAppTheme::dark(),
                    focused,
                },
            );
        }
    }

    pub fn handle_key(&mut self, panel_id: &str, key: KeyEvent) -> bool {
        let Some(hosted) = self.panels.get_mut(panel_id) else {
            return false;
        };
        let mut effects = TuiAppEffects::default();
        let handled = hosted.panel.handle_key(key, &mut effects);
        self.extend_effects(panel_id, effects);
        handled
    }

    pub fn apply_event(&mut self, panel_id: &str, event: TuiAppEvent) {
        let Some(hosted) = self.panels.get_mut(panel_id) else {
            return;
        };
        let mut effects = TuiAppEffects::default();
        hosted.panel.apply_event(event, &mut effects);
        self.extend_effects(panel_id, effects);
    }

    #[must_use]
    pub fn actions(&self) -> Vec<HostedAppAction> {
        self.panels
            .iter()
            .flat_map(|(panel_id, hosted)| {
                hosted
                    .panel
                    .actions()
                    .into_iter()
                    .map(|action| HostedAppAction {
                        app_id: hosted.app_id.clone(),
                        panel_id: panel_id.clone(),
                        action,
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn dispatch_action(&mut self, panel_id: &str, action_id: &str) -> bool {
        let Some(hosted) = self.panels.get_mut(panel_id) else {
            return false;
        };
        let mut effects = TuiAppEffects::default();
        let handled = hosted.panel.dispatch_action(action_id, &mut effects);
        self.extend_effects(panel_id, effects);
        handled
    }

    pub fn handle_command(&mut self, command: &str) -> bool {
        let panel_ids = self.panel_ids();
        for panel_id in panel_ids {
            let Some(hosted) = self.panels.get_mut(&panel_id) else {
                continue;
            };
            let mut effects = TuiAppEffects::default();
            let handled = hosted.panel.handle_command(command, &mut effects);
            self.extend_effects(&panel_id, effects);
            if handled {
                return true;
            }
        }
        false
    }

    #[must_use]
    pub fn take_effects(&mut self) -> Vec<PendingAppEffect> {
        std::mem::take(&mut self.pending_effects)
    }

    #[must_use]
    pub fn take_startup_notices(&mut self) -> Vec<String> {
        std::mem::take(&mut self.startup_notices)
    }

    fn extend_effects(&mut self, panel_id: &str, mut effects: TuiAppEffects) {
        let Some(hosted) = self.panels.get(panel_id) else {
            return;
        };
        let app_id = hosted.app_id.clone();
        self.pending_effects
            .extend(effects.take().into_iter().map(|effect| PendingAppEffect {
                app_id: app_id.clone(),
                panel_id: panel_id.to_string(),
                effect,
            }));
    }
}
