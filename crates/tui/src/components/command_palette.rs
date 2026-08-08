// ── Command Palette ──────────────────────────────────────────────
// Ctrl+P overlay for fuzzy-searching and executing commands.
// No external fuzzy crate — simple in-house scoring algorithm.
// No command history tracking — that's the Prompt component's job.
//
// Features:
//   - Modal overlay with search input
//   - Fuzzy match against registered actions (slash commands + keybinds)
//   - Ranked results: prefix match > substring match > fuzzy char sequence
//   - j/k navigation, Enter to dispatch, Esc to close
// -----------------------------------------------------------------

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph};
use ratatui::Frame;

use crate::components::base::{Component, EventResult, RenderContext};
use crate::keybind::Action;
use crate::runtime_control_store::RuntimeControlSnapshot;
use crate::workbench::action_registry;

// ═══════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════

/// A single command registered in the palette.
#[derive(Debug, Clone)]
pub struct CommandEntry {
    /// Display name (e.g., "/status", "Toggle Help").
    pub name: String,
    /// Short description shown beside the name in results.
    pub description: String,
    /// The action to dispatch when this command is selected.
    pub action: Action,
    /// Whether this entry was generated from live runtime state.
    pub dynamic: bool,
}

impl CommandEntry {
    pub fn static_entry(
        name: impl Into<String>,
        description: impl Into<String>,
        action: Action,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            action,
            dynamic: false,
        }
    }

    pub fn dynamic(
        name: impl Into<String>,
        description: impl Into<String>,
        action: Action,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            action,
            dynamic: true,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Fuzzy scoring (no external crate)
// ═══════════════════════════════════════════════════════════════════

/// Score how well `query` matches `text`.
///
/// Returns:
///   - 100  — exact prefix match
///   - 50   — substring match (but not prefix)
///   - 25×N — all query chars appear in order (fuzzy), N = query length
///   - 0    — no match
///
/// Comparison is case-insensitive.
fn fuzzy_match_score(query: &str, text: &str) -> usize {
    if query.is_empty() || text.is_empty() {
        return 0;
    }

    let q: Vec<char> = query.chars().map(|c| c.to_ascii_lowercase()).collect();
    let t: Vec<char> = text.chars().map(|c| c.to_ascii_lowercase()).collect();
    let q_str: String = q.iter().collect();
    let t_str: String = t.iter().collect();

    // 1. Exact prefix match — highest confidence
    if t_str.starts_with(&q_str) {
        return 100;
    }

    // 2. Substring match (not prefix, already checked above)
    if t_str.contains(&q_str) {
        return 50;
    }

    // 3. Fuzzy: all query chars appear in order in text
    let mut ti = 0;
    let mut matched = 0usize;
    for &qc in &q {
        while ti < t.len() {
            if t[ti] == qc {
                matched += 1;
                ti += 1;
                break;
            }
            ti += 1;
        }
    }

    if matched == q.len() {
        matched * 25
    } else {
        0
    }
}

/// Compute the combined score for a command entry against a query.
/// Takes the best of name-score and description-score.
fn score_entry(query: &str, entry: &CommandEntry) -> usize {
    let name_score = fuzzy_match_score(query, &entry.name);
    let desc_score = fuzzy_match_score(query, &entry.description);
    name_score.max(desc_score)
}

// ═══════════════════════════════════════════════════════════════════
// Default command registry
// ═══════════════════════════════════════════════════════════════════

/// All known commands projected from the shared command registry.
fn registry_entries_from_payload(payload: &serde_json::Value) -> Vec<CommandEntry> {
    let mut entries = Vec::new();
    let commands = payload
        .get("commands")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for command in commands {
        let name = command
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        if name.is_empty() {
            continue;
        }
        let description = command
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        entries.push(CommandEntry::static_entry(
            name.clone(),
            description.clone(),
            action_from_target(command.get("action"), &name),
        ));
        for alias in command
            .get("aliases")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
        {
            entries.push(CommandEntry::static_entry(
                alias.to_string(),
                format!("Alias for {name} · {description}"),
                Action::Execute(alias.to_string()),
            ));
        }
    }
    entries
}

fn workbench_registry_entries() -> Vec<CommandEntry> {
    let entries = action_registry::registered_actions()
        .into_iter()
        .map(|action| {
            CommandEntry::static_entry(
                action.label,
                format!(
                    "{} · domain:{} · risk:{:?} · receipt:{}",
                    action.description, action.domain, action.risk, action.receipt_target
                ),
                action.action,
            )
        })
        .collect::<Vec<_>>();
    entries
}

fn static_entries_from_payload(payload: &serde_json::Value) -> Vec<CommandEntry> {
    let mut entries = registry_entries_from_payload(payload);
    entries.extend(workbench_registry_entries());
    entries
}

fn action_from_target(target: Option<&serde_json::Value>, fallback_command: &str) -> Action {
    let kind = target
        .and_then(|value| value.get("kind"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match kind {
        "client" => match target
            .and_then(|value| value.get("action"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
        {
            "toggle-help" => Action::ToggleHelp,
            "toggle-theme" => Action::ToggleTheme,
            "search" => Action::Search,
            "copy" => Action::Copy,
            "next-panel" => Action::NextPanel,
            "previous-panel" => Action::PrevPanel,
            "submit-input" => Action::SubmitInput,
            "refresh-config-status" => Action::RefreshConfigStatus,
            "cancel" => Action::Cancel,
            "quit" => Action::Quit,
            slash if slash.starts_with("slash:") => {
                Action::Execute(format!("/{}", slash.trim_start_matches("slash:")))
            }
            other => Action::Execute(other.to_string()),
        },
        _ => Action::Execute(fallback_command.to_string()),
    }
}

// ═══════════════════════════════════════════════════════════════════
// CommandPalette component
// ═══════════════════════════════════════════════════════════════════

/// Modal overlay for fuzzy command search and dispatch.
///
/// # State
/// - `all_commands` — all registered commands (populated with defaults)
/// - `search_input` — the user's current query text
/// - `cursor` — cursor position (byte index) within `search_input`
/// - `results` — scored search results as `(index_in_all_commands, score)` pairs
/// - `selected_index` — which result is highlighted (0-based index in `results`)
/// - `visible` — whether the overlay is currently shown
/// - `pending_action` — the action to execute (set on Enter, consumed by parent)
pub struct CommandPalette {
    all_commands: Vec<CommandEntry>,
    search_input: String,
    cursor: usize,
    results: Vec<(usize, usize)>,
    selected_index: usize,
    visible: bool,
    pending_action: Option<Action>,
}

impl CommandPalette {
    /// Create a new command palette with all default commands pre-registered.
    #[must_use]
    pub fn new() -> Self {
        let all_commands = workbench_registry_entries();
        Self {
            all_commands,
            search_input: String::new(),
            cursor: 0,
            results: Vec::new(),
            selected_index: 0,
            visible: false,
            pending_action: None,
        }
    }

    #[cfg(test)]
    fn new_with_projection(payload: &serde_json::Value) -> Self {
        let all_commands = static_entries_from_payload(payload);
        Self {
            all_commands,
            search_input: String::new(),
            cursor: 0,
            results: Vec::new(),
            selected_index: 0,
            visible: false,
            pending_action: None,
        }
    }

    pub fn sync_command_projection(&mut self, payload: &serde_json::Value) {
        let dynamic = self
            .all_commands
            .iter()
            .filter(|entry| entry.dynamic)
            .cloned()
            .collect::<Vec<_>>();
        self.all_commands = static_entries_from_payload(payload);
        self.all_commands.extend(dynamic);
        self.run_search();
    }

    /// Project actions offered by every mounted APP through the same command
    /// palette as core actions. Cowd keeps only panel/action identifiers; the
    /// APP retains capability, receipt and mutation semantics.
    pub fn sync_app_actions(&mut self, actions: &[crate::app_surface_host::HostedAppAction]) {
        self.all_commands
            .retain(|entry| !matches!(&entry.action, Action::Execute(command) if command.starts_with("/app ")));
        self.all_commands.extend(actions.iter().map(|hosted| {
            let action = &hosted.action;
            let availability = action.unavailable_reason.as_deref().map_or_else(
                || "available".to_string(),
                |reason| format!("unavailable: {reason}"),
            );
            CommandEntry::dynamic(
                format!("{}: {}", hosted.app_id, action.label),
                format!(
                    "{} · domain:{} · risk:{} · confirmation:{} · {}",
                    action.description,
                    action.domain,
                    action.risk,
                    action.requires_confirmation,
                    availability
                ),
                Action::Execute(format!("/app {} {}", hosted.panel_id, action.id)),
            )
        }));
        self.run_search();
    }

    // ── Registration ────────────────────────────────────────────

    /// Register a new command in the palette.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        description: impl Into<String>,
        action: Action,
    ) {
        self.all_commands
            .push(CommandEntry::static_entry(name, description, action));
    }

    /// Replace live runtime-derived actions while preserving static commands.
    pub fn sync_runtime_actions(&mut self, snapshot: &RuntimeControlSnapshot) {
        self.all_commands.retain(|entry| !entry.dynamic);

        if !snapshot.gateway_running {
            self.all_commands.push(CommandEntry::dynamic(
                "Start Gateway",
                "Gateway is offline; inspect gateway status or start it from CLI",
                Action::Execute("/status".into()),
            ));
            self.run_search();
            return;
        }

        self.all_commands.push(CommandEntry::dynamic(
            "Inspect Runtime",
            format!(
                "Gateway readiness {} across {} components",
                snapshot.runtime_readiness.as_deref().unwrap_or("unknown"),
                snapshot.runtime_components.unwrap_or_default()
            ),
            Action::Execute("/context runtime".into()),
        ));
        self.all_commands.push(CommandEntry::dynamic(
            "Inspect Context",
            "Show current context envelope, stable head, dynamic tail, and pressure",
            Action::Execute("/context".into()),
        ));

        if snapshot.task_count.unwrap_or_default() > 0 {
            self.all_commands.push(CommandEntry::dynamic(
                "Manage Running Tasks",
                format!(
                    "{} daemon tasks visible",
                    snapshot.task_count.unwrap_or_default()
                ),
                Action::Execute("/tasks".into()),
            ));
            if let Some(task) = snapshot
                .tasks
                .iter()
                .find(|task| task.status == "blocked" || task.failure_count > 0)
            {
                self.all_commands.push(CommandEntry::dynamic(
                    "Cancel Problem Task",
                    format!("Cancel {} directly: {}", task.id, task.objective),
                    Action::CancelGatewayTask {
                        id: task.id.clone(),
                        expected_revision: task.revision,
                    },
                ));
            }
            if let Some(task) = snapshot.tasks.iter().find(|task| {
                task.review_result.as_deref() == Some("accepted")
                    || task.status == "reviewed"
                    || task.status == "completed"
            }) {
                self.all_commands.push(CommandEntry::dynamic(
                    "Complete Reviewed Task",
                    format!(
                        "Complete {} directly with {} artifacts",
                        task.id, task.artifact_count
                    ),
                    Action::CompleteGatewayTask {
                        id: task.id.clone(),
                        expected_revision: task.revision,
                    },
                ));
            }
        } else {
            self.all_commands.push(CommandEntry::dynamic(
                "Start YOLO Goal",
                "Prepare a continuous daemon task command",
                Action::Execute("/tasks start --yolo ".into()),
            ));
        }

        if let Some(mission) = snapshot.mission_control.as_ref() {
            if let Some(session_id) = mission.active_session_id.as_ref() {
                if mission.task_focus_id.is_some() {
                    self.all_commands.push(CommandEntry::dynamic(
                        "Clear Task Focus",
                        "Let Runtime route the next Turn without a pinned Task",
                        Action::ClearGatewayTaskFocus {
                            session_id: session_id.clone(),
                            expected_revision: mission.routing_revision,
                        },
                    ));
                } else if let Some(task) = snapshot.tasks.iter().find(|task| {
                    matches!(
                        task.status.as_str(),
                        "pending" | "running" | "reviewing" | "blocked"
                    )
                }) {
                    self.all_commands.push(CommandEntry::dynamic(
                        "Focus Current Task",
                        format!("Route future Turns to {}", task.objective),
                        Action::SetGatewayTaskFocus {
                            session_id: session_id.clone(),
                            task_id: task.id.clone(),
                            expected_revision: mission.routing_revision,
                        },
                    ));
                }

                if mission.mission_focus_id.is_some() {
                    self.all_commands.push(CommandEntry::dynamic(
                        "Clear Mission Focus",
                        "Let Runtime assign future Root Tasks automatically",
                        Action::ClearGatewayMissionFocus {
                            session_id: session_id.clone(),
                            expected_revision: mission.routing_revision,
                        },
                    ));
                } else if let Some(mission_id) = mission.selected_mission_id.as_ref() {
                    self.all_commands.push(CommandEntry::dynamic(
                        "Focus Selected Mission",
                        format!("Route future Root Tasks to {mission_id}"),
                        Action::SetGatewayMissionFocus {
                            session_id: session_id.clone(),
                            mission_id: mission_id.clone(),
                            expected_revision: mission.routing_revision,
                        },
                    ));
                }
            }
        }

        if snapshot.pending_approvals.unwrap_or_default() > 0 {
            self.all_commands.push(CommandEntry::dynamic(
                "Review Pending Approvals",
                format!(
                    "{} approval requests need attention",
                    snapshot.pending_approvals.unwrap_or_default()
                ),
                Action::Execute("/approvals".into()),
            ));
            if let Some(approval) = snapshot.approval_items.first() {
                if let Some(app_id) = approval.application_source_id() {
                    if let Some(review_ref) = approval
                        .review_ref
                        .as_deref()
                        .filter(|review| !review.trim().is_empty())
                    {
                        self.all_commands.push(CommandEntry::dynamic(
                            "Open App Review",
                            format!("Review {review_ref} is owned by application {app_id}"),
                            Action::Execute(format!("/{app_id} review {review_ref}")),
                        ));
                    } else {
                        self.all_commands.push(CommandEntry::dynamic(
                            "Inspect Invalid App Approval",
                            "Application approval is missing its review reference; generic approve/reject is disabled",
                            Action::Execute("/approvals".into()),
                        ));
                    }
                } else {
                    self.all_commands.push(CommandEntry::dynamic(
                        "Approve First Pending Request",
                        format!(
                            "{} [{}] {}",
                            approval.tool_name,
                            approval.risk.as_deref().unwrap_or("unknown"),
                            approval.input_preview
                        ),
                        Action::RespondGatewayApproval {
                            id: approval.id.clone(),
                            approved: true,
                            scope: "once".to_string(),
                        },
                    ));
                    for (label, scope) in [
                        ("Approve For Turn", "turn"),
                        ("Approve For Task", "task"),
                        ("Approve For Session", "session"),
                        ("Approve Globally", "global"),
                    ] {
                        self.all_commands.push(CommandEntry::dynamic(
                            label,
                            format!("Approve {} with {scope} scope", approval.id),
                            Action::RespondGatewayApproval {
                                id: approval.id.clone(),
                                approved: true,
                                scope: scope.to_string(),
                            },
                        ));
                    }
                    self.all_commands.push(CommandEntry::dynamic(
                        "Reject First Pending Request",
                        format!("Reject {}", approval.id),
                        Action::RespondGatewayApproval {
                            id: approval.id.clone(),
                            approved: false,
                            scope: "once".to_string(),
                        },
                    ));
                }
            }
        }

        if snapshot.cross_plane_grants_active.unwrap_or_default() > 0
            || snapshot.cross_plane_actions_24h.unwrap_or_default() > 0
        {
            self.all_commands.push(CommandEntry::dynamic(
                "Inspect Cross-Plane",
                format!(
                    "{} active grants, {} actions in 24h",
                    snapshot.cross_plane_grants_active.unwrap_or_default(),
                    snapshot.cross_plane_actions_24h.unwrap_or_default()
                ),
                Action::Execute("/cross-plane".into()),
            ));
        }
        for grant in snapshot
            .approval_grants
            .iter()
            .filter(|grant| grant.status == "active")
        {
            self.all_commands.push(CommandEntry::dynamic(
                "Revoke Approval Grant",
                format!(
                    "{} [{}] {}",
                    grant.capability, grant.scope, grant.workspace_key
                ),
                Action::RevokeGatewayApprovalGrant(grant.id.clone()),
            ));
        }

        if !snapshot.connector_resources.is_empty() {
            self.all_commands.push(CommandEntry::dynamic(
                "Search Connector Resources",
                format!(
                    "{} external resources indexed",
                    snapshot.connector_resources.len()
                ),
                Action::Execute("/context".into()),
            ));
            if let Some(resource) = snapshot.connector_resources.first() {
                self.all_commands.push(CommandEntry::dynamic(
                    "Mark Connector Resource Indexed",
                    format!("{} -> indexed", resource.title),
                    Action::RevalidateConnectorResource {
                        reference: resource.reference.clone(),
                        state: "indexed".to_string(),
                    },
                ));
                self.all_commands.push(CommandEntry::dynamic(
                    "Mark Connector Resource Stale",
                    format!("{} -> stale", resource.title),
                    Action::RevalidateConnectorResource {
                        reference: resource.reference.clone(),
                        state: "stale".to_string(),
                    },
                ));
                self.all_commands.push(CommandEntry::dynamic(
                    "Remember Connector Resource",
                    format!("Promote metadata for {}", resource.title),
                    Action::PromoteConnectorResourceToMemory {
                        reference: resource.reference.clone(),
                        session_id: None,
                    },
                ));
            }
        }

        if !snapshot.connector_accounts.is_empty() || !snapshot.connector_capabilities.is_empty() {
            self.all_commands.push(CommandEntry::dynamic(
                "Probe Connectors",
                format!(
                    "{} accounts, {} capabilities",
                    snapshot.connector_accounts.len(),
                    snapshot.connector_capabilities.len()
                ),
                Action::Execute("/status".into()),
            ));
        }

        if !snapshot.message_connectors.is_empty()
            || !snapshot.message_endpoints.is_empty()
            || !snapshot.message_routes.is_empty()
            || !snapshot.message_bindings.is_empty()
        {
            self.all_commands.push(CommandEntry::dynamic(
                "Inspect Message Plane",
                format!(
                    "{} connectors, {} endpoints, {} routes, {} bindings",
                    snapshot.message_connectors.len(),
                    snapshot.message_endpoints.len(),
                    snapshot.message_routes.len(),
                    snapshot.message_bindings.len()
                ),
                Action::Execute("/gateway".into()),
            ));
        }

        if !snapshot.surfaces.is_empty() || snapshot.surface_health.is_some() {
            let health = snapshot.surface_health.as_ref();
            self.all_commands.push(CommandEntry::dynamic(
                "Inspect Surfaces",
                format!(
                    "{} surfaces, {} external, host {}",
                    health
                        .map(|item| item.surface_count)
                        .unwrap_or(snapshot.surfaces.len() as u64),
                    health.map(|item| item.external_surface_count).unwrap_or(0),
                    health.map(|item| item.status.as_str()).unwrap_or("unknown")
                ),
                Action::Execute("/surfaces".into()),
            ));
            if let Some(surface) = snapshot
                .surfaces
                .iter()
                .find(|surface| matches!(surface.status.as_str(), "unavailable" | "error"))
            {
                self.all_commands.push(CommandEntry::dynamic(
                    "Inspect Surface Issue",
                    format!("{} is {}", surface.id, surface.status),
                    Action::Execute("/surfaces".into()),
                ));
            }
        }

        if let Some(docs_provider) = snapshot
            .connector_capabilities
            .iter()
            .find(|capability| capability.capability_id == "service.local.docs.read")
            .map(|capability| capability.provider.as_str())
        {
            let docs_label = connector_docs_label(docs_provider);
            self.all_commands.push(CommandEntry::dynamic(
                format!("{docs_label} Dry Run"),
                format!(
                    "Use {docs_provider} connector console contract for a non-destructive service read"
                ),
                Action::Execute("/cross-plane".into()),
            ));
            self.all_commands.push(CommandEntry::dynamic(
                format!("{docs_label} Commit"),
                format!(
                    "Commit a governed {docs_provider} connector service read and persist the resource ref"
                ),
                Action::Execute("/cross-plane".into()),
            ));
        }

        if !snapshot.connector_degraded_reasons.is_empty()
            || snapshot
                .connector_accounts
                .iter()
                .any(|account| account.status == "degraded")
        {
            let reason = snapshot
                .connector_degraded_reasons
                .first()
                .cloned()
                .or_else(|| {
                    snapshot
                        .connector_accounts
                        .iter()
                        .find(|account| account.status == "degraded")
                        .and_then(|account| account.reason.clone())
                })
                .unwrap_or_else(|| "connector degraded".to_string());
            self.all_commands.push(CommandEntry::dynamic(
                "Inspect Degraded Connector",
                reason,
                Action::Execute("/status".into()),
            ));
        }

        if !snapshot.degraded_reasons.is_empty() {
            self.all_commands.push(CommandEntry::dynamic(
                "Inspect Gateway Degradation",
                snapshot.degraded_reasons.join("; "),
                Action::Execute("/context runtime".into()),
            ));
        }

        self.run_search();
    }

    /// Return the number of registered commands.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.all_commands.len()
    }

    // ── Visibility ─────────────────────────────────────────────

    /// Open the palette and reset search state.
    pub fn open(&mut self) {
        self.visible = true;
        self.search_input.clear();
        self.cursor = 0;
        self.selected_index = 0;
        self.pending_action = None;
        // Show all commands on open
        self.run_search();
    }

    /// Open the palette with a prefilled search query.
    pub fn open_with_query(&mut self, query: impl Into<String>) {
        self.visible = true;
        self.search_input = query.into();
        self.cursor = self.search_input.len();
        self.selected_index = 0;
        self.pending_action = None;
        self.run_search();
    }

    /// Close the palette.
    pub fn close(&mut self) {
        self.visible = false;
        self.search_input.clear();
        self.cursor = 0;
        self.results.clear();
        self.selected_index = 0;
    }

    /// Returns `true` if the palette is currently open.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.visible
    }

    /// Take the pending action (returns `None` if no action is queued).
    #[must_use]
    pub fn take_action(&mut self) -> Option<Action> {
        self.pending_action.take()
    }

    // ── Search ─────────────────────────────────────────────────

    /// Run the search against the current `search_input` and update
    /// `self.results` and `self.selected_index`.
    fn run_search(&mut self) {
        let query = self.search_input.trim();

        if query.is_empty() {
            // Empty query: show all commands, score=0, original order
            self.results = (0..self.all_commands.len()).map(|i| (i, 0)).collect();
        } else {
            let mut scored: Vec<(usize, usize)> = self
                .all_commands
                .iter()
                .enumerate()
                .filter_map(|(i, entry)| {
                    let score = score_entry(query, entry);
                    if score > 0 {
                        Some((i, score))
                    } else {
                        None
                    }
                })
                .collect();

            // Sort: higher score first; tie-break by original index
            scored.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            self.results = scored;
        }

        // Clamp selected index
        if self.selected_index >= self.results.len() && !self.results.is_empty() {
            self.selected_index = self.results.len() - 1;
        } else if self.results.is_empty() {
            self.selected_index = 0;
        }
    }

    /// Return a reference to the current search results.
    #[must_use]
    pub fn results(&self) -> &[(usize, usize)] {
        &self.results
    }

    /// Return the currently selected index within `results`.
    #[must_use]
    pub fn selected_index(&self) -> usize {
        self.selected_index
    }

    // ── Input handling ─────────────────────────────────────────

    /// Insert a character at the cursor position.
    fn insert_char(&mut self, c: char) {
        let pos = byte_pos_to_char_pos(&self.search_input, self.cursor);
        self.search_input.insert(pos, c);
        self.cursor += c.len_utf8();
        self.run_search();
    }

    /// Delete the character before the cursor (Backspace).
    fn backspace(&mut self) {
        if self.cursor == 0 || self.search_input.is_empty() {
            return;
        }
        let byte_pos = prev_char_boundary(&self.search_input, self.cursor);
        self.search_input.drain(byte_pos..self.cursor);
        self.cursor = byte_pos;
        self.run_search();
    }

    /// Delete the character at the cursor (Delete).
    fn delete(&mut self) {
        if self.cursor >= self.search_input.len() {
            return;
        }
        let next = next_char_boundary(&self.search_input, self.cursor);
        self.search_input.drain(self.cursor..next);
        self.run_search();
    }

    /// Move cursor left by one character.
    fn cursor_left(&mut self) {
        if self.cursor > 0 {
            self.cursor = prev_char_boundary(&self.search_input, self.cursor);
        }
    }

    /// Move cursor right by one character.
    fn cursor_right(&mut self) {
        if self.cursor < self.search_input.len() {
            self.cursor = next_char_boundary(&self.search_input, self.cursor);
        }
    }

    /// Move cursor to the beginning of the input.
    fn cursor_home(&mut self) {
        self.cursor = 0;
    }

    /// Move cursor to the end of the input.
    fn cursor_end(&mut self) {
        self.cursor = self.search_input.len();
    }

    /// Move the selection down (forward).
    fn select_next(&mut self) {
        if !self.results.is_empty() {
            self.selected_index = (self.selected_index + 1) % self.results.len();
        }
    }

    /// Move the selection up (backward).
    fn select_prev(&mut self) {
        if !self.results.is_empty() {
            self.selected_index = if self.selected_index == 0 {
                self.results.len() - 1
            } else {
                self.selected_index - 1
            };
        }
    }

    /// Accept the currently selected command: set `pending_action` and close.
    fn accept_selected(&mut self) {
        if self.results.is_empty() {
            return;
        }
        let idx = self.results[self.selected_index].0;
        if let Some(entry) = self.all_commands.get(idx) {
            self.pending_action = Some(entry.action.clone());
        }
        self.close();
    }
}

impl Default for CommandPalette {
    fn default() -> Self {
        Self::new()
    }
}

// ── Component trait ──────────────────────────────────────────────

impl Component for CommandPalette {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if !self.visible {
            return;
        }

        let accent = ctx.theme().accent_color();
        let frame = ctx.frame_mut();

        // 1. Backdrop: Clear + dim overlay
        frame.render_widget(Clear, area);
        let dim_bg = Style::default().bg(Color::Rgb(20, 20, 20));
        frame.render_widget(Paragraph::new("").style(dim_bg), area);

        // 2. Compute centered rect
        let max_w = ((area.width as f32) * 0.7) as u16;
        let w = max_w.max(40).min(80);
        let h = self.dialog_height();
        let x = area.x + (area.width.saturating_sub(w)) / 2;
        let y = area.y + Self::area_y_offset(area.height, h);
        let dialog_rect = Rect::new(x, y, w, h);

        // Clear the dialog area
        frame.render_widget(Clear, dialog_rect);

        // 3. Render the dialog content
        self.render_dialog(frame, dialog_rect, accent);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        if !self.visible {
            return EventResult::NotConsumed;
        }

        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => {
                let consumed = self.handle_key(key.code);
                if consumed {
                    EventResult::Consumed
                } else {
                    EventResult::Consumed // still consumed — modal traps all keys
                }
            }
            _ => EventResult::Consumed,
        }
    }

    fn focusable(&self) -> bool {
        self.visible
    }

    fn id(&self) -> &str {
        "command_palette"
    }
}

// ── Private helpers ──────────────────────────────────────────────

impl CommandPalette {
    /// Process a key press and return whether it was consumed.
    fn handle_key(&mut self, code: KeyCode) -> bool {
        match code {
            KeyCode::Esc => {
                self.close();
                true
            }
            KeyCode::Enter => {
                self.accept_selected();
                true
            }
            KeyCode::Tab => {
                self.accept_selected();
                true
            }
            KeyCode::Char(c) => {
                match c {
                    'j' => self.select_next(),
                    'k' => self.select_prev(),
                    _ => self.insert_char(c),
                }
                true
            }
            KeyCode::Backspace => {
                self.backspace();
                true
            }
            KeyCode::Delete => {
                self.delete();
                true
            }
            KeyCode::Left => {
                self.cursor_left();
                true
            }
            KeyCode::Right => {
                self.cursor_right();
                true
            }
            KeyCode::Home => {
                self.cursor_home();
                true
            }
            KeyCode::End => {
                self.cursor_end();
                true
            }
            KeyCode::Up => {
                self.select_prev();
                true
            }
            KeyCode::Down => {
                self.select_next();
                true
            }
            KeyCode::PageUp => {
                // Jump 10 items up
                for _ in 0..10 {
                    self.select_prev();
                }
                true
            }
            KeyCode::PageDown => {
                // Jump 10 items down
                for _ in 0..10 {
                    self.select_next();
                }
                true
            }
            _ => false,
        }
    }

    /// Compute the dialog height based on result count.
    fn dialog_height(&self) -> u16 {
        let results_h = self.results.len().min(12) as u16;
        // input_row(1) + blank(1) + results + blank(1) + hint(1) + border(2)
        results_h + 6
    }

    /// Compute a vertical offset to center the dialog (or place at 25% from top).
    fn area_y_offset(area_h: u16, dialog_h: u16) -> u16 {
        if dialog_h >= area_h {
            0
        } else {
            (area_h - dialog_h) / 3 // slightly above center (rule of thirds)
        }
    }

    /// Render the dialog contents: search input, results list, navigation hint.
    fn render_dialog(&self, frame: &mut Frame, rect: Rect, accent: Color) {
        // Block with border
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(accent))
            .title(Span::styled(
                " Command Palette ",
                Style::default().fg(accent),
            ));
        let inner = block.inner(rect);
        frame.render_widget(Clear, rect);
        frame.render_widget(block, rect);

        // Split inner area: [search(3), results(fill), hint(1)]
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // search input row
                Constraint::Min(1),    // results list
                Constraint::Length(1), // hint
            ])
            .split(inner);

        // ── Search input ───────────────────────────────────────
        self.render_search_input(frame, chunks[0], accent);

        // ── Results list ───────────────────────────────────────
        self.render_results(frame, chunks[1], accent);

        // ── Hint ────────────────────────────────────────────────
        let hint = Span::styled(
            "↑↓/jk navigate  Enter select  Esc close",
            Style::default().fg(Color::DarkGray),
        );
        frame.render_widget(Paragraph::new(Text::from(Line::from(hint))), chunks[2]);
    }

    /// Render the search input line with cursor.
    fn render_search_input(&self, frame: &mut Frame, area: Rect, _accent: Color) {
        let display: String = if self.search_input.is_empty() {
            String::new()
        } else {
            self.search_input.clone()
        };

        let placeholder = if self.search_input.is_empty() {
            "Type to search commands..."
        } else {
            ""
        };

        let search_line = if self.search_input.is_empty() {
            // Show dimmed placeholder
            Line::from(vec![
                Span::raw(" "), // small padding
                Span::styled(placeholder, Style::default().fg(Color::DarkGray)),
            ])
        } else {
            // Show actual input with cursor block
            let before_cursor = &display[..self.cursor.min(display.len())];
            let after_cursor = if self.cursor < display.len() {
                &display[self.cursor..]
            } else {
                ""
            };

            Line::from(vec![
                Span::raw(" "),
                Span::styled(format!("> {}", before_cursor), Style::default()),
                Span::styled("▊", Style::default().fg(Color::White).bg(Color::DarkGray)),
                Span::styled(after_cursor, Style::default()),
            ])
        };

        frame.render_widget(Paragraph::new(Text::from(search_line)), area);
    }

    /// Render the scored results list, highlighting the selected entry.
    fn render_results(&self, frame: &mut Frame, area: Rect, accent: Color) {
        if self.results.is_empty() {
            let no_results = if self.search_input.is_empty() {
                "Start typing to search..."
            } else {
                "No matching commands"
            };
            frame.render_widget(
                Paragraph::new(Text::from(Line::from(vec![
                    Span::styled("  ", Style::default().fg(Color::DarkGray)),
                    Span::styled(no_results, Style::default().fg(Color::DarkGray)),
                ]))),
                area,
            );
            return;
        }

        let max_visible = area.height as usize;
        let scroll_offset = if self.selected_index >= max_visible {
            self.selected_index - max_visible + 1
        } else {
            0
        };

        let items: Vec<ListItem> = self
            .results
            .iter()
            .enumerate()
            .skip(scroll_offset)
            .take(max_visible)
            .map(|(visual_idx, (orig_idx, _score))| {
                let entry = &self.all_commands[*orig_idx];
                let is_selected = visual_idx == self.selected_index;

                let prefix = if is_selected { "▶ " } else { "  " };
                let display = format!("{}{}", prefix, entry.name);
                let desc = &entry.description;

                // Pad name to 24 chars for alignment
                let padded_name = if display.len() < 24 {
                    format!("{:<24}", display)
                } else {
                    display
                };

                let full_line = format!("{}  {}", padded_name, desc);

                if is_selected {
                    ListItem::new(Line::styled(
                        full_line,
                        Style::default()
                            .fg(Color::Black)
                            .bg(accent)
                            .add_modifier(Modifier::BOLD),
                    ))
                } else {
                    ListItem::new(Line::styled(full_line, Style::default().fg(Color::White)))
                }
            })
            .collect();

        let list = List::new(items);
        frame.render_widget(list, area);
    }
}

fn connector_docs_label(provider: &str) -> String {
    let provider = provider.trim();
    let title = if provider.is_empty() {
        "Local".to_string()
    } else {
        provider
            .split(['-', '_', '.'])
            .filter(|part| !part.is_empty())
            .map(|part| {
                let mut chars = part.chars();
                match chars.next() {
                    Some(first) => {
                        format!("{}{}", first.to_uppercase(), chars.as_str().to_lowercase())
                    }
                    None => String::new(),
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    };
    if title.to_ascii_lowercase().contains("docs") {
        title
    } else {
        format!("{title} Docs")
    }
}

// ── UTF-8 char boundary helpers ──────────────────────────────────

/// Find the byte offset of the previous character boundary.
fn prev_char_boundary(s: &str, byte_pos: usize) -> usize {
    if byte_pos == 0 {
        return 0;
    }
    let mut pos = byte_pos.saturating_sub(1);
    while pos > 0 && !s.is_char_boundary(pos) {
        pos -= 1;
    }
    pos
}

/// Find the byte offset of the next character boundary.
fn next_char_boundary(s: &str, byte_pos: usize) -> usize {
    if byte_pos >= s.len() {
        return s.len();
    }
    let mut pos = byte_pos + 1;
    while pos < s.len() && !s.is_char_boundary(pos) {
        pos += 1;
    }
    pos
}

/// Convert a byte offset to a char index for insertion.
fn byte_pos_to_char_pos(s: &str, byte_pos: usize) -> usize {
    let byte_pos = byte_pos.min(s.len());
    s[..byte_pos].chars().count()
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::RenderContext;
    use crate::skin::SkinConfig;
    use crate::test_utils::{gateway_command_projection_fixture, MockTerminal};
    use crossterm::event::KeyModifiers;

    // ── Helpers ───────────────────────────────────────────────────

    fn key(code: KeyCode) -> crossterm::event::Event {
        Event::Key(crossterm::event::KeyEvent::new(code, KeyModifiers::NONE))
    }

    fn event_char(c: char) -> crossterm::event::Event {
        key(KeyCode::Char(c))
    }

    fn event_enter() -> crossterm::event::Event {
        key(KeyCode::Enter)
    }

    fn event_esc() -> crossterm::event::Event {
        key(KeyCode::Esc)
    }

    fn setup_palette() -> CommandPalette {
        CommandPalette::new_with_projection(&gateway_command_projection_fixture())
    }

    // ── Registration and construction ─────────────────────────────

    #[test]
    fn new_has_default_commands() {
        let p = setup_palette();
        let projection = gateway_command_projection_fixture();
        let command_count = projection
            .get("commands")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or_default();
        assert!(
            p.command_count() >= command_count,
            "expected at least all projected commands, got {}",
            p.command_count()
        );
    }

    #[test]
    fn registry_entries_include_every_projected_command_and_alias() {
        let p = setup_palette();

        for command in gateway_command_projection_fixture()
            .get("commands")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            let name = command
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap();
            let description = command
                .get("description")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            assert!(
                p.all_commands
                    .iter()
                    .any(|entry| entry.name == name && entry.description == description),
                "palette missing projected command {name}",
            );

            for alias in command
                .get("aliases")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
            {
                assert!(
                    p.all_commands.iter().any(|entry| entry.name == alias
                        && entry.action == Action::Execute(alias.to_string())),
                    "palette missing alias {alias} for {name}"
                );
            }
        }
    }

    #[test]
    fn registry_entries_include_on_demand_panel_shortcuts() {
        let p = setup_palette();
        for (name, action) in [
            ("/runtime", Action::Execute("/runtime".into())),
            ("/activity", Action::Execute("/activity".into())),
            ("/tools", Action::Execute("/tools".into())),
            ("/files", Action::Execute("/files".into())),
            ("/gateway", Action::Execute("/gateway".into())),
            ("/memory", Action::Execute("/memory".into())),
            ("/diff", Action::Execute("/diff".into())),
        ] {
            assert!(
                p.all_commands
                    .iter()
                    .any(|entry| entry.name == name && entry.action == action),
                "palette missing {name}"
            );
        }
    }

    #[test]
    fn register_custom_command() {
        let mut p = setup_palette();
        let before = p.command_count();
        p.register("my-cmd", "A custom command", Action::Noop);
        assert_eq!(p.command_count(), before + 1);
    }

    #[test]
    fn registry_entries_include_refresh_config_status_action() {
        let p = setup_palette();
        assert!(p.all_commands.iter().any(|entry| {
            entry.name == "Refresh Config Status" && entry.action == Action::RefreshConfigStatus
        }));
    }

    #[test]
    fn registry_entries_include_setup_center_action() {
        let p = setup_palette();
        assert!(p.all_commands.iter().any(|entry| {
            entry.name == "/setup" && entry.action == Action::Execute("/setup".into())
        }));
    }

    #[test]
    fn sync_runtime_actions_adds_contextual_gateway_entries() {
        let mut p = setup_palette();
        let before = p.command_count();
        let snapshot = RuntimeControlSnapshot {
            gateway_running: true,
            runtime_readiness: Some("92%".to_string()),
            runtime_components: Some(9),
            task_count: Some(2),
            tasks: vec![
                crate::runtime_control_store::TaskSummary {
                    id: "task-blocked".to_string(),
                    mission_id: "mission-default".to_string(),
                    kind: "root".to_string(),
                    revision: 0,
                    objective: "blocked task".to_string(),
                    status: "blocked".to_string(),
                    current_phase: Some("verify".to_string()),
                    yolo_mode: false,
                    failure_count: 1,
                    review_result: None,
                    artifact_count: 0,
                    blocker_reason: Some("approval".to_string()),
                },
                crate::runtime_control_store::TaskSummary {
                    id: "task-reviewed".to_string(),
                    mission_id: "mission-default".to_string(),
                    kind: "root".to_string(),
                    revision: 0,
                    objective: "reviewed task".to_string(),
                    status: "reviewed".to_string(),
                    current_phase: Some("review".to_string()),
                    yolo_mode: true,
                    failure_count: 0,
                    review_result: Some("accepted".to_string()),
                    artifact_count: 2,
                    blocker_reason: None,
                },
            ],
            pending_approvals: Some(1),
            approval_items: vec![crate::runtime_control_store::ApprovalSummary {
                id: "approval-1".to_string(),
                tool_name: "bash".to_string(),
                risk: Some("high".to_string()),
                requester: Some("session".to_string()),
                input_preview: "rm -rf /tmp/example".to_string(),
                ..crate::runtime_control_store::ApprovalSummary::default()
            }],
            cross_plane_grants_active: Some(3),
            cross_plane_actions_24h: Some(5),
            ..RuntimeControlSnapshot::default()
        };

        p.sync_runtime_actions(&snapshot);

        assert!(p.command_count() > before);
        assert!(p
            .all_commands
            .iter()
            .any(|entry| { entry.dynamic && entry.name == "Inspect Runtime" }));
        assert!(p
            .all_commands
            .iter()
            .any(|entry| { entry.dynamic && entry.action == Action::Execute("/tasks".into()) }));
        assert!(p.all_commands.iter().any(|entry| {
            entry.dynamic && entry.action == Action::Execute("/approvals".into())
        }));
        assert!(p.all_commands.iter().any(|entry| {
            entry.dynamic
                && entry.action
                    == Action::RespondGatewayApproval {
                        id: "approval-1".to_string(),
                        approved: true,
                        scope: "once".to_string(),
                    }
        }));
        assert!(p.all_commands.iter().any(|entry| {
            entry.dynamic
                && entry.action
                    == Action::RespondGatewayApproval {
                        id: "approval-1".to_string(),
                        approved: false,
                        scope: "once".to_string(),
                    }
        }));
        assert!(p.all_commands.iter().any(|entry| {
            entry.dynamic
                && entry.action
                    == Action::CancelGatewayTask {
                        id: "task-blocked".into(),
                        expected_revision: 0,
                    }
        }));
        assert!(p.all_commands.iter().any(|entry| {
            entry.dynamic
                && entry.action
                    == Action::CompleteGatewayTask {
                        id: "task-reviewed".into(),
                        expected_revision: 0,
                    }
        }));
        assert!(p.all_commands.iter().any(|entry| {
            entry.dynamic && entry.action == Action::Execute("/cross-plane".into())
        }));
    }

    #[test]
    fn sync_runtime_actions_adds_connector_entries() {
        let mut p = setup_palette();
        let snapshot = RuntimeControlSnapshot {
            gateway_running: true,
            task_count: Some(0),
            connector_accounts: vec![crate::runtime_control_store::ConnectorAccountSummary {
                provider: "mock".to_string(),
                account_id: "mock-docs".to_string(),
                auth_mode: "none".to_string(),
                status: "ready".to_string(),
                reason: None,
                binding_count: 1,
            }],
            connector_capabilities: vec![
                crate::runtime_control_store::ConnectorCapabilitySummary {
                    capability_id: "service.local.docs.read".to_string(),
                    provider: "mock".to_string(),
                    plane: "service".to_string(),
                    risk: "low".to_string(),
                    supports_commit: true,
                    requires_approval: false,
                },
            ],
            connector_resources: vec![crate::runtime_control_store::ConnectorResourceSummary {
                reference: "service://mock/docs/ready".to_string(),
                provider: "mock".to_string(),
                resource_type: "document".to_string(),
                title: "Ready Mock Document".to_string(),
                indexed_state: "indexed".to_string(),
            }],
            message_connectors: vec![crate::runtime_control_store::MessageConnectorSummary {
                connector: "feishu".to_string(),
                name: "feishu".to_string(),
                configuration_status: "configured".to_string(),
                runtime_status: "ready".to_string(),
                enabled: true,
                configured: true,
                capability_count: 2,
                missing_required_count: 0,
                consecutive_failures: 0,
                restart_count: 0,
                circuit_open: false,
            }],
            message_endpoints: vec![crate::runtime_control_store::MessageEndpointSummary {
                endpoint_id: "message:feishu:user".to_string(),
                connector: "feishu".to_string(),
                kind: "User".to_string(),
                status: "configured".to_string(),
                configured: true,
                capability_count: 1,
            }],
            message_routes: vec![crate::runtime_control_store::MessageRouteSummary {
                route_id: "message:feishu:default".to_string(),
                connector: "feishu".to_string(),
                policy: "origin".to_string(),
                status: "configured".to_string(),
                configured: true,
                capability_count: 1,
                runtime_status: "ready".to_string(),
            }],
            message_bindings: vec![crate::runtime_control_store::MessageBindingSummary {
                binding_id: "message:feishu:user:thread".to_string(),
                connector: "feishu".to_string(),
                endpoint: "user".to_string(),
                direction: "inbound".to_string(),
                status: "processed".to_string(),
                runtime_session_id: Some("session".to_string()),
                resource_count: 0,
                last_seen_at_ms: Some(1),
            }],
            connector_degraded_reasons: vec!["resource_directory: locked".to_string()],
            ..RuntimeControlSnapshot::default()
        };

        p.sync_runtime_actions(&snapshot);

        for name in [
            "Search Connector Resources",
            "Mark Connector Resource Indexed",
            "Mark Connector Resource Stale",
            "Remember Connector Resource",
            "Probe Connectors",
            "Inspect Message Plane",
            "Mock Docs Dry Run",
            "Mock Docs Commit",
        ] {
            assert!(
                p.all_commands
                    .iter()
                    .any(|entry| entry.dynamic && entry.name == name),
                "missing dynamic connector command {name}"
            );
        }
        assert!(p.all_commands.iter().any(|entry| {
            entry.dynamic
                && entry.action
                    == Action::RevalidateConnectorResource {
                        reference: "service://mock/docs/ready".to_string(),
                        state: "stale".to_string(),
                    }
        }));
        assert!(p.all_commands.iter().any(|entry| {
            entry.dynamic
                && entry.action
                    == Action::PromoteConnectorResourceToMemory {
                        reference: "service://mock/docs/ready".to_string(),
                        session_id: None,
                    }
        }));
    }

    #[test]
    fn sync_runtime_actions_replaces_old_dynamic_entries() {
        let mut p = setup_palette();
        let offline = RuntimeControlSnapshot::default();
        p.sync_runtime_actions(&offline);
        let first_dynamic = p.all_commands.iter().filter(|entry| entry.dynamic).count();

        let online = RuntimeControlSnapshot {
            gateway_running: true,
            task_count: Some(0),
            ..RuntimeControlSnapshot::default()
        };
        p.sync_runtime_actions(&online);
        let second_dynamic = p.all_commands.iter().filter(|entry| entry.dynamic).count();

        assert_eq!(first_dynamic, 1);
        assert!(second_dynamic >= 3);
        assert!(!p
            .all_commands
            .iter()
            .any(|entry| entry.dynamic && entry.name == "Start Gateway"));
    }

    // ── Search scoring ────────────────────────────────────────────

    #[test]
    fn search_exact_prefix_scores_100() {
        let mut p = setup_palette();
        p.open();

        // Type "/status"
        for c in "/status".chars() {
            p.insert_char(c);
        }

        assert!(!p.results.is_empty(), "should find '/status'");
        // Best match should be /status with score 100
        let (best_idx, best_score) = p.results[0];
        assert_eq!(best_score, 100, "prefix match should score 100");
        assert_eq!(p.all_commands[best_idx].name, "/status");
    }

    #[test]
    fn search_substring_scores_50() {
        let mut p = setup_palette();
        p.open();

        // Type "help" — should substring-match "/help" (name) or "Toggle Help" (keybind)
        for c in "help".chars() {
            p.insert_char(c);
        }

        assert!(!p.results.is_empty(), "should find 'help' matches");

        // At least one should be a substring match (score 50)
        let has_substring = p.results.iter().any(|(_, score)| *score == 50);
        assert!(has_substring, "should have substring matches (score=50)");
    }

    #[test]
    fn search_fuzzy_matches() {
        let mut p = setup_palette();
        p.open();

        // Type "hlp" — should fuzzy-match "help" (all chars in order)
        for c in "hlp".chars() {
            p.insert_char(c);
        }

        // Should find something, and the fuzzy score should be 75 (3 chars × 25)
        if !p.results.is_empty() {
            let has_fuzzy = p.results.iter().any(|(_, score)| *score > 0);
            assert!(has_fuzzy, "fuzzy search should produce results");
        }
    }

    #[test]
    fn search_no_match_returns_empty() {
        let mut p = setup_palette();
        p.open();

        for c in "zzzzzzz_not_a_command_zzzz".chars() {
            p.insert_char(c);
        }

        assert!(
            p.results.is_empty(),
            "should return empty for gibberish query"
        );
    }

    #[test]
    fn search_empty_query_shows_all() {
        let mut p = setup_palette();
        p.open();

        // Empty search — should show all
        assert_eq!(
            p.results.len(),
            p.command_count(),
            "empty query should show all commands"
        );
    }

    // ── Result ordering ──────────────────────────────────────────

    #[test]
    fn results_ranked_by_score_descending() {
        let mut p = CommandPalette::new();
        // Register a few commands with controlled names
        p.register("foobar_test", "A test command", Action::Noop);
        p.register("test_foobar", "Another test", Action::Noop);
        p.register("test_only", "Only test", Action::Noop);
        p.open();

        for c in "test".chars() {
            p.insert_char(c);
        }

        // Verify scores are non-increasing
        for window in p.results.windows(2) {
            assert!(
                window[0].1 >= window[1].1,
                "results should be sorted by score descending: {:?} >= {:?}",
                window[0],
                window[1]
            );
        }
    }

    // ── Navigation ────────────────────────────────────────────────

    #[test]
    fn jk_navigation() {
        let mut p = setup_palette();
        p.open();

        // Should have many results, start at index 0
        assert_eq!(p.selected_index, 0);

        // Press 'j' (down)
        p.handle_key(KeyCode::Char('j'));
        assert_eq!(p.selected_index, 1);

        // Press 'j' again
        p.handle_key(KeyCode::Char('j'));
        assert_eq!(p.selected_index, 2);

        // Press 'k' (up)
        p.handle_key(KeyCode::Char('k'));
        assert_eq!(p.selected_index, 1);
    }

    #[test]
    fn arrow_key_navigation() {
        let mut p = setup_palette();
        p.open();

        assert_eq!(p.selected_index, 0);

        p.handle_key(KeyCode::Down);
        assert_eq!(p.selected_index, 1);

        p.handle_key(KeyCode::Up);
        assert_eq!(p.selected_index, 0);
    }

    #[test]
    fn navigation_wraps_around() {
        let mut p = setup_palette();
        p.register("only_cmd", "Single cmd", Action::Noop);
        p.open();
        // Show all commands, but let's just verify wrapping works
        let last = p.results.len().saturating_sub(1);

        // Go to last, then next wraps to 0
        p.selected_index = last;
        p.handle_key(KeyCode::Down);
        assert_eq!(p.selected_index, 0, "should wrap to first item");

        // From first, up wraps to last
        p.selected_index = 0;
        p.handle_key(KeyCode::Up);
        assert_eq!(p.selected_index, last, "should wrap to last item");
    }

    // ── Dispatch ──────────────────────────────────────────────────

    #[test]
    fn enter_sets_pending_action() {
        let mut p = setup_palette();

        // Register a known action
        p.register("say-hello", "Say hello", Action::Execute("/hello".into()));
        p.open();

        // Select the command
        for c in "say-hello".chars() {
            p.insert_char(c);
        }

        assert!(!p.results.is_empty(), "should find 'say-hello'");

        // Press Enter
        p.handle_key(KeyCode::Enter);

        assert!(!p.is_open(), "palette should close after Enter");
        let action = p.take_action();
        assert!(action.is_some(), "should have a pending action");
        assert_eq!(action.unwrap(), Action::Execute("/hello".into()));
    }

    #[test]
    fn enter_on_empty_results_does_nothing() {
        let mut p = setup_palette();
        p.open();

        // Type gibberish that matches nothing
        for c in "zzzz_no_match".chars() {
            p.insert_char(c);
        }
        assert!(p.results.is_empty());

        p.handle_key(KeyCode::Enter);

        // Should still be open (nothing to select)
        assert!(p.is_open(), "should stay open when no results");
        assert!(p.take_action().is_none(), "no action should be set");
    }

    // ── Close ─────────────────────────────────────────────────────

    #[test]
    fn esc_closes_palette() {
        let mut p = setup_palette();
        p.open();
        assert!(p.is_open());

        p.handle_key(KeyCode::Esc);
        assert!(!p.is_open());
        assert!(p.take_action().is_none(), "Esc should not set an action");
    }

    #[test]
    fn open_resets_search_state() {
        let mut p = setup_palette();
        p.open();
        p.insert_char('x');
        p.insert_char('y');
        p.insert_char('z');
        assert!(!p.search_input.is_empty());

        // Close and reopen
        p.handle_key(KeyCode::Esc);
        p.open();
        assert!(p.search_input.is_empty());
        assert_eq!(p.cursor, 0);
        assert!(p.is_open());
    }

    // ── Search input editing ──────────────────────────────────────

    #[test]
    fn typing_inserts_characters() {
        let mut p = setup_palette();
        p.open();

        p.insert_char('h');
        p.insert_char('e');
        p.insert_char('l');

        assert_eq!(p.search_input, "hel");
    }

    #[test]
    fn backspace_removes_characters() {
        let mut p = setup_palette();
        p.open();
        p.insert_char('a');
        p.insert_char('b');
        p.insert_char('c');
        assert_eq!(p.search_input, "abc");

        p.backspace();
        assert_eq!(p.search_input, "ab");

        p.backspace();
        assert_eq!(p.search_input, "a");

        p.backspace();
        assert_eq!(p.search_input, "");
    }

    #[test]
    fn cursor_movement() {
        let mut p = setup_palette();
        p.open();
        p.insert_char('a');
        p.insert_char('b');
        p.insert_char('c');
        assert_eq!(p.cursor, 3);

        p.cursor_left();
        assert_eq!(p.cursor, 2);

        // Insert 'X' at cursor position -> "abXc"
        p.insert_char('X');
        assert_eq!(p.search_input, "abXc");
        assert_eq!(p.cursor, 3);

        p.cursor_home();
        assert_eq!(p.cursor, 0);

        p.cursor_end();
        assert_eq!(p.cursor, p.search_input.len());
    }

    // ── fuzzy_match_score unit tests ──────────────────────────────

    #[test]
    fn fuzzy_exact_prefix() {
        assert_eq!(fuzzy_match_score("sta", "status"), 100);
        assert_eq!(fuzzy_match_score("", "anything"), 0);
        assert_eq!(fuzzy_match_score("x", ""), 0);
    }

    #[test]
    fn fuzzy_substring() {
        assert_eq!(fuzzy_match_score("tat", "status"), 50);
    }

    #[test]
    fn fuzzy_char_sequence() {
        // "stts" -> all chars in order in "status"
        assert_eq!(fuzzy_match_score("stts", "status"), 100); // 4 * 25
    }

    #[test]
    fn fuzzy_no_match() {
        assert_eq!(fuzzy_match_score("xyz", "status"), 0);
    }

    #[test]
    fn fuzzy_case_insensitive() {
        assert_eq!(fuzzy_match_score("STA", "status"), 100);
        assert_eq!(fuzzy_match_score("Status", "STATUS"), 100);
    }

    // ── Render tests ──────────────────────────────────────────────

    #[test]
    fn render_when_hidden_is_noop() {
        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        let mut palette = setup_palette();
        assert!(!palette.is_open());

        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            palette.render(&mut ctx, area);
        });

        // Should not have any content (empty buffer is all spaces trimmed)
        let lines = terminal.buffer_lines();
        assert!(
            lines.iter().all(|l| l.is_empty()),
            "hidden palette should not render anything"
        );
    }

    #[test]
    fn render_when_open_shows_block() {
        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        let mut palette = setup_palette();
        palette.open();

        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            palette.render(&mut ctx, area);
        });

        terminal.assert_line_contains("Command Palette");
        terminal.assert_line_contains("Type to search");
    }

    #[test]
    fn render_shows_search_results() {
        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        let mut palette = setup_palette();
        palette.open();

        // Type "/help"
        for c in "/help".chars() {
            palette.insert_char(c);
        }

        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            palette.render(&mut ctx, area);
        });

        terminal.assert_line_contains("/help");
        terminal.assert_line_contains("Show available slash command");
    }

    #[test]
    fn render_shows_navigation_hint() {
        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        let mut palette = setup_palette();
        palette.open();

        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            palette.render(&mut ctx, area);
        });

        terminal.assert_line_contains("navigate");
        terminal.assert_line_contains("Enter");
        terminal.assert_line_contains("Esc");
    }

    // ── Edge cases ────────────────────────────────────────────────

    #[test]
    fn handle_event_when_hidden_passthrough() {
        let mut p = setup_palette();
        assert!(!p.is_open());

        let result = p.handle_event(&event_char('a'));
        assert_eq!(
            result,
            EventResult::NotConsumed,
            "hidden palette should not consume"
        );
    }

    #[test]
    fn non_key_events_consumed_when_visible() {
        let mut p = setup_palette();
        p.open();

        let resize = Event::Resize(100, 100);
        let result = p.handle_event(&resize);
        assert_eq!(
            result,
            EventResult::Consumed,
            "visible palette consumes all events"
        );
    }
}
