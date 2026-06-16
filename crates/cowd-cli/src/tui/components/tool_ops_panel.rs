use std::collections::BTreeMap;

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::components::{Component, EventResult, RenderContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOpsMode {
    Registry,
    Operations,
    Mutations,
    Checkpoints,
    Cache,
    Ledger,
    Risk,
}

impl ToolOpsMode {
    fn label(self) -> &'static str {
        match self {
            Self::Registry => "Registry",
            Self::Operations => "Operations",
            Self::Mutations => "Mutations",
            Self::Checkpoints => "Checkpoints",
            Self::Cache => "Cache",
            Self::Ledger => "Ledger",
            Self::Risk => "Risk",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolOpsArmedAction {
    ApplyMutation,
    RestoreCheckpoint(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRow {
    pub name: String,
    pub safety: String,
    pub cache: String,
    pub readonly: bool,
    pub concurrency: String,
    pub tags: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointRow {
    pub id: String,
    pub label: String,
    pub files: String,
    pub created: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValueRow {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolLedgerRow {
    pub sequence: String,
    pub kind: String,
    pub status: String,
    pub tool: String,
    pub at: String,
}

#[derive(Debug, Clone)]
pub struct ToolOpsPanel {
    pub mode: ToolOpsMode,
    pub registry: Vec<ToolRow>,
    pub checkpoints: Vec<CheckpointRow>,
    pub cache_rows: Vec<KeyValueRow>,
    pub ledger_rows: Vec<ToolLedgerRow>,
    pub selected: usize,
    pub intent_prompt: String,
    pub fanout_prompt: String,
    pub edits_buffer: String,
    pub batch_buffer: String,
    pub expected_hashes: BTreeMap<String, String>,
    pub last_receipt: Option<serde_json::Value>,
    pub armed_action: Option<ToolOpsArmedAction>,
    pub status: String,
}

impl Default for ToolOpsPanel {
    fn default() -> Self {
        Self {
            mode: ToolOpsMode::Registry,
            registry: Vec::new(),
            checkpoints: Vec::new(),
            cache_rows: Vec::new(),
            ledger_rows: Vec::new(),
            selected: 0,
            intent_prompt: "inspect current workspace".to_string(),
            fanout_prompt: "collect safe context for current workspace".to_string(),
            edits_buffer: "[]".to_string(),
            batch_buffer: r#"[{"name":"tool_cache_stats","input":{}}]"#.to_string(),
            expected_hashes: BTreeMap::new(),
            last_receipt: None,
            armed_action: None,
            status: "degraded: gateway data not loaded".to_string(),
        }
    }
}

impl ToolOpsPanel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_mode(&mut self, mode: ToolOpsMode) {
        self.mode = mode;
        self.selected = 0;
        self.armed_action = None;
    }

    pub fn set_status(&mut self, status: impl Into<String>) {
        self.status = status.into();
    }

    pub fn selected_tool_name(&self) -> Option<&str> {
        self.registry
            .get(self.selected)
            .map(|row| row.name.as_str())
    }

    pub fn selected_checkpoint_id(&self) -> Option<&str> {
        self.checkpoints
            .get(self.selected)
            .map(|row| row.id.as_str())
    }

    pub fn sync_registry(&mut self, payload: &serde_json::Value) {
        let tools = payload
            .get("tools")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        self.registry = tools
            .into_iter()
            .map(|tool| ToolRow {
                name: text_at(&tool, "name"),
                safety: text_at(&tool, "safety_category"),
                cache: text_at(&tool, "cache_policy"),
                readonly: tool
                    .get("prepared_readonly_supported")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                concurrency: text_at(&tool, "max_concurrency"),
                tags: tool
                    .get("managed_tags")
                    .and_then(serde_json::Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .unwrap_or_else(|| "-".to_string()),
            })
            .collect();
        self.clamp_selection();
        self.status = format!("registry loaded: {} tools", self.registry.len());
    }

    pub fn sync_cache(&mut self, payload: &serde_json::Value) {
        let data = payload.get("data").unwrap_or(payload);
        self.cache_rows = data
            .as_object()
            .map(|object| {
                object
                    .iter()
                    .filter(|(key, _)| !key.starts_with("__"))
                    .map(|(key, value)| KeyValueRow {
                        key: key.clone(),
                        value: compact_value(value),
                    })
                    .collect()
            })
            .unwrap_or_default();
        self.status = format!("cache loaded: {} metrics", self.cache_rows.len());
    }

    pub fn sync_checkpoints(&mut self, payload: &serde_json::Value) {
        let data = payload.get("data").unwrap_or(payload);
        let checkpoints = data
            .get("checkpoints")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        self.checkpoints = checkpoints
            .into_iter()
            .map(|checkpoint| CheckpointRow {
                id: text_at(&checkpoint, "id"),
                label: text_at(&checkpoint, "label"),
                files: text_at(&checkpoint, "file_count"),
                created: text_at(&checkpoint, "created_at"),
            })
            .collect();
        self.clamp_selection();
        self.status = format!("checkpoints loaded: {}", self.checkpoints.len());
    }

    pub fn sync_ledger(&mut self, payload: &serde_json::Value) {
        let events = payload
            .get("events")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        self.ledger_rows = events
            .into_iter()
            .filter(|event| {
                event
                    .get("kind")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .contains("tool")
            })
            .map(|event| ToolLedgerRow {
                sequence: text_at(&event, "sequence"),
                kind: text_at(&event, "kind"),
                status: text_at(&event, "status"),
                tool: text_at(&event, "tool_name"),
                at: text_at(&event, "timestamp"),
            })
            .collect();
        self.clamp_selection();
        self.status = format!("ledger loaded: {} tool events", self.ledger_rows.len());
    }

    pub fn record_receipt(&mut self, payload: serde_json::Value) {
        self.extract_expected_hashes(&payload);
        self.last_receipt = Some(payload);
        self.armed_action = None;
        self.status = "receipt recorded".to_string();
    }

    fn extract_expected_hashes(&mut self, payload: &serde_json::Value) {
        let files = payload
            .get("data")
            .and_then(|data| data.get("files"))
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut hashes = BTreeMap::new();
        for file in files {
            if let (Some(path), Some(hash)) = (
                file.get("path").and_then(serde_json::Value::as_str),
                file.get("expected_hash")
                    .or_else(|| file.get("expectedHash"))
                    .and_then(serde_json::Value::as_str),
            ) {
                hashes.insert(path.to_string(), hash.to_string());
            }
        }
        if !hashes.is_empty() {
            self.expected_hashes = hashes;
        }
    }

    pub fn arm_apply_mutation(&mut self) -> bool {
        if self.armed_action == Some(ToolOpsArmedAction::ApplyMutation) {
            self.armed_action = None;
            true
        } else {
            self.armed_action = Some(ToolOpsArmedAction::ApplyMutation);
            self.status = "Press A again to confirm mutation apply".to_string();
            false
        }
    }

    pub fn arm_restore_checkpoint(&mut self, id: impl Into<String>) -> bool {
        let id = id.into();
        if self.armed_action == Some(ToolOpsArmedAction::RestoreCheckpoint(id.clone())) {
            self.armed_action = None;
            true
        } else {
            self.armed_action = Some(ToolOpsArmedAction::RestoreCheckpoint(id));
            self.status = "Press R again to confirm checkpoint restore".to_string();
            false
        }
    }

    pub fn clear_armed(&mut self) {
        self.armed_action = None;
    }

    fn current_len(&self) -> usize {
        match self.mode {
            ToolOpsMode::Registry => self.registry.len(),
            ToolOpsMode::Checkpoints => self.checkpoints.len(),
            ToolOpsMode::Cache => self.cache_rows.len(),
            ToolOpsMode::Ledger => self.ledger_rows.len(),
            ToolOpsMode::Operations | ToolOpsMode::Mutations | ToolOpsMode::Risk => 1,
        }
    }

    fn clamp_selection(&mut self) {
        let len = self.current_len();
        if len == 0 {
            self.selected = 0;
        } else if self.selected >= len {
            self.selected = len - 1;
        }
    }

    fn move_down(&mut self) {
        self.clear_armed();
        let len = self.current_len();
        if len > 0 {
            self.selected = (self.selected + 1).min(len - 1);
        }
    }

    fn move_up(&mut self) {
        self.clear_armed();
        self.selected = self.selected.saturating_sub(1);
    }

    fn lines(&self) -> Vec<Line<'static>> {
        let mut lines =
            vec![
            Line::from(vec![
                Span::styled("Mode: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.mode.label(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(Span::styled(
                "r Registry | o Ops | m Mutations | c Checkpoints | a Cache | l Ledger | p Risk | U refresh",
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(Span::styled(self.status.clone(), Style::default().fg(Color::Yellow))),
        ];

        match self.mode {
            ToolOpsMode::Registry => {
                lines.push(Line::from(Span::styled(
                    "x execute selected as read-only",
                    Style::default().fg(Color::DarkGray),
                )));
                push_rows(
                    &mut lines,
                    &self.registry,
                    self.selected,
                    "No tools loaded",
                    |row| {
                        format!(
                            "{} safety:{} cache:{} readonly:{} tags:{}",
                            row.name, row.safety, row.cache, row.readonly, row.tags
                        )
                    },
                );
            }
            ToolOpsMode::Operations => {
                lines.push(Line::from(Span::styled(
                    "i intent plan · f fanout plan · b batch readonly",
                    Style::default().fg(Color::DarkGray),
                )));
                lines.push(Line::from(format!("Intent: {}", self.intent_prompt)));
                lines.push(Line::from(format!("Fanout: {}", self.fanout_prompt)));
                lines.push(Line::from(format!(
                    "Batch: {}",
                    preview(&self.batch_buffer, 96)
                )));
            }
            ToolOpsMode::Mutations => {
                lines.push(Line::from(Span::styled(
                    "v preview · A apply with second confirmation",
                    Style::default().fg(Color::DarkGray),
                )));
                lines.push(Line::from(format!(
                    "Edits: {}",
                    preview(&self.edits_buffer, 120)
                )));
                lines.push(Line::from(format!(
                    "Expected hashes: {}",
                    self.expected_hashes.len()
                )));
                if self.armed_action == Some(ToolOpsArmedAction::ApplyMutation) {
                    lines.push(Line::from(Span::styled(
                        "Armed: mutation apply",
                        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                    )));
                }
            }
            ToolOpsMode::Checkpoints => {
                lines.push(Line::from(Span::styled(
                    "n create · d diff · R restore with second confirmation",
                    Style::default().fg(Color::DarkGray),
                )));
                push_rows(
                    &mut lines,
                    &self.checkpoints,
                    self.selected,
                    "No checkpoints loaded",
                    |row| {
                        format!(
                            "{} {} files:{} {}",
                            row.id, row.label, row.files, row.created
                        )
                    },
                );
                if matches!(
                    self.armed_action,
                    Some(ToolOpsArmedAction::RestoreCheckpoint(_))
                ) {
                    lines.push(Line::from(Span::styled(
                        "Armed: checkpoint restore",
                        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                    )));
                }
            }
            ToolOpsMode::Cache => {
                push_rows(
                    &mut lines,
                    &self.cache_rows,
                    self.selected,
                    "No cache stats",
                    |row| format!("{}: {}", row.key, row.value),
                );
            }
            ToolOpsMode::Ledger => {
                push_rows(
                    &mut lines,
                    &self.ledger_rows,
                    self.selected,
                    "No tool ledger events",
                    |row| format!("#{} {} {} {}", row.sequence, row.kind, row.status, row.tool),
                );
            }
            ToolOpsMode::Risk => {
                lines.push(Line::from("Risk: policy simulate / preflight"));
                lines.push(Line::from("s simulate policy · p run preflight"));
            }
        }

        if let Some(receipt) = &self.last_receipt {
            lines.push(Line::from(format!(
                "Receipt: {} {}",
                text_at(receipt, "tool_name"),
                text_at(receipt, "status")
            )));
        }
        lines
    }
}

impl Component for ToolOpsPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Tools Ops ");
        ctx.frame_mut().render_widget(
            Paragraph::new(self.lines())
                .block(block)
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        if key.kind != KeyEventKind::Press {
            return EventResult::NotConsumed;
        }
        match key.code {
            KeyCode::Char('r') => self.set_mode(ToolOpsMode::Registry),
            KeyCode::Char('o') => self.set_mode(ToolOpsMode::Operations),
            KeyCode::Char('m') => self.set_mode(ToolOpsMode::Mutations),
            KeyCode::Char('c') => self.set_mode(ToolOpsMode::Checkpoints),
            KeyCode::Char('a') => self.set_mode(ToolOpsMode::Cache),
            KeyCode::Char('l') => self.set_mode(ToolOpsMode::Ledger),
            KeyCode::Char('p') => self.set_mode(ToolOpsMode::Risk),
            KeyCode::Char('j') | KeyCode::Down => self.move_down(),
            KeyCode::Char('k') | KeyCode::Up => self.move_up(),
            KeyCode::Char('g') => {
                self.clear_armed();
                self.selected = 0;
            }
            KeyCode::Char('G') => {
                self.clear_armed();
                self.selected = self.current_len().saturating_sub(1);
            }
            KeyCode::Esc => self.clear_armed(),
            _ => return EventResult::NotConsumed,
        }
        EventResult::Consumed
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "tool_ops_panel"
    }
}

fn push_rows<T>(
    lines: &mut Vec<Line<'static>>,
    rows: &[T],
    selected: usize,
    empty: &'static str,
    format: impl Fn(&T) -> String,
) {
    if rows.is_empty() {
        lines.push(Line::from(Span::styled(
            empty,
            Style::default().fg(Color::DarkGray),
        )));
        return;
    }
    for (idx, row) in rows.iter().take(12).enumerate() {
        let marker = if idx == selected { "> " } else { "  " };
        let color = if idx == selected {
            Color::Cyan
        } else {
            Color::White
        };
        lines.push(Line::from(vec![
            Span::styled(marker, Style::default().fg(Color::DarkGray)),
            Span::styled(format(row), Style::default().fg(color)),
        ]));
    }
}

fn text_at(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .map(compact_value)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "-".to_string())
}

fn compact_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Null => "-".to_string(),
        other => preview(&other.to_string(), 80),
    }
}

fn preview(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(3)).collect();
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::components::base::Component;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    fn render_panel(panel: &mut ToolOpsPanel, width: u16, height: u16) -> String {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|frame| {
            let mut ctx = RenderContext::new(frame, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines().join("\n")
    }

    #[test]
    fn tool_ops_panel_switches_modes_and_clamps_selection() {
        let mut panel = ToolOpsPanel::new();
        panel.sync_registry(&serde_json::json!({
            "tools": [
                { "name": "read_file", "safety_category": "read_only", "cache_policy": "stable", "prepared_readonly_supported": true, "max_concurrency": 8, "managed_tags": ["fs"] },
                { "name": "tool_cache_stats", "safety_category": "read_only", "cache_policy": "volatile", "prepared_readonly_supported": true, "max_concurrency": 1, "managed_tags": ["cache"] }
            ]
        }));
        panel.handle_event(&Event::Key(crossterm::event::KeyEvent::new(
            KeyCode::Down,
            crossterm::event::KeyModifiers::NONE,
        )));
        assert_eq!(panel.selected_tool_name(), Some("tool_cache_stats"));
        panel.handle_event(&Event::Key(crossterm::event::KeyEvent::new(
            KeyCode::Char('c'),
            crossterm::event::KeyModifiers::NONE,
        )));
        assert_eq!(panel.mode, ToolOpsMode::Checkpoints);
        assert_eq!(panel.selected, 0);
    }

    #[test]
    fn tool_ops_panel_requires_second_confirmation_for_dangerous_actions() {
        let mut panel = ToolOpsPanel::new();
        assert!(!panel.arm_apply_mutation());
        assert_eq!(panel.armed_action, Some(ToolOpsArmedAction::ApplyMutation));
        assert!(panel.arm_apply_mutation());
        assert_eq!(panel.armed_action, None);

        assert!(!panel.arm_restore_checkpoint("cp-1"));
        assert_eq!(
            panel.armed_action,
            Some(ToolOpsArmedAction::RestoreCheckpoint("cp-1".to_string()))
        );
        panel.handle_event(&Event::Key(crossterm::event::KeyEvent::new(
            KeyCode::Esc,
            crossterm::event::KeyModifiers::NONE,
        )));
        assert_eq!(panel.armed_action, None);
    }

    #[test]
    fn tool_ops_panel_renders_structured_sections_without_raw_json_primary_view() {
        let mut panel = ToolOpsPanel::new();
        panel.sync_cache(&serde_json::json!({ "data": { "hits": 2, "misses": 1 } }));
        panel.set_mode(ToolOpsMode::Cache);
        let rendered = render_panel(&mut panel, 96, 20);
        assert!(rendered.contains("Tools Ops"));
        assert!(rendered.contains("Mode: Cache"));
        assert!(rendered.contains("hits: 2"));
        assert!(!rendered.contains("{\"hits\""));
    }

    #[test]
    fn tool_ops_panel_extracts_expected_hashes_from_preview_receipt() {
        let mut panel = ToolOpsPanel::new();
        panel.record_receipt(serde_json::json!({
            "tool_name": "mutation_preview",
            "status": "ok",
            "data": {
                "files": [
                    { "path": "README.md", "expected_hash": "hash-1" }
                ]
            }
        }));
        assert_eq!(
            panel.expected_hashes.get("README.md").map(String::as_str),
            Some("hash-1")
        );
    }
}
