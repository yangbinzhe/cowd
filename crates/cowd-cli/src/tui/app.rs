#![allow(dead_code)]
use tui_textarea::TextArea;
use ratatui::widgets::{Block, Borders};
use crate::tui::TuiEvent;

#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct ToolCard {
    pub id: String,
    pub name: String,
    pub output: String,
    pub done: bool,
    pub expanded: bool,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct SessionSummary {
    pub id: String,
    pub path: String,
    pub updated_at_ms: u64,
    pub message_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Panel { Chat, Gateway, Files, Delegate, Memory, Skills }

pub struct App {
    pub model: String,
    pub session_id: String,
    pub messages: Vec<ChatMessage>,
    pub input: TextArea<'static>,
    pub is_loading: bool,
    pub spinner_idx: usize,
    pub should_quit: bool,
    pub tool_cards: Vec<ToolCard>,
    pub token_count: u64,
    pub cost_estimate: Option<f64>,
    pub compaction_count: u32,
    pub cache_hits: u64,
    pub picker_active: bool,
    pub picker_sessions: Vec<SessionSummary>,
    pub picker_idx: usize,
    pub theme: Theme,
    pub approval: Option<ApprovalRequest>,
    pub current_panel: Panel,
    pub gateway_sessions: Vec<GatewaySession>,
    pub gateway_platform: String,
    pub file_entries: Vec<FileEntry>,
    pub delegate_tasks: Vec<DelegateTask>,
    pub memory_entries: Vec<MemoryEntry>,
    pub skill_list: Vec<SkillSummary>,
    pub skin: crate::tui::skin::SkinConfig,
    pub memory_status: Option<String>,
    pub scroll_offset: u16,
    pub auto_scroll: bool,
    pub turn_active: bool,
    pub streaming_thinking: String,
    pub thinking_expanded: bool,
    pub thinking_complete: bool,
    pub thinking_scroll_offset: u16,
    pub thinking_auto_scroll: bool,
    streaming_received: bool,
    pub msg_version: u64,
    pub last_drawn_version: u64,
    pub context_window: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct MemoryEntry {
    pub layer: String,
    pub content: String,
    pub priority: String,
}

#[derive(Debug, Clone)]
pub struct GatewaySession {
    pub platform: String,
    pub id: String,
    pub title: String,
    pub message_count: usize,
}

#[derive(Debug, Clone)]
pub struct SkillSummary {
    pub name: String,
    pub description: String,
    pub installed: bool,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct DelegateTask {
    pub id: String,
    pub description: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub tool_name: String,
    pub input_preview: String,
    pub approved: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Theme { Dark, Light }

impl Theme {
    pub fn bg(&self) -> ratatui::style::Color {
        match self { Self::Dark => ratatui::style::Color::Black, Self::Light => ratatui::style::Color::White }
    }
    pub fn fg(&self) -> ratatui::style::Color {
        match self { Self::Dark => ratatui::style::Color::White, Self::Light => ratatui::style::Color::Black }
    }
    pub fn accent(&self) -> ratatui::style::Color {
        match self { Self::Dark => ratatui::style::Color::Cyan, Self::Light => ratatui::style::Color::Blue }
    }
    pub fn user_color(&self) -> ratatui::style::Color {
        match self { Self::Dark => ratatui::style::Color::Green, Self::Light => ratatui::style::Color::DarkGray }
    }
    pub fn toggle(&mut self) { *self = match self { Self::Dark => Self::Light, Self::Light => Self::Dark }; }
}

impl App {
    pub fn new(model: &str, session_id: &str) -> Self {
        let mut input = TextArea::default();
        input.set_block(Block::default().borders(Borders::ALL).title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "));
        input.set_style(ratatui::style::Style::default().fg(ratatui::style::Color::White));

        Self {
            model: model.to_string(),
            session_id: session_id.to_string(),
            messages: Vec::new(),
            input,
            is_loading: false,
            spinner_idx: 0,
            should_quit: false,
            tool_cards: Vec::new(),
            token_count: 0,
            cost_estimate: None,
            compaction_count: 0,
            cache_hits: 0,
            picker_active: false,
            picker_sessions: Vec::new(),
            picker_idx: 0,
            theme: Theme::Dark,
            approval: None,
            current_panel: Panel::Chat,
            gateway_sessions: Vec::new(),
            gateway_platform: String::new(),
            file_entries: Vec::new(),
            delegate_tasks: Vec::new(),
            memory_entries: Vec::new(),
            skill_list: Vec::new(),
            skin: crate::tui::skin::SkinConfig::default(),
            memory_status: None,
            scroll_offset: 0,
            auto_scroll: true,
            turn_active: false,
            streaming_thinking: String::new(),
            thinking_expanded: false,
            thinking_complete: false,
            thinking_scroll_offset: 0,
            thinking_auto_scroll: true,
            streaming_received: false,
            msg_version: 0,
            last_drawn_version: u64::MAX,
            context_window: 0,
            input_tokens: 0,
            output_tokens: 0,
        }
    }

    pub fn next_panel(&mut self) {
        self.current_panel = match self.current_panel {
            Panel::Chat => Panel::Gateway,
            Panel::Gateway => Panel::Files,
            Panel::Files => Panel::Memory,
            Panel::Memory => Panel::Skills,
            Panel::Skills => Panel::Delegate,
            Panel::Delegate => Panel::Chat,
        };
    }

    pub fn spinner_char(&self) -> &'static str {
        const F: &[&str] = &["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"];
        F[self.spinner_idx % F.len()]
    }

    pub fn tick(&mut self) { self.spinner_idx = self.spinner_idx.wrapping_add(1); }

    pub fn add_message(&mut self, role: &str, content: &str) {
        self.messages.push(ChatMessage { role: role.to_string(), content: content.to_string() });
        self.msg_version = self.msg_version.wrapping_add(1);
        const MAX: usize = 2000;
        if self.messages.len() > MAX {
            let excess = self.messages.len() - MAX;
            self.messages.drain(0..excess);
            self.scroll_offset = self.scroll_offset.saturating_sub(excess as u16);
        }
    }

    pub fn add_tool_card(&mut self, id: &str, name: &str) {
        const MAX_CARDS: usize = 200;
        if self.tool_cards.len() >= MAX_CARDS {
            self.tool_cards.remove(0);
        }
        self.tool_cards.push(ToolCard {
            id: id.to_string(), name: name.to_string(), output: String::new(),
            done: false, expanded: true, exit_code: None,
        });
    }

    pub fn update_tool_card(&mut self, id: &str, output: &str, exit_code: Option<i32>) {
        if let Some(card) = self.tool_cards.iter_mut().find(|c| c.id == id) {
            card.output = output.to_string();
            card.done = true;
            card.exit_code = exit_code;
        }
    }

    pub fn open_session_picker(&mut self, sessions: Vec<SessionSummary>) {
        self.picker_sessions = sessions;
        self.picker_idx = 0;
        self.picker_active = true;
    }

    pub fn close_session_picker(&mut self) {
        self.picker_active = false;
        self.picker_sessions.clear();
        self.picker_idx = 0;
    }

    pub fn picker_up(&mut self) {
        if self.picker_idx > 0 { self.picker_idx -= 1; }
    }

    pub fn picker_down(&mut self) {
        if self.picker_idx + 1 < self.picker_sessions.len() { self.picker_idx += 1; }
    }

    pub fn picker_selected_id(&self) -> Option<&str> {
        self.picker_sessions.get(self.picker_idx).map(|s| s.id.as_str())
    }

    /// Apply a TuiEvent from the background turn runner to the display state.
    pub fn apply_event(&mut self, event: TuiEvent) {
        match event {
            TuiEvent::TextDelta { text } => {
                self.streaming_received = true;
                self.auto_scroll = true;
                if let Some(last) = self.messages.last_mut() {
                    if last.role == "assistant" && last.content != "✓ Done" {
                        last.content.push_str(&text);
                        return;
                    }
                }
                self.add_message("assistant", &text);
            }
            TuiEvent::ThinkingDelta { thinking } => {
                self.streaming_thinking.push_str(&thinking);
            }
            TuiEvent::ThinkingComplete => {
                self.thinking_complete = true;
            }
            TuiEvent::ToolStart { id, name, preview: _ } => {
                self.auto_scroll = true;
                self.add_tool_card(&id, &name);
            }
            TuiEvent::ToolProgress { id, name: _, progress } => {
                if let Some(card) = self.tool_cards.iter_mut().find(|c| c.id == id) {
                    card.done = false;
                    card.output.push_str(&progress);
                    if card.output.len() > 4096 {
                        card.output = card.output[card.output.len() - 4096..].to_string();
                    }
                }
            }
            TuiEvent::ToolComplete { id, name: _, summary, exit_code } => {
                self.update_tool_card(&id, &summary, exit_code);
            }
            TuiEvent::TokenUsage { input, output, cache_create, cache_read } => {
                self.input_tokens = input;
                self.output_tokens = output;
                self.token_count = input + output + cache_create + cache_read;
            }
            TuiEvent::CostEstimate { cost_usd } => {
                self.cost_estimate = Some(cost_usd);
            }
            TuiEvent::TurnStarted => {
                self.is_loading = true;
                self.turn_active = true;
                self.streaming_thinking.clear();
                self.thinking_complete = false;
                self.thinking_expanded = false;
                self.thinking_scroll_offset = 0;
                self.thinking_auto_scroll = true;
                self.streaming_received = false;
                self.tool_cards.clear();
            }
            TuiEvent::TurnComplete { assistant_text, iterations: _ } => {
                self.is_loading = false;
                self.turn_active = false;
                self.thinking_complete = true;
                if !assistant_text.is_empty() && !self.streaming_received {
                    self.add_message("assistant", &assistant_text);
                }
                self.add_message("assistant", "✓ Done");
            }
            TuiEvent::TurnError { error } => {
                self.is_loading = false;
                self.turn_active = false;
                self.add_message("system", &format!("Error: {error}"));
            }
            TuiEvent::CompactionNotice { removed_count } => {
                self.compaction_count += 1;
                self.add_message("system", &format!("Compacted {removed_count} earlier messages to save context."));
            }
        }
    }
}
