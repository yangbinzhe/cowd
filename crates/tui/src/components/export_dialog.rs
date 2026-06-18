// ── Export Dialog Component ───────────────────────────────────────────
// Configurable export dialog with filename textbox and checkboxes for
// thinking, tools, and metadata inclusion.
//
// Tab=switch focus, Space=toggle, Enter=confirm, Esc=cancel
// -----------------------------------------------------------------

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

use crate::components::base::{Component, EventResult, RenderContext};

/// Export options returned when the dialog is confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportOptions {
    pub filename: String,
    pub include_thinking: bool,
    pub include_tools: bool,
    pub include_metadata: bool,
}

/// Export Dialog: filename textbox + checkbox options.
///
/// # Key bindings
/// | Key | Action |
/// |-----|--------|
/// | Tab | Switch focus (filename ↔ checkbox group) |
/// | Space | Toggle checkbox |
/// | Enter | Confirm + return options |
/// | Esc | Cancel |
pub struct ExportDialog {
    /// Current filename input.
    filename: String,
    /// Include thinking blocks.
    include_thinking: bool,
    /// Include tool call outputs.
    include_tools: bool,
    /// Include metadata (timestamps, token counts).
    include_metadata: bool,
    /// Focus index: 0=filename, 1=thinking, 2=tools, 3=metadata
    focus: usize,
    /// Set to Some(options) when confirmed, None when active.
    pub result: Option<ExportOptions>,
    /// Set to true when cancelled.
    pub cancelled: bool,
}

impl ExportDialog {
    /// Create a new export dialog with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            filename: "session.md".to_string(),
            include_thinking: true,
            include_tools: true,
            include_metadata: true,
            focus: 0,
            result: None,
            cancelled: false,
        }
    }

    /// Reset the dialog state for re-use.
    pub fn reset(&mut self) {
        self.filename = "session.md".to_string();
        self.include_thinking = true;
        self.include_tools = true;
        self.include_metadata = true;
        self.focus = 0;
        self.result = None;
        self.cancelled = false;
    }

    fn advance_focus(&mut self) {
        self.focus = (self.focus + 1) % 4;
    }

    fn prev_focus(&mut self) {
        self.focus = if self.focus == 0 { 3 } else { self.focus - 1 };
    }

    fn toggle_current(&mut self) {
        match self.focus {
            1 => self.include_thinking = !self.include_thinking,
            2 => self.include_tools = !self.include_tools,
            3 => self.include_metadata = !self.include_metadata,
            _ => {}
        }
    }

    fn confirm(&mut self) {
        self.result = Some(ExportOptions {
            filename: self.filename.clone(),
            include_thinking: self.include_thinking,
            include_tools: self.include_tools,
            include_metadata: self.include_metadata,
        });
    }

    fn cancel(&mut self) {
        self.cancelled = true;
    }
}

impl Default for ExportDialog {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for ExportDialog {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if self.result.is_some() || self.cancelled {
            return; // Dialog already dismissed
        }

        let accent = ctx.theme().accent_color();

        // Backdrop
        ctx.frame_mut().render_widget(Clear, area);
        let dim_bg = Style::default().bg(Color::Rgb(20, 20, 20));
        ctx.frame_mut()
            .render_widget(Paragraph::new("").style(dim_bg), area);

        // Compute dialog rect (centered)
        let w = 50u16.min(area.width.saturating_sub(4));
        let h = 14u16;
        let x = area.x + (area.width.saturating_sub(w)) / 2;
        let y = area.y + (area.height.saturating_sub(h)) / 2;
        let dialog_rect = Rect::new(x, y, w, h);

        ctx.frame_mut().render_widget(Clear, dialog_rect);

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Export Options ")
            .fg(accent);
        let inner = block.inner(dialog_rect);
        ctx.frame_mut().render_widget(block, dialog_rect);

        // Filename row
        let filename_label = if self.focus == 0 {
            Span::styled(
                " Filename: ",
                Style::default().fg(accent).add_modifier(Modifier::BOLD),
            )
        } else {
            Span::styled(" Filename: ", Style::default().fg(Color::Gray))
        };
        let filename_val = if self.filename.is_empty() {
            Span::styled("session.md", Style::default().fg(Color::DarkGray))
        } else {
            Span::styled(self.filename.clone(), Style::default().fg(Color::White))
        };
        let cursor = if self.focus == 0 {
            Span::styled("▊", Style::default().fg(accent))
        } else {
            Span::raw("")
        };
        let filename_line = Line::from(vec![filename_label, filename_val, cursor]);

        // Checkboxes
        let make_checkbox = |label: &str, checked: bool, focus: bool, idx: usize| -> Line {
            let is_focused = focus && self.focus == idx;
            let checkbox = if checked { "[x]" } else { "[ ]" };
            let prefix = if is_focused { " ▸ " } else { "   " };
            let style = if is_focused {
                Style::default().fg(accent).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            };
            Line::from(vec![Span::styled(
                format!("{}{} {}", prefix, checkbox, label),
                style,
            )])
        };

        let items = vec![
            Line::from(""), // spacer
            filename_line,
            Line::from(""), // spacer
            make_checkbox("Include thinking", self.include_thinking, true, 1),
            make_checkbox("Include tools", self.include_tools, true, 2),
            make_checkbox("Include metadata", self.include_metadata, true, 3),
            Line::from(""),
            Line::from(vec![Span::styled(
                " Tab:focus  Space:toggle  Enter:confirm  Esc:cancel ",
                Style::default().fg(Color::DarkGray),
            )]),
        ];

        let text = Text::from(items);
        let paragraph = Paragraph::new(text).wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, inner);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => self.handle_key(key),
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "export_dialog"
    }
}

impl ExportDialog {
    fn handle_key(&mut self, key: &KeyEvent) -> EventResult {
        if self.result.is_some() || self.cancelled {
            return EventResult::NotConsumed;
        }

        match key.code {
            KeyCode::Tab => {
                self.advance_focus();
                EventResult::Consumed
            }
            KeyCode::BackTab => {
                self.prev_focus();
                EventResult::Consumed
            }
            KeyCode::Char(' ') if self.focus > 0 => {
                self.toggle_current();
                EventResult::Consumed
            }
            KeyCode::Enter => {
                self.confirm();
                EventResult::Consumed
            }
            KeyCode::Esc => {
                self.cancel();
                EventResult::Consumed
            }
            // Filename input
            KeyCode::Char(c) if self.focus == 0 => {
                self.filename.push(c);
                EventResult::Consumed
            }
            KeyCode::Backspace if self.focus == 0 => {
                self.filename.pop();
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::RenderContext;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;

    fn key_event(code: KeyCode) -> Event {
        Event::Key(KeyEvent::new(code, crossterm::event::KeyModifiers::NONE))
    }

    fn key_with_mod(code: KeyCode, modifiers: crossterm::event::KeyModifiers) -> Event {
        Event::Key(KeyEvent::new(code, modifiers))
    }

    fn render_dialog(dialog: &mut ExportDialog, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            dialog.render(&mut ctx, area);
        });
        terminal.buffer_lines()
    }

    // ── Test: export_dialog_shows_options ────────────────────────────

    #[test]
    fn export_dialog_shows_options() {
        let mut dialog = ExportDialog::new();
        let lines = render_dialog(&mut dialog, 80, 24);
        let joined = lines.join("\n");

        assert!(joined.contains("Export Options"), "Should show title");
        assert!(joined.contains("Filename"), "Should show filename label");
        assert!(
            joined.contains("session.md"),
            "Should show default filename"
        );
        assert!(
            joined.contains("Include thinking"),
            "Should show thinking option"
        );
        assert!(joined.contains("Include tools"), "Should show tools option");
        assert!(
            joined.contains("Include metadata"),
            "Should show metadata option"
        );
    }

    // ── Test: toggle_changes_state ────────────────────────────────────

    #[test]
    fn toggle_changes_state() {
        let mut dialog = ExportDialog::new();
        assert!(dialog.include_thinking);
        assert!(dialog.include_tools);
        assert!(dialog.include_metadata);

        // Tab to focus 1 (thinking checkbox)
        dialog.handle_event(&key_event(KeyCode::Tab));
        assert_eq!(dialog.focus, 1);

        // Space to toggle off
        dialog.handle_event(&key_event(KeyCode::Char(' ')));
        assert!(!dialog.include_thinking, "Thinking should be toggled off");

        // Tab to focus 2 (tools)
        dialog.handle_event(&key_event(KeyCode::Tab));
        assert_eq!(dialog.focus, 2);

        // Space to toggle off
        dialog.handle_event(&key_event(KeyCode::Char(' ')));
        assert!(!dialog.include_tools, "Tools should be toggled off");

        // Tab to focus 3 (metadata)
        dialog.handle_event(&key_event(KeyCode::Tab));
        assert_eq!(dialog.focus, 3);

        // Space to toggle off
        dialog.handle_event(&key_event(KeyCode::Char(' ')));
        assert!(!dialog.include_metadata, "Metadata should be toggled off");
    }

    // ── Test: confirm_returns_options ─────────────────────────────────

    #[test]
    fn confirm_returns_options() {
        let mut dialog = ExportDialog::new();
        assert!(dialog.result.is_none());

        // Modify some options
        // Tab to focus 1
        dialog.handle_event(&key_event(KeyCode::Tab));
        dialog.handle_event(&key_event(KeyCode::Char(' '))); // toggle off thinking

        // Enter to confirm
        dialog.handle_event(&key_event(KeyCode::Enter));
        assert!(
            dialog.result.is_some(),
            "Result should be set after confirm"
        );

        let options = dialog.result.as_ref().unwrap();
        assert_eq!(options.filename, "session.md");
        assert!(!options.include_thinking);
        assert!(options.include_tools);
        assert!(options.include_metadata);
    }

    // ── Cancel and filename tests ────────────────────────────────────

    #[test]
    fn export_dialog_esc_cancels() {
        let mut dialog = ExportDialog::new();
        assert!(!dialog.cancelled);

        dialog.handle_event(&key_event(KeyCode::Esc));
        assert!(dialog.cancelled, "Dialog should be cancelled on Esc");
    }

    #[test]
    fn export_dialog_filename_input() {
        let mut dialog = ExportDialog::new();
        dialog.filename.clear();

        dialog.handle_event(&key_with_mod(
            KeyCode::Char('m'),
            crossterm::event::KeyModifiers::NONE,
        ));
        dialog.handle_event(&key_with_mod(
            KeyCode::Char('d'),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(dialog.filename, "md");

        dialog.handle_event(&key_event(KeyCode::Backspace));
        assert_eq!(dialog.filename, "m");
    }

    #[test]
    fn export_dialog_tab_cycles_focus() {
        let mut dialog = ExportDialog::new();
        assert_eq!(dialog.focus, 0);

        // Tab cycles through 0→1→2→3→0
        dialog.handle_event(&key_event(KeyCode::Tab));
        assert_eq!(dialog.focus, 1);

        dialog.handle_event(&key_event(KeyCode::Tab));
        assert_eq!(dialog.focus, 2);

        dialog.handle_event(&key_event(KeyCode::Tab));
        assert_eq!(dialog.focus, 3);

        dialog.handle_event(&key_event(KeyCode::Tab));
        assert_eq!(dialog.focus, 0);
    }

    #[test]
    fn export_dialog_focusable_and_id() {
        let dialog = ExportDialog::new();
        assert!(dialog.focusable());
        assert_eq!(dialog.id(), "export_dialog");
    }
}
