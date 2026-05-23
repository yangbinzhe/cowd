// ── WhichKey Overlay ────────────────────────────────────────────────
// Renders a centered overlay grid showing keybindings available for
// the current modal context and pending chord prefix.
//
// Layout:
//   ┌───── Which-Key ──────┐   (or " SPC => " when prefix active)
//   │  j       Scroll down  │
//   │  k       Scroll up    │
//   │  Ctrl-c  Quit         │
//   │  SPC f   Find file    │
//   │  ...                  │
//   └───────────────────────┘
//
// Column 1: chord display (e.g. "SPC f", "Ctrl-c", "gg")
// Column 2: description text
// -------------------------------------------------------------------

#![allow(dead_code)]

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use crate::tui::components::base::{Component, EventResult, RenderContext};

use super::engine::KeybindEngine;

// ── WhichKey ──────────────────────────────────────────────────────

/// Renders a which-key overlay showing available keybindings.
///
/// Implements [`Component`] for integration with the TUI component
/// tree. `render()` delegates to [`Self::draw()`] when the engine's
/// `which_key_visible` flag is `true`.
///
/// All binding data comes from the engine — no hardcoded keybindings.
pub struct WhichKey;

impl WhichKey {
    /// Render the which-key overlay if the engine has it visible.
    ///
    /// The overlay is centered on the screen. Content depends on the
    /// engine's pending prefix:
    /// - Empty prefix → all single-key top-level bindings.
    /// - Space prefix → all space-leader chords (SPC f, SPC p, ...).
    /// - Other prefix → continuation options.
    ///
    /// When no bindings match the current scope the overlay is skipped
    /// (no empty box is drawn).
    pub fn draw(frame: &mut Frame, area: Rect, engine: &KeybindEngine) {
        if !engine.which_key_visible {
            return;
        }

        let bindings = engine.visible_bindings();
        if bindings.is_empty() {
            return;
        }

        // ── Calculate overlay dimensions ────────────────────────────
        let chord_col_width: u16 = 12; // space for "SPC x" / "Ctrl-c"
        let gap: u16 = 2;
        let max_desc_len: u16 = bindings
            .iter()
            .map(|b| b.description.len() as u16)
            .max()
            .unwrap_or(20);

        let content_width = chord_col_width + gap + max_desc_len;
        let content_height = bindings.len() as u16;
        let total_width = (content_width + 4).min(area.width.saturating_sub(2));
        let total_height = (content_height + 2).min(area.height.saturating_sub(2));

        // ── Center the overlay ──────────────────────────────────────
        let x = (area.width.saturating_sub(total_width)) / 2;
        let y = (area.height.saturating_sub(total_height)) / 2;
        let overlay_area = Rect::new(x, y, total_width, total_height.max(3));

        // ── Title ───────────────────────────────────────────────────
        let title = if engine.pending_chord().is_empty() {
            " Which-Key ".to_string()
        } else {
            format!(" {} => ", chord_to_string(engine.pending_chord()))
        };

        // ── Build binding rows ──────────────────────────────────────
        let mut lines: Vec<Line<'_>> = Vec::with_capacity(bindings.len());

        for binding in &bindings {
            let chord_str = chord_to_string(&binding.chord.keys);
            let padding = " ".repeat(
                chord_col_width
                    .saturating_sub(chord_str.len() as u16)
                    .max(1) as usize,
            );

            lines.push(Line::from(vec![
                Span::styled(
                    format!("{padding}{chord_str}"),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled(
                    binding.description,
                    Style::default().fg(Color::White),
                ),
            ]));
        }

        // ── Render ──────────────────────────────────────────────────
        let paragraph = Paragraph::new(lines).block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow)),
        );

        frame.render_widget(paragraph, overlay_area);
    }
}

impl Component for WhichKey {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        // WhichKey does not hold engine state directly — it is
        // rendered via Self::draw() called externally with the
        // engine reference. The Component impl provides a no-op
        // default for integration with the component tree.
        let _ = ctx;
        let _ = area;
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        // WhichKey is a passive overlay; events pass through to
        // the active panel.
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        false
    }

    fn id(&self) -> &str {
        "which_key"
    }
}

// ── Chord → String ────────────────────────────────────────────────

/// Convert a sequence of key events into a human-readable string.
///
/// Examples:
/// - `[j]` → `"j"`
/// - `[Ctrl-c]` → `"Ctrl-c"`
/// - `[Space, f]` → `"SPC f"`
/// - `[g, g]` → `"g g"`
/// - `[Tab]` → `"Tab"`
/// - `[Esc]` → `"Esc"`
/// - `[Shift-Tab]` → `"S-Tab"`
pub fn chord_to_string(keys: &[KeyEvent]) -> String {
    keys.iter()
        .map(key_event_to_label)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Convert a single `KeyEvent` to a concise label.
fn key_event_to_label(event: &KeyEvent) -> String {
    let mods = event.modifiers;
    match event.code {
        KeyCode::Char(' ') if mods.is_empty() => "SPC".to_string(),
        KeyCode::Char(c) => {
            let mut label = String::new();
            if mods.contains(KeyModifiers::CONTROL) {
                label.push_str("Ctrl-");
            }
            if mods.contains(KeyModifiers::ALT) {
                label.push_str("Alt-");
            }
            if mods.contains(KeyModifiers::SHIFT) && c.is_ascii_uppercase() {
                // already uppercase letter, just note shift
                let _ = c;
            } else if mods.contains(KeyModifiers::SHIFT) {
                label.push('S');
                label.push('-');
            }
            if mods.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) {
                label.push(c.to_ascii_lowercase());
            } else if mods.contains(KeyModifiers::SHIFT) {
                label.push(c);
            } else {
                label.push(c);
            }
            label
        }
        KeyCode::Enter => "Enter".to_string(),
        KeyCode::Esc => "Esc".to_string(),
        KeyCode::Tab => "Tab".to_string(),
        KeyCode::BackTab => "S-Tab".to_string(),
        KeyCode::Backspace => "BS".to_string(),
        KeyCode::Delete => "Del".to_string(),
        KeyCode::Insert => "Ins".to_string(),
        KeyCode::Home => "Home".to_string(),
        KeyCode::End => "End".to_string(),
        KeyCode::PageUp => "PgUp".to_string(),
        KeyCode::PageDown => "PgDn".to_string(),
        KeyCode::Up => "↑".to_string(),
        KeyCode::Down => "↓".to_string(),
        KeyCode::Left => "←".to_string(),
        KeyCode::Right => "→".to_string(),
        KeyCode::F(n) => format!("F{n}"),
        _ => format!("{:?}", event.code),
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::test_utils::MockTerminal;

    use super::super::engine::default_bindings;

    // ── chord_to_string ────────────────────────────────────────────

    #[test]
    fn chord_label_single_char() {
        let keys = [KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE)];
        assert_eq!(chord_to_string(&keys), "j");
    }

    #[test]
    fn chord_label_ctrl_key() {
        let keys = [KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)];
        assert_eq!(chord_to_string(&keys), "Ctrl-c");
    }

    #[test]
    fn chord_label_space_leader() {
        let keys = [
            KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
            KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE),
        ];
        assert_eq!(chord_to_string(&keys), "SPC f");
    }

    #[test]
    fn chord_label_double_g() {
        let keys = [
            KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE),
            KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE),
        ];
        assert_eq!(chord_to_string(&keys), "g g");
    }

    #[test]
    fn chord_label_special_keys() {
        assert_eq!(
            chord_to_string(&[KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)]),
            "Enter"
        );
        assert_eq!(
            chord_to_string(&[KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)]),
            "Esc"
        );
        assert_eq!(
            chord_to_string(&[KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE)]),
            "Tab"
        );
        assert_eq!(
            chord_to_string(&[KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE)]),
            "F5"
        );
    }

    // ── whichkey_renders ───────────────────────────────────────────

    #[test]
    fn whichkey_renders_overlay_when_visible() {
        let mut engine = KeybindEngine::new(default_bindings());
        // Activate which-key by pressing Space — title becomes " SPC => "
        engine.handle_key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        assert!(engine.which_key_visible);

        let mut terminal = MockTerminal::new(80, 24);
        terminal.draw(|f: &mut Frame| {
            WhichKey::draw(f, f.area(), &engine);
        });

        // Title uses the pending prefix: " SPC => "
        terminal.assert_line_contains("SPC =>");
        // Should contain space-leader binding entries
        let lines = terminal.buffer_lines();
        let full_buffer = lines.join("\n");
        assert!(
            full_buffer.contains("SPC f"),
            "buffer should contain 'SPC f':\n{full_buffer}"
        );
        assert!(
            full_buffer.contains("Find / search"),
            "buffer should contain 'Find / search':\n{full_buffer}"
        );
    }

    #[test]
    fn whichkey_title_is_which_key_when_no_pending_prefix() {
        // Manually set which_key_visible without any pending chord
        let mut engine = KeybindEngine::new(default_bindings());
        // Flush any pending state, then simulate external which-key activation
        engine.flush_pending();
        engine.which_key_visible = true;

        let mut terminal = MockTerminal::new(80, 24);
        terminal.draw(|f: &mut Frame| {
            WhichKey::draw(f, f.area(), &engine);
        });

        terminal.assert_line_contains("Which-Key");
    }

    #[test]
    fn whichkey_does_not_render_when_not_visible() {
        let engine = KeybindEngine::new(default_bindings());
        assert!(!engine.which_key_visible);

        let mut terminal = MockTerminal::new(80, 24);
        terminal.draw(|f: &mut Frame| {
            WhichKey::draw(f, f.area(), &engine);
        });

        // Buffer should be empty (no overlay drawn)
        let lines = terminal.buffer_lines();
        let all_empty = lines.iter().all(|l| l.is_empty());
        assert!(all_empty, "expected empty buffer, got:\n{}", lines.join("\n"));
    }

    #[test]
    fn whichkey_renders_narrowed_options_with_pending_prefix() {
        let mut engine = KeybindEngine::new(default_bindings());
        // Press Space to start leader prefix
        engine.handle_key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        assert!(engine.which_key_visible);

        let mut terminal = MockTerminal::new(80, 24);
        terminal.draw(|f: &mut Frame| {
            WhichKey::draw(f, f.area(), &engine);
        });

        // Title should show " SPC => " since Space is the pending prefix
        let lines = terminal.buffer_lines();
        let full_buffer = lines.join("\n");
        assert!(
            full_buffer.contains("SPC =>"),
            "title should show 'SPC =>':\n{full_buffer}"
        );
        // Should show space-leader chords
        assert!(full_buffer.contains("SPC f"));
        assert!(full_buffer.contains("SPC p"));
        assert!(full_buffer.contains("SPC q"));
    }

    // ── Component trait ────────────────────────────────────────────

    #[test]
    fn whichkey_component_not_focusable() {
        let wk = WhichKey;
        assert!(!wk.focusable());
    }

    #[test]
    fn whichkey_component_id() {
        let wk = WhichKey;
        assert_eq!(wk.id(), "which_key");
    }

    #[test]
    fn whichkey_component_handle_event_not_consumed() {
        let mut wk = WhichKey;
        let event = crossterm::event::Event::Key(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        ));
        let result = wk.handle_event(&event);
        assert!(result.is_not_consumed());
    }
}
