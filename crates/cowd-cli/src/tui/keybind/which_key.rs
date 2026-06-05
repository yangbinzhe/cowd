// ── WhichKey Overlay ────────────────────────────────────────────────
// Renders a centered overlay grid showing keybindings available for
// the current modal context and pending chord prefix.
//
// Layout:
//   ┌───── Which-Key ──────┐
//   │  [Nav] [Session] [Files] [Dialog] [System]
//   │  j       Scroll down  │
//   │  k       Scroll up    │
//   │  Ctrl-c  Quit         │
//   │  SPC f   Find file    │
//   └───────────────────────┘
//
// Group tabs + filtering. Column 1: chord display, Column 2: description.
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

use super::engine::{
    KeybindEngine, GROUP_DIALOG, GROUP_FILES, GROUP_NAVIGATION, GROUP_SESSION, GROUP_SYSTEM,
};

const ALL_GROUPS: &[&str] = &[
    GROUP_NAVIGATION,
    GROUP_SESSION,
    GROUP_FILES,
    GROUP_DIALOG,
    GROUP_SYSTEM,
];

pub struct WhichKey;

impl WhichKey {
    pub fn draw(frame: &mut Frame, area: Rect, engine: &KeybindEngine) {
        if !engine.which_key_visible {
            return;
        }

        let bindings = engine.visible_bindings();
        if bindings.is_empty() {
            return;
        }

        let selected_group = group_name(engine);
        let filtered: Vec<&super::types::KeyBinding> = if selected_group == GROUP_SYSTEM {
            // Show ALL bindings for the System tab
            bindings.iter().copied().collect()
        } else {
            bindings
                .iter()
                .copied()
                .filter(|b| b.group == selected_group)
                .collect()
        };

        if filtered.is_empty() {
            return;
        }

        let chord_col_width: u16 = 12;
        let gap: u16 = 2;
        let max_desc_len: u16 = filtered
            .iter()
            .map(|b| b.description.len() as u16)
            .max()
            .unwrap_or(20);
        let content_width = (chord_col_width + gap + max_desc_len).max(42); // min width for tab bar
        let content_height = filtered.len() as u16 + 2;
        let total_width = (content_width + 4).min(area.width.saturating_sub(2));
        let total_height = (content_height + 2).min(area.height.saturating_sub(2));
        let x = (area.width.saturating_sub(total_width)) / 2;
        let y = (area.height.saturating_sub(total_height)) / 2;
        let overlay_area = Rect::new(x, y, total_width, total_height.max(5));

        let title = if engine.pending_chord().is_empty() {
            " Which-Key ".to_string()
        } else {
            format!(" {} => ", super::chord_to_string(engine.pending_chord()))
        };

        let mut lines: Vec<Line<'_>> = Vec::with_capacity(filtered.len() + 2);

        let tab_spans: Vec<Span> = ALL_GROUPS
            .iter()
            .enumerate()
            .map(|(i, g)| {
                let short = short_label(g);
                Span::styled(
                    format!(" {short} "),
                    if i == engine.which_key_group {
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::Cyan)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    },
                )
            })
            .collect();
        lines.push(Line::from(tab_spans));
        lines.push(Line::raw(""));

        for binding in &filtered {
            let chord_str = super::chord_to_string(&binding.chord.keys);
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
                Span::styled(binding.description, Style::default().fg(Color::White)),
            ]));
        }

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
        let _ = (ctx, area);
    }
    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }
    fn focusable(&self) -> bool {
        false
    }
    fn id(&self) -> &str {
        "which_key"
    }
}

fn short_label(group: &str) -> &'static str {
    match group {
        GROUP_NAVIGATION => "Nav",
        GROUP_SESSION => "Session",
        GROUP_FILES => "Files",
        GROUP_DIALOG => "Dialog",
        GROUP_SYSTEM => "System",
        _ => "?",
    }
}

fn group_name(engine: &KeybindEngine) -> &'static str {
    ALL_GROUPS[engine.which_key_group.min(ALL_GROUPS.len() - 1)]
}

/// Convert a sequence of key events into a human-readable string.
pub fn chord_to_string(keys: &[KeyEvent]) -> String {
    keys.iter()
        .map(key_event_to_label)
        .collect::<Vec<_>>()
        .join(" ")
}

fn key_event_to_label(event: &KeyEvent) -> String {
    use KeyCode::*;
    let mods = event.modifiers;
    match event.code {
        Char(' ') if mods.is_empty() => "SPC".to_string(),
        Char(c) => {
            let mut label = String::new();
            if mods.contains(KeyModifiers::CONTROL) {
                label.push_str("Ctrl-");
            }
            if mods.contains(KeyModifiers::ALT) {
                label.push_str("Alt-");
            }
            if mods.contains(KeyModifiers::SHIFT) && c.is_ascii_uppercase() {
            } else if mods.contains(KeyModifiers::SHIFT) {
                label.push('S');
                label.push('-');
            }
            if mods.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) {
                label.push(c.to_ascii_lowercase());
            } else {
                label.push(c);
            }
            label
        }
        Enter => "Enter".to_string(),
        Esc => "Esc".to_string(),
        Tab => "Tab".to_string(),
        BackTab => "S-Tab".to_string(),
        Backspace => "BS".to_string(),
        Delete => "Del".to_string(),
        Insert => "Ins".to_string(),
        Home => "Home".to_string(),
        End => "End".to_string(),
        PageUp => "PgUp".to_string(),
        PageDown => "PgDn".to_string(),
        Up => "\u{2191}".to_string(),
        Down => "\u{2193}".to_string(),
        Left => "\u{2190}".to_string(),
        Right => "\u{2192}".to_string(),
        F(n) => format!("F{n}"),
        _ => format!("{:?}", event.code),
    }
}

pub fn next_group(engine: &mut KeybindEngine) {
    engine.which_key_group = (engine.which_key_group + 1) % ALL_GROUPS.len();
}

pub fn prev_group(engine: &mut KeybindEngine) {
    let len = ALL_GROUPS.len();
    engine.which_key_group = if engine.which_key_group == 0 {
        len - 1
    } else {
        engine.which_key_group - 1
    };
}

pub fn select_group(engine: &mut KeybindEngine, group: &str) {
    if let Some(idx) = ALL_GROUPS.iter().position(|g| *g == group) {
        engine.which_key_group = idx;
    }
}

#[cfg(test)]
mod tests {
    use super::super::engine::default_bindings;
    use super::*;
    use crate::tui::test_utils::MockTerminal;

    #[test]
    fn chord_label_single_char() {
        let keys = [KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE)];
        assert_eq!(super::super::chord_to_string(&keys), "j");
    }

    #[test]
    fn chord_label_ctrl_key() {
        let keys = [KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)];
        assert_eq!(super::super::chord_to_string(&keys), "Ctrl-c");
    }

    #[test]
    fn whichkey_groups_show_tabs() {
        let mut engine = KeybindEngine::new(default_bindings());
        engine.flush_pending();
        engine.which_key_visible = true;
        let mut terminal = MockTerminal::new(80, 24);
        terminal.draw(|f: &mut Frame| {
            WhichKey::draw(f, f.area(), &engine);
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Nav"),
            "Should show Nav tab, buffer:\n{joined}"
        );
        assert!(joined.contains("Session"));
        assert!(joined.contains("Files"));
        assert!(joined.contains("Dialog"));
        assert!(joined.contains("System"));
    }

    #[test]
    fn tab_switches_group() {
        let mut engine = KeybindEngine::new(default_bindings());
        engine.flush_pending();
        engine.which_key_visible = true;
        assert_eq!(group_name(&engine), GROUP_NAVIGATION);
        next_group(&mut engine);
        assert_eq!(group_name(&engine), GROUP_SESSION);
        prev_group(&mut engine);
        assert_eq!(group_name(&engine), GROUP_NAVIGATION);
    }

    #[test]
    fn bindings_filter_by_group() {
        let mut engine = KeybindEngine::new(default_bindings());
        engine.flush_pending();
        engine.which_key_visible = true;
        select_group(&mut engine, GROUP_NAVIGATION);
        let bindings = engine.visible_bindings();
        let nav: Vec<_> = bindings
            .iter()
            .filter(|b| b.group == GROUP_NAVIGATION)
            .collect();
        assert!(!nav.is_empty(), "Nav should have bindings");
        assert!(
            nav.iter().any(|b| b.description.contains("Scroll")),
            "Nav should include scroll"
        );
    }

    #[test]
    fn group_wraps_around() {
        let mut engine = KeybindEngine::new(default_bindings());
        engine.flush_pending();
        engine.which_key_visible = true;
        assert_eq!(group_name(&engine), GROUP_NAVIGATION);
        prev_group(&mut engine);
        assert_eq!(group_name(&engine), GROUP_SYSTEM);
        next_group(&mut engine);
        assert_eq!(group_name(&engine), GROUP_NAVIGATION);
    }

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
        let event = crossterm::event::Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(wk.handle_event(&event).is_not_consumed());
    }
}
