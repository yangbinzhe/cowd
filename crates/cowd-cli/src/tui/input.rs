#![allow(dead_code)]
use std::io;
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use tui_textarea::TextArea;
use ratatui::widgets::{Block, Borders};
use super::app::App;

pub enum InputResult {
    Submit(String),
    Cancel,
    Exit,
    Nothing,
    ResumeSession(String),
}

pub fn handle_input(app: &mut App) -> io::Result<InputResult> {
    let poll_ms = if app.turn_active { 5 } else { 10 };
    if !crossterm::event::poll(std::time::Duration::from_millis(poll_ms))? {
        if app.turn_active { app.tick(); }
        return Ok(InputResult::Nothing);
    }
    match crossterm::event::read()? {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            if app.picker_active { return handle_picker(app, key.code); }
            if app.approval.is_some() { return handle_approval(app, key.code); }

            // ── Search mode: typing the search query ──
            if app.search_active {
                return handle_search_input(app, key);
            }

            // ── Normal mode ──
            match key.code {
                KeyCode::Esc => {
                    if app.help_visible {
                        app.help_visible = false;
                        return Ok(InputResult::Nothing);
                    }
                    if !app.search_matches.is_empty() {
                        app.cancel_search();
                        return Ok(InputResult::Nothing);
                    }
                    if app.turn_active { return Ok(InputResult::Cancel); }
                    Ok(InputResult::Exit)
                }
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    if app.turn_active { return Ok(InputResult::Cancel); }
                    Ok(InputResult::Exit)
                }
                KeyCode::Enter => {
                    if key.modifiers.contains(KeyModifiers::SHIFT) {
                        app.input.insert_newline();
                        return Ok(InputResult::Nothing);
                    }
                    if app.input.is_empty() {
                        let focused = app.timeline.get(app.timeline_cursor);
                        if let Some(entry) = focused {
                            if entry.is_collapsible() {
                                app.toggle_expand_current();
                                return Ok(InputResult::Nothing);
                            }
                        }
                    }
                    let text = app.input.lines().join("\n").trim().to_string();
                    app.input = TextArea::default();
                    app.input.set_block(Block::default().borders(Borders::ALL).title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "));
                    Ok(InputResult::Submit(text))
                }
                KeyCode::Tab => { app.next_panel(); Ok(InputResult::Nothing) }
                KeyCode::Up => {
                    // Alt+Up: browse input history (older)
                    if key.modifiers.contains(KeyModifiers::ALT) {
                        return handle_history(app, true);
                    }
                    if app.input.is_empty() {
                        if app.cursor_up() {
                            app.scroll_to_entry(app.timeline_cursor);
                        }
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::Down => {
                    // Alt+Down: browse input history (newer)
                    if key.modifiers.contains(KeyModifiers::ALT) {
                        return handle_history(app, false);
                    }
                    if app.input.is_empty() {
                        if app.cursor_down() {
                            app.scroll_to_entry(app.timeline_cursor);
                        }
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::PageUp => {
                    if app.input.is_empty() {
                        app.auto_scroll = false;
                        app.scroll_page_up();
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::PageDown => {
                    if app.input.is_empty() {
                        app.scroll_page_down();
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::Home => {
                    if app.input.is_empty() {
                        app.scroll_offset = 0;
                        app.auto_scroll = false;
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::End => {
                    if app.input.is_empty() {
                        app.auto_scroll = true;
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::Char('t') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    app.theme.toggle(); Ok(InputResult::Nothing)
                }
                KeyCode::Char('y') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    if app.input.is_empty() {
                        app.copy_focused_content();
                    }
                    Ok(InputResult::Nothing)
                }
                // ── Model switching: Ctrl+M ──
                KeyCode::Char('m') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    if let Some(model) = app.next_model() {
                        app.show_notification(&format!("Switched to model: {model}"));
                    }
                    Ok(InputResult::Nothing)
                }
                // ── Search trigger: '/' ──
                KeyCode::Char('/') => {
                    if app.input.is_empty() {
                        app.search_active = true;
                        app.search_query.clear();
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                // ── Search navigation: n/N ──
                KeyCode::Char('n') => {
                    if app.input.is_empty() && !app.search_matches.is_empty() {
                        app.search_next();
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::Char('N') => {
                    if app.input.is_empty() && !app.search_matches.is_empty() {
                        app.search_prev();
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                // ── Help panel ──
                KeyCode::Char('?') => {
                    app.help_visible = !app.help_visible;
                    Ok(InputResult::Nothing)
                }
                _ => { app.input.input(key); Ok(InputResult::Nothing) }
            }
        }
        _ => Ok(InputResult::Nothing),
    }
}

fn handle_history(app: &mut App, prev: bool) -> io::Result<InputResult> {
    let text = if prev {
        app.history_prev()
    } else {
        app.history_next()
    };
    if let Some(text) = text {
        // Clear and refill the textarea
        let mut ta = TextArea::default();
        ta.set_block(Block::default().borders(Borders::ALL).title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "));
        ta.set_style(ratatui::style::Style::default().fg(ratatui::style::Color::White));
        if !text.is_empty() {
            ta.insert_str(&text);
        }
        app.input = ta;
    }
    Ok(InputResult::Nothing)
}

fn handle_search_input(app: &mut App, key: crossterm::event::KeyEvent) -> io::Result<InputResult> {
    match key.code {
        KeyCode::Esc => {
            app.cancel_search();
            Ok(InputResult::Nothing)
        }
        KeyCode::Enter => {
            let query = app.search_query.clone();
            app.search_active = false;
            if !query.is_empty() {
                app.execute_search(&query);
            }
            Ok(InputResult::Nothing)
        }
        KeyCode::Backspace => {
            app.search_query.pop();
            Ok(InputResult::Nothing)
        }
        KeyCode::Char(c) => {
            app.search_query.push(c);
            Ok(InputResult::Nothing)
        }
        _ => Ok(InputResult::Nothing),
    }
}

fn handle_picker(app: &mut App, code: KeyCode) -> io::Result<InputResult> {
    match code {
        KeyCode::Esc => { app.close_session_picker(); Ok(InputResult::Nothing) }
        KeyCode::Up | KeyCode::Char('k') => { app.picker_up(); Ok(InputResult::Nothing) }
        KeyCode::Down | KeyCode::Char('j') => { app.picker_down(); Ok(InputResult::Nothing) }
        KeyCode::Enter => {
            let id = app.picker_selected_id().map(String::from);
            app.close_session_picker();
            Ok(id.map(InputResult::ResumeSession).unwrap_or(InputResult::Nothing))
        }
        _ => Ok(InputResult::Nothing),
    }
}

fn handle_approval(app: &mut App, code: KeyCode) -> io::Result<InputResult> {
    match code {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            app.approval = None;
            Ok(InputResult::Submit("__approval_approved__".into()))
        }
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
            app.approval = None;
            Ok(InputResult::Submit("__approval_denied__".into()))
        }
        _ => Ok(InputResult::Nothing),
    }
}
