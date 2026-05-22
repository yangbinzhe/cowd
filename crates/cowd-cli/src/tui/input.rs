#![allow(dead_code)]
use std::io;
use crossterm::event::{Event, KeyCode, KeyEventKind};
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
            match key.code {
                KeyCode::Esc => {
                    if app.turn_active { return Ok(InputResult::Cancel); }
                    Ok(InputResult::Exit)
                }
                KeyCode::Char('c') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    if app.turn_active { return Ok(InputResult::Cancel); }
                    Ok(InputResult::Exit)
                }
                KeyCode::Enter => {
                    // Shift+Enter: insert newline
                    if key.modifiers.contains(crossterm::event::KeyModifiers::SHIFT) {
                        app.input.insert_newline();
                        return Ok(InputResult::Nothing);
                    }
                    // If input is empty and there's a focused collapsible entry, toggle it
                    if app.input.is_empty() {
                        let focused = app.timeline.get(app.timeline_cursor);
                        if let Some(entry) = focused {
                            if entry.is_collapsible() {
                                app.toggle_expand_current();
                                return Ok(InputResult::Nothing);
                            }
                        }
                    }
                    // Otherwise: submit
                    let text = app.input.lines().join("\n").trim().to_string();
                    app.input = TextArea::default();
                    app.input.set_block(Block::default().borders(Borders::ALL).title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "));
                    Ok(InputResult::Submit(text))
                }
                KeyCode::Tab => { app.next_panel(); Ok(InputResult::Nothing) }
                KeyCode::Up => {
                    if app.input.is_empty() {
                        // Navigate timeline cursor up
                        if app.cursor_up() {
                            app.scroll_offset = app.scroll_offset.saturating_sub(1);
                        }
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::Down => {
                    if app.input.is_empty() {
                        if app.cursor_down() {
                            app.scroll_offset = app.scroll_offset.saturating_add(1);
                        }
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::PageUp => {
                    if app.input.is_empty() {
                        app.auto_scroll = false;
                        app.scroll_offset = app.scroll_offset.saturating_sub(10);
                        return Ok(InputResult::Nothing);
                    }
                    app.input.input(key);
                    Ok(InputResult::Nothing)
                }
                KeyCode::PageDown => {
                    if app.input.is_empty() {
                        app.scroll_offset = app.scroll_offset.saturating_add(10);
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
                KeyCode::Char('t') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    app.theme.toggle(); Ok(InputResult::Nothing)
                }
                KeyCode::Char('y') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    if app.input.is_empty() {
                        app.copy_focused_content();
                    }
                    Ok(InputResult::Nothing)
                }
                _ => { app.input.input(key); Ok(InputResult::Nothing) }
            }
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
