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
    if !crossterm::event::poll(std::time::Duration::from_millis(50))? {
        if app.is_loading { app.tick(); }
        return Ok(InputResult::Nothing);
    }
    match crossterm::event::read()? {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            if app.picker_active { return handle_picker(app, key.code); }
            if app.approval.is_some() { return handle_approval(app, key.code); }
            match key.code {
                KeyCode::Esc => Ok(InputResult::Exit),
                KeyCode::Enter => {
                    let text = app.input.lines().join("\n").trim().to_string();
                    app.input = TextArea::default();
                    app.input.set_block(Block::default().borders(Borders::ALL).title(" Input (Enter=send, Esc=quit, / for commands) "));
                    Ok(InputResult::Submit(text))
                }
                KeyCode::Tab => { app.next_panel(); Ok(InputResult::Nothing) }
                KeyCode::Char('t') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    app.theme.toggle(); Ok(InputResult::Nothing)
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
