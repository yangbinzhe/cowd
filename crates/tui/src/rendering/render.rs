/// Top-level TUI render entry point.
///
/// Bridges the legacy `App` struct with the `TuiState` rendering pipeline.
/// Converts `App` → `TuiState`, calls `render()`, then extracts the `App` back.
pub fn draw(frame: &mut ratatui::Frame, app: &mut crate::app::App) {
    let model = app.shell.model.clone();
    let session_id = app.shell.session_id.clone();
    let fresh = crate::app::App::new(&model, &session_id);
    let real = std::mem::replace(app, fresh);
    let mut state = crate::state::TuiState::from_app(real);
    state.render(frame);
    let rendered = state.into_app();
    let _ = std::mem::replace(app, rendered);
}
