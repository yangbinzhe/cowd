use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::app::{App, CurrentTaskSummary};
use crate::tui::components::{Component, EventResult, RenderContext};
use crate::tui::runtime_control_store::DaemonTaskSummary;

#[derive(Debug, Clone, Default)]
pub struct GoalWorkbenchPanel {
    current_task: Option<CurrentTaskSummary>,
    daemon_tasks: Vec<DaemonTaskSummary>,
    daemon_task_count: Option<u64>,
    pending_approvals: Option<u64>,
}

impl GoalWorkbenchPanel {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.current_task = app.current_task.clone();
        self.daemon_tasks = app.daemon_tasks.clone();
        self.daemon_task_count = app.daemon_task_count;
        self.pending_approvals = app.daemon_pending_approvals;
    }

    fn render_empty(&self) -> Text<'static> {
        Text::from(vec![
            Line::from(Span::styled(
                "No active daemon goal.",
                Style::default().fg(Color::DarkGray),
            )),
            Line::raw(""),
            Line::from("Start: /tasks start <objective>"),
            Line::from("YOLO:  /tasks start --yolo <objective>"),
            Line::raw(""),
            Line::from(Span::styled(
                "Ctrl+P recommends goal actions from runtime state.",
                Style::default().fg(Color::DarkGray),
            )),
        ])
    }
}

impl Component for GoalWorkbenchPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if area.width < 12 || area.height < 3 {
            return;
        }

        let title = format!(
            " Goals ({}) ",
            self.daemon_task_count
                .unwrap_or(self.daemon_tasks.len() as u64)
        );
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(Style::default().fg(ctx.theme().accent_color()));
        let inner = block.inner(area);
        ctx.frame_mut().render_widget(block, area);

        let mut lines = Vec::new();
        if let Some(task) = self.current_task.as_ref() {
            lines.push(Line::from(vec![
                Span::styled("Current: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    &task.objective,
                    Style::default().add_modifier(Modifier::BOLD),
                ),
            ]));
            lines.push(Line::from(format!("Status: {}", task.status)));
            if let Some(phase) = task.current_phase.as_deref() {
                lines.push(Line::from(format!("Phase: {phase}")));
            }
            lines.push(Line::raw(""));
        }

        if self.daemon_tasks.is_empty() {
            ctx.frame_mut().render_widget(
                Paragraph::new(self.render_empty()).wrap(Wrap { trim: false }),
                inner,
            );
            return;
        }

        lines.push(Line::from(Span::styled(
            "Daemon Tasks",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )));
        for task in self
            .daemon_tasks
            .iter()
            .take(area.height.saturating_sub(5) as usize)
        {
            let mode = if task.yolo_mode { "yolo" } else { "solo" };
            let phase = task.current_phase.as_deref().unwrap_or("-");
            lines.push(Line::from(vec![
                Span::styled(
                    status_icon(&task.status),
                    Style::default().fg(status_color(&task.status)),
                ),
                Span::raw(" "),
                Span::styled(short_id(&task.id), Style::default().fg(Color::Cyan)),
                Span::raw(format!(" [{}:{}] ", task.status, mode)),
                Span::styled(truncate(&task.objective, 52), Style::default()),
            ]));
            lines.push(Line::from(Span::styled(
                format!("   phase: {phase}  failures: {}", task.failure_count),
                Style::default().fg(Color::DarkGray),
            )));
            let review = task.review_result.as_deref().unwrap_or("pending");
            lines.push(Line::from(Span::styled(
                format!("   review: {review}  artifacts: {}", task.artifact_count),
                Style::default().fg(Color::DarkGray),
            )));
            if let Some(blocker) = task.blocker_reason.as_deref() {
                lines.push(Line::from(Span::styled(
                    format!("   blocker: {}", truncate(blocker, 58)),
                    Style::default().fg(Color::Red),
                )));
            }
        }

        if self.pending_approvals.unwrap_or_default() > 0 {
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                format!(
                    "{} approvals waiting: /approvals",
                    self.pending_approvals.unwrap_or_default()
                ),
                Style::default().fg(Color::Yellow),
            )));
        }
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "/tasks  /tasks start --yolo  /approvals",
            Style::default().fg(Color::DarkGray),
        )));

        ctx.frame_mut().render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "goal_workbench_panel"
    }
}

fn status_icon(status: &str) -> &'static str {
    match status {
        "completed" => "✓",
        "failed" | "blocked" => "!",
        "cancelled" => "x",
        "running" | "reviewing" => ">",
        _ => "-",
    }
}

fn status_color(status: &str) -> Color {
    match status {
        "completed" => Color::Green,
        "failed" | "blocked" => Color::Red,
        "cancelled" => Color::DarkGray,
        "running" | "reviewing" => Color::Yellow,
        _ => Color::White,
    }
}

fn short_id(id: &str) -> &str {
    id.get(..id.len().min(12)).unwrap_or(id)
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut out = value
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    fn render_panel(panel: &mut GoalWorkbenchPanel, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|frame| {
            let area = frame.area();
            let mut ctx = RenderContext::new(frame, &skin);
            panel.render(&mut ctx, area);
        });
        terminal.buffer_lines()
    }

    #[test]
    fn renders_daemon_task_summary() {
        let mut panel = GoalWorkbenchPanel::new();
        let mut app = App::new("model", "session");
        app.daemon_task_count = Some(1);
        app.daemon_tasks = vec![DaemonTaskSummary {
            id: "task-1234567890".to_string(),
            objective: "ship next generation TUI workbench".to_string(),
            status: "running".to_string(),
            current_phase: Some("implementation".to_string()),
            yolo_mode: true,
            failure_count: 0,
            review_result: Some("accepted".to_string()),
            artifact_count: 2,
            blocker_reason: None,
        }];
        panel.sync_from_app(&app);

        let lines = render_panel(&mut panel, 80, 12);
        let joined = lines.join("\n");
        assert!(joined.contains("Goals (1)"), "{joined}");
        assert!(
            joined.contains("ship next generation TUI workbench"),
            "{joined}"
        );
        assert!(joined.contains("implementation"), "{joined}");
        assert!(joined.contains("accepted"), "{joined}");
        assert!(joined.contains("artifacts: 2"), "{joined}");
    }

    #[test]
    fn renders_daemon_task_blocker_reason() {
        let mut panel = GoalWorkbenchPanel::new();
        let mut app = App::new("model", "session");
        app.daemon_tasks = vec![DaemonTaskSummary {
            id: "task-blocked".to_string(),
            objective: "finish migration".to_string(),
            status: "blocked".to_string(),
            current_phase: Some("verification".to_string()),
            yolo_mode: false,
            failure_count: 3,
            review_result: None,
            artifact_count: 0,
            blocker_reason: Some("waiting for approval".to_string()),
        }];
        panel.sync_from_app(&app);

        let lines = render_panel(&mut panel, 80, 16);
        let joined = lines.join("\n");
        assert!(joined.contains("blocked"), "{joined}");
        assert!(joined.contains("waiting for approval"), "{joined}");
    }

    #[test]
    fn renders_empty_goal_guidance() {
        let mut panel = GoalWorkbenchPanel::new();
        let app = App::new("model", "session");
        panel.sync_from_app(&app);

        let lines = render_panel(&mut panel, 70, 10);
        let joined = lines.join("\n");
        assert!(joined.contains("No active daemon goal"), "{joined}");
        assert!(joined.contains("/tasks start --yolo"), "{joined}");
    }
}
