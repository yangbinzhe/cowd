use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::app::{App, CurrentTaskSummary};
use crate::components::panel_scroll::PanelScrollState;
use crate::components::{Component, EventResult, RenderContext};
use crate::runtime_control_store::TaskSummary;

#[derive(Debug, Clone, Default)]
pub struct GoalWorkbenchPanel {
    current_task: Option<CurrentTaskSummary>,
    gateway_tasks: Vec<TaskSummary>,
    gateway_task_count: Option<u64>,
    pending_approvals: Option<u64>,
    scroll: PanelScrollState,
}

impl GoalWorkbenchPanel {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.current_task = app.current_task.clone();
        self.gateway_tasks = app.gateway_tasks.clone();
        self.gateway_task_count = app.gateway_task_count;
        self.pending_approvals = app.gateway_pending_approvals;
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
            self.gateway_task_count
                .unwrap_or(self.gateway_tasks.len() as u64)
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

        if self.gateway_tasks.is_empty() {
            ctx.frame_mut().render_widget(
                Paragraph::new(self.render_empty()).wrap(Wrap { trim: false }),
                inner,
            );
            return;
        }

        lines.push(Line::from(Span::styled(
            "Gateway Tasks",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )));
        for task in self
            .gateway_tasks
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
            {
                self.scroll
                    .sync(lines.len(), usize::from(inner.height.max(1)));
                let visible = lines
                    .into_iter()
                    .skip(self.scroll.offset)
                    .take(self.scroll.viewport_len)
                    .collect::<Vec<_>>();
                Paragraph::new(Text::from(visible))
                    .wrap(Wrap { trim: false })
                    .scroll((0, 0))
            },
            inner,
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
            KeyCode::Char('j') | KeyCode::Down => self.scroll.line_down(),
            KeyCode::Char('k') | KeyCode::Up => self.scroll.line_up(),
            KeyCode::PageDown => self.scroll.page_down(),
            KeyCode::PageUp => self.scroll.page_up(),
            KeyCode::Home => self.scroll.top(),
            KeyCode::End => self.scroll.bottom(),
            _ => return EventResult::NotConsumed,
        }
        EventResult::Consumed
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
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;

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
    fn renders_gateway_task_summary() {
        let mut panel = GoalWorkbenchPanel::new();
        let mut app = App::new("model", "session");
        app.gateway_task_count = Some(1);
        app.gateway_tasks = vec![TaskSummary {
            id: "task-1234567890".to_string(),
            mission_id: "mission-default".to_string(),
            kind: "root".to_string(),
            revision: 1,
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
    fn renders_gateway_task_blocker_reason() {
        let mut panel = GoalWorkbenchPanel::new();
        let mut app = App::new("model", "session");
        app.gateway_tasks = vec![TaskSummary {
            id: "task-blocked".to_string(),
            mission_id: "mission-default".to_string(),
            kind: "root".to_string(),
            revision: 1,
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
