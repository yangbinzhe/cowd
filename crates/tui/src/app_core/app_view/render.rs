use cowd_app_protocol::{AppComponentKindV1, AppComponentV1};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Text},
    widgets::{Block, Borders, Gauge, Paragraph, Tabs, Wrap},
    Frame,
};
use serde_json::Value;

use super::AppViewState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppViewRenderLimits {
    pub maximum_depth: usize,
    pub maximum_text_chars: usize,
    pub maximum_visible_items: usize,
}

impl Default for AppViewRenderLimits {
    fn default() -> Self {
        Self {
            maximum_depth: 32,
            maximum_text_chars: 65_536,
            maximum_visible_items: 1_024,
        }
    }
}

pub fn render_app_view(frame: &mut Frame<'_>, area: Rect, state: &AppViewState) {
    render_component(
        frame,
        area,
        &state.document().root,
        state,
        0,
        AppViewRenderLimits::default(),
    );
}

fn render_component(
    frame: &mut Frame<'_>,
    area: Rect,
    component: &AppComponentV1,
    state: &AppViewState,
    depth: usize,
    limits: AppViewRenderLimits,
) {
    if area.width == 0 || area.height == 0 || depth > limits.maximum_depth {
        return;
    }
    match component.kind {
        AppComponentKindV1::Stack => render_children(
            frame,
            area,
            component,
            state,
            depth,
            limits,
            Direction::Vertical,
        ),
        AppComponentKindV1::Split => {
            let direction = if string_property(component, "direction") == Some("vertical") {
                Direction::Vertical
            } else {
                Direction::Horizontal
            };
            render_children(frame, area, component, state, depth, limits, direction);
        }
        AppComponentKindV1::Tabs => render_tabs(frame, area, component, state, depth, limits),
        AppComponentKindV1::Progress => render_progress(frame, area, component, state),
        AppComponentKindV1::Status => render_leaf(
            frame,
            area,
            component,
            state,
            status_lines(component),
            limits,
        ),
        AppComponentKindV1::Metric => render_leaf(
            frame,
            area,
            component,
            state,
            metric_lines(component),
            limits,
        ),
        AppComponentKindV1::Table => render_leaf(
            frame,
            area,
            component,
            state,
            table_lines(component),
            limits,
        ),
        AppComponentKindV1::List => render_leaf(
            frame,
            area,
            component,
            state,
            list_lines(component, "items"),
            limits,
        ),
        AppComponentKindV1::Tree => {
            render_leaf(frame, area, component, state, tree_lines(component), limits)
        }
        AppComponentKindV1::Graph => render_leaf(
            frame,
            area,
            component,
            state,
            graph_lines(component),
            limits,
        ),
        AppComponentKindV1::Timeline => render_leaf(
            frame,
            area,
            component,
            state,
            list_lines(component, "events"),
            limits,
        ),
        AppComponentKindV1::Markdown => render_leaf(
            frame,
            area,
            component,
            state,
            text_lines(component, "markdown"),
            limits,
        ),
        AppComponentKindV1::Code => {
            render_leaf(frame, area, component, state, code_lines(component), limits)
        }
        AppComponentKindV1::Form => render_leaf(
            frame,
            area,
            component,
            state,
            form_lines(component, state),
            limits,
        ),
        AppComponentKindV1::Detail => render_leaf(
            frame,
            area,
            component,
            state,
            detail_lines(component),
            limits,
        ),
        AppComponentKindV1::Empty => render_leaf(
            frame,
            area,
            component,
            state,
            text_lines(component, "message"),
            limits,
        ),
        AppComponentKindV1::Error => render_leaf(
            frame,
            area,
            component,
            state,
            error_lines(component),
            limits,
        ),
        AppComponentKindV1::ActionBar => render_leaf(
            frame,
            area,
            component,
            state,
            action_lines(component, state),
            limits,
        ),
    }
}

fn render_children(
    frame: &mut Frame<'_>,
    area: Rect,
    component: &AppComponentV1,
    state: &AppViewState,
    depth: usize,
    limits: AppViewRenderLimits,
    direction: Direction,
) {
    if component.children.is_empty() {
        render_leaf(
            frame,
            area,
            component,
            state,
            vec!["No content".to_owned()],
            limits,
        );
        return;
    }
    let denominator = u32::try_from(component.children.len()).unwrap_or(u32::MAX);
    let constraints = vec![Constraint::Ratio(1, denominator); component.children.len()];
    let chunks = Layout::default()
        .direction(direction)
        .constraints(constraints)
        .split(area);
    for (child, child_area) in component.children.iter().zip(chunks.iter().copied()) {
        render_component(frame, child_area, child, state, depth + 1, limits);
    }
}

fn render_tabs(
    frame: &mut Frame<'_>,
    area: Rect,
    component: &AppComponentV1,
    state: &AppViewState,
    depth: usize,
    limits: AppViewRenderLimits,
) {
    if component.children.is_empty() {
        render_leaf(
            frame,
            area,
            component,
            state,
            vec!["No tabs".to_owned()],
            limits,
        );
        return;
    }
    let selected = state
        .selection_for(&component.component_id)
        .min(component.children.len() - 1);
    let labels: Vec<Line<'_>> = component
        .children
        .iter()
        .map(|child| Line::from(child.label.as_deref().unwrap_or(&child.accessibility_label)))
        .collect();
    let chunks = Layout::vertical([Constraint::Length(3), Constraint::Min(1)]).split(area);
    frame.render_widget(
        Tabs::new(labels)
            .select(selected)
            .block(component_block(component, state))
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        chunks[0],
    );
    render_component(
        frame,
        chunks[1],
        &component.children[selected],
        state,
        depth + 1,
        limits,
    );
}

fn render_progress(
    frame: &mut Frame<'_>,
    area: Rect,
    component: &AppComponentV1,
    state: &AppViewState,
) {
    let ratio = component
        .properties
        .get("value")
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);
    frame.render_widget(
        Gauge::default()
            .block(component_block(component, state))
            .ratio(ratio)
            .label(format!("{:.0}%", ratio * 100.0)),
        area,
    );
}

fn render_leaf(
    frame: &mut Frame<'_>,
    area: Rect,
    component: &AppComponentV1,
    state: &AppViewState,
    lines: Vec<String>,
    limits: AppViewRenderLimits,
) {
    let selected = state.selection_for(&component.component_id);
    let scroll = state
        .scroll_for(&component.component_id)
        .min(usize::from(u16::MAX)) as u16;
    frame.render_widget(
        Paragraph::new(bounded_lines(lines, selected, limits))
            .block(component_block(component, state))
            .scroll((scroll, 0))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn component_block<'a>(component: &'a AppComponentV1, state: &AppViewState) -> Block<'a> {
    let focused = state.focused_component_id() == Some(component.component_id.as_str());
    Block::default()
        .borders(Borders::ALL)
        .title(
            component
                .label
                .as_deref()
                .unwrap_or(&component.accessibility_label),
        )
        .border_style(if focused {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        })
}

fn bounded_lines(
    lines: Vec<String>,
    selected: usize,
    limits: AppViewRenderLimits,
) -> Text<'static> {
    let mut remaining = limits.maximum_text_chars;
    Text::from(
        lines
            .into_iter()
            .take(limits.maximum_visible_items)
            .enumerate()
            .map(|(index, line)| {
                let bounded: String = line.chars().take(remaining).collect();
                remaining = remaining.saturating_sub(bounded.chars().count());
                let prefix = if index == selected { "› " } else { "  " };
                Line::styled(
                    format!("{prefix}{bounded}"),
                    if index == selected {
                        Style::default().add_modifier(Modifier::REVERSED)
                    } else {
                        Style::default()
                    },
                )
            })
            .collect::<Vec<_>>(),
    )
}

fn status_lines(component: &AppComponentV1) -> Vec<String> {
    vec![format!(
        "{} {}",
        string_property(component, "status").unwrap_or("unknown"),
        string_property(component, "message").unwrap_or("")
    )]
}

fn metric_lines(component: &AppComponentV1) -> Vec<String> {
    vec![format!(
        "{} {}  {}",
        value_text(component.properties.get("value")),
        string_property(component, "unit").unwrap_or(""),
        value_text(component.properties.get("delta"))
    )]
}

fn table_lines(component: &AppComponentV1) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(headers) = component
        .properties
        .get("headers")
        .and_then(Value::as_array)
    {
        lines.push(
            headers
                .iter()
                .map(|item| value_text(Some(item)))
                .collect::<Vec<_>>()
                .join(" │ "),
        );
    }
    if let Some(rows) = component.properties.get("rows").and_then(Value::as_array) {
        lines.extend(rows.iter().map(|row| {
            match row {
                Value::Array(cells) => cells
                    .iter()
                    .map(|item| value_text(Some(item)))
                    .collect::<Vec<_>>()
                    .join(" │ "),
                _ => value_text(Some(row)),
            }
        }));
    }
    lines
}

fn list_lines(component: &AppComponentV1, key: &str) -> Vec<String> {
    component
        .properties
        .get(key)
        .and_then(Value::as_array)
        .map(|items| items.iter().map(|item| value_text(Some(item))).collect())
        .unwrap_or_default()
}

fn tree_lines(component: &AppComponentV1) -> Vec<String> {
    fn visit(value: &Value, depth: usize, lines: &mut Vec<String>) {
        if depth > 32 || lines.len() >= 1_024 {
            return;
        }
        let label = value
            .get("label")
            .or_else(|| value.get("id"))
            .map(|item| value_text(Some(item)))
            .unwrap_or_else(|| value_text(Some(value)));
        lines.push(format!("{}{}", "  ".repeat(depth), label));
        if let Some(children) = value.get("children").and_then(Value::as_array) {
            for child in children {
                visit(child, depth + 1, lines);
            }
        }
    }
    let mut lines = Vec::new();
    if let Some(nodes) = component.properties.get("nodes").and_then(Value::as_array) {
        for node in nodes {
            visit(node, 0, &mut lines);
        }
    }
    lines
}

fn graph_lines(component: &AppComponentV1) -> Vec<String> {
    let mut lines = list_lines(component, "nodes")
        .into_iter()
        .map(|node| format!("● {node}"))
        .collect::<Vec<_>>();
    lines.extend(
        list_lines(component, "edges")
            .into_iter()
            .map(|edge| format!("  ↳ {edge}")),
    );
    lines
}

fn text_lines(component: &AppComponentV1, key: &str) -> Vec<String> {
    string_property(component, key)
        .unwrap_or("")
        .lines()
        .map(str::to_owned)
        .collect()
}

fn code_lines(component: &AppComponentV1) -> Vec<String> {
    let mut lines = vec![format!(
        "language: {}",
        string_property(component, "language").unwrap_or("text")
    )];
    lines.extend(text_lines(component, "code"));
    lines
}

fn form_lines(component: &AppComponentV1, state: &AppViewState) -> Vec<String> {
    let mut lines = list_lines(component, "fields");
    lines.push(format!("> {}", state.form_value(&component.component_id)));
    lines
}

fn detail_lines(component: &AppComponentV1) -> Vec<String> {
    component
        .properties
        .get("entries")
        .and_then(Value::as_object)
        .map(|entries| {
            entries
                .iter()
                .map(|(key, value)| format!("{key}: {}", value_text(Some(value))))
                .collect()
        })
        .unwrap_or_default()
}

fn error_lines(component: &AppComponentV1) -> Vec<String> {
    vec![format!(
        "{}: {}",
        string_property(component, "code").unwrap_or("ERROR"),
        string_property(component, "message").unwrap_or("Unknown application error")
    )]
}

fn action_lines(component: &AppComponentV1, state: &AppViewState) -> Vec<String> {
    state
        .document()
        .actions
        .iter()
        .filter(|action| action.component_id == component.component_id)
        .map(|action| {
            format!(
                "{}{}{}",
                if action.enabled {
                    "[Enter] "
                } else {
                    "[disabled] "
                },
                action.label,
                if action.requires_confirmation {
                    " (confirm)"
                } else {
                    ""
                }
            )
        })
        .collect()
}

fn string_property<'a>(component: &'a AppComponentV1, key: &str) -> Option<&'a str> {
    component.properties.get(key).and_then(Value::as_str)
}

fn value_text(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "—".to_owned(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Number(value)) => value.to_string(),
        Some(other) => serde_json::to_string(other).unwrap_or_else(|_| "<invalid>".to_owned()),
    }
}
