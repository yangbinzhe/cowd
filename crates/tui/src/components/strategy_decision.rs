use harness_contract::projection::{
    ProjectionEntity, StrategyActualStatus, StrategyDecisionProjection, StrategyProofStatus,
};
use ratatui::{
    style::{Color, Modifier, Style},
    text::{Line, Span},
};

#[must_use]
pub fn strategy_matches_target(strategy: &StrategyDecisionProjection, target: &str) -> bool {
    let Some(target) = target.trim().strip_prefix("runtime-execution://") else {
        return false;
    };
    let target = target.split(['/', '?', '#']).next().unwrap_or_default();
    let Some(target) = public_strategy_identifier(target) else {
        return false;
    };
    [
        strategy.execution_id.as_deref(),
        strategy.team_execution_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    .filter_map(public_strategy_identifier)
    .any(|candidate| candidate == target)
}

#[must_use]
pub fn strategy_agent_ids(
    strategy: &StrategyDecisionProjection,
    agents: &[ProjectionEntity],
) -> Vec<String> {
    let Some(team_execution_id) = strategy
        .team_execution_id
        .as_deref()
        .and_then(public_strategy_identifier)
    else {
        return Vec::new();
    };
    let mut ids = agents
        .iter()
        .filter(|agent| {
            agent
                .detail
                .as_ref()
                .and_then(serde_json::Value::as_object)
                .and_then(|detail| detail.get("graph_id"))
                .and_then(serde_json::Value::as_str)
                .and_then(public_strategy_identifier)
                .as_deref()
                == Some(team_execution_id.as_str())
        })
        .filter_map(|agent| public_strategy_identifier(&agent.id))
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    ids
}

/// Build only canonical, safe Runtime backlink targets.  This is shared by
/// all TUI surfaces so a legacy projection cannot put a raw team execution
/// path into a rendered link, focus state, or subsequent Gateway request.
#[must_use]
pub fn strategy_runtime_backlink_targets(
    strategy: &StrategyDecisionProjection,
    agent_ids: &[String],
) -> Vec<String> {
    let Some(execution_id) = strategy
        .team_execution_id
        .as_deref()
        .and_then(public_strategy_identifier)
    else {
        return Vec::new();
    };
    let mut targets = vec![format!("runtime-execution://{execution_id}")];
    targets.extend(agent_ids.iter().filter_map(|agent_id| {
        public_strategy_identifier(agent_id)
            .map(|agent_id| format!("runtime-execution://{execution_id}?agent_id={agent_id}"))
    }));
    targets
}

#[must_use]
pub fn strategy_summary_lines(
    strategy: &StrategyDecisionProjection,
    width: usize,
    agent_ids: &[String],
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    let candidate = strategy
        .selected_candidate
        .map(|candidate| candidate.as_str())
        .unwrap_or("legacy");
    let pattern = strategy
        .pattern
        .map(|pattern| pattern.as_str())
        .unwrap_or("unknown");
    let status = normalized_strategy_status(strategy.status.as_deref());
    lines.push(Line::from(vec![
        Span::styled(
            "Strategy ",
            Style::default()
                .fg(Color::LightMagenta)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{candidate} / {pattern}"),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" · {status} · r{}", strategy.revision),
            Style::default().fg(strategy_status_color(status)),
        ),
    ]));

    if strategy.decision_id.is_none() {
        lines.push(Line::from(Span::styled(
            "Legacy projection · typed estimate and actual outcome are unavailable",
            Style::default().fg(Color::Yellow),
        )));
        return lines;
    }

    let estimate_label = strategy.estimated.as_ref().map_or("unknown", |estimate| {
        if estimate.assumed {
            "assumed"
        } else {
            "calibrated"
        }
    });
    let proof = match strategy.proof_status {
        Some(StrategyProofStatus::Calibrated) => "paired proof",
        Some(StrategyProofStatus::NotProven) => "not proven",
        None => "proof unknown",
    };
    lines.push(Line::from(format!(
        "Model: {estimate_label} · {proof} · source={} · confidence={}",
        strategy
            .source
            .map(strategy_source_label)
            .unwrap_or_else(|| "unknown".to_string()),
        strategy
            .confidence
            .map(|confidence| format!("{confidence}%"))
            .unwrap_or_else(|| "unknown".to_string())
    )));

    if let Some(reason) = strategy.benefit_reasons.first() {
        lines.push(labelled_line(
            "Why: ",
            &public_strategy_text(reason),
            Color::Green,
            width,
        ));
    }
    if let Some(reason) = strategy.cost_reasons.first() {
        lines.push(labelled_line(
            "Cost: ",
            &public_strategy_text(reason),
            Color::Yellow,
            width,
        ));
    }

    let estimated = strategy.estimated.as_ref().map(|estimate| {
        format!(
            "{}ms / merge {}ms / score {}",
            estimate.estimated_critical_path_ms, estimate.merge_cost_ms, estimate.net_benefit_score
        )
    });
    let actual = strategy.actual.as_ref().map(|actual| {
        let speedup = actual
            .actual_speedup_ratio_bp
            .map(|ratio| format!(" / speedup {:.2}x", f64::from(ratio) / 10_000.0))
            .unwrap_or_default();
        let quality = actual
            .quality_score_bp
            .map(|score| format!(" / quality {:.1}%", f64::from(score) / 100.0))
            .unwrap_or_default();
        format!(
            "{}ms / {} tokens / {} tools / merge {}ms{speedup}{quality}",
            actual.duration_ms,
            actual
                .input_tokens
                .saturating_add(actual.output_tokens)
                .saturating_add(actual.cached_tokens),
            actual.tool_calls,
            actual.merge_cost_ms
        )
    });
    lines.push(Line::from(format!(
        "Estimate: {} · Actual: {}",
        estimated.as_deref().unwrap_or("unknown"),
        actual.as_deref().unwrap_or(match strategy.actual_status {
            Some(StrategyActualStatus::Observed) => "observed without metrics",
            Some(StrategyActualStatus::Unknown) | None
                if !matches!(
                    status,
                    "complete" | "completed" | "cancelled" | "failed" | "error"
                ) =>
            {
                "unknown (running)"
            }
            Some(StrategyActualStatus::Unknown) | None => "not observed",
        })
    )));

    let scope_refs = strategy
        .evidence_scopes
        .iter()
        .map(|scope| {
            scope
                .capability_cropped_refs
                .iter()
                .filter(|reference| public_strategy_reference(reference).is_some())
                .count()
        })
        .sum::<usize>();
    let overlap = strategy
        .actual
        .as_ref()
        .filter(|actual| actual.evidence_overlap_observed)
        .map(|actual| format!("{}bp observed", actual.evidence_overlap_bp))
        .unwrap_or_else(|| "unknown".to_string());
    lines.push(Line::from(format!(
        "Scope: {} lanes / {} cropped refs · overlap {overlap}",
        strategy.evidence_scopes.len(),
        scope_refs
    )));
    for scope in strategy.evidence_scopes.iter().take(3) {
        lines.push(labelled_line(
            "Lane: ",
            &format!(
                "{} / {} · {}",
                public_strategy_text(&scope.role_id),
                public_strategy_text(&scope.focus_id),
                public_strategy_text(&scope.responsibility_summary)
            ),
            Color::LightCyan,
            width,
        ));
    }

    if !strategy.downgrades.is_empty() || !strategy.early_stops.is_empty() {
        lines.push(Line::from(format!(
            "Policy changes: {} downgrade / {} early stop",
            strategy.downgrades.len(),
            strategy.early_stops.len()
        )));
    }
    for transition in &strategy.downgrades {
        lines.push(labelled_line(
            "Downgrade: ",
            &format!(
                "r{} · {}",
                transition.revision,
                public_strategy_text(&transition.summary)
            ),
            Color::Yellow,
            width,
        ));
    }
    for transition in &strategy.early_stops {
        lines.push(labelled_line(
            "Early stop: ",
            &format!(
                "r{} · {}",
                transition.revision,
                public_strategy_text(&transition.summary)
            ),
            Color::Yellow,
            width,
        ));
    }
    for reference in strategy
        .evidence_refs
        .iter()
        .filter_map(|reference| public_strategy_reference(reference))
        .take(3)
    {
        lines.push(labelled_line(
            "Evidence backlink: ",
            &reference,
            Color::LightCyan,
            width,
        ));
    }
    let team_id = strategy
        .team_id
        .as_deref()
        .and_then(public_strategy_identifier);
    let team_execution_id = strategy
        .team_execution_id
        .as_deref()
        .and_then(public_strategy_identifier);
    if team_id.is_some() || team_execution_id.is_some() {
        lines.push(Line::from(format!(
            "Team: {} · execution {}",
            team_id.as_deref().unwrap_or("unknown"),
            team_execution_id.as_deref().unwrap_or("unknown")
        )));
        if let Some(execution_id) = team_execution_id.as_deref() {
            lines.push(labelled_line(
                "Team backlink: ",
                &format!("runtime-execution://{execution_id}"),
                Color::LightCyan,
                width,
            ));
        }
    }
    if !agent_ids.is_empty() {
        let public_agent_ids = agent_ids
            .iter()
            .filter_map(|agent_id| public_strategy_identifier(agent_id))
            .take(6)
            .collect::<Vec<_>>();
        if public_agent_ids.is_empty() {
            return lines;
        }
        lines.push(labelled_line(
            "Agents: ",
            &public_agent_ids.join(", "),
            Color::LightCyan,
            width,
        ));
        if let Some(execution_id) = team_execution_id.as_deref() {
            for agent_id in agent_ids.iter().take(6) {
                let Some(agent_id) = public_strategy_identifier(agent_id) else {
                    continue;
                };
                lines.push(labelled_line(
                    "Agent backlink: ",
                    &format!("runtime-execution://{execution_id}?agent_id={agent_id}"),
                    Color::LightCyan,
                    width,
                ));
            }
        }
    }
    lines
}

fn labelled_line(label: &'static str, value: &str, color: Color, width: usize) -> Line<'static> {
    let available = width.saturating_sub(label.chars().count()).max(16);
    Line::from(vec![
        Span::styled(label, Style::default().fg(color)),
        Span::styled(preview(value, available), Style::default().fg(Color::White)),
    ])
}

fn preview(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        value
            .chars()
            .take(max.saturating_sub(1))
            .collect::<String>()
            + "…"
    }
}

fn public_strategy_text(value: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = normalized.to_ascii_lowercase();
    if lower.contains("prompt")
        || lower.contains("chain of thought")
        || lower.contains("reasoning")
        || lower.contains("hidden")
        || lower.contains("../")
        || lower.contains("..\\")
        || contains_absolute_path(&normalized)
    {
        return "redacted by strategy surface policy".to_string();
    }
    normalized
}

/// Projection identity is an opaque runtime key, never a pathname.  Returning
/// `None` instead of a partially redacted key keeps generated TUI backlinks
/// from turning compatibility input into a navigable, but invalid, target.
fn public_strategy_identifier(value: &str) -> Option<String> {
    let identifier = value.trim();
    if identifier.is_empty()
        || identifier.len() > 256
        || identifier.chars().any(char::is_whitespace)
        || identifier.contains('/')
        || identifier.contains('\\')
        || identifier.contains("../")
        || identifier.contains("..\\")
        || contains_absolute_path(identifier)
    {
        return None;
    }
    let public = public_strategy_text(identifier);
    (public != "redacted by strategy surface policy").then_some(public)
}

fn contains_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    if value.to_ascii_lowercase().contains("file:") {
        return true;
    }
    bytes.iter().enumerate().any(|(index, byte)| {
        let previous = index.checked_sub(1).and_then(|offset| bytes.get(offset));
        let boundary = previous.is_none_or(|value| {
            value.is_ascii_whitespace()
                || matches!(
                    *value,
                    b'(' | b'[' | b'{' | b':' | b'=' | b',' | b'\'' | b'"' | b'`'
                        | b'>' | b'<' | b';' | b'|' | b'&' | b'-' | b'_'
                )
        });
        if *byte == b'/' {
            return boundary
                && bytes
                    .get(index + 1)
                    .is_some_and(|next| *next != b'/');
        }
        byte.is_ascii_alphabetic()
            && bytes.get(index + 1) == Some(&b':')
            && bytes
                .get(index + 2)
                .is_some_and(|next| matches!(*next, b'/' | b'\\'))
            && boundary
    })
}

fn normalized_strategy_status(value: Option<&str>) -> &'static str {
    match value
        .map(public_strategy_text)
        .unwrap_or_else(|| "unknown".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "selected" => "selected",
        "running" => "running",
        "downgraded" => "downgraded",
        "early_stopped" => "early_stopped",
        "completed" => "completed",
        "complete" => "complete",
        "cancelled" => "cancelled",
        "failed" => "failed",
        "error" => "error",
        "degraded" => "degraded",
        _ => "unknown",
    }
}

fn public_strategy_reference(value: &str) -> Option<String> {
    let reference = value.trim();
    let lower = reference.to_ascii_lowercase();
    if reference.is_empty()
        || reference.len() > 256
        || reference.chars().any(char::is_whitespace)
        || reference.starts_with('/')
        || reference.starts_with('\\')
        || reference.contains("../")
        || reference.contains("..\\")
        || lower.starts_with("file:")
        || contains_absolute_path(reference)
    {
        return None;
    }
    Some(reference.to_string())
}

fn strategy_status_color(status: &str) -> Color {
    match status {
        "completed" | "complete" => Color::Green,
        "downgraded" | "early_stopped" | "not_proven" => Color::Yellow,
        "failed" | "cancelled" | "error" => Color::Red,
        _ => Color::Cyan,
    }
}

fn strategy_source_label(source: harness_contract::strategy::StrategyDecisionSource) -> String {
    use harness_contract::strategy::StrategyDecisionSource;
    match source {
        StrategyDecisionSource::Deterministic => "deterministic",
        StrategyDecisionSource::ModelValidated => "model_validated",
        StrategyDecisionSource::ExperienceAdapted => "experience_adapted",
        StrategyDecisionSource::ResourceAdapted => "resource_adapted",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> StrategyDecisionProjection {
        serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-projection-v1.json"
        ))
        .expect("shared strategy projection fixture")
    }

    fn rendered_text(strategy: &StrategyDecisionProjection) -> String {
        strategy_summary_lines(strategy, 120, &["agent-547".to_string()])
            .into_iter()
            .map(|line| {
                line.spans
                    .into_iter()
                    .map(|span| span.content.into_owned())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn shared_projection_renders_decision_proof_outcome_and_backlinks() {
        let strategy = fixture();
        let rendered = rendered_text(&strategy);

        assert!(rendered.contains("team / collaborate"));
        assert!(rendered.contains("calibrated · paired proof"));
        assert!(rendered.contains("Estimate: 48000ms"));
        assert!(rendered.contains("Actual: 51000ms"));
        assert!(rendered.contains("2 lanes / 2 cropped refs"));
        assert!(rendered.contains("Team: team-547 · execution execution-547"));
        assert!(rendered.contains("Agents: agent-547"));
        assert!(rendered.contains("Downgrade: r2"));
        assert!(rendered.contains("Early stop: r3"));
        assert!(rendered.contains("Team backlink: runtime-execution://execution-547"));
        assert!(
            rendered
                .contains("Agent backlink: runtime-execution://execution-547?agent_id=agent-547")
        );
        assert!(strategy_matches_target(
            &strategy,
            "runtime-execution://execution-547"
        ));
        assert!(!strategy_matches_target(&strategy, "execution-547"));
        assert!(!strategy_matches_target(
            &strategy,
            "mfg:execution:execution-547"
        ));
    }

    #[test]
    fn legacy_projection_is_explicitly_unknown_instead_of_inferred() {
        let strategy: StrategyDecisionProjection = serde_json::from_value(serde_json::json!({
            "id": "legacy-strategy",
            "kind": "strategy",
            "revision": 1,
            "evidence_refs": []
        }))
        .expect("legacy strategy entity");
        let rendered = rendered_text(&strategy);

        assert!(rendered.contains("legacy / unknown"));
        assert!(rendered.contains("typed estimate and actual outcome are unavailable"));
    }

    #[test]
    fn agents_are_linked_only_by_the_selected_team_execution_graph() {
        let strategy = fixture();
        let agents = vec![
            ProjectionEntity {
                id: "agent-matching".to_string(),
                kind: "agent".to_string(),
                revision: 1,
                status: Some("completed".to_string()),
                summary: None,
                evidence_refs: Vec::new(),
                detail: Some(serde_json::json!({"graph_id": "execution-547"})),
            },
            ProjectionEntity {
                id: "agent-unrelated".to_string(),
                kind: "agent".to_string(),
                revision: 1,
                status: Some("completed".to_string()),
                summary: None,
                evidence_refs: Vec::new(),
                detail: Some(serde_json::json!({"graph_id": "execution-other"})),
            },
        ];

        assert_eq!(
            strategy_agent_ids(&strategy, &agents),
            vec!["agent-matching".to_string()]
        );
    }

    #[test]
    fn downgraded_strategy_without_outcome_remains_running_on_every_surface() {
        let mut strategy = fixture();
        strategy.status = Some("downgraded".to_string());
        strategy.actual = None;
        strategy.actual_status = Some(StrategyActualStatus::Unknown);

        assert!(rendered_text(&strategy).contains("Actual: unknown (running)"));
    }

    #[test]
    fn tui_strategy_summary_redacts_legacy_paths_and_unsafe_evidence_refs() {
        let mut strategy = fixture();
        strategy.benefit_reasons = vec!["inspect /etc/shadow".to_string()];
        strategy.cost_reasons = vec!["file:///srv/private-output".to_string()];
        strategy.downgrades[0].summary = "../private/strategy-state".to_string();
        strategy.evidence_refs = vec![
            "/var/lib/private".to_string(),
            "..\\windows-secret".to_string(),
            "evidence-safe".to_string(),
        ];
        strategy.evidence_scopes[0].responsibility_summary = "C:\\secrets\\operator".to_string();
        strategy.evidence_scopes[0].capability_cropped_refs =
            vec!["/etc/passwd".to_string(), "scope-evidence-safe".to_string()];

        let rendered = rendered_text(&strategy);
        for secret in [
            "/etc/shadow",
            "file:///srv/private-output",
            "../private/strategy-state",
            "/var/lib/private",
            "..\\windows-secret",
            "C:\\secrets\\operator",
        ] {
            assert!(!rendered.contains(secret));
        }
        assert!(rendered.contains("redacted by strategy surface policy"));
        assert!(rendered.contains("evidence-safe"));
        assert!(rendered.contains("1 cropped refs"));
    }

    #[test]
    fn tui_strategy_summary_rejects_every_shared_path_syntax() {
        let corpus: Vec<String> = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-public-redaction-corpus.json"
        ))
        .expect("shared redaction corpus");
        for secret in corpus {
            assert_eq!(
                public_strategy_text(&format!("strategy detail {secret}")),
                "redacted by strategy surface policy",
                "{secret}"
            );
            assert!(
                public_strategy_reference(&secret).is_none(),
                "unsafe reference {secret}"
            );
        }
    }

    #[test]
    fn tui_strategy_renderer_fails_closed_for_untrusted_status_and_identity_fields() {
        let corpus: Vec<String> = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-public-redaction-corpus.json"
        ))
        .expect("shared redaction corpus");
        for secret in corpus {
            let mut strategy = fixture();
            strategy.status = Some(secret.clone());
            strategy.team_id = Some(secret.clone());
            strategy.team_execution_id = Some(secret.clone());
            let rendered = strategy_summary_lines(&strategy, 120, &[secret.clone()])
                .into_iter()
                .map(|line| {
                    line.spans
                        .into_iter()
                        .map(|span| span.content.into_owned())
                        .collect::<String>()
                })
                .collect::<Vec<_>>()
                .join("\n");

            assert!(!rendered.contains(&secret), "renderer leaked {secret}");
            assert!(rendered.contains("unknown"));
            assert!(!strategy_matches_target(
                &strategy,
                &format!("runtime-execution://{secret}")
            ));
        }
    }
}
