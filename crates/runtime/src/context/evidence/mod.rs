pub mod projection;
pub mod raw;

use harness_contract::context::EvidenceContentKind;
use harness_contract::core::EvidenceRef;
use serde::{Deserialize, Serialize};

use crate::context_ledger::estimate_text_tokens;

pub use harness_contract::context::EvidenceAuditProjection as AuditProjection;
pub use projection::audit_projection;
pub use raw::{
    migrate_legacy_raw_evidence, RawEvidenceMigrationOptions, RawEvidenceMigrationReport,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelReceipt {
    pub evidence_ref: EvidenceRef,
    pub content_kind: EvidenceContentKind,
    pub summary: String,
    pub raw_tokens: u64,
    pub receipt_tokens: u64,
    pub omitted_tokens: u64,
    pub truncated: bool,
}

#[must_use]
pub fn build_tool_receipt(
    tool_name: &str,
    output: &str,
    is_error: bool,
    evidence_ref: EvidenceRef,
    token_budget: u64,
) -> ModelReceipt {
    let content_kind = classify_content(output, is_error);
    let raw_tokens = estimate_text_tokens(output);
    let fixed_prefix = format!(
        "Tool `{tool_name}` {}. Evidence: tool://{}. ",
        if is_error { "failed" } else { "completed" },
        evidence_ref.id()
    );
    let prefix_tokens = estimate_text_tokens(&fixed_prefix);
    let body_budget = token_budget.saturating_sub(prefix_tokens).max(1);
    let body = summarize_body(output, content_kind, body_budget);
    let summary = format!("{fixed_prefix}{body}");
    let receipt_tokens = estimate_text_tokens(&summary);
    ModelReceipt {
        evidence_ref,
        content_kind,
        summary,
        raw_tokens,
        receipt_tokens,
        omitted_tokens: raw_tokens.saturating_sub(receipt_tokens),
        truncated: receipt_tokens < raw_tokens,
    }
}

fn classify_content(output: &str, is_error: bool) -> EvidenceContentKind {
    if is_error {
        EvidenceContentKind::Error
    } else if serde_json::from_str::<serde_json::Value>(output).is_ok() {
        EvidenceContentKind::Json
    } else if output.lines().any(|line| {
        line.starts_with("diff --git") || line.starts_with("@@") || line.starts_with("+++")
    }) {
        EvidenceContentKind::Diff
    } else {
        EvidenceContentKind::Text
    }
}

fn summarize_body(output: &str, kind: EvidenceContentKind, token_budget: u64) -> String {
    if output.is_empty() {
        return "No output.".to_string();
    }
    let normalized = match kind {
        EvidenceContentKind::Json => summarize_json(output),
        EvidenceContentKind::Diff => summarize_diff(output),
        EvidenceContentKind::Error => summarize_error(output),
        EvidenceContentKind::Text | EvidenceContentKind::Media => output.to_string(),
    };
    truncate_head_tail(&normalized, token_budget)
}

fn summarize_json(output: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(output) else {
        return output.to_string();
    };
    match value {
        serde_json::Value::Object(map) => {
            let keys = map.keys().take(32).cloned().collect::<Vec<_>>().join(", ");
            format!("JSON object with {} keys: {keys}. {output}", map.len())
        }
        serde_json::Value::Array(items) => {
            format!("JSON array with {} items. {output}", items.len())
        }
        _ => output.to_string(),
    }
}

fn summarize_diff(output: &str) -> String {
    let added = output
        .lines()
        .filter(|line| line.starts_with('+') && !line.starts_with("+++"))
        .count();
    let removed = output
        .lines()
        .filter(|line| line.starts_with('-') && !line.starts_with("---"))
        .count();
    format!("Diff summary: +{added} -{removed}. {output}")
}

fn summarize_error(output: &str) -> String {
    let lines = output.lines().collect::<Vec<_>>();
    let start = lines.len().saturating_sub(40);
    format!("Error tail: {}", lines[start..].join("\n"))
}

fn truncate_head_tail(value: &str, max_tokens: u64) -> String {
    if estimate_text_tokens(value) <= max_tokens {
        return value.to_string();
    }
    let char_budget = usize::try_from(max_tokens.saturating_mul(3)).unwrap_or(usize::MAX);
    let head = char_budget.saturating_mul(2) / 3;
    let tail = char_budget.saturating_sub(head);
    let chars = value.chars().collect::<Vec<_>>();
    let head_text = chars.iter().take(head).collect::<String>();
    let tail_text = chars
        .iter()
        .skip(chars.len().saturating_sub(tail))
        .collect::<String>();
    format!("{head_text}\n...[omitted; retrieve by evidence ref]...\n{tail_text}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_json_receipt_preserves_reference_and_budget() {
        let output = serde_json::json!({"items": vec!["x".repeat(200); 100]}).to_string();
        let receipt = build_tool_receipt(
            "read_data",
            &output,
            false,
            EvidenceRef::new("tool", "raw-1"),
            120,
        );
        assert!(receipt.truncated);
        assert!(receipt.summary.contains("tool://raw-1"));
        assert!(receipt.receipt_tokens <= 180);
        assert!(receipt.omitted_tokens > 0);
    }
}
