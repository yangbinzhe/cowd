pub mod projection;
pub mod raw;

use harness_contract::context::EvidenceContentKind;
use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::context_ledger::estimate_text_tokens;

pub use harness_contract::context::EvidenceAuditProjection as AuditProjection;
pub use projection::audit_projection;

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
    let raw_lines = output.lines().count();
    let body = summarize_body(tool_name, output, content_kind, body_budget, raw_tokens, raw_lines);
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

fn summarize_body(
    tool_name: &str,
    output: &str,
    kind: EvidenceContentKind,
    token_budget: u64,
    raw_tokens: u64,
    raw_lines: usize,
) -> String {
    if output.is_empty() {
        return "No output.".to_string();
    }
    let normalized = match kind {
        EvidenceContentKind::Json => summarize_json(tool_name, output),
        EvidenceContentKind::Diff => summarize_diff(output),
        EvidenceContentKind::Error => summarize_error(output),
        EvidenceContentKind::Text | EvidenceContentKind::Media => output.to_string(),
    };
    truncate_head_tail(&normalized, token_budget, raw_tokens, raw_lines)
}

fn summarize_json(tool_name: &str, output: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(output) else {
        return output.to_string();
    };
    if matches!(tool_name, "write_file" | "edit_file") {
        if let Some(receipt) = mutation_receipt(tool_name, &value) {
            return receipt;
        }
    }
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

fn mutation_receipt(tool_name: &str, value: &serde_json::Value) -> Option<String> {
    let object = value.as_object()?;
    let operation = object
        .get("type")
        .or_else(|| object.get("operation"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("mutation");
    let path = ["filePath", "path", "target"]
        .into_iter()
        .find_map(|key| object.get(key).and_then(serde_json::Value::as_str))
        .unwrap_or("unknown");
    let original = object
        .get("originalFile")
        .and_then(serde_json::Value::as_str);
    let (content, replacement_count, replace_all) = if tool_name == "edit_file" {
        let original = original?;
        let old = object.get("oldString")?.as_str()?;
        let new = object.get("newString")?.as_str()?;
        let replace_all = object
            .get("replaceAll")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let replacement_count = if replace_all {
            original.matches(old).count()
        } else {
            usize::from(original.contains(old))
        };
        let updated = if replace_all {
            original.replace(old, new)
        } else {
            original.replacen(old, new, 1)
        };
        (updated, replacement_count, replace_all)
    } else {
        (
            object.get("content")?.as_str()?.to_string(),
            usize::from(original.is_some()),
            true,
        )
    };
    let prior_bytes = original.map_or(0, str::len);
    let prior_sha256 = original.map(|value| format!("{:x}", Sha256::digest(value.as_bytes())));
    Some(
        serde_json::json!({
            "operation": operation,
            "path": path,
            "content_bytes": content.len(),
            "content_sha256": format!("{:x}", Sha256::digest(content.as_bytes())),
            "prior_bytes": prior_bytes,
            "prior_sha256": prior_sha256,
            "replacement_count": replacement_count,
            "replace_all": replace_all,
            "detail": "full mutation output is available through the evidence URI",
        })
        .to_string(),
    )
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

fn truncate_head_tail(value: &str, max_tokens: u64, raw_tokens: u64, raw_lines: usize) -> String {
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
    // Announce the original size and the omission so the model can decide to
    // retrieve the full body by evidence ref instead of guessing (codex parity:
    // "truncated output (original token count: N) / Total output lines: M").
    let kept_tokens = estimate_text_tokens(&format!("{head_text}\n{tail_text}"));
    let omitted_tokens = raw_tokens.saturating_sub(kept_tokens);
    format!(
        "{head_text}\n...[omitted {omitted_tokens} of {raw_tokens} tokens; {raw_lines} total lines; retrieve by evidence ref]...\n{tail_text}"
    )
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
            EvidenceRef::observed("tool", "raw-1"),
            120,
        );
        assert!(receipt.truncated);
        assert!(receipt.summary.contains("tool://raw-1"));
        assert!(receipt.receipt_tokens <= 180);
        assert!(receipt.omitted_tokens > 0);
    }

    #[test]
    fn truncated_receipt_announces_original_size_and_omission() {
        let output = "line\n".repeat(4_000);
        let receipt = build_tool_receipt(
            "read_file",
            &output,
            false,
            EvidenceRef::observed("tool", "big-raw"),
            80,
        );
        assert!(receipt.truncated);
        assert!(receipt.summary.contains("tool://big-raw"));
        assert!(
            receipt.summary.contains("omitted") && receipt.summary.contains("total lines"),
            "the omission marker must announce the original size and line count: {}",
            receipt.summary
        );
    }

    #[test]
    fn mutation_receipt_does_not_echo_file_or_patch_bodies() {
        let output = serde_json::json!({
            "type": "update",
            "filePath": "src/large.rs",
            "oldString": "old-secret-body",
            "newString": "new-important-body".repeat(1_000),
            "originalFile": format!("prefix\n{}\nsuffix\n", "old-secret-body"),
            "structuredPatch": [{
                "oldLines": 40,
                "newLines": 55,
                "lines": vec!["+duplicated-line"; 5_000],
            }],
        })
        .to_string();
        let receipt = build_tool_receipt(
            "edit_file",
            &output,
            false,
            EvidenceRef::observed("tool", "mutation-raw"),
            100_000,
        );
        assert!(receipt.summary.contains("src/large.rs"));
        assert!(receipt.summary.contains("tool://mutation-raw"));
        assert!(!receipt.summary.contains("old-secret-body"));
        assert!(!receipt.summary.contains("duplicated-line"));
        assert!(receipt.receipt_tokens < 256);
        assert!(receipt.truncated);
    }

    #[test]
    fn already_compact_transaction_receipt_keeps_per_file_evidence() {
        let output = serde_json::json!({
            "type": "mutation_apply",
            "appliedCount": 2,
            "applied": [
                {"path": "src/a.rs", "sha256": "aaa", "replacementCount": 1},
                {"path": "src/b.rs", "sha256": "bbb", "replacementCount": 2},
            ],
        })
        .to_string();
        let receipt = build_tool_receipt(
            "apply_patch_transaction",
            &output,
            false,
            EvidenceRef::observed("tool", "transaction-raw"),
            1_000,
        );
        assert!(receipt.summary.contains("src/a.rs"));
        assert!(receipt.summary.contains("src/b.rs"));
        assert!(receipt.summary.contains("aaa"));
        assert!(receipt.summary.contains("bbb"));
    }
}
