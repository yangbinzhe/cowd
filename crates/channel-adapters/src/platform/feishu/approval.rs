//! Interactive approval card builder for Feishu.
//!
//! This module provides [`ApprovalCard`], a builder for constructing the
//! interactive approval card shown to users when a command requires human
//! approval (matching Hermes' `send_exec_approval`).
//!
//! The card includes:
//! - Orange warning header: "⚠️ Command Approval Required"
//! - Code block previewing the command
//! - Four action buttons (Allow Once / Session / Always / Deny)
//! - Token-based deduplication with 15-minute TTL to prevent double-processing
//!
//! After a decision is made, [`ApprovalCard::build_resolved`] produces a
//! resolved card with green (approved) or red (denied) header.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// TTL in seconds for card action deduplication tokens (15 minutes).
/// Matches Hermes `_FEISHU_CARD_ACTION_DEDUP_TTL_SECONDS`.
pub const CARD_ACTION_DEDUP_TTL_SECONDS: i64 = 900;

/// Orange header warning text shown on the original approval card.
pub const APPROVAL_HEADER_TEXT: &str = "⚠️ Command Approval Required";

/// Label for the "Allow Once" button.
pub const LABEL_ALLOW_ONCE: &str = "✅ Allow Once";
/// Label for the "Approve Session" button.
pub const LABEL_APPROVE_SESSION: &str = "✅ Session";
/// Label for the "Approve Always" button.
pub const LABEL_APPROVE_ALWAYS: &str = "✅ Always";
/// Label for the "Deny" button.
pub const LABEL_DENY: &str = "❌ Deny";

/// Hermes action identifier for single-command approval.
pub const HERMES_ACTION_APPROVE_ONCE: &str = "approve_once";
/// Hermes action identifier for session-wide approval.
pub const HERMES_ACTION_APPROVE_SESSION: &str = "approve_session";
/// Hermes action identifier for permanent approval.
pub const HERMES_ACTION_APPROVE_ALWAYS: &str = "approve_always";
/// Hermes action identifier for denial.
pub const HERMES_ACTION_DENY: &str = "deny";

// ---------------------------------------------------------------------------
// ApprovalCard
// ---------------------------------------------------------------------------

/// Builder for a Feishu interactive approval card.
///
/// Produces the JSON payload for a card with an orange warning header,
/// a code-block preview of the command, and four action buttons.
///
/// # Examples
///
/// ```ignore
/// let card = ApprovalCard::new(42, "rm -rf /tmp/data")
///     .with_description("Deletes temporary analysis data")
///     .build();
/// // card is a JSON string ready to send as a Feishu interactive message
/// ```
pub struct ApprovalCard {
    /// The shell command that requires approval.
    command: String,
    /// Optional human-readable description of the command's purpose.
    description: String,
    /// Unique identifier for this approval request.
    approval_id: u64,
}

impl ApprovalCard {
    /// Create a new approval card builder.
    ///
    /// `approval_id` must be unique per request — it is embedded in button
    /// values so the callback handler can match the response to the request.
    /// `command` is the shell command string shown to the user.
    pub fn new(approval_id: u64, command: &str) -> Self {
        Self {
            command: command.to_string(),
            description: String::new(),
            approval_id,
        }
    }

    /// Attach a human-readable description of what the command does.
    ///
    /// This appears above the code block in the card body.  When omitted,
    /// only the command preview is shown.
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    /// Build the interactive card JSON payload.
    ///
    /// Returns a JSON string suitable for sending as a Feishu `"interactive"`
    /// message.  The card includes:
    ///
    /// - Orange header: "⚠️ Command Approval Required"
    /// - Description text (if set)
    /// - Code block with the command
    /// - Action row with four buttons: Allow Once, Session, Always, Deny
    pub fn build(&self) -> String {
        // Build markdown content
        let mut md_parts: Vec<String> = Vec::new();
        if !self.description.is_empty() {
            md_parts.push(self.description.clone());
        }
        md_parts.push(format!("**Command:**\n```\n{}\n```", self.command));
        let content = md_parts.join("\n");

        // Build action buttons
        let actions = vec![
            self.build_button(LABEL_ALLOW_ONCE, HERMES_ACTION_APPROVE_ONCE, "primary"),
            self.build_button(
                LABEL_APPROVE_SESSION,
                HERMES_ACTION_APPROVE_SESSION,
                "default",
            ),
            self.build_button(
                LABEL_APPROVE_ALWAYS,
                HERMES_ACTION_APPROVE_ALWAYS,
                "default",
            ),
            self.build_button(LABEL_DENY, HERMES_ACTION_DENY, "danger"),
        ];

        json!({
            "config": {"wide_screen_mode": true},
            "header": {
                "title": {"tag": "plain_text", "content": APPROVAL_HEADER_TEXT},
                "template": "orange"
            },
            "elements": [
                {"tag": "markdown", "content": content},
                {"tag": "action", "actions": actions}
            ]
        })
        .to_string()
    }

    /// Build a resolved (post-decision) card.
    ///
    /// After the user clicks a button, replace the original card with this
    /// resolved version to confirm the decision.
    ///
    /// - `choice`: one of the `HERMES_ACTION_*` constants (or `"approved"` /
    ///   `"denied"` for generic resolved cards).
    /// - `user_name`: display name of the user who made the decision.
    ///
    /// Approved cards get a green header (`"✅ Approved by ..."`) and denied
    /// cards get a red header (`"❌ Denied by ..."`).
    pub fn build_resolved(choice: &str, user_name: &str) -> String {
        let (emoji, label, template) = if is_deny_action(choice) {
            ("❌", "Denied", "red")
        } else {
            ("✅", "Approved", "green")
        };

        json!({
            "config": {"wide_screen_mode": true},
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": format!("{} {} by {}", emoji, label, user_name)
                },
                "template": template
            }
        })
        .to_string()
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Build a single action button JSON value.
    fn build_button(&self, label: &str, action: &str, style: &str) -> Value {
        json!({
            "tag": "button",
            "text": {"tag": "plain_text", "content": label},
            "type": style,
            "value": {
                "hermes_action": action,
                "approval_id": self.approval_id
            }
        })
    }
}

// ---------------------------------------------------------------------------
// is_deny_action
// ---------------------------------------------------------------------------

/// Returns `true` if the choice string represents a denial.
fn is_deny_action(choice: &str) -> bool {
    matches!(
        choice,
        HERMES_ACTION_DENY | "denied" | "reject" | "rejected" | "decline" | "declined"
    )
}

// ---------------------------------------------------------------------------
// CardActionDedup
// ---------------------------------------------------------------------------

/// Thread-safe token deduplication store for card action callbacks.
///
/// Feishu may deliver the same card action callback multiple times (at-least-once
/// delivery semantics).  This store tracks which action tokens have already been
/// processed so the handler can skip duplicates.
///
/// Tokens expire after [`CARD_ACTION_DEDUP_TTL_SECONDS`] (15 minutes).
///
/// # Thread safety
///
/// Wraps the inner state in `Arc<Mutex<…>>` so it can be shared across
/// async tasks without needing `tokio::sync` primitives.  The critical
/// section is extremely short (hash-map lookup + insert).
pub struct CardActionDedup {
    /// Map from token → expiry timestamp (Unix seconds).
    inner: Arc<Mutex<HashMap<String, i64>>>,
}

impl CardActionDedup {
    /// Create a new empty dedup store.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Check whether the given `token` has already been processed.
    ///
    /// If the token is new (or its previous entry has expired), it is inserted
    /// and this call returns `false` (= not a duplicate, safe to process).
    /// If the token was already processed within the TTL, returns `true`.
    pub fn is_duplicate(&self, token: &str) -> bool {
        let now = Self::current_timestamp();
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        // If the token exists and hasn't expired, it IS a duplicate.
        if let Some(&expiry) = map.get(token) {
            if now < expiry {
                return true;
            }
        }

        // Insert/update the token.
        map.insert(token.to_string(), now + CARD_ACTION_DEDUP_TTL_SECONDS);
        false
    }

    /// Remove the given token from the store (e.g., on handler failure so it
    /// can be retried).
    pub fn remove(&self, token: &str) {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.remove(token);
    }

    /// Return the number of tokens currently tracked (including expired).
    pub fn len(&self) -> usize {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.len()
    }

    /// Return `true` if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Current Unix timestamp in seconds.
    ///
    /// Uses `std::time::SystemTime::now()` instead of `chrono` to avoid
    /// pulling in a `chrono` dep just for this single call.  The resolution
    /// is second-level, which is sufficient for a 15-minute TTL.
    fn current_timestamp() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
}

impl Default for CardActionDedup {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // build() — card JSON structure
    // ------------------------------------------------------------------

    #[test]
    fn test_build_card_structure() {
        let card = ApprovalCard::new(42, "ls -la /tmp");
        let payload = card.build();
        let v: Value = serde_json::from_str(&payload).expect("valid JSON");

        // Config
        assert_eq!(v["config"]["wide_screen_mode"], true);

        // Header
        assert_eq!(v["header"]["title"]["content"], APPROVAL_HEADER_TEXT);
        assert_eq!(v["header"]["template"], "orange");

        // Elements
        let elements = v["elements"].as_array().expect("elements array");
        assert_eq!(elements.len(), 2, "expected markdown + action row");

        // First element: markdown with command preview
        let md = &elements[0];
        assert_eq!(md["tag"], "markdown");
        let md_content = md["content"].as_str().unwrap();
        assert!(md_content.contains("ls -la /tmp"));
        assert!(md_content.contains("**Command:**"));
        assert!(md_content.contains("```"));

        // Second element: action row with 4 buttons
        let action_row = &elements[1];
        assert_eq!(action_row["tag"], "action");
        let buttons = action_row["actions"].as_array().unwrap();
        assert_eq!(buttons.len(), 4);

        // Verify each button
        let expected: Vec<(&str, &str, &str)> = vec![
            (LABEL_ALLOW_ONCE, HERMES_ACTION_APPROVE_ONCE, "primary"),
            (
                LABEL_APPROVE_SESSION,
                HERMES_ACTION_APPROVE_SESSION,
                "default",
            ),
            (
                LABEL_APPROVE_ALWAYS,
                HERMES_ACTION_APPROVE_ALWAYS,
                "default",
            ),
            (LABEL_DENY, HERMES_ACTION_DENY, "danger"),
        ];

        for (i, (label, action, style)) in expected.iter().enumerate() {
            let btn = &buttons[i];
            assert_eq!(btn["tag"], "button");
            assert_eq!(btn["text"]["tag"], "plain_text");
            assert_eq!(btn["text"]["content"], *label);
            assert_eq!(btn["type"], *style);
            assert_eq!(btn["value"]["hermes_action"], *action);
            assert_eq!(btn["value"]["approval_id"], 42);
        }
    }

    #[test]
    fn test_build_card_with_description() {
        let card =
            ApprovalCard::new(1, "echo hello").with_description("Prints a greeting to stdout");
        let payload = card.build();
        let v: Value = serde_json::from_str(&payload).expect("valid JSON");

        let elements = v["elements"].as_array().unwrap();
        let md_content = elements[0]["content"].as_str().unwrap();
        assert!(md_content.contains("Prints a greeting to stdout"));
        assert!(md_content.contains("echo hello"));
    }

    #[test]
    fn test_build_card_without_description() {
        let card = ApprovalCard::new(7, "cargo build --release");
        let payload = card.build();
        let v: Value = serde_json::from_str(&payload).expect("valid JSON");

        let elements = v["elements"].as_array().unwrap();
        let md_content = elements[0]["content"].as_str().unwrap();
        // Command block should still be there
        assert!(md_content.contains("cargo build --release"));
        // Should start directly with "**Command:**" (no leading description line)
        assert!(md_content.starts_with("**Command:**"));
    }

    #[test]
    fn test_build_card_different_approval_ids() {
        let card_a = ApprovalCard::new(100, "cmd-a");
        let card_b = ApprovalCard::new(200, "cmd-b");

        let va: Value = serde_json::from_str(&card_a.build()).unwrap();
        let vb: Value = serde_json::from_str(&card_b.build()).unwrap();

        let id_a = va["elements"][1]["actions"][0]["value"]["approval_id"].as_i64();
        let id_b = vb["elements"][1]["actions"][0]["value"]["approval_id"].as_i64();

        assert_eq!(id_a, Some(100));
        assert_eq!(id_b, Some(200));
    }

    // ------------------------------------------------------------------
    // build_resolved() — post-decision cards
    // ------------------------------------------------------------------

    #[test]
    fn test_build_resolved_approve_once() {
        let payload = ApprovalCard::build_resolved("approve_once", "Alice");
        let v: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(v["header"]["template"], "green");
        let title = v["header"]["title"]["content"].as_str().unwrap();
        assert!(title.starts_with("✅"));
        assert!(title.contains("Approved"));
        assert!(title.contains("Alice"));
    }

    #[test]
    fn test_build_resolved_approve_session() {
        let payload = ApprovalCard::build_resolved("approve_session", "Bob");
        let v: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(v["header"]["template"], "green");
        assert!(v["header"]["title"]["content"]
            .as_str()
            .unwrap()
            .contains("Bob"));
    }

    #[test]
    fn test_build_resolved_approve_always() {
        let payload = ApprovalCard::build_resolved("approve_always", "Carol");
        let v: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(v["header"]["template"], "green");
        assert!(v["header"]["title"]["content"]
            .as_str()
            .unwrap()
            .contains("Carol"));
    }

    #[test]
    fn test_build_resolved_generic_approved() {
        let payload = ApprovalCard::build_resolved("approved", "Dave");
        let v: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(v["header"]["template"], "green");
    }

    #[test]
    fn test_build_resolved_deny() {
        let payload = ApprovalCard::build_resolved("deny", "Eve");
        let v: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(v["header"]["template"], "red");
        let title = v["header"]["title"]["content"].as_str().unwrap();
        assert!(title.starts_with("❌"));
        assert!(title.contains("Denied"));
        assert!(title.contains("Eve"));
    }

    #[test]
    fn test_build_resolved_deny_variants() {
        for variant in &["denied", "reject", "rejected", "decline", "declined"] {
            let payload = ApprovalCard::build_resolved(variant, "User");
            let v: Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(
                v["header"]["template"], "red",
                "variant '{}' should be red",
                variant
            );
        }
    }

    #[test]
    fn test_build_resolved_has_config() {
        let payload = ApprovalCard::build_resolved("approve_once", "Name");
        let v: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["config"]["wide_screen_mode"], true);
    }

    // ------------------------------------------------------------------
    // CardActionDedup — token deduplication
    // ------------------------------------------------------------------

    #[test]
    fn test_dedup_token_not_duplicate_first_time() {
        let dedup = CardActionDedup::new();
        assert!(!dedup.is_duplicate("token-abc"));
        assert_eq!(dedup.len(), 1);
    }

    #[test]
    fn test_dedup_token_is_duplicate_second_time() {
        let dedup = CardActionDedup::new();
        assert!(!dedup.is_duplicate("token-xyz"));
        assert!(dedup.is_duplicate("token-xyz"));
        // len should still be 1 (same key updated)
        assert_eq!(dedup.len(), 1);
    }

    #[test]
    fn test_dedup_different_tokens_independent() {
        let dedup = CardActionDedup::new();
        assert!(!dedup.is_duplicate("tok-1"));
        assert!(!dedup.is_duplicate("tok-2"));
        assert!(dedup.is_duplicate("tok-1"));
        assert!(dedup.is_duplicate("tok-2"));
        assert_eq!(dedup.len(), 2);
    }

    #[test]
    fn test_dedup_remove_allows_reprocessing() {
        let dedup = CardActionDedup::new();
        // First time: not a duplicate
        assert!(!dedup.is_duplicate("retry-token"));
        // Second time: IS a duplicate
        assert!(dedup.is_duplicate("retry-token"));
        // Remove the token
        dedup.remove("retry-token");
        // Now it should NOT be a duplicate again
        assert!(!dedup.is_duplicate("retry-token"));
    }

    #[test]
    fn test_dedup_expired_token_not_duplicate() {
        let dedup = CardActionDedup::new();

        // Insert a token with an already-expired expiry by manipulating the
        // timestamp: we insert it, then set expiry to a time in the past.
        {
            let mut map = dedup.inner.lock().unwrap();
            // Insert with expiry = 1 (January 1, 1970 00:00:01 UTC — well in the past)
            map.insert("expired-token".to_string(), 1);
        }

        // Token should NOT be considered a duplicate because it's expired.
        assert!(!dedup.is_duplicate("expired-token"));
    }

    #[test]
    fn test_dedup_new_and_empty() {
        let dedup = CardActionDedup::new();
        assert!(dedup.is_empty());
        assert_eq!(dedup.len(), 0);
    }

    #[test]
    fn test_dedup_not_empty_after_insert() {
        let dedup = CardActionDedup::new();
        dedup.is_duplicate("a");
        assert!(!dedup.is_empty());
    }

    #[test]
    fn test_dedup_default_creates_empty() {
        let dedup = CardActionDedup::default();
        assert!(dedup.is_empty());
    }

    // ------------------------------------------------------------------
    // Constants
    // ------------------------------------------------------------------

    #[test]
    fn test_ttl_constant_matches_hermes() {
        assert_eq!(CARD_ACTION_DEDUP_TTL_SECONDS, 900);
    }

    #[test]
    fn test_action_constants_are_distinct() {
        let actions = [
            HERMES_ACTION_APPROVE_ONCE,
            HERMES_ACTION_APPROVE_SESSION,
            HERMES_ACTION_APPROVE_ALWAYS,
            HERMES_ACTION_DENY,
        ];
        let mut seen = std::collections::HashSet::new();
        for a in &actions {
            assert!(seen.insert(a), "action '{}' appeared more than once", a);
        }
    }

    // ------------------------------------------------------------------
    // TDD tests (from spec)
    // ------------------------------------------------------------------

    #[test]
    fn test_approval_card_build_has_correct_structure() {
        let card = ApprovalCard::new(1, "rm -rf /");
        let json = card.build();
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["config"]["wide_screen_mode"], true);
        assert_eq!(
            v["header"]["title"]["content"],
            "⚠️ Command Approval Required"
        );
        assert_eq!(v["header"]["template"], "orange");
        let actions = &v["elements"][1]["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 4);
        assert_eq!(actions[0]["text"]["content"], "✅ Allow Once");
        assert_eq!(actions[0]["type"], "primary");
        assert_eq!(actions[3]["text"]["content"], "❌ Deny");
        assert_eq!(actions[3]["type"], "danger");
    }

    #[test]
    fn test_approval_card_resolved_approved() {
        let json = ApprovalCard::build_resolved("approve_once", "testuser");
        assert!(json.contains("✅"));
        assert!(json.contains("testuser"));
        assert!(json.contains("Approved"));
    }

    #[test]
    fn test_approval_card_resolved_denied() {
        let json = ApprovalCard::build_resolved("deny", "testuser");
        assert!(json.contains("❌"));
        assert!(json.contains("Denied"));
    }

    // ------------------------------------------------------------------
    // Card callback parsing tests
    // ------------------------------------------------------------------

    #[test]
    fn test_card_action_callback_parsing() {
        let callback = serde_json::json!({
            "action": {
                "tag": "button",
                "value": {"hermes_action": "approve_once", "approval_id": 42}
            }
        });

        let action = callback.get("action").unwrap();
        let value = action.get("value").unwrap();
        assert_eq!(value["hermes_action"], "approve_once");
        assert_eq!(value["approval_id"], 42);
    }

    #[test]
    fn test_parse_hermes_action_deny() {
        let callback = serde_json::json!({
            "action": {
                "tag": "button",
                "value": {"hermes_action": "deny", "approval_id": 99}
            }
        });

        let action = callback.get("action").unwrap();
        let value = action.get("value").unwrap();
        assert_eq!(value["hermes_action"], "deny");
        assert_eq!(value["approval_id"], 99);
    }
}
