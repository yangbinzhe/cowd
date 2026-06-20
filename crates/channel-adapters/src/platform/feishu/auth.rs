//! Feishu Access Control — group policy engine, @mention gating, bot sender filtering.
//!
//! Matches Hermes' `FeishuGroupRule` + `_admit()` pattern.
//!
//! ## Admission Logic
//!
//! 1. Self-echo prevention: reject messages from the bot itself
//! 2. Admin bypass: admins are always admitted
//! 3. Bot sender gating: filter bot/app senders per `AllowBots` policy
//! 4. P2P bypass: direct messages always admitted
//! 5. Group policy: per-group `GroupRule` with `Policy`, `require_mention`, allowlist/blacklist

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

/// TTL in seconds for the sender name cache (matches Hermes `_FEISHU_SENDER_NAME_TTL_SECONDS`).
const SENDER_NAME_TTL_SECONDS: i64 = 10 * 60; // 600 seconds

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Group-level admission policy.
///
/// Mirrors Hermes `Policy` enum: `open`, `allowlist`, `blacklist`, `admin_only`, `disabled`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Policy {
    /// Admit all messages (subject to other checks like mention gating).
    Open,
    /// Only admit senders in the group's allowlist.
    Allowlist,
    /// Admit all senders except those in the group's blacklist.
    Blacklist,
    /// Only admins are admitted (other checks skipped, handled at step 2).
    AdminOnly,
    /// The group is disabled — no messages are admitted.
    Disabled,
}

/// Bot sender filtering policy.
///
/// Mirrors Hermes `AllowBots` enum: `"none"`, `"mentions"`, `"all"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllowBots {
    /// Drop all messages from bots/apps.
    None,
    /// Only admit bot messages that mention this bot.
    Mentions,
    /// Admit all bot messages.
    All,
}

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

/// Per-group access rule.
///
/// Mirrors Hermes `FeishuGroupRule`.
#[derive(Debug, Clone)]
pub struct GroupRule {
    /// The admission policy for this group.
    pub policy: Policy,
    /// Set of allowed sender identities (union_id or open_id).
    pub allowlist: HashSet<String>,
    /// Set of blocked sender identities (union_id or open_id).
    pub blacklist: HashSet<String>,
    /// Whether @mention is required in this group.
    ///
    /// `None` means "inherit the global `require_mention` setting".
    pub require_mention: Option<bool>,
}

impl Default for GroupRule {
    fn default() -> Self {
        Self {
            policy: Policy::Open,
            allowlist: HashSet::new(),
            blacklist: HashSet::new(),
            require_mention: None,
        }
    }
}

impl GroupRule {
    /// Create a new group rule with the given policy.
    pub fn new(policy: Policy) -> Self {
        Self {
            policy,
            ..Default::default()
        }
    }

    /// Add a sender to the allowlist.
    pub fn with_allowlist_entry(mut self, id: impl Into<String>) -> Self {
        self.allowlist.insert(id.into());
        self
    }

    /// Add a sender to the blacklist.
    pub fn with_blacklist_entry(mut self, id: impl Into<String>) -> Self {
        self.blacklist.insert(id.into());
        self
    }

    /// Set whether @mention is required.
    pub fn with_require_mention(mut self, require: bool) -> Self {
        self.require_mention = Some(require);
        self
    }
}

/// Result of an admission check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmitResult {
    /// Whether the sender is admitted.
    pub admitted: bool,
    /// Human-readable reason for rejection (or `None` when admitted).
    pub reason: Option<String>,
}

impl AdmitResult {
    /// Create an admit result.
    pub fn admit() -> Self {
        Self {
            admitted: true,
            reason: None,
        }
    }

    /// Create a reject result with a reason.
    pub fn reject(reason: impl Into<String>) -> Self {
        Self {
            admitted: false,
            reason: Some(reason.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// AccessControl
// ---------------------------------------------------------------------------

/// Central access control for Feishu message admission.
///
/// Mirrors Hermes' combined group policy engine, bot gating, admin bypass,
/// and sender-name cache.
pub struct AccessControl {
    /// Global @mention requirement toggle.
    pub require_mention: bool,
    /// Global bot sender filtering policy.
    pub allow_bots: AllowBots,
    /// The bot's own open_id (used for self-echo prevention and self-mention detection).
    pub bot_open_id: String,
    /// The bot's own display name.
    pub bot_name: String,
    /// Admin identities (union_ids or open_ids) that bypass all checks.
    pub admins: HashSet<String>,
    /// Per-group rules: `chat_id` → `GroupRule`.
    pub group_rules: HashMap<String, GroupRule>,
    /// Default policy for groups without an explicit rule.
    pub default_group_policy: Policy,
    /// Sender name cache: `open_id` → `(name, expire_at_epoch_seconds)`.
    pub sender_name_cache: Arc<RwLock<HashMap<String, (String, i64)>>>,
}

impl AccessControl {
    /// Create a new access control instance.
    pub fn new(bot_open_id: &str, bot_name: &str) -> Self {
        Self {
            require_mention: false,
            allow_bots: AllowBots::None,
            bot_open_id: bot_open_id.to_string(),
            bot_name: bot_name.to_string(),
            admins: HashSet::new(),
            group_rules: HashMap::new(),
            default_group_policy: Policy::Open,
            sender_name_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ------------------------------------------------------------------
    // Admission
    // ------------------------------------------------------------------

    /// Check whether a sender should be admitted.
    ///
    /// # Arguments
    ///
    /// * `chat_id` — the Feishu chat ID.
    /// * `chat_type` — `"p2p"` or `"group"`.
    /// * `sender_open_id` — the sender's open_id.
    /// * `sender_union_id` — the sender's union_id (optional).
    /// * `is_bot_sender` — whether the sender is a bot/app.
    /// * `bot_was_mentioned` — whether this bot was @mentioned in the message.
    pub async fn admit(
        &self,
        chat_id: &str,
        chat_type: &str,
        sender_open_id: &str,
        sender_union_id: Option<&str>,
        is_bot_sender: bool,
        bot_was_mentioned: bool,
    ) -> AdmitResult {
        // 1. Self-echo prevention
        if self.is_self(sender_open_id, sender_union_id) {
            return AdmitResult::reject("self echo prevention: sender is the bot itself");
        }

        // 2. Admin bypass (check both open_id and union_id)
        if self.is_admin(sender_open_id, sender_union_id) {
            return AdmitResult::admit();
        }

        // 3. Bot sender gating
        if is_bot_sender {
            return self.check_bot_sender(bot_was_mentioned);
        }

        // 4. P2P bypass
        if chat_type == "p2p" {
            return AdmitResult::admit();
        }

        // 5. Group policy
        self.check_group_policy(chat_id, sender_open_id, sender_union_id, bot_was_mentioned)
    }

    // ------------------------------------------------------------------
    // Sender name cache
    // ------------------------------------------------------------------

    /// Cache a sender's display name with a 10-minute TTL.
    pub async fn cache_sender_name(&self, open_id: &str, name: &str) {
        let expire_at = chrono::Utc::now().timestamp() + SENDER_NAME_TTL_SECONDS;
        self.sender_name_cache
            .write()
            .await
            .insert(open_id.to_string(), (name.to_string(), expire_at));
    }

    /// Get a cached sender name (returns `None` if expired or not cached).
    pub async fn get_cached_name(&self, open_id: &str) -> Option<String> {
        let cache = self.sender_name_cache.read().await;
        let entry = cache.get(open_id)?;
        let now = chrono::Utc::now().timestamp();
        if now > entry.1 {
            // Expired — drop the lock and clean up
            drop(cache);
            self.sender_name_cache.write().await.remove(open_id);
            return None;
        }
        Some(entry.0.clone())
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Check if the sender is the bot itself.
    fn is_self(&self, open_id: &str, union_id: Option<&str>) -> bool {
        if open_id == self.bot_open_id {
            return true;
        }
        if let Some(uid) = union_id {
            if uid == self.bot_open_id {
                return true;
            }
        }
        false
    }

    /// Check if the sender is an admin.
    fn is_admin(&self, open_id: &str, union_id: Option<&str>) -> bool {
        if self.admins.contains(open_id) {
            return true;
        }
        if let Some(uid) = union_id {
            if self.admins.contains(uid) {
                return true;
            }
        }
        false
    }

    /// Check bot sender against the global `AllowBots` policy.
    fn check_bot_sender(&self, bot_was_mentioned: bool) -> AdmitResult {
        match self.allow_bots {
            AllowBots::None => AdmitResult::reject("bot sender filtered by allow_bots=none"),
            AllowBots::Mentions => {
                if bot_was_mentioned {
                    AdmitResult::admit()
                } else {
                    AdmitResult::reject(
                        "bot sender filtered: @mention required for bots under allow_bots=mentions",
                    )
                }
            }
            AllowBots::All => AdmitResult::admit(),
        }
    }

    /// Check group-level admission policy.
    fn check_group_policy(
        &self,
        chat_id: &str,
        sender_open_id: &str,
        sender_union_id: Option<&str>,
        bot_was_mentioned: bool,
    ) -> AdmitResult {
        let rule = self
            .group_rules
            .get(chat_id)
            .cloned()
            .unwrap_or_else(|| GroupRule::new(self.default_group_policy.clone()));

        // Mention gating (per-group override, fallback to global)
        let require_mention = rule.require_mention.unwrap_or(self.require_mention);
        if require_mention && !bot_was_mentioned {
            return AdmitResult::reject("@mention required in group");
        }

        match &rule.policy {
            Policy::Disabled => AdmitResult::reject("group disabled"),
            Policy::Open => AdmitResult::admit(),
            Policy::AdminOnly => {
                // Admin check was step 2 — if we reached here, sender is not an admin
                AdmitResult::reject("admin-only group: sender is not an admin")
            }
            Policy::Allowlist => {
                if self
                    .sender_identity_iter(sender_open_id, sender_union_id)
                    .any(|id| rule.allowlist.contains(id))
                {
                    AdmitResult::admit()
                } else {
                    AdmitResult::reject("sender not in group allowlist")
                }
            }
            Policy::Blacklist => {
                if self
                    .sender_identity_iter(sender_open_id, sender_union_id)
                    .any(|id| rule.blacklist.contains(id))
                {
                    AdmitResult::reject("sender is in group blacklist")
                } else {
                    AdmitResult::admit()
                }
            }
        }
    }

    fn sender_identity_iter<'a>(
        &self,
        open_id: &'a str,
        union_id: Option<&'a str>,
    ) -> impl Iterator<Item = &'a str> {
        let ids: Vec<&'a str> = if let Some(uid) = union_id {
            vec![open_id, uid]
        } else {
            vec![open_id]
        };
        ids.into_iter()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helpers -------------------------------------------------------------

    /// Create a minimal AccessControl for testing.
    fn test_ac() -> AccessControl {
        AccessControl::new("bot_open_id", "TestBot")
    }

    /// Create an AccessControl with the given global require_mention and allow_bots.
    fn ac_with(require_mention: bool, allow_bots: AllowBots) -> AccessControl {
        let mut ac = AccessControl::new("bot_open_id", "TestBot");
        ac.require_mention = require_mention;
        ac.allow_bots = allow_bots;
        ac
    }

    /// Shorthand admit call.
    async fn admit(
        ac: &AccessControl,
        chat_type: &str,
        sender_open_id: &str,
        is_bot_sender: bool,
        bot_was_mentioned: bool,
    ) -> AdmitResult {
        ac.admit(
            "chat_001",
            chat_type,
            sender_open_id,
            None,
            is_bot_sender,
            bot_was_mentioned,
        )
        .await
    }

    // -- Tests ---------------------------------------------------------------

    /// Step 1: Self-echo prevention.
    #[tokio::test]
    async fn test_self_echo_prevention() {
        let ac = test_ac();
        let result = ac
            .admit("chat_001", "group", "bot_open_id", None, false, true)
            .await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("self echo"));
    }

    /// Step 1: Self-echo prevention via union_id.
    #[tokio::test]
    async fn test_self_echo_prevention_via_union_id() {
        let ac = test_ac();
        let result = ac
            .admit(
                "chat_001",
                "group",
                "random_open_id",
                Some("bot_open_id"),
                false,
                true,
            )
            .await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("self echo"));
    }

    /// Step 2: Admin bypasses all checks.
    #[tokio::test]
    async fn test_admin_bypasses_all_checks() {
        let mut ac = test_ac();
        ac.admins.insert("admin_ou_001".to_string());
        ac.default_group_policy = Policy::Disabled;
        // Even in a disabled group, admin should be admitted
        let result = ac
            .admit("chat_001", "group", "admin_ou_001", None, false, false)
            .await;
        assert!(result.admitted);
    }

    /// Step 3: Bot sender filtered when allow_bots=None.
    #[tokio::test]
    async fn test_bot_sender_filtered_when_allow_bots_none() {
        let ac = ac_with(false, AllowBots::None);
        let result = admit(&ac, "group", "some_bot_open_id", true, false).await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("allow_bots=none"));
    }

    /// Step 3: Bot sender admitted when allow_bots=Mentions and mentioned.
    #[tokio::test]
    async fn test_bot_sender_admitted_with_mention() {
        let ac = ac_with(false, AllowBots::Mentions);
        let result = admit(&ac, "group", "some_bot_open_id", true, true).await;
        assert!(result.admitted);
    }

    /// Step 3: Bot sender rejected when allow_bots=Mentions but not mentioned.
    #[tokio::test]
    async fn test_bot_sender_rejected_without_mention() {
        let ac = ac_with(false, AllowBots::Mentions);
        let result = admit(&ac, "group", "some_bot_open_id", true, false).await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("mention"));
    }

    /// Step 3: Bot sender admitted when allow_bots=All.
    #[tokio::test]
    async fn test_bot_sender_admitted_when_allow_bots_all() {
        let ac = ac_with(false, AllowBots::All);
        let result = admit(&ac, "group", "some_bot_open_id", true, false).await;
        assert!(result.admitted);
    }

    /// Step 4: @mention NOT required in P2P (direct messages bypass group policy).
    #[tokio::test]
    async fn test_p2p_bypasses_group_policy() {
        let mut ac = test_ac();
        ac.require_mention = true;
        ac.default_group_policy = Policy::Disabled;
        // P2P should bypass mention requirement and disabled policy
        let result = admit(&ac, "p2p", "user_ou_001", false, false).await;
        assert!(result.admitted);
    }

    /// Step 5: @mention required in group (reject without mention).
    #[tokio::test]
    async fn test_mention_required_in_group_reject_without_mention() {
        let ac = ac_with(true, AllowBots::All);
        let result = admit(&ac, "group", "user_ou_001", false, false).await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("mention"));
    }

    /// Step 5: @mention required in group (admit with mention).
    #[tokio::test]
    async fn test_mention_required_in_group_admit_with_mention() {
        let ac = ac_with(true, AllowBots::All);
        let result = admit(&ac, "group", "user_ou_001", false, true).await;
        assert!(result.admitted);
    }

    /// Step 5: Group-level require_mention overrides global.
    #[tokio::test]
    async fn test_group_level_require_mention_overrides_global() {
        let mut ac = ac_with(true, AllowBots::All); // global require_mention = true
        ac.group_rules.insert(
            "chat_001".to_string(),
            GroupRule::new(Policy::Open).with_require_mention(false),
        );
        // Group overrides to false, so mention not required
        let result = admit(&ac, "group", "user_ou_001", false, false).await;
        assert!(result.admitted);
    }

    /// Step 5: Policy::Disabled rejects all.
    #[tokio::test]
    async fn test_disabled_policy_rejects() {
        let mut ac = test_ac();
        ac.default_group_policy = Policy::Disabled;
        let result = admit(&ac, "group", "user_ou_001", false, false).await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("disabled"));
    }

    /// Step 5: Allowlist admits only listed users.
    #[tokio::test]
    async fn test_allowlist_admits_listed_users() {
        let mut ac = test_ac();
        ac.group_rules.insert(
            "chat_001".to_string(),
            GroupRule::new(Policy::Allowlist).with_allowlist_entry("user_ou_001"),
        );
        // Listed user
        let result = admit(&ac, "group", "user_ou_001", false, false).await;
        assert!(result.admitted);
        // Unlisted user
        let result = admit(&ac, "group", "user_ou_002", false, false).await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("allowlist"));
    }

    /// Step 5: Allowlist matches via union_id.
    #[tokio::test]
    async fn test_allowlist_matches_via_union_id() {
        let mut ac = test_ac();
        ac.group_rules.insert(
            "chat_001".to_string(),
            GroupRule::new(Policy::Allowlist).with_allowlist_entry("user_union_001"),
        );
        let result = ac
            .admit(
                "chat_001",
                "group",
                "random_open_id",
                Some("user_union_001"),
                false,
                false,
            )
            .await;
        assert!(result.admitted);
    }

    /// Step 5: Blacklist rejects listed users.
    #[tokio::test]
    async fn test_blacklist_rejects_listed_users() {
        let mut ac = test_ac();
        ac.group_rules.insert(
            "chat_001".to_string(),
            GroupRule::new(Policy::Blacklist).with_blacklist_entry("user_ou_001"),
        );
        // Blacklisted user
        let result = admit(&ac, "group", "user_ou_001", false, false).await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("blacklist"));
        // Non-blacklisted user
        let result = admit(&ac, "group", "user_ou_002", false, false).await;
        assert!(result.admitted);
    }

    /// Step 5: Policy::AdminOnly rejects non-admin (admin check was step 2).
    #[tokio::test]
    async fn test_admin_only_rejects_non_admin() {
        let mut ac = test_ac();
        ac.default_group_policy = Policy::AdminOnly;
        let result = admit(&ac, "group", "user_ou_001", false, false).await;
        assert!(!result.admitted);
        assert!(result.reason.unwrap().contains("admin-only"));
    }

    /// Step 5: Policy::Open admits.
    #[tokio::test]
    async fn test_open_policy_admits() {
        let ac = test_ac();
        let result = admit(&ac, "group", "user_ou_001", false, false).await;
        assert!(result.admitted);
    }

    /// Default group policy fallback when no group rule exists.
    #[tokio::test]
    async fn test_default_group_policy_fallback() {
        let mut ac = test_ac();
        ac.default_group_policy = Policy::Blacklist;
        ac.group_rules.insert(
            "chat_002".to_string(),
            GroupRule::new(Policy::Open), // Only for chat_002
        );
        // chat_001 has no rule, falls back to default (Blacklist)
        let result = admit(&ac, "group", "user_ou_001", false, false).await;
        assert!(result.admitted); // Not blacklisted, so admitted

        // Now blacklist the user in the default fallback context
        ac.group_rules.insert(
            "chat_001".to_string(),
            GroupRule::new(Policy::Blacklist).with_blacklist_entry("user_ou_001"),
        );
        let result = admit(&ac, "group", "user_ou_001", false, false).await;
        assert!(!result.admitted);
    }

    /// Sender name cache: store and retrieve.
    #[tokio::test]
    async fn test_sender_name_cache_store_and_retrieve() {
        let ac = test_ac();
        ac.cache_sender_name("user_ou_001", "Alice").await;
        let name = ac.get_cached_name("user_ou_001").await;
        assert_eq!(name, Some("Alice".to_string()));
    }

    /// Sender name cache: expired entry returns None.
    #[tokio::test]
    async fn test_sender_name_cache_expired_entry() {
        let ac = test_ac();
        // Insert with an already-expired TTL
        let expired_at = chrono::Utc::now().timestamp() - 1;
        ac.sender_name_cache
            .write()
            .await
            .insert("user_ou_001".to_string(), ("Alice".to_string(), expired_at));
        let name = ac.get_cached_name("user_ou_001").await;
        assert_eq!(name, None);
    }

    /// Sender name cache: missing entry returns None.
    #[tokio::test]
    async fn test_sender_name_cache_missing_entry() {
        let ac = test_ac();
        let name = ac.get_cached_name("nobody").await;
        assert_eq!(name, None);
    }

    /// GroupRule builder methods.
    #[test]
    fn test_group_rule_builders() {
        let rule = GroupRule::new(Policy::Allowlist)
            .with_allowlist_entry("alice")
            .with_allowlist_entry("bob")
            .with_blacklist_entry("charlie")
            .with_require_mention(true);

        assert_eq!(rule.policy, Policy::Allowlist);
        assert!(rule.allowlist.contains("alice"));
        assert!(rule.allowlist.contains("bob"));
        assert!(rule.blacklist.contains("charlie"));
        assert_eq!(rule.require_mention, Some(true));
    }

    /// GroupRule default values.
    #[test]
    fn test_group_rule_defaults() {
        let rule = GroupRule::default();
        assert_eq!(rule.policy, Policy::Open);
        assert!(rule.allowlist.is_empty());
        assert!(rule.blacklist.is_empty());
        assert_eq!(rule.require_mention, None);
    }

    /// AdmitResult builders.
    #[test]
    fn test_admit_result_builders() {
        let admit = AdmitResult::admit();
        assert!(admit.admitted);
        assert!(admit.reason.is_none());

        let reject = AdmitResult::reject("test reason");
        assert!(!reject.admitted);
        assert_eq!(reject.reason, Some("test reason".to_string()));
    }

    /// TTL constant matches Hermes `_FEISHU_SENDER_NAME_TTL_SECONDS`.
    #[test]
    fn test_ttl_constant_matches_hermes() {
        assert_eq!(SENDER_NAME_TTL_SECONDS, 600);
    }
}
