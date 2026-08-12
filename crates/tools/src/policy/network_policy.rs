//! Unified network domain policy (T6).
//!
//! The policy is a hard ceiling applied after model input: per-call
//! `allowed_domains`/`blocked_domains` can narrow the policy but cannot widen
//! it. Violations are returned as structured receipts so the model can see
//! exactly which domain was rejected and why.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkDomainMode {
    /// Only explicitly allowed domains may be contacted; per-call filters
    /// can narrow further.
    Allow,
    /// Contacting any external domain requires approval; the tool returns a
    /// structured approval requirement instead of executing.
    Ask,
    /// All external network contact is denied.
    Deny,
}

impl Default for NetworkDomainMode {
    fn default() -> Self {
        Self::Allow
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkDomainPolicy {
    #[serde(default)]
    pub mode: NetworkDomainMode,
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub block: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkViolation {
    pub field: String,
    pub domain: String,
    pub reason: String,
    #[serde(rename = "actionTaken")]
    pub action_taken: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkPolicyReceipt {
    pub mode: NetworkDomainMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub violations: Vec<NetworkViolation>,
    #[serde(default)]
    pub requires_approval: bool,
    #[serde(default)]
    pub denied: bool,
}

impl NetworkDomainPolicy {
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            mode: std::env::var("COWD_NETWORK_DOMAIN_MODE")
                .ok()
                .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
                    "allow" => Some(NetworkDomainMode::Allow),
                    "ask" => Some(NetworkDomainMode::Ask),
                    "deny" => Some(NetworkDomainMode::Deny),
                    _ => None,
                })
                .unwrap_or_default(),
            allow: csv_env("COWD_NETWORK_DOMAIN_ALLOW"),
            block: csv_env("COWD_NETWORK_DOMAIN_BLOCK"),
        }
    }

    /// Enforce the policy against one URL (fetch path).
    pub fn enforce_url(&self, url: &str) -> Result<NetworkPolicyReceipt, String> {
        let host = reqwest::Url::parse(url)
            .map_err(|error| format!("invalid URL `{url}`: {error}"))?
            .host_str()
            .ok_or_else(|| format!("URL `{url}` has no host"))?
            .to_ascii_lowercase();
        self.enforce_domain(&host, false)
    }

    /// Enforce the policy against a domain plus per-call domain filters.
    pub fn enforce_domain(
        &self,
        host: &str,
        per_call_allowed: bool,
    ) -> Result<NetworkPolicyReceipt, String> {
        let mut violations = Vec::new();
        if self.mode == NetworkDomainMode::Deny {
            violations.push(NetworkViolation {
                field: "network".to_string(),
                domain: host.to_string(),
                reason: "network domain policy mode is deny".to_string(),
                action_taken: "blocked".to_string(),
            });
            return Ok(NetworkPolicyReceipt {
                mode: self.mode,
                violations,
                requires_approval: false,
                denied: true,
            });
        }
        if is_private_host(host) && !std::env::var("COWD_ALLOW_PRIVATE_NETWORK").is_ok() {
            violations.push(NetworkViolation {
                field: "network".to_string(),
                domain: host.to_string(),
                reason: "private, loopback, or link-local network targets are blocked by default"
                    .to_string(),
                action_taken: "blocked".to_string(),
            });
            return Ok(NetworkPolicyReceipt {
                mode: self.mode,
                violations,
                requires_approval: false,
                denied: true,
            });
        }
        let block_list = self
            .block
            .iter()
            .map(|d| normalize_domain(d))
            .collect::<BTreeSet<_>>();
        if host_matches_any(host, &block_list) {
            violations.push(NetworkViolation {
                field: "network".to_string(),
                domain: host.to_string(),
                reason: "domain is on the network policy block list".to_string(),
                action_taken: "blocked".to_string(),
            });
            return Ok(NetworkPolicyReceipt {
                mode: self.mode,
                violations,
                requires_approval: false,
                denied: true,
            });
        }
        if self.mode == NetworkDomainMode::Ask {
            return Ok(NetworkPolicyReceipt {
                mode: self.mode,
                violations,
                requires_approval: true,
                denied: false,
            });
        }
        let allow_list = self
            .allow
            .iter()
            .map(|d| normalize_domain(d))
            .collect::<BTreeSet<_>>();
        if !allow_list.is_empty() && !host_matches_any(host, &allow_list) {
            violations.push(NetworkViolation {
                field: "network".to_string(),
                domain: host.to_string(),
                reason: "domain is not on the network policy allow list".to_string(),
                action_taken: "blocked".to_string(),
            });
            return Ok(NetworkPolicyReceipt {
                mode: self.mode,
                violations,
                requires_approval: false,
                denied: true,
            });
        }
        if per_call_allowed && !allow_list.is_empty() {
            // Per-call allow lists can only narrow; nothing to record here.
        }
        Ok(NetworkPolicyReceipt {
            mode: self.mode,
            violations,
            requires_approval: false,
            denied: false,
        })
    }

    /// Merge per-call domain filters with the policy and return the effective
    /// filters plus violations for model-supplied attempts to widen policy.
    pub fn merge_call_filters(
        &self,
        call_allowed: Option<&[String]>,
        call_blocked: Option<&[String]>,
    ) -> NetworkPolicyReceipt {
        let mut violations = Vec::new();
        let mut blocked = self.block.clone();
        if let Some(call_blocked) = call_blocked {
            for domain in call_blocked {
                let normalized = normalize_domain(domain);
                if !blocked.iter().any(|d| normalize_domain(d) == normalized) {
                    blocked.push(domain.clone());
                }
            }
        }
        let policy_block = self
            .block
            .iter()
            .map(|d| normalize_domain(d))
            .collect::<BTreeSet<_>>();
        let policy_allow = self
            .allow
            .iter()
            .map(|d| normalize_domain(d))
            .collect::<BTreeSet<_>>();
        if let Some(call_allowed) = call_allowed {
            for domain in call_allowed {
                let normalized = normalize_domain(domain);
                if policy_block.contains(&normalized) {
                    violations.push(NetworkViolation {
                        field: "allowed_domains".to_string(),
                        domain: domain.clone(),
                        reason: "model allowed_domains entry is on the policy block list"
                            .to_string(),
                        action_taken: "dropped".to_string(),
                    });
                } else if !policy_allow.is_empty() && !policy_allow.contains(&normalized) {
                    violations.push(NetworkViolation {
                        field: "allowed_domains".to_string(),
                        domain: domain.clone(),
                        reason: "model allowed_domains entry is outside the policy allow list"
                            .to_string(),
                        action_taken: "dropped".to_string(),
                    });
                }
            }
        }
        NetworkPolicyReceipt {
            mode: self.mode,
            violations,
            requires_approval: self.mode == NetworkDomainMode::Ask,
            denied: self.mode == NetworkDomainMode::Deny,
        }
    }
}

fn csv_env(key: &str) -> Vec<String> {
    std::env::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_domain(domain: &str) -> String {
    let trimmed = domain.trim();
    reqwest::Url::parse(trimmed)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| trimmed.to_string())
        .trim()
        .trim_start_matches('.')
        .trim_end_matches('/')
        .to_ascii_lowercase()
}

fn host_matches_any(host: &str, domains: &BTreeSet<String>) -> bool {
    let host = host.trim_start_matches("www.").to_ascii_lowercase();
    domains.iter().any(|domain| {
        let domain = domain.trim_start_matches("www.").to_ascii_lowercase();
        !domain.is_empty() && (host == domain || host.ends_with(&format!(".{domain}")))
    })
}

fn is_private_host(host: &str) -> bool {
    if matches!(host, "localhost" | "localhost.localdomain") {
        return true;
    }
    if let Ok(address) = host.parse::<std::net::IpAddr>() {
        return match address {
            std::net::IpAddr::V4(ipv4) => {
                ipv4.is_loopback()
                    || ipv4.is_private()
                    || ipv4.is_link_local()
                    || ipv4.is_unspecified()
                    || ipv4.is_multicast()
            }
            std::net::IpAddr::V6(ipv6) => {
                ipv6.is_loopback()
                    || ipv6.is_unspecified()
                    || ipv6.is_multicast()
                    || matches!(
                        ipv6.segments(),
                        [0xfe80, ..] | [0xfc00, ..] | [0xfd00, ..] | [0x2001, 0x0db8, ..]
                    )
            }
        };
    }
    if let Some(ipv4) = host.parse::<std::net::Ipv4Addr>().ok() {
        return ipv4.is_loopback() || ipv4.is_private() || ipv4.is_link_local();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(mode: NetworkDomainMode, allow: &[&str], block: &[&str]) -> NetworkDomainPolicy {
        NetworkDomainPolicy {
            mode,
            allow: allow.iter().map(|value| value.to_string()).collect(),
            block: block.iter().map(|value| value.to_string()).collect(),
        }
    }

    #[test]
    fn deny_mode_blocks_every_domain_with_receipt() {
        let receipt = policy(NetworkDomainMode::Deny, &[], &[])
            .enforce_domain("example.com", false)
            .expect("receipt");
        assert!(receipt.denied);
        assert_eq!(receipt.violations.len(), 1);
    }

    #[test]
    fn allow_mode_blocks_outside_allow_list() {
        let receipt = policy(NetworkDomainMode::Allow, &["docs.rs"], &[])
            .enforce_domain("example.com", false)
            .expect("receipt");
        assert!(receipt.denied);
        assert!(receipt
            .violations
            .iter()
            .any(|violation| violation.action_taken == "blocked"));
    }

    #[test]
    fn ask_mode_requires_approval_without_blocking() {
        let receipt = policy(NetworkDomainMode::Ask, &[], &[])
            .enforce_domain("example.com", false)
            .expect("receipt");
        assert!(receipt.requires_approval);
        assert!(!receipt.denied);
    }

    #[test]
    fn model_cannot_widen_call_filters() {
        let p = policy(NetworkDomainMode::Allow, &["docs.rs"], &["evil.example"]);
        let receipt = p.merge_call_filters(
            Some(&["evil.example".to_string(), "outside.example".to_string()]),
            Some(&["docs.rs".to_string()]),
        );
        assert_eq!(receipt.violations.len(), 2);
        assert!(receipt
            .violations
            .iter()
            .all(|violation| violation.action_taken == "dropped"));
    }

    #[test]
    fn private_network_targets_are_denied_by_default() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::env::var_os("COWD_ALLOW_PRIVATE_NETWORK");
        std::env::remove_var("COWD_ALLOW_PRIVATE_NETWORK");
        let receipt = policy(NetworkDomainMode::Allow, &["127.0.0.1"], &[])
            .enforce_domain("127.0.0.1", false)
            .expect("receipt");
        assert!(receipt.denied);
        assert!(receipt
            .violations
            .iter()
            .any(|violation| violation.reason.contains("private")));
        match previous {
            Some(value) => std::env::set_var("COWD_ALLOW_PRIVATE_NETWORK", value),
            None => std::env::remove_var("COWD_ALLOW_PRIVATE_NETWORK"),
        }
    }

    #[test]
    fn subdomain_matching_is_suffix_based() {
        let p = policy(NetworkDomainMode::Allow, &["example.com"], &[]);
        assert!(
            !p.enforce_domain("docs.example.com", false)
                .expect("ok")
                .denied
        );
        assert!(!p.enforce_domain("example.com", false).expect("ok").denied);
        assert!(
            p.enforce_domain("notexample.com", false)
                .expect("blocked")
                .denied
        );
    }
}
