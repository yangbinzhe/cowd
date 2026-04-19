//! Skill Security Scanner
//!
//! Scans skill content for security issues:
//! - Credential exposure (API keys, passwords, tokens)
//! - Command injection patterns
//! - Suspicious external resource access
//! - Excessive permission requests
//! - Data exfiltration patterns

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::fs;

/// Security scan result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityScanResult {
    /// Overall scan status
    pub status: SecurityStatus,
    /// List of findings
    pub findings: Vec<SecurityFinding>,
    /// Scan timestamp
    pub scanned_at: String,
    /// Skill name
    pub skill_name: String,
}

/// Security status level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecurityStatus {
    Safe,
    Warning,
    Danger,
}

/// Individual security finding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityFinding {
    /// Severity level
    pub severity: Severity,
    /// Category of the issue
    pub category: FindingCategory,
    /// Description of the finding
    pub description: String,
    /// Location in the content (line number or range)
    pub location: Option<String>,
    /// Suggestion for fixing
    pub suggestion: Option<String>,
}

/// Finding severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

/// Category of security finding
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingCategory {
    CredentialExposure,
    CommandInjection,
    ExternalResource,
    PermissionEscalation,
    DataExfiltration,
    UnsafeExecution,
    SensitiveData,
}

/// Scan a skill file for security issues
pub fn scan_skill_file(path: &Path) -> std::io::Result<SecurityScanResult> {
    let content = fs::read_to_string(path)?;
    let skill_name = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    Ok(scan_skill_content(&content, &skill_name))
}

/// Scan skill content for security issues
pub fn scan_skill_content(content: &str, skill_name: &str) -> SecurityScanResult {
    let mut findings = Vec::new();

    // Run all scanners
    findings.extend(scan_credentials(content));
    findings.extend(scan_command_injection(content));
    findings.extend(scan_external_resources(content));
    findings.extend(scan_permission_escalation(content));
    findings.extend(scan_data_exfiltration(content));

    // Determine overall status
    let status = determine_status(&findings);

    let scanned_at = chrono_lite_timestamp();

    SecurityScanResult {
        status,
        findings,
        scanned_at,
        skill_name: skill_name.to_string(),
    }
}

/// Scan for credential exposure
fn scan_credentials(content: &str) -> Vec<SecurityFinding> {
    let mut findings = Vec::new();

    // Patterns for common credential formats
    let credential_patterns: Vec<(&str, &str)> = vec![
        (r"(?i)(api[_-]?key)\s*[:=]\s*[a-zA-Z0-9_-]{20,}", "Potential API key"),
        (r"(?i)(password|passwd|pwd)\s*[:=]\s*[^\s]+", "Hardcoded password"),
        (r"(?i)(secret|token|auth)\s*[:=]\s*[a-zA-Z0-9_-]{20,}", "Potential secret/token"),
        (r"(?i)bearer\s+[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+", "JWT token"),
        (r"(?i)(aws[_-]?access[_-]?key|aws[_-]?secret)", "AWS credential reference"),
        (r"ghp_[a-zA-Z0-9]{36}", "GitHub Personal Access Token"),
        (r"sk-[a-zA-Z0-9]{48}", "OpenAI API Key"),
        (r"sk-proj-[a-zA-Z0-9_-]{48,}", "OpenAI Project Key"),
    ];

    for (pattern, description) in &credential_patterns {
        if let Ok(re) = regex::Regex::new(pattern) {
            for (line_num, line) in content.lines().enumerate() {
                if re.is_match(line) {
                    findings.push(SecurityFinding {
                        severity: Severity::Critical,
                        category: FindingCategory::CredentialExposure,
                        description: format!("{} detected in skill content", description),
                        location: Some(format!("line {}", line_num + 1)),
                        suggestion: Some("Use environment variables instead of hardcoded credentials".to_string()),
                    });
                }
            }
        }
    }

    findings
}

/// Scan for command injection patterns
fn scan_command_injection(content: &str) -> Vec<SecurityFinding> {
    let mut findings = Vec::new();

    let injection_patterns = [
        (r"(?i)\brm\s+-rf\s+(/\*|/home|/etc|/var)", "Destructive command with root path"),
        (r"(?i);\s*(rm|del|format)\b", "Command chaining with destructive command"),
        (r"(?i)\|\s*(bash|sh|cmd|powershell)\b", "Pipe to shell execution"),
        (r"(?i)\$\([^)]{50,}\)", "Command substitution with long content"),
        (r"(?i)eval\s*\(\s*\$", "Eval with variable interpolation"),
        (r"(?i)`[^`]{50,}`", "Backtick command substitution"),
    ];

    for (pattern, description) in &injection_patterns {
        if let Ok(re) = regex::Regex::new(pattern) {
            for (line_num, line) in content.lines().enumerate() {
                if re.is_match(line) {
                    findings.push(SecurityFinding {
                        severity: Severity::High,
                        category: FindingCategory::CommandInjection,
                        description: description.to_string(),
                        location: Some(format!("line {}", line_num + 1)),
                        suggestion: Some("Validate and sanitize all user inputs before command execution".to_string()),
                    });
                }
            }
        }
    }

    findings
}

/// Scan for external resource access
fn scan_external_resources(content: &str) -> Vec<SecurityFinding> {
    let mut findings = Vec::new();

    let external_patterns: Vec<(&str, &str)> = vec![
        (r"https?://[a-zA-Z0-9.-]+\.(tk|ml|ga|cf|gq)\b", "External domain (.tk, .ml, etc.)"),
        (r"https?://[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}", "IP address URL"),
        (r"(?i)(curl|wget)\s+https?://", "External download"),
        (r"(?i)fetch\s*\(\s*https?://", "External fetch request"),
    ];

    for (pattern, description) in &external_patterns {
        if let Ok(re) = regex::Regex::new(pattern) {
            for (line_num, line) in content.lines().enumerate() {
                if re.is_match(line) {
                    findings.push(SecurityFinding {
                        severity: Severity::Medium,
                        category: FindingCategory::ExternalResource,
                        description: description.to_string(),
                        location: Some(format!("line {}", line_num + 1)),
                        suggestion: Some("Verify external URLs are from trusted sources".to_string()),
                    });
                }
            }
        }
    }

    findings
}

/// Scan for permission escalation patterns
fn scan_permission_escalation(content: &str) -> Vec<SecurityFinding> {
    let mut findings = Vec::new();

    let escalation_patterns = [
        (r"(?i)sudo\s+", "Sudo command execution"),
        (r"(?i)(chmod|chown)\s+777\b", "Overly permissive file permissions"),
        (r"(?i)su\s+root", "Root user switch"),
        (r"(?i)--privileged", "Privileged container mode"),
        (r"(?i)--cap-add\s+ALL", "All capabilities added"),
    ];

    for (pattern, description) in &escalation_patterns {
        if let Ok(re) = regex::Regex::new(pattern) {
            for (line_num, line) in content.lines().enumerate() {
                if re.is_match(line) {
                    findings.push(SecurityFinding {
                        severity: Severity::Medium,
                        category: FindingCategory::PermissionEscalation,
                        description: description.to_string(),
                        location: Some(format!("line {}", line_num + 1)),
                        suggestion: Some("Use least-privilege principle for permissions".to_string()),
                    });
                }
            }
        }
    }

    findings
}

/// Scan for data exfiltration patterns
fn scan_data_exfiltration(content: &str) -> Vec<SecurityFinding> {
    let mut findings = Vec::new();

    let exfiltration_patterns = [
        (r"(?i)(curl|wget).*\$?(HOME|USER|PATH|SECRET|TOKEN|KEY)", "Potential env data exfiltration"),
        (r"(?i)base64.*\$(HOME|USER|SECRET|TOKEN)", "Encoded env data exfiltration"),
        (r"(?i)(cat|read)\s+\.(ssh|aws|config|env)", "Reading sensitive files"),
    ];

    for (pattern, description) in &exfiltration_patterns {
        if let Ok(re) = regex::Regex::new(pattern) {
            for (line_num, line) in content.lines().enumerate() {
                if re.is_match(line) {
                    findings.push(SecurityFinding {
                        severity: Severity::High,
                        category: FindingCategory::DataExfiltration,
                        description: description.to_string(),
                        location: Some(format!("line {}", line_num + 1)),
                        suggestion: Some("Avoid exfiltrating sensitive environment data".to_string()),
                    });
                }
            }
        }
    }

    findings
}

/// Determine overall security status from findings
fn determine_status(findings: &[SecurityFinding]) -> SecurityStatus {
    if findings.is_empty() {
        SecurityStatus::Safe
    } else if findings.iter().any(|f| f.severity == Severity::Critical) {
        SecurityStatus::Danger
    } else if findings.iter().any(|f| f.severity == Severity::High) {
        SecurityStatus::Danger
    } else if findings.iter().any(|f| f.severity == Severity::Medium) {
        SecurityStatus::Warning
    } else {
        SecurityStatus::Warning
    }
}

/// Simple timestamp (avoiding chrono dependency)
fn chrono_lite_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_content() {
        let content = "# My Skill\n\nA simple skill for demonstration.\n\n## Usage\nRun this when needed.";
        let result = scan_skill_content(content, "test-skill");
        assert_eq!(result.status, SecurityStatus::Safe);
    }

    #[test]
    fn test_credential_detection() {
        let content = r#"
# API Skill
Set API_KEY=sk-1234567890abcdefghij
        "#;
        let result = scan_skill_content(content, "api-skill");
        assert!(!result.findings.is_empty());
        assert_eq!(result.findings[0].category, FindingCategory::CredentialExposure);
    }

    #[test]
    fn test_command_injection_detection() {
        let content = r#"
# Shell Skill
rm -rf /home/*
        "#;
        let result = scan_skill_content(content, "shell-skill");
        assert!(!result.findings.is_empty());
        assert_eq!(result.findings[0].category, FindingCategory::CommandInjection);
    }
}
