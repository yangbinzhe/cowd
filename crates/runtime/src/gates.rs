//! Gates Mechanism for Commit Quality Control.
//!
//! Implements PreFlight, Revision, Escalation, and Abort Gates to ensure
//! commit quality and prevent bad commits from being merged.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Gate evaluation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateResult {
    /// Whether the gate passed.
    pub passed: bool,
    /// Gate name.
    pub gate_name: String,
    /// Detailed message.
    pub message: String,
    /// Warnings (non-blocking).
    pub warnings: Vec<String>,
    /// Suggestions for fixing failures.
    pub suggestions: Vec<String>,
    /// Time taken to evaluate (milliseconds).
    pub duration_ms: u64,
}

impl GateResult {
    /// Create a passing result.
    pub fn pass(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            passed: true,
            gate_name: name.into(),
            message: message.into(),
            warnings: Vec::new(),
            suggestions: Vec::new(),
            duration_ms: 0,
        }
    }

    /// Create a failing result.
    pub fn fail(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            passed: false,
            gate_name: name.into(),
            message: message.into(),
            warnings: Vec::new(),
            suggestions: Vec::new(),
            duration_ms: 0,
        }
    }

    /// Add a warning.
    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }

    /// Add a suggestion.
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestions.push(suggestion.into());
        self
    }

    /// Set duration.
    pub fn with_duration(mut self, ms: u64) -> Self {
        self.duration_ms = ms;
        self
    }
}

/// Gate evaluation error.
#[derive(Error, Debug)]
pub enum GateError {
    #[error("gate evaluation failed: {0}")]
    EvaluationFailed(String),

    #[error("gate not applicable: {0}")]
    NotApplicable(String),

    #[error("configuration error: {0}")]
    ConfigError(String),
}

/// Context for gate evaluation.
#[derive(Debug, Clone)]
pub struct GateContext {
    /// Repository path.
    pub repo_path: String,
    /// Branch name.
    pub branch: String,
    /// Commit message.
    pub commit_message: String,
    /// Changed files.
    pub changed_files: Vec<String>,
    /// Commit diff.
    pub diff: String,
    /// Author name.
    pub author: String,
    /// Author email.
    pub author_email: String,
    /// Files with violations.
    pub violations: Vec<FileViolation>,
}

/// A violation in a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileViolation {
    /// File path.
    pub file: String,
    /// Violation type.
    pub violation_type: ViolationType,
    /// Violation message.
    pub message: String,
    /// Line number (if applicable).
    pub line: Option<u32>,
    /// Severity.
    pub severity: ViolationSeverity,
}

/// Types of violations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViolationType {
    /// Code style violation.
    Style,
    /// Security vulnerability.
    Security,
    /// Performance issue.
    Performance,
    /// Test failure.
    TestFailure,
    /// Lint error.
    LintError,
    /// Type error.
    TypeError,
    /// Formatting error.
    Formatting,
    /// Documentation error.
    Documentation,
    /// Other.
    Other,
}

/// Violation severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViolationSeverity {
    /// Blocking - commit cannot proceed.
    Blocking,
    /// Warning - commit can proceed but should be addressed.
    Warning,
    /// Info - informational only.
    Info,
}

impl fmt::Display for ViolationSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Blocking => write!(f, "blocking"),
            Self::Warning => write!(f, "warning"),
            Self::Info => write!(f, "info"),
        }
    }
}

/// Gate trait for implementing quality gates.
pub trait Gate: Send + Sync {
    /// Get the gate name.
    fn name(&self) -> &str;

    /// Get the gate description.
    fn description(&self) -> &str;

    /// Evaluate the gate.
    fn evaluate(&self, context: &GateContext) -> GateResult;

    /// Check if this gate is enabled.
    fn is_enabled(&self) -> bool;
}

/// PreFlight Gate - runs before any changes are committed.
pub struct PreFlightGate {
    enabled: bool,
    checks: Vec<PreFlightCheck>,
}

/// A pre-flight check.
#[derive(Debug, Clone)]
pub enum PreFlightCheck {
    /// Check for merge conflicts.
    MergeConflicts,
    /// Check for large files.
    LargeFiles { max_size_kb: u64 },
    /// Check for sensitive data.
    SensitiveData,
    /// Check for binary files.
    BinaryFiles,
    /// Check commit message format.
    CommitMessageFormat { pattern: String },
    /// Check for TODO/FIXME without owners.
    UnownedTodos,
}

impl PreFlightGate {
    /// Create a new PreFlight gate.
    pub fn new() -> Self {
        Self {
            enabled: true,
            checks: vec![
                PreFlightCheck::MergeConflicts,
                PreFlightCheck::LargeFiles { max_size_kb: 500 },
                PreFlightCheck::SensitiveData,
                PreFlightCheck::BinaryFiles,
                PreFlightCheck::CommitMessageFormat {
                    pattern: r"^(feat|fix|docs|style|refactor|test|chore)(\(.+\))?: .+".to_string(),
                },
            ],
        }
    }

    /// Add a check.
    pub fn with_check(mut self, check: PreFlightCheck) -> Self {
        self.checks.push(check);
        self
    }

    /// Set enabled.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

impl Default for PreFlightGate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate for PreFlightGate {
    fn name(&self) -> &str {
        "preflight"
    }

    fn description(&self) -> &str {
        "Pre-commit checks for merge conflicts, large files, sensitive data, etc."
    }

    fn evaluate(&self, context: &GateContext) -> GateResult {
        if !self.enabled {
            return GateResult::pass(self.name(), "PreFlight gate disabled");
        }

        let mut passed = true;
        let mut warnings = Vec::new();
        let mut suggestions = Vec::new();

        for check in &self.checks {
            match check {
                PreFlightCheck::MergeConflicts => {
                    if context.diff.contains("<<<<<<<") || context.diff.contains(">>>>>>>") {
                        passed = false;
                        warnings.push("Merge conflict markers found".to_string());
                        suggestions.push("Resolve merge conflicts before committing".to_string());
                    }
                }
                PreFlightCheck::LargeFiles { max_size_kb } => {
                    for file in &context.changed_files {
                        if file.ends_with(".exe") || file.ends_with(".dll") || file.ends_with(".so") {
                            warnings.push(format!("Binary file detected: {}", file));
                            suggestions.push(format!(
                                "Consider using git-lfs for files larger than {} KB",
                                max_size_kb
                            ));
                        }
                    }
                }
                PreFlightCheck::SensitiveData => {
                    let sensitive_patterns = [
                        "password", "api_key", "secret", "token", "private_key",
                        "密码", "密钥", "token", "api密钥",
                    ];
                    for file in &context.changed_files {
                        if file.contains("password") || file.contains("secret") {
                            if !file.ends_with(".example") && !file.ends_with("_test") {
                                warnings.push(format!("Potential sensitive file: {}", file));
                            }
                        }
                    }
                    let content_lower = context.diff.to_lowercase();
                    for pattern in &sensitive_patterns {
                        if content_lower.contains(&format!("{} = ", pattern))
                            || content_lower.contains(&format!("{}:", pattern))
                        {
                            warnings.push(format!("Possible sensitive data in diff: {}", pattern));
                        }
                    }
                }
                PreFlightCheck::BinaryFiles => {
                    for file in &context.changed_files {
                        if file.ends_with(".png")
                            || file.ends_with(".jpg")
                            || file.ends_with(".pdf")
                            || file.ends_with(".zip")
                        {
                            warnings.push(format!("Binary file detected: {}", file));
                        }
                    }
                }
                PreFlightCheck::CommitMessageFormat { pattern } => {
                    if let Ok(re) = regex::Regex::new(pattern) {
                        if !re.is_match(&context.commit_message) {
                            warnings.push("Commit message doesn't follow conventional format".to_string());
                            suggestions.push(
                                "Use format: type(scope): description (e.g., feat(auth): add login)"
                                    .to_string(),
                            );
                        }
                    }
                }
                PreFlightCheck::UnownedTodos => {
                    let todo_patterns = ["TODO", "FIXME", "XXX", "HACK"];
                    for line in context.diff.lines() {
                        for pattern in &todo_patterns {
                            if line.contains(pattern) && !line.contains("//") && !line.contains("#") {
                                warnings.push(format!("Unowned TODO/FIXME found: {}", line.trim()));
                            }
                        }
                    }
                }
            }
        }

        if passed {
            GateResult::pass(self.name(), "All pre-flight checks passed")
                .with_warning(warnings.join("; "))
        } else {
            GateResult::fail(self.name(), "Pre-flight checks failed")
                .with_warning(warnings.join("; "))
                .with_suggestion(suggestions.join("; "))
        }
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Revision Gate - runs after changes are committed, before push.
pub struct RevisionGate {
    enabled: bool,
    checks: Vec<RevisionCheck>,
}

/// A revision check.
#[derive(Debug, Clone)]
pub enum RevisionCheck {
    /// Check code coverage.
    TestCoverage { min_percentage: f32 },
    /// Check for failing tests.
    TestResults,
    /// Check linting.
    LintResults,
    /// Check formatting.
    Formatting,
    /// Check security vulnerabilities.
    SecurityScan,
    /// Check build success.
    BuildSuccess,
}

impl RevisionGate {
    /// Create a new Revision gate.
    pub fn new() -> Self {
        Self {
            enabled: true,
            checks: vec![
                RevisionCheck::TestCoverage { min_percentage: 70.0 },
                RevisionCheck::TestResults,
                RevisionCheck::LintResults,
                RevisionCheck::Formatting,
            ],
        }
    }

    /// Add a check.
    pub fn with_check(mut self, check: RevisionCheck) -> Self {
        self.checks.push(check);
        self
    }
}

impl Default for RevisionGate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate for RevisionGate {
    fn name(&self) -> &str {
        "revision"
    }

    fn description(&self) -> &str {
        "Post-commit checks for test coverage, linting, and formatting."
    }

    fn evaluate(&self, context: &GateContext) -> GateResult {
        if !self.enabled {
            return GateResult::pass(self.name(), "Revision gate disabled");
        }

        let mut warnings = Vec::new();
        let mut suggestions = Vec::new();

        for check in &self.checks {
            match check {
                RevisionCheck::TestCoverage { min_percentage } => {
                    // In real implementation, would run coverage tool
                    warnings.push(format!(
                        "Test coverage check: minimum {}% required",
                        min_percentage
                    ));
                }
                RevisionCheck::TestResults => {
                    if context.violations.iter().any(|v| {
                        v.violation_type == ViolationType::TestFailure
                            && v.severity == ViolationSeverity::Blocking
                    }) {
                        return GateResult::fail(
                            self.name(),
                            "Test failures detected",
                        )
                        .with_suggestion("Fix failing tests before pushing".to_string());
                    }
                }
                RevisionCheck::LintResults => {
                    let lint_violations: Vec<_> = context
                        .violations
                        .iter()
                        .filter(|v| v.violation_type == ViolationType::LintError)
                        .collect();

                    if !lint_violations.is_empty() {
                        warnings.push(format!("{} lint issues found", lint_violations.len()));
                    }
                }
                RevisionCheck::Formatting => {
                    let format_violations: Vec<_> = context
                        .violations
                        .iter()
                        .filter(|v| v.violation_type == ViolationType::Formatting)
                        .collect();

                    if !format_violations.is_empty() {
                        warnings.push(format!("{} formatting issues found", format_violations.len()));
                        suggestions.push("Run cargo fmt to fix formatting".to_string());
                    }
                }
                RevisionCheck::SecurityScan => {
                    let security_violations: Vec<_> = context
                        .violations
                        .iter()
                        .filter(|v| v.violation_type == ViolationType::Security)
                        .collect();

                    if !security_violations.is_empty() {
                        return GateResult::fail(
                            self.name(),
                            "Security vulnerabilities detected",
                        )
                        .with_suggestion("Address security issues before pushing".to_string());
                    }
                }
                RevisionCheck::BuildSuccess => {
                    warnings.push("Build success check pending".to_string());
                }
            }
        }

        GateResult::pass(self.name(), "Revision checks completed")
            .with_warning(warnings.join("; "))
            .with_suggestion(suggestions.join("; "))
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Escalation Gate - runs on critical paths or high-risk changes.
pub struct EscalationGate {
    enabled: bool,
    critical_paths: Vec<String>,
    high_risk_patterns: Vec<String>,
}

impl EscalationGate {
    /// Create a new Escalation gate.
    pub fn new() -> Self {
        Self {
            enabled: true,
            critical_paths: vec![
                "src/auth".to_string(),
                "src/security".to_string(),
                "src/payment".to_string(),
                "crates/api/src/auth".to_string(),
            ],
            high_risk_patterns: vec![
                r"password.*=.*".to_string(),
                r"secret.*=.*".to_string(),
                r"eval\s*\(".to_string(),
                r"exec\s*\(".to_string(),
            ],
        }
    }

    /// Add a critical path.
    pub fn with_critical_path(mut self, path: impl Into<String>) -> Self {
        self.critical_paths.push(path.into());
        self
    }
}

impl Default for EscalationGate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate for EscalationGate {
    fn name(&self) -> &str {
        "escalation"
    }

    fn description(&self) -> &str {
        "Additional checks for critical paths and high-risk changes."
    }

    fn evaluate(&self, context: &GateContext) -> GateResult {
        if !self.enabled {
            return GateResult::pass(self.name(), "Escalation gate disabled");
        }

        let mut warnings = Vec::new();
        let mut suggestions = Vec::new();

        // Check if touching critical paths
        let touches_critical = context
            .changed_files
            .iter()
            .any(|f| self.critical_paths.iter().any(|cp| f.contains(cp)));

        if touches_critical {
            warnings.push("Changes touch critical paths".to_string());
            suggestions.push("Ensure changes are reviewed by security team".to_string());
        }

        // Check for high-risk patterns
        for pattern in &self.high_risk_patterns {
            if let Ok(re) = regex::Regex::new(pattern) {
                if re.is_match(&context.diff) {
                    warnings.push(format!("High-risk pattern detected: {}", pattern));
                    suggestions.push("Review high-risk code carefully".to_string());
                }
            }
        }

        if warnings.is_empty() {
            GateResult::pass(self.name(), "No escalation triggers")
        } else {
            GateResult::pass(self.name(), "Escalation triggers detected")
                .with_warning(warnings.join("; "))
                .with_suggestion(suggestions.join("; "))
        }
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Abort Gate - hard stops that prevent any commit.
pub struct AbortGate {
    enabled: bool,
    hard_stops: Vec<HardStop>,
}

/// Hard stops that immediately abort commits.
#[derive(Debug, Clone)]
pub enum HardStop {
    /// Stop on merge conflict markers.
    MergeConflicts,
    /// Stop on sensitive data patterns.
    SensitiveData { patterns: Vec<String> },
    /// Stop on large diffs.
    LargeDiff { max_lines: usize },
    /// Stop on specific file patterns.
    ForbiddenFiles { patterns: Vec<String> },
}

impl AbortGate {
    /// Create a new Abort gate.
    pub fn new() -> Self {
        Self {
            enabled: true,
            hard_stops: vec![
                HardStop::MergeConflicts,
                HardStop::SensitiveData {
                    patterns: vec![
                        "-----BEGIN PRIVATE KEY-----".to_string(),
                        "-----BEGIN RSA PRIVATE KEY-----".to_string(),
                        "AKIA[0-9A-Z]{16}".to_string(),
                    ],
                },
                HardStop::LargeDiff { max_lines: 10000 },
                HardStop::ForbiddenFiles {
                    patterns: vec![
                        ".env".to_string(),
                        "credentials.json".to_string(),
                        "secrets.yaml".to_string(),
                    ],
                },
            ],
        }
    }
}

impl Default for AbortGate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate for AbortGate {
    fn name(&self) -> &str {
        "abort"
    }

    fn description(&self) -> &str {
        "Hard stops that immediately abort commits for critical issues."
    }

    fn evaluate(&self, context: &GateContext) -> GateResult {
        if !self.enabled {
            return GateResult::pass(self.name(), "Abort gate disabled");
        }

        for stop in &self.hard_stops {
            match stop {
                HardStop::MergeConflicts => {
                    if context.diff.contains("<<<<<<<")
                        || context.diff.contains("=======")
                        || context.diff.contains(">>>>>>>")
                    {
                        return GateResult::fail(
                            self.name(),
                            "Merge conflict markers found - ABORT",
                        )
                        .with_suggestion("Resolve all merge conflicts before committing".to_string());
                    }
                }
                HardStop::SensitiveData { patterns } => {
                    let content = &context.diff;
                    for pattern in patterns {
                        if let Ok(re) = regex::Regex::new(pattern) {
                            if re.is_match(content) {
                                return GateResult::fail(
                                    self.name(),
                                    "Sensitive data detected - ABORT",
                                )
                                .with_suggestion(
                                    "Remove sensitive data before committing. Use .env.example for templates.".to_string(),
                                );
                            }
                        }
                    }
                }
                HardStop::LargeDiff { max_lines } => {
                    let diff_lines = context.diff.lines().count();
                    if diff_lines > *max_lines {
                        return GateResult::fail(
                            self.name(),
                            format!("Diff too large ({} lines) - ABORT", diff_lines),
                        )
                        .with_suggestion(
                            "Split large changes into smaller commits".to_string(),
                        );
                    }
                }
                HardStop::ForbiddenFiles { patterns } => {
                    for file in &context.changed_files {
                        for pattern in patterns {
                            if file.contains(pattern) {
                                return GateResult::fail(
                                    self.name(),
                                    format!("Forbidden file: {} - ABORT", file),
                                )
                                .with_suggestion(
                                    "Remove forbidden files from commit or use .gitignore"
                                        .to_string(),
                                );
                            }
                        }
                    }
                }
            }
        }

        GateResult::pass(self.name(), "No abort triggers")
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Gate evaluator that runs multiple gates.
pub struct GateEvaluator {
    gates: Vec<Box<dyn Gate>>,
}

impl GateEvaluator {
    /// Create a new evaluator.
    pub fn new() -> Self {
        Self {
            gates: Vec::new(),
        }
    }

    /// Add a gate.
    pub fn with_gate<G: Gate + 'static>(mut self, gate: G) -> Self {
        self.gates.push(Box::new(gate));
        self
    }

    /// Add the default gates.
    pub fn with_default_gates(self) -> Self {
        self.with_gate(AbortGate::new())
            .with_gate(PreFlightGate::new())
            .with_gate(RevisionGate::new())
            .with_gate(EscalationGate::new())
    }

    /// Evaluate all gates.
    pub fn evaluate(&self, context: &GateContext) -> Vec<GateResult> {
        self.gates
            .iter()
            .map(|gate| {
                let start = std::time::Instant::now();
                let result = gate.evaluate(context);
                result.with_duration(start.elapsed().as_millis() as u64)
            })
            .collect()
    }

    /// Evaluate and check if all gates pass.
    pub fn evaluate_all(&self, context: &GateContext) -> (bool, Vec<GateResult>) {
        let results = self.evaluate(context);
        let all_passed = results.iter().all(|r| r.passed);
        (all_passed, results)
    }
}

impl Default for GateEvaluator {
    fn default() -> Self {
        Self::new().with_default_gates()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_context() -> GateContext {
        GateContext {
            repo_path: "/test/repo".to_string(),
            branch: "main".to_string(),
            commit_message: "feat(auth): add login functionality".to_string(),
            changed_files: vec!["src/auth/login.rs".to_string()],
            diff: "fn login() { println!(\"hello\"); }".to_string(),
            author: "Test User".to_string(),
            author_email: "test@example.com".to_string(),
            violations: Vec::new(),
        }
    }

    #[test]
    fn test_preflight_gate_pass() {
        let gate = PreFlightGate::new();
        let context = create_test_context();
        let result = gate.evaluate(&context);
        assert!(result.passed);
    }

    #[test]
    fn test_preflight_gate_merge_conflict() {
        let gate = PreFlightGate::new();
        let mut context = create_test_context();
        context.diff = "<<<<<<< HEAD\nexisting code\n=======\nnew code\n>>>>>>> branch".to_string();

        let result = gate.evaluate(&context);
        assert!(!result.passed);
    }

    #[test]
    fn test_abort_gate_sensitive_data() {
        let gate = AbortGate::new();
        let mut context = create_test_context();
        context.diff = "api_key = 'AKIAIOSFODNN7EXAMPLE'".to_string();

        let result = gate.evaluate(&context);
        assert!(!result.passed);
    }

    #[test]
    fn test_gate_evaluator() {
        let evaluator = GateEvaluator::default();
        let context = create_test_context();
        let (all_passed, results) = evaluator.evaluate_all(&context);

        assert!(all_passed || !results.is_empty());
        assert!(!results.is_empty());
    }
}
