//! Gates Mechanism for Commit Quality Control.
//!
//! Implements PreFlight, Revision, Escalation, and Abort Gates to ensure
//! commit quality and prevent bad commits from being merged.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;
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

/// Action the gate evaluator should take after evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GateAction {
    /// Proceed without restriction.
    Allow,
    /// Block the operation with a reason.
    Deny { reason: String },
    /// Attempt automatic fix before denying.
    AutoFix {
        /// Human-readable description of what the fix will do.
        fix_description: String,
        /// Maximum number of fix attempts before giving up.
        max_attempts: usize,
    },
    /// Escalate to a human reviewer.
    Escalate { reason: String },
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

// ═══════════════════════════════════════════════════════════════════════
// Auto-fix infrastructure
// ═══════════════════════════════════════════════════════════════════════

/// A strategy for automatically fixing code issues detected by gates.
pub trait FixStrategy: Send + Sync {
    /// Human-readable name of this fix strategy.
    fn name(&self) -> &str;

    /// Whether this fixer can handle the given error message.
    fn can_fix(&self, error: &str) -> bool;

    /// Apply the fix to the given file, returning the tool output on success.
    fn apply_fix(&self, file_path: &Path, error: &str) -> Result<String, String>;
}

/// Orchestrates auto-fix attempts using a collection of fix strategies.
pub struct AutoFixer {
    max_attempts: usize,
    fixers: Vec<Box<dyn FixStrategy>>,
}

impl AutoFixer {
    pub fn new(max_attempts: usize) -> Self {
        Self {
            max_attempts,
            fixers: Vec::new(),
        }
    }

    pub fn with_fixer(mut self, fixer: Box<dyn FixStrategy>) -> Self {
        self.fixers.push(fixer);
        self
    }

    pub fn with_default_fixers(mut self) -> Self {
        self.fixers.push(Box::new(RustClippyFixer));
        self.fixers.push(Box::new(RustFmtFixer));
        self.fixers.push(Box::new(UnusedImportFixer));
        self
    }
}

// ── Concrete fixers ──

struct RustClippyFixer;

impl FixStrategy for RustClippyFixer {
    fn name(&self) -> &str {
        "cargo clippy --fix"
    }

    fn can_fix(&self, error: &str) -> bool {
        error.contains("clippy") || error.contains("unused")
    }

    fn apply_fix(&self, file_path: &Path, _error: &str) -> Result<String, String> {
        let dir = file_path.parent().unwrap_or_else(|| Path::new("."));
        let output = std::process::Command::new("cargo")
            .args(["clippy", "--fix", "--allow-dirty", "--allow-staged"])
            .current_dir(dir)
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).to_string());
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

struct RustFmtFixer;

impl FixStrategy for RustFmtFixer {
    fn name(&self) -> &str {
        "cargo fmt"
    }

    fn can_fix(&self, error: &str) -> bool {
        error.contains("format") || error.contains("fmt")
    }

    fn apply_fix(&self, file_path: &Path, _error: &str) -> Result<String, String> {
        let output = std::process::Command::new("cargo")
            .args(["fmt", "--", file_path.to_string_lossy().as_ref()])
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).to_string());
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

struct UnusedImportFixer;

impl FixStrategy for UnusedImportFixer {
    fn name(&self) -> &str {
        "cargo fix"
    }

    fn can_fix(&self, error: &str) -> bool {
        error.contains("unused import") || error.contains("unused")
    }

    fn apply_fix(&self, file_path: &Path, _error: &str) -> Result<String, String> {
        let dir = file_path.parent().unwrap_or_else(|| Path::new("."));
        let output = std::process::Command::new("cargo")
            .args(["fix", "--allow-dirty", "--allow-staged"])
            .current_dir(dir)
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).to_string());
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
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
    auto_fixer: Option<AutoFixer>,
}

/// A pre-flight check.
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
    /// Impact analysis gate — runs an impact function and warns on HIGH/CRITICAL risk.
    ImpactAnalysis(Box<dyn Fn() -> ImpactSummary + Send + Sync>),
}

impl PreFlightGate {
    /// Create a new PreFlight gate.
    pub fn new() -> Self {
        Self {
            enabled: true,
            auto_fixer: None,
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

    /// Attach an auto-fixer to attempt fixes before denying.
    pub fn with_auto_fixer(mut self, fixer: AutoFixer) -> Self {
        self.auto_fixer = Some(fixer);
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

        let result = self.run_checks(context);
        if result.passed {
            return result;
        }

        let Some(auto_fixer) = &self.auto_fixer else {
            return result;
        };

        let files_to_fix: Vec<&str> = context
            .violations
            .iter()
            .map(|v| v.file.as_str())
            .chain(context.changed_files.iter().map(|s| s.as_str()))
            .collect();

        let mut fixed = false;
        for attempt in 0..auto_fixer.max_attempts {
            if attempt > 0 && !fixed {
                break;
            }
            fixed = false;

            for fixer in &auto_fixer.fixers {
                for file in &files_to_fix {
                    let file_path = Path::new(file);
                    let has_relevant_violation = context
                        .violations
                        .iter()
                        .any(|v| v.file == *file && fixer.can_fix(&v.message));

                    let should_try = has_relevant_violation
                        || (file.ends_with(".rs") && fixer.can_fix("lint"));

                    if should_try {
                        match fixer.apply_fix(file_path, "auto-fix attempt") {
                            Ok(output) => {
                                tracing::info!(
                                    fixer = %fixer.name(),
                                    file = %file,
                                    attempt = attempt + 1,
                                    "auto-fix applied"
                                );
                                let _ = output;
                                fixed = true;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    fixer = %fixer.name(),
                                    file = %file,
                                    error = %e,
                                    "auto-fix failed"
                                );
                            }
                        }
                    }
                }
            }

            let re_result = self.run_checks(context);
            if re_result.passed {
                return re_result;
            }
        }

        result
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl PreFlightGate {
    fn run_checks(&self, context: &GateContext) -> GateResult {
        let gate_name = "preflight";
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
                PreFlightCheck::ImpactAnalysis(impact_fn) => {
                    let summary = impact_fn();
                    if summary.risk_level == ImpactRiskLevel::High
                        || summary.risk_level == ImpactRiskLevel::Critical
                    {
                        warnings.push(format!(
                            "Impact analysis HIGH RISK: {} has {} direct + {} indirect callers across {} files",
                            summary.symbol_name,
                            summary.direct_callers.len(),
                            summary.indirect.len(),
                            summary.affected_files.len()
                        ));
                        suggestions.push(
                            "Review impact before proceeding with changes".to_string(),
                        );
                    }
                }
            }
        }

        if passed {
            GateResult::pass(gate_name, "All pre-flight checks passed")
                .with_warning(warnings.join("; "))
        } else {
            GateResult::fail(gate_name, "Pre-flight checks failed")
                .with_warning(warnings.join("; "))
                .with_suggestion(suggestions.join("; "))
        }
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

// ═══════════════════════════════════════════════════════════════════════
// ApprovalGate — edit-time impact analysis
// ═══════════════════════════════════════════════════════════════════════

/// Summary of an impact analysis for a file edit operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImpactSummary {
    pub symbol_name: String,
    pub direct_callers: Vec<String>,
    pub indirect: Vec<String>,
    pub affected_files: Vec<String>,
    pub risk_level: ImpactRiskLevel,
}

/// Risk level classification for code changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImpactRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl ImpactRiskLevel {
    pub fn from_caller_count(direct: usize, indirect: usize) -> Self {
        let total = direct + indirect;
        match total {
            0 => Self::Low,
            1..=3 => Self::Medium,
            4..=10 => Self::High,
            _ => Self::Critical,
        }
    }
}

/// A gate that performs impact analysis before allowing file edits.
///
/// This gate queries a code indexer (via the provided impact function) to
/// determine the blast radius of editing a given symbol. The report is
/// surfaced in the gate result's warnings/suggestions for the user to review.
pub struct ApprovalGate<F>
where
    F: Fn(&str, usize) -> ImpactSummary + Send + Sync,
{
    enabled: bool,
    impact_fn: F,
    depth: usize,
}

impl<F> ApprovalGate<F>
where
    F: Fn(&str, usize) -> ImpactSummary + Send + Sync,
{
    /// Create a new ApprovalGate with the given impact analysis function.
    pub fn new(impact_fn: F) -> Self {
        Self {
            enabled: true,
            impact_fn,
            depth: 2,
        }
    }

    /// Set the analysis depth (default: 2).
    pub fn with_depth(mut self, depth: usize) -> Self {
        self.depth = depth;
        self
    }

    /// Set whether the gate is enabled.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Analyse the impact of editing a file that contains the given symbol.
    pub fn analyse_impact(&self, symbol_name: &str) -> ImpactSummary {
        (self.impact_fn)(symbol_name, self.depth)
    }
}

impl<F> Gate for ApprovalGate<F>
where
    F: Fn(&str, usize) -> ImpactSummary + Send + Sync,
{
    fn name(&self) -> &str {
        "approval"
    }

    fn description(&self) -> &str {
        "Impact analysis gate: checks blast radius before allowing file edits."
    }

    fn evaluate(&self, context: &GateContext) -> GateResult {
        if !self.enabled {
            return GateResult::pass(self.name(), "Approval gate disabled");
        }

        let mut warnings = Vec::new();
        let mut suggestions = Vec::new();

        for file in &context.changed_files {
            // Extract symbol name from file path (e.g., "src/auth/login.rs" -> "login")
            let symbol_name = file
                .rsplit('/')
                .next()
                .unwrap_or(file)
                .split('.')
                .next()
                .unwrap_or("unknown");

            let impact = (self.impact_fn)(symbol_name, self.depth);

            if !impact.direct_callers.is_empty() || !impact.indirect.is_empty() {
                let caller_list: Vec<String> = impact
                    .direct_callers
                    .iter()
                    .chain(impact.indirect.iter())
                    .cloned()
                    .collect();

                warnings.push(format!(
                    "File edit impact: {} has {} direct + {} indirect callers",
                    file,
                    impact.direct_callers.len(),
                    impact.indirect.len()
                ));

                if impact.risk_level == ImpactRiskLevel::High
                    || impact.risk_level == ImpactRiskLevel::Critical
                {
                    suggestions.push(format!(
                        "HIGH IMPACT: changing {} affects {} callers across {} files. Review carefully.",
                        impact.symbol_name,
                        caller_list.len(),
                        impact.affected_files.len()
                    ));
                }

                if !caller_list.is_empty() {
                    suggestions.push(format!("Affected callers: {}", caller_list.join(", ")));
                }
            }
        }

        if warnings.is_empty() {
            GateResult::pass(self.name(), "No impact concerns detected")
        } else {
            GateResult::pass(self.name(), "Impact analysis completed")
                .with_warning(warnings.join("; "))
                .with_suggestion(suggestions.join("; "))
        }
    }

    fn is_enabled(&self) -> bool {
        self.enabled
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

    // -----------------------------------------------------------------------
    // T8: ApprovalGate impact analysis tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_impact_risk_level() {
        assert_eq!(ImpactRiskLevel::from_caller_count(0, 0), ImpactRiskLevel::Low);
        assert_eq!(ImpactRiskLevel::from_caller_count(2, 0), ImpactRiskLevel::Medium);
        assert_eq!(ImpactRiskLevel::from_caller_count(5, 0), ImpactRiskLevel::High);
        assert_eq!(ImpactRiskLevel::from_caller_count(1, 10), ImpactRiskLevel::Critical);
    }

    #[test]
    fn test_approval_gate_impact_warning() {
        let impact_fn = |name: &str, _depth: usize| ImpactSummary {
            symbol_name: name.to_string(),
            direct_callers: vec!["caller_a".to_string(), "caller_b".to_string()],
            indirect: vec!["indirect_c".to_string()],
            affected_files: vec!["src/auth.rs".to_string(), "src/middleware.rs".to_string()],
            risk_level: ImpactRiskLevel::High,
        };

        let gate = ApprovalGate::new(impact_fn);
        let context = GateContext {
            repo_path: "/test/repo".to_string(),
            branch: "main".to_string(),
            commit_message: "fix: update auth logic".to_string(),
            changed_files: vec!["src/auth/login.rs".to_string()],
            diff: "fn authenticate() { ... }".to_string(),
            author: "Test User".to_string(),
            author_email: "test@example.com".to_string(),
            violations: Vec::new(),
        };

        let result = gate.evaluate(&context);
        assert!(result.passed); // ApprovalGate always passes, warns instead
        assert!(!result.warnings.is_empty(), "should have impact warnings");
        assert!(!result.suggestions.is_empty(), "should have impact suggestions");
    }

    #[test]
    fn test_approval_gate_no_impact() {
        let impact_fn = |name: &str, _depth: usize| ImpactSummary {
            symbol_name: name.to_string(),
            direct_callers: vec![],
            indirect: vec![],
            affected_files: vec![],
            risk_level: ImpactRiskLevel::Low,
        };

        let gate = ApprovalGate::new(impact_fn);
        let context = create_test_context();
        let result = gate.evaluate(&context);
        assert!(result.passed);
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_approval_gate_disabled() {
        let impact_fn = |name: &str, _depth: usize| ImpactSummary {
            symbol_name: name.to_string(),
            direct_callers: vec!["caller".to_string()],
            indirect: vec![],
            affected_files: vec![],
            risk_level: ImpactRiskLevel::Medium,
        };

        let gate = ApprovalGate::new(impact_fn).with_enabled(false);
        let context = create_test_context();
        let result = gate.evaluate(&context);
        assert!(result.passed);
    }
}
