use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};

/// Typed scope for a task, replacing the previous free-form `String`.
///
/// Backward-compatible: on deserialization, known strings (`"workspace"`,
/// `"module"`, `"single_file"`) map to their respective variants; any other
/// string becomes `Custom(inner)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskScope {
    Workspace,
    Module,
    SingleFile,
    Custom(String),
}

impl TaskScope {
    /// Returns `true` for named variants (Workspace / Module / SingleFile).
    #[must_use]
    pub fn is_named(&self) -> bool {
        !matches!(self, Self::Custom(_))
    }

    /// Returns the inner string for `Custom`, or the variant name otherwise.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Workspace => "workspace",
            Self::Module => "module",
            Self::SingleFile => "single_file",
            Self::Custom(s) => s,
        }
    }
}

impl Display for TaskScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// --- Backward-compatible serde: always a plain string ----------------------

impl Serialize for TaskScope {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TaskScope {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        match raw.as_str() {
            "workspace" => Ok(Self::Workspace),
            "module" => Ok(Self::Module),
            "single_file" => Ok(Self::SingleFile),
            other => Ok(Self::Custom(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPacket {
    pub objective: String,
    pub scope: TaskScope,
    /// Optional path qualifier — required when scope is `Module` or `SingleFile`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_path: Option<String>,
    /// Optional git worktree for isolated checkout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<String>,
    pub repo: String,
    pub branch_policy: String,
    pub acceptance_tests: Vec<String>,
    pub commit_policy: String,
    pub reporting_contract: String,
    pub escalation_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPacketValidationError {
    errors: Vec<String>,
}

impl TaskPacketValidationError {
    #[must_use]
    pub fn new(errors: Vec<String>) -> Self {
        Self { errors }
    }

    #[must_use]
    pub fn errors(&self) -> &[String] {
        &self.errors
    }
}

impl Display for TaskPacketValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.errors.join("; "))
    }
}

impl std::error::Error for TaskPacketValidationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPacket(TaskPacket);

impl ValidatedPacket {
    #[must_use]
    pub fn packet(&self) -> &TaskPacket {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> TaskPacket {
        self.0
    }
}

pub fn validate_packet(packet: TaskPacket) -> Result<ValidatedPacket, TaskPacketValidationError> {
    let mut errors = Vec::new();

    validate_required("objective", &packet.objective, &mut errors);
    validate_scope(&packet.scope, &mut errors);
    validate_scope_requirements(&packet.scope, &packet.scope_path, &mut errors);
    validate_required("repo", &packet.repo, &mut errors);
    validate_required("branch_policy", &packet.branch_policy, &mut errors);
    validate_required("commit_policy", &packet.commit_policy, &mut errors);
    validate_required(
        "reporting_contract",
        &packet.reporting_contract,
        &mut errors,
    );
    validate_required("escalation_policy", &packet.escalation_policy, &mut errors);

    for (index, test) in packet.acceptance_tests.iter().enumerate() {
        if test.trim().is_empty() {
            errors.push(format!(
                "acceptance_tests contains an empty value at index {index}"
            ));
        }
    }

    if errors.is_empty() {
        Ok(ValidatedPacket(packet))
    } else {
        Err(TaskPacketValidationError::new(errors))
    }
}

fn validate_scope(scope: &TaskScope, errors: &mut Vec<String>) {
    if let TaskScope::Custom(s) = scope {
        if s.trim().is_empty() {
            errors.push("scope must not be empty".to_string());
        }
    }
}

/// `Module` and `SingleFile` scopes require a `scope_path`; `Workspace` does not.
fn validate_scope_requirements(
    scope: &TaskScope,
    scope_path: &Option<String>,
    errors: &mut Vec<String>,
) {
    match scope {
        TaskScope::Module | TaskScope::SingleFile => {
            if scope_path.as_ref().is_none_or(|p| p.trim().is_empty()) {
                errors.push(format!(
                    "scope_path is required when scope is {scope}"
                ));
            }
        }
        TaskScope::Workspace | TaskScope::Custom(_) => {}
    }
}

fn validate_required(field: &str, value: &str, errors: &mut Vec<String>) {
    if value.trim().is_empty() {
        errors.push(format!("{field} must not be empty"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_packet() -> TaskPacket {
        TaskPacket {
            objective: "Implement typed task packet format".to_string(),
            scope: TaskScope::Custom("runtime/task system".to_string()),
            scope_path: None,
            worktree: None,
            repo: "cowd-code-parity".to_string(),
            branch_policy: "origin/main only".to_string(),
            acceptance_tests: vec![
                "cargo build --workspace".to_string(),
                "cargo test --workspace".to_string(),
            ],
            commit_policy: "single verified commit".to_string(),
            reporting_contract: "print build result, test result, commit sha".to_string(),
            escalation_policy: "stop only on destructive ambiguity".to_string(),
        }
    }

    #[test]
    fn valid_packet_passes_validation() {
        let packet = sample_packet();
        let validated = validate_packet(packet.clone()).expect("packet should validate");
        assert_eq!(validated.packet(), &packet);
        assert_eq!(validated.into_inner(), packet);
    }

    #[test]
    fn invalid_packet_accumulates_errors() {
        let packet = TaskPacket {
            objective: " ".to_string(),
            scope: TaskScope::Custom(String::new()),
            scope_path: None,
            worktree: None,
            repo: String::new(),
            branch_policy: "\t".to_string(),
            acceptance_tests: vec!["ok".to_string(), " ".to_string()],
            commit_policy: String::new(),
            reporting_contract: String::new(),
            escalation_policy: String::new(),
        };

        let error = validate_packet(packet).expect_err("packet should be rejected");

        assert!(error.errors().len() >= 7);
        assert!(error
            .errors()
            .contains(&"objective must not be empty".to_string()));
        assert!(error
            .errors()
            .contains(&"scope must not be empty".to_string()));
        assert!(error
            .errors()
            .contains(&"repo must not be empty".to_string()));
        assert!(error
            .errors()
            .contains(&"acceptance_tests contains an empty value at index 1".to_string()));
    }

    #[test]
    fn serialization_roundtrip_preserves_packet() {
        let packet = sample_packet();
        let serialized = serde_json::to_string(&packet).expect("packet should serialize");
        let deserialized: TaskPacket =
            serde_json::from_str(&serialized).expect("packet should deserialize");
        assert_eq!(deserialized, packet);
    }

    #[test]
    fn task_scope_named_variants_serialize_as_strings() {
        assert_eq!(
            serde_json::to_string(&TaskScope::Workspace).unwrap(),
            "\"workspace\""
        );
        assert_eq!(
            serde_json::to_string(&TaskScope::Module).unwrap(),
            "\"module\""
        );
        assert_eq!(
            serde_json::to_string(&TaskScope::SingleFile).unwrap(),
            "\"single_file\""
        );
    }

    #[test]
    fn task_scope_custom_serializes_as_inner_string() {
        assert_eq!(
            serde_json::to_string(&TaskScope::Custom("runtime/task system".to_string())).unwrap(),
            "\"runtime/task system\""
        );
    }

    #[test]
    fn task_scope_deserialize_known_strings() {
        let ws: TaskScope = serde_json::from_str("\"workspace\"").unwrap();
        assert_eq!(ws, TaskScope::Workspace);

        let md: TaskScope = serde_json::from_str("\"module\"").unwrap();
        assert_eq!(md, TaskScope::Module);

        let sf: TaskScope = serde_json::from_str("\"single_file\"").unwrap();
        assert_eq!(sf, TaskScope::SingleFile);
    }

    #[test]
    fn task_scope_deserialize_unknown_becomes_custom() {
        let c: TaskScope = serde_json::from_str("\"runtime/task system\"").unwrap();
        assert_eq!(c, TaskScope::Custom("runtime/task system".to_string()));
    }

    #[test]
    fn backward_compat_old_json_deserializes() {
        let json = r#"{"objective":"obj","scope":"runtime/task system","repo":"r","branch_policy":"bp","acceptance_tests":[],"commit_policy":"cp","reporting_contract":"rc","escalation_policy":"ep"}"#;
        let pkt: TaskPacket = serde_json::from_str(json).unwrap();
        assert_eq!(pkt.scope, TaskScope::Custom("runtime/task system".to_string()));
        assert_eq!(pkt.scope_path, None);
        assert_eq!(pkt.worktree, None);
    }

    #[test]
    fn module_scope_requires_scope_path() {
        let packet = TaskPacket {
            objective: "obj".to_string(),
            scope: TaskScope::Module,
            scope_path: None,
            worktree: None,
            repo: "repo".to_string(),
            branch_policy: "bp".to_string(),
            acceptance_tests: vec![],
            commit_policy: "cp".to_string(),
            reporting_contract: "rc".to_string(),
            escalation_policy: "ep".to_string(),
        };
        let err = validate_packet(packet).expect_err("Module without scope_path should fail");
        assert!(err.errors().iter().any(|e| e.contains("scope_path is required")));
    }

    #[test]
    fn workspace_scope_does_not_require_scope_path() {
        let packet = TaskPacket {
            objective: "obj".to_string(),
            scope: TaskScope::Workspace,
            scope_path: None,
            worktree: None,
            repo: "repo".to_string(),
            branch_policy: "bp".to_string(),
            acceptance_tests: vec![],
            commit_policy: "cp".to_string(),
            reporting_contract: "rc".to_string(),
            escalation_policy: "ep".to_string(),
        };
        assert!(validate_packet(packet).is_ok());
    }

    #[test]
    fn task_scope_display() {
        assert_eq!(TaskScope::Workspace.to_string(), "workspace");
        assert_eq!(TaskScope::Module.to_string(), "module");
        assert_eq!(TaskScope::SingleFile.to_string(), "single_file");
        assert_eq!(TaskScope::Custom("foo".to_string()).to_string(), "foo");
    }

    #[test]
    fn task_scope_is_named() {
        assert!(TaskScope::Workspace.is_named());
        assert!(TaskScope::Module.is_named());
        assert!(TaskScope::SingleFile.is_named());
        assert!(!TaskScope::Custom("foo".to_string()).is_named());
    }
}
