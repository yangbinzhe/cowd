use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GreenLevel {
    TargetedTests,
    Package,
    Workspace,
    MergeReady,
}

impl GreenLevel {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TargetedTests => "targeted_tests",
            Self::Package => "package",
            Self::Workspace => "workspace",
            Self::MergeReady => "merge_ready",
        }
    }
}

impl std::fmt::Display for GreenLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
