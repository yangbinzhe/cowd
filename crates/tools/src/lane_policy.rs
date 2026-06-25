use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::stale_branch::BranchFreshness;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GreenLevel {
    TargetedTests,
    Package,
    Workspace,
    MergeReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneBlocker {
    None,
    Startup,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewStatus {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffScope {
    Full,
    Scoped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneContext {
    pub lane_id: String,
    pub green_level: GreenLevel,
    pub branch_freshness: Duration,
    pub stale_branch: Option<BranchFreshness>,
    pub blocker: LaneBlocker,
    pub review_status: ReviewStatus,
    pub diff_scope: DiffScope,
    pub completed: bool,
    pub reconciled: bool,
}

impl LaneContext {
    #[must_use]
    pub fn new(
        lane_id: impl Into<String>,
        green_level: GreenLevel,
        branch_freshness: Duration,
        blocker: LaneBlocker,
        review_status: ReviewStatus,
        diff_scope: DiffScope,
        completed: bool,
    ) -> Self {
        Self {
            lane_id: lane_id.into(),
            green_level,
            branch_freshness,
            stale_branch: None,
            blocker,
            review_status,
            diff_scope,
            completed,
            reconciled: false,
        }
    }
}

#[must_use]
pub fn iso8601_now() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}
