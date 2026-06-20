//! Tool transaction planning for Cowd AI work kernel.

use std::collections::BTreeSet;

use crate::core::{AiKernelError, AiKernelResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolAccessMode {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRisk {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOperation {
    pub id: String,
    pub tool_name: String,
    pub access: ToolAccessMode,
    pub risk: ToolRisk,
    pub path: Option<String>,
}

impl ToolOperation {
    #[must_use]
    pub fn read(tool_name: impl Into<String>, path: Option<String>) -> Self {
        Self::new(tool_name, ToolAccessMode::Read, ToolRisk::Low, path)
    }

    #[must_use]
    pub fn write(tool_name: impl Into<String>, risk: ToolRisk, path: Option<String>) -> Self {
        Self::new(tool_name, ToolAccessMode::Write, risk, path)
    }

    fn new(
        tool_name: impl Into<String>,
        access: ToolAccessMode,
        risk: ToolRisk,
        path: Option<String>,
    ) -> Self {
        Self {
            id: format!("tool-op-{}", uuid::Uuid::new_v4()),
            tool_name: tool_name.into(),
            access,
            risk,
            path: path.map(normalize_path),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolTransactionPlan {
    pub id: String,
    pub batches: Vec<Vec<ToolOperation>>,
    pub requires_checkpoint: bool,
    pub requires_human_confirm: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolTransactionReceipt {
    pub transaction_id: String,
    pub completed_operations: usize,
    pub failed_operations: usize,
    pub checkpoint_created: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ToolTransactionPlanner;

impl ToolTransactionPlanner {
    pub fn plan(&self, operations: Vec<ToolOperation>) -> AiKernelResult<ToolTransactionPlan> {
        detect_write_conflicts(&operations)?;
        let requires_checkpoint = operations.iter().any(|operation| {
            operation.access == ToolAccessMode::Write
                && matches!(
                    operation.risk,
                    ToolRisk::Medium | ToolRisk::High | ToolRisk::Critical
                )
        });
        let requires_human_confirm = operations
            .iter()
            .any(|operation| operation.risk == ToolRisk::Critical);
        let warnings = operations
            .iter()
            .filter(|operation| {
                operation.access == ToolAccessMode::Write && operation.path.is_none()
            })
            .map(|operation| format!("write operation {} has no path", operation.tool_name))
            .collect();

        let mut read_batch = Vec::new();
        let mut batches = Vec::new();
        for operation in operations {
            match operation.access {
                ToolAccessMode::Read => read_batch.push(operation),
                ToolAccessMode::Write => {
                    if !read_batch.is_empty() {
                        batches.push(std::mem::take(&mut read_batch));
                    }
                    batches.push(vec![operation]);
                }
            }
        }
        if !read_batch.is_empty() {
            batches.push(read_batch);
        }

        Ok(ToolTransactionPlan {
            id: format!("tool-tx-{}", uuid::Uuid::new_v4()),
            batches,
            requires_checkpoint,
            requires_human_confirm,
            warnings,
        })
    }
}

impl ToolTransactionPlan {
    #[must_use]
    pub fn receipt(
        &self,
        completed_operations: usize,
        failed_operations: usize,
    ) -> ToolTransactionReceipt {
        ToolTransactionReceipt {
            transaction_id: self.id.clone(),
            completed_operations,
            failed_operations,
            checkpoint_created: self.requires_checkpoint,
        }
    }
}

fn detect_write_conflicts(operations: &[ToolOperation]) -> AiKernelResult<()> {
    let mut seen = BTreeSet::new();
    for operation in operations
        .iter()
        .filter(|operation| operation.access == ToolAccessMode::Write)
    {
        let Some(path) = &operation.path else {
            continue;
        };
        if !seen.insert(path.clone()) {
            return Err(AiKernelError::Conflict(format!(
                "multiple write operations target {path}"
            )));
        }
    }
    Ok(())
}

fn normalize_path(path: String) -> String {
    path.trim().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_operations_share_a_parallel_batch() {
        let plan = ToolTransactionPlanner
            .plan(vec![
                ToolOperation::read("read_file", Some("a.rs".to_string())),
                ToolOperation::read("grep", None),
            ])
            .unwrap();

        assert_eq!(plan.batches.len(), 1);
        assert_eq!(plan.batches[0].len(), 2);
        assert!(!plan.requires_checkpoint);
    }

    #[test]
    fn writes_are_serialized_and_checkpointed() {
        let plan = ToolTransactionPlanner
            .plan(vec![
                ToolOperation::read("read_file", Some("a.rs".to_string())),
                ToolOperation::write("apply_patch", ToolRisk::High, Some("a.rs".to_string())),
                ToolOperation::write("apply_patch", ToolRisk::Medium, Some("b.rs".to_string())),
            ])
            .unwrap();

        assert_eq!(plan.batches.len(), 3);
        assert!(plan.requires_checkpoint);
    }

    #[test]
    fn same_path_write_conflict_is_rejected() {
        let error = ToolTransactionPlanner
            .plan(vec![
                ToolOperation::write("apply_patch", ToolRisk::Medium, Some("a.rs".to_string())),
                ToolOperation::write("write_file", ToolRisk::Medium, Some("a.rs".to_string())),
            ])
            .unwrap_err();

        assert_eq!(error.kind(), "conflict");
    }

    #[test]
    fn critical_operation_requires_human_confirm() {
        let plan = ToolTransactionPlanner
            .plan(vec![ToolOperation::write(
                "danger",
                ToolRisk::Critical,
                Some("db.sqlite".to_string()),
            )])
            .unwrap();

        assert!(plan.requires_human_confirm);
    }
}
