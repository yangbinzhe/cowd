use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{mutation::MfgMutationContextV1, receipt::MfgReceiptV1, version::MfgContractVersion};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgMutationRequestV1<T> {
    pub mutation_context: MfgMutationContextV1,
    pub input: T,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MfgNoBodyRequestV1 {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MfgIncidentListQuery {
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgAssignmentCompletionEvidenceV1 {
    pub correlation_id: String,
    pub owner_kind: String,
    pub task_ref: String,
    #[serde(default)]
    pub workflow_node_id: Option<String>,
    pub terminal_status: String,
    pub receipt_ref: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgReadResponseV1 {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(flatten)]
    pub payload: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgMutationResponseV1 {
    #[serde(default)]
    pub receipt: Option<serde_json::Value>,
    #[serde(default, rename = "_mfg_receipt")]
    pub middleware_receipt: Option<MfgReceiptV1>,
    #[serde(flatten)]
    pub payload: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgReadResourceV1<T> {
    pub contract_version: MfgContractVersion,
    pub resource: T,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgReadCollectionV1<T> {
    pub contract_version: MfgContractVersion,
    #[serde(default)]
    pub items: Vec<T>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgContractDiagnosticV1 {
    pub contract_version: MfgContractVersion,
    pub route_count: usize,
    pub active_route_count: usize,
    pub action_count: usize,
    #[serde(default)]
    pub last_receipt: Option<MfgReceiptV1>,
}
