use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    require_bounded, require_schema, AppErrorDetailV1, AppId, ProtocolValidate,
    ProtocolValidationError, Sha256Digest,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppViewDocumentV1 {
    pub schema_version: u16,
    pub app_id: AppId,
    pub view_id: String,
    pub revision: String,
    pub title: String,
    pub root: AppComponentV1,
    #[serde(default)]
    pub bindings: BTreeMap<String, Value>,
    #[serde(default)]
    pub actions: Vec<AppViewActionDescriptorV1>,
    #[serde(default)]
    pub subscriptions: Vec<AppViewSubscriptionV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focus_component_id: Option<String>,
    pub refresh_policy: AppViewRefreshPolicyV1,
}

impl ProtocolValidate for AppViewDocumentV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppViewDocumentV1", self.schema_version)?;
        self.app_id.validate_value()?;
        require_bounded("view_id", &self.view_id, 256)?;
        require_bounded("revision", &self.revision, 256)?;
        require_bounded("title", &self.title, 256)?;
        self.root.validate_depth(0)?;
        for action in &self.actions {
            action.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppComponentV1 {
    pub component_id: String,
    pub kind: AppComponentKindV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub accessibility_label: String,
    #[serde(default)]
    pub properties: BTreeMap<String, Value>,
    #[serde(default)]
    pub children: Vec<AppComponentV1>,
}

impl AppComponentV1 {
    fn validate_depth(&self, depth: usize) -> Result<(), ProtocolValidationError> {
        if depth > 32 {
            return Err(ProtocolValidationError::InvalidField {
                field: "component.children",
                reason: "component tree exceeds 32 levels".to_owned(),
            });
        }
        require_bounded("component_id", &self.component_id, 256)?;
        require_bounded(
            "component.accessibility_label",
            &self.accessibility_label,
            512,
        )?;
        if self.children.len() > 1024 {
            return Err(ProtocolValidationError::InvalidField {
                field: "component.children",
                reason: "component has more than 1024 direct children".to_owned(),
            });
        }
        for child in &self.children {
            child.validate_depth(depth + 1)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AppComponentKindV1 {
    Stack,
    Split,
    Tabs,
    Status,
    Metric,
    Table,
    List,
    Tree,
    Graph,
    Timeline,
    Markdown,
    Code,
    Form,
    Progress,
    Detail,
    Empty,
    Error,
    ActionBar,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppViewActionDescriptorV1 {
    pub action_id: String,
    pub component_id: String,
    pub label: String,
    pub enabled: bool,
    pub requires_confirmation: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_capability: Option<String>,
}

impl ProtocolValidate for AppViewActionDescriptorV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("action_id", &self.action_id, 256)?;
        require_bounded("component_id", &self.component_id, 256)?;
        require_bounded("action.label", &self.label, 256)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppViewSubscriptionV1 {
    pub subscription_id: String,
    pub stream_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum AppViewRefreshPolicyV1 {
    Manual,
    Interval { interval_ms: u64 },
    Subscription,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppActionV1 {
    pub schema_version: u16,
    pub app_id: AppId,
    pub view_id: String,
    pub document_revision: String,
    pub component_id: String,
    pub action_id: String,
    #[serde(default)]
    pub selection: Value,
    #[serde(default)]
    pub form: Value,
    pub confirmed: bool,
}

impl ProtocolValidate for AppActionV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppActionV1", self.schema_version)?;
        self.app_id.validate_value()?;
        require_bounded("view_id", &self.view_id, 256)?;
        require_bounded("document_revision", &self.document_revision, 256)?;
        require_bounded("component_id", &self.component_id, 256)?;
        require_bounded("action_id", &self.action_id, 256)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppViewPatchV1 {
    pub schema_version: u16,
    pub app_id: AppId,
    pub view_id: String,
    pub base_revision: String,
    pub revision: String,
    pub operations: Vec<AppViewPatchOperationV1>,
}

impl ProtocolValidate for AppViewPatchV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppViewPatchV1", self.schema_version)?;
        self.app_id.validate_value()?;
        require_bounded("view_id", &self.view_id, 256)?;
        require_bounded("base_revision", &self.base_revision, 256)?;
        require_bounded("revision", &self.revision, 256)?;
        if self.base_revision == self.revision || self.operations.is_empty() {
            return Err(ProtocolValidationError::InvalidField {
                field: "view_patch",
                reason: "must advance revision and contain operations".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum AppViewPatchOperationV1 {
    Replace { path: String, value: Value },
    Add { path: String, value: Value },
    Remove { path: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IframeBridgeMessageV1 {
    HostInit {
        schema_version: u16,
        app_id: AppId,
        frame_nonce: String,
        message_id: String,
        protocol_digest: Sha256Digest,
        catalog_generation: Sha256Digest,
    },
    AppReady(IframeBridgeHeaderV1),
    AppNavigate {
        #[serde(flatten)]
        header: IframeBridgeHeaderV1,
        route: String,
    },
    AppResize {
        #[serde(flatten)]
        header: IframeBridgeHeaderV1,
        height_css_px: u32,
    },
    AppRequestCoreNavigation {
        #[serde(flatten)]
        header: IframeBridgeHeaderV1,
        object_kind: String,
        object_id: String,
    },
    HostTheme {
        #[serde(flatten)]
        header: IframeBridgeHeaderV1,
        theme: String,
    },
    HostLocale {
        #[serde(flatten)]
        header: IframeBridgeHeaderV1,
        locale: String,
    },
    HostVisibility {
        #[serde(flatten)]
        header: IframeBridgeHeaderV1,
        visible: bool,
    },
    HostError {
        #[serde(flatten)]
        header: IframeBridgeHeaderV1,
        error: AppErrorDetailV1,
    },
}

impl ProtocolValidate for IframeBridgeMessageV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        match self {
            Self::HostInit {
                schema_version,
                app_id,
                frame_nonce,
                message_id,
                protocol_digest,
                catalog_generation,
            } => {
                require_schema("IframeBridgeMessageV1", *schema_version)?;
                app_id.validate_value()?;
                require_bounded("frame_nonce", frame_nonce, 512)?;
                require_bounded("message_id", message_id, 128)?;
                protocol_digest.validate_value("protocol_digest")?;
                catalog_generation.validate_value("catalog_generation")
            }
            Self::AppReady(header)
            | Self::AppNavigate { header, .. }
            | Self::AppResize { header, .. }
            | Self::AppRequestCoreNavigation { header, .. }
            | Self::HostTheme { header, .. }
            | Self::HostLocale { header, .. }
            | Self::HostVisibility { header, .. }
            | Self::HostError { header, .. } => header.validate(),
        }
    }
}

/// Window-level bridge messages are accepted only from the exact iframe
/// `event.source`, opaque origin `"null"`, and a matching nonce/app identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IframeBridgeHeaderV1 {
    pub schema_version: u16,
    pub app_id: AppId,
    pub frame_nonce: String,
    pub message_id: String,
}

impl ProtocolValidate for IframeBridgeHeaderV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("IframeBridgeHeaderV1", self.schema_version)?;
        self.app_id.validate_value()?;
        require_bounded("frame_nonce", &self.frame_nonce, 512)?;
        require_bounded("message_id", &self.message_id, 128)
    }
}

/// Dedicated MessageChannel traffic. Gateway credentials never enter the
/// iframe; the host restricts requests to the current APP route prefix.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IframeApiFrameV1 {
    AppApiRequest {
        schema_version: u16,
        request_id: String,
        method: String,
        path: String,
        deadline_unix_ms: u64,
        headers: BTreeMap<String, String>,
        body: Value,
    },
    AppApiCancel {
        schema_version: u16,
        request_id: String,
    },
    AppApiCredit {
        schema_version: u16,
        request_id: String,
        bytes: u64,
    },
    HostApiHeaders {
        schema_version: u16,
        request_id: String,
        status: u16,
        headers: BTreeMap<String, String>,
    },
    HostApiData {
        schema_version: u16,
        request_id: String,
        sequence: u64,
        data_base64url: String,
    },
    HostApiEnd {
        schema_version: u16,
        request_id: String,
        sequence: u64,
    },
    HostApiError {
        schema_version: u16,
        request_id: String,
        error: AppErrorDetailV1,
    },
}

impl ProtocolValidate for IframeApiFrameV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        let (schema_version, request_id) = match self {
            Self::AppApiRequest {
                schema_version,
                request_id,
                ..
            }
            | Self::AppApiCancel {
                schema_version,
                request_id,
            }
            | Self::AppApiCredit {
                schema_version,
                request_id,
                ..
            }
            | Self::HostApiHeaders {
                schema_version,
                request_id,
                ..
            }
            | Self::HostApiData {
                schema_version,
                request_id,
                ..
            }
            | Self::HostApiEnd {
                schema_version,
                request_id,
                ..
            }
            | Self::HostApiError {
                schema_version,
                request_id,
                ..
            } => (*schema_version, request_id),
        };
        require_schema("IframeApiFrameV1", schema_version)?;
        require_bounded("request_id", request_id, 128)?;
        if let Self::AppApiRequest {
            method,
            path,
            deadline_unix_ms,
            headers,
            ..
        } = self
        {
            require_bounded("api.method", method, 16)?;
            require_bounded("api.path", path, 2048)?;
            if !path.starts_with('/') || path.contains("..") || *deadline_unix_ms == 0 {
                return Err(ProtocolValidationError::InvalidField {
                    field: "api.request",
                    reason: "path must be normalized and deadline must be non-zero".to_owned(),
                });
            }
            for name in headers.keys() {
                if matches!(
                    name.to_ascii_lowercase().as_str(),
                    "authorization" | "cookie" | "x-cowd-principal-token"
                ) {
                    return Err(ProtocolValidationError::InvalidField {
                        field: "api.headers",
                        reason: "reserved authentication headers are host-owned".to_owned(),
                    });
                }
            }
        }
        if let Self::AppApiCredit { bytes, .. } = self {
            if *bytes == 0 || *bytes > 16 * 1024 * 1024 {
                return Err(ProtocolValidationError::InvalidField {
                    field: "api.credit.bytes",
                    reason: "credit must be within 1..=16MiB".to_owned(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionOutcomeV1 {
    pub schema_version: u16,
    pub outcome_id: String,
    pub status: ApplicationExecutionStatusV1,
    #[serde(default)]
    pub governed_intents: Vec<GovernedIntentRefV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_view: Option<AppViewDocumentV1>,
    pub evidence_digest: Sha256Digest,
}

impl ProtocolValidate for ApplicationExecutionOutcomeV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("ApplicationExecutionOutcomeV1", self.schema_version)?;
        require_bounded("outcome_id", &self.outcome_id, 256)?;
        self.evidence_digest.validate_value("evidence_digest")?;
        if let Some(view) = &self.result_view {
            view.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationExecutionStatusV1 {
    Proposed,
    AwaitingApproval,
    Running,
    Completed,
    Rejected,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GovernedIntentRefV1 {
    pub intent_id: String,
    pub capability: String,
    pub target: String,
    pub payload_digest: Sha256Digest,
}
