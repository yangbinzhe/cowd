use std::collections::{BTreeMap, BTreeSet};

use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::digest::canonical_digest_v1;
use crate::{
    require_bounded, require_canonical_string_set, require_schema, AppErrorDetailV1, AppId,
    ProtocolValidate, ProtocolValidationError, Sha256Digest,
};

const TUI_OPEN_REQUEST_SCHEMA_DOMAIN_V1: &str = "cowd.app.tui.open-request-schema/v1";
const TUI_OPEN_RESPONSE_SCHEMA_DOMAIN_V1: &str = "cowd.app.tui.open-response-schema/v1";
const TUI_ACTION_REQUEST_SCHEMA_DOMAIN_V1: &str = "cowd.app.tui.action-request-schema/v1";
const TUI_ACTION_RESPONSE_SCHEMA_DOMAIN_V1: &str = "cowd.app.tui.action-response-schema/v1";
const TUI_STREAM_REQUEST_SCHEMA_DOMAIN_V1: &str = "cowd.app.tui.stream-request-schema/v1";
const TUI_VIEW_DOCUMENT_SCHEMA_DOMAIN_V1: &str = "cowd.app.tui.view-document-schema/v1";
const TUI_VIEW_PATCH_SCHEMA_DOMAIN_V1: &str = "cowd.app.tui.view-patch-schema/v1";

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
        if self.bindings.len() > 1024
            || self.actions.len() > 1024
            || self.subscriptions.len() > 1024
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "view.collections",
                reason:
                    "bindings, actions, and subscriptions must each contain at most 1024 values"
                        .to_owned(),
            });
        }
        for key in self.bindings.keys() {
            require_bounded("view.bindings.key", key, 256)?;
        }
        let mut component_ids = BTreeSet::new();
        self.root.validate_depth(0, &mut component_ids)?;
        if self
            .actions
            .windows(2)
            .any(|pair| pair[0].action_id >= pair[1].action_id)
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "view.actions",
                reason: "must be sorted by action_id in strictly ascending order".to_owned(),
            });
        }
        for action in &self.actions {
            action.validate()?;
            if !component_ids.contains(&action.component_id) {
                return Err(ProtocolValidationError::InvalidField {
                    field: "view.actions.component_id",
                    reason: "must reference a component in the document".to_owned(),
                });
            }
        }
        if self
            .subscriptions
            .windows(2)
            .any(|pair| pair[0].subscription_id >= pair[1].subscription_id)
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "view.subscriptions",
                reason: "must be sorted by subscription_id in strictly ascending order".to_owned(),
            });
        }
        for subscription in &self.subscriptions {
            subscription.validate()?;
        }
        if let Some(component_id) = &self.focus_component_id {
            require_bounded("focus_component_id", component_id, 256)?;
            if !component_ids.contains(component_id) {
                return Err(ProtocolValidationError::InvalidField {
                    field: "focus_component_id",
                    reason: "must reference a component in the document".to_owned(),
                });
            }
        }
        if let AppViewRefreshPolicyV1::Interval { interval_ms } = &self.refresh_policy {
            if *interval_ms == 0 || *interval_ms > 86_400_000 {
                return Err(ProtocolValidationError::InvalidField {
                    field: "refresh_policy.interval_ms",
                    reason: "must be within 1..=86400000".to_owned(),
                });
            }
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
    fn validate_depth(
        &self,
        depth: usize,
        component_ids: &mut BTreeSet<String>,
    ) -> Result<(), ProtocolValidationError> {
        if depth > 32 {
            return Err(ProtocolValidationError::InvalidField {
                field: "component.children",
                reason: "component tree exceeds 32 levels".to_owned(),
            });
        }
        require_bounded("component_id", &self.component_id, 256)?;
        if !component_ids.insert(self.component_id.clone()) {
            return Err(ProtocolValidationError::InvalidField {
                field: "component_id",
                reason: "must be unique within one view document".to_owned(),
            });
        }
        if let Some(label) = &self.label {
            require_bounded("component.label", label, 512)?;
        }
        require_bounded(
            "component.accessibility_label",
            &self.accessibility_label,
            512,
        )?;
        if self.children.len() > 1024 || self.properties.len() > 256 {
            return Err(ProtocolValidationError::InvalidField {
                field: "component.collections",
                reason: "component has more than 1024 children or 256 properties".to_owned(),
            });
        }
        for key in self.properties.keys() {
            require_bounded("component.properties.key", key, 256)?;
        }
        for child in &self.children {
            child.validate_depth(depth + 1, component_ids)?;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl ProtocolValidate for AppViewSubscriptionV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("subscription_id", &self.subscription_id, 256)?;
        if let Some(cursor) = &self.cursor {
            require_bounded("cursor", cursor, 2_048)?;
        }
        Ok(())
    }
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

/// Closed request payload for opening one signed TUI view operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppTuiViewOpenRequestV1 {
    pub schema_version: u16,
    pub view_id: String,
}

impl ProtocolValidate for AppTuiViewOpenRequestV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppTuiViewOpenRequestV1", self.schema_version)?;
        require_bounded("view_id", &self.view_id, 256)
    }
}

/// Closed response payload for opening one signed TUI view operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppTuiViewOpenResponseV1 {
    pub schema_version: u16,
    pub document: AppViewDocumentV1,
}

impl ProtocolValidate for AppTuiViewOpenResponseV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppTuiViewOpenResponseV1", self.schema_version)?;
        self.document.validate()
    }
}

/// Closed action response. Every result advances or explicitly preserves one
/// identified view revision; arbitrary top-level response envelopes are not
/// accepted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppTuiViewActionResponseV1 {
    pub schema_version: u16,
    pub view_id: String,
    pub revision: String,
    pub update: AppTuiViewUpdateV1,
}

impl ProtocolValidate for AppTuiViewActionResponseV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppTuiViewActionResponseV1", self.schema_version)?;
        require_bounded("view_id", &self.view_id, 256)?;
        require_bounded("revision", &self.revision, 256)?;
        match &self.update {
            AppTuiViewUpdateV1::Document { document } => {
                document.validate()?;
                if document.view_id != self.view_id || document.revision != self.revision {
                    return Err(ProtocolValidationError::InvalidField {
                        field: "update.document",
                        reason: "view_id and revision must match the action response".to_owned(),
                    });
                }
            }
            AppTuiViewUpdateV1::Patch { patch } => {
                patch.validate()?;
                if patch.view_id != self.view_id || patch.revision != self.revision {
                    return Err(ProtocolValidationError::InvalidField {
                        field: "update.patch",
                        reason: "view_id and revision must match the action response".to_owned(),
                    });
                }
            }
            AppTuiViewUpdateV1::NoChange => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AppTuiViewUpdateV1 {
    Document { document: Box<AppViewDocumentV1> },
    Patch { patch: AppViewPatchV1 },
    NoChange,
}

/// Closed request payload for opening the signed stream operation of one TUI
/// view. The cursor and document revision are independently bounded replay
/// facts; neither may select an operation or transport path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppTuiViewStreamRequestV1 {
    pub schema_version: u16,
    pub view_id: String,
    pub document_revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl ProtocolValidate for AppTuiViewStreamRequestV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppTuiViewStreamRequestV1", self.schema_version)?;
        require_bounded("view_id", &self.view_id, 256)?;
        require_bounded("document_revision", &self.document_revision, 256)?;
        if let Some(cursor) = &self.cursor {
            require_bounded("cursor", cursor, 2_048)?;
        }
        Ok(())
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
        if self.operations.len() > 1024 {
            return Err(ProtocolValidationError::InvalidField {
                field: "view_patch.operations",
                reason: "must contain at most 1024 operations".to_owned(),
            });
        }
        for operation in &self.operations {
            operation.validate()?;
        }
        Ok(())
    }
}

fn schema_digest_v1<T: JsonSchema>(
    domain: &'static str,
) -> Result<Sha256Digest, ProtocolValidationError> {
    canonical_digest_v1(domain, &schema_for!(T))
}

pub fn app_tui_view_open_request_schema_digest_v1() -> Result<Sha256Digest, ProtocolValidationError>
{
    schema_digest_v1::<AppTuiViewOpenRequestV1>(TUI_OPEN_REQUEST_SCHEMA_DOMAIN_V1)
}

pub fn app_tui_view_open_response_schema_digest_v1() -> Result<Sha256Digest, ProtocolValidationError>
{
    schema_digest_v1::<AppTuiViewOpenResponseV1>(TUI_OPEN_RESPONSE_SCHEMA_DOMAIN_V1)
}

pub fn app_tui_view_action_request_schema_digest_v1(
) -> Result<Sha256Digest, ProtocolValidationError> {
    schema_digest_v1::<AppActionV1>(TUI_ACTION_REQUEST_SCHEMA_DOMAIN_V1)
}

pub fn app_tui_view_action_response_schema_digest_v1(
) -> Result<Sha256Digest, ProtocolValidationError> {
    schema_digest_v1::<AppTuiViewActionResponseV1>(TUI_ACTION_RESPONSE_SCHEMA_DOMAIN_V1)
}

pub fn app_tui_view_stream_request_schema_digest_v1(
) -> Result<Sha256Digest, ProtocolValidationError> {
    schema_digest_v1::<AppTuiViewStreamRequestV1>(TUI_STREAM_REQUEST_SCHEMA_DOMAIN_V1)
}

pub fn app_tui_view_document_schema_digest_v1() -> Result<Sha256Digest, ProtocolValidationError> {
    schema_digest_v1::<AppViewDocumentV1>(TUI_VIEW_DOCUMENT_SCHEMA_DOMAIN_V1)
}

pub fn app_tui_view_patch_schema_digest_v1() -> Result<Sha256Digest, ProtocolValidationError> {
    schema_digest_v1::<AppViewPatchV1>(TUI_VIEW_PATCH_SCHEMA_DOMAIN_V1)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum AppViewPatchOperationV1 {
    Replace { path: String, value: Value },
    Add { path: String, value: Value },
    Remove { path: String },
}

impl ProtocolValidate for AppViewPatchOperationV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        let path = match self {
            Self::Replace { path, .. } | Self::Add { path, .. } | Self::Remove { path } => path,
        };
        require_bounded("view_patch.operations.path", path, 2048)?;
        if !path.starts_with('/') {
            return Err(ProtocolValidationError::InvalidField {
                field: "view_patch.operations.path",
                reason: "must be an absolute JSON Pointer".to_owned(),
            });
        }
        Ok(())
    }
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

pub const APPEND_APPLICATION_EXECUTION_SUMMARY_INTENT_V1: &str =
    "cowd.work_context.append_application_execution_summary.v1";

/// Timeline-oriented execution summary. This is deliberately distinct from
/// [`ApplicationExecutionOutcomeV1`], which models governed APP effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionSummaryV1 {
    pub schema_version: u16,
    pub summary_id: String,
    pub kind: ApplicationExecutionSummaryKindV1,
    pub status: ApplicationExecutionSummaryStatusV1,
    pub title: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default)]
    pub refs: Vec<ApplicationExecutionSummaryRefV1>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub metric_refs: Vec<String>,
    #[serde(default)]
    pub counters: Vec<ApplicationExecutionSummaryCounterV1>,
    pub occurred_at_ms: u64,
}

impl ApplicationExecutionSummaryV1 {
    fn validate_fields(&self) -> Result<(), ProtocolValidationError> {
        require_schema("ApplicationExecutionSummaryV1", self.schema_version)?;
        require_bounded("summary_id", &self.summary_id, 256)?;
        require_bounded("title", &self.title, 512)?;
        require_bounded("summary", &self.summary, 16 * 1024)?;
        if let Some(domain) = &self.domain {
            require_bounded("domain", domain, 256)?;
        }
        if self.refs.len() > 128 || self.counters.len() > 128 {
            return Err(ProtocolValidationError::InvalidField {
                field: "execution_summary.collections",
                reason: "refs and counters must each contain at most 128 values".to_owned(),
            });
        }
        for reference in &self.refs {
            reference.validate()?;
        }
        for counter in &self.counters {
            counter.validate()?;
        }
        require_canonical_string_set("evidence_refs", &self.evidence_refs, 128, 256)?;
        require_canonical_string_set("metric_refs", &self.metric_refs, 128, 256)
    }

    /// Produces the only durable collection ordering accepted by the wire
    /// contract. Duplicate reference identities and counter names are rejected
    /// instead of being silently collapsed.
    pub fn normalized(&self) -> Result<Self, ProtocolValidationError> {
        let mut normalized = self.clone();
        normalized.refs.sort_by(|left, right| {
            (&left.ref_type, &left.id, &left.label).cmp(&(&right.ref_type, &right.id, &right.label))
        });
        if normalized
            .refs
            .windows(2)
            .any(|pair| (&pair[0].ref_type, &pair[0].id) == (&pair[1].ref_type, &pair[1].id))
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "refs",
                reason: "reference (type, id) identities must be unique".to_owned(),
            });
        }
        normalized.evidence_refs.sort();
        if normalized
            .evidence_refs
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "evidence_refs",
                reason: "must not contain duplicates".to_owned(),
            });
        }
        normalized.metric_refs.sort();
        if normalized
            .metric_refs
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "metric_refs",
                reason: "must not contain duplicates".to_owned(),
            });
        }
        normalized
            .counters
            .sort_by(|left, right| left.name.cmp(&right.name));
        if normalized
            .counters
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(ProtocolValidationError::InvalidField {
                field: "counters",
                reason: "counter names must be unique".to_owned(),
            });
        }
        normalized.validate_fields()?;
        Ok(normalized)
    }
}

impl ProtocolValidate for ApplicationExecutionSummaryV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        self.validate_fields()?;
        if &self.normalized()? != self {
            return Err(ProtocolValidationError::InvalidField {
                field: "execution_summary.collections",
                reason: "refs, evidence_refs, metric_refs, and counters must be canonical"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationExecutionSummaryKindV1 {
    Tool,
    Agent,
    Task,
    StructuredIngest,
    StructuredFact,
    StructuredEvidence,
    ApplicationCompute,
    ApplicationAction,
    SkillRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationExecutionSummaryStatusV1 {
    Planned,
    Running,
    Succeeded,
    Failed,
    Blocked,
    Partial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionSummaryRefV1 {
    #[serde(rename = "type")]
    pub ref_type: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl ProtocolValidate for ApplicationExecutionSummaryRefV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("ref.type", &self.ref_type, 256)?;
        require_bounded("ref.id", &self.id, 256)?;
        if let Some(label) = &self.label {
            require_bounded("ref.label", label, 512)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionSummaryCounterV1 {
    pub name: String,
    pub value: i64,
}

impl ProtocolValidate for ApplicationExecutionSummaryCounterV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_bounded("counter.name", &self.name, 256)
    }
}

/// APP-submitted intent. The authenticated producer identity is intentionally
/// host-owned and never accepted from this payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionSummaryIntentV1 {
    pub schema_version: u16,
    pub session_id: String,
    pub summary: ApplicationExecutionSummaryV1,
}

impl ProtocolValidate for ApplicationExecutionSummaryIntentV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("ApplicationExecutionSummaryIntentV1", self.schema_version)?;
        require_bounded("session_id", &self.session_id, 256)?;
        self.summary.validate()
    }
}

/// Host-created producer binding used as the durable idempotency identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionSummaryIdempotencyV1 {
    pub schema_version: u16,
    pub producer_id: String,
    pub summary_id: String,
}

impl ApplicationExecutionSummaryIdempotencyV1 {
    pub fn bind(
        producer_id: impl Into<String>,
        summary: &ApplicationExecutionSummaryV1,
    ) -> Result<Self, ProtocolValidationError> {
        summary.validate()?;
        let binding = Self {
            schema_version: 1,
            producer_id: producer_id.into(),
            summary_id: summary.summary_id.clone(),
        };
        binding.validate()?;
        Ok(binding)
    }

    #[must_use]
    pub fn event_id(&self) -> String {
        format!(
            "application-execution-summary:v{}:p{}:s{}",
            self.schema_version,
            hex_component(&self.producer_id),
            hex_component(&self.summary_id)
        )
    }
}

impl ProtocolValidate for ApplicationExecutionSummaryIdempotencyV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema(
            "ApplicationExecutionSummaryIdempotencyV1",
            self.schema_version,
        )?;
        require_bounded("producer_id", &self.producer_id, 256)?;
        require_bounded("summary_id", &self.summary_id, 256)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExecutionSummaryReceiptV1 {
    pub schema_version: u16,
    pub producer_id: String,
    pub summary_id: String,
    pub sequence: u64,
    pub replayed: bool,
}

impl ProtocolValidate for ApplicationExecutionSummaryReceiptV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("ApplicationExecutionSummaryReceiptV1", self.schema_version)?;
        require_bounded("producer_id", &self.producer_id, 256)?;
        require_bounded("summary_id", &self.summary_id, 256)?;
        if self.sequence == 0 {
            return Err(ProtocolValidationError::InvalidField {
                field: "sequence",
                reason: "must be non-zero".to_owned(),
            });
        }
        Ok(())
    }
}

fn hex_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.as_bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
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
