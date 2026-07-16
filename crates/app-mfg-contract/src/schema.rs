use std::collections::BTreeMap;

use schemars::{schema_for, JsonSchema};
use serde_json::Value;

use crate::{
    capability::MfgEntitlementProjectionV2,
    cockpit::{MfgContractFreshnessV1, MfgSurfaceStatusV1},
    error::{MfgApiErrorV1, MfgRecoveryAction},
    live::{
        MfgLiveDeltaV1, MfgLiveEnvelopeV1, MfgLiveHeartbeatV1, MfgLiveResyncV1,
        MfgLiveSnapshotStateV1, MfgLiveSnapshotV1,
    },
    mutation::{MfgActionContract, MfgMutationContextV1},
    operations::{
        MfgContractDiagnosticV1, MfgIncidentListQuery, MfgMutationResponseV1, MfgNoBodyRequestV1,
        MfgReadCollectionV1, MfgReadResourceV1, MfgReadResponseV1,
    },
    receipt::MfgReceiptV1,
    review::{
        MfgReportDeliveryReview, MfgReportDeliveryReviewCollection,
        MfgReportDeliveryReviewCreateRequest, MfgReportDeliveryReviewDecisionRequest,
        MfgReportDeliveryReviewEffect, MfgReportDeliveryReviewSummary,
    },
    route::MfgRouteContract,
    surface::{MfgFrontendContractV1, MfgSurfaceContract},
};

/// JSON Schema component registry owned by the canonical MFG contract crate.
///
/// Gateway may add canonical Matrix or application DTOs through
/// `register_type`, but it may not hand-write a second field schema.
#[derive(Debug, Clone, Default)]
pub struct MfgOpenApiSchemaRegistry {
    schemas: BTreeMap<String, Value>,
}

impl MfgOpenApiSchemaRegistry {
    #[must_use]
    pub fn canonical() -> Self {
        let mut registry = Self::default();
        registry.register_type::<MfgFrontendContractV1>("MfgFrontendContractV1");
        registry.register_type::<MfgSurfaceContract>("MfgSurfaceContract");
        registry.register_type::<MfgRouteContract>("MfgRouteContract");
        registry.register_type::<MfgActionContract>("MfgActionContract");
        registry.register_type::<MfgMutationContextV1>("MfgMutationContextV1");
        registry.register_type::<MfgApiErrorV1>("MfgApiErrorV1");
        registry.register_type::<MfgRecoveryAction>("MfgRecoveryAction");
        registry.register_type::<MfgReceiptV1>("MfgReceiptV1");
        registry.register_type::<MfgLiveEnvelopeV1>("MfgLiveEnvelopeV1");
        registry.register_type::<MfgLiveSnapshotV1>("MfgLiveSnapshotV1");
        registry.register_type::<MfgLiveSnapshotStateV1>("MfgLiveSnapshotStateV1");
        registry.register_type::<MfgLiveDeltaV1>("MfgLiveDeltaV1");
        registry.register_type::<MfgLiveResyncV1>("MfgLiveResyncV1");
        registry.register_type::<MfgLiveHeartbeatV1>("MfgLiveHeartbeatV1");
        registry.register_type::<MfgReportDeliveryReview>("MfgReportDeliveryReview");
        registry.register_type::<MfgReportDeliveryReviewCollection>(
            "MfgReportDeliveryReviewCollection",
        );
        registry.register_type::<MfgReportDeliveryReviewCreateRequest>(
            "MfgReportDeliveryReviewCreateRequest",
        );
        registry.register_type::<MfgReportDeliveryReviewDecisionRequest>(
            "MfgReportDeliveryReviewDecisionRequest",
        );
        registry.register_type::<MfgReportDeliveryReviewSummary>("MfgReportDeliveryReviewSummary");
        registry.register_type::<MfgReportDeliveryReviewEffect>("MfgReportDeliveryReviewEffect");
        registry.register_type::<MfgEntitlementProjectionV2>("MfgEntitlementProjectionV2");
        registry.register_type::<MfgContractFreshnessV1>("MfgContractFreshnessV1");
        registry.register_type::<MfgSurfaceStatusV1>("MfgSurfaceStatusV1");
        registry.register_type::<MfgContractDiagnosticV1>("MfgContractDiagnosticV1");
        registry.register_type::<MfgNoBodyRequestV1>("MfgNoBodyRequestV1");
        registry.register_type::<MfgIncidentListQuery>("MfgIncidentListQuery");
        registry.register_type::<MfgReadResponseV1>("MfgReadResponseV1");
        registry.register_type::<MfgMutationResponseV1>("MfgMutationResponseV1");
        registry.register_type::<MfgReadResourceV1<MfgRouteContract>>("MfgRouteContractResourceV1");
        registry
            .register_type::<MfgReadCollectionV1<MfgRouteContract>>("MfgRouteContractCollectionV1");
        registry
    }

    pub fn register_type<T: JsonSchema>(&mut self, name: impl Into<String>) {
        let name = name.into();
        let root = schema_for!(T);
        let mut value = serde_json::to_value(root).unwrap_or(Value::Bool(false));
        rewrite_component_local_refs(&mut value, &name);
        self.schemas.insert(name, value);
    }

    pub fn register_schema(&mut self, name: impl Into<String>, schema: Value) {
        self.schemas.insert(name.into(), schema);
    }

    #[must_use]
    pub fn into_components(self) -> BTreeMap<String, Value> {
        self.schemas
    }
}

fn rewrite_component_local_refs(value: &mut Value, component: &str) {
    match value {
        Value::Object(object) => {
            let rewritten = object
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/$defs/"))
                .map(|path| format!("#/components/schemas/{component}/$defs/{path}"));
            if let Some(reference) = rewritten {
                object.insert("$ref".to_string(), Value::String(reference));
            }
            for child in object.values_mut() {
                rewrite_component_local_refs(child, component);
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_component_local_refs(item, component);
            }
        }
        _ => {}
    }
}
