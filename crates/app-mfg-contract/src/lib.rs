//! Canonical cross-surface contract for the manufacturing application.
//!
//! This crate owns transport DTOs and semantic metadata only. Repository
//! rows, SQLite inputs and Runtime services remain in `app-mfg`.

pub mod capability;
pub mod cockpit;
pub mod error;
pub mod live;
pub mod mutation;
pub mod operations;
pub mod receipt;
pub mod review;
pub mod route;
pub mod schema;
pub mod surface;
pub mod version;

pub use capability::{
    active_mfg_capabilities_for_surface, core_profile_capabilities, mfg_profile_capabilities,
    MfgCapabilityId, MfgCoreProfileId, MfgEntitlementProjectionV2, MfgProfileId,
};
pub use cockpit::{MfgContractFreshnessV1, MfgSurfaceStatusV1};
pub use error::{MfgApiErrorV1, MfgErrorCode, MfgRecoveryAction, MfgRecoveryActionKind};
pub use live::{
    MfgLiveDeltaV1, MfgLiveEnvelopeV1, MfgLiveEventV1, MfgLiveHeartbeatV1, MfgLiveResyncV1,
    MfgLiveSnapshotStateV1, MfgLiveSnapshotV1,
};
pub use mutation::{
    mfg_action_contracts, MfgActionAvailability, MfgActionContract, MfgActionId, MfgActionRisk,
    MfgConfirmationKind, MfgIdempotencySemantics, MfgMultiActionId, MfgMutationClass,
    MfgMutationContextV1, MfgMutationSemantics, MfgRevisionSemantics,
};
pub use operations::{
    MfgContractDiagnosticV1, MfgMutationRequestV1, MfgMutationResponseV1, MfgNoBodyRequestV1,
    MfgReadCollectionV1, MfgReadResourceV1, MfgReadResponseV1,
};
pub use receipt::{MfgReceiptStatus, MfgReceiptV1};
pub use review::{
    MfgReportDeliveryReview, MfgReportDeliveryReviewCollection,
    MfgReportDeliveryReviewCreateRequest, MfgReportDeliveryReviewDecision,
    MfgReportDeliveryReviewDecisionRequest, MfgReportDeliveryReviewEffect,
    MfgReportDeliveryReviewRerouteTarget, MfgReportDeliveryReviewStatus,
    MfgReportDeliveryReviewSummary,
};
pub use route::{
    mfg_route_contract, mfg_route_contracts, MfgCapabilityRequirement, MfgConsumer,
    MfgRouteContract, MfgRouteId, MfgSchemaOwner,
};
pub use schema::MfgOpenApiSchemaRegistry;
pub use surface::{MfgFrontendContractV1, MfgSurfaceContract, MfgSurfaceKind, MfgSurfaceRole};
pub use version::{MfgContractVersion, MFG_CONTRACT_VERSION};

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn route_inventory_has_v541_and_v545_boundaries() {
        let routes = mfg_route_contracts();
        assert_eq!(routes.len(), 104);
        assert_eq!(
            routes
                .iter()
                .filter(|route| route.availability == MfgActionAvailability::Active)
                .count(),
            103
        );
        assert_eq!(
            routes
                .iter()
                .filter(|route| route.availability == MfgActionAvailability::PlannedV541)
                .count(),
            0
        );
        assert_eq!(
            routes
                .iter()
                .filter(|route| route.availability == MfgActionAvailability::PlannedV545)
                .count(),
            1
        );
        let method_paths = routes
            .iter()
            .map(|route| (route.method.as_str(), route.path.as_str()))
            .collect::<BTreeSet<_>>();
        assert_eq!(method_paths.len(), routes.len());
        let encoded = serde_json::to_string(&routes).expect("route contract JSON");
        let decoded = serde_json::from_str::<Vec<MfgRouteContract>>(&encoded)
            .expect("route contract roundtrip");
        assert_eq!(decoded, routes);
    }

    #[test]
    fn every_mutation_action_has_security_and_receipt_semantics() {
        let actions = mfg_action_contracts();
        let ids = actions
            .iter()
            .map(|action| action.action_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), actions.len());
        assert!(actions.iter().all(|action| {
            !action.required_capabilities.is_empty()
                && matches!(
                    action.mutation,
                    MfgMutationSemantics::PreviewReceipt
                        | MfgMutationSemantics::DurableReceipt { .. }
                )
        }));
    }

    #[test]
    fn nine_upsert_actions_have_distinct_create_and_revision_checked_update_semantics() {
        let actions = mfg_action_contracts();
        for prefix in [
            "mfg.reality.source_pack",
            "mfg.reality.metric_dependency",
            "mfg.reality.entity",
            "mfg.reality.relation",
            "mfg.playbook",
            "mfg.cockpit.profile",
            "mfg.alert_rule",
            "mfg.alert_subscription",
            "mfg.assignment",
        ] {
            let create = actions
                .iter()
                .find(|action| action.action_id.as_str() == format!("{prefix}.create"))
                .expect("create action");
            let update = actions
                .iter()
                .find(|action| action.action_id.as_str() == format!("{prefix}.update"))
                .expect("update action");
            assert!(matches!(
                create.mutation,
                MfgMutationSemantics::DurableReceipt {
                    revision: MfgRevisionSemantics::CreateOnly,
                    ..
                }
            ));
            assert!(matches!(
                update.mutation,
                MfgMutationSemantics::DurableReceipt {
                    revision: MfgRevisionSemantics::Required,
                    ..
                }
            ));
        }
    }

    #[test]
    fn canonical_schema_registry_contains_transport_contracts() {
        let schemas = MfgOpenApiSchemaRegistry::canonical().into_components();
        for required in [
            "MfgFrontendContractV1",
            "MfgApiErrorV1",
            "MfgReceiptV1",
            "MfgLiveEnvelopeV1",
            "MfgReportDeliveryReview",
            "MfgEntitlementProjectionV2",
        ] {
            assert!(schemas.contains_key(required), "missing schema {required}");
        }
    }

    #[test]
    fn surface_capability_inventory_uses_only_active_consumable_actions() {
        let webui = active_mfg_capabilities_for_surface("webui");
        let tui = active_mfg_capabilities_for_surface("tui");
        let unknown = active_mfg_capabilities_for_surface("unknown");
        assert!(webui.contains(&"mfg.cockpit.manage".to_string()));
        assert!(!tui.contains(&"mfg.cockpit.manage".to_string()));
        assert!(tui.contains(&"mfg.read".to_string()));
        assert!(webui.contains(&"mfg.report.review".to_string()));
        assert!(tui.contains(&"mfg.report.review".to_string()));
        assert!(unknown.is_empty());
    }

    #[test]
    fn entitlement_profile_catalogs_preserve_legacy_ceiling_and_new_role_boundaries() {
        let legacy_core = core_profile_capabilities(MfgCoreProfileId::CoreLegacy09530);
        assert!(legacy_core.contains(&"definition.manage"));
        assert!(!legacy_core.contains(&"definition.default.set"));
        assert!(!legacy_core.contains(&"definition.rollback"));

        let legacy_mfg = mfg_profile_capabilities(MfgProfileId::MfgLegacy09529);
        assert!(legacy_mfg.contains(&MfgCapabilityId::DataManage));
        assert!(!legacy_mfg.contains(&MfgCapabilityId::ReportReview));
        assert!(!legacy_mfg.contains(&MfgCapabilityId::AssignmentLifecycle));
        assert_eq!(
            mfg_profile_capabilities(MfgProfileId::MfgViewer),
            &[MfgCapabilityId::Read]
        );
        assert!(mfg_profile_capabilities(MfgProfileId::MfgReviewer)
            .contains(&MfgCapabilityId::ReportReview));
        assert!(mfg_profile_capabilities(MfgProfileId::MfgManager)
            .contains(&MfgCapabilityId::AssignmentLifecycle));
    }
}
