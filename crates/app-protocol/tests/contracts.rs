#![allow(clippy::expect_used)]

use std::collections::BTreeMap;

use cowd_app_protocol::*;
use serde_json::json;

const DIGEST: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn digest() -> Sha256Digest {
    Sha256Digest(DIGEST.to_owned())
}

fn principal() -> PrincipalContextV1 {
    PrincipalContextV1 {
        subject: "user:1".to_owned(),
        tenant_id: "tenant:1".to_owned(),
        workspace_id: "workspace:1".to_owned(),
        delegation: DelegationKindV1::User,
        grant_id: "grant:1".to_owned(),
        authorization_profile_id: "operator".to_owned(),
        authorization_revision: 7,
        granted_capabilities: vec![
            "approval.respond".to_owned(),
            "reference-app.read".to_owned(),
            "reference-app.write".to_owned(),
        ],
        granted_scopes: vec!["workspace:read".to_owned(), "workspace:write".to_owned()],
        credential_epoch: 11,
        expires_at_unix_ms: Some(4_000_000_000_000),
    }
}

fn command_descriptor() -> OperationDescriptorV1 {
    OperationDescriptorV1 {
        operation_id: "reference.command.v1".to_owned(),
        kind: OperationKindV1::Command,
        input_schema_digest: digest(),
        output_schema_digest: digest(),
        required_capabilities: vec![
            "approval.respond".to_owned(),
            "reference-app.write".to_owned(),
        ],
        delegation: OperationDelegationV1::User,
        tenant_scoped: true,
        workspace_scoped: true,
        read_only: false,
        idempotency: IdempotencySemanticsV1::Required,
        default_deadline_ms: 3_000,
        maximum_deadline_ms: 10_000,
        maximum_request_bytes: 65_536,
        maximum_response_bytes: 1_048_576,
        maximum_frame_bytes: 1_048_576,
        streaming: false,
        replay_window_seconds: None,
        degraded_read_allowed: false,
        audit_classification: "domain_write".to_owned(),
    }
}

fn command_envelope() -> AppInvocationEnvelopeV1 {
    AppInvocationEnvelopeV1 {
        schema_version: 1,
        operation_id: "reference.command.v1".to_owned(),
        request_id: "request:1".to_owned(),
        correlation_id: "correlation:1".to_owned(),
        causation_id: None,
        deadline_unix_ms: 4_000_000_000_000,
        idempotency_key: Some("idempotency:1".to_owned()),
        expected_revision: Some("7".to_owned()),
        call_chain: vec!["core:runtime".to_owned()],
        max_hops: 4,
        input_schema_digest: digest(),
        principal: principal(),
        execution: ExecutionContextV1 {
            surface: "tui".to_owned(),
            session_id: Some("session:1".to_owned()),
            turn_id: Some("turn:1".to_owned()),
            task_id: Some("task:1".to_owned()),
        },
        payload: json!({"value": 1}),
    }
}

#[test]
fn golden_fixtures_decode_and_validate() {
    let handshake: AppHandshakeV1 = decode_strict(include_bytes!(
        "../contracts/v1/golden/handshake-success.json"
    ))
    .expect("handshake fixture");
    assert_eq!(handshake.app_id.0, "reference-app");
    let manifest: AppManifestV1 =
        decode_strict(include_bytes!("../contracts/v1/golden/app-manifest.json"))
            .expect("manifest fixture");
    handshake
        .validate_against_manifest(&manifest)
        .expect("handshake is bound to the signed manifest");

    let query: AppProviderResponseV1 =
        decode_strict(include_bytes!("../contracts/v1/golden/query-success.json"))
            .expect("query fixture");
    assert_eq!(query.revision.as_deref(), Some("7"));

    let receipt: DurableReceiptV1 = decode_strict(include_bytes!(
        "../contracts/v1/golden/command-receipt.json"
    ))
    .expect("receipt fixture");
    assert_eq!(receipt.status, ReceiptStatusV1::Completed);

    let frame: AppStreamFrameV1 =
        decode_strict(include_bytes!("../contracts/v1/golden/stream-open.json"))
            .expect("stream fixture");
    assert_eq!(frame.sequence(), 0);

    let error: AppErrorResponseV1 =
        decode_strict(include_bytes!("../contracts/v1/golden/error-cycle.json"))
            .expect("error fixture");
    assert_eq!(error.error.code.http_status(), 409);

    let bridge: IframeBridgeMessageV1 = decode_strict(include_bytes!(
        "../contracts/v1/golden/iframe-host-init.json"
    ))
    .expect("iframe fixture");
    assert!(matches!(bridge, IframeBridgeMessageV1::HostInit { .. }));

    let query_invocation: AppInvocationEnvelopeV1 = decode_strict(include_bytes!(
        "../contracts/v1/golden/query-invocation.json"
    ))
    .expect("query invocation fixture");
    assert_eq!(query_invocation.execution.surface, "web");

    let command_invocation: AppInvocationEnvelopeV1 = decode_strict(include_bytes!(
        "../contracts/v1/golden/command-invocation.json"
    ))
    .expect("command invocation fixture");
    command_invocation
        .validate_at(1_800_000_000_000, &command_descriptor())
        .expect("verified command invocation fixture");

    let manifest: AppManifestV1 =
        decode_strict(include_bytes!("../contracts/v1/golden/app-manifest.json"))
            .expect("signed manifest fixture");
    let catalog: CoreOperationCatalogV1 = decode_strict(include_bytes!(
        "../contracts/v1/golden/core-operation-catalog.json"
    ))
    .expect("core operation catalog fixture");
    catalog
        .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
        .expect("golden APP-scoped catalog");
    assert_eq!(
        handshake.capability_digest,
        manifest_capability_digest_v1(&manifest).expect("handshake capability digest")
    );
    assert_eq!(
        handshake.authorization_profile_digest,
        manifest_authorization_profile_digest_v1(&manifest)
            .expect("handshake authorization profile digest")
    );

    let manifest_digests: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../contracts/v1/golden/manifest-digests.json"
    ))
    .expect("manifest digest fixture");
    assert_eq!(
        manifest_digests["capability_digest"],
        serde_json::to_value(
            manifest_capability_digest_v1(&manifest).expect("golden capability digest")
        )
        .expect("capability digest JSON")
    );
    assert_eq!(
        manifest_digests["authorization_profile_digest"],
        serde_json::to_value(
            manifest_authorization_profile_digest_v1(&manifest)
                .expect("golden authorization profile digest")
        )
        .expect("profile digest JSON")
    );
}

#[test]
fn core_operation_catalog_path_is_frozen() {
    assert_eq!(CORE_OPERATIONS_PATH_V1, "/_cowd/core/v1/operations");
}

#[test]
fn strict_decode_rejects_unknown_fields_and_versions() {
    let unknown = br#"{
        "schema_version":1,
        "protocol_revision":1,
        "app_id":"reference-app",
        "generation":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "gateway_pid":1,
        "worker_pid":2,
        "unexpected":true
    }"#;
    assert!(decode_strict::<AppHandshakeRequestV1>(unknown).is_err());

    let incompatible = AppHandshakeRequestV1 {
        schema_version: 1,
        protocol_revision: 2,
        app_id: AppId("reference-app".to_owned()),
        generation: GenerationId(DIGEST.to_owned()),
        gateway_pid: 1,
        worker_pid: 2,
    };
    assert!(matches!(
        incompatible.validate(),
        Err(ProtocolValidationError::UnsupportedProtocol { actual: 2 })
    ));
}

#[test]
fn invalid_identity_signature_and_path_fail_closed() {
    let mut manifest = valid_manifest();
    manifest.app_id = AppId("../escape".to_owned());
    assert!(manifest.validate().is_err());

    let mut manifest = valid_manifest();
    manifest.executable = "../outside".to_owned();
    assert!(manifest.validate().is_err());

    let mut manifest = valid_manifest();
    manifest.signature.signature.clear();
    assert!(manifest.validate().is_err());
}

#[test]
fn command_requires_idempotency_and_call_chain_rejects_cycles() {
    let descriptor = command_descriptor();
    let mut envelope = command_envelope();
    envelope.idempotency_key = None;
    assert!(envelope.validate_for(&descriptor).is_err());

    let mut envelope = command_envelope();
    let result = envelope.append_authority("core:runtime".to_owned());
    assert!(matches!(
        result,
        Err(ProtocolValidationError::InvalidField {
            field: "call_chain",
            ..
        })
    ));
}

#[test]
fn verified_context_maps_every_legacy_request_fact_without_payload_inference() {
    #[derive(Debug, PartialEq, Eq)]
    struct LegacyRequestFacts<'a> {
        principal_id: &'a str,
        workspace_id: &'a str,
        surface: &'a str,
        request_id: &'a str,
        granted_capabilities: &'a [String],
        profile_revision: u64,
        granted_scopes: &'a [String],
        credential_epoch: u64,
        expires_at_ms: Option<u64>,
    }

    let envelope = command_envelope();
    let mapped = LegacyRequestFacts {
        principal_id: &envelope.principal.subject,
        workspace_id: &envelope.principal.workspace_id,
        surface: &envelope.execution.surface,
        request_id: &envelope.request_id,
        granted_capabilities: &envelope.principal.granted_capabilities,
        profile_revision: envelope.principal.authorization_revision,
        granted_scopes: &envelope.principal.granted_scopes,
        credential_epoch: envelope.principal.credential_epoch,
        expires_at_ms: envelope.principal.expires_at_unix_ms,
    };
    assert_eq!(mapped.principal_id, "user:1");
    assert_eq!(mapped.workspace_id, "workspace:1");
    assert_eq!(mapped.surface, "tui");
    assert_eq!(mapped.request_id, "request:1");
    assert_eq!(mapped.profile_revision, 7);
    assert_eq!(mapped.credential_epoch, 11);
    assert_eq!(mapped.expires_at_ms, Some(4_000_000_000_000));
    assert_eq!(mapped.granted_capabilities.len(), 3);
    assert_eq!(mapped.granted_scopes.len(), 2);
}

#[test]
fn reference_invocations_round_trip() {
    for bytes in [
        include_bytes!("../contracts/v1/golden/query-invocation.json").as_slice(),
        include_bytes!("../contracts/v1/golden/command-invocation.json").as_slice(),
    ] {
        let wire: serde_json::Value = serde_json::from_slice(bytes).expect("reference wire JSON");
        let envelope: AppInvocationEnvelopeV1 =
            decode_strict(bytes).expect("reference invocation interop");
        assert_eq!(
            serde_json::to_value(envelope).expect("serialize reference invocation"),
            wire
        );
    }
}

#[test]
fn core_bridge_invocation_requires_the_exact_signed_origin_edge() {
    let manifest = valid_manifest();
    let catalog = valid_core_catalog(&manifest);
    let descriptor = &catalog.operations[0];
    let mut invocation = command_envelope();
    invocation.operation_id = descriptor.operation_id.clone();
    invocation.input_schema_digest = descriptor.input_schema_digest.clone();
    invocation.call_chain = vec!["app:reference-app".to_owned()];
    invocation.principal.granted_capabilities = vec![
        "approval.respond".to_owned(),
        "reference-app.write".to_owned(),
    ];
    let bridge = CoreBridgeInvocationV1 {
        schema_version: 1,
        originating_app_operation_id: "reference-app.command.v1".to_owned(),
        invocation,
    };
    bridge
        .validate_at_for_manifest(3_999_999_999_000, descriptor, &manifest)
        .expect("signed edge invocation");

    let mut wrong_origin = bridge.clone();
    wrong_origin.originating_app_operation_id = "reference-app.other.v1".to_owned();
    assert!(wrong_origin
        .validate_at_for_manifest(3_999_999_999_000, descriptor, &manifest)
        .is_err());

    let mut missing_origin_authority = bridge.clone();
    missing_origin_authority.invocation.call_chain = vec!["core:runtime".to_owned()];
    assert!(missing_origin_authority
        .validate_at_for_manifest(3_999_999_999_000, descriptor, &manifest)
        .is_err());

    let mut missing_app_capability = bridge.clone();
    missing_app_capability
        .invocation
        .principal
        .granted_capabilities = vec!["approval.respond".to_owned()];
    assert!(missing_app_capability
        .validate_at_for_manifest(3_999_999_999_000, descriptor, &manifest)
        .is_err());

    let mut missing_core_capability = bridge;
    missing_core_capability
        .invocation
        .principal
        .granted_capabilities = vec!["reference-app.write".to_owned()];
    assert!(missing_core_capability
        .validate_at_for_manifest(3_999_999_999_000, descriptor, &manifest)
        .is_err());
}

#[test]
fn invocation_context_is_canonical_complete_and_deny_unknown() {
    let value = serde_json::to_value(command_envelope()).expect("serialize envelope");
    for pointer in [
        "/request_id",
        "/principal/subject",
        "/principal/tenant_id",
        "/principal/workspace_id",
        "/principal/delegation",
        "/principal/grant_id",
        "/principal/authorization_profile_id",
        "/principal/authorization_revision",
        "/principal/granted_capabilities",
        "/principal/granted_scopes",
        "/principal/credential_epoch",
        "/principal/expires_at_unix_ms",
        "/execution/surface",
    ] {
        let mut missing = value.clone();
        let (parent, field) = pointer.rsplit_once('/').expect("fixture pointer");
        missing
            .pointer_mut(parent)
            .and_then(serde_json::Value::as_object_mut)
            .expect("fixture object")
            .remove(field);
        let bytes = serde_json::to_vec(&missing).expect("encode missing field fixture");
        assert!(
            decode_strict::<AppInvocationEnvelopeV1>(&bytes).is_err(),
            "missing {pointer} must fail closed"
        );
    }

    let mut no_expiry = command_envelope();
    no_expiry.principal.expires_at_unix_ms = None;
    decode_strict::<AppInvocationEnvelopeV1>(
        &serde_json::to_vec(&no_expiry).expect("nullable expiry fixture"),
    )
    .expect("an explicit null expiry remains valid");

    let mut unknown = value;
    unknown
        .pointer_mut("/principal")
        .and_then(serde_json::Value::as_object_mut)
        .expect("principal object")
        .insert("unverified_role".to_owned(), json!("admin"));
    assert!(decode_strict::<AppInvocationEnvelopeV1>(
        &serde_json::to_vec(&unknown).expect("unknown fixture")
    )
    .is_err());
}

#[test]
fn descriptor_binding_rejects_operation_and_schema_tamper() {
    let descriptor = command_descriptor();
    let mut operation_tamper = command_envelope();
    operation_tamper.operation_id = "attacker.command.v1".to_owned();
    assert!(operation_tamper.validate_for(&descriptor).is_err());

    let mut schema_tamper = command_envelope();
    schema_tamper.input_schema_digest = Sha256Digest(
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
    );
    assert!(schema_tamper.validate_for(&descriptor).is_err());
}

#[test]
fn capabilities_and_scopes_must_be_unique_sorted_and_bounded() {
    let mut envelope = command_envelope();
    envelope.principal.granted_capabilities = vec![
        "reference-app.write".to_owned(),
        "reference-app.read".to_owned(),
    ];
    assert!(envelope.validate().is_err());

    let mut envelope = command_envelope();
    envelope
        .principal
        .granted_scopes
        .push("workspace:write".to_owned());
    assert!(envelope.validate().is_err());

    let mut envelope = command_envelope();
    envelope.principal.authorization_profile_id = "x".repeat(129);
    assert!(envelope.validate().is_err());

    let mut envelope = command_envelope();
    envelope.execution.surface = "x".repeat(129);
    assert!(envelope.validate().is_err());
}

#[test]
fn operation_requirements_are_non_empty_canonical_and_all_of() {
    let mut empty = command_descriptor();
    empty.required_capabilities.clear();
    assert!(empty.validate().is_err());

    let mut duplicate = command_descriptor();
    duplicate
        .required_capabilities
        .push("reference-app.write".to_owned());
    assert!(duplicate.validate().is_err());

    let mut unsorted = command_descriptor();
    unsorted.required_capabilities.reverse();
    assert!(unsorted.validate().is_err());

    let mut missing_core_grant = command_envelope();
    missing_core_grant.principal.granted_capabilities = vec![
        "reference-app.read".to_owned(),
        "reference-app.write".to_owned(),
    ];
    assert!(missing_core_grant
        .validate_for(&command_descriptor())
        .is_err());
}

#[test]
fn validate_at_enforces_expiry_deadline_delegation_capability_and_scope() {
    let descriptor = command_descriptor();
    let envelope = command_envelope();
    envelope
        .validate_at(3_999_999_999_999, &descriptor)
        .expect("grant is live");
    assert_eq!(envelope.effective_deadline_unix_ms(), 4_000_000_000_000);
    assert!(envelope
        .validate_at(4_000_000_000_000, &descriptor)
        .is_err());

    let mut expired_grant = command_envelope();
    expired_grant.deadline_unix_ms = 4_100_000_000_000;
    expired_grant.principal.expires_at_unix_ms = Some(4_000_000_000_000);
    assert_eq!(
        expired_grant.effective_deadline_unix_ms(),
        4_000_000_000_000
    );
    assert!(expired_grant
        .validate_at(4_000_000_000_000, &descriptor)
        .is_err());

    let mut wrong_delegation = command_envelope();
    wrong_delegation.principal.delegation = DelegationKindV1::Service;
    assert!(wrong_delegation.validate_for(&descriptor).is_err());

    let mut missing_capability = command_envelope();
    missing_capability.principal.granted_capabilities = vec![
        "approval.respond".to_owned(),
        "reference-app.read".to_owned(),
    ];
    assert!(missing_capability.validate_for(&descriptor).is_err());

    let mut missing_tenant = command_envelope();
    missing_tenant.principal.tenant_id.clear();
    assert!(missing_tenant.validate_for(&descriptor).is_err());

    let mut missing_workspace = command_envelope();
    missing_workspace.principal.workspace_id.clear();
    assert!(missing_workspace.validate_for(&descriptor).is_err());
}

#[test]
fn frozen_negative_invocation_fixtures_fail_closed() {
    for (name, bytes) in [
        (
            "missing-authorization-revision",
            include_bytes!("../contracts/v1/golden/negative/missing-authorization-revision.json")
                .as_slice(),
        ),
        (
            "duplicate-capability",
            include_bytes!("../contracts/v1/golden/negative/duplicate-capability.json").as_slice(),
        ),
        (
            "unsorted-scope",
            include_bytes!("../contracts/v1/golden/negative/unsorted-scope.json").as_slice(),
        ),
        (
            "unknown-principal-field",
            include_bytes!("../contracts/v1/golden/negative/unknown-principal-field.json")
                .as_slice(),
        ),
    ] {
        assert!(
            decode_strict::<AppInvocationEnvelopeV1>(bytes).is_err(),
            "negative fixture {name} must fail closed"
        );
    }

    let expired: AppInvocationEnvelopeV1 = decode_strict(include_bytes!(
        "../contracts/v1/golden/negative/expired-grant.json"
    ))
    .expect("expired fixture has a valid wire shape");
    assert!(expired
        .validate_at(1_800_000_000_000, &command_descriptor())
        .is_err());

    let wrong_delegation: AppInvocationEnvelopeV1 = decode_strict(include_bytes!(
        "../contracts/v1/golden/negative/wrong-delegation.json"
    ))
    .expect("delegation fixture has a valid wire shape");
    assert!(wrong_delegation
        .validate_for(&command_descriptor())
        .is_err());

    let missing_capability: AppInvocationEnvelopeV1 = decode_strict(include_bytes!(
        "../contracts/v1/golden/negative/missing-capability.json"
    ))
    .expect("capability fixture has a valid wire shape");
    assert!(missing_capability
        .validate_for(&command_descriptor())
        .is_err());
}

#[test]
fn stream_and_patch_revisions_are_monotonic() {
    let bad_open = AppStreamFrameV1::Open {
        schema_version: 1,
        subscription_id: "subscription:1".to_owned(),
        sequence: 1,
        schema_digest: digest(),
    };
    assert!(bad_open.validate().is_err());

    let patch = AppViewPatchV1 {
        schema_version: 1,
        app_id: AppId("reference-app".to_owned()),
        view_id: "main".to_owned(),
        base_revision: "7".to_owned(),
        revision: "7".to_owned(),
        operations: vec![AppViewPatchOperationV1::Remove {
            path: "/root/children/0".to_owned(),
        }],
    };
    assert!(patch.validate().is_err());
}

#[test]
fn iframe_api_rejects_auth_headers_and_unbounded_credit() {
    let request = IframeApiFrameV1::AppApiRequest {
        schema_version: 1,
        request_id: "request:1".to_owned(),
        method: "POST".to_owned(),
        path: "/api/apps/reference-app/items".to_owned(),
        deadline_unix_ms: 4_000_000_000_000,
        headers: BTreeMap::from([("Authorization".to_owned(), "secret".to_owned())]),
        body: json!({}),
    };
    assert!(request.validate().is_err());

    let credit = IframeApiFrameV1::AppApiCredit {
        schema_version: 1,
        request_id: "request:1".to_owned(),
        bytes: 0,
    };
    assert!(credit.validate().is_err());
}

#[test]
fn catalog_rejects_duplicate_apps_and_inconsistent_web_surface() {
    let app = catalog_entry();
    let catalog = AppCatalogV1 {
        schema_version: 1,
        protocol_revision: 1,
        protocol_digest: digest(),
        catalog_generation: digest(),
        apps: vec![app.clone(), app],
    };
    assert!(catalog.validate().is_err());

    let mut app = catalog_entry();
    app.web_surface.available = false;
    assert!(app.validate().is_err());
}

#[test]
fn app_scoped_core_catalog_closes_manifest_authorization_and_schema_binding() {
    let manifest = valid_manifest();
    manifest.validate().expect("bound manifest");
    let catalog = valid_core_catalog(&manifest);
    catalog
        .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
        .expect("APP-scoped core catalog");

    assert!(catalog
        .validate_for_manifest(
            &manifest,
            &GenerationId(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
            ),
        )
        .is_err());

    let mut unauthorized = catalog.clone();
    let mut extra = command_descriptor();
    extra.operation_id = "core.unrequested.command.v1".to_owned();
    unauthorized.operations.push(extra);
    unauthorized
        .operations
        .sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    unauthorized
        .bind_canonical_catalog_digest()
        .expect("bind unauthorized fixture");
    assert!(unauthorized
        .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
        .is_err());

    let mut remapped = catalog.clone();
    remapped.operations[0].operation_id = "core.remapped.command.v1".to_owned();
    remapped
        .bind_canonical_catalog_digest()
        .expect("bind remap fixture");
    assert!(remapped
        .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
        .is_err());

    let mut schema_mismatch = catalog.clone();
    schema_mismatch.operations[0].input_schema_digest = Sha256Digest(
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
    );
    schema_mismatch
        .bind_canonical_catalog_digest()
        .expect("bind schema mismatch fixture");
    assert!(schema_mismatch
        .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
        .is_err());

    let mut kind_mismatch = catalog;
    let descriptor = &mut kind_mismatch.operations[0];
    descriptor.kind = OperationKindV1::Query;
    descriptor.read_only = true;
    descriptor.idempotency = IdempotencySemanticsV1::ReadOnly;
    kind_mismatch
        .bind_canonical_catalog_digest()
        .expect("bind kind mismatch fixture");
    assert!(kind_mismatch
        .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
        .is_err());

    let mut capability_mismatch = valid_core_catalog(&manifest);
    capability_mismatch.operations[0].required_capabilities =
        vec!["reference-app.write".to_owned()];
    capability_mismatch
        .bind_canonical_catalog_digest()
        .expect("bind capability mismatch fixture");
    assert!(capability_mismatch
        .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
        .is_err());

    let mut unsigned_app_capability = valid_core_catalog(&manifest);
    unsigned_app_capability.operations[0].required_capabilities = vec![
        "approval.respond".to_owned(),
        "reference-app.read".to_owned(),
    ];
    unsigned_app_capability
        .bind_canonical_catalog_digest()
        .expect("bind unsigned APP capability fixture");
    assert!(unsigned_app_capability
        .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
        .is_err());
}

#[test]
fn core_catalog_and_signed_manifest_reject_tamper_and_unknown_fields() {
    let manifest = valid_manifest();
    let mut catalog = valid_core_catalog(&manifest);
    catalog.operations[0].audit_classification = "tampered".to_owned();
    assert!(catalog.validate().is_err());

    let mut requirement_tamper = manifest.clone();
    requirement_tamper.core_bridge_requirements[0].core_operation_id =
        "core.tampered.command.v1".to_owned();
    assert!(requirement_tamper.validate().is_err());

    let mut unknown = serde_json::to_value(valid_core_catalog(&manifest)).expect("catalog JSON");
    unknown
        .as_object_mut()
        .expect("catalog object")
        .insert("system_operations".to_owned(), json!([]));
    assert!(decode_strict::<CoreOperationCatalogV1>(
        &serde_json::to_vec(&unknown).expect("unknown catalog fixture")
    )
    .is_err());

    assert!(decode_strict::<AppManifestV1>(include_bytes!(
        "../contracts/v1/golden/negative/manifest-requirement-tamper.json"
    ))
    .is_err());
    assert!(decode_strict::<CoreOperationCatalogV1>(include_bytes!(
        "../contracts/v1/golden/negative/core-catalog-tamper.json"
    ))
    .is_err());
    assert!(decode_strict::<CoreOperationCatalogV1>(include_bytes!(
        "../contracts/v1/golden/negative/core-catalog-unknown.json"
    ))
    .is_err());

    for bytes in [
        include_bytes!("../contracts/v1/golden/negative/manifest-unsorted-capabilities.json")
            .as_slice(),
        include_bytes!("../contracts/v1/golden/negative/manifest-no-default-profile.json")
            .as_slice(),
        include_bytes!("../contracts/v1/golden/negative/manifest-duplicate-requirement.json")
            .as_slice(),
        include_bytes!("../contracts/v1/golden/negative/manifest-cross-namespace-capability.json")
            .as_slice(),
        include_bytes!("../contracts/v1/golden/negative/manifest-cross-namespace-profile.json")
            .as_slice(),
        include_bytes!("../contracts/v1/golden/negative/manifest-cross-namespace-surface.json")
            .as_slice(),
    ] {
        assert!(decode_strict::<AppManifestV1>(bytes).is_err());
    }

    for bytes in [
        include_bytes!(
            "../contracts/v1/golden/negative/operation-empty-required-capabilities.json"
        )
        .as_slice(),
        include_bytes!(
            "../contracts/v1/golden/negative/operation-duplicate-required-capabilities.json"
        )
        .as_slice(),
        include_bytes!(
            "../contracts/v1/golden/negative/operation-unsorted-required-capabilities.json"
        )
        .as_slice(),
        include_bytes!("../contracts/v1/golden/negative/operation-legacy-required-capability.json")
            .as_slice(),
    ] {
        assert!(decode_strict::<OperationDescriptorV1>(bytes).is_err());
    }

    for bytes in [
        include_bytes!("../contracts/v1/golden/negative/core-catalog-unauthorized.json").as_slice(),
        include_bytes!("../contracts/v1/golden/negative/core-catalog-remapped.json").as_slice(),
        include_bytes!("../contracts/v1/golden/negative/core-catalog-schema-mismatch.json")
            .as_slice(),
        include_bytes!("../contracts/v1/golden/negative/core-catalog-capability-mismatch.json")
            .as_slice(),
        include_bytes!("../contracts/v1/golden/negative/core-catalog-unsigned-app-capability.json")
            .as_slice(),
    ] {
        let catalog: CoreOperationCatalogV1 =
            decode_strict(bytes).expect("negative catalog remains structurally valid");
        assert!(catalog
            .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
            .is_err());
    }
}

#[test]
fn manifest_collections_are_canonical_and_handshake_digests_use_frozen_helpers() {
    let manifest = valid_manifest();
    let capability_digest = manifest_capability_digest_v1(&manifest).expect("capability digest");
    let profile_digest =
        manifest_authorization_profile_digest_v1(&manifest).expect("profile digest");
    assert_ne!(capability_digest, profile_digest);
    assert_eq!(
        capability_digest,
        manifest_capability_digest_v1(&manifest).expect("repeat capability digest")
    );
    assert_eq!(
        profile_digest,
        manifest_authorization_profile_digest_v1(&manifest).expect("repeat profile digest")
    );

    let mut unsorted_capabilities = manifest.clone();
    unsorted_capabilities.capabilities.reverse();
    assert!(unsorted_capabilities.validate().is_err());
    assert!(manifest_capability_digest_v1(&unsorted_capabilities).is_err());

    let mut duplicate_profile_capability = manifest.clone();
    duplicate_profile_capability.authorization_profiles[0]
        .capabilities
        .push("reference-app.write".to_owned());
    assert!(duplicate_profile_capability.validate().is_err());

    let mut no_default = manifest.clone();
    no_default.authorization_profiles[0].is_default = false;
    no_default
        .bind_canonical_signed_digest()
        .expect("bind no-default fixture");
    assert!(no_default.validate().is_err());
    assert!(manifest_authorization_profile_digest_v1(&no_default).is_err());

    let mut top_level_core_capability = valid_manifest();
    top_level_core_capability.capabilities = vec!["approval.respond".to_owned()];
    top_level_core_capability.authorization_profiles[0].capabilities =
        vec!["approval.respond".to_owned()];
    top_level_core_capability.core_bridge_requirements[0].required_app_capabilities =
        vec!["approval.respond".to_owned()];
    top_level_core_capability
        .bind_canonical_signed_digest()
        .expect("bind cross-namespace APP capability");
    assert!(top_level_core_capability.validate().is_err());

    let mut profile_core_capability = valid_manifest();
    profile_core_capability.authorization_profiles[0]
        .capabilities
        .push("approval.respond".to_owned());
    profile_core_capability.authorization_profiles[0]
        .capabilities
        .sort();
    profile_core_capability
        .bind_canonical_signed_digest()
        .expect("bind cross-namespace profile capability");
    assert!(profile_core_capability.validate().is_err());

    let mut surface_core_capability = valid_manifest();
    surface_core_capability.authorization_profiles[0]
        .surface_capabilities
        .insert("web".to_owned(), vec!["approval.respond".to_owned()]);
    surface_core_capability
        .bind_canonical_signed_digest()
        .expect("bind cross-namespace surface capability");
    assert!(surface_core_capability.validate().is_err());

    let mut two_defaults = manifest.clone();
    let mut second_profile = two_defaults.authorization_profiles[0].clone();
    second_profile.profile_id = "viewer".to_owned();
    second_profile.display_name = "Viewer".to_owned();
    two_defaults.authorization_profiles.push(second_profile);
    two_defaults
        .bind_canonical_signed_digest()
        .expect("bind two-default fixture");
    assert!(two_defaults.validate().is_err());

    let mut empty = manifest;
    empty.authorization_profiles.clear();
    empty
        .bind_canonical_signed_digest()
        .expect("bind empty profiles");
    empty.validate().expect("empty profiles carry no default");
}

#[test]
fn core_bridge_requirements_form_a_sorted_many_to_many_edge_graph() {
    let mut manifest = valid_manifest();
    let first = manifest.core_bridge_requirements[0].clone();

    let mut unsorted = manifest.clone();
    let mut earlier = first.clone();
    earlier.app_operation_id = "reference-app.a.command.v1".to_owned();
    earlier.core_operation_id = "core.reference.a.command.v1".to_owned();
    unsorted.core_bridge_requirements.push(earlier);
    unsorted
        .bind_canonical_signed_digest()
        .expect("bind unsorted requirements");
    assert!(unsorted.validate().is_err());

    let mut duplicate_edge = manifest.clone();
    duplicate_edge.core_bridge_requirements.push(first.clone());
    duplicate_edge
        .bind_canonical_signed_digest()
        .expect("bind duplicate edge");
    assert!(duplicate_edge.validate().is_err());

    let mut same_app_second_core = first.clone();
    same_app_second_core.core_operation_id = "core.reference.other.command.v1".to_owned();
    let mut same_core_second_app = first.clone();
    same_core_second_app.app_operation_id = "reference-app.tui.main.action".to_owned();
    manifest
        .core_bridge_requirements
        .extend([same_app_second_core, same_core_second_app]);
    manifest.core_bridge_requirements.sort_by(|left, right| {
        (&left.app_operation_id, &left.core_operation_id)
            .cmp(&(&right.app_operation_id, &right.core_operation_id))
    });
    manifest
        .bind_canonical_signed_digest()
        .expect("bind many-to-many graph");
    manifest.validate().expect("many-to-many graph");

    let mut catalog = valid_core_catalog(&manifest);
    let mut second_core = catalog.operations[0].clone();
    second_core.operation_id = "core.reference.other.command.v1".to_owned();
    catalog.operations.push(second_core);
    catalog
        .operations
        .sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    catalog
        .bind_canonical_catalog_digest()
        .expect("bind distinct Core catalog");
    catalog
        .validate_for_manifest(&manifest, &GenerationId(DIGEST.to_owned()))
        .expect("two Core descriptors close three signed edges");

    let mut wrong_app_namespace = valid_manifest();
    wrong_app_namespace.core_bridge_requirements[0].app_operation_id =
        "core.reference.command.v1".to_owned();
    wrong_app_namespace
        .bind_canonical_signed_digest()
        .expect("bind wrong APP namespace");
    assert!(wrong_app_namespace.validate().is_err());

    let mut wrong_core_namespace = valid_manifest();
    wrong_core_namespace.core_bridge_requirements[0].core_operation_id =
        "reference.command.v1".to_owned();
    wrong_core_namespace
        .bind_canonical_signed_digest()
        .expect("bind wrong Core namespace");
    assert!(wrong_core_namespace.validate().is_err());
}

#[test]
fn result_contracts_are_signed_namespaced_bounded_and_canonical() {
    let manifest = valid_manifest();
    manifest.validate().expect("valid signed result contract");

    let mut wrong_namespace = manifest.clone();
    wrong_namespace
        .presentation
        .as_mut()
        .expect("presentation")
        .result_contracts[0]
        .contract_id = "other-app.result.v1".to_owned();
    wrong_namespace
        .bind_canonical_signed_digest()
        .expect("bind wrong namespace");
    assert!(wrong_namespace.validate().is_err());

    let mut duplicate = manifest.clone();
    let contract = duplicate
        .presentation
        .as_ref()
        .expect("presentation")
        .result_contracts[0]
        .clone();
    duplicate
        .presentation
        .as_mut()
        .expect("presentation")
        .result_contracts
        .push(contract);
    duplicate
        .bind_canonical_signed_digest()
        .expect("bind duplicate");
    assert!(duplicate.validate().is_err());

    let mut unbounded = manifest;
    unbounded
        .presentation
        .as_mut()
        .expect("presentation")
        .result_contracts[0]
        .max_bytes = 64 * 1024 * 1024 + 1;
    unbounded
        .bind_canonical_signed_digest()
        .expect("bind unbounded contract");
    assert!(unbounded.validate().is_err());
}

#[test]
fn execution_summary_is_canonical_and_producer_bound() {
    let mut summary = ApplicationExecutionSummaryV1 {
        schema_version: 1,
        summary_id: "summary-1".to_owned(),
        kind: ApplicationExecutionSummaryKindV1::ApplicationAction,
        status: ApplicationExecutionSummaryStatusV1::Succeeded,
        title: "Action completed".to_owned(),
        summary: "The requested APP action completed.".to_owned(),
        domain: Some("manufacturing".to_owned()),
        refs: vec![
            ApplicationExecutionSummaryRefV1 {
                ref_type: "evidence".to_owned(),
                id: "evidence-2".to_owned(),
                label: None,
            },
            ApplicationExecutionSummaryRefV1 {
                ref_type: "action".to_owned(),
                id: "action-1".to_owned(),
                label: Some("Action".to_owned()),
            },
        ],
        evidence_refs: vec!["evidence:2".to_owned(), "evidence:1".to_owned()],
        metric_refs: vec!["metric:throughput".to_owned()],
        counters: vec![
            ApplicationExecutionSummaryCounterV1 {
                name: "warnings".to_owned(),
                value: 0,
            },
            ApplicationExecutionSummaryCounterV1 {
                name: "affected_rows".to_owned(),
                value: 3,
            },
        ],
        occurred_at_ms: 42,
    };
    assert!(summary.validate().is_err());
    summary = summary.normalized().expect("canonical summary");
    summary.validate().expect("canonical summary validates");

    let left = ApplicationExecutionSummaryIdempotencyV1::bind("app:one", &summary)
        .expect("first producer binding");
    let right = ApplicationExecutionSummaryIdempotencyV1::bind("app:two", &summary)
        .expect("second producer binding");
    assert_ne!(left.event_id(), right.event_id());

    let intent = ApplicationExecutionSummaryIntentV1 {
        schema_version: 1,
        session_id: "session-1".to_owned(),
        summary,
    };
    intent.validate().expect("valid producer-neutral intent");
    assert!(serde_json::to_value(intent)
        .expect("intent JSON")
        .get("producer_id")
        .is_none());

    let unknown_receipt = json!({
        "schema_version": 1,
        "producer_id": "app:one",
        "summary_id": "summary-1",
        "sequence": 1,
        "replayed": false,
        "payload": {}
    });
    assert!(
        serde_json::from_value::<ApplicationExecutionSummaryReceiptV1>(unknown_receipt).is_err()
    );
}

#[test]
fn signed_tui_descriptor_is_the_only_transport_authority() {
    let manifest = valid_manifest();
    let operations = valid_app_operations();
    let handshake = AppHandshakeV1 {
        schema_version: 1,
        protocol_revision: 1,
        app_id: manifest.app_id.clone(),
        generation: GenerationId(DIGEST.to_owned()),
        artifact_version: manifest.artifact_version.clone(),
        worker_pid: 7,
        worker_nonce: "nonce".to_owned(),
        operations: operations.clone(),
        operation_catalog_digest: app_operation_catalog_digest_v1(&manifest.app_id, &operations)
            .expect("catalog digest"),
        capability_digest: manifest_capability_digest_v1(&manifest).expect("capability digest"),
        authorization_profile_digest: manifest_authorization_profile_digest_v1(&manifest)
            .expect("profile digest"),
    };
    handshake
        .validate_against_manifest(&manifest)
        .expect("signed TUI catalog binding");

    let mut omitted_operation = handshake.clone();
    omitted_operation.operations.pop();
    omitted_operation.operation_catalog_digest =
        app_operation_catalog_digest_v1(&omitted_operation.app_id, &omitted_operation.operations)
            .expect("truncated catalog digest");
    assert!(omitted_operation
        .validate_against_manifest(&manifest)
        .is_err());

    let mut wrong_role = handshake;
    let open = wrong_role
        .operations
        .iter_mut()
        .find(|operation| operation.operation_id == "reference-app.tui.main.open")
        .expect("open operation");
    open.kind = OperationKindV1::Command;
    open.read_only = false;
    open.idempotency = IdempotencySemanticsV1::Required;
    open.degraded_read_allowed = false;
    wrong_role.operation_catalog_digest =
        app_operation_catalog_digest_v1(&wrong_role.app_id, &wrong_role.operations)
            .expect("wrong-role digest");
    let mut wrong_role_manifest = manifest.clone();
    wrong_role_manifest.operation_catalog_digest = wrong_role.operation_catalog_digest.clone();
    wrong_role_manifest
        .bind_canonical_signed_digest()
        .expect("bind wrong-role manifest");
    assert!(wrong_role
        .validate_against_manifest(&wrong_role_manifest)
        .is_err());

    let local_action: AppViewActionDescriptorV1 = serde_json::from_value(json!({
        "action_id": "refresh",
        "component_id": "toolbar",
        "label": "Refresh",
        "enabled": true,
        "requires_confirmation": false
    }))
    .expect("local action discriminator");
    local_action
        .validate()
        .expect("local action is not an operation id");
    assert!(serde_json::from_value::<AppViewActionDescriptorV1>(json!({
        "action_id": "refresh",
        "component_id": "toolbar",
        "label": "Refresh",
        "enabled": true,
        "requires_confirmation": false,
        "required_capability": "reference-app.write"
    }))
    .is_err());
    assert!(serde_json::from_value::<AppViewSubscriptionV1>(json!({
        "subscription_id": "updates",
        "stream_path": "/arbitrary/worker/path"
    }))
    .is_err());
}

fn valid_manifest() -> AppManifestV1 {
    let app_id = AppId("reference-app".to_owned());
    let operations = valid_app_operations();
    let mut manifest = AppManifestV1 {
        schema_version: 1,
        app_id: app_id.clone(),
        display_name: "Reference APP".to_owned(),
        artifact_version: "1.0.0".to_owned(),
        required_protocol: ProtocolRangeV1::exact_v1(),
        executable: "bin/reference-worker".to_owned(),
        web_root: Some("webui".to_owned()),
        capabilities: vec![
            "reference-app.read".to_owned(),
            "reference-app.write".to_owned(),
        ],
        authorization_profiles: vec![AuthorizationProfileV1 {
            profile_id: "operator".to_owned(),
            display_name: "Operator".to_owned(),
            capabilities: vec![
                "reference-app.read".to_owned(),
                "reference-app.write".to_owned(),
            ],
            surface_capabilities: BTreeMap::new(),
            is_default: true,
        }],
        operation_catalog_digest: app_operation_catalog_digest_v1(&app_id, &operations)
            .expect("valid APP operation catalog digest"),
        core_bridge_requirements: vec![CoreBridgeRequirementV1 {
            app_operation_id: "reference-app.command.v1".to_owned(),
            core_operation_id: "core.reference.command.v1".to_owned(),
            accepted_input_schema_digest: digest(),
            accepted_output_schema_digest: digest(),
            required_app_capabilities: vec!["reference-app.write".to_owned()],
            kind: OperationKindV1::Command,
            streaming: false,
        }],
        surfaces: AppSurfacesV1 {
            web: true,
            tui_view: true,
        },
        integrity: BundleIntegrityV1 {
            algorithm: IntegrityAlgorithmV1::Sha256,
            files: BTreeMap::from([("bin/reference-worker".to_owned(), digest())]),
            manifest_digest: digest(),
        },
        signature: BundleSignatureV1 {
            algorithm: SignatureAlgorithmV1::Ed25519,
            key_id: "release-key-1".to_owned(),
            signature: "base64url-signature".to_owned(),
            signed_digest: digest(),
            expires_unix_ms: None,
            provenance_digest: Some(digest()),
        },
        sandbox: SandboxProfileV1 {
            filesystem: FilesystemPolicyV1::BundleReadOnlyDataReadWrite,
            network: NetworkPolicyV1::Deny,
            max_processes: 8,
            max_open_files: 256,
            max_memory_bytes: 256 * 1024 * 1024,
            cpu_quota_millis_per_second: 1_000,
        },
        presentation: Some(AppPresentationV1 {
            result_shape_revision: 1,
            result_contracts: vec![AppResultContractV1 {
                contract_id: "reference-app.result.v1".to_owned(),
                schema_id: "cowd.reference.result.v1".to_owned(),
                schema_version: 1,
                schema_digest: digest(),
                max_bytes: 256 * 1024,
            }],
            tui_views: vec![AppTuiViewDescriptorV1 {
                view_id: "main".to_owned(),
                open_operation_id: "reference-app.tui.main.open".to_owned(),
                action_operation_id: "reference-app.tui.main.action".to_owned(),
                stream_operation_id: "reference-app.tui.main.stream".to_owned(),
            }],
            core_navigation_kinds: vec!["reality.object".to_owned()],
        }),
    };
    manifest
        .bind_canonical_signed_digest()
        .expect("bind reference manifest");
    manifest
}

fn valid_app_operations() -> Vec<OperationDescriptorV1> {
    let mut bridge = command_descriptor();
    bridge.operation_id = "reference-app.command.v1".to_owned();
    bridge.required_capabilities = vec!["reference-app.write".to_owned()];

    let mut action = command_descriptor();
    action.operation_id = "reference-app.tui.main.action".to_owned();
    action.input_schema_digest =
        app_tui_view_action_request_schema_digest_v1().expect("action input digest");
    action.output_schema_digest =
        app_tui_view_action_response_schema_digest_v1().expect("action output digest");
    action.required_capabilities = vec!["reference-app.write".to_owned()];
    action.audit_classification = "tui_interaction".to_owned();

    let mut open = command_descriptor();
    open.operation_id = "reference-app.tui.main.open".to_owned();
    open.kind = OperationKindV1::Query;
    open.input_schema_digest =
        app_tui_view_open_request_schema_digest_v1().expect("open input digest");
    open.output_schema_digest =
        app_tui_view_open_response_schema_digest_v1().expect("open output digest");
    open.required_capabilities = vec!["reference-app.read".to_owned()];
    open.read_only = true;
    open.idempotency = IdempotencySemanticsV1::ReadOnly;
    open.degraded_read_allowed = true;
    open.audit_classification = "tui_interaction".to_owned();

    let stream = OperationDescriptorV1 {
        operation_id: "reference-app.tui.main.stream".to_owned(),
        kind: OperationKindV1::Subscribe,
        input_schema_digest: app_tui_view_stream_request_schema_digest_v1()
            .expect("stream input digest"),
        output_schema_digest: app_tui_view_patch_schema_digest_v1().expect("stream output digest"),
        required_capabilities: vec!["reference-app.read".to_owned()],
        delegation: OperationDelegationV1::User,
        tenant_scoped: true,
        workspace_scoped: true,
        read_only: true,
        idempotency: IdempotencySemanticsV1::SubscriptionCursor,
        default_deadline_ms: 3_000,
        maximum_deadline_ms: 10_000,
        maximum_request_bytes: 65_536,
        maximum_response_bytes: 1_048_576,
        maximum_frame_bytes: 1_048_576,
        streaming: true,
        replay_window_seconds: Some(60),
        degraded_read_allowed: true,
        audit_classification: "tui_interaction".to_owned(),
    };
    stream.validate().expect("valid stream descriptor");

    let mut operations = vec![bridge, action, open, stream];
    operations.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    operations
}

fn valid_core_catalog(manifest: &AppManifestV1) -> CoreOperationCatalogV1 {
    let mut operation = command_descriptor();
    operation.operation_id = "core.reference.command.v1".to_owned();
    operation.required_capabilities = vec!["approval.respond".to_owned()];
    let mut catalog = CoreOperationCatalogV1 {
        schema_version: 1,
        protocol_revision: 1,
        app_id: manifest.app_id.clone(),
        generation: GenerationId(DIGEST.to_owned()),
        catalog_digest: digest(),
        operations: vec![operation],
    };
    catalog
        .bind_canonical_catalog_digest()
        .expect("bind reference core catalog");
    catalog
}

fn catalog_entry() -> AppCatalogEntryV1 {
    AppCatalogEntryV1 {
        app_id: AppId("reference-app".to_owned()),
        display_name: "Reference APP".to_owned(),
        artifact_version: "1.0.0".to_owned(),
        generation: GenerationId(DIGEST.to_owned()),
        required: false,
        activation: AppActivationPolicyV1::Lazy,
        lifecycle: AppLifecycleV1 {
            state: AppLifecycleStateV1::Mounted,
            reason_code: None,
            retryable: false,
            retry_after_ms: None,
        },
        compatibility: AppCompatibilityV1 {
            status: AppCompatibilityStatusV1::Compatible,
            gateway_supported_minimum: 1,
            gateway_supported_maximum: 1,
            app_required_minimum: 1,
            app_required_maximum: 1,
        },
        web_surface: AppWebSurfaceV1 {
            available: true,
            entry_path: Some("/apps/reference-app/index.html".to_owned()),
            bridge_revision: 1,
        },
        effective_capabilities: vec!["reference-app.read".to_owned()],
        effective_authorization_profile: "operator".to_owned(),
    }
}
