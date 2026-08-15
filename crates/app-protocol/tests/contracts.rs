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
    }
}

fn command_descriptor() -> OperationDescriptorV1 {
    OperationDescriptorV1 {
        operation_id: "reference.command.v1".to_owned(),
        kind: OperationKindV1::Command,
        input_schema_digest: digest(),
        output_schema_digest: digest(),
        required_capability: "app.reference.write".to_owned(),
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

fn valid_manifest() -> AppManifestV1 {
    AppManifestV1 {
        schema_version: 1,
        app_id: AppId("reference-app".to_owned()),
        display_name: "Reference APP".to_owned(),
        artifact_version: "1.0.0".to_owned(),
        required_protocol: ProtocolRangeV1::exact_v1(),
        executable: "bin/reference-worker".to_owned(),
        web_root: Some("webui".to_owned()),
        capabilities: vec!["app.reference.read".to_owned()],
        authorization_profiles: vec![AuthorizationProfileV1 {
            profile_id: "operator".to_owned(),
            display_name: "Operator".to_owned(),
            capabilities: vec!["app.reference.read".to_owned()],
            surface_capabilities: BTreeMap::new(),
            is_default: true,
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
            view_ids: vec!["main".to_owned()],
            core_navigation_kinds: vec!["reality.object".to_owned()],
        }),
    }
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
        effective_capabilities: vec!["app.reference.read".to_owned()],
        effective_authorization_profile: "operator".to_owned(),
    }
}
