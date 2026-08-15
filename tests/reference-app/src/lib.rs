use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cowd_app_protocol::{
    app_operation_catalog_digest_v1, manifest_authorization_profile_digest_v1,
    manifest_capability_digest_v1, AppId, AppManifestV1, AppPresentationV1, AppResultContractV1,
    AppSurfacesV1, AppTuiViewDescriptorV1, AuthorizationProfileV1, BundleIntegrityV1,
    BundleSignatureV1, CoreBridgeRequirementV1, FilesystemPolicyV1, IdempotencySemanticsV1,
    IntegrityAlgorithmV1, NetworkPolicyV1, OperationDelegationV1, OperationDescriptorV1,
    OperationKindV1, ProtocolRangeV1, ProtocolValidate, SandboxProfileV1, Sha256Digest,
    SignatureAlgorithmV1,
};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};

pub const APP_ID: &str = "reference-app";
pub const ARTIFACT_VERSION: &str = "1.0.0";
pub const KEY_ID: &str = "reference-app-fixture-ed25519-v1";
pub const PROTOCOL_ARTIFACT_SHA256: &str =
    "8ac0b8482cc04846e15ca1296badf2de62fbb7aa2676bd9cd2765d024c76b71e";
pub const PROTOCOL_SOURCE_COMMIT: &str = "32422e08b960a756476f7bd6d006225e752edfb7";
pub const PROTOCOL_WIRE_DIGEST: &str =
    "sha256:2c61ceb144b819e88060a3f813585ebe3d064ad2acf8888104719fb82363c766";

const SIGNING_SEED: [u8; 32] = [
    0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
    0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
];

const TUI_ACTION_INPUT_SCHEMA: &str =
    "sha256:f7006908646c383932e27cbf4e7844c94cbdde31aafb63ed9ac91b1d236af4e1";
const TUI_ACTION_OUTPUT_SCHEMA: &str =
    "sha256:eae6cd26946e76fbdcad0fb226ac1935b32b333b1dc588ce7de8335c8ad0799f";
const TUI_OPEN_INPUT_SCHEMA: &str =
    "sha256:f08207dc1c30c68aa71284f830d29ef4e005a02b888063478a4627968129afcb";
const TUI_OPEN_OUTPUT_SCHEMA: &str =
    "sha256:793f71787696a637a65bba47e1ca873fddb614e0772d4a5587483029049fa559";
const TUI_STREAM_INPUT_SCHEMA: &str =
    "sha256:807f711df7ba418983639724b84898b634c31b2741ad8b6f4f27ea59111cf747";
const TUI_STREAM_OUTPUT_SCHEMA: &str =
    "sha256:92f7464d31bad03b6965110c1ccc3c4773d438577b1a55c90c94fae5015e74d8";

#[derive(Debug, thiserror::Error)]
pub enum ReferenceError {
    #[error("I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON failure: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protocol validation failed: {0}")]
    Protocol(String),
    #[error("bundle rejected: {0}")]
    Bundle(String),
}

pub type Result<T> = std::result::Result<T, ReferenceError>;

#[must_use]
pub fn operations() -> Vec<OperationDescriptorV1> {
    let mut operations = vec![
        operation(
            "reference-app.counter.increment",
            OperationKindV1::Command,
            "reference-app.command",
            false,
        ),
        operation(
            "reference-app.echo",
            OperationKindV1::Query,
            "reference-app.query",
            false,
        ),
        operation(
            "reference-app.events",
            OperationKindV1::Subscribe,
            "reference-app.subscribe",
            true,
        ),
        operation(
            "reference-app.export",
            OperationKindV1::Export,
            "reference-app.export",
            true,
        ),
        tui_operation(
            "reference-app.tui.reference-main.action",
            OperationKindV1::Command,
            "reference-app.command",
            Sha256Digest(TUI_ACTION_INPUT_SCHEMA.to_owned()),
            Sha256Digest(TUI_ACTION_OUTPUT_SCHEMA.to_owned()),
        ),
        tui_operation(
            "reference-app.tui.reference-main.open",
            OperationKindV1::Query,
            "reference-app.query",
            Sha256Digest(TUI_OPEN_INPUT_SCHEMA.to_owned()),
            Sha256Digest(TUI_OPEN_OUTPUT_SCHEMA.to_owned()),
        ),
        tui_operation(
            "reference-app.tui.reference-main.stream",
            OperationKindV1::Subscribe,
            "reference-app.subscribe",
            Sha256Digest(TUI_STREAM_INPUT_SCHEMA.to_owned()),
            Sha256Digest(TUI_STREAM_OUTPUT_SCHEMA.to_owned()),
        ),
    ];
    operations.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    operations
}

fn tui_operation(
    id: &str,
    kind: OperationKindV1,
    capability: &str,
    input_schema_digest: Sha256Digest,
    output_schema_digest: Sha256Digest,
) -> OperationDescriptorV1 {
    let mut descriptor = operation(
        id,
        kind,
        capability,
        matches!(kind, OperationKindV1::Subscribe),
    );
    descriptor.input_schema_digest = input_schema_digest;
    descriptor.output_schema_digest = output_schema_digest;
    "tui_interaction".clone_into(&mut descriptor.audit_classification);
    descriptor
}

fn operation(
    id: &str,
    kind: OperationKindV1,
    capability: &str,
    streaming: bool,
) -> OperationDescriptorV1 {
    let (read_only, idempotency, replay_window_seconds) = match kind {
        OperationKindV1::Query => (true, IdempotencySemanticsV1::ReadOnly, None),
        OperationKindV1::Command => (false, IdempotencySemanticsV1::Required, None),
        OperationKindV1::Subscribe => (true, IdempotencySemanticsV1::SubscriptionCursor, Some(60)),
        OperationKindV1::Export => (true, IdempotencySemanticsV1::ContentAddressed, None),
    };
    OperationDescriptorV1 {
        operation_id: id.to_owned(),
        kind,
        input_schema_digest: label_digest(&format!("{id}.input/v1")),
        output_schema_digest: label_digest(&format!("{id}.output/v1")),
        required_capabilities: vec![capability.to_owned()],
        delegation: OperationDelegationV1::Either,
        tenant_scoped: false,
        workspace_scoped: false,
        read_only,
        idempotency,
        default_deadline_ms: 5_000,
        maximum_deadline_ms: 30_000,
        maximum_request_bytes: 64 * 1024,
        maximum_response_bytes: 256 * 1024,
        maximum_frame_bytes: 16 * 1024,
        streaming,
        replay_window_seconds,
        degraded_read_allowed: read_only,
        audit_classification: "reference".to_owned(),
    }
}

#[must_use]
pub fn label_digest(label: &str) -> Sha256Digest {
    Sha256Digest(format!("sha256:{:x}", Sha256::digest(label.as_bytes())))
}

/// Computes the capability and authorization-profile digests used by handshake.
///
/// # Errors
/// Returns an error if the frozen manifest projection fails protocol validation.
pub fn manifest_digests() -> Result<(Sha256Digest, Sha256Digest, Sha256Digest)> {
    let manifest = unsigned_manifest(BTreeMap::from([(
        "bin/reference-app-worker".to_owned(),
        label_digest("placeholder"),
    )]))?;
    let capabilities = manifest_capability_digest_v1(&manifest)
        .map_err(|error| ReferenceError::Protocol(error.to_string()))?;
    let profiles = manifest_authorization_profile_digest_v1(&manifest)
        .map_err(|error| ReferenceError::Protocol(error.to_string()))?;
    Ok((manifest.operation_catalog_digest, capabilities, profiles))
}

fn unsigned_manifest(files: BTreeMap<String, Sha256Digest>) -> Result<AppManifestV1> {
    let capabilities = vec![
        "reference-app.command".to_owned(),
        "reference-app.export".to_owned(),
        "reference-app.query".to_owned(),
        "reference-app.subscribe".to_owned(),
    ];
    let placeholder = label_digest("manifest-placeholder");
    let app_id = AppId(APP_ID.to_owned());
    let operation_catalog_digest = app_operation_catalog_digest_v1(&app_id, &operations())
        .map_err(|error| ReferenceError::Protocol(error.to_string()))?;
    Ok(AppManifestV1 {
        schema_version: 1,
        app_id,
        display_name: "Cowd Reference APP".to_owned(),
        artifact_version: ARTIFACT_VERSION.to_owned(),
        required_protocol: ProtocolRangeV1::exact_v1(),
        executable: "bin/reference-app-worker".to_owned(),
        web_root: Some("webui".to_owned()),
        capabilities: capabilities.clone(),
        authorization_profiles: vec![AuthorizationProfileV1 {
            profile_id: "operator".to_owned(),
            display_name: "Reference operator".to_owned(),
            capabilities,
            surface_capabilities: BTreeMap::new(),
            is_default: true,
        }],
        operation_catalog_digest,
        core_bridge_requirements: Vec::<CoreBridgeRequirementV1>::new(),
        surfaces: AppSurfacesV1 {
            web: true,
            tui_view: true,
        },
        integrity: BundleIntegrityV1 {
            algorithm: IntegrityAlgorithmV1::Sha256,
            files,
            manifest_digest: placeholder.clone(),
        },
        signature: BundleSignatureV1 {
            algorithm: SignatureAlgorithmV1::Ed25519,
            key_id: KEY_ID.to_owned(),
            signature: "placeholder".to_owned(),
            signed_digest: placeholder,
            expires_unix_ms: None,
            provenance_digest: Some(Sha256Digest(PROTOCOL_WIRE_DIGEST.to_owned())),
        },
        sandbox: SandboxProfileV1 {
            filesystem: FilesystemPolicyV1::BundleReadOnlyDataReadWrite,
            network: NetworkPolicyV1::Deny,
            max_processes: 8,
            max_open_files: 256,
            max_memory_bytes: 128 * 1024 * 1024,
            cpu_quota_millis_per_second: 1_000,
        },
        presentation: Some(AppPresentationV1 {
            result_shape_revision: 1,
            result_contracts: vec![AppResultContractV1 {
                contract_id: "reference-app.result.v1".to_owned(),
                schema_id: "cowd.reference.result.v1".to_owned(),
                schema_version: 1,
                schema_digest: label_digest("reference-app.result.v1/schema/v1"),
                max_bytes: 256 * 1024,
            }],
            tui_views: vec![AppTuiViewDescriptorV1 {
                view_id: "reference.main".to_owned(),
                open_operation_id: "reference-app.tui.reference-main.open".to_owned(),
                action_operation_id: "reference-app.tui.reference-main.action".to_owned(),
                stream_operation_id: "reference-app.tui.reference-main.stream".to_owned(),
            }],
            core_navigation_kinds: Vec::new(),
        }),
    })
}

#[must_use]
pub fn verifying_key_bytes() -> [u8; 32] {
    SigningKey::from_bytes(&SIGNING_SEED)
        .verifying_key()
        .to_bytes()
}

/// Publishes one signed Bundle through a staging directory and a single rename.
///
/// # Errors
/// Returns an error for unsafe paths, existing output, I/O, signing, or protocol failure.
pub fn package(worker: &Path, output: &Path) -> Result<AppManifestV1> {
    if output.exists() {
        return Err(ReferenceError::Bundle("output already exists".to_owned()));
    }
    let name = output
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| ReferenceError::Bundle("output needs a UTF-8 name".to_owned()))?;
    let parent = output
        .parent()
        .ok_or_else(|| ReferenceError::Bundle("output needs a parent".to_owned()))?;
    let staging = parent.join(format!(".{name}.staging"));
    if staging.exists() {
        return Err(ReferenceError::Bundle("staging already exists".to_owned()));
    }
    fs::create_dir_all(staging.join("bin"))?;
    fs::create_dir_all(staging.join("webui"))?;
    let result = (|| {
        copy_sync(worker, &staging.join("bin/reference-app-worker"), 0o555)?;
        write_sync(
            &staging.join("webui/index.html"),
            include_bytes!("../webui/index.html"),
            0o444,
        )?;
        write_sync(
            &staging.join("webui/app.js"),
            include_bytes!("../webui/app.js"),
            0o444,
        )?;
        write_sync(
            &staging.join("LICENSE"),
            include_bytes!("../LICENSE"),
            0o444,
        )?;
        write_sync(&staging.join("NOTICE"), include_bytes!("../NOTICE"), 0o444)?;
        let files = inventory(&staging)?;
        let mut manifest = unsigned_manifest(files)?;
        let digest = manifest
            .bind_canonical_signed_digest()
            .map_err(|error| ReferenceError::Protocol(error.to_string()))?;
        let signature = SigningKey::from_bytes(&SIGNING_SEED).sign(digest.0.as_bytes());
        manifest.signature.signature = URL_SAFE_NO_PAD.encode(signature.to_bytes());
        manifest
            .validate()
            .map_err(|error| ReferenceError::Protocol(error.to_string()))?;
        write_sync(
            &staging.join("app.json"),
            &serde_json::to_vec_pretty(&manifest)?,
            0o444,
        )?;
        seal_bundle_directories(&staging)?;
        validate_bundle(&staging)?;
        File::open(&staging)?.sync_all()?;
        fs::rename(&staging, output)?;
        File::open(parent)?.sync_all()?;
        Ok(manifest)
    })();
    if result.is_err() && staging.exists() {
        make_bundle_removable(&staging);
        let _cleanup = fs::remove_dir_all(&staging);
    }
    result
}

/// Validates the closed file inventory, every digest, manifest, and Ed25519 signature.
///
/// # Errors
/// Returns an error when the Bundle differs from the frozen reference contract.
pub fn validate_bundle(root: &Path) -> Result<AppManifestV1> {
    let metadata = fs::symlink_metadata(root)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o222 != 0
    {
        return Err(ReferenceError::Bundle(
            "bundle root must be a real read-only directory".to_owned(),
        ));
    }
    validate_immutable_tree(root)?;
    let app_json = root.join("app.json");
    let app_json_metadata = fs::symlink_metadata(&app_json)?;
    if !app_json_metadata.is_file()
        || app_json_metadata.file_type().is_symlink()
        || app_json_metadata.permissions().mode() & 0o222 != 0
    {
        return Err(ReferenceError::Bundle(
            "app.json must be a real read-only file".to_owned(),
        ));
    }
    let bytes = fs::read(app_json)?;
    let manifest: AppManifestV1 = serde_json::from_slice(&bytes)?;
    manifest
        .validate()
        .map_err(|error| ReferenceError::Protocol(error.to_string()))?;
    if manifest.app_id.0 != APP_ID || manifest.signature.key_id != KEY_ID {
        return Err(ReferenceError::Bundle(
            "fixture identity differs".to_owned(),
        ));
    }
    let actual = inventory(root)?;
    if actual != manifest.integrity.files {
        return Err(ReferenceError::Bundle(
            "integrity inventory differs".to_owned(),
        ));
    }
    let encoded = URL_SAFE_NO_PAD
        .decode(&manifest.signature.signature)
        .map_err(|_| ReferenceError::Bundle("signature encoding invalid".to_owned()))?;
    let signature = Signature::from_slice(&encoded)
        .map_err(|_| ReferenceError::Bundle("signature length invalid".to_owned()))?;
    let key = VerifyingKey::from_bytes(&verifying_key_bytes())
        .map_err(|_| ReferenceError::Bundle("fixture public key invalid".to_owned()))?;
    key.verify(manifest.signature.signed_digest.0.as_bytes(), &signature)
        .map_err(|_| ReferenceError::Bundle("signature verification failed".to_owned()))?;
    Ok(manifest)
}

/// Installs a verified Bundle under an APP root through one atomic rename.
///
/// # Errors
/// Returns an error for invalid input, an occupied APP identity, or an I/O failure.
pub fn install_bundle(bundle: &Path, apps_root: &Path) -> Result<PathBuf> {
    let manifest = validate_bundle(bundle)?;
    fs::create_dir_all(apps_root)?;
    let destination = apps_root.join(&manifest.app_id.0);
    let staging = apps_root.join(format!(".{}.installing", manifest.app_id.0));
    if destination.exists() || staging.exists() {
        return Err(ReferenceError::Bundle(
            "APP is already installed".to_owned(),
        ));
    }
    fs::create_dir_all(staging.join("bin"))?;
    fs::create_dir_all(staging.join("webui"))?;
    let result = (|| {
        for (relative, mode) in [
            ("LICENSE", 0o444),
            ("NOTICE", 0o444),
            ("app.json", 0o444),
            ("bin/reference-app-worker", 0o555),
            ("webui/app.js", 0o444),
            ("webui/index.html", 0o444),
        ] {
            copy_sync(&bundle.join(relative), &staging.join(relative), mode)?;
        }
        seal_bundle_directories(&staging)?;
        validate_bundle(&staging)?;
        File::open(&staging)?.sync_all()?;
        fs::rename(&staging, &destination)?;
        File::open(apps_root)?.sync_all()?;
        Ok(destination.clone())
    })();
    if result.is_err() && staging.exists() {
        make_bundle_removable(&staging);
        let _cleanup = fs::remove_dir_all(staging);
    }
    result
}

/// Discovers and validates every immediate Bundle below an APP root.
///
/// # Errors
/// Returns an error if any candidate is invalid or two candidates claim one APP identity.
pub fn discover_bundles(apps_root: &Path) -> Result<Vec<(PathBuf, AppManifestV1)>> {
    let mut candidates = fs::read_dir(apps_root)?
        .map(|entry| entry.map(|value| value.path()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    candidates.sort();
    let mut app_ids = BTreeSet::new();
    let mut discovered = Vec::new();
    for candidate in candidates {
        let name = candidate
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if name.starts_with('.') {
            continue;
        }
        let manifest = validate_bundle(&candidate)?;
        if !app_ids.insert(manifest.app_id.clone()) {
            return Err(ReferenceError::Bundle("duplicate APP identity".to_owned()));
        }
        discovered.push((candidate, manifest));
    }
    Ok(discovered)
}

fn inventory(root: &Path) -> Result<BTreeMap<String, Sha256Digest>> {
    let mut paths = Vec::new();
    visit(root, root, &mut paths)?;
    paths.sort();
    let expected = BTreeSet::from([
        "LICENSE".to_owned(),
        "NOTICE".to_owned(),
        "bin/reference-app-worker".to_owned(),
        "webui/app.js".to_owned(),
        "webui/index.html".to_owned(),
    ]);
    if paths.iter().cloned().collect::<BTreeSet<_>>() != expected {
        return Err(ReferenceError::Bundle("bundle file set differs".to_owned()));
    }
    paths
        .into_iter()
        .map(|relative| {
            let path = root.join(&relative);
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(ReferenceError::Bundle(format!(
                    "unsafe bundle file {relative}"
                )));
            }
            Ok((relative, file_digest(&path)?))
        })
        .collect()
}

fn visit(root: &Path, path: &Path, output: &mut Vec<String>) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.file_type()?;
        if metadata.is_symlink() {
            return Err(ReferenceError::Bundle("symlink rejected".to_owned()));
        }
        if metadata.is_dir() {
            visit(root, &entry.path(), output)?;
        } else if entry.file_name() != "app.json" {
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| ReferenceError::Bundle("path escape".to_owned()))?
                .to_string_lossy()
                .replace('\\', "/");
            output.push(relative);
        }
    }
    Ok(())
}

fn file_digest(path: &Path) -> Result<Sha256Digest> {
    Ok(Sha256Digest(format!(
        "sha256:{:x}",
        Sha256::digest(fs::read(path)?)
    )))
}

fn copy_sync(source: &Path, destination: &Path, mode: u32) -> Result<()> {
    let bytes = fs::read(source)?;
    write_sync(destination, &bytes, mode)
}

fn write_sync(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.set_permissions(fs::Permissions::from_mode(mode))?;
    file.sync_all()?;
    Ok(())
}

fn validate_immutable_tree(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() || metadata.permissions().mode() & 0o222 != 0 {
            return Err(ReferenceError::Bundle(format!(
                "mutable or symlink bundle node {}",
                entry.path().display()
            )));
        }
        if metadata.is_dir() {
            validate_immutable_tree(&entry.path())?;
        }
    }
    Ok(())
}

fn seal_bundle_directories(root: &Path) -> Result<()> {
    fs::set_permissions(root.join("bin"), fs::Permissions::from_mode(0o555))?;
    fs::set_permissions(root.join("webui"), fs::Permissions::from_mode(0o555))?;
    fs::set_permissions(root, fs::Permissions::from_mode(0o555))?;
    Ok(())
}

fn make_bundle_removable(root: &Path) {
    let _root = fs::set_permissions(root, fs::Permissions::from_mode(0o700));
    for directory in [root.join("bin"), root.join("webui")] {
        let _directory = fs::set_permissions(directory, fs::Permissions::from_mode(0o700));
    }
}

#[must_use]
pub fn bundle_worker_path(root: &Path) -> PathBuf {
    root.join("bin/reference-app-worker")
}
