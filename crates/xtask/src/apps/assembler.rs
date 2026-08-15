//! Deterministic, atomic product assembly from independently built artifacts.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use cowd_app_protocol::{
    AppManifestV1, ProtocolValidate, SignatureAlgorithmV1, PROTOCOL_REVISION_V1,
};
use ring::signature::{UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const APP_MANIFEST_FILE: &str = "app.json";
const RELEASE_LOCK_FILE: &str = "release-lock.json";
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_TRUST_STORE_BYTES: u64 = 1024 * 1024;
const MAX_ARTIFACT_NODES: usize = 100_000;
const TREE_DIGEST_DOMAIN: &[u8] = b"cowd.product.artifact-tree/v1\0";

#[derive(Debug, Clone)]
struct AppInput {
    bundle: PathBuf,
    required: bool,
    input_index: usize,
}

#[derive(Debug, Clone)]
struct AssembleRequest {
    core: PathBuf,
    edge: PathBuf,
    apps: Vec<AppInput>,
    trust_store: PathBuf,
    protocol_digest: String,
    generation: String,
    output: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AssembleReport {
    destination: PathBuf,
    release_lock_digest: String,
    rejected_optional_apps: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseLockV1 {
    schema_version: u16,
    kind: String,
    generation: String,
    protocol: ProtocolLockV1,
    core: ArtifactLockV1,
    edge: ArtifactLockV1,
    apps: Vec<AppLockV1>,
    rejected_optional_apps: Vec<RejectedAppLockV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtocolLockV1 {
    revision: u16,
    digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactLockV1 {
    artifact_tree_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppLockV1 {
    app_id: String,
    required: bool,
    artifact_tree_digest: String,
    manifest_digest: String,
    signature_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectedAppLockV1 {
    input_index: usize,
    app_id: Option<String>,
    artifact_tree_digest: Option<String>,
    reason_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustStoreFileV1 {
    schema_version: u16,
    keys: Vec<TrustKeyFileV1>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustKeyFileV1 {
    key_id: String,
    public_key_base64url: String,
    revoked: bool,
}

#[derive(Debug)]
struct TrustStore {
    expected_uid: u32,
    keys: BTreeMap<String, TrustKey>,
}

#[derive(Debug)]
struct TrustKey {
    public_key: Vec<u8>,
    revoked: bool,
}

#[derive(Debug, Clone)]
struct ValidatedBundle {
    source: PathBuf,
    app_id: String,
    required: bool,
    input_index: usize,
    artifact_tree_digest: String,
    manifest_digest: String,
    signature_digest: String,
}

#[derive(Debug)]
struct BundleFailure {
    code: &'static str,
    detail: String,
}

impl BundleFailure {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone)]
struct TreeSnapshot {
    digest: String,
    files: BTreeMap<String, String>,
}

#[derive(Debug)]
struct TreeNode {
    relative: String,
    kind: TreeNodeKind,
    executable: bool,
    digest: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum TreeNodeKind {
    Directory,
    File,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishFault {
    None,
    #[cfg(test)]
    BeforeRename,
}

pub(crate) fn run_cli(arguments: &[String]) -> Result<(), String> {
    let request = parse_cli(arguments)?;
    let report = assemble(request)?;
    println!(
        "product generation={} release_lock_digest={} rejected_optional_apps={}",
        report.destination.display(),
        report.release_lock_digest,
        report.rejected_optional_apps
    );
    Ok(())
}

fn parse_cli(arguments: &[String]) -> Result<AssembleRequest, String> {
    let mut core = None;
    let mut edge = None;
    let mut trust_store = None;
    let mut protocol_digest = None;
    let mut generation = None;
    let mut output = None;
    let mut apps = Vec::new();
    let mut index = 0usize;
    while index < arguments.len() {
        let flag = arguments[index].as_str();
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| format!("{flag} requires one value"))?;
        match flag {
            "--core" => set_once(&mut core, PathBuf::from(value), flag)?,
            "--edge" => set_once(&mut edge, PathBuf::from(value), flag)?,
            "--trust-store" => set_once(&mut trust_store, PathBuf::from(value), flag)?,
            "--protocol-digest" => set_once(&mut protocol_digest, value.clone(), flag)?,
            "--generation" => set_once(&mut generation, value.clone(), flag)?,
            "--output" => set_once(&mut output, PathBuf::from(value), flag)?,
            "--required-app" | "--optional-app" => apps.push(AppInput {
                bundle: PathBuf::from(value),
                required: flag == "--required-app",
                input_index: apps.len(),
            }),
            _ => return Err(format!("unknown assembler flag {flag}")),
        }
        index += 2;
    }
    Ok(AssembleRequest {
        core: core.ok_or_else(|| "--core is required".to_owned())?,
        edge: edge.ok_or_else(|| "--edge is required".to_owned())?,
        apps,
        trust_store: trust_store.ok_or_else(|| "--trust-store is required".to_owned())?,
        protocol_digest: protocol_digest
            .ok_or_else(|| "--protocol-digest is required".to_owned())?,
        generation: generation.ok_or_else(|| "--generation is required".to_owned())?,
        output: output.ok_or_else(|| "--output is required".to_owned())?,
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T, flag: &str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        return Err(format!("{flag} may be supplied only once"));
    }
    Ok(())
}

fn assemble(request: AssembleRequest) -> Result<AssembleReport, String> {
    assemble_with_fault(request, PublishFault::None)
}

fn assemble_with_fault(
    request: AssembleRequest,
    fault: PublishFault,
) -> Result<AssembleReport, String> {
    validate_generation(&request.generation)?;
    validate_sha256_digest(&request.protocol_digest, "protocol digest")?;
    let trust = load_trust_store(&request.trust_store)?;
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    for input in &request.apps {
        match validate_bundle(
            &input.bundle,
            input.required,
            input.input_index,
            &trust,
            &request.protocol_digest,
        ) {
            Ok(bundle) => accepted.push(bundle),
            Err(failure) if input.required => {
                return Err(format!(
                    "required APP Bundle {} rejected [{}]: {}",
                    input.bundle.display(),
                    failure.code,
                    failure.detail
                ));
            }
            Err(failure) => rejected.push(rejection(input, &failure)),
        }
    }
    reject_duplicate_apps(&mut accepted, &mut rejected)?;
    accepted.sort_by(|left, right| left.app_id.cmp(&right.app_id));
    rejected.sort_by_key(|item| item.input_index);

    let core = canonical_input(&request.core, "Core artifact")?;
    let edge = canonical_input(&request.edge, "Edge artifact")?;
    fs::create_dir_all(&request.output)
        .map_err(|error| format!("create output root {}: {error}", request.output.display()))?;
    let output_metadata = fs::symlink_metadata(&request.output)
        .map_err(|error| format!("inspect output root {}: {error}", request.output.display()))?;
    if output_metadata.file_type().is_symlink() || !output_metadata.is_dir() {
        return Err("assembler output must be a real directory".to_owned());
    }
    let output = fs::canonicalize(&request.output)
        .map_err(|error| format!("canonicalize output root: {error}"))?;
    reject_recursive_input(&core, &output, "Core artifact")?;
    reject_recursive_input(&edge, &output, "Edge artifact")?;
    let destination = output.join(&request.generation);
    let staging = output.join(format!(".{}.staging", request.generation));
    if destination.exists() {
        return Err(format!(
            "product generation already exists: {}",
            destination.display()
        ));
    }
    if staging.exists() {
        return Err(format!(
            "stale product staging directory exists: {}",
            staging.display()
        ));
    }
    fs::create_dir(&staging)
        .map_err(|error| format!("create staging directory {}: {error}", staging.display()))?;
    fs::set_permissions(&staging, fs::Permissions::from_mode(0o700))
        .map_err(|error| format!("secure staging directory: {error}"))?;
    let mut guard = StagingGuard::new(staging.clone());

    copy_artifact(&core, &staging.join("core"))?;
    let core_snapshot = snapshot_tree(&staging.join("core"))?;
    copy_artifact(&edge, &staging.join("edge"))?;
    let edge_snapshot = snapshot_tree(&staging.join("edge"))?;
    fs::create_dir(staging.join("apps")).map_err(|error| format!("create apps stage: {error}"))?;

    let mut app_locks = Vec::new();
    for source_bundle in accepted {
        let app_destination = staging.join("apps").join(&source_bundle.app_id);
        let stage_result = (|| {
            copy_artifact(&source_bundle.source, &app_destination)?;
            seal_tree(&app_destination)?;
            let staged = validate_bundle(
                &app_destination,
                source_bundle.required,
                source_bundle.input_index,
                &trust,
                &request.protocol_digest,
            )
            .map_err(|failure| format!("[{}] {}", failure.code, failure.detail))?;
            if staged.app_id != source_bundle.app_id
                || staged.artifact_tree_digest != source_bundle.artifact_tree_digest
                || staged.manifest_digest != source_bundle.manifest_digest
                || staged.signature_digest != source_bundle.signature_digest
            {
                return Err("Bundle changed while being staged".to_owned());
            }
            Ok(staged)
        })();
        match stage_result {
            Ok(staged) => app_locks.push(AppLockV1 {
                app_id: staged.app_id,
                required: staged.required,
                artifact_tree_digest: staged.artifact_tree_digest,
                manifest_digest: staged.manifest_digest,
                signature_digest: staged.signature_digest,
            }),
            Err(detail) if source_bundle.required => {
                return Err(format!(
                    "required APP {} failed staging: {detail}",
                    source_bundle.app_id
                ));
            }
            Err(detail) => {
                if app_destination.exists() {
                    make_tree_writable(&app_destination);
                    fs::remove_dir_all(&app_destination).map_err(|error| {
                        format!("remove rejected optional APP staging directory: {error}")
                    })?;
                }
                rejected.push(RejectedAppLockV1 {
                    input_index: source_bundle.input_index,
                    app_id: Some(source_bundle.app_id),
                    artifact_tree_digest: Some(source_bundle.artifact_tree_digest),
                    reason_code: stable_failure_code(&detail),
                });
            }
        }
    }
    app_locks.sort_by(|left, right| left.app_id.cmp(&right.app_id));
    rejected.sort_by_key(|item| item.input_index);
    let release_lock = ReleaseLockV1 {
        schema_version: 1,
        kind: "cowd.product.release-lock-provenance.v1".to_owned(),
        generation: request.generation,
        protocol: ProtocolLockV1 {
            revision: PROTOCOL_REVISION_V1,
            digest: request.protocol_digest,
        },
        core: ArtifactLockV1 {
            artifact_tree_digest: core_snapshot.digest,
        },
        edge: ArtifactLockV1 {
            artifact_tree_digest: edge_snapshot.digest,
        },
        apps: app_locks,
        rejected_optional_apps: rejected,
    };
    let mut lock_bytes = serde_json::to_vec_pretty(&release_lock)
        .map_err(|error| format!("serialize release lock: {error}"))?;
    lock_bytes.push(b'\n');
    write_sync(&staging.join(RELEASE_LOCK_FILE), &lock_bytes, 0o444)?;
    seal_tree(&staging)?;
    sync_tree(&staging)?;

    #[cfg(test)]
    if fault == PublishFault::BeforeRename {
        guard.preserve();
        return Err("fault injection: crash before product generation rename".to_owned());
    }
    let _ = fault;

    fs::rename(&staging, &destination)
        .map_err(|error| format!("publish product generation: {error}"))?;
    guard.disarm();
    File::open(&output)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync product generation parent: {error}"))?;
    Ok(AssembleReport {
        destination,
        release_lock_digest: digest_bytes(&lock_bytes),
        rejected_optional_apps: release_lock.rejected_optional_apps.len(),
    })
}

fn reject_duplicate_apps(
    accepted: &mut Vec<ValidatedBundle>,
    rejected: &mut Vec<RejectedAppLockV1>,
) -> Result<(), String> {
    let mut by_id = BTreeMap::<String, Vec<usize>>::new();
    for (position, bundle) in accepted.iter().enumerate() {
        by_id
            .entry(bundle.app_id.clone())
            .or_default()
            .push(position);
    }
    let duplicates = by_id
        .into_iter()
        .filter(|(_, positions)| positions.len() > 1)
        .collect::<Vec<_>>();
    for (app_id, positions) in &duplicates {
        if positions
            .iter()
            .any(|position| accepted[*position].required)
        {
            return Err(format!("required APP identity is duplicated: {app_id}"));
        }
    }
    let duplicate_positions = duplicates
        .into_iter()
        .flat_map(|(_, positions)| positions)
        .collect::<BTreeSet<_>>();
    let mut retained = Vec::new();
    for (position, bundle) in accepted.drain(..).enumerate() {
        if duplicate_positions.contains(&position) {
            rejected.push(RejectedAppLockV1 {
                input_index: bundle.input_index,
                app_id: Some(bundle.app_id),
                artifact_tree_digest: Some(bundle.artifact_tree_digest),
                reason_code: "duplicate_app_id".to_owned(),
            });
        } else {
            retained.push(bundle);
        }
    }
    *accepted = retained;
    Ok(())
}

fn rejection(input: &AppInput, failure: &BundleFailure) -> RejectedAppLockV1 {
    RejectedAppLockV1 {
        input_index: input.input_index,
        app_id: peek_app_id(&input.bundle),
        artifact_tree_digest: snapshot_tree(&input.bundle).ok().map(|tree| tree.digest),
        reason_code: failure.code.to_owned(),
    }
}

fn stable_failure_code(detail: &str) -> String {
    if let Some(code) = detail
        .strip_prefix('[')
        .and_then(|value| value.split_once(']'))
        .map(|(code, _)| code)
    {
        return code.to_owned();
    }
    "staging_failed".to_owned()
}

fn validate_bundle(
    bundle: &Path,
    required: bool,
    input_index: usize,
    trust: &TrustStore,
    protocol_digest: &str,
) -> Result<ValidatedBundle, BundleFailure> {
    let source = canonical_bundle_root(bundle)?;
    require_immutable_bundle_tree(&source, trust.expected_uid)?;
    let manifest_path = source.join(APP_MANIFEST_FILE);
    let metadata = fs::symlink_metadata(&manifest_path)
        .map_err(|error| BundleFailure::new("manifest_unreadable", error.to_string()))?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_MANIFEST_BYTES
    {
        return Err(BundleFailure::new(
            "manifest_invalid_file",
            "app.json must be a regular file no larger than 1 MiB",
        ));
    }
    let bytes = fs::read(&manifest_path)
        .map_err(|error| BundleFailure::new("manifest_unreadable", error.to_string()))?;
    let manifest: AppManifestV1 = serde_json::from_slice(&bytes)
        .map_err(|error| BundleFailure::new("manifest_invalid_json", error.to_string()))?;
    manifest
        .validate()
        .map_err(|error| BundleFailure::new("manifest_contract_invalid", error.to_string()))?;
    if manifest.required_protocol.minimum > PROTOCOL_REVISION_V1
        || manifest.required_protocol.maximum < PROTOCOL_REVISION_V1
    {
        return Err(BundleFailure::new(
            "protocol_range_incompatible",
            "Bundle protocol range excludes the assembled product protocol revision",
        ));
    }
    if manifest
        .signature
        .provenance_digest
        .as_ref()
        .is_some_and(|digest| digest.0 != protocol_digest)
    {
        return Err(BundleFailure::new(
            "protocol_digest_mismatch",
            "Bundle provenance digest differs from the assembled product protocol digest",
        ));
    }
    verify_signature(&manifest, trust)?;
    let snapshot = snapshot_tree(&source)
        .map_err(|error| BundleFailure::new("integrity_tree_invalid", error))?;
    let actual_signed_files = snapshot
        .files
        .iter()
        .filter(|(relative, _)| relative.as_str() != APP_MANIFEST_FILE)
        .map(|(relative, digest)| (relative.clone(), digest.clone()))
        .collect::<BTreeMap<_, _>>();
    let expected_signed_files = manifest
        .integrity
        .files
        .iter()
        .map(|(relative, digest)| (relative.clone(), digest.0.clone()))
        .collect::<BTreeMap<_, _>>();
    if actual_signed_files != expected_signed_files {
        return Err(BundleFailure::new(
            "integrity_exact_set_mismatch",
            "Bundle files or content differ from the signed integrity inventory",
        ));
    }
    let executable = source.join(&manifest.executable);
    let executable_metadata = fs::symlink_metadata(&executable)
        .map_err(|error| BundleFailure::new("executable_missing", error.to_string()))?;
    if !executable_metadata.is_file()
        || executable_metadata.file_type().is_symlink()
        || executable_metadata.mode() & 0o111 == 0
    {
        return Err(BundleFailure::new(
            "executable_invalid",
            "manifest executable must be a real executable file",
        ));
    }
    if let Some(web_root) = &manifest.web_root {
        let web = source.join(web_root);
        let web_metadata = fs::symlink_metadata(&web)
            .map_err(|error| BundleFailure::new("web_root_missing", error.to_string()))?;
        if !web_metadata.is_dir() || web_metadata.file_type().is_symlink() {
            return Err(BundleFailure::new(
                "web_root_invalid",
                "manifest web_root must be a real directory",
            ));
        }
    }
    let signature = URL_SAFE_NO_PAD
        .decode(&manifest.signature.signature)
        .map_err(|_| BundleFailure::new("signature_encoding_invalid", "invalid base64url"))?;
    Ok(ValidatedBundle {
        source,
        app_id: manifest.app_id.0,
        required,
        input_index,
        artifact_tree_digest: snapshot.digest,
        manifest_digest: manifest.integrity.manifest_digest.0,
        signature_digest: digest_bytes(&signature),
    })
}

fn verify_signature(manifest: &AppManifestV1, trust: &TrustStore) -> Result<(), BundleFailure> {
    if manifest.signature.algorithm != SignatureAlgorithmV1::Ed25519 {
        return Err(BundleFailure::new(
            "signature_algorithm_unsupported",
            "only Ed25519 Bundle signatures are supported",
        ));
    }
    if manifest.signature.signed_digest != manifest.integrity.manifest_digest {
        return Err(BundleFailure::new(
            "signature_digest_mismatch",
            "signature digest differs from canonical manifest digest",
        ));
    }
    if manifest
        .signature
        .expires_unix_ms
        .is_some_and(|expires| expires <= now_unix_ms())
    {
        return Err(BundleFailure::new(
            "signature_expired",
            "Bundle signature is expired",
        ));
    }
    let key = trust.keys.get(&manifest.signature.key_id).ok_or_else(|| {
        BundleFailure::new(
            "signature_key_untrusted",
            "signing key is absent from trust store",
        )
    })?;
    if key.revoked {
        return Err(BundleFailure::new(
            "signature_key_revoked",
            "signing key is revoked",
        ));
    }
    let signature = URL_SAFE_NO_PAD
        .decode(&manifest.signature.signature)
        .map_err(|_| BundleFailure::new("signature_encoding_invalid", "invalid base64url"))?;
    UnparsedPublicKey::new(&ED25519, &key.public_key)
        .verify(manifest.signature.signed_digest.0.as_bytes(), &signature)
        .map_err(|_| BundleFailure::new("signature_invalid", "Ed25519 verification failed"))
}

fn canonical_bundle_root(path: &Path) -> Result<PathBuf, BundleFailure> {
    let before = fs::symlink_metadata(path)
        .map_err(|error| BundleFailure::new("bundle_unreadable", error.to_string()))?;
    if before.file_type().is_symlink() || !before.is_dir() {
        return Err(BundleFailure::new(
            "bundle_root_invalid",
            "Bundle root must be a real directory",
        ));
    }
    fs::canonicalize(path)
        .map_err(|error| BundleFailure::new("bundle_unreadable", error.to_string()))
}

fn require_immutable_bundle_tree(root: &Path, expected_uid: u32) -> Result<(), BundleFailure> {
    let mut stack = vec![root.to_path_buf()];
    let mut nodes = 0usize;
    let mut inodes = BTreeSet::new();
    while let Some(path) = stack.pop() {
        nodes += 1;
        if nodes > MAX_ARTIFACT_NODES {
            return Err(BundleFailure::new(
                "bundle_capacity_exceeded",
                "Bundle contains too many filesystem nodes",
            ));
        }
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| BundleFailure::new("bundle_unreadable", error.to_string()))?;
        if metadata.file_type().is_symlink()
            || metadata.uid() != expected_uid
            || metadata.mode() & 0o222 != 0
        {
            return Err(BundleFailure::new(
                "bundle_tree_mutable_or_unsafe",
                format!("unsafe Bundle node {}", path.display()),
            ));
        }
        if metadata.is_dir() {
            for entry in fs::read_dir(&path)
                .map_err(|error| BundleFailure::new("bundle_unreadable", error.to_string()))?
            {
                stack.push(
                    entry
                        .map_err(|error| {
                            BundleFailure::new("bundle_unreadable", error.to_string())
                        })?
                        .path(),
                );
            }
        } else if !metadata.is_file()
            || metadata.nlink() != 1
            || !inodes.insert((metadata.dev(), metadata.ino()))
        {
            return Err(BundleFailure::new(
                "bundle_tree_special_or_linked",
                format!("special or multiply linked Bundle node {}", path.display()),
            ));
        }
    }
    Ok(())
}

fn load_trust_store(path: &Path) -> Result<TrustStore, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect trust store {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.mode() & 0o777 != 0o600
        || metadata.len() > MAX_TRUST_STORE_BYTES
    {
        return Err("trust store must be a real owner-only 0600 file no larger than 1 MiB".into());
    }
    let decoded: TrustStoreFileV1 = serde_json::from_slice(
        &fs::read(path).map_err(|error| format!("read trust store: {error}"))?,
    )
    .map_err(|error| format!("invalid trust store JSON: {error}"))?;
    if decoded.schema_version != 1 {
        return Err("trust store schema_version must equal 1".into());
    }
    let mut keys = BTreeMap::new();
    for key in decoded.keys {
        if key.key_id.trim().is_empty() || keys.contains_key(&key.key_id) {
            return Err("trust store key ids must be non-empty and unique".into());
        }
        let public_key = URL_SAFE_NO_PAD
            .decode(&key.public_key_base64url)
            .map_err(|_| "trust store public keys must be base64url".to_owned())?;
        if public_key.len() != 32 || URL_SAFE_NO_PAD.encode(&public_key) != key.public_key_base64url
        {
            return Err("trust store public keys must be canonical 32-byte Ed25519 keys".into());
        }
        keys.insert(
            key.key_id,
            TrustKey {
                public_key,
                revoked: key.revoked,
            },
        );
    }
    Ok(TrustStore {
        expected_uid: metadata.uid(),
        keys,
    })
}

fn canonical_input(path: &Path, label: &str) -> Result<PathBuf, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {label} {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
        return Err(format!("{label} must be a real file or directory"));
    }
    fs::canonicalize(path).map_err(|error| format!("canonicalize {label}: {error}"))
}

fn reject_recursive_input(source: &Path, output: &Path, label: &str) -> Result<(), String> {
    if source.is_dir() && output.starts_with(source) {
        return Err(format!(
            "assembler output must not be nested in the {label}"
        ));
    }
    Ok(())
}

fn copy_artifact(source: &Path, destination: &Path) -> Result<(), String> {
    fs::create_dir(destination).map_err(|error| {
        format!(
            "create artifact directory {}: {error}",
            destination.display()
        )
    })?;
    fs::set_permissions(destination, fs::Permissions::from_mode(0o700))
        .map_err(|error| format!("secure artifact directory: {error}"))?;
    let metadata = fs::symlink_metadata(source)
        .map_err(|error| format!("inspect artifact {}: {error}", source.display()))?;
    let mut inodes = BTreeSet::new();
    let mut nodes = 0usize;
    if metadata.is_file() {
        copy_file(
            source,
            &destination.join("artifact"),
            &mut inodes,
            &mut nodes,
        )?;
    } else if metadata.is_dir() && !metadata.file_type().is_symlink() {
        copy_directory_contents(source, destination, &mut inodes, &mut nodes)?;
    } else {
        return Err(format!("unsafe artifact root {}", source.display()));
    }
    if nodes == 0 {
        return Err(format!("artifact {} is empty", source.display()));
    }
    Ok(())
}

fn copy_directory_contents(
    source: &Path,
    destination: &Path,
    inodes: &mut BTreeSet<(u64, u64)>,
    nodes: &mut usize,
) -> Result<(), String> {
    let mut entries = fs::read_dir(source)
        .map_err(|error| format!("read artifact directory {}: {error}", source.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read artifact entry: {error}"))?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source_path)
            .map_err(|error| format!("inspect artifact node: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "artifact symlink rejected: {}",
                source_path.display()
            ));
        }
        if metadata.is_dir() {
            *nodes += 1;
            if *nodes > MAX_ARTIFACT_NODES {
                return Err("artifact contains too many filesystem nodes".to_owned());
            }
            fs::create_dir(&destination_path)
                .map_err(|error| format!("create artifact subdirectory: {error}"))?;
            fs::set_permissions(&destination_path, fs::Permissions::from_mode(0o700))
                .map_err(|error| format!("secure artifact subdirectory: {error}"))?;
            copy_directory_contents(&source_path, &destination_path, inodes, nodes)?;
        } else if metadata.is_file() {
            copy_file(&source_path, &destination_path, inodes, nodes)?;
        } else {
            return Err(format!(
                "special artifact node rejected: {}",
                source_path.display()
            ));
        }
    }
    Ok(())
}

fn copy_file(
    source: &Path,
    destination: &Path,
    inodes: &mut BTreeSet<(u64, u64)>,
    nodes: &mut usize,
) -> Result<(), String> {
    let metadata =
        fs::symlink_metadata(source).map_err(|error| format!("inspect artifact file: {error}"))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.nlink() != 1
        || !inodes.insert((metadata.dev(), metadata.ino()))
    {
        return Err(format!(
            "unsafe or multiply linked artifact file {}",
            source.display()
        ));
    }
    *nodes += 1;
    if *nodes > MAX_ARTIFACT_NODES {
        return Err("artifact contains too many filesystem nodes".to_owned());
    }
    let mut input = File::open(source)
        .map_err(|error| format!("open artifact file {}: {error}", source.display()))?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .map_err(|error| format!("create artifact file {}: {error}", destination.display()))?;
    std::io::copy(&mut input, &mut output)
        .map_err(|error| format!("copy artifact file {}: {error}", source.display()))?;
    let mode = if metadata.mode() & 0o111 != 0 {
        0o555
    } else {
        0o444
    };
    output
        .set_permissions(fs::Permissions::from_mode(mode))
        .map_err(|error| format!("seal artifact file: {error}"))?;
    output
        .sync_all()
        .map_err(|error| format!("sync artifact file: {error}"))
}

fn snapshot_tree(root: &Path) -> Result<TreeSnapshot, String> {
    let metadata = fs::symlink_metadata(root)
        .map_err(|error| format!("inspect artifact tree {}: {error}", root.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("artifact tree root must be a real directory".to_owned());
    }
    let mut nodes = Vec::new();
    let mut inodes = BTreeSet::new();
    collect_tree(root, root, &mut nodes, &mut inodes)?;
    nodes.sort_by(|left, right| left.relative.cmp(&right.relative));
    if nodes.is_empty() {
        return Err("artifact tree is empty".to_owned());
    }
    let mut digest = Sha256::new();
    digest.update(TREE_DIGEST_DOMAIN);
    let mut files = BTreeMap::new();
    for node in nodes {
        digest.update((node.relative.len() as u64).to_be_bytes());
        digest.update(node.relative.as_bytes());
        digest.update([match node.kind {
            TreeNodeKind::Directory => b'd',
            TreeNodeKind::File => b'f',
        }]);
        digest.update([u8::from(node.executable)]);
        if let Some(file_digest) = node.digest {
            digest.update((file_digest.len() as u64).to_be_bytes());
            digest.update(file_digest.as_bytes());
            files.insert(node.relative, file_digest);
        } else {
            digest.update(0_u64.to_be_bytes());
        }
    }
    Ok(TreeSnapshot {
        digest: format!("sha256:{:x}", digest.finalize()),
        files,
    })
}

fn collect_tree(
    root: &Path,
    directory: &Path,
    nodes: &mut Vec<TreeNode>,
    inodes: &mut BTreeSet<(u64, u64)>,
) -> Result<(), String> {
    let mut entries = fs::read_dir(directory)
        .map_err(|error| format!("read artifact tree {}: {error}", directory.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read artifact tree entry: {error}"))?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        if nodes.len() >= MAX_ARTIFACT_NODES {
            return Err("artifact tree contains too many nodes".to_owned());
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("inspect artifact tree node: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "artifact tree symlink rejected: {}",
                path.display()
            ));
        }
        let relative = relative_utf8(root, &path)?;
        if metadata.is_dir() {
            nodes.push(TreeNode {
                relative,
                kind: TreeNodeKind::Directory,
                executable: false,
                digest: None,
            });
            collect_tree(root, &path, nodes, inodes)?;
        } else if metadata.is_file()
            && metadata.nlink() == 1
            && inodes.insert((metadata.dev(), metadata.ino()))
        {
            let file_digest = digest_file(&path)?;
            nodes.push(TreeNode {
                relative,
                kind: TreeNodeKind::File,
                executable: metadata.mode() & 0o111 != 0,
                digest: Some(file_digest),
            });
        } else {
            return Err(format!(
                "special or multiply linked artifact node rejected: {}",
                path.display()
            ));
        }
    }
    Ok(())
}

fn relative_utf8(root: &Path, path: &Path) -> Result<String, String> {
    path.strip_prefix(root)
        .map_err(|_| "artifact path escaped its root".to_owned())?
        .to_str()
        .map(|value| value.replace('\\', "/"))
        .ok_or_else(|| "artifact paths must be UTF-8".to_owned())
}

fn digest_file(path: &Path) -> Result<String, String> {
    let mut file = File::open(path)
        .map_err(|error| format!("open digest input {}: {error}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("read digest input {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn validate_sha256_digest(value: &str, label: &str) -> Result<(), String> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(format!("{label} must use the sha256: prefix"));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{label} must contain 64 lowercase hexadecimal characters"
        ));
    }
    Ok(())
}

fn validate_generation(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || value == "."
        || value == ".."
        || value.starts_with('.')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err("generation must be a non-hidden portable path component".to_owned());
    }
    Ok(())
}

fn write_sync(path: &Path, bytes: &[u8], mode: u32) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| format!("create {}: {error}", path.display()))?;
    file.write_all(bytes)
        .map_err(|error| format!("write {}: {error}", path.display()))?;
    file.set_permissions(fs::Permissions::from_mode(mode))
        .map_err(|error| format!("set permissions on {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync {}: {error}", path.display()))
}

fn seal_tree(root: &Path) -> Result<(), String> {
    let mut directories = vec![root.to_path_buf()];
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in
            fs::read_dir(&directory).map_err(|error| format!("read tree while sealing: {error}"))?
        {
            let path = entry
                .map_err(|error| format!("read tree entry while sealing: {error}"))?
                .path();
            let metadata = fs::symlink_metadata(&path)
                .map_err(|error| format!("inspect tree while sealing: {error}"))?;
            if metadata.file_type().is_symlink() {
                return Err(format!("cannot seal symlink {}", path.display()));
            }
            if metadata.is_dir() {
                directories.push(path.clone());
                stack.push(path);
            } else if metadata.is_file() {
                let mode = if metadata.mode() & 0o111 != 0 {
                    0o555
                } else {
                    0o444
                };
                fs::set_permissions(&path, fs::Permissions::from_mode(mode))
                    .map_err(|error| format!("seal file {}: {error}", path.display()))?;
            } else {
                return Err(format!("cannot seal special node {}", path.display()));
            }
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o555))
            .map_err(|error| format!("seal directory {}: {error}", directory.display()))?;
    }
    Ok(())
}

fn sync_tree(root: &Path) -> Result<(), String> {
    let mut directories = vec![root.to_path_buf()];
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in
            fs::read_dir(&directory).map_err(|error| format!("read tree while syncing: {error}"))?
        {
            let path = entry
                .map_err(|error| format!("read tree entry while syncing: {error}"))?
                .path();
            let metadata = fs::symlink_metadata(&path)
                .map_err(|error| format!("inspect tree while syncing: {error}"))?;
            if metadata.is_dir() {
                directories.push(path.clone());
                stack.push(path);
            } else if metadata.is_file() {
                File::open(&path)
                    .and_then(|file| file.sync_all())
                    .map_err(|error| format!("sync file {}: {error}", path.display()))?;
            }
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        File::open(&directory)
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("sync directory {}: {error}", directory.display()))?;
    }
    Ok(())
}

fn make_tree_writable(root: &Path) {
    let _ = fs::set_permissions(root, fs::Permissions::from_mode(0o700));
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            make_tree_writable(&path);
        } else {
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
        }
    }
}

fn peek_app_id(bundle: &Path) -> Option<String> {
    let bytes = fs::read(bundle.join(APP_MANIFEST_FILE)).ok()?;
    serde_json::from_slice::<AppManifestV1>(&bytes)
        .ok()
        .map(|manifest| manifest.app_id.0)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

struct StagingGuard {
    path: PathBuf,
    cleanup: bool,
}

impl StagingGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            cleanup: true,
        }
    }

    fn disarm(&mut self) {
        self.cleanup = false;
    }

    #[cfg(test)]
    fn preserve(&mut self) {
        self.cleanup = false;
    }
}

impl Drop for StagingGuard {
    fn drop(&mut self) {
        if self.cleanup && self.path.exists() {
            make_tree_writable(&self.path);
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowd_app_protocol::{
        AppId, AppSurfacesV1, AuthorizationProfileV1, BundleIntegrityV1, BundleSignatureV1,
        FilesystemPolicyV1, IntegrityAlgorithmV1, NetworkPolicyV1, ProtocolRangeV1,
        SandboxProfileV1, Sha256Digest,
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};

    const KEY_ID: &str = "assembler-test-key-v1";
    const SEED: [u8; 32] = [7_u8; 32];
    const PROTOCOL_DIGEST: &str =
        "sha256:2c61ceb144b819e88060a3f813585ebe3d064ad2acf8888104719fb82363c766";

    struct Fixture {
        _temporary: tempfile::TempDir,
        root: PathBuf,
        core: PathBuf,
        edge: PathBuf,
        trust: PathBuf,
        output: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let temporary = tempfile::tempdir().expect("temporary assembler fixture");
            let root = temporary.path().to_path_buf();
            let core = root.join("core-artifact");
            let edge = root.join("edge-artifact");
            fs::create_dir(&core).expect("core artifact directory");
            fs::create_dir(&edge).expect("edge artifact directory");
            fs::write(core.join("cowd"), b"core-binary").expect("core artifact");
            fs::set_permissions(core.join("cowd"), fs::Permissions::from_mode(0o755))
                .expect("core executable");
            fs::write(edge.join("index.html"), b"edge-ui").expect("edge artifact");
            let trust = root.join("trust.json");
            let key = Ed25519KeyPair::from_seed_unchecked(&SEED).expect("test signing key");
            fs::write(
                &trust,
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "keys": [{
                        "key_id": KEY_ID,
                        "public_key_base64url": URL_SAFE_NO_PAD.encode(key.public_key().as_ref()),
                        "revoked": false
                    }]
                }))
                .expect("trust JSON"),
            )
            .expect("trust store");
            fs::set_permissions(&trust, fs::Permissions::from_mode(0o600)).expect("trust mode");
            let output = root.join("generations");
            Self {
                _temporary: temporary,
                root,
                core,
                edge,
                trust,
                output,
            }
        }

        fn bundle(&self, app_id: &str) -> PathBuf {
            let path = self.root.join(format!("bundle-{app_id}"));
            create_signed_bundle(&path, app_id);
            path
        }

        fn request(&self, generation: &str, apps: Vec<AppInput>) -> AssembleRequest {
            AssembleRequest {
                core: self.core.clone(),
                edge: self.edge.clone(),
                apps,
                trust_store: self.trust.clone(),
                protocol_digest: PROTOCOL_DIGEST.to_owned(),
                generation: generation.to_owned(),
                output: self.output.clone(),
            }
        }
    }

    #[test]
    fn assembles_zero_apps_as_one_fresh_generation() {
        let fixture = Fixture::new();
        let report = assemble(fixture.request("zero-apps", Vec::new())).expect("assemble");
        assert_eq!(report.destination, fixture.output.join("zero-apps"));
        let lock = read_lock(&report.destination);
        assert!(lock.apps.is_empty());
        assert!(lock.rejected_optional_apps.is_empty());
        assert!(report.destination.join("core/cowd").is_file());
        assert!(report.destination.join("edge/index.html").is_file());
        assert!(!fixture.output.join(".zero-apps.staging").exists());
    }

    #[test]
    fn assembles_one_and_many_required_or_optional_apps() {
        let fixture = Fixture::new();
        let one = fixture.bundle("alpha");
        let one_report = assemble(fixture.request(
            "one-app",
            vec![AppInput {
                bundle: one,
                required: true,
                input_index: 0,
            }],
        ))
        .expect("one APP generation");
        assert_eq!(read_lock(&one_report.destination).apps.len(), 1);

        let alpha = fixture.bundle("many-alpha");
        let beta = fixture.bundle("many-beta");
        let gamma = fixture.bundle("many-gamma");
        let many_report = assemble(fixture.request(
            "many-apps",
            vec![
                app_input(alpha, true, 0),
                app_input(beta, false, 1),
                app_input(gamma, true, 2),
            ],
        ))
        .expect("many APP generation");
        let lock = read_lock(&many_report.destination);
        assert_eq!(lock.apps.len(), 3);
        assert_eq!(
            lock.apps
                .iter()
                .map(|app| app.app_id.as_str())
                .collect::<Vec<_>>(),
            vec!["many-alpha", "many-beta", "many-gamma"]
        );
        assert!(lock.apps[0].required);
        assert!(!lock.apps[1].required);
    }

    #[test]
    fn required_tamper_or_signature_failure_publishes_nothing() {
        for signature_tamper in [false, true] {
            let fixture = Fixture::new();
            let bundle = fixture.bundle(if signature_tamper {
                "bad-signature"
            } else {
                "bad-content"
            });
            if signature_tamper {
                tamper_signature(&bundle);
            } else {
                tamper_file(&bundle.join("bin/worker"), b"tampered-worker");
            }
            let result =
                assemble(fixture.request("required-failure", vec![app_input(bundle, true, 0)]));
            assert!(result.is_err());
            assert!(!fixture.output.join("required-failure").exists());
            assert!(!fixture.output.join(".required-failure.staging").exists());
        }
    }

    #[test]
    fn manifest_protocol_executable_and_web_root_fail_closed() {
        let fixture = Fixture::new();
        let manifest_tamper = fixture.bundle("bad-manifest-digest");
        tamper_manifest_field(&manifest_tamper, "display_name", "tampered display name");
        let error = assemble(fixture.request(
            "manifest-digest-failure",
            vec![app_input(manifest_tamper, true, 0)],
        ))
        .expect_err("canonical manifest digest tamper must fail");
        assert!(error.contains("manifest_contract_invalid"));
        assert!(!fixture.output.join("manifest-digest-failure").exists());

        let fixture = Fixture::new();
        let protocol_mismatch = fixture.bundle("bad-protocol-digest");
        let mut request = fixture.request(
            "protocol-digest-failure",
            vec![app_input(protocol_mismatch, true, 0)],
        );
        request.protocol_digest = digest_bytes(b"different protocol artifact");
        let error = assemble(request).expect_err("protocol digest mismatch must fail");
        assert!(error.contains("protocol_digest_mismatch"));
        assert!(!fixture.output.join("protocol-digest-failure").exists());

        let fixture = Fixture::new();
        let non_executable = fixture.bundle("bad-executable");
        fs::set_permissions(
            non_executable.join("bin/worker"),
            fs::Permissions::from_mode(0o444),
        )
        .expect("remove executable mode");
        let error = assemble(fixture.request(
            "executable-failure",
            vec![app_input(non_executable, true, 0)],
        ))
        .expect_err("non-executable entrypoint must fail");
        assert!(error.contains("executable_invalid"));
        assert!(!fixture.output.join("executable-failure").exists());

        let fixture = Fixture::new();
        let invalid_web_root = fixture.bundle("bad-web-root");
        resign_manifest(&invalid_web_root, |manifest| {
            manifest.web_root = Some("bin/worker".to_owned());
        });
        let error = assemble(fixture.request(
            "web-root-failure",
            vec![app_input(invalid_web_root, true, 0)],
        ))
        .expect_err("non-directory web root must fail");
        assert!(error.contains("web_root_invalid"));
        assert!(!fixture.output.join("web-root-failure").exists());
    }

    #[test]
    fn optional_failure_is_recorded_without_a_partial_bundle() {
        let fixture = Fixture::new();
        let bundle = fixture.bundle("optional-bad");
        tamper_file(&bundle.join("webui/index.html"), b"tampered-webui");
        let report =
            assemble(fixture.request("optional-failure", vec![app_input(bundle, false, 0)]))
                .expect("optional rejection still publishes");
        let lock = read_lock(&report.destination);
        assert!(lock.apps.is_empty());
        assert_eq!(lock.rejected_optional_apps.len(), 1);
        assert_eq!(
            lock.rejected_optional_apps[0].reason_code,
            "integrity_exact_set_mismatch"
        );
        assert!(!report.destination.join("apps/optional-bad").exists());
    }

    #[test]
    fn assembly_is_deterministic_and_release_lock_has_no_source_coupling() {
        let fixture = Fixture::new();
        let alpha = fixture.bundle("deterministic-alpha");
        let beta = fixture.bundle("deterministic-beta");
        let first_output = fixture.root.join("first-output");
        let second_output = fixture.root.join("second-output");
        let mut first = fixture.request(
            "deterministic",
            vec![
                app_input(beta.clone(), false, 1),
                app_input(alpha.clone(), true, 0),
            ],
        );
        first.output = first_output;
        let mut second = fixture.request(
            "deterministic",
            vec![app_input(beta, false, 1), app_input(alpha, true, 0)],
        );
        second.output = second_output;
        let first = assemble(first).expect("first deterministic assembly");
        let second = assemble(second).expect("second deterministic assembly");
        let first_bytes = fs::read(first.destination.join(RELEASE_LOCK_FILE)).expect("first lock");
        let second_bytes =
            fs::read(second.destination.join(RELEASE_LOCK_FILE)).expect("second lock");
        assert_eq!(first_bytes, second_bytes);
        assert_eq!(
            snapshot_tree(&first.destination)
                .expect("first tree")
                .digest,
            snapshot_tree(&second.destination)
                .expect("second tree")
                .digest
        );
        let text = String::from_utf8(first_bytes).expect("UTF-8 release lock");
        assert!(!text.contains("git"));
        assert!(!text.contains(&fixture.root.display().to_string()));
        assert!(!text.contains("artifact_version"));
    }

    #[test]
    fn crash_before_rename_preserves_old_generation() {
        let fixture = Fixture::new();
        fs::create_dir_all(fixture.output.join("old")).expect("old generation");
        fs::write(fixture.output.join("old/sentinel"), b"old-generation")
            .expect("old generation sentinel");
        let bundle = fixture.bundle("crash-fixture");
        let result = assemble_with_fault(
            fixture.request("new", vec![app_input(bundle, true, 0)]),
            PublishFault::BeforeRename,
        );
        assert!(result.is_err());
        assert_eq!(
            fs::read(fixture.output.join("old/sentinel")).expect("old sentinel"),
            b"old-generation"
        );
        assert!(!fixture.output.join("new").exists());
        let staging = fixture.output.join(".new.staging");
        assert!(staging.is_dir());
        make_tree_writable(&staging);
        fs::remove_dir_all(staging).expect("remove simulated crash residue");
    }

    fn app_input(bundle: PathBuf, required: bool, input_index: usize) -> AppInput {
        AppInput {
            bundle,
            required,
            input_index,
        }
    }

    fn read_lock(generation: &Path) -> ReleaseLockV1 {
        serde_json::from_slice(
            &fs::read(generation.join(RELEASE_LOCK_FILE)).expect("release lock bytes"),
        )
        .expect("release lock contract")
    }

    fn create_signed_bundle(root: &Path, app_id: &str) {
        fs::create_dir_all(root.join("bin")).expect("bundle bin");
        fs::create_dir_all(root.join("webui")).expect("bundle webui");
        fs::write(root.join("bin/worker"), format!("worker:{app_id}")).expect("bundle worker");
        fs::set_permissions(root.join("bin/worker"), fs::Permissions::from_mode(0o555))
            .expect("worker executable");
        fs::write(root.join("webui/index.html"), format!("web:{app_id}")).expect("bundle webui");
        let files = BTreeMap::from([
            (
                "bin/worker".to_owned(),
                Sha256Digest(digest_file(&root.join("bin/worker")).expect("worker digest")),
            ),
            (
                "webui/index.html".to_owned(),
                Sha256Digest(digest_file(&root.join("webui/index.html")).expect("web digest")),
            ),
        ]);
        let placeholder = Sha256Digest(digest_bytes(b"placeholder"));
        let mut manifest = AppManifestV1 {
            schema_version: 1,
            app_id: AppId(app_id.to_owned()),
            display_name: format!("Fixture {app_id}"),
            artifact_version: "1.0.0".to_owned(),
            required_protocol: ProtocolRangeV1::exact_v1(),
            executable: "bin/worker".to_owned(),
            web_root: Some("webui".to_owned()),
            capabilities: Vec::new(),
            authorization_profiles: Vec::<AuthorizationProfileV1>::new(),
            operation_catalog_digest: Sha256Digest(digest_bytes(app_id.as_bytes())),
            core_bridge_requirements: Vec::new(),
            surfaces: AppSurfacesV1 {
                web: true,
                tui_view: false,
            },
            integrity: BundleIntegrityV1 {
                algorithm: IntegrityAlgorithmV1::Sha256,
                files,
                manifest_digest: placeholder.clone(),
            },
            signature: BundleSignatureV1 {
                algorithm: SignatureAlgorithmV1::Ed25519,
                key_id: KEY_ID.to_owned(),
                signature: "pending".to_owned(),
                signed_digest: placeholder,
                expires_unix_ms: None,
                provenance_digest: Some(Sha256Digest(PROTOCOL_DIGEST.to_owned())),
            },
            sandbox: SandboxProfileV1 {
                filesystem: FilesystemPolicyV1::BundleReadOnlyDataReadWrite,
                network: NetworkPolicyV1::Deny,
                max_processes: 4,
                max_open_files: 64,
                max_memory_bytes: 64 * 1024 * 1024,
                cpu_quota_millis_per_second: 500,
            },
            presentation: None,
        };
        let digest = manifest
            .bind_canonical_signed_digest()
            .expect("canonical manifest digest");
        let key = Ed25519KeyPair::from_seed_unchecked(&SEED).expect("fixture signing key");
        manifest.signature.signature =
            URL_SAFE_NO_PAD.encode(key.sign(digest.0.as_bytes()).as_ref());
        manifest.validate().expect("valid fixture manifest");
        fs::write(
            root.join(APP_MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).expect("manifest JSON"),
        )
        .expect("manifest file");
        seal_tree(root).expect("seal bundle");
    }

    fn tamper_file(path: &Path, content: &[u8]) {
        fs::set_permissions(path, fs::Permissions::from_mode(0o644)).expect("unseal fixture file");
        fs::write(path, content).expect("tamper fixture file");
        fs::set_permissions(path, fs::Permissions::from_mode(0o444)).expect("reseal fixture file");
    }

    fn tamper_signature(root: &Path) {
        let path = root.join(APP_MANIFEST_FILE);
        let mut document: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("manifest bytes")).expect("manifest");
        document["signature"]["signature"] =
            serde_json::Value::String(URL_SAFE_NO_PAD.encode([0_u8; 64]));
        tamper_file(
            &path,
            &serde_json::to_vec_pretty(&document).expect("tampered manifest"),
        );
    }

    fn tamper_manifest_field(root: &Path, field: &str, value: &str) {
        let path = root.join(APP_MANIFEST_FILE);
        let mut document: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("manifest bytes")).expect("manifest");
        document[field] = serde_json::Value::String(value.to_owned());
        tamper_file(
            &path,
            &serde_json::to_vec_pretty(&document).expect("tampered manifest"),
        );
    }

    fn resign_manifest(root: &Path, update: impl FnOnce(&mut AppManifestV1)) {
        let path = root.join(APP_MANIFEST_FILE);
        let mut manifest: AppManifestV1 =
            serde_json::from_slice(&fs::read(&path).expect("manifest bytes")).expect("manifest");
        update(&mut manifest);
        let digest = manifest
            .bind_canonical_signed_digest()
            .expect("rebind canonical manifest digest");
        let key = Ed25519KeyPair::from_seed_unchecked(&SEED).expect("fixture signing key");
        manifest.signature.signature =
            URL_SAFE_NO_PAD.encode(key.sign(digest.0.as_bytes()).as_ref());
        tamper_file(
            &path,
            &serde_json::to_vec_pretty(&manifest).expect("resigned manifest"),
        );
    }
}
