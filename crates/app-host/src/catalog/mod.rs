use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use cowd_app_protocol::{
    AppActivationPolicyV1, AppCatalogEntryV1, AppCompatibilityStatusV1, AppCompatibilityV1, AppId,
    AppLifecycleStateV1, AppLifecycleV1, AppManifestV1, AppWebSurfaceV1, GenerationId,
    ProtocolValidate, Sha256Digest, PROTOCOL_REVISION_V1,
};
use ring::signature::{UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const MANIFEST_FILE: &str = "app.json";
const MAX_BUNDLES_PER_ROOT: usize = 1_024;
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppCatalogSnapshot {
    generation: Sha256Digest,
    accepted: Arc<BTreeMap<AppId, AdmittedApp>>,
    diagnostics: Arc<Vec<AppCatalogDiagnostic>>,
}

impl AppCatalogSnapshot {
    #[must_use]
    pub fn generation(&self) -> &Sha256Digest {
        &self.generation
    }

    #[must_use]
    pub fn get(&self, app_id: &AppId) -> Option<&AdmittedApp> {
        self.accepted.get(app_id)
    }

    pub fn apps(&self) -> impl ExactSizeIterator<Item = &AdmittedApp> {
        self.accepted.values()
    }

    #[must_use]
    pub fn diagnostics(&self) -> &[AppCatalogDiagnostic] {
        &self.diagnostics
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedApp {
    pub manifest: AppManifestV1,
    pub bundle_root: PathBuf,
    pub executable: PathBuf,
    pub web_root: Option<PathBuf>,
    pub generation: GenerationId,
    pub policy: EffectiveAppPolicy,
}

impl AdmittedApp {
    #[must_use]
    pub fn catalog_entry(&self) -> AppCatalogEntryV1 {
        let profile = self
            .manifest
            .authorization_profiles
            .iter()
            .find(|profile| profile.is_default)
            .or_else(|| self.manifest.authorization_profiles.first());
        AppCatalogEntryV1 {
            app_id: self.manifest.app_id.clone(),
            display_name: self.manifest.display_name.clone(),
            artifact_version: self.manifest.artifact_version.clone(),
            generation: self.generation.clone(),
            required: self.policy.required,
            activation: self.policy.activation,
            lifecycle: AppLifecycleV1 {
                state: AppLifecycleStateV1::Mounted,
                reason_code: None,
                retryable: false,
                retry_after_ms: None,
            },
            compatibility: AppCompatibilityV1 {
                status: AppCompatibilityStatusV1::Compatible,
                gateway_supported_minimum: PROTOCOL_REVISION_V1,
                gateway_supported_maximum: PROTOCOL_REVISION_V1,
                app_required_minimum: self.manifest.required_protocol.minimum,
                app_required_maximum: self.manifest.required_protocol.maximum,
            },
            web_surface: AppWebSurfaceV1 {
                available: self.manifest.surfaces.web,
                entry_path: self
                    .manifest
                    .surfaces
                    .web
                    .then(|| format!("/apps/{}/index.html", self.manifest.app_id)),
                bridge_revision: PROTOCOL_REVISION_V1,
            },
            effective_capabilities: profile
                .map(|profile| profile.capabilities.clone())
                .unwrap_or_default(),
            effective_authorization_profile: profile
                .map(|profile| profile.profile_id.clone())
                .unwrap_or_else(|| "none".to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveAppPolicy {
    pub enabled: bool,
    pub required: bool,
    pub activation: AppActivationPolicyV1,
    pub config_file: Option<PathBuf>,
}

impl Default for EffectiveAppPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            required: false,
            activation: AppActivationPolicyV1::Lazy,
            config_file: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppCatalogPolicy {
    pub entries: BTreeMap<AppId, EffectiveAppPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedSigningKey {
    pub key_id: String,
    pub public_key: Vec<u8>,
    pub revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppTrustStore {
    keys: BTreeMap<String, TrustedSigningKey>,
}

impl AppTrustStore {
    #[must_use]
    pub fn new(keys: impl IntoIterator<Item = TrustedSigningKey>) -> Self {
        Self {
            keys: keys
                .into_iter()
                .map(|key| (key.key_id.clone(), key))
                .collect(),
        }
    }

    fn verify(&self, manifest: &AppManifestV1, now_unix_ms: u64) -> Result<(), CatalogRejection> {
        let signature = &manifest.signature;
        let key = self
            .keys
            .get(&signature.key_id)
            .ok_or(CatalogRejection::UntrustedSigningKey)?;
        if key.revoked {
            return Err(CatalogRejection::RevokedSigningKey);
        }
        if signature
            .expires_unix_ms
            .is_some_and(|expires| expires <= now_unix_ms)
        {
            return Err(CatalogRejection::ExpiredSignature);
        }
        if signature.signed_digest != manifest.integrity.manifest_digest {
            return Err(CatalogRejection::SignatureDigestMismatch);
        }
        let encoded = URL_SAFE_NO_PAD
            .decode(&signature.signature)
            .map_err(|_| CatalogRejection::InvalidSignatureEncoding)?;
        UnparsedPublicKey::new(&ED25519, &key.public_key)
            .verify(signature.signed_digest.0.as_bytes(), &encoded)
            .map_err(|_| CatalogRejection::InvalidSignature)
    }
}

#[derive(Debug, Clone)]
pub struct AppCatalogBuilder {
    roots: Vec<PathBuf>,
    policy: AppCatalogPolicy,
    trust: AppTrustStore,
    expected_uid: u32,
    now_unix_ms: u64,
}

impl AppCatalogBuilder {
    #[must_use]
    pub fn new(
        roots: Vec<PathBuf>,
        policy: AppCatalogPolicy,
        trust: AppTrustStore,
        expected_uid: u32,
        now_unix_ms: u64,
    ) -> Self {
        Self {
            roots,
            policy,
            trust,
            expected_uid,
            now_unix_ms,
        }
    }

    pub fn build(&self) -> Result<AppCatalogSnapshot, CatalogBuildError> {
        let mut accepted = BTreeMap::<AppId, AdmittedApp>::new();
        let mut diagnostics = Vec::new();
        for (priority, configured_root) in self.roots.iter().enumerate() {
            let root = match fs::canonicalize(configured_root) {
                Ok(root) => root,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    diagnostics.push(AppCatalogDiagnostic {
                        root: configured_root.clone(),
                        candidate: None,
                        app_id: None,
                        priority,
                        outcome: CatalogCandidateOutcome::RootUnavailable,
                        detail: error.to_string(),
                    });
                    continue;
                }
                Err(error) => {
                    return Err(CatalogBuildError::ReadRoot {
                        path: configured_root.clone(),
                        source: error,
                    })
                }
            };
            let mut candidates = read_candidates(&root)?;
            candidates.sort();
            if candidates.len() > MAX_BUNDLES_PER_ROOT {
                return Err(CatalogBuildError::RootCapacity {
                    path: root,
                    count: candidates.len(),
                    maximum: MAX_BUNDLES_PER_ROOT,
                });
            }
            for candidate in candidates {
                match self.admit(&root, &candidate) {
                    Ok(app) => {
                        let app_id = app.manifest.app_id.clone();
                        if !app.policy.enabled {
                            diagnostics.push(diagnostic(
                                &root,
                                &candidate,
                                Some(app_id),
                                priority,
                                CatalogCandidateOutcome::Disabled,
                                "disabled by Gateway policy",
                            ));
                        } else if accepted.contains_key(&app_id) {
                            diagnostics.push(diagnostic(
                                &root,
                                &candidate,
                                Some(app_id),
                                priority,
                                CatalogCandidateOutcome::Shadowed,
                                "a higher-priority valid Bundle owns this app_id",
                            ));
                        } else {
                            diagnostics.push(diagnostic(
                                &root,
                                &candidate,
                                Some(app_id.clone()),
                                priority,
                                CatalogCandidateOutcome::Admitted,
                                "Bundle admitted",
                            ));
                            accepted.insert(app_id, app);
                        }
                    }
                    Err(rejection) => diagnostics.push(diagnostic(
                        &root,
                        &candidate,
                        rejection.app_id,
                        priority,
                        CatalogCandidateOutcome::Invalid,
                        rejection.reason.to_string(),
                    )),
                }
            }
        }
        let generation = catalog_generation(&accepted)?;
        Ok(AppCatalogSnapshot {
            generation,
            accepted: Arc::new(accepted),
            diagnostics: Arc::new(diagnostics),
        })
    }

    fn admit(&self, root: &Path, candidate: &Path) -> Result<AdmittedApp, CandidateRejection> {
        let canonical = fs::canonicalize(candidate).map_err(CandidateRejection::io)?;
        if !canonical.starts_with(root) {
            return Err(CandidateRejection::new(CatalogRejection::PathEscape));
        }
        require_secure_file(&canonical, self.expected_uid, true)
            .map_err(CandidateRejection::new)?;
        let manifest_path = canonical.join(MANIFEST_FILE);
        require_secure_file(&manifest_path, self.expected_uid, false)
            .map_err(CandidateRejection::new)?;
        let metadata = fs::metadata(&manifest_path).map_err(CandidateRejection::io)?;
        if metadata.len() > MAX_MANIFEST_BYTES {
            return Err(CandidateRejection::new(CatalogRejection::ManifestTooLarge));
        }
        let bytes = fs::read(&manifest_path).map_err(CandidateRejection::io)?;
        let manifest: AppManifestV1 = serde_json::from_slice(&bytes).map_err(|error| {
            CandidateRejection::new(CatalogRejection::Manifest(error.to_string()))
        })?;
        let app_id = manifest.app_id.clone();
        manifest.validate().map_err(|error| {
            CandidateRejection::with_app(
                app_id.clone(),
                CatalogRejection::Manifest(error.to_string()),
            )
        })?;
        if !manifest.required_protocol.minimum.le(&PROTOCOL_REVISION_V1)
            || !manifest.required_protocol.maximum.ge(&PROTOCOL_REVISION_V1)
        {
            return Err(CandidateRejection::with_app(
                app_id,
                CatalogRejection::ProtocolIncompatible,
            ));
        }
        self.trust
            .verify(&manifest, self.now_unix_ms)
            .map_err(|reason| CandidateRejection::with_app(app_id.clone(), reason))?;
        verify_integrity(&canonical, &manifest)
            .map_err(|reason| CandidateRejection::with_app(app_id.clone(), reason))?;
        let executable = canonical.join(&manifest.executable);
        require_secure_file(&executable, self.expected_uid, false)
            .map_err(|reason| CandidateRejection::with_app(app_id.clone(), reason))?;
        if fs::metadata(&executable)
            .map_err(CandidateRejection::io)?
            .mode()
            & 0o111
            == 0
        {
            return Err(CandidateRejection::with_app(
                app_id,
                CatalogRejection::ExecutablePermission,
            ));
        }
        let web_root = manifest.web_root.as_ref().map(|path| canonical.join(path));
        if let Some(path) = &web_root {
            require_secure_file(path, self.expected_uid, true)
                .map_err(|reason| CandidateRejection::with_app(app_id.clone(), reason))?;
        }
        let config_file = self
            .policy
            .entries
            .get(&app_id)
            .and_then(|entry| entry.config_file.clone());
        if let Some(path) = &config_file {
            require_secure_file(path, self.expected_uid, false)
                .map_err(|reason| CandidateRejection::with_app(app_id.clone(), reason))?;
        }
        let generation = bundle_generation(&manifest)?;
        let mut policy = self
            .policy
            .entries
            .get(&app_id)
            .cloned()
            .unwrap_or_default();
        policy.config_file = config_file;
        Ok(AdmittedApp {
            manifest,
            bundle_root: canonical,
            executable,
            web_root,
            generation,
            policy,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppCatalogDiagnostic {
    pub root: PathBuf,
    pub candidate: Option<PathBuf>,
    pub app_id: Option<AppId>,
    pub priority: usize,
    pub outcome: CatalogCandidateOutcome,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogCandidateOutcome {
    Admitted,
    Disabled,
    Shadowed,
    Invalid,
    RootUnavailable,
}

#[derive(Debug, Error)]
pub enum CatalogBuildError {
    #[error("cannot read APP root {path}: {source}")]
    ReadRoot {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("APP root {path} contains {count} candidates; maximum is {maximum}")]
    RootCapacity {
        path: PathBuf,
        count: usize,
        maximum: usize,
    },
    #[error("cannot serialize admitted APP catalog: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CatalogRejection {
    #[error("candidate path escapes configured root")]
    PathEscape,
    #[error("path is not owned by the Gateway uid")]
    WrongOwner,
    #[error("path is writable by group or other users")]
    InsecurePermission,
    #[error("path is not the expected regular file or directory")]
    InvalidFileType,
    #[error("manifest exceeds the 1 MiB limit")]
    ManifestTooLarge,
    #[error("invalid manifest: {0}")]
    Manifest(String),
    #[error("APP protocol range does not include Gateway protocol v1")]
    ProtocolIncompatible,
    #[error("Bundle signing key is not trusted")]
    UntrustedSigningKey,
    #[error("Bundle signing key is revoked")]
    RevokedSigningKey,
    #[error("Bundle signature is expired")]
    ExpiredSignature,
    #[error("Bundle signature digest does not match manifest integrity digest")]
    SignatureDigestMismatch,
    #[error("Bundle signature is not base64url")]
    InvalidSignatureEncoding,
    #[error("Bundle signature verification failed")]
    InvalidSignature,
    #[error("Bundle integrity path is invalid")]
    IntegrityPath,
    #[error("Bundle integrity mismatch for {0}")]
    IntegrityMismatch(String),
    #[error("Bundle executable is not executable")]
    ExecutablePermission,
    #[error("I/O failure: {0}")]
    Io(String),
}

#[derive(Debug)]
struct CandidateRejection {
    app_id: Option<AppId>,
    reason: CatalogRejection,
}

impl CandidateRejection {
    fn new(reason: CatalogRejection) -> Self {
        Self {
            app_id: None,
            reason,
        }
    }

    fn with_app(app_id: AppId, reason: CatalogRejection) -> Self {
        Self {
            app_id: Some(app_id),
            reason,
        }
    }

    fn io(error: std::io::Error) -> Self {
        Self::new(CatalogRejection::Io(error.to_string()))
    }
}

fn read_candidates(root: &Path) -> Result<Vec<PathBuf>, CatalogBuildError> {
    fs::read_dir(root)
        .map_err(|source| CatalogBuildError::ReadRoot {
            path: root.to_path_buf(),
            source,
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|source| CatalogBuildError::ReadRoot {
                    path: root.to_path_buf(),
                    source,
                })
        })
        .collect()
}

fn require_secure_file(
    path: &Path,
    expected_uid: u32,
    directory: bool,
) -> Result<(), CatalogRejection> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| CatalogRejection::Io(error.to_string()))?;
    if metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        return Err(CatalogRejection::InvalidFileType);
    }
    if metadata.uid() != expected_uid {
        return Err(CatalogRejection::WrongOwner);
    }
    if metadata.mode() & 0o022 != 0 {
        return Err(CatalogRejection::InsecurePermission);
    }
    Ok(())
}

fn verify_integrity(root: &Path, manifest: &AppManifestV1) -> Result<(), CatalogRejection> {
    let mut declared = BTreeSet::new();
    for (relative, expected) in &manifest.integrity.files {
        let path = root.join(relative);
        let canonical =
            fs::canonicalize(&path).map_err(|error| CatalogRejection::Io(error.to_string()))?;
        if !canonical.starts_with(root) || !declared.insert(canonical.clone()) {
            return Err(CatalogRejection::IntegrityPath);
        }
        let bytes =
            fs::read(&canonical).map_err(|error| CatalogRejection::Io(error.to_string()))?;
        let actual = format!("sha256:{:x}", Sha256::digest(bytes));
        if actual != expected.0 {
            return Err(CatalogRejection::IntegrityMismatch(relative.clone()));
        }
    }
    Ok(())
}

fn bundle_generation(manifest: &AppManifestV1) -> Result<GenerationId, CandidateRejection> {
    let bytes = serde_json::to_vec(manifest)
        .map_err(|error| CandidateRejection::new(CatalogRejection::Manifest(error.to_string())))?;
    Ok(GenerationId(format!("sha256:{:x}", Sha256::digest(bytes))))
}

fn catalog_generation(
    accepted: &BTreeMap<AppId, AdmittedApp>,
) -> Result<Sha256Digest, CatalogBuildError> {
    let identity = accepted
        .iter()
        .map(|(app_id, app)| (app_id, &app.generation, &app.policy))
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&identity)?;
    Ok(Sha256Digest(format!("sha256:{:x}", Sha256::digest(bytes))))
}

fn diagnostic(
    root: &Path,
    candidate: &Path,
    app_id: Option<AppId>,
    priority: usize,
    outcome: CatalogCandidateOutcome,
    detail: impl Into<String>,
) -> AppCatalogDiagnostic {
    AppCatalogDiagnostic {
        root: root.to_path_buf(),
        candidate: Some(candidate.to_path_buf()),
        app_id,
        priority,
        outcome,
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        os::unix::fs::{symlink, PermissionsExt},
        path::Path,
    };

    use cowd_app_protocol::{
        AppPresentationV1, AppSurfacesV1, AuthorizationProfileV1, BundleIntegrityV1,
        BundleSignatureV1, FilesystemPolicyV1, IntegrityAlgorithmV1, NetworkPolicyV1,
        ProtocolRangeV1, SandboxProfileV1, SignatureAlgorithmV1,
    };
    use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
    use tempfile::TempDir;

    use super::*;

    struct FixtureSigningKey {
        pair: Ed25519KeyPair,
        key_id: String,
    }

    impl FixtureSigningKey {
        fn generate() -> Self {
            let random = SystemRandom::new();
            let document = Ed25519KeyPair::generate_pkcs8(&random).expect("generate fixture key");
            Self {
                pair: Ed25519KeyPair::from_pkcs8(document.as_ref()).expect("parse fixture key"),
                key_id: "fixture-release-key".to_owned(),
            }
        }

        fn trust_store(&self) -> AppTrustStore {
            use ring::signature::KeyPair;
            AppTrustStore::new([TrustedSigningKey {
                key_id: self.key_id.clone(),
                public_key: self.pair.public_key().as_ref().to_vec(),
                revoked: false,
            }])
        }
    }

    #[test]
    fn empty_and_missing_roots_do_not_fail_the_catalog() {
        let directory = TempDir::new().expect("temp root");
        let missing = directory.path().join("missing");
        let builder = AppCatalogBuilder::new(
            vec![directory.path().to_path_buf(), missing],
            AppCatalogPolicy::default(),
            AppTrustStore::default(),
            current_uid(directory.path()),
            1,
        );
        let snapshot = builder.build().expect("empty catalog");
        assert_eq!(snapshot.apps().len(), 0);
        assert_eq!(snapshot.diagnostics().len(), 1);
        assert_eq!(
            snapshot.diagnostics()[0].outcome,
            CatalogCandidateOutcome::RootUnavailable
        );
    }

    #[test]
    fn signed_bundle_is_admitted_without_starting_a_worker() {
        let directory = TempDir::new().expect("temp root");
        let key = FixtureSigningKey::generate();
        write_bundle(directory.path(), "reference", "reference-app", &key);
        let builder = AppCatalogBuilder::new(
            vec![directory.path().to_path_buf()],
            AppCatalogPolicy::default(),
            key.trust_store(),
            current_uid(directory.path()),
            1,
        );
        let snapshot = builder.build().expect("catalog");
        let app = snapshot
            .get(&AppId("reference-app".to_owned()))
            .expect("admitted APP");
        assert_eq!(app.policy.activation, AppActivationPolicyV1::Lazy);
        assert_eq!(
            app.catalog_entry().lifecycle.state,
            AppLifecycleStateV1::Mounted
        );
        assert!(app.executable.exists());
    }

    #[test]
    fn highest_priority_valid_duplicate_wins() {
        let first = TempDir::new().expect("first root");
        let second = TempDir::new().expect("second root");
        let key = FixtureSigningKey::generate();
        write_bundle(first.path(), "first", "reference-app", &key);
        write_bundle(second.path(), "second", "reference-app", &key);
        let snapshot = AppCatalogBuilder::new(
            vec![first.path().to_path_buf(), second.path().to_path_buf()],
            AppCatalogPolicy::default(),
            key.trust_store(),
            current_uid(first.path()),
            1,
        )
        .build()
        .expect("catalog");
        let app = snapshot
            .get(&AppId("reference-app".to_owned()))
            .expect("admitted APP");
        assert_eq!(
            app.bundle_root.file_name().and_then(|name| name.to_str()),
            Some("first")
        );
        assert!(snapshot
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.outcome == CatalogCandidateOutcome::Shadowed));
    }

    #[test]
    fn tampering_revocation_and_world_writable_files_are_isolated() {
        let directory = TempDir::new().expect("temp root");
        let key = FixtureSigningKey::generate();
        let bundle = write_bundle(directory.path(), "reference", "reference-app", &key);
        fs::write(bundle.join("bin/worker"), b"tampered").expect("tamper worker");
        let snapshot = AppCatalogBuilder::new(
            vec![directory.path().to_path_buf()],
            AppCatalogPolicy::default(),
            key.trust_store(),
            current_uid(directory.path()),
            1,
        )
        .build()
        .expect("catalog");
        assert_eq!(snapshot.apps().len(), 0);
        assert!(snapshot.diagnostics()[0]
            .detail
            .contains("integrity mismatch"));

        fs::set_permissions(&bundle, fs::Permissions::from_mode(0o777))
            .expect("make bundle insecure");
        let snapshot = AppCatalogBuilder::new(
            vec![directory.path().to_path_buf()],
            AppCatalogPolicy::default(),
            key.trust_store(),
            current_uid(directory.path()),
            1,
        )
        .build()
        .expect("catalog");
        assert_eq!(snapshot.apps().len(), 0);
        assert!(snapshot.diagnostics()[0]
            .detail
            .contains("writable by group"));

        let revoked = AppTrustStore::new([TrustedSigningKey {
            key_id: key.key_id.clone(),
            public_key: Vec::new(),
            revoked: true,
        }]);
        fs::set_permissions(&bundle, fs::Permissions::from_mode(0o755))
            .expect("restore bundle permissions");
        let snapshot = AppCatalogBuilder::new(
            vec![directory.path().to_path_buf()],
            AppCatalogPolicy::default(),
            revoked,
            current_uid(directory.path()),
            1,
        )
        .build()
        .expect("catalog");
        assert_eq!(snapshot.apps().len(), 0);
        assert!(snapshot.diagnostics()[0].detail.contains("revoked"));
    }

    #[test]
    fn symlink_candidate_cannot_escape_the_configured_root() {
        let root = TempDir::new().expect("root");
        let outside = TempDir::new().expect("outside");
        let key = FixtureSigningKey::generate();
        let bundle = write_bundle(outside.path(), "outside", "reference-app", &key);
        symlink(bundle, root.path().join("linked")).expect("create symlink");
        let snapshot = AppCatalogBuilder::new(
            vec![root.path().to_path_buf()],
            AppCatalogPolicy::default(),
            key.trust_store(),
            current_uid(root.path()),
            1,
        )
        .build()
        .expect("catalog");
        assert_eq!(snapshot.apps().len(), 0);
        assert!(snapshot.diagnostics()[0].detail.contains("escapes"));
    }

    #[test]
    fn explicit_required_resident_policy_is_projected() {
        let directory = TempDir::new().expect("temp root");
        let key = FixtureSigningKey::generate();
        write_bundle(directory.path(), "reference", "reference-app", &key);
        let app_id = AppId("reference-app".to_owned());
        let policy = AppCatalogPolicy {
            entries: BTreeMap::from([(
                app_id.clone(),
                EffectiveAppPolicy {
                    enabled: true,
                    required: true,
                    activation: AppActivationPolicyV1::Resident,
                    config_file: None,
                },
            )]),
        };
        let snapshot = AppCatalogBuilder::new(
            vec![directory.path().to_path_buf()],
            policy,
            key.trust_store(),
            current_uid(directory.path()),
            1,
        )
        .build()
        .expect("catalog");
        let entry = snapshot.get(&app_id).expect("APP").catalog_entry();
        assert!(entry.required);
        assert_eq!(entry.activation, AppActivationPolicyV1::Resident);
    }

    fn write_bundle(
        root: &Path,
        directory_name: &str,
        app_id: &str,
        key: &FixtureSigningKey,
    ) -> PathBuf {
        let bundle = root.join(directory_name);
        fs::create_dir_all(bundle.join("bin")).expect("create bin");
        fs::create_dir_all(bundle.join("webui")).expect("create webui");
        fs::set_permissions(&bundle, fs::Permissions::from_mode(0o755))
            .expect("bundle permissions");
        let worker = bundle.join("bin/worker");
        fs::write(&worker, b"#!/bin/sh\nexit 0\n").expect("worker");
        fs::set_permissions(&worker, fs::Permissions::from_mode(0o755))
            .expect("worker permissions");
        let index = bundle.join("webui/index.html");
        fs::write(&index, b"<!doctype html><title>Reference</title>").expect("index");
        fs::set_permissions(&index, fs::Permissions::from_mode(0o644)).expect("index permissions");
        fs::set_permissions(bundle.join("bin"), fs::Permissions::from_mode(0o755))
            .expect("bin permissions");
        fs::set_permissions(bundle.join("webui"), fs::Permissions::from_mode(0o755))
            .expect("webui permissions");

        let signed_digest = Sha256Digest(format!(
            "sha256:{:x}",
            Sha256::digest(format!("{app_id}:1.0.0").as_bytes())
        ));
        let signature = URL_SAFE_NO_PAD.encode(key.pair.sign(signed_digest.0.as_bytes()).as_ref());
        let manifest = AppManifestV1 {
            schema_version: 1,
            app_id: AppId(app_id.to_owned()),
            display_name: "Reference APP".to_owned(),
            artifact_version: "1.0.0".to_owned(),
            required_protocol: ProtocolRangeV1::exact_v1(),
            executable: "bin/worker".to_owned(),
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
                files: BTreeMap::from([
                    ("bin/worker".to_owned(), file_digest(&worker)),
                    ("webui/index.html".to_owned(), file_digest(&index)),
                ]),
                manifest_digest: signed_digest.clone(),
            },
            signature: BundleSignatureV1 {
                algorithm: SignatureAlgorithmV1::Ed25519,
                key_id: key.key_id.clone(),
                signature,
                signed_digest,
                expires_unix_ms: None,
                provenance_digest: None,
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
        };
        fs::write(
            bundle.join(MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).expect("manifest JSON"),
        )
        .expect("manifest");
        fs::set_permissions(
            bundle.join(MANIFEST_FILE),
            fs::Permissions::from_mode(0o644),
        )
        .expect("manifest permissions");
        bundle
    }

    fn file_digest(path: &Path) -> Sha256Digest {
        Sha256Digest(format!(
            "sha256:{:x}",
            Sha256::digest(fs::read(path).expect("fixture file"))
        ))
    }

    fn current_uid(path: &Path) -> u32 {
        fs::metadata(path).expect("metadata").uid()
    }
}
