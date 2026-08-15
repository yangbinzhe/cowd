//! Production coordinator for the offline ownership split cutover.
//!
//! The coordinator is the only publisher of `publication/active.json`. Core,
//! the external MFG administrator and the legacy exporter remain independent
//! process/database owners behind typed ports. A database generation is never
//! deleted here: failures leave it invisible and rollback publishes a new
//! pointer to an exact historical Core/MFG pair.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::ownership_cutover_contract::{
    validate_active_publication, CutoverActivationKindV1, CutoverBackendV1, CutoverCountsV1,
    CutoverExecutionReceiptsV1, CutoverSourceV1, CutoverStateReceiptsV1, CutoverTargetV1,
    OwnershipCutoverManifestV1, PreviousCutoverV1,
};

const CONTRACT_ID: &str = "cowd.ownership-cutover/v1";
const OWNERSHIP_CONTRACT_DIGEST: &str =
    "sha256:61ed3c6becf145fcf1029b4ee39b2ac4d0aa39177ae2e195fe7ec2b052f270e5";
const EXECUTION_DIGEST_DOMAIN: &[u8] = b"cowd.ownership-cutover.execution.v1\0";
const EXPORT_RECEIPT_DIGEST_DOMAIN: &[u8] = b"cowd.ownership.export-receipt.v1\0";
const EXECUTION_RECEIPT_DIGEST_DOMAIN: &[u8] = b"cowd.ownership-cutover.execution-receipt.v1\0";
const ACL_RECEIPT_DIGEST_DOMAIN: &[u8] = b"cowd.ownership-cutover.acl-receipt.v1\0";
const STATE_RECEIPT_DIGEST_DOMAIN: &[u8] = b"cowd.ownership-cutover.state-receipt.v1\0";
const MAX_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_RECEIPT_BYTES: u64 = 1024 * 1024;

/// Credential carrier accepted by external ownership administrators.
///
/// There is deliberately no inline/string credential variant. The concrete
/// process adapter communicates only the channel through child environment;
/// secret bytes therefore never appear in argv, request JSON or diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub enum CredentialSource {
    Environment { variable: String },
    File { path: PathBuf },
    Stdin,
}

impl CredentialSource {
    fn validate(&self) -> Result<(), CutoverError> {
        match self {
            Self::Environment { variable }
                if !variable.is_empty()
                    && variable.len() <= 128
                    && variable.bytes().all(|byte| {
                        byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                    })
                    && variable
                        .as_bytes()
                        .first()
                        .is_some_and(u8::is_ascii_uppercase) =>
            {
                Ok(())
            }
            Self::File { path } => validate_credential_file(path),
            Self::Stdin => Ok(()),
            _ => Err(CutoverError::InvalidRequest(
                "credential source must be an uppercase environment name, a private file, or stdin",
            )),
        }
    }

    fn configure(&self, command: &mut Command) {
        match self {
            Self::Environment { variable } => {
                command.env("COWD_CUTOVER_CREDENTIAL_ENV", variable);
                command.stdin(Stdio::null());
            }
            Self::File { path } => {
                command.env("COWD_CUTOVER_CREDENTIAL_FILE", path);
                command.stdin(Stdio::null());
            }
            Self::Stdin => {
                command.env("COWD_CUTOVER_CREDENTIAL_STDIN", "1");
                command.stdin(Stdio::inherit());
            }
        }
    }
}

pub enum LegacySourceLocation {
    Sqlite {
        path: PathBuf,
    },
    Postgres {
        namespace: String,
        credential: CredentialSource,
    },
}

pub struct LegacySourceRequest {
    pub location: LegacySourceLocation,
    pub source_version: String,
    pub schema_version: u64,
    pub maintenance_fence_id: String,
    pub exported_at: String,
}

pub struct TargetGenerationRequest {
    pub namespace: String,
    pub generation: String,
    pub credential: CredentialSource,
}

pub struct ActiveCutoverRequest {
    pub root: PathBuf,
    pub publication_generation: String,
    pub activation_fence_id: String,
    pub created_at: String,
    pub source: LegacySourceRequest,
    pub core: TargetGenerationRequest,
    pub mfg: TargetGenerationRequest,
}

pub struct RollbackCutoverRequest {
    pub root: PathBuf,
    pub publication_generation: String,
    pub activation_fence_id: String,
    pub created_at: String,
    pub target_manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CutoverPublication {
    pub publication_generation: String,
    pub manifest_digest: String,
    pub core_generation: String,
    pub mfg_generation: String,
    pub rollback: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivateRequestDocumentV1 {
    schema_version: u16,
    external_program: PathBuf,
    root: PathBuf,
    publication_generation: String,
    activation_fence_id: String,
    created_at: String,
    source: ActivateSourceDocumentV1,
    core: TargetDocumentV1,
    mfg: TargetDocumentV1,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivateSourceDocumentV1 {
    backend: String,
    namespace: Option<String>,
    path: Option<PathBuf>,
    source_version: String,
    schema_version: u64,
    maintenance_fence_id: String,
    exported_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetDocumentV1 {
    namespace: String,
    generation: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RollbackRequestDocumentV1 {
    schema_version: u16,
    external_program: PathBuf,
    root: PathBuf,
    publication_generation: String,
    activation_fence_id: String,
    created_at: String,
    target_manifest_digest: String,
}

/// Execute the production offline coordinator from the operator CLI.
///
/// The request document contains topology only. Credential bytes are accepted
/// exclusively through an environment name, a private file, or stdin.
pub fn run_operator_command(args: &[String]) -> Result<CutoverPublication, CutoverError> {
    let parsed = OperatorArguments::parse(args)?;
    let bytes = read_regular_bounded(&parsed.request_path, MAX_RECEIPT_BYTES)?;
    match parsed.action {
        OperatorAction::Activate => {
            let document: ActivateRequestDocumentV1 = serde_json::from_slice(&bytes)
                .map_err(|_| CutoverError::InvalidRequest("activate request JSON is invalid"))?;
            if document.schema_version != 1 {
                return Err(CutoverError::InvalidRequest(
                    "activate request schema_version must be 1",
                ));
            }
            let credential = parsed.credential.ok_or(CutoverError::InvalidRequest(
                "activate requires exactly one credential channel",
            ))?;
            match document.source.backend.as_str() {
                "sqlite"
                    if document.source.path.is_none() || document.source.namespace.is_some() =>
                {
                    return Err(CutoverError::InvalidRequest(
                        "SQLite source requires only path",
                    ));
                }
                "postgres"
                    if document.source.namespace.is_none() || document.source.path.is_some() =>
                {
                    return Err(CutoverError::InvalidRequest(
                        "PostgreSQL source requires only namespace",
                    ));
                }
                _ => {}
            }
            let source_location =
                match document.source.backend.as_str() {
                    "sqlite" => LegacySourceLocation::Sqlite {
                        path: document
                            .source
                            .path
                            .ok_or(CutoverError::InvalidRequest("SQLite source requires path"))?,
                    },
                    "postgres" => LegacySourceLocation::Postgres {
                        namespace: document.source.namespace.ok_or(
                            CutoverError::InvalidRequest("PostgreSQL source requires namespace"),
                        )?,
                        credential: credential.clone(),
                    },
                    _ => {
                        return Err(CutoverError::InvalidRequest(
                            "source backend must be sqlite or postgres",
                        ))
                    }
                };
            let external = ExternalOwnershipProgram::new(document.external_program)?;
            let maintenance = FileMaintenancePort;
            OwnershipCutoverCoordinator::new(
                &maintenance,
                &external,
                &external,
                &external,
                &external,
            )
            .activate(ActiveCutoverRequest {
                root: document.root,
                publication_generation: document.publication_generation,
                activation_fence_id: document.activation_fence_id,
                created_at: document.created_at,
                source: LegacySourceRequest {
                    location: source_location,
                    source_version: document.source.source_version,
                    schema_version: document.source.schema_version,
                    maintenance_fence_id: document.source.maintenance_fence_id,
                    exported_at: document.source.exported_at,
                },
                core: TargetGenerationRequest {
                    namespace: document.core.namespace,
                    generation: document.core.generation,
                    credential: credential.clone(),
                },
                mfg: TargetGenerationRequest {
                    namespace: document.mfg.namespace,
                    generation: document.mfg.generation,
                    credential,
                },
            })
        }
        OperatorAction::Rollback => {
            if parsed.credential.is_some() {
                return Err(CutoverError::InvalidRequest(
                    "rollback does not consume a credential channel",
                ));
            }
            let document: RollbackRequestDocumentV1 = serde_json::from_slice(&bytes)
                .map_err(|_| CutoverError::InvalidRequest("rollback request JSON is invalid"))?;
            if document.schema_version != 1 {
                return Err(CutoverError::InvalidRequest(
                    "rollback request schema_version must be 1",
                ));
            }
            let external = ExternalOwnershipProgram::new(document.external_program)?;
            let maintenance = FileMaintenancePort;
            OwnershipCutoverCoordinator::new(
                &maintenance,
                &external,
                &external,
                &external,
                &external,
            )
            .rollback(RollbackCutoverRequest {
                root: document.root,
                publication_generation: document.publication_generation,
                activation_fence_id: document.activation_fence_id,
                created_at: document.created_at,
                target_manifest_digest: document.target_manifest_digest,
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorAction {
    Activate,
    Rollback,
}

struct OperatorArguments {
    action: OperatorAction,
    request_path: PathBuf,
    credential: Option<CredentialSource>,
}

impl OperatorArguments {
    fn parse(args: &[String]) -> Result<Self, CutoverError> {
        let action = match args.first().map(String::as_str) {
            Some("activate") => OperatorAction::Activate,
            Some("rollback") => OperatorAction::Rollback,
            _ => {
                return Err(CutoverError::InvalidRequest(
                    "usage: ownership-cutover activate|rollback --request <json> [--credential-env <NAME>|--credential-file <path>|--credential-stdin]",
                ))
            }
        };
        let mut request_path = None;
        let mut credential = None;
        let mut index = 1;
        while index < args.len() {
            let (next, consumed) = match args[index].as_str() {
                "--request" => (
                    Some(args.get(index + 1).ok_or(CutoverError::InvalidRequest(
                        "--request requires a JSON path",
                    ))?),
                    2,
                ),
                "--credential-env" => {
                    let value = args.get(index + 1).ok_or(CutoverError::InvalidRequest(
                        "--credential-env requires a variable name",
                    ))?;
                    if credential
                        .replace(CredentialSource::Environment {
                            variable: value.clone(),
                        })
                        .is_some()
                    {
                        return Err(CutoverError::InvalidRequest(
                            "credential channels are mutually exclusive",
                        ));
                    }
                    (None, 2)
                }
                "--credential-file" => {
                    let value = args.get(index + 1).ok_or(CutoverError::InvalidRequest(
                        "--credential-file requires a path",
                    ))?;
                    if credential
                        .replace(CredentialSource::File {
                            path: PathBuf::from(value),
                        })
                        .is_some()
                    {
                        return Err(CutoverError::InvalidRequest(
                            "credential channels are mutually exclusive",
                        ));
                    }
                    (None, 2)
                }
                "--credential-stdin" => {
                    if credential.replace(CredentialSource::Stdin).is_some() {
                        return Err(CutoverError::InvalidRequest(
                            "credential channels are mutually exclusive",
                        ));
                    }
                    (None, 1)
                }
                _ => {
                    return Err(CutoverError::InvalidRequest(
                        "ownership-cutover accepts only request and credential-channel flags",
                    ))
                }
            };
            if let Some(path) = next {
                if request_path.replace(PathBuf::from(path)).is_some() {
                    return Err(CutoverError::InvalidRequest(
                        "--request may be supplied only once",
                    ));
                }
            }
            index += consumed;
        }
        Ok(Self {
            action,
            request_path: request_path
                .ok_or(CutoverError::InvalidRequest("--request is required"))?,
            credential,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortFailureCode {
    Busy,
    GatewayRunning,
    WorkersRunning,
    SourceMutable,
    ExportRejected,
    TargetCollision,
    ImportRejected,
    VerificationRejected,
    ExternalProcessFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortFailure {
    pub code: PortFailureCode,
}

impl PortFailure {
    #[must_use]
    pub const fn new(code: PortFailureCode) -> Self {
        Self { code }
    }
}

pub trait MaintenanceLease: Send {
    fn receipt_bytes(&self) -> &[u8];
}

pub trait MaintenancePort: Send + Sync {
    fn acquire(
        &self,
        root: &Path,
        maintenance_fence_id: &str,
    ) -> Result<Box<dyn MaintenanceLease>, PortFailure>;
}

pub struct QuiescenceRequest<'a> {
    pub root: &'a Path,
    pub maintenance_fence_id: &'a str,
    pub gateway_receipt_path: &'a Path,
    pub workers_receipt_path: &'a Path,
    pub request_file: &'a Path,
}

pub trait QuiescencePort: Send + Sync {
    fn confirm_stopped(&self, request: &QuiescenceRequest<'_>) -> Result<(), PortFailure>;
}

pub struct LegacyExportPortRequest<'a> {
    pub source: &'a LegacySourceRequest,
    pub snapshot_path: &'a Path,
    pub export_receipt_path: &'a Path,
    pub readonly_receipt_path: &'a Path,
    pub request_file: &'a Path,
}

pub trait LegacyExporterPort: Send + Sync {
    fn export_readonly(&self, request: &LegacyExportPortRequest<'_>) -> Result<(), PortFailure>;

    fn attest_readonly(
        &self,
        maintenance_fence_id: &str,
        receipt_path: &Path,
        request_file: &Path,
    ) -> Result<(), PortFailure>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetOwner {
    Core,
    Mfg,
}

impl TargetOwner {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::Mfg => "mfg",
        }
    }
}

pub struct TargetImportPortRequest<'a> {
    pub owner: TargetOwner,
    pub target: &'a TargetGenerationRequest,
    pub source_snapshot_path: &'a Path,
    pub source_export_receipt_path: &'a Path,
    pub generation_directory: &'a Path,
    pub import_receipt_path: &'a Path,
    pub acl_receipt_path: &'a Path,
    pub request_file: &'a Path,
}

pub trait CoreImporterPort: Send + Sync {
    fn initialize_import_and_verify(
        &self,
        request: &TargetImportPortRequest<'_>,
    ) -> Result<(), PortFailure>;
}

pub trait MfgAdminPort: Send + Sync {
    fn initialize_import_and_verify(
        &self,
        request: &TargetImportPortRequest<'_>,
    ) -> Result<(), PortFailure>;
}

/// A concrete maintenance lock whose lease spans the complete cutover.
#[derive(Debug, Default)]
pub struct FileMaintenancePort;

struct FileMaintenanceLease {
    path: PathBuf,
    receipt: Vec<u8>,
}

impl MaintenancePort for FileMaintenancePort {
    fn acquire(
        &self,
        root: &Path,
        maintenance_fence_id: &str,
    ) -> Result<Box<dyn MaintenanceLease>, PortFailure> {
        let path = root.join("maintenance.lock");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|_| PortFailure::new(PortFailureCode::Busy))?;
        let receipt = execution_receipt(
            ExecutionReceiptKind::MaintenanceLock,
            maintenance_fence_id,
            "ownership_cutover",
        )
        .map_err(|_| PortFailure::new(PortFailureCode::Busy))?;
        file.write_all(&receipt)
            .and_then(|()| file.sync_all())
            .map_err(|_| PortFailure::new(PortFailureCode::Busy))?;
        Ok(Box::new(FileMaintenanceLease { path, receipt }))
    }
}

impl MaintenanceLease for FileMaintenanceLease {
    fn receipt_bytes(&self) -> &[u8] {
        &self.receipt
    }
}

impl Drop for FileMaintenanceLease {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        if let Some(parent) = self.path.parent() {
            let _ = sync_directory(parent);
        }
    }
}

/// Process implementation of the no-secret request-file protocol.
///
/// The external program receives only `OPERATION REQUEST_FILE OUTPUT_DIR` in
/// argv. A credential channel descriptor is placed in child environment and
/// stdout/stderr are discarded so a faulty administrator cannot echo a secret
/// into Gateway diagnostics.
#[derive(Clone)]
pub struct ExternalOwnershipProgram {
    executable: PathBuf,
}

impl ExternalOwnershipProgram {
    pub fn new(executable: PathBuf) -> Result<Self, CutoverError> {
        let metadata = fs::symlink_metadata(&executable).map_err(|source| CutoverError::Io {
            operation: "inspect external ownership program",
            path: executable.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(CutoverError::InvalidRequest(
                "external ownership program must be a regular non-symlink file",
            ));
        }
        Ok(Self { executable })
    }

    fn run(
        &self,
        operation: &str,
        request_file: &Path,
        output_directory: &Path,
        credential: Option<&CredentialSource>,
    ) -> Result<(), PortFailure> {
        let mut command = Command::new(&self.executable);
        command
            .arg(operation)
            .arg(request_file)
            .arg(output_directory)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(credential) = credential {
            credential.configure(&mut command);
        } else {
            command.stdin(Stdio::null());
        }
        let status = command
            .status()
            .map_err(|_| PortFailure::new(PortFailureCode::ExternalProcessFailed))?;
        if status.success() {
            Ok(())
        } else {
            Err(PortFailure::new(PortFailureCode::ExternalProcessFailed))
        }
    }
}

impl QuiescencePort for ExternalOwnershipProgram {
    fn confirm_stopped(&self, request: &QuiescenceRequest<'_>) -> Result<(), PortFailure> {
        self.run("confirm-stopped", request.request_file, request.root, None)
    }
}

impl LegacyExporterPort for ExternalOwnershipProgram {
    fn export_readonly(&self, request: &LegacyExportPortRequest<'_>) -> Result<(), PortFailure> {
        let credential = match &request.source.location {
            LegacySourceLocation::Sqlite { .. } => None,
            LegacySourceLocation::Postgres { credential, .. } => Some(credential),
        };
        let output = request
            .snapshot_path
            .parent()
            .ok_or_else(|| PortFailure::new(PortFailureCode::ExportRejected))?;
        self.run("export-readonly", request.request_file, output, credential)
    }

    fn attest_readonly(
        &self,
        _maintenance_fence_id: &str,
        receipt_path: &Path,
        request_file: &Path,
    ) -> Result<(), PortFailure> {
        let output = receipt_path
            .parent()
            .ok_or_else(|| PortFailure::new(PortFailureCode::SourceMutable))?;
        self.run("attest-readonly", request_file, output, None)
    }
}

impl CoreImporterPort for ExternalOwnershipProgram {
    fn initialize_import_and_verify(
        &self,
        request: &TargetImportPortRequest<'_>,
    ) -> Result<(), PortFailure> {
        self.run(
            "initialize-import-verify-core",
            request.request_file,
            request.generation_directory,
            Some(&request.target.credential),
        )
    }
}

impl MfgAdminPort for ExternalOwnershipProgram {
    fn initialize_import_and_verify(
        &self,
        request: &TargetImportPortRequest<'_>,
    ) -> Result<(), PortFailure> {
        self.run(
            "initialize-import-verify-mfg",
            request.request_file,
            request.generation_directory,
            Some(&request.target.credential),
        )
    }
}

pub struct OwnershipCutoverCoordinator<'a> {
    maintenance: &'a dyn MaintenancePort,
    quiescence: &'a dyn QuiescencePort,
    exporter: &'a dyn LegacyExporterPort,
    core: &'a dyn CoreImporterPort,
    mfg: &'a dyn MfgAdminPort,
    fault: PublishFault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishFault {
    None,
    #[cfg(test)]
    BeforeRename,
}

impl<'a> OwnershipCutoverCoordinator<'a> {
    #[must_use]
    pub fn new(
        maintenance: &'a dyn MaintenancePort,
        quiescence: &'a dyn QuiescencePort,
        exporter: &'a dyn LegacyExporterPort,
        core: &'a dyn CoreImporterPort,
        mfg: &'a dyn MfgAdminPort,
    ) -> Self {
        Self {
            maintenance,
            quiescence,
            exporter,
            core,
            mfg,
            fault: PublishFault::None,
        }
    }

    pub fn activate(
        &self,
        request: ActiveCutoverRequest,
    ) -> Result<CutoverPublication, CutoverError> {
        let root = prepare_active_request(&request)?;
        let history_state = load_history_state(&root)?;
        let lease = self
            .maintenance
            .acquire(&root, &request.source.maintenance_fence_id)
            .map_err(|failure| port_error("maintenance", "acquire", failure))?;

        let evidence = EvidencePaths::new(&root, &request.publication_generation)?;
        let maintenance_digest = write_and_validate_execution_receipt(
            &evidence.maintenance_receipt,
            lease.receipt_bytes(),
            ExecutionReceiptKind::MaintenanceLock,
            &request.source.maintenance_fence_id,
        )?;
        write_port_request(
            &evidence.quiescence_request,
            &serde_json::json!({
                "schema_version": 1,
                "operation": "confirm_stopped",
                "maintenance_fence_id": request.source.maintenance_fence_id,
                "gateway_receipt_path": evidence.gateway_receipt,
                "workers_receipt_path": evidence.workers_receipt,
            }),
        )?;
        self.quiescence
            .confirm_stopped(&QuiescenceRequest {
                root: &root,
                maintenance_fence_id: &request.source.maintenance_fence_id,
                gateway_receipt_path: &evidence.gateway_receipt,
                workers_receipt_path: &evidence.workers_receipt,
                request_file: &evidence.quiescence_request,
            })
            .map_err(|failure| port_error("quiescence", "confirm_stopped", failure))?;
        let gateway_digest = validate_execution_receipt_file(
            &evidence.gateway_receipt,
            ExecutionReceiptKind::GatewayStopped,
            &request.source.maintenance_fence_id,
        )?;
        let workers_digest = validate_execution_receipt_file(
            &evidence.workers_receipt,
            ExecutionReceiptKind::WorkersStopped,
            &request.source.maintenance_fence_id,
        )?;

        let source_paths = SourcePaths::new(&root, &request.publication_generation)?;
        write_port_request(
            &evidence.export_request,
            &export_request_value(&request.source, &source_paths),
        )?;
        self.exporter
            .export_readonly(&LegacyExportPortRequest {
                source: &request.source,
                snapshot_path: &source_paths.snapshot,
                export_receipt_path: &source_paths.receipt,
                readonly_receipt_path: &evidence.readonly_receipt,
                request_file: &evidence.export_request,
            })
            .map_err(|failure| port_error("legacy_exporter", "export_readonly", failure))?;
        let readonly_digest = validate_execution_receipt_file(
            &evidence.readonly_receipt,
            ExecutionReceiptKind::LegacyReadonly,
            &request.source.maintenance_fence_id,
        )?;
        let source = validate_exported_source(&source_paths, &request.source)?;

        let core_paths = TargetPaths::new(&root, TargetOwner::Core, &request.core.generation)?;
        let mfg_paths = TargetPaths::new(&root, TargetOwner::Mfg, &request.mfg.generation)?;
        let core_target = self.run_target(
            TargetOwner::Core,
            &request.core,
            &source,
            &source_paths,
            &core_paths,
            &evidence.core_request,
        )?;
        let mfg_target = self.run_target(
            TargetOwner::Mfg,
            &request.mfg,
            &source,
            &source_paths,
            &mfg_paths,
            &evidence.mfg_request,
        )?;

        let staged_digest = write_state_receipt(
            &evidence.staged_receipt,
            StateReceiptV1::for_pair(
                "staged",
                &request.publication_generation,
                &source,
                &core_target,
                &mfg_target,
            )?,
        )?;
        let verified_digest = write_state_receipt(
            &evidence.verified_receipt,
            StateReceiptV1::for_pair(
                "verified",
                &request.publication_generation,
                &source,
                &core_target,
                &mfg_target,
            )?,
        )?;

        sync_cutover_evidence(&source_paths, &core_paths, &mfg_paths, &evidence)?;
        let mut manifest = OwnershipCutoverManifestV1 {
            schema_version: 1,
            contract_id: CONTRACT_ID.to_owned(),
            ownership_contract_digest: OWNERSHIP_CONTRACT_DIGEST.to_owned(),
            execution_contract_digest: execution_contract_digest()?,
            publication_generation: request.publication_generation,
            activation_fence_id: request.activation_fence_id,
            source: source.contract,
            core: core_target.contract,
            mfg: mfg_target.contract,
            execution_receipts: CutoverExecutionReceiptsV1 {
                maintenance_lock_receipt_digest: maintenance_digest,
                gateway_stopped_receipt_digest: gateway_digest,
                apps_stopped_receipt_digest: workers_digest,
                legacy_readonly_receipt_digest: readonly_digest,
            },
            state_receipts: CutoverStateReceiptsV1 {
                staged_receipt_digest: staged_digest,
                verified_receipt_digest: verified_digest,
            },
            previous: history_state.current.as_ref().map(previous_summary),
            created_at: request.created_at,
            activation_kind: CutoverActivationKindV1::Active,
            rollback_target_manifest_digest: None,
            manifest_digest: String::new(),
        };
        manifest.manifest_digest = manifest
            .canonical_manifest_digest()
            .map_err(contract_error)?;
        manifest
            .validate(
                &root,
                history_state.current.as_ref(),
                &history_state.history,
            )
            .map_err(contract_error)?;
        let publication =
            publish_manifest(&root, &manifest, history_state.current.as_ref(), self.fault)?;
        drop(lease);
        Ok(publication)
    }

    pub fn rollback(
        &self,
        request: RollbackCutoverRequest,
    ) -> Result<CutoverPublication, CutoverError> {
        validate_identifier(&request.publication_generation, "publication generation")?;
        validate_identifier(&request.activation_fence_id, "activation fence")?;
        validate_digest(&request.target_manifest_digest)?;
        parse_utc(&request.created_at)?;
        let root = prepare_root(&request.root)?;
        let history_state = load_history_state(&root)?;
        let current = history_state
            .current
            .as_ref()
            .ok_or(CutoverError::InvalidState(
                "rollback requires one current active publication",
            ))?;
        let target = history_state
            .history
            .iter()
            .find(|manifest| manifest.manifest_digest == request.target_manifest_digest)
            .ok_or(CutoverError::InvalidRequest(
                "rollback target is not a verified historical publication",
            ))?;
        let lease = self
            .maintenance
            .acquire(&root, &target.source.maintenance_fence_id)
            .map_err(|failure| port_error("maintenance", "acquire", failure))?;
        let evidence = EvidencePaths::new(&root, &request.publication_generation)?;
        let maintenance_digest = write_and_validate_execution_receipt(
            &evidence.maintenance_receipt,
            lease.receipt_bytes(),
            ExecutionReceiptKind::MaintenanceLock,
            &target.source.maintenance_fence_id,
        )?;
        write_port_request(
            &evidence.quiescence_request,
            &serde_json::json!({
                "schema_version": 1,
                "operation": "confirm_stopped",
                "maintenance_fence_id": target.source.maintenance_fence_id,
                "gateway_receipt_path": evidence.gateway_receipt,
                "workers_receipt_path": evidence.workers_receipt,
            }),
        )?;
        self.quiescence
            .confirm_stopped(&QuiescenceRequest {
                root: &root,
                maintenance_fence_id: &target.source.maintenance_fence_id,
                gateway_receipt_path: &evidence.gateway_receipt,
                workers_receipt_path: &evidence.workers_receipt,
                request_file: &evidence.quiescence_request,
            })
            .map_err(|failure| port_error("quiescence", "confirm_stopped", failure))?;
        let gateway_digest = validate_execution_receipt_file(
            &evidence.gateway_receipt,
            ExecutionReceiptKind::GatewayStopped,
            &target.source.maintenance_fence_id,
        )?;
        let workers_digest = validate_execution_receipt_file(
            &evidence.workers_receipt,
            ExecutionReceiptKind::WorkersStopped,
            &target.source.maintenance_fence_id,
        )?;
        write_port_request(
            &evidence.export_request,
            &serde_json::json!({
                "schema_version": 1,
                "operation": "attest_readonly",
                "maintenance_fence_id": target.source.maintenance_fence_id,
                "receipt_path": evidence.readonly_receipt,
            }),
        )?;
        self.exporter
            .attest_readonly(
                &target.source.maintenance_fence_id,
                &evidence.readonly_receipt,
                &evidence.export_request,
            )
            .map_err(|failure| port_error("legacy_exporter", "attest_readonly", failure))?;
        let readonly_digest = validate_execution_receipt_file(
            &evidence.readonly_receipt,
            ExecutionReceiptKind::LegacyReadonly,
            &target.source.maintenance_fence_id,
        )?;
        let rollback_source = ValidatedSource {
            contract: target.source.clone(),
        };
        let core_paths = TargetPaths::existing(&root, &target.core)?;
        let mfg_paths = TargetPaths::existing(&root, &target.mfg)?;
        let core_target = validate_target_output(
            TargetOwner::Core,
            &target.core.namespace,
            &target.core.generation,
            &rollback_source,
            &core_paths,
        )?;
        let mfg_target = validate_target_output(
            TargetOwner::Mfg,
            &target.mfg.namespace,
            &target.mfg.generation,
            &rollback_source,
            &mfg_paths,
        )?;
        if core_target.contract != target.core || mfg_target.contract != target.mfg {
            return Err(CutoverError::Evidence(
                "historical target receipts no longer bind the exact rollback pair".to_owned(),
            ));
        }
        let staged_digest = write_state_receipt(
            &evidence.staged_receipt,
            StateReceiptV1::for_pair(
                "staged",
                &request.publication_generation,
                &rollback_source,
                &core_target,
                &mfg_target,
            )?,
        )?;
        let verified_digest = write_state_receipt(
            &evidence.verified_receipt,
            StateReceiptV1::for_pair(
                "verified",
                &request.publication_generation,
                &rollback_source,
                &core_target,
                &mfg_target,
            )?,
        )?;
        sync_existing_target(&root, &target.core)?;
        sync_existing_target(&root, &target.mfg)?;
        sync_tree(&evidence.directory)?;

        let mut manifest = OwnershipCutoverManifestV1 {
            schema_version: 1,
            contract_id: CONTRACT_ID.to_owned(),
            ownership_contract_digest: OWNERSHIP_CONTRACT_DIGEST.to_owned(),
            execution_contract_digest: execution_contract_digest()?,
            publication_generation: request.publication_generation,
            activation_fence_id: request.activation_fence_id,
            source: target.source.clone(),
            core: target.core.clone(),
            mfg: target.mfg.clone(),
            execution_receipts: CutoverExecutionReceiptsV1 {
                maintenance_lock_receipt_digest: maintenance_digest,
                gateway_stopped_receipt_digest: gateway_digest,
                apps_stopped_receipt_digest: workers_digest,
                legacy_readonly_receipt_digest: readonly_digest,
            },
            state_receipts: CutoverStateReceiptsV1 {
                staged_receipt_digest: staged_digest,
                verified_receipt_digest: verified_digest,
            },
            previous: Some(previous_summary(current)),
            created_at: request.created_at,
            activation_kind: CutoverActivationKindV1::Rollback,
            rollback_target_manifest_digest: Some(request.target_manifest_digest),
            manifest_digest: String::new(),
        };
        manifest.manifest_digest = manifest
            .canonical_manifest_digest()
            .map_err(contract_error)?;
        manifest
            .validate(&root, Some(current), &history_state.history)
            .map_err(contract_error)?;
        let publication = publish_manifest(&root, &manifest, Some(current), self.fault)?;
        drop(lease);
        Ok(publication)
    }

    fn run_target(
        &self,
        owner: TargetOwner,
        target: &TargetGenerationRequest,
        source: &ValidatedSource,
        source_paths: &SourcePaths,
        paths: &TargetPaths,
        request_file: &Path,
    ) -> Result<ValidatedTarget, CutoverError> {
        write_port_request(
            request_file,
            &serde_json::json!({
                "schema_version": 1,
                "operation": "initialize_import_verify",
                "owner": owner,
                "backend": "postgres",
                "namespace": target.namespace,
                "generation": target.generation,
                "source_snapshot": source_paths.snapshot,
                "source_export_receipt": source_paths.receipt,
                "generation_directory": paths.directory,
                "import_receipt": paths.import_receipt,
                "acl_receipt": paths.acl_receipt,
            }),
        )?;
        let port_request = TargetImportPortRequest {
            owner,
            target,
            source_snapshot_path: &source_paths.snapshot,
            source_export_receipt_path: &source_paths.receipt,
            generation_directory: &paths.directory,
            import_receipt_path: &paths.import_receipt,
            acl_receipt_path: &paths.acl_receipt,
            request_file,
        };
        let result = match owner {
            TargetOwner::Core => self.core.initialize_import_and_verify(&port_request),
            TargetOwner::Mfg => self.mfg.initialize_import_and_verify(&port_request),
        };
        result
            .map_err(|failure| port_error(owner.as_str(), "initialize_import_verify", failure))?;
        validate_target_output(owner, &target.namespace, &target.generation, source, paths)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CutoverError {
    #[error("ownership cutover request is invalid: {0}")]
    InvalidRequest(&'static str),
    #[error("ownership cutover state is invalid: {0}")]
    InvalidState(&'static str),
    #[error("ownership cutover port {owner}.{operation} failed with {code:?}")]
    Port {
        owner: &'static str,
        operation: &'static str,
        code: PortFailureCode,
    },
    #[error("ownership cutover I/O failed during {operation} at {path}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("ownership cutover evidence is invalid: {0}")]
    Evidence(String),
}

fn contract_error(error: crate::ownership_cutover_contract::OwnershipCutoverError) -> CutoverError {
    CutoverError::Evidence(format!("ownership contract rejected evidence: {error}"))
}

fn port_error(owner: &'static str, operation: &'static str, failure: PortFailure) -> CutoverError {
    CutoverError::Port {
        owner,
        operation,
        code: failure.code,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionReceiptKind {
    MaintenanceLock,
    GatewayStopped,
    WorkersStopped,
    LegacyReadonly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionReceiptV1 {
    schema_version: u16,
    kind: ExecutionReceiptKind,
    maintenance_fence_id: String,
    subject: String,
    satisfied: bool,
    receipt_digest: String,
}

fn execution_receipt(
    kind: ExecutionReceiptKind,
    fence: &str,
    subject: &str,
) -> Result<Vec<u8>, CutoverError> {
    let mut receipt = ExecutionReceiptV1 {
        schema_version: 1,
        kind,
        maintenance_fence_id: fence.to_owned(),
        subject: subject.to_owned(),
        satisfied: true,
        receipt_digest: String::new(),
    };
    receipt.receipt_digest = embedded_digest(EXECUTION_RECEIPT_DIGEST_DOMAIN, &receipt)?;
    pretty_json(&receipt)
}

fn validate_execution_receipt_file(
    path: &Path,
    expected_kind: ExecutionReceiptKind,
    expected_fence: &str,
) -> Result<String, CutoverError> {
    let bytes = read_regular_bounded(path, MAX_RECEIPT_BYTES)?;
    validate_execution_receipt(&bytes, expected_kind, expected_fence)
}

fn write_and_validate_execution_receipt(
    path: &Path,
    bytes: &[u8],
    expected_kind: ExecutionReceiptKind,
    expected_fence: &str,
) -> Result<String, CutoverError> {
    write_new_sync(path, bytes, 0o400)?;
    validate_execution_receipt(bytes, expected_kind, expected_fence)
}

fn validate_execution_receipt(
    bytes: &[u8],
    expected_kind: ExecutionReceiptKind,
    expected_fence: &str,
) -> Result<String, CutoverError> {
    let receipt: ExecutionReceiptV1 = serde_json::from_slice(bytes)
        .map_err(|error| CutoverError::Evidence(format!("execution receipt JSON: {error}")))?;
    if receipt.schema_version != 1
        || receipt.kind != expected_kind
        || receipt.maintenance_fence_id != expected_fence
        || receipt.subject.is_empty()
        || !receipt.satisfied
        || embedded_digest(EXECUTION_RECEIPT_DIGEST_DOMAIN, &receipt)? != receipt.receipt_digest
    {
        return Err(CutoverError::Evidence(
            "execution receipt does not bind the required satisfied fence".to_owned(),
        ));
    }
    Ok(receipt.receipt_digest)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportReceiptV1 {
    schema: String,
    generation: String,
    snapshot_file_digest: String,
    contract_digest: String,
    schema_digest: String,
    external_catalog_digest: String,
    revision_baseline_digest: String,
    execution_profile_digest: String,
    source: ExportSourceV1,
    counts: CountsV1,
    excluded_actions: Vec<ExcludedActionV1>,
    receipt_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportSourceV1 {
    backend: String,
    namespace: String,
    source_version: String,
    schema_version: u64,
    maintenance_fence_id: String,
    exported_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CountsV1 {
    tables: u64,
    mfg_objects: u64,
    core_objects: u64,
    reconciliation: u64,
    excluded: u64,
}

impl CountsV1 {
    fn contract(&self) -> CutoverCountsV1 {
        CutoverCountsV1 {
            tables: self.tables,
            mfg_objects: self.mfg_objects,
            core_objects: self.core_objects,
            reconciliation: self.reconciliation,
            excluded: self.excluded,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExcludedActionV1 {
    source_table: String,
    reason: String,
    regeneration: String,
}

struct ValidatedSource {
    contract: CutoverSourceV1,
}

fn validate_exported_source(
    paths: &SourcePaths,
    request: &LegacySourceRequest,
) -> Result<ValidatedSource, CutoverError> {
    let snapshot_bytes = read_regular_bounded(&paths.snapshot, MAX_SNAPSHOT_BYTES)?;
    matrix_core::MfgOwnershipSplitSnapshotV1::decode_strict(&snapshot_bytes)
        .map_err(|error| CutoverError::Evidence(error.to_string()))?;
    let receipt_bytes = read_regular_bounded(&paths.receipt, MAX_RECEIPT_BYTES)?;
    let receipt: ExportReceiptV1 = serde_json::from_slice(&receipt_bytes)
        .map_err(|error| CutoverError::Evidence(format!("export receipt JSON: {error}")))?;
    let backend = match &request.location {
        LegacySourceLocation::Sqlite { .. } => "sqlite",
        LegacySourceLocation::Postgres { namespace, .. } => {
            if namespace != &receipt.source.namespace {
                return Err(CutoverError::Evidence(
                    "export source namespace differs from request".to_owned(),
                ));
            }
            "postgres"
        }
    };
    if receipt.schema != "OwnershipExportReceiptV1"
        || receipt.contract_digest != OWNERSHIP_CONTRACT_DIGEST
        || receipt.source.backend != backend
        || receipt.source.source_version != request.source_version
        || receipt.source.schema_version != request.schema_version
        || receipt.source.maintenance_fence_id != request.maintenance_fence_id
        || receipt.source.exported_at != request.exported_at
        || receipt.counts.tables != 46
        || receipt.counts.excluded != 3
        || receipt.excluded_actions.len() as u64 != receipt.counts.excluded
        || receipt.snapshot_file_digest != digest_bytes(&snapshot_bytes)
        || embedded_digest(EXPORT_RECEIPT_DIGEST_DOMAIN, &receipt)? != receipt.receipt_digest
    {
        return Err(CutoverError::Evidence(
            "export receipt does not bind the frozen F7.2F source snapshot".to_owned(),
        ));
    }
    for digest in [
        &receipt.snapshot_file_digest,
        &receipt.contract_digest,
        &receipt.schema_digest,
        &receipt.external_catalog_digest,
        &receipt.revision_baseline_digest,
        &receipt.execution_profile_digest,
        &receipt.receipt_digest,
    ] {
        validate_digest(digest)?;
    }
    let snapshot: Value = serde_json::from_slice(&snapshot_bytes)
        .map_err(|error| CutoverError::Evidence(format!("snapshot JSON: {error}")))?;
    let whole = json_string(&snapshot, "whole_snapshot_digest")?;
    let core = json_string(
        json_field(&snapshot, "core_matrix_domain")?,
        "section_digest",
    )?;
    let mfg = json_string(json_field(&snapshot, "mfg_domain")?, "section_digest")?;
    validate_digest(whole)?;
    validate_digest(core)?;
    validate_digest(mfg)?;
    let relative_snapshot = relative_from_root(&paths.root, &paths.snapshot)?;
    let relative_receipt = relative_from_root(&paths.root, &paths.receipt)?;
    Ok(ValidatedSource {
        contract: CutoverSourceV1 {
            backend: if backend == "sqlite" {
                CutoverBackendV1::Sqlite
            } else {
                CutoverBackendV1::Postgres
            },
            namespace: receipt.source.namespace,
            source_version: receipt.source.source_version,
            schema_version: receipt.source.schema_version,
            maintenance_fence_id: receipt.source.maintenance_fence_id,
            snapshot_relative_path: relative_snapshot,
            snapshot_whole_digest: whole.to_owned(),
            snapshot_file_digest: receipt.snapshot_file_digest,
            export_receipt_relative_path: relative_receipt,
            export_receipt_digest: receipt.receipt_digest,
            core_section_digest: core.to_owned(),
            mfg_section_digest: mfg.to_owned(),
            counts: receipt.counts.contract(),
        },
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportReceiptV1 {
    schema_version: u16,
    owner: String,
    backend: String,
    namespace: String,
    generation: String,
    ownership_contract_digest: String,
    section_digest: String,
    source_snapshot_whole_digest: String,
    source_version: String,
    source_schema_version: u64,
    maintenance_fence_id: String,
    counts: CountsV1,
    target_checkpoint: TargetCheckpointV1,
    durable: bool,
    completed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetCheckpointV1 {
    source_generation: String,
    imported_object_count: u64,
    reconciliation_count: u64,
    journal_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AclReceiptV1 {
    schema_version: u16,
    owner: TargetOwner,
    namespace: String,
    generation: String,
    runtime_role_digest: String,
    migrator_role_digest: String,
    runtime_dml_allowed: bool,
    runtime_ddl_denied: bool,
    runtime_cross_owner_denied: bool,
    public_access_denied: bool,
    receipt_digest: String,
}

struct ValidatedTarget {
    contract: CutoverTargetV1,
    acl_receipt_digest: String,
    checkpoint_digest: String,
}

fn validate_target_output(
    owner: TargetOwner,
    namespace: &str,
    generation: &str,
    source: &ValidatedSource,
    paths: &TargetPaths,
) -> Result<ValidatedTarget, CutoverError> {
    let import_bytes = read_regular_bounded(&paths.import_receipt, MAX_RECEIPT_BYTES)?;
    let receipt: ImportReceiptV1 = serde_json::from_slice(&import_bytes)
        .map_err(|error| CutoverError::Evidence(format!("import receipt JSON: {error}")))?;
    let section_digest = if owner == TargetOwner::Core {
        &source.contract.core_section_digest
    } else {
        &source.contract.mfg_section_digest
    };
    let expected_objects = if owner == TargetOwner::Core {
        source.contract.counts.core_objects
    } else {
        source.contract.counts.mfg_objects
    };
    if receipt.schema_version != 1
        || receipt.owner != owner.as_str()
        || receipt.backend != "postgres"
        || receipt.namespace != namespace
        || receipt.generation != generation
        || receipt.ownership_contract_digest != OWNERSHIP_CONTRACT_DIGEST
        || receipt.section_digest != *section_digest
        || receipt.source_snapshot_whole_digest != source.contract.snapshot_whole_digest
        || receipt.source_version != source.contract.source_version
        || receipt.source_schema_version != source.contract.schema_version
        || receipt.maintenance_fence_id != source.contract.maintenance_fence_id
        || receipt.counts.contract() != source.contract.counts
        || receipt.target_checkpoint.source_generation != source.contract.source_version
        || receipt.target_checkpoint.imported_object_count != expected_objects
        || receipt.target_checkpoint.reconciliation_count != source.contract.counts.reconciliation
        || !receipt.durable
    {
        return Err(CutoverError::Evidence(format!(
            "{} import receipt digest/count/checkpoint binding failed",
            owner.as_str()
        )));
    }
    parse_utc(&receipt.completed_at)?;
    validate_digest(&receipt.target_checkpoint.journal_digest)?;
    let checkpoint_digest = canonical_digest(
        b"cowd.ownership-cutover.target-checkpoint.v1\0",
        &serde_json::to_value(&receipt.target_checkpoint)
            .map_err(|error| CutoverError::Evidence(error.to_string()))?,
    )?;
    let acl_bytes = read_regular_bounded(&paths.acl_receipt, MAX_RECEIPT_BYTES)?;
    let acl: AclReceiptV1 = serde_json::from_slice(&acl_bytes)
        .map_err(|error| CutoverError::Evidence(format!("ACL receipt JSON: {error}")))?;
    if acl.schema_version != 1
        || acl.owner != owner
        || acl.namespace != namespace
        || acl.generation != generation
        || !acl.runtime_dml_allowed
        || !acl.runtime_ddl_denied
        || !acl.runtime_cross_owner_denied
        || !acl.public_access_denied
        || acl.runtime_role_digest == acl.migrator_role_digest
        || embedded_digest(ACL_RECEIPT_DIGEST_DOMAIN, &acl)? != acl.receipt_digest
    {
        return Err(CutoverError::Evidence(format!(
            "{} ACL verification receipt failed",
            owner.as_str()
        )));
    }
    validate_digest(&acl.runtime_role_digest)?;
    validate_digest(&acl.migrator_role_digest)?;
    let import_digest = digest_bytes(&import_bytes);
    Ok(ValidatedTarget {
        contract: CutoverTargetV1 {
            backend: CutoverBackendV1::Postgres,
            namespace: namespace.to_owned(),
            generation: generation.to_owned(),
            relative_path: relative_from_root(&paths.root, &paths.directory)?,
            section_digest: section_digest.clone(),
            durable_import_receipt_relative_path: relative_from_root(
                &paths.root,
                &paths.import_receipt,
            )?,
            durable_import_receipt_digest: import_digest,
        },
        acl_receipt_digest: acl.receipt_digest,
        checkpoint_digest,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateReceiptV1 {
    schema_version: u16,
    state: String,
    publication_generation: String,
    source_snapshot_whole_digest: String,
    core_generation: String,
    core_section_digest: String,
    core_import_receipt_digest: String,
    core_acl_receipt_digest: String,
    core_checkpoint_digest: String,
    mfg_generation: String,
    mfg_section_digest: String,
    mfg_import_receipt_digest: String,
    mfg_acl_receipt_digest: String,
    mfg_checkpoint_digest: String,
    receipt_digest: String,
}

impl StateReceiptV1 {
    fn for_pair(
        state: &str,
        publication_generation: &str,
        source: &ValidatedSource,
        core: &ValidatedTarget,
        mfg: &ValidatedTarget,
    ) -> Result<Self, CutoverError> {
        Self::new(
            state,
            publication_generation,
            &source.contract.snapshot_whole_digest,
            &core.contract,
            &core.acl_receipt_digest,
            &core.checkpoint_digest,
            &mfg.contract,
            &mfg.acl_receipt_digest,
            &mfg.checkpoint_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        state: &str,
        publication_generation: &str,
        source_snapshot_whole_digest: &str,
        core: &CutoverTargetV1,
        core_acl: &str,
        core_checkpoint: &str,
        mfg: &CutoverTargetV1,
        mfg_acl: &str,
        mfg_checkpoint: &str,
    ) -> Result<Self, CutoverError> {
        let mut receipt = Self {
            schema_version: 1,
            state: state.to_owned(),
            publication_generation: publication_generation.to_owned(),
            source_snapshot_whole_digest: source_snapshot_whole_digest.to_owned(),
            core_generation: core.generation.clone(),
            core_section_digest: core.section_digest.clone(),
            core_import_receipt_digest: core.durable_import_receipt_digest.clone(),
            core_acl_receipt_digest: core_acl.to_owned(),
            core_checkpoint_digest: core_checkpoint.to_owned(),
            mfg_generation: mfg.generation.clone(),
            mfg_section_digest: mfg.section_digest.clone(),
            mfg_import_receipt_digest: mfg.durable_import_receipt_digest.clone(),
            mfg_acl_receipt_digest: mfg_acl.to_owned(),
            mfg_checkpoint_digest: mfg_checkpoint.to_owned(),
            receipt_digest: String::new(),
        };
        receipt.receipt_digest = embedded_digest(STATE_RECEIPT_DIGEST_DOMAIN, &receipt)?;
        Ok(receipt)
    }
}

fn write_state_receipt(path: &Path, receipt: StateReceiptV1) -> Result<String, CutoverError> {
    let digest = receipt.receipt_digest.clone();
    write_new_sync(path, &pretty_json(&receipt)?, 0o400)?;
    Ok(digest)
}

struct SourcePaths {
    root: PathBuf,
    directory: PathBuf,
    snapshot: PathBuf,
    receipt: PathBuf,
}

impl SourcePaths {
    fn new(root: &Path, generation: &str) -> Result<Self, CutoverError> {
        let directory = root.join("source").join(generation);
        create_fresh_directory(&directory)?;
        Ok(Self {
            root: root.to_path_buf(),
            snapshot: directory.join("snapshot.json"),
            receipt: directory.join("export-receipt.json"),
            directory,
        })
    }
}

struct TargetPaths {
    root: PathBuf,
    directory: PathBuf,
    import_receipt: PathBuf,
    acl_receipt: PathBuf,
}

impl TargetPaths {
    fn new(root: &Path, owner: TargetOwner, generation: &str) -> Result<Self, CutoverError> {
        let directory = root
            .join("generations")
            .join(format!("{}-{generation}", owner.as_str()));
        create_fresh_directory(&directory)?;
        Ok(Self {
            root: root.to_path_buf(),
            import_receipt: directory.join("import-receipt.json"),
            acl_receipt: directory.join("acl-receipt.json"),
            directory,
        })
    }

    fn existing(root: &Path, target: &CutoverTargetV1) -> Result<Self, CutoverError> {
        let directory = safe_join(root, &target.relative_path)?;
        require_real_directory(&directory)?;
        let import_receipt = safe_join(root, &target.durable_import_receipt_relative_path)?;
        if import_receipt.parent() != Some(directory.as_path()) {
            return Err(CutoverError::InvalidState(
                "historical import receipt is outside its exact target generation",
            ));
        }
        Ok(Self {
            root: root.to_path_buf(),
            import_receipt,
            acl_receipt: directory.join("acl-receipt.json"),
            directory,
        })
    }
}

struct EvidencePaths {
    directory: PathBuf,
    maintenance_receipt: PathBuf,
    gateway_receipt: PathBuf,
    workers_receipt: PathBuf,
    readonly_receipt: PathBuf,
    staged_receipt: PathBuf,
    verified_receipt: PathBuf,
    quiescence_request: PathBuf,
    export_request: PathBuf,
    core_request: PathBuf,
    mfg_request: PathBuf,
}

impl EvidencePaths {
    fn new(root: &Path, generation: &str) -> Result<Self, CutoverError> {
        let directory = root.join("evidence").join(generation);
        create_fresh_directory(&directory)?;
        Ok(Self {
            maintenance_receipt: directory.join("maintenance-lock.json"),
            gateway_receipt: directory.join("gateway-stopped.json"),
            workers_receipt: directory.join("workers-stopped.json"),
            readonly_receipt: directory.join("legacy-readonly.json"),
            staged_receipt: directory.join("staged.json"),
            verified_receipt: directory.join("verified.json"),
            quiescence_request: directory.join("quiescence-request.json"),
            export_request: directory.join("export-request.json"),
            core_request: directory.join("core-request.json"),
            mfg_request: directory.join("mfg-request.json"),
            directory,
        })
    }
}

struct HistoryState {
    history: Vec<OwnershipCutoverManifestV1>,
    current: Option<OwnershipCutoverManifestV1>,
}

fn load_history_state(root: &Path) -> Result<HistoryState, CutoverError> {
    let publication = root.join("publication");
    let active_path = publication.join("active.json");
    if publication.exists() {
        require_real_directory(&publication)?;
        let mut names = fs::read_dir(&publication)
            .map_err(io("read publication directory", &publication))?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(io("read publication entry", &publication))?;
        names.sort();
        if names != Vec::<std::ffi::OsString>::new()
            && names != vec![std::ffi::OsString::from("active.json")]
        {
            return Err(CutoverError::InvalidState(
                "publication directory may contain only active.json",
            ));
        }
    }
    let current = if active_path.exists() {
        Some(
            OwnershipCutoverManifestV1::decode(&read_regular_bounded(
                &active_path,
                MAX_RECEIPT_BYTES,
            )?)
            .map_err(contract_error)?,
        )
    } else {
        None
    };
    let history_directory = root.join("history");
    fs::create_dir_all(&history_directory)
        .map_err(io("create history directory", &history_directory))?;
    require_real_directory(&history_directory)?;
    let mut history = Vec::new();
    for entry in fs::read_dir(&history_directory)
        .map_err(io("read history directory", &history_directory))?
    {
        let path = entry
            .map_err(io("read history entry", &history_directory))?
            .path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            return Err(CutoverError::InvalidState(
                "history directory may contain only JSON manifests",
            ));
        }
        let manifest =
            OwnershipCutoverManifestV1::decode(&read_regular_bounded(&path, MAX_RECEIPT_BYTES)?)
                .map_err(contract_error)?;
        let expected_name = format!("{}.json", manifest.publication_generation);
        if path.file_name().and_then(|value| value.to_str()) != Some(expected_name.as_str()) {
            return Err(CutoverError::InvalidState(
                "history filename differs from publication generation",
            ));
        }
        if current
            .as_ref()
            .is_none_or(|active| active.manifest_digest != manifest.manifest_digest)
        {
            history.push(manifest);
        }
    }
    history.sort_by(|left, right| left.created_at.cmp(&right.created_at));
    if current.is_none() && !history.is_empty() {
        return Err(CutoverError::InvalidState(
            "history exists without an active publication",
        ));
    }
    for index in 0..history.len() {
        let predecessor = index.checked_sub(1).map(|previous| &history[previous]);
        history[index]
            .validate(root, predecessor, &history[..index.saturating_sub(1)])
            .map_err(contract_error)?;
    }
    if current.is_some() {
        let predecessor = history.last();
        let earlier = if history.is_empty() {
            &[][..]
        } else {
            &history[..history.len() - 1]
        };
        validate_active_publication(root, predecessor, earlier).map_err(contract_error)?;
    }
    Ok(HistoryState { history, current })
}

fn publish_manifest(
    root: &Path,
    manifest: &OwnershipCutoverManifestV1,
    current: Option<&OwnershipCutoverManifestV1>,
    fault: PublishFault,
) -> Result<CutoverPublication, CutoverError> {
    if let Some(current) = current {
        archive_current(root, current)?;
    }
    let publication = root.join("publication");
    fs::create_dir_all(&publication).map_err(io("create publication directory", &publication))?;
    require_real_directory(&publication)?;
    let temporary = publication.join(format!(".active.{}.tmp", manifest.publication_generation));
    let active = publication.join("active.json");
    let bytes = pretty_json(manifest)?;
    let mut renamed = false;
    let result = (|| {
        write_new_sync(&temporary, &bytes, 0o400)?;
        sync_directory(&publication)?;
        #[cfg(test)]
        if fault == PublishFault::BeforeRename {
            return Err(CutoverError::InvalidState(
                "fault injection before active pointer rename",
            ));
        }
        let _ = fault;
        fs::rename(&temporary, &active).map_err(io("rename active pointer", &active))?;
        renamed = true;
        sync_directory(&publication)?;
        Ok(CutoverPublication {
            publication_generation: manifest.publication_generation.clone(),
            manifest_digest: manifest.manifest_digest.clone(),
            core_generation: manifest.core.generation.clone(),
            mfg_generation: manifest.mfg.generation.clone(),
            rollback: manifest.activation_kind == CutoverActivationKindV1::Rollback,
        })
    })();
    if result.is_err() && !renamed && temporary.exists() {
        let _ = fs::remove_file(&temporary);
        let _ = sync_directory(&publication);
    }
    result
}

fn archive_current(root: &Path, current: &OwnershipCutoverManifestV1) -> Result<(), CutoverError> {
    let directory = root.join("history");
    fs::create_dir_all(&directory).map_err(io("create history directory", &directory))?;
    let path = directory.join(format!("{}.json", current.publication_generation));
    let bytes = pretty_json(current)?;
    if path.exists() {
        if read_regular_bounded(&path, MAX_RECEIPT_BYTES)? != bytes {
            return Err(CutoverError::InvalidState(
                "existing history manifest differs from current active publication",
            ));
        }
    } else {
        write_new_sync(&path, &bytes, 0o400)?;
    }
    sync_directory(&directory)
}

fn previous_summary(manifest: &OwnershipCutoverManifestV1) -> PreviousCutoverV1 {
    PreviousCutoverV1 {
        publication_generation: manifest.publication_generation.clone(),
        manifest_digest: manifest.manifest_digest.clone(),
        core_generation: manifest.core.generation.clone(),
        core_relative_path: manifest.core.relative_path.clone(),
        mfg_generation: manifest.mfg.generation.clone(),
        mfg_relative_path: manifest.mfg.relative_path.clone(),
    }
}

fn prepare_active_request(request: &ActiveCutoverRequest) -> Result<PathBuf, CutoverError> {
    validate_identifier(&request.publication_generation, "publication generation")?;
    validate_identifier(&request.activation_fence_id, "activation fence")?;
    validate_identifier(&request.source.source_version, "source version")?;
    validate_identifier(&request.source.maintenance_fence_id, "maintenance fence")?;
    validate_identifier(&request.core.namespace, "Core namespace")?;
    validate_identifier(&request.core.generation, "Core generation")?;
    validate_identifier(&request.mfg.namespace, "MFG namespace")?;
    validate_identifier(&request.mfg.generation, "MFG generation")?;
    if request.source.schema_version == 0
        || request.core.namespace == request.mfg.namespace
        || request.core.generation == request.mfg.generation
    {
        return Err(CutoverError::InvalidRequest(
            "source schema must be positive and target ownership must be distinct",
        ));
    }
    parse_utc(&request.source.exported_at)?;
    parse_utc(&request.created_at)?;
    request.core.credential.validate()?;
    request.mfg.credential.validate()?;
    match &request.source.location {
        LegacySourceLocation::Sqlite { path } => validate_sqlite_source(path)?,
        LegacySourceLocation::Postgres {
            namespace,
            credential,
        } => {
            validate_identifier(namespace, "source PostgreSQL namespace")?;
            credential.validate()?;
        }
    }
    prepare_root(&request.root)
}

fn prepare_root(path: &Path) -> Result<PathBuf, CutoverError> {
    fs::create_dir_all(path).map_err(io("create cutover root", path))?;
    require_real_directory(path)?;
    fs::canonicalize(path).map_err(io("canonicalize cutover root", path))
}

fn validate_sqlite_source(path: &Path) -> Result<(), CutoverError> {
    let metadata = fs::symlink_metadata(path).map_err(io("inspect SQLite source", path))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CutoverError::InvalidRequest(
            "legacy SQLite source must be a regular non-symlink file",
        ));
    }
    Ok(())
}

fn validate_credential_file(path: &Path) -> Result<(), CutoverError> {
    let metadata = fs::symlink_metadata(path).map_err(io("inspect credential file", path))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            return Err(CutoverError::InvalidRequest(
                "credential file must be a regular non-symlink 0600 file",
            ));
        }
    }
    #[cfg(not(unix))]
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CutoverError::InvalidRequest(
            "credential file must be a regular non-symlink file",
        ));
    }
    Ok(())
}

fn validate_identifier(value: &str, _field: &str) -> Result<(), CutoverError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(CutoverError::InvalidRequest(
            "cutover identifiers must be bounded portable components",
        ));
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), CutoverError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CutoverError::Evidence(
            "digest must use canonical SHA-256".to_owned(),
        ));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CutoverError::Evidence(
            "digest must use canonical SHA-256".to_owned(),
        ));
    }
    Ok(())
}

fn parse_utc(value: &str) -> Result<DateTime<Utc>, CutoverError> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| CutoverError::InvalidRequest("timestamps must be canonical RFC3339 UTC"))?;
    if parsed.offset().local_minus_utc() != 0 || !value.ends_with('Z') {
        return Err(CutoverError::InvalidRequest(
            "timestamps must be canonical RFC3339 UTC",
        ));
    }
    Ok(parsed.with_timezone(&Utc))
}

fn execution_contract_digest() -> Result<String, CutoverError> {
    let value: Value = serde_json::from_str(include_str!(
        "../../../../contracts/ownership-cutover/v1/execution-contract.json"
    ))
    .map_err(|error| CutoverError::Evidence(error.to_string()))?;
    canonical_digest(EXECUTION_DIGEST_DOMAIN, &value)
}

fn embedded_digest<T: Serialize>(domain: &[u8], value: &T) -> Result<String, CutoverError> {
    let mut value =
        serde_json::to_value(value).map_err(|error| CutoverError::Evidence(error.to_string()))?;
    value
        .as_object_mut()
        .ok_or_else(|| CutoverError::Evidence("receipt must be an object".to_owned()))?
        .remove("receipt_digest");
    canonical_digest(domain, &value)
}

fn canonical_digest(domain: &[u8], value: &Value) -> Result<String, CutoverError> {
    let bytes = serde_json::to_vec(&canonicalize(value))
        .map_err(|error| CutoverError::Evidence(error.to_string()))?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(bytes);
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize).collect()),
        _ => value.clone(),
    }
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn pretty_json<T: Serialize>(value: &T) -> Result<Vec<u8>, CutoverError> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| CutoverError::Evidence(error.to_string()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn write_port_request(path: &Path, value: &Value) -> Result<(), CutoverError> {
    write_new_sync(path, &pretty_json(value)?, 0o400)
}

fn write_new_sync(path: &Path, bytes: &[u8], mode: u32) -> Result<(), CutoverError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(io("create evidence file", path))?;
    file.write_all(bytes)
        .map_err(io("write evidence file", path))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(mode))
            .map_err(io("set evidence permissions", path))?;
    }
    let _ = mode;
    file.sync_all().map_err(io("sync evidence file", path))
}

fn read_regular_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, CutoverError> {
    let metadata = fs::symlink_metadata(path).map_err(io("inspect evidence file", path))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > maximum {
        return Err(CutoverError::Evidence(format!(
            "{} is not a bounded regular evidence file",
            path.display()
        )));
    }
    fs::read(path).map_err(io("read evidence file", path))
}

fn create_fresh_directory(path: &Path) -> Result<(), CutoverError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io("create staging parent", parent))?;
        require_real_directory(parent)?;
    }
    fs::create_dir(path).map_err(io("create fresh staging directory", path))?;
    require_real_directory(path)
}

fn require_real_directory(path: &Path) -> Result<(), CutoverError> {
    let metadata = fs::symlink_metadata(path).map_err(io("inspect directory", path))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CutoverError::InvalidState(
            "cutover directories must be real directories",
        ));
    }
    Ok(())
}

fn relative_from_root(root: &Path, path: &Path) -> Result<String, CutoverError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| CutoverError::InvalidState("evidence path escaped cutover root"))?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CutoverError::InvalidState(
            "evidence path is not normalized beneath cutover root",
        ));
    }
    relative
        .to_str()
        .map(|value| value.replace('\\', "/"))
        .ok_or(CutoverError::InvalidState("evidence paths must be UTF-8"))
}

fn sync_cutover_evidence(
    source: &SourcePaths,
    core: &TargetPaths,
    mfg: &TargetPaths,
    evidence: &EvidencePaths,
) -> Result<(), CutoverError> {
    sync_tree(&source.directory)?;
    sync_tree(&core.directory)?;
    sync_tree(&mfg.directory)?;
    sync_tree(&evidence.directory)
}

fn sync_existing_target(root: &Path, target: &CutoverTargetV1) -> Result<(), CutoverError> {
    let path = safe_join(root, &target.relative_path)?;
    sync_tree(&path)
}

fn sync_tree(root: &Path) -> Result<(), CutoverError> {
    require_real_directory(root)?;
    let mut directories = vec![root.to_path_buf()];
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory).map_err(io("read sync tree", &directory))? {
            let path = entry
                .map_err(io("read sync tree entry", &directory))?
                .path();
            let metadata = fs::symlink_metadata(&path).map_err(io("inspect sync node", &path))?;
            if metadata.file_type().is_symlink() {
                return Err(CutoverError::Evidence(
                    "symlink in cutover evidence tree".to_owned(),
                ));
            }
            if metadata.is_dir() {
                directories.push(path.clone());
                stack.push(path);
            } else if metadata.is_file() {
                File::open(&path)
                    .and_then(|file| file.sync_all())
                    .map_err(io("sync evidence node", &path))?;
            } else {
                return Err(CutoverError::Evidence(
                    "special node in cutover evidence tree".to_owned(),
                ));
            }
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), CutoverError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(io("sync directory", path))
}

fn safe_join(root: &Path, relative: &str) -> Result<PathBuf, CutoverError> {
    let mut path = root.to_path_buf();
    let relative = Path::new(relative);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CutoverError::InvalidState(
            "historical target path is not normalized",
        ));
    }
    path.push(relative);
    Ok(path)
}

fn export_request_value(source: &LegacySourceRequest, paths: &SourcePaths) -> Value {
    let location = match &source.location {
        LegacySourceLocation::Sqlite { path } => {
            serde_json::json!({"backend":"sqlite","path":path})
        }
        LegacySourceLocation::Postgres { namespace, .. } => {
            serde_json::json!({"backend":"postgres","namespace":namespace})
        }
    };
    serde_json::json!({
        "schema_version": 1,
        "operation": "export_readonly",
        "location": location,
        "source_version": source.source_version,
        "source_schema_version": source.schema_version,
        "maintenance_fence_id": source.maintenance_fence_id,
        "exported_at": source.exported_at,
        "snapshot_path": paths.snapshot,
        "export_receipt_path": paths.receipt,
    })
}

fn json_field<'a>(value: &'a Value, field: &str) -> Result<&'a Value, CutoverError> {
    value
        .get(field)
        .ok_or_else(|| CutoverError::Evidence(format!("snapshot is missing {field}")))
}

fn json_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, CutoverError> {
    json_field(value, field)?
        .as_str()
        .ok_or_else(|| CutoverError::Evidence(format!("snapshot field {field} is not a string")))
}

fn io<'a>(
    operation: &'static str,
    path: &'a Path,
) -> impl FnOnce(std::io::Error) -> CutoverError + 'a {
    move |source| CutoverError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use rusqlite::{Connection, OpenFlags};
    use serial_test::serial;
    use storage::{PostgresConnectionConfig, PostgresExecutor, StaticSecretRefResolver};
    use tempfile::TempDir;

    use super::*;

    const NO_TAMPER: u8 = 0;
    const TAMPER_CORE_ACL: u8 = 1;

    #[test]
    fn operator_arguments_accept_only_request_path_and_one_credential_channel() {
        let parsed = OperatorArguments::parse(&[
            "activate".to_owned(),
            "--request".to_owned(),
            "/tmp/request.json".to_owned(),
            "--credential-env".to_owned(),
            "COWD_TEST_POSTGRES_URL".to_owned(),
        ])
        .expect("bounded operator arguments");
        assert_eq!(parsed.action, OperatorAction::Activate);
        assert_eq!(parsed.request_path, PathBuf::from("/tmp/request.json"));
        assert!(matches!(
            parsed.credential,
            Some(CredentialSource::Environment { ref variable })
                if variable == "COWD_TEST_POSTGRES_URL"
        ));

        for args in [
            vec![
                "activate".to_owned(),
                "--request".to_owned(),
                "/tmp/request.json".to_owned(),
                "--credential".to_owned(),
                "secret".to_owned(),
            ],
            vec![
                "activate".to_owned(),
                "--request".to_owned(),
                "/tmp/request.json".to_owned(),
                "--credential-stdin".to_owned(),
                "--credential-env".to_owned(),
                "COWD_TEST_POSTGRES_URL".to_owned(),
            ],
        ] {
            assert!(OperatorArguments::parse(&args).is_err());
        }
    }

    #[test]
    fn rollback_rejects_credential_channel_before_running_ports() {
        let temp = TempDir::new().expect("temp");
        let request = temp.path().join("rollback.json");
        fs::write(
            &request,
            br#"{"schema_version":1,"external_program":"/missing","root":"/tmp/cutover","publication_generation":"rollback-1","activation_fence_id":"fence-1","created_at":"2026-08-15T00:00:00Z","target_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
        )
        .expect("request");
        let error = run_operator_command(&[
            "rollback".to_owned(),
            "--request".to_owned(),
            request.display().to_string(),
            "--credential-stdin".to_owned(),
        ])
        .expect_err("unused rollback credential");
        assert!(matches!(error, CutoverError::InvalidRequest(_)));
    }

    struct FixturePorts {
        sqlite_was_readonly: AtomicBool,
        imports: AtomicUsize,
        tamper: AtomicU8,
    }

    impl Default for FixturePorts {
        fn default() -> Self {
            Self {
                sqlite_was_readonly: AtomicBool::new(false),
                imports: AtomicUsize::new(0),
                tamper: AtomicU8::new(NO_TAMPER),
            }
        }
    }

    impl QuiescencePort for FixturePorts {
        fn confirm_stopped(&self, request: &QuiescenceRequest<'_>) -> Result<(), PortFailure> {
            write_test_receipt(
                request.gateway_receipt_path,
                ExecutionReceiptKind::GatewayStopped,
                request.maintenance_fence_id,
                "gateway",
            )?;
            write_test_receipt(
                request.workers_receipt_path,
                ExecutionReceiptKind::WorkersStopped,
                request.maintenance_fence_id,
                "workers",
            )
        }
    }

    impl LegacyExporterPort for FixturePorts {
        fn export_readonly(
            &self,
            request: &LegacyExportPortRequest<'_>,
        ) -> Result<(), PortFailure> {
            let LegacySourceLocation::Sqlite { path } = &request.source.location else {
                return Err(PortFailure::new(PortFailureCode::ExportRejected));
            };
            let connection = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|_| PortFailure::new(PortFailureCode::SourceMutable))?;
            connection
                .execute_batch("PRAGMA query_only = ON")
                .map_err(|_| PortFailure::new(PortFailureCode::SourceMutable))?;
            let query_only: i64 = connection
                .query_row("PRAGMA query_only", [], |row| row.get(0))
                .map_err(|_| PortFailure::new(PortFailureCode::SourceMutable))?;
            let tables: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|_| PortFailure::new(PortFailureCode::ExportRejected))?;
            if query_only != 1 || tables != 46 {
                return Err(PortFailure::new(PortFailureCode::SourceMutable));
            }
            self.sqlite_was_readonly.store(true, Ordering::SeqCst);
            let fixture = if request.source.source_version == "fixture-1" {
                "v0"
            } else if request.source.source_version == "fixture-v1" {
                "v1"
            } else {
                return Err(PortFailure::new(PortFailureCode::ExportRejected));
            };
            fs::copy(
                golden(&format!("source/{fixture}/snapshot.json")),
                request.snapshot_path,
            )
            .and_then(|_| {
                fs::copy(
                    golden(&format!("source/{fixture}/export-receipt.json")),
                    request.export_receipt_path,
                )
            })
            .map_err(|_| PortFailure::new(PortFailureCode::ExportRejected))?;
            write_test_receipt(
                request.readonly_receipt_path,
                ExecutionReceiptKind::LegacyReadonly,
                &request.source.maintenance_fence_id,
                "legacy-source",
            )
        }

        fn attest_readonly(
            &self,
            maintenance_fence_id: &str,
            receipt_path: &Path,
            _request_file: &Path,
        ) -> Result<(), PortFailure> {
            write_test_receipt(
                receipt_path,
                ExecutionReceiptKind::LegacyReadonly,
                maintenance_fence_id,
                "legacy-source",
            )
        }
    }

    impl CoreImporterPort for FixturePorts {
        fn initialize_import_and_verify(
            &self,
            request: &TargetImportPortRequest<'_>,
        ) -> Result<(), PortFailure> {
            self.import_fixture(request)
        }
    }

    impl MfgAdminPort for FixturePorts {
        fn initialize_import_and_verify(
            &self,
            request: &TargetImportPortRequest<'_>,
        ) -> Result<(), PortFailure> {
            self.import_fixture(request)
        }
    }

    impl FixturePorts {
        fn import_fixture(&self, request: &TargetImportPortRequest<'_>) -> Result<(), PortFailure> {
            self.imports.fetch_add(1, Ordering::SeqCst);
            let fixture = format!(
                "generations/{}/import-receipt.json",
                request.target.generation
            );
            fs::copy(golden(&fixture), request.import_receipt_path)
                .map_err(|_| PortFailure::new(PortFailureCode::ImportRejected))?;
            let mut acl = AclReceiptV1 {
                schema_version: 1,
                owner: request.owner,
                namespace: request.target.namespace.clone(),
                generation: request.target.generation.clone(),
                runtime_role_digest: digest_bytes(
                    format!("{}-runtime", request.owner.as_str()).as_bytes(),
                ),
                migrator_role_digest: digest_bytes(
                    format!("{}-migrator", request.owner.as_str()).as_bytes(),
                ),
                runtime_dml_allowed: true,
                runtime_ddl_denied: true,
                runtime_cross_owner_denied: true,
                public_access_denied: true,
                receipt_digest: String::new(),
            };
            acl.receipt_digest = embedded_digest(ACL_RECEIPT_DIGEST_DOMAIN, &acl)
                .map_err(|_| PortFailure::new(PortFailureCode::VerificationRejected))?;
            if self.tamper.load(Ordering::SeqCst) == TAMPER_CORE_ACL
                && request.owner == TargetOwner::Core
            {
                acl.runtime_ddl_denied = false;
            }
            write_new_sync(
                request.acl_receipt_path,
                &pretty_json(&acl)
                    .map_err(|_| PortFailure::new(PortFailureCode::VerificationRejected))?,
                0o400,
            )
            .map_err(|_| PortFailure::new(PortFailureCode::VerificationRejected))
        }
    }

    fn write_test_receipt(
        path: &Path,
        kind: ExecutionReceiptKind,
        fence: &str,
        subject: &str,
    ) -> Result<(), PortFailure> {
        let bytes = execution_receipt(kind, fence, subject)
            .map_err(|_| PortFailure::new(PortFailureCode::VerificationRejected))?;
        write_new_sync(path, &bytes, 0o400)
            .map_err(|_| PortFailure::new(PortFailureCode::VerificationRejected))
    }

    fn golden(relative: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../contracts/ownership-cutover/v1/golden")
            .join(relative)
    }

    fn real_sqlite(directory: &Path) -> PathBuf {
        let path = directory.join("legacy.sqlite3");
        let connection = Connection::open(&path).expect("create real SQLite fixture");
        for index in 0..46 {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE source_{index:02} (id INTEGER PRIMARY KEY, value TEXT NOT NULL);"
                ))
                .expect("create source table");
        }
        connection
            .execute(
                "INSERT INTO source_00(value) VALUES ('immutable-source')",
                [],
            )
            .expect("seed source");
        drop(connection);
        path
    }

    fn target(namespace: &str, generation: &str) -> TargetGenerationRequest {
        TargetGenerationRequest {
            namespace: namespace.to_owned(),
            generation: generation.to_owned(),
            credential: CredentialSource::Environment {
                variable: "COWD_TEST_POSTGRES_URL".to_owned(),
            },
        }
    }

    fn active_request(root: &Path, sqlite: &Path, version: u8) -> ActiveCutoverRequest {
        let (publication, source_version, fence, exported_at, core, mfg, created_at) =
            if version == 0 {
                (
                    "active-v0",
                    "fixture-1",
                    "fixture-fence",
                    "2026-08-15T00:00:00Z",
                    "core-v0",
                    "mfg-v0",
                    "2026-08-15T01:00:00Z",
                )
            } else {
                (
                    "active-v1",
                    "fixture-v1",
                    "fence-v1",
                    "2026-08-15T02:00:00Z",
                    "core-v1",
                    "mfg-v1",
                    "2026-08-15T03:00:00Z",
                )
            };
        ActiveCutoverRequest {
            root: root.to_path_buf(),
            publication_generation: publication.to_owned(),
            activation_fence_id: format!("activation-{version}"),
            created_at: created_at.to_owned(),
            source: LegacySourceRequest {
                location: LegacySourceLocation::Sqlite {
                    path: sqlite.to_path_buf(),
                },
                source_version: source_version.to_owned(),
                schema_version: 1,
                maintenance_fence_id: fence.to_owned(),
                exported_at: exported_at.to_owned(),
            },
            core: target("cowd_core", core),
            mfg: target("cowd_mfg", mfg),
        }
    }

    fn coordinator<'a>(ports: &'a FixturePorts) -> OwnershipCutoverCoordinator<'a> {
        OwnershipCutoverCoordinator::new(&FileMaintenancePort, ports, ports, ports, ports)
    }

    #[test]
    fn real_sqlite_cutover_publishes_only_one_atomic_active_pointer() {
        let temp = TempDir::new().expect("temporary cutover root");
        let sqlite = real_sqlite(temp.path());
        let root = temp.path().join("cutover");
        let ports = FixturePorts::default();

        let publication = coordinator(&ports)
            .activate(active_request(&root, &sqlite, 0))
            .expect("activate exact frozen receipts");

        assert_eq!(publication.core_generation, "core-v0");
        assert_eq!(publication.mfg_generation, "mfg-v0");
        assert!(ports.sqlite_was_readonly.load(Ordering::SeqCst));
        assert_eq!(ports.imports.load(Ordering::SeqCst), 2);
        let names = fs::read_dir(root.join("publication"))
            .expect("publication directory")
            .map(|entry| entry.expect("publication entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(names, vec![std::ffi::OsString::from("active.json")]);
        validate_active_publication(&root, None, &[]).expect("published contract validates");
    }

    #[test]
    fn tampered_acl_leaves_old_active_pointer_byte_exact() {
        let temp = TempDir::new().expect("temporary cutover root");
        let sqlite = real_sqlite(temp.path());
        let root = temp.path().join("cutover");
        let ports = FixturePorts::default();
        coordinator(&ports)
            .activate(active_request(&root, &sqlite, 0))
            .expect("initial activation");
        let active = root.join("publication/active.json");
        let old_bytes = fs::read(&active).expect("read old active");
        ports.tamper.store(TAMPER_CORE_ACL, Ordering::SeqCst);

        let error = coordinator(&ports)
            .activate(active_request(&root, &sqlite, 1))
            .expect_err("tampered ACL must fail");

        assert!(matches!(error, CutoverError::Evidence(_)));
        assert_eq!(fs::read(active).expect("old active remains"), old_bytes);
    }

    #[test]
    fn crash_before_rename_preserves_old_generation() {
        let temp = TempDir::new().expect("temporary cutover root");
        let sqlite = real_sqlite(temp.path());
        let root = temp.path().join("cutover");
        let ports = FixturePorts::default();
        coordinator(&ports)
            .activate(active_request(&root, &sqlite, 0))
            .expect("initial activation");
        let active = root.join("publication/active.json");
        let old_bytes = fs::read(&active).expect("read old active");
        let mut cutover = coordinator(&ports);
        cutover.fault = PublishFault::BeforeRename;

        cutover
            .activate(active_request(&root, &sqlite, 1))
            .expect_err("injected pre-rename crash");

        assert_eq!(fs::read(active).expect("old active remains"), old_bytes);
        assert_eq!(
            fs::read_dir(root.join("publication"))
                .expect("publication directory")
                .count(),
            1
        );
    }

    #[test]
    fn rollback_publishes_new_pointer_to_exact_history_pair_without_import_or_delete() {
        let temp = TempDir::new().expect("temporary cutover root");
        let sqlite = real_sqlite(temp.path());
        let root = temp.path().join("cutover");
        let ports = FixturePorts::default();
        let v0 = coordinator(&ports)
            .activate(active_request(&root, &sqlite, 0))
            .expect("activate v0");
        coordinator(&ports)
            .activate(active_request(&root, &sqlite, 1))
            .expect("activate v1");
        assert_eq!(ports.imports.load(Ordering::SeqCst), 4);

        let rollback = coordinator(&ports)
            .rollback(RollbackCutoverRequest {
                root: root.clone(),
                publication_generation: "rollback-v2".to_owned(),
                activation_fence_id: "activation-rollback-v2".to_owned(),
                created_at: "2026-08-15T05:00:00Z".to_owned(),
                target_manifest_digest: v0.manifest_digest,
            })
            .expect("rollback exact historical pair");

        assert!(rollback.rollback);
        assert_eq!(rollback.core_generation, "core-v0");
        assert_eq!(rollback.mfg_generation, "mfg-v0");
        assert_eq!(ports.imports.load(Ordering::SeqCst), 4);
        assert!(root.join("generations/core-core-v1").is_dir());
        assert!(root.join("generations/mfg-mfg-v1").is_dir());
    }

    #[cfg(unix)]
    #[test]
    #[serial]
    fn external_program_keeps_environment_credential_out_of_argv_and_diagnostics() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temporary program root");
        let program_path = temp.path().join("admin.sh");
        fs::write(
            &program_path,
            b"#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$3/argv.txt\"\nprintf '%s' \"$COWD_CUTOVER_CREDENTIAL_ENV\" > \"$3/channel.txt\"\nprintf '%s' \"$C07B_TEST_SECRET\" > \"$3/seen.txt\"\n",
        )
        .expect("write test program");
        fs::set_permissions(&program_path, fs::Permissions::from_mode(0o700))
            .expect("make program executable");
        let request_path = temp.path().join("request.json");
        fs::write(&request_path, b"{}\n").expect("write request");
        let output = temp.path().join("output");
        fs::create_dir(&output).expect("create output");
        let secret = "credential-must-never-enter-argv";
        std::env::set_var("C07B_TEST_SECRET", secret);
        let program = ExternalOwnershipProgram::new(program_path).expect("validate program");

        let result = program.run(
            "probe",
            &request_path,
            &output,
            Some(&CredentialSource::Environment {
                variable: "C07B_TEST_SECRET".to_owned(),
            }),
        );
        std::env::remove_var("C07B_TEST_SECRET");
        result.expect("run external program");

        let argv = fs::read_to_string(output.join("argv.txt")).expect("argv evidence");
        assert!(!argv.contains(secret));
        assert_eq!(
            fs::read_to_string(output.join("channel.txt")).expect("channel evidence"),
            "C07B_TEST_SECRET"
        );
        assert_eq!(
            fs::read_to_string(output.join("seen.txt")).expect("child receives credential"),
            secret
        );
        assert!(!fs::read_to_string(request_path)
            .expect("request remains")
            .contains(secret));
    }

    struct PostgresFixturePorts {
        fixture: FixturePorts,
        executor: PostgresExecutor,
    }

    impl QuiescencePort for PostgresFixturePorts {
        fn confirm_stopped(&self, request: &QuiescenceRequest<'_>) -> Result<(), PortFailure> {
            self.fixture.confirm_stopped(request)
        }
    }

    impl LegacyExporterPort for PostgresFixturePorts {
        fn export_readonly(
            &self,
            request: &LegacyExportPortRequest<'_>,
        ) -> Result<(), PortFailure> {
            self.fixture.export_readonly(request)
        }

        fn attest_readonly(
            &self,
            maintenance_fence_id: &str,
            receipt_path: &Path,
            request_file: &Path,
        ) -> Result<(), PortFailure> {
            self.fixture
                .attest_readonly(maintenance_fence_id, receipt_path, request_file)
        }
    }

    impl CoreImporterPort for PostgresFixturePorts {
        fn initialize_import_and_verify(
            &self,
            request: &TargetImportPortRequest<'_>,
        ) -> Result<(), PortFailure> {
            self.import_postgres(request)
        }
    }

    impl MfgAdminPort for PostgresFixturePorts {
        fn initialize_import_and_verify(
            &self,
            request: &TargetImportPortRequest<'_>,
        ) -> Result<(), PortFailure> {
            self.import_postgres(request)
        }
    }

    impl PostgresFixturePorts {
        fn import_postgres(
            &self,
            request: &TargetImportPortRequest<'_>,
        ) -> Result<(), PortFailure> {
            let failure = || PortFailure::new(PortFailureCode::ImportRejected);
            let snapshot: Value = serde_json::from_slice(
                &fs::read(request.source_snapshot_path).map_err(|_| failure())?,
            )
            .map_err(|_| failure())?;
            let export: ExportReceiptV1 = serde_json::from_slice(
                &fs::read(request.source_export_receipt_path).map_err(|_| failure())?,
            )
            .map_err(|_| failure())?;
            let whole = json_string(&snapshot, "whole_snapshot_digest").map_err(|_| failure())?;
            let section = if request.owner == TargetOwner::Core {
                json_string(
                    json_field(&snapshot, "core_matrix_domain").map_err(|_| failure())?,
                    "section_digest",
                )
            } else {
                json_string(
                    json_field(&snapshot, "mfg_domain").map_err(|_| failure())?,
                    "section_digest",
                )
            }
            .map_err(|_| failure())?;
            let imported = if request.owner == TargetOwner::Core {
                export.counts.core_objects
            } else {
                export.counts.mfg_objects
            };
            let namespace = &request.target.namespace;
            let runtime_role = format!("{namespace}_runtime");
            let migrator_role = format!("{namespace}_migrator");
            let peer_schema = format!("{namespace}_peer");
            for identifier in [namespace, &runtime_role, &migrator_role, &peer_schema] {
                validate_identifier(identifier, "PostgreSQL test identifier")
                    .map_err(|_| failure())?;
            }
            let checkpoint_table = format!("{namespace}.ownership_cutover_checkpoint");
            let mut connection = self.executor.checkout_critical().map_err(|_| failure())?;
            let mut transaction = connection.transaction().map_err(|_| failure())?;
            transaction
                .batch_execute(&format!(
                    "SET LOCAL synchronous_commit = on;
                     CREATE ROLE \"{runtime_role}\" NOLOGIN;
                     CREATE ROLE \"{migrator_role}\" NOLOGIN;
                     CREATE SCHEMA \"{namespace}\";
                     CREATE SCHEMA \"{peer_schema}\";
                     CREATE TABLE \"{namespace}\".ownership_cutover_checkpoint (
                         source_generation TEXT PRIMARY KEY,
                         imported_object_count BIGINT NOT NULL,
                         reconciliation_count BIGINT NOT NULL,
                         section_digest TEXT NOT NULL
                     );
                     INSERT INTO \"{namespace}\".ownership_cutover_checkpoint
                         VALUES ('{}', {imported}, {}, '{section}');
                     REVOKE ALL ON SCHEMA \"{namespace}\" FROM PUBLIC;
                     REVOKE ALL ON SCHEMA \"{peer_schema}\" FROM PUBLIC;
                     GRANT USAGE ON SCHEMA \"{namespace}\" TO \"{runtime_role}\";
                     GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{namespace}\"
                         TO \"{runtime_role}\";
                     GRANT USAGE, CREATE ON SCHEMA \"{namespace}\" TO \"{migrator_role}\";",
                    export.source.source_version, export.counts.reconciliation,
                ))
                .map_err(|_| failure())?;
            transaction.commit().map_err(|_| failure())?;

            let row = connection
                .query_one(
                    &format!(
                        "SELECT source_generation, imported_object_count, reconciliation_count,
                                section_digest
                         FROM \"{namespace}\".ownership_cutover_checkpoint"
                    ),
                    &[],
                )
                .map_err(|_| failure())?;
            let stored_source: String = row.get(0);
            let stored_imported: i64 = row.get(1);
            let stored_reconciliation: i64 = row.get(2);
            let stored_section: String = row.get(3);
            if stored_source != export.source.source_version
                || stored_imported != imported as i64
                || stored_reconciliation != export.counts.reconciliation as i64
                || stored_section != section
            {
                return Err(PortFailure::new(PortFailureCode::VerificationRejected));
            }
            let acl_row = connection
                .query_one(
                    "SELECT
                         has_table_privilege($1, $2, 'SELECT')
                           AND has_table_privilege($1, $2, 'INSERT')
                           AND has_table_privilege($1, $2, 'UPDATE')
                           AND has_table_privilege($1, $2, 'DELETE'),
                         NOT has_schema_privilege($1, $3, 'CREATE'),
                         NOT has_schema_privilege($1, $4, 'USAGE'),
                         NOT has_schema_privilege('public', $3, 'USAGE')",
                    &[&runtime_role, &checkpoint_table, namespace, &peer_schema],
                )
                .map_err(|_| failure())?;
            let runtime_dml_allowed: bool = acl_row.get(0);
            let runtime_ddl_denied: bool = acl_row.get(1);
            let runtime_cross_owner_denied: bool = acl_row.get(2);
            let public_access_denied: bool = acl_row.get(3);
            if !runtime_dml_allowed
                || !runtime_ddl_denied
                || !runtime_cross_owner_denied
                || !public_access_denied
            {
                return Err(PortFailure::new(PortFailureCode::VerificationRejected));
            }
            let journal_digest = digest_bytes(
                format!(
                    "{stored_source}\0{stored_imported}\0{stored_reconciliation}\0{stored_section}"
                )
                .as_bytes(),
            );
            let receipt = ImportReceiptV1 {
                schema_version: 1,
                owner: request.owner.as_str().to_owned(),
                backend: "postgres".to_owned(),
                namespace: namespace.clone(),
                generation: request.target.generation.clone(),
                ownership_contract_digest: OWNERSHIP_CONTRACT_DIGEST.to_owned(),
                section_digest: section.to_owned(),
                source_snapshot_whole_digest: whole.to_owned(),
                source_version: export.source.source_version,
                source_schema_version: export.source.schema_version,
                maintenance_fence_id: export.source.maintenance_fence_id,
                counts: export.counts,
                target_checkpoint: TargetCheckpointV1 {
                    source_generation: stored_source,
                    imported_object_count: imported,
                    reconciliation_count: stored_reconciliation as u64,
                    journal_digest,
                },
                durable: true,
                completed_at: "2026-08-15T00:30:00Z".to_owned(),
            };
            write_new_sync(
                request.import_receipt_path,
                &pretty_json(&receipt).map_err(|_| failure())?,
                0o400,
            )
            .map_err(|_| failure())?;
            let mut acl = AclReceiptV1 {
                schema_version: 1,
                owner: request.owner,
                namespace: namespace.clone(),
                generation: request.target.generation.clone(),
                runtime_role_digest: digest_bytes(runtime_role.as_bytes()),
                migrator_role_digest: digest_bytes(migrator_role.as_bytes()),
                runtime_dml_allowed,
                runtime_ddl_denied,
                runtime_cross_owner_denied,
                public_access_denied,
                receipt_digest: String::new(),
            };
            acl.receipt_digest =
                embedded_digest(ACL_RECEIPT_DIGEST_DOMAIN, &acl).map_err(|_| failure())?;
            write_new_sync(
                request.acl_receipt_path,
                &pretty_json(&acl).map_err(|_| failure())?,
                0o400,
            )
            .map_err(|_| failure())
        }

        fn cleanup(&self, namespaces: &[&str]) {
            let mut connection = self.executor.checkout_critical().expect("cleanup checkout");
            for namespace in namespaces {
                connection
                    .batch_execute(&format!(
                        "DROP SCHEMA IF EXISTS \"{namespace}\" CASCADE;
                         DROP SCHEMA IF EXISTS \"{namespace}_peer\" CASCADE;
                         DROP ROLE IF EXISTS \"{namespace}_runtime\";
                         DROP ROLE IF EXISTS \"{namespace}_migrator\";"
                    ))
                    .expect("clean isolated PostgreSQL fixture");
            }
        }
    }

    #[test]
    #[ignore = "requires COWD_TEST_POSTGRES_URL (the 127.0.0.1:55432 ownership database)"]
    #[serial]
    fn postgres_55432_transaction_checkpoint_and_acl_gate_publish() {
        let url = std::env::var("COWD_TEST_POSTGRES_URL")
            .expect("set COWD_TEST_POSTGRES_URL through an environment or file-backed secret");
        let resolver = StaticSecretRefResolver::new([("c07b-test-secret".to_owned(), url)]);
        let mut config = PostgresConnectionConfig::new(
            "c07b-cutover-test",
            "c07b-test-secret",
            "cowd-c07b-cutover-test",
        );
        config.max_connections = 1;
        config.min_idle_connections = Some(1);
        let executor = PostgresExecutor::connect(config, &resolver).expect("connect PostgreSQL");
        let suffix = format!(
            "{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
                % 1_000_000
        );
        let core_namespace = format!("c07b_core_{suffix}");
        let mfg_namespace = format!("c07b_mfg_{suffix}");
        let ports = PostgresFixturePorts {
            fixture: FixturePorts::default(),
            executor,
        };
        ports.cleanup(&[&core_namespace, &mfg_namespace]);
        let temp = TempDir::new().expect("temporary cutover root");
        let sqlite = real_sqlite(temp.path());
        let root = temp.path().join("cutover");
        let mut request = active_request(&root, &sqlite, 0);
        request.publication_generation = format!("pg-{suffix}");
        request.core = target(&core_namespace, &format!("core-pg-{suffix}"));
        request.mfg = target(&mfg_namespace, &format!("mfg-pg-{suffix}"));
        let coordinator =
            OwnershipCutoverCoordinator::new(&FileMaintenancePort, &ports, &ports, &ports, &ports);

        let publication = coordinator
            .activate(request)
            .expect("publish only after both PostgreSQL transactions and ACL checks");

        assert_eq!(publication.core_generation, format!("core-pg-{suffix}"));
        assert_eq!(publication.mfg_generation, format!("mfg-pg-{suffix}"));
        assert!(ports.fixture.sqlite_was_readonly.load(Ordering::SeqCst));
        validate_active_publication(&root, None, &[]).expect("published PG contract validates");
        ports.cleanup(&[&core_namespace, &mfg_namespace]);
    }
}
