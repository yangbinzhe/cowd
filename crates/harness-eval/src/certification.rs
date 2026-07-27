//! Fail-closed certification over independently observed artifacts.
//!
//! Sources are collected before expectations are evaluated. Expected values
//! are comparison operands only and cannot populate an observed event,
//! receipt, database result, process log, or provider/tool trace.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::report_store::now_ms;

const CERTIFICATION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertificationManifest {
    pub schema_version: u32,
    pub id: String,
    pub run_id: String,
    pub capability: String,
    pub fixture: CertificationFixture,
    pub seed: u64,
    pub command: CertificationCommand,
    pub expected_events: Vec<String>,
    pub forbidden_events: Vec<String>,
    pub evidence_paths: Vec<PathBuf>,
    pub timeout_policy: CertificationTimeoutPolicy,
    pub failure_code: Option<String>,
    pub provider_requirement: CertificationProviderRequirement,
    pub load_levels: Vec<u32>,
    pub baseline_commit: String,
    pub pass_thresholds: BTreeMap<String, f64>,
    pub sources: Vec<CertificationSource>,
    pub checks: Vec<CertificationCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertificationFixture {
    pub id: String,
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertificationCommand {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertificationTimeoutPolicy {
    pub scenario_ms: u64,
    pub source_default_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificationProviderRequirement {
    None,
    Configured,
    Real,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertificationSource {
    pub id: String,
    pub kind: CertificationSourceKind,
    #[serde(default = "default_required")]
    pub required: bool,
    pub collector: CertificationCollector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificationSourceKind {
    RuntimeEvents,
    DatabaseState,
    ProcessLog,
    ProviderTrace,
    ToolTrace,
    SurfaceReceipt,
    RuntimeHealth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CertificationCollector {
    File {
        path: PathBuf,
    },
    HttpJson {
        url: String,
        token_env: Option<String>,
        timeout_ms: Option<u64>,
    },
    Command {
        program: String,
        #[serde(default)]
        args: Vec<String>,
        cwd: Option<PathBuf>,
        timeout_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertificationCheck {
    pub id: String,
    pub source_id: String,
    #[serde(default = "default_required")]
    pub required: bool,
    pub selector: CertificationSelector,
    pub comparison: CertificationComparison,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CertificationSelector {
    JsonPointer { pointer: String },
    JsonPointerLength { pointer: String },
    EventKindCount { kind: String },
    Text,
    HttpStatus,
    ExitCode,
    ByteLength,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CertificationComparison {
    Exists,
    NonEmpty,
    Equals {
        expected: Value,
    },
    Contains {
        expected: String,
    },
    AtLeast {
        expected: f64,
    },
    AtMost {
        expected: f64,
    },
    EqualsObserved {
        source_id: String,
        selector: CertificationSelector,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CertificationReport {
    pub kind: String,
    pub schema_version: u32,
    pub scenario_id: String,
    pub run_id: String,
    pub capability: String,
    pub fixture: CertificationFixture,
    pub seed: u64,
    pub command: CertificationCommand,
    pub expected_events: Vec<String>,
    pub forbidden_events: Vec<String>,
    pub evidence_paths: Vec<PathBuf>,
    pub timeout_policy: CertificationTimeoutPolicy,
    pub failure_code: Option<String>,
    pub provider_requirement: CertificationProviderRequirement,
    pub load_levels: Vec<u32>,
    pub baseline_commit: String,
    pub pass_thresholds: BTreeMap<String, f64>,
    pub status: String,
    pub started_at_ms: u128,
    pub finished_at_ms: u128,
    pub elapsed_ms: u128,
    pub manifest_path: String,
    pub manifest_artifact: String,
    pub manifest_sha256: String,
    pub output_dir: String,
    pub source_results: Vec<CertificationSourceResult>,
    pub check_results: Vec<CertificationCheckResult>,
    pub required_source_failures: usize,
    pub required_check_failures: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CertificationSourceResult {
    pub id: String,
    pub kind: CertificationSourceKind,
    pub required: bool,
    pub status: String,
    pub collector: String,
    pub raw_artifact: Option<String>,
    pub sha256: Option<String>,
    pub byte_length: usize,
    pub http_status: Option<u16>,
    pub exit_code: Option<i32>,
    pub collected_at_ms: u128,
    pub elapsed_ms: u128,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CertificationCheckResult {
    pub id: String,
    pub source_id: String,
    pub required: bool,
    pub status: String,
    pub selector: CertificationSelector,
    pub comparison: CertificationComparison,
    pub observed: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug)]
struct CollectedSource {
    result: CertificationSourceResult,
    observed: Value,
}

struct CollectionContext<'a> {
    base_dir: &'a Path,
    output_dir: &'a Path,
    default_timeout_ms: u64,
    environment: &'a [(String, String)],
}

struct CollectionOutput {
    bytes: Vec<u8>,
    http_status: Option<u16>,
    exit_code: Option<i32>,
    collector: String,
}

#[must_use]
const fn default_required() -> bool {
    true
}

pub fn run_certification_manifest(
    manifest_path: impl AsRef<Path>,
    output_dir: impl AsRef<Path>,
) -> Result<CertificationReport, String> {
    let started = Instant::now();
    let started_at_ms = now_ms();
    let manifest_path = manifest_path.as_ref();
    let output_dir = output_dir.as_ref();
    let manifest_bytes = fs::read(manifest_path)
        .map_err(|error| format!("cannot read {}: {error}", manifest_path.display()))?;
    let manifest = serde_json::from_slice::<CertificationManifest>(&manifest_bytes)
        .map_err(|error| format!("cannot parse {}: {error}", manifest_path.display()))?;
    validate_manifest(&manifest)?;
    ensure_empty_output_dir(output_dir)?;
    fs::create_dir_all(output_dir.join("sources"))
        .map_err(|error| format!("cannot create certification output: {error}"))?;
    let manifest_artifact = "certification-manifest.json".to_string();
    fs::write(output_dir.join(&manifest_artifact), &manifest_bytes)
        .map_err(|error| format!("cannot preserve certification manifest: {error}"))?;
    let manifest_sha256 = format!("{:x}", Sha256::digest(&manifest_bytes));
    let base_dir = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    verify_fixture(&manifest.fixture, base_dir)?;
    let prior_file_digests = prior_file_digests(&manifest.sources, base_dir)?;
    let environment = scenario_environment(&manifest, output_dir);
    let collection_context = CollectionContext {
        base_dir,
        output_dir,
        default_timeout_ms: manifest.timeout_policy.source_default_ms,
        environment: &environment,
    };

    let mut collected = BTreeMap::new();
    let scenario_source = CertificationSource {
        id: "scenario-command".to_string(),
        kind: CertificationSourceKind::ProcessLog,
        required: true,
        collector: CertificationCollector::Command {
            program: manifest.command.program.clone(),
            args: manifest.command.args.clone(),
            cwd: manifest.command.cwd.clone(),
            timeout_ms: Some(manifest.timeout_policy.scenario_ms),
        },
    };
    collected.insert(
        scenario_source.id.clone(),
        collect_source(&scenario_source, &collection_context, None),
    );
    for source in &manifest.sources {
        collected.insert(
            source.id.clone(),
            collect_source(
                source,
                &collection_context,
                prior_file_digests.get(&source.id).map(String::as_str),
            ),
        );
    }

    let mut check_results = manifest
        .checks
        .iter()
        .map(|check| evaluate_check(check, &collected))
        .collect::<Vec<_>>();
    check_results.extend(event_contract_checks(&manifest, &collected));
    let source_results = std::iter::once(&scenario_source)
        .chain(manifest.sources.iter())
        .filter_map(|source| {
            collected
                .get(&source.id)
                .map(|collected| collected.result.clone())
        })
        .collect::<Vec<_>>();
    let required_source_failures = source_results
        .iter()
        .filter(|source| source.required && source.status != "passed")
        .count();
    let required_check_failures = check_results
        .iter()
        .filter(|check| check.required && check.status != "passed")
        .count();
    let report = CertificationReport {
        kind: "harness_eval.certification_report".to_string(),
        schema_version: CERTIFICATION_SCHEMA_VERSION,
        scenario_id: manifest.id,
        run_id: manifest.run_id,
        capability: manifest.capability,
        fixture: manifest.fixture,
        seed: manifest.seed,
        command: manifest.command,
        expected_events: manifest.expected_events,
        forbidden_events: manifest.forbidden_events,
        evidence_paths: manifest.evidence_paths,
        timeout_policy: manifest.timeout_policy,
        failure_code: manifest.failure_code,
        provider_requirement: manifest.provider_requirement,
        load_levels: manifest.load_levels,
        baseline_commit: manifest.baseline_commit,
        pass_thresholds: manifest.pass_thresholds,
        status: if required_source_failures == 0 && required_check_failures == 0 {
            "passed".to_string()
        } else {
            "failed".to_string()
        },
        started_at_ms,
        finished_at_ms: now_ms(),
        elapsed_ms: started.elapsed().as_millis(),
        manifest_path: manifest_path.display().to_string(),
        manifest_artifact,
        manifest_sha256,
        output_dir: output_dir.display().to_string(),
        source_results,
        check_results,
        required_source_failures,
        required_check_failures,
    };
    write_report(output_dir, &report)?;
    Ok(report)
}

fn validate_manifest(manifest: &CertificationManifest) -> Result<(), String> {
    if manifest.schema_version != CERTIFICATION_SCHEMA_VERSION {
        return Err(format!(
            "unsupported certification manifest schema {}; expected {}",
            manifest.schema_version, CERTIFICATION_SCHEMA_VERSION
        ));
    }
    validate_id("scenario", &manifest.id)?;
    validate_id("run", &manifest.run_id)?;
    validate_id("capability", &manifest.capability)?;
    validate_id("fixture", &manifest.fixture.id)?;
    if manifest.fixture.path.as_os_str().is_empty()
        || manifest.fixture.sha256.len() != 64
        || !manifest
            .fixture
            .sha256
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Err("certification fixture requires a path and SHA-256 digest".to_string());
    }
    if manifest.command.program.trim().is_empty() {
        return Err("certification scenario command is required".to_string());
    }
    if manifest.timeout_policy.scenario_ms == 0 || manifest.timeout_policy.source_default_ms == 0 {
        return Err("certification timeout policy values must be positive".to_string());
    }
    if manifest.load_levels.is_empty()
        || manifest.load_levels.contains(&0)
        || !manifest
            .load_levels
            .windows(2)
            .all(|levels| levels[0] < levels[1])
    {
        return Err(
            "certification load_levels must be non-empty, positive, sorted, and unique".to_string(),
        );
    }
    if manifest.baseline_commit.len() < 7
        || manifest.baseline_commit.len() > 40
        || !manifest
            .baseline_commit
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Err("certification baseline_commit must be a 7-40 character Git SHA".to_string());
    }
    let mut event_contract = BTreeSet::new();
    for kind in manifest
        .expected_events
        .iter()
        .chain(manifest.forbidden_events.iter())
    {
        if kind.trim().is_empty() || !event_contract.insert(kind.as_str()) {
            return Err(format!(
                "certification event contract contains an empty or duplicate kind `{kind}`"
            ));
        }
    }
    if manifest
        .failure_code
        .as_ref()
        .is_some_and(|code| code.trim().is_empty())
    {
        return Err("certification failure_code cannot be blank".to_string());
    }
    if manifest.sources.is_empty() || manifest.checks.is_empty() {
        return Err(
            "certification manifest requires at least one source and one check".to_string(),
        );
    }
    let mut source_ids = BTreeSet::new();
    for source in &manifest.sources {
        validate_id("source", &source.id)?;
        if source.id == "scenario-command" {
            return Err("source id `scenario-command` is reserved".to_string());
        }
        if !source_ids.insert(source.id.as_str()) {
            return Err(format!("duplicate certification source id `{}`", source.id));
        }
        match &source.collector {
            CertificationCollector::File { path } if path.as_os_str().is_empty() => {
                return Err(format!("source `{}` has an empty file path", source.id));
            }
            CertificationCollector::HttpJson { url, .. }
                if !(url.starts_with("http://") || url.starts_with("https://")) =>
            {
                return Err(format!("source `{}` must use an http(s) URL", source.id));
            }
            CertificationCollector::Command { program, .. } if program.trim().is_empty() => {
                return Err(format!("source `{}` has an empty program", source.id));
            }
            CertificationCollector::HttpJson {
                timeout_ms: Some(0),
                ..
            }
            | CertificationCollector::Command {
                timeout_ms: Some(0),
                ..
            } => {
                return Err(format!(
                    "source `{}` collection timeout must be positive",
                    source.id
                ));
            }
            _ => {}
        }
    }
    let runtime_event_sources = manifest
        .sources
        .iter()
        .filter(|source| source.kind == CertificationSourceKind::RuntimeEvents && source.required)
        .count();
    if (!manifest.expected_events.is_empty()
        || !manifest.forbidden_events.is_empty()
        || manifest.failure_code.is_some())
        && runtime_event_sources != 1
    {
        return Err(
            "event contracts require exactly one required runtime_events source".to_string(),
        );
    }
    if manifest.provider_requirement != CertificationProviderRequirement::None
        && !manifest
            .sources
            .iter()
            .any(|source| source.kind == CertificationSourceKind::ProviderTrace && source.required)
    {
        return Err(
            "configured or real provider certification requires a provider_trace source"
                .to_string(),
        );
    }
    for evidence_path in &manifest.evidence_paths {
        if evidence_path.as_os_str().is_empty()
            || !manifest.sources.iter().any(|source| {
                source.required
                    && matches!(
                        &source.collector,
                        CertificationCollector::File { path } if path == evidence_path
                    )
            })
        {
            return Err(format!(
                "evidence path `{}` must name a required file source",
                evidence_path.display()
            ));
        }
    }
    let mut check_ids = BTreeSet::new();
    for check in &manifest.checks {
        validate_id("check", &check.id)?;
        if !check_ids.insert(check.id.as_str()) {
            return Err(format!("duplicate certification check id `{}`", check.id));
        }
        if !source_ids.contains(check.source_id.as_str()) {
            return Err(format!(
                "check `{}` references unknown source `{}`",
                check.id, check.source_id
            ));
        }
        validate_selector(&check.id, &check.selector)?;
        if let CertificationComparison::EqualsObserved {
            source_id,
            selector,
        } = &check.comparison
        {
            if !source_ids.contains(source_id.as_str()) {
                return Err(format!(
                    "check `{}` compares against unknown source `{source_id}`",
                    check.id
                ));
            }
            validate_selector(&check.id, selector)?;
        }
    }
    for (check_id, threshold) in &manifest.pass_thresholds {
        if !threshold.is_finite() {
            return Err(format!(
                "pass threshold `{check_id}` must be a finite number"
            ));
        }
        let Some(check) = manifest.checks.iter().find(|check| check.id == *check_id) else {
            return Err(format!(
                "pass threshold `{check_id}` has no matching certification check"
            ));
        };
        let comparison_threshold = match check.comparison {
            CertificationComparison::AtLeast { expected }
            | CertificationComparison::AtMost { expected } => Some(expected),
            _ => None,
        };
        if comparison_threshold != Some(*threshold) {
            return Err(format!(
                "pass threshold `{check_id}` must match its numeric check comparison"
            ));
        }
    }
    for check in &manifest.checks {
        let comparison_threshold = match check.comparison {
            CertificationComparison::AtLeast { expected }
            | CertificationComparison::AtMost { expected } => Some(expected),
            _ => None,
        };
        if let Some(expected) = comparison_threshold {
            match manifest.pass_thresholds.get(&check.id) {
                Some(threshold) if *threshold == expected => {}
                _ => {
                    return Err(format!(
                        "numeric check `{}` must declare its exact pass_threshold",
                        check.id
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_selector(check_id: &str, selector: &CertificationSelector) -> Result<(), String> {
    let pointer = match selector {
        CertificationSelector::JsonPointer { pointer }
        | CertificationSelector::JsonPointerLength { pointer } => Some(pointer),
        CertificationSelector::EventKindCount { kind } => {
            if kind.trim().is_empty() {
                return Err(format!(
                    "check `{check_id}` event kind selector cannot be blank"
                ));
            }
            None
        }
        CertificationSelector::Text
        | CertificationSelector::HttpStatus
        | CertificationSelector::ExitCode
        | CertificationSelector::ByteLength => None,
    };
    if pointer.is_some_and(|pointer| !pointer.is_empty() && !pointer.starts_with('/')) {
        return Err(format!(
            "check `{check_id}` JSON pointer must be empty or start with /"
        ));
    }
    Ok(())
}

fn validate_id(kind: &str, id: &str) -> Result<(), String> {
    if id.is_empty()
        || !id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
    {
        return Err(format!(
            "certification {kind} id `{id}` may only contain ASCII letters, digits, dot, underscore, and dash"
        ));
    }
    Ok(())
}

fn ensure_empty_output_dir(output_dir: &Path) -> Result<(), String> {
    if output_dir.exists() {
        let mut entries = fs::read_dir(output_dir)
            .map_err(|error| format!("cannot inspect {}: {error}", output_dir.display()))?;
        if entries.next().is_some() {
            return Err(format!(
                "certification output directory {} is not empty; stale evidence is forbidden",
                output_dir.display()
            ));
        }
    }
    Ok(())
}

fn verify_fixture(fixture: &CertificationFixture, base_dir: &Path) -> Result<(), String> {
    let path = resolve_path(base_dir, &fixture.path);
    let digest = fixture_digest(&path)?;
    if !digest.eq_ignore_ascii_case(&fixture.sha256) {
        return Err(format!(
            "fixture `{}` SHA-256 mismatch: expected {}, observed {digest}",
            fixture.id, fixture.sha256
        ));
    }
    Ok(())
}

fn fixture_digest(path: &Path) -> Result<String, String> {
    if path.is_symlink() {
        return Err(format!(
            "fixture {} cannot be a symbolic link",
            path.display()
        ));
    }
    if path.is_file() {
        let bytes =
            fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        return Ok(format!("{:x}", Sha256::digest(bytes)));
    }
    if !path.is_dir() {
        return Err(format!("fixture {} does not exist", path.display()));
    }

    let mut files = Vec::new();
    collect_fixture_files(path, path, &mut files)?;
    files.sort();
    let mut digest = Sha256::new();
    for relative in files {
        let absolute = path.join(&relative);
        digest.update(relative.to_string_lossy().as_bytes());
        digest.update([0]);
        digest.update(
            fs::read(&absolute)
                .map_err(|error| format!("cannot read {}: {error}", absolute.display()))?,
        );
        digest.update([0]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn collect_fixture_files(
    root: &Path,
    directory: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("cannot inspect fixture {}: {error}", directory.display()))?;
    for entry in entries {
        let entry = entry
            .map_err(|error| format!("cannot inspect fixture {}: {error}", directory.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
        if file_type.is_symlink() {
            return Err(format!(
                "fixture {} contains symbolic link {}",
                root.display(),
                path.display()
            ));
        }
        if file_type.is_dir() {
            collect_fixture_files(root, &path, files)?;
        } else if file_type.is_file() {
            files.push(
                path.strip_prefix(root)
                    .map_err(|error| format!("cannot relativize {}: {error}", path.display()))?
                    .to_path_buf(),
            );
        }
    }
    Ok(())
}

fn resolve_path(base_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

fn scenario_environment(
    manifest: &CertificationManifest,
    output_dir: &Path,
) -> Vec<(String, String)> {
    vec![
        ("COWD_CERTIFICATION_ID".to_string(), manifest.id.clone()),
        (
            "COWD_CERTIFICATION_RUN_ID".to_string(),
            manifest.run_id.clone(),
        ),
        (
            "COWD_CERTIFICATION_CAPABILITY".to_string(),
            manifest.capability.clone(),
        ),
        (
            "COWD_CERTIFICATION_FIXTURE".to_string(),
            manifest.fixture.path.display().to_string(),
        ),
        (
            "COWD_CERTIFICATION_FIXTURE_SHA256".to_string(),
            manifest.fixture.sha256.clone(),
        ),
        (
            "COWD_CERTIFICATION_SEED".to_string(),
            manifest.seed.to_string(),
        ),
        (
            "COWD_CERTIFICATION_BASELINE_COMMIT".to_string(),
            manifest.baseline_commit.clone(),
        ),
        (
            "COWD_CERTIFICATION_OUTPUT_DIR".to_string(),
            output_dir.display().to_string(),
        ),
        (
            "COWD_CERTIFICATION_PROVIDER_REQUIREMENT".to_string(),
            serde_json::to_value(manifest.provider_requirement)
                .unwrap_or(Value::Null)
                .as_str()
                .unwrap_or("none")
                .to_string(),
        ),
        (
            "COWD_CERTIFICATION_LOAD_LEVELS".to_string(),
            manifest
                .load_levels
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
    ]
}

fn prior_file_digests(
    sources: &[CertificationSource],
    base_dir: &Path,
) -> Result<BTreeMap<String, String>, String> {
    sources
        .iter()
        .filter_map(|source| match &source.collector {
            CertificationCollector::File { path } => Some((source, resolve_path(base_dir, path))),
            _ => None,
        })
        .filter(|(_, path)| path.exists())
        .map(|(source, path)| {
            let bytes = fs::read(&path)
                .map_err(|error| format!("cannot snapshot {}: {error}", path.display()))?;
            Ok((source.id.clone(), format!("{:x}", Sha256::digest(bytes))))
        })
        .collect()
}

fn collect_source(
    source: &CertificationSource,
    context: &CollectionContext<'_>,
    prior_file_digest: Option<&str>,
) -> CollectedSource {
    let started = Instant::now();
    let collected_at_ms = now_ms();
    let collected = match &source.collector {
        CertificationCollector::File { path } => {
            let path = resolve_path(context.base_dir, path);
            fs::read(&path)
                .map(|bytes| CollectionOutput {
                    bytes,
                    http_status: None,
                    exit_code: None,
                    collector: format!("file:{}", path.display()),
                })
                .map_err(|error| format!("cannot read {}: {error}", path.display()))
        }
        CertificationCollector::HttpJson {
            url,
            token_env,
            timeout_ms,
        } => collect_http(
            url,
            token_env.as_deref(),
            *timeout_ms,
            context.default_timeout_ms,
        ),
        CertificationCollector::Command {
            program,
            args,
            cwd,
            timeout_ms,
        } => collect_command(source, program, args, cwd.as_deref(), *timeout_ms, context),
    };

    match collected {
        Ok(CollectionOutput {
            bytes,
            http_status,
            exit_code,
            collector,
        }) => {
            let digest = format!("{:x}", Sha256::digest(&bytes));
            let relative_artifact = format!("sources/{}.raw", source.id);
            let artifact_path = context.output_dir.join(&relative_artifact);
            let artifact_error = fs::write(&artifact_path, &bytes)
                .err()
                .map(|error| format!("cannot write {}: {error}", artifact_path.display()));
            let command_failed = matches!(source.collector, CertificationCollector::Command { .. })
                && exit_code != Some(0);
            let http_failed = matches!(source.collector, CertificationCollector::HttpJson { .. })
                && !http_status.is_some_and(|status| (200..300).contains(&status));
            let artifact_written = artifact_error.is_none();
            let error = artifact_error
                .or_else(|| {
                    (prior_file_digest == Some(digest.as_str()))
                        .then(|| "file evidence was unchanged by the scenario command".to_string())
                })
                .or_else(|| command_failed.then(|| format!("command exited with {exit_code:?}")))
                .or_else(|| http_failed.then(|| format!("HTTP status was {http_status:?}")));
            CollectedSource {
                result: CertificationSourceResult {
                    id: source.id.clone(),
                    kind: source.kind,
                    required: source.required,
                    status: if error.is_none() {
                        "passed".to_string()
                    } else {
                        "failed".to_string()
                    },
                    collector,
                    raw_artifact: artifact_written.then_some(relative_artifact),
                    sha256: Some(digest),
                    byte_length: bytes.len(),
                    http_status,
                    exit_code,
                    collected_at_ms,
                    elapsed_ms: started.elapsed().as_millis(),
                    error,
                },
                observed: parse_observed(&bytes),
            }
        }
        Err(error) => CollectedSource {
            result: CertificationSourceResult {
                id: source.id.clone(),
                kind: source.kind,
                required: source.required,
                status: "failed".to_string(),
                collector: collector_label(&source.collector),
                raw_artifact: None,
                sha256: None,
                byte_length: 0,
                http_status: None,
                exit_code: None,
                collected_at_ms,
                elapsed_ms: started.elapsed().as_millis(),
                error: Some(error),
            },
            observed: Value::Null,
        },
    }
}

fn collect_http(
    url: &str,
    token_env: Option<&str>,
    timeout_ms: Option<u64>,
    default_timeout_ms: u64,
) -> Result<CollectionOutput, String> {
    let client = Client::builder()
        .timeout(Duration::from_millis(
            timeout_ms.unwrap_or(default_timeout_ms).max(1),
        ))
        .build()
        .map_err(|error| format!("cannot build HTTP client: {error}"))?;
    let mut request = client.get(url);
    if let Some(token_env) = token_env {
        let token = std::env::var(token_env)
            .map_err(|_| format!("HTTP token env `{token_env}` is not set"))?;
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .map_err(|error| format!("HTTP collection failed: {error}"))?;
    let status = response.status().as_u16();
    let bytes = response
        .bytes()
        .map_err(|error| format!("HTTP response body failed: {error}"))?
        .to_vec();
    Ok(CollectionOutput {
        bytes,
        http_status: Some(status),
        exit_code: None,
        collector: format!("http:{url}"),
    })
}

fn collect_command(
    source: &CertificationSource,
    program: &str,
    args: &[String],
    cwd: Option<&Path>,
    timeout_ms: Option<u64>,
    context: &CollectionContext<'_>,
) -> Result<CollectionOutput, String> {
    let scratch = context.output_dir.join(".collector");
    fs::create_dir_all(&scratch)
        .map_err(|error| format!("cannot create command collector scratch: {error}"))?;
    let stdout_path = scratch.join(format!("{}.stdout", source.id));
    let stderr_path = scratch.join(format!("{}.stderr", source.id));
    let stdout = File::create(&stdout_path)
        .map_err(|error| format!("cannot create {}: {error}", stdout_path.display()))?;
    let stderr = File::create(&stderr_path)
        .map_err(|error| format!("cannot create {}: {error}", stderr_path.display()))?;
    let command_cwd = cwd.map_or_else(
        || context.base_dir.to_path_buf(),
        |cwd| {
            if cwd.is_absolute() {
                cwd.to_path_buf()
            } else {
                context.base_dir.join(cwd)
            }
        },
    );
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(&command_cwd)
        .envs(context.environment.iter().cloned())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let mut child = command.spawn().map_err(|error| {
        format!(
            "cannot execute `{program}` in {}: {error}",
            command_cwd.display()
        )
    })?;
    let timeout = Duration::from_millis(timeout_ms.unwrap_or(context.default_timeout_ms).max(1));
    let started = Instant::now();
    let status = loop {
        match child
            .try_wait()
            .map_err(|error| format!("cannot observe `{program}`: {error}"))?
        {
            Some(status) => break status,
            None if started.elapsed() >= timeout => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = fs::remove_file(&stdout_path);
                let _ = fs::remove_file(&stderr_path);
                return Err(format!(
                    "command `{program}` exceeded collection timeout of {}ms",
                    timeout.as_millis()
                ));
            }
            None => thread::sleep(Duration::from_millis(10)),
        }
    };
    let mut bytes = fs::read(&stdout_path)
        .map_err(|error| format!("cannot read {}: {error}", stdout_path.display()))?;
    let stderr = fs::read(&stderr_path)
        .map_err(|error| format!("cannot read {}: {error}", stderr_path.display()))?;
    if !stderr.is_empty() {
        bytes.extend_from_slice(b"\n--- stderr ---\n");
        bytes.extend_from_slice(&stderr);
    }
    let _ = fs::remove_file(stdout_path);
    let _ = fs::remove_file(stderr_path);
    let _ = fs::remove_dir(scratch);
    Ok(CollectionOutput {
        bytes,
        http_status: None,
        exit_code: status.code(),
        collector: format!("command:{program}"),
    })
}

fn collector_label(collector: &CertificationCollector) -> String {
    match collector {
        CertificationCollector::File { path } => format!("file:{}", path.display()),
        CertificationCollector::HttpJson { url, .. } => format!("http:{url}"),
        CertificationCollector::Command { program, .. } => format!("command:{program}"),
    }
}

fn parse_observed(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap_or_else(|_| {
        String::from_utf8(bytes.to_vec())
            .map(Value::String)
            .unwrap_or(Value::Null)
    })
}

fn evaluate_check(
    check: &CertificationCheck,
    collected: &BTreeMap<String, CollectedSource>,
) -> CertificationCheckResult {
    let source = collected.get(&check.source_id);
    let Some(source) = source else {
        return failed_check(check, None, "source was not collected");
    };
    if source.result.status != "passed" {
        return failed_check(
            check,
            None,
            source
                .result
                .error
                .as_deref()
                .unwrap_or("source collection failed"),
        );
    }
    let observed = select_observed(source, &check.selector);
    let passed = observed
        .as_ref()
        .is_some_and(|observed| compare(observed, &check.comparison, collected));
    CertificationCheckResult {
        id: check.id.clone(),
        source_id: check.source_id.clone(),
        required: check.required,
        status: if passed { "passed" } else { "failed" }.to_string(),
        selector: check.selector.clone(),
        comparison: check.comparison.clone(),
        observed,
        error: (!passed).then(|| "observed value did not satisfy comparison".to_string()),
    }
}

fn select_observed(source: &CollectedSource, selector: &CertificationSelector) -> Option<Value> {
    match selector {
        CertificationSelector::JsonPointer { pointer } => {
            if pointer.is_empty() {
                Some(source.observed.clone())
            } else {
                source.observed.pointer(pointer).cloned()
            }
        }
        CertificationSelector::JsonPointerLength { pointer } => {
            let value = if pointer.is_empty() {
                Some(&source.observed)
            } else {
                source.observed.pointer(pointer)
            }?;
            match value {
                Value::Array(items) => Some(Value::from(items.len())),
                Value::Object(items) => Some(Value::from(items.len())),
                Value::String(value) => Some(Value::from(value.len())),
                _ => None,
            }
        }
        CertificationSelector::EventKindCount { kind } => {
            Some(Value::from(count_event_kind(&source.observed, kind)))
        }
        CertificationSelector::Text => Some(match &source.observed {
            Value::String(value) => Value::String(value.clone()),
            value => Value::String(value.to_string()),
        }),
        CertificationSelector::HttpStatus => source.result.http_status.map(Value::from),
        CertificationSelector::ExitCode => source.result.exit_code.map(Value::from),
        CertificationSelector::ByteLength => Some(Value::from(source.result.byte_length)),
    }
}

fn count_event_kind(value: &Value, expected: &str) -> usize {
    match value {
        Value::Array(items) => items
            .iter()
            .map(|item| count_event_kind(item, expected))
            .sum(),
        Value::Object(fields) => {
            let own = usize::from(
                fields
                    .get("kind")
                    .or_else(|| fields.get("event_type"))
                    .and_then(Value::as_str)
                    == Some(expected),
            );
            own + fields
                .values()
                .map(|value| count_event_kind(value, expected))
                .sum::<usize>()
        }
        Value::String(text) => text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .map(|value| count_event_kind(&value, expected))
            .sum(),
        _ => 0,
    }
}

fn event_contract_checks(
    manifest: &CertificationManifest,
    collected: &BTreeMap<String, CollectedSource>,
) -> Vec<CertificationCheckResult> {
    let Some(source_id) = manifest
        .sources
        .iter()
        .find(|source| source.required && source.kind == CertificationSourceKind::RuntimeEvents)
        .map(|source| source.id.clone())
    else {
        return Vec::new();
    };
    let mut checks = manifest
        .expected_events
        .iter()
        .map(|kind| CertificationCheck {
            id: format!("expected-event-{kind}"),
            source_id: source_id.clone(),
            required: true,
            selector: CertificationSelector::EventKindCount { kind: kind.clone() },
            comparison: CertificationComparison::AtLeast { expected: 1.0 },
        })
        .chain(
            manifest
                .forbidden_events
                .iter()
                .map(|kind| CertificationCheck {
                    id: format!("forbidden-event-{kind}"),
                    source_id: source_id.clone(),
                    required: true,
                    selector: CertificationSelector::EventKindCount { kind: kind.clone() },
                    comparison: CertificationComparison::AtMost { expected: 0.0 },
                }),
        )
        .map(|check| evaluate_check(&check, collected))
        .collect::<Vec<_>>();
    if let Some(failure_code) = &manifest.failure_code {
        let check = CertificationCheck {
            id: "expected-failure-code".to_string(),
            source_id,
            required: true,
            selector: CertificationSelector::Text,
            comparison: CertificationComparison::Contains {
                expected: failure_code.clone(),
            },
        };
        checks.push(evaluate_check(&check, collected));
    }
    checks
}

fn failed_check(
    check: &CertificationCheck,
    observed: Option<Value>,
    error: &str,
) -> CertificationCheckResult {
    CertificationCheckResult {
        id: check.id.clone(),
        source_id: check.source_id.clone(),
        required: check.required,
        status: "failed".to_string(),
        selector: check.selector.clone(),
        comparison: check.comparison.clone(),
        observed,
        error: Some(error.to_string()),
    }
}

fn compare(
    observed: &Value,
    comparison: &CertificationComparison,
    collected: &BTreeMap<String, CollectedSource>,
) -> bool {
    match comparison {
        CertificationComparison::Exists => !observed.is_null(),
        CertificationComparison::NonEmpty => match observed {
            Value::Null => false,
            Value::String(value) => !value.trim().is_empty(),
            Value::Array(value) => !value.is_empty(),
            Value::Object(value) => !value.is_empty(),
            _ => true,
        },
        CertificationComparison::Equals { expected } => observed == expected,
        CertificationComparison::Contains { expected } => observed
            .as_str()
            .is_some_and(|value| value.contains(expected)),
        CertificationComparison::AtLeast { expected } => {
            observed.as_f64().is_some_and(|value| value >= *expected)
        }
        CertificationComparison::AtMost { expected } => {
            observed.as_f64().is_some_and(|value| value <= *expected)
        }
        CertificationComparison::EqualsObserved {
            source_id,
            selector,
        } => collected
            .get(source_id)
            .filter(|source| source.result.status == "passed")
            .and_then(|source| select_observed(source, selector))
            .is_some_and(|expected| expected == *observed),
    }
}

fn write_report(output_dir: &Path, report: &CertificationReport) -> Result<(), String> {
    let report_path = output_dir.join("certification-report.json");
    fs::write(
        &report_path,
        serde_json::to_vec_pretty(report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("cannot write {}: {error}", report_path.display()))?;
    let mut markdown = format!(
        "# Harness Certification\n\n- Scenario: `{}`\n- Run: `{}`\n- Capability: `{}`\n- Status: `{}`\n- Baseline: `{}`\n- Provider requirement: `{:?}`\n- Fixture: `{}` (`{}`)\n- Load levels: `{:?}`\n- Manifest SHA-256: `{}`\n- Sources: {}\n- Checks: {}\n- Required source failures: {}\n- Required check failures: {}\n\n## Sources\n\n",
        report.scenario_id,
        report.run_id,
        report.capability,
        report.status,
        report.baseline_commit,
        report.provider_requirement,
        report.fixture.id,
        report.fixture.sha256,
        report.load_levels,
        report.manifest_sha256,
        report.source_results.len(),
        report.check_results.len(),
        report.required_source_failures,
        report.required_check_failures,
    );
    for source in &report.source_results {
        markdown.push_str(&format!(
            "- `{}` ({:?}): {}; bytes={}; sha256={}\n",
            source.id,
            source.kind,
            source.status,
            source.byte_length,
            source.sha256.as_deref().unwrap_or("-"),
        ));
    }
    markdown.push_str("\n## Checks\n\n");
    for check in &report.check_results {
        markdown.push_str(&format!(
            "- `{}` <- `{}`: {}\n",
            check.id, check.source_id, check.status
        ));
    }
    fs::write(output_dir.join("certification-report.md"), markdown)
        .map_err(|error| format!("cannot write certification markdown: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;

    fn manifest(
        root: &Path,
        run_id: &str,
        sources: Vec<CertificationSource>,
        checks: Vec<CertificationCheck>,
    ) -> CertificationManifest {
        let fixture = root.join("fixture.json");
        fs::write(&fixture, br#"{"fixture":"immutable"}"#).expect("fixture");
        let driver = root.join("scenario-driver");
        fs::write(
            &driver,
            b"#!/bin/sh\nset -eu\nfor input in ./*.input; do\n  [ -e \"$input\" ] || continue\n  cp \"$input\" \"${input%.input}.json\"\ndone\n",
        )
        .expect("scenario driver");
        let mut permissions = fs::metadata(&driver)
            .expect("driver metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&driver, permissions).expect("driver permissions");
        let pass_thresholds = checks
            .iter()
            .filter_map(|check| match check.comparison {
                CertificationComparison::AtLeast { expected }
                | CertificationComparison::AtMost { expected } => {
                    Some((check.id.clone(), expected))
                }
                _ => None,
            })
            .collect();
        CertificationManifest {
            schema_version: 1,
            id: format!("scenario-{run_id}"),
            run_id: run_id.to_string(),
            capability: "harness.certification".to_string(),
            fixture: CertificationFixture {
                id: "immutable".to_string(),
                path: PathBuf::from("fixture.json"),
                sha256: fixture_digest(&fixture).expect("fixture digest"),
            },
            seed: 7,
            command: CertificationCommand {
                program: driver.display().to_string(),
                args: Vec::new(),
                cwd: None,
            },
            expected_events: Vec::new(),
            forbidden_events: Vec::new(),
            evidence_paths: sources
                .iter()
                .filter_map(|source| match &source.collector {
                    CertificationCollector::File { path } if source.required => Some(path.clone()),
                    _ => None,
                })
                .collect(),
            timeout_policy: CertificationTimeoutPolicy {
                scenario_ms: 5_000,
                source_default_ms: 5_000,
            },
            failure_code: None,
            provider_requirement: CertificationProviderRequirement::None,
            load_levels: vec![1],
            baseline_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            pass_thresholds,
            sources,
            checks,
        }
    }

    fn write_manifest(root: &Path, manifest: &CertificationManifest) -> PathBuf {
        let path = root.join("manifest.json");
        fs::write(
            &path,
            serde_json::to_vec_pretty(manifest).expect("manifest"),
        )
        .expect("manifest file");
        path
    }

    #[test]
    fn observed_artifact_is_collected_before_expected_value_is_compared() {
        let root = tempfile::tempdir().expect("root");
        fs::write(
            root.path().join("runtime.input"),
            serde_json::to_vec(&json!({
                "events": [{"kind": "agent.terminal"}],
                "worker": {"running": true}
            }))
            .expect("json"),
        )
        .expect("source");
        let manifest = manifest(
            root.path(),
            "certification-pass",
            vec![CertificationSource {
                id: "runtime-events".to_string(),
                kind: CertificationSourceKind::RuntimeEvents,
                required: true,
                collector: CertificationCollector::File {
                    path: PathBuf::from("runtime.json"),
                },
            }],
            vec![
                CertificationCheck {
                    id: "terminal-kind".to_string(),
                    source_id: "runtime-events".to_string(),
                    required: true,
                    selector: CertificationSelector::JsonPointer {
                        pointer: "/events/0/kind".to_string(),
                    },
                    comparison: CertificationComparison::Equals {
                        expected: Value::String("agent.terminal".to_string()),
                    },
                },
                CertificationCheck {
                    id: "worker-running".to_string(),
                    source_id: "runtime-events".to_string(),
                    required: true,
                    selector: CertificationSelector::JsonPointer {
                        pointer: "/worker/running".to_string(),
                    },
                    comparison: CertificationComparison::Equals {
                        expected: Value::Bool(true),
                    },
                },
            ],
        );
        let manifest_path = write_manifest(root.path(), &manifest);

        let output = root.path().join("result");
        let report =
            run_certification_manifest(&manifest_path, &output).expect("certification report");
        assert_eq!(report.status, "passed");
        assert!(output.join("sources/scenario-command.raw").exists());
        assert!(output.join("sources/runtime-events.raw").exists());
        assert!(output.join("certification-manifest.json").exists());
        assert!(output.join("certification-report.json").exists());
        assert!(report.source_results[0].sha256.is_some());
        assert!(!report.manifest_sha256.is_empty());
    }

    #[test]
    fn missing_observation_fails_closed_and_is_not_backfilled_from_expected() {
        let root = tempfile::tempdir().expect("root");
        let manifest = manifest(
            root.path(),
            "certification-fail",
            vec![CertificationSource {
                id: "surface-receipt".to_string(),
                kind: CertificationSourceKind::SurfaceReceipt,
                required: true,
                collector: CertificationCollector::File {
                    path: PathBuf::from("missing.json"),
                },
            }],
            vec![CertificationCheck {
                id: "receipt-id".to_string(),
                source_id: "surface-receipt".to_string(),
                required: true,
                selector: CertificationSelector::JsonPointer {
                    pointer: "/receipt_id".to_string(),
                },
                comparison: CertificationComparison::Equals {
                    expected: Value::String("must-not-be-backfilled".to_string()),
                },
            }],
        );
        let manifest_path = write_manifest(root.path(), &manifest);

        let report = run_certification_manifest(&manifest_path, root.path().join("result"))
            .expect("failed certification is still an auditable report");
        assert_eq!(report.status, "failed");
        assert_eq!(report.required_source_failures, 1);
        assert_eq!(report.required_check_failures, 1);
        assert_eq!(report.check_results[0].observed, None);
    }

    #[test]
    fn cross_source_identity_must_match_two_independent_observations() {
        let root = tempfile::tempdir().expect("root");
        fs::write(
            root.path().join("event.input"),
            br#"{"execution_id":"execution-7","events":[1,2]}"#,
        )
        .expect("event source");
        fs::write(
            root.path().join("receipt.input"),
            br#"{"execution_id":"execution-7","receipt_id":"receipt-9"}"#,
        )
        .expect("receipt source");
        let manifest = manifest(
            root.path(),
            "cross-source",
            vec![
                CertificationSource {
                    id: "event".to_string(),
                    kind: CertificationSourceKind::RuntimeEvents,
                    required: true,
                    collector: CertificationCollector::File {
                        path: PathBuf::from("event.json"),
                    },
                },
                CertificationSource {
                    id: "receipt".to_string(),
                    kind: CertificationSourceKind::SurfaceReceipt,
                    required: true,
                    collector: CertificationCollector::File {
                        path: PathBuf::from("receipt.json"),
                    },
                },
            ],
            vec![
                CertificationCheck {
                    id: "identity".to_string(),
                    source_id: "receipt".to_string(),
                    required: true,
                    selector: CertificationSelector::JsonPointer {
                        pointer: "/execution_id".to_string(),
                    },
                    comparison: CertificationComparison::EqualsObserved {
                        source_id: "event".to_string(),
                        selector: CertificationSelector::JsonPointer {
                            pointer: "/execution_id".to_string(),
                        },
                    },
                },
                CertificationCheck {
                    id: "event-count".to_string(),
                    source_id: "event".to_string(),
                    required: true,
                    selector: CertificationSelector::JsonPointerLength {
                        pointer: "/events".to_string(),
                    },
                    comparison: CertificationComparison::AtLeast { expected: 2.0 },
                },
            ],
        );
        let manifest_path = write_manifest(root.path(), &manifest);

        let report = run_certification_manifest(&manifest_path, root.path().join("result"))
            .expect("certification");
        assert_eq!(report.status, "passed");
        assert_eq!(report.check_results[0].observed, Some(json!("execution-7")));
        assert_eq!(report.check_results[1].observed, Some(json!(2)));
    }

    #[test]
    fn event_contracts_are_evaluated_from_runtime_observation() {
        let root = tempfile::tempdir().expect("root");
        fs::write(
            root.path().join("runtime.input"),
            br#"{"events":[{"kind":"agent.terminal"},{"kind":"evolution.candidate.created"}]}"#,
        )
        .expect("runtime event source");
        let mut manifest = manifest(
            root.path(),
            "event-contract",
            vec![CertificationSource {
                id: "runtime-events".to_string(),
                kind: CertificationSourceKind::RuntimeEvents,
                required: true,
                collector: CertificationCollector::File {
                    path: PathBuf::from("runtime.json"),
                },
            }],
            vec![CertificationCheck {
                id: "runtime-event-source".to_string(),
                source_id: "runtime-events".to_string(),
                required: true,
                selector: CertificationSelector::ByteLength,
                comparison: CertificationComparison::AtLeast { expected: 1.0 },
            }],
        );
        manifest.expected_events = vec![
            "agent.terminal".to_string(),
            "evolution.candidate.created".to_string(),
        ];
        manifest.forbidden_events = vec!["external.effect.duplicated".to_string()];
        let manifest_path = write_manifest(root.path(), &manifest);
        let report = run_certification_manifest(&manifest_path, root.path().join("result"))
            .expect("certification");
        assert_eq!(report.status, "passed");
        assert_eq!(report.check_results.len(), 4);
    }

    #[test]
    fn stale_output_and_changed_fixture_are_rejected_before_collection() {
        let root = tempfile::tempdir().expect("root");
        fs::write(root.path().join("source.input"), br#"{"ok":true}"#).expect("source");
        let manifest = manifest(
            root.path(),
            "stale-output",
            vec![CertificationSource {
                id: "source".to_string(),
                kind: CertificationSourceKind::DatabaseState,
                required: true,
                collector: CertificationCollector::File {
                    path: PathBuf::from("source.json"),
                },
            }],
            vec![CertificationCheck {
                id: "source-present".to_string(),
                source_id: "source".to_string(),
                required: true,
                selector: CertificationSelector::ByteLength,
                comparison: CertificationComparison::AtLeast { expected: 1.0 },
            }],
        );
        let manifest_path = write_manifest(root.path(), &manifest);
        let output = root.path().join("result");
        fs::create_dir_all(&output).expect("output");
        fs::write(output.join("stale"), b"stale").expect("stale");
        assert!(run_certification_manifest(&manifest_path, &output)
            .expect_err("stale output must fail")
            .contains("not empty"));

        fs::remove_dir_all(&output).expect("clean output");
        fs::write(root.path().join("source.json"), br#"{"ok":true}"#).expect("stale source");
        let report = run_certification_manifest(&manifest_path, &output)
            .expect("unchanged evidence produces an auditable failed report");
        assert_eq!(report.status, "failed");
        assert!(report
            .source_results
            .iter()
            .any(|source| source.error.as_deref()
                == Some("file evidence was unchanged by the scenario command")));

        fs::remove_dir_all(&output).expect("clean output");
        fs::write(root.path().join("fixture.json"), b"changed").expect("changed fixture");
        assert!(run_certification_manifest(&manifest_path, &output)
            .expect_err("changed fixture must fail")
            .contains("SHA-256 mismatch"));
    }
}
