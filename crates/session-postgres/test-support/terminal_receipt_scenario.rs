use super::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use storage::{PostgresMigrationMode, StaticSecretRefResolver};

type StepResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    run_id: String,
    action_id: String,
    action: Action,
    database_secret_file: PathBuf,
    database_name: String,
    namespace: String,
    producer_evidence_file: PathBuf,
    frame_fixture_file: PathBuf,
    arm: String,
    capture_file: PathBuf,
    selected_client_id: Option<String>,
    selected_actor_id: Option<String>,
    expected_before_sha256: String,
    output_file: PathBuf,
    provisioning_file: PathBuf,
    allocation_file: PathBuf,
    case_id: String,
    mutation_id: String,
    previous_witness_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum Action {
    Capture,
    UsageOnly,
    BodyB,
    RestoreBodyA,
    Observe,
    ExpireSelected,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessIdentity {
    pid: u32,
    start_ticks: String,
    executable_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DockerIdentity {
    container_id: String,
    image_id: String,
    owner_uid: u32,
    state_pid: u32,
    started_at: String,
    postmaster_started_at: String,
    owner_label: String,
    host_port: u16,
    data_volume: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerInspection {
    id: String,
    image: String,
    restart_count: u64,
    state: DockerState,
    config: DockerLabels,
    network_settings: DockerNetwork,
    mounts: Vec<DockerMount>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerNetwork {
    ports: BTreeMap<String, Option<Vec<DockerPort>>>,
}

#[derive(Deserialize)]
struct DockerPort {
    #[serde(rename = "HostIp")]
    host_ip: String,
    #[serde(rename = "HostPort")]
    host_port: String,
}

#[derive(Deserialize)]
struct DockerMount {
    #[serde(rename = "Type")]
    kind: String,
    #[serde(rename = "Name", default)]
    name: String,
    #[serde(rename = "Destination")]
    destination: String,
    #[serde(rename = "RW")]
    writable: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerState {
    running: bool,
    restarting: bool,
    dead: bool,
    pid: u32,
    started_at: String,
    error: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerLabels {
    labels: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Provisioning {
    suite_id: String,
    isolation_nonce: String,
    run_directory: PathBuf,
    candidate_identity: String,
    database_name: String,
    database_oid: u32,
    database_owner: String,
    database_comment: String,
    namespace: String,
    namespace_oid: u32,
    namespace_owner: String,
    namespace_comment: String,
    role: String,
    server_address: Option<String>,
    server_port: Option<i32>,
    postgres_container: DockerIdentity,
    gateway_process: ProcessIdentity,
    gateway_url: String,
    hashes: BTreeMap<PathBuf, String>,
    relations: BTreeMap<String, u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Allocation {
    candidate_identity: String,
    cases: Vec<AllocationCase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AllocationCase {
    case_id: String,
    mutation_id: String,
    run_id: String,
    database_oid: u32,
    namespace_oid: u32,
    arm: String,
    session_id: String,
    message_id: String,
}

fn require(condition: bool, message: &str) -> StepResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn ownership_comment(actual: Option<&str>, suite: &str, nonce: &str) -> StepResult<()> {
    require(
        !suite.is_empty()
            && !nonce.is_empty()
            && actual == Some(format!("{suite}:{nonce}").as_str()),
        "ownership nonce mismatch",
    )
}

fn exact_database(actual: (&str, u32), expected: (&str, u32)) -> StepResult<()> {
    require(actual == expected, "owned DB identity mismatch")
}

fn exact_namespace(actual: (&str, u32), expected: (&str, u32)) -> StepResult<()> {
    require(actual == expected, "owned namespace identity mismatch")
}

fn expected_state(actual: &Value, expected: &str) -> StepResult<String> {
    let hash = sha(&serde_json::to_vec(actual)?);
    require(hash == expected, "expected-before full state mismatch")?;
    Ok(hash)
}

fn transition(phase: &str, action: Action) -> StepResult<&str> {
    Ok(match (phase, action) {
        ("initial", Action::Capture) => "captured",
        ("captured", Action::UsageOnly) => "usage",
        ("usage", Action::BodyB) => "body-b",
        ("body-b", Action::RestoreBodyA) => "restored",
        ("captured" | "usage" | "body-b" | "restored" | "expired", Action::Observe) => phase,
        ("captured" | "usage", Action::ExpireSelected) => "expired",
        _ => return Err("replayed or out-of-order step; case spent".into()),
    })
}

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn private(path: &Path, directory: bool) -> StepResult<()> {
    require(path.is_absolute(), "absolute path required")?;
    require(
        !path
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir)),
        "parent traversal",
    )?;
    let mut cursor = PathBuf::new();
    for part in path.components() {
        cursor.push(part);
        require(
            !fs::symlink_metadata(&cursor)?.file_type().is_symlink(),
            "symlink input",
        )?;
    }
    let metadata = fs::symlink_metadata(path)?;
    let uid = fs::metadata("/proc/self")?.uid();
    require(
        metadata.uid() == uid
            && metadata.permissions().mode() & 0o077 == 0
            && if directory {
                metadata.is_dir()
            } else {
                metadata.is_file()
            },
        "input is not owned and private",
    )
}

fn frozen_regular(path: &Path) -> StepResult<Vec<u8>> {
    require(
        path.is_absolute()
            && fs::canonicalize(path)? == path
            && fs::symlink_metadata(path)?.is_file(),
        "frozen input is not an absolute regular file",
    )?;
    Ok(fs::read(path)?)
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> StepResult<T> {
    private(path, false)?;
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn process(identity: &ProcessIdentity) -> StepResult<()> {
    let root = PathBuf::from(format!("/proc/{}", identity.pid));
    require(
        fs::metadata(&root)?.uid() == fs::metadata("/proc/self")?.uid(),
        "foreign process owner",
    )?;
    let stat = fs::read_to_string(root.join("stat"))?;
    let tail = stat.rsplit_once(") ").ok_or("process stat")?.1;
    require(
        tail.split_whitespace().nth(19) == Some(identity.start_ticks.as_str()),
        "process restarted",
    )?;
    require(
        sha(&fs::read(root.join("exe"))?) == identity.executable_sha256,
        "process executable changed",
    )
}

fn docker(identity: &DockerIdentity, suite: &str, nonce: &str) -> StepResult<()> {
    use std::io::Read;
    let hex = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    require(
        hex(&identity.container_id)
            && identity.image_id.strip_prefix("sha256:").is_some_and(hex)
            && identity.state_pid != 0
            && identity.owner_uid == fs::metadata("/proc/self")?.uid(),
        "invalid owned Docker identity",
    )?;
    let mut child = std::process::Command::new("/usr/bin/docker")
        .args(["inspect", "--format", "{{json .}}", &identity.container_id])
        .env("DOCKER_HOST", "unix:///var/run/docker.sock")
        .env_remove("DOCKER_CONTEXT")
        .env_remove("DOCKER_TLS_VERIFY")
        .env_remove("DOCKER_CERT_PATH")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let stdout = child.stdout.take().ok_or("Docker stdout")?;
    let reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map(|_| bytes)
    });
    let start = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break Some(status);
        }
        if start.elapsed() > std::time::Duration::from_secs(5) {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    let bytes = reader
        .join()
        .map_err(|_| "Docker inspect reader failed")??;
    require(
        status.is_some_and(|status| status.success()) && bytes.len() <= 1024 * 1024,
        "Docker inspect unavailable or timed out",
    )?;
    let observed: DockerInspection =
        serde_json::from_slice(&bytes).map_err(|_| "invalid protected Docker inspect fields")?;
    require(
        observed.id == identity.container_id
            && observed.image == identity.image_id
            && observed.restart_count == 0
            && observed.state.running
            && !observed.state.restarting
            && !observed.state.dead
            && observed.state.error.is_empty()
            && observed.state.pid == identity.state_pid
            && observed.state.started_at == identity.started_at,
        "Docker instance changed, restarted or stopped",
    )?;
    require(
        identity.owner_label == "kernel-terminal-20261016"
            && !nonce.is_empty()
            && suite.ends_with(&format!("-{nonce}")),
        "suite label must bind the parent-owned isolation nonce",
    )?;
    for (key, expected) in [
        ("cowd.owner", identity.owner_label.clone()),
        ("cowd.suite", suite.to_string()),
    ] {
        require(
            observed.config.labels.get(key) == Some(&expected),
            "foreign Docker owner label",
        )?;
    }
    let ports = observed
        .network_settings
        .ports
        .get("5432/tcp")
        .and_then(Option::as_ref)
        .ok_or("Docker PG loopback binding missing")?;
    require(
        ports.len() == 1
            && ports[0].host_ip == "127.0.0.1"
            && ports[0].host_port == identity.host_port.to_string()
            && identity.host_port != 0,
        "Docker PG endpoint changed",
    )?;
    let mounts = observed
        .mounts
        .iter()
        .filter(|mount| mount.destination == "/var/lib/postgresql/data")
        .collect::<Vec<_>>();
    require(
        mounts.len() == 1
            && mounts[0].kind == "volume"
            && mounts[0].name == identity.data_volume
            && mounts[0].writable,
        "PG is not using the parent-owned disk volume",
    )?;
    let started = chrono::DateTime::parse_from_rfc3339(&identity.started_at)?;
    let postmaster = chrono::DateTime::parse_from_rfc3339(&identity.postmaster_started_at)?;
    require(
        postmaster >= started,
        "postmaster predates owned container start",
    )
}

fn publish(path: &Path, value: &Value) -> StepResult<()> {
    private(path.parent().ok_or("output parent")?, true)?;
    require(!path.exists(), "output already exists")?;
    let temporary = tempfile::Builder::new()
        .prefix(".receipt-")
        .tempfile_in(path.parent().unwrap())?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    let mut file = temporary.as_file();
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    temporary
        .persist_noclobber(path)
        .map_err(|_| "no-replace publication failed")?;
    fs::File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

struct CaseLock(PathBuf);
impl Drop for CaseLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn reserve_case(
    directory: &Path,
    case_id: &str,
    action_id: &str,
    previous: Option<&Path>,
    output: &Path,
    input_hash: &str,
) -> StepResult<CaseLock> {
    private(directory, true)?;
    let lock_path = directory.join(format!("{}.lock", sha(case_id.as_bytes())));
    let lock = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&lock_path)?;
    lock.sync_all()?;
    let guard = CaseLock(lock_path);
    let prefix = format!("{}-", sha(case_id.as_bytes()));
    let mut completed = BTreeMap::new();
    let mut referenced = std::collections::BTreeSet::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".applying"))
        {
            continue;
        }
        let prior: Value = read_json(&path)?;
        require(prior["action_id"] != action_id, "action id already spent")?;
        let output = PathBuf::from(prior["output"].as_str().ok_or("applying output")?);
        let done: Value =
            read_json(&output).map_err(|_| "indeterminate prior action; case spent")?;
        require(
            done["status"] == "done" && done["input_sha256"] == prior["input_sha256"],
            "incomplete prior action; case spent",
        )?;
        if let Some(previous) = prior["previous"].as_str() {
            referenced.insert(PathBuf::from(previous));
        }
        completed.insert(output, done);
    }
    let heads = completed
        .keys()
        .filter(|path| !referenced.contains(*path))
        .collect::<Vec<_>>();
    require(
        if completed.is_empty() {
            previous.is_none()
        } else {
            heads.len() == 1 && previous == Some(heads[0].as_path())
        },
        "forked, replayed or missing previous witness",
    )?;
    let marker = directory.join(format!("{prefix}{}.applying", sha(action_id.as_bytes())));
    let mut applying = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&marker)?;
    applying.write_all(&serde_json::to_vec(&json!({"action_id":action_id,"output":output,"previous":previous,"input_sha256":input_hash}))?)?;
    applying.sync_all()?;
    fs::File::open(directory)?.sync_all()?;
    Ok(guard)
}

fn verified_capture(path: &Path, expected: &str) -> StepResult<Value> {
    private(path, false)?;
    let bytes = fs::read(path)?;
    require(sha(&bytes) == expected, "capture changed")?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn connect_executor(config: &Config) -> StepResult<PostgresExecutor> {
    private(&config.database_secret_file, false)?;
    let resolver = StaticSecretRefResolver::new([(
        "r1.receipt.scenario.pg".into(),
        fs::read_to_string(&config.database_secret_file)?
            .trim()
            .into(),
    )]);
    let mut connection = PostgresConnectionConfig::new(
        format!("r1-receipt:{}", config.run_id),
        "r1.receipt.scenario.pg",
        "r1-receipt-scenario-step",
    );
    connection.max_connections = 2;
    Ok(PostgresExecutor::connect_with_migration_mode(
        connection,
        &resolver,
        PostgresMigrationMode::RuntimeReadiness,
    )?
    .scoped_namespace(&config.namespace)?)
}

fn table(namespace: &str, name: &str) -> String {
    format!("\"{namespace}\".\"{name}\"")
}

fn state(
    transaction: &mut PostgresTransaction<'_>,
    namespace: &str,
    session: &str,
    message: &str,
    turn: &str,
) -> StepResult<Value> {
    let rows = transaction.query(&format!("SELECT to_jsonb(message)||jsonb_build_object('sequence',sequence::text,'terminal_runtime_commit_cursor',terminal_runtime_commit_cursor::text) FROM {} message WHERE session_id=$1 AND stable_message_id=$2 FOR UPDATE", table(namespace,"session_messages")), &[&session, &message])?;
    require(rows.len() == 1, "message cardinality")?;
    let message: Value = rows[0].get(0);
    let inputs = transaction.query(&format!("SELECT jsonb_build_object('input_id',input_id,'request_id',request_id,'message_id',message_id,'turn_id',turn_id,'session_generation',session_generation::text,'sequence',sequence::text,'status',status,'runtime_commit_cursor',runtime_commit_cursor::text) FROM {} WHERE session_id=$1 AND turn_id=$2 FOR UPDATE",table(namespace,"session_runtime_outbox")), &[&session,&turn])?;
    require(inputs.len() == 1, "input cardinality")?;
    let input: Value = inputs[0].get(0);
    require(
        input["status"] == "completed",
        "producer input not completed",
    )?;
    Ok(json!({"message":message,"input":input}))
}

pub(super) fn run() -> StepResult<()> {
    let path = std::env::var_os("COWD_R1_RECEIPT_STEP_CONFIG").ok_or("missing step config")?;
    execute(&PathBuf::from(path))
}

fn execute(path: &Path) -> StepResult<()> {
    let config: Config = read_json(path)?;
    let provisioning: Provisioning = read_json(&config.provisioning_file)?;
    private(&provisioning.run_directory, true)?;
    require(
        config.arm == "a" && !config.case_id.is_empty() && !config.action_id.is_empty(),
        "unallocated target",
    )?;
    require(
        !provisioning.isolation_nonce.is_empty() && !provisioning.suite_id.is_empty(),
        "missing ownership nonce",
    )?;
    for input in [
        path,
        &config.database_secret_file,
        &config.producer_evidence_file,
        &config.frame_fixture_file,
        &config.provisioning_file,
        &config.allocation_file,
        &config.capture_file,
        &config.output_file,
    ] {
        require(
            input.starts_with(&provisioning.run_directory),
            "path outside allocated run",
        )?;
        private(input.parent().ok_or("input parent")?, true)?;
    }
    require(
        config.database_name == provisioning.database_name
            && config.namespace == provisioning.namespace,
        "foreign DB/schema",
    )?;
    docker(
        &provisioning.postgres_container,
        &provisioning.suite_id,
        &provisioning.isolation_nonce,
    )?;
    process(&provisioning.gateway_process)?;
    for (input, expected) in &provisioning.hashes {
        require(
            sha(&frozen_regular(input)?) == *expected,
            "frozen input hash changed",
        )?;
    }
    for input in [
        &config.producer_evidence_file,
        &config.frame_fixture_file,
        &config.allocation_file,
        &config.database_secret_file,
    ] {
        require(
            provisioning.hashes.contains_key(input),
            "unbound provenance input",
        )?;
    }
    let executable = std::env::current_exe()?;
    require(
        provisioning.hashes.get(&executable) == Some(&sha(&fs::read(&executable)?)),
        "step binary not frozen",
    )?;
    let evidence: Value = read_json(&config.producer_evidence_file)?;
    let fixture: Value = read_json(&config.frame_fixture_file)?;
    require(
        evidence["status"] == "ready"
            && evidence["run_id"] == config.run_id
            && fixture["base_url"] == provisioning.gateway_url,
        "producer not ready or foreign",
    )?;
    let witnesses = evidence["witnesses"]
        .as_array()
        .ok_or("producer witnesses")?;
    require(witnesses.len() == 2, "two producer arms required")?;
    for arm in ["a", "b"] {
        let matches = witnesses
            .iter()
            .filter(|witness| witness["arm"] == arm)
            .collect::<Vec<_>>();
        require(matches.len() == 1, "producer arm cardinality")?;
        let witness = matches[0];
        let source = &fixture[format!("source_{arm}")];
        require(
            witness["program"]["status"] == "verified"
                && witness["program"]["objective_verdict"]["kind"] == "satisfied"
                && witness["program"]["unresolved"] == json!([]),
            "producer review missing",
        )?;
        require(
            witness["context"]["session_id"] == source["session_id"]
                && witness["context"]["session_source"]["turn_id"] == source["turn_id"]
                && witness["snapshot"]["execution_id"] == source["execution_id"]
                && witness["snapshot"]["graph"]["lineage"]["root_task_id"]
                    == source["root_task_id"]
                && witness["snapshot"]["graph"]["lineage"]["generation"]
                    .as_u64()
                    .map(|value| value.to_string())
                    .as_deref()
                    == source["lineage_generation"].as_str(),
            "producer source mismatch",
        )?;
        require(
            witness["context"]["message_id"] == fixture[format!("message_{arm}")]
                && witness["context"]["receipt"].is_null(),
            "producer terminal mismatch",
        )?;
        require(
            witness["file"]["sha256"]
                .as_str()
                .is_some_and(|hash| hash.len() == 64),
            "missing real file witness",
        )?;
    }
    let source = &fixture["source_a"];
    let session = source["session_id"].as_str().ok_or("session")?;
    let message = fixture["message_a"].as_str().ok_or("message")?;
    let turn = source["turn_id"].as_str().ok_or("turn")?;
    let allocation: Allocation = read_json(&config.allocation_file)?;
    require(
        allocation.candidate_identity == provisioning.candidate_identity
            && allocation.cases.len() >= 6,
        "allocation candidate",
    )?;
    let mut targets = std::collections::BTreeSet::new();
    let mut runs = std::collections::BTreeSet::new();
    let mut databases = std::collections::BTreeSet::new();
    for case in &allocation.cases {
        require(
            targets.insert((
                case.database_oid,
                case.namespace_oid,
                case.session_id.clone(),
                case.message_id.clone(),
            )),
            "shared destructive target",
        )?;
        require(
            runs.insert(case.run_id.clone()) && databases.insert(case.database_oid),
            "conservative profile requires an independent producer run and database per case",
        )?;
    }
    let allocated = allocation
        .cases
        .iter()
        .filter(|case| case.case_id == config.case_id && case.mutation_id == config.mutation_id)
        .collect::<Vec<_>>();
    require(allocated.len() == 1, "case allocation cardinality")?;
    let allocated = allocated[0];
    require(
        allocated.run_id == config.run_id
            && allocated.database_oid == provisioning.database_oid
            && allocated.namespace_oid == provisioning.namespace_oid
            && allocated.arm == config.arm
            && allocated.session_id == session
            && allocated.message_id == message,
        "foreign allocation",
    )?;
    let database_url = fs::read_to_string(&config.database_secret_file)?;
    let endpoint: postgres::Config = database_url
        .trim()
        .parse()
        .map_err(|_| "invalid private PostgreSQL endpoint")?;
    require(
        endpoint.get_dbname() == Some(config.database_name.as_str())
            && endpoint.get_hosts().len() == 1
            && endpoint.get_hostaddrs().is_empty()
            && matches!(&endpoint.get_hosts()[0],postgres::config::Host::Tcp(host) if host == "127.0.0.1")
            && endpoint.get_ports() == [provisioning.postgres_container.host_port].as_slice(),
        "secret URL does not name the owned Docker endpoint",
    )?;
    let executor = connect_executor(&config)?;
    {
        let mut connection = executor.checkout_critical()?;
        let mut transaction = connection.transaction()?;
        transaction.batch_execute("SET TRANSACTION READ ONLY")?;
        let durability = transaction.query_one("SELECT current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes')", &[])?;
        for index in 0..3 {
            require(
                durability.get::<_, String>(index) == "on",
                "owned PG durability is not enabled",
            )?;
        }
        let postmaster: String = transaction.query_one("SELECT to_char(pg_postmaster_start_time() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')", &[])?.get(0);
        require(
            chrono::DateTime::parse_from_rfc3339(&postmaster)?
                == chrono::DateTime::parse_from_rfc3339(
                    &provisioning.postgres_container.postmaster_started_at,
                )?,
            "PostgreSQL postmaster start identity changed",
        )?;
        let identity = transaction.query_one("SELECT d.oid,d.datname,pg_get_userbyid(d.datdba),shobj_description(d.oid,'pg_database'),current_user,inet_server_addr()::text,inet_server_port(),r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls FROM pg_database d JOIN pg_roles r ON r.rolname=current_user WHERE d.datname=current_database()", &[])?;
        exact_database(
            (&identity.get::<_, String>(1), identity.get(0)),
            (&provisioning.database_name, provisioning.database_oid),
        )?;
        require(
            identity.get::<_, u32>(0) == provisioning.database_oid
                && identity.get::<_, String>(1) == provisioning.database_name
                && identity.get::<_, String>(2) == provisioning.database_owner
                && identity.get::<_, Option<String>>(3).as_deref()
                    == Some(&provisioning.database_comment)
                && identity.get::<_, String>(4) == provisioning.role
                && identity.get::<_, Option<String>>(5) == provisioning.server_address
                && identity.get::<_, Option<i32>>(6) == provisioning.server_port,
            "owned DB identity mismatch",
        )?;
        for index in 7..12 {
            require(!identity.get::<_, bool>(index), "unrestricted role")?;
        }
        require(
            identity.get::<_, String>(4) != provisioning.database_owner
                && identity.get::<_, String>(4) != provisioning.namespace_owner,
            "mutator must not own the database or schema",
        )?;
        let unrestricted: i64 = transaction.query_one("SELECT count(*) FROM pg_roles WHERE rolname<>current_user AND pg_has_role(current_user,oid,'MEMBER') AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls OR rolname IN ('pg_read_all_data','pg_write_all_data','pg_execute_server_program','pg_read_server_files','pg_write_server_files'))", &[])?.get(0);
        require(unrestricted == 0, "privileged inherited role")?;
        let ddl: bool = transaction.query_one("SELECT has_database_privilege(current_user,current_database(),'CREATE') OR has_schema_privilege(current_user,$1,'CREATE')", &[&config.namespace])?.get(0);
        require(!ddl, "mutator has DDL privilege")?;
        let nonce = format!("{}:{}", provisioning.suite_id, provisioning.isolation_nonce);
        ownership_comment(
            identity.get::<_, Option<String>>(3).as_deref(),
            &provisioning.suite_id,
            &provisioning.isolation_nonce,
        )?;
        require(
            provisioning.database_comment == nonce && provisioning.namespace_comment == nonce,
            "ownership comments mismatch",
        )?;
        let namespace = transaction.query_one("SELECT oid,pg_get_userbyid(nspowner),obj_description(oid,'pg_namespace'),nspname FROM pg_namespace WHERE nspname=$1", &[&config.namespace])?;
        exact_namespace(
            (&namespace.get::<_, String>(3), namespace.get(0)),
            (&provisioning.namespace, provisioning.namespace_oid),
        )?;
        ownership_comment(
            namespace.get::<_, Option<String>>(2).as_deref(),
            &provisioning.suite_id,
            &provisioning.isolation_nonce,
        )?;
        require(
            namespace.get::<_, u32>(0) == provisioning.namespace_oid
                && namespace.get::<_, String>(1) == provisioning.namespace_owner
                && namespace.get::<_, Option<String>>(2).as_deref()
                    == Some(&provisioning.namespace_comment),
            "namespace ownership mismatch",
        )?;
        for name in [
            "session_records",
            "session_messages",
            "session_runtime_outbox",
            "session_presence_projection",
            "session_terminal_message_acks",
            "cowd_schema_migrations",
            "cowd_schema_catalogs",
        ] {
            let relation = transaction.query_one("SELECT c.oid,c.relnamespace,pg_get_userbyid(c.relowner),c.relkind::text FROM pg_class c WHERE c.oid=to_regclass($1)", &[&name])?;
            exact_namespace(
                (&config.namespace, relation.get(1)),
                (&provisioning.namespace, provisioning.namespace_oid),
            )?;
            require(
                provisioning.relations.get(name) == Some(&relation.get::<_, u32>(0))
                    && relation.get::<_, u32>(1) == provisioning.namespace_oid
                    && relation.get::<_, String>(2) == provisioning.namespace_owner
                    && relation.get::<_, String>(3) == "r",
                "foreign or shadow relation",
            )?;
        }
        let producer = transaction.query(&format!("SELECT request_id,input_id,sequence,session_generation,runtime_commit_cursor FROM {} WHERE session_id=$1 AND turn_id=$2 AND status='completed'",table(&config.namespace,"session_runtime_outbox")), &[&session,&turn])?;
        require(producer.len() == 1, "producer completed identity")?;
        let witness = witnesses
            .iter()
            .find(|witness| witness["arm"] == "a")
            .ok_or("arm a")?;
        let admission = &witness["admission"];
        let completed = &producer[0];
        require(
            witness["context"]["session_source"]["request_id"] == completed.get::<_, String>(0)
                && admission["input"]["input_id"] == completed.get::<_, String>(1)
                && witness["context"]["session_source"]["input_sequence"]
                    == completed.get::<_, i64>(2).to_string()
                && witness["context"]["session_source"]["session_generation"]
                    == completed.get::<_, i64>(3).to_string()
                && witness["context"]["runtime_commit_cursor"]
                    == completed.get::<_, i64>(4).to_string(),
            "producer completed tuple differs",
        )?;
        let messages = transaction.query(&format!("SELECT role,sequence::text,content_json,terminal_runtime_commit_cursor::text,terminal_content_digest FROM {} WHERE session_id=$1 AND stable_message_id=$2",table(&config.namespace,"session_messages")), &[&session,&message])?;
        require(
            messages.len() == 1
                && messages[0].get::<_, String>(0) == "assistant"
                && witness["context"]["message_sequence"] == messages[0].get::<_, String>(1),
            "foreign producer message",
        )?;
        if config.action == Action::Capture {
            let public = session::public_terminal_payload_v1(&messages[0].get::<_, String>(2))
                .map_err(|error| format!("producer raw bytes: {error:?}"))?;
            require(
                witness["context"]["payload_json"] == public.payload_json
                    && witness["context"]["content_digest"] == public.content_digest
                    && messages[0].get::<_, Option<String>>(3).as_deref()
                        == witness["context"]["runtime_commit_cursor"].as_str()
                    && messages[0].get::<_, Option<String>>(4).as_deref()
                        == witness["context"]["content_digest"].as_str(),
                "producer message binding mismatch",
            )?;
        }
        transaction.commit()?;
    }
    let _store = PostgresSessionStore::new(executor.clone())?;
    require(
        executor.verify_registered_migration_catalogs()? > 0,
        "catalog not verified",
    )?;
    let _guard = reserve_case(
        &provisioning.run_directory,
        &config.case_id,
        &config.action_id,
        config.previous_witness_file.as_deref(),
        &config.output_file,
        &sha(&fs::read(path)?),
    )?;
    let previous: Option<Value> = config
        .previous_witness_file
        .as_ref()
        .map(|path| read_json(path))
        .transpose()?;
    if let Some(previous) = &previous {
        require(
            previous["case_id"] == config.case_id
                && previous["run_id"] == config.run_id
                && previous["mutation_id"] == config.mutation_id
                && previous["status"] == "done",
            "foreign or incomplete previous witness",
        )?;
        require(
            previous["producer_sha256"] == sha(&fs::read(&config.producer_evidence_file)?)
                && previous["provisioning_sha256"] == sha(&fs::read(&config.provisioning_file)?),
            "provenance changed within the case",
        )?;
    }
    let phase = previous
        .as_ref()
        .and_then(|value| value["phase"].as_str())
        .unwrap_or("initial");
    let next_phase = transition(phase, config.action)?;
    let selected = config.selected_client_id.is_some() || config.selected_actor_id.is_some();
    require(
        !selected
            || (matches!(config.action, Action::Observe | Action::ExpireSelected)
                && config.selected_client_id.is_some()
                && config.selected_actor_id.is_some()),
        "selected identity shape",
    )?;
    require(
        config.action != Action::ExpireSelected || selected,
        "expiry requires observed actor",
    )?;
    if !selected {
        require(
            config.selected_actor_id.is_none() && config.selected_client_id.is_none(),
            "unexpected actor",
        )?;
    }
    let mut connection = executor.checkout_critical()?;
    let mut transaction = connection.transaction()?;
    transaction.batch_execute("SET LOCAL statement_timeout='12s'; SET LOCAL lock_timeout='3s'")?;
    let mut before = state(&mut transaction, &config.namespace, session, message, turn)?;
    if config.action == Action::ExpireSelected {
        before["presence"] = transaction
            .query_one(
                &format!(
                    "SELECT to_jsonb(presence) FROM {} presence WHERE session_id=$1 FOR UPDATE",
                    table(&config.namespace, "session_presence_projection")
                ),
                &[&session],
            )?
            .get::<_, Value>(0);
    }
    let before_hash = expected_state(&before, &config.expected_before_sha256)?;
    let raw = before["message"]["content_json"]
        .as_str()
        .ok_or("raw message bytes")?;
    let public = session::public_terminal_payload_v1(raw)
        .map_err(|error| format!("invalid public bytes: {error:?}"))?;
    let capture: Option<Value> = if config.action == Action::Capture {
        None
    } else {
        Some(verified_capture(
            &config.capture_file,
            previous
                .as_ref()
                .and_then(|value| value["capture_sha256"].as_str())
                .ok_or("previous capture hash missing")?,
        )?)
    };
    if let Some(capture) = &capture {
        require(
            capture["case_id"] == config.case_id
                && capture["run_id"] == config.run_id
                && capture["mutation_id"] == config.mutation_id
                && capture["state"]["input"] == before["input"],
            "foreign capture",
        )?;
    }
    let mut affected = 0;
    match config.action {
        Action::Capture => {
            let context = &witnesses
                .iter()
                .find(|witness| witness["arm"] == "a")
                .unwrap()["context"];
            require(
                before["message"]["terminal_runtime_commit_cursor"]
                    == context["runtime_commit_cursor"]
                    && before["message"]["terminal_content_digest"] == context["content_digest"]
                    && public.content_digest == context["content_digest"]
                    && public.payload_json == context["payload_json"],
                "capture is not fresh producer bytes",
            )?;
        }
        Action::UsageOnly => {
            let created = before["message"]["created_at_ms"]
                .as_i64()
                .ok_or("created time")?
                .checked_add(1)
                .ok_or("time overflow")?;
            affected = transaction.execute(&format!("UPDATE {} SET token_usage_json='{{\"output_tokens\":2}}',blocks_count=99,created_at_ms=$3 WHERE session_id=$1 AND stable_message_id=$2",table(&config.namespace,"session_messages")), &[&session,&message,&created])?;
        }
        Action::BodyB | Action::RestoreBodyA => {
            let original = capture.as_ref().unwrap()["state"]["message"]["content_json"]
                .as_str()
                .ok_or("captured bytes")?;
            let body = if config.action == Action::BodyB {
                r#"[{"type":"text","text":"r1 scenario body B"}]"#
            } else {
                original
            };
            require(body != raw, "no-op body action")?;
            if config.action == Action::BodyB {
                require(
                    session::public_terminal_payload_v1(body)
                        .map_err(|error| format!("body B: {error:?}"))?
                        .content_digest
                        != public.content_digest,
                    "body B must change public bytes",
                )?;
            }
            if config.action == Action::RestoreBodyA {
                require(
                    before["message"]["terminal_content_digest"].is_null(),
                    "restore would rebind",
                )?;
            }
            affected = transaction.execute(
                &format!(
                    "UPDATE {} SET content_json=$3 WHERE session_id=$1 AND stable_message_id=$2",
                    table(&config.namespace, "session_messages")
                ),
                &[&session, &message, &body],
            )?;
        }
        Action::Observe | Action::ExpireSelected => {
            let presence = transaction.query_one(
                &format!(
                    "SELECT attachments_json FROM {} WHERE session_id=$1 FOR UPDATE",
                    table(&config.namespace, "session_presence_projection")
                ),
                &[&session],
            )?;
            let attachments: Value = presence.get(0);
            let ordinal = attachments
                .as_array()
                .ok_or("attachments")?
                .iter()
                .enumerate()
                .filter(|(_, attachment)| {
                    attachment["actor"]["id"].as_str() == config.selected_actor_id.as_deref()
                        && attachment["client_id"].as_str() == config.selected_client_id.as_deref()
                })
                .map(|(ordinal, _)| ordinal)
                .collect::<Vec<_>>();
            let historical = previous.as_ref().is_some_and(|witness| {
                witness["selected_client_id"].as_str() == config.selected_client_id.as_deref()
                    && witness["selected_actor_id"].as_str() == config.selected_actor_id.as_deref()
            });
            require(
                !selected || ordinal.len() == 1 || (config.action == Action::Observe && historical),
                "foreign actor/client attachment",
            )?;
            if config.action == Action::ExpireSelected {
                let path = vec![ordinal[0].to_string(), "client_expires_at_ms".into()];
                affected = transaction.execute(&format!("UPDATE {} SET attachments_json=jsonb_set(attachments_json,$2,'0',false) WHERE session_id=$1",table(&config.namespace,"session_presence_projection")), &[&session,&path])?;
                let changed: Value = transaction
                    .query_one(
                        &format!(
                            "SELECT attachments_json FROM {} WHERE session_id=$1",
                            table(&config.namespace, "session_presence_projection")
                        ),
                        &[&session],
                    )?
                    .get(0);
                let mut expected = attachments;
                expected[ordinal[0]]["client_expires_at_ms"] = json!(0);
                require(changed == expected, "unselected attachment changed")?;
                let mut exact_presence = before["presence"].clone();
                exact_presence["attachments_json"] = expected;
                let changed_presence: Value = transaction
                    .query_one(
                        &format!(
                            "SELECT to_jsonb(presence) FROM {} presence WHERE session_id=$1",
                            table(&config.namespace, "session_presence_projection")
                        ),
                        &[&session],
                    )?
                    .get(0);
                require(
                    changed_presence == exact_presence,
                    "expiry changed unselected presence bytes",
                )?;
            }
        }
    }
    require(
        affected
            == if matches!(config.action, Action::Capture | Action::Observe) {
                0
            } else {
                1
            },
        "affected row count",
    )?;
    transaction.commit()?;
    drop(connection);
    let reopened = connect_executor(&config)?;
    let mut fresh = reopened.checkout_critical()?;
    let mut observation = fresh.transaction()?;
    let after = state(&mut observation, &config.namespace, session, message, turn)?;
    require(
        after["input"] == before["input"],
        "step changed immutable completed input identity",
    )?;
    if matches!(config.action, Action::BodyB | Action::RestoreBodyA) {
        require(
            after["message"]["terminal_runtime_commit_cursor"].is_null()
                && after["message"]["terminal_content_digest"].is_null(),
            "body change retained binding",
        )?;
        let expected_raw = if config.action == Action::BodyB {
            r#"[{"type":"text","text":"r1 scenario body B"}]"#
        } else {
            capture.as_ref().unwrap()["state"]["message"]["content_json"]
                .as_str()
                .ok_or("captured restore bytes")?
        };
        require(
            after["message"]["content_json"] == expected_raw,
            "body UPDATE did not preserve the requested raw bytes",
        )?;
        let mut exact_message = before["message"].clone();
        exact_message["content_json"] = expected_raw.into();
        exact_message["terminal_runtime_commit_cursor"] = Value::Null;
        exact_message["terminal_content_digest"] = Value::Null;
        require(
            after["message"] == exact_message,
            "body UPDATE changed unrelated message fields",
        )?;
    }
    if config.action == Action::UsageOnly {
        for field in [
            "content_json",
            "role",
            "sequence",
            "tool_use_id",
            "tool_name",
            "terminal_runtime_commit_cursor",
            "terminal_content_digest",
        ] {
            require(
                before["message"][field] == after["message"][field],
                "usage changed eligibility",
            )?;
        }
    }
    if matches!(
        config.action,
        Action::Capture | Action::Observe | Action::ExpireSelected
    ) {
        require(
            before["message"] == after["message"],
            "non-message action changed message bytes",
        )?;
    }
    let receipts = observation.query(&format!("SELECT to_jsonb(ack)||jsonb_build_object('runtime_commit_cursor',runtime_commit_cursor::text) FROM {} ack WHERE session_id=$1 AND message_id=$2 ORDER BY runtime_commit_cursor,client_id",table(&config.namespace,"session_terminal_message_acks")), &[&session,&message])?
        .iter().map(|row| row.get::<_,Value>(0)).collect::<Vec<_>>();
    let presence: Value = observation
        .query_one(
            &format!(
                "SELECT to_jsonb(presence) FROM {} presence WHERE session_id=$1",
                table(&config.namespace, "session_presence_projection")
            ),
            &[&session],
        )?
        .get(0);
    observation.commit()?;
    if config.action == Action::Capture {
        publish(
            &config.capture_file,
            &json!({"case_id":config.case_id,"run_id":config.run_id,"mutation_id":config.mutation_id,"state":before}),
        )?;
    }
    let done = json!({"status":"done","case_id":config.case_id,"run_id":config.run_id,"mutation_id":config.mutation_id,
        "action_id":config.action_id,"action":config.action,"phase":next_phase,"input_sha256":sha(&fs::read(path)?),
        "producer_sha256":sha(&fs::read(&config.producer_evidence_file)?),"provisioning_sha256":sha(&fs::read(&config.provisioning_file)?),
        "capture_sha256":sha(&fs::read(&config.capture_file)?),"previous_sha256":config.previous_witness_file.as_ref().map(|path| fs::read(path).map(|bytes|sha(&bytes))).transpose()?,
        "before_sha256":before_hash,"after_sha256":sha(&serde_json::to_vec(&after)?),"state":after,"receipts":receipts,"presence":presence,
        "expiry_before_sha256":sha(&serde_json::to_vec(&json!({"message":after["message"],"input":after["input"],"presence":presence}))?),
        "selected_client_id":config.selected_client_id.as_deref().or_else(||previous.as_ref().and_then(|value|value["selected_client_id"].as_str())),
        "selected_actor_id":config.selected_actor_id.as_deref().or_else(||previous.as_ref().and_then(|value|value["selected_actor_id"].as_str())),"affected":affected});
    publish(&config.output_file, &done)?;
    Ok(())
}

pub(super) fn safety_selftest() -> StepResult<()> {
    let fixture = TerminalReceiptFixture::new();
    let mut connection = fixture.store.executor.checkout_critical()?;
    let mut transaction = connection.transaction()?;
    transaction.batch_execute("SET TRANSACTION READ ONLY")?;
    let database = transaction.query_one("SELECT d.datname,d.oid,n.nspname,n.oid FROM pg_database d JOIN pg_namespace n ON n.nspname=current_schema() WHERE d.datname=current_database()", &[])?;
    let database_name: String = database.get(0);
    let database_oid: u32 = database.get(1);
    let namespace: String = database.get(2);
    let namespace_oid: u32 = database.get(3);
    exact_database(
        (&database_name, database_oid),
        (&database_name, database_oid),
    )?;
    exact_namespace((&namespace, namespace_oid), (&namespace, namespace_oid))?;
    assert!(exact_database((&database_name, database_oid), ("foreign", database_oid)).is_err());
    assert!(exact_database(
        (&database_name, database_oid),
        (
            &database_name,
            database_oid.checked_add(1).ok_or("OID overflow")?
        )
    )
    .is_err());
    assert!(exact_namespace((&namespace, namespace_oid), ("public", namespace_oid)).is_err());
    assert!(exact_namespace(
        (&namespace, namespace_oid),
        (
            &namespace,
            namespace_oid.checked_add(1).ok_or("OID overflow")?
        )
    )
    .is_err());
    for name in [
        "session_records",
        "session_messages",
        "session_runtime_outbox",
        "session_presence_projection",
        "session_terminal_message_acks",
        "cowd_schema_migrations",
        "cowd_schema_catalogs",
    ] {
        let actual: u32 = transaction
            .query_one(
                "SELECT relnamespace FROM pg_class WHERE oid=to_regclass($1)",
                &[&name],
            )?
            .get(0);
        exact_namespace((&namespace, actual), (&namespace, namespace_oid))?;
        let public_oid: u32 = transaction
            .query_one("SELECT oid FROM pg_namespace WHERE nspname='public'", &[])?
            .get(0);
        assert!(exact_namespace((&namespace, public_oid), (&namespace, namespace_oid)).is_err());
    }
    transaction.commit()?;
    let mut transaction = connection.transaction()?;
    let original = state(
        &mut transaction,
        &namespace,
        &fixture.authority.session_id,
        &fixture.commit.terminal_message_id,
        &fixture.commit.turn_id,
    )?;
    let hash = sha(&serde_json::to_vec(&original)?);
    expected_state(&original, &hash)?;
    assert!(expected_state(&original, &sha(b"foreign expected row")).is_err());
    for (session, message, turn) in [
        (
            "foreign",
            fixture.commit.terminal_message_id.as_str(),
            fixture.commit.turn_id.as_str(),
        ),
        (
            fixture.authority.session_id.as_str(),
            "foreign",
            fixture.commit.turn_id.as_str(),
        ),
        (
            fixture.authority.session_id.as_str(),
            fixture.commit.terminal_message_id.as_str(),
            "foreign",
        ),
    ] {
        assert!(state(&mut transaction, &namespace, session, message, turn).is_err());
    }
    assert_eq!(
        state(
            &mut transaction,
            &namespace,
            &fixture.authority.session_id,
            &fixture.commit.terminal_message_id,
            &fixture.commit.turn_id
        )?,
        original
    );
    transaction.rollback()?;
    drop(connection);
    let received = fixture
        .store
        .record_terminal_message_ack(&fixture.command())?;
    assert_eq!(
        fixture
            .store
            .record_terminal_message_ack(&fixture.command())?,
        received
    );
    let mut wrong = fixture.command();
    wrong.binding.runtime_commit_cursor = wrong
        .binding
        .runtime_commit_cursor
        .checked_add(1)
        .ok_or("cursor overflow")?;
    assert!(fixture.store.record_terminal_message_ack(&wrong).is_err());
    assert_eq!(fixture.history(), vec![received]);
    let suite = unique_id("receipt-safety");
    let nonce = sha(&serde_json::to_vec(&original)?);
    let comment = format!("{suite}:{nonce}");
    ownership_comment(Some(&comment), &suite, &nonce)?;
    assert!(ownership_comment(Some(&comment), &suite, "foreign-nonce").is_err());
    assert!(ownership_comment(None, &suite, &nonce).is_err());
    let mut phase = "initial";
    for action in [
        Action::Capture,
        Action::UsageOnly,
        Action::BodyB,
        Action::RestoreBodyA,
    ] {
        phase = transition(phase, action)?;
        assert!(transition(phase, action).is_err());
        assert_eq!(transition(phase, Action::Observe)?, phase);
    }
    assert!(transition("initial", Action::RestoreBodyA).is_err());
    assert!(transition("body-b", Action::UsageOnly).is_err());
    let directory = tempfile::tempdir()?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
    let output = directory.path().join("done.json");
    publish(&output, &original)?;
    assert!(publish(&output, &json!({"replacement":true})).is_err());
    require(
        read_json::<Value>(&output)? == original,
        "witness was replaced",
    )?;
    let lock = directory.path().join("case.lock");
    let owned = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&lock)?;
    assert!(OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&lock)
        .is_err());
    drop(owned);
    let incomplete = directory.path().join("applying");
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&incomplete)?;
    file.sync_all()?;
    assert!(read_json::<Value>(&incomplete).is_err());
    assert!(read_json::<Value>(&directory.path().join("missing-done.json")).is_err());
    let first = directory.path().join("step-one.json");
    let case_id = unique_id("case");
    let reservation = reserve_case(directory.path(), &case_id, "capture", None, &first, &hash)?;
    assert!(reserve_case(directory.path(), &case_id, "parallel", None, &first, &hash).is_err());
    publish(
        &first,
        &json!({"status":"done","input_sha256":hash,"state":original}),
    )?;
    drop(reservation);
    assert!(reserve_case(
        directory.path(),
        &case_id,
        "capture",
        Some(&first),
        &first,
        &hash
    )
    .is_err());
    assert!(reserve_case(directory.path(), &case_id, "fork", None, &first, &hash).is_err());
    let unfinished_output = directory.path().join("step-two.json");
    let reservation = reserve_case(
        directory.path(),
        &case_id,
        "observe",
        Some(&first),
        &unfinished_output,
        &hash,
    )?;
    drop(reservation);
    let rejected = reserve_case(
        directory.path(),
        &case_id,
        "replay-after-unknown",
        Some(&first),
        &unfinished_output,
        &hash,
    );
    assert!(matches!(rejected,Err(error) if error.to_string().contains("indeterminate")));
    let capture_hash = sha(&fs::read(&output)?);
    assert_eq!(verified_capture(&output, &capture_hash)?, original);
    fs::write(&output, serde_json::to_vec(&json!({"tampered":original}))?)?;
    assert!(verified_capture(&output, &capture_hash).is_err());
    assert_eq!(fixture.history().len(), 1);
    Ok(())
}
