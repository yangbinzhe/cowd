#![allow(clippy::expect_used)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use cowd_app_protocol::{
    app_operation_catalog_digest_v1, AppActivationPolicyV1, AppId, AppLifecycleStateV1,
    AppManifestV1, AppPresentationV1, AppSurfacesV1, BundleIntegrityV1, BundleSignatureV1,
    FilesystemPolicyV1, GenerationId, IntegrityAlgorithmV1, NetworkPolicyV1, ProtocolRangeV1,
    SandboxProfileV1, Sha256Digest, SignatureAlgorithmV1,
};
use managed_worker_runtime::{CancellationToken, ManagedWorkerHandle, ManagedWorkerSpec};
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::task::JoinSet;

use crate::catalog::{
    AppCatalogBuilder, AppCatalogPolicy, AppTrustStore, EffectiveAppPolicy, TrustedSigningKey,
};

use super::*;

const LONG_WORKER: &str = "#!/bin/sh\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n";
const CRASH_WORKER: &str = "#!/bin/sh\nprintf crash >&2\nexit 17\n";

#[derive(Debug, Clone, Copy)]
struct AppFixture<'a> {
    id: &'a str,
    activation: AppActivationPolicyV1,
    required: bool,
    script: &'a str,
}

struct FixtureKey {
    pair: Ed25519KeyPair,
    key_id: String,
}

impl FixtureKey {
    fn generate() -> Self {
        let document =
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("fixture signing key");
        Self {
            pair: Ed25519KeyPair::from_pkcs8(document.as_ref()).expect("parse signing key"),
            key_id: "supervisor-fixture-key".to_owned(),
        }
    }

    fn trust(&self) -> AppTrustStore {
        use ring::signature::KeyPair;
        AppTrustStore::new([TrustedSigningKey {
            key_id: self.key_id.clone(),
            public_key: self.pair.public_key().as_ref().to_vec(),
            revoked: false,
        }])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FakeConnection {
    pid: u32,
}

#[derive(Debug)]
struct FakeConnector {
    failures: BTreeSet<AppId>,
    delay: Duration,
    connects: AtomicUsize,
    disconnects: AtomicUsize,
    concurrent: AtomicUsize,
    max_concurrent: AtomicUsize,
}

impl FakeConnector {
    fn new(failures: impl IntoIterator<Item = AppId>, delay: Duration) -> Self {
        Self {
            failures: failures.into_iter().collect(),
            delay,
            connects: AtomicUsize::new(0),
            disconnects: AtomicUsize::new(0),
            concurrent: AtomicUsize::new(0),
            max_concurrent: AtomicUsize::new(0),
        }
    }

    fn connect_count(&self) -> usize {
        self.connects.load(Ordering::Acquire)
    }

    fn disconnect_count(&self) -> usize {
        self.disconnects.load(Ordering::Acquire)
    }
}

impl AppWorkerConnector for FakeConnector {
    type Connection = FakeConnection;

    fn configure(&self, _app: &AdmittedApp, mut spec: ManagedWorkerSpec) -> ManagedWorkerSpec {
        spec.require_socket = false;
        spec.startup_timeout = Duration::from_millis(200);
        spec.graceful_shutdown_timeout = Duration::from_millis(50);
        spec.direct_test_process()
    }

    fn connect<'a>(
        &'a self,
        app: &'a AdmittedApp,
        worker: &'a ManagedWorkerHandle,
        cancellation: &'a CancellationToken,
    ) -> ConnectorFuture<'a, Self::Connection> {
        Box::pin(async move {
            self.connects.fetch_add(1, Ordering::AcqRel);
            let concurrent = self.concurrent.fetch_add(1, Ordering::AcqRel) + 1;
            self.max_concurrent.fetch_max(concurrent, Ordering::AcqRel);
            let outcome = tokio::select! {
                () = cancellation.cancelled() => Err(SupervisorError::Cancelled),
                () = tokio::time::sleep(self.delay) => {
                    if self.failures.contains(&app.manifest.app_id) {
                        Err(SupervisorError::Worker {
                            app_id: app.manifest.app_id.clone(),
                            detail: "injected handshake failure".to_owned(),
                        })
                    } else {
                        Ok(FakeConnection { pid: worker.pid() })
                    }
                }
            };
            self.concurrent.fetch_sub(1, Ordering::AcqRel);
            outcome
        })
    }

    fn health<'a>(
        &'a self,
        app: &'a AdmittedApp,
        worker: &'a ManagedWorkerHandle,
        connection: &'a Self::Connection,
        _cancellation: &'a CancellationToken,
    ) -> ConnectorFuture<'a, ()> {
        Box::pin(async move {
            if connection.pid != worker.pid() || worker.try_wait().await.ok().flatten().is_some() {
                Err(SupervisorError::Worker {
                    app_id: app.manifest.app_id.clone(),
                    detail: "worker is unhealthy".to_owned(),
                })
            } else {
                Ok(())
            }
        })
    }

    fn disconnect(&self, _app: &AdmittedApp, _connection: &Self::Connection) {
        self.disconnects.fetch_add(1, Ordering::AcqRel);
    }
}

fn fixture(
    apps: &[AppFixture<'_>],
) -> (TempDir, Arc<AppCatalogSnapshot>, Vec<(AppId, GenerationId)>) {
    let root = TempDir::new().expect("fixture root");
    let key = FixtureKey::generate();
    let mut policy = AppCatalogPolicy::default();
    for app in apps {
        write_bundle(root.path(), app.id, app.script, &key);
        policy.entries.insert(
            AppId(app.id.to_owned()),
            EffectiveAppPolicy {
                enabled: true,
                required: app.required,
                activation: app.activation,
                config_file: None,
            },
        );
    }
    let snapshot = Arc::new(
        AppCatalogBuilder::new(
            vec![root.path().to_path_buf()],
            policy,
            key.trust(),
            fs::metadata(root.path()).expect("root metadata").uid(),
            1,
        )
        .build()
        .expect("catalog"),
    );
    let identities = snapshot
        .apps()
        .map(|app| (app.manifest.app_id.clone(), app.generation.clone()))
        .collect();
    (root, snapshot, identities)
}

fn config(root: &Path) -> AppRuntimeSupervisorConfig {
    AppRuntimeSupervisorConfig {
        runtime_root: root.join("runtime"),
        activation_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_millis(100),
        idle_scan_interval: Duration::from_millis(5),
        crash_window: Duration::from_secs(2),
        crash_budget: 3,
        restart_backoff_initial: Duration::from_millis(5),
        restart_backoff_maximum: Duration::from_millis(20),
        ..AppRuntimeSupervisorConfig::default()
    }
}

fn app(id: &str, activation: AppActivationPolicyV1, required: bool) -> AppFixture<'_> {
    AppFixture {
        id,
        activation,
        required,
        script: LONG_WORKER,
    }
}

#[test]
fn runtime_directory_key_is_stable_bounded_and_identity_bound() {
    let app = AppId("reference-app".to_owned());
    let generation = GenerationId(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    );
    let key = app_runtime_directory_key(&app, &generation);
    assert_eq!(key.len(), RUNTIME_KEY_HEX_CHARS);
    assert!(key
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
    assert_eq!(key, app_runtime_directory_key(&app, &generation));
    assert_ne!(
        key,
        app_runtime_directory_key(&AppId("other-app".to_owned()), &generation)
    );
    assert_ne!(
        key,
        app_runtime_directory_key(
            &app,
            &GenerationId(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
            )
        )
    );
}

#[test]
fn supervisor_rejects_a_runtime_root_that_cannot_fit_unix_sockets() {
    let root = PathBuf::from("/").join("x".repeat(80));
    let (_fixture, snapshot, _) = fixture(&[]);
    let result = AppRuntimeSupervisor::new(
        snapshot,
        Arc::new(FakeConnector::new([], Duration::ZERO)),
        AppRuntimeSupervisorConfig {
            runtime_root: root,
            ..AppRuntimeSupervisorConfig::default()
        },
    );
    let Err(error) = result else {
        panic!("overlong runtime root must fail closed");
    };
    assert!(
        matches!(error, SupervisorError::InvalidConfiguration(message) if message.contains("too long for Unix sockets"))
    );
}

#[test]
fn runtime_slot_owner_mismatch_is_preserved_and_rejected() {
    let fixture = TempDir::new().expect("fixture root");
    let root = fixture.path().join("runtime");
    let app = AppId("reference-app".to_owned());
    let generation = GenerationId("sha256:expected".to_owned());
    let slot = ensure_app_runtime_slot_identity(&root, &app, &generation).expect("initial claim");
    let marker = slot.join(SLOT_OWNER_FILE);
    let foreign = RuntimeSlotOwnerV1 {
        schema_version: 1,
        app_id: AppId("foreign-app".to_owned()),
        generation: GenerationId("sha256:foreign".to_owned()),
    };
    let bytes = serde_json::to_vec(&foreign).expect("foreign owner encode");
    std::fs::write(&marker, &bytes).expect("foreign owner fixture");

    assert!(matches!(
        ensure_app_runtime_slot_identity(&root, &app, &generation),
        Err(SupervisorError::InvalidConfiguration(message))
            if message.contains("runtime slot identity mismatch")
    ));
    assert_eq!(std::fs::read(&marker).expect("preserved owner"), bytes);
}

#[test]
fn concurrent_runtime_slot_claims_converge_on_one_identity() {
    let fixture = TempDir::new().expect("fixture root");
    let root = Arc::new(fixture.path().join("runtime"));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let root = Arc::clone(&root);
        threads.push(std::thread::spawn(move || {
            ensure_app_runtime_slot_identity(
                &root,
                &AppId("reference-app".to_owned()),
                &GenerationId("sha256:concurrent".to_owned()),
            )
        }));
    }
    let slots = threads
        .into_iter()
        .map(|thread| thread.join().expect("claim thread").expect("claim"))
        .collect::<Vec<_>>();
    assert!(slots.windows(2).all(|pair| pair[0] == pair[1]));
    let owner: RuntimeSlotOwnerV1 = serde_json::from_slice(
        &std::fs::read(slots[0].join(SLOT_OWNER_FILE)).expect("owner marker"),
    )
    .expect("owner decode");
    assert_eq!(owner.app_id.0, "reference-app");
    assert_eq!(owner.generation.0, "sha256:concurrent");
}

async fn wait_for_state<C: AppWorkerConnector>(
    supervisor: &AppRuntimeSupervisor<C>,
    app_id: &AppId,
    expected: AppLifecycleStateV1,
) -> AppRuntimeStatus {
    for _ in 0..400 {
        let status = supervisor.status(app_id).await.expect("status");
        if status.state == expected {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("state did not become {expected:?}");
}

#[tokio::test]
async fn empty_catalog_starts_and_stops() {
    let (root, snapshot, identities) = fixture(&[]);
    assert!(identities.is_empty());
    let supervisor = AppRuntimeSupervisor::new(
        snapshot,
        Arc::new(FakeConnector::new([], Duration::ZERO)),
        config(root.path()),
    )
    .expect("supervisor");
    supervisor.start_resident().await.expect("empty start");
    assert!(supervisor.statuses().await.is_empty());
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn optional_resident_failure_is_isolated_but_required_failure_fails_startup() {
    let apps = [
        app("optional-bad", AppActivationPolicyV1::Resident, false),
        app("required-bad", AppActivationPolicyV1::Resident, true),
        app("resident-good", AppActivationPolicyV1::Resident, true),
    ];
    let (root, snapshot, identities) = fixture(&apps);
    let failures = [
        AppId("optional-bad".to_owned()),
        AppId("required-bad".to_owned()),
    ];
    let supervisor = AppRuntimeSupervisor::new(
        snapshot,
        Arc::new(FakeConnector::new(failures, Duration::ZERO)),
        config(root.path()),
    )
    .expect("supervisor");
    assert!(matches!(
        supervisor.start_resident().await,
        Err(SupervisorError::RequiredResidentsFailed { apps, .. })
            if apps == vec![AppId("required-bad".to_owned())]
    ));
    wait_for_state(
        &supervisor,
        &AppId("optional-bad".to_owned()),
        AppLifecycleStateV1::CircuitOpen,
    )
    .await;
    assert_eq!(
        supervisor
            .status(&AppId("resident-good".to_owned()))
            .await
            .expect("good status")
            .state,
        AppLifecycleStateV1::Ready
    );
    assert_eq!(identities.len(), 3);
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn lazy_is_demand_started_while_resident_is_eager_and_none_ttl_keeps_it_loaded() {
    let apps = [
        app("lazy-app", AppActivationPolicyV1::Lazy, false),
        app("resident-app", AppActivationPolicyV1::Resident, false),
    ];
    let (root, snapshot, ids) = fixture(&apps);
    let connector = Arc::new(FakeConnector::new([], Duration::ZERO));
    let supervisor =
        AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), config(root.path()))
            .expect("supervisor");
    supervisor.start_resident().await.expect("resident start");
    assert_eq!(connector.connect_count(), 1);
    let (lazy_id, lazy_generation) = ids
        .iter()
        .find(|(id, _)| id.0 == "lazy-app")
        .expect("lazy identity");
    assert_eq!(
        supervisor.status(lazy_id).await.expect("mounted").state,
        AppLifecycleStateV1::Mounted
    );
    let lease = supervisor
        .acquire(
            lazy_id,
            lazy_generation,
            Duration::from_secs(1),
            &CancellationToken::default(),
        )
        .await
        .expect("lazy acquire");
    lease.release().await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let status = supervisor.status(lazy_id).await.expect("idle");
    assert_eq!(status.state, AppLifecycleStateV1::Idle);
    assert!(status.pid.is_some());
    supervisor.shutdown().await.expect("shutdown");
    assert_eq!(connector.disconnect_count(), 2);
}

#[tokio::test]
async fn two_hundred_fifty_six_callers_share_exactly_one_spawn() {
    let (root, snapshot, ids) = fixture(&[app("singleflight", AppActivationPolicyV1::Lazy, false)]);
    let connector = Arc::new(FakeConnector::new([], Duration::from_millis(50)));
    let supervisor = Arc::new(
        AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), config(root.path()))
            .expect("supervisor"),
    );
    let (app_id, generation) = ids[0].clone();
    let mut callers = JoinSet::new();
    for _ in 0..256 {
        let supervisor = Arc::clone(&supervisor);
        let app_id = app_id.clone();
        let generation = generation.clone();
        callers.spawn(async move {
            let started = Instant::now();
            let result = supervisor
                .acquire(
                    &app_id,
                    &generation,
                    Duration::from_secs(2),
                    &CancellationToken::default(),
                )
                .await;
            (result, started.elapsed())
        });
    }
    let mut leases = Vec::new();
    let mut elapsed_us = Vec::new();
    while let Some(result) = callers.join_next().await {
        let (lease, elapsed) = result.expect("caller");
        leases.push(lease.expect("lease"));
        elapsed_us.push(elapsed.as_micros());
    }
    elapsed_us.sort_unstable();
    assert_eq!(connector.connect_count(), 1);
    assert!(leases
        .iter()
        .all(|lease| lease.connection().pid == leases[0].connection().pid));
    for lease in leases {
        lease.release().await;
    }
    supervisor.shutdown().await.expect("shutdown");
    eprintln!(
        "COWD_PERF_JSON {}",
        serde_json::json!({
            "case": "supervisor_singleflight_256",
            "callers": 256,
            "spawns": connector.connect_count(),
            "p95_us": nearest_rank(&elapsed_us, 95),
            "p99_us": nearest_rank(&elapsed_us, 99),
            "max_us": elapsed_us.last().copied().unwrap_or_default(),
        })
    );
}

#[tokio::test]
async fn waiter_overload_is_rejected_without_duplicate_spawn() {
    let (root, snapshot, ids) = fixture(&[app("overloaded", AppActivationPolicyV1::Lazy, false)]);
    let connector = Arc::new(FakeConnector::new([], Duration::from_millis(100)));
    let mut cfg = config(root.path());
    cfg.max_waiters_per_app = 1;
    let supervisor = Arc::new(
        AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), cfg).expect("supervisor"),
    );
    let (app_id, generation) = ids[0].clone();
    let first = {
        let supervisor = Arc::clone(&supervisor);
        let app_id = app_id.clone();
        let generation = generation.clone();
        tokio::spawn(async move {
            supervisor
                .acquire(
                    &app_id,
                    &generation,
                    Duration::from_secs(1),
                    &CancellationToken::default(),
                )
                .await
        })
    };
    wait_for_state(&supervisor, &app_id, AppLifecycleStateV1::Starting).await;
    assert!(matches!(
        supervisor
            .acquire(
                &app_id,
                &generation,
                Duration::from_secs(1),
                &CancellationToken::default()
            )
            .await,
        Err(SupervisorError::WaiterOverloaded(_))
    ));
    first
        .await
        .expect("first task")
        .expect("first lease")
        .release()
        .await;
    assert_eq!(connector.connect_count(), 1);
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn request_cancellation_does_not_cancel_shared_activation() {
    let (root, snapshot, ids) =
        fixture(&[app("cancel-shared", AppActivationPolicyV1::Lazy, false)]);
    let connector = Arc::new(FakeConnector::new([], Duration::from_millis(80)));
    let supervisor = Arc::new(
        AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), config(root.path()))
            .expect("supervisor"),
    );
    let (app_id, generation) = ids[0].clone();
    let cancellation = CancellationToken::default();
    let cancelled_waiter = {
        let supervisor = Arc::clone(&supervisor);
        let app_id = app_id.clone();
        let generation = generation.clone();
        let cancellation = cancellation.clone();
        tokio::spawn(async move {
            supervisor
                .acquire(&app_id, &generation, Duration::from_secs(1), &cancellation)
                .await
        })
    };
    wait_for_state(&supervisor, &app_id, AppLifecycleStateV1::Starting).await;
    let surviving_waiter = {
        let supervisor = Arc::clone(&supervisor);
        let app_id = app_id.clone();
        let generation = generation.clone();
        tokio::spawn(async move {
            supervisor
                .acquire(
                    &app_id,
                    &generation,
                    Duration::from_secs(1),
                    &CancellationToken::default(),
                )
                .await
        })
    };
    cancellation.cancel();
    assert!(matches!(
        cancelled_waiter.await.expect("cancelled waiter task"),
        Err(SupervisorError::Cancelled)
    ));
    let lease = surviving_waiter
        .await
        .expect("surviving waiter task")
        .expect("shared activation survived");
    assert_eq!(connector.connect_count(), 1);
    lease.release().await;
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn idle_ttl_reaps_and_next_request_restarts() {
    let (root, snapshot, ids) = fixture(&[app("idle-restart", AppActivationPolicyV1::Lazy, false)]);
    let connector = Arc::new(FakeConnector::new([], Duration::ZERO));
    let mut cfg = config(root.path());
    cfg.idle_ttl = Some(Duration::from_millis(20));
    let supervisor =
        AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), cfg).expect("supervisor");
    let (app_id, generation) = &ids[0];
    supervisor
        .acquire(
            app_id,
            generation,
            Duration::from_secs(1),
            &CancellationToken::default(),
        )
        .await
        .expect("first")
        .release()
        .await;
    let reap_started = Instant::now();
    wait_for_state(&supervisor, app_id, AppLifecycleStateV1::Stopped).await;
    let reap_elapsed = reap_started.elapsed();
    assert!(reap_elapsed <= Duration::from_millis(20) + Duration::from_secs(1));
    supervisor
        .acquire(
            app_id,
            generation,
            Duration::from_secs(1),
            &CancellationToken::default(),
        )
        .await
        .expect("restart")
        .release()
        .await;
    assert_eq!(connector.connect_count(), 2);
    supervisor.shutdown().await.expect("shutdown");
    eprintln!(
        "COWD_PERF_JSON {}",
        serde_json::json!({
            "case": "supervisor_idle_reap",
            "idle_ttl_ms": 20,
            "elapsed_ms": reap_elapsed.as_millis(),
            "restart_spawns": connector.connect_count(),
        })
    );
}

#[tokio::test]
async fn repeated_resident_crashes_open_the_circuit() {
    let crashing = AppFixture {
        id: "crash-loop",
        activation: AppActivationPolicyV1::Resident,
        required: false,
        script: CRASH_WORKER,
    };
    let (root, snapshot, ids) = fixture(&[crashing]);
    let connector = Arc::new(FakeConnector::new([], Duration::ZERO));
    let supervisor =
        AppRuntimeSupervisor::new(snapshot, connector, config(root.path())).expect("supervisor");
    let crash_started = Instant::now();
    let _ = supervisor.start_resident().await;
    wait_for_state(&supervisor, &ids[0].0, AppLifecycleStateV1::CircuitOpen).await;
    let crash_elapsed = crash_started.elapsed();
    assert!(crash_elapsed <= Duration::from_secs(3));
    assert!(supervisor
        .logs(&ids[0].0)
        .await
        .expect("crash logs")
        .stderr
        .bytes
        .ends_with(b"crash"));
    assert!(matches!(
        supervisor
            .acquire(
                &ids[0].0,
                &ids[0].1,
                Duration::from_secs(1),
                &CancellationToken::default()
            )
            .await,
        Err(SupervisorError::CircuitOpen(_))
    ));
    supervisor.shutdown().await.expect("shutdown");
    eprintln!(
        "COWD_PERF_JSON {}",
        serde_json::json!({
            "case": "supervisor_crash_budget",
            "crash_budget": 3,
            "elapsed_ms": crash_elapsed.as_millis(),
            "circuit_open": true,
        })
    );
}

#[tokio::test]
async fn resident_handshake_failures_retry_until_the_circuit_opens() {
    let (root, snapshot, ids) = fixture(&[app(
        "handshake-loop",
        AppActivationPolicyV1::Resident,
        false,
    )]);
    let connector = Arc::new(FakeConnector::new(
        [AppId("handshake-loop".to_owned())],
        Duration::ZERO,
    ));
    let supervisor =
        AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), config(root.path()))
            .expect("supervisor");
    supervisor
        .start_resident()
        .await
        .expect("optional resident is isolated");
    wait_for_state(&supervisor, &ids[0].0, AppLifecycleStateV1::CircuitOpen).await;
    assert_eq!(connector.connect_count(), 3);
    supervisor.shutdown().await.expect("shutdown");
}

#[test]
fn starting_limit_cannot_exceed_active_limit() {
    let cfg = AppRuntimeSupervisorConfig {
        max_starting_workers: 2,
        max_active_workers: 1,
        ..AppRuntimeSupervisorConfig::default()
    };
    assert!(matches!(
        cfg.validate(),
        Err(SupervisorError::InvalidConfiguration(_))
    ));
}

#[tokio::test]
async fn global_starting_limit_serializes_independent_apps() {
    let apps = [
        app("starting-a", AppActivationPolicyV1::Lazy, false),
        app("starting-b", AppActivationPolicyV1::Lazy, false),
    ];
    let (root, snapshot, ids) = fixture(&apps);
    let connector = Arc::new(FakeConnector::new([], Duration::from_millis(40)));
    let mut cfg = config(root.path());
    cfg.max_starting_workers = 1;
    cfg.max_active_workers = 2;
    let supervisor = Arc::new(
        AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), cfg).expect("supervisor"),
    );
    let mut starts = JoinSet::new();
    for (app_id, generation) in ids {
        let supervisor = Arc::clone(&supervisor);
        starts.spawn(async move {
            supervisor
                .acquire(
                    &app_id,
                    &generation,
                    Duration::from_secs(1),
                    &CancellationToken::default(),
                )
                .await
        });
    }
    let mut leases = Vec::new();
    while let Some(result) = starts.join_next().await {
        leases.push(result.expect("start task").expect("lease"));
    }
    assert_eq!(connector.connect_count(), 2);
    assert_eq!(connector.max_concurrent.load(Ordering::Acquire), 1);
    for lease in leases {
        lease.release().await;
    }
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn active_worker_limits_hold_for_one_four_and_sixteen_apps_without_orphans() {
    for active_limit in [1, 4, 16] {
        let case_started = std::time::Instant::now();
        let app_ids = (0..=active_limit)
            .map(|index| format!("active-{active_limit:02}-{index:02}"))
            .collect::<Vec<_>>();
        let apps = app_ids
            .iter()
            .map(|app_id| app(app_id, AppActivationPolicyV1::Lazy, false))
            .collect::<Vec<_>>();
        let (root, snapshot, ids) = fixture(&apps);
        let connector = Arc::new(FakeConnector::new([], Duration::from_millis(40)));
        let mut cfg = config(root.path());
        cfg.max_starting_workers = active_limit;
        cfg.max_active_workers = active_limit;
        assert_eq!(cfg.max_starting_workers, active_limit);
        assert_eq!(cfg.max_active_workers, active_limit);
        let supervisor = Arc::new(
            AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), cfg)
                .expect("capacity supervisor"),
        );

        let mut starts = JoinSet::new();
        for (app_id, generation) in ids.iter().take(active_limit) {
            let supervisor = Arc::clone(&supervisor);
            let app_id = app_id.clone();
            let generation = generation.clone();
            starts.spawn(async move {
                let started = Instant::now();
                let result = supervisor
                    .acquire(
                        &app_id,
                        &generation,
                        Duration::from_secs(2),
                        &CancellationToken::default(),
                    )
                    .await;
                (result, started.elapsed())
            });
        }
        let mut leases = Vec::with_capacity(active_limit);
        let mut elapsed_us = Vec::with_capacity(active_limit);
        while let Some(result) = starts.join_next().await {
            let (lease, elapsed) = result.expect("start task");
            leases.push(lease.expect("active lease"));
            elapsed_us.push(elapsed.as_micros());
        }
        elapsed_us.sort_unstable();

        assert_eq!(leases.len(), active_limit);
        assert_eq!(connector.connect_count(), active_limit);
        assert_eq!(
            connector.max_concurrent.load(Ordering::Acquire),
            active_limit
        );
        let worker_pids = leases
            .iter()
            .map(|lease| lease.connection().pid)
            .collect::<BTreeSet<_>>();
        assert_eq!(worker_pids.len(), active_limit, "duplicate worker spawn");
        let active_statuses = supervisor
            .statuses()
            .await
            .into_iter()
            .filter(|status| status.pid.is_some())
            .count();
        assert_eq!(active_statuses, active_limit);

        let overflow = &ids[active_limit];
        assert!(matches!(
            supervisor
                .acquire(
                    &overflow.0,
                    &overflow.1,
                    Duration::from_millis(50),
                    &CancellationToken::default(),
                )
                .await,
            Err(SupervisorError::DeadlineExceeded(_))
        ));
        assert_eq!(connector.connect_count(), active_limit);
        assert_eq!(
            supervisor
                .status(&overflow.0)
                .await
                .expect("overflow status")
                .state,
            AppLifecycleStateV1::Mounted
        );

        for lease in leases {
            lease.release().await;
        }
        supervisor.shutdown().await.expect("capacity shutdown");
        assert_eq!(connector.disconnect_count(), active_limit);
        assert!(supervisor.statuses().await.iter().all(|status| {
            status.state == AppLifecycleStateV1::Stopped && status.pid.is_none()
        }));
        assert!(worker_pids
            .iter()
            .all(|pid| !Path::new("/proc").join(pid.to_string()).exists()));

        eprintln!(
            "COWD_PERF_JSON {}",
            serde_json::json!({
                "case": "supervisor_active_fairness",
                "active_limit": active_limit,
                "spawned": active_limit,
                "duplicate_spawns": 0,
                "orphaned": 0,
                "p95_us": nearest_rank(&elapsed_us, 95),
                "max_us": elapsed_us.last().copied().unwrap_or_default(),
                "case_elapsed_ms": case_started.elapsed().as_millis(),
            })
        );
    }
}

#[tokio::test]
async fn generation_fence_rejects_stale_callers_before_spawn() {
    let (root, snapshot, ids) = fixture(&[app("generation", AppActivationPolicyV1::Lazy, false)]);
    let connector = Arc::new(FakeConnector::new([], Duration::ZERO));
    let supervisor =
        AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), config(root.path()))
            .expect("supervisor");
    assert!(matches!(
        supervisor
            .acquire(
                &ids[0].0,
                &GenerationId(format!("sha256:{}", "0".repeat(64))),
                Duration::from_secs(1),
                &CancellationToken::default()
            )
            .await,
        Err(SupervisorError::StaleGeneration { .. })
    ));
    assert_eq!(connector.connect_count(), 0);
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn health_logs_restart_and_global_limits_are_enforced() {
    let apps = [
        app("limited-a", AppActivationPolicyV1::Lazy, false),
        app("limited-b", AppActivationPolicyV1::Lazy, false),
    ];
    let (root, snapshot, ids) = fixture(&apps);
    let connector = Arc::new(FakeConnector::new([], Duration::from_millis(20)));
    let mut cfg = config(root.path());
    cfg.max_starting_workers = 1;
    cfg.max_active_workers = 1;
    let supervisor =
        AppRuntimeSupervisor::new(snapshot, Arc::clone(&connector), cfg).expect("supervisor");
    let first = supervisor
        .acquire(
            &ids[0].0,
            &ids[0].1,
            Duration::from_secs(1),
            &CancellationToken::default(),
        )
        .await
        .expect("first");
    supervisor
        .health(
            &ids[0].0,
            &ids[0].1,
            Duration::from_secs(1),
            &CancellationToken::default(),
        )
        .await
        .expect("health");
    let _logs = supervisor.logs(&ids[0].0).await.expect("logs");
    assert!(matches!(
        supervisor
            .acquire(
                &ids[1].0,
                &ids[1].1,
                Duration::from_millis(50),
                &CancellationToken::default()
            )
            .await,
        Err(SupervisorError::DeadlineExceeded(_))
    ));
    first.release().await;
    supervisor
        .restart(&ids[0].0, &ids[0].1, &CancellationToken::default())
        .await
        .expect("restart");
    assert_eq!(connector.max_concurrent.load(Ordering::Acquire), 1);
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_drains_leases_then_leaves_no_child() {
    let (root, snapshot, ids) = fixture(&[app("shutdown", AppActivationPolicyV1::Lazy, false)]);
    let supervisor = Arc::new(
        AppRuntimeSupervisor::new(
            snapshot,
            Arc::new(FakeConnector::new([], Duration::ZERO)),
            config(root.path()),
        )
        .expect("supervisor"),
    );
    let lease = supervisor
        .acquire(
            &ids[0].0,
            &ids[0].1,
            Duration::from_secs(1),
            &CancellationToken::default(),
        )
        .await
        .expect("lease");
    let pid = lease.connection().pid;
    let shutdown = {
        let supervisor = Arc::clone(&supervisor);
        tokio::spawn(async move { supervisor.shutdown().await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!shutdown.is_finished(), "shutdown skipped lease drain");
    lease.release().await;
    let released_at = Instant::now();
    shutdown.await.expect("shutdown task").expect("shutdown");
    let shutdown_elapsed = released_at.elapsed();
    assert!(shutdown_elapsed <= Duration::from_millis(100) + Duration::from_secs(1));
    assert!(
        !Path::new("/proc").join(pid.to_string()).exists(),
        "worker child survived shutdown"
    );
    assert_eq!(
        supervisor.status(&ids[0].0).await.expect("status").state,
        AppLifecycleStateV1::Stopped
    );
    eprintln!(
        "COWD_PERF_JSON {}",
        serde_json::json!({
            "case": "supervisor_shutdown",
            "shutdown_grace_ms": 100,
            "elapsed_after_release_ms": shutdown_elapsed.as_millis(),
            "orphaned": 0,
        })
    );
}

fn nearest_rank(samples: &[u128], percentile: usize) -> u128 {
    let rank = samples.len().saturating_mul(percentile).saturating_add(99) / 100;
    samples[rank.saturating_sub(1).min(samples.len().saturating_sub(1))]
}

fn write_bundle(root: &Path, app_id: &str, script: &str, key: &FixtureKey) -> PathBuf {
    let bundle = root.join(app_id);
    fs::create_dir_all(bundle.join("bin")).expect("bundle bin");
    fs::set_permissions(&bundle, fs::Permissions::from_mode(0o755)).expect("bundle mode");
    let worker = bundle.join("bin/worker");
    fs::write(&worker, script).expect("worker");
    fs::set_permissions(&worker, fs::Permissions::from_mode(0o755)).expect("worker mode");
    fs::set_permissions(bundle.join("bin"), fs::Permissions::from_mode(0o755)).expect("bin mode");
    let signed_digest = Sha256Digest(format!(
        "sha256:{:x}",
        Sha256::digest(format!("{app_id}:supervisor").as_bytes())
    ));
    let mut manifest = AppManifestV1 {
        schema_version: 1,
        app_id: AppId(app_id.to_owned()),
        display_name: app_id.to_owned(),
        artifact_version: "1.0.0".to_owned(),
        required_protocol: ProtocolRangeV1::exact_v1(),
        executable: "bin/worker".to_owned(),
        web_root: None,
        capabilities: Vec::new(),
        operation_catalog_digest: app_operation_catalog_digest_v1(&AppId(app_id.to_owned()), &[])
            .expect("empty operation catalog digest"),
        core_bridge_requirements: Vec::new(),
        authorization_profiles: Vec::new(),
        surfaces: AppSurfacesV1 {
            web: false,
            tui_view: false,
        },
        integrity: BundleIntegrityV1 {
            algorithm: IntegrityAlgorithmV1::Sha256,
            files: BTreeMap::from([("bin/worker".to_owned(), file_digest(&worker))]),
            manifest_digest: signed_digest.clone(),
        },
        signature: BundleSignatureV1 {
            algorithm: SignatureAlgorithmV1::Ed25519,
            key_id: key.key_id.clone(),
            signature: String::new(),
            signed_digest,
            expires_unix_ms: None,
            provenance_digest: None,
        },
        sandbox: SandboxProfileV1 {
            filesystem: FilesystemPolicyV1::BundleReadOnlyDataReadWrite,
            network: NetworkPolicyV1::Deny,
            max_processes: 8,
            max_open_files: 256,
            max_memory_bytes: 64 * 1024 * 1024,
            cpu_quota_millis_per_second: 1_000,
        },
        presentation: Some(AppPresentationV1 {
            result_shape_revision: 1,
            result_contracts: Vec::new(),
            tui_views: Vec::new(),
            core_navigation_kinds: Vec::new(),
        }),
    };
    let signed_digest = manifest
        .bind_canonical_signed_digest()
        .expect("canonical manifest digest");
    manifest.signature.signature =
        URL_SAFE_NO_PAD.encode(key.pair.sign(signed_digest.0.as_bytes()).as_ref());
    fs::write(
        bundle.join("app.json"),
        serde_json::to_vec(&manifest).expect("manifest JSON"),
    )
    .expect("manifest");
    fs::set_permissions(bundle.join("app.json"), fs::Permissions::from_mode(0o644))
        .expect("manifest mode");
    bundle
}

fn file_digest(path: &Path) -> Sha256Digest {
    Sha256Digest(format!(
        "sha256:{:x}",
        Sha256::digest(fs::read(path).expect("fixture file"))
    ))
}
