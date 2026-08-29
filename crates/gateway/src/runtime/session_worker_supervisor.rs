//! Ownership boundary for long-lived Session workers and shutdown recovery.

use super::*;

/// Sole owner of every long-lived Session worker and its shutdown lifecycle.
///
/// A timed-out worker is explicitly aborted and awaited; no JoinHandle is
/// ever dropped while the task can continue detached from Gateway ownership.
pub(crate) struct SessionWorkerSupervisor {
    accepting: std::sync::atomic::AtomicBool,
    pub(super) shutdown: watch::Sender<bool>,
    workers: Mutex<Option<Vec<SupervisedWorker>>>,
    states: Arc<Mutex<BTreeMap<String, SessionWorkerObservation>>>,
    reconciliation: Arc<Mutex<BTreeMap<String, SessionReconciliationProgress>>>,
    forced_aborts: Arc<std::sync::atomic::AtomicU64>,
    claim_lease_lost: Arc<std::sync::atomic::AtomicU64>,
    recovery: Mutex<crate::services::session_service::activation::SessionRecoverySummary>,
    recovery_completed_at_ms: std::sync::atomic::AtomicU64,
}

impl SessionWorkerSupervisor {
    #[cfg(test)]
    pub(crate) fn for_tests() -> Arc<Self> {
        let (shutdown, _) = watch::channel(false);
        let states = REQUIRED_SESSION_WORKERS
            .into_iter()
            .map(|name| {
                (
                    name.to_string(),
                    SessionWorkerObservation {
                        state: SessionWorkerState::Running,
                        restart_count: 0,
                        last_error: None,
                        next_retry_at_ms: None,
                        last_backend_success_at_ms: Some(now_ms()),
                        last_backend_error_at_ms: None,
                        last_backend_error: None,
                        consecutive_backend_failures: 0,
                        oldest_queue_age_ms: None,
                    },
                )
            })
            .collect();
        let reconciliation = reconciliation_progress_map();
        Arc::new(Self {
            accepting: std::sync::atomic::AtomicBool::new(true),
            shutdown,
            workers: Mutex::new(Some(Vec::new())),
            states: Arc::new(Mutex::new(states)),
            reconciliation: Arc::new(Mutex::new(reconciliation)),
            forced_aborts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            claim_lease_lost: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            recovery: Mutex::new(Default::default()),
            recovery_completed_at_ms: std::sync::atomic::AtomicU64::new(now_ms()),
        })
    }

    pub(crate) async fn start(
        runtime_service: Arc<RuntimeService>,
        session_service: Arc<SessionService>,
        event_bus: Arc<SessionProjectionHub>,
    ) -> Result<Arc<Self>, String> {
        let (shutdown, _) = watch::channel(false);
        let claim_lease_lost = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ingress_runtime = GatewaySessionIngressExecutor {
            runtime: Arc::clone(&runtime_service),
            session: Arc::clone(&session_service),
            admission: runtime_service.runtime_services().session_turn_admission(),
            lease_lost: Arc::clone(&claim_lease_lost),
        };
        let ingress_service = Arc::clone(&session_service);
        let ingress_wake = runtime_service.session_input_router().wake_signal();
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let reconciliation = Arc::new(Mutex::new(reconciliation_progress_map()));
        let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ingress_states = Arc::clone(&states);
        let ingress_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let session_service = Arc::clone(&ingress_service);
            let executor = ingress_runtime.clone();
            let wake = Arc::clone(&ingress_wake);
            let reporter = WorkerBackendReporter {
                name: "ingress",
                states: Arc::clone(&ingress_states),
            };
            Box::pin(async move {
                run_ingress_worker(session_service, executor, wake, reporter, shutdown, ready)
                    .await?;
                Ok(())
            })
        });
        let ingress = spawn_supervised(
            "ingress",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            ingress_factory,
            WorkerSupervisorConfig::default(),
        );
        let delivery_runtime = Arc::clone(&runtime_service);
        let delivery_event_bus = Arc::clone(&event_bus);
        let delivery_store = delivery_runtime
            .runtime_services()
            .session_terminal_delivery();
        let delivery_artifacts = Arc::clone(delivery_runtime.runtime_services().artifact_store());
        let delivery_runtime_services = delivery_runtime.runtime_services();
        let delivery_session_service = Arc::clone(&session_service);
        let delivery_states = Arc::clone(&states);
        let delivery_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let delivery_store = delivery_store.clone();
            let session_service = Arc::clone(&delivery_session_service);
            let event_bus = Arc::clone(&delivery_event_bus);
            let artifacts = Arc::clone(&delivery_artifacts);
            let runtime_services = Arc::clone(&delivery_runtime_services);
            let reporter = WorkerBackendReporter {
                name: "terminal_delivery",
                states: Arc::clone(&delivery_states),
            };
            Box::pin(async move {
                run_delivery_worker(
                    delivery_store,
                    artifacts,
                    session_service,
                    event_bus,
                    Some(runtime_services),
                    reporter,
                    shutdown,
                    ready,
                )
                .await?;
                Ok(())
            })
        });
        let delivery = spawn_supervised(
            "terminal_delivery",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            delivery_factory,
            WorkerSupervisorConfig::default(),
        );
        let cleanup_service = Arc::clone(&session_service);
        let cleanup_states = Arc::clone(&states);
        let cleanup_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let session_service = Arc::clone(&cleanup_service);
            let reporter = WorkerBackendReporter {
                name: "working_set_cleanup",
                states: Arc::clone(&cleanup_states),
            };
            Box::pin(async move {
                run_session_cleanup_worker(session_service, reporter, shutdown, ready).await?;
                Ok(())
            })
        });
        let cleanup = spawn_supervised(
            "working_set_cleanup",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            cleanup_factory,
            WorkerSupervisorConfig::default(),
        );
        let lifecycle_service = Arc::clone(&session_service);
        let lifecycle_runtime_service = Arc::clone(&runtime_service);
        let lifecycle_runtime_services = runtime_service.runtime_services();
        let lifecycle_event_bus = Arc::clone(&event_bus);
        let lifecycle_progress = Arc::clone(&reconciliation);
        let lifecycle_states = Arc::clone(&states);
        let lifecycle_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let session_service = Arc::clone(&lifecycle_service);
            let progress = Arc::clone(&lifecycle_progress);
            let runtime_services = Arc::clone(&lifecycle_runtime_services);
            let runtime_service = Arc::clone(&lifecycle_runtime_service);
            let event_bus = Arc::clone(&lifecycle_event_bus);
            let reporter = WorkerBackendReporter {
                name: "lifecycle_reconciliation",
                states: Arc::clone(&lifecycle_states),
            };
            Box::pin(run_lifecycle_reconciliation_worker(
                session_service,
                Some(runtime_service),
                Some(runtime_services),
                Some(event_bus),
                progress,
                reporter,
                shutdown,
                ready,
            ))
        });
        let lifecycle = spawn_supervised(
            "lifecycle_reconciliation",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            lifecycle_factory,
            WorkerSupervisorConfig::default(),
        );
        let branch_service = Arc::clone(&session_service);
        let branch_progress = Arc::clone(&reconciliation);
        let branch_states = Arc::clone(&states);
        let branch_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let session_service = Arc::clone(&branch_service);
            let progress = Arc::clone(&branch_progress);
            let reporter = WorkerBackendReporter {
                name: "branch_activation_reconciliation",
                states: Arc::clone(&branch_states),
            };
            Box::pin(run_branch_activation_reconciliation_worker(
                session_service,
                progress,
                reporter,
                shutdown,
                ready,
            ))
        });
        let branch = spawn_supervised(
            "branch_activation_reconciliation",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            branch_factory,
            WorkerSupervisorConfig::default(),
        );
        let mut workers = vec![ingress, delivery, cleanup, lifecycle, branch];
        if let Err(error) =
            await_initial_worker_readiness(&mut workers, WorkerSupervisorConfig::default()).await
        {
            rollback_started_workers(
                &shutdown,
                &mut workers,
                &states,
                &forced_aborts,
                WorkerSupervisorConfig::default(),
            )
            .await;
            return Err(error);
        }
        Ok(Arc::new(Self {
            accepting: std::sync::atomic::AtomicBool::new(true),
            shutdown,
            workers: Mutex::new(Some(workers)),
            states,
            reconciliation,
            forced_aborts,
            claim_lease_lost,
            recovery: Mutex::new(Default::default()),
            recovery_completed_at_ms: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    pub(crate) fn record_recovery(
        &self,
        recovery: crate::services::session_service::activation::SessionRecoverySummary,
    ) {
        tracing::info!(
            discovered = recovery.discovered,
            metadata_loaded = recovery.metadata_loaded,
            required = recovery.required,
            attached = recovery.attached,
            recent = recovery.recent,
            recovered = recovery.recovered,
            already_active = recovery.already_active,
            metadata_only = recovery.metadata_only,
            model_rebind_required = recovery.model_rebind_required,
            failed = recovery.failed,
            hot_bytes = recovery.hot_bytes,
            "Session supervisor startup recovery completed"
        );
        for failure in &recovery.failures {
            tracing::warn!(error = %failure, "Session startup recovery item failed");
        }
        *self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = recovery;
        self.recovery_completed_at_ms
            .store(now_ms(), std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn health(&self) -> SessionWorkerHealth {
        if let Some(workers) = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            for worker in workers {
                if worker.handle.is_finished() {
                    let exited_while_running = self
                        .states
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(worker.name)
                        .is_some_and(|worker| worker.state == SessionWorkerState::Running);
                    if exited_while_running {
                        set_worker_failure(
                            &self.states,
                            worker.name,
                            "worker exited before supervised shutdown",
                        );
                    }
                }
            }
        }
        SessionWorkerHealth {
            accepting: self.accepting.load(std::sync::atomic::Ordering::Acquire),
            forced_aborts: self
                .forced_aborts
                .load(std::sync::atomic::Ordering::Relaxed),
            claim_lease_lost: self
                .claim_lease_lost
                .load(std::sync::atomic::Ordering::Relaxed),
            workers: self
                .states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            recovery: self
                .recovery
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            recovery_completed_at_ms: self
                .recovery_completed_at_ms
                .load(std::sync::atomic::Ordering::Acquire),
            reconciliation: self
                .reconciliation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }

    #[must_use]
    pub(crate) fn is_accepting(&self) -> bool {
        self.accepting.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn stop_accepting(&self) {
        self.accepting
            .store(false, std::sync::atomic::Ordering::Release);
    }

    pub(crate) async fn shutdown(&self) {
        self.stop_accepting();
        self.shutdown.send_replace(true);
        let workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_default();
        for mut worker in workers {
            match tokio::time::timeout(SUPERVISOR_JOIN_TIMEOUT, &mut worker.handle).await {
                Ok(Ok(())) => {
                    if worker_observation(&self.states, worker.name)
                        .map_or(true, |state| state.state != SessionWorkerState::Aborted)
                    {
                        set_worker_state(&self.states, worker.name, SessionWorkerState::Stopped);
                    }
                }
                Ok(Err(error)) => {
                    tracing::error!(worker = worker.name, %error, "Session worker failed");
                    set_worker_failure(&self.states, worker.name, error.to_string());
                }
                Err(_) => {
                    worker.handle.abort();
                    let _ = worker.handle.await;
                    self.forced_aborts
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    set_worker_state(&self.states, worker.name, SessionWorkerState::Aborted);
                }
            }
        }
    }
}
