use std::{future::Future, pin::Pin};

use managed_worker_runtime::{CancellationToken, ManagedWorkerHandle, ManagedWorkerSpec};

use crate::catalog::AdmittedApp;

use super::SupervisorError;

pub type ConnectorFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SupervisorError>> + Send + 'a>>;

/// Product connector for protocol handshake and health semantics.
///
/// Process ownership deliberately remains in the supervisor and the managed
/// worker kernel. A connector may only shape a worker specification and build
/// a protocol-specific connection after the worker is controlled.
pub trait AppWorkerConnector: Send + Sync + 'static {
    type Connection: Send + Sync + 'static;

    fn configure(&self, _app: &AdmittedApp, mut spec: ManagedWorkerSpec) -> ManagedWorkerSpec {
        spec.socket_env = Some("COWD_WORKER_SOCKET".to_owned());
        spec.credential_env = Some("COWD_WORKER_CREDENTIAL".to_owned());
        spec.generation_env = Some("COWD_WORKER_GENERATION".to_owned());
        spec
    }

    fn connect<'a>(
        &'a self,
        app: &'a AdmittedApp,
        worker: &'a ManagedWorkerHandle,
        cancellation: &'a CancellationToken,
    ) -> ConnectorFuture<'a, Self::Connection>;

    fn health<'a>(
        &'a self,
        app: &'a AdmittedApp,
        worker: &'a ManagedWorkerHandle,
        connection: &'a Self::Connection,
        cancellation: &'a CancellationToken,
    ) -> ConnectorFuture<'a, ()>;

    /// Retires protocol-side authorization when the supervisor fences a
    /// connection. Implementations must be generation-aware and idempotent.
    fn disconnect(&self, _app: &AdmittedApp, _connection: &Self::Connection) {}
}
