//! Fixture-only pause inside the real native backend, after its effects and
//! before its return/local-exit publication. No Graph or Task state is written.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::watch;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key(String, String, String, String, u32);
fn key(packet: &harness_contract::agent::AgentTaskPacket) -> Key {
    Key(
        packet.graph_id().into(),
        packet.node_id().into(),
        packet.run_id().into(),
        packet.agent_id().into(),
        packet.attempt,
    )
}
struct State {
    entered: watch::Sender<bool>,
    released: watch::Sender<bool>,
    exited: watch::Sender<bool>,
}
fn pending() -> &'static Mutex<BTreeMap<Key, Arc<State>>> {
    static PENDING: OnceLock<Mutex<BTreeMap<Key, Arc<State>>>> = OnceLock::new();
    PENDING.get_or_init(Default::default)
}
/// The controller owns release even during a fixture panic/cancellation.
pub struct BackendExitHold {
    key: Key,
    state: Arc<State>,
}
impl BackendExitHold {
    pub async fn entered(&self) -> Result<(), String> {
        wait(&self.state.entered).await
    }
    pub async fn exited(&self) -> Result<(), String> {
        wait(&self.state.exited).await
    }
    pub fn is_released(&self) -> bool {
        *self.state.released.borrow()
    }
    pub fn has_exited(&self) -> bool {
        *self.state.exited.borrow()
    }
    pub fn release(&self) {
        self.state.released.send_replace(true);
    }
}
impl Drop for BackendExitHold {
    fn drop(&mut self) {
        self.release();
        let mut entries = pending()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries
            .get(&self.key)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            entries.remove(&self.key);
        }
    }
}
async fn wait(signal: &watch::Sender<bool>) -> Result<(), String> {
    let mut receiver = signal.subscribe();
    while !*receiver.borrow_and_update() {
        receiver
            .changed()
            .await
            .map_err(|_| "backend exit handshake closed")?;
    }
    Ok(())
}
pub(super) fn register(
    packet: &harness_contract::agent::AgentTaskPacket,
) -> Result<BackendExitHold, String> {
    register_key(key(packet))
}
fn register_key(key: Key) -> Result<BackendExitHold, String> {
    let mut entries = pending()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if entries.contains_key(&key) {
        return Err("backend exit hold already registered for exact attempt".into());
    }
    let state = Arc::new(State {
        entered: watch::channel(false).0,
        released: watch::channel(false).0,
        exited: watch::channel(false).0,
    });
    entries.insert(key.clone(), Arc::clone(&state));
    Ok(BackendExitHold { key, state })
}
pub(super) struct ExitGuard(Arc<State>);
impl Drop for ExitGuard {
    fn drop(&mut self) {
        self.0.exited.send_replace(true);
    }
}
pub(super) async fn enter(packet: &harness_contract::agent::AgentTaskPacket) -> Option<ExitGuard> {
    enter_key(&key(packet)).await
}
async fn enter_key(key: &Key) -> Option<ExitGuard> {
    let state = pending()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(key)?;
    let guard = ExitGuard(state);
    guard.0.entered.send_replace(true);
    let _ = wait(&guard.0.released).await;
    Some(guard)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity(case: &str) -> Key {
        Key(
            case.into(),
            "native-node".into(),
            "original-run".into(),
            "original-agent".into(),
            1,
        )
    }
    async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(2), future)
            .await
            .expect("handshake stalled")
    }
    #[tokio::test]
    async fn held_native_exit_requires_release_and_closes_after_the_owner_returns() {
        let key = identity("normal");
        let hold = register_key(key.clone()).unwrap();
        let owner = tokio::spawn(async move {
            let _exit = enter_key(&key).await.unwrap();
        });
        bounded(hold.entered()).await.unwrap();
        assert!(!hold.is_released());
        assert!(!hold.has_exited());
        hold.release();
        bounded(owner).await.unwrap();
        bounded(hold.exited()).await.unwrap();
    }
    #[tokio::test]
    async fn cancelling_the_held_future_publishes_exit() {
        let key = identity("cancel");
        let hold = register_key(key.clone()).unwrap();
        let owner = tokio::spawn(async move {
            let _exit = enter_key(&key).await.unwrap();
        });
        bounded(hold.entered()).await.unwrap();
        owner.abort();
        assert!(bounded(owner).await.unwrap_err().is_cancelled());
        bounded(hold.exited()).await.unwrap();
        assert!(!hold.is_released());
    }
    #[tokio::test]
    async fn panic_unwinds_the_exit_guard() {
        let key = identity("panic");
        let hold = register_key(key.clone()).unwrap();
        let owner = tokio::spawn(async move {
            let _exit = enter_key(&key).await.unwrap();
            panic!("injected fixture panic");
        });
        bounded(hold.entered()).await.unwrap();
        hold.release();
        assert!(bounded(owner).await.unwrap_err().is_panic());
        bounded(hold.exited()).await.unwrap();
    }
    #[tokio::test]
    async fn controller_drop_releases_the_owner_and_foreign_attempt_cannot_enter() {
        let key = identity("drop");
        let hold = register_key(key.clone()).unwrap();
        let mut foreign = key.clone();
        foreign.4 = 2;
        assert!(enter_key(&foreign).await.is_none());
        assert!(register_key(key.clone()).is_err());
        let owner = tokio::spawn(async move {
            let _exit = enter_key(&key).await.unwrap();
        });
        bounded(hold.entered()).await.unwrap();
        let state = Arc::clone(&hold.state);
        drop(hold);
        bounded(owner).await.unwrap();
        assert!(*state.released.borrow());
        assert!(*state.exited.borrow());
    }
}
