//! F20: TransactionManager — reversible file edits with rollback.
//!
//! Provides a RAII-based transaction system where side effects (file writes,
//! tool executions) can be rolled back in LIFO order if the operation is
//! cancelled or fails partway through.
//!
//! Only `FileEdit` and `ExecuteTool` need reversible wrappers for v1.
//! `ApiCall`, `MemorySearch`, and `EmitEvent` are read-only and do not
//! participate in transactions.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// ReversibleEffect trait
// ---------------------------------------------------------------------------

/// A side effect that can be applied and later rolled back.
///
/// Implementations must store enough state to undo the effect in `rollback()`.
pub trait ReversibleEffect: fmt::Debug + Send + Sync {
    /// Execute the side effect. Called once when the effect is first recorded.
    fn apply(&self) -> Result<(), TransactionError>;

    /// Undo the side effect, restoring the previous state.
    fn rollback(&self) -> Result<(), TransactionError>;

    /// Human-readable label for auditing / debugging.
    fn label(&self) -> &str;
}

// ---------------------------------------------------------------------------
// TransactionError
// ---------------------------------------------------------------------------

/// Errors that can occur during transaction operations.
#[derive(Debug)]
pub enum TransactionError {
    /// An I/O error occurred while applying or rolling back an effect.
    Io(io::Error),
    /// Attempted to use a transaction guard after it was already consumed.
    GuardConsumed,
    /// A custom error message.
    Other(String),
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionError::Io(e) => write!(f, "I/O error: {}", e),
            TransactionError::GuardConsumed => write!(f, "Transaction guard already consumed"),
            TransactionError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for TransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TransactionError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for TransactionError {
    fn from(e: io::Error) -> Self {
        TransactionError::Io(e)
    }
}

// ---------------------------------------------------------------------------
// FileEditEffect
// ---------------------------------------------------------------------------

/// A reversible file edit that snapshots the original content before applying
/// the new content, and restores the snapshot on rollback.
#[derive(Debug)]
pub struct FileEditEffect {
    /// Absolute path to the file being edited.
    path: PathBuf,
    /// Original file content captured before the write (None = file did not exist).
    snapshot: Option<Vec<u8>>,
    /// The new content to write.
    new_content: Vec<u8>,
}

impl FileEditEffect {
    /// Prepare a file edit effect. The snapshot is captured immediately.
    ///
    /// Returns an error if the snapshot read fails (permissions, etc.).
    /// Writing the new content is deferred to `apply()`.
    pub fn prepare(
        path: impl Into<PathBuf>,
        new_content: impl Into<Vec<u8>>,
    ) -> Result<Self, TransactionError> {
        let path: PathBuf = path.into();
        let snapshot = if path.exists() {
            Some(fs::read(&path)?)
        } else {
            None
        };
        Ok(Self {
            path,
            snapshot,
            new_content: new_content.into(),
        })
    }

    /// Return a reference to the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the size of the snapshot in bytes (0 if file did not exist).
    pub fn snapshot_size(&self) -> usize {
        self.snapshot.as_ref().map_or(0, |s| s.len())
    }

    /// Return the size of the new content in bytes.
    pub fn new_content_size(&self) -> usize {
        self.new_content.len()
    }
}

impl ReversibleEffect for FileEditEffect {
    fn apply(&self) -> Result<(), TransactionError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.path, &self.new_content)?;
        Ok(())
    }

    fn rollback(&self) -> Result<(), TransactionError> {
        match &self.snapshot {
            Some(original) => {
                fs::write(&self.path, original)?;
            }
            None => {
                if self.path.exists() {
                    fs::remove_file(&self.path)?;
                }
            }
        }
        Ok(())
    }

    fn label(&self) -> &str {
        "FileEdit"
    }
}

// ---------------------------------------------------------------------------
// TransactionManager
// ---------------------------------------------------------------------------

/// Manages a stack of reversible effects for a single transaction.
///
/// Effects are pushed in application order and rolled back in LIFO order.
/// Use `begin()` to start a transaction; the returned `TransactionGuard`
/// auto-rolls back on drop unless `commit()` is called.
///
/// # Example
///
/// ```rust,no_run
/// use cowd_memory::transaction::{TransactionManager, FileEditEffect};
///
/// let manager = TransactionManager::new();
/// let mut guard = manager.begin();
/// let effect = FileEditEffect::prepare("/tmp/test.txt", "hello").unwrap();
/// guard.record(effect).unwrap();
/// guard.commit();
/// ```
#[derive(Debug, Default)]
pub struct TransactionManager {
    /// Stack of effects recorded during the current transaction.
    effects: Mutex<Vec<Box<dyn ReversibleEffect>>>,
}

impl TransactionManager {
    /// Create a new empty transaction manager.
    pub fn new() -> Self {
        Self {
            effects: Mutex::new(Vec::new()),
        }
    }

    /// Begin a new transaction. Returns a [`TransactionGuard`] that must be
    /// explicitly committed, or will auto-rollback on drop.
    pub fn begin(&self) -> TransactionGuard<'_> {
        TransactionGuard {
            manager: self,
            effects: Vec::new(),
            committed: false,
            rolled_back: false,
        }
    }

    /// Roll back all effects recorded in the manager.
    ///
    /// This is the top-level rollback; for guard-based usage prefer
    /// `TransactionGuard::rollback()`.
    pub fn rollback_all(&self) -> Result<(), TransactionError> {
        let mut effects = self.effects.lock();
        Self::rollback_stack(&effects)?;
        effects.clear();
        Ok(())
    }

    /// Commit all effects recorded in the manager (drop the stack).
    pub fn commit_all(&self) {
        self.effects.lock().clear();
    }

    /// Return the number of effects currently in the manager.
    pub fn effect_count(&self) -> usize {
        self.effects.lock().len()
    }

    /// Roll back a slice of effects in LIFO order, eating errors.
    fn rollback_stack(effects: &[Box<dyn ReversibleEffect>]) -> Result<(), TransactionError> {
        let mut last_err = None;
        for effect in effects.iter().rev() {
            if let Err(e) = effect.rollback() {
                tracing::warn!(
                    effect = effect.label(),
                    error = %e,
                    "Rollback of effect failed; continuing"
                );
                last_err = Some(e);
            }
        }
        match last_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// TransactionGuard
// ---------------------------------------------------------------------------

/// RAII guard for a transaction. Created by [`TransactionManager::begin()`].
///
/// On drop, if the guard has not been explicitly committed, all recorded
/// effects are rolled back in LIFO order.
///
/// # Safety note
///
/// The guard tracks a local list of effects recorded during its lifetime
/// and pushes them to the manager only on commit. This prevents partial
/// effects from leaking into the manager on rollback.
#[derive(Debug)]
pub struct TransactionGuard<'m> {
    manager: &'m TransactionManager,
    effects: Vec<Box<dyn ReversibleEffect>>,
    committed: bool,
    rolled_back: bool,
}

impl<'m> TransactionGuard<'m> {
    /// Record a reversible effect in this transaction and apply it.
    ///
    /// The effect's `apply()` is called immediately. If it fails, the
    /// error is returned and the effect is not added to the transaction.
    pub fn record(
        &mut self,
        effect: impl ReversibleEffect + 'static,
    ) -> Result<(), TransactionError> {
        let boxed: Box<dyn ReversibleEffect> = Box::new(effect);
        boxed.apply()?;
        self.effects.push(boxed);
        Ok(())
    }

    /// Record a `FileEditEffect` — convenience for the most common case.
    ///
    /// Equivalent to `guard.record(effect)`. The effect's `apply()` is called
    /// immediately; the snapshot must have been captured at construction time
    /// (via `FileEditEffect::prepare`).
    pub fn record_file_edit(&mut self, effect: FileEditEffect) -> Result<(), TransactionError> {
        self.record(effect)
    }

    /// Commit the transaction: all recorded effects are finalised and will
    /// not be rolled back.
    ///
    /// After calling this, the guard is consumed and the effects are pushed
    /// into the manager so they can be rolled back at a higher level if
    /// needed. For v1 the manager-level rollback is the safety net; typically
    /// the guard is the top-level boundary.
    pub fn commit(mut self) {
        self.committed = true;
        // Push all our effects into the manager's stack so they participate
        // in any higher-level rollback.
        let mut manager_effects = self.manager.effects.lock();
        manager_effects.append(&mut self.effects);
    }

    /// Explicitly roll back all effects recorded in this guard.
    pub fn rollback(mut self) -> Result<(), TransactionError> {
        self.rolled_back = true;
        TransactionManager::rollback_stack(&self.effects)
    }

    /// Returns the number of effects recorded in this guard.
    pub fn effect_count(&self) -> usize {
        self.effects.len()
    }

    /// Returns `true` if the guard has been committed.
    pub fn is_committed(&self) -> bool {
        self.committed
    }

    /// Returns `true` if the guard has been rolled back.
    pub fn is_rolled_back(&self) -> bool {
        self.rolled_back
    }
}

impl Drop for TransactionGuard<'_> {
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back && !self.effects.is_empty() {
            if let Err(e) = TransactionManager::rollback_stack(&self.effects) {
                tracing::error!(
                    error = %e,
                    effect_count = self.effects.len(),
                    "Auto-rollback on guard drop failed"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_file() -> PathBuf {
        let dir = std::env::temp_dir().join("cowd_memory_transaction_test");
        let _ = fs::create_dir_all(&dir);
        dir.join(format!("test_{}.txt", uuid::Uuid::new_v4()))
    }

    #[test]
    fn file_edit_apply_and_rollback() {
        let path = temp_file();
        let original = b"original content";

        // Create initial file
        fs::write(&path, original).unwrap();

        let effect = FileEditEffect::prepare(&path, "new content").unwrap();
        assert!(effect.snapshot.is_some());
        assert_eq!(effect.snapshot.as_ref().unwrap(), original);

        // Apply
        effect.apply().unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "new content");

        // Rollback
        effect.rollback().unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "original content");

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn file_edit_rollback_deletes_new_file() {
        let path = temp_file();
        assert!(!path.exists());

        let effect = FileEditEffect::prepare(&path, "brand new").unwrap();
        assert!(effect.snapshot.is_none());

        effect.apply().unwrap();
        assert!(path.exists());

        effect.rollback().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn guard_commit_keeps_effects() {
        let path = temp_file();
        fs::write(&path, "before").unwrap();

        let manager = TransactionManager::new();
        {
            let mut guard = manager.begin();
            let effect = FileEditEffect::prepare(&path, "after").unwrap();
            guard.record_file_edit(effect).unwrap();
            assert_eq!(guard.effect_count(), 1);
            guard.commit();
        }
        // After commit, the change should persist
        assert_eq!(fs::read_to_string(&path).unwrap(), "after");
        assert_eq!(manager.effect_count(), 1);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn guard_drop_rolls_back() {
        let path = temp_file();
        fs::write(&path, "before").unwrap();

        let manager = TransactionManager::new();
        {
            let mut guard = manager.begin();
            let effect = FileEditEffect::prepare(&path, "after").unwrap();
            guard.record_file_edit(effect).unwrap();
            // Guard dropped without commit — should roll back
        }

        assert_eq!(fs::read_to_string(&path).unwrap(), "before");
        assert_eq!(manager.effect_count(), 0);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn guard_explicit_rollback() {
        let path = temp_file();
        fs::write(&path, "before").unwrap();

        let manager = TransactionManager::new();
        let mut guard = manager.begin();
        let effect = FileEditEffect::prepare(&path, "after").unwrap();
        guard.record_file_edit(effect).unwrap();

        guard.rollback().unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "before");

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn multiple_effects_rollback_lifo() {
        let path_a = temp_file();
        let path_b = temp_file();
        fs::write(&path_a, "a_orig").unwrap();
        fs::write(&path_b, "b_orig").unwrap();

        let manager = TransactionManager::new();
        {
            let mut guard = manager.begin();
            guard
                .record_file_edit(FileEditEffect::prepare(&path_a, "a_new").unwrap())
                .unwrap();
            guard
                .record_file_edit(FileEditEffect::prepare(&path_b, "b_new").unwrap())
                .unwrap();
            // Both applied
            assert_eq!(fs::read_to_string(&path_a).unwrap(), "a_new");
            assert_eq!(fs::read_to_string(&path_b).unwrap(), "b_new");
            // Drop without commit
        }

        // Both rolled back
        assert_eq!(fs::read_to_string(&path_a).unwrap(), "a_orig");
        assert_eq!(fs::read_to_string(&path_b).unwrap(), "b_orig");

        let _ = fs::remove_file(&path_a);
        let _ = fs::remove_file(&path_b);
    }

    #[test]
    fn apply_fails_does_not_record() {
        let path = PathBuf::from("/nonexistent_dir_xyz_zzz/test.txt");
        let effect = FileEditEffect::prepare(&path, "data").unwrap(); // snapshot is fine

        let manager = TransactionManager::new();
        let mut guard = manager.begin();
        // apply() should fail because the parent dir doesn't exist
        let result = guard.record(effect);
        assert!(result.is_err());
        assert_eq!(guard.effect_count(), 0);
    }

    #[test]
    fn transaction_manager_rollback_all() {
        let path = temp_file();
        fs::write(&path, "before").unwrap();

        let manager = TransactionManager::new();
        {
            let mut guard = manager.begin();
            guard
                .record_file_edit(FileEditEffect::prepare(&path, "after").unwrap())
                .unwrap();
            guard.commit();
        }
        assert_eq!(manager.effect_count(), 1);
        manager.rollback_all().unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "before");
        assert_eq!(manager.effect_count(), 0);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn empty_guard_drop_is_noop() {
        let manager = TransactionManager::new();
        {
            let _guard = manager.begin();
            // Empty guard, no effects — drop should be no-op
        }
        assert_eq!(manager.effect_count(), 0);
    }
}
