use std::{
    fs,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::{rngs::OsRng, RngCore};

use crate::{ManagedWorkerError, ManagedWorkerResult, WorkerRuntimeDir};

/// Secret bytes are overwritten when their owner is dropped.
#[derive(Debug)]
pub struct CredentialSecret(Vec<u8>);

impl CredentialSecret {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for CredentialSecret {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Owner-only bootstrap credential with an atomic single-consumer file handoff.
#[derive(Debug)]
pub struct CredentialLease {
    path: PathBuf,
    owner_uid: u32,
    consumed: AtomicBool,
}

impl CredentialLease {
    pub fn create(runtime: &WorkerRuntimeDir) -> ManagedWorkerResult<(Self, CredentialSecret)> {
        let path = runtime.credential_path();
        if path.exists() {
            return Err(ManagedWorkerError::InvalidSpec(format!(
                "credential path already exists: {}",
                path.display()
            )));
        }
        let mut raw = [0_u8; 32];
        OsRng.fill_bytes(&mut raw);
        let encoded = URL_SAFE_NO_PAD.encode(raw).into_bytes();
        raw.fill(0);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .map_err(|error| ManagedWorkerError::io(&path, error))?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_all())
            .map_err(|error| ManagedWorkerError::io(&path, error))?;
        let metadata = file
            .metadata()
            .map_err(|error| ManagedWorkerError::io(&path, error))?;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ManagedWorkerError::CredentialPermissions {
                mode: metadata.permissions().mode() & 0o777,
            });
        }
        if metadata.uid() != runtime.owner_uid() {
            return Err(ManagedWorkerError::CredentialOwnerChanged {
                expected: runtime.owner_uid(),
                actual: metadata.uid(),
            });
        }
        Ok((
            Self {
                path,
                owner_uid: metadata.uid(),
                consumed: AtomicBool::new(false),
            },
            CredentialSecret(encoded),
        ))
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn consume(&self) -> ManagedWorkerResult<CredentialSecret> {
        if self.consumed.swap(true, Ordering::AcqRel) {
            return Err(ManagedWorkerError::CredentialConsumed);
        }
        let result = self.consume_file();
        if result.is_err() {
            self.consumed.store(false, Ordering::Release);
        }
        result
    }

    fn consume_file(&self) -> ManagedWorkerResult<CredentialSecret> {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(|error| ManagedWorkerError::io(&self.path, error))?;
        let metadata = file
            .metadata()
            .map_err(|error| ManagedWorkerError::io(&self.path, error))?;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(ManagedWorkerError::CredentialPermissions { mode });
        }
        if metadata.uid() != self.owner_uid {
            return Err(ManagedWorkerError::CredentialOwnerChanged {
                expected: self.owner_uid,
                actual: metadata.uid(),
            });
        }
        let mut secret = Vec::new();
        file.read_to_end(&mut secret)
            .map_err(|error| ManagedWorkerError::io(&self.path, error))?;
        file.set_len(0)
            .and_then(|()| file.sync_all())
            .map_err(|error| ManagedWorkerError::io(&self.path, error))?;
        fs::remove_file(&self.path).map_err(|error| ManagedWorkerError::io(&self.path, error))?;
        if secret.len() < 32 || secret.len() > 128 {
            secret.fill(0);
            return Err(ManagedWorkerError::InvalidSpec(
                "credential length is outside the accepted range".to_string(),
            ));
        }
        Ok(CredentialSecret(secret))
    }
}

impl Drop for CredentialLease {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %self.path.display(), %error, "credential cleanup failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime(label: &str) -> WorkerRuntimeDir {
        WorkerRuntimeDir::create(std::env::temp_dir().join(format!(
            "managed-worker-credential-{label}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        )))
        .expect("runtime")
    }

    #[test]
    fn credential_is_owner_only_and_single_use() {
        let runtime = runtime("single-use");
        let (lease, expected) = CredentialLease::create(&runtime).expect("credential");
        assert_eq!(
            fs::metadata(lease.path())
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let observed = lease.consume().expect("consume");
        assert_eq!(observed.as_bytes(), expected.as_bytes());
        assert!(!lease.path().exists());
        assert!(matches!(
            lease.consume(),
            Err(ManagedWorkerError::CredentialConsumed)
        ));
        fs::remove_dir_all(runtime.root()).expect("cleanup");
    }

    #[test]
    fn permissive_credential_is_rejected_without_deleting_evidence() {
        let runtime = runtime("permissions");
        let (lease, _expected) = CredentialLease::create(&runtime).expect("credential");
        fs::set_permissions(lease.path(), fs::Permissions::from_mode(0o644))
            .expect("make permissive");
        assert!(matches!(
            lease.consume(),
            Err(ManagedWorkerError::CredentialPermissions { .. })
        ));
        assert!(lease.path().exists());
        fs::remove_dir_all(runtime.root()).expect("cleanup");
    }
}
