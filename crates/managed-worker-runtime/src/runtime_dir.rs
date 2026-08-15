use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

use crate::{ManagedWorkerError, ManagedWorkerResult};

/// Private per-generation directory. Only known ephemeral entries are removed.
#[derive(Debug, Clone)]
pub struct WorkerRuntimeDir {
    root: PathBuf,
    owner_uid: u32,
}

impl WorkerRuntimeDir {
    pub fn create(root: impl Into<PathBuf>) -> ManagedWorkerResult<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|error| ManagedWorkerError::io(&root, error))?;
        let initial_metadata =
            fs::symlink_metadata(&root).map_err(|error| ManagedWorkerError::io(&root, error))?;
        if !initial_metadata.file_type().is_dir() || initial_metadata.file_type().is_symlink() {
            return Err(ManagedWorkerError::InvalidSpec(format!(
                "runtime root is not a real directory: {}",
                root.display()
            )));
        }
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .map_err(|error| ManagedWorkerError::io(&root, error))?;
        let metadata =
            fs::symlink_metadata(&root).map_err(|error| ManagedWorkerError::io(&root, error))?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(ManagedWorkerError::InvalidSpec(format!(
                "runtime root changed while securing it: {}",
                root.display()
            )));
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ManagedWorkerError::InvalidSpec(format!(
                "runtime root is not owner-only: {}",
                root.display()
            )));
        }
        Ok(Self {
            root,
            owner_uid: metadata.uid(),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn owner_uid(&self) -> u32 {
        self.owner_uid
    }

    #[must_use]
    pub fn socket_path(&self) -> PathBuf {
        self.root.join("w.sock")
    }

    #[must_use]
    pub fn credential_path(&self) -> PathBuf {
        self.root.join("credential")
    }

    #[must_use]
    pub fn identity_path(&self) -> PathBuf {
        self.root.join("worker-identity.json")
    }

    #[must_use]
    pub fn identity_temp_path(&self) -> PathBuf {
        self.root.join("worker-identity.tmp")
    }

    #[must_use]
    pub fn launch_spec_path(&self) -> PathBuf {
        self.root.join("launch-spec.json")
    }

    #[must_use]
    pub fn status_socket_path(&self) -> PathBuf {
        self.root.join("l.sock")
    }

    pub fn cleanup_ephemeral(&self) -> ManagedWorkerResult<()> {
        for path in [
            self.socket_path(),
            self.credential_path(),
            self.launch_spec_path(),
            self.status_socket_path(),
            self.identity_temp_path(),
        ] {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(ManagedWorkerError::io(path, error)),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "managed-worker-runtime-{label}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ))
    }

    #[test]
    fn runtime_directory_is_private_and_cleanup_is_bounded() {
        let root = temp_path("runtime");
        let runtime = WorkerRuntimeDir::create(&root).expect("runtime");
        assert_eq!(
            fs::metadata(&root).expect("metadata").permissions().mode() & 0o777,
            0o700
        );
        fs::write(runtime.socket_path(), b"stale").expect("socket fixture");
        fs::write(runtime.credential_path(), b"stale").expect("credential fixture");
        fs::write(root.join("preserved"), b"data").expect("preserved fixture");
        runtime.cleanup_ephemeral().expect("cleanup");
        assert!(!runtime.socket_path().exists());
        assert!(!runtime.credential_path().exists());
        assert!(root.join("preserved").exists());
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn runtime_directory_rejects_a_symlink_without_repermissioning_its_target() {
        let target = temp_path("symlink-target");
        let link = temp_path("symlink-link");
        fs::create_dir(&target).expect("target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755))
            .expect("target permissions");
        symlink(&target, &link).expect("link");
        assert!(matches!(
            WorkerRuntimeDir::create(&link),
            Err(ManagedWorkerError::InvalidSpec(_))
        ));
        assert_eq!(
            fs::metadata(&target)
                .expect("target metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
        fs::remove_file(link).expect("remove link");
        fs::remove_dir(target).expect("remove target");
    }
}
