//! Lease-scoped filesystem boundary for workspace tools.

use std::env;
use std::io;
use std::path::{Component, Path, PathBuf};

/// Resolves tool-supplied paths without allowing them to leave the workspace
/// or enter COWD's control-plane state.
#[derive(Debug, Clone)]
pub struct WorkspacePathPolicy {
    workspace_root: PathBuf,
    protected_paths: Vec<PathBuf>,
}

impl WorkspacePathPolicy {
    #[must_use]
    pub fn new(workspace_root: impl AsRef<Path>) -> Self {
        Self::with_config_home(
            workspace_root,
            env::var_os("COWD_CONFIG_HOME").map(PathBuf::from),
        )
    }

    fn with_config_home(workspace_root: impl AsRef<Path>, config_home: Option<PathBuf>) -> Self {
        let workspace_root = absolute_lexical(workspace_root.as_ref());
        let workspace_root = workspace_root.canonicalize().unwrap_or(workspace_root);
        let mut protected_paths = vec![workspace_root.join(".cowd")];

        if let Some(config_home) = config_home {
            protected_paths.push(canonical_or_lexical(&config_home));
        }

        Self {
            workspace_root,
            protected_paths,
        }
    }

    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Resolve a file or directory. Existing symlinks must resolve back into
    /// the workspace; missing leaves are checked through their nearest
    /// existing parent so writes cannot traverse a symlinked parent.
    pub fn resolve(&self, input: &str) -> io::Result<PathBuf> {
        if input.trim().is_empty() {
            return Err(denied("path must not be empty"));
        }

        let raw = Path::new(input);
        let candidate = if raw.is_absolute() {
            lexical_normalize(raw)
        } else {
            lexical_normalize(&self.workspace_root.join(raw))
        };
        self.ensure_lexically_allowed(&candidate)?;

        let resolved = resolve_existing_ancestor(&candidate)?;
        self.ensure_resolved_allowed(&resolved)?;
        Ok(resolved)
    }

    /// Validate a glob expression before expansion. The non-glob prefix is
    /// resolved as a path, and every resulting match must be resolved again.
    pub fn resolve_glob_pattern(&self, pattern: &str) -> io::Result<PathBuf> {
        if pattern.trim().is_empty() {
            return Err(denied("glob pattern must not be empty"));
        }
        let raw = Path::new(pattern);
        let candidate = if raw.is_absolute() {
            lexical_normalize(raw)
        } else {
            lexical_normalize(&self.workspace_root.join(raw))
        };
        self.ensure_lexically_allowed(&candidate)?;

        let prefix = non_glob_prefix(&candidate);
        let resolved_prefix = resolve_existing_ancestor(&prefix)?;
        self.ensure_resolved_allowed(&resolved_prefix)?;
        Ok(candidate)
    }

    pub fn ensure_resolved_path(&self, path: &Path) -> io::Result<PathBuf> {
        let resolved = path.canonicalize()?;
        self.ensure_resolved_allowed(&resolved)?;
        Ok(resolved)
    }

    fn ensure_lexically_allowed(&self, candidate: &Path) -> io::Result<()> {
        if !candidate.starts_with(&self.workspace_root) {
            return Err(denied("path must remain inside the leased workspace"));
        }
        if self.is_protected(candidate) {
            return Err(denied("path targets protected COWD control-plane state"));
        }
        Ok(())
    }

    fn ensure_resolved_allowed(&self, resolved: &Path) -> io::Result<()> {
        if !resolved.starts_with(&self.workspace_root) {
            return Err(denied(
                "path resolves outside the leased workspace (symlink and proc-fd paths are not allowed)",
            ));
        }
        if self.is_protected(resolved) {
            return Err(denied("path targets protected COWD control-plane state"));
        }
        Ok(())
    }

    fn is_protected(&self, path: &Path) -> bool {
        self.protected_paths.iter().any(|protected| {
            path == protected
                || path.starts_with(protected)
                || canonical_or_lexical(protected) == path
        })
    }
}

fn denied(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

fn absolute_lexical(path: &Path) -> PathBuf {
    if path.is_absolute() {
        lexical_normalize(path)
    } else {
        let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        lexical_normalize(&cwd.join(path))
    }
}

fn canonical_or_lexical(path: &Path) -> PathBuf {
    let path = absolute_lexical(path);
    path.canonicalize().unwrap_or(path)
}

fn resolve_existing_ancestor(candidate: &Path) -> io::Result<PathBuf> {
    if let Ok(resolved) = candidate.canonicalize() {
        return Ok(resolved);
    }

    let mut ancestor = candidate;
    while std::fs::symlink_metadata(ancestor).is_err() {
        ancestor = ancestor
            .parent()
            .ok_or_else(|| denied("path has no existing ancestor"))?;
    }
    let resolved_ancestor = ancestor.canonicalize()?;
    let suffix = candidate
        .strip_prefix(ancestor)
        .map_err(|_| denied("path could not be resolved"))?;
    Ok(resolved_ancestor.join(suffix))
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(component) => normalized.push(component),
        }
    }
    normalized
}

fn non_glob_prefix(path: &Path) -> PathBuf {
    let mut prefix = PathBuf::new();
    for component in path.components() {
        let value = component.as_os_str().to_string_lossy();
        if value.contains(['*', '?', '[', '{']) {
            break;
        }
        prefix.push(component.as_os_str());
    }
    if prefix.as_os_str().is_empty() {
        PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
    } else {
        prefix
    }
}

#[cfg(test)]
mod tests {
    use super::WorkspacePathPolicy;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("cowd-path-policy-{name}-{unique}"));
        fs::create_dir_all(&path).expect("temp dir");
        path
    }

    #[test]
    fn permits_workspace_relative_paths_and_rejects_escapes() {
        let root = temp_dir("relative");
        fs::write(root.join("allowed.txt"), "ok").expect("file");
        let policy = WorkspacePathPolicy::new(&root);

        assert_eq!(
            policy.resolve("allowed.txt").expect("allowed"),
            root.join("allowed.txt")
        );
        assert!(policy.resolve("../outside.txt").is_err());
        assert!(policy.resolve("/etc/passwd").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_and_proc_fd_escapes() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("symlink");
        let outside = temp_dir("outside").join("secret.txt");
        fs::write(&outside, "secret").expect("outside file");
        symlink(&outside, root.join("escape")).expect("symlink");
        let policy = WorkspacePathPolicy::new(&root);

        assert!(policy.resolve("escape").is_err());
        assert!(policy.resolve("/proc/self/fd/0").is_err());
    }

    #[test]
    fn rejects_workspace_control_plane_paths() {
        let root = temp_dir("control-plane");
        fs::create_dir_all(root.join(".cowd").join("auth-broker")).expect("control plane");
        let policy = WorkspacePathPolicy::new(&root);

        assert!(policy.resolve(".cowd/runtime.sqlite").is_err());
        assert!(policy.resolve(".cowd/auth-broker/token").is_err());
    }

    #[test]
    fn rejects_config_home_when_it_is_inside_the_workspace() {
        let root = temp_dir("config-home");
        let config_home = root.join("state");
        fs::create_dir_all(&config_home).expect("config home");
        let policy = WorkspacePathPolicy::with_config_home(&root, Some(config_home));

        assert!(policy.resolve("state/auth-broker.sqlite").is_err());
    }
}
