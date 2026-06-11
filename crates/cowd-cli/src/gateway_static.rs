use std::path::PathBuf;

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StaticWebUiSource {
    pub(crate) kind: String,
    pub(crate) path: PathBuf,
    pub(crate) available: bool,
    pub(crate) index_path: PathBuf,
}

impl StaticWebUiSource {
    fn new(kind: impl Into<String>, path: PathBuf) -> Self {
        let index_path = path.join("index.html");
        Self {
            kind: kind.into(),
            available: index_path.is_file(),
            index_path,
            path,
        }
    }
}

pub(crate) fn has_webui_index(path: &std::path::Path) -> bool {
    path.join("index.html").is_file()
}

pub(crate) fn resolve_static_webui_source() -> StaticWebUiSource {
    if let Some(path) = std::env::var_os("COWD_WEBUI_DIR").map(PathBuf::from) {
        let source = StaticWebUiSource::new("env:COWD_WEBUI_DIR", path);
        if source.available {
            return source;
        }
        tracing::warn!(
            path = %source.path.display(),
            "COWD_WEBUI_DIR does not contain index.html; trying fallback paths"
        );
    }

    if let Ok(cwd) = std::env::current_dir() {
        let source = StaticWebUiSource::new("source-tree:webui", cwd.join("webui"));
        if source.available {
            return source;
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            if let Some(install_dir) = exe_dir.parent() {
                let source =
                    StaticWebUiSource::new("installed:exe-dir/../webui", install_dir.join("webui"));
                if source.available {
                    return source;
                }
            }
            let source = StaticWebUiSource::new("installed:exe-dir/webui", exe_dir.join("webui"));
            if source.available {
                return source;
            }
        }
    }

    let fallback_path = std::env::current_dir()
        .map(|cwd| cwd.join("webui"))
        .unwrap_or_else(|_| PathBuf::from("webui"));
    let source = StaticWebUiSource::new("missing:fallback-webui", fallback_path);
    tracing::warn!(
        path = %source.path.display(),
        "WebUI index.html was not found; static file serving may return 404"
    );
    source
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn restore_env(previous: Option<std::ffi::OsString>) {
        if let Some(value) = previous {
            std::env::set_var("COWD_WEBUI_DIR", value);
        } else {
            std::env::remove_var("COWD_WEBUI_DIR");
        }
    }

    fn temp_webui_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "cowd-gateway-static-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).expect("create temp webui dir");
        path
    }

    #[test]
    fn static_webui_index_served_from_packaged_assets() {
        let _guard = env_lock().lock().expect("env lock");
        let previous = std::env::var_os("COWD_WEBUI_DIR");
        let dir = temp_webui_dir("packaged");
        std::fs::write(dir.join("index.html"), "<!doctype html>").expect("write index");
        std::env::set_var("COWD_WEBUI_DIR", &dir);

        let source = resolve_static_webui_source();

        assert_eq!(source.kind, "env:COWD_WEBUI_DIR");
        assert_eq!(source.path, dir);
        assert!(source.available);
        assert!(source.index_path.ends_with("index.html"));
        restore_env(previous);
        let _ = std::fs::remove_dir_all(source.path);
    }

    #[test]
    fn static_webui_missing_env_falls_back_without_panicking() {
        let _guard = env_lock().lock().expect("env lock");
        let previous = std::env::var_os("COWD_WEBUI_DIR");
        let dir = temp_webui_dir("missing-index");
        std::env::set_var("COWD_WEBUI_DIR", &dir);

        let source = resolve_static_webui_source();

        assert_ne!(source.path, dir);
        assert!(!source.kind.is_empty());
        restore_env(previous);
        let _ = std::fs::remove_dir_all(dir);
    }
}
