use std::path::{Path, PathBuf};

use serde::Serialize;

const WEBUI_CONFIG_KEY: &str = "gateway.webui_dir";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StaticWebUiStatus {
    Ready,
    MissingConfig,
    MissingIndex,
}

impl StaticWebUiStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::MissingConfig => "missing_config",
            Self::MissingIndex => "missing_index",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StaticWebUiSource {
    pub(crate) required: bool,
    pub(crate) config_key: &'static str,
    pub(crate) status: StaticWebUiStatus,
    pub(crate) configured_path: Option<PathBuf>,
    pub(crate) available: bool,
    pub(crate) index_path: Option<PathBuf>,
}

impl StaticWebUiSource {
    pub(crate) fn missing_config() -> Self {
        Self {
            required: false,
            config_key: WEBUI_CONFIG_KEY,
            status: StaticWebUiStatus::MissingConfig,
            configured_path: None,
            available: false,
            index_path: None,
        }
    }
}

pub(crate) fn has_webui_index(path: &Path) -> bool {
    path.join("index.html").is_file()
}

pub(crate) fn resolve_static_webui_source(configured_dir: Option<&Path>) -> StaticWebUiSource {
    let Some(path) = configured_dir else {
        return StaticWebUiSource::missing_config();
    };
    let index_path = path.join("index.html");
    if index_path.is_file() {
        return StaticWebUiSource {
            required: false,
            config_key: WEBUI_CONFIG_KEY,
            status: StaticWebUiStatus::Ready,
            configured_path: Some(path.to_path_buf()),
            available: true,
            index_path: Some(index_path),
        };
    }
    StaticWebUiSource {
        required: false,
        config_key: WEBUI_CONFIG_KEY,
        status: StaticWebUiStatus::MissingIndex,
        configured_path: Some(path.to_path_buf()),
        available: false,
        index_path: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_webui_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "cowd-gateway-static-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).expect("create temp webui dir");
        path
    }

    #[test]
    fn static_webui_missing_config_is_optional() {
        let source = resolve_static_webui_source(None);

        assert!(!source.required);
        assert_eq!(source.config_key, "gateway.webui_dir");
        assert_eq!(source.status, StaticWebUiStatus::MissingConfig);
        assert!(!source.available);
        assert!(source.configured_path.is_none());
        assert!(source.index_path.is_none());
    }

    #[test]
    fn static_webui_missing_index_is_optional() {
        let dir = temp_webui_dir("missing-index");

        let source = resolve_static_webui_source(Some(&dir));

        assert!(!source.required);
        assert_eq!(source.status, StaticWebUiStatus::MissingIndex);
        assert_eq!(source.configured_path.as_deref(), Some(dir.as_path()));
        assert!(!source.available);
        assert!(source.index_path.is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn static_webui_ready_from_configured_dir() {
        let dir = temp_webui_dir("ready");
        std::fs::write(dir.join("index.html"), "<!doctype html>").expect("write index");

        let source = resolve_static_webui_source(Some(&dir));

        assert!(!source.required);
        assert_eq!(source.status, StaticWebUiStatus::Ready);
        assert_eq!(source.configured_path.as_deref(), Some(dir.as_path()));
        assert!(source.available);
        let expected_index = dir.join("index.html");
        assert_eq!(source.index_path.as_deref(), Some(expected_index.as_path()));
        let _ = std::fs::remove_dir_all(dir);
    }
}
