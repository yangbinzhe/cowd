use std::path::PathBuf;

pub fn dot_dir() -> String {
    std::env::var("COWD_DIR_NAME").unwrap_or_else(|_| ".cowd".to_string())
}

pub fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn config_home_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("COWD_CONFIG_HOME") {
        return PathBuf::from(path);
    }
    home_dir().join(dot_dir())
}
