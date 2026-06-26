use std::path::PathBuf;

pub(crate) fn default_skill_install_root() -> std::io::Result<PathBuf> {
    let root = std::env::var("COWD_CONFIG_HOME")
        .or_else(|_| std::env::var("CC_CONFIG_HOME"))
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|home| PathBuf::from(home).join(".cowd")))
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::NotFound, error))?;
    Ok(root.join("skills"))
}
