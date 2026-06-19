use std::path::{Path, PathBuf};

pub(crate) fn run_install(
    systemd: bool,
    path: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let raw_install_dir = path
        .map(PathBuf::from)
        .unwrap_or_else(runtime::cowd_dirs::config_home_dir);
    let install_dir = if raw_install_dir.file_name().and_then(|name| name.to_str()) == Some("bin") {
        raw_install_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or(raw_install_dir)
    } else {
        raw_install_dir
    };
    let bin_dir = install_dir.join("bin");
    std::fs::create_dir_all(&bin_dir)?;

    let current_exe = std::env::current_exe()?;
    let target = bin_dir.join("cowd");
    std::fs::copy(&current_exe, &target)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))?;
    }

    println!("Installed cowd to {}", target.display());
    println!("WebUI assets are optional; configure gateway.webui_dir to enable browser UI.");

    if systemd {
        let unit = format!(
            r#"[Unit]
Description=COWD Gateway Process
After=network.target

[Service]
ExecStart={} gateway start
Restart=always
RestartSec=5
Environment=RUST_LOG=warn

[Install]
WantedBy=default.target
"#,
            target.display()
        );
        let home_dir = std::env::var("HOME").unwrap_or_else(|_| "~".to_string());
        let unit_path = PathBuf::from(&home_dir)
            .join(".config")
            .join("systemd")
            .join("user")
            .join("cowd-gateway.service");
        if let Some(parent) = unit_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&unit_path, &unit)?;
        println!("Created systemd unit at {}", unit_path.display());
        println!(
            "To enable: systemctl --user enable --now {}",
            unit_path.display()
        );
    }
    Ok(())
}
