use std::path::{Path, PathBuf};

pub struct ProjectState {
    path: PathBuf,
}

impl ProjectState {
    pub fn load_or_create(cwd: &Path) -> std::io::Result<Self> {
        let cowd_dir = cwd.join(".cowd");
        std::fs::create_dir_all(&cowd_dir).ok();
        let path = cowd_dir.join("state.md");
        if !path.exists() {
            std::fs::write(&path, "# Project State\n\n## Decisions\n\n## Active Tasks\n\n## Completed\n\n## Blockers\n\n")?;
        }
        Ok(Self { path })
    }

    pub fn render(&self) -> String {
        std::fs::read_to_string(&self.path).unwrap_or_else(|_| "State not available.".into())
    }

    pub fn add_decision(&self, content: &str) -> std::io::Result<()> {
        let mut text = std::fs::read_to_string(&self.path)?;
        let now = chrono::Utc::now().format("%Y-%m-%d").to_string();
        if let Some(pos) = text.find("## Decisions\n") {
            text.insert_str(pos + 15, &format!("- {}: {}\n", now, content));
        }
        std::fs::write(&self.path, text)
    }

    pub fn add_task(&self, description: &str) -> std::io::Result<()> {
        let mut text = std::fs::read_to_string(&self.path)?;
        if let Some(pos) = text.find("## Active Tasks\n") {
            text.insert_str(pos + 17, &format!("- [ ] {}\n", description));
        }
        std::fs::write(&self.path, text)
    }
}
