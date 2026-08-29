//! Repository architecture inventory and release governance.

use std::path::{Path, PathBuf};

mod duplicate_authority;
mod inventory;
mod source_size;
mod structural_limits;

#[derive(Debug, Clone)]
struct Roots {
    core: PathBuf,
    edge: PathBuf,
}

impl Roots {
    fn parse(arguments: &[String]) -> Result<(Self, Vec<String>), String> {
        let mut core = None;
        let mut edge = None;
        let mut rest = Vec::new();
        let mut index = 0;
        while index < arguments.len() {
            match arguments[index].as_str() {
                "--core" => {
                    index += 1;
                    core = arguments.get(index).map(PathBuf::from);
                }
                "--edge" => {
                    index += 1;
                    edge = arguments.get(index).map(PathBuf::from);
                }
                value => rest.push(value.to_owned()),
            }
            index += 1;
        }
        let core = core.unwrap_or(std::env::current_dir().map_err(|error| error.to_string())?);
        let edge =
            edge.unwrap_or_else(|| core.parent().unwrap_or(Path::new(".")).join("cowd-edge"));
        if !core.join("Cargo.toml").is_file() {
            return Err(format!(
                "Core root is not a Cargo workspace: {}",
                core.display()
            ));
        }
        if !edge.join("surfaces/webui/package.json").is_file() {
            return Err(format!(
                "Edge root is not a Cowd Edge checkout: {}",
                edge.display()
            ));
        }
        Ok((Self { core, edge }, rest))
    }
}

pub(crate) fn run_cli(arguments: &[String]) -> Result<(), String> {
    let Some((command, tail)) = arguments.split_first() else {
        return Err(usage());
    };
    if command == "help" || command == "--help" || command == "-h" {
        println!("{}", usage());
        return Ok(());
    }
    let (roots, rest) = Roots::parse(tail)?;
    match command.as_str() {
        "inventory" => inventory::run(&roots, &rest),
        "source-size" => source_size::run(&roots, &rest),
        "structural-limits" => structural_limits::run(&roots, &rest),
        "duplicate-authority" => duplicate_authority::run(&roots, &rest),
        "audit" => {
            inventory::run(&roots, &["--check".to_owned()])?;
            source_size::run(&roots, &["--check".to_owned()])?;
            structural_limits::run(&roots, &["--check".to_owned()])?;
            duplicate_authority::run(&roots, &["--check".to_owned()])
        }
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: cargo xtask architecture <inventory|source-size|structural-limits|duplicate-authority|audit> [--core PATH] [--edge PATH] [--output PATH] [--check]".to_owned()
}

fn option_path(arguments: &[String], name: &str) -> Result<Option<PathBuf>, String> {
    let Some(index) = arguments.iter().position(|argument| argument == name) else {
        return Ok(None);
    };
    arguments
        .get(index + 1)
        .map(PathBuf::from)
        .map(Some)
        .ok_or_else(|| format!("{name} requires a path"))
}

fn has_flag(arguments: &[String], name: &str) -> bool {
    arguments.iter().any(|argument| argument == name)
}
