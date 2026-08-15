use std::path::PathBuf;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cowd_reference_app::{
    discover_bundles, install_bundle, package, validate_bundle, verifying_key_bytes,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("reference APP bundle failed: {error}");
        std::process::exit(2);
    }
}

fn run() -> cowd_reference_app::Result<()> {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("package") => {
            let worker = value(&mut arguments, "--worker")?;
            let output = value(&mut arguments, "--output")?;
            if arguments.next().is_some() {
                return Err(cowd_reference_app::ReferenceError::Bundle(
                    "unexpected package argument".to_owned(),
                ));
            }
            let manifest = package(&worker, &output)?;
            println!(
                "{}",
                serde_json::json!({
                    "bundle": output,
                    "app_id": manifest.app_id.0,
                    "artifact_version": manifest.artifact_version,
                    "manifest_digest": manifest.integrity.manifest_digest.0,
                    "key_id": manifest.signature.key_id,
                    "public_key_base64url": URL_SAFE_NO_PAD.encode(verifying_key_bytes())
                })
            );
        }
        Some("verify") => {
            let bundle = value(&mut arguments, "--bundle")?;
            if arguments.next().is_some() {
                return Err(cowd_reference_app::ReferenceError::Bundle(
                    "unexpected verify argument".to_owned(),
                ));
            }
            let manifest = validate_bundle(&bundle)?;
            println!(
                "{}",
                serde_json::json!({
                    "valid": true,
                    "app_id": manifest.app_id.0,
                    "manifest_digest": manifest.integrity.manifest_digest.0
                })
            );
        }
        Some("install") => {
            let bundle = value(&mut arguments, "--bundle")?;
            let apps_root = value(&mut arguments, "--apps-root")?;
            if arguments.next().is_some() {
                return Err(cowd_reference_app::ReferenceError::Bundle(
                    "unexpected install argument".to_owned(),
                ));
            }
            let installed = install_bundle(&bundle, &apps_root)?;
            println!("{}", serde_json::json!({"installed": installed}));
        }
        Some("discover") => {
            let apps_root = value(&mut arguments, "--apps-root")?;
            if arguments.next().is_some() {
                return Err(cowd_reference_app::ReferenceError::Bundle(
                    "unexpected discover argument".to_owned(),
                ));
            }
            let apps = discover_bundles(&apps_root)?
                .into_iter()
                .map(|(path, manifest)| serde_json::json!({"path":path,"app_id":manifest.app_id.0,"artifact_version":manifest.artifact_version}))
                .collect::<Vec<_>>();
            println!("{}", serde_json::json!({"apps":apps}));
        }
        _ => {
            return Err(cowd_reference_app::ReferenceError::Bundle(
                "usage: reference-app-bundle package|verify|install|discover".to_owned(),
            ));
        }
    }
    Ok(())
}

fn value(
    arguments: &mut impl Iterator<Item = String>,
    expected: &str,
) -> cowd_reference_app::Result<PathBuf> {
    if arguments.next().as_deref() != Some(expected) {
        return Err(cowd_reference_app::ReferenceError::Bundle(format!(
            "expected {expected}"
        )));
    }
    arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| cowd_reference_app::ReferenceError::Bundle(format!("missing {expected}")))
}
