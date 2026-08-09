use std::{env, fs, path::PathBuf};

use cowd_app_sdk::presentation::{
    result_shape_schema, result_shape_schema_digest, PRESENTATION_SCHEMA_ID,
    PRESENTATION_SCHEMA_VERSION,
};
use serde_json::json;

fn main() {
    let output = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: presentation_schema <output.json>");
    let document = json!({
        "contract_id": PRESENTATION_SCHEMA_ID,
        "contract_version": PRESENTATION_SCHEMA_VERSION,
        "schema_sha256": result_shape_schema_digest(),
        "schema": result_shape_schema(),
    });
    let bytes = serde_json::to_vec_pretty(&document).expect("serialize presentation schema");
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).expect("create presentation contract directory");
    }
    fs::write(output, bytes).expect("write presentation schema");
}
