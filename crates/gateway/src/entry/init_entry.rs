use crate::CliOutputFormat;

pub(crate) fn init_claude_md() -> Result<String, Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    Ok(crate::init::initialize_repo(&cwd)?.render())
}

pub(crate) fn run_init(output_format: CliOutputFormat) -> Result<(), Box<dyn std::error::Error>> {
    let message = init_claude_md()?;
    match output_format {
        CliOutputFormat::Text => println!("{message}"),
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&init_json_value(&message))?
        ),
    }
    Ok(())
}

pub(crate) fn init_json_value(message: &str) -> serde_json::Value {
    serde_json::json!({
        "kind": "init",
        "message": message,
    })
}
