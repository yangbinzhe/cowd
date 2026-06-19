use crate::CliOutputFormat;

pub(crate) struct PluginEntryOutcome {
    pub(crate) message: String,
    pub(crate) reload_runtime: bool,
}

pub(crate) fn execute_plugin_command(
    action: Option<&str>,
    target: Option<&str>,
) -> Result<PluginEntryOutcome, Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let loader = runtime::ConfigLoader::default_for(&cwd);
    let runtime_config = loader.load()?;
    let mut manager =
        crate::runtime_bootstrap::build_plugin_manager(&cwd, &loader, &runtime_config);
    let result = crate::plugin_static::handle_plugins_slash_command(action, target, &mut manager)?;
    Ok(PluginEntryOutcome {
        message: result.message,
        reload_runtime: result.reload_runtime,
    })
}

pub(crate) fn print_plugin_command(
    action: Option<&str>,
    target: Option<&str>,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = execute_plugin_command(action, target)?;
    match output_format {
        CliOutputFormat::Text => println!("{}", outcome.message),
        CliOutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "plugin",
                "action": action.unwrap_or("list"),
                "target": target,
                "message": outcome.message,
                "reload_runtime": outcome.reload_runtime,
            }))?
        ),
    }
    Ok(())
}
