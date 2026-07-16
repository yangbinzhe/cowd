mod api;
mod llm;
mod reporter;
mod scenarios;
mod server;
mod tui;

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    // Parse flags
    let list_only = args.iter().any(|a| a == "--list");
    let run_all = args.iter().any(|a| a == "--all" || a == "-a");

    // Find scenario filter: --scenarios comma,list or positional
    let scenario_filter: Option<Vec<String>> =
        if let Some(pos) = args.iter().position(|a| a == "--scenarios" || a == "-s") {
            args.get(pos + 1)
                .map(|combo| combo.split(',').map(|s| s.to_string()).collect())
        } else if args.len() > 1 && !args[1].starts_with("--") {
            Some(vec![args[1].clone()])
        } else if run_all {
            Some(vec!["all".to_string()])
        } else {
            None
        };

    if list_only {
        println!("\nAvailable scenarios:");
        scenarios::list();
        return;
    }

    let filter_name = scenario_filter.as_ref().map(|v| v.join(","));
    println!("═══ Cowd Interactive Tests ═══");
    if let Some(ref f) = filter_name {
        println!("Filter: {}", f);
    } else {
        println!("Usage: cargo run -- [scenario_name]");
        println!("       cargo run -- --list");
        println!("       cargo run -- --all");
        println!("       cargo run -- --scenarios tui_basic,cross_cut");
        println!("\nAvailable scenarios:");
        scenarios::list();
        return;
    }

    let mut runner = reporter::TestRunner::new();
    if let Err(error) = scenarios::run_all(&mut runner, filter_name) {
        eprintln!("Error: {error}");
        runner.record_failure("interactive-suite", error.to_string());
    }
    runner.report();

    // LLM-powered analysis (optional — requires ANTHROPIC_API_KEY or local Ollama)
    let results_json = serde_json::to_string_pretty(&runner.results).unwrap_or_default();
    let analysis = llm::analyze_results(&results_json);
    if !analysis.is_empty() {
        println!("{}", analysis);
    }
    if runner.executed_count() == 0 {
        eprintln!("No interactive scenario assertions were executed");
        std::process::exit(1);
    }
    if runner.has_failures() {
        std::process::exit(1);
    }
}
