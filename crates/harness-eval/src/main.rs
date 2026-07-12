use harness_eval::{
    default_report_root, run_eval, run_paired_performance, terminal_gate_report_with_report,
    HarnessEvalLevel, HarnessEvalReportStore, HarnessEvalRunnerOptions, PairedPerformanceOptions,
};
use std::{path::PathBuf, time::Duration};

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if matches!(
        args.first().map(String::as_str),
        Some("--help") | Some("-h")
    ) {
        print_help();
        return;
    }
    match args.first().map(String::as_str) {
        Some("paired-performance") => {
            let baseline_url = required_option(&args[1..], "--baseline-url");
            let candidate_url = required_option(&args[1..], "--candidate-url");
            let model = required_option(&args[1..], "--provider");
            let output = required_option(&args[1..], "--output");
            let pairs = option_value(&args[1..], "--pairs")
                .as_deref()
                .unwrap_or("5")
                .parse::<usize>()
                .unwrap_or_else(|_| {
                    eprintln!("--pairs must be a positive integer");
                    std::process::exit(2);
                });
            let timeout_secs = option_value(&args[1..], "--timeout-secs")
                .as_deref()
                .unwrap_or("600")
                .parse::<u64>()
                .unwrap_or_else(|_| {
                    eprintln!("--timeout-secs must be an integer");
                    std::process::exit(2);
                });
            let poll_interval_ms = option_value(&args[1..], "--poll-interval-ms")
                .as_deref()
                .unwrap_or("20")
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .unwrap_or_else(|| {
                    eprintln!("--poll-interval-ms must be a positive integer");
                    std::process::exit(2);
                });
            let token = std::env::var("COWD_API_TOKEN").ok();
            let report = run_paired_performance(PairedPerformanceOptions {
                baseline_url,
                candidate_url,
                model,
                pairs,
                token,
                timeout: Duration::from_secs(timeout_secs),
                // Public message polling is part of this end-to-end measurement.
                // A 100 ms interval quantized sub-100 ms Runtime differences into
                // an entire extra poll and produced false performance regressions.
                poll_interval: Duration::from_millis(poll_interval_ms),
            })
            .unwrap_or_else(|error| {
                eprintln!("paired performance evaluation failed: {error}");
                std::process::exit(1);
            });
            let output = PathBuf::from(output);
            if let Some(parent) = output.parent() {
                std::fs::create_dir_all(parent).unwrap_or_else(|error| {
                    eprintln!("cannot create paired performance report directory: {error}");
                    std::process::exit(1);
                });
            }
            std::fs::write(
                &output,
                serde_json::to_vec_pretty(&report).expect("paired performance json"),
            )
            .unwrap_or_else(|error| {
                eprintln!("cannot write paired performance report: {error}");
                std::process::exit(1);
            });
            println!("paired-performance-report: {}", output.display());
            if report["status"].as_str() != Some("passed") {
                eprintln!(
                    "paired performance release gate failed; see {}",
                    output.display()
                );
                std::process::exit(1);
            }
            return;
        }
        Some("review-report") => {
            let run_dir = option_value(&args[1..], "--run-dir")
                .or_else(|| option_value(&args[1..], "--report-dir"))
                .unwrap_or_else(|| {
                    eprintln!("review-report requires --run-dir <path>");
                    std::process::exit(2);
                });
            let options = HarnessEvalRunnerOptions {
                level: HarnessEvalLevel::Deep,
                provider: option_value(&args[1..], "--provider"),
                budget: option_value(&args[1..], "--budget").or_else(|| Some("review".to_string())),
                allow_real_model: args.iter().any(|value| value == "--allow-real-model"),
            };
            let output = HarnessEvalReportStore::review_report_dir(run_dir, options)
                .unwrap_or_else(|error| {
                    eprintln!("failed to review harness eval report: {error}");
                    std::process::exit(1);
                });
            println!("full-analysis-report: {}", output.display());
            return;
        }
        Some("terminal-gate") => {
            let evidence_dir = option_value(&args[1..], "--evidence-dir")
                .unwrap_or_else(|| "../plan/0706-AIHarness终局100闭环升级/90-审计证据".to_string());
            let report_json = option_value(&args[1..], "--report-json").map(PathBuf::from);
            let gate = terminal_gate_report_with_report(PathBuf::from(evidence_dir), report_json);
            println!(
                "{}",
                serde_json::to_string_pretty(&gate).expect("terminal gate json")
            );
            return;
        }
        _ => {}
    }

    let level = match args.first().map(String::as_str) {
        Some(value) if value.starts_with('-') => HarnessEvalLevel::Quick,
        Some(value) => HarnessEvalLevel::from_str(value).unwrap_or_else(|| {
            eprintln!("unknown harness eval level: {value}");
            print_help();
            std::process::exit(2);
        }),
        None => HarnessEvalLevel::Quick,
    };

    let option_args = if args
        .first()
        .is_some_and(|value| !value.starts_with('-') && HarnessEvalLevel::from_str(value).is_some())
    {
        &args[1..]
    } else {
        &args[..]
    };

    let options = HarnessEvalRunnerOptions {
        level,
        provider: option_value(option_args, "--provider"),
        budget: option_value(option_args, "--budget").or_else(|| Some("low".to_string())),
        allow_real_model: option_args
            .iter()
            .any(|value| value == "--allow-real-model"),
    };

    let store = HarnessEvalReportStore::new(default_report_root(default_config_home()));
    let record = run_eval(&store, options).unwrap_or_else(|error| {
        eprintln!("failed to run harness eval: {error}");
        std::process::exit(1);
    });

    println!("mission harness {} eval: {}", record.level, record.status);
    println!("run: {}", record.run_id);
    println!("message: {}", record.message);
    if let Some(path) = &record.report_path {
        println!("json: {path}");
    }
    if let Some(report_id) = &record.report_id {
        if let Ok(Some(detail)) = store.get_report(report_id) {
            if let Some(markdown_path) = detail.summary.markdown_path {
                println!("markdown: {markdown_path}");
            }
        }
    }
}

fn print_help() {
    println!(
        "Usage:\n  harness-eval quick [--budget low]\n  harness-eval full [--budget full]\n  harness-eval deep-real --provider <model> --budget full --allow-real-model\n  harness-eval paired-performance --baseline-url <url> --candidate-url <url> --provider <model> --output <path> [--pairs 5] [--timeout-secs 600] [--poll-interval-ms 20]\n  harness-eval review-report --run-dir <dir> [--provider <model>] [--allow-real-model]\n  harness-eval terminal-gate [--evidence-dir <dir>] [--report-json <path>]"
    );
}

fn option_value(args: &[String], key: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == key)
        .map(|pair| pair[1].clone())
}

fn required_option(args: &[String], key: &str) -> String {
    option_value(args, key).unwrap_or_else(|| {
        eprintln!("paired-performance requires {key} <value>");
        std::process::exit(2);
    })
}

fn default_config_home() -> PathBuf {
    std::env::var_os("COWD_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .map(|root| root.join("cowd"))
        })
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".config").join("cowd"))
        })
        .unwrap_or_else(|| PathBuf::from(".cowd"))
}
