use harness_eval::{
    default_report_root, run_auto_strategy_paired, run_certification_manifest, run_eval,
    run_paired_performance, run_provider_cache_calibration, terminal_gate_report_with_report,
    write_auto_strategy_report, AutoStrategyPairedOptions, HarnessEvalLevel,
    HarnessEvalReportStore, HarnessEvalRunnerOptions, PairedPerformanceOptions,
    ProviderCacheCalibrationOptions,
};
use std::{path::PathBuf, time::Duration};

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("harness-eval: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if matches!(
        args.first().map(String::as_str),
        Some("--help") | Some("-h")
    ) {
        print_help();
        return Ok(());
    }
    match args.first().map(String::as_str) {
        Some("provider-cache-calibration") => {
            let output = PathBuf::from(required_option(&args[1..], "--output")?);
            let report = run_provider_cache_calibration(ProviderCacheCalibrationOptions {
                model: required_option(&args[1..], "--provider")?,
                stable_context: PathBuf::from(required_option(&args[1..], "--stable-context")?),
                output: output.clone(),
                allow_real_model: args.iter().any(|value| value == "--allow-real-model"),
            })?;
            println!("provider-cache-calibration-report: {}", output.display());
            println!(
                "warm-provider-cache-ratio-bp: {}",
                report["summary"]["warm_provider_cache_ratio_bp"]
            );
            return Ok(());
        }
        Some("auto-strategy-paired") => {
            let output = PathBuf::from(required_option(&args[1..], "--output")?);
            let repetitions = option_value(&args[1..], "--repetitions")
                .as_deref()
                .unwrap_or("3")
                .parse::<usize>()
                .map_err(|_| "--repetitions must be an integer".to_string())?;
            let timeout_secs = option_value(&args[1..], "--timeout-secs")
                .as_deref()
                .unwrap_or("900")
                .parse::<u64>()
                .map_err(|_| "--timeout-secs must be an integer".to_string())?;
            let poll_interval_ms = option_value(&args[1..], "--poll-interval-ms")
                .as_deref()
                // TTFT has a dedicated SSE observer. Polling the growing
                // execution projection at 20Hz only repeats serialization and
                // local transfer; 2Hz keeps terminal detection responsive
                // without making the evaluator the gateway's CPU bottleneck.
                .unwrap_or("500")
                .parse::<u64>()
                .map_err(|_| "--poll-interval-ms must be an integer".to_string())?;
            let report = run_auto_strategy_paired(AutoStrategyPairedOptions {
                direct_url: option_value(&args[1..], "--direct-url")
                    .unwrap_or_else(|| "http://127.0.0.1:18652".to_string()),
                parallel_url: option_value(&args[1..], "--parallel-url")
                    .unwrap_or_else(|| "http://127.0.0.1:18653".to_string()),
                auto_url: option_value(&args[1..], "--auto-url")
                    .unwrap_or_else(|| "http://127.0.0.1:18654".to_string()),
                provider: required_option(&args[1..], "--provider")?,
                judge_model: required_option(&args[1..], "--judge-model")?,
                output: output.clone(),
                corpus: PathBuf::from(option_value(&args[1..], "--corpus").unwrap_or_else(|| {
                    "crates/harness-eval/corpora/auto-strategy-v1.json".to_string()
                })),
                rubric: PathBuf::from(option_value(&args[1..], "--rubric").unwrap_or_else(|| {
                    "crates/harness-eval/rubrics/auto-strategy-rubric-v1.json".to_string()
                })),
                repetitions,
                timeout: Duration::from_secs(timeout_secs),
                poll_interval: Duration::from_millis(poll_interval_ms.max(1)),
                token: std::env::var("COWD_API_TOKEN").ok(),
                allow_real_model: args.iter().any(|value| value == "--allow-real-model"),
            })?;
            write_auto_strategy_report(&output, &report)?;
            println!("auto-strategy-paired-report: {}", output.display());
            let diagnostic = std::env::var("COWD_AUTO_STRATEGY_DIAGNOSTIC_TASK_ID")
                .is_ok_and(|value| !value.trim().is_empty());
            let accepted = if diagnostic {
                report["status"] == "diagnostic_passed"
                    && report["gate"]["diagnostic_passed"] == true
                    && report["gate"]["claim_allowed"] == false
            } else {
                report["status"] == "passed"
            };
            if !accepted {
                return Err(format!(
                    "auto strategy proof gate is {}; see {}",
                    report["status"].as_str().unwrap_or("failed"),
                    output.display()
                ));
            }
            return Ok(());
        }
        Some("paired-performance") => {
            let baseline_url = required_option(&args[1..], "--baseline-url")?;
            let candidate_url = required_option(&args[1..], "--candidate-url")?;
            let model = required_option(&args[1..], "--provider")?;
            let output = required_option(&args[1..], "--output")?;
            let pairs = option_value(&args[1..], "--pairs")
                .as_deref()
                .unwrap_or("20")
                .parse::<usize>()
                .map_err(|_| "--pairs must be a positive integer".to_string())?;
            let min_pairs = option_value(&args[1..], "--min-pairs")
                .as_deref()
                .unwrap_or("5")
                .parse::<usize>()
                .map_err(|_| "--min-pairs must be a positive integer".to_string())?;
            let target_relative_ci_half_width_bp =
                option_value(&args[1..], "--target-relative-ci-half-width-bp")
                    .as_deref()
                    .unwrap_or("500")
                    .parse::<u64>()
                    .map_err(|_| {
                        "--target-relative-ci-half-width-bp must be a positive integer".to_string()
                    })?;
            let timeout_secs = option_value(&args[1..], "--timeout-secs")
                .as_deref()
                .unwrap_or("600")
                .parse::<u64>()
                .map_err(|_| "--timeout-secs must be an integer".to_string())?;
            let poll_interval_ms = option_value(&args[1..], "--poll-interval-ms")
                .as_deref()
                .unwrap_or("20")
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| "--poll-interval-ms must be a positive integer".to_string())?;
            let token = std::env::var("COWD_API_TOKEN").ok();
            let report = run_paired_performance(PairedPerformanceOptions {
                baseline_url,
                candidate_url,
                model,
                min_pairs,
                pairs,
                target_relative_ci_half_width_bp,
                token,
                timeout: Duration::from_secs(timeout_secs),
                // Public message polling is part of this end-to-end measurement.
                // A 100 ms interval quantized sub-100 ms Runtime differences into
                // an entire extra poll and produced false performance regressions.
                poll_interval: Duration::from_millis(poll_interval_ms),
            })
            .map_err(|error| format!("paired performance evaluation failed: {error}"))?;
            let output = PathBuf::from(output);
            if let Some(parent) = output.parent() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    format!("cannot create paired performance report directory: {error}")
                })?;
            }
            let report_json = serde_json::to_vec_pretty(&report)
                .map_err(|error| format!("serialize paired performance report: {error}"))?;
            std::fs::write(&output, report_json)
                .map_err(|error| format!("cannot write paired performance report: {error}"))?;
            println!("paired-performance-report: {}", output.display());
            if report["status"].as_str() != Some("passed") {
                eprintln!(
                    "paired performance release gate failed; see {}",
                    output.display()
                );
                return Err(format!(
                    "paired performance release gate failed; see {}",
                    output.display()
                ));
            }
            return Ok(());
        }
        Some("review-report") => {
            let run_dir = option_value(&args[1..], "--run-dir")
                .or_else(|| option_value(&args[1..], "--report-dir"))
                .ok_or_else(|| "review-report requires --run-dir <path>".to_string())?;
            let options = HarnessEvalRunnerOptions {
                level: HarnessEvalLevel::Deep,
                provider: option_value(&args[1..], "--provider"),
                budget: option_value(&args[1..], "--budget").or_else(|| Some("review".to_string())),
                allow_real_model: args.iter().any(|value| value == "--allow-real-model"),
            };
            let output = HarnessEvalReportStore::review_report_dir(run_dir, options)
                .map_err(|error| format!("failed to review harness eval report: {error}"))?;
            println!("full-analysis-report: {}", output.display());
            return Ok(());
        }
        Some("terminal-gate") => {
            let evidence_dir = option_value(&args[1..], "--evidence-dir")
                .unwrap_or_else(|| "../plan/0706-AIHarness终局100闭环升级/90-审计证据".to_string());
            let report_json = option_value(&args[1..], "--report-json").map(PathBuf::from);
            let gate = terminal_gate_report_with_report(PathBuf::from(evidence_dir), report_json);
            let gate_json = serde_json::to_string_pretty(&gate)
                .map_err(|error| format!("serialize terminal gate report: {error}"))?;
            println!("{gate_json}");
            if gate["status"].as_str() != Some("passed") {
                return Err(format!(
                    "terminal gate failed; see {}",
                    gate["report_path"]
                        .as_str()
                        .unwrap_or("<report unavailable>")
                ));
            }
            return Ok(());
        }
        Some("certify") => {
            let manifest = required_option(&args[1..], "--manifest")?;
            let output = required_option(&args[1..], "--output")?;
            let report = run_certification_manifest(&manifest, &output)
                .map_err(|error| format!("certification failed: {error}"))?;
            println!(
                "certification-report: {}",
                PathBuf::from(&output)
                    .join("certification-report.json")
                    .display()
            );
            if report.status != "passed" {
                return Err(format!(
                    "certification gate failed with {} required source failures and {} required check failures",
                    report.required_source_failures, report.required_check_failures
                ));
            }
            return Ok(());
        }
        _ => {}
    }

    let level = match args.first().map(String::as_str) {
        Some(value) if value.starts_with('-') => HarnessEvalLevel::Quick,
        Some(value) => HarnessEvalLevel::from_str(value)
            .ok_or_else(|| format!("unknown harness eval level: {value}"))?,
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
    let record = run_eval(&store, options)
        .map_err(|error| format!("failed to run harness eval: {error}"))?;

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
    if record.status == "failed" {
        return Err(format!(
            "harness evaluation failed; see {}",
            record
                .report_path
                .as_deref()
                .unwrap_or("<report unavailable>")
        ));
    }
    Ok(())
}

fn print_help() {
    println!(
        "Usage:\n  harness-eval quick [--budget low]\n  harness-eval full [--budget full]\n  harness-eval deep-real --provider <model> --budget full --allow-real-model\n  harness-eval provider-cache-calibration --provider <deepseek-model> --stable-context <path> --output <path> --allow-real-model\n  harness-eval auto-strategy-paired --provider <model> --judge-model <model> --output <path> --allow-real-model [--direct-url http://127.0.0.1:18652] [--parallel-url http://127.0.0.1:18653] [--auto-url http://127.0.0.1:18654] [--repetitions 3] (set COWD_AUTO_STRATEGY_DIAGNOSTIC_TASK_ID for a non-claiming frozen-task diagnostic)\n  harness-eval paired-performance --baseline-url <url> --candidate-url <url> --provider <model> --output <path> [--min-pairs 5] [--pairs 20] [--target-relative-ci-half-width-bp 500] [--timeout-secs 600] [--poll-interval-ms 20]\n  harness-eval review-report --run-dir <dir> [--provider <model>] [--allow-real-model]\n  harness-eval terminal-gate [--evidence-dir <dir>] [--report-json <path>]\n  harness-eval certify --manifest <path> --output <dir>"
    );
}

fn option_value(args: &[String], key: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == key)
        .map(|pair| pair[1].clone())
}

fn required_option(args: &[String], key: &str) -> Result<String, String> {
    option_value(args, key).ok_or_else(|| format!("command requires {key} <value>"))
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
