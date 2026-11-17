use harness_eval::{
    default_report_root, project_outer_cleanup_report, run_auto_strategy_paired,
    run_certification_manifest, run_eval_cli, run_paired_performance, run_provider_cache_calibration,
    terminal_gate_report_with_report, write_auto_strategy_report, AutoStrategyPairedOptions,
    CliEvalDisposition, HarnessEvalLevel, HarnessEvalReportStore, HarnessEvalRunnerOptions,
    OuterCleanupReceipts, PairedPerformanceOptions, ProviderCacheCalibrationOptions,
};
use std::{path::PathBuf, time::Duration};

fn main() -> std::process::ExitCode {
    exit_for(run())
}

fn exit_for(result: Result<CliEvalDisposition, String>) -> std::process::ExitCode {
    match result {
        Ok(CliEvalDisposition::Finished) => std::process::ExitCode::SUCCESS,
        Ok(CliEvalDisposition::PendingOuterCleanup) => std::process::ExitCode::from(75),
        Err(error) => {
            eprintln!("harness-eval: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<CliEvalDisposition, String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if matches!(
        args.first().map(String::as_str),
        Some("--help") | Some("-h")
    ) {
        print_help();
        return Ok(CliEvalDisposition::Finished);
    }
    match args.first().map(String::as_str) {
        Some("canonical-gate") => {
            // The canonical read of a persisted report: it re-verifies an
            // outer cleanup projection from the receipts the report names, so
            // the same document always reads the same way.
            let report_path = PathBuf::from(required_option(&args[1..], "--report")?);
            let bytes = std::fs::read(&report_path)
                .map_err(|error| format!("report {} unreadable: {error}", report_path.display()))?;
            let report: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|error| format!("report is not JSON: {error}"))?;
            let gate = harness_eval::evaluate_report_gate(&report);
            println!("canonical-gate-status: {}", gate.status);
            println!("canonical-gate-passed: {}", gate.passed);
            println!("canonical-gate-failed: {}", gate.failed);
            return Ok(CliEvalDisposition::Finished);
        }
        Some("project-outer-cleanup") => {
            // The outer cleanup happens after the harness exits, so it is
            // projected from the durable receipts instead of claimed by the
            // report. A missing, late or foreign receipt leaves the
            // observation unobserved and never produces a passing report.
            let output = PathBuf::from(required_option(&args[1..], "--output")?);
            let harness_report_path =
                PathBuf::from(required_option(&args[1..], "--harness-report")?);
            let inner_receipt_path =
                PathBuf::from(required_option(&args[1..], "--inner-receipt")?);
            let final_receipt_path = optional_option(&args[1..], "--final-receipt")
                .map(PathBuf::from);
            let (harness_bytes, harness_mtime) = read_receipt(&harness_report_path)?;
            let (inner_bytes, inner_mtime) = read_receipt(&inner_receipt_path)?;
            let (final_bytes, final_mtime) = match &final_receipt_path {
                Some(path) => match read_receipt(path) {
                    Ok(value) => value,
                    Err(_) => (Vec::new(), None),
                },
                None => (Vec::new(), None),
            };
            // An unreadable outer receipt is an absent fact, not a crash: the
            // projection reports it as unobserved and never as observed.
            let cleanup_result_path =
                optional_option(&args[1..], "--cleanup-result").map(PathBuf::from);
            let owner_report_path =
                optional_option(&args[1..], "--owner-report").map(PathBuf::from);

            let (cleanup, cleanup_digest, cleanup_mtime) =
                match optional_option(&args[1..], "--cleanup-result") {
                    Some(path) => match read_receipt(&PathBuf::from(path)) {
                        Ok((bytes, mtime)) => {
                            let digest = harness_eval::receipt_digest(&bytes);
                            (Some(parse_receipt(&bytes)?), Some(digest), mtime)
                        }
                        Err(_) => (None, None, None),
                    },
                    None => (None, None, None),
                };
            let shutdown_validation_path =
                optional_option(&args[1..], "--shutdown-validation").map(PathBuf::from);
            let shutdown_validation = match &shutdown_validation_path {
                Some(path) => match read_receipt(path) {
                    Ok((bytes, _)) => Some(parse_receipt(&bytes)?),
                    Err(_) => None,
                },
                None => None,
            };
            let (owner_report, owner_mtime) = match optional_option(&args[1..], "--owner-report")
            {
                Some(path) => match read_receipt(&PathBuf::from(path)) {
                    Ok((bytes, mtime)) => (Some(parse_receipt(&bytes)?), mtime),
                    Err(_) => (None, None),
                },
                None => (None, None),
            };
            let manifest_path = optional_option(&args[1..], "--manifest").map(PathBuf::from);
            let (manifest, manifest_mtime, manifest_digest) = match &manifest_path {
                Some(path) => match read_receipt(path) {
                    Ok((bytes, mtime)) => (
                        Some(parse_receipt(&bytes)?),
                        mtime,
                        Some(harness_eval::receipt_digest(&bytes)),
                    ),
                    Err(_) => (None, None, None),
                },
                None => (None, None, None),
            };
            let sealed_key_path = optional_option(&args[1..], "--sealed-key").or_else(|| {
                // The owner report names the sealed evidence key; an explicit
                // path only overrides it.
                owner_report
                    .as_ref()
                    .and_then(|owner| owner.pointer("/encrypted_evidence/key_reference"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            });
            let shutdown_receipt_sha256 = shutdown_validation
                .as_ref()
                .and_then(|value| value.get("receipt_sha256"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            let owner_report_digest = match &owner_report_path {
                Some(path) => match read_receipt(path) {
                    Ok((bytes, _)) => Some(harness_eval::receipt_digest(&bytes)),
                    Err(_) => None,
                },
                None => None,
            };
            let shutdown_validation_digest = match &shutdown_validation_path {
                Some(path) => match read_receipt(path) {
                    Ok((bytes, _)) => Some(harness_eval::receipt_digest(&bytes)),
                    Err(_) => None,
                },
                None => None,
            };
            let receipts = OuterCleanupReceipts {
                harness_report: parse_receipt(&harness_bytes)?,
                harness_report_sha256: harness_eval::receipt_digest(&harness_bytes),
                inner_receipt: parse_receipt(&inner_bytes)?,
                inner_receipt_sha256: harness_eval::receipt_digest(&inner_bytes),
                final_receipt: if final_bytes.is_empty() {
                    serde_json::Value::Null
                } else {
                    parse_receipt(&final_bytes)?
                },
                final_receipt_sha256: harness_eval::receipt_digest(&final_bytes),
                cleanup_result: cleanup,
                cleanup_result_sha256: cleanup_digest,
                shutdown_validation,
                shutdown_validation_sha256: shutdown_validation_digest,
                shutdown_receipt_sha256,
                owner_report,
                owner_report_sha256: owner_report_digest,
                manifest,
                manifest_sha256: manifest_digest,
                sealed_key_path: sealed_key_path.clone(),
                sources: harness_eval::ReceiptSources {
                    harness_report: harness_report_path.display().to_string(),
                    inner_receipt: inner_receipt_path.display().to_string(),
                    final_receipt: final_receipt_path
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_default(),
                    cleanup_result: cleanup_result_path.map(|path| path.display().to_string()),
                    shutdown_validation: shutdown_validation_path
                        .map(|path| path.display().to_string()),
                    owner_report: owner_report_path
                        .as_ref()
                        .map(|path| path.display().to_string()),
                    manifest: manifest_path.map(|path| path.display().to_string()),
                    sealed_key: sealed_key_path.clone(),
                },
                harness_report_mtime_ms: harness_mtime,
                final_receipt_mtime_ms: final_mtime,
                owner_report_mtime_ms: owner_mtime,
            };
            let projection =
                project_outer_cleanup_report(&receipts.harness_report, &receipts);
            let scenario_output = optional_option(&args[1..], "--scenario-output")
                .map(PathBuf::from);
            let scenario_artifact = optional_option(&args[1..], "--scenario-artifact")
                .map(PathBuf::from);
            let mut projected_report = projection.report.clone();
            harness_eval::apply_gate_verdict(
                &mut projected_report,
                projection.gate.status == "passed",
            );
            let body = serde_json::to_string_pretty(&projected_report)
                .map_err(|error| error.to_string())?;
            if let Some(parent) = output.parent() {
                std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
            }
            std::fs::write(&output, body).map_err(|error| error.to_string())?;
            match &projection.verdict {
                Ok(facts) => {
                    println!("outer-cleanup-projection: observed");
                    println!(
                        "outer-cleanup-projection-detail: run_id={} scenario={} remaining_owned={}",
                        facts.run_id, facts.scenario, facts.remaining_owned
                    );
                    println!("outer-cleanup-gate: {}", projection.gate.status);
                    if let (Some(artifact), Some(output)) = (&scenario_artifact, &scenario_output) {
                        let mut artifact = parse_receipt(&read_receipt(artifact)?.0)?;
                        harness_eval::project_scenario_artifact(&mut artifact, facts);
                        harness_eval::apply_gate_verdict_to_scenario(
                            &mut artifact,
                            projection.gate.status == "passed",
                        );
                        let body = serde_json::to_string_pretty(&artifact)
                            .map_err(|error| error.to_string())?;
                        if let Some(parent) = output.parent() {
                            std::fs::create_dir_all(parent)
                                .map_err(|error| error.to_string())?;
                        }
                        std::fs::write(output, body).map_err(|error| error.to_string())?;
                        println!("outer-cleanup-scenario: {}", output.display());
                    }
                }
                Err(gap) => {
                    println!("outer-cleanup-projection: {}", gap.reason());
                    println!("outer-cleanup-detail: {}", gap.detail());
                    println!("outer-cleanup-gate: {}", projection.gate.status);
                    std::process::exit(3);
                }
            }
            if projection.gate.status != "passed" {
                // The outer cleanup was observed; the remaining verdict is the
                // business one and it is not this projection's to decide.
                println!("outer-cleanup-gate-not-passed: {}", projection.gate.status);
                std::process::exit(4);
            }
            return Ok(CliEvalDisposition::Finished);
        }
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
            return Ok(CliEvalDisposition::Finished);
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
            return Ok(CliEvalDisposition::Finished);
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
                allow_real_model: args[1..].iter().any(|arg| arg == "--allow-real-model"),
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
            return Ok(CliEvalDisposition::Finished);
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
            return Ok(CliEvalDisposition::Finished);
        }
        Some("terminal-gate") => {
            let evidence_dir = option_value(&args[1..], "--evidence-dir").unwrap_or_else(|| {
                "../plan/2026-07/0706-AIHarness终局100闭环升级/90-审计证据".to_string()
            });
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
            return Ok(CliEvalDisposition::Finished);
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
                    "certification gate {} with {} required source failures and {} required check failures",
                    report.status, report.required_source_failures, report.required_check_failures
                ));
            }
            return Ok(CliEvalDisposition::Finished);
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
    let (record, disposition) = run_eval_cli(&store, options)
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
    Ok(disposition)
}

fn print_help() {
    println!(
        "Usage:\n  harness-eval quick [--budget low]\n  harness-eval full [--budget full]\n  harness-eval deep-real --provider <model> --budget full --allow-real-model\n  harness-eval provider-cache-calibration --provider <deepseek-model> --stable-context <path> --output <path> --allow-real-model\n  harness-eval auto-strategy-paired --provider <model> --judge-model <model> --output <path> --allow-real-model [--direct-url http://127.0.0.1:18652] [--parallel-url http://127.0.0.1:18653] [--auto-url http://127.0.0.1:18654] [--repetitions 3] (set COWD_AUTO_STRATEGY_DIAGNOSTIC_TASK_ID for a non-claiming frozen-task diagnostic)\n  harness-eval paired-performance --baseline-url <url> --candidate-url <url> --provider <model> --output <path> --allow-real-model [--min-pairs 5] [--pairs 20] [--target-relative-ci-half-width-bp 500] [--timeout-secs 600] [--poll-interval-ms 20]\n  harness-eval review-report --run-dir <dir> [--provider <model>] [--allow-real-model]\n  harness-eval terminal-gate [--evidence-dir <dir>] [--report-json <path>]\n  harness-eval certify --manifest <path> --output <dir>"
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

#[cfg(test)]
mod cli_disposition_tests {
    use super::*;
    #[test]
    fn only_typed_pending_returns_reserved75() {
        assert_eq!(
            exit_for(Ok(CliEvalDisposition::PendingOuterCleanup)),
            std::process::ExitCode::from(75)
        );
        assert_eq!(
            exit_for(Ok(CliEvalDisposition::Finished)),
            std::process::ExitCode::SUCCESS
        );
        for text in ["pending_outer_cleanup", "75", "failed", "cancelled"] {
            assert_eq!(exit_for(Err(text.into())), std::process::ExitCode::FAILURE);
        }
    }
}

/// Read a receipt file with its modification time; a missing receipt is an
/// error the caller turns into an unobserved projection, never a default.
fn read_receipt(path: &std::path::Path) -> Result<(Vec<u8>, Option<u64>), String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("receipt {} unreadable: {error}", path.display()))?;
    let mtime = std::fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|value| value.as_millis() as u64);
    Ok((bytes, mtime))
}

fn parse_receipt(bytes: &[u8]) -> Result<serde_json::Value, String> {
    serde_json::from_slice(bytes).map_err(|error| format!("receipt is not JSON: {error}"))
}

fn optional_option(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|value| value == name)
        .and_then(|index| args.get(index + 1))
        .cloned()
}
