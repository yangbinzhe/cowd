use harness_eval::{
    default_report_root, run_eval, terminal_gate_report_with_report, HarnessEvalLevel,
    HarnessEvalReportStore, HarnessEvalRunnerOptions,
};
use std::path::PathBuf;

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
        "Usage:\n  harness-eval quick [--budget low]\n  harness-eval full [--budget full]\n  harness-eval deep-real --provider <model> --budget full --allow-real-model\n  harness-eval review-report --run-dir <dir> [--provider <model>] [--allow-real-model]\n  harness-eval terminal-gate [--evidence-dir <dir>] [--report-json <path>]"
    );
}

fn option_value(args: &[String], key: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == key)
        .map(|pair| pair[1].clone())
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
