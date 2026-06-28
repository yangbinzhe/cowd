use harness_eval::{
    default_report_root, run_eval, HarnessEvalLevel, HarnessEvalReportStore,
    HarnessEvalRunnerOptions,
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
        "Usage: harness-eval [quick|full|deep] [--provider configured] [--budget low] [--allow-real-model]"
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
