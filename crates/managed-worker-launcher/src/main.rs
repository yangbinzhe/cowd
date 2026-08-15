fn main() {
    if let Err(error) = managed_worker_launcher::run_from_args(std::env::args_os().skip(1)) {
        eprintln!("managed-worker-launcher: {error}");
        std::process::exit(125);
    }
}
