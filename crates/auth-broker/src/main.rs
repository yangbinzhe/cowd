use std::io::{self, BufRead};
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut root = None;
    let mut socket = None;
    let mut credential_stdin = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => root = args.next().map(PathBuf::from),
            "--socket" => socket = args.next().map(PathBuf::from),
            "--credential-stdin" => credential_stdin = true,
            "--help" | "-h" => {
                eprintln!(
                    "usage: cowd-auth-broker --root <dir> --socket <path> --credential-stdin"
                );
                return ExitCode::SUCCESS;
            }
            _ => {
                eprintln!("unsupported argument: {arg}");
                return ExitCode::from(64);
            }
        }
    }
    let (Some(root), Some(socket)) = (root, socket) else {
        eprintln!("--root and --socket are required");
        return ExitCode::from(64);
    };
    if !credential_stdin {
        eprintln!("--credential-stdin is required");
        return ExitCode::from(64);
    }
    let mut credential = String::new();
    if io::stdin().lock().read_line(&mut credential).is_err() || credential.trim().is_empty() {
        eprintln!("a non-empty enrollment credential is required on stdin");
        return ExitCode::from(64);
    }
    match auth_broker::serve_local(root, credential.trim(), socket) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("auth broker failed: {error}");
            ExitCode::from(1)
        }
    }
}
