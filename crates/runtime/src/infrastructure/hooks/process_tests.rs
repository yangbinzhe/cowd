use super::*;
use crate::hooks::{CommandExecution, HookAbortSignal};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

fn command(script: &str) -> Command {
    let mut command = Command::new("sh");
    command
        .args(["-c", script])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn await_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !path.exists() {
        assert!(Instant::now() < deadline, "original child did not enter");
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn unread_stdin_observes_both_original_cancellation_owners() {
    for owner_shutdown in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let entered = root.path().join("entered");
        let scope = HookExecutionScope::default();
        let run_scope = scope.clone();
        let token = crate::CancellationToken::new();
        let signal = if owner_shutdown {
            HookAbortSignal::new().with_owner_cancellation(token.clone())
        } else {
            HookAbortSignal::new().with_cancellation(token.clone())
        };
        let script = format!("printf entered > '{}'; exec sleep 60", entered.display());
        let run = std::thread::spawn(move || {
            run_scope.execute(
                &mut command(&script),
                &vec![b'x'; 2 * 1024 * 1024],
                Some(&signal),
            )
        });
        await_file(&entered);
        assert!(!scope.is_closed());
        token.cancel();
        assert!(matches!(
            run.join().unwrap().unwrap(),
            CommandExecution::Cancelled
        ));
        assert!(
            scope.is_closed(),
            "cancelled requires real leader/group/EOF closure"
        );
    }
}

#[test]
fn pumps_large_input_and_both_outputs_without_serial_pipe_deadlock() {
    let scope = HookExecutionScope::default();
    let input = vec![b'x'; 256 * 1024];
    let output = scope
        .execute(
            &mut command("head -c 98304 /dev/zero; head -c 81920 /dev/zero >&2; cat"),
            &input,
            None,
        )
        .unwrap();
    let CommandExecution::Finished(output) = output else {
        panic!("positive command cancelled")
    };
    assert!(output.status.success());
    let mut expected = vec![0; 98304];
    expected.extend_from_slice(&input);
    assert_eq!(output.stdout, expected);
    assert_eq!(output.stderr, vec![0; 81920]);
    assert!(scope.is_closed());
}

#[test]
fn broken_pipe_keeps_actual_exit_status_and_output() {
    let scope = HookExecutionScope::default();
    for code in [0, 2, 17] {
        let result = scope
            .execute(
                &mut command(&format!(
                    "exec 0<&-; printf original; printf stderr >&2; exit {code}"
                )),
                &vec![b'x'; 256 * 1024],
                None,
            )
            .unwrap();
        let CommandExecution::Finished(output) = result else {
            panic!("not cancelled")
        };
        assert_eq!(output.status.code(), Some(code));
        assert_eq!(output.stdout, b"original");
        assert_eq!(output.stderr, b"stderr");
        assert!(scope.is_closed());
    }
}

#[test]
fn same_group_descendant_cannot_hold_output_after_leader_exit() {
    let scope = HookExecutionScope::default();
    let result = scope
        .execute(
            &mut command("sleep 60 & printf receipted; exit 0"),
            &[],
            None,
        )
        .unwrap();
    let CommandExecution::Finished(output) = result else {
        panic!("not cancelled")
    };
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stdout, b"receipted");
    assert!(scope.is_closed());
}

struct EscapedChild(PathBuf);
impl Drop for EscapedChild {
    fn drop(&mut self) {
        if let Ok(pid) = std::fs::read_to_string(&self.0) {
            if let Ok(pid) = pid.trim().parse() {
                let _ = tools::bash::terminate_owned_process_group(pid);
            }
        }
    }
}

#[tokio::test]
async fn unknown_pipe_retains_original_scope_and_refuses_next_spawn() {
    let root = tempfile::tempdir().unwrap();
    let pid = root.path().join("escaped-pid");
    let release = root.path().join("release");
    let sentinel = root.path().join("must-not-start");
    let escaped = EscapedChild(pid.clone());
    let scope = HookExecutionScope::default();
    // This child deliberately changes its group. Group quiet alone must not
    // conceal its inherited stdout/stderr. Its test owner releases it below.
    let script = format!(
        "setsid sh -c 'echo $$ > {}; while ! test -e {}; do sleep 0.02; done' & while ! test -s {}; do sleep 0.01; done; exit 0",
        pid.display(), release.display(), pid.display());
    let error = scope
        .execute(&mut command(&script), &[], None)
        .err()
        .expect("EOF unknown");
    assert!(error.to_string().contains("remains unknown"));
    assert!(!scope.is_closed());
    let rejected = scope.execute(
        &mut command(&format!("touch {}", sentinel.display())),
        &[],
        None,
    );
    assert!(rejected.is_err());
    assert!(!sentinel.exists());
    std::fs::write(&release, b"release").unwrap();
    assert!(
        scope.quiesce().await,
        "original owner observes late EOF without respawn"
    );
    assert!(scope.is_closed());
    drop(escaped);
}

#[tokio::test]
async fn pipe_setup_failure_still_kills_and_reaps_but_keeps_unknown_eof() {
    use std::os::unix::process::CommandExt;
    let scope = HookExecutionScope::default();
    let mut command = command("exec sleep 60");
    command.process_group(0);
    let child = command.spawn().unwrap();
    let pid = child.id();
    scope.0.lock().unwrap().child = Some(HookChild {
        child,
        pgid: pid,
        status: None,
        stdout: Vec::new(),
        stderr: Vec::new(),
        cleanup_started: None,
        fail_pipe_setup: true,
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(!scope.try_quiesce(), "setup failure cannot certify EOF");
        let reaped = scope
            .0
            .lock()
            .unwrap()
            .child
            .as_ref()
            .unwrap()
            .status
            .is_some();
        if reaped {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "pipe error bypassed real kill/reap"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(tools::bash::owned_process_group_is_quiet(pid).unwrap());
    assert!(!scope.is_closed());
    scope
        .0
        .lock()
        .unwrap()
        .child
        .as_mut()
        .unwrap()
        .fail_pipe_setup = false;
    assert!(scope.quiesce().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_hook_workers_do_not_starve_the_async_cancellation_owner() {
    let root = tempfile::tempdir().unwrap();
    let token = crate::CancellationToken::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let started = Instant::now();
    let mut runs = Vec::new();
    let entered = [root.path().join("entered-a"), root.path().join("entered-b")];
    for path in entered.iter().cloned() {
        let token = token.clone();
        let barrier = barrier.clone();
        runs.push(tokio::spawn(async move {
            let signal = HookAbortSignal::new().with_cancellation(token);
            let runner = crate::HookRunner::new(crate::RuntimeHookConfig::new(
                vec![format!(
                    "printf entered > '{}'; exec sleep 3",
                    path.display()
                )],
                vec![],
                vec![],
            ));
            barrier.wait().await;
            let result = runner.run_pre_tool_use_with_signal("read_file", "{}", Some(&signal));
            (result, runner.execution_scope())
        }));
    }
    barrier.wait().await;
    // This cancellation sender runs on Tokio, with no external OS thread.
    while !entered.iter().all(|path| path.exists()) {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "Hook occupied the cancellation executor"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(started.elapsed() < Duration::from_secs(2));
    token.cancel();
    for run in runs {
        let (result, scope) = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_cancelled());
        assert!(scope.is_closed());
    }
}

#[test]
fn already_cancelled_scope_does_not_spawn_a_fresh_hook() {
    let root = tempfile::tempdir().unwrap();
    let sentinel = root.path().join("must-not-start");
    let token = crate::CancellationToken::new();
    let signal = HookAbortSignal::new().with_cancellation(token.clone());
    token.cancel();
    let scope = HookExecutionScope::default();
    let result = scope
        .execute(
            &mut command(&format!("touch {}", sentinel.display())),
            &[],
            Some(&signal),
        )
        .unwrap();
    assert!(matches!(result, CommandExecution::Cancelled));
    assert!(!sentinel.exists());
    assert!(scope.is_closed());
}
