// The subreaper must be isolated from the multi-threaded libtest process.
#[derive(Debug, Clone, serde::Serialize)]
struct GateProcess {
    pid: i32,
    state: char,
    ppid: i32,
    pgrp: i32,
    starttime: u64,
}

struct GateCleanup {
    child: GateProcess,
    leader: GateProcess,
    reaped: bool,
}

impl Drop for GateCleanup {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        if let Ok(current) = gate_process(self.child.pid) {
            if current.starttime == self.child.starttime {
                if let Some(pid) = rustix::process::Pid::from_raw(current.pid) {
                    if let Ok(fd) =
                        rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())
                    {
                        if gate_process(current.pid)
                            .is_ok_and(|p| p.starttime == self.child.starttime)
                        {
                            let _ = rustix::process::pidfd_send_signal(
                                &fd,
                                rustix::process::Signal::KILL,
                            );
                        }
                    } else if current.ppid == std::process::id() as i32 {
                        // An unreaped direct child cannot have its PID reused.
                        let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
                    }
                }
            }
        }
        if gate_process(self.leader.pid)
            .is_ok_and(|p| p.starttime == self.leader.starttime && p.pgrp == p.pid)
        {
            if let Some(pid) = rustix::process::Pid::from_raw(self.leader.pid) {
                let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
            }
        }
        if let Some(pid) = rustix::process::Pid::from_raw(self.child.pid) {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while std::time::Instant::now() < deadline {
                match rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG) {
                    Ok(Some(_)) => break,
                    Ok(None) | Err(rustix::io::Errno::CHILD) => {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

fn gate_process(pid: i32) -> std::io::Result<GateProcess> {
    let raw = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let tail = raw
        .rsplit_once(") ")
        .ok_or_else(|| std::io::Error::other("invalid proc stat"))?
        .1;
    let fields: Vec<_> = tail.split_whitespace().collect();
    let parse = |index: usize| {
        fields
            .get(index)
            .ok_or_else(|| std::io::Error::other("short proc stat"))
    };
    Ok(GateProcess {
        pid,
        state: parse(0)?
            .chars()
            .next()
            .ok_or_else(|| std::io::Error::other("missing state"))?,
        ppid: parse(1)?
            .parse()
            .map_err(|_| std::io::Error::other("invalid ppid"))?,
        pgrp: parse(2)?
            .parse()
            .map_err(|_| std::io::Error::other("invalid pgrp"))?,
        starttime: parse(19)?
            .parse()
            .map_err(|_| std::io::Error::other("invalid starttime"))?,
    })
}

fn gate_socket_exists(path: &Path) -> std::io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn gate_group(pgrp: i32) -> std::io::Result<Vec<GateProcess>> {
    let mut members = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<i32>().ok())
        else {
            continue;
        };
        let process = gate_process(pid)?;
        if process.pgrp == pgrp {
            members.push(process);
        }
    }
    Ok(members)
}

async fn gate_group_bounded(pgrp: i32) -> Result<Vec<GateProcess>, String> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match gate_group(pgrp) {
            Ok(members) => return Ok(members),
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(format!("process group collection failed: {error}")),
        }
    }
}

fn gate_evidence(path: &Path, run: &str, mode: &str, phase: &str, details: serde_json::Value) {
    use std::io::Write as _;
    let value = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    let clock = (value.tv_sec as u64) * 1_000_000_000 + value.tv_nsec as u64;
    let mut value = serde_json::json!({
        "run_id":run,"sample_id":mode,"monotonic_ns":clock,"phase":phase,
        "worker_pid":null,"pgid":null,"descendant_pid":null,"descendant_starttime":null,
        "descendant_state":null,"descendant_ppid":null,"descendant_pgrp":null,
        "pidfd_open_result":null,"signal":null,"signal_result":null,"errno":null,
        "waitid_or_waitpid_result":null,"group_members":null,"socket_exists":null,
        "shutdown_result":null
    });
    if let Some(fields) = details.as_object() {
        value.as_object_mut().unwrap().extend(fields.clone());
    }
    let mut out = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("gate evidence open");
    writeln!(out, "{value}").expect("gate evidence write");
    out.sync_data().expect("gate evidence sync");
}

pub(super) fn gate_record_group_signal(
    pid: u32,
    signal: &str,
    result: &std::io::Result<ExitStatus>,
) {
    let (Some(path), Ok(run), Ok(mode)) = (
        std::env::var_os("COWD_WORKER_GATE_EVIDENCE"),
        std::env::var("COWD_WORKER_GATE_RUN"),
        std::env::var("COWD_WORKER_GATE_MODE"),
    ) else {
        return;
    };
    gate_evidence(
        &PathBuf::from(path),
        &run,
        &mode,
        "group_signal",
        serde_json::json!({
            "worker_pid":pid,"pgid":pid,"signal":signal,
            "signal_result":format!("{result:?}"),
            "errno":result.as_ref().err().and_then(std::io::Error::raw_os_error)
        }),
    );
}

fn gate_cleanup_timed_out_helper(evidence: &Path, mode: &str) -> Result<(), String> {
    let records = fs::read_to_string(evidence).map_err(|error| error.to_string())?;
    let records: Vec<serde_json::Value> = records
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()
        .map_err(|error| format!("invalid evidence JSON: {error}"))?;
    let handshake = records
        .iter()
        .find(|value| value["phase"] == "handshake" && value["sample_id"] == mode)
        .ok_or_else(|| "helper timed out before identity was recorded".to_string())?;
    for name in ["worker", "descendant"] {
        let (pid, start) = if name == "worker" {
            (
                handshake["worker_pid"].as_i64(),
                handshake["worker"]["starttime"].as_u64(),
            )
        } else {
            (
                handshake["descendant_pid"].as_i64(),
                handshake["descendant_starttime"].as_u64(),
            )
        };
        let pid = pid.ok_or_else(|| format!("{name} pid missing"))? as i32;
        let start = start.ok_or_else(|| format!("{name} starttime missing"))?;
        match gate_process(pid) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(format!("{name} identity unavailable: {error}")),
            Ok(process) if process.starttime != start => {
                return Err(format!("{name} identity reused"))
            }
            Ok(process) => {
                let pid =
                    rustix::process::Pid::from_raw(pid).ok_or_else(|| "invalid pid".to_string())?;
                let fd = rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())
                    .map_err(|error| format!("{name} pidfd unavailable: {error}"))?;
                let again = gate_process(process.pid).map_err(|error| error.to_string())?;
                if again.starttime != start {
                    return Err(format!("{name} identity changed"));
                }
                if name == "worker" && again.pgrp == again.pid {
                    rustix::process::kill_process_group(pid, rustix::process::Signal::KILL)
                        .map_err(|error| format!("worker group cleanup failed: {error}"))?;
                } else {
                    rustix::process::pidfd_send_signal(&fd, rustix::process::Signal::KILL)
                        .map_err(|error| format!("{name} cleanup failed: {error}"))?;
                }
            }
        }
    }
    Ok(())
}

#[ignore = "invoked explicitly in a separate process by the parent gate"]
#[tokio::test]
async fn shutdown_subreaper_gate_helper() {
    let Ok(mode) = std::env::var("COWD_WORKER_GATE_MODE") else {
        return;
    };
    let evidence =
        PathBuf::from(std::env::var_os("COWD_WORKER_GATE_EVIDENCE").expect("evidence path"));
    let run = std::env::var("COWD_WORKER_GATE_RUN").expect("run id");
    let panic_evidence = evidence.clone();
    let panic_run = run.clone();
    let panic_mode = mode.clone();
    std::panic::set_hook(Box::new(move |info| {
        gate_evidence(
            &panic_evidence,
            &panic_run,
            &panic_mode,
            "collection_error",
            serde_json::json!({"error":info.to_string(), "classification":"invalid"}),
        );
        eprintln!("{info}");
    }));
    rustix::process::set_child_subreaper(Some(rustix::process::Pid::INIT)).unwrap_or_else(
        |error| {
            panic!(
                "subreaper unavailable; evidence {}: {error}",
                evidence.display()
            )
        },
    );
    let root = temp_path(&format!("subreaper-{mode}"));
    let handshake = root.join("ready.json");
    let script = r#"
import json, os, signal, socket, time
r, w = os.pipe()
child = os.fork()
if child == 0:
    os.close(r)
    if os.environ['GATE_MODE'] == 'ignore_term': signal.signal(signal.SIGTERM, signal.SIG_IGN)
    os.write(w, b'1'); os.close(w)
    while True: signal.pause()
os.close(w); assert os.read(r, 1) == b'1'; os.close(r)
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.bind(os.environ['WORKER_SOCKET']); os.chmod(os.environ['WORKER_SOCKET'], 0o600); s.listen(1)
temporary = os.environ['GATE_HANDSHAKE'] + '.tmp'
with open(temporary, 'w') as f:
    json.dump({'child': child}, f); f.flush(); os.fsync(f.fileno())
os.replace(temporary, os.environ['GATE_HANDSHAKE'])
while True: time.sleep(1)
"#;
    let mut spec = configure_test_spec(
        ManagedWorkerSpec::new("/usr/bin/python3", &root, "generation-1").args(["-c", script]),
    );
    spec.socket_env = Some("WORKER_SOCKET".into());
    spec.startup_timeout = Duration::from_secs(3);
    spec.graceful_shutdown_timeout = Duration::from_millis(150);
    spec.env.insert("GATE_MODE".into(), mode.clone());
    spec.env.insert(
        "GATE_HANDSHAKE".into(),
        handshake.to_string_lossy().into_owned(),
    );
    spec.allowed_env_keys
        .extend(["GATE_MODE".into(), "GATE_HANDSHAKE".into()]);
    let handle = ManagedWorkerHandle::spawn(spec)
        .await
        .expect("spawn managed worker");
    let leader = handle.pid() as i32;
    let ready = Instant::now() + Duration::from_secs(3);
    while !handshake.exists() && Instant::now() < ready {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let data: serde_json::Value =
        serde_json::from_slice(&fs::read(&handshake).expect("handshake missing"))
            .expect("handshake invalid");
    let descendant = data["child"].as_i64().expect("descendant missing") as i32;
    let leader_identity = gate_process(leader).expect("worker identity unavailable");
    let initial = gate_process(descendant).expect("descendant identity unavailable");
    assert_eq!(
        leader_identity.pgrp, leader,
        "worker is not process-group leader"
    );
    assert_eq!(
        initial.pgrp, leader,
        "descendant outside owned process group"
    );
    assert_eq!(initial.ppid, leader, "descendant not owned by worker");
    assert!(
        !matches!(initial.state, 'Z' | 'X'),
        "descendant was not live"
    );
    let mut cleanup = GateCleanup {
        child: initial.clone(),
        leader: leader_identity.clone(),
        reaped: false,
    };
    let child_pid = rustix::process::Pid::from_raw(descendant).expect("valid descendant pid");
    let (pidfd, pidfd_result) =
        match rustix::process::pidfd_open(child_pid, rustix::process::PidfdFlags::empty()) {
            Ok(fd) => (Some(fd), "opened".to_string()),
            Err(error)
                if error == rustix::io::Errno::NOSYS || error == rustix::io::Errno::INVAL =>
            {
                (None, format!("unsupported: {error}"))
            }
            Err(error) => panic!(
                "pidfd collection failed: {error}; evidence {}",
                evidence.display()
            ),
        };
    let initial_group = gate_group_bounded(leader)
        .await
        .expect("initial group unavailable");
    gate_evidence(
        &evidence,
        &run,
        &mode,
        "handshake",
        serde_json::json!({
            "worker_pid":leader,"worker":leader_identity,"pgid":leader,"descendant_pid":descendant,
            "descendant_starttime":initial.starttime,"descendant_state":initial.state,"descendant_ppid":initial.ppid,
            "descendant_pgrp":initial.pgrp,"pidfd_open_result":pidfd_result,"group_members":initial_group,
            "socket_exists":gate_socket_exists(&handle.socket_path()).expect("socket observation failed"),"signal":null,"signal_result":null,
            "waitid_or_waitpid_result":null,"shutdown_result":null
        }),
    );
    let resistance_verified = if mode == "ignore_term" {
        let result = match &pidfd {
            Some(fd) => rustix::process::pidfd_send_signal(fd, rustix::process::Signal::TERM),
            None => rustix::process::kill_process(child_pid, rustix::process::Signal::TERM),
        };
        gate_evidence(
            &evidence,
            &run,
            &mode,
            "term_control_probe",
            serde_json::json!({
                "worker_pid":leader,"pgid":leader,"descendant_pid":descendant,
                "descendant_starttime":initial.starttime,"signal":"TERM",
                "signal_result":format!("{result:?}"),"errno":result.err().map(|error| error.raw_os_error())
            }),
        );
        result.expect("TERM control probe failed");
        tokio::time::sleep(Duration::from_millis(20)).await;
        let observed = gate_process(descendant).expect("TERM control observation failed");
        assert_eq!(
            observed.starttime, initial.starttime,
            "TERM control identity changed"
        );
        !matches!(observed.state, 'Z' | 'X')
    } else {
        false
    };
    let started = Instant::now();
    let task = if mode == "skip_shutdown" {
        None
    } else {
        let owned = handle.clone();
        Some(tokio::spawn(async move { owned.shutdown().await }))
    };
    tokio::time::sleep(Duration::from_millis(60)).await;
    let early = match gate_process(descendant) {
        Ok(observed) if observed.starttime == initial.starttime => Some(observed),
        Ok(_) => panic!("descendant identity reused"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("descendant observation failed: {error}"),
    };
    let early_survivor = early
        .as_ref()
        .is_some_and(|item| !matches!(item.state, 'Z' | 'X'));
    let shutdown = match task {
        Some(task) => match tokio::time::timeout(Duration::from_secs(4), task).await {
            Ok(Ok(result)) => format!("{result:?}"),
            other => format!("timeout or join failure: {other:?}"),
        },
        None => "intentionally skipped".to_string(),
    };
    let wait_deadline = Instant::now() + Duration::from_millis(500);
    let wait_result = loop {
        match rustix::process::waitpid(Some(child_pid), rustix::process::WaitOptions::NOHANG) {
            Ok(Some((pid, status))) => break format!("reaped pid={pid:?} status={status:?}"),
            Ok(None) | Err(rustix::io::Errno::CHILD) => {}
            Err(error) => break format!("waitpid error: {error}"),
        }
        if Instant::now() >= wait_deadline {
            break "not reaped before deadline".to_string();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    cleanup.reaped = wait_result.starts_with("reaped ");
    let group = gate_group_bounded(leader)
        .await
        .expect("final group unavailable");
    let live: Vec<_> = group
        .iter()
        .filter(|p| !matches!(p.state, 'Z' | 'X'))
        .collect();
    let worker_exited = handle.try_wait().await.expect("worker poll").is_some();
    let socket_exists =
        gate_socket_exists(&handle.socket_path()).expect("socket observation failed");
    let pass = mode != "skip_shutdown"
        && shutdown.starts_with("Ok(")
        && wait_result.starts_with("reaped ")
        && live.is_empty()
        && worker_exited
        && !socket_exists;
    gate_evidence(
        &evidence,
        &run,
        &mode,
        "verdict",
        serde_json::json!({
            "worker_pid":leader,"pgid":leader,"descendant_pid":descendant,"descendant_starttime":initial.starttime,
            "descendant_state":early.as_ref().map(|p| p.state),"descendant_ppid":early.as_ref().map(|p| p.ppid),
            "descendant_pgrp":early.as_ref().map(|p| p.pgrp),"pidfd_open_result":pidfd_result,
            "signal":if mode == "skip_shutdown" {None} else {Some("TERM requested by shutdown")},
            "signal_result":if mode == "skip_shutdown" {"not sent"} else {"see group_signal records"},"waitid_or_waitpid_result":wait_result,
            "group_members":group,"socket_exists":socket_exists,"shutdown_result":shutdown,
            "elapsed_ms":started.elapsed().as_millis(),"worker_exited":worker_exited,"early_survivor":early_survivor,
            "resistance_verified":resistance_verified,"pass":pass
        }),
    );
    // Cleanup is performed only after observing the verdict and checking the
    // original descendant identity; cleanup signals never make the gate pass.
    if let Ok(current) = gate_process(descendant) {
        assert_eq!(
            current.starttime, initial.starttime,
            "unsafe cleanup identity"
        );
        if !matches!(current.state, 'Z' | 'X') {
            let result = match &pidfd {
                Some(fd) => rustix::process::pidfd_send_signal(fd, rustix::process::Signal::KILL),
                None => rustix::process::kill_process(child_pid, rustix::process::Signal::KILL),
            };
            gate_evidence(
                &evidence,
                &run,
                &mode,
                "cleanup",
                serde_json::json!({
                    "worker_pid":leader,"pgid":leader,"descendant_pid":descendant,
                    "descendant_starttime":initial.starttime,"signal":"SIGKILL",
                    "signal_result":format!("{result:?}"),"errno":result.err().map(|error| error.raw_os_error())
                }),
            );
            result.expect("cleanup signal failed");
        }
    }
    if mode == "skip_shutdown" {
        let _ = handle.shutdown().await;
    }
    let end = Instant::now() + Duration::from_secs(2);
    let mut cleanup_reaped = wait_result.starts_with("reaped ");
    while !cleanup_reaped && Instant::now() < end {
        match rustix::process::waitpid(Some(child_pid), rustix::process::WaitOptions::NOHANG) {
            Ok(Some(result)) => {
                cleanup_reaped = true;
                gate_evidence(
                    &evidence,
                    &run,
                    &mode,
                    "cleanup_reaped",
                    serde_json::json!({"worker_pid":leader,"pgid":leader,"descendant_pid":descendant,
                        "descendant_starttime":initial.starttime,"waitid_or_waitpid_result":format!("{result:?}")}),
                );
            }
            Ok(None) | Err(rustix::io::Errno::CHILD) => {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            Err(error) => panic!("cleanup waitpid failed: {error}"),
        }
    }
    assert!(cleanup_reaped, "descendant cleanup was not reaped");
    cleanup.reaped = true;
    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn shutdown_kills_descendant_process_group_and_removes_socket() {
    let evidence_root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target-gatefix/worker-gate-evidence");
    fs::create_dir_all(&evidence_root).expect("evidence directory");
    let run = format!(
        "worker-gate-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    );
    let evidence = evidence_root.join(format!("{run}.jsonl"));
    let mut verdicts = Vec::new();
    for mode in ["normal", "ignore_term", "skip_shutdown"] {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process::tests::shutdown_subreaper_gate_helper",
                "--ignored",
                "--nocapture",
            ])
            .env("COWD_WORKER_GATE_MODE", mode)
            .env("COWD_WORKER_GATE_EVIDENCE", &evidence)
            .env("COWD_WORKER_GATE_RUN", &run)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn isolated helper");
        if tokio::time::timeout(Duration::from_secs(15), child.wait())
            .await
            .is_err()
        {
            let cleanup = gate_cleanup_timed_out_helper(&evidence, mode);
            let _ = child.start_kill();
            let _ = child.wait().await;
            panic!(
                "helper {mode} timed out; cleanup {cleanup:?}; evidence {}",
                evidence.display()
            );
        }
        let output = child.wait_with_output().await.expect("helper wait");
        assert!(
            output.status.success(),
            "helper {mode} failed; evidence {}: {}",
            evidence.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        let records = fs::read_to_string(&evidence).expect("evidence missing");
        let parsed: Vec<serde_json::Value> = records
            .lines()
            .map(|line| serde_json::from_str(line).expect("invalid gate evidence JSON"))
            .collect();
        let current: Vec<_> = parsed
            .iter()
            .filter(|value| value["sample_id"] == mode)
            .collect();
        assert!(
            current
                .iter()
                .all(|value| value["phase"] != "collection_error"),
            "{mode} evidence collection failed; evidence {}",
            evidence.display()
        );
        let verdict_index = current
            .iter()
            .position(|value| value["phase"] == "verdict")
            .unwrap_or_else(|| panic!("missing {mode} verdict in {}", evidence.display()));
        let signals: Vec<_> = current[..verdict_index]
            .iter()
            .filter(|value| value["phase"] == "group_signal")
            .collect();
        if mode == "skip_shutdown" {
            assert!(
                signals.is_empty(),
                "shutdown ran before negative verdict; evidence {}",
                evidence.display()
            );
        } else {
            assert!(
                signals.iter().any(|value| value["signal"] == "TERM"
                    && value["worker_pid"] == current[verdict_index]["worker_pid"]
                    && value["signal_result"]
                        .as_str()
                        .is_some_and(|result| result.starts_with("Ok("))),
                "{mode} original group signal was not captured; evidence {}",
                evidence.display()
            );
        }
        let matching: Vec<_> = parsed
            .into_iter()
            .filter(|value| value["phase"] == "verdict" && value["sample_id"] == mode)
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "missing or duplicate {mode} verdict in {}",
            evidence.display()
        );
        let record = matching.into_iter().next().unwrap();
        verdicts.push(record);
    }
    assert!(
        verdicts[2]["pass"] == false && verdicts[2]["early_survivor"] == true,
        "skip-shutdown negative control invalid; evidence {}",
        evidence.display()
    );
    assert_eq!(
        verdicts[1]["resistance_verified"],
        true,
        "TERM-resistant negative control not observed; evidence {}",
        evidence.display()
    );
    assert_eq!(
        verdicts[0]["pass"],
        true,
        "normal shutdown failed; evidence {}",
        evidence.display()
    );
    assert_eq!(
        verdicts[1]["pass"],
        true,
        "TERM-resistant descendant not terminated/reaped; evidence {}",
        evidence.display()
    );
}
