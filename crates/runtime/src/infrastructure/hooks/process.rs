//! The original Hook owner retains the actual process and pipe observations.
//! No detached IO tasks: cancellation is checked between bounded IO operations.
use std::sync::{Arc, Mutex};
#[cfg(unix)]
use std::{
    io::{self, Read, Write},
    process::{Child, Command, ExitStatus, Output},
    time::Instant,
};

#[derive(Clone, Debug, Default)]
pub struct HookExecutionScope(Arc<Mutex<ScopeState>>);

#[derive(Debug, Default)]
struct ScopeState {
    running: bool,
    #[cfg(unix)]
    child: Option<HookChild>,
}

#[cfg(unix)]
#[derive(Debug)]
struct HookChild {
    child: Child,
    pgid: u32,
    status: Option<ExitStatus>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    cleanup_started: Option<Instant>,
    #[cfg(test)]
    fail_pipe_setup: bool,
}

impl HookExecutionScope {
    pub fn is_same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn is_closed(&self) -> bool {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(unix)]
        {
            !state.running && state.child.is_none()
        }
        #[cfg(not(unix))]
        {
            !state.running
        }
    }

    /// One nonblocking observation by the original retirement owner. An active
    /// command owns its own pump; observers cannot steal it or publish closure.
    pub fn try_quiesce(&self) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.running {
            return false;
        }
        #[cfg(unix)]
        if let Some(child) = state.child.as_mut() {
            child.child.stdin.take();
            if child.pump(&[], &mut 0, true).unwrap_or(false) {
                state.child = None;
            }
        }
        #[cfg(unix)]
        {
            state.child.is_none()
        }
        #[cfg(not(unix))]
        {
            true
        }
    }

    pub async fn quiesce(&self) -> bool {
        let deadline = tokio::time::Instant::now() + tools::bash::BASH_PROCESS_QUIESCENCE_TIMEOUT;
        loop {
            if self.try_quiesce() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[cfg(unix)]
    pub(super) fn execute(
        &self,
        command: &mut Command,
        input: &[u8],
        abort: Option<&super::HookAbortSignal>,
    ) -> io::Result<super::CommandExecution> {
        use std::os::unix::process::CommandExt;
        {
            let mut state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.running || state.child.is_some() {
                return Err(io::Error::other(
                    "original Hook physical scope is not closed",
                ));
            }
            if abort.is_some_and(super::HookAbortSignal::is_aborted) {
                return Ok(super::CommandExecution::Cancelled);
            }
            command.process_group(0);
            let child = command.spawn()?;
            state.child = Some(HookChild {
                pgid: child.id(),
                child,
                status: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                cleanup_started: None,
                #[cfg(test)]
                fail_pipe_setup: false,
            });
            state.running = true;
        }
        let _running = RunningGuard(self.clone());
        let mut written = 0;
        let mut failure = None;
        let mut cancelled = false;
        // The handles are registered before fcntl; an error retains ownership.
        {
            let mut state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let child = state
                .child
                .as_mut()
                .ok_or_else(|| io::Error::other("Hook child missing"))?;
            let flags = child.nonblocking();
            if let Err(error) = flags {
                failure = Some(error);
            }
        }
        loop {
            cancelled |= abort.is_some_and(super::HookAbortSignal::is_aborted);
            let mut state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let child = state
                .child
                .as_mut()
                .ok_or_else(|| io::Error::other("Hook child missing"))?;
            if let Err(error) = child.pump(input, &mut written, cancelled || failure.is_some()) {
                failure.get_or_insert(error);
                child.cleanup_started.get_or_insert_with(Instant::now);
            }
            // Pump again on later passes after a recoverable IO failure. Only
            // real EOF + leader reap + group quiet can release the handle.
            let closed = child.status.is_some()
                && child.child.stdout.is_none()
                && child.child.stderr.is_none()
                && tools::bash::owned_process_group_is_quiet(child.pgid).unwrap_or(false);
            if closed {
                let child = state
                    .child
                    .take()
                    .ok_or_else(|| io::Error::other("Hook child missing"))?;
                if let Some(error) = failure {
                    return Err(error);
                }
                return if cancelled {
                    Ok(super::CommandExecution::Cancelled)
                } else {
                    Ok(super::CommandExecution::Finished(Output {
                        status: child
                            .status
                            .ok_or_else(|| io::Error::other("Hook status missing"))?,
                        stdout: child.stdout,
                        stderr: child.stderr,
                    }))
                };
            }
            if child
                .cleanup_started
                .is_some_and(|started| started.elapsed() >= tools::bash::BASH_IO_DRAIN_TIMEOUT)
            {
                return Err(io::Error::other(
                    "Hook physical exit or pipe EOF remains unknown; original owner retained",
                ));
            }
            drop(state);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
}

#[cfg(unix)]
struct RunningGuard(HookExecutionScope);
#[cfg(unix)]
impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0
             .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .running = false;
    }
}

#[cfg(unix)]
impl HookChild {
    fn nonblocking(&self) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_pipe_setup {
            return Err(io::Error::other("controlled Hook pipe setup failure"));
        }
        fn set<F: std::os::fd::AsFd>(fd: &F) -> io::Result<()> {
            let flags = rustix::fs::fcntl_getfl(fd)?;
            rustix::fs::fcntl_setfl(fd, flags | rustix::fs::OFlags::NONBLOCK)?;
            Ok(())
        }
        if let Some(pipe) = &self.child.stdin {
            set(pipe)?;
        }
        if let Some(pipe) = &self.child.stdout {
            set(pipe)?;
        }
        if let Some(pipe) = &self.child.stderr {
            set(pipe)?;
        }
        Ok(())
    }

    fn pump(&mut self, input: &[u8], written: &mut usize, terminate: bool) -> io::Result<bool> {
        // Retire/reap even when pipe setup or reads failed. No IO error is
        // allowed to bypass the original process owner's cleanup attempt.
        let signal_result = if terminate {
            self.child.stdin.take();
            self.cleanup_started.get_or_insert_with(Instant::now);
            tools::bash::terminate_owned_process_group(self.pgid)
        } else {
            Ok(())
        };
        // A failed signal must not suppress an independently available reap.
        if self.status.is_none() {
            self.status = self.child.try_wait()?;
        }
        signal_result?;
        if self.status.is_some() {
            self.child.stdin.take();
            self.cleanup_started.get_or_insert_with(Instant::now);
            tools::bash::terminate_owned_process_group(self.pgid)?;
        }
        // Reapply after an interrupted setup; no blocking IO is allowed below.
        self.nonblocking()?;
        if !terminate {
            if let Some(stdin) = self.child.stdin.as_mut() {
                if *written < input.len() {
                    let end = input.len().min(written.saturating_add(16 * 1024));
                    match stdin.write(&input[*written..end]) {
                        Ok(0) => {
                            return Err(io::Error::new(io::ErrorKind::WriteZero, "Hook stdin"))
                        }
                        Ok(count) => *written += count,
                        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                            self.child.stdin.take();
                        }
                        Err(error)
                            if matches!(
                                error.kind(),
                                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                            ) => {}
                        Err(error) => return Err(error),
                    }
                }
                if *written == input.len() {
                    self.child.stdin.take();
                }
            }
        }
        read_chunk(&mut self.child.stdout, &mut self.stdout)?;
        read_chunk(&mut self.child.stderr, &mut self.stderr)?;
        if self.status.is_none() {
            self.status = self.child.try_wait()?;
        }
        if self.status.is_some() {
            self.child.stdin.take();
            self.cleanup_started.get_or_insert_with(Instant::now);
            tools::bash::terminate_owned_process_group(self.pgid)?;
        }
        Ok(self.status.is_some()
            && self.child.stdout.is_none()
            && self.child.stderr.is_none()
            && tools::bash::owned_process_group_is_quiet(self.pgid)?)
    }
}

#[cfg(unix)]
fn read_chunk<R: Read>(pipe: &mut Option<R>, output: &mut Vec<u8>) -> io::Result<()> {
    let Some(reader) = pipe.as_mut() else {
        return Ok(());
    };
    let mut buffer = [0_u8; 16 * 1024];
    match reader.read(&mut buffer) {
        Ok(0) => {
            pipe.take();
        }
        Ok(count) => output.extend_from_slice(&buffer[..count]),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
#[path = "process_tests.rs"]
mod tests;
