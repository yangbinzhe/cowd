use std::path::Path;

use super::{
    candidate::EvolutionCandidate, isolated_runner::IsolatedRunner,
    runner_policy::EvolutionRunnerPolicy, runner_result::EvolutionRunnerResult,
};

#[derive(Debug, Clone)]
pub struct WorktreeRunner {
    inner: IsolatedRunner,
}

impl WorktreeRunner {
    #[must_use]
    pub fn new(root: impl AsRef<Path>, policy: EvolutionRunnerPolicy) -> Self {
        Self {
            inner: IsolatedRunner::new(root, policy),
        }
    }

    pub fn run_candidate_check(
        &self,
        candidate: &EvolutionCandidate,
    ) -> Result<EvolutionRunnerResult, String> {
        let command = if candidate.candidate_command.trim().is_empty()
            || candidate.candidate_command.contains("cowd-evolution")
        {
            "true"
        } else {
            candidate.candidate_command.as_str()
        };
        self.inner.run_command(candidate, command)
    }
}
