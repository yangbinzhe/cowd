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
        self.inner
            .run_named_command(candidate, "candidate", &candidate.candidate_command)
    }
}
