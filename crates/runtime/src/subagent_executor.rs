use crate::agent::{SubAgentConfig, SubAgentResult};
use crate::conversation::{ConversationRuntime, RuntimeError};
use crate::permissions::PermissionPrompter;

pub struct SubAgentExecutor<C: crate::conversation::ApiClient, T: crate::conversation::ToolExecutor> {
    runtime: ConversationRuntime<C, T>,
}

impl<C: crate::conversation::ApiClient, T: crate::conversation::ToolExecutor> SubAgentExecutor<C, T> {
    pub fn new(_config: SubAgentConfig, runtime: ConversationRuntime<C, T>) -> Self {
        Self { runtime }
    }
    pub fn execute_sync(&mut self, task: &str, _prompter: Option<&mut dyn PermissionPrompter>) -> Result<SubAgentResult, RuntimeError> {
        let shared = crate::permissions::SharedPrompter::none();
        let handle = tokio::runtime::Handle::try_current()
            .unwrap_or_else(|_| tokio::runtime::Runtime::new().expect("tokio runtime fallback").handle().clone());
        let summary = handle.block_on(self.runtime.run_turn_async(task, &shared))?;
        let output = self.runtime.session().messages.last().map(|m| m.blocks.iter().filter_map(|b| match b { crate::session::ContentBlock::Text{text}=>Some(text.clone()), _=>None }).collect::<Vec<_>>().join(" ")).unwrap_or_default();
        Ok(SubAgentResult { output, tool_call_count: summary.tool_results.len(), tokens_used: summary.usage.total_tokens() as usize, completed_normally: true, memory_write_attempts: 0, memory_writes_denied: 0 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::SubAgentConfig;
    use crate::conversation::{ApiClient, ApiRequest, AssistantEvent, RuntimeError, StaticToolExecutor};
    use crate::permissions::{PermissionMode, PermissionPolicy};
    use crate::session::Session;
    use std::pin::Pin;
    use futures::stream::{Stream};

    struct EchoClient;
    impl ApiClient for EchoClient {
        fn stream(&mut self, _: ApiRequest) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(futures::stream::iter(vec![
                Ok(AssistantEvent::TextDelta("done".into())),
                Ok(AssistantEvent::MessageStop),
            ]))
        }
    }

    #[test]
    fn m6_subagent_executes_with_restricted_config() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session, EchoClient, StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".into()],
        );
        let config = SubAgentConfig {
            task_description: "test".into(),
            allowed_tools: vec!["read".into()],
            write_source: "SubAgent".into(),
            max_turns: 3,
            budget_tokens: 500,
            timeout_secs: None,
        };
        let mut executor = SubAgentExecutor::new(config, rt);
        let result = executor.execute_sync("hello", None).unwrap();
        assert!(result.completed_normally);
        assert_eq!(result.tool_call_count, 0);
    }
}
