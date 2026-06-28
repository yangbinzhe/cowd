//! Evidence-driven Skill draft generation.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillGenerationContext {
    pub task_description: String,
    pub tool_call_count: usize,
    pub error_count: usize,
    pub user_corrections: usize,
    pub accepted_plan_refs: Vec<String>,
    pub test_report_refs: Vec<String>,
    pub knowledge_refs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillGenerationTrigger {
    ExplicitTaskDescription,
    HighToolReuse,
    RepeatedErrors,
    UserCorrections,
    AcceptedPlan,
    TestEvidence,
    KnowledgeReuse,
}

impl SkillGenerationTrigger {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitTaskDescription => "explicit_task_description",
            Self::HighToolReuse => "high_tool_reuse",
            Self::RepeatedErrors => "repeated_errors",
            Self::UserCorrections => "user_corrections",
            Self::AcceptedPlan => "accepted_plan",
            Self::TestEvidence => "test_evidence",
            Self::KnowledgeReuse => "knowledge_reuse",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillDraft {
    pub should_generate: bool,
    pub name: String,
    pub content: String,
    pub triggers: Vec<SkillGenerationTrigger>,
}

#[must_use]
pub fn generate_skill_draft(
    requested_name: Option<String>,
    context: SkillGenerationContext,
) -> SkillDraft {
    let triggers = generation_triggers(&context);
    if triggers.is_empty() {
        return SkillDraft {
            should_generate: false,
            name: String::new(),
            content: String::new(),
            triggers,
        };
    }
    let name = requested_name.unwrap_or_else(|| generate_skill_name(&context.task_description));
    let content = render_skill_draft(&name, &context, &triggers);
    SkillDraft {
        should_generate: true,
        name,
        content,
        triggers,
    }
}

fn generation_triggers(context: &SkillGenerationContext) -> Vec<SkillGenerationTrigger> {
    let mut triggers = Vec::new();
    if !context.task_description.trim().is_empty() {
        triggers.push(SkillGenerationTrigger::ExplicitTaskDescription);
    }
    if context.tool_call_count >= 8 {
        triggers.push(SkillGenerationTrigger::HighToolReuse);
    }
    if context.error_count >= 2 {
        triggers.push(SkillGenerationTrigger::RepeatedErrors);
    }
    if context.user_corrections > 0 {
        triggers.push(SkillGenerationTrigger::UserCorrections);
    }
    if !context.accepted_plan_refs.is_empty() {
        triggers.push(SkillGenerationTrigger::AcceptedPlan);
    }
    if !context.test_report_refs.is_empty() {
        triggers.push(SkillGenerationTrigger::TestEvidence);
    }
    if !context.knowledge_refs.is_empty() {
        triggers.push(SkillGenerationTrigger::KnowledgeReuse);
    }
    triggers
}

fn render_skill_draft(
    name: &str,
    context: &SkillGenerationContext,
    triggers: &[SkillGenerationTrigger],
) -> String {
    let trigger_lines = triggers
        .iter()
        .map(|trigger| format!("- {}", trigger.as_str()))
        .collect::<Vec<_>>()
        .join("\n");
    let plan_refs = render_refs(&context.accepted_plan_refs);
    let test_refs = render_refs(&context.test_report_refs);
    let knowledge_refs = render_refs(&context.knowledge_refs);
    format!(
        r#"---
name: {name}
description: Reusable workflow generated from observed task evidence.
status: draft
---

# {name}

## Purpose
{purpose}

## Generation Evidence
{trigger_lines}

## When To Use
- Use when a future task matches the purpose above.
- Prefer this skill when the workflow, verification, or user corrections are reusable.

## Procedure
1. Confirm the task matches the trigger conditions.
2. Gather the minimum required context and evidence.
3. Execute the workflow using existing runtime tools and adapters.
4. Record evidence, verification output, and any user corrections.

## Verification
- The result must cite the plan or code facts it relies on.
- The result must state which checks were run or why they were skipped.
- Corrections should be fed back into a future revision candidate.

## References
### Accepted Plans
{plan_refs}

### Test Reports
{test_refs}

### Knowledge
{knowledge_refs}
"#,
        purpose = if context.task_description.trim().is_empty() {
            "A reusable workflow candidate."
        } else {
            context.task_description.trim()
        },
    )
}

fn render_refs(values: &[String]) -> String {
    if values.is_empty() {
        "- none".to_string()
    } else {
        values
            .iter()
            .map(|value| format!("- {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn generate_skill_name(description: &str) -> String {
    let words = description
        .split_whitespace()
        .take(4)
        .map(|word| {
            word.chars()
                .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
                .collect::<String>()
                .to_lowercase()
        })
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    if words.is_empty() {
        "generated-skill".to_string()
    } else {
        words.join("-")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_generation_creates_draft_from_reusable_signals() {
        let draft = generate_skill_draft(
            None,
            SkillGenerationContext {
                task_description: "review architecture plan and verify implementation".to_string(),
                tool_call_count: 9,
                error_count: 0,
                user_corrections: 1,
                accepted_plan_refs: vec!["plan.md".to_string()],
                test_report_refs: vec!["report.json".to_string()],
                knowledge_refs: Vec::new(),
            },
        );

        assert!(draft.should_generate);
        assert!(draft.content.contains("status: draft"));
        assert!(draft
            .triggers
            .contains(&SkillGenerationTrigger::HighToolReuse));
        assert!(draft
            .triggers
            .contains(&SkillGenerationTrigger::UserCorrections));
    }
}
