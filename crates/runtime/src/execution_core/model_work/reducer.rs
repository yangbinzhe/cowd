use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelWorkReductionInput {
    pub summary: String,
    pub required: bool,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReducedModelWork {
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub omitted_items: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelWorkReducer {
    maximum_chars: usize,
}

impl ModelWorkReducer {
    #[must_use]
    pub const fn new(maximum_chars: usize) -> Self {
        Self { maximum_chars }
    }

    #[must_use]
    pub fn reduce(&self, mut inputs: Vec<ModelWorkReductionInput>) -> ReducedModelWork {
        inputs.sort_by_key(|input| !input.required);
        let mut output = ReducedModelWork::default();
        let mut evidence = BTreeSet::new();
        for input in inputs {
            evidence.extend(input.evidence_refs);
            let separator = usize::from(!output.summary.is_empty()) * 2;
            let current_chars = output.summary.chars().count();
            let input_chars = input.summary.chars().count();
            if current_chars
                .saturating_add(separator)
                .saturating_add(input_chars)
                > self.maximum_chars
            {
                output.omitted_items = output.omitted_items.saturating_add(1);
                if input.required {
                    let available = self
                        .maximum_chars
                        .saturating_sub(current_chars.saturating_add(separator));
                    if available > 0 {
                        if separator > 0 {
                            output.summary.push_str("\n\n");
                        }
                        output.summary.extend(input.summary.chars().take(available));
                    }
                }
                continue;
            }
            if !output.summary.is_empty() {
                output.summary.push_str("\n\n");
            }
            output.summary.push_str(&input.summary);
        }
        output.evidence_refs = evidence.into_iter().collect();
        output
    }
}

impl Default for ModelWorkReducer {
    fn default() -> Self {
        Self::new(32_768)
    }
}
