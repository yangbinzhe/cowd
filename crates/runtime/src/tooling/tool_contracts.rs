#[must_use]
pub fn attach_input_examples(
    definitions: Vec<tools::RuntimeToolDefinition>,
) -> Vec<tools::RuntimeToolDefinition> {
    definitions
        .into_iter()
        .map(|mut definition| {
            if let Some(example) =
                harness_contract::tool_input_contract::input_example(&definition.input_schema)
            {
                let rendered = example.to_string();
                let description = definition.description.unwrap_or_default();
                definition.description =
                    Some(format!("{description}\n\nExample input: {rendered}"));
            }
            definition
        })
        .collect()
}
