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
                // A degraded example is not an instance: it names the fields
                // instead of showing one. Calling it "Example input" would
                // teach the model to send a shape that is not an input.
                let (label, rendered) =
                    match harness_contract::tool_input_contract::degraded_example(&example) {
                        Some(degraded) => (
                            "Example degraded",
                            harness_contract::tool_input_contract::degraded_example_text(&degraded),
                        ),
                        None => ("Example input", example.to_string()),
                    };
                let description = definition.description.unwrap_or_default();
                definition.description =
                    Some(format!("{description}\n\n{label}: {rendered}"));
            }
            definition
        })
        .collect()
}
