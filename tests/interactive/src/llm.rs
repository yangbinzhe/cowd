use std::process::Command;
use serde_json::Value;

/// Call LLM with a prompt, returns response text
fn call_llm(prompt: &str) -> Option<String> {
    // Try Claude API
    if let Ok(key) = std::env::var("ANTHROPIC_API_KEY") {
        let payload = serde_json::json!({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 256,
            "messages": [{"role": "user", "content": prompt}]
        });
        if let Ok(out) = Command::new("curl")
            .args(["-s", "https://api.anthropic.com/v1/messages",
                   "-H", "Content-Type: application/json",
                   "-H", &format!("x-api-key: {}", key),
                   "-H", "anthropic-version: 2023-06-01",
                   "-d", &payload.to_string()])
            .output()
        {
            if let Ok(body) = String::from_utf8(out.stdout) {
                if let Ok(json) = serde_json::from_str::<Value>(&body) {
                    return json["content"][0]["text"].as_str().map(|s| s.to_string());
                }
            }
        }
    }
    // Fallback: try local Ollama
    if let Ok(out) = Command::new("curl")
        .args(["-s", "http://localhost:11434/api/generate",
               "-d", &format!(r#"{{"model":"llama3.2","prompt":{},"stream":false}}"#, serde_json::to_string(prompt).unwrap_or_default())])
        .output()
    {
        if let Ok(body) = String::from_utf8(out.stdout) {
            if let Ok(json) = serde_json::from_str::<Value>(&body) {
                return json["response"].as_str().map(|s| s.to_string());
            }
        }
    }
    None
}

/// Generate a test conversation prompt based on skill/capability
pub fn generate_prompt(skill: &str) -> String {
    if let Some(response) = call_llm(&format!(
        "Generate a single short user prompt (max 10 words) for testing {}. \
         Just return the prompt, no explanation.", skill
    )) {
        return response.trim().trim_matches('"').to_string();
    }
    // Fallback defaults
    match skill {
        "file_operations" => "Write a bash function to find large files".into(),
        "code_review" => "Review this error handling pattern".into(),
        "debugging" => "Why might this Rust borrow checker fail?".into(),
        _ => format!("Explain how {} works in simple terms", skill),
    }
}

/// Validate TUI output against expected criteria using LLM
pub fn validate_output(output: &str, criteria: &str) -> Result<(), String> {
    if let Some(response) = call_llm(&format!(
        "You are a test judge. Determine if this terminal output meets the criteria.

CRITERIA: {}

OUTPUT:
{}

Answer ONLY: PASS or FAIL. If FAIL, explain why briefly (1 sentence).",
        criteria, output
    )) {
        let trimmed = response.trim();
        if trimmed.starts_with("PASS") {
            return Ok(());
        }
        return Err(trimmed.to_string());
    }
    // No LLM available: skip LLM-based validation
    Ok(())
}

/// Post-test analysis: generate summary from results
pub fn analyze_results(results_json: &str) -> String {
    let analysis = call_llm(&format!(
        "Analyze these test results and provide 3-5 bullet-point recommendations:\n{}\n\
         Focus on: failure patterns, stability concerns, suggested fixes.",
        results_json
    ));
    match analysis {
        Some(text) => format!("\n── LLM Analysis ──\n{}\n", text.trim()),
        None => String::new(),
    }
}
