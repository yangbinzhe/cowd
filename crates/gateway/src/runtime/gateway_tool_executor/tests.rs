#[cfg(test)]
mod tests {
    use super::*;

    include!("tests/agentic_and_evidence.rs");
    include!("tests/authorization.rs");
    include!("tests/context_retrieval.rs");
    include!("tests/receipts.rs");
}
