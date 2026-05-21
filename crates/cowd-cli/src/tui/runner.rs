pub enum TurnOutcome {
    Success {
        runtime: crate::BuiltRuntime,
        message_count: usize,
        iterations: usize,
    },
    Cancelled,
    Error(String),
}
