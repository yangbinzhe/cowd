// Test shards share one module scope so existing fixtures retain their exact
// visibility and behavior while no source shard exceeds the governance limit.
include!("provider.rs");
include!("context.rs");
