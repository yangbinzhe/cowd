// Includes intentionally share one module scope so helper fixtures and the
// complete historical test inventory retain their behavior during extraction.
include!("auth.rs");
include!("session.rs");
include!("runtime.rs");
include!("security.rs");
include!("surface.rs");
include!("platform.rs");
