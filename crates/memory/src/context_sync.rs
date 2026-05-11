// 08: ContextSync — cross-session context sharing.
// Derived from claude-context sync/ module.

use std::collections::HashMap;

pub struct ContextSync {
    shared: HashMap<String, Vec<String>>,
}

impl ContextSync {
    pub fn new() -> Self { Self { shared: HashMap::new() } }

    pub fn store(&mut self, session_id: &str, key_points: Vec<String>) {
        self.shared.insert(session_id.to_string(), key_points);
    }

    pub fn remove(&mut self, session_id: &str) {
        self.shared.remove(session_id);
    }

    pub fn inject_from_others(&self, target_session: &str, context: &mut String) -> usize {
        let mut injected = 0;
        for (sid, points) in &self.shared {
            if sid == target_session { continue; }
            if injected == 0 { context.push_str("\n<cross_session_context>\n"); }
            for point in points.iter().take(3) {
                context.push_str(&format!("  <synced from=\"{}\">{}</synced>\n",
                    &sid[..sid.len().min(8)], point));
                injected += 1;
            }
        }
        if injected > 0 { context.push_str("</cross_session_context>\n"); }
        injected
    }

    pub fn session_count(&self) -> usize { self.shared.len() }
}

impl Default for ContextSync {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t08_cross_session_inject() {
        let mut sync = ContextSync::new();
        sync.store("session-a", vec!["User is a Rust developer".into()]);
        sync.store("session-b", vec!["Project uses tokio".into()]);
        let mut ctx = String::from("base context");
        let count = sync.inject_from_others("session-a", &mut ctx);
        assert!(count > 0);
        assert!(ctx.contains("cross_session_context"));
    }

    #[test]
    fn t08_isolation_target_not_included() {
        let mut sync = ContextSync::new();
        sync.store("session-a", vec!["data from A".into()]);
        sync.store("session-b", vec!["data from B".into()]);
        let mut ctx = String::new();
        sync.inject_from_others("session-a", &mut ctx);
        assert!(!ctx.contains("data from A"));
        assert!(ctx.contains("data from B"));
    }

    #[test]
    fn t08_empty_sync_no_effect() {
        let sync = ContextSync::new();
        let mut ctx = String::from("original");
        let count = sync.inject_from_others("s1", &mut ctx);
        assert_eq!(count, 0);
        assert_eq!(ctx, "original");
    }
}