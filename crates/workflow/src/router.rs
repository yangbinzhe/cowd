// M7.1: IntentRouter — keyword-based routing to workflow templates
pub struct IntentRouter { pub rules: Vec<RoutingRule> }
pub struct RoutingRule { pub keywords: Vec<String>, pub workflow: String, pub priority: u8 }
impl IntentRouter {
    pub fn new() -> Self { Self { rules: Vec::new() } }
    pub fn add(&mut self, keywords: Vec<String>, workflow: &str, priority: u8) { self.rules.push(RoutingRule{keywords,workflow:workflow.to_string(),priority}); }
    pub fn route(&self, input: &str) -> Option<&str> {
        let lower = input.to_lowercase();
        self.rules.iter().filter(|r| r.keywords.iter().any(|k| lower.contains(k))).max_by_key(|r| r.priority).map(|r| r.workflow.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn m7_route_by_keyword_match() {
        let mut r = IntentRouter::new();
        r.add(vec!["bug".into(), "error".into(), "crash".into()], "debug", 10);
        r.add(vec!["feature".into(), "add".into()], "feature", 5);
        r.add(vec!["refactor".into(), "clean".into()], "refactor", 8);
        r.add(vec!["deploy".into(), "ship".into(), "release".into()], "ship", 7);
        r.add(vec!["research".into(), "explore".into(), "investigate".into()], "research", 3);
        assert_eq!(r.route("there is a bug in the code"), Some("debug"));
        assert_eq!(r.route("add new feature"), Some("feature"));
        assert_eq!(r.route("let's refactor this module"), Some("refactor"));
        assert_eq!(r.route("deploy to production"), Some("ship"));
        assert_eq!(r.route("investigate the memory leak"), Some("research"));
        assert_eq!(r.route("hello world"), None);
    }

    #[test]
    fn m7_higher_priority_wins_on_conflict() {
        let mut r = IntentRouter::new();
        r.add(vec!["bug".into()], "quick-fix", 3);
        r.add(vec!["bug".into(), "critical".into()], "incident", 10);
        assert_eq!(r.route("critical bug found"), Some("incident"));
    }
}
