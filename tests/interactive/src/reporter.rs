use std::time::Instant;

#[derive(Debug, Clone, serde::Serialize)]
pub struct TestResult {
    pub name: String,
    pub passed: bool,
    pub duration_ms: u64,
    pub error: Option<String>,
}

pub struct TestRunner {
    pub results: Vec<TestResult>,
    start: Instant,
    tui_scenarios: u32,
    server_scenarios: u32,
    cross_scenarios: u32,
}

impl TestRunner {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
            start: Instant::now(),
            tui_scenarios: 0,
            server_scenarios: 0,
            cross_scenarios: 0,
        }
    }

    pub fn run<F>(&mut self, name: &str, f: F)
    where F: FnOnce() -> anyhow::Result<()>
    {
        let test_start = Instant::now();
        // Classify scenario
        if name.contains("cross") || name.contains("e2e") { self.cross_scenarios += 1; }
        else if name.contains("server") { self.server_scenarios += 1; }
        else { self.tui_scenarios += 1; }

        match f() {
            Ok(()) => {
                let ms = test_start.elapsed().as_millis() as u64;
                println!("  ✅ {:<30} {:>4}.{:03}s", name, ms / 1000, ms % 1000);
                self.results.push(TestResult {
                    name: name.to_string(), passed: true, duration_ms: ms, error: None,
                });
            }
            Err(e) => {
                let ms = test_start.elapsed().as_millis() as u64;
                println!("  ❌ {:<30} {:>4}.{:03}s  {}", name, ms / 1000, ms % 1000, e);
                self.results.push(TestResult {
                    name: name.to_string(), passed: false, duration_ms: ms, error: Some(e.to_string()),
                });
            }
        }
    }

    pub fn report(&self) {
        let total = self.results.len();
        let passed = self.results.iter().filter(|r| r.passed).count();
        let failed = total - passed;
        let elapsed = self.start.elapsed().as_secs_f64();

        println!("\n═══════════════════════════════════════════");
        println!("  TEST REPORT");
        println!("═══════════════════════════════════════════");
        println!("  Duration:     {:.1}s", elapsed);
        println!("  Total:        {total}");
        println!("  Passed:       {passed}");
        println!("  Failed:       {failed}");
        println!("  Pass rate:    {:.0}%", if total > 0 { passed as f64 / total as f64 * 100.0 } else { 0.0 });
        println!("  ───────────────────────────────────────");
        println!("  TUI tests:    {}", self.tui_scenarios);
        println!("  Server tests: {}", self.server_scenarios);
        println!("  Cross tests:  {}", self.cross_scenarios);

        if failed > 0 {
            println!("  ───────────────────────────────────────");
            println!("  FAILURES:");
            for r in &self.results {
                if !r.passed {
                    if let Some(ref e) = r.error {
                        println!("    ❌ {}: {}", r.name, e);
                    }
                }
            }
        }

        // Recommendations based on results
        println!("  ───────────────────────────────────────");
        println!("  RECOMMENDATIONS:");
        if failed == 0 {
            println!("    ✅ All tests passed. System is stable.");
        } else {
            let fail_rate = failed as f64 / total as f64 * 100.0;
            if fail_rate > 50.0 {
                println!("    🔴 Critical: {:.0}% failure rate. Check TUI/Server runtime.", fail_rate);
            } else if fail_rate > 20.0 {
                println!("    🟡 Warning: {:.0}% failure rate. Investigate failures.", fail_rate);
            } else {
                println!("    🟢 Note: {:.0}% failure rate. Minor issues detected.", fail_rate);
            }
            if failed > 0 && self.cross_scenarios > 0 {
                let cross_failed = self.results.iter().filter(|r| !r.passed && r.name.contains("cross")).count();
                if cross_failed > 0 {
                    println!("    🔄 Cross-test failures detected. TUI↔Server sync may be broken.");
                }
            }
        }

        // Overall verdict
        println!("  ───────────────────────────────────────");
        if failed == 0 {
            println!("  VERDICT: ✅ PASS");
        } else {
            println!("  VERDICT: ❌ FAIL ({failed}/{total} failed)");
        }
        println!("═══════════════════════════════════════════");
    }
}
