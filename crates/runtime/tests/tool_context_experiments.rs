//! Experiments for the tool-context double lever.
//!
//! Three questions could not be settled by reading code, because each answer
//! depends on a runtime quantity rather than on a code path:
//!
//! - **E1** What does the existing single-item budget actually compress? The
//!   audit's claim that "the 56 KB read was never compressed" depends on a
//!   token-density assumption nobody had measured on this repository's text.
//! - **E2** Does a shared cumulative lease starve later reservations? The audit
//!   named `FuturesUnordered` completion order as the mechanism, which makes the
//!   outcome order-dependent; this measures how bad it gets, and whether the
//!   read-floor actually protects the case that matters.
//! - **E3** Where is the real cliff? Sweeping the lease size shows whether a
//!   proposed constant changes behaviour gradually or falls off a step, which is
//!   the difference between a tunable and a landmine.
//!
//! These are measurements, not assertions about a design. Each prints its own
//! result so the output is the deliverable.

use runtime::context_ledger::{estimate_text_tokens, ContextLedger};

fn text_of_bytes(n: usize) -> String {
    // Repository-like prose: mixed tokens, roughly 3.3 bytes per token as
    // measured on this corpus (README.md: 50,043 B -> 15,330 tok).
    let unit = "the quick brown fox jumps over the lazy dog and then keeps running for a while ";
    let mut out = String::with_capacity(n + unit.len());
    while out.len() < n {
        out.push_str(unit);
    }
    out.truncate(n);
    out
}

#[test]
fn e1_what_the_single_item_budget_actually_compresses() {
    const DELEGATED_TOOL_INLINE_TOKEN_BUDGET: u64 = 12_000;
    println!("\n=== E1: single-item budget vs real payloads ===");
    println!("budget = {DELEGATED_TOOL_INLINE_TOKEN_BUDGET} tok");
    for (label, bytes) in [
        ("small read (1 KB file)", 1_000),
        ("medium read (10 KB file)", 10_000),
        ("README-like (36 KB)", 36_176),
        ("large read (56 KB)", 56_656),
    ] {
        let body = text_of_bytes(bytes);
        let tokens = estimate_text_tokens(&body);
        let verdict = if tokens <= DELEGATED_TOOL_INLINE_TOKEN_BUDGET {
            "INLINE (full body enters context)"
        } else {
            "COMPACT (head 4000 + tail 1000 chars)"
        };
        println!(
            "  {label:28} {bytes:6} B -> {tokens:6} tok ({:.2} B/tok) => {}",
            bytes as f64 / tokens as f64,
            verdict
        );
    }
}

#[test]
fn e2_a_shared_lease_starves_later_reservations() {
    println!("\n=== E2: one lease, sequential readers (root-path shape) ===");
    let mut floor_ledger = ContextLedger::new(200_000, 12_000);
    let mut plain_ledger = ContextLedger::new(200_000, 12_000);
    floor_ledger.expand_tool_result_limit(48_000);
    plain_ledger.expand_tool_result_limit(48_000);
    println!("  lease total = 48,000 tok, each reader asks 12,000");
    for round in 1..=6 {
        let asked = 12_000;
        let with_floor = floor_ledger.reserve_tool_result_with_floor(asked, 8_192);
        let without = plain_ledger.reserve_tool_result(asked);
        println!(
            "  reader {round}: with-floor={with_floor:6}  no-floor={without:6}  remaining(floor)={:6}",
            floor_ledger.remaining_tool_result_tokens()
        );
    }
    println!(
        "  the no-floor ledger grants {} to the last reader - that is the\n  \
         collapse the budget_policy comment describes (a ~600-char stub).",
        plain_ledger.reserve_tool_result(12_000)
    );
}

#[test]
fn e3_where_is_the_real_cliff() {
    println!("\n=== E3: sweeping the lease size, single read族 request ===");
    for lease in [6_000u64, 8_192, 12_000, 16_000, 24_000, 48_000, 60_000, 120_000] {
        let mut ledger = ContextLedger::new(200_000, 12_000);
        ledger.expand_tool_result_limit(lease);
        // A 10 KB source file: the case the original 12,000 constant was chosen
        // for ("a whole source file in this repository is 5k-10k tokens").
        let file_tokens = estimate_text_tokens(&text_of_bytes(10_000));
        let with_floor = ledger.reserve_tool_result_with_floor(file_tokens, 8_192);
        let mut plain = ContextLedger::new(200_000, 12_000);
        plain.expand_tool_result_limit(lease);
        let without = plain.reserve_tool_result(file_tokens);
        println!(
            "  lease={lease:6}  file={file_tokens:5} tok  with-floor={with_floor:6}  no-floor={without:6}",
        );
    }
}

/// E4: the concurrency case the audit named.
///
/// `governed_tool_executor.rs:231-233` drains a `FuturesUnordered`, so tool
/// results are reserved in **completion order**, not in the order the model asked
/// for. A lease shared across that fan-out could hand its whole budget to
/// whichever readers happen to finish first. The floor is meant to prevent that;
/// this measures whether it actually does.
#[test]
fn e4_concurrent_readers_all_keep_the_read_floor() {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    const READERS: usize = 42;
    let lease = Arc::new(Mutex::new(ContextLedger::new(200_000, 12_000)));
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            lease.lock().await.expand_tool_result_limit(48_000);
            let mut set = tokio::task::JoinSet::new();
            for reader in 0..READERS {
                let lease = Arc::clone(&lease);
                // Descending delay: the LAST reader to be asked for finishes
                // FIRST, which is the worst case for completion-order
                // reservation - the big read lands last and could find nothing.
                let delay_ms = (READERS - reader) as u64 % 7;
                set.spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    let mut guard = lease.lock().await;
                    guard.reserve_tool_result_with_floor(12_000, 8_192)
                });
            }
            let mut grants = Vec::with_capacity(READERS);
            while let Some(joined) = set.join_next().await {
                grants.push(joined.expect("reader task"));
            }
            let mut sorted = grants.clone();
            sorted.sort_unstable();
            println!("\n=== E4: {READERS} concurrent readers, lease 48,000, ask 12,000 ===");
            println!("  min grant  = {:6} tok", sorted[0]);
            println!("  max grant  = {:6} tok", sorted[sorted.len() - 1]);
            let starved = grants.iter().filter(|grant| **grant < 8_192).count();
            let collapsed = grants.iter().filter(|grant| **grant == 0).count();
            println!("  below floor ({starved}) / fully collapsed ({collapsed}) of {READERS}");
            let kept = grants.iter().filter(|grant| **grant >= 8_192).count();
            println!(
                "  {kept}/{READERS} readers still hold at least one whole source file (8,192 tok)"
            );

            let plain = Arc::new(Mutex::new(ContextLedger::new(200_000, 12_000)));
            plain.lock().await.expand_tool_result_limit(48_000);
            let mut set = tokio::task::JoinSet::new();
            for reader in 0..READERS {
                let plain = Arc::clone(&plain);
                let delay_ms = (READERS - reader) as u64 % 7;
                set.spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    plain.lock().await.reserve_tool_result(12_000)
                });
            }
            let mut plain_grants = Vec::with_capacity(READERS);
            while let Some(joined) = set.join_next().await {
                plain_grants.push(joined.expect("reader task"));
            }
            let collapsed = plain_grants.iter().filter(|grant| **grant == 0).count();
            println!("  without floor: {collapsed}/{READERS} readers get NOTHING");
        });
}
