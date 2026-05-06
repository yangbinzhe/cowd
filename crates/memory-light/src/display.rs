pub fn render_memory_closet_section(store: &std::sync::Arc<crate::store::MemoryStore>) -> String {
    let entries = store.get_top_entries(crate::MemoryLayer::L1, crate::Priority::Normal, 30).unwrap_or_default();
    
    let closets: Vec<String> = entries.iter().map(|e| {
        let c = crate::MemoryCloset::from_entry(e);
        format!("- `{}` | W={} | {}", c.entities.join(","), c.weight, c.key_quote.chars().take(50).collect::<String>())
    }).collect();
    
    if closets.is_empty() {
        return String::from("No memory rooms yet. Conversations will auto-build them.\n");
    }
    
    let mut out = String::from("## Memory Rooms (Closets)\n\n");
    out.push_str("| Entities | W | Preview |\n|----------|---|---|\n");
    for c in closets.iter().take(15) {
        out.push_str(c);
        out.push('\n');
    }
    out.push_str("\nUse `/memory` to see full details.\n");
    out
}
