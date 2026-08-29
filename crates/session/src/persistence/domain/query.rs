//! Shared bounded-query policy; adapters retain only SQL dialect and row mapping.

#[must_use]
pub const fn bounded_limit(requested: usize, default: usize, maximum: usize) -> usize {
    let selected = if requested == 0 { default } else { requested };
    if selected > maximum {
        maximum
    } else {
        selected
    }
}
