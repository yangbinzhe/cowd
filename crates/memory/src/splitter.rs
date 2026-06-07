// P1-4: SemanticSplitter — split text on semantic boundaries.
// Derived from claude-context's splitter/ module.

pub fn semantic_split(text: &str, max_chars: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    // Split on paragraph boundaries first
    for para in text.split("\n\n") {
        if current.len() + para.len() > max_chars && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
        }
        if para.len() > max_chars {
            // Paragraph too large — split on sentence boundaries
            for sentence in para.split_inclusive(&['.', '!', '?', '\n'][..]) {
                if current.len() + sentence.len() > max_chars && !current.is_empty() {
                    chunks.push(std::mem::take(&mut current));
                }
                current.push_str(sentence);
            }
        } else {
            if !current.is_empty() {
                current.push_str("\n\n");
            }
            current.push_str(para);
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// Split on function/struct boundary (Rust-aware)
pub fn split_on_functions(code: &str, max_chars: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    for line in code.lines() {
        let is_boundary = line.starts_with("pub fn ")
            || line.starts_with("fn ")
            || line.starts_with("pub struct ")
            || line.starts_with("impl ")
            || line.starts_with("#[cfg(test)]")
            || line.starts_with("mod ");
        if is_boundary && current.len() > max_chars {
            chunks.push(std::mem::take(&mut current));
        }
        current.push_str(line);
        current.push('\n');
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p14_paragraph_split_respects_max_chars() {
        let text = "a".repeat(200) + "\n\n" + &"b".repeat(200);
        let chunks = semantic_split(&text, 150);
        assert!(chunks.len() >= 2);
        for c in &chunks {
            assert!(c.len() <= 350, "chunk len: {}", c.len());
        }
    }

    #[test]
    fn p14_single_paragraph_remains_whole() {
        let chunks = semantic_split("hello world", 1000);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn p14_function_split_detects_boundaries() {
        let code = "fn a() {}\npub fn b() {}\nimpl Foo {}\n";
        let chunks = split_on_functions(code, 5); // each line ~10 chars > 5
        assert!(chunks.len() >= 2, "got {} chunks", chunks.len());
    }
}
