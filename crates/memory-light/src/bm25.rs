pub struct Bm25Ranker {
    pub k1: f32,
    pub b: f32,
}

impl Default for Bm25Ranker {
    fn default() -> Self { Self { k1: 1.5, b: 0.75 } }
}

impl Bm25Ranker {
    pub fn rerank(&self, query: &str, entries: Vec<crate::MemoryEntry>) -> Vec<(f32, crate::MemoryEntry)> {
        if entries.is_empty() { return Vec::new(); }
        let q_terms: Vec<String> = tokenize(query);
        if q_terms.is_empty() { return entries.into_iter().map(|e| (0.0, e)).collect(); }

        let n = entries.len() as f32;
        let doc_lens: Vec<usize> = entries.iter().map(|e| e.content.len()).collect();
        let avg_dl = doc_lens.iter().sum::<usize>() as f32 / n;

        let contents: Vec<String> = entries.iter().map(|e| e.content.to_lowercase()).collect();

        let mut scored: Vec<(f32, crate::MemoryEntry)> = entries.into_iter().enumerate().map(|(i, e)| {
            let dl = doc_lens[i] as f32;
            let score: f32 = q_terms.iter().map(|t| {
                let tf = contents[i].matches(t.as_str()).count() as f32;
                let df = contents.iter().filter(|c| c.contains(t.as_str())).count() as f32;
                let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln().max(0.0);
                idf * (tf * (self.k1 + 1.0)) / (tf + self.k1 * (1.0 - self.b + self.b * dl / avg_dl.max(1.0)))
            }).sum();
            (score, e)
        }).collect();

        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored
    }
}

fn tokenize(text: &str) -> Vec<String> {
    let stop: &[&str] = &["the","a","an","is","are","was","were","to","of","in","for","on","and","or","not","this","that","with","from","at","by","be","as","it","its","but"];
    text.split(|c: char| !c.is_alphanumeric() && c != '_')
        .map(|w| w.trim().to_lowercase())
        .filter(|w| w.len() > 2 && !stop.contains(&w.as_str()))
        .collect()
}
