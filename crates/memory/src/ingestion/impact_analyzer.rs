use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::code_indexer::{CodeSymbol, SymbolEdgeType};
use crate::error::MemoryError;
use crate::store::MemoryStore;

/// Adjacency-list representation of a code call graph.
#[derive(Debug, Clone, Default)]
pub struct CallGraph {
    /// For each symbol ID, the set of symbols that call it (upstream).
    pub callers: HashMap<String, HashSet<String>>,
    /// For each symbol ID, the set of symbols it calls (downstream).
    pub callees: HashMap<String, HashSet<String>>,
}

/// BFS-based impact report grouped by traversal depth.
#[derive(Debug, Clone)]
pub struct ImpactReport {
    /// Human-readable symbol name.
    pub symbol_name: String,
    /// Unique symbol identifier.
    pub symbol_id: String,
    /// Callers/callees grouped by BFS depth (d=1, d=2, …).
    pub by_depth: HashMap<usize, HashSet<String>>,
    /// All file paths touched by the traversal.
    pub affected_files: Vec<String>,
}

/// Performs BFS-based impact analysis on the code call graph.
///
/// Loads all symbols and edges from a [`MemoryStore`] on construction
/// and builds in-memory adjacency lists for fast BFS traversal.
pub struct ImpactAnalyzer {
    graph: CallGraph,
    symbols: HashMap<String, CodeSymbol>,
}

impl ImpactAnalyzer {
    /// Load all symbols and edges from the store and build the call graph.
    pub async fn new(store: &Arc<dyn MemoryStore>) -> Result<Self, MemoryError> {
        let symbols = store.list_all_symbols().await?;
        let edges = store.list_all_edges().await?;

        let mut callers: HashMap<String, HashSet<String>> = HashMap::new();
        let mut callees: HashMap<String, HashSet<String>> = HashMap::new();
        let mut symbol_map: HashMap<String, CodeSymbol> = HashMap::new();

        for sym in &symbols {
            symbol_map.insert(sym.id.clone(), sym.clone());
        }

        for edge in &edges {
            if edge.edge_type == SymbolEdgeType::Calls {
                callers
                    .entry(edge.target_id.clone())
                    .or_default()
                    .insert(edge.source_id.clone());
                callees
                    .entry(edge.source_id.clone())
                    .or_default()
                    .insert(edge.target_id.clone());
            }
        }

        Ok(Self {
            graph: CallGraph { callers, callees },
            symbols: symbol_map,
        })
    }

    /// BFS on the callers graph (upstream). Returns symbols grouped by depth.
    pub fn analyze_upstream(&self, symbol_id: &str, depth: usize) -> ImpactReport {
        self.bfs(symbol_id, depth, true)
    }

    /// BFS on the callees graph (downstream). Returns symbols grouped by depth.
    pub fn analyze_downstream(&self, symbol_id: &str, depth: usize) -> ImpactReport {
        self.bfs(symbol_id, depth, false)
    }

    fn bfs(&self, start_id: &str, max_depth: usize, upstream: bool) -> ImpactReport {
        let graph = if upstream {
            &self.graph.callers
        } else {
            &self.graph.callees
        };

        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();
        let mut by_depth: HashMap<usize, HashSet<String>> = HashMap::new();
        let mut affected_files: HashSet<String> = HashSet::new();

        queue.push_back((start_id.to_string(), 0));
        visited.insert(start_id.to_string());

        while let Some((current, current_depth)) = queue.pop_front() {
            if current_depth > 0 {
                by_depth
                    .entry(current_depth)
                    .or_default()
                    .insert(current.clone());
                if let Some(sym) = self.symbols.get(&current) {
                    affected_files.insert(sym.file_path.clone());
                }
            }

            if current_depth < max_depth {
                if let Some(neighbors) = graph.get(&current) {
                    for neighbor in neighbors {
                        if !visited.contains(neighbor) {
                            visited.insert(neighbor.clone());
                            queue.push_back((neighbor.clone(), current_depth + 1));
                        }
                    }
                }
            }
        }

        // Include start symbol's own file
        if let Some(sym) = self.symbols.get(start_id) {
            affected_files.insert(sym.file_path.clone());
        }

        let symbol_name = self
            .symbols
            .get(start_id)
            .map(|s| s.name.clone())
            .unwrap_or_else(|| start_id.to_string());

        ImpactReport {
            symbol_name,
            symbol_id: start_id.to_string(),
            by_depth,
            affected_files: affected_files.into_iter().collect(),
        }
    }

    /// Format an impact report as a human-readable string for LLM context.
    pub fn format_impact_report(&self, report: &ImpactReport, direction: &str) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "=== Impact Report: {} ({}) ===\n",
            report.symbol_name, direction
        ));
        out.push_str(&format!("Symbol ID: {}\n\n", report.symbol_id));

        if report.by_depth.is_empty() {
            out.push_str("No callers/callees found within the specified depth.\n");
        } else {
            let mut depths: Vec<&usize> = report.by_depth.keys().collect();
            depths.sort();

            for d in &depths {
                let items = &report.by_depth[d];
                out.push_str(&format!("Depth {} ({} items):\n", d, items.len()));
                for item in items {
                    let name = self
                        .symbols
                        .get(item)
                        .map(|s| format!("{} ({:?})", s.name, s.kind))
                        .unwrap_or_else(|| item.clone());
                    out.push_str(&format!("  - {name}\n"));
                }
                out.push('\n');
            }
        }

        if !report.affected_files.is_empty() {
            out.push_str("Affected files:\n");
            for f in &report.affected_files {
                out.push_str(&format!("  - {f}\n"));
            }
        }

        out
    }
}
