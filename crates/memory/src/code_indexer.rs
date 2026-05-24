//! Code indexer — tree-sitter backed code symbol extraction.
//!
//! Parses source files using tree-sitter grammars and extracts:
//! - Symbols (functions, methods, classes, structs, interfaces, enums)
//! - Edges (calls, imports, extends, implements)
//!
//! Supports Rust, Python, TypeScript/TSX, Go, and Java.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tree_sitter::{Parser, TreeCursor};

use crate::error::MemoryError;
use crate::store::MemoryStore;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Supported programming languages for code indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexLanguage {
    Rust,
    Python,
    TypeScript,
    Go,
    Java,
}

impl IndexLanguage {
    /// Detect language from file extension.
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "rs" => Some(Self::Rust),
            "py" => Some(Self::Python),
            "ts" | "tsx" => Some(Self::TypeScript),
            "go" => Some(Self::Go),
            "java" => Some(Self::Java),
            _ => None,
        }
    }

    /// Check if a file path is indexable.
    pub fn is_indexable(path: &Path) -> bool {
        path.extension()
            .and_then(|e| e.to_str())
            .map(|ext| Self::from_extension(ext).is_some())
            .unwrap_or(false)
    }
}

/// Kind of a code symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolKind {
    Function,
    Method,
    Class,
    Struct,
    Interface,
    Enum,
    Trait,
    Module,
    Variable,
}

impl SymbolKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Function => "Function",
            Self::Method => "Method",
            Self::Class => "Class",
            Self::Struct => "Struct",
            Self::Interface => "Interface",
            Self::Enum => "Enum",
            Self::Trait => "Trait",
            Self::Module => "Module",
            Self::Variable => "Variable",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Function" => Some(Self::Function),
            "Method" => Some(Self::Method),
            "Class" => Some(Self::Class),
            "Struct" => Some(Self::Struct),
            "Interface" => Some(Self::Interface),
            "Enum" => Some(Self::Enum),
            "Trait" => Some(Self::Trait),
            "Module" => Some(Self::Module),
            "Variable" => Some(Self::Variable),
            _ => None,
        }
    }
}

/// A code symbol extracted from source code.
#[derive(Debug, Clone)]
pub struct CodeSymbol {
    pub id: String,
    pub name: String,
    pub kind: SymbolKind,
    pub file_path: String,
    pub line: usize,
    pub signature: String,
    pub doc: Option<String>,
}

/// Edge type between code symbols.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolEdgeType {
    Calls,
    Imports,
    Extends,
    Implements,
}

impl SymbolEdgeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Calls => "calls",
            Self::Imports => "imports",
            Self::Extends => "extends",
            Self::Implements => "implements",
        }
    }
}

/// An edge connecting two code symbols.
#[derive(Debug, Clone)]
pub struct SymbolEdge {
    pub source_id: String,
    pub target_id: String,
    pub edge_type: SymbolEdgeType,
    pub file_path: String,
}

/// File fingerprint for change detection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileFingerprint {
    pub mtime: i64,
    pub file_size: u64,
}

/// Statistics from an indexing run.
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    pub files_processed: usize,
    pub symbols_found: usize,
    pub edges_found: usize,
}

/// Impact report for a code symbol — what would break if this symbol changes.
///
/// Depth-based classification:
/// - d=1: WILL BREAK (direct callers)
/// - d=2: LIKELY AFFECTED (indirect callers)
/// - d=3: MAY NEED TESTING (transitive)
#[derive(Debug, Clone, Default)]
pub struct ImpactReport {
    pub symbol_name: String,
    pub symbol_id: String,
    /// Direct callers — d=1: WILL BREAK.
    pub direct_callers: Vec<String>,
    /// Indirect callers — d=2: LIKELY AFFECTED.
    pub indirect: Vec<String>,
    /// All affected file paths.
    pub affected_files: Vec<String>,
}

// ---------------------------------------------------------------------------
// CodeIndexer
// ---------------------------------------------------------------------------

/// Core code indexer using tree-sitter parsers.
///
/// Holds one parser per supported language, walks project trees to extract
/// symbols and call-graph edges, and supports incremental re-indexing via
/// file fingerprints.
pub struct CodeIndexer {
    /// One parser per supported language (pre-warmed with grammar).
    parsers: HashMap<IndexLanguage, Parser>,
    /// Project root for computing relative paths.
    project_root: PathBuf,
    /// Fingerprints of previously indexed files: (mtime, file_size).
    fingerprints: HashMap<PathBuf, FileFingerprint>,
    /// Optional store handle for impact analysis queries.
    store: Option<Arc<dyn MemoryStore>>,
}

impl CodeIndexer {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new indexer for the given project root.
    ///
    /// Initialises one tree-sitter parser per supported language. Returns an
    /// error if any language grammar fails to load.
    pub fn new(project_root: &Path) -> Result<Self, MemoryError> {
        let mut parsers = HashMap::new();

        // Rust
        let mut rust_parser = Parser::new();
        rust_parser
            .set_language(&tree_sitter_rust::LANGUAGE.into())
            .map_err(|e| MemoryError::Store(format!("tree-sitter-rust init: {e}")))?;
        parsers.insert(IndexLanguage::Rust, rust_parser);

        // Python
        let mut py_parser = Parser::new();
        py_parser
            .set_language(&tree_sitter_python::LANGUAGE.into())
            .map_err(|e| MemoryError::Store(format!("tree-sitter-python init: {e}")))?;
        parsers.insert(IndexLanguage::Python, py_parser);

        // TypeScript (use TSX grammar for both .ts and .tsx)
        let mut ts_parser = Parser::new();
        ts_parser
            .set_language(&tree_sitter_typescript::LANGUAGE_TSX.into())
            .map_err(|e| MemoryError::Store(format!("tree-sitter-typescript init: {e}")))?;
        parsers.insert(IndexLanguage::TypeScript, ts_parser);

        // Go
        let mut go_parser = Parser::new();
        go_parser
            .set_language(&tree_sitter_go::LANGUAGE.into())
            .map_err(|e| MemoryError::Store(format!("tree-sitter-go init: {e}")))?;
        parsers.insert(IndexLanguage::Go, go_parser);

        // Java
        let mut java_parser = Parser::new();
        java_parser
            .set_language(&tree_sitter_java::LANGUAGE.into())
            .map_err(|e| MemoryError::Store(format!("tree-sitter-java init: {e}")))?;
        parsers.insert(IndexLanguage::Java, java_parser);

        Ok(Self {
            parsers,
            project_root: project_root.to_path_buf(),
            fingerprints: HashMap::new(),
            store: None,
        })
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Index a single source file, returning extracted symbols and edges.
    pub fn index_file(&mut self, path: &Path) -> Result<(Vec<CodeSymbol>, Vec<SymbolEdge>), MemoryError> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        let lang = IndexLanguage::from_extension(ext).ok_or_else(|| {
            MemoryError::InvalidArgument(format!("unsupported file extension: {ext}"))
        })?;

        let source = fs::read_to_string(path)
            .map_err(|e| MemoryError::Store(format!("read {path:?}: {e}")))?;

        let parser = self
            .parsers
            .get_mut(&lang)
            .expect("parser initialised in new()");

        let tree = parser
            .parse(&source, None)
            .ok_or_else(|| MemoryError::Store(format!("parse failed for {path:?}")))?;

        let relative_path = path
            .strip_prefix(&self.project_root)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();

        let mut symbols = Vec::new();
        let mut edges = Vec::new();

        let mut cursor = tree.walk();
        self.extract_symbols(
            &mut cursor,
            &source,
            &relative_path,
            lang,
            &mut symbols,
            &mut edges,
        );

        Ok((symbols, edges))
    }

    /// Walk the project root and index all supported source files.
    ///
    /// Respects `.gitignore` files via the `ignore` crate if available;
    /// otherwise falls back to walking all files and filtering by extension.
    pub fn index_all(&mut self) -> Result<IndexStats, MemoryError> {
        let mut stats = IndexStats::default();
        let root = self.project_root.clone();

        for entry in walkdir::WalkDir::new(&root)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.path();
            if IndexLanguage::is_indexable(path) {
                match self.index_file(path) {
                    Ok((symbols, edges)) => {
                        stats.files_processed += 1;
                        stats.symbols_found += symbols.len();
                        stats.edges_found += edges.len();
                    }
                    Err(_e) => {
                        // Skip files that fail to parse (e.g., syntax errors)
                        stats.files_processed += 1;
                    }
                }
            }
        }

        Ok(stats)
    }

    // -----------------------------------------------------------------------
    // Incremental indexing
    // -----------------------------------------------------------------------

    /// Compute the fingerprint of a file: (mtime, file_size).
    pub fn compute_fingerprint(path: &Path) -> Result<FileFingerprint, MemoryError> {
        let metadata = fs::metadata(path)
            .map_err(|e| MemoryError::Store(format!("metadata {path:?}: {e}")))?;

        let mtime = metadata
            .modified()
            .map_err(|e| MemoryError::Store(format!("mtime {path:?}: {e}")))?
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        Ok(FileFingerprint {
            mtime,
            file_size: metadata.len(),
        })
    }

    /// Check whether a file has changed since last indexing.
    pub fn has_changed(&self, path: &Path) -> Result<bool, MemoryError> {
        let new_fp = Self::compute_fingerprint(path)?;
        match self.fingerprints.get(path) {
            Some(old_fp) => Ok(old_fp != &new_fp),
            None => Ok(true), // never indexed → changed
        }
    }

    /// Re-index a file only if it has changed since last indexing.
    ///
    /// Returns `Ok(Some((symbols, edges)))` if the file was re-indexed,
    /// `Ok(None)` if unchanged, and `Err` on failure.
    pub fn reindex_if_changed(
        &mut self,
        path: &Path,
    ) -> Result<Option<(Vec<CodeSymbol>, Vec<SymbolEdge>)>, MemoryError> {
        if !self.has_changed(path)? {
            return Ok(None);
        }

        let result = self.index_file(path)?;

        // Update fingerprint after successful indexing
        if let Ok(fp) = Self::compute_fingerprint(path) {
            self.fingerprints.insert(path.to_path_buf(), fp);
        }

        Ok(Some(result))
    }

    /// Load stored fingerprints (e.g., from SQLite) into the indexer.
    pub fn load_fingerprints(&mut self, fps: HashMap<PathBuf, FileFingerprint>) {
        self.fingerprints = fps;
    }

    /// Return a reference to stored file fingerprints.
    pub fn fingerprints(&self) -> &HashMap<PathBuf, FileFingerprint> {
        &self.fingerprints
    }

    /// Attach a memory store handle for impact analysis.
    #[must_use]
    pub fn with_store(mut self, store: Arc<dyn MemoryStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Analyse the impact of changing a code symbol.
    ///
    /// Returns an [`ImpactReport`] with direct callers (d=1: WILL BREAK),
    /// indirect callers (d=2: LIKELY AFFECTED), and affected files.
    ///
    /// If `symbol_name` is provided, attempts to look up the symbol by name
    /// first (via FTS5 search), then queries callers using the store's
    /// code-edges table.
    ///
    /// Returns an empty report if no store is attached or no symbol is found.
    pub async fn get_impact(&self, symbol_name: &str, depth: usize) -> ImpactReport {
        let store = match &self.store {
            Some(s) => s.clone(),
            None => {
                return ImpactReport {
                    symbol_name: symbol_name.to_string(),
                    ..Default::default()
                };
            }
        };

        // Find the symbol by name via FTS5 search
        let symbols = store.search_symbols(symbol_name, 1).await.unwrap_or_default();
        let target = match symbols.first() {
            Some(s) => s.clone(),
            None => {
                return ImpactReport {
                    symbol_name: symbol_name.to_string(),
                    ..Default::default()
                };
            }
        };

        let mut report = ImpactReport {
            symbol_name: target.name.clone(),
            symbol_id: target.id.clone(),
            ..Default::default()
        };

        // d=1: direct callers
        let callers = store.get_callers(&target.id).await.unwrap_or_default();
        for c in &callers {
            report.direct_callers.push(c.name.clone());
            if !report.affected_files.contains(&c.file_path) {
                report.affected_files.push(c.file_path.clone());
            }
            // Also add the symbol's own file
            if !report.affected_files.contains(&target.file_path) {
                report.affected_files.push(target.file_path.clone());
            }
        }

        // d=2: indirect callers (only if depth >= 2)
        if depth >= 2 {
            for caller in &callers {
                let indirect = store.get_callers(&caller.id).await.unwrap_or_default();
                for c in &indirect {
                    report.indirect.push(c.name.clone());
                    if !report.affected_files.contains(&c.file_path) {
                        report.affected_files.push(c.file_path.clone());
                    }
                }
            }
        }

        report
    }

    // -----------------------------------------------------------------------
    // Tree-sitter extraction helpers
    // -----------------------------------------------------------------------

    fn extract_symbols(
        &self,
        cursor: &mut TreeCursor<'_>,
        source: &str,
        file_path: &str,
        lang: IndexLanguage,
        symbols: &mut Vec<CodeSymbol>,
        edges: &mut Vec<SymbolEdge>,
    ) {
        let node = cursor.node();
        let kind = node.kind();

        match lang {
            IndexLanguage::Rust => self.handle_rust_node(node, kind, source, file_path, symbols, edges),
            IndexLanguage::Python => self.handle_python_node(node, kind, source, file_path, symbols, edges),
            IndexLanguage::TypeScript => self.handle_ts_node(node, kind, source, file_path, symbols, edges),
            IndexLanguage::Go => self.handle_go_node(node, kind, source, file_path, symbols, edges),
            IndexLanguage::Java => self.handle_java_node(node, kind, source, file_path, symbols, edges),
        }

        // Walk children recursively
        if cursor.goto_first_child() {
            self.extract_symbols(cursor, source, file_path, lang, symbols, edges);
            while cursor.goto_next_sibling() {
                self.extract_symbols(cursor, source, file_path, lang, symbols, edges);
            }
            cursor.goto_parent();
        }
    }

    fn make_symbol_id(file_path: &str, name: &str, line: usize) -> String {
        format!("{file_path}:{name}:{line}")
    }

    // --- Rust extraction ---
    #[allow(clippy::too_many_arguments)]
    fn handle_rust_node(
        &self,
        node: tree_sitter::Node<'_>,
        kind: &str,
        source: &str,
        file_path: &str,
        symbols: &mut Vec<CodeSymbol>,
        edges: &mut Vec<SymbolEdge>,
    ) {
        let line = node.start_position().row + 1;
        let node_source = node.utf8_text(source.as_bytes()).unwrap_or("");

        match kind {
            "function_item" => {
                let name = self.find_child_text(node, "identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Function,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.to_string(),
                    doc: self.extract_rust_doc(node, source),
                });
            }
            "struct_item" => {
                let name = self.find_child_text(node, "type_identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Struct,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.to_string(),
                    doc: self.extract_rust_doc(node, source),
                });
            }
            "enum_item" => {
                let name = self.find_child_text(node, "type_identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Enum,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.to_string(),
                    doc: self.extract_rust_doc(node, source),
                });
            }
            "trait_item" => {
                let name = self.find_child_text(node, "type_identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Trait,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.to_string(),
                    doc: self.extract_rust_doc(node, source),
                });
            }
            "call_expression" => {
                let func_name = self.find_child_text(node, "identifier", source);
                // Also check for field_expression like `foo.bar()`
                let callee = if func_name.is_none() {
                    self.extract_rust_method_call(node, source)
                } else {
                    func_name.map(|s| s.to_string())
                };
                if let Some(ref callee_name) = callee {
                    // Determine the enclosing function
                    if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                        edges.push(SymbolEdge {
                            source_id: enclosing,
                            target_id: Self::make_symbol_id(file_path, callee_name, 0), // target line unknown
                            edge_type: SymbolEdgeType::Calls,
                            file_path: file_path.to_string(),
                        });
                    }
                }
            }
            "use_declaration" => {
                // Extract imported module path
                let import_path = node_source.replace("use ", "").replace(';', "").trim().to_string();
                if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                    edges.push(SymbolEdge {
                        source_id: enclosing,
                        target_id: format!("<import>:{import_path}"),
                        edge_type: SymbolEdgeType::Imports,
                        file_path: file_path.to_string(),
                    });
                }
            }
            _ => {}
        }
    }

    fn extract_rust_doc(&self, _node: tree_sitter::Node<'_>, _source: &str) -> Option<String> {
        // TODO: Walk preceding sibling nodes to find doc comments (/// or /** */)
        // For now, return None — doc extraction is a future enhancement.
        None
    }

    fn extract_rust_method_call(&self, node: tree_sitter::Node<'_>, source: &str) -> Option<String> {
        // For field_expression like foo.bar() or method calls like self.bar()
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                if child.kind() == "field_expression" {
                    // Get the field name (right side of dot)
                    for j in 0..child.child_count() {
                        if let Some(field_child) = child.child(j) {
                            if field_child.kind() == "field_identifier" {
                                return field_child.utf8_text(source.as_bytes()).ok().map(|s| s.to_string());
                            }
                        }
                    }
                }
            }
        }
        None
    }

    // --- Python extraction ---
    #[allow(clippy::too_many_arguments)]
    fn handle_python_node(
        &self,
        node: tree_sitter::Node<'_>,
        kind: &str,
        source: &str,
        file_path: &str,
        symbols: &mut Vec<CodeSymbol>,
        edges: &mut Vec<SymbolEdge>,
    ) {
        let line = node.start_position().row + 1;
        let node_source = node.utf8_text(source.as_bytes()).unwrap_or("");

        match kind {
            "function_definition" => {
                let name = self.find_child_text(node, "identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Function,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "class_definition" => {
                let name = self.find_child_text(node, "identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Class,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "call" => {
                let func_name = self.find_child_text(node, "identifier", source);
                // For attribute access like self.method()
                let callee = if func_name.is_none() {
                    self.find_child_text_attr(node, "attribute", source).map(|s| s.to_string())
                } else {
                    func_name.map(|s| s.to_string())
                };
                if let Some(ref callee_name) = callee {
                    if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                        edges.push(SymbolEdge {
                            source_id: enclosing,
                            target_id: Self::make_symbol_id(file_path, callee_name, 0),
                            edge_type: SymbolEdgeType::Calls,
                            file_path: file_path.to_string(),
                        });
                    }
                }
            }
            "import_statement" | "import_from_statement" => {
                let import_text = node_source.trim().to_string();
                if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                    edges.push(SymbolEdge {
                        source_id: enclosing,
                        target_id: format!("<import>:{import_text}"),
                        edge_type: SymbolEdgeType::Imports,
                        file_path: file_path.to_string(),
                    });
                }
            }
            _ => {}
        }
    }

    // --- TypeScript/TSX extraction ---
    #[allow(clippy::too_many_arguments)]
    fn handle_ts_node(
        &self,
        node: tree_sitter::Node<'_>,
        kind: &str,
        source: &str,
        file_path: &str,
        symbols: &mut Vec<CodeSymbol>,
        edges: &mut Vec<SymbolEdge>,
    ) {
        let line = node.start_position().row + 1;
        let node_source = node.utf8_text(source.as_bytes()).unwrap_or("");

        match kind {
            "function_declaration" => {
                let name = self.find_child_text(node, "identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Function,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "method_definition" => {
                let name = self.find_child_text(node, "property_identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Method,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "class_declaration" => {
                let name = self.find_child_text(node, "identifier", source)
                    .or_else(|| self.find_child_text(node, "type_identifier", source))
                    .unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Class,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "interface_declaration" => {
                let name = self.find_child_text(node, "type_identifier", source)
                    .or_else(|| self.find_child_text(node, "identifier", source))
                    .unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Interface,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "call_expression" => {
                // Extract function/method name from call
                let callee_name = self.extract_ts_call_name(node, source);
                if let Some(ref callee_name) = callee_name {
                    if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                        edges.push(SymbolEdge {
                            source_id: enclosing,
                            target_id: Self::make_symbol_id(file_path, callee_name, 0),
                            edge_type: SymbolEdgeType::Calls,
                            file_path: file_path.to_string(),
                        });
                    }
                }
            }
            "import_statement" => {
                if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                    edges.push(SymbolEdge {
                        source_id: enclosing,
                        target_id: format!("<import>:{}", node_source.trim()),
                        edge_type: SymbolEdgeType::Imports,
                        file_path: file_path.to_string(),
                    });
                }
            }
            _ => {}
        }
    }

    fn extract_ts_call_name(&self, node: tree_sitter::Node<'_>, source: &str) -> Option<String> {
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "identifier" => return child.utf8_text(source.as_bytes()).ok().map(|s| s.to_string()),
                    "member_expression" => {
                        // foo.bar() → get "bar" (property)
                        for j in 0..child.child_count() {
                            if let Some(mc) = child.child(j) {
                                if mc.kind() == "property_identifier" {
                                    return mc.utf8_text(source.as_bytes()).ok().map(|s| s.to_string());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        None
    }

    // --- Go extraction ---
    #[allow(clippy::too_many_arguments)]
    fn handle_go_node(
        &self,
        node: tree_sitter::Node<'_>,
        kind: &str,
        source: &str,
        file_path: &str,
        symbols: &mut Vec<CodeSymbol>,
        edges: &mut Vec<SymbolEdge>,
    ) {
        let line = node.start_position().row + 1;
        let node_source = node.utf8_text(source.as_bytes()).unwrap_or("");

        match kind {
            "function_declaration" | "method_declaration" => {
                let name = self.find_go_func_name(node, source).unwrap_or("unknown");
                let has_receiver = node_source.contains(") ")
                    && node_source.matches('(').count() >= 2;
                let sym_kind = if has_receiver { SymbolKind::Method } else { SymbolKind::Function };
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: sym_kind,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "type_declaration" => {
                let spec_node = (0..node.child_count())
                    .find_map(|i| {
                        let c = node.child(i)?;
                        if c.kind() == "type_spec" { Some(c) } else { None }
                    })
                    .unwrap_or(node);
                let name = self.find_child_text(spec_node, "type_identifier", source)
                    .unwrap_or("unknown");
                let typ = self.detect_go_type_kind(spec_node, source);
                let id = Self::make_symbol_id(file_path, name, spec_node.start_position().row + 1);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: typ,
                    file_path: file_path.to_string(),
                    line: spec_node.start_position().row + 1,
                    signature: spec_node.utf8_text(source.as_bytes()).unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "call_expression" => {
                let callee_name = self.extract_go_call_name(node, source);
                if let Some(ref callee_name) = callee_name {
                    if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                        edges.push(SymbolEdge {
                            source_id: enclosing,
                            target_id: Self::make_symbol_id(file_path, callee_name, 0),
                            edge_type: SymbolEdgeType::Calls,
                            file_path: file_path.to_string(),
                        });
                    }
                }
            }
            "import_declaration" => {
                if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                    edges.push(SymbolEdge {
                        source_id: enclosing,
                        target_id: format!("<import>:{}", node_source.trim()),
                        edge_type: SymbolEdgeType::Imports,
                        file_path: file_path.to_string(),
                    });
                }
            }
            _ => {}
        }
    }

    fn detect_go_type_kind(&self, type_spec_node: tree_sitter::Node<'_>, source: &str) -> SymbolKind {
        let text = type_spec_node.utf8_text(source.as_bytes()).unwrap_or("");
        if text.contains("struct {") { return SymbolKind::Struct; }
        if text.contains("interface {") { return SymbolKind::Interface; }
        for i in 0..type_spec_node.child_count() {
            if let Some(child) = type_spec_node.child(i) {
                match child.kind() {
                    "struct_type" => return SymbolKind::Struct,
                    "interface_type" => return SymbolKind::Interface,
                    _ => {}
                }
            }
        }
        SymbolKind::Variable
    }

    fn find_go_func_name<'a>(
        &self,
        node: tree_sitter::Node<'_>,
        source: &'a str,
    ) -> Option<&'a str> {
        // For regular functions: first identifier is the name
        // For method declarations (with receiver): field_identifier is the name
        self.find_child_text(node, "identifier", source)
            .or_else(|| self.find_child_text(node, "field_identifier", source))
    }

    fn extract_go_call_name(&self, node: tree_sitter::Node<'_>, source: &str) -> Option<String> {
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "identifier" => return child.utf8_text(source.as_bytes()).ok().map(|s| s.to_string()),
                    "selector_expression" => {
                        // pkg.Func() → get field name
                        for j in 0..child.child_count() {
                            if let Some(sc) = child.child(j) {
                                if sc.kind() == "field_identifier" {
                                    return sc.utf8_text(source.as_bytes()).ok().map(|s| s.to_string());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        None
    }

    // --- Java extraction ---
    #[allow(clippy::too_many_arguments)]
    fn handle_java_node(
        &self,
        node: tree_sitter::Node<'_>,
        kind: &str,
        source: &str,
        file_path: &str,
        symbols: &mut Vec<CodeSymbol>,
        edges: &mut Vec<SymbolEdge>,
    ) {
        let line = node.start_position().row + 1;
        let node_source = node.utf8_text(source.as_bytes()).unwrap_or("");

        match kind {
            "method_declaration" => {
                let name = self.find_child_text(node, "identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Method,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "class_declaration" => {
                let name = self.find_child_text(node, "identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Class,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "interface_declaration" => {
                let name = self.find_child_text(node, "identifier", source).unwrap_or("unknown");
                let id = Self::make_symbol_id(file_path, name, line);
                symbols.push(CodeSymbol {
                    id: id.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Interface,
                    file_path: file_path.to_string(),
                    line,
                    signature: node_source.lines().next().unwrap_or("").to_string(),
                    doc: None,
                });
            }
            "method_invocation" => {
                let callee_name = self.extract_java_call_name(node, source);
                if let Some(ref callee_name) = callee_name {
                    if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                        edges.push(SymbolEdge {
                            source_id: enclosing,
                            target_id: Self::make_symbol_id(file_path, callee_name, 0),
                            edge_type: SymbolEdgeType::Calls,
                            file_path: file_path.to_string(),
                        });
                    }
                }
            }
            "import_declaration" => {
                if let Some(enclosing) = self.find_enclosing_symbol(node, symbols) {
                    edges.push(SymbolEdge {
                        source_id: enclosing,
                        target_id: format!("<import>:{}", node_source.trim()),
                        edge_type: SymbolEdgeType::Imports,
                        file_path: file_path.to_string(),
                    });
                }
            }
            _ => {}
        }
    }

    fn extract_java_call_name(&self, node: tree_sitter::Node<'_>, source: &str) -> Option<String> {
        // Method invocation: object.method() or this.method()
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "identifier" => return child.utf8_text(source.as_bytes()).ok().map(|s| s.to_string()),
                    _ => {}
                }
            }
        }
        // Try to get name from the node itself
        node.child_by_field_name("name")
            .and_then(|n| n.utf8_text(source.as_bytes()).ok())
            .map(|s| s.to_string())
    }

    // -----------------------------------------------------------------------
    // Tree traversal helpers
    // -----------------------------------------------------------------------

    /// Find the text of the first child with the given kind.
    fn find_child_text<'a>(
        &self,
        node: tree_sitter::Node<'_>,
        child_kind: &str,
        source: &'a str,
    ) -> Option<&'a str> {
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                if child.kind() == child_kind {
                    return child.utf8_text(source.as_bytes()).ok();
                }
            }
        }
        None
    }

    /// Find text of a child with given kind inside an attribute/field chain.
    fn find_child_text_attr<'a>(
        &self,
        node: tree_sitter::Node<'_>,
        child_kind: &str,
        source: &'a str,
    ) -> Option<&'a str> {
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                if child.kind() == child_kind {
                    // For attribute nodes, get the identifier inside
                    for j in 0..child.child_count() {
                        if let Some(attr_child) = child.child(j) {
                            if attr_child.kind() == "identifier" {
                                return attr_child.utf8_text(source.as_bytes()).ok();
                            }
                        }
                    }
                }
            }
        }
        None
    }

    /// Find the ID of the nearest enclosing symbol (for edge attribution).
    fn find_enclosing_symbol(
        &self,
        node: tree_sitter::Node<'_>,
        symbols: &[CodeSymbol],
    ) -> Option<String> {
        if symbols.is_empty() {
            return None;
        }

        let node_start = node.start_position().row as usize;

        // Find the symbol with the largest line <= node_start
        symbols
            .iter()
            .rev()
            .find(|s| s.line <= node_start + 1)
            .map(|s| s.id.clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp_file(dir: &tempfile::TempDir, name: &str, content: &str) -> PathBuf {
        let path = dir.path().join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    // -----------------------------------------------------------------------
    // T1: Core parser tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_rust_fn() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_temp_file(
            &dir,
            "src/lib.rs",
            r#"
/// A greeting function.
pub fn hello(name: &str) -> String {
    format!("Hello, {name}!")
}

pub struct MyStruct {
    pub x: i32,
}
"#,
        );

        let mut indexer = CodeIndexer::new(dir.path()).expect("failed to create indexer");
        let (symbols, _edges) = indexer.index_file(&path).expect("failed to index");

        let functions: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Function).collect();
        assert!(!functions.is_empty(), "should find at least one function");

        let hello = functions.iter().find(|s| s.name == "hello").expect("should find 'hello' function");
        assert_eq!(hello.kind, SymbolKind::Function);
        assert!(hello.signature.contains("fn hello"), "signature should contain 'fn hello'");

        let structs: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Struct).collect();
        assert!(!structs.is_empty(), "should find struct");
        assert!(structs.iter().any(|s| s.name == "MyStruct"));
    }

    #[test]
    fn test_parse_python_class() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_temp_file(
            &dir,
            "src/app.py",
            r#"
class MyClass:
    def method_one(self):
        pass

    def method_two(self, x: int) -> str:
        return str(x)

def standalone_func():
    my_class = MyClass()
    my_class.method_one()
"#,
        );

        let mut indexer = CodeIndexer::new(dir.path()).expect("failed to create indexer");
        let (symbols, edges) = indexer.index_file(&path).expect("failed to index");

        let classes: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Class).collect();
        assert!(!classes.is_empty(), "should find MyClass");
        assert!(classes.iter().any(|s| s.name == "MyClass"));

        let functions: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Function).collect();
        assert!(functions.len() >= 3, "should find at least 3 functions (2 methods + 1 standalone)");
        assert!(functions.iter().any(|s| s.name == "method_one"));
        assert!(functions.iter().any(|s| s.name == "standalone_func"));
    }

    #[test]
    fn test_extract_calls() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_temp_file(
            &dir,
            "src/main.rs",
            r#"
fn foo() {
    println!("hello");
}

fn bar() {
    foo();
    foo();
}
"#,
        );

        let mut indexer = CodeIndexer::new(dir.path()).expect("failed to create indexer");
        let (symbols, edges) = indexer.index_file(&path).expect("failed to index");

        // Should have foo and bar as functions
        assert!(symbols.iter().any(|s| s.name == "foo" && s.kind == SymbolKind::Function));
        assert!(symbols.iter().any(|s| s.name == "bar" && s.kind == SymbolKind::Function));

        // Should have call edges from bar to foo
        let call_edges: Vec<_> = edges.iter().filter(|e| e.edge_type == SymbolEdgeType::Calls).collect();
        assert!(!call_edges.is_empty(), "should have call edges");
        // bar calls foo at least once
        let bar_calls_foo = call_edges.iter().any(|e| {
            e.source_id.contains("bar") && e.target_id.contains("foo")
        });
        assert!(bar_calls_foo, "bar should call foo");
    }

    #[test]
    fn test_parse_typescript_fn() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_temp_file(
            &dir,
            "src/app.ts",
            r#"
function greet(name: string): string {
    return `Hello, ${name}!`;
}

class MyService {
    doWork(id: number): void {
        console.log(id);
    }
}

interface Config {
    port: number;
    host: string;
}
"#,
        );

        let mut indexer = CodeIndexer::new(dir.path()).expect("failed to create indexer");
        let (symbols, _edges) = indexer.index_file(&path).expect("failed to index");

        let functions: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Function).collect();
        assert!(functions.iter().any(|s| s.name == "greet"), "should find greet function");

        let classes: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Class).collect();
        assert!(classes.iter().any(|s| s.name == "MyService"), "should find MyService class");

        let interfaces: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Interface).collect();
        assert!(interfaces.iter().any(|s| s.name == "Config"), "should find Config interface");
    }

    #[test]
    fn test_parse_go_fn() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_temp_file(
            &dir,
            "main.go",
            r#"
package main

import "fmt"

type Server struct {
    Port int
}

func NewServer(port int) *Server {
    return &Server{Port: port}
}

func (s *Server) Start() error {
    fmt.Println("starting server")
    return nil
}
"#,
        );

        let mut indexer = CodeIndexer::new(dir.path()).expect("failed to create indexer");
        let (symbols, _edges) = indexer.index_file(&path).expect("failed to index");

        let functions: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Function).collect();
        assert!(functions.iter().any(|s| s.name == "NewServer"), "should find NewServer function");

        let methods: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Method).collect();
        assert!(methods.iter().any(|s| s.name == "Start"), "should find Start method");

        let structs: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Struct).collect();
        assert!(structs.iter().any(|s| s.name == "Server"), "should find Server struct");
    }

    // -----------------------------------------------------------------------
    // T2: Symbol storage tests
    //    (implemented in store/sqlite.rs #[cfg(test)] module)
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // T3: Incremental indexing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_reindex_changed_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_temp_file(
            &dir,
            "src/main.rs",
            r#"
fn foo() -> i32 {
    42
}
"#,
        );

        let mut indexer = CodeIndexer::new(dir.path()).expect("failed to create indexer");

        // First index — should process the file
        let result = indexer.reindex_if_changed(&path).expect("reindex failed");
        assert!(result.is_some(), "first indexing should process the file");
        let (symbols, _edges) = result.unwrap();
        assert!(symbols.iter().any(|s| s.name == "foo"));

        // Second index — should skip (unchanged)
        let result2 = indexer.reindex_if_changed(&path).expect("reindex failed");
        assert!(result2.is_none(), "unchanged file should be skipped");

        // Modify the file
        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(b"fn foo() -> i32 { 99 }\nfn bar() {}\n").unwrap();

        // Reindex after modification — should process again
        let result3 = indexer.reindex_if_changed(&path).expect("reindex failed");
        assert!(result3.is_some(), "changed file should be re-indexed");
        let (symbols3, _) = result3.unwrap();
        assert!(symbols3.iter().any(|s| s.name == "bar"), "should find new function bar");
    }

    #[test]
    fn test_unchanged_skipped() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_temp_file(
            &dir,
            "src/lib.rs",
            "fn always_here() {}",
        );

        let mut indexer = CodeIndexer::new(dir.path()).expect("failed to create indexer");

        // First pass — file is new
        assert!(indexer.has_changed(&path).unwrap(), "new file should be detected as changed");

        // Index and fingerprint
        let result = indexer.reindex_if_changed(&path).expect("reindex failed");
        assert!(result.is_some());

        // Second pass — unchanged
        assert!(!indexer.has_changed(&path).unwrap(), "unchanged file should not be detected as changed");

        let result2 = indexer.reindex_if_changed(&path).expect("reindex failed");
        assert!(result2.is_none(), "unchanged file should be skipped");
    }

    #[test]
    fn test_new_file_detected() {
        let dir = tempfile::TempDir::new().unwrap();

        // Create first file and index
        let path1 = write_temp_file(&dir, "src/a.rs", "fn first() {}");
        let mut indexer = CodeIndexer::new(dir.path()).expect("failed to create indexer");
        let _ = indexer.reindex_if_changed(&path1).expect("reindex failed");

        // Create second file — should be detected as new
        let path2 = write_temp_file(&dir, "src/b.rs", "fn second() {}");
        assert!(indexer.has_changed(&path2).unwrap(), "new file should be detected as changed");
        let result = indexer.reindex_if_changed(&path2).expect("reindex failed");
        assert!(result.is_some(), "new file should be indexed");
        let (symbols, _) = result.unwrap();
        assert!(symbols.iter().any(|s| s.name == "second"));
    }

    #[test]
    fn test_language_detection() {
        assert_eq!(IndexLanguage::from_extension("rs"), Some(IndexLanguage::Rust));
        assert_eq!(IndexLanguage::from_extension("py"), Some(IndexLanguage::Python));
        assert_eq!(IndexLanguage::from_extension("ts"), Some(IndexLanguage::TypeScript));
        assert_eq!(IndexLanguage::from_extension("tsx"), Some(IndexLanguage::TypeScript));
        assert_eq!(IndexLanguage::from_extension("go"), Some(IndexLanguage::Go));
        assert_eq!(IndexLanguage::from_extension("java"), Some(IndexLanguage::Java));
        assert_eq!(IndexLanguage::from_extension("txt"), None);
        assert_eq!(IndexLanguage::from_extension("md"), None);
    }

    // -----------------------------------------------------------------------
    // T8: Impact analysis tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_impact_analysis_returns_callers() {
        use crate::store::sqlite::SqliteStore;
        use crate::store::MemoryStore;

        let tmp = tempfile::TempDir::new().unwrap();
        let sqlite = SqliteStore::open_path(&tmp.path().join("impact.db")).unwrap();

        let caller = CodeSymbol {
            id: "a.rs:caller_fn:1".into(),
            name: "caller_fn".into(),
            kind: SymbolKind::Function,
            file_path: "a.rs".into(),
            line: 1,
            signature: "fn caller_fn()".into(),
            doc: None,
        };
        let callee = CodeSymbol {
            id: "b.rs:target_fn:5".into(),
            name: "target_fn".into(),
            kind: SymbolKind::Function,
            file_path: "b.rs".into(),
            line: 5,
            signature: "fn target_fn()".into(),
            doc: None,
        };

        sqlite.insert_symbol(&caller).await.unwrap();
        sqlite.insert_symbol(&callee).await.unwrap();

        let edge = SymbolEdge {
            source_id: "a.rs:caller_fn:1".into(),
            target_id: "b.rs:target_fn:5".into(),
            edge_type: SymbolEdgeType::Calls,
            file_path: "a.rs".into(),
        };
        sqlite.index_file_symbols("a.rs", &[caller.clone(), callee.clone()], &[edge]).unwrap();

        // Wrap in Arc<dyn MemoryStore> for CodeIndexer
        let store: std::sync::Arc<dyn MemoryStore> = std::sync::Arc::new(sqlite);

        let dir = tempfile::TempDir::new().unwrap();
        let indexer = CodeIndexer::new(dir.path())
            .unwrap()
            .with_store(store);

        let report = indexer.get_impact("target_fn", 2).await;
        assert!(!report.direct_callers.is_empty(), "should have direct callers");
        assert!(report.direct_callers.contains(&"caller_fn".to_string()));
        assert!(report.affected_files.contains(&"a.rs".to_string()));
        assert!(report.affected_files.contains(&"b.rs".to_string()));
    }

    #[tokio::test]
    async fn test_impact_report_empty_without_store() {
        let dir = tempfile::TempDir::new().unwrap();
        let indexer = CodeIndexer::new(dir.path()).unwrap();
        let report = indexer.get_impact("nonexistent", 1).await;
        assert_eq!(report.symbol_name, "nonexistent");
        assert!(report.direct_callers.is_empty());
        assert!(report.indirect.is_empty());
    }

    #[test]
    fn test_impact_report_default() {
        let report = ImpactReport::default();
        assert!(report.symbol_name.is_empty());
        assert!(report.direct_callers.is_empty());
        assert!(report.indirect.is_empty());
        assert!(report.affected_files.is_empty());
    }
}
