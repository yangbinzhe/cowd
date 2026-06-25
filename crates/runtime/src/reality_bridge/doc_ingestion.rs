//! Document Ingestion and Classification.
//!
//! Provides tools for classifying and ingesting documents into the memory system.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentContent {
    pub title: String,
    pub body: String,
    pub source: Option<String>,
    pub author: Option<String>,
    pub created_at: Option<String>,
    pub modified_at: Option<String>,
    pub language: Option<String>,
}

impl DocumentContent {
    #[must_use]
    pub fn new(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            body: body.into(),
            source: None,
            author: None,
            created_at: None,
            modified_at: None,
            language: None,
        }
    }
}

/// Document category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentCategory {
    /// Technical documentation.
    Technical,
    /// User documentation/guide.
    UserGuide,
    /// API reference.
    ApiReference,
    /// Architecture/design document.
    Architecture,
    /// Meeting notes.
    MeetingNotes,
    /// Task/issue tracking.
    Task,
    /// Configuration file.
    Configuration,
    /// Code review.
    CodeReview,
    /// Knowledge base article.
    KnowledgeBase,
    /// Uncategorized.
    Other,
}

impl DocumentCategory {
    /// Get the display name.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Technical => "Technical Documentation",
            Self::UserGuide => "User Guide",
            Self::ApiReference => "API Reference",
            Self::Architecture => "Architecture Document",
            Self::MeetingNotes => "Meeting Notes",
            Self::Task => "Task/Issue",
            Self::Configuration => "Configuration",
            Self::CodeReview => "Code Review",
            Self::KnowledgeBase => "Knowledge Base",
            Self::Other => "Other",
        }
    }

    /// Get the memory layer priority.
    pub fn layer_priority(&self) -> u8 {
        match self {
            // High priority: frequently accessed
            Self::Configuration => 4,
            Self::UserGuide => 3,
            Self::ApiReference => 3,
            // Medium priority
            Self::Technical => 2,
            Self::Architecture => 2,
            Self::KnowledgeBase => 2,
            // Lower priority: one-time access
            Self::MeetingNotes => 1,
            Self::Task => 1,
            Self::CodeReview => 1,
            Self::Other => 0,
        }
    }
}

/// Document metadata for classification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    /// Document title.
    pub title: String,
    /// Document category.
    pub category: DocumentCategory,
    /// Confidence score for classification (0.0 - 1.0).
    pub confidence: f32,
    /// Keywords extracted from the document.
    pub keywords: Vec<String>,
    /// Tags for additional classification.
    pub tags: Vec<String>,
    /// Source URL or path.
    pub source: Option<String>,
    /// Author information.
    pub author: Option<String>,
    /// Creation date.
    pub created_at: Option<String>,
    /// Last modified date.
    pub modified_at: Option<String>,
    /// Document language.
    pub language: String,
}

/// Classification result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationResult {
    /// The document metadata.
    pub metadata: DocumentMetadata,
    /// Classification reasoning.
    pub reasoning: Vec<String>,
    /// Suggested memory layer.
    pub suggested_layer: u8,
}

/// Document classifier for categorizing documents.
pub struct DocumentClassifier {
    /// Keyword mappings for categories.
    keyword_map: HashMap<DocumentCategory, Vec<String>>,
    /// Custom rules.
    custom_rules: Vec<ClassificationRule>,
}

/// A classification rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationRule {
    /// Rule name.
    pub name: String,
    /// Category to assign if rule matches.
    pub category: DocumentCategory,
    /// Keywords that trigger this rule.
    pub keywords: Vec<String>,
    /// File patterns (e.g., "*.md", "*.json").
    pub patterns: Vec<String>,
    /// Minimum confidence if matched.
    pub min_confidence: f32,
}

impl DocumentClassifier {
    /// Create a new classifier with default rules.
    pub fn new() -> Self {
        Self {
            keyword_map: default_keyword_map(),
            custom_rules: Vec::new(),
        }
    }

    /// Create with custom rules.
    pub fn with_rules(rules: Vec<ClassificationRule>) -> Self {
        Self {
            keyword_map: default_keyword_map(),
            custom_rules: rules,
        }
    }

    /// Add a custom rule.
    pub fn add_rule(&mut self, rule: ClassificationRule) {
        self.custom_rules.push(rule);
    }

    /// Classify a document.
    pub fn classify(&self, content: &DocumentContent) -> ClassificationResult {
        let mut reasoning = Vec::new();
        let mut scores: HashMap<DocumentCategory, f32> = HashMap::new();

        // Initialize scores
        for category in [
            DocumentCategory::Technical,
            DocumentCategory::UserGuide,
            DocumentCategory::ApiReference,
            DocumentCategory::Architecture,
            DocumentCategory::MeetingNotes,
            DocumentCategory::Task,
            DocumentCategory::Configuration,
            DocumentCategory::CodeReview,
            DocumentCategory::KnowledgeBase,
            DocumentCategory::Other,
        ] {
            scores.insert(category, 0.0);
        }

        // Extract text from content
        let text = self.extract_text(content);
        let text_lower = text.to_lowercase();

        // Score based on keyword matches
        for (category, keywords) in &self.keyword_map {
            let matches = keywords
                .iter()
                .filter(|kw| text_lower.contains(&kw.to_lowercase()))
                .count();
            if matches > 0 {
                let score = (matches as f32) * 0.2;
                *scores.entry(*category).or_insert(0.0) += score;
                reasoning.push(format!(
                    "Found {} keyword matches for {:?}",
                    matches, category
                ));
            }
        }

        // Check custom rules
        for rule in &self.custom_rules {
            let keyword_matches = rule
                .keywords
                .iter()
                .filter(|kw| text_lower.contains(&kw.to_lowercase()))
                .count();
            let pattern_matches = rule
                .patterns
                .iter()
                .filter(|p| content.title.to_lowercase().contains(&p.to_lowercase()))
                .count();

            if keyword_matches > 0 || pattern_matches > 0 {
                let base_score = if keyword_matches > 0 { 0.3 } else { 0.1 };
                let multiplier = ((keyword_matches + pattern_matches) as f32).min(3.0);
                *scores.entry(rule.category).or_insert(0.0) += base_score * multiplier;
                reasoning.push(format!(
                    "Rule '{}' matched for {:?}",
                    rule.name, rule.category
                ));
            }
        }

        // Title-based scoring
        let title_lower = content.title.to_lowercase();
        if title_lower.contains("api") || title_lower.contains("reference") {
            *scores.entry(DocumentCategory::ApiReference).or_insert(0.0) += 0.4;
            reasoning.push("Title suggests API Reference".to_string());
        }
        if title_lower.contains("config")
            || title_lower.contains(".yaml")
            || title_lower.contains(".json")
        {
            *scores.entry(DocumentCategory::Configuration).or_insert(0.0) += 0.5;
            reasoning.push("Title suggests Configuration".to_string());
        }
        if title_lower.contains("meeting") || title_lower.contains("notes") {
            *scores.entry(DocumentCategory::MeetingNotes).or_insert(0.0) += 0.5;
            reasoning.push("Title suggests Meeting Notes".to_string());
        }
        if title_lower.contains("architecture") || title_lower.contains("design") {
            *scores.entry(DocumentCategory::Architecture).or_insert(0.0) += 0.5;
            reasoning.push("Title suggests Architecture".to_string());
        }
        if title_lower.contains("guide") || title_lower.contains("tutorial") {
            *scores.entry(DocumentCategory::UserGuide).or_insert(0.0) += 0.4;
            reasoning.push("Title suggests User Guide".to_string());
        }

        // Normalize scores and find best match
        let total: f32 = scores.values().sum();
        let mut best_category = DocumentCategory::Other;
        let mut best_score = 0.0f32;

        for (category, score) in &scores {
            let normalized = if total > 0.0 { score / total } else { 0.0 };
            if normalized > best_score {
                best_score = normalized;
                best_category = *category;
            }
        }

        // Extract keywords
        let keywords = self.extract_keywords(&text);
        let tags = self.generate_tags(&text, best_category);

        let confidence = best_score.min(1.0).max(0.1);
        let suggested_layer = best_category.layer_priority();

        let metadata = DocumentMetadata {
            title: content.title.clone(),
            category: best_category,
            confidence,
            keywords,
            tags,
            source: content.source.clone(),
            author: content.author.clone(),
            created_at: content.created_at.clone(),
            modified_at: content.modified_at.clone(),
            language: content
                .language
                .clone()
                .unwrap_or_else(|| "zh-CN".to_string()),
        };

        reasoning.push(format!(
            "Classified as {:?} with confidence {:.2}",
            best_category, confidence
        ));

        ClassificationResult {
            metadata,
            reasoning,
            suggested_layer,
        }
    }

    /// Extract plain text from document content.
    fn extract_text(&self, content: &DocumentContent) -> String {
        content.body.clone()
    }

    /// Extract keywords from text.
    fn extract_keywords(&self, text: &str) -> Vec<String> {
        let stop_words = vec![
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with",
            "by", "from", "as", "is", "was", "are", "were", "been", "be", "have", "has", "had",
            "do", "does", "did", "will", "would", "could", "should", "may", "might", "must",
            "shall", "这", "的", "是", "在", "了", "和", "与", "或", "但", "为", "与", "被", "由",
            "对", "于", "上", "下", "中", "内", "外", "前", "后",
        ];

        let words: Vec<String> = text
            .split_whitespace()
            .map(|w| {
                w.trim_matches(|c: char| !c.is_alphanumeric())
                    .to_lowercase()
            })
            .filter(|w| w.len() > 3 && !stop_words.contains(&w.as_str()))
            .collect();

        // Count word frequencies
        let mut freq: HashMap<String, usize> = HashMap::new();
        for word in &words {
            *freq.entry(word.clone()).or_insert(0) += 1;
        }

        // Get top keywords
        let mut keywords_vec: Vec<(usize, String)> =
            freq.into_iter().map(|(k, v)| (v, k)).collect();
        keywords_vec.sort_by(|a, b| b.0.cmp(&a.0));
        keywords_vec
            .iter()
            .take(10)
            .map(|(_, k)| k.clone())
            .collect()
    }

    /// Generate tags based on content and category.
    fn generate_tags(&self, text: &str, category: DocumentCategory) -> Vec<String> {
        let mut tags = vec![category.display_name().to_lowercase().replace(' ', "_")];

        // Add common technical tags
        let tech_tags = [
            ("rust", vec!["rust", "cargo", "crate"]),
            ("javascript", vec!["javascript", "js", "node", "npm"]),
            ("python", vec!["python", "pip", "py"]),
            ("api", vec!["api", "rest", "grpc", "endpoint"]),
            (
                "database",
                vec!["database", "sql", "db", "postgres", "mysql"],
            ),
            ("docker", vec!["docker", "container", "kubernetes", "k8s"]),
            ("git", vec!["git", "github", "commit", "branch"]),
            ("test", vec!["test", "testing", "unit", "integration"]),
            (
                "security",
                vec!["security", "auth", "oauth", "jwt", "token"],
            ),
            (
                "performance",
                vec!["performance", "optimization", "speed", "latency"],
            ),
        ];

        let text_lower = text.to_lowercase();
        for (tag, keywords) in tech_tags {
            if keywords.iter().any(|kw| text_lower.contains(kw)) {
                tags.push(tag.to_string());
            }
        }

        tags
    }
}

impl Default for DocumentClassifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Default keyword mappings for categories.
fn default_keyword_map() -> HashMap<DocumentCategory, Vec<String>> {
    let mut map = HashMap::new();

    map.insert(
        DocumentCategory::Technical,
        vec![
            "implementation".to_string(),
            "algorithm".to_string(),
            "performance".to_string(),
            "optimization".to_string(),
            "benchmark".to_string(),
        ],
    );

    map.insert(
        DocumentCategory::UserGuide,
        vec![
            "how to".to_string(),
            "tutorial".to_string(),
            "getting started".to_string(),
            "step by step".to_string(),
            "入门".to_string(),
            "教程".to_string(),
            "使用指南".to_string(),
        ],
    );

    map.insert(
        DocumentCategory::ApiReference,
        vec![
            "endpoint".to_string(),
            "method".to_string(),
            "parameter".to_string(),
            "response".to_string(),
            "error code".to_string(),
            "API".to_string(),
            "接口".to_string(),
        ],
    );

    map.insert(
        DocumentCategory::Architecture,
        vec![
            "architecture".to_string(),
            "design pattern".to_string(),
            "system design".to_string(),
            "component".to_string(),
            "module".to_string(),
            "架构".to_string(),
            "设计".to_string(),
        ],
    );

    map.insert(
        DocumentCategory::MeetingNotes,
        vec![
            "meeting".to_string(),
            "agenda".to_string(),
            "minutes".to_string(),
            "action item".to_string(),
            "decision".to_string(),
            "会议".to_string(),
            "纪要".to_string(),
        ],
    );

    map.insert(
        DocumentCategory::Task,
        vec![
            "task".to_string(),
            "issue".to_string(),
            "bug".to_string(),
            "feature".to_string(),
            "todo".to_string(),
            "任务".to_string(),
            "工单".to_string(),
        ],
    );

    map.insert(
        DocumentCategory::Configuration,
        vec![
            "config".to_string(),
            "setting".to_string(),
            "environment".to_string(),
            "variable".to_string(),
            "配置".to_string(),
            "环境变量".to_string(),
        ],
    );

    map.insert(
        DocumentCategory::CodeReview,
        vec![
            "review".to_string(),
            "pull request".to_string(),
            "pr".to_string(),
            "code review".to_string(),
            "审核".to_string(),
            "审查".to_string(),
        ],
    );

    map.insert(
        DocumentCategory::KnowledgeBase,
        vec![
            "knowledge".to_string(),
            "faq".to_string(),
            "troubleshooting".to_string(),
            "best practice".to_string(),
            "知识库".to_string(),
            "常见问题".to_string(),
        ],
    );

    map
}

/// Conflict resolution strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictStrategy {
    /// Keep the newer document.
    NewestWins,
    /// Keep the older document.
    OldestWins,
    /// Merge both documents.
    Merge,
    /// Keep the one with higher confidence.
    HighestConfidence,
    /// Keep both as separate versions.
    KeepBoth,
}

/// Ingestion result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionResult {
    /// Whether ingestion was successful.
    pub success: bool,
    /// Document metadata.
    pub metadata: DocumentMetadata,
    /// Assigned memory layer.
    pub layer: u8,
    /// Error message if failed.
    pub error: Option<String>,
    /// Warnings during ingestion.
    pub warnings: Vec<String>,
}

/// Document ingestor.
pub struct DocumentIngestor {
    classifier: DocumentClassifier,
    conflict_strategy: ConflictStrategy,
}

impl DocumentIngestor {
    /// Create a new ingestor.
    pub fn new() -> Self {
        Self {
            classifier: DocumentClassifier::new(),
            conflict_strategy: ConflictStrategy::HighestConfidence,
        }
    }

    /// Set the conflict resolution strategy.
    pub fn with_conflict_strategy(mut self, strategy: ConflictStrategy) -> Self {
        self.conflict_strategy = strategy;
        self
    }

    /// Ingest a document.
    pub fn ingest(&self, content: &DocumentContent) -> IngestionResult {
        let mut warnings = Vec::new();

        // Classify the document
        let result = self.classifier.classify(content);

        warnings.extend(result.reasoning.clone());

        // Check for potential conflicts
        let has_conflict = self.detect_conflict(content);
        if has_conflict {
            warnings.push("Potential conflict detected with existing documents".to_string());
        }

        IngestionResult {
            success: true,
            metadata: result.metadata,
            layer: result.suggested_layer,
            error: None,
            warnings,
        }
    }

    /// Detect potential conflicts with existing documents.
    fn detect_conflict(&self, _content: &DocumentContent) -> bool {
        // This would query existing documents in the memory system
        // For now, return false (no conflict detection implemented)
        false
    }
}

impl Default for DocumentIngestor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn create_test_doc(title: &str, text: &str) -> DocumentContent {
        DocumentContent::new(title, text)
    }

    #[test]
    fn test_document_category() {
        assert_eq!(DocumentCategory::Configuration.layer_priority(), 4);
        assert_eq!(DocumentCategory::MeetingNotes.layer_priority(), 1);
    }

    #[test]
    fn test_document_classifier() {
        let classifier = DocumentClassifier::new();

        let doc = create_test_doc(
            "API Reference Guide",
            "This document describes the REST API endpoints and parameters.",
        );

        let result = classifier.classify(&doc);
        assert!(result.metadata.confidence > 0.0);
    }

    #[test]
    fn test_keyword_extraction() {
        let classifier = DocumentClassifier::new();
        let text = "The rust programming language is great for performance critical code.";

        let keywords = classifier.extract_keywords(text);
        assert!(keywords.contains(&"rust".to_string()));
        assert!(keywords.contains(&"performance".to_string()));
    }

    #[test]
    fn test_tag_generation() {
        let classifier = DocumentClassifier::new();
        let text = "Using Docker with Kubernetes for container orchestration.";

        let tags = classifier.generate_tags(text, DocumentCategory::Technical);
        assert!(tags.contains(&"technical_documentation".to_string()));
    }
}
