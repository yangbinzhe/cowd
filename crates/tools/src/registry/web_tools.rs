//! Web search and fetch tools.
//!
//! - `WebSearchTool`: queries a search engine API and returns structured results.
//! - `WebFetchTool`: fetches a URL and converts HTML to plain text/markdown.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// WebSearchTool
// ---------------------------------------------------------------------------

/// Configuration for the web search tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSearchConfig {
    /// Search engine API URL (e.g. Searxng instance or DuckDuckGo API).
    #[serde(default = "default_search_url")]
    pub search_url: String,
    /// Maximum number of results to return.
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    /// Request timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_search_url() -> String {
    "https://api.duckduckgo.com/".to_string()
}

fn default_max_results() -> usize {
    5
}

fn default_timeout_secs() -> u64 {
    15
}

impl Default for WebSearchConfig {
    fn default() -> Self {
        Self {
            search_url: default_search_url(),
            max_results: default_max_results(),
            timeout_secs: default_timeout_secs(),
        }
    }
}

/// A single search result item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub title: String,
    pub url: String,
    pub snippet: String,
}

/// Web search tool that queries an external search API.
pub struct WebSearchTool {
    config: WebSearchConfig,
    http: reqwest::Client,
}

impl WebSearchTool {
    /// Create a new web search tool with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(WebSearchConfig::default())
    }

    /// Create with custom configuration.
    #[must_use]
    pub fn with_config(config: WebSearchConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .user_agent("cc-rust/0.1")
            .build()
            .unwrap_or_default();
        Self { config, http }
    }

    /// Execute a web search query.
    ///
    /// Uses DuckDuckGo Instant Answer API as the default backend.
    /// Returns structured search results.
    pub async fn search(&self, query: &str) -> Result<Vec<SearchResult>, WebToolError> {
        let url = format!(
            "{}?q={}&format=json&no_html=1",
            self.config.search_url,
            urlencoding::encode(query)
        );

        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(WebToolError::Http)?;

        if !response.status().is_success() {
            return Err(WebToolError::Api(format!(
                "search API returned status {}",
                response.status()
            )));
        }

        let body: serde_json::Value = response.json().await.map_err(WebToolError::Http)?;

        // Parse DuckDuckGo Instant Answer API response
        let mut results = Vec::new();

        // Extract abstract text
        if let Some(abstract_text) = body.get("AbstractText").and_then(|v| v.as_str()) {
            if !abstract_text.is_empty() {
                results.push(SearchResult {
                    title: body
                        .get("Heading")
                        .and_then(|v| v.as_str())
                        .unwrap_or(query)
                        .to_string(),
                    url: body
                        .get("AbstractURL")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    snippet: abstract_text.to_string(),
                });
            }
        }

        // Extract related topics
        if let Some(topics) = body.get("RelatedTopics").and_then(|v| v.as_array()) {
            for topic in topics.iter().take(self.config.max_results) {
                if let Some(text) = topic.get("Text").and_then(|v| v.as_str()) {
                    results.push(SearchResult {
                        title: topic
                            .get("Text")
                            .and_then(|v| v.as_str())
                            .map(|t| t.chars().take(80).collect())
                            .unwrap_or_default(),
                        url: topic
                            .get("FirstURL")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        snippet: text.to_string(),
                    });
                }
            }
        }

        results.truncate(self.config.max_results);
        Ok(results)
    }

    /// Format search results as a readable string for tool output.
    pub fn format_results(&self, results: &[SearchResult]) -> String {
        if results.is_empty() {
            return "No results found.".to_string();
        }

        let mut output = String::new();
        for (i, result) in results.iter().enumerate() {
            output.push_str(&format!(
                "{}. {}{}\n   {}\n\n",
                i + 1,
                result.title,
                if result.url.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", result.url)
                },
                result.snippet,
            ));
        }
        output
    }
}

impl Default for WebSearchTool {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// WebFetchTool
// ---------------------------------------------------------------------------

/// Configuration for the web fetch tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebFetchConfig {
    /// Maximum response body size in bytes (default: 50KB).
    #[serde(default = "default_max_size")]
    pub max_size_bytes: usize,
    /// Request timeout in seconds.
    #[serde(default = "default_fetch_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_max_size() -> usize {
    50 * 1024
}

fn default_fetch_timeout_secs() -> u64 {
    15
}

impl Default for WebFetchConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: default_max_size(),
            timeout_secs: default_fetch_timeout_secs(),
        }
    }
}

/// Web fetch tool that retrieves and converts web pages to text.
pub struct WebFetchTool {
    config: WebFetchConfig,
    http: reqwest::Client,
}

impl WebFetchTool {
    /// Create a new web fetch tool with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(WebFetchConfig::default())
    }

    /// Create with custom configuration.
    #[must_use]
    pub fn with_config(config: WebFetchConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .user_agent("cc-rust/0.1")
            .build()
            .unwrap_or_default();
        Self { config, http }
    }

    /// Fetch a URL and convert the response body to readable text.
    pub async fn fetch(&self, url: &str) -> Result<String, WebToolError> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(WebToolError::Http)?;

        if !response.status().is_success() {
            return Err(WebToolError::Api(format!(
                "fetch returned status {}",
                response.status()
            )));
        }

        let body = response.text().await.map_err(WebToolError::Http)?;

        // Truncate if too large
        let body = if body.len() > self.config.max_size_bytes {
            format!(
                "{}\n\n[truncated: {} bytes omitted]",
                &body[..self.config.max_size_bytes],
                body.len() - self.config.max_size_bytes
            )
        } else {
            body
        };

        // Strip HTML tags for a simple text conversion
        let text = strip_html_tags(&body);

        Ok(text)
    }
}

impl Default for WebFetchTool {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from web tools.
#[derive(Debug, thiserror::Error)]
pub enum WebToolError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error: {0}")]
    Api(String),
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Minimal HTML tag stripping — removes everything between < and >.
fn strip_html_tags(html: &str) -> String {
    let mut result = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut in_script = false;

    let lower = html.to_ascii_lowercase();
    let bytes = html.as_bytes();

    for (i, &b) in bytes.iter().enumerate() {
        let c = b as char;

        // Detect script/style blocks
        if lower[i..].starts_with("<script") || lower[i..].starts_with("<style") {
            in_script = true;
        }
        if in_script && (lower[i..].starts_with("</script") || lower[i..].starts_with("</style")) {
            in_script = false;
            continue;
        }
        if in_script {
            continue;
        }

        if c == '<' {
            in_tag = true;
            continue;
        }
        if c == '>' && in_tag {
            in_tag = false;
            result.push(' ');
            continue;
        }
        if !in_tag {
            result.push(c);
        }
    }

    // Collapse whitespace
    let mut cleaned = String::new();
    let mut last_was_space = false;
    for c in result.chars() {
        if c.is_whitespace() {
            if !last_was_space {
                cleaned.push(' ');
                last_was_space = true;
            }
        } else {
            cleaned.push(c);
            last_was_space = false;
        }
    }

    cleaned.trim().to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_html_basic() {
        let html = "<html><body><h1>Hello</h1><p>World</p></body></html>";
        let text = strip_html_tags(html);
        assert!(text.contains("Hello"));
        assert!(text.contains("World"));
        assert!(!text.contains('<'));
    }

    #[test]
    fn strip_html_removes_script() {
        let html = "<p>Before</p><script>alert('xss')</script><p>After</p>";
        let text = strip_html_tags(html);
        assert!(text.contains("Before"));
        assert!(text.contains("After"));
        assert!(!text.contains("alert"));
    }

    #[test]
    fn strip_html_collapse_whitespace() {
        let html = "<p>  Hello   World  </p>";
        let text = strip_html_tags(html);
        assert_eq!(text, "Hello World");
    }

    #[test]
    fn search_config_defaults() {
        let config = WebSearchConfig::default();
        assert_eq!(config.max_results, 5);
        assert_eq!(config.timeout_secs, 15);
    }

    #[test]
    fn fetch_config_defaults() {
        let config = WebFetchConfig::default();
        assert_eq!(config.max_size_bytes, 50 * 1024);
        assert_eq!(config.timeout_secs, 15);
    }

    #[test]
    fn format_empty_results() {
        let tool = WebSearchTool::new();
        let output = tool.format_results(&[]);
        assert_eq!(output, "No results found.");
    }

    #[test]
    fn format_results_with_items() {
        let tool = WebSearchTool::new();
        let results = vec![SearchResult {
            title: "Test".to_string(),
            url: "https://example.com".to_string(),
            snippet: "A test result".to_string(),
        }];
        let output = tool.format_results(&results);
        assert!(output.contains("Test"));
        assert!(output.contains("example.com"));
        assert!(output.contains("A test result"));
    }
}
