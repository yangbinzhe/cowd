//! Feishu Document Tools.
//!
//! Provides tools for reading and interacting with Feishu documents.

use crate::platform::adapter::{PlatformError, PlatformResult};
use crate::platform::feishu::FeishuAdapter;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Document type in Feishu.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentType {
    /// Document (docx).
    Doc,
    /// Spreadsheet (xlsx).
    Sheet,
    /// Bitable (database).
    Bitable,
    /// Mind notes.
    MindNote,
}

/// Document metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    /// Document token.
    pub token: String,
    /// Document type.
    pub doc_type: DocumentType,
    /// Document title.
    pub title: String,
    /// Owner open ID.
    pub owner_open_id: String,
    /// Create time.
    pub created_at: DateTime<Utc>,
    /// Last modified time.
    pub updated_at: DateTime<Utc>,
    /// Whether it's a folder.
    pub is_folder: bool,
    /// Parent folder token (if any).
    pub parent_token: Option<String>,
}

/// Raw document content element.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DocumentElement {
    /// Paragraph.
    Paragraph {
        elements: Vec<TextElement>,
        style: Option<ParagraphStyle>,
    },
    /// Heading.
    Heading {
        level: u8,
        elements: Vec<TextElement>,
        style: Option<ParagraphStyle>,
    },
    /// Code block.
    CodeBlock {
        language: String,
        elements: Vec<TextElement>,
    },
    /// Quote.
    Quote {
        elements: Vec<TextElement>,
    },
    /// Bullet list.
    BulletList {
        items: Vec<ListItem>,
    },
    /// Numbered list.
    NumberedList {
        items: Vec<ListItem>,
    },
    /// Table.
    Table {
        rows: Vec<TableRow>,
    },
    /// Image.
    Image {
        token: String,
        width: u32,
        height: u32,
    },
    /// Divider.
    Divider,
}

/// Text element within a paragraph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextElement {
    /// Text content.
    pub text: String,
    /// Text style.
    pub style: Option<TextStyle>,
}

/// Text style.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextStyle {
    /// Bold.
    pub bold: Option<bool>,
    /// Italic.
    pub italic: Option<bool>,
    /// Strikethrough.
    pub strikethrough: Option<bool>,
    /// Underline.
    pub underline: Option<bool>,
    /// Text color.
    pub text_color: Option<String>,
    /// Background color.
    pub background_color: Option<String>,
    /// Link URL.
    pub link: Option<String>,
}

/// Paragraph style.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParagraphStyle {
    /// Alignment.
    pub alignment: Option<String>,
    /// Indent level.
    pub indent_level: Option<u32>,
}

/// List item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListItem {
    /// Item content elements.
    pub elements: Vec<TextElement>,
    /// Nested items (for sublists).
    pub children: Vec<ListItem>,
}

/// Table row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRow {
    /// Cells in the row.
    pub cells: Vec<TableCell>,
    /// Whether this is a header row.
    pub is_header: bool,
}

/// Table cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCell {
    /// Cell content.
    pub content: Vec<DocumentElement>,
}

/// Document content with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentContent {
    /// Document metadata.
    pub metadata: DocumentMetadata,
    /// Document elements.
    pub elements: Vec<DocumentElement>,
    /// Raw document blocks (for advanced processing).
    #[serde(default)]
    pub raw_blocks: Vec<serde_json::Value>,
}

/// Feishu Document API Client.
pub struct DocumentClient {
    adapter: Arc<FeishuAdapter>,
}

impl DocumentClient {
    /// Create a new document client.
    pub fn new(adapter: Arc<FeishuAdapter>) -> Self {
        Self { adapter }
    }

    /// Get the base URL for Feishu document API.
    fn doc_api_base() -> &'static str {
        "https://open.feishu.cn/open-apis/docx/v1/documents"
    }

    /// Get document metadata.
    pub async fn get_metadata(&self, doc_token: &str) -> PlatformResult<DocumentMetadata> {
        let token = self.adapter.ensure_token().await?;
        let client = reqwest::Client::new();

        let response = client
            .get(&format!("{}/{}", Self::doc_api_base(), doc_token))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to fetch document: {}", e)))?;

        #[derive(Deserialize)]
        struct MetaResponse {
            code: i32,
            msg: String,
            data: Option<MetaData>,
        }

        #[derive(Deserialize)]
        struct MetaData {
            document: Option<DocMeta>,
        }

        #[derive(Deserialize)]
        struct DocMeta {
            document_id: String,
            title: Option<String>,
            owner_id: Option<OwnerId>,
            create_time: Option<String>,
            update_time: Option<String>,
            parent_token: Option<String>,
        }

        #[derive(Deserialize)]
        struct OwnerId {
            open_id: Option<String>,
        }

        if !response.status().is_success() {
            return Err(PlatformError::Unknown(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let resp: MetaResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to parse response: {}", e)))?;

        if resp.code != 0 {
            return Err(PlatformError::Unknown(format!(
                "API error: {}",
                resp.msg
            )));
        }

        let doc = resp.data.and_then(|d| d.document)
            .ok_or_else(|| PlatformError::Unknown("no document in response".to_string()))?;

        Ok(DocumentMetadata {
            token: doc.document_id,
            doc_type: DocumentType::Doc,
            title: doc.title.unwrap_or_else(|| "Untitled".to_string()),
            owner_open_id: doc.owner_id.and_then(|o| o.open_id).unwrap_or_default(),
            created_at: doc.create_time
                .and_then(|t| DateTime::parse_from_rfc3339(&t).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now),
            updated_at: doc.update_time
                .and_then(|t| DateTime::parse_from_rfc3339(&t).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now),
            is_folder: false,
            parent_token: doc.parent_token,
        })
    }

    /// Get document raw blocks (without parsing).
    pub async fn get_raw_blocks(&self, doc_token: &str) -> PlatformResult<Vec<serde_json::Value>> {
        let token = self.adapter.ensure_token().await?;
        let client = reqwest::Client::new();

        let response = client
            .get(&format!("{}/{}/document_blocks", Self::doc_api_base(), doc_token))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to fetch blocks: {}", e)))?;

        #[derive(Deserialize)]
        struct BlocksResponse {
            code: i32,
            msg: String,
            data: Option<BlocksData>,
        }

        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct BlocksData {
            items: Option<Vec<serde_json::Value>>,
            page_token: Option<String>,
            has_more: Option<bool>,
        }

        if !response.status().is_success() {
            return Err(PlatformError::Unknown(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let resp: BlocksResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to parse response: {}", e)))?;

        if resp.code != 0 {
            return Err(PlatformError::Unknown(format!(
                "API error: {}",
                resp.msg
            )));
        }

        Ok(resp.data.and_then(|d| d.items).unwrap_or_default())
    }

    /// Get full document content.
    pub async fn get_content(&self, doc_token: &str) -> PlatformResult<DocumentContent> {
        let metadata = self.get_metadata(doc_token).await?;
        let raw_blocks = self.get_raw_blocks(doc_token).await?;

        // Parse raw blocks into structured elements
        let elements = self.parse_blocks(&raw_blocks);

        Ok(DocumentContent {
            metadata,
            elements,
            raw_blocks,
        })
    }

    /// Parse raw blocks into document elements.
    fn parse_blocks(&self, blocks: &[serde_json::Value]) -> Vec<DocumentElement> {
        let mut elements = Vec::new();

        for block in blocks {
            if let Some(element) = self.parse_block(block) {
                elements.push(element);
            }
        }

        elements
    }

    /// Parse a single block into a document element.
    fn parse_block(&self, block: &serde_json::Value) -> Option<DocumentElement> {
        let block_type = block.get("block_type")?.as_i64()?;

        match block_type {
            // Paragraph
            2 | 3 | 4 | 5 | 6 | 7 => {
                let children = block.get("children").and_then(|c| c.as_array());
                let text = self.extract_text_from_children(children);

                if block_type == 2 {
                    Some(DocumentElement::Paragraph {
                        elements: text,
                        style: self.parse_paragraph_style(block),
                    })
                } else {
                    // Heading levels 1-5
                    let level = match block_type {
                        3 => 1,
                        4 => 2,
                        5 => 3,
                        6 => 4,
                        7 => 5,
                        _ => 1,
                    };
                    Some(DocumentElement::Heading {
                        level,
                        elements: text,
                        style: self.parse_paragraph_style(block),
                    })
                }
            }
            // Code block
            14 => {
                let language = block
                    .get("code")
                    .and_then(|c| c.get("language"))
                    .and_then(|l| l.as_str())
                    .unwrap_or("plaintext")
                    .to_string();
                let children = block.get("children").and_then(|c| c.as_array());
                let text = self.extract_text_from_children(children);

                Some(DocumentElement::CodeBlock { language, elements: text })
            }
            // Quote
            15 => {
                let children = block.get("children").and_then(|c| c.as_array());
                let text = self.extract_text_from_children(children);

                Some(DocumentElement::Quote { elements: text })
            }
            // Bullet list
            21 => {
                let items = self.parse_list_items(block, false);
                Some(DocumentElement::BulletList { items })
            }
            // Numbered list
            22 => {
                let items = self.parse_list_items(block, true);
                Some(DocumentElement::NumberedList { items })
            }
            // Table
            31 => {
                let rows = self.parse_table_rows(block);
                Some(DocumentElement::Table { rows })
            }
            // Image
            27 | 28 => {
                let token = block
                    .get("image")
                    .and_then(|i| i.get("token"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                let width = block
                    .get("image")
                    .and_then(|i| i.get("width"))
                    .and_then(|w| w.as_u64())
                    .unwrap_or(0) as u32;
                let height = block
                    .get("image")
                    .and_then(|i| i.get("height"))
                    .and_then(|h| h.as_u64())
                    .unwrap_or(0) as u32;

                Some(DocumentElement::Image { token, width, height })
            }
            // Divider
            37 => Some(DocumentElement::Divider),
            _ => None,
        }
    }

    /// Extract text from children blocks.
    fn extract_text_from_children(&self, children: Option<&Vec<serde_json::Value>>) -> Vec<TextElement> {
        let mut elements = Vec::new();

        if let Some(children_arr) = children {
            for child in children_arr {
                if let Some(text) = self.extract_text_element(child) {
                    elements.push(text);
                }
            }
        }

        elements
    }

    /// Extract a text element from a text block.
    fn extract_text_element(&self, block: &serde_json::Value) -> Option<TextElement> {
        let text = block
            .get("text_run")
            .and_then(|t| t.get("text"))
            .and_then(|t| t.as_str())
            .map(|s| s.to_string())?;

        let style = block.get("text_run").and_then(|t| {
            Some(TextStyle {
                bold: t.get("bold").and_then(|b| b.as_bool()),
                italic: t.get("italic").and_then(|i| i.as_bool()),
                strikethrough: t.get("strike_through").and_then(|s| s.as_bool()),
                underline: t.get("underline").and_then(|u| u.as_bool()),
                text_color: t.get("text_color").and_then(|c| c.as_str()).map(|s| s.to_string()),
                background_color: t.get("highlight_color").and_then(|c| c.as_str()).map(|s| s.to_string()),
                link: t.get("link").and_then(|l| l.get("url")).and_then(|u| u.as_str()).map(|s| s.to_string()),
            })
        });

        Some(TextElement { text, style })
    }

    /// Parse paragraph style.
    fn parse_paragraph_style(&self, block: &serde_json::Value) -> Option<ParagraphStyle> {
        Some(ParagraphStyle {
            alignment: block
                .get("paragraph_style")
                .and_then(|p| p.get("align"))
                .and_then(|a| a.as_str())
                .map(|s| s.to_string()),
            indent_level: block
                .get("paragraph_style")
                .and_then(|p| p.get("indent_level"))
                .and_then(|l| l.as_u64())
                .map(|l| l as u32),
        })
    }

    /// Parse list items.
    fn parse_list_items(&self, block: &serde_json::Value, numbered: bool) -> Vec<ListItem> {
        let mut items = Vec::new();

        if let Some(children) = block.get("children").and_then(|c| c.as_array()) {
            for child in children {
                let elements = self.extract_text_from_children(
                    child.get("children").and_then(|c| c.as_array())
                );
                let children_items = self.parse_list_items(child, numbered);

                items.push(ListItem {
                    elements,
                    children: children_items,
                });
            }
        }

        items
    }

    /// Parse table rows.
    fn parse_table_rows(&self, block: &serde_json::Value) -> Vec<TableRow> {
        let mut rows = Vec::new();

        if let Some(cells_data) = block.get("table").and_then(|t| t.get("cells")) {
            if let Some(cells_arr) = cells_data.as_array() {
                for (idx, cell_data) in cells_arr.iter().enumerate() {
                    let text_elements = self.extract_text_from_children(
                        cell_data.get("children").and_then(|c| c.as_array())
                    );

                    // Wrap text in a Paragraph element
                    let content = vec![DocumentElement::Paragraph {
                        elements: text_elements,
                        style: None,
                    }];

                    rows.push(TableRow {
                        cells: vec![TableCell { content }],
                        is_header: idx == 0,
                    });
                }
            }
        }

        rows
    }

    /// Convert document content to markdown.
    pub fn to_markdown(&self, content: &DocumentContent) -> String {
        let mut md = String::new();

        md.push_str(&format!("# {}\n\n", content.metadata.title));
        md.push_str(&format!("> Last updated: {}\n\n", content.metadata.updated_at));

        for element in &content.elements {
            self.element_to_markdown(element, &mut md, 0);
            md.push('\n');
        }

        md
    }

    /// Convert a single element to markdown.
    fn element_to_markdown(&self, element: &DocumentElement, output: &mut String, indent: usize) {
        let indent_str = "  ".repeat(indent);

        match element {
            DocumentElement::Paragraph { elements, .. } => {
                let text = self.elements_to_text(elements);
                output.push_str(&format!("{}{}\n", indent_str, text));
            }
            DocumentElement::Heading { level, elements, .. } => {
                let text = self.elements_to_text(elements);
                let prefix = "#".repeat(*level as usize);
                output.push_str(&format!("{} {} {}\n", indent_str, prefix, text));
            }
            DocumentElement::CodeBlock { language, elements, .. } => {
                let text = self.elements_to_text(elements);
                output.push_str(&format!("{}```{}\n{}{}\n```\n", indent_str, language, indent_str, text));
            }
            DocumentElement::Quote { elements, .. } => {
                let text = self.elements_to_text(elements);
                output.push_str(&format!("{}> {}\n", indent_str, text));
            }
            DocumentElement::BulletList { items } => {
                for item in items {
                    self.list_item_to_markdown(item, output, indent, "- ");
                }
            }
            DocumentElement::NumberedList { items } => {
                for (idx, item) in items.iter().enumerate() {
                    self.list_item_to_markdown(item, output, indent, &format!("{}. ", idx + 1));
                }
            }
            DocumentElement::Table { rows } => {
                for row in rows {
                    let cells: Vec<String> = row.cells.iter()
                        .map(|c| self.elements_to_text_from_doc_elements(&c.content))
                        .collect();
                    let cell_str = cells.join(" | ");
                    if row.is_header {
                        output.push_str(&format!("{}| {} |\n", indent_str, cell_str));
                        output.push_str(&format!("{}|{}|\n", indent_str, "-".repeat(cells.iter().map(|s| s.len()).max().unwrap_or(1) + 2)));
                    } else {
                        output.push_str(&format!("{}| {} |\n", indent_str, cell_str));
                    }
                }
            }
            DocumentElement::Image { .. } => {
                output.push_str(&format!("{}![image](...)\n", indent_str));
            }
            DocumentElement::Divider => {
                output.push_str(&format!("{}---\n", indent_str));
            }
        }
    }

    /// Convert list item to markdown.
    fn list_item_to_markdown(&self, item: &ListItem, output: &mut String, indent: usize, prefix: &str) {
        let indent_str = "  ".repeat(indent);
        let text = self.elements_to_text(&item.elements);
        output.push_str(&format!("{}{}{}\n", indent_str, prefix, text));

        if !item.children.is_empty() {
            for child in &item.children {
                self.list_item_to_markdown(child, output, indent + 1, "- ");
            }
        }
    }

    /// Convert text elements to plain text.
    fn elements_to_text(&self, elements: &[TextElement]) -> String {
        elements.iter()
            .map(|e| e.text.clone())
            .collect::<Vec<_>>()
            .join("")
    }

    /// Convert document elements to plain text (for table cells).
    fn elements_to_text_from_doc_elements(&self, elements: &[DocumentElement]) -> String {
        elements.iter()
            .filter_map(|e| match e {
                DocumentElement::Paragraph { elements, .. } => Some(self.elements_to_text(elements)),
                DocumentElement::Heading { elements, .. } => Some(self.elements_to_text(elements)),
                DocumentElement::CodeBlock { elements, .. } => Some(self.elements_to_text(elements)),
                DocumentElement::Quote { elements, .. } => Some(self.elements_to_text(elements)),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Request to search documents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchDocumentsRequest {
    /// Search query.
    pub query: String,
    /// Document types to search.
    pub doc_types: Option<Vec<DocumentType>>,
    /// Search in owned documents only.
    pub owned_only: Option<bool>,
    /// Search in shared documents.
    pub include_shared: Option<bool>,
    /// Page token for pagination.
    pub page_token: Option<String>,
    /// Page size.
    pub page_size: Option<u32>,
}

/// Search result item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Document token.
    pub token: String,
    /// Document type.
    pub doc_type: DocumentType,
    /// Document title.
    pub title: String,
    /// Highlighting snippets.
    pub snippets: Vec<String>,
    /// Owner name.
    pub owner_name: Option<String>,
}

/// Search results response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchDocumentsResponse {
    /// Search results.
    pub results: Vec<SearchResult>,
    /// Next page token.
    pub next_page_token: Option<String>,
    /// Total count.
    pub total: u32,
}

impl DocumentClient {
    /// Search for documents.
    pub async fn search(&self, request: SearchDocumentsRequest) -> PlatformResult<SearchDocumentsResponse> {
        let token = self.adapter.ensure_token().await?;
        let client = reqwest::Client::new();

        let count = request.page_size.unwrap_or(20).to_string();
        let mut query_params: Vec<(&str, &str)> = vec![
            ("search_key", &request.query),
            ("count", &count),
        ];

        if let Some(page_token) = &request.page_token {
            query_params.push(("page_token", page_token));
        }

        if let Some(owned_only) = request.owned_only {
            let search_type = if owned_only { "1" } else { "2" };
            query_params.push(("docs_search_type", search_type));
        }

        let response = client
            .get("https://open.feishu.cn/open-apis/suite/docs-api/search/object")
            .header("Authorization", format!("Bearer {}", token))
            .query(&query_params)
            .send()
            .await
            .map_err(|e| PlatformError::Unknown(format!("search failed: {}", e)))?;

        #[derive(Deserialize)]
        struct SearchApiResponse {
            code: i32,
            msg: String,
            data: Option<SearchApiData>,
        }

        #[derive(Deserialize)]
        struct SearchApiData {
            docs: Option<Vec<SearchApiItem>>,
            page_token: Option<String>,
            total: Option<u32>,
        }

        #[derive(Deserialize, Clone)]
        struct SearchApiItem {
            doc: Option<SearchApiDoc>,
            snippets: Option<Vec<String>>,
        }

        #[derive(Deserialize, Clone)]
        struct SearchApiDoc {
            doc_id: Option<String>,
            doc_type: Option<String>,
            title: Option<String>,
            owner: Option<SearchApiOwner>,
        }

        #[derive(Deserialize, Clone)]
        struct SearchApiOwner {
            name: Option<String>,
        }

        if !response.status().is_success() {
            return Err(PlatformError::Unknown(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let resp: SearchApiResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to parse response: {}", e)))?;

        if resp.code != 0 {
            return Err(PlatformError::Unknown(format!(
                "API error: {}",
                resp.msg
            )));
        }

        let results: Vec<SearchResult> = resp.data
            .as_ref()
            .and_then(|d| d.docs.clone())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|item| {
                let doc = item.doc?;
                Some(SearchResult {
                    token: doc.doc_id?,
                    doc_type: match doc.doc_type.as_deref() {
                        Some("doc" | "docx") => DocumentType::Doc,
                        Some("sheet" | "xlsx") => DocumentType::Sheet,
                        Some("bitable") => DocumentType::Bitable,
                        _ => DocumentType::Doc,
                    },
                    title: doc.title.unwrap_or_default(),
                    snippets: item.snippets.unwrap_or_default(),
                    owner_name: doc.owner.and_then(|o| o.name),
                })
            })
            .collect();

        let next_page_token = resp.data.as_ref().and_then(|d| d.page_token.clone());
        let total = resp.data.as_ref().and_then(|d| d.total).unwrap_or(0);

        Ok(SearchDocumentsResponse {
            results,
            next_page_token,
            total,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_type_serialization() {
        let json = serde_json::to_string(&DocumentType::Doc).unwrap();
        assert_eq!(json, "\"doc\"");
    }

    #[test]
    fn test_text_element_serialization() {
        let element = TextElement {
            text: "Hello".to_string(),
            style: Some(TextStyle {
                bold: Some(true),
                italic: None,
                strikethrough: None,
                underline: None,
                text_color: None,
                background_color: None,
                link: None,
            }),
        };

        let json = serde_json::to_string(&element).unwrap();
        assert!(json.contains("Hello"));
        assert!(json.contains("bold"));
    }
}
