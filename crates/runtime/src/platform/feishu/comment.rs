//! Feishu Comment Handler.
//!
//! Provides functionality for managing comments on Feishu documents.

use crate::platform::adapter::{PlatformError, PlatformResult};
use crate::platform::feishu::FeishuAdapter;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Comment thread status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommentStatus {
    /// Comment is open and awaiting response.
    Open,
    /// Comment has been resolved.
    Resolved,
    /// Comment was deleted.
    Deleted,
}

/// A comment on a Feishu document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeishuComment {
    /// Comment ID.
    pub id: String,
    /// Document token.
    pub doc_token: String,
    /// Comment content.
    pub content: String,
    /// Author information.
    pub author: CommentAuthor,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
    /// Comment status.
    pub status: CommentStatus,
    /// Parent comment ID (for replies).
    pub parent_id: Option<String>,
    /// Reply count.
    pub reply_count: u32,
    /// Position in document (if anchored).
    pub position: Option<CommentPosition>,
}

/// Author information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentAuthor {
    /// User open ID.
    pub open_id: String,
    /// User name.
    pub name: String,
    /// Avatar URL.
    pub avatar_url: Option<String>,
}

/// Position in document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentPosition {
    /// Start index.
    pub start_index: u32,
    /// End index.
    pub end_index: u32,
    /// Text being commented on.
    pub quoted_text: String,
}

/// Request to create a comment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCommentRequest {
    /// Document token.
    pub doc_token: String,
    /// Comment content.
    pub content: String,
    /// Parent comment ID for replies.
    pub parent_id: Option<String>,
    /// Position info (optional).
    pub position: Option<CommentPosition>,
}

/// Request to reply to a comment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyCommentRequest {
    /// Comment ID to reply to.
    pub comment_id: String,
    /// Reply content.
    pub content: String,
}

/// Request to update a comment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateCommentRequest {
    /// Comment ID.
    pub comment_id: String,
    /// New content.
    pub content: String,
}

/// Feishu Comment Handler.
pub struct CommentHandler {
    adapter: Arc<FeishuAdapter>,
    /// Cache of recently fetched comments.
    cache: Arc<RwLock<std::collections::HashMap<String, Vec<FeishuComment>>>>,
}

impl CommentHandler {
    /// Create a new comment handler.
    pub fn new(adapter: Arc<FeishuAdapter>) -> Self {
        Self {
            adapter,
            cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Get the base URL for Feishu document API.
    fn doc_api_base() -> &'static str {
        "https://open.feishu.cn/open-apis/docx/v1/documents"
    }

    /// List comments for a document.
    pub async fn list_comments(&self, doc_token: &str) -> PlatformResult<Vec<FeishuComment>> {
        let token = self.adapter.ensure_token().await?;
        let client = reqwest::Client::new();

        let response = client
            .get(&format!("{}/{}/comments", Self::doc_api_base(), doc_token))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to fetch comments: {}", e)))?;

        #[derive(Deserialize)]
        struct CommentsResponse {
            code: i32,
            msg: String,
            data: Option<CommentsData>,
        }

        #[derive(Deserialize)]
        struct CommentsData {
            items: Option<Vec<CommentItem>>,
        }

        #[derive(Deserialize)]
        struct CommentItem {
            comment_id: String,
            content: Option<ContentItem>,
            create_time: String,
            update_time: String,
            user_info: Option<UserInfo>,
            #[serde(rename = "is_deleted")]
            is_deleted: Option<bool>,
            parent_id: Option<String>,
            reply_count: Option<u32>,
            position: Option<PositionInfo>,
        }

        #[derive(Deserialize)]
        struct ContentItem {
            elements: Option<Vec<ElementItem>>,
        }

        #[derive(Deserialize)]
        struct ElementItem {
            text_run: Option<TextRun>,
        }

        #[derive(Deserialize)]
        struct TextRun {
            text: Option<String>,
        }

        #[derive(Deserialize)]
        struct UserInfo {
            user_id: Option<UserIdInfo>,
        }

        #[derive(Deserialize)]
        struct UserIdInfo {
            open_id: Option<String>,
            name: Option<String>,
            avatar_url: Option<String>,
        }

        #[derive(Deserialize)]
        struct PositionInfo {
            start_index: Option<u32>,
            end_index: Option<u32>,
            text: Option<String>,
        }

        if !response.status().is_success() {
            return Err(PlatformError::Unknown(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let resp: CommentsResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to parse response: {}", e)))?;

        if resp.code != 0 {
            return Err(PlatformError::Unknown(format!(
                "API error: {}",
                resp.msg
            )));
        }

        let items = resp.data.and_then(|d| d.items).unwrap_or_default();

        let comments: Vec<FeishuComment> = items
            .into_iter()
            .map(|item| {
                // Extract text from content elements
                let content = item
                    .content
                    .and_then(|c| c.elements)
                    .map(|elements| {
                        elements
                            .iter()
                            .filter_map(|e| e.text_run.as_ref().and_then(|t| t.text.clone()))
                            .collect::<String>()
                    })
                    .unwrap_or_default();

                // Parse author info
                let author = item
                    .user_info
                    .as_ref()
                    .and_then(|u| u.user_id.as_ref())
                    .map(|uid| CommentAuthor {
                        open_id: uid.open_id.clone().unwrap_or_default(),
                        name: uid.name.clone().unwrap_or_default(),
                        avatar_url: uid.avatar_url.clone(),
                    })
                    .unwrap_or_else(|| CommentAuthor {
                        open_id: String::new(),
                        name: String::new(),
                        avatar_url: None,
                    });

                // Parse position
                let position = item.position.map(|p| CommentPosition {
                    start_index: p.start_index.unwrap_or(0),
                    end_index: p.end_index.unwrap_or(0),
                    quoted_text: p.text.unwrap_or_default(),
                });

                FeishuComment {
                    id: item.comment_id,
                    doc_token: doc_token.to_string(),
                    content,
                    author,
                    created_at: DateTime::parse_from_rfc3339(&item.create_time)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    updated_at: DateTime::parse_from_rfc3339(&item.update_time)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    status: if item.is_deleted.unwrap_or(false) {
                        CommentStatus::Deleted
                    } else {
                        CommentStatus::Open
                    },
                    parent_id: item.parent_id,
                    reply_count: item.reply_count.unwrap_or(0),
                    position,
                }
            })
            .collect();

        // Cache the results
        let mut cache = self.cache.write().await;
        cache.insert(doc_token.to_string(), comments.clone());

        Ok(comments)
    }

    /// Get a single comment by ID.
    pub async fn get_comment(&self, doc_token: &str, comment_id: &str) -> PlatformResult<Option<FeishuComment>> {
        let comments = self.list_comments(doc_token).await?;
        Ok(comments.into_iter().find(|c| c.id == comment_id))
    }

    /// Create a new comment.
    pub async fn create_comment(&self, request: CreateCommentRequest) -> PlatformResult<FeishuComment> {
        let token = self.adapter.ensure_token().await?;
        let client = reqwest::Client::new();

        #[derive(Serialize)]
        struct CreateCommentRequestBody {
            content: ContentWrapper,
            position: Option<PositionWrapper>,
        }

        #[derive(Serialize)]
        struct ContentWrapper {
            elements: Vec<ElementWrapper>,
        }

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct ElementWrapper {
            text_run: TextRunWrapper,
        }

        #[derive(Serialize)]
        struct TextRunWrapper {
            text: String,
        }

        #[derive(Serialize)]
        struct PositionWrapper {
            start_index: u32,
            end_index: u32,
            #[serde(rename = "document_text_object_id")]
            text_object_id: Option<String>,
        }

        let body = CreateCommentRequestBody {
            content: ContentWrapper {
                elements: vec![ElementWrapper {
                    text_run: TextRunWrapper {
                        text: request.content.clone(),
                    },
                }],
            },
            position: request.position.map(|p| PositionWrapper {
                start_index: p.start_index,
                end_index: p.end_index,
                text_object_id: None,
            }),
        };

        let url = if let Some(ref parent_id) = request.parent_id {
            format!(
                "{}/{}/comments/{}/replies",
                Self::doc_api_base(),
                request.doc_token,
                parent_id
            )
        } else {
            format!("{}/{}/comments", Self::doc_api_base(), request.doc_token)
        };

        let response = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&body)
            .send()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to create comment: {}", e)))?;

        #[derive(Deserialize)]
        struct CreateResponse {
            code: i32,
            msg: String,
            data: Option<CreateData>,
        }

        #[derive(Deserialize)]
        struct CreateData {
            comment: Option<CommentItem>,
        }

        #[derive(Deserialize)]
        struct CommentItem {
            comment_id: String,
            content: Option<ContentItem>,
            create_time: String,
            update_time: String,
            user_info: Option<UserInfo>,
            #[serde(rename = "is_deleted")]
            is_deleted: Option<bool>,
            parent_id: Option<String>,
            reply_count: Option<u32>,
            position: Option<PositionInfo>,
        }

        #[derive(Deserialize)]
        struct ContentItem {
            elements: Option<Vec<ElementItem>>,
        }

        #[derive(Deserialize)]
        struct ElementItem {
            text_run: Option<TextRun>,
        }

        #[derive(Deserialize)]
        struct TextRun {
            text: Option<String>,
        }

        #[derive(Deserialize)]
        struct UserInfo {
            user_id: Option<UserIdInfo>,
        }

        #[derive(Deserialize)]
        struct UserIdInfo {
            open_id: Option<String>,
            name: Option<String>,
            avatar_url: Option<String>,
        }

        #[derive(Deserialize)]
        struct PositionInfo {
            start_index: Option<u32>,
            end_index: Option<u32>,
            text: Option<String>,
        }

        if !response.status().is_success() {
            return Err(PlatformError::Unknown(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let resp: CreateResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to parse response: {}", e)))?;

        if resp.code != 0 {
            return Err(PlatformError::Unknown(format!(
                "API error: {}",
                resp.msg
            )));
        }

        let item = resp.data.and_then(|d| d.comment)
            .ok_or_else(|| PlatformError::Unknown("no comment in response".to_string()))?;

        // Parse the comment
        let content = item
            .content
            .and_then(|c| c.elements)
            .map(|elements| {
                elements
                    .iter()
                    .filter_map(|e| e.text_run.as_ref().and_then(|t| t.text.clone()))
                    .collect::<String>()
            })
            .unwrap_or_default();

        let author = item
            .user_info
            .as_ref()
            .and_then(|u| u.user_id.as_ref())
            .map(|uid| CommentAuthor {
                open_id: uid.open_id.clone().unwrap_or_default(),
                name: uid.name.clone().unwrap_or_default(),
                avatar_url: uid.avatar_url.clone(),
            })
            .unwrap_or_else(|| CommentAuthor {
                open_id: String::new(),
                name: String::new(),
                avatar_url: None,
            });

        let position = item.position.map(|p| CommentPosition {
            start_index: p.start_index.unwrap_or(0),
            end_index: p.end_index.unwrap_or(0),
            quoted_text: p.text.unwrap_or_default(),
        });

        let comment = FeishuComment {
            id: item.comment_id,
            doc_token: request.doc_token.clone(),
            content,
            author,
            created_at: DateTime::parse_from_rfc3339(&item.create_time)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            updated_at: DateTime::parse_from_rfc3339(&item.update_time)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            status: if item.is_deleted.unwrap_or(false) {
                CommentStatus::Deleted
            } else {
                CommentStatus::Open
            },
            parent_id: item.parent_id,
            reply_count: item.reply_count.unwrap_or(0),
            position,
        };

        // Invalidate cache
        let mut cache = self.cache.write().await;
        cache.remove(&request.doc_token);

        Ok(comment)
    }

    /// Reply to a comment.
    pub async fn reply_comment(&self, request: ReplyCommentRequest) -> PlatformResult<FeishuComment> {
        // Get the parent comment to find the doc_token
        let mut cache = self.cache.write().await;

        for (doc_token, comments) in cache.iter() {
            if let Some(parent) = comments.iter().find(|c| c.id == request.comment_id) {
                drop(cache);
                return self.create_comment(CreateCommentRequest {
                    doc_token: doc_token.clone(),
                    content: request.content,
                    parent_id: Some(request.comment_id),
                    position: None,
                }).await;
            }
        }
        drop(cache);

        Err(PlatformError::Unknown(
            "parent comment not found".to_string()
        ))
    }

    /// Update a comment.
    pub async fn update_comment(&self, request: UpdateCommentRequest) -> PlatformResult<FeishuComment> {
        let token = self.adapter.ensure_token().await?;
        let client = reqwest::Client::new();

        #[derive(Serialize)]
        struct UpdateRequestBody {
            content: ContentWrapper,
        }

        #[derive(Serialize)]
        struct ContentWrapper {
            elements: Vec<ElementWrapper>,
        }

        #[derive(Serialize)]
        struct ElementWrapper {
            text_run: TextRunWrapper,
        }

        #[derive(Serialize)]
        struct TextRunWrapper {
            text: String,
        }

        let body = UpdateRequestBody {
            content: ContentWrapper {
                elements: vec![ElementWrapper {
                    text_run: TextRunWrapper {
                        text: request.content.clone(),
                    },
                }],
            },
        };

        // Need to find the document token first
        let mut cache = self.cache.write().await;
        let (doc_token, _) = cache.iter()
            .find(|(_, comments)| comments.iter().any(|c| c.id == request.comment_id))
            .ok_or_else(|| PlatformError::Unknown("comment not found".to_string()))?
            .clone();
        drop(cache);

        let url = format!(
            "{}/{}/comments/{}",
            Self::doc_api_base(),
            doc_token,
            request.comment_id
        );

        let response = client
            .patch(&url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&body)
            .send()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to update comment: {}", e)))?;

        #[derive(Deserialize)]
        struct UpdateResponse {
            code: i32,
            msg: String,
        }

        if !response.status().is_success() {
            return Err(PlatformError::Unknown(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let resp: UpdateResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to parse response: {}", e)))?;

        if resp.code != 0 {
            return Err(PlatformError::Unknown(format!(
                "API error: {}",
                resp.msg
            )));
        }

        // Invalidate cache
        let mut cache = self.cache.write().await;
        cache.remove(&doc_token);

        // Return updated comment info (API doesn't return full comment on update)
        Ok(FeishuComment {
            id: request.comment_id,
            doc_token,
            content: request.content,
            author: CommentAuthor {
                open_id: String::new(),
                name: String::new(),
                avatar_url: None,
            },
            created_at: Utc::now(),
            updated_at: Utc::now(),
            status: CommentStatus::Open,
            parent_id: None,
            reply_count: 0,
            position: None,
        })
    }

    /// Resolve a comment (mark as resolved).
    pub async fn resolve_comment(&self, doc_token: &str, comment_id: &str) -> PlatformResult<()> {
        let token = self.adapter.ensure_token().await?;
        let client = reqwest::Client::new();

        let url = format!(
            "{}/{}/comments/{}/resolve",
            Self::doc_api_base(),
            doc_token,
            comment_id
        );

        let response = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to resolve comment: {}", e)))?;

        #[derive(Deserialize)]
        struct ResolveResponse {
            code: i32,
            msg: String,
        }

        if !response.status().is_success() {
            return Err(PlatformError::Unknown(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let resp: ResolveResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to parse response: {}", e)))?;

        if resp.code != 0 {
            return Err(PlatformError::Unknown(format!(
                "API error: {}",
                resp.msg
            )));
        }

        // Invalidate cache
        let mut cache = self.cache.write().await;
        cache.remove(doc_token);

        Ok(())
    }

    /// Delete a comment.
    pub async fn delete_comment(&self, doc_token: &str, comment_id: &str) -> PlatformResult<()> {
        let token = self.adapter.ensure_token().await?;
        let client = reqwest::Client::new();

        let url = format!(
            "{}/{}/comments/{}",
            Self::doc_api_base(),
            doc_token,
            comment_id
        );

        let response = client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to delete comment: {}", e)))?;

        #[derive(Deserialize)]
        struct DeleteResponse {
            code: i32,
            msg: String,
        }

        if !response.status().is_success() {
            return Err(PlatformError::Unknown(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let resp: DeleteResponse = response
            .json()
            .await
            .map_err(|e| PlatformError::Unknown(format!("failed to parse response: {}", e)))?;

        if resp.code != 0 {
            return Err(PlatformError::Unknown(format!(
                "API error: {}",
                resp.msg
            )));
        }

        // Invalidate cache
        let mut cache = self.cache.write().await;
        cache.remove(doc_token);

        Ok(())
    }

    /// Clear the comment cache.
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }
}

/// Filter for listing comments.
#[derive(Debug, Clone, Default)]
pub struct CommentFilter {
    /// Filter by status.
    pub status: Option<CommentStatus>,
    /// Filter by author.
    pub author_open_id: Option<String>,
    /// Include replies.
    pub include_replies: bool,
}

impl CommentHandler {
    /// List comments with filters.
    pub async fn list_comments_filtered(
        &self,
        doc_token: &str,
        filter: &CommentFilter,
    ) -> PlatformResult<Vec<FeishuComment>> {
        let mut comments = self.list_comments(doc_token).await?;

        // Apply filters
        if !filter.include_replies {
            comments.retain(|c| c.parent_id.is_none());
        }

        if let Some(status) = &filter.status {
            comments.retain(|c| c.status == *status);
        }

        if let Some(author_id) = &filter.author_open_id {
            comments.retain(|c| c.author.open_id == *author_id);
        }

        Ok(comments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comment_status_serialization() {
        let json = serde_json::to_string(&CommentStatus::Open).unwrap();
        assert_eq!(json, "\"open\"");

        let json = serde_json::to_string(&CommentStatus::Resolved).unwrap();
        assert_eq!(json, "\"resolved\"");
    }

    #[test]
    fn test_create_comment_request_serialization() {
        let request = CreateCommentRequest {
            doc_token: "test_doc".to_string(),
            content: "Test comment".to_string(),
            parent_id: None,
            position: Some(CommentPosition {
                start_index: 0,
                end_index: 10,
                quoted_text: "quoted".to_string(),
            }),
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("test_doc"));
        assert!(json.contains("Test comment"));
    }
}
