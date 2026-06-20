//! Email platform adapter.
//!
//! Provides SMTP email sending and IMAP email receiving with attachment handling.

use crate::platform::adapter::{
    InboundMessage, MessageType, OutboundMessage, Platform, PlatformAdapter, PlatformError,
    PlatformResult,
};
use crate::platform::types::{SendResult, SessionKey};
use async_trait::async_trait;
use chrono::Utc;
use lettre::Transport;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Email adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailConfig {
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_username: String,
    pub smtp_password: String,
    pub use_tls: bool,
    pub imap_host: Option<String>,
    pub imap_port: Option<u16>,
    pub imap_username: Option<String>,
    pub imap_password: Option<String>,
    pub from_address: String,
    pub polling_interval_secs: u64,
}

impl Default for EmailConfig {
    fn default() -> Self {
        Self {
            smtp_host: String::new(),
            smtp_port: 587,
            smtp_username: String::new(),
            smtp_password: String::new(),
            use_tls: true,
            imap_host: None,
            imap_port: None,
            imap_username: None,
            imap_password: None,
            from_address: String::new(),
            polling_interval_secs: 60,
        }
    }
}

impl EmailConfig {
    pub fn is_smtp_configured(&self) -> bool {
        !self.smtp_host.is_empty() && !self.from_address.is_empty()
    }

    pub fn is_imap_configured(&self) -> bool {
        self.imap_host.as_ref().map_or(false, |h| !h.is_empty()) && self.imap_username.is_some()
    }

    pub fn new(
        smtp_host: impl Into<String>,
        smtp_username: impl Into<String>,
        smtp_password: impl Into<String>,
        from_address: impl Into<String>,
    ) -> Self {
        Self {
            smtp_host: smtp_host.into(),
            smtp_port: 587,
            smtp_username: smtp_username.into(),
            smtp_password: smtp_password.into(),
            use_tls: true,
            imap_host: None,
            imap_port: None,
            imap_username: None,
            imap_password: None,
            from_address: from_address.into(),
            polling_interval_secs: 60,
        }
    }

    pub fn with_imap(
        mut self,
        host: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.imap_host = Some(host.into());
        self.imap_port = Some(993);
        self.imap_username = Some(username.into());
        self.imap_password = Some(password.into());
        self
    }
}

/// Email platform adapter with real SMTP sending and IMAP receiving.
pub struct EmailAdapter {
    config: EmailConfig,
    connected: Arc<AtomicBool>,
    last_poll: Arc<RwLock<Option<Instant>>>,
}

impl EmailAdapter {
    pub fn new(config: EmailConfig) -> Self {
        Self {
            config,
            connected: Arc::new(AtomicBool::new(false)),
            last_poll: Arc::new(RwLock::new(None)),
        }
    }

    pub fn is_valid_email(email: &str) -> bool {
        email.contains('@') && email.contains('.') && email.len() > 5
    }

    /// Send an email via SMTP.
    async fn send_email(&self, msg: &OutboundMessage) -> PlatformResult<()> {
        if !self.config.is_smtp_configured() {
            tracing::warn!("SMTP not configured, skipping email send");
            return Ok(());
        }

        let to_address = &msg.session_key.user_id;
        let subject = msg
            .metadata
            .get("subject")
            .and_then(|v| v.as_str())
            .unwrap_or("Message from AI");

        let from = self
            .config
            .from_address
            .parse::<lettre::message::Mailbox>()
            .map_err(|e| PlatformError::SendFailed(format!("invalid from address: {}", e)))?;
        let to = to_address
            .parse::<lettre::message::Mailbox>()
            .map_err(|e| PlatformError::SendFailed(format!("invalid to address: {}", e)))?;

        let email = lettre::Message::builder()
            .from(from)
            .to(to)
            .subject(subject)
            .multipart(
                lettre::message::MultiPart::alternative().singlepart(
                    lettre::message::SinglePart::builder()
                        .header(lettre::message::header::ContentType::TEXT_PLAIN)
                        .body(msg.text.clone()),
                ),
            )
            .map_err(|e| PlatformError::SendFailed(format!("failed to build email: {}", e)))?;

        let creds = lettre::transport::smtp::authentication::Credentials::new(
            self.config.smtp_username.clone(),
            self.config.smtp_password.clone(),
        );

        let mailer = if self.config.use_tls {
            lettre::SmtpTransport::relay(&self.config.smtp_host)
                .map_err(|e| PlatformError::SendFailed(format!("SMTP relay error: {}", e)))?
                .port(self.config.smtp_port)
                .credentials(creds)
                .build()
        } else {
            lettre::SmtpTransport::builder_dangerous(&self.config.smtp_host)
                .port(self.config.smtp_port)
                .credentials(creds)
                .build()
        };

        // lettre send is blocking, so we run it in a spawn_blocking context
        let result: Result<
            lettre::transport::smtp::response::Response,
            lettre::transport::smtp::Error,
        > = tokio::task::spawn_blocking(move || mailer.send(&email))
            .await
            .map_err(|e| PlatformError::SendFailed(format!("send task error: {}", e)))?;

        match result {
            Ok(_) => {
                tracing::info!(to = %to_address, "email sent successfully via SMTP");
                Ok(())
            }
            Err(e) => Err(PlatformError::SendFailed(format!(
                "SMTP send failed: {}",
                e
            ))),
        }
    }

    /// Receive unread emails via IMAP.
    async fn receive_emails(&self) -> PlatformResult<Vec<InboundMessage>> {
        if !self.config.is_imap_configured() {
            return Ok(Vec::new());
        }

        let imap_host = self
            .config
            .imap_host
            .clone()
            .ok_or_else(|| PlatformError::ConfigError("missing imap_host".into()))?;
        let imap_port = self.config.imap_port.unwrap_or(993);
        let imap_user = self
            .config
            .imap_username
            .clone()
            .ok_or_else(|| PlatformError::ConfigError("missing imap_username".into()))?;
        let imap_pass = self
            .config
            .imap_password
            .clone()
            .ok_or_else(|| PlatformError::ConfigError("missing imap_password".into()))?;

        // IMAP is blocking, run in spawn_blocking
        let result = tokio::task::spawn_blocking(move || -> PlatformResult<Vec<InboundMessage>> {
            let client = imap::ClientBuilder::new(&imap_host, imap_port)
                .connect()
                .map_err(|e| PlatformError::ReceiveFailed(format!("IMAP connect error: {}", e)))?;

            let mut session = client.login(&imap_user, &imap_pass).map_err(|e| {
                PlatformError::AuthenticationFailed(format!("IMAP login error: {}", e.0))
            })?;

            session.select("INBOX").map_err(|e| {
                PlatformError::ReceiveFailed(format!("IMAP select INBOX error: {}", e))
            })?;

            // Search for unseen messages
            let uids = session
                .uid_search("UNSEEN")
                .map_err(|e| PlatformError::ReceiveFailed(format!("IMAP search error: {}", e)))?;

            let mut messages = Vec::new();

            for uid in uids.iter().take(50) {
                // Limit to 50 messages per poll
                let msg_data = session.uid_fetch(&uid.to_string(), "RFC822").map_err(|e| {
                    PlatformError::ReceiveFailed(format!("IMAP fetch error: {}", e))
                })?;

                if let Some(msg) = msg_data.iter().next() {
                    if let Some(body) = msg.body() {
                        if let Some(parsed) = mail_parser::MessageParser::default().parse(body) {
                            let subject = parsed.subject().unwrap_or("(No Subject)");
                            let from = parsed
                                .from()
                                .and_then(|addr| addr.first())
                                .and_then(|a| a.address.as_deref().or(a.name.as_deref()))
                                .unwrap_or("unknown")
                                .to_string();
                            let text = parsed
                                .body_text(0)
                                .map(|t| t.to_string())
                                .unwrap_or_default();

                            // Extract attachment count for metadata
                            let attachment_count = parsed.attachment_count();

                            let sender = from.clone();
                            let session_key = SessionKey::new(Platform::Email.name(), &sender);

                            messages.push(InboundMessage {
                                platform: Platform::Email,
                                session_key,
                                text: format!("Subject: {}\n\n{}", subject, text),
                                sender_name: Some(from),
                                timestamp: Utc::now(),
                                metadata: serde_json::json!({
                                    "subject": subject,
                                    "uid": uid,
                                    "attachments": attachment_count,
                                }),
                                message_type: MessageType::Text,
                                message_id: None,
                                reply_to_message_id: None,
                                media_urls: vec![],
                                media_types: vec![],
                            });
                        }
                    }
                }
            }

            session.logout().ok();
            Ok(messages)
        })
        .await
        .map_err(|e| PlatformError::ReceiveFailed(format!("IMAP task error: {}", e)))?;

        result
    }

    /// Check if enough time has elapsed since the last poll.
    async fn should_poll(&self) -> bool {
        let last = self.last_poll.read().await;
        match *last {
            Some(instant) => {
                let elapsed = instant.elapsed().as_secs();
                elapsed >= self.config.polling_interval_secs
            }
            None => true,
        }
    }

    /// Update the last poll timestamp.
    async fn update_poll_time(&self) {
        *self.last_poll.write().await = Some(Instant::now());
    }
}

pub fn create_email_adapter(settings: &serde_json::Value) -> PlatformResult<EmailAdapter> {
    let config = serde_json::from_value(settings.clone())
        .map_err(|e| PlatformError::ConfigError(format!("invalid email config: {}", e)))?;
    Ok(EmailAdapter::new(config))
}

#[async_trait]
impl PlatformAdapter for EmailAdapter {
    fn platform(&self) -> Platform {
        Platform::Email
    }
    fn platform_name(&self) -> &str {
        "email"
    }

    async fn connect(&mut self) -> PlatformResult<()> {
        if self.config.is_smtp_configured() {
            tracing::info!(host = %self.config.smtp_host, port = self.config.smtp_port, "email adapter: SMTP configured");
        }
        if self.config.is_imap_configured() {
            tracing::info!(
                host = %self.config.imap_host.as_deref().unwrap_or_default(),
                "email adapter: IMAP configured, polling every {}s",
                self.config.polling_interval_secs
            );
        }
        self.connected.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn disconnect(&mut self) -> PlatformResult<()> {
        self.connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    async fn receive(&mut self) -> PlatformResult<Option<InboundMessage>> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(None);
        }

        // Respect polling interval
        if !self.should_poll().await {
            return Ok(None);
        }

        self.update_poll_time().await;
        let messages = self.receive_emails().await?;
        Ok(messages.into_iter().next())
    }

    async fn send(&self, msg: &OutboundMessage) -> PlatformResult<SendResult> {
        self.send_email(msg).await?;
        Ok(SendResult::success(None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_email_config() {
        let config = EmailConfig::new("smtp.example.com", "user", "pass", "from@example.com");
        assert_eq!(config.smtp_host, "smtp.example.com");
        assert!(config.is_smtp_configured());
    }

    #[test]
    fn test_email_config_with_imap() {
        let config = EmailConfig::new("smtp.example.com", "user", "pass", "from@example.com")
            .with_imap("imap.example.com", "user", "pass");
        assert!(config.is_imap_configured());
        assert_eq!(config.imap_port, Some(993));
    }

    #[test]
    fn test_email_validation() {
        assert!(EmailAdapter::is_valid_email("user@example.com"));
        assert!(!EmailAdapter::is_valid_email("invalid"));
        assert!(!EmailAdapter::is_valid_email("a@b"));
    }

    #[tokio::test]
    async fn test_email_polling_throttle() {
        let config = EmailConfig::new("smtp.example.com", "user", "pass", "from@example.com");
        let adapter = EmailAdapter::new(config);

        // First poll should be allowed
        assert!(adapter.should_poll().await);

        // Update poll time, then second poll should be throttled (interval is 60s)
        adapter.update_poll_time().await;
        assert!(!adapter.should_poll().await);
    }
}
