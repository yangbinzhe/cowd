//! Email platform adapter.
//!
//! Provides a framework for SMTP email sending and IMAP receiving.

use crate::platform::adapter::{InboundMessage, OutboundMessage, Platform, PlatformAdapter, PlatformError, PlatformResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
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
        self.imap_host.is_some() && self.imap_username.is_some()
    }

    pub fn new(smtp_host: impl Into<String>, smtp_username: impl Into<String>, smtp_password: impl Into<String>, from_address: impl Into<String>) -> Self {
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

    pub fn with_imap(mut self, host: impl Into<String>, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.imap_host = Some(host.into());
        self.imap_port = Some(993);
        self.imap_username = Some(username.into());
        self.imap_password = Some(password.into());
        self
    }
}

/// Email platform adapter.
pub struct EmailAdapter {
    config: EmailConfig,
    connected: Arc<RwLock<bool>>,
}

impl EmailAdapter {
    pub fn new(config: EmailConfig) -> Self {
        Self {
            config,
            connected: Arc::new(RwLock::new(false)),
        }
    }

    pub fn is_valid_email(email: &str) -> bool {
        email.contains('@') && email.contains('.') && email.len() > 5
    }

    async fn send_email(&self, msg: &OutboundMessage) -> PlatformResult<()> {
        if !self.config.is_smtp_configured() {
            tracing::warn!("SMTP not configured, skipping email send");
            return Ok(());
        }

        tracing::info!(
            to = %msg.session_key.user_id,
            from = %self.config.from_address,
            subject = %msg.metadata.get("subject").and_then(|v| v.as_str()).unwrap_or("Message from AI"),
            body_len = msg.text.len(),
            "email would be sent via {}:{}", 
            self.config.smtp_host,
            self.config.smtp_port
        );

        Ok(())
    }

    async fn receive_emails(&self) -> PlatformResult<Vec<InboundMessage>> {
        if !self.config.is_imap_configured() {
            return Ok(Vec::new());
        }
        tracing::debug!("IMAP receive not yet implemented");
        Ok(Vec::new())
    }
}

pub fn create_email_adapter(settings: &serde_json::Value) -> PlatformResult<EmailAdapter> {
    let config = serde_json::from_value(settings.clone())
        .map_err(|e| PlatformError::ConfigError(format!("invalid email config: {}", e)))?;
    Ok(EmailAdapter::new(config))
}

#[async_trait]
impl PlatformAdapter for EmailAdapter {
    fn platform(&self) -> Platform { Platform::Email }
    fn platform_name(&self) -> &str { "email" }

    async fn connect(&mut self) -> PlatformResult<()> {
        if self.config.is_smtp_configured() {
            tracing::info!(host = %self.config.smtp_host, port = self.config.smtp_port, "email adapter: SMTP configured");
        }
        *self.connected.write().await = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> PlatformResult<()> {
        *self.connected.write().await = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        *self.connected.blocking_read()
    }

    async fn receive(&mut self) -> PlatformResult<Option<InboundMessage>> {
        if !*self.connected.read().await {
            return Ok(None);
        }
        let messages = self.receive_emails().await?;
        Ok(messages.into_iter().next())
    }

    async fn send(&self, msg: &OutboundMessage) -> PlatformResult<()> {
        self.send_email(msg).await
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
    fn test_email_validation() {
        assert!(EmailAdapter::is_valid_email("user@example.com"));
        assert!(!EmailAdapter::is_valid_email("invalid"));
    }
}
