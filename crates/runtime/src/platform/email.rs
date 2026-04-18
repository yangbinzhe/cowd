//! Email platform adapter.

use crate::platform::adapter::{InboundMessage, OutboundMessage, Platform, PlatformAdapter, PlatformError, PlatformResult};
use crate::platform::types::SessionKey;
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Email adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailConfig {
    /// SMTP server hostname.
    pub smtp_host: String,
    /// SMTP port.
    pub smtp_port: u16,
    /// SMTP username.
    pub smtp_username: String,
    /// SMTP password.
    pub smtp_password: String,
    /// Use TLS/SSL.
    pub use_tls: bool,
    /// IMAP server hostname (for receiving).
    pub imap_host: Option<String>,
    /// IMAP port.
    pub imap_port: Option<u16>,
    /// IMAP username.
    pub imap_username: Option<String>,
    /// IMAP password.
    pub imap_password: Option<String>,
    /// Default sender address.
    pub from_address: String,
    /// Polling interval in seconds.
    pub polling_interval_secs: u64,
}

impl EmailConfig {
    /// Create a new Email config with SMTP settings.
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

    /// Enable IMAP for receiving emails.
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
    /// Create a new Email adapter.
    pub fn new(config: EmailConfig) -> Self {
        Self {
            config,
            connected: Arc::new(RwLock::new(false)),
        }
    }

    /// Send an email via SMTP.
    async fn send_email(&self, msg: &OutboundMessage) -> PlatformResult<()> {
        // Extract email address from session key
        let to_address = &msg.session_key.user_id;

        // For now, we'll use lettre for SMTP
        // This is a placeholder implementation
        tracing::debug!(to = %to_address, "would send email");

        // In a real implementation:
        // 1. Create an email message using lettre
        // 2. Connect to SMTP server
        // 3. Send the message

        Ok(())
    }

    /// Receive emails via IMAP.
    async fn receive_emails(&self) -> PlatformResult<Vec<InboundMessage>> {
        let connected = self.connected.read().await;
        if !*connected {
            return Ok(Vec::new());
        }

        // In a real implementation:
        // 1. Connect to IMAP server
        // 2. Search for new messages
        // 3. Fetch and parse messages
        // 4. Return as InboundMessage vector

        Ok(Vec::new())
    }
}

/// Create an email adapter from JSON settings.
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
        // In a real implementation, verify SMTP/IMAP connection
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
        assert_eq!(config.from_address, "from@example.com");
    }

    #[test]
    fn test_email_config_with_imap() {
        let config = EmailConfig::new("smtp.example.com", "user", "pass", "from@example.com")
            .with_imap("imap.example.com", "user", "pass");
        assert!(config.imap_host.is_some());
        assert_eq!(config.imap_port, Some(993));
    }
}
