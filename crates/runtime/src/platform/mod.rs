//! Platform adapters module for unified multi-platform integration.
//!
//! This module provides a unified interface for integrating with various platforms
//! (Feishu, WeChat, Email, etc.) directly within the serve runtime, eliminating
//! the need for a separate Gateway service.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    ConversationRuntime                       │
//! └─────────────────────────────┬───────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    PlatformRuntime                          │
//! │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐  │
//! │  │  Feishu   │ │  WeChat   │ │   Email   │ │  Custom   │  │
//! │  │ Adapter   │ │ Adapter   │ │ Adapter   │ │ Adapter   │  │
//! │  └───────────┘ └───────────┘ └───────────┘ └───────────┘  │
//! └─────────────────────────────────────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Platform Services                        │
//! │     (Feishu API, WeChat API, SMTP, etc.)                   │
//! └─────────────────────────────────────────────────────────────┘
//! ```

pub mod adapter;
pub mod config;
pub mod feishu;
pub mod email;
pub mod wecom;
pub mod runtime;
pub mod types;

pub use adapter::{InboundMessage, OutboundMessage, Platform, PlatformAdapter, PlatformError, PlatformResult};
pub use config::PlatformConfig;
pub use runtime::PlatformRuntime;
pub use types::{SessionKey, PlatformSession};

pub use feishu::FeishuAdapter;
pub use email::EmailAdapter;
pub use wecom::WeComAdapter;
