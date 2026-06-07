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
pub mod dedup;
pub mod email;
pub mod feishu;
pub mod runtime;
pub mod types;
pub mod wechat_ilink;
pub mod wecom;

pub use adapter::{ChatInfo, MessageType, PlatformEvent, SendResult};
pub use adapter::{
    InboundMessage, OutboundDispatch, OutboundMessage, OutboundPayloadKind, Platform,
    PlatformAdapter, PlatformError, PlatformResult,
};
pub use config::PlatformConfig;
pub use runtime::PlatformRuntime;
pub use types::{PlatformSession, SessionKey};

pub use email::EmailAdapter;
pub use feishu::FeishuAdapter;
pub use wechat_ilink::{WeChatLinkAdapter, WeChatLinkConfig};
