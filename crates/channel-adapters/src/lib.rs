//! Gateway-owned channel adapter host.
//!
//! Platform SDK integration is a gateway/channel concern, not an AI harness
//! runtime concern. This crate carries the platform adapter runtime used by
//! gateway while the historical runtime module is migrated out.

pub mod mirror;

pub mod config {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
    #[serde(rename_all = "lowercase")]
    pub enum SessionResetPolicy {
        Daily,
        Idle,
        Both,
        Always,
        #[default]
        None,
    }
}

pub mod cowd_dirs;

pub mod platform;
