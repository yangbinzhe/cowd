//! DM Pairing Security System.
//!
//! Provides secure device pairing via 8-character codes with rate limiting,
//! attempt counting, and lockout. Inspired by hermes pairing.py (OWASP + NIST SP 800-63-4).

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tokio::sync::RwLock;

/// 32-character unambiguous alphabet (excludes 0, O, 1, l, I).
const ALPHABET: &[u8] = b"abcdefghjkmnpqrstuvwxyz23456789";

/// Code length in characters.
const CODE_LENGTH: usize = 8;

/// Code expiry time in seconds (1 hour).
const EXPIRY_SECS: i64 = 3600;

/// Rate limit interval in seconds (10 minutes).
const RATE_LIMIT_SECS: i64 = 600;

/// Maximum failed verification attempts before lockout.
const MAX_FAILURES: usize = 5;

/// Lockout duration in seconds (1 hour).
const LOCKOUT_SECS: i64 = 3600;

/// A pending pairing code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingCode {
    /// The 8-character pairing code.
    pub code: String,
    /// The channel requesting pairing.
    pub channel: String,
    /// The requester session reference.
    pub session_ref: String,
    /// When the code was created.
    pub created_at: DateTime<Utc>,
    /// Number of failed verification attempts.
    pub attempts: usize,
}

/// Rate limit entry for code generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RateLimitEntry {
    /// Last code generation time.
    last_generated: DateTime<Utc>,
}

/// Lockout entry for failed verifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LockoutEntry {
    /// Lockout start time.
    locked_at: DateTime<Utc>,
    /// Number of failures that triggered lockout.
    failure_count: usize,
}

/// DM Pairing Manager.
///
/// Manages secure pairing codes with:
/// - 8-character codes from an unambiguous alphabet
/// - Rate limiting (1 code per 10 minutes per session)
/// - Maximum 5 verification attempts per code
/// - 1-hour lockout after exceeding max failures
/// - File persistence with 0o600 permissions
pub struct PairingManager {
    /// Pending pairing codes, keyed by code string.
    pending_codes: RwLock<HashMap<String, PendingCode>>,
    /// Rate limits per session key.
    rate_limits: RwLock<HashMap<String, RateLimitEntry>>,
    /// Lockout entries per session key.
    lockouts: RwLock<HashMap<String, LockoutEntry>>,
    /// Storage directory for persistence.
    storage_path: PathBuf,
}

impl PairingManager {
    /// Create a new PairingManager with the given storage path.
    pub fn new(storage_path: PathBuf) -> Self {
        let manager = Self {
            pending_codes: RwLock::new(HashMap::new()),
            rate_limits: RwLock::new(HashMap::new()),
            lockouts: RwLock::new(HashMap::new()),
            storage_path,
        };

        // Ensure storage directory exists
        if let Err(e) = fs::create_dir_all(&manager.storage_path) {
            tracing::warn!(path = %manager.storage_path.display(), error = %e, "failed to create pairing storage dir");
        }

        manager
    }

    /// Generate a new pairing code for a session.
    ///
    /// Returns an error if:
    /// - The session is currently locked out
    /// - The rate limit has not expired (1 code per 10 minutes)
    pub async fn generate_code(
        &self,
        channel: &str,
        session_ref: &str,
    ) -> Result<String, PairingError> {
        let channel = normalize_channel_ref(channel)?;
        let session_ref = normalize_session_ref(session_ref)?;
        let key_str = format!("{channel}:{session_ref}");

        // Check lockout
        {
            let lockouts = self.lockouts.read().await;
            if let Some(lockout) = lockouts.get(&key_str) {
                if lockout.locked_at + ChronoDuration::seconds(LOCKOUT_SECS) > Utc::now() {
                    let remaining = (lockout.locked_at + ChronoDuration::seconds(LOCKOUT_SECS)
                        - Utc::now())
                    .num_seconds();
                    return Err(PairingError::LockedOut {
                        remaining_secs: remaining.max(0),
                    });
                }
            }
        }

        // Check rate limit
        {
            let rate_limits = self.rate_limits.read().await;
            if let Some(entry) = rate_limits.get(&key_str) {
                if entry.last_generated + ChronoDuration::seconds(RATE_LIMIT_SECS) > Utc::now() {
                    let remaining = (entry.last_generated
                        + ChronoDuration::seconds(RATE_LIMIT_SECS)
                        - Utc::now())
                    .num_seconds();
                    return Err(PairingError::RateLimited {
                        remaining_secs: remaining.max(0),
                    });
                }
            }
        }

        // Generate 8-character code
        let code = Self::generate_random_code();

        let pending = PendingCode {
            code: code.clone(),
            channel: channel.clone(),
            session_ref: session_ref.clone(),
            created_at: Utc::now(),
            attempts: 0,
        };

        // Store in memory
        self.pending_codes
            .write()
            .await
            .insert(code.clone(), pending.clone());

        // Update rate limit
        self.rate_limits.write().await.insert(
            key_str.to_string(),
            RateLimitEntry {
                last_generated: Utc::now(),
            },
        );

        // Persist to disk with restricted permissions
        self.persist_code(&pending).ok();

        tracing::info!(
            code = %code,
            channel = %channel,
            session = %key_str,
            "pairing code generated"
        );

        Ok(code)
    }

    /// Verify a pairing code.
    ///
    /// Returns the channel and session reference on success.
    /// On failure, increments the attempt counter and may lock out after 5 failures.
    pub async fn verify_code(&self, code: &str) -> Result<(String, String), PairingError> {
        // Take ownership of the entry to avoid borrow conflicts
        let mut codes = self.pending_codes.write().await;
        let mut pending = codes.remove(code).ok_or(PairingError::InvalidCode)?;

        // Check expiry
        if pending.created_at + ChronoDuration::seconds(EXPIRY_SECS) <= Utc::now() {
            drop(codes);
            self.remove_persisted_code(code).ok();
            return Err(PairingError::Expired);
        }

        // Increment attempts
        pending.attempts += 1;
        let attempts = pending.attempts;
        let channel = pending.channel.clone();
        let session_ref = pending.session_ref.clone();
        let session_key = format!("{channel}:{session_ref}");

        drop(codes);
        self.remove_persisted_code(code).ok();

        // Check if too many attempts
        if attempts > MAX_FAILURES {
            // Lock out this session
            self.lockouts.write().await.insert(
                session_key,
                LockoutEntry {
                    locked_at: Utc::now(),
                    failure_count: attempts,
                },
            );

            return Err(PairingError::LockedOut {
                remaining_secs: LOCKOUT_SECS,
            });
        }

        // Code is valid - clear any lockout on success
        self.lockouts.write().await.remove(&session_key);

        tracing::info!(
            code = %code,
            channel = %channel,
            "pairing code verified successfully"
        );

        Ok((channel, session_ref))
    }

    /// Clean up expired codes.
    pub async fn cleanup_expired(&self) {
        let mut codes = self.pending_codes.write().await;
        let now = Utc::now();
        let expired_keys: Vec<String> = codes
            .iter()
            .filter(|(_, v)| v.created_at + ChronoDuration::seconds(EXPIRY_SECS) <= now)
            .map(|(k, _)| k.clone())
            .collect();

        for key in &expired_keys {
            codes.remove(key);
            self.remove_persisted_code(key).ok();
        }

        if !expired_keys.is_empty() {
            tracing::info!(
                count = expired_keys.len(),
                "cleaned up expired pairing codes"
            );
        }

        // Also clean up expired lockouts
        let mut lockouts = self.lockouts.write().await;
        lockouts.retain(|_, v| v.locked_at + ChronoDuration::seconds(LOCKOUT_SECS) > now);

        // Clean up old rate limits
        let mut rate_limits = self.rate_limits.write().await;
        rate_limits
            .retain(|_, v| v.last_generated + ChronoDuration::seconds(RATE_LIMIT_SECS) > now);
    }

    /// Generate a random 8-character code from the unambiguous alphabet.
    fn generate_random_code() -> String {
        let mut rng = rand::thread_rng();
        (0..CODE_LENGTH)
            .map(|_| {
                let idx = rng.gen_range(0..ALPHABET.len());
                ALPHABET[idx] as char
            })
            .collect()
    }

    /// Persist a code to disk with restricted file permissions (0o600).
    fn persist_code(&self, code: &PendingCode) -> std::io::Result<()> {
        let path = self.storage_path.join(format!("{}.json", code.code));
        let json = serde_json::to_string_pretty(code)?;

        let mut file = fs::File::create(&path)?;
        // Set file permissions to owner-only (0o600)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    /// Remove a persisted code from disk.
    fn remove_persisted_code(&self, code: &str) -> std::io::Result<()> {
        let path = self.storage_path.join(format!("{}.json", code));
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

fn normalize_channel_ref(channel: &str) -> Result<String, PairingError> {
    let value = channel.trim().to_ascii_lowercase();
    if value.is_empty() {
        return Err(PairingError::InvalidChannel);
    }
    Ok(match value.as_str() {
        "wechat_ilink" | "wechat" => "wechat-ilink".to_string(),
        other => other.to_string(),
    })
}

fn normalize_session_ref(session_ref: &str) -> Result<String, PairingError> {
    let value = session_ref.trim();
    if value.is_empty() {
        return Err(PairingError::InvalidSessionRef);
    }
    Ok(value.to_string())
}

/// Pairing error types.
#[derive(Debug, thiserror::Error)]
pub enum PairingError {
    #[error("rate limited: try again in {remaining_secs}s")]
    RateLimited { remaining_secs: i64 },

    #[error("locked out: try again in {remaining_secs}s")]
    LockedOut { remaining_secs: i64 },

    #[error("invalid or expired pairing code")]
    InvalidCode,

    #[error("pairing code has expired")]
    Expired,

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("invalid channel")]
    InvalidChannel,

    #[error("invalid session reference")]
    InvalidSessionRef,
}

impl From<std::io::Error> for PairingError {
    fn from(e: std::io::Error) -> Self {
        PairingError::StorageError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_code_generation_format() {
        let code = PairingManager::generate_random_code();
        assert_eq!(code.len(), CODE_LENGTH);
        // All chars should be from the unambiguous alphabet
        for c in code.chars() {
            assert!(
                ALPHABET.contains(&(c as u8)),
                "char '{}' not in alphabet",
                c
            );
        }
    }

    #[test]
    fn test_code_uniqueness() {
        let codes: Vec<String> = (0..100)
            .map(|_| PairingManager::generate_random_code())
            .collect();
        // All codes should be unique (extremely unlikely to collide with 32^8 space)
        let unique: std::collections::HashSet<_> = codes.iter().collect();
        assert_eq!(unique.len(), 100);
    }

    #[tokio::test]
    async fn test_generate_and_verify() {
        let dir = tempfile::tempdir().unwrap();
        let manager = PairingManager::new(dir.path().to_path_buf());

        let code = manager.generate_code("chat", "user123").await.unwrap();

        let (channel, verified_ref) = manager.verify_code(&code).await.unwrap();
        assert_eq!(channel, "chat");
        assert_eq!(verified_ref, "user123");
    }

    #[tokio::test]
    async fn test_verify_invalid_code() {
        let dir = tempfile::tempdir().unwrap();
        let manager = PairingManager::new(dir.path().to_path_buf());

        let result = manager.verify_code("invalid1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_verify_consumed_code() {
        let dir = tempfile::tempdir().unwrap();
        let manager = PairingManager::new(dir.path().to_path_buf());

        let code = manager.generate_code("chat", "user456").await.unwrap();

        // First verification succeeds
        let _ = manager.verify_code(&code).await.unwrap();

        // Second verification fails (code consumed)
        let result = manager.verify_code(&code).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rate_limiting() {
        let dir = tempfile::tempdir().unwrap();
        let manager = PairingManager::new(dir.path().to_path_buf());

        let _ = manager.generate_code("chat", "user789").await.unwrap();

        // Second generation should be rate limited
        let result = manager.generate_code("chat", "user789").await;
        assert!(result.is_err());
        if let Err(PairingError::RateLimited { remaining_secs }) = result {
            assert!(remaining_secs > 0);
        } else {
            panic!("expected RateLimited error");
        }
    }
}
