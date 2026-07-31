//! Encryption-at-rest for opaque Provider transcript state.
//!
//! Reasoning signatures and private reasoning must survive Runtime reactivation
//! so a Provider can validate the next request, but they are not ordinary
//! Session history. This module seals those values before the canonical
//! transcript reaches SQLite and opens them only while hydrating Runtime.

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};

const SEALED_PREFIX: &str = "cowd-provider-transcript:v1:";
const KEY_FILE: &str = "provider-transcript-v1.key";
const KEY_BYTES: usize = 32;
const AAD: &[u8] = b"cowd/provider-transcript/v1";

#[must_use]
pub fn is_sealed_provider_transcript(value: &str) -> bool {
    value.starts_with(SEALED_PREFIX)
}

pub fn seal_provider_transcript(value: &str) -> Result<String, String> {
    let key = load_or_create_key()?;
    seal_with_key(value, &key)
}

pub fn open_provider_transcript(value: &str) -> Result<String, String> {
    if !is_sealed_provider_transcript(value) {
        // Existing in-memory messages and explicitly imported historical
        // sessions can still be consumed. Every new canonical write uses the
        // sealed representation.
        return Ok(value.to_string());
    }
    let key = load_or_create_key()?;
    open_with_key(value, &key)
}

fn key_path() -> PathBuf {
    crate::cowd_dirs::config_home_dir()
        .join(crate::cowd_dirs::CREDENTIALS_DIR)
        .join(KEY_FILE)
}

fn load_or_create_key() -> Result<[u8; KEY_BYTES], String> {
    let path = key_path();
    if let Ok(encoded) = fs::read_to_string(&path) {
        return decode_key(&encoded);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create Provider transcript key directory: {error}"))?;
    }

    let mut key = [0_u8; KEY_BYTES];
    SystemRandom::new()
        .fill(&mut key)
        .map_err(|_| "generate Provider transcript key".to_string())?;
    let encoded = BASE64.encode(key);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    match options.open(&path) {
        Ok(mut file) => {
            file.write_all(encoded.as_bytes())
                .and_then(|()| file.write_all(b"\n"))
                .and_then(|()| file.sync_all())
                .map_err(|error| format!("persist Provider transcript key: {error}"))?;
            Ok(key)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => fs::read_to_string(&path)
            .map_err(|read_error| format!("read concurrently created transcript key: {read_error}"))
            .and_then(|encoded| decode_key(&encoded)),
        Err(error) => Err(format!("create Provider transcript key: {error}")),
    }
}

fn decode_key(encoded: &str) -> Result<[u8; KEY_BYTES], String> {
    let bytes = BASE64
        .decode(encoded.trim())
        .map_err(|error| format!("decode Provider transcript key: {error}"))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        format!(
            "Provider transcript key has {} bytes, expected {KEY_BYTES}",
            bytes.len()
        )
    })
}

fn seal_with_key(value: &str, key: &[u8; KEY_BYTES]) -> Result<String, String> {
    let mut nonce_bytes = [0_u8; NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce_bytes)
        .map_err(|_| "generate Provider transcript nonce".to_string())?;
    let mut payload = value.as_bytes().to_vec();
    less_safe_key(key)?
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce_bytes),
            Aad::from(AAD),
            &mut payload,
        )
        .map_err(|_| "seal Provider transcript".to_string())?;
    let mut envelope = nonce_bytes.to_vec();
    envelope.extend_from_slice(&payload);
    Ok(format!("{SEALED_PREFIX}{}", BASE64.encode(envelope)))
}

fn open_with_key(value: &str, key: &[u8; KEY_BYTES]) -> Result<String, String> {
    let encoded = value
        .strip_prefix(SEALED_PREFIX)
        .ok_or_else(|| "Provider transcript envelope prefix is missing".to_string())?;
    let mut envelope = BASE64
        .decode(encoded)
        .map_err(|error| format!("decode Provider transcript envelope: {error}"))?;
    if envelope.len() <= NONCE_LEN {
        return Err("Provider transcript envelope is truncated".to_string());
    }
    let nonce_bytes: [u8; NONCE_LEN] = envelope[..NONCE_LEN]
        .try_into()
        .map_err(|_| "Provider transcript nonce is invalid".to_string())?;
    let plaintext = less_safe_key(key)?
        .open_in_place(
            Nonce::assume_unique_for_key(nonce_bytes),
            Aad::from(AAD),
            &mut envelope[NONCE_LEN..],
        )
        .map_err(|_| "open Provider transcript envelope".to_string())?;
    String::from_utf8(plaintext.to_vec())
        .map_err(|error| format!("Provider transcript plaintext is not UTF-8: {error}"))
}

fn less_safe_key(key: &[u8; KEY_BYTES]) -> Result<LessSafeKey, String> {
    UnboundKey::new(&AES_256_GCM, key)
        .map(LessSafeKey::new)
        .map_err(|_| "initialize Provider transcript cipher".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealed_transcript_round_trips_without_plaintext() {
        let key = [7_u8; KEY_BYTES];
        let sealed = seal_with_key("private-reasoning/signature", &key).unwrap();
        assert!(is_sealed_provider_transcript(&sealed));
        assert!(!sealed.contains("private-reasoning"));
        assert_eq!(
            open_with_key(&sealed, &key).unwrap(),
            "private-reasoning/signature"
        );
    }

    #[test]
    fn tampered_transcript_fails_closed() {
        let key = [9_u8; KEY_BYTES];
        let mut sealed = seal_with_key("signature", &key).unwrap().into_bytes();
        let last = sealed.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        let sealed = String::from_utf8(sealed).unwrap();
        assert!(open_with_key(&sealed, &key).is_err());
    }
}
