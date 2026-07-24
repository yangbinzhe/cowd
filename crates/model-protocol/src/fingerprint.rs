//! Stable, allocation-light fingerprints shared by protocol owners.

use serde::Serialize;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[must_use]
pub fn hash_serializable<T: Serialize>(value: &T) -> u64 {
    let json = serde_json::to_vec(value).unwrap_or_default();
    stable_hash_bytes(&json)
}

#[must_use]
pub fn stable_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::stable_hash_bytes;

    #[test]
    fn stable_hash_is_deterministic() {
        assert_eq!(stable_hash_bytes(b"cowd"), stable_hash_bytes(b"cowd"));
        assert_ne!(stable_hash_bytes(b"cowd"), stable_hash_bytes(b"cowd-next"));
    }
}
