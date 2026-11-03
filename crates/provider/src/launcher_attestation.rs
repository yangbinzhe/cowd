//! Verification primitive for the launcher handoff. This module does not install
//! a send capability: the caller must bootstrap its key and expected identity
//! through an authenticated channel and connect it to a durable send owner.
use std::sync::atomic::{AtomicBool, Ordering};

use ring::{rand::{SecureRandom, SystemRandom}, signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519}};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

const DOMAIN: &[u8] = b"cowd:provider-launcher-attestation:v1\0";
const MAX_LIFETIME_MS: u64 = 300_000;

/// Launcher-held, ephemeral signing key. The private seed is generated from
/// the OS RNG, never serialized, and the temporary seed is zeroized. Possession
/// of its public key alone does not authenticate the launcher: it must arrive
/// through a separately authenticated bootstrap channel.
pub struct LauncherSigningKey(Ed25519KeyPair);

impl LauncherSigningKey {
    pub fn generate() -> Result<Self, &'static str> {
        let mut seed = Zeroizing::new([0u8; 32]);
        SystemRandom::new().fill(&mut *seed).map_err(|_| "launcher key randomness unavailable")?;
        Ed25519KeyPair::from_seed_unchecked(&*seed)
            .map(Self).map_err(|_| "launcher key generation failed")
    }

    pub fn public_key(&self) -> [u8; 32] {
        let mut public = [0u8; 32];
        public.copy_from_slice(self.0.public_key().as_ref());
        public
    }

    pub fn sign(&self, value: &LauncherAttestation) -> Result<[u8; 64], &'static str> {
        let bytes = value.signing_bytes().map_err(|_| "attestation encoding failed")?;
        let signature = self.0.sign(&bytes);
        let mut output = [0u8; 64];
        output.copy_from_slice(signature.as_ref());
        Ok(output)
    }
}

/// The identities the launcher has verified before handoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LauncherAttestation {
    pub gateway_sha256: String,
    pub harness_sha256: String,
    pub candidate_head: String,
    pub crates_sha256: String,
    pub launcher_pid: u32,
    pub launcher_starttime: u64,
    pub gateway_pid: u32,
    pub gateway_starttime: u64,
    pub harness_pid: u32,
    pub harness_starttime: u64,
    pub nonce: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
}

impl LauncherAttestation {
    /// Bind this payload to the current process as observed in this process's
    /// Linux proc namespace. This is not launcher authorization by itself.
    pub fn bind_current_launcher(&mut self) -> Result<(), &'static str> {
        let pid = std::process::id();
        let starttime = observe_process_starttime(pid)?;
        self.launcher_pid = pid;
        self.launcher_starttime = starttime;
        Ok(())
    }

    /// A dedicated signer child binds its OS-observed parent as launcher.
    /// An authenticated inherited channel is still required to establish that
    /// the parent is the authorized owner of the run.
    #[cfg(target_os = "linux")]
    pub fn bind_parent_launcher(&mut self) -> Result<(), &'static str> {
        let stat = std::fs::read_to_string("/proc/self/stat")
            .map_err(|_| "signer proc identity unavailable")?;
        let (_, tail) = stat.rsplit_once(") ").ok_or("invalid signer proc stat")?;
        let parent = tail.split_whitespace().nth(1).ok_or("missing signer parent pid")?
            .parse::<u32>().map_err(|_| "invalid signer parent pid")?;
        check_launcher_pid_namespace(parent)?;
        self.launcher_starttime = observe_process_starttime(parent)?;
        self.launcher_pid = parent;
        Ok(())
    }

    /// Exact signed bytes; field order and domain are fixed by this contract.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = DOMAIN.to_vec();
        bytes.extend(serde_json::to_vec(self)?);
        Ok(bytes)
    }
}

/// A verifier consumes at most one valid handoff for its startup instance.
/// Supplying the trusted key and expected values is the bootstrap owner's duty;
/// a key or expected identity received in the signed payload is not trusted.
pub struct LauncherAttestationVerifier {
    trusted_public_key: [u8; 32],
    expected: LauncherAttestation,
    consumed: AtomicBool,
}

/// A verified value, not by itself permission to send: per-send durable
/// confirmation and authenticated bootstrap are still required.
#[derive(Debug, Clone)]
pub struct VerifiedLauncherIdentity(LauncherAttestation);

impl VerifiedLauncherIdentity {
    pub fn attestation(&self) -> &LauncherAttestation { &self.0 }
}

fn lowercase_hex(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(target_os = "linux")]
fn observe_process_starttime(pid: u32) -> Result<u64, &'static str> {
    if pid == 0 { return Err("invalid launcher pid"); }
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|_| "launcher proc identity unavailable")?;
    // comm is parenthesized and may itself contain spaces or ')'. Parse after
    // the final delimiter rather than splitting the entire stat line.
    let (prefix, tail) = stat.rsplit_once(") ").ok_or("invalid launcher proc stat")?;
    let observed_pid = prefix.split_once(' ').ok_or("invalid launcher proc stat")?.0
        .parse::<u32>().map_err(|_| "invalid launcher proc pid")?;
    if observed_pid != pid { return Err("launcher proc pid mismatch"); }
    let fields: Vec<&str> = tail.split_whitespace().collect();
    if matches!(fields.first(), Some(&"Z" | &"X" | &"x")) {
        return Err("launcher process is not live");
    }
    fields.get(19).ok_or("missing launcher proc starttime")?
        .parse::<u64>().map_err(|_| "invalid launcher proc starttime")
}

#[cfg(not(target_os = "linux"))]
fn observe_process_starttime(_pid: u32) -> Result<u64, &'static str> {
    Err("launcher proc identity is unsupported on this platform")
}

#[cfg(target_os = "linux")]
fn check_launcher_pid_namespace(pid: u32) -> Result<(), &'static str> {
    let own = std::fs::read_link("/proc/self/ns/pid")
        .map_err(|_| "own pid namespace unavailable")?;
    let launcher = std::fs::read_link(format!("/proc/{pid}/ns/pid"))
        .map_err(|_| "launcher pid namespace unavailable")?;
    if own != launcher { return Err("launcher pid namespace mismatch"); }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn check_launcher_pid_namespace(_pid: u32) -> Result<(), &'static str> {
    Err("launcher pid namespace is unsupported on this platform")
}

#[cfg(target_os = "linux")]
fn verify_owned_child_image(pid: u32, start: u64, parent: u32, expected: &str) -> Result<(), &'static str> {
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;
    let check = || -> Result<(), &'static str> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map_err(|_| "child proc identity unavailable")?;
        let (_, tail) = stat.rsplit_once(") ").ok_or("invalid child proc stat")?;
        let fields: Vec<_> = tail.split_whitespace().collect();
        if matches!(fields.first(), Some(&"Z" | &"X" | &"x"))
            || fields.get(1).and_then(|s| s.parse::<u32>().ok()) != Some(parent)
            || fields.get(19).and_then(|s| s.parse::<u64>().ok()) != Some(start)
        { return Err("child ownership or generation mismatch"); }
        Ok(())
    };
    check()?;
    let mut image = std::fs::File::open(format!("/proc/{pid}/exe"))
        .map_err(|_| "child image unavailable")?;
    let opened = image.metadata().map_err(|_| "child image metadata unavailable")?;
    let mut hash = Sha256::new();
    let mut bytes = [0u8; 65536];
    loop {
        let n = image.read(&mut bytes).map_err(|_| "child image read failed")?;
        if n == 0 { break; }
        hash.update(&bytes[..n]);
    }
    check()?;
    let current = std::fs::metadata(format!("/proc/{pid}/exe"))
        .map_err(|_| "child image metadata unavailable")?;
    if (opened.dev(), opened.ino()) != (current.dev(), current.ino())
        || format!("{:x}", hash.finalize()) != expected
    { return Err("child image changed or hash mismatch"); }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn verify_owned_child_image(_pid: u32, _start: u64, _parent: u32, _expected: &str) -> Result<(), &'static str> {
    Err("child image verification is unsupported on this platform")
}

impl LauncherAttestationVerifier {
    pub fn new(trusted_public_key: [u8; 32], expected: LauncherAttestation) -> Self {
        Self { trusted_public_key, expected, consumed: AtomicBool::new(false) }
    }

    pub fn verify(&self, value: &LauncherAttestation, signature: &[u8])
        -> Result<VerifiedLauncherIdentity, &'static str>
    {
        check_launcher_pid_namespace(value.launcher_pid)?;
        if observe_process_starttime(value.launcher_pid)? != value.launcher_starttime {
            return Err("launcher proc starttime mismatch");
        }
        verify_owned_child_image(value.gateway_pid, value.gateway_starttime, value.launcher_pid, &value.gateway_sha256)?;
        verify_owned_child_image(value.harness_pid, value.harness_starttime, value.launcher_pid, &value.harness_sha256)?;
        let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| "system clock before epoch")?.as_millis();
        let now_ms = u64::try_from(now_ms).map_err(|_| "system clock overflow")?;
        self.verify_at(value, signature, now_ms)
    }

    fn verify_at(&self, value: &LauncherAttestation, signature: &[u8], now_ms: u64)
        -> Result<VerifiedLauncherIdentity, &'static str>
    {
        if !lowercase_hex(&value.gateway_sha256, 64)
            || !lowercase_hex(&value.harness_sha256, 64)
            || !lowercase_hex(&value.crates_sha256, 64)
            || !(lowercase_hex(&value.candidate_head, 40) || lowercase_hex(&value.candidate_head, 64))
            || !lowercase_hex(&value.nonce, 64)
            || value.launcher_pid == 0 || value.launcher_starttime == 0
            || value.gateway_pid == 0 || value.harness_pid == 0
            || value.gateway_starttime == 0 || value.harness_starttime == 0
        { return Err("invalid attestation identity"); }
        if value.issued_at_ms > now_ms || value.expires_at_ms < now_ms
            || value.expires_at_ms < value.issued_at_ms
            || value.expires_at_ms - value.issued_at_ms > MAX_LIFETIME_MS
        { return Err("attestation expired or invalid lifetime"); }
        if value != &self.expected { return Err("attestation does not match expected identity"); }
        let bytes = value.signing_bytes().map_err(|_| "attestation encoding failed")?;
        UnparsedPublicKey::new(&ED25519, self.trusted_public_key)
            .verify(&bytes, signature).map_err(|_| "attestation signature invalid")?;
        self.consumed.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| "attestation already consumed")?;
        Ok(VerifiedLauncherIdentity(value.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::{rand::SystemRandom, signature::{Ed25519KeyPair, KeyPair}};

    fn fixture() -> (Ed25519KeyPair, LauncherAttestation) {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let key = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let value = LauncherAttestation { gateway_sha256: "a".repeat(64), harness_sha256: "b".repeat(64), candidate_head: "c".repeat(40), crates_sha256: "d".repeat(64), launcher_pid: 9, launcher_starttime: 10, gateway_pid: 11, gateway_starttime: 12, harness_pid: 13, harness_starttime: 14, nonce: "e".repeat(64), issued_at_ms: 1000, expires_at_ms: 2000 };
        (key, value)
    }
    fn verifier(key: &Ed25519KeyPair, value: &LauncherAttestation) -> LauncherAttestationVerifier {
        LauncherAttestationVerifier::new(key.public_key().as_ref().try_into().unwrap(), value.clone())
    }
    #[test]
    fn authentic_identity_is_single_use_and_bound_to_expected_image() {
        let (key, value) = fixture();
        let signature = key.sign(&value.signing_bytes().unwrap());
        let checker = verifier(&key, &value);
        assert_eq!(checker.verify_at(&value, signature.as_ref(), 1500).unwrap().attestation(), &value);
        assert_eq!(checker.verify_at(&value, signature.as_ref(), 1500).unwrap_err(), "attestation already consumed");
        let mut other = value.clone(); other.harness_sha256 = "f".repeat(64);
        let other_signature = key.sign(&other.signing_bytes().unwrap());
        assert_eq!(verifier(&key, &value).verify_at(&other, other_signature.as_ref(), 1500).unwrap_err(), "attestation does not match expected identity");
    }
    #[test]
    fn forged_and_expired_identity_are_rejected() {
        let (key, value) = fixture();
        let (other_key, _) = fixture();
        let forged = other_key.sign(&value.signing_bytes().unwrap());
        assert_eq!(verifier(&key, &value).verify_at(&value, forged.as_ref(), 1500).unwrap_err(), "attestation signature invalid");
        let valid = key.sign(&value.signing_bytes().unwrap());
        assert_eq!(verifier(&key, &value).verify_at(&value, valid.as_ref(), 2001).unwrap_err(), "attestation expired or invalid lifetime");
    }

    #[test]
    fn launcher_generated_key_signs_and_other_instance_cannot_impersonate_it() {
        let (_, value) = fixture();
        let launcher = LauncherSigningKey::generate().unwrap();
        let other = LauncherSigningKey::generate().unwrap();
        assert_ne!(launcher.public_key(), other.public_key());
        let verifier = LauncherAttestationVerifier::new(launcher.public_key(), value.clone());
        let forged = other.sign(&value).unwrap();
        assert_eq!(verifier.verify_at(&value, &forged, 1500).unwrap_err(), "attestation signature invalid");
        let authentic = launcher.sign(&value).unwrap();
        assert_eq!(verifier.verify_at(&value, &authentic, 1500).unwrap().attestation(), &value);
        assert_eq!(verifier.verify_at(&value, &authentic, 1500).unwrap_err(), "attestation already consumed");
    }

    #[test]
    fn launcher_process_identity_is_signed_and_expected() {
        let (_, value) = fixture();
        let launcher = LauncherSigningKey::generate().unwrap();
        let mut wrong_process = value.clone();
        wrong_process.launcher_starttime += 1;
        let foreign_signature = launcher.sign(&wrong_process).unwrap();
        let verifier = LauncherAttestationVerifier::new(launcher.public_key(), value.clone());
        assert_eq!(verifier.verify_at(&wrong_process, &foreign_signature, 1500).unwrap_err(),
            "attestation does not match expected identity");
        let original_signature = launcher.sign(&value).unwrap();
        assert!(verifier.verify_at(&value, &original_signature, 1500).is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn observed_launcher_proc_identity_is_checked_before_acceptance() {
        struct Reap(std::process::Child);
        impl Drop for Reap { fn drop(&mut self) { let _ = self.0.kill(); let _ = self.0.wait(); } }
        let gateway = Reap(std::process::Command::new("sleep").arg("30").spawn().unwrap());
        let harness = Reap(std::process::Command::new("sleep").arg("30").spawn().unwrap());
        let (_, mut value) = fixture();
        value.bind_current_launcher().unwrap();
        value.gateway_pid = gateway.0.id();
        value.gateway_starttime = observe_process_starttime(value.gateway_pid).unwrap();
        value.gateway_sha256 = format!("{:x}", Sha256::digest(std::fs::read(format!("/proc/{}/exe", value.gateway_pid)).unwrap()));
        value.harness_pid = harness.0.id();
        value.harness_starttime = observe_process_starttime(value.harness_pid).unwrap();
        value.harness_sha256 = format!("{:x}", Sha256::digest(std::fs::read(format!("/proc/{}/exe", value.harness_pid)).unwrap()));
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .unwrap().as_millis() as u64;
        value.issued_at_ms = now.saturating_sub(1);
        value.expires_at_ms = now + 60_000;
        let signer = LauncherSigningKey::generate().unwrap();
        let mut wrong = value.clone();
        wrong.launcher_starttime += 1;
        let signature = signer.sign(&wrong).unwrap();
        let verifier = LauncherAttestationVerifier::new(signer.public_key(), wrong.clone());
        assert_eq!(verifier.verify(&wrong, &signature).unwrap_err(), "launcher proc starttime mismatch");
        let signature = signer.sign(&value).unwrap();
        let verifier = LauncherAttestationVerifier::new(signer.public_key(), value.clone());
        assert!(verifier.verify(&value, &signature).is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn separate_live_process_in_same_pid_namespace_is_observed_and_exit_is_rejected() {
        struct Reap(std::process::Child);
        impl Drop for Reap {
            fn drop(&mut self) { let _ = self.0.kill(); let _ = self.0.wait(); }
        }
        let mut child = Reap(std::process::Command::new("sleep").arg("30").spawn().unwrap());
        {
            let pid = child.0.id();
            check_launcher_pid_namespace(pid).unwrap();
            let starttime = observe_process_starttime(pid).unwrap();
            assert!(starttime > 0);
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            assert!(observe_process_starttime(pid).is_err());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn actual_different_pid_namespace_is_refused_before_acceptance() {
        use std::io::BufRead;
        use std::process::{Command, Stdio};
        struct Reap(std::process::Child, Option<(u32, u64)>);
        impl Drop for Reap {
            fn drop(&mut self) {
                if let Some((pid, starttime)) = self.1 {
                    if observe_process_starttime(pid) == Ok(starttime) {
                        // The namespace init can ignore SIGTERM; killing this
                        // owned init also tears down the test PID namespace.
                        let _ = Command::new("kill").args(["-KILL", &pid.to_string()]).status();
                    }
                }
                let _ = self.0.kill(); let _ = self.0.wait();
            }
        }
        let mut child = Reap(Command::new("bwrap")
            .args(["--die-with-parent", "--unshare-pid", "--ro-bind", "/", "/",
                "--proc", "/proc", "--dev", "/dev", "--info-fd", "1", "--", "sleep", "30"])
            .stdout(Stdio::piped()).spawn().unwrap(), None);
        let stdout = child.0.stdout.take().unwrap();
        let mut info = String::new();
        for line in std::io::BufReader::new(stdout).lines() {
            let line = line.unwrap();
            info.push_str(&line);
            if line.trim() == "}" { break; }
        }
        let info: serde_json::Value = serde_json::from_str(&info).unwrap();
        let pid = u32::try_from(info["child-pid"].as_u64().unwrap()).unwrap();
        child.1 = Some((pid, observe_process_starttime(pid).unwrap()));
        assert_ne!(std::fs::read_link("/proc/self/ns/pid").unwrap(),
            std::fs::read_link(format!("/proc/{pid}/ns/pid")).unwrap());
        let (_, mut value) = fixture();
        value.launcher_pid = pid;
        value.launcher_starttime = observe_process_starttime(pid).unwrap();
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .unwrap().as_millis() as u64;
        value.issued_at_ms = now.saturating_sub(1);
        value.expires_at_ms = now + 60_000;
        let signer = LauncherSigningKey::generate().unwrap();
        let signature = signer.sign(&value).unwrap();
        let verifier = LauncherAttestationVerifier::new(signer.public_key(), value.clone());
        assert_eq!(verifier.verify(&value, &signature).unwrap_err(), "launcher pid namespace mismatch");
    }

    #[test]
    fn public_image_measurement_cannot_be_used_as_a_signing_secret() {
        // The image digest is observable to both the legitimate launcher and
        // an attacker. Deriving a private key from it authenticates neither.
        let (_, value) = fixture();
        let public_measurement = [0xabu8; 32];
        let legitimate = Ed25519KeyPair::from_seed_unchecked(&public_measurement).unwrap();
        let attacker = Ed25519KeyPair::from_seed_unchecked(&public_measurement).unwrap();
        assert_eq!(legitimate.public_key().as_ref(), attacker.public_key().as_ref());
        let forged = attacker.sign(&value.signing_bytes().unwrap());
        assert!(verifier(&legitimate, &value).verify_at(&value, forged.as_ref(), 1500).is_ok(),
            "a public digest-derived signing key cannot establish launcher identity");
    }
}
