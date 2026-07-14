use harness_contract::agent::{
    AgentDefinitionManifest, AgentDefinitionRevision, AgentDefinitionRevisionRef,
};
use sha2::{Digest, Sha256};

use super::store::DefinitionStoreError;

pub(crate) const MANIFEST_FILE_NAME: &str = "agent.yaml";
pub(crate) const INSTRUCTIONS_FILE_NAME: &str = "AGENT.md";

pub(crate) fn validate_agent_markdown(value: &str) -> Result<(), DefinitionStoreError> {
    if value.trim().is_empty() {
        return Err(DefinitionStoreError::InvalidAgentMarkdown(
            "AGENT.md cannot be blank".to_string(),
        ));
    }
    if value.contains('\0') {
        return Err(DefinitionStoreError::InvalidAgentMarkdown(
            "AGENT.md cannot contain NUL bytes".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn normalize_agent_markdown(value: &str) -> String {
    let normalized = value.replace("\r\n", "\n").replace('\r', "\n");
    if normalized.ends_with('\n') {
        normalized
    } else {
        format!("{normalized}\n")
    }
}

pub(crate) fn manifest_yaml(
    manifest: &AgentDefinitionManifest,
) -> Result<String, DefinitionStoreError> {
    serde_yaml::to_string(manifest).map_err(DefinitionStoreError::serialize)
}

pub(crate) fn parse_manifest_yaml(
    bytes: &[u8],
) -> Result<AgentDefinitionManifest, DefinitionStoreError> {
    serde_yaml::from_slice(bytes).map_err(DefinitionStoreError::deserialize)
}

pub(crate) fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn content_digest(manifest_bytes: &[u8], instructions: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"cowd.agent.revision/v1\0manifest\0");
    digest.update(manifest_bytes);
    digest.update(b"\0instructions\0");
    digest.update(instructions);
    format!("{:x}", digest.finalize())
}

pub(crate) fn build_revision(
    manifest: AgentDefinitionManifest,
    instructions: &str,
) -> Result<(AgentDefinitionRevision, String), DefinitionStoreError> {
    validate_agent_markdown(instructions)?;
    let instructions = normalize_agent_markdown(instructions);
    let instructions_digest = digest_hex(instructions.as_bytes());
    if manifest.instructions_digest != instructions_digest {
        return Err(DefinitionStoreError::DigestMismatch {
            subject: "manifest.instructions_digest".to_string(),
            expected: manifest.instructions_digest,
            actual: instructions_digest,
        });
    }
    manifest
        .validate()
        .map_err(DefinitionStoreError::contract)?;
    let manifest_bytes = manifest_yaml(&manifest)?.into_bytes();
    let revision = AgentDefinitionRevision {
        revision_ref: manifest.revision_ref(),
        manifest,
        content_digest: content_digest(&manifest_bytes, instructions.as_bytes()),
    };
    revision
        .validate()
        .map_err(DefinitionStoreError::contract)?;
    Ok((revision, instructions))
}

pub(crate) fn verify_read_revision(
    manifest_bytes: &[u8],
    instructions_bytes: &[u8],
) -> Result<(AgentDefinitionRevision, String), DefinitionStoreError> {
    let manifest = parse_manifest_yaml(manifest_bytes)?;
    let instructions = String::from_utf8(instructions_bytes.to_vec()).map_err(|error| {
        DefinitionStoreError::InvalidAgentMarkdown(format!("AGENT.md is not UTF-8: {error}"))
    })?;
    validate_agent_markdown(&instructions)?;
    let actual_instruction_digest = digest_hex(instructions.as_bytes());
    if manifest.instructions_digest != actual_instruction_digest {
        return Err(DefinitionStoreError::DigestMismatch {
            subject: "manifest.instructions_digest".to_string(),
            expected: manifest.instructions_digest,
            actual: actual_instruction_digest,
        });
    }
    manifest
        .validate()
        .map_err(DefinitionStoreError::contract)?;
    let revision = AgentDefinitionRevision {
        revision_ref: manifest.revision_ref(),
        manifest,
        content_digest: content_digest(manifest_bytes, instructions.as_bytes()),
    };
    revision
        .validate()
        .map_err(DefinitionStoreError::contract)?;
    Ok((revision, instructions))
}

pub(crate) fn ensure_same_revision_ref(
    expected: &AgentDefinitionRevisionRef,
    actual: &AgentDefinitionRevisionRef,
) -> Result<(), DefinitionStoreError> {
    if expected != actual {
        return Err(DefinitionStoreError::CorruptRevision {
            revision: expected.clone(),
            reason: format!(
                "stored manifest identifies `{}` revision {}",
                actual.definition_id.as_str(),
                actual.revision
            ),
        });
    }
    Ok(())
}
