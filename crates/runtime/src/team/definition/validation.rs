use harness_contract::team::{TeamTemplateManifest, TeamTemplateRevision, TeamTemplateRevisionRef};
use sha2::{Digest, Sha256};

use super::store::TeamDefinitionStoreError;

pub(crate) const MANIFEST_FILE_NAME: &str = "team.yaml";
pub(crate) const INSTRUCTIONS_FILE_NAME: &str = "TEAM.md";

pub(crate) fn normalize_team_markdown(value: &str) -> String {
    let normalized = value.replace("\r\n", "\n").replace('\r', "\n");
    if normalized.ends_with('\n') {
        normalized
    } else {
        format!("{normalized}\n")
    }
}

pub(crate) fn validate_team_markdown(value: &str) -> Result<(), TeamDefinitionStoreError> {
    if value.trim().is_empty() {
        return Err(TeamDefinitionStoreError::InvalidTeamMarkdown(
            "TEAM.md cannot be blank".to_string(),
        ));
    }
    if value.contains('\0') {
        return Err(TeamDefinitionStoreError::InvalidTeamMarkdown(
            "TEAM.md cannot contain NUL bytes".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn content_digest(manifest_bytes: &[u8], instructions: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"cowd.team.revision/v1\0manifest\0");
    digest.update(manifest_bytes);
    digest.update(b"\0instructions\0");
    digest.update(instructions);
    format!("{:x}", digest.finalize())
}

pub(crate) fn manifest_yaml(
    manifest: &TeamTemplateManifest,
) -> Result<String, TeamDefinitionStoreError> {
    serde_yaml::to_string(manifest).map_err(TeamDefinitionStoreError::serialize)
}

pub(crate) fn parse_manifest_yaml(
    bytes: &[u8],
) -> Result<TeamTemplateManifest, TeamDefinitionStoreError> {
    serde_yaml::from_slice(bytes).map_err(TeamDefinitionStoreError::deserialize)
}

pub(crate) fn build_revision(
    manifest: TeamTemplateManifest,
    team_markdown: &str,
) -> Result<(TeamTemplateRevision, String), TeamDefinitionStoreError> {
    validate_team_markdown(team_markdown)?;
    let team_markdown = normalize_team_markdown(team_markdown);
    let actual_digest = digest_hex(team_markdown.as_bytes());
    if manifest.instructions_digest != actual_digest {
        return Err(TeamDefinitionStoreError::DigestMismatch {
            subject: "manifest.instructions_digest".to_string(),
            expected: manifest.instructions_digest,
            actual: actual_digest,
        });
    }
    manifest
        .validate()
        .map_err(TeamDefinitionStoreError::contract)?;
    let manifest_bytes = manifest_yaml(&manifest)?.into_bytes();
    let revision = TeamTemplateRevision {
        revision_ref: manifest.revision_ref(),
        manifest,
        content_digest: content_digest(&manifest_bytes, team_markdown.as_bytes()),
    };
    revision
        .validate()
        .map_err(TeamDefinitionStoreError::contract)?;
    Ok((revision, team_markdown))
}

pub(crate) fn verify_read_revision(
    manifest_bytes: &[u8],
    instructions_bytes: &[u8],
) -> Result<(TeamTemplateRevision, String), TeamDefinitionStoreError> {
    let manifest = parse_manifest_yaml(manifest_bytes)?;
    let team_markdown = String::from_utf8(instructions_bytes.to_vec()).map_err(|error| {
        TeamDefinitionStoreError::InvalidTeamMarkdown(format!("TEAM.md is not UTF-8: {error}"))
    })?;
    validate_team_markdown(&team_markdown)?;
    let actual_digest = digest_hex(team_markdown.as_bytes());
    if manifest.instructions_digest != actual_digest {
        return Err(TeamDefinitionStoreError::DigestMismatch {
            subject: "manifest.instructions_digest".to_string(),
            expected: manifest.instructions_digest,
            actual: actual_digest,
        });
    }
    manifest
        .validate()
        .map_err(TeamDefinitionStoreError::contract)?;
    let revision = TeamTemplateRevision {
        revision_ref: manifest.revision_ref(),
        manifest,
        content_digest: content_digest(manifest_bytes, team_markdown.as_bytes()),
    };
    revision
        .validate()
        .map_err(TeamDefinitionStoreError::contract)?;
    Ok((revision, team_markdown))
}

pub(crate) fn ensure_same_revision_ref(
    expected: &TeamTemplateRevisionRef,
    actual: &TeamTemplateRevisionRef,
) -> Result<(), TeamDefinitionStoreError> {
    if expected != actual {
        return Err(TeamDefinitionStoreError::CorruptRevision {
            revision: expected.clone(),
            reason: format!(
                "stored manifest identifies `{}` revision {}",
                actual.template_id.as_str(),
                actual.revision
            ),
        });
    }
    Ok(())
}
