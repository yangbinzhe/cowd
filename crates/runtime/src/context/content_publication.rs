//! Immutable publication in the existing ArtifactStore; adapters supply authorized bytes.
use harness_contract::context::{ArtifactRef, ArtifactWriteDescriptor};
use sha2::{Digest, Sha256};

pub fn verify_content_revision(bytes: &[u8], expected: &str) -> Result<(), String> {
    let actual = format!("{:x}", Sha256::digest(bytes));
    if actual != expected.strip_prefix("sha256:").unwrap_or(expected) {
        return Err("source_version_conflict".into());
    }
    Ok(())
}

impl crate::RuntimeServices {
    pub async fn publish_authorized_content(
        &self,
        bytes: &[u8],
        expected_sha256: &str,
        media_type: &str,
        session_id: &str,
    ) -> Result<ArtifactRef, String> {
        if session_id.trim().is_empty() || media_type.trim().is_empty() {
            return Err("publication requires Session scope and media type".into());
        }
        verify_content_revision(bytes, expected_sha256)?;
        self.artifact_store()
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: media_type.into(),
                    visibility_scope: format!("session:{session_id}"),
                    expected_bytes: Some(bytes.len() as u64),
                    original_name: None,
                },
                bytes,
            )
            .await
            .map_err(|error| error.to_string())
    }

    pub async fn read_authorized_publication(
        &self,
        content_ref: &str,
        expected_sha256: &str,
        session_id: &str,
        authorized_scopes: &[String],
    ) -> Result<(ArtifactRef, Vec<u8>), String> {
        let artifact = self
            .artifact_store()
            .resolve(content_ref)
            .map_err(|error| error.to_string())?;
        if artifact.visibility_scope != format!("session:{session_id}")
            && !authorized_scopes
                .iter()
                .any(|scope| scope == &artifact.visibility_scope)
        {
            return Err("content is outside authorized scope".into());
        }
        let bytes = self
            .artifact_store()
            .read(&artifact, &artifact.visibility_scope, None)
            .await
            .map_err(|error| error.to_string())?;
        verify_content_revision(&bytes, expected_sha256)?;
        Ok((artifact, bytes))
    }
}
