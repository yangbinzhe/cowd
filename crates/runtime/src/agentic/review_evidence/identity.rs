use super::super::program::AgenticProgramProjection;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum MaterialIdentity {
    Publication(String),
    Content(String),
    Receipt(String),
    Other(String),
}

impl MaterialIdentity {
    pub(super) fn reference(&self) -> &str {
        match self {
            Self::Publication(reference)
            | Self::Content(reference)
            | Self::Receipt(reference)
            | Self::Other(reference) => reference,
        }
    }
}

pub(super) fn auxiliary_identity(
    program: &AgenticProgramProjection,
    reference: &str,
) -> Result<MaterialIdentity, String> {
    let publications = program
        .artifacts
        .values()
        .filter(|artifact| super::same_reference_spelling(&artifact.artifact_ref, reference))
        .collect::<Vec<_>>();
    match publications.as_slice() {
        [artifact] => Ok(MaterialIdentity::Publication(artifact.artifact_ref.clone())),
        [] if reference.starts_with("artifact://") => {
            Ok(MaterialIdentity::Content(reference.to_string()))
        }
        [] if reference.starts_with("tool://") => {
            Ok(MaterialIdentity::Receipt(reference.to_string()))
        }
        [] => Ok(MaterialIdentity::Other(reference.to_string())),
        _ => Err(format!(
            "proof artifact identity is ambiguous: reference {reference}"
        )),
    }
}

pub(super) fn auxiliary_reference(
    program: &AgenticProgramProjection,
    reference: &str,
) -> Result<String, String> {
    auxiliary_identity(program, reference).map(|identity| identity.reference().to_string())
}

pub(crate) fn review_result_identity(
    program: &AgenticProgramProjection,
    reference: &str,
) -> Result<String, String> {
    let identity = auxiliary_identity(program, reference)?;
    if !matches!(identity, MaterialIdentity::Content(_)) {
        return Ok(identity.reference().to_string());
    }
    let publications = program
        .artifacts
        .values()
        .filter(|artifact| super::same_reference_spelling(&artifact.content_ref, reference))
        .collect::<Vec<_>>();
    match publications.as_slice() {
        [] => Ok(reference.to_string()),
        [artifact] => Ok(artifact.artifact_ref.clone()),
        _ => Err(format!(
            "proof artifact identity is ambiguous: reference {reference}"
        )),
    }
}
