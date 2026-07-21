use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::String,
    vec::Vec,
};
use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{ArtifactId, Sha256Digest, StageInstanceId, MAX_CONTROL_REVISION};

/// Immutable typed metadata for one content-addressed artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRecord {
    pub artifact_id: ArtifactId,
    pub artifact_type: String,
    pub schema_id: String,
    pub schema_version: u32,
    pub sha256: Sha256Digest,
    pub size: u64,
    pub media_type: String,
    pub producer_stage_instance_id: StageInstanceId,
    /// Zero is reserved for coordinator-authored stage ingress before the
    /// first agent attempt; agent-produced artifacts use a positive attempt.
    pub producer_attempt: u32,
    pub producer_event_sequence: u64,
    pub pipeline_definition_digest: Sha256Digest,
    pub component_digest: Sha256Digest,
    pub input_manifest_digest: Sha256Digest,
    pub retention_class: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactRecordValidationError {
    InvalidArtifactType,
    SchemaIdentityMismatch,
    InvalidSchemaVersion,
    InvalidMediaType,
    InvalidProducerEventSequence,
    InvalidRetentionClass,
}

impl ArtifactRecord {
    pub fn validate(&self) -> Result<(), ArtifactRecordValidationError> {
        let (type_schema, type_version) = self
            .artifact_type
            .rsplit_once("/v")
            .ok_or(ArtifactRecordValidationError::InvalidArtifactType)?;
        if !is_token(type_schema) {
            return Err(ArtifactRecordValidationError::InvalidArtifactType);
        }
        if self.schema_version == 0 {
            return Err(ArtifactRecordValidationError::InvalidSchemaVersion);
        }
        let parsed_version = type_version
            .parse::<u32>()
            .map_err(|_| ArtifactRecordValidationError::InvalidArtifactType)?;
        if parsed_version != self.schema_version {
            return Err(ArtifactRecordValidationError::InvalidSchemaVersion);
        }
        if self.schema_id != type_schema || !is_token(&self.schema_id) {
            return Err(ArtifactRecordValidationError::SchemaIdentityMismatch);
        }
        if !is_media_type(&self.media_type) {
            return Err(ArtifactRecordValidationError::InvalidMediaType);
        }
        if self.producer_event_sequence == 0 || self.producer_event_sequence > MAX_CONTROL_REVISION
        {
            return Err(ArtifactRecordValidationError::InvalidProducerEventSequence);
        }
        if !is_token(&self.retention_class) {
            return Err(ArtifactRecordValidationError::InvalidRetentionClass);
        }
        Ok(())
    }
}

fn is_token(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.' | b'_')
        })
}

fn is_media_type(value: &str) -> bool {
    let Some((kind, subtype)) = value.split_once('/') else {
        return false;
    };
    !subtype.contains('/') && is_media_token(kind) && is_media_token(subtype)
}

fn is_media_token(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                )
        })
}

/// Pure, append-only artifact lineage graph. Parents must already be registered,
/// so every accepted edge preserves acyclicity without graph search.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactCatalog {
    records: BTreeMap<ArtifactId, ArtifactRecord>,
    children: BTreeMap<ArtifactId, BTreeSet<ArtifactId>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactCatalogError {
    InvalidRecord(ArtifactRecordValidationError),
    ArtifactAlreadyExists,
    UnknownParent(ArtifactId),
    DuplicateParent(ArtifactId),
    SelfDependency,
}

impl ArtifactCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        record: ArtifactRecord,
        parents: &[ArtifactId],
    ) -> Result<(), ArtifactCatalogError> {
        record
            .validate()
            .map_err(ArtifactCatalogError::InvalidRecord)?;
        if self.records.contains_key(&record.artifact_id) {
            return Err(ArtifactCatalogError::ArtifactAlreadyExists);
        }

        let mut unique_parents = BTreeSet::new();
        for parent in parents {
            if parent == &record.artifact_id {
                return Err(ArtifactCatalogError::SelfDependency);
            }
            if !unique_parents.insert(parent.clone()) {
                return Err(ArtifactCatalogError::DuplicateParent(parent.clone()));
            }
            if !self.records.contains_key(parent) {
                return Err(ArtifactCatalogError::UnknownParent(parent.clone()));
            }
        }

        let artifact_id = record.artifact_id.clone();
        self.records.insert(artifact_id.clone(), record);
        for parent in unique_parents {
            self.children
                .entry(parent)
                .or_default()
                .insert(artifact_id.clone());
        }
        Ok(())
    }

    pub fn record(&self, artifact_id: &ArtifactId) -> Option<&ArtifactRecord> {
        self.records.get(artifact_id)
    }

    /// Returns all transitive descendants in canonical artifact-ID order.
    pub fn descendants_of(&self, artifact_id: &ArtifactId) -> Vec<ArtifactId> {
        let mut descendants = BTreeSet::new();
        let mut pending = Vec::from([artifact_id.clone()]);
        while let Some(parent) = pending.pop() {
            if let Some(children) = self.children.get(&parent) {
                for child in children {
                    if descendants.insert(child.clone()) {
                        pending.push(child.clone());
                    }
                }
            }
        }
        descendants.into_iter().collect()
    }
}

impl fmt::Display for ArtifactCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecord(error) => error.fmt(formatter),
            Self::ArtifactAlreadyExists => formatter.write_str("artifact already exists"),
            Self::UnknownParent(parent) => write!(formatter, "unknown artifact parent {parent}"),
            Self::DuplicateParent(parent) => {
                write!(formatter, "duplicate artifact parent {parent}")
            }
            Self::SelfDependency => formatter.write_str("artifact cannot depend on itself"),
        }
    }
}

impl fmt::Display for ArtifactRecordValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArtifactType => formatter.write_str("invalid versioned artifact type"),
            Self::SchemaIdentityMismatch => {
                formatter.write_str("schema identity does not match artifact type")
            }
            Self::InvalidSchemaVersion => formatter.write_str("invalid schema version"),
            Self::InvalidMediaType => formatter.write_str("invalid media type"),
            Self::InvalidProducerEventSequence => {
                formatter.write_str("invalid producer event sequence")
            }
            Self::InvalidRetentionClass => formatter.write_str("invalid retention class"),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::{format, string::ToString, vec};

    use super::*;

    const A: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const B: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
    const C: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";

    fn record(id: &str, sequence: u64) -> ArtifactRecord {
        ArtifactRecord {
            artifact_id: ArtifactId::parse(format!("art_{id}")).unwrap(),
            artifact_type: "approved-tdd/v1".to_string(),
            schema_id: "approved-tdd".to_string(),
            schema_version: 1,
            sha256: Sha256Digest::of_bytes(id.as_bytes()),
            size: id.len() as u64,
            media_type: "text/markdown".to_string(),
            producer_stage_instance_id: StageInstanceId::parse(format!("stage_{A}")).unwrap(),
            producer_attempt: 1,
            producer_event_sequence: sequence,
            pipeline_definition_digest: Sha256Digest::of_bytes(b"pipeline"),
            component_digest: Sha256Digest::of_bytes(b"component"),
            input_manifest_digest: Sha256Digest::of_bytes(b"inputs"),
            retention_class: "design-record".to_string(),
        }
    }

    #[test]
    fn validates_schema_identity_and_versioned_type() {
        let mut value = record(A, 1);
        assert_eq!(value.validate(), Ok(()));

        value.schema_version = 2;
        assert_eq!(
            value.validate(),
            Err(ArtifactRecordValidationError::InvalidSchemaVersion)
        );
        value.schema_version = 1;
        value.schema_id = "other".to_string();
        assert_eq!(
            value.validate(),
            Err(ArtifactRecordValidationError::SchemaIdentityMismatch)
        );
    }

    #[test]
    fn registration_is_append_only_and_rejects_unknown_or_duplicate_parents() {
        let root = record(A, 1);
        let root_id = root.artifact_id.clone();
        let child = record(B, 2);
        let child_id = child.artifact_id.clone();
        let mut catalog = ArtifactCatalog::new();

        assert_eq!(catalog.register(root.clone(), &[]), Ok(()));
        assert_eq!(
            catalog.register(root, &[]),
            Err(ArtifactCatalogError::ArtifactAlreadyExists)
        );
        assert_eq!(
            catalog.register(child.clone(), &[child_id]),
            Err(ArtifactCatalogError::SelfDependency)
        );
        assert_eq!(
            catalog.register(child.clone(), &[root_id.clone(), root_id.clone()]),
            Err(ArtifactCatalogError::DuplicateParent(root_id.clone()))
        );
        let unknown = ArtifactId::parse(format!("art_{C}")).unwrap();
        assert_eq!(
            catalog.register(child.clone(), core::slice::from_ref(&unknown)),
            Err(ArtifactCatalogError::UnknownParent(unknown))
        );
        assert_eq!(catalog.register(child, &[root_id]), Ok(()));
    }

    #[test]
    fn descendants_are_transitive_unique_and_deterministically_ordered() {
        let root = record(A, 1);
        let root_id = root.artifact_id.clone();
        let middle = record(B, 2);
        let middle_id = middle.artifact_id.clone();
        let leaf = record(C, 3);
        let leaf_id = leaf.artifact_id.clone();
        let mut catalog = ArtifactCatalog::new();
        catalog.register(root, &[]).unwrap();
        catalog
            .register(middle, core::slice::from_ref(&root_id))
            .unwrap();
        catalog
            .register(leaf, &[root_id.clone(), middle_id.clone()])
            .unwrap();

        assert_eq!(catalog.descendants_of(&root_id), vec![middle_id, leaf_id]);
    }
}
