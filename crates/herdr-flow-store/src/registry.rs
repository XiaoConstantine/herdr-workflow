use std::{collections::BTreeSet, str::FromStr};

use herdr_flow_core::{
    canonical_json, ArtifactCatalog, ArtifactCatalogError, ArtifactId, ArtifactRecord,
    ArtifactRecordValidationError, RunId, Sha256Digest, StageEvent, StageEventKind,
    StageInstanceId, StageState,
};
use rusqlite::{params, OptionalExtension, Transaction};

use crate::{from_sql_integer, to_sql_integer, ArtifactStore, ArtifactStoreError, StoreError};

pub struct ArtifactRegistration<'a> {
    pub record: &'a ArtifactRecord,
    pub parent_artifact_ids: &'a [ArtifactId],
    pub bytes: &'a [u8],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredArtifactRecord {
    pub run_id: RunId,
    pub record: ArtifactRecord,
    pub parent_artifact_ids: Vec<ArtifactId>,
    pub record_digest: Sha256Digest,
}

#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub(crate) struct ArtifactCommitment {
    pub artifact_id: ArtifactId,
    pub record_digest: Sha256Digest,
    pub parent_artifact_ids: Vec<ArtifactId>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedArtifact {
    pub(crate) record: ArtifactRecord,
    pub(crate) parent_artifact_ids: Vec<ArtifactId>,
    pub(crate) record_json: Vec<u8>,
    pub(crate) record_digest: Sha256Digest,
    pub(crate) bytes: Vec<u8>,
}

pub(crate) fn prepare_artifacts(
    artifact_store: &ArtifactStore,
    registrations: &[ArtifactRegistration<'_>],
) -> Result<Vec<PreparedArtifact>, StoreError> {
    let mut ids = BTreeSet::new();
    let mut prepared = Vec::with_capacity(registrations.len());
    for registration in registrations {
        registration
            .record
            .validate()
            .map_err(StoreError::ArtifactValidation)?;
        if !ids.insert(registration.record.artifact_id.clone()) {
            return Err(StoreError::ArtifactIdConflict);
        }
        let stored = artifact_store
            .put(registration.bytes)
            .map_err(StoreError::ArtifactStore)?;
        if stored.sha256 != registration.record.sha256 || stored.size != registration.record.size {
            return Err(StoreError::ArtifactMetadataMismatch(
                "record digest or size does not match durable bytes",
            ));
        }
        let value = serde_json::to_value(registration.record).map_err(StoreError::Serialization)?;
        let record_json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
        let mut parent_artifact_ids = registration.parent_artifact_ids.to_vec();
        parent_artifact_ids.sort();
        prepared.push(PreparedArtifact {
            record: registration.record.clone(),
            parent_artifact_ids,
            record_digest: Sha256Digest::of_bytes(&record_json),
            record_json,
            bytes: registration.bytes.to_vec(),
        });
    }
    Ok(prepared)
}

pub(crate) fn artifact_commitments(artifacts: &[PreparedArtifact]) -> Vec<ArtifactCommitment> {
    let mut commitments = artifacts
        .iter()
        .map(|artifact| ArtifactCommitment {
            artifact_id: artifact.record.artifact_id.clone(),
            record_digest: artifact.record_digest,
            parent_artifact_ids: artifact.parent_artifact_ids.clone(),
        })
        .collect::<Vec<_>>();
    commitments.sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
    commitments
}

pub(crate) fn validate_new_artifacts(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    producer_state: &StageState,
    next_state: &StageState,
    event: &StageEvent,
    event_sequence: u64,
    artifacts: &[PreparedArtifact],
) -> Result<(), StoreError> {
    let pipeline_definition_digest = run_pipeline_definition_digest(transaction, run_id)?;
    let mut preceding_batch_ids = BTreeSet::new();
    for artifact in artifacts {
        let record = &artifact.record;
        if record.producer_stage_instance_id != producer_state.stage_instance_id {
            return Err(StoreError::ArtifactMetadataMismatch(
                "producer stage does not match accepted event",
            ));
        }
        match &event.kind {
            StageEventKind::NodeReady {
                input_manifest_digest,
            } => {
                let manifest: herdr_flow_core::StageInputManifest =
                    serde_json::from_slice(&artifact.bytes).map_err(StoreError::Serialization)?;
                let canonical = manifest
                    .canonical_bytes()
                    .map_err(StoreError::Canonicalization)?;
                let mut expected_parents = manifest
                    .artifacts
                    .iter()
                    .map(|input| input.artifact_id.clone())
                    .collect::<Vec<_>>();
                expected_parents.sort();
                expected_parents.dedup();
                if canonical != artifact.bytes
                    || manifest.stage_instance_id != producer_state.stage_instance_id
                    || record.producer_attempt != 0
                    || record.sha256 != *input_manifest_digest
                    || Sha256Digest::of_bytes(&canonical) != *input_manifest_digest
                    || artifact.parent_artifact_ids != expected_parents
                {
                    return Err(StoreError::ArtifactMetadataMismatch(
                        "stage input manifest is not bound to the ready event and exact inputs",
                    ));
                }
            }
            _ if next_state.attempt > 0 && record.producer_attempt == next_state.attempt => {}
            _ => {
                return Err(StoreError::ArtifactMetadataMismatch(
                    "producer attempt does not match accepted event",
                ));
            }
        }
        if record.producer_event_sequence != event_sequence {
            return Err(StoreError::ArtifactMetadataMismatch(
                "producer sequence does not match accepted event",
            ));
        }
        if record.component_digest != producer_state.component_digest {
            return Err(StoreError::ArtifactMetadataMismatch(
                "component digest does not match producer stage",
            ));
        }
        if record.pipeline_definition_digest != pipeline_definition_digest {
            return Err(StoreError::ArtifactMetadataMismatch(
                "pipeline definition digest does not match the run",
            ));
        }
        if next_state.input_manifest_digest != Some(record.input_manifest_digest) {
            return Err(StoreError::ArtifactMetadataMismatch(
                "input manifest does not match producer stage",
            ));
        }
        if artifact_run_id(transaction, &record.artifact_id)?.is_some() {
            return Err(StoreError::ArtifactIdConflict);
        }

        let mut unique_parents = BTreeSet::new();
        for parent in &artifact.parent_artifact_ids {
            if parent == &record.artifact_id {
                return Err(StoreError::ArtifactGraph(
                    ArtifactCatalogError::SelfDependency,
                ));
            }
            if !unique_parents.insert(parent.clone()) {
                return Err(StoreError::ArtifactGraph(
                    ArtifactCatalogError::DuplicateParent(parent.clone()),
                ));
            }
            if preceding_batch_ids.contains(parent) {
                continue;
            }
            match artifact_run_id(transaction, parent)? {
                Some(parent_run_id) if parent_run_id == *run_id => {}
                Some(_) => return Err(StoreError::ArtifactRunMismatch),
                None => {
                    return Err(StoreError::ArtifactGraph(
                        ArtifactCatalogError::UnknownParent(parent.clone()),
                    ));
                }
            }
        }
        preceding_batch_ids.insert(record.artifact_id.clone());
    }
    Ok(())
}

pub(crate) fn validate_event_artifact_references(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    event: &StageEvent,
    artifacts: &[PreparedArtifact],
) -> Result<(), StoreError> {
    let required = match &event.kind {
        StageEventKind::NodeReady {
            input_manifest_digest,
        } => vec![*input_manifest_digest],
        StageEventKind::NodeBlocked { reason_digest }
        | StageEventKind::NodeFailed { reason_digest }
        | StageEventKind::NodePaused { reason_digest } => vec![*reason_digest],
        StageEventKind::NodeCompleted {
            output_manifest_digest,
            completion_evidence_digest,
            ..
        } => vec![*output_manifest_digest, *completion_evidence_digest],
        StageEventKind::NodeInvalidated { cause_digest } => vec![*cause_digest],
        StageEventKind::NodeProvisioning
        | StageEventKind::NodeStarted { .. }
        | StageEventKind::NodeResumed
        | StageEventKind::NodeReconciled { .. } => Vec::new(),
    };
    for digest in required {
        if artifacts
            .iter()
            .any(|artifact| artifact.record.sha256 == digest)
        {
            continue;
        }
        let exists = transaction
            .query_row(
                "SELECT 1 FROM artifacts WHERE run_id = ?1 AND sha256 = ?2 LIMIT 1",
                params![run_id.as_str(), digest.to_prefixed_string()],
                |_| Ok(()),
            )
            .optional()
            .map_err(StoreError::Sqlite)?
            .is_some();
        if !exists {
            return Err(StoreError::ArtifactReferenceNotFound);
        }
    }
    Ok(())
}

pub(crate) fn insert_artifacts(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    artifacts: &[PreparedArtifact],
) -> Result<(), StoreError> {
    for artifact in artifacts {
        let record = &artifact.record;
        claim_artifact_identity(transaction, run_id, record)?;
        transaction
            .execute(
                "INSERT INTO artifacts(
                    artifact_id, run_id, sha256, size, producer_stage_instance_id,
                    producer_event_sequence, record_digest, record_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    record.artifact_id.as_str(),
                    run_id.as_str(),
                    record.sha256.to_prefixed_string(),
                    to_sql_integer(record.size)?,
                    record.producer_stage_instance_id.as_str(),
                    to_sql_integer(record.producer_event_sequence)?,
                    artifact.record_digest.to_prefixed_string(),
                    artifact.record_json,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        for parent in &artifact.parent_artifact_ids {
            let ingress_parent: bool = transaction
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM run_ingress_artifacts
                        WHERE artifact_id = ?1 AND run_id = ?2
                     )",
                    params![parent.as_str(), run_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(StoreError::Sqlite)?;
            let table = if ingress_parent {
                "artifact_ingress_edges"
            } else {
                "artifact_edges"
            };
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {table}(run_id, parent_artifact_id, child_artifact_id)
                         VALUES (?1, ?2, ?3)"
                    ),
                    params![
                        run_id.as_str(),
                        parent.as_str(),
                        record.artifact_id.as_str()
                    ],
                )
                .map_err(StoreError::Sqlite)?;
        }
    }
    Ok(())
}

fn claim_artifact_identity(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    record: &ArtifactRecord,
) -> Result<(), StoreError> {
    type Identity = (String, String, Option<String>, Option<String>);
    let identity: Option<Identity> = transaction
        .query_row(
            "SELECT run_id, identity_kind, expected_producer_stage_instance_id,
                    expected_artifact_type
             FROM artifact_identities WHERE artifact_id = ?1",
            params![record.artifact_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    match identity {
        None => {
            transaction
                .execute(
                    "INSERT INTO artifact_identities(
                        artifact_id, run_id, identity_kind,
                        expected_producer_stage_instance_id, expected_artifact_type
                     ) VALUES (?1, ?2, 'REGULAR', NULL, NULL)",
                    params![record.artifact_id.as_str(), run_id.as_str()],
                )
                .map_err(StoreError::Sqlite)?;
        }
        Some((stored_run, kind, producer, artifact_type))
            if stored_run == run_id.as_str()
                && kind == "RESERVED"
                && producer.as_deref() == Some(record.producer_stage_instance_id.as_str())
                && artifact_type.as_deref() == Some(record.artifact_type.as_str()) =>
        {
            let updated = transaction
                .execute(
                    "UPDATE artifact_identities
                     SET identity_kind = 'REGULAR',
                         expected_producer_stage_instance_id = NULL,
                         expected_artifact_type = NULL
                     WHERE artifact_id = ?1 AND identity_kind = 'RESERVED'",
                    params![record.artifact_id.as_str()],
                )
                .map_err(StoreError::Sqlite)?;
            if updated != 1 {
                return Err(StoreError::ArtifactIdConflict);
            }
        }
        Some(_) => return Err(StoreError::ArtifactIdConflict),
    }
    Ok(())
}

pub(crate) fn verify_committed_artifacts(
    transaction: &rusqlite::Connection,
    run_id: &RunId,
    event_sequence: u64,
    producer_stage_instance_id: &StageInstanceId,
    commitments: &[ArtifactCommitment],
) -> Result<(), StoreError> {
    let count: i64 = transaction
        .query_row(
            "SELECT COUNT(*) FROM artifacts
             WHERE run_id = ?1 AND producer_event_sequence = ?2",
            params![run_id.as_str(), to_sql_integer(event_sequence)?],
            |row| row.get(0),
        )
        .map_err(StoreError::Sqlite)?;
    if from_sql_integer(count)? != commitments.len() as u64 {
        return Err(StoreError::ArtifactMetadataMismatch(
            "event artifact commitment count mismatch",
        ));
    }
    let pipeline_definition_digest = run_pipeline_definition_digest(transaction, run_id)?;
    for commitment in commitments {
        let stored = load_artifact_record(transaction, run_id, &commitment.artifact_id)?
            .ok_or(StoreError::ArtifactNotFound)?;
        if stored.record.pipeline_definition_digest != pipeline_definition_digest {
            return Err(StoreError::ArtifactMetadataMismatch(
                "pipeline definition digest does not match the run",
            ));
        }
        if stored.record.producer_stage_instance_id != *producer_stage_instance_id {
            return Err(StoreError::ArtifactMetadataMismatch(
                "producer stage does not match the committed event",
            ));
        }
        if stored.record.producer_event_sequence != event_sequence
            || stored.parent_artifact_ids != commitment.parent_artifact_ids
            || stored.record_digest != commitment.record_digest
        {
            return Err(StoreError::ArtifactMetadataMismatch(
                "event artifact commitment mismatch",
            ));
        }
    }
    Ok(())
}

pub(crate) fn load_and_verify_run_artifacts(
    transaction: &Transaction<'_>,
    run_id: &RunId,
) -> Result<Vec<StoredArtifactRecord>, StoreError> {
    let mut statement = transaction
        .prepare(
            "SELECT artifact_id
             FROM artifacts
             WHERE run_id = ?1
             ORDER BY producer_event_sequence, artifact_id",
        )
        .map_err(StoreError::Sqlite)?;
    let ids = statement
        .query_map(params![run_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(StoreError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Sqlite)?;
    drop(statement);

    let pipeline_definition_digest = run_pipeline_definition_digest(transaction, run_id)?;
    let mut pending = Vec::with_capacity(ids.len());
    for id in ids {
        let artifact_id = ArtifactId::from_str(&id).map_err(StoreError::Identifier)?;
        let stored = load_artifact_record(transaction, run_id, &artifact_id)?
            .ok_or(StoreError::ArtifactNotFound)?;
        if stored.record.pipeline_definition_digest != pipeline_definition_digest {
            return Err(StoreError::ArtifactMetadataMismatch(
                "pipeline definition digest does not match the run",
            ));
        }
        let identity: Option<(String, String)> = transaction
            .query_row(
                "SELECT run_id, identity_kind FROM artifact_identities WHERE artifact_id = ?1",
                params![stored.record.artifact_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(StoreError::Sqlite)?;
        if identity
            .as_ref()
            .map(|(run, kind)| (run.as_str(), kind.as_str()))
            != Some((run_id.as_str(), "REGULAR"))
        {
            return Err(StoreError::ArtifactIdConflict);
        }
        pending.push(stored);
    }

    let mut catalog = ArtifactCatalog::new();
    let mut accepted_ids = BTreeSet::new();
    let mut stored_records = crate::coordinator::load_run_ingress_records(transaction, run_id)?;
    for stored in &stored_records {
        catalog
            .register(stored.record.clone(), &stored.parent_artifact_ids)
            .map_err(StoreError::ArtifactGraph)?;
        accepted_ids.insert(stored.record.artifact_id.clone());
    }
    stored_records.reserve(pending.len());
    while !pending.is_empty() {
        let Some(index) = pending.iter().position(|stored| {
            stored
                .parent_artifact_ids
                .iter()
                .all(|parent| accepted_ids.contains(parent))
        }) else {
            let parent = pending[0]
                .parent_artifact_ids
                .iter()
                .find(|parent| !accepted_ids.contains(*parent))
                .cloned()
                .ok_or(StoreError::ArtifactGraph(
                    ArtifactCatalogError::SelfDependency,
                ))?;
            return Err(StoreError::ArtifactGraph(
                ArtifactCatalogError::UnknownParent(parent),
            ));
        };
        let stored = pending.remove(index);
        catalog
            .register(stored.record.clone(), &stored.parent_artifact_ids)
            .map_err(StoreError::ArtifactGraph)?;
        accepted_ids.insert(stored.record.artifact_id.clone());
        stored_records.push(stored);
    }
    Ok(stored_records)
}

pub(crate) fn load_artifact_record(
    transaction: &rusqlite::Connection,
    run_id: &RunId,
    artifact_id: &ArtifactId,
) -> Result<Option<StoredArtifactRecord>, StoreError> {
    type RawArtifact = (String, String, i64, String, i64, String, Vec<u8>);
    let row: Option<RawArtifact> = transaction
        .query_row(
            "SELECT sha256, run_id, size, producer_stage_instance_id,
                    producer_event_sequence, record_digest, record_json
             FROM artifacts WHERE artifact_id = ?1",
            params![artifact_id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let Some((
        stored_sha256,
        stored_run_id,
        stored_size,
        stored_producer_stage_instance_id,
        stored_producer_event_sequence,
        stored_record_digest,
        record_json,
    )) = row
    else {
        return Ok(None);
    };
    let stored_run_id = RunId::from_str(&stored_run_id).map_err(StoreError::Identifier)?;
    if stored_run_id != *run_id {
        return Err(StoreError::ArtifactRunMismatch);
    }
    let stored_producer_stage_instance_id =
        StageInstanceId::from_str(&stored_producer_stage_instance_id)
            .map_err(StoreError::Identifier)?;
    let record: ArtifactRecord =
        serde_json::from_slice(&record_json).map_err(StoreError::Serialization)?;
    record.validate().map_err(StoreError::ArtifactValidation)?;
    if record.artifact_id != *artifact_id
        || record.sha256.to_prefixed_string() != stored_sha256
        || record.size != from_sql_integer(stored_size)?
        || record.producer_stage_instance_id != stored_producer_stage_instance_id
        || record.producer_event_sequence != from_sql_integer(stored_producer_event_sequence)?
    {
        return Err(StoreError::ArtifactMetadataMismatch(
            "indexed artifact columns do not match canonical record",
        ));
    }
    let value = serde_json::to_value(&record).map_err(StoreError::Serialization)?;
    let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    if canonical != record_json {
        return Err(StoreError::ArtifactMetadataMismatch(
            "artifact record is not canonical JSON",
        ));
    }
    let record_digest =
        Sha256Digest::from_str(&stored_record_digest).map_err(StoreError::Digest)?;
    if Sha256Digest::of_bytes(&canonical) != record_digest {
        return Err(StoreError::ArtifactMetadataMismatch(
            "artifact record digest mismatch",
        ));
    }

    let mut statement = transaction
        .prepare(
            "SELECT parent_artifact_id FROM artifact_edges
             WHERE run_id = ?1 AND child_artifact_id = ?2
             UNION ALL
             SELECT parent_artifact_id FROM artifact_ingress_edges
             WHERE run_id = ?1 AND child_artifact_id = ?2
             ORDER BY parent_artifact_id",
        )
        .map_err(StoreError::Sqlite)?;
    let parent_artifact_ids = statement
        .query_map(params![run_id.as_str(), artifact_id.as_str()], |row| {
            row.get::<_, String>(0)
        })
        .map_err(StoreError::Sqlite)?
        .map(|value| {
            ArtifactId::from_str(&value.map_err(StoreError::Sqlite)?)
                .map_err(StoreError::Identifier)
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some(StoredArtifactRecord {
        run_id: stored_run_id,
        record,
        parent_artifact_ids,
        record_digest,
    }))
}

fn run_pipeline_definition_digest(
    connection: &rusqlite::Connection,
    run_id: &RunId,
) -> Result<Sha256Digest, StoreError> {
    let value = connection
        .query_row(
            "SELECT pipeline_definition_digest FROM runs WHERE run_id = ?1",
            params![run_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)?
        .ok_or(StoreError::RunNotFound)?;
    Sha256Digest::from_str(&value).map_err(StoreError::Digest)
}

fn artifact_run_id(
    transaction: &Transaction<'_>,
    artifact_id: &ArtifactId,
) -> Result<Option<RunId>, StoreError> {
    transaction
        .query_row(
            "SELECT run_id FROM artifacts WHERE artifact_id = ?1
             UNION ALL
             SELECT run_id FROM run_ingress_artifacts WHERE artifact_id = ?1",
            params![artifact_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)?
        .map(|value| RunId::from_str(&value).map_err(StoreError::Identifier))
        .transpose()
}

pub(crate) fn verify_artifact_bytes(
    artifact_store: &ArtifactStore,
    records: &[StoredArtifactRecord],
) -> Result<(), StoreError> {
    for stored in records {
        let bytes = artifact_store
            .read_verified(stored.record.sha256)
            .map_err(StoreError::ArtifactStore)?;
        let size = u64::try_from(bytes.len()).map_err(|_| {
            StoreError::ArtifactMetadataMismatch("verified artifact size exceeds this platform")
        })?;
        if size != stored.record.size {
            return Err(StoreError::ArtifactMetadataMismatch(
                "verified bytes do not match artifact record size",
            ));
        }
    }
    Ok(())
}

impl From<ArtifactStoreError> for StoreError {
    fn from(error: ArtifactStoreError) -> Self {
        Self::ArtifactStore(error)
    }
}

impl From<ArtifactRecordValidationError> for StoreError {
    fn from(error: ArtifactRecordValidationError) -> Self {
        Self::ArtifactValidation(error)
    }
}
