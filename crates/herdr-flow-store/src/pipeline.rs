use std::{collections::BTreeSet, str::FromStr};

use herdr_flow_core::{
    canonical_json, replay_pipeline, BatchId, EventId, MessageId, PipelineEvent, PipelineEventKind,
    PipelineState, RunId, Sha256Digest, StageEvent, MAX_CONTROL_REVISION,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::{
    event_by_message_id, event_id_exists, from_sql_integer, registry, require_run, to_sql_integer,
    verified_stage, verify_run_journal, ArtifactRegistration, ArtifactStore,
    CanonicalCommittedEvent, SqliteStore, StoreError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredPipelineEvent {
    pub event_id: EventId,
    pub run_id: RunId,
    pub sequence: u64,
    pub message_id: MessageId,
    pub message_digest: Sha256Digest,
    pub event_digest: Sha256Digest,
    pub event: PipelineEvent,
}

pub struct SemanticStageEntry<'a> {
    pub event_id: &'a EventId,
    pub message_id: &'a MessageId,
    pub message_digest: &'a Sha256Digest,
    pub event: &'a StageEvent,
    pub artifacts: &'a [ArtifactRegistration<'a>],
}

pub struct SemanticPipelineEntry<'a> {
    pub event_id: &'a EventId,
    pub message_id: &'a MessageId,
    pub message_digest: &'a Sha256Digest,
    pub event: &'a PipelineEvent,
}

pub struct SemanticBatch<'a> {
    pub batch_id: &'a BatchId,
    pub run_id: &'a RunId,
    pub stage_entries: &'a [SemanticStageEntry<'a>],
    pub pipeline_entries: &'a [SemanticPipelineEntry<'a>],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticCommitOutcome {
    Committed,
    Duplicate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CommittedSemanticStage {
    event_id: EventId,
    message_id: MessageId,
    message_digest: Sha256Digest,
    event: StageEvent,
    artifact_commitments: Vec<registry::ArtifactCommitment>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CommittedSemanticPipeline {
    event_id: EventId,
    message_id: MessageId,
    message_digest: Sha256Digest,
    event: PipelineEvent,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CanonicalSemanticBatch {
    batch_id: BatchId,
    run_id: RunId,
    first_sequence: u64,
    stage_entries: Vec<CommittedSemanticStage>,
    pipeline_entries: Vec<CommittedSemanticPipeline>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CanonicalCommittedPipelineEvent {
    event_id: EventId,
    run_id: RunId,
    sequence: u64,
    message_id: MessageId,
    message_digest: Sha256Digest,
    prior_control_revision: u64,
    event: PipelineEvent,
}

impl SqliteStore {
    pub fn register_pipeline(
        &mut self,
        run_id: &RunId,
        state: &PipelineState,
    ) -> Result<(), StoreError> {
        if !state.is_pristine() {
            return Err(StoreError::InvalidInitialPipeline);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        require_run(&transaction, run_id)?;
        verify_run_journal(&transaction, run_id)?;
        let stage_root_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM stage_snapshots WHERE run_id = ?1",
                params![run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        if stage_root_count != 0 {
            return Err(StoreError::PipelineRootConflict);
        }
        let run_digest: String = transaction
            .query_row(
                "SELECT pipeline_definition_digest FROM runs WHERE run_id = ?1",
                params![run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        if Sha256Digest::from_str(&run_digest).map_err(StoreError::Digest)?
            != state.definition_digest
        {
            return Err(StoreError::PipelineDefinitionMismatch);
        }
        let state_json = serde_json::to_vec(state).map_err(StoreError::Serialization)?;
        for stage in state.stage_states() {
            let stage_json = serde_json::to_vec(&stage).map_err(StoreError::Serialization)?;
            transaction
                .execute(
                    "INSERT INTO stage_snapshots(
                        stage_instance_id, run_id, control_revision,
                        initial_state_json, state_json
                     ) VALUES (?1, ?2, ?3, ?4, ?4)",
                    params![
                        stage.stage_instance_id.as_str(),
                        run_id.as_str(),
                        to_sql_integer(stage.control_revision)?,
                        stage_json,
                    ],
                )
                .map_err(StoreError::Sqlite)?;
        }
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO pipeline_snapshots(
                    run_id, control_revision, initial_state_json, state_json
                 ) VALUES (?1, ?2, ?3, ?3)",
                params![
                    run_id.as_str(),
                    to_sql_integer(state.control_revision)?,
                    state_json
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if inserted == 0 {
            return Err(StoreError::PipelineAlreadyExists);
        }
        transaction.commit().map_err(StoreError::Sqlite)
    }

    pub fn append_semantic_batch(
        &mut self,
        artifact_store: &ArtifactStore,
        batch: SemanticBatch<'_>,
    ) -> Result<SemanticCommitOutcome, StoreError> {
        if batch.pipeline_entries.is_empty() {
            return Err(StoreError::EmptySemanticBatch);
        }
        self.verify_run_artifacts(batch.run_id, artifact_store)?;
        let prepared_artifacts = batch
            .stage_entries
            .iter()
            .map(|entry| registry::prepare_artifacts(artifact_store, entry.artifacts))
            .collect::<Result<Vec<_>, _>>()?;
        validate_semantic_batch_shape(&batch)?;

        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, batch.run_id)?;
        let next_sequence: i64 = transaction
            .query_row(
                "SELECT next_event_sequence FROM runs WHERE run_id = ?1",
                params![batch.run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        let first_sequence = from_sql_integer(next_sequence)?;
        let committed_first_sequence = semantic_batch_first_sequence(&transaction, batch.batch_id)?;
        let commitment = semantic_batch_commitment(
            &batch,
            &prepared_artifacts,
            committed_first_sequence.unwrap_or(first_sequence),
        );
        let commitment_value =
            serde_json::to_value(&commitment).map_err(StoreError::Serialization)?;
        let commitment_json =
            canonical_json::to_vec(&commitment_value).map_err(StoreError::Canonicalization)?;
        let commitment_digest = Sha256Digest::of_bytes(&commitment_json);
        if semantic_batch_is_duplicate(
            &transaction,
            &commitment,
            &commitment_json,
            commitment_digest,
        )? {
            return Ok(SemanticCommitOutcome::Duplicate);
        }

        let entry_count = batch
            .stage_entries
            .len()
            .checked_add(batch.pipeline_entries.len())
            .ok_or(StoreError::EventSequenceExhausted)?;
        let final_sequence = first_sequence
            .checked_add(
                u64::try_from(entry_count).map_err(|_| StoreError::EventSequenceExhausted)?,
            )
            .ok_or(StoreError::EventSequenceExhausted)?;
        if final_sequence > MAX_CONTROL_REVISION {
            return Err(StoreError::EventSequenceExhausted);
        }

        let initial_pipeline = verified_pipeline(&transaction, batch.run_id)?;
        let mut staged_states = Vec::with_capacity(batch.stage_entries.len());
        let mut sequence = first_sequence;
        for (entry, artifacts) in batch.stage_entries.iter().zip(&prepared_artifacts) {
            let state = verified_stage(&transaction, batch.run_id, &entry.event.stage_instance_id)?;
            let next_state = state
                .apply(entry.event)
                .map_err(StoreError::StageTransition)?;
            let commitments = registry::artifact_commitments(artifacts);
            registry::validate_new_artifacts(
                &transaction,
                batch.run_id,
                &state,
                &next_state,
                entry.event,
                sequence,
                artifacts,
            )?;
            registry::validate_event_artifact_references(
                &transaction,
                batch.run_id,
                entry.event,
                artifacts,
            )?;
            insert_stage_event(&transaction, batch.run_id, sequence, entry, &commitments)?;
            registry::insert_artifacts(&transaction, batch.run_id, artifacts)?;
            let next_state_json =
                serde_json::to_vec(&next_state).map_err(StoreError::Serialization)?;
            let updated = transaction
                .execute(
                    "UPDATE stage_snapshots
                     SET control_revision = ?1, state_json = ?2
                     WHERE stage_instance_id = ?3 AND run_id = ?4 AND control_revision = ?5",
                    params![
                        to_sql_integer(next_state.control_revision)?,
                        next_state_json,
                        next_state.stage_instance_id.as_str(),
                        batch.run_id.as_str(),
                        to_sql_integer(state.control_revision)?,
                    ],
                )
                .map_err(StoreError::Sqlite)?;
            if updated != 1 {
                return Err(StoreError::ConcurrentUpdate);
            }
            staged_states.push((next_state.stage_instance_id.clone(), next_state));
            sequence += 1;
        }

        let mut pipeline_state = initial_pipeline.clone();
        for entry in batch.pipeline_entries {
            let next_state = pipeline_state
                .apply(entry.event)
                .map_err(StoreError::PipelineTransition)?;
            validate_pipeline_artifact_acceptance(&transaction, batch.run_id, entry.event)?;
            insert_pipeline_event(&transaction, batch.run_id, sequence, entry)?;
            pipeline_state = next_state;
            sequence += 1;
        }
        for (stage_id, staged_state) in staged_states {
            if pipeline_state.stage(&stage_id) != Some(&staged_state) {
                return Err(StoreError::PipelineStageMismatch);
            }
        }

        transaction
            .execute(
                "INSERT INTO semantic_batches(batch_id, run_id, batch_digest, batch_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    batch.batch_id.as_str(),
                    batch.run_id.as_str(),
                    commitment_digest.to_prefixed_string(),
                    commitment_json,
                ],
            )
            .map_err(StoreError::Sqlite)?;

        #[cfg(test)]
        if self.fail_after_event_insert {
            return Err(StoreError::CorruptData("injected semantic batch failure"));
        }
        let pipeline_json =
            serde_json::to_vec(&pipeline_state).map_err(StoreError::Serialization)?;
        let pipeline_updated = transaction
            .execute(
                "UPDATE pipeline_snapshots
                 SET control_revision = ?1, state_json = ?2
                 WHERE run_id = ?3 AND control_revision = ?4",
                params![
                    to_sql_integer(pipeline_state.control_revision)?,
                    pipeline_json,
                    batch.run_id.as_str(),
                    to_sql_integer(initial_pipeline.control_revision)?,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if pipeline_updated != 1 {
            return Err(StoreError::ConcurrentUpdate);
        }
        let run_updated = transaction
            .execute(
                "UPDATE runs SET next_event_sequence = ?1
                 WHERE run_id = ?2 AND next_event_sequence = ?3",
                params![
                    to_sql_integer(final_sequence)?,
                    batch.run_id.as_str(),
                    next_sequence,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if run_updated != 1 {
            return Err(StoreError::ConcurrentUpdate);
        }
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(SemanticCommitOutcome::Committed)
    }

    pub fn load_pipeline(&self, run_id: &RunId) -> Result<PipelineState, StoreError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, run_id)?;
        let state = verified_pipeline(&transaction, run_id)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(state)
    }

    #[cfg(test)]
    pub(crate) fn load_pipeline_events(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<StoredPipelineEvent>, StoreError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, run_id)?;
        let events = load_pipeline_events(&transaction, run_id)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(events)
    }
}

fn validate_semantic_batch_shape(batch: &SemanticBatch<'_>) -> Result<(), StoreError> {
    let mut embedded_stage_events = Vec::new();
    for entry in batch.pipeline_entries {
        match &entry.event.kind {
            PipelineEventKind::StageScheduled { stage_event, .. }
            | PipelineEventKind::StageEventObserved { stage_event } => {
                embedded_stage_events.push(stage_event)
            }
            PipelineEventKind::ArtifactInvalidated {
                invalidated_stage_events,
                ..
            } => embedded_stage_events.extend(invalidated_stage_events),
            PipelineEventKind::ArtifactAccepted { .. }
            | PipelineEventKind::ArtifactInvalidationFinalized { .. } => {}
        }
    }
    if embedded_stage_events.len() != batch.stage_entries.len()
        || embedded_stage_events
            .iter()
            .zip(batch.stage_entries)
            .any(|(embedded, entry)| *embedded != entry.event)
    {
        return Err(StoreError::PipelineStageMismatch);
    }

    let mut event_ids = BTreeSet::new();
    let mut message_ids = BTreeSet::new();
    for (event_id, message_id) in batch
        .stage_entries
        .iter()
        .map(|entry| (entry.event_id, entry.message_id))
        .chain(
            batch
                .pipeline_entries
                .iter()
                .map(|entry| (entry.event_id, entry.message_id)),
        )
    {
        if !event_ids.insert(event_id.clone()) {
            return Err(StoreError::EventIdConflict);
        }
        if !message_ids.insert(message_id.clone()) {
            return Err(StoreError::MessageIdConflict);
        }
    }
    Ok(())
}

fn semantic_batch_commitment(
    batch: &SemanticBatch<'_>,
    prepared_artifacts: &[Vec<registry::PreparedArtifact>],
    first_sequence: u64,
) -> CanonicalSemanticBatch {
    CanonicalSemanticBatch {
        batch_id: batch.batch_id.clone(),
        run_id: batch.run_id.clone(),
        first_sequence,
        stage_entries: batch
            .stage_entries
            .iter()
            .zip(prepared_artifacts)
            .map(|(entry, artifacts)| CommittedSemanticStage {
                event_id: entry.event_id.clone(),
                message_id: entry.message_id.clone(),
                message_digest: *entry.message_digest,
                event: entry.event.clone(),
                artifact_commitments: registry::artifact_commitments(artifacts),
            })
            .collect(),
        pipeline_entries: batch
            .pipeline_entries
            .iter()
            .map(|entry| CommittedSemanticPipeline {
                event_id: entry.event_id.clone(),
                message_id: entry.message_id.clone(),
                message_digest: *entry.message_digest,
                event: entry.event.clone(),
            })
            .collect(),
    }
}

fn semantic_batch_first_sequence(
    connection: &Connection,
    batch_id: &BatchId,
) -> Result<Option<u64>, StoreError> {
    let json: Option<Vec<u8>> = connection
        .query_row(
            "SELECT batch_json FROM semantic_batches WHERE batch_id = ?1",
            params![batch_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    json.map(|value| {
        serde_json::from_slice::<CanonicalSemanticBatch>(&value)
            .map(|batch| batch.first_sequence)
            .map_err(StoreError::Serialization)
    })
    .transpose()
}

fn semantic_batch_is_duplicate(
    transaction: &Transaction<'_>,
    commitment: &CanonicalSemanticBatch,
    commitment_json: &[u8],
    commitment_digest: Sha256Digest,
) -> Result<bool, StoreError> {
    let existing: Option<(String, String, Vec<u8>)> = transaction
        .query_row(
            "SELECT run_id, batch_digest, batch_json
             FROM semantic_batches WHERE batch_id = ?1",
            params![commitment.batch_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    if let Some((run_id, digest, json)) = existing {
        if run_id != commitment.run_id.as_str()
            || digest != commitment_digest.to_prefixed_string()
            || json != commitment_json
        {
            return Err(StoreError::SemanticBatchConflict);
        }
        verify_semantic_batch_entries(transaction, commitment)?;
        return Ok(true);
    }

    for stage in &commitment.stage_entries {
        if event_id_exists(transaction, &stage.event_id)?
            || event_by_message_id(transaction, &stage.message_id)?.is_some()
            || pipeline_message_id_exists(transaction, &stage.message_id)?
        {
            return Err(StoreError::PartialSemanticBatch);
        }
    }
    for pipeline in &commitment.pipeline_entries {
        if event_id_exists(transaction, &pipeline.event_id)?
            || event_by_message_id(transaction, &pipeline.message_id)?.is_some()
            || pipeline_event_by_message_id(transaction, &pipeline.message_id)?.is_some()
        {
            return Err(StoreError::PartialSemanticBatch);
        }
    }
    Ok(false)
}

fn verify_semantic_batch_entries(
    connection: &Connection,
    commitment: &CanonicalSemanticBatch,
) -> Result<(), StoreError> {
    for (index, stage) in commitment.stage_entries.iter().enumerate() {
        let expected_sequence = commitment
            .first_sequence
            .checked_add(u64::try_from(index).map_err(|_| StoreError::EventSequenceExhausted)?)
            .ok_or(StoreError::EventSequenceExhausted)?;
        let existing = event_by_message_id(connection, &stage.message_id)?
            .ok_or(StoreError::PartialSemanticBatch)?;
        if existing.event_id != stage.event_id
            || existing.run_id != commitment.run_id
            || existing.sequence != expected_sequence
            || existing.message_digest != stage.message_digest
            || existing.event != stage.event
            || existing.artifact_commitments != stage.artifact_commitments
        {
            return Err(StoreError::SemanticBatchConflict);
        }
        registry::verify_committed_artifacts(
            connection,
            &commitment.run_id,
            existing.sequence,
            &existing.event.stage_instance_id,
            &stage.artifact_commitments,
        )?;
    }
    let pipeline_offset = commitment
        .first_sequence
        .checked_add(
            u64::try_from(commitment.stage_entries.len())
                .map_err(|_| StoreError::EventSequenceExhausted)?,
        )
        .ok_or(StoreError::EventSequenceExhausted)?;
    for (index, pipeline) in commitment.pipeline_entries.iter().enumerate() {
        let expected_sequence = pipeline_offset
            .checked_add(u64::try_from(index).map_err(|_| StoreError::EventSequenceExhausted)?)
            .ok_or(StoreError::EventSequenceExhausted)?;
        let existing = pipeline_event_by_message_id(connection, &pipeline.message_id)?
            .ok_or(StoreError::PartialSemanticBatch)?;
        if existing.event_id != pipeline.event_id
            || existing.run_id != commitment.run_id
            || existing.sequence != expected_sequence
            || existing.message_digest != pipeline.message_digest
            || existing.event != pipeline.event
        {
            return Err(StoreError::SemanticBatchConflict);
        }
    }
    Ok(())
}

fn insert_stage_event(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    sequence: u64,
    entry: &SemanticStageEntry<'_>,
    artifact_commitments: &[registry::ArtifactCommitment],
) -> Result<(), StoreError> {
    let canonical_record = CanonicalCommittedEvent {
        event_id: entry.event_id.clone(),
        run_id: run_id.clone(),
        sequence,
        message_id: entry.message_id.clone(),
        message_digest: *entry.message_digest,
        stage_instance_id: entry.event.stage_instance_id.clone(),
        prior_control_revision: entry.event.prior_control_revision,
        artifact_commitments: artifact_commitments.to_vec(),
        event: entry.event.clone(),
    };
    let value = serde_json::to_value(&canonical_record).map_err(StoreError::Serialization)?;
    let event_json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    let event_digest = Sha256Digest::of_bytes(&event_json);
    transaction
        .execute(
            "INSERT INTO events(
                event_id, run_id, sequence, message_id, message_digest,
                stage_instance_id, prior_control_revision, event_digest, event_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                entry.event_id.as_str(),
                run_id.as_str(),
                to_sql_integer(sequence)?,
                entry.message_id.as_str(),
                entry.message_digest.to_prefixed_string(),
                entry.event.stage_instance_id.as_str(),
                to_sql_integer(entry.event.prior_control_revision)?,
                event_digest.to_prefixed_string(),
                event_json,
            ],
        )
        .map_err(StoreError::Sqlite)?;
    Ok(())
}

fn insert_pipeline_event(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    sequence: u64,
    entry: &SemanticPipelineEntry<'_>,
) -> Result<(), StoreError> {
    let canonical_record = CanonicalCommittedPipelineEvent {
        event_id: entry.event_id.clone(),
        run_id: run_id.clone(),
        sequence,
        message_id: entry.message_id.clone(),
        message_digest: *entry.message_digest,
        prior_control_revision: entry.event.prior_control_revision,
        event: entry.event.clone(),
    };
    let value = serde_json::to_value(&canonical_record).map_err(StoreError::Serialization)?;
    let event_json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    let event_digest = Sha256Digest::of_bytes(&event_json);
    transaction
        .execute(
            "INSERT INTO pipeline_events(
                event_id, run_id, sequence, message_id, message_digest,
                prior_control_revision, event_digest, event_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                entry.event_id.as_str(),
                run_id.as_str(),
                to_sql_integer(sequence)?,
                entry.message_id.as_str(),
                entry.message_digest.to_prefixed_string(),
                to_sql_integer(entry.event.prior_control_revision)?,
                event_digest.to_prefixed_string(),
                event_json,
            ],
        )
        .map_err(StoreError::Sqlite)?;
    Ok(())
}

fn validate_pipeline_artifact_acceptance(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    event: &PipelineEvent,
) -> Result<(), StoreError> {
    let PipelineEventKind::ArtifactAccepted {
        artifact_id,
        sha256,
        parent_artifact_ids,
    } = &event.kind
    else {
        return Ok(());
    };
    let stored = registry::load_artifact_record(transaction, run_id, artifact_id)?
        .ok_or(StoreError::ArtifactNotFound)?;
    if stored.record.sha256 != *sha256 || stored.parent_artifact_ids != *parent_artifact_ids {
        return Err(StoreError::ArtifactMetadataMismatch(
            "pipeline artifact acceptance does not match the registry",
        ));
    }
    Ok(())
}

pub(crate) fn verify_pipeline_journal(
    connection: &Connection,
    run_id: &RunId,
) -> Result<Vec<u64>, StoreError> {
    let events = load_pipeline_events(connection, run_id)?;
    if pipeline_snapshot_exists(connection, run_id)? {
        verify_semantic_batches(connection, run_id)?;
        verified_pipeline(connection, run_id)?;
    } else if !events.is_empty() {
        return Err(StoreError::PipelineNotFound);
    }
    Ok(events.into_iter().map(|event| event.sequence).collect())
}

fn verify_semantic_batches(connection: &Connection, run_id: &RunId) -> Result<(), StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT batch_id, batch_digest, batch_json
             FROM semantic_batches WHERE run_id = ?1 ORDER BY batch_id",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map(params![run_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .map_err(StoreError::Sqlite)?;
    let mut committed_event_ids = BTreeSet::new();
    for row in rows {
        let (batch_id, batch_digest, batch_json) = row.map_err(StoreError::Sqlite)?;
        let batch_id = BatchId::from_str(&batch_id).map_err(StoreError::Identifier)?;
        let batch_digest = Sha256Digest::from_str(&batch_digest).map_err(StoreError::Digest)?;
        let commitment: CanonicalSemanticBatch =
            serde_json::from_slice(&batch_json).map_err(StoreError::Serialization)?;
        let value = serde_json::to_value(&commitment).map_err(StoreError::Serialization)?;
        let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
        if commitment.batch_id != batch_id
            || commitment.run_id != *run_id
            || canonical != batch_json
            || Sha256Digest::of_bytes(&canonical) != batch_digest
        {
            return Err(StoreError::SemanticBatchConflict);
        }
        verify_semantic_batch_entries(connection, &commitment)?;
        for event_id in commitment
            .stage_entries
            .iter()
            .map(|entry| &entry.event_id)
            .chain(
                commitment
                    .pipeline_entries
                    .iter()
                    .map(|entry| &entry.event_id),
            )
        {
            if !committed_event_ids.insert(event_id.clone()) {
                return Err(StoreError::SemanticBatchConflict);
            }
        }
    }
    drop(statement);
    let journal_count: i64 = connection
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM events WHERE run_id = ?1) +
                (SELECT COUNT(*) FROM pipeline_events WHERE run_id = ?1)",
            params![run_id.as_str()],
            |row| row.get(0),
        )
        .map_err(StoreError::Sqlite)?;
    if u64::try_from(journal_count).map_err(|_| StoreError::SemanticBatchConflict)?
        != committed_event_ids.len() as u64
    {
        return Err(StoreError::PartialSemanticBatch);
    }
    Ok(())
}

pub(crate) fn pipeline_message_id_exists(
    transaction: &Transaction<'_>,
    message_id: &MessageId,
) -> Result<bool, StoreError> {
    transaction
        .query_row(
            "SELECT 1 FROM pipeline_events WHERE message_id = ?1",
            params![message_id.as_str()],
            |_| Ok(true),
        )
        .optional()
        .map(Option::unwrap_or_default)
        .map_err(StoreError::Sqlite)
}

fn verified_pipeline(connection: &Connection, run_id: &RunId) -> Result<PipelineState, StoreError> {
    let row: Option<(i64, Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT control_revision, initial_state_json, state_json
             FROM pipeline_snapshots WHERE run_id = ?1",
            params![run_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let (stored_revision, initial_json, snapshot_json) = row.ok_or(StoreError::PipelineNotFound)?;
    let initial: PipelineState =
        serde_json::from_slice(&initial_json).map_err(StoreError::Serialization)?;
    let snapshot: PipelineState =
        serde_json::from_slice(&snapshot_json).map_err(StoreError::Serialization)?;
    let run_definition_digest: String = connection
        .query_row(
            "SELECT pipeline_definition_digest FROM runs WHERE run_id = ?1",
            params![run_id.as_str()],
            |row| row.get(0),
        )
        .map_err(StoreError::Sqlite)?;
    let run_definition_digest =
        Sha256Digest::from_str(&run_definition_digest).map_err(StoreError::Digest)?;
    if initial.definition_digest != run_definition_digest {
        return Err(StoreError::PipelineDefinitionMismatch);
    }
    if !initial.is_pristine()
        || initial.definition_digest != snapshot.definition_digest
        || from_sql_integer(stored_revision)? != snapshot.control_revision
    {
        return Err(StoreError::PipelineSnapshotMismatch);
    }
    let events = load_pipeline_events(connection, run_id)?;
    let replayed = replay_pipeline(&initial, events.iter().map(|stored| &stored.event))
        .map_err(StoreError::PipelineTransition)?;
    if replayed != snapshot {
        return Err(StoreError::PipelineSnapshotMismatch);
    }
    for pipeline_stage in replayed.stage_states() {
        let stored_stage = verified_stage(connection, run_id, &pipeline_stage.stage_instance_id)?;
        if stored_stage != pipeline_stage {
            return Err(StoreError::PipelineStageMismatch);
        }
    }
    Ok(replayed)
}

fn load_pipeline_events(
    connection: &Connection,
    run_id: &RunId,
) -> Result<Vec<StoredPipelineEvent>, StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT event_id, run_id, sequence, message_id, message_digest,
                    prior_control_revision, event_digest, event_json
             FROM pipeline_events WHERE run_id = ?1 ORDER BY sequence",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map(params![run_id.as_str()], raw_pipeline_event_row)
        .map_err(StoreError::Sqlite)?;
    rows.map(|row| {
        row.map_err(StoreError::Sqlite)
            .and_then(decode_pipeline_event)
    })
    .collect()
}

struct RawPipelineEventRow {
    event_id: String,
    run_id: String,
    sequence: i64,
    message_id: String,
    message_digest: String,
    prior_revision: i64,
    event_digest: String,
    event_json: Vec<u8>,
}

fn raw_pipeline_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawPipelineEventRow> {
    Ok(RawPipelineEventRow {
        event_id: row.get(0)?,
        run_id: row.get(1)?,
        sequence: row.get(2)?,
        message_id: row.get(3)?,
        message_digest: row.get(4)?,
        prior_revision: row.get(5)?,
        event_digest: row.get(6)?,
        event_json: row.get(7)?,
    })
}

fn decode_pipeline_event(row: RawPipelineEventRow) -> Result<StoredPipelineEvent, StoreError> {
    let event_id = EventId::from_str(&row.event_id).map_err(StoreError::Identifier)?;
    let run_id = RunId::from_str(&row.run_id).map_err(StoreError::Identifier)?;
    let sequence = from_sql_integer(row.sequence)?;
    let message_id = MessageId::from_str(&row.message_id).map_err(StoreError::Identifier)?;
    let message_digest = Sha256Digest::from_str(&row.message_digest).map_err(StoreError::Digest)?;
    let prior_revision = from_sql_integer(row.prior_revision)?;
    let event_digest = Sha256Digest::from_str(&row.event_digest).map_err(StoreError::Digest)?;
    let record: CanonicalCommittedPipelineEvent =
        serde_json::from_slice(&row.event_json).map_err(StoreError::Serialization)?;
    let value = serde_json::to_value(&record).map_err(StoreError::Serialization)?;
    let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    if record.event_id != event_id
        || record.run_id != run_id
        || record.sequence != sequence
        || record.message_id != message_id
        || record.message_digest != message_digest
        || record.prior_control_revision != prior_revision
        || record.event.prior_control_revision != prior_revision
        || canonical != row.event_json
        || Sha256Digest::of_bytes(&canonical) != event_digest
    {
        return Err(StoreError::CorruptData(
            "stored pipeline event failed integrity verification",
        ));
    }
    Ok(StoredPipelineEvent {
        event_id,
        run_id,
        sequence,
        message_id,
        message_digest,
        event_digest,
        event: record.event,
    })
}

fn pipeline_event_by_message_id(
    transaction: &Connection,
    message_id: &MessageId,
) -> Result<Option<StoredPipelineEvent>, StoreError> {
    let row = transaction
        .query_row(
            "SELECT event_id, run_id, sequence, message_id, message_digest,
                    prior_control_revision, event_digest, event_json
             FROM pipeline_events WHERE message_id = ?1",
            params![message_id.as_str()],
            raw_pipeline_event_row,
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    row.map(decode_pipeline_event).transpose()
}

pub(crate) fn pipeline_snapshot_exists(
    connection: &Connection,
    run_id: &RunId,
) -> Result<bool, StoreError> {
    connection
        .query_row(
            "SELECT 1 FROM pipeline_snapshots WHERE run_id = ?1",
            params![run_id.as_str()],
            |_| Ok(true),
        )
        .optional()
        .map(Option::unwrap_or_default)
        .map_err(StoreError::Sqlite)
}

#[cfg(test)]
mod tests {
    use herdr_flow_core::{
        ArtifactId, ArtifactRecord, PipelineCommand, PipelineNodeDefinition, StageCommand,
        StageInstanceId, StageState,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::StoreError;

    const RUN: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const STAGE: &str = "01BX5ZZKBKACTAV9WEVGEMMVRZ";
    const EVENT_1: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA0";
    const EVENT_2: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA1";
    const EVENT_3: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA2";
    const MESSAGE_1: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA3";
    const MESSAGE_2: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA4";
    const MESSAGE_3: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA5";
    const ARTIFACT: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA6";
    const BATCH_1: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA7";
    const BATCH_2: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA8";
    const BATCH_3: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA9";

    struct TestStore {
        _directory: TempDir,
        store: SqliteStore,
        run_id: RunId,
        stage: StageState,
        initial: PipelineState,
    }

    fn digest(value: &[u8]) -> Sha256Digest {
        Sha256Digest::of_bytes(value)
    }

    fn event_id(value: &str) -> EventId {
        format!("evt_{value}").parse().unwrap()
    }

    fn message_id(value: &str) -> MessageId {
        format!("msg_{value}").parse().unwrap()
    }

    fn batch_id(value: &str) -> BatchId {
        format!("batch_{value}").parse().unwrap()
    }

    fn setup() -> TestStore {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("run.sqlite3");
        let mut store = SqliteStore::open(&path).unwrap();
        let run_id = format!("flow_{RUN}").parse().unwrap();
        let stage = StageState::new(
            StageInstanceId::parse(format!("stage_{STAGE}")).unwrap(),
            digest(b"component"),
            digest(b"predicate"),
        );
        let initial = PipelineState::new(
            digest(b"pipeline"),
            vec![PipelineNodeDefinition {
                stage: stage.clone(),
                needs: vec![],
                required_input_artifact_ids: vec![],
            }],
        )
        .unwrap();
        store.create_run(&run_id, &digest(b"pipeline")).unwrap();
        store.register_pipeline(&run_id, &initial).unwrap();
        TestStore {
            _directory: directory,
            store,
            run_id,
            stage,
            initial,
        }
    }

    #[test]
    fn semantic_batch_atomically_schedules_stage_and_registers_ingress() {
        let mut test = setup();
        let artifact_store = ArtifactStore::open(test._directory.path().join("artifacts")).unwrap();
        let scheduled = test
            .initial
            .decide(PipelineCommand::ScheduleStage {
                expected_revision: 0,
                stage_instance_id: test.stage.stage_instance_id.clone(),
            })
            .unwrap();
        let PipelineEventKind::StageScheduled {
            stage_event,
            input_manifest,
        } = &scheduled.kind
        else {
            panic!("expected scheduled stage");
        };
        let manifest_bytes = input_manifest.canonical_bytes().unwrap();
        let manifest_digest = digest(&manifest_bytes);
        let artifact_id: ArtifactId = format!("art_{ARTIFACT}").parse().unwrap();
        let record = ArtifactRecord {
            artifact_id: artifact_id.clone(),
            artifact_type: "stage-input-manifest/v1".into(),
            schema_id: "stage-input-manifest".into(),
            schema_version: 1,
            sha256: manifest_digest,
            size: manifest_bytes.len() as u64,
            media_type: "application/json".into(),
            producer_stage_instance_id: test.stage.stage_instance_id.clone(),
            producer_attempt: 0,
            producer_event_sequence: 1,
            pipeline_definition_digest: digest(b"pipeline"),
            component_digest: test.stage.component_digest,
            input_manifest_digest: manifest_digest,
            retention_class: "run-record".into(),
        };
        let registration = [ArtifactRegistration {
            record: &record,
            parent_artifact_ids: &[],
            bytes: &manifest_bytes,
        }];
        let stage_event_id = event_id(EVENT_1);
        let stage_message_id = message_id(MESSAGE_1);
        let stage_message_digest = digest(b"stage-ready");
        let pipeline_event_id = event_id(EVENT_2);
        let pipeline_message_id = message_id(MESSAGE_2);
        let pipeline_message_digest = digest(b"pipeline-ready");
        let stage_entries = [SemanticStageEntry {
            event_id: &stage_event_id,
            message_id: &stage_message_id,
            message_digest: &stage_message_digest,
            event: stage_event,
            artifacts: &registration,
        }];
        let pipeline_entries = [SemanticPipelineEntry {
            event_id: &pipeline_event_id,
            message_id: &pipeline_message_id,
            message_digest: &pipeline_message_digest,
            event: &scheduled,
        }];
        let schedule_batch_id = batch_id(BATCH_1);
        let batch = || SemanticBatch {
            batch_id: &schedule_batch_id,
            run_id: &test.run_id,
            stage_entries: &stage_entries,
            pipeline_entries: &pipeline_entries,
        };

        test.store.fail_after_event_insert = true;
        assert!(matches!(
            test.store.append_semantic_batch(&artifact_store, batch()),
            Err(StoreError::CorruptData("injected semantic batch failure"))
        ));
        test.store.fail_after_event_insert = false;
        assert_eq!(
            test.store.load_pipeline(&test.run_id).unwrap(),
            test.initial
        );
        assert_eq!(
            test.store
                .load_stage(&test.run_id, &test.stage.stage_instance_id)
                .unwrap(),
            test.stage
        );
        let artifact_count: i64 = test
            .store
            .connection
            .query_row("SELECT COUNT(*) FROM artifacts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(artifact_count, 0);

        assert_eq!(
            test.store
                .append_semantic_batch(&artifact_store, batch())
                .unwrap(),
            SemanticCommitOutcome::Committed
        );
        assert_eq!(
            test.store
                .append_semantic_batch(&artifact_store, batch())
                .unwrap(),
            SemanticCommitOutcome::Duplicate
        );
        let pipeline = test.store.load_pipeline(&test.run_id).unwrap();
        let stage = test
            .store
            .load_stage(&test.run_id, &test.stage.stage_instance_id)
            .unwrap();
        assert_eq!(pipeline.stage(&stage.stage_instance_id), Some(&stage));
        assert_eq!(stage.control_revision, 1);
        assert_eq!(
            test.store
                .load_artifact(&test.run_id, &artifact_id, &artifact_store)
                .unwrap()
                .record,
            record
        );
        assert_eq!(
            test.store
                .load_stage_events(&test.run_id, &stage.stage_instance_id)
                .unwrap()[0]
                .sequence,
            1
        );
        assert_eq!(
            test.store.load_pipeline_events(&test.run_id).unwrap()[0].sequence,
            2
        );
        assert_eq!(test.store.next_event_sequence(&test.run_id).unwrap(), 3);
        let reopened = SqliteStore::open(test._directory.path().join("run.sqlite3")).unwrap();
        assert_eq!(reopened.load_pipeline(&test.run_id).unwrap(), pipeline);
        assert_eq!(
            reopened
                .load_stage(&test.run_id, &stage.stage_instance_id)
                .unwrap(),
            stage
        );
        reopened
            .verify_run_artifacts(&test.run_id, &artifact_store)
            .unwrap();

        let wrong_acceptance = pipeline
            .decide(PipelineCommand::AcceptArtifact {
                expected_revision: pipeline.control_revision,
                artifact_id: artifact_id.clone(),
                sha256: digest(b"wrong"),
                parent_artifact_ids: vec![],
            })
            .unwrap();
        let acceptance_event_id = event_id(EVENT_3);
        let acceptance_message_id = message_id(MESSAGE_3);
        let acceptance_message_digest = digest(b"acceptance");
        let wrong_entries = [SemanticPipelineEntry {
            event_id: &acceptance_event_id,
            message_id: &acceptance_message_id,
            message_digest: &acceptance_message_digest,
            event: &wrong_acceptance,
        }];
        assert!(matches!(
            test.store.append_semantic_batch(
                &artifact_store,
                SemanticBatch {
                    batch_id: &batch_id(BATCH_2),
                    run_id: &test.run_id,
                    stage_entries: &[],
                    pipeline_entries: &wrong_entries,
                },
            ),
            Err(StoreError::ArtifactMetadataMismatch(_))
        ));
        assert_eq!(test.store.load_pipeline(&test.run_id).unwrap(), pipeline);

        let acceptance = pipeline
            .decide(PipelineCommand::AcceptArtifact {
                expected_revision: pipeline.control_revision,
                artifact_id,
                sha256: record.sha256,
                parent_artifact_ids: vec![],
            })
            .unwrap();
        let acceptance_entries = [SemanticPipelineEntry {
            event_id: &acceptance_event_id,
            message_id: &acceptance_message_id,
            message_digest: &acceptance_message_digest,
            event: &acceptance,
        }];
        let acceptance_batch_id = batch_id(BATCH_2);
        let acceptance_batch = || SemanticBatch {
            batch_id: &acceptance_batch_id,
            run_id: &test.run_id,
            stage_entries: &[],
            pipeline_entries: &acceptance_entries,
        };
        assert_eq!(
            test.store
                .append_semantic_batch(&artifact_store, acceptance_batch())
                .unwrap(),
            SemanticCommitOutcome::Committed
        );
        assert_eq!(
            test.store
                .append_semantic_batch(&artifact_store, acceptance_batch())
                .unwrap(),
            SemanticCommitOutcome::Duplicate
        );
        assert!(matches!(
            test.store.append_semantic_batch(
                &artifact_store,
                SemanticBatch {
                    batch_id: &batch_id(BATCH_3),
                    run_id: &test.run_id,
                    stage_entries: &[],
                    pipeline_entries: &acceptance_entries,
                },
            ),
            Err(StoreError::PartialSemanticBatch)
        ));
        assert!(test
            .store
            .load_pipeline(&test.run_id)
            .unwrap()
            .artifact_is_valid(&record.artifact_id));

        let original_batch_digest: String = test
            .store
            .connection
            .query_row(
                "SELECT batch_digest FROM semantic_batches WHERE batch_id = ?1",
                params![schedule_batch_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        test.store
            .connection
            .execute(
                "UPDATE semantic_batches SET batch_digest = ?1 WHERE batch_id = ?2",
                params![
                    digest(b"tampered").to_prefixed_string(),
                    schedule_batch_id.as_str()
                ],
            )
            .unwrap();
        assert!(matches!(
            test.store.load_pipeline(&test.run_id),
            Err(StoreError::SemanticBatchConflict)
        ));
        test.store
            .connection
            .execute(
                "UPDATE semantic_batches SET batch_digest = ?1 WHERE batch_id = ?2",
                params![original_batch_digest, schedule_batch_id.as_str()],
            )
            .unwrap();
    }

    #[test]
    fn registered_pipeline_rejects_independent_stage_roots() {
        let mut test = setup();

        assert!(matches!(
            test.store.register_stage(&test.run_id, &test.stage),
            Err(StoreError::PipelineStageRegistrationRequired)
        ));
        let ready = test
            .stage
            .decide(StageCommand::AcceptInputs {
                expected_revision: 0,
                input_manifest_digest: digest(b"input"),
            })
            .unwrap();
        assert!(matches!(
            test.store.append_stage_event(crate::AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id(EVENT_1),
                message_id: &message_id(MESSAGE_1),
                message_digest: &digest(b"legacy"),
                event: &ready,
            }),
            Err(StoreError::PipelineSemanticCommitRequired)
        ));
    }

    #[test]
    fn registration_requires_pristine_state_and_matching_run_definition() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = SqliteStore::open(directory.path().join("run.sqlite3")).unwrap();
        let run_id: RunId = format!("flow_{RUN}").parse().unwrap();
        store
            .create_run(&run_id, &digest(b"other-pipeline"))
            .unwrap();
        let stage = StageState::new(
            format!("stage_{STAGE}").parse().unwrap(),
            digest(b"component"),
            digest(b"predicate"),
        );
        let initial = PipelineState::new(
            digest(b"pipeline"),
            vec![PipelineNodeDefinition {
                stage,
                needs: vec![],
                required_input_artifact_ids: vec![],
            }],
        )
        .unwrap();
        assert!(matches!(
            store.register_pipeline(&run_id, &initial),
            Err(StoreError::PipelineDefinitionMismatch)
        ));

        let directory = tempfile::tempdir().unwrap();
        let mut store = SqliteStore::open(directory.path().join("run.sqlite3")).unwrap();
        store.create_run(&run_id, &digest(b"pipeline")).unwrap();
        let stage_id = format!("stage_{STAGE}").parse().unwrap();
        let stage = initial.stage(&stage_id).unwrap().clone();
        store.register_stage(&run_id, &stage).unwrap();
        assert!(matches!(
            store.register_pipeline(&run_id, &initial),
            Err(StoreError::PipelineRootConflict)
        ));
    }
}
