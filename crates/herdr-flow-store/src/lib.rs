#![forbid(unsafe_code)]

//! Atomic persistence adapters for Herdr Flow.

mod artifact;
mod git;
mod pipeline;
mod registry;

pub use artifact::{ArtifactStore, ArtifactStoreError, StoredArtifact};
pub use git::{GitRepository, GitRepositoryError, SnapshotRefOutcome};
pub use pipeline::{
    SemanticBatch, SemanticCommitOutcome, SemanticPipelineEntry, SemanticStageEntry,
};
pub use registry::{ArtifactRegistration, StoredArtifactRecord};

use std::{fmt, path::Path, str::FromStr, time::Duration};

use herdr_flow_core::{
    canonical_json, replay_stage, ArtifactCatalog, ArtifactCatalogError, ArtifactId,
    ArtifactRecordValidationError, EventId, IdentifierError, MessageId, PipelineTransitionError,
    RunId, Sha256Digest, StageEvent, StageInstanceId, StageState, StageTransitionError,
    BASE_PROTOCOL, MAX_CONTROL_REVISION,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

const INITIAL_EVENT_SEQUENCE: u64 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub fn base_protocol() -> &'static str {
    BASE_PROTOCOL
}

pub struct SqliteStore {
    connection: Connection,
    #[cfg(test)]
    fail_after_event_insert: bool,
}

pub struct AppendStageEvent<'a> {
    pub run_id: &'a RunId,
    pub event_id: &'a EventId,
    pub message_id: &'a MessageId,
    pub message_digest: &'a Sha256Digest,
    pub event: &'a StageEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredStageEvent {
    pub event_id: EventId,
    pub run_id: RunId,
    pub sequence: u64,
    pub message_id: MessageId,
    pub message_digest: Sha256Digest,
    pub event_digest: Sha256Digest,
    pub event: StageEvent,
    artifact_commitments: Vec<registry::ArtifactCommitment>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CanonicalCommittedEvent {
    event_id: EventId,
    run_id: RunId,
    sequence: u64,
    message_id: MessageId,
    message_digest: Sha256Digest,
    stage_instance_id: StageInstanceId,
    prior_control_revision: u64,
    artifact_commitments: Vec<registry::ArtifactCommitment>,
    event: StageEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    Committed(StoredStageEvent),
    Duplicate(StoredStageEvent),
}

#[derive(Debug)]
pub enum StoreError {
    Sqlite(rusqlite::Error),
    Serialization(serde_json::Error),
    Canonicalization(canonical_json::CanonicalJsonError),
    StageTransition(StageTransitionError),
    Identifier(IdentifierError),
    Digest(herdr_flow_core::DigestParseError),
    RunAlreadyExists,
    RunNotFound,
    StageAlreadyExists,
    InvalidInitialStage,
    StageNotFound,
    StageRunMismatch,
    MessageIdConflict,
    EventIdConflict,
    ConcurrentUpdate,
    EventSequenceExhausted,
    IncompatiblePragma(&'static str),
    SnapshotMismatch,
    CorruptData(&'static str),
    ArtifactStore(ArtifactStoreError),
    ArtifactValidation(ArtifactRecordValidationError),
    ArtifactGraph(ArtifactCatalogError),
    ArtifactMetadataMismatch(&'static str),
    ArtifactIdConflict,
    ArtifactNotFound,
    ArtifactRunMismatch,
    ArtifactReferenceNotFound,
    PipelineTransition(PipelineTransitionError),
    PipelineAlreadyExists,
    InvalidInitialPipeline,
    PipelineNotFound,
    PipelineDefinitionMismatch,
    PipelineSnapshotMismatch,
    PipelineRootConflict,
    PipelineStageRegistrationRequired,
    EmptySemanticBatch,
    PipelineStageMismatch,
    PartialSemanticBatch,
    PipelineSemanticCommitRequired,
    SemanticBatchConflict,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connection = Connection::open(path).map_err(StoreError::Sqlite)?;
        Self::from_connection(connection)
    }

    fn from_connection(connection: Connection) -> Result<Self, StoreError> {
        connection
            .busy_timeout(BUSY_TIMEOUT)
            .map_err(StoreError::Sqlite)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(StoreError::Sqlite)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(StoreError::Sqlite)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(StoreError::Sqlite)?;
        connection
            .pragma_update(None, "trusted_schema", "OFF")
            .map_err(StoreError::Sqlite)?;
        connection
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline_definition_digest TEXT NOT NULL,
                    next_event_sequence INTEGER NOT NULL
                        CHECK(next_event_sequence >= 1 AND next_event_sequence <= 9007199254740991)
                ) STRICT;

                CREATE TABLE IF NOT EXISTS stage_snapshots (
                    stage_instance_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
                    control_revision INTEGER NOT NULL
                        CHECK(control_revision >= 0 AND control_revision <= 9007199254740991),
                    initial_state_json BLOB NOT NULL,
                    state_json BLOB NOT NULL,
                    UNIQUE(stage_instance_id, run_id)
                ) STRICT;

                CREATE INDEX IF NOT EXISTS stage_snapshots_run_id
                    ON stage_snapshots(run_id);

                CREATE TABLE IF NOT EXISTS pipeline_snapshots (
                    run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
                    control_revision INTEGER NOT NULL
                        CHECK(control_revision >= 0 AND control_revision <= 9007199254740991),
                    initial_state_json BLOB NOT NULL,
                    state_json BLOB NOT NULL
                ) STRICT;

                CREATE TABLE IF NOT EXISTS pipeline_events (
                    event_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
                    sequence INTEGER NOT NULL
                        CHECK(sequence >= 1 AND sequence <= 9007199254740991),
                    message_id TEXT NOT NULL UNIQUE,
                    message_digest TEXT NOT NULL,
                    prior_control_revision INTEGER NOT NULL
                        CHECK(prior_control_revision >= 0 AND prior_control_revision <= 9007199254740991),
                    event_digest TEXT NOT NULL,
                    event_json BLOB NOT NULL,
                    UNIQUE(run_id, sequence)
                ) STRICT;

                CREATE INDEX IF NOT EXISTS pipeline_events_run_sequence
                    ON pipeline_events(run_id, sequence);

                CREATE TABLE IF NOT EXISTS semantic_batches (
                    batch_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
                    batch_digest TEXT NOT NULL,
                    batch_json BLOB NOT NULL
                ) STRICT;

                CREATE INDEX IF NOT EXISTS semantic_batches_run_id
                    ON semantic_batches(run_id, batch_id);

                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
                    sequence INTEGER NOT NULL
                        CHECK(sequence >= 1 AND sequence <= 9007199254740991),
                    message_id TEXT NOT NULL UNIQUE,
                    message_digest TEXT NOT NULL,
                    stage_instance_id TEXT NOT NULL,
                    prior_control_revision INTEGER NOT NULL
                        CHECK(prior_control_revision >= 0 AND prior_control_revision <= 9007199254740991),
                    event_digest TEXT NOT NULL,
                    event_json BLOB NOT NULL,
                    UNIQUE(run_id, sequence),
                    FOREIGN KEY(stage_instance_id, run_id)
                        REFERENCES stage_snapshots(stage_instance_id, run_id)
                        ON DELETE RESTRICT
                ) STRICT;

                CREATE INDEX IF NOT EXISTS events_stage_sequence
                    ON events(run_id, stage_instance_id, sequence);

                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
                    sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL CHECK(size >= 0),
                    producer_stage_instance_id TEXT NOT NULL,
                    producer_event_sequence INTEGER NOT NULL
                        CHECK(producer_event_sequence >= 1 AND producer_event_sequence <= 9007199254740991),
                    record_digest TEXT NOT NULL,
                    record_json BLOB NOT NULL,
                    UNIQUE(artifact_id, run_id),
                    FOREIGN KEY(run_id, producer_event_sequence)
                        REFERENCES events(run_id, sequence) ON DELETE RESTRICT,
                    FOREIGN KEY(producer_stage_instance_id, run_id)
                        REFERENCES stage_snapshots(stage_instance_id, run_id) ON DELETE RESTRICT
                ) STRICT;

                CREATE INDEX IF NOT EXISTS artifacts_run_sequence
                    ON artifacts(run_id, producer_event_sequence, artifact_id);

                CREATE TABLE IF NOT EXISTS artifact_edges (
                    run_id TEXT NOT NULL,
                    parent_artifact_id TEXT NOT NULL,
                    child_artifact_id TEXT NOT NULL,
                    PRIMARY KEY(run_id, parent_artifact_id, child_artifact_id),
                    CHECK(parent_artifact_id <> child_artifact_id),
                    FOREIGN KEY(parent_artifact_id, run_id)
                        REFERENCES artifacts(artifact_id, run_id) ON DELETE RESTRICT,
                    FOREIGN KEY(child_artifact_id, run_id)
                        REFERENCES artifacts(artifact_id, run_id) ON DELETE RESTRICT
                ) STRICT;

                CREATE INDEX IF NOT EXISTS artifact_edges_child
                    ON artifact_edges(run_id, child_artifact_id, parent_artifact_id);
                ",
            )
            .map_err(StoreError::Sqlite)?;
        verify_pragmas(&connection)?;
        Ok(Self {
            connection,
            #[cfg(test)]
            fail_after_event_insert: false,
        })
    }

    /// Creates an empty run before any stage instances or events are registered.
    pub fn create_run(
        &mut self,
        run_id: &RunId,
        pipeline_definition_digest: &Sha256Digest,
    ) -> Result<(), StoreError> {
        let inserted = self
            .connection
            .execute(
                "INSERT OR IGNORE INTO runs(
                    run_id, pipeline_definition_digest, next_event_sequence
                 ) VALUES (?1, ?2, ?3)",
                params![
                    run_id.as_str(),
                    pipeline_definition_digest.to_prefixed_string(),
                    INITIAL_EVENT_SEQUENCE as i64
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if inserted == 0 {
            return Err(StoreError::RunAlreadyExists);
        }
        Ok(())
    }

    /// Registers the deterministic initial snapshot for a stage before its first
    /// lifecycle event. Pipeline scheduling will eventually own this bootstrap.
    pub fn register_stage(&mut self, run_id: &RunId, state: &StageState) -> Result<(), StoreError> {
        if !state.is_pristine() {
            return Err(StoreError::InvalidInitialStage);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        require_run(&transaction, run_id)?;
        if pipeline::pipeline_snapshot_exists(&transaction, run_id)? {
            return Err(StoreError::PipelineStageRegistrationRequired);
        }
        let state_json = serde_json::to_vec(state).map_err(StoreError::Serialization)?;
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO stage_snapshots(
                    stage_instance_id, run_id, control_revision, initial_state_json, state_json
                 ) VALUES (?1, ?2, ?3, ?4, ?4)",
                params![
                    state.stage_instance_id.as_str(),
                    run_id.as_str(),
                    to_sql_integer(state.control_revision)?,
                    state_json,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if inserted == 0 {
            return Err(StoreError::StageAlreadyExists);
        }
        transaction.commit().map_err(StoreError::Sqlite)
    }

    /// Atomically appends one accepted stage event and its derived snapshot.
    #[cfg(test)]
    pub fn append_stage_event(
        &mut self,
        request: AppendStageEvent<'_>,
    ) -> Result<AppendOutcome, StoreError> {
        self.append_stage_event_inner(request, &[], false)
    }

    /// Makes artifact bytes durable first, then commits their immutable records,
    /// provenance edges, accepted event, and derived snapshot in one transaction.
    pub fn append_stage_event_with_artifacts(
        &mut self,
        request: AppendStageEvent<'_>,
        artifact_store: &ArtifactStore,
        registrations: &[ArtifactRegistration<'_>],
    ) -> Result<AppendOutcome, StoreError> {
        // The run lease excludes concurrent coordinators. Verify all previously
        // committed metadata and bytes before accepting a transition that may
        // consume them, then make this event's new bytes durable.
        self.verify_run_artifacts(request.run_id, artifact_store)?;
        let prepared = registry::prepare_artifacts(artifact_store, registrations)?;
        self.append_stage_event_inner(request, &prepared, true)
    }

    fn append_stage_event_inner(
        &mut self,
        request: AppendStageEvent<'_>,
        artifacts: &[registry::PreparedArtifact],
        enforce_artifact_references: bool,
    ) -> Result<AppendOutcome, StoreError> {
        #[cfg(test)]
        let fail_after_event_insert = self.fail_after_event_insert;
        let artifact_commitments = registry::artifact_commitments(artifacts);

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;

        verify_run_journal(&transaction, request.run_id)?;
        if pipeline::pipeline_snapshot_exists(&transaction, request.run_id)? {
            return Err(StoreError::PipelineSemanticCommitRequired);
        }

        if pipeline::pipeline_message_id_exists(&transaction, request.message_id)? {
            return Err(StoreError::MessageIdConflict);
        }
        if let Some(existing) = event_by_message_id(&transaction, request.message_id)? {
            if existing.run_id == *request.run_id
                && existing.message_digest == *request.message_digest
                && existing.event == *request.event
                && existing.artifact_commitments == artifact_commitments
            {
                registry::verify_committed_artifacts(
                    &transaction,
                    request.run_id,
                    existing.sequence,
                    &existing.event.stage_instance_id,
                    &artifact_commitments,
                )?;
                return Ok(AppendOutcome::Duplicate(existing));
            }
            return Err(StoreError::MessageIdConflict);
        }
        if event_id_exists(&transaction, request.event_id)? {
            return Err(StoreError::EventIdConflict);
        }

        let state = verified_stage(
            &transaction,
            request.run_id,
            &request.event.stage_instance_id,
        )?;
        let next_state = state
            .apply(request.event)
            .map_err(StoreError::StageTransition)?;
        let next_state_json = serde_json::to_vec(&next_state).map_err(StoreError::Serialization)?;

        let next_sequence: i64 = transaction
            .query_row(
                "SELECT next_event_sequence FROM runs WHERE run_id = ?1",
                params![request.run_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Sqlite)?
            .ok_or(StoreError::RunNotFound)?;
        let sequence = from_sql_integer(next_sequence)?;
        if sequence >= MAX_CONTROL_REVISION {
            return Err(StoreError::EventSequenceExhausted);
        }

        let canonical_record = CanonicalCommittedEvent {
            event_id: request.event_id.clone(),
            run_id: request.run_id.clone(),
            sequence,
            message_id: request.message_id.clone(),
            message_digest: *request.message_digest,
            stage_instance_id: request.event.stage_instance_id.clone(),
            prior_control_revision: request.event.prior_control_revision,
            artifact_commitments: artifact_commitments.clone(),
            event: request.event.clone(),
        };
        let record_value =
            serde_json::to_value(&canonical_record).map_err(StoreError::Serialization)?;
        let event_json =
            canonical_json::to_vec(&record_value).map_err(StoreError::Canonicalization)?;
        let event_digest = Sha256Digest::of_bytes(&event_json);

        registry::validate_new_artifacts(
            &transaction,
            request.run_id,
            &state,
            &next_state,
            request.event,
            sequence,
            artifacts,
        )?;
        if enforce_artifact_references {
            registry::validate_event_artifact_references(
                &transaction,
                request.run_id,
                request.event,
                artifacts,
            )?;
        }

        transaction
            .execute(
                "INSERT INTO events(
                    event_id, run_id, sequence, message_id, message_digest,
                    stage_instance_id, prior_control_revision, event_digest, event_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    request.event_id.as_str(),
                    request.run_id.as_str(),
                    next_sequence,
                    request.message_id.as_str(),
                    request.message_digest.to_prefixed_string(),
                    request.event.stage_instance_id.as_str(),
                    to_sql_integer(request.event.prior_control_revision)?,
                    event_digest.to_prefixed_string(),
                    event_json,
                ],
            )
            .map_err(StoreError::Sqlite)?;

        registry::insert_artifacts(&transaction, request.run_id, artifacts)?;

        #[cfg(test)]
        if fail_after_event_insert {
            return Err(StoreError::CorruptData("injected post-insert failure"));
        }

        let updated = transaction
            .execute(
                "UPDATE stage_snapshots
                 SET control_revision = ?1, state_json = ?2
                 WHERE stage_instance_id = ?3 AND run_id = ?4 AND control_revision = ?5",
                params![
                    to_sql_integer(next_state.control_revision)?,
                    next_state_json,
                    next_state.stage_instance_id.as_str(),
                    request.run_id.as_str(),
                    to_sql_integer(state.control_revision)?,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if updated != 1 {
            return Err(StoreError::ConcurrentUpdate);
        }

        let run_updated = transaction
            .execute(
                "UPDATE runs SET next_event_sequence = ?1
                 WHERE run_id = ?2 AND next_event_sequence = ?3",
                params![
                    to_sql_integer(sequence + 1)?,
                    request.run_id.as_str(),
                    next_sequence,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if run_updated != 1 {
            return Err(StoreError::ConcurrentUpdate);
        }

        let stored = StoredStageEvent {
            event_id: request.event_id.clone(),
            run_id: request.run_id.clone(),
            sequence,
            message_id: request.message_id.clone(),
            message_digest: *request.message_digest,
            event_digest,
            event: request.event.clone(),
            artifact_commitments,
        };
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(AppendOutcome::Committed(stored))
    }

    pub fn load_stage(
        &self,
        run_id: &RunId,
        stage_instance_id: &StageInstanceId,
    ) -> Result<StageState, StoreError> {
        self.load_stage_consistently(run_id, stage_instance_id, || {})
    }

    fn load_stage_consistently<F>(
        &self,
        run_id: &RunId,
        stage_instance_id: &StageInstanceId,
        after_journal_verification: F,
    ) -> Result<StageState, StoreError>
    where
        F: FnOnce(),
    {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, run_id)?;
        after_journal_verification();
        let state = verified_stage(&transaction, run_id, stage_instance_id)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(state)
    }

    pub fn load_stage_events(
        &self,
        run_id: &RunId,
        stage_instance_id: &StageInstanceId,
    ) -> Result<Vec<StoredStageEvent>, StoreError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, run_id)?;
        let events = load_events(&transaction, run_id, stage_instance_id)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(events)
    }

    pub fn next_event_sequence(&self, run_id: &RunId) -> Result<u64, StoreError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, run_id)?;
        let sequence: i64 = transaction
            .query_row(
                "SELECT next_event_sequence FROM runs WHERE run_id = ?1",
                params![run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        from_sql_integer(sequence)
    }

    pub fn event_count(&self, run_id: &RunId) -> Result<u64, StoreError> {
        let count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE run_id = ?1",
                params![run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        from_sql_integer(count)
    }

    /// Verifies the complete registry snapshot and every referenced byte object.
    pub fn verify_run_artifacts(
        &self,
        run_id: &RunId,
        artifact_store: &ArtifactStore,
    ) -> Result<Vec<StoredArtifactRecord>, StoreError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, run_id)?;
        let records = registry::load_and_verify_run_artifacts(&transaction, run_id)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        registry::verify_artifact_bytes(artifact_store, &records)?;
        Ok(records)
    }

    pub fn load_artifact(
        &self,
        run_id: &RunId,
        artifact_id: &ArtifactId,
        artifact_store: &ArtifactStore,
    ) -> Result<StoredArtifactRecord, StoreError> {
        self.verify_run_artifacts(run_id, artifact_store)?
            .into_iter()
            .find(|stored| stored.record.artifact_id == *artifact_id)
            .ok_or(StoreError::ArtifactNotFound)
    }

    /// Computes deterministic transitive invalidation targets from verified
    /// provenance edges. The changed root itself is not included.
    pub fn artifact_descendants(
        &self,
        run_id: &RunId,
        artifact_id: &ArtifactId,
        artifact_store: &ArtifactStore,
    ) -> Result<Vec<ArtifactId>, StoreError> {
        let records = self.verify_run_artifacts(run_id, artifact_store)?;
        let mut catalog = ArtifactCatalog::new();
        for stored in records {
            catalog
                .register(stored.record, &stored.parent_artifact_ids)
                .map_err(StoreError::ArtifactGraph)?;
        }
        if catalog.record(artifact_id).is_none() {
            return Err(StoreError::ArtifactNotFound);
        }
        Ok(catalog.descendants_of(artifact_id))
    }

    #[cfg(test)]
    fn inject_failure_after_event_insert(&mut self, enabled: bool) {
        self.fail_after_event_insert = enabled;
    }
}

fn verify_pragmas(connection: &Connection) -> Result<(), StoreError> {
    let journal_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(StoreError::Sqlite)?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(StoreError::IncompatiblePragma("journal_mode"));
    }
    let synchronous: i64 = connection
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .map_err(StoreError::Sqlite)?;
    if synchronous != 2 {
        return Err(StoreError::IncompatiblePragma("synchronous"));
    }
    let foreign_keys: i64 = connection
        .pragma_query_value(None, "foreign_keys", |row| row.get(0))
        .map_err(StoreError::Sqlite)?;
    if foreign_keys != 1 {
        return Err(StoreError::IncompatiblePragma("foreign_keys"));
    }
    let trusted_schema: i64 = connection
        .pragma_query_value(None, "trusted_schema", |row| row.get(0))
        .map_err(StoreError::Sqlite)?;
    if trusted_schema != 0 {
        return Err(StoreError::IncompatiblePragma("trusted_schema"));
    }
    Ok(())
}

fn verified_stage(
    connection: &Connection,
    run_id: &RunId,
    stage_instance_id: &StageInstanceId,
) -> Result<StageState, StoreError> {
    let (snapshot_run_id, stored_revision, initial_json, snapshot_json) = connection
        .query_row(
            "SELECT run_id, control_revision, initial_state_json, state_json
             FROM stage_snapshots WHERE stage_instance_id = ?1",
            params![stage_instance_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(StoreError::Sqlite)?
        .ok_or(StoreError::StageNotFound)?;
    if snapshot_run_id != run_id.as_str() {
        return Err(StoreError::StageRunMismatch);
    }

    let initial: StageState =
        serde_json::from_slice(&initial_json).map_err(StoreError::Serialization)?;
    let snapshot: StageState =
        serde_json::from_slice(&snapshot_json).map_err(StoreError::Serialization)?;
    if initial.stage_instance_id != *stage_instance_id
        || snapshot.stage_instance_id != *stage_instance_id
        || from_sql_integer(stored_revision)? != snapshot.control_revision
    {
        return Err(StoreError::SnapshotMismatch);
    }

    let events = load_events(connection, run_id, stage_instance_id)?;
    let replayed = replay_stage(&initial, events.iter().map(|stored| &stored.event))
        .map_err(StoreError::StageTransition)?;
    if replayed != snapshot {
        return Err(StoreError::SnapshotMismatch);
    }
    Ok(replayed)
}

fn load_events(
    connection: &Connection,
    run_id: &RunId,
    stage_instance_id: &StageInstanceId,
) -> Result<Vec<StoredStageEvent>, StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT event_id, run_id, sequence, message_id, message_digest,
                    stage_instance_id, prior_control_revision, event_digest, event_json
             FROM events
             WHERE run_id = ?1 AND stage_instance_id = ?2
             ORDER BY sequence",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map(
            params![run_id.as_str(), stage_instance_id.as_str()],
            raw_event_row,
        )
        .map_err(StoreError::Sqlite)?;
    rows.map(|row| row.map_err(StoreError::Sqlite).and_then(decode_event_row))
        .collect()
}

fn verify_run_journal(connection: &Connection, run_id: &RunId) -> Result<(), StoreError> {
    let next_sequence: i64 = connection
        .query_row(
            "SELECT next_event_sequence FROM runs WHERE run_id = ?1",
            params![run_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)?
        .ok_or(StoreError::RunNotFound)?;
    let next_sequence = from_sql_integer(next_sequence)?;

    let mut statement = connection
        .prepare(
            "SELECT event_id, run_id, sequence, message_id, message_digest,
                    stage_instance_id, prior_control_revision, event_digest, event_json
             FROM events WHERE run_id = ?1 ORDER BY sequence",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map(params![run_id.as_str()], raw_event_row)
        .map_err(StoreError::Sqlite)?;
    let mut sequences = Vec::new();
    for row in rows {
        let stored = decode_event_row(row.map_err(StoreError::Sqlite)?)?;
        if stored.run_id != *run_id {
            return Err(StoreError::CorruptData("event belongs to another run"));
        }
        registry::verify_committed_artifacts(
            connection,
            run_id,
            stored.sequence,
            &stored.event.stage_instance_id,
            &stored.artifact_commitments,
        )?;
        sequences.push(stored.sequence);
    }
    drop(statement);
    sequences.extend(pipeline::verify_pipeline_journal(connection, run_id)?);
    let (entry_count, distinct_event_ids, distinct_message_ids): (i64, i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COUNT(DISTINCT event_id), COUNT(DISTINCT message_id)
             FROM (
                SELECT event_id, message_id FROM events WHERE run_id = ?1
                UNION ALL
                SELECT event_id, message_id FROM pipeline_events WHERE run_id = ?1
             )",
            params![run_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(StoreError::Sqlite)?;
    if entry_count != distinct_event_ids || entry_count != distinct_message_ids {
        return Err(StoreError::CorruptData(
            "run journal contains duplicate event or message IDs",
        ));
    }
    sequences.sort_unstable();
    let mut expected_sequence = INITIAL_EVENT_SEQUENCE;
    for sequence in sequences {
        if sequence != expected_sequence {
            return Err(StoreError::CorruptData(
                "run event sequence is not contiguous",
            ));
        }
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(StoreError::EventSequenceExhausted)?;
    }
    if next_sequence != expected_sequence {
        return Err(StoreError::CorruptData(
            "run sequence counter does not match the journal",
        ));
    }
    Ok(())
}

fn require_run(transaction: &Transaction<'_>, run_id: &RunId) -> Result<(), StoreError> {
    let exists = transaction
        .query_row(
            "SELECT 1 FROM runs WHERE run_id = ?1",
            params![run_id.as_str()],
            |_| Ok(()),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    exists.ok_or(StoreError::RunNotFound)
}

struct RawEventRow {
    event_id: String,
    run_id: String,
    sequence: i64,
    message_id: String,
    message_digest: String,
    stage_instance_id: String,
    prior_control_revision: i64,
    event_digest: String,
    event_json: Vec<u8>,
}

fn raw_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawEventRow> {
    Ok(RawEventRow {
        event_id: row.get(0)?,
        run_id: row.get(1)?,
        sequence: row.get(2)?,
        message_id: row.get(3)?,
        message_digest: row.get(4)?,
        stage_instance_id: row.get(5)?,
        prior_control_revision: row.get(6)?,
        event_digest: row.get(7)?,
        event_json: row.get(8)?,
    })
}

fn decode_event_row(row: RawEventRow) -> Result<StoredStageEvent, StoreError> {
    let event_id = EventId::from_str(&row.event_id).map_err(StoreError::Identifier)?;
    let run_id = RunId::from_str(&row.run_id).map_err(StoreError::Identifier)?;
    let sequence = from_sql_integer(row.sequence)?;
    let message_id = MessageId::from_str(&row.message_id).map_err(StoreError::Identifier)?;
    let message_digest = Sha256Digest::from_str(&row.message_digest).map_err(StoreError::Digest)?;
    let stage_instance_id =
        StageInstanceId::from_str(&row.stage_instance_id).map_err(StoreError::Identifier)?;
    let prior_control_revision = from_sql_integer(row.prior_control_revision)?;
    let event_digest = Sha256Digest::from_str(&row.event_digest).map_err(StoreError::Digest)?;
    let record: CanonicalCommittedEvent =
        serde_json::from_slice(&row.event_json).map_err(StoreError::Serialization)?;
    let value = serde_json::to_value(&record).map_err(StoreError::Serialization)?;
    let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;

    let columns_match = record.event_id == event_id
        && record.run_id == run_id
        && record.sequence == sequence
        && record.message_id == message_id
        && record.message_digest == message_digest
        && record.stage_instance_id == stage_instance_id
        && record.prior_control_revision == prior_control_revision
        && record.event.stage_instance_id == stage_instance_id
        && record.event.prior_control_revision == prior_control_revision;
    if !columns_match
        || canonical != row.event_json
        || Sha256Digest::of_bytes(&canonical) != event_digest
    {
        return Err(StoreError::CorruptData(
            "stored committed event failed integrity verification",
        ));
    }

    Ok(StoredStageEvent {
        event_id,
        run_id,
        sequence,
        message_id,
        message_digest,
        event_digest,
        event: record.event,
        artifact_commitments: record.artifact_commitments,
    })
}

fn event_by_message_id(
    transaction: &Connection,
    message_id: &MessageId,
) -> Result<Option<StoredStageEvent>, StoreError> {
    let row = transaction
        .query_row(
            "SELECT event_id, run_id, sequence, message_id, message_digest,
                    stage_instance_id, prior_control_revision, event_digest, event_json
             FROM events WHERE message_id = ?1",
            params![message_id.as_str()],
            raw_event_row,
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    row.map(decode_event_row).transpose()
}

fn event_id_exists(transaction: &Transaction<'_>, event_id: &EventId) -> Result<bool, StoreError> {
    transaction
        .query_row(
            "SELECT 1 FROM (
                SELECT event_id FROM events WHERE event_id = ?1
                UNION ALL
                SELECT event_id FROM pipeline_events WHERE event_id = ?1
             ) LIMIT 1",
            params![event_id.as_str()],
            |_| Ok(true),
        )
        .optional()
        .map(Option::unwrap_or_default)
        .map_err(StoreError::Sqlite)
}

fn to_sql_integer(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::CorruptData("integer exceeds SQLite range"))
}

fn from_sql_integer(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::CorruptData("negative SQLite integer"))
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => error.fmt(formatter),
            Self::Serialization(error) => error.fmt(formatter),
            Self::Canonicalization(error) => error.fmt(formatter),
            Self::StageTransition(error) => error.fmt(formatter),
            Self::Identifier(error) => error.fmt(formatter),
            Self::Digest(error) => error.fmt(formatter),
            Self::RunAlreadyExists => formatter.write_str("run already exists"),
            Self::RunNotFound => formatter.write_str("run does not exist"),
            Self::StageAlreadyExists => formatter.write_str("stage already exists"),
            Self::InvalidInitialStage => {
                formatter.write_str("stage bootstrap state is not a pristine pending state")
            }
            Self::StageNotFound => formatter.write_str("stage does not exist"),
            Self::StageRunMismatch => formatter.write_str("stage belongs to another run"),
            Self::MessageIdConflict => {
                formatter.write_str("message ID was reused with different content")
            }
            Self::EventIdConflict => formatter.write_str("event ID already exists"),
            Self::ConcurrentUpdate => formatter.write_str("snapshot compare-and-swap failed"),
            Self::EventSequenceExhausted => formatter.write_str("event sequence is exhausted"),
            Self::IncompatiblePragma(name) => {
                write!(
                    formatter,
                    "SQLite {name} pragma is incompatible with the durability contract"
                )
            }
            Self::SnapshotMismatch => {
                formatter.write_str("stage snapshot does not match deterministic event replay")
            }
            Self::CorruptData(message) => formatter.write_str(message),
            Self::ArtifactStore(error) => error.fmt(formatter),
            Self::ArtifactValidation(error) => error.fmt(formatter),
            Self::ArtifactGraph(error) => error.fmt(formatter),
            Self::ArtifactMetadataMismatch(message) => formatter.write_str(message),
            Self::ArtifactIdConflict => formatter.write_str("artifact ID already exists"),
            Self::ArtifactNotFound => formatter.write_str("artifact does not exist"),
            Self::ArtifactRunMismatch => formatter.write_str("artifact belongs to another run"),
            Self::ArtifactReferenceNotFound => {
                formatter.write_str("event references an unregistered artifact digest")
            }
            Self::PipelineTransition(error) => error.fmt(formatter),
            Self::PipelineAlreadyExists => formatter.write_str("pipeline already exists"),
            Self::InvalidInitialPipeline => {
                formatter.write_str("pipeline bootstrap state is not pristine")
            }
            Self::PipelineNotFound => formatter.write_str("pipeline does not exist"),
            Self::PipelineDefinitionMismatch => {
                formatter.write_str("pipeline definition digest does not match the run")
            }
            Self::PipelineSnapshotMismatch => {
                formatter.write_str("pipeline snapshot does not match deterministic replay")
            }
            Self::PipelineRootConflict => {
                formatter.write_str("pipeline cannot be registered after stage roots")
            }
            Self::PipelineStageRegistrationRequired => {
                formatter.write_str("stages in a registered pipeline require unified registration")
            }
            Self::EmptySemanticBatch => formatter.write_str("semantic batch has no pipeline event"),
            Self::PipelineStageMismatch => {
                formatter.write_str("pipeline and stage effects do not match")
            }
            Self::PartialSemanticBatch => {
                formatter.write_str("semantic batch was only partially committed")
            }
            Self::PipelineSemanticCommitRequired => {
                formatter.write_str("registered pipeline runs require a unified semantic commit")
            }
            Self::SemanticBatchConflict => {
                formatter.write_str("semantic batch ID was reused with different content")
            }
        }
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use herdr_flow_core::{replay_stage, ArtifactRecord, StageCommand, StageEventKind};
    use tempfile::TempDir;

    use super::*;

    const RUN_ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const STAGE_ULID: &str = "01BX5ZZKBKACTAV9WEVGEMMVRZ";
    const EVENT_ULID_1: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA0";
    const EVENT_ULID_2: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA1";
    const MESSAGE_ULID_1: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA2";
    const MESSAGE_ULID_2: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA3";
    const EVENT_ULID_3: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA4";
    const EVENT_ULID_4: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA5";
    const MESSAGE_ULID_3: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA6";
    const MESSAGE_ULID_4: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA7";
    const ARTIFACT_ULID_1: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA8";
    const ARTIFACT_ULID_2: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA9";

    struct TestStore {
        _directory: TempDir,
        path: PathBuf,
        store: SqliteStore,
        run_id: RunId,
        initial: StageState,
    }

    fn digest(value: &[u8]) -> Sha256Digest {
        Sha256Digest::of_bytes(value)
    }

    fn test_store() -> TestStore {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("run.sqlite3");
        let mut store = SqliteStore::open(&path).unwrap();
        let run_id = format!("flow_{RUN_ULID}").parse().unwrap();
        let initial = StageState::new(
            format!("stage_{STAGE_ULID}").parse().unwrap(),
            digest(b"component"),
            digest(b"predicate"),
        );
        store.create_run(&run_id, &digest(b"pipeline")).unwrap();
        store.register_stage(&run_id, &initial).unwrap();
        TestStore {
            _directory: directory,
            path,
            store,
            run_id,
            initial,
        }
    }

    fn event_id(value: &str) -> EventId {
        format!("evt_{value}").parse().unwrap()
    }

    fn message_id(value: &str) -> MessageId {
        format!("msg_{value}").parse().unwrap()
    }

    fn ready_event(state: &StageState, input: &[u8]) -> StageEvent {
        state
            .decide(StageCommand::AcceptInputs {
                expected_revision: state.control_revision,
                input_manifest_digest: digest(input),
            })
            .unwrap()
    }

    fn append_transition(
        test: &mut TestStore,
        event: &StageEvent,
        event_ulid: &str,
        message_ulid: &str,
    ) {
        test.store
            .append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id(event_ulid),
                message_id: &message_id(message_ulid),
                message_digest: &digest(message_ulid.as_bytes()),
                event,
            })
            .unwrap();
    }

    fn advance_to_running(test: &mut TestStore) -> StageState {
        let ready = ready_event(&test.initial, b"input-manifest");
        append_transition(test, &ready, EVENT_ULID_1, MESSAGE_ULID_1);
        let ready_state = test
            .store
            .load_stage(&test.run_id, &test.initial.stage_instance_id)
            .unwrap();
        let provisioning = ready_state
            .decide(StageCommand::BeginProvisioning {
                expected_revision: ready_state.control_revision,
            })
            .unwrap();
        append_transition(test, &provisioning, EVENT_ULID_2, MESSAGE_ULID_2);
        let provisioning_state = test
            .store
            .load_stage(&test.run_id, &test.initial.stage_instance_id)
            .unwrap();
        let started = provisioning_state
            .decide(StageCommand::StartAttempt {
                expected_revision: provisioning_state.control_revision,
            })
            .unwrap();
        append_transition(test, &started, EVENT_ULID_3, MESSAGE_ULID_3);
        test.store
            .load_stage(&test.run_id, &test.initial.stage_instance_id)
            .unwrap()
    }

    fn artifact_record(
        artifact_ulid: &str,
        artifact_type: &str,
        schema_id: &str,
        bytes: &[u8],
        state: &StageState,
    ) -> ArtifactRecord {
        ArtifactRecord {
            artifact_id: format!("art_{artifact_ulid}").parse().unwrap(),
            artifact_type: artifact_type.to_owned(),
            schema_id: schema_id.to_owned(),
            schema_version: 1,
            sha256: digest(bytes),
            size: bytes.len() as u64,
            media_type: "application/json".to_owned(),
            producer_stage_instance_id: state.stage_instance_id.clone(),
            producer_attempt: state.attempt,
            producer_event_sequence: 4,
            pipeline_definition_digest: digest(b"pipeline"),
            component_digest: state.component_digest,
            input_manifest_digest: state.input_manifest_digest.unwrap(),
            retention_class: "run-record".to_owned(),
        }
    }

    #[test]
    fn enables_required_sqlite_durability_settings() {
        assert!(matches!(
            SqliteStore::from_connection(Connection::open_in_memory().unwrap()),
            Err(StoreError::IncompatiblePragma("journal_mode"))
        ));
        let test = test_store();
        let journal_mode: String = test
            .store
            .connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        let synchronous: i64 = test
            .store
            .connection
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        let foreign_keys: i64 = test
            .store
            .connection
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();
        let trusted_schema: i64 = test
            .store
            .connection
            .pragma_query_value(None, "trusted_schema", |row| row.get(0))
            .unwrap();

        assert_eq!(journal_mode, "wal");
        assert_eq!(synchronous, 2);
        assert_eq!(foreign_keys, 1);
        assert_eq!(trusted_schema, 0);
    }

    #[test]
    fn commits_events_and_snapshots_atomically_and_replays_after_restart() {
        let mut test = test_store();
        let ready = ready_event(&test.initial, b"input");
        let event_id_1 = event_id(EVENT_ULID_1);
        let message_id_1 = message_id(MESSAGE_ULID_1);
        let outcome = test
            .store
            .append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id_1,
                message_id: &message_id_1,
                message_digest: &digest(b"message-1"),
                event: &ready,
            })
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Committed(_)));

        let ready_state = test
            .store
            .load_stage(&test.run_id, &test.initial.stage_instance_id)
            .unwrap();
        let provisioning = ready_state
            .decide(StageCommand::BeginProvisioning {
                expected_revision: ready_state.control_revision,
            })
            .unwrap();
        test.store
            .append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id(EVENT_ULID_2),
                message_id: &message_id(MESSAGE_ULID_2),
                message_digest: &digest(b"message-2"),
                event: &provisioning,
            })
            .unwrap();

        drop(test.store);
        let reopened = SqliteStore::open(&test.path).unwrap();
        let snapshot = reopened
            .load_stage(&test.run_id, &test.initial.stage_instance_id)
            .unwrap();
        let stored = reopened
            .load_stage_events(&test.run_id, &test.initial.stage_instance_id)
            .unwrap();
        let events: Vec<_> = stored.iter().map(|stored| &stored.event).collect();

        assert_eq!(stored[0].sequence, 1);
        assert_eq!(stored[1].sequence, 2);
        assert_eq!(replay_stage(&test.initial, events).unwrap(), snapshot);
        assert_eq!(snapshot.control_revision, 2);
    }

    #[test]
    fn recovery_reads_one_consistent_wal_snapshot_during_concurrent_append() {
        let test = test_store();
        let mut writer = SqliteStore::open(&test.path).unwrap();
        let event = ready_event(&test.initial, b"input");
        let state_during_append = test
            .store
            .load_stage_consistently(&test.run_id, &test.initial.stage_instance_id, || {
                writer
                    .append_stage_event(AppendStageEvent {
                        run_id: &test.run_id,
                        event_id: &event_id(EVENT_ULID_1),
                        message_id: &message_id(MESSAGE_ULID_1),
                        message_digest: &digest(b"message-1"),
                        event: &event,
                    })
                    .unwrap();
            })
            .unwrap();

        assert_eq!(state_during_append, test.initial);
        assert_eq!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id)
                .unwrap()
                .control_revision,
            1
        );
    }

    #[test]
    fn exact_message_retry_is_idempotent() {
        let mut test = test_store();
        let event = ready_event(&test.initial, b"input");
        let event_id = event_id(EVENT_ULID_1);
        let message_id = message_id(MESSAGE_ULID_1);
        let message_digest = digest(b"message-1");
        let request = || AppendStageEvent {
            run_id: &test.run_id,
            event_id: &event_id,
            message_id: &message_id,
            message_digest: &message_digest,
            event: &event,
        };

        assert!(matches!(
            test.store.append_stage_event(request()).unwrap(),
            AppendOutcome::Committed(_)
        ));
        assert!(matches!(
            test.store.append_stage_event(request()).unwrap(),
            AppendOutcome::Duplicate(_)
        ));
        assert_eq!(test.store.event_count(&test.run_id).unwrap(), 1);
        assert_eq!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id)
                .unwrap()
                .control_revision,
            1
        );
    }

    #[test]
    fn reused_message_id_with_different_content_is_rejected() {
        let mut test = test_store();
        let first = ready_event(&test.initial, b"first");
        let second = ready_event(&test.initial, b"second");
        let event_id = event_id(EVENT_ULID_1);
        let message_id = message_id(MESSAGE_ULID_1);
        test.store
            .append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id,
                message_id: &message_id,
                message_digest: &digest(b"first-message"),
                event: &first,
            })
            .unwrap();

        assert!(matches!(
            test.store.append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id,
                message_id: &message_id,
                message_digest: &digest(b"second-message"),
                event: &second,
            }),
            Err(StoreError::MessageIdConflict)
        ));
        assert_eq!(test.store.event_count(&test.run_id).unwrap(), 1);
    }

    #[test]
    fn message_retry_requires_the_same_authenticated_message_digest() {
        let mut test = test_store();
        let event = ready_event(&test.initial, b"input");
        let event_id = event_id(EVENT_ULID_1);
        let message_id = message_id(MESSAGE_ULID_1);
        test.store
            .append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id,
                message_id: &message_id,
                message_digest: &digest(b"original-message"),
                event: &event,
            })
            .unwrap();

        assert!(matches!(
            test.store.append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id,
                message_id: &message_id,
                message_digest: &digest(b"different-message"),
                event: &event,
            }),
            Err(StoreError::MessageIdConflict)
        ));
    }

    #[test]
    fn transaction_rolls_back_after_event_insert_failure_and_restart() {
        let mut test = test_store();
        let event = ready_event(&test.initial, b"input");
        test.store.inject_failure_after_event_insert(true);

        assert!(matches!(
            test.store.append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id(EVENT_ULID_1),
                message_id: &message_id(MESSAGE_ULID_1),
                message_digest: &digest(b"message-1"),
                event: &event,
            }),
            Err(StoreError::CorruptData("injected post-insert failure"))
        ));
        drop(test.store);

        let reopened = SqliteStore::open(&test.path).unwrap();
        assert_eq!(reopened.event_count(&test.run_id).unwrap(), 0);
        assert_eq!(
            reopened
                .load_stage(&test.run_id, &test.initial.stage_instance_id)
                .unwrap(),
            test.initial
        );
    }

    #[test]
    fn invalid_transition_rolls_back_event_and_snapshot() {
        let mut test = test_store();
        let invalid = StageEvent {
            stage_instance_id: test.initial.stage_instance_id.clone(),
            prior_control_revision: 0,
            kind: StageEventKind::NodeStarted { attempt: 1 },
        };

        assert!(matches!(
            test.store.append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id(EVENT_ULID_1),
                message_id: &message_id(MESSAGE_ULID_1),
                message_digest: &digest(b"invalid-message"),
                event: &invalid,
            }),
            Err(StoreError::StageTransition(_))
        ));
        assert_eq!(test.store.event_count(&test.run_id).unwrap(), 0);
        assert_eq!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id)
                .unwrap(),
            test.initial
        );
    }

    #[test]
    fn replay_detects_tampered_snapshots_and_events() {
        let mut test = test_store();
        let event = ready_event(&test.initial, b"input");
        test.store
            .append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id(EVENT_ULID_1),
                message_id: &message_id(MESSAGE_ULID_1),
                message_digest: &digest(b"message-1"),
                event: &event,
            })
            .unwrap();

        let initial_json = serde_json::to_vec(&test.initial).unwrap();
        test.store
            .connection
            .execute(
                "UPDATE stage_snapshots SET state_json = ?1 WHERE stage_instance_id = ?2",
                params![initial_json, test.initial.stage_instance_id.as_str()],
            )
            .unwrap();
        assert!(matches!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id),
            Err(StoreError::SnapshotMismatch)
        ));

        let correct_state = test.initial.apply(&event).unwrap();
        test.store
            .connection
            .execute(
                "UPDATE stage_snapshots SET state_json = ?1 WHERE stage_instance_id = ?2",
                params![
                    serde_json::to_vec(&correct_state).unwrap(),
                    test.initial.stage_instance_id.as_str()
                ],
            )
            .unwrap();
        test.store
            .connection
            .execute(
                "UPDATE events SET event_digest = ?1 WHERE event_id = ?2",
                params![
                    digest(b"tampered").to_prefixed_string(),
                    event_id(EVENT_ULID_1).as_str()
                ],
            )
            .unwrap();
        assert!(matches!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id),
            Err(StoreError::CorruptData(_))
        ));
    }

    #[test]
    fn recovery_rejects_index_and_run_sequence_corruption() {
        let mut test = test_store();
        let event = ready_event(&test.initial, b"input");
        test.store
            .append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id(EVENT_ULID_1),
                message_id: &message_id(MESSAGE_ULID_1),
                message_digest: &digest(b"message-1"),
                event: &event,
            })
            .unwrap();

        test.store
            .connection
            .execute(
                "UPDATE events SET sequence = 2 WHERE event_id = ?1",
                params![event_id(EVENT_ULID_1).as_str()],
            )
            .unwrap();
        assert!(matches!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id),
            Err(StoreError::CorruptData(_))
        ));

        test.store
            .connection
            .execute(
                "UPDATE events SET sequence = 1 WHERE event_id = ?1",
                params![event_id(EVENT_ULID_1).as_str()],
            )
            .unwrap();
        test.store
            .connection
            .execute(
                "UPDATE runs SET next_event_sequence = 3 WHERE run_id = ?1",
                params![test.run_id.as_str()],
            )
            .unwrap();
        assert!(matches!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id),
            Err(StoreError::CorruptData(
                "run sequence counter does not match the journal"
            ))
        ));
    }

    #[test]
    fn rejects_non_pristine_stage_bootstrap() {
        let mut test = test_store();
        let mut invalid = StageState::new(
            format!("stage_{RUN_ULID}").parse().unwrap(),
            digest(b"component"),
            digest(b"predicate"),
        );
        invalid.status_reason_digest = Some(digest(b"impossible-pending-reason"));

        assert!(matches!(
            test.store.register_stage(&test.run_id, &invalid),
            Err(StoreError::InvalidInitialStage)
        ));
    }

    #[test]
    fn production_api_bootstraps_ingress_and_reaches_running() {
        let mut test = test_store();
        let artifact_store = ArtifactStore::open(test._directory.path().join("artifacts")).unwrap();
        let input_bytes = br#"{"inputs":[]}"#;
        let input_digest = digest(input_bytes);
        let ingress = ArtifactRecord {
            artifact_id: format!("art_{ARTIFACT_ULID_1}").parse().unwrap(),
            artifact_type: "stage-input-manifest/v1".to_owned(),
            schema_id: "stage-input-manifest".to_owned(),
            schema_version: 1,
            sha256: input_digest,
            size: input_bytes.len() as u64,
            media_type: "application/json".to_owned(),
            producer_stage_instance_id: test.initial.stage_instance_id.clone(),
            producer_attempt: 0,
            producer_event_sequence: 1,
            pipeline_definition_digest: digest(b"pipeline"),
            component_digest: test.initial.component_digest,
            input_manifest_digest: input_digest,
            retention_class: "run-record".to_owned(),
        };
        let ready = ready_event(&test.initial, input_bytes);
        test.store
            .append_stage_event_with_artifacts(
                AppendStageEvent {
                    run_id: &test.run_id,
                    event_id: &event_id(EVENT_ULID_1),
                    message_id: &message_id(MESSAGE_ULID_1),
                    message_digest: &digest(b"ready-message"),
                    event: &ready,
                },
                &artifact_store,
                &[ArtifactRegistration {
                    record: &ingress,
                    parent_artifact_ids: &[],
                    bytes: input_bytes,
                }],
            )
            .unwrap();

        let ready_state = test
            .store
            .load_stage(&test.run_id, &test.initial.stage_instance_id)
            .unwrap();
        let provisioning = ready_state
            .decide(StageCommand::BeginProvisioning {
                expected_revision: ready_state.control_revision,
            })
            .unwrap();
        test.store
            .append_stage_event_with_artifacts(
                AppendStageEvent {
                    run_id: &test.run_id,
                    event_id: &event_id(EVENT_ULID_2),
                    message_id: &message_id(MESSAGE_ULID_2),
                    message_digest: &digest(b"provisioning-message"),
                    event: &provisioning,
                },
                &artifact_store,
                &[],
            )
            .unwrap();
        let provisioning_state = test
            .store
            .load_stage(&test.run_id, &test.initial.stage_instance_id)
            .unwrap();
        let started = provisioning_state
            .decide(StageCommand::StartAttempt {
                expected_revision: provisioning_state.control_revision,
            })
            .unwrap();
        test.store
            .append_stage_event_with_artifacts(
                AppendStageEvent {
                    run_id: &test.run_id,
                    event_id: &event_id(EVENT_ULID_3),
                    message_id: &message_id(MESSAGE_ULID_3),
                    message_digest: &digest(b"started-message"),
                    event: &started,
                },
                &artifact_store,
                &[],
            )
            .unwrap();

        assert_eq!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id)
                .unwrap()
                .phase,
            herdr_flow_core::StagePhase::Running
        );
        assert_eq!(
            test.store
                .verify_run_artifacts(&test.run_id, &artifact_store)
                .unwrap()
                .len(),
            1
        );
    }

    fn complete_with_artifacts(
        test: &mut TestStore,
        artifact_store: &ArtifactStore,
    ) -> (ArtifactRecord, ArtifactRecord) {
        let running = advance_to_running(test);
        let parent_bytes = br#"{"kind":"parent"}"#;
        let child_bytes = br#"{"kind":"child"}"#;
        let parent = artifact_record(
            ARTIFACT_ULID_1,
            "review-package/v1",
            "review-package",
            parent_bytes,
            &running,
        );
        let child = artifact_record(
            ARTIFACT_ULID_2,
            "publication-manifest/v1",
            "publication-manifest",
            child_bytes,
            &running,
        );
        let completed = running
            .decide(StageCommand::Complete {
                expected_revision: running.control_revision,
                output_manifest_digest: child.sha256,
                completion_predicate_digest: running.completion_predicate_digest,
                completion_evidence_digest: parent.sha256,
            })
            .unwrap();
        let event_id = event_id(EVENT_ULID_4);
        let message_id = message_id(MESSAGE_ULID_4);
        let message_digest = digest(b"completion-message");
        let registrations = [
            ArtifactRegistration {
                record: &parent,
                parent_artifact_ids: &[],
                bytes: parent_bytes,
            },
            ArtifactRegistration {
                record: &child,
                parent_artifact_ids: std::slice::from_ref(&parent.artifact_id),
                bytes: child_bytes,
            },
        ];
        test.store
            .append_stage_event_with_artifacts(
                AppendStageEvent {
                    run_id: &test.run_id,
                    event_id: &event_id,
                    message_id: &message_id,
                    message_digest: &message_digest,
                    event: &completed,
                },
                artifact_store,
                &registrations,
            )
            .unwrap();
        (parent, child)
    }

    #[test]
    fn artifact_bytes_records_edges_and_completion_commit_as_one_unit() {
        let mut test = test_store();
        let artifact_store = ArtifactStore::open(test._directory.path().join("artifacts")).unwrap();
        let (parent, child) = complete_with_artifacts(&mut test, &artifact_store);

        let records = test
            .store
            .verify_run_artifacts(&test.run_id, &artifact_store)
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(
            test.store
                .load_artifact(&test.run_id, &child.artifact_id, &artifact_store)
                .unwrap()
                .parent_artifact_ids,
            vec![parent.artifact_id.clone()]
        );
        assert_eq!(
            test.store
                .artifact_descendants(&test.run_id, &parent.artifact_id, &artifact_store)
                .unwrap(),
            vec![child.artifact_id.clone()]
        );
        assert_eq!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id)
                .unwrap()
                .phase,
            herdr_flow_core::StagePhase::Completed
        );

        let events = test
            .store
            .load_stage_events(&test.run_id, &test.initial.stage_instance_id)
            .unwrap();
        assert!(matches!(
            test.store.append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &event_id(EVENT_ULID_4),
                message_id: &message_id(MESSAGE_ULID_4),
                message_digest: &digest(b"completion-message"),
                event: &events[3].event,
            }),
            Err(StoreError::MessageIdConflict)
        ));

        let parent_bytes = br#"{"kind":"parent"}"#;
        let child_bytes = br#"{"kind":"child"}"#;
        let registrations = [
            ArtifactRegistration {
                record: &parent,
                parent_artifact_ids: &[],
                bytes: parent_bytes,
            },
            ArtifactRegistration {
                record: &child,
                parent_artifact_ids: std::slice::from_ref(&parent.artifact_id),
                bytes: child_bytes,
            },
        ];
        assert!(matches!(
            test.store
                .append_stage_event_with_artifacts(
                    AppendStageEvent {
                        run_id: &test.run_id,
                        event_id: &event_id(EVENT_ULID_4),
                        message_id: &message_id(MESSAGE_ULID_4),
                        message_digest: &digest(b"completion-message"),
                        event: &events[3].event,
                    },
                    &artifact_store,
                    &registrations,
                )
                .unwrap(),
            AppendOutcome::Duplicate(_)
        ));
        assert_eq!(test.store.event_count(&test.run_id).unwrap(), 4);
    }

    #[test]
    fn post_insert_failure_rolls_back_artifacts_event_and_snapshot_together() {
        let mut test = test_store();
        let artifact_store = ArtifactStore::open(test._directory.path().join("artifacts")).unwrap();
        let running = advance_to_running(&mut test);
        let bytes = br#"{"kind":"output"}"#;
        let record = artifact_record(
            ARTIFACT_ULID_1,
            "review-package/v1",
            "review-package",
            bytes,
            &running,
        );
        let completed = running
            .decide(StageCommand::Complete {
                expected_revision: running.control_revision,
                output_manifest_digest: record.sha256,
                completion_predicate_digest: running.completion_predicate_digest,
                completion_evidence_digest: record.sha256,
            })
            .unwrap();
        let event_id = event_id(EVENT_ULID_4);
        let message_id = message_id(MESSAGE_ULID_4);
        let message_digest = digest(b"completion-message");
        let registration = [ArtifactRegistration {
            record: &record,
            parent_artifact_ids: &[],
            bytes,
        }];
        test.store.inject_failure_after_event_insert(true);

        assert!(matches!(
            test.store.append_stage_event_with_artifacts(
                AppendStageEvent {
                    run_id: &test.run_id,
                    event_id: &event_id,
                    message_id: &message_id,
                    message_digest: &message_digest,
                    event: &completed,
                },
                &artifact_store,
                &registration,
            ),
            Err(StoreError::CorruptData("injected post-insert failure"))
        ));
        assert_eq!(test.store.event_count(&test.run_id).unwrap(), 3);
        let artifact_count: i64 = test
            .store
            .connection
            .query_row("SELECT COUNT(*) FROM artifacts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(artifact_count, 0);
        assert_eq!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id)
                .unwrap(),
            running
        );
    }

    #[test]
    fn accepted_event_cannot_reference_an_unregistered_artifact_digest() {
        let mut test = test_store();
        let artifact_store = ArtifactStore::open(test._directory.path().join("artifacts")).unwrap();
        let running = advance_to_running(&mut test);
        let bytes = b"output";
        let record = artifact_record(
            ARTIFACT_ULID_1,
            "review-package/v1",
            "review-package",
            bytes,
            &running,
        );
        let completed = running
            .decide(StageCommand::Complete {
                expected_revision: running.control_revision,
                output_manifest_digest: record.sha256,
                completion_predicate_digest: running.completion_predicate_digest,
                completion_evidence_digest: digest(b"missing-evidence"),
            })
            .unwrap();

        assert!(matches!(
            test.store.append_stage_event_with_artifacts(
                AppendStageEvent {
                    run_id: &test.run_id,
                    event_id: &event_id(EVENT_ULID_4),
                    message_id: &message_id(MESSAGE_ULID_4),
                    message_digest: &digest(b"completion-message"),
                    event: &completed,
                },
                &artifact_store,
                &[ArtifactRegistration {
                    record: &record,
                    parent_artifact_ids: &[],
                    bytes,
                }],
            ),
            Err(StoreError::ArtifactReferenceNotFound)
        ));
        assert_eq!(test.store.event_count(&test.run_id).unwrap(), 3);
        assert_eq!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id)
                .unwrap(),
            running
        );
    }

    #[test]
    fn artifact_registration_rejects_unbound_metadata_without_accepting_event() {
        let mut test = test_store();
        let artifact_store = ArtifactStore::open(test._directory.path().join("artifacts")).unwrap();
        let running = advance_to_running(&mut test);
        let bytes = b"output";
        let mut record = artifact_record(
            ARTIFACT_ULID_1,
            "review-package/v1",
            "review-package",
            bytes,
            &running,
        );
        record.pipeline_definition_digest = digest(b"another-pipeline");
        let completed = running
            .decide(StageCommand::Complete {
                expected_revision: running.control_revision,
                output_manifest_digest: record.sha256,
                completion_predicate_digest: running.completion_predicate_digest,
                completion_evidence_digest: digest(b"evidence"),
            })
            .unwrap();

        let append = |store: &mut SqliteStore, record: &ArtifactRecord| {
            store.append_stage_event_with_artifacts(
                AppendStageEvent {
                    run_id: &test.run_id,
                    event_id: &event_id(EVENT_ULID_4),
                    message_id: &message_id(MESSAGE_ULID_4),
                    message_digest: &digest(b"completion-message"),
                    event: &completed,
                },
                &artifact_store,
                &[ArtifactRegistration {
                    record,
                    parent_artifact_ids: &[],
                    bytes,
                }],
            )
        };
        assert!(matches!(
            append(&mut test.store, &record),
            Err(StoreError::ArtifactMetadataMismatch(
                "pipeline definition digest does not match the run"
            ))
        ));
        record.pipeline_definition_digest = digest(b"pipeline");
        record.producer_event_sequence = 5;
        assert!(matches!(
            append(&mut test.store, &record),
            Err(StoreError::ArtifactMetadataMismatch(
                "producer sequence does not match accepted event"
            ))
        ));
        assert_eq!(test.store.event_count(&test.run_id).unwrap(), 3);
        assert_eq!(
            test.store
                .load_stage(&test.run_id, &test.initial.stage_instance_id)
                .unwrap(),
            running
        );
    }

    #[test]
    fn recovery_rejects_missing_bytes_and_tampered_registry_records() {
        let mut test = test_store();
        let artifact_store = ArtifactStore::open(test._directory.path().join("artifacts")).unwrap();
        let (parent, child) = complete_with_artifacts(&mut test, &artifact_store);
        test.store
            .connection
            .execute(
                "UPDATE runs SET pipeline_definition_digest = ?1 WHERE run_id = ?2",
                params![
                    digest(b"another-pipeline").to_prefixed_string(),
                    test.run_id.as_str()
                ],
            )
            .unwrap();
        assert!(matches!(
            test.store
                .verify_run_artifacts(&test.run_id, &artifact_store),
            Err(StoreError::ArtifactMetadataMismatch(
                "pipeline definition digest does not match the run"
            ))
        ));
        test.store
            .connection
            .execute(
                "UPDATE runs SET pipeline_definition_digest = ?1 WHERE run_id = ?2",
                params![
                    digest(b"pipeline").to_prefixed_string(),
                    test.run_id.as_str()
                ],
            )
            .unwrap();

        let second_stage = StageState::new(
            format!("stage_{RUN_ULID}").parse().unwrap(),
            digest(b"other-component"),
            digest(b"other-predicate"),
        );
        test.store
            .register_stage(&test.run_id, &second_stage)
            .unwrap();
        test.store
            .connection
            .execute(
                "UPDATE artifacts SET producer_stage_instance_id = ?1 WHERE artifact_id = ?2",
                params![
                    second_stage.stage_instance_id.as_str(),
                    parent.artifact_id.as_str()
                ],
            )
            .unwrap();
        assert!(matches!(
            test.store
                .verify_run_artifacts(&test.run_id, &artifact_store),
            Err(StoreError::ArtifactMetadataMismatch(
                "indexed artifact columns do not match canonical record"
            ))
        ));
        test.store
            .connection
            .execute(
                "UPDATE artifacts SET producer_stage_instance_id = ?1 WHERE artifact_id = ?2",
                params![
                    parent.producer_stage_instance_id.as_str(),
                    parent.artifact_id.as_str()
                ],
            )
            .unwrap();
        test.store
            .connection
            .execute(
                "DELETE FROM artifact_edges WHERE child_artifact_id = ?1",
                params![child.artifact_id.as_str()],
            )
            .unwrap();
        assert!(matches!(
            test.store
                .verify_run_artifacts(&test.run_id, &artifact_store),
            Err(StoreError::ArtifactMetadataMismatch(
                "event artifact commitment mismatch"
            ))
        ));
        test.store
            .connection
            .execute(
                "INSERT INTO artifact_edges(run_id, parent_artifact_id, child_artifact_id)
                 VALUES (?1, ?2, ?3)",
                params![
                    test.run_id.as_str(),
                    parent.artifact_id.as_str(),
                    child.artifact_id.as_str()
                ],
            )
            .unwrap();
        test.store
            .connection
            .execute(
                "UPDATE artifacts SET record_digest = ?1 WHERE artifact_id = ?2",
                params![
                    digest(b"tampered").to_prefixed_string(),
                    parent.artifact_id.as_str()
                ],
            )
            .unwrap();
        assert!(matches!(
            test.store
                .verify_run_artifacts(&test.run_id, &artifact_store),
            Err(StoreError::ArtifactMetadataMismatch(
                "artifact record digest mismatch"
            ))
        ));

        test.store
            .connection
            .execute(
                "UPDATE artifacts SET record_digest = ?1 WHERE artifact_id = ?2",
                params![
                    Sha256Digest::of_bytes(
                        &canonical_json::to_vec(&serde_json::to_value(&parent).unwrap()).unwrap()
                    )
                    .to_prefixed_string(),
                    parent.artifact_id.as_str()
                ],
            )
            .unwrap();
        let digest_text = parent.sha256.to_prefixed_string();
        fs::remove_file(
            artifact_store
                .root()
                .join("sha256")
                .join(&digest_text["sha256:".len()..]),
        )
        .unwrap();
        assert!(matches!(
            test.store
                .verify_run_artifacts(&test.run_id, &artifact_store),
            Err(StoreError::ArtifactStore(_))
        ));
    }

    #[test]
    fn event_ids_are_globally_unique() {
        let mut test = test_store();
        let ready = ready_event(&test.initial, b"input");
        let shared_event_id = event_id(EVENT_ULID_1);
        test.store
            .append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &shared_event_id,
                message_id: &message_id(MESSAGE_ULID_1),
                message_digest: &digest(b"message-1"),
                event: &ready,
            })
            .unwrap();
        let ready_state = test
            .store
            .load_stage(&test.run_id, &test.initial.stage_instance_id)
            .unwrap();
        let provisioning = ready_state
            .decide(StageCommand::BeginProvisioning {
                expected_revision: 1,
            })
            .unwrap();

        assert!(matches!(
            test.store.append_stage_event(AppendStageEvent {
                run_id: &test.run_id,
                event_id: &shared_event_id,
                message_id: &message_id(MESSAGE_ULID_2),
                message_digest: &digest(b"message-2"),
                event: &provisioning,
            }),
            Err(StoreError::EventIdConflict)
        ));
    }
}
