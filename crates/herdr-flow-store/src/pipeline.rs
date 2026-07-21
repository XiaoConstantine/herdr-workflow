use std::str::FromStr;

#[cfg(test)]
use herdr_flow_core::MAX_CONTROL_REVISION;
use herdr_flow_core::{
    canonical_json, replay_pipeline, EventId, MessageId, PipelineEvent, PipelineState, RunId,
    Sha256Digest,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::{event_by_message_id, require_run, to_sql_integer};
use crate::{from_sql_integer, verify_run_journal, SqliteStore, StoreError};

#[cfg(test)]
pub(crate) struct AppendPipelineEvent<'a> {
    pub run_id: &'a RunId,
    pub event_id: &'a EventId,
    pub message_id: &'a MessageId,
    pub message_digest: &'a Sha256Digest,
    pub event: &'a PipelineEvent,
}

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

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AppendPipelineOutcome {
    Committed(StoredPipelineEvent),
    Duplicate(StoredPipelineEvent),
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
    #[cfg(test)]
    pub(crate) fn register_pipeline(
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

    #[cfg(test)]
    pub(crate) fn append_pipeline_event(
        &mut self,
        request: AppendPipelineEvent<'_>,
    ) -> Result<AppendPipelineOutcome, StoreError> {
        let fail_after_event_insert = self.fail_after_event_insert;
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, request.run_id)?;

        if event_by_message_id(&transaction, request.message_id)?.is_some() {
            return Err(StoreError::MessageIdConflict);
        }
        if let Some(existing) = pipeline_event_by_message_id(&transaction, request.message_id)? {
            if existing.run_id == *request.run_id
                && existing.message_digest == *request.message_digest
                && existing.event == *request.event
            {
                return Ok(AppendPipelineOutcome::Duplicate(existing));
            }
            return Err(StoreError::MessageIdConflict);
        }
        if crate::event_id_exists(&transaction, request.event_id)? {
            return Err(StoreError::EventIdConflict);
        }

        let state = verified_pipeline(&transaction, request.run_id)?;
        let next_state = state
            .apply(request.event)
            .map_err(StoreError::PipelineTransition)?;
        let next_state_json = serde_json::to_vec(&next_state).map_err(StoreError::Serialization)?;
        let next_sequence: i64 = transaction
            .query_row(
                "SELECT next_event_sequence FROM runs WHERE run_id = ?1",
                params![request.run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        let sequence = from_sql_integer(next_sequence)?;
        if sequence >= MAX_CONTROL_REVISION {
            return Err(StoreError::EventSequenceExhausted);
        }

        let canonical_record = CanonicalCommittedPipelineEvent {
            event_id: request.event_id.clone(),
            run_id: request.run_id.clone(),
            sequence,
            message_id: request.message_id.clone(),
            message_digest: *request.message_digest,
            prior_control_revision: request.event.prior_control_revision,
            event: request.event.clone(),
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
                    request.event_id.as_str(),
                    request.run_id.as_str(),
                    next_sequence,
                    request.message_id.as_str(),
                    request.message_digest.to_prefixed_string(),
                    to_sql_integer(request.event.prior_control_revision)?,
                    event_digest.to_prefixed_string(),
                    event_json,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        #[cfg(test)]
        if fail_after_event_insert {
            return Err(StoreError::CorruptData(
                "injected pipeline post-insert failure",
            ));
        }
        let updated = transaction
            .execute(
                "UPDATE pipeline_snapshots
                 SET control_revision = ?1, state_json = ?2
                 WHERE run_id = ?3 AND control_revision = ?4",
                params![
                    to_sql_integer(next_state.control_revision)?,
                    next_state_json,
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
                    next_sequence
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if run_updated != 1 {
            return Err(StoreError::ConcurrentUpdate);
        }
        let stored = StoredPipelineEvent {
            event_id: request.event_id.clone(),
            run_id: request.run_id.clone(),
            sequence,
            message_id: request.message_id.clone(),
            message_digest: *request.message_digest,
            event_digest,
            event: request.event.clone(),
        };
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(AppendPipelineOutcome::Committed(stored))
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

pub(crate) fn verify_pipeline_journal(
    connection: &Connection,
    run_id: &RunId,
) -> Result<Vec<u64>, StoreError> {
    let events = load_pipeline_events(connection, run_id)?;
    if pipeline_snapshot_exists(connection, run_id)? {
        verified_pipeline(connection, run_id)?;
    } else if !events.is_empty() {
        return Err(StoreError::PipelineNotFound);
    }
    Ok(events.into_iter().map(|event| event.sequence).collect())
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

#[cfg(test)]
fn pipeline_event_by_message_id(
    transaction: &Transaction<'_>,
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
    use std::path::PathBuf;

    use herdr_flow_core::{PipelineCommand, PipelineNodeDefinition, StageInstanceId, StageState};
    use tempfile::TempDir;

    use super::*;
    use crate::StoreError;

    const RUN: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const STAGE: &str = "01BX5ZZKBKACTAV9WEVGEMMVRZ";
    const EVENT_1: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA0";
    const MESSAGE_1: &str = "01ARZ3NDEKTSV4RRFFQ69G5FA3";

    struct TestStore {
        _directory: TempDir,
        path: PathBuf,
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
            path,
            store,
            run_id,
            stage,
            initial,
        }
    }

    #[test]
    fn pipeline_events_replay_identically_after_restart_and_retry_idempotently() {
        let mut test = setup();
        let event = test
            .initial
            .decide(PipelineCommand::ScheduleStage {
                expected_revision: 0,
                stage_instance_id: test.stage.stage_instance_id.clone(),
            })
            .unwrap();
        let event_id = event_id(EVENT_1);
        let message_id = message_id(MESSAGE_1);
        let message_digest = digest(b"schedule");
        let request = || AppendPipelineEvent {
            run_id: &test.run_id,
            event_id: &event_id,
            message_id: &message_id,
            message_digest: &message_digest,
            event: &event,
        };

        assert!(matches!(
            test.store.append_pipeline_event(request()).unwrap(),
            AppendPipelineOutcome::Committed(_)
        ));
        assert!(matches!(
            test.store.append_pipeline_event(request()).unwrap(),
            AppendPipelineOutcome::Duplicate(_)
        ));
        let expected = test.initial.apply(&event).unwrap();
        assert_eq!(test.store.load_pipeline(&test.run_id).unwrap(), expected);

        drop(test.store);
        let reopened = SqliteStore::open(&test.path).unwrap();
        assert_eq!(reopened.load_pipeline(&test.run_id).unwrap(), expected);
        assert_eq!(
            reopened.load_pipeline_events(&test.run_id).unwrap().len(),
            1
        );
    }

    #[test]
    fn registered_pipeline_rejects_independent_stage_roots() {
        let mut test = setup();

        assert!(matches!(
            test.store.register_stage(&test.run_id, &test.stage),
            Err(StoreError::PipelineStageRegistrationRequired)
        ));
    }

    #[test]
    fn pipeline_event_failure_rolls_back_event_snapshot_and_run_sequence() {
        let mut test = setup();
        let event = test
            .initial
            .decide(PipelineCommand::ScheduleStage {
                expected_revision: 0,
                stage_instance_id: test.stage.stage_instance_id.clone(),
            })
            .unwrap();
        test.store.fail_after_event_insert = true;

        assert!(matches!(
            test.store.append_pipeline_event(AppendPipelineEvent {
                run_id: &test.run_id,
                event_id: &event_id(EVENT_1),
                message_id: &message_id(MESSAGE_1),
                message_digest: &digest(b"schedule"),
                event: &event,
            }),
            Err(StoreError::CorruptData(
                "injected pipeline post-insert failure"
            ))
        ));
        assert_eq!(
            test.store.load_pipeline(&test.run_id).unwrap(),
            test.initial
        );
        let count: i64 = test
            .store
            .connection
            .query_row("SELECT COUNT(*) FROM pipeline_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn recovery_rejects_tampered_pipeline_snapshots_and_global_sequence_collisions() {
        let mut test = setup();
        let event = test
            .initial
            .decide(PipelineCommand::ScheduleStage {
                expected_revision: 0,
                stage_instance_id: test.stage.stage_instance_id.clone(),
            })
            .unwrap();
        test.store
            .append_pipeline_event(AppendPipelineEvent {
                run_id: &test.run_id,
                event_id: &event_id(EVENT_1),
                message_id: &message_id(MESSAGE_1),
                message_digest: &digest(b"schedule"),
                event: &event,
            })
            .unwrap();
        test.store
            .connection
            .execute(
                "UPDATE pipeline_snapshots SET control_revision = 2 WHERE run_id = ?1",
                params![test.run_id.as_str()],
            )
            .unwrap();
        assert!(matches!(
            test.store.load_pipeline(&test.run_id),
            Err(StoreError::PipelineSnapshotMismatch)
        ));
        test.store
            .connection
            .execute(
                "UPDATE pipeline_snapshots SET control_revision = 1 WHERE run_id = ?1",
                params![test.run_id.as_str()],
            )
            .unwrap();

        test.store
            .connection
            .execute(
                "UPDATE pipeline_events SET sequence = 2 WHERE event_id = ?1",
                params![event_id(EVENT_1).as_str()],
            )
            .unwrap();
        assert!(matches!(
            test.store.load_pipeline(&test.run_id),
            Err(StoreError::CorruptData(_))
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
