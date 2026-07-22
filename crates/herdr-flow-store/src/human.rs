use herdr_flow_core::{
    canonical_json, MessageId, PublicationGateCommand, PublicationGateEventKind, RunId,
    Sha256Digest, StageInstanceId,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::{
    pipeline::verified_publication_gate, SemanticPublicationGateEntry, SqliteStore, StoreError,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueuedHumanActionStatus {
    Pending,
    Committed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QueuedHumanAction {
    pub message_id: MessageId,
    pub run_id: RunId,
    pub gate_stage_instance_id: StageInstanceId,
    pub command_digest: Sha256Digest,
    pub message_digest: Sha256Digest,
    pub command: PublicationGateCommand,
    pub status: QueuedHumanActionStatus,
}

impl SqliteStore {
    pub fn queue_human_publication_action(
        &mut self,
        message_id: &MessageId,
        run_id: &RunId,
        gate_stage_instance_id: &StageInstanceId,
        command: &PublicationGateCommand,
    ) -> Result<QueuedHumanAction, StoreError> {
        if !matches!(
            command,
            PublicationGateCommand::HumanApprove { .. }
                | PublicationGateCommand::HumanRequestChanges { .. }
                | PublicationGateCommand::HumanCancel { .. }
        ) {
            return Err(StoreError::HumanActionConflict);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        let gate = verified_publication_gate(&transaction, run_id, gate_stage_instance_id)?;
        gate.decide(command.clone())
            .map_err(StoreError::PublicationGateTransition)?;
        let value = serde_json::to_value(command).map_err(StoreError::Serialization)?;
        let json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
        let digest = Sha256Digest::of_bytes(&json);
        let message_digest =
            human_message_digest(message_id, run_id, gate_stage_instance_id, digest)?;
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO human_action_inbox(
                    message_id, run_id, gate_stage_instance_id,
                    command_digest, message_digest, command_json, status
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'PENDING')",
                params![
                    message_id.as_str(),
                    run_id.as_str(),
                    gate_stage_instance_id.as_str(),
                    digest.to_prefixed_string(),
                    message_digest.to_prefixed_string(),
                    json,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if inserted == 0 {
            let existing =
                load_action(&transaction, message_id)?.ok_or(StoreError::HumanActionConflict)?;
            if existing.run_id != *run_id
                || existing.gate_stage_instance_id != *gate_stage_instance_id
                || existing.command_digest != digest
                || existing.message_digest != message_digest
                || existing.command != *command
            {
                return Err(StoreError::HumanActionConflict);
            }
            transaction.commit().map_err(StoreError::Sqlite)?;
            return Ok(existing);
        }
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(QueuedHumanAction {
            message_id: message_id.clone(),
            run_id: run_id.clone(),
            gate_stage_instance_id: gate_stage_instance_id.clone(),
            command_digest: digest,
            message_digest,
            command: command.clone(),
            status: QueuedHumanActionStatus::Pending,
        })
    }

    pub fn load_human_action(
        &self,
        message_id: &MessageId,
    ) -> Result<QueuedHumanAction, StoreError> {
        load_action(&self.connection, message_id)?.ok_or(StoreError::HumanActionNotFound)
    }
}

fn human_message_digest(
    message_id: &MessageId,
    run_id: &RunId,
    gate_stage_instance_id: &StageInstanceId,
    command_digest: Sha256Digest,
) -> Result<Sha256Digest, StoreError> {
    #[derive(Serialize)]
    struct HumanMessageCommitment<'a> {
        message_id: &'a MessageId,
        run_id: &'a RunId,
        gate_stage_instance_id: &'a StageInstanceId,
        command_digest: Sha256Digest,
    }
    let value = serde_json::to_value(HumanMessageCommitment {
        message_id,
        run_id,
        gate_stage_instance_id,
        command_digest,
    })
    .map_err(StoreError::Serialization)?;
    let json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    Ok(Sha256Digest::of_bytes(&json))
}

pub(crate) fn commit_matching_action(
    transaction: &Transaction<'_>,
    entry: &SemanticPublicationGateEntry<'_>,
) -> Result<(), StoreError> {
    let is_human = matches!(
        entry.event.kind,
        PublicationGateEventKind::HumanApproved { .. }
            | PublicationGateEventKind::HumanRequestedChanges { .. }
            | PublicationGateEventKind::HumanCancelled { .. }
    );
    if !is_human {
        return Ok(());
    }
    let action =
        load_action(transaction, entry.message_id)?.ok_or(StoreError::HumanActionNotFound)?;
    if action.status != QueuedHumanActionStatus::Pending
        || action.run_id != entry.event.run_id
        || action.gate_stage_instance_id != entry.event.stage_instance_id
        || action.message_digest != *entry.message_digest
    {
        return Err(StoreError::HumanActionConflict);
    }
    let gate =
        verified_publication_gate(transaction, &action.run_id, &action.gate_stage_instance_id)?;
    let expected_event = gate
        .decide(action.command.clone())
        .map_err(StoreError::PublicationGateTransition)?;
    if expected_event != *entry.event {
        return Err(StoreError::HumanActionConflict);
    }
    let updated = transaction
        .execute(
            "UPDATE human_action_inbox SET status = 'COMMITTED'
             WHERE message_id = ?1 AND status = 'PENDING'",
            params![entry.message_id.as_str()],
        )
        .map_err(StoreError::Sqlite)?;
    if updated != 1 {
        return Err(StoreError::HumanActionConflict);
    }
    Ok(())
}

pub(crate) fn load_action(
    connection: &Connection,
    message_id: &MessageId,
) -> Result<Option<QueuedHumanAction>, StoreError> {
    let row: Option<(String, String, String, String, Vec<u8>, String)> = connection
        .query_row(
            "SELECT run_id, gate_stage_instance_id, command_digest, message_digest,
                    command_json, status
             FROM human_action_inbox WHERE message_id = ?1",
            params![message_id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    row.map(|(run_id, stage_id, digest, message_digest, json, status)| {
        let run_id = RunId::parse(run_id).map_err(StoreError::Identifier)?;
        let gate_stage_instance_id =
            StageInstanceId::parse(stage_id).map_err(StoreError::Identifier)?;
        let command_digest = digest.parse().map_err(StoreError::Digest)?;
        let message_digest = message_digest.parse().map_err(StoreError::Digest)?;
        if message_digest
            != human_message_digest(message_id, &run_id, &gate_stage_instance_id, command_digest)?
        {
            return Err(StoreError::CorruptData(
                "human action message commitment failed integrity verification",
            ));
        }
        let command: PublicationGateCommand =
            serde_json::from_slice(&json).map_err(StoreError::Serialization)?;
        let value = serde_json::to_value(&command).map_err(StoreError::Serialization)?;
        let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
        if canonical != json || Sha256Digest::of_bytes(&canonical) != command_digest {
            return Err(StoreError::CorruptData(
                "human action command failed integrity verification",
            ));
        }
        let status = match status.as_str() {
            "PENDING" => QueuedHumanActionStatus::Pending,
            "COMMITTED" => QueuedHumanActionStatus::Committed,
            _ => return Err(StoreError::CorruptData("invalid human action status")),
        };
        Ok(QueuedHumanAction {
            message_id: message_id.clone(),
            run_id,
            gate_stage_instance_id,
            command_digest,
            message_digest,
            command,
            status,
        })
    })
    .transpose()
}
