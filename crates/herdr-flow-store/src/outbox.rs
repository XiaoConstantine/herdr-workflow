use std::{fmt, str::FromStr};

use herdr_flow_core::{
    canonical_json, OperationId, PublicationAuthorization, PublicationManifest,
    PublicationObservation, PublicationSideEffectKind, RunId, Sha256Digest, StageInstanceId,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

type ClaimedIntentRow = (
    String,
    i64,
    String,
    Vec<u8>,
    String,
    Option<String>,
    Option<i64>,
);
type GenerationRow = (String, String, String, String, String, String, Vec<u8>);

use crate::{
    from_sql_integer,
    lease::{lease_now, LeasedRun, RunLeaseFence, UnixMillisClock},
    pipeline::{verified_pipeline, verified_publication_gate},
    to_sql_integer, verify_run_journal, SqliteStore, StoreError,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicationIntentKind {
    PushRef,
    CreateChangeRequest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationIntent {
    pub operation_id: OperationId,
    pub run_id: RunId,
    pub ordinal: u8,
    pub gate_stage_instance_id: StageInstanceId,
    pub manifest_digest: Sha256Digest,
    pub authorization: PublicationAuthorization,
    pub manifest: PublicationManifest,
    pub kind: PublicationIntentKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PublicationGeneration {
    run_id: RunId,
    manifest_digest: Sha256Digest,
    push_operation_id: OperationId,
    push_request_digest: Sha256Digest,
    change_request_operation_id: OperationId,
    change_request_request_digest: Sha256Digest,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicationOutboxStatus {
    Pending,
    Claimed,
    EffectUnknown,
    Completed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PublicationResult {
    Push(PushResult),
    ChangeRequest(ChangeRequestResult),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationIntentRecord {
    pub intent: PublicationIntent,
    pub status: PublicationOutboxStatus,
    pub claim_owner: Option<OperationId>,
    pub result: Option<PublicationResult>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PushResult {
    pub manifest_digest: Sha256Digest,
    pub project_identity_digest: Sha256Digest,
    pub canonical_remote_url_digest: Sha256Digest,
    pub remote_ref: String,
    pub object: herdr_flow_core::GitObjectId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChangeRequestResult {
    pub manifest_digest: Sha256Digest,
    pub project_identity_digest: Sha256Digest,
    pub run_id: RunId,
    pub head_ref: String,
    pub target_ref: String,
    pub url: String,
    pub provider_id: String,
    pub existing: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileOutcome {
    Idle,
    CompletedPush(PushResult),
    CompletedChangeRequest(ChangeRequestResult),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicationProviderError {
    ProviderMismatch,
    RefLeaseConflict,
    RemoteObjectMismatch,
    Retryable(String),
    EffectUnknown(String),
    InvalidResult,
}

impl fmt::Display for PublicationProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

pub trait PublicationProvider {
    fn provider_id(&self) -> &str;
    fn project_identity_digest(&self) -> Sha256Digest;
    fn canonical_remote_url_digest(&self) -> Sha256Digest;

    fn observe(
        &mut self,
        manifest: &PublicationManifest,
    ) -> Result<PublicationObservation, PublicationProviderError>;

    /// Must implement absent-ref create or exact expected-object lease. Repeating
    /// the same request after an uncertain result must converge on the same ref.
    fn push_ref(
        &mut self,
        intent: &PublicationIntent,
    ) -> Result<PushResult, PublicationProviderError>;

    fn find_change_request(
        &mut self,
        intent: &PublicationIntent,
    ) -> Result<Option<ChangeRequestResult>, PublicationProviderError>;

    /// Must be externally idempotent for `intent.operation_id`: concurrent or
    /// repeated calls for the same immutable intent must return one stable
    /// provider object and URL, never create a second change request.
    fn create_change_request_idempotent(
        &mut self,
        intent: &PublicationIntent,
    ) -> Result<ChangeRequestResult, PublicationProviderError>;
}

impl SqliteStore {
    #[cfg(test)]
    pub fn enqueue_publication(
        &mut self,
        push_operation_id: &OperationId,
        change_request_operation_id: &OperationId,
        authorization: &PublicationAuthorization,
        observation: &PublicationObservation,
    ) -> Result<(), StoreError> {
        self.enqueue_publication_inner(
            push_operation_id,
            change_request_operation_id,
            authorization,
            observation,
            None,
        )
    }

    fn enqueue_publication_inner(
        &mut self,
        push_operation_id: &OperationId,
        change_request_operation_id: &OperationId,
        authorization: &PublicationAuthorization,
        observation: &PublicationObservation,
        lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
    ) -> Result<(), StoreError> {
        if push_operation_id == change_request_operation_id {
            return Err(StoreError::PublicationOutboxConflict);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        if let Some((fence, clock)) = lease {
            lease_now(&transaction, fence, clock)?;
            if fence.run_id() != &authorization.run_id {
                return Err(StoreError::RunLeaseRequired);
            }
        }
        verify_run_journal(&transaction, &authorization.run_id)?;
        let gate = verified_publication_gate(
            &transaction,
            &authorization.run_id,
            &authorization.gate_stage_instance_id,
        )?;
        let pipeline = verified_pipeline(&transaction, &authorization.run_id)?;
        if !pipeline.artifact_is_valid(&gate.expected_authorization_artifact_id)
            || pipeline.stage_is_frozen(&gate.expected_publication_stage_instance_id)
            || pipeline.stage_is_invalidated(&gate.expected_publication_stage_instance_id)
        {
            return Err(StoreError::M1PublicationGateAtomicity);
        }
        gate.validate_pre_side_effect(
            authorization,
            observation,
            PublicationSideEffectKind::PushRef,
        )
        .map_err(StoreError::PublicationGateTransition)?;
        let manifest = gate
            .manifest
            .clone()
            .ok_or(StoreError::M1PublicationGateAtomicity)?;
        let intents = [
            PublicationIntent {
                operation_id: push_operation_id.clone(),
                run_id: authorization.run_id.clone(),
                ordinal: 0,
                gate_stage_instance_id: authorization.gate_stage_instance_id.clone(),
                manifest_digest: authorization.manifest_digest,
                authorization: authorization.clone(),
                manifest: manifest.clone(),
                kind: PublicationIntentKind::PushRef,
            },
            PublicationIntent {
                operation_id: change_request_operation_id.clone(),
                run_id: authorization.run_id.clone(),
                ordinal: 1,
                gate_stage_instance_id: authorization.gate_stage_instance_id.clone(),
                manifest_digest: authorization.manifest_digest,
                authorization: authorization.clone(),
                manifest,
                kind: PublicationIntentKind::CreateChangeRequest,
            },
        ];
        insert_generation(
            &transaction,
            &PublicationGeneration {
                run_id: authorization.run_id.clone(),
                manifest_digest: authorization.manifest_digest,
                push_operation_id: push_operation_id.clone(),
                push_request_digest: intent_digest(&intents[0])?,
                change_request_operation_id: change_request_operation_id.clone(),
                change_request_request_digest: intent_digest(&intents[1])?,
            },
        )?;
        for intent in intents {
            insert_intent(&transaction, &intent)?;
        }
        transaction.commit().map_err(StoreError::Sqlite)
    }

    pub fn load_publication_intents(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<PublicationIntentRecord>, StoreError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, run_id)?;
        verify_outbox_generations(&transaction, run_id)?;
        let mut statement = transaction
            .prepare(
                "SELECT operation_id, ordinal, request_digest, request_json, status,
                        claim_owner, result_digest, result_json,
                        (SELECT lease_epoch FROM publication_outbox_claim_fences
                         WHERE operation_id = publication_outbox.operation_id)
                 FROM publication_outbox WHERE run_id = ?1 ORDER BY rowid, ordinal",
            )
            .map_err(StoreError::Sqlite)?;
        let rows = statement
            .query_map(params![run_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<Vec<u8>>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                ))
            })
            .map_err(StoreError::Sqlite)?;
        let mut intents = Vec::new();
        for row in rows {
            let (
                operation_id,
                ordinal,
                digest,
                json,
                status,
                claim_owner,
                result_digest,
                result_json,
                claim_epoch,
            ) = row.map_err(StoreError::Sqlite)?;
            let has_claim_fence = claim_epoch.map(from_sql_integer).transpose()?.is_some();
            if matches!(status.as_str(), "CLAIMED" | "EFFECT_UNKNOWN") != has_claim_fence {
                return Err(StoreError::CorruptData(
                    "publication outbox status and claim fence disagree",
                ));
            }
            let intent = decode_intent(&operation_id, ordinal, &digest, &json)?;
            let (status, result) = match status.as_str() {
                "PENDING" => (PublicationOutboxStatus::Pending, None),
                "CLAIMED" => (PublicationOutboxStatus::Claimed, None),
                "EFFECT_UNKNOWN" => (PublicationOutboxStatus::EffectUnknown, None),
                "COMPLETED" => (
                    PublicationOutboxStatus::Completed,
                    Some(verify_result(
                        &intent,
                        result_digest.as_deref(),
                        result_json.as_deref(),
                    )?),
                ),
                _ => return Err(StoreError::CorruptData("invalid publication outbox status")),
            };
            let claim_owner = claim_owner
                .map(|owner| OperationId::from_str(&owner).map_err(StoreError::Identifier))
                .transpose()?;
            intents.push(PublicationIntentRecord {
                intent,
                status,
                claim_owner,
                result,
            });
        }
        drop(statement);
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(intents)
    }
}

impl LeasedRun<'_, '_> {
    pub fn enqueue_publication(
        &mut self,
        push_operation_id: &OperationId,
        change_request_operation_id: &OperationId,
        authorization: &PublicationAuthorization,
        observation: &PublicationObservation,
    ) -> Result<(), StoreError> {
        self.store.enqueue_publication_inner(
            push_operation_id,
            change_request_operation_id,
            authorization,
            observation,
            Some((&self.fence, self.clock)),
        )
    }
}

impl LeasedRun<'_, '_> {
    pub fn reconcile_publication(
        &mut self,
        provider: &mut impl PublicationProvider,
        claim_owner: &OperationId,
    ) -> Result<ReconcileOutcome, StoreError> {
        let run_id = self.fence.run_id().clone();
        reconcile_publication_inner(
            self.store,
            provider,
            &run_id,
            claim_owner,
            Some((&self.fence, self.clock)),
        )
    }
}

#[cfg(test)]
pub fn reconcile_publication(
    store: &mut SqliteStore,
    provider: &mut impl PublicationProvider,
    run_id: &RunId,
    claim_owner: &OperationId,
) -> Result<ReconcileOutcome, StoreError> {
    reconcile_publication_inner(store, provider, run_id, claim_owner, None)
}

fn reconcile_publication_inner(
    store: &mut SqliteStore,
    provider: &mut impl PublicationProvider,
    run_id: &RunId,
    claim_owner: &OperationId,
    lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
) -> Result<ReconcileOutcome, StoreError> {
    let claimed = claim_next_intent(store, run_id, claim_owner, lease)?;
    let Some((intent, effect_unknown)) = claimed else {
        return Ok(ReconcileOutcome::Idle);
    };
    if provider.provider_id() != intent.manifest.provider
        || provider.project_identity_digest() != intent.manifest.project_identity_digest
        || provider.canonical_remote_url_digest() != intent.manifest.canonical_remote_url_digest
    {
        release_if_known(store, &intent, claim_owner, effect_unknown, lease)?;
        return Err(StoreError::PublicationProvider(
            PublicationProviderError::ProviderMismatch,
        ));
    }
    if intent.ordinal == 1 && !outbox_ordinal_completed(store, run_id, intent.manifest_digest, 0)? {
        release_if_known(store, &intent, claim_owner, effect_unknown, lease)?;
        return Err(StoreError::PublicationOutboxNotReady);
    }
    let observation = match provider.observe(&intent.manifest) {
        Ok(observation) => observation,
        Err(error) => {
            release_if_known(store, &intent, claim_owner, effect_unknown, lease)?;
            return Err(StoreError::PublicationProvider(error));
        }
    };
    if let Err(error) = store.validate_publication_side_effect(
        run_id,
        &intent.gate_stage_instance_id,
        &intent.authorization,
        &intent.manifest,
        &observation,
        match intent.kind {
            PublicationIntentKind::PushRef => PublicationSideEffectKind::PushRef,
            PublicationIntentKind::CreateChangeRequest => {
                PublicationSideEffectKind::CreateChangeRequest
            }
        },
    ) {
        release_if_known(store, &intent, claim_owner, effect_unknown, lease)?;
        return Err(error);
    }
    match intent.kind {
        PublicationIntentKind::PushRef => {
            mark_effect_unknown(store, &intent, claim_owner, lease)?;
            let result = match provider.push_ref(&intent) {
                Ok(result) => result,
                Err(error) => {
                    if !effect_unknown
                        && !matches!(error, PublicationProviderError::EffectUnknown(_))
                    {
                        release_fresh_uncertain(store, &intent, claim_owner, lease)?;
                    }
                    return Err(StoreError::PublicationProvider(error));
                }
            };
            if result.manifest_digest != intent.manifest_digest
                || result.project_identity_digest != intent.manifest.project_identity_digest
                || result.canonical_remote_url_digest != intent.manifest.canonical_remote_url_digest
                || result.remote_ref != intent.manifest.deterministic_head_ref
                || result.object != intent.authorization.final_object
            {
                mark_effect_unknown(store, &intent, claim_owner, lease)?;
                return Err(StoreError::PublicationProvider(
                    PublicationProviderError::InvalidResult,
                ));
            }
            complete_intent(store, &intent, claim_owner, &result, lease)?;
            Ok(ReconcileOutcome::CompletedPush(result))
        }
        PublicationIntentKind::CreateChangeRequest => {
            let found = match provider.find_change_request(&intent) {
                Ok(found) => found,
                Err(error) => {
                    release_if_known(store, &intent, claim_owner, effect_unknown, lease)?;
                    return Err(StoreError::PublicationProvider(error));
                }
            };
            let result = match found {
                Some(mut existing) => {
                    existing.existing = true;
                    existing
                }
                None => {
                    mark_effect_unknown(store, &intent, claim_owner, lease)?;
                    match provider.create_change_request_idempotent(&intent) {
                        Ok(result) => result,
                        Err(error) => {
                            if !effect_unknown
                                && !matches!(error, PublicationProviderError::EffectUnknown(_))
                            {
                                release_fresh_uncertain(store, &intent, claim_owner, lease)?;
                            }
                            return Err(StoreError::PublicationProvider(error));
                        }
                    }
                }
            };
            if result.manifest_digest != intent.manifest_digest
                || result.project_identity_digest != intent.manifest.project_identity_digest
                || result.run_id != intent.run_id
                || result.head_ref != intent.manifest.deterministic_head_ref
                || result.target_ref != intent.manifest.target_ref
                || result.url.is_empty()
                || result.provider_id.is_empty()
            {
                mark_effect_unknown(store, &intent, claim_owner, lease)?;
                return Err(StoreError::PublicationProvider(
                    PublicationProviderError::InvalidResult,
                ));
            }
            complete_intent(store, &intent, claim_owner, &result, lease)?;
            Ok(ReconcileOutcome::CompletedChangeRequest(result))
        }
    }
}

fn intent_digest(intent: &PublicationIntent) -> Result<Sha256Digest, StoreError> {
    let value = serde_json::to_value(intent).map_err(StoreError::Serialization)?;
    let json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    Ok(Sha256Digest::of_bytes(&json))
}

fn insert_generation(
    transaction: &rusqlite::Transaction<'_>,
    generation: &PublicationGeneration,
) -> Result<(), StoreError> {
    let value = serde_json::to_value(generation).map_err(StoreError::Serialization)?;
    let json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    let digest = Sha256Digest::of_bytes(&json);
    let inserted = transaction
        .execute(
            "INSERT OR IGNORE INTO publication_outbox_generations(
                run_id, manifest_digest, push_operation_id, push_request_digest,
                change_request_operation_id, change_request_request_digest,
                generation_digest, generation_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                generation.run_id.as_str(),
                generation.manifest_digest.to_prefixed_string(),
                generation.push_operation_id.as_str(),
                generation.push_request_digest.to_prefixed_string(),
                generation.change_request_operation_id.as_str(),
                generation
                    .change_request_request_digest
                    .to_prefixed_string(),
                digest.to_prefixed_string(),
                json,
            ],
        )
        .map_err(StoreError::Sqlite)?;
    if inserted == 0 {
        let existing: (String, Vec<u8>) = transaction
            .query_row(
                "SELECT generation_digest, generation_json
                 FROM publication_outbox_generations
                 WHERE run_id = ?1 AND manifest_digest = ?2",
                params![
                    generation.run_id.as_str(),
                    generation.manifest_digest.to_prefixed_string(),
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(StoreError::Sqlite)?;
        if existing != (digest.to_prefixed_string(), json) {
            return Err(StoreError::PublicationOutboxConflict);
        }
    }
    Ok(())
}

fn insert_intent(
    transaction: &rusqlite::Transaction<'_>,
    intent: &PublicationIntent,
) -> Result<(), StoreError> {
    let value = serde_json::to_value(intent).map_err(StoreError::Serialization)?;
    let json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    let digest = Sha256Digest::of_bytes(&json);
    let existing: Option<(String, Vec<u8>)> = transaction
        .query_row(
            "SELECT request_digest, request_json FROM publication_outbox
             WHERE operation_id = ?1
                OR (run_id = ?2 AND manifest_digest = ?3 AND ordinal = ?4)",
            params![
                intent.operation_id.as_str(),
                intent.run_id.as_str(),
                intent.manifest_digest.to_prefixed_string(),
                i64::from(intent.ordinal),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    if let Some((existing_digest, existing_json)) = existing {
        if existing_digest == digest.to_prefixed_string() && existing_json == json {
            return Ok(());
        }
        return Err(StoreError::PublicationOutboxConflict);
    }
    transaction
        .execute(
            "INSERT INTO publication_outbox(
                operation_id, run_id, ordinal, gate_stage_instance_id,
                manifest_digest, request_digest, request_json, status
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'PENDING')",
            params![
                intent.operation_id.as_str(),
                intent.run_id.as_str(),
                i64::from(intent.ordinal),
                intent.gate_stage_instance_id.as_str(),
                intent.manifest_digest.to_prefixed_string(),
                digest.to_prefixed_string(),
                json,
            ],
        )
        .map_err(StoreError::Sqlite)?;
    Ok(())
}

fn decode_intent(
    operation_id: &str,
    ordinal: i64,
    digest: &str,
    json: &[u8],
) -> Result<PublicationIntent, StoreError> {
    let intent: PublicationIntent =
        serde_json::from_slice(json).map_err(StoreError::Serialization)?;
    let value = serde_json::to_value(&intent).map_err(StoreError::Serialization)?;
    let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    if canonical != json
        || Sha256Digest::of_bytes(&canonical).to_prefixed_string() != digest
        || intent
            .manifest
            .digest()
            .map_err(StoreError::PublicationGateTransition)?
            != intent.manifest_digest
        || intent.authorization.manifest_digest != intent.manifest_digest
        || intent.authorization.run_id != intent.run_id
        || intent.authorization.gate_stage_instance_id != intent.gate_stage_instance_id
        || intent.authorization.publication_stage_instance_id
            != intent.manifest.publication_stage_instance_id
        || intent.authorization.publisher_component_digest
            != intent.manifest.publisher_component_digest
        || intent.authorization.final_object != intent.manifest.final_object
        || intent.authorization.observed_target_object != intent.manifest.observed_target_object
        || intent.authorization.reviewed_merge_base != intent.manifest.reviewed_merge_base
        || intent.operation_id
            != OperationId::from_str(operation_id).map_err(StoreError::Identifier)?
        || i64::from(intent.ordinal) != ordinal
    {
        return Err(StoreError::CorruptData(
            "publication outbox intent failed integrity verification",
        ));
    }
    Ok(intent)
}

fn verify_result(
    intent: &PublicationIntent,
    digest: Option<&str>,
    json: Option<&[u8]>,
) -> Result<PublicationResult, StoreError> {
    let json = json.ok_or(StoreError::CorruptData(
        "completed publication intent has no result",
    ))?;
    let (result, value) = match intent.kind {
        PublicationIntentKind::PushRef => {
            let result: PushResult =
                serde_json::from_slice(json).map_err(StoreError::Serialization)?;
            if result.manifest_digest != intent.manifest_digest
                || result.project_identity_digest != intent.manifest.project_identity_digest
                || result.canonical_remote_url_digest != intent.manifest.canonical_remote_url_digest
                || result.remote_ref != intent.manifest.deterministic_head_ref
                || result.object != intent.manifest.final_object
            {
                return Err(StoreError::CorruptData(
                    "durable push result does not match its intent",
                ));
            }
            let value = serde_json::to_value(&result).map_err(StoreError::Serialization)?;
            (PublicationResult::Push(result), value)
        }
        PublicationIntentKind::CreateChangeRequest => {
            let result: ChangeRequestResult =
                serde_json::from_slice(json).map_err(StoreError::Serialization)?;
            if result.manifest_digest != intent.manifest_digest
                || result.project_identity_digest != intent.manifest.project_identity_digest
                || result.run_id != intent.run_id
                || result.head_ref != intent.manifest.deterministic_head_ref
                || result.target_ref != intent.manifest.target_ref
                || result.url.is_empty()
                || result.provider_id.is_empty()
            {
                return Err(StoreError::CorruptData(
                    "durable change request result does not match its intent",
                ));
            }
            let value = serde_json::to_value(&result).map_err(StoreError::Serialization)?;
            (PublicationResult::ChangeRequest(result), value)
        }
    };
    let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    let expected_digest = Sha256Digest::of_bytes(&canonical).to_prefixed_string();
    if canonical != json || digest != Some(expected_digest.as_str()) {
        return Err(StoreError::CorruptData(
            "publication outbox result failed integrity verification",
        ));
    }
    Ok(result)
}

fn verify_outbox_generations(
    connection: &rusqlite::Connection,
    run_id: &RunId,
) -> Result<(), StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT manifest_digest, push_operation_id, push_request_digest,
                    change_request_operation_id, change_request_request_digest,
                    generation_digest, generation_json
             FROM publication_outbox_generations WHERE run_id = ?1 ORDER BY rowid",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map(
            params![run_id.as_str()],
            |row| -> rusqlite::Result<GenerationRow> {
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
        .map_err(StoreError::Sqlite)?;
    let mut generation_count = 0_u64;
    for row in rows {
        generation_count += 1;
        let (
            manifest_digest,
            push_operation_id,
            push_request_digest,
            change_request_operation_id,
            change_request_request_digest,
            digest,
            json,
        ) = row.map_err(StoreError::Sqlite)?;
        let generation: PublicationGeneration =
            serde_json::from_slice(&json).map_err(StoreError::Serialization)?;
        let value = serde_json::to_value(&generation).map_err(StoreError::Serialization)?;
        let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
        if generation.run_id != *run_id
            || generation.manifest_digest.to_prefixed_string() != manifest_digest
            || generation.push_operation_id.as_str() != push_operation_id
            || generation.push_request_digest.to_prefixed_string() != push_request_digest
            || generation.change_request_operation_id.as_str() != change_request_operation_id
            || generation
                .change_request_request_digest
                .to_prefixed_string()
                != change_request_request_digest
            || canonical != json
            || Sha256Digest::of_bytes(&canonical).to_prefixed_string() != digest
        {
            return Err(StoreError::CorruptData(
                "publication outbox generation failed integrity verification",
            ));
        }
        let mut intent_statement = connection
            .prepare(
                "SELECT operation_id, ordinal, request_digest, request_json, status,
                        result_digest, result_json
                 FROM publication_outbox
                 WHERE run_id = ?1 AND manifest_digest = ?2 ORDER BY ordinal",
            )
            .map_err(StoreError::Sqlite)?;
        let intent_rows = intent_statement
            .query_map(params![run_id.as_str(), manifest_digest], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<Vec<u8>>>(6)?,
                ))
            })
            .map_err(StoreError::Sqlite)?;
        let mut intents = Vec::new();
        for row in intent_rows {
            let (
                operation_id,
                ordinal,
                request_digest,
                request_json,
                status,
                result_digest,
                result_json,
            ) = row.map_err(StoreError::Sqlite)?;
            let intent = decode_intent(&operation_id, ordinal, &request_digest, &request_json)?;
            if intent.run_id != *run_id || intent.manifest_digest != generation.manifest_digest {
                return Err(StoreError::CorruptData(
                    "publication outbox indexed identity mismatch",
                ));
            }
            if status == "COMPLETED" {
                verify_result(&intent, result_digest.as_deref(), result_json.as_deref())?;
            } else if !matches!(status.as_str(), "PENDING" | "CLAIMED" | "EFFECT_UNKNOWN") {
                return Err(StoreError::CorruptData("invalid publication outbox status"));
            }
            intents.push(intent);
        }
        if intents.len() != 2
            || intents[0].operation_id != generation.push_operation_id
            || intents[0].ordinal != 0
            || intent_digest(&intents[0])? != generation.push_request_digest
            || intents[1].operation_id != generation.change_request_operation_id
            || intents[1].ordinal != 1
            || intent_digest(&intents[1])? != generation.change_request_request_digest
        {
            return Err(StoreError::CorruptData(
                "publication outbox generation is incomplete",
            ));
        }
    }
    drop(statement);
    let outbox_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM publication_outbox WHERE run_id = ?1",
            params![run_id.as_str()],
            |row| row.get(0),
        )
        .map_err(StoreError::Sqlite)?;
    if u64::try_from(outbox_count).map_err(|_| StoreError::PublicationOutboxConflict)?
        != generation_count * 2
    {
        return Err(StoreError::CorruptData(
            "publication outbox has an uncommitted generation",
        ));
    }
    Ok(())
}

fn require_reconcile_fence(
    transaction: &rusqlite::Transaction<'_>,
    run_id: &RunId,
    lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
) -> Result<(), StoreError> {
    let Some((fence, clock)) = lease else {
        #[cfg(test)]
        return Ok(());
        #[cfg(not(test))]
        return Err(StoreError::RunLeaseRequired);
    };
    lease_now(transaction, fence, clock)?;
    if fence.run_id() != run_id {
        return Err(StoreError::RunLeaseRequired);
    }
    Ok(())
}

fn require_claim_fence(
    transaction: &rusqlite::Transaction<'_>,
    intent: &PublicationIntent,
    lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
) -> Result<(), StoreError> {
    require_reconcile_fence(transaction, &intent.run_id, lease)?;
    let Some((fence, _)) = lease else {
        return Ok(());
    };
    let epoch: Option<i64> = transaction
        .query_row(
            "SELECT lease_epoch FROM publication_outbox_claim_fences
             WHERE operation_id = ?1",
            params![intent.operation_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    if epoch.map(from_sql_integer).transpose()? != Some(fence.lease_epoch()) {
        return Err(StoreError::PublicationOutboxNotReady);
    }
    Ok(())
}

fn clear_claim_fence(
    transaction: &rusqlite::Transaction<'_>,
    intent: &PublicationIntent,
) -> Result<(), StoreError> {
    transaction
        .execute(
            "DELETE FROM publication_outbox_claim_fences WHERE operation_id = ?1",
            params![intent.operation_id.as_str()],
        )
        .map_err(StoreError::Sqlite)?;
    Ok(())
}

fn claim_next_intent(
    store: &mut SqliteStore,
    run_id: &RunId,
    claim_owner: &OperationId,
    lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
) -> Result<Option<(PublicationIntent, bool)>, StoreError> {
    let transaction = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(StoreError::Sqlite)?;
    require_reconcile_fence(&transaction, run_id, lease)?;
    verify_outbox_generations(&transaction, run_id)?;
    let row: Option<ClaimedIntentRow> = transaction
        .query_row(
            "SELECT operation_id, ordinal, request_digest, request_json, status, claim_owner,
                    (SELECT lease_epoch FROM publication_outbox_claim_fences
                     WHERE operation_id = publication_outbox.operation_id)
             FROM publication_outbox
             WHERE run_id = ?1 AND status IN ('PENDING', 'CLAIMED', 'EFFECT_UNKNOWN')
             ORDER BY rowid, ordinal LIMIT 1",
            params![run_id.as_str()],
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
    let Some((operation_id, ordinal, digest, json, status, existing_owner, claim_epoch)) = row
    else {
        transaction.commit().map_err(StoreError::Sqlite)?;
        return Ok(None);
    };
    let owned_by_another = matches!(status.as_str(), "CLAIMED" | "EFFECT_UNKNOWN")
        && existing_owner.as_deref() != Some(claim_owner.as_str());
    let current_epoch = lease.map(|(fence, _)| fence.lease_epoch());
    if status == "PENDING" {
        if claim_epoch.is_some() {
            return Err(StoreError::CorruptData(
                "pending publication intent has a claim fence",
            ));
        }
    } else {
        match (
            claim_epoch.map(from_sql_integer).transpose()?,
            current_epoch,
        ) {
            (Some(claim_epoch), Some(current_epoch)) if claim_epoch == current_epoch => {
                if owned_by_another {
                    return Err(StoreError::PublicationOutboxNotReady);
                }
            }
            (Some(claim_epoch), Some(current_epoch)) if claim_epoch < current_epoch => {}
            (None, None) if !owned_by_another => {}
            _ => return Err(StoreError::PublicationOutboxNotReady),
        }
    }
    let taking_over = matches!(
        (claim_epoch.map(from_sql_integer).transpose()?, current_epoch),
        (Some(claim_epoch), Some(current_epoch)) if claim_epoch < current_epoch
    );
    if status == "PENDING" || taking_over {
        let updated = transaction
            .execute(
                "UPDATE publication_outbox SET
                    status = CASE WHEN status = 'PENDING' THEN 'CLAIMED' ELSE status END,
                    claim_owner = ?1
                 WHERE operation_id = ?2
                   AND status IN ('PENDING', 'CLAIMED', 'EFFECT_UNKNOWN')",
                params![claim_owner.as_str(), operation_id],
            )
            .map_err(StoreError::Sqlite)?;
        if updated != 1 {
            return Err(StoreError::PublicationOutboxNotReady);
        }
        if let Some(epoch) = current_epoch {
            transaction
                .execute(
                    "INSERT INTO publication_outbox_claim_fences(operation_id, lease_epoch)
                     VALUES (?1, ?2)
                     ON CONFLICT(operation_id) DO UPDATE SET lease_epoch = excluded.lease_epoch",
                    params![operation_id, to_sql_integer(epoch)?],
                )
                .map_err(StoreError::Sqlite)?;
        }
    }
    let intent = decode_intent(&operation_id, ordinal, &digest, &json)?;
    transaction.commit().map_err(StoreError::Sqlite)?;
    Ok(Some((intent, status == "EFFECT_UNKNOWN")))
}

fn outbox_ordinal_completed(
    store: &SqliteStore,
    run_id: &RunId,
    manifest_digest: Sha256Digest,
    ordinal: u8,
) -> Result<bool, StoreError> {
    store
        .connection
        .query_row(
            "SELECT status = 'COMPLETED' FROM publication_outbox
             WHERE run_id = ?1 AND manifest_digest = ?2 AND ordinal = ?3",
            params![
                run_id.as_str(),
                manifest_digest.to_prefixed_string(),
                i64::from(ordinal),
            ],
            |row| row.get(0),
        )
        .optional()
        .map(Option::unwrap_or_default)
        .map_err(StoreError::Sqlite)
}

fn mark_effect_unknown(
    store: &mut SqliteStore,
    intent: &PublicationIntent,
    claim_owner: &OperationId,
    lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
) -> Result<(), StoreError> {
    let transaction = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(StoreError::Sqlite)?;
    require_claim_fence(&transaction, intent, lease)?;
    let updated = transaction
        .execute(
            "UPDATE publication_outbox SET status = 'EFFECT_UNKNOWN'
             WHERE operation_id = ?1 AND status IN ('CLAIMED', 'EFFECT_UNKNOWN')
               AND claim_owner = ?2",
            params![intent.operation_id.as_str(), claim_owner.as_str()],
        )
        .map_err(StoreError::Sqlite)?;
    if updated != 1 {
        return Err(StoreError::PublicationOutboxConflict);
    }
    transaction.commit().map_err(StoreError::Sqlite)
}

fn release_fresh_uncertain(
    store: &mut SqliteStore,
    intent: &PublicationIntent,
    claim_owner: &OperationId,
    lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
) -> Result<(), StoreError> {
    let transaction = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(StoreError::Sqlite)?;
    require_claim_fence(&transaction, intent, lease)?;
    let updated = transaction
        .execute(
            "UPDATE publication_outbox SET status = 'PENDING', claim_owner = NULL
             WHERE operation_id = ?1 AND status = 'EFFECT_UNKNOWN' AND claim_owner = ?2",
            params![intent.operation_id.as_str(), claim_owner.as_str()],
        )
        .map_err(StoreError::Sqlite)?;
    if updated != 1 {
        return Err(StoreError::PublicationOutboxConflict);
    }
    clear_claim_fence(&transaction, intent)?;
    transaction.commit().map_err(StoreError::Sqlite)
}

fn release_if_known(
    store: &mut SqliteStore,
    intent: &PublicationIntent,
    claim_owner: &OperationId,
    effect_unknown: bool,
    lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
) -> Result<(), StoreError> {
    if effect_unknown {
        Ok(())
    } else {
        release_claim(store, intent, claim_owner, lease)
    }
}

fn release_claim(
    store: &mut SqliteStore,
    intent: &PublicationIntent,
    claim_owner: &OperationId,
    lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
) -> Result<(), StoreError> {
    let transaction = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(StoreError::Sqlite)?;
    require_claim_fence(&transaction, intent, lease)?;
    let updated = transaction
        .execute(
            "UPDATE publication_outbox
             SET status = 'PENDING', claim_owner = NULL
             WHERE operation_id = ?1 AND status = 'CLAIMED' AND claim_owner = ?2",
            params![intent.operation_id.as_str(), claim_owner.as_str()],
        )
        .map_err(StoreError::Sqlite)?;
    if updated != 1 {
        return Err(StoreError::PublicationOutboxConflict);
    }
    clear_claim_fence(&transaction, intent)?;
    transaction.commit().map_err(StoreError::Sqlite)
}

fn complete_intent<T: Serialize>(
    store: &mut SqliteStore,
    intent: &PublicationIntent,
    claim_owner: &OperationId,
    result: &T,
    lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
) -> Result<(), StoreError> {
    let value = serde_json::to_value(result).map_err(StoreError::Serialization)?;
    let json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    let digest = Sha256Digest::of_bytes(&json);
    let transaction = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(StoreError::Sqlite)?;
    require_claim_fence(&transaction, intent, lease)?;
    let updated = transaction
        .execute(
            "UPDATE publication_outbox
             SET status = 'COMPLETED', claim_owner = NULL,
                 result_digest = ?1, result_json = ?2
             WHERE operation_id = ?3
               AND status IN ('CLAIMED', 'EFFECT_UNKNOWN') AND claim_owner = ?4",
            params![
                digest.to_prefixed_string(),
                json,
                intent.operation_id.as_str(),
                claim_owner.as_str(),
            ],
        )
        .map_err(StoreError::Sqlite)?;
    if updated != 1 {
        return Err(StoreError::PublicationOutboxConflict);
    }
    clear_claim_fence(&transaction, intent)?;
    transaction.commit().map_err(StoreError::Sqlite)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    };

    use herdr_flow_core::{
        ArtifactId, GitObjectFormat, GitObjectId, PublicationGateRegistration,
        PublicationGateState, TargetDriftPolicy,
    };

    use super::*;

    const U0: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const U1: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
    const U2: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
    const U3: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAY";
    const U4: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";

    #[derive(Clone)]
    struct ManualClock(Arc<AtomicU64>);

    impl ManualClock {
        fn new(now: u64) -> Self {
            Self(Arc::new(AtomicU64::new(now)))
        }

        fn set(&self, now: u64) {
            self.0.store(now, Ordering::SeqCst);
        }
    }

    impl UnixMillisClock for ManualClock {
        fn now_unix_ms(&self) -> Result<u64, crate::ClockError> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    fn digest(value: &[u8]) -> Sha256Digest {
        Sha256Digest::of_bytes(value)
    }

    fn oid(value: char) -> GitObjectId {
        GitObjectId::from_hex(GitObjectFormat::Sha1, &value.to_string().repeat(40)).unwrap()
    }

    fn stage(value: &str) -> StageInstanceId {
        StageInstanceId::parse(format!("stage_{value}")).unwrap()
    }

    fn artifact(value: &str) -> ArtifactId {
        ArtifactId::parse(format!("art_{value}")).unwrap()
    }

    fn operation(value: &str) -> OperationId {
        OperationId::parse(format!("op_{value}")).unwrap()
    }

    fn intent(kind: PublicationIntentKind) -> PublicationIntent {
        let run_id = RunId::parse(format!("flow_{U0}")).unwrap();
        let manifest = PublicationManifest {
            run_id: run_id.clone(),
            publication_stage_instance_id: stage(U1),
            publisher_component_digest: digest(b"publisher"),
            provider: "fake".into(),
            project_identity_digest: digest(b"project"),
            canonical_remote_url_digest: digest(b"remote"),
            final_object: oid('2'),
            deterministic_head_ref: "refs/heads/herdr/test".into(),
            target_ref: "refs/heads/main".into(),
            observed_target_object: oid('1'),
            reviewed_merge_base: oid('1'),
            target_drift_policy: TargetDriftPolicy::FailClosed,
            expected_head_object: None,
            title_digest: digest(b"title"),
            body_digest: digest(b"body"),
            metadata_digest: digest(b"metadata"),
            pipeline_definition_digest: digest(b"pipeline"),
            requirements_digest: digest(b"requirements"),
            review_package_digest: digest(b"review"),
            reviewed_object: oid('2'),
            review_outcome: herdr_flow_core::PublicationReviewOutcome::AgentAligned,
            review_override_authorization_digest: None,
            gate_input_manifest_digest: digest(b"input"),
            check_policy_digest: digest(b"policy"),
            check_result_digest: digest(b"checks"),
            artifact_lineage_digest: digest(b"lineage"),
            frozen_review_state_revision: 1,
        };
        let authorization = PublicationAuthorization {
            run_id: run_id.clone(),
            gate_stage_instance_id: stage(U2),
            publication_stage_instance_id: stage(U1),
            publisher_component_digest: digest(b"publisher"),
            manifest_digest: manifest.digest().unwrap(),
            final_object: oid('2'),
            observed_target_object: oid('1'),
            reviewed_merge_base: oid('1'),
            authorization_digest: digest(b"human"),
            gate_control_revision: 3,
        };
        let ordinal = match kind {
            PublicationIntentKind::PushRef => 0,
            PublicationIntentKind::CreateChangeRequest => 1,
        };
        PublicationIntent {
            operation_id: operation(if ordinal == 0 { U3 } else { U4 }),
            run_id,
            ordinal,
            gate_stage_instance_id: stage(U2),
            manifest_digest: authorization.manifest_digest,
            authorization,
            manifest,
            kind,
        }
    }

    struct FakeProvider {
        head: Option<GitObjectId>,
        target: GitObjectId,
        merge_base: GitObjectId,
        changes: BTreeMap<String, ChangeRequestResult>,
        fail_after_push_once: bool,
        pushes: usize,
        creates: usize,
    }

    impl PublicationProvider for FakeProvider {
        fn provider_id(&self) -> &str {
            "fake"
        }

        fn project_identity_digest(&self) -> Sha256Digest {
            digest(b"project")
        }

        fn canonical_remote_url_digest(&self) -> Sha256Digest {
            digest(b"remote")
        }

        fn observe(
            &mut self,
            _manifest: &PublicationManifest,
        ) -> Result<PublicationObservation, PublicationProviderError> {
            Ok(PublicationObservation {
                target_object: self.target.clone(),
                merge_base: self.merge_base.clone(),
                head_object: self.head.clone(),
            })
        }

        fn push_ref(
            &mut self,
            intent: &PublicationIntent,
        ) -> Result<PushResult, PublicationProviderError> {
            if self.head.as_ref() != intent.manifest.expected_head_object.as_ref()
                && self.head.as_ref() != Some(&intent.manifest.final_object)
            {
                return Err(PublicationProviderError::RefLeaseConflict);
            }
            if self.head.as_ref() != Some(&intent.manifest.final_object) {
                self.head = Some(intent.manifest.final_object.clone());
                self.pushes += 1;
            }
            if self.fail_after_push_once {
                self.fail_after_push_once = false;
                return Err(PublicationProviderError::EffectUnknown(
                    "uncertain push".into(),
                ));
            }
            Ok(PushResult {
                manifest_digest: intent.manifest_digest,
                project_identity_digest: intent.manifest.project_identity_digest,
                canonical_remote_url_digest: intent.manifest.canonical_remote_url_digest,
                remote_ref: intent.manifest.deterministic_head_ref.clone(),
                object: intent.manifest.final_object.clone(),
            })
        }

        fn find_change_request(
            &mut self,
            intent: &PublicationIntent,
        ) -> Result<Option<ChangeRequestResult>, PublicationProviderError> {
            Ok(self.changes.get(intent.run_id.as_str()).cloned())
        }

        fn create_change_request_idempotent(
            &mut self,
            intent: &PublicationIntent,
        ) -> Result<ChangeRequestResult, PublicationProviderError> {
            if let Some(existing) = self.changes.get(intent.run_id.as_str()) {
                let mut existing = existing.clone();
                existing.existing = true;
                return Ok(existing);
            }
            self.creates += 1;
            let result = ChangeRequestResult {
                manifest_digest: intent.manifest_digest,
                project_identity_digest: intent.manifest.project_identity_digest,
                run_id: intent.run_id.clone(),
                head_ref: intent.manifest.deterministic_head_ref.clone(),
                target_ref: intent.manifest.target_ref.clone(),
                url: "https://fake.invalid/change/1".into(),
                provider_id: "1".into(),
                existing: false,
            };
            self.changes
                .insert(intent.run_id.to_string(), result.clone());
            Ok(result)
        }
    }

    #[test]
    fn fake_provider_reconciles_uncertain_push_and_change_request_without_duplication() {
        let push = intent(PublicationIntentKind::PushRef);
        let change = intent(PublicationIntentKind::CreateChangeRequest);
        let mut provider = FakeProvider {
            head: None,
            target: oid('1'),
            merge_base: oid('1'),
            changes: BTreeMap::new(),
            fail_after_push_once: true,
            pushes: 0,
            creates: 0,
        };
        assert!(matches!(
            provider.push_ref(&push),
            Err(PublicationProviderError::EffectUnknown(_))
        ));
        assert_eq!(provider.push_ref(&push).unwrap().object, oid('2'));
        assert_eq!(provider.pushes, 1);
        assert!(provider.find_change_request(&change).unwrap().is_none());
        let created = provider.create_change_request_idempotent(&change).unwrap();
        let repeated = provider.create_change_request_idempotent(&change).unwrap();
        assert_eq!(created.provider_id, repeated.provider_id);
        assert_eq!(created.url, repeated.url);
        assert!(repeated.existing);
        assert_eq!(provider.creates, 1);
    }

    #[test]
    fn intent_commitment_is_durable_idempotent_and_conflict_detecting() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("outbox.sqlite3");
        let mut store = SqliteStore::open(&database).unwrap();
        let push = intent(PublicationIntentKind::PushRef);
        store
            .create_run(&push.run_id, &push.manifest.pipeline_definition_digest)
            .unwrap();
        let gate = PublicationGateState::new(PublicationGateRegistration {
            run_id: push.run_id.clone(),
            stage_instance_id: push.gate_stage_instance_id.clone(),
            expected_publication_stage_instance_id: push
                .manifest
                .publication_stage_instance_id
                .clone(),
            expected_publication_component_digest: push.manifest.publisher_component_digest,
            expected_review_stage_instance_id: stage(U3),
            expected_review_component_digest: digest(b"review-component"),
            expected_implementation_stage_instance_id: stage(U4),
            expected_implementation_component_digest: digest(b"implementation-component"),
            expected_review_package_artifact_id: artifact(U3),
            expected_authorization_artifact_id: artifact(U4),
            pipeline_definition_digest: push.manifest.pipeline_definition_digest,
            gate_component_digest: digest(b"gate-component"),
        });
        let gate_json = serde_json::to_vec(&gate).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO publication_gate_snapshots(
                    stage_instance_id, run_id, control_revision, initial_state_json, state_json
                 ) VALUES (?1, ?2, 0, ?3, ?3)",
                params![
                    gate.stage_instance_id.as_str(),
                    gate.run_id.as_str(),
                    gate_json
                ],
            )
            .unwrap();
        let transaction = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_generation(
            &transaction,
            &PublicationGeneration {
                run_id: push.run_id.clone(),
                manifest_digest: push.manifest_digest,
                push_operation_id: push.operation_id.clone(),
                push_request_digest: intent_digest(&push).unwrap(),
                change_request_operation_id: operation(U4),
                change_request_request_digest: intent_digest(&intent(
                    PublicationIntentKind::CreateChangeRequest,
                ))
                .unwrap(),
            },
        )
        .unwrap();
        insert_intent(&transaction, &push).unwrap();
        insert_intent(
            &transaction,
            &intent(PublicationIntentKind::CreateChangeRequest),
        )
        .unwrap();
        insert_intent(&transaction, &push).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            store.load_publication_intents(&push.run_id).unwrap(),
            vec![
                PublicationIntentRecord {
                    intent: push.clone(),
                    status: PublicationOutboxStatus::Pending,
                    claim_owner: None,
                    result: None,
                },
                PublicationIntentRecord {
                    intent: intent(PublicationIntentKind::CreateChangeRequest),
                    status: PublicationOutboxStatus::Pending,
                    claim_owner: None,
                    result: None,
                },
            ]
        );
        let mut conflicting = push.clone();
        conflicting.manifest.title_digest = digest(b"different");
        let transaction = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(matches!(
            insert_intent(&transaction, &conflicting),
            Err(StoreError::PublicationOutboxConflict)
        ));
        drop(transaction);

        let clock = ManualClock::new(1_000);
        let first_lease = store
            .acquire_run(
                &push.run_id,
                &operation("01ARZ3NDEKTSV4RRFFQ69G5FB2"),
                100,
                &clock,
            )
            .unwrap();
        let first_fence = first_lease.fence().clone();
        drop(first_lease);
        let first_lease = Some((&first_fence, &clock as &dyn UnixMillisClock));
        let claim = operation("01ARZ3NDEKTSV4RRFFQ69G5FB0");
        assert_eq!(
            claim_next_intent(&mut store, &push.run_id, &claim, first_lease).unwrap(),
            Some((push.clone(), false))
        );
        let other_claim = operation("01ARZ3NDEKTSV4RRFFQ69G5FB1");
        assert!(matches!(
            claim_next_intent(&mut store, &push.run_id, &other_claim, first_lease),
            Err(StoreError::PublicationOutboxNotReady)
        ));
        mark_effect_unknown(&mut store, &push, &claim, first_lease).unwrap();
        store
            .connection
            .execute("DROP TABLE publication_outbox_claim_fences", [])
            .unwrap();
        drop(store);
        let mut store = SqliteStore::open(&database).unwrap();
        let migrated_epoch: i64 = store
            .connection
            .query_row(
                "SELECT lease_epoch FROM publication_outbox_claim_fences WHERE operation_id = ?1",
                params![push.operation_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(migrated_epoch, 0);
        clock.set(1_100);
        let takeover = store
            .acquire_run(
                &push.run_id,
                &operation("01ARZ3NDEKTSV4RRFFQ69G5FB3"),
                100,
                &clock,
            )
            .unwrap();
        let takeover_fence = takeover.fence().clone();
        drop(takeover);
        let takeover_lease = Some((&takeover_fence, &clock as &dyn UnixMillisClock));
        assert_eq!(
            claim_next_intent(&mut store, &push.run_id, &other_claim, takeover_lease).unwrap(),
            Some((push.clone(), true))
        );
        store
            .connection
            .execute(
                "DELETE FROM publication_outbox_claim_fences WHERE operation_id = ?1",
                params![push.operation_id.as_str()],
            )
            .unwrap();
        drop(store);
        let mut store = SqliteStore::open(&database).unwrap();
        assert!(matches!(
            store.load_publication_intents(&push.run_id),
            Err(StoreError::CorruptData(_))
        ));
        store
            .connection
            .execute(
                "INSERT INTO publication_outbox_claim_fences(operation_id, lease_epoch)
                 VALUES (?1, ?2)",
                params![
                    push.operation_id.as_str(),
                    to_sql_integer(takeover_fence.lease_epoch()).unwrap()
                ],
            )
            .unwrap();
        assert!(matches!(
            complete_intent(
                &mut store,
                &push,
                &claim,
                &PushResult {
                    manifest_digest: push.manifest_digest,
                    project_identity_digest: push.manifest.project_identity_digest,
                    canonical_remote_url_digest: push.manifest.canonical_remote_url_digest,
                    remote_ref: push.manifest.deterministic_head_ref.clone(),
                    object: push.manifest.final_object.clone(),
                },
                first_lease,
            ),
            Err(StoreError::RunLeaseExpired)
        ));
        let uncertain = store.load_publication_intents(&push.run_id).unwrap();
        assert_eq!(uncertain[0].status, PublicationOutboxStatus::EffectUnknown);
        assert_eq!(uncertain[0].claim_owner, Some(other_claim.clone()));
        complete_intent(
            &mut store,
            &push,
            &other_claim,
            &PushResult {
                manifest_digest: push.manifest_digest,
                project_identity_digest: push.manifest.project_identity_digest,
                canonical_remote_url_digest: push.manifest.canonical_remote_url_digest,
                remote_ref: push.manifest.deterministic_head_ref.clone(),
                object: push.manifest.final_object.clone(),
            },
            takeover_lease,
        )
        .unwrap();
        let claim_fence_count: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM publication_outbox_claim_fences WHERE operation_id = ?1",
                params![push.operation_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claim_fence_count, 0);
        assert_eq!(
            store.load_publication_intents(&push.run_id).unwrap()[0].status,
            PublicationOutboxStatus::Completed
        );
        store
            .connection
            .execute(
                "UPDATE publication_outbox SET result_digest = ?1 WHERE operation_id = ?2",
                params![
                    digest(b"tampered").to_prefixed_string(),
                    push.operation_id.as_str()
                ],
            )
            .unwrap();
        assert!(matches!(
            store.load_publication_intents(&push.run_id),
            Err(StoreError::CorruptData(_))
        ));
    }
}
