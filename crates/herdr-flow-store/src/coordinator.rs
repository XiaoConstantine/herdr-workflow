use std::str::FromStr;

use herdr_flow_core::{
    canonical_json, m1_adversarial_pipeline, ArtifactId, ArtifactRecord, BatchId, EventId,
    M1PipelineArtifacts, M1PipelineStages, MessageId, OperationId, PipelineCommand, PipelineEvent,
    PipelineEventKind, PipelineState, PublicationGateRegistration, PublicationGateState, RunId,
    RunIngressArtifactRecord, Sha256Digest, StageInstanceId, StagePhase, BASE_PROTOCOL,
    MAX_CONTROL_REVISION,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::{
    lease::{LeasedRun, RunLeaseFence, UnixMillisClock},
    pipeline::{insert_pipeline_event, validate_gate_registration, SemanticPipelineEntry},
    to_sql_integer, AdversarialReviewRegistration, ArtifactStore, SqliteStore, StoreError,
    StoredArtifactRecord,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct M1StartDescriptor {
    pub protocol: String,
    pub run_id: RunId,
    pub pipeline_definition_digest: Sha256Digest,
    pub stages: M1PipelineStages,
    pub artifacts: M1PipelineArtifacts,
    pub review: AdversarialReviewRegistration,
}

impl M1StartDescriptor {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, StoreError> {
        canonical_bytes(self)
    }

    fn pipeline(&self) -> Result<PipelineState, StoreError> {
        if self.protocol != BASE_PROTOCOL {
            return Err(StoreError::InvalidM1Start);
        }
        m1_adversarial_pipeline(
            self.pipeline_definition_digest,
            self.stages.clone(),
            self.artifacts.clone(),
        )
        .map_err(|_| StoreError::InvalidM1Start)
    }

    fn gate(&self) -> PublicationGateState {
        PublicationGateState::new(PublicationGateRegistration {
            run_id: self.run_id.clone(),
            stage_instance_id: self.stages.publication_gate.stage_instance_id.clone(),
            expected_publication_stage_instance_id: self.stages.publisher.stage_instance_id.clone(),
            expected_publication_component_digest: self.stages.publisher.component_digest,
            expected_review_stage_instance_id: self
                .stages
                .adversarial_review
                .stage_instance_id
                .clone(),
            expected_review_component_digest: self.stages.adversarial_review.component_digest,
            expected_implementation_stage_instance_id: self
                .stages
                .implementation
                .stage_instance_id
                .clone(),
            expected_implementation_component_digest: self.stages.implementation.component_digest,
            expected_review_package_artifact_id: self.artifacts.review_package.clone(),
            expected_authorization_artifact_id: self.artifacts.publication_authorization.clone(),
            pipeline_definition_digest: self.pipeline_definition_digest,
            gate_component_digest: self.stages.publication_gate.component_digest,
        })
    }

    fn validate(
        &self,
        pipeline: &PipelineState,
        gate: &PublicationGateState,
    ) -> Result<(), StoreError> {
        if self.review.stage_instance_id != self.stages.adversarial_review.stage_instance_id
            || self.review.evidence_producer_stage_instance_id != self.review.stage_instance_id
            || self.review.evidence_component_digest
                != self.stages.adversarial_review.component_digest
            || self.review.validate().is_err()
        {
            return Err(StoreError::InvalidM1Start);
        }
        validate_gate_registration(pipeline, gate)
    }
}

pub struct M1RunIngress<'a> {
    pub artifact_type: &'a str,
    pub schema_id: &'a str,
    pub schema_version: u32,
    pub media_type: &'a str,
    pub retention_class: &'a str,
    pub bytes: &'a [u8],
}

pub trait M1IdSource {
    fn next_batch_id(&mut self) -> Result<BatchId, StoreError>;
    fn next_event_id(&mut self) -> Result<EventId, StoreError>;
    fn next_message_id(&mut self) -> Result<MessageId, StoreError>;
    fn next_artifact_id(&mut self) -> Result<ArtifactId, StoreError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum M1StartOutcome {
    Started,
    Resumed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum M1ReconcileOutcome {
    ScheduledImplementation { input_manifest_digest: Sha256Digest },
    NeedsAgentTransport,
    AwaitingReport,
    AwaitingHuman,
    PublicationPending,
    Complete,
}

pub struct ResumableM1Run<'store, 'clock> {
    lease: LeasedRun<'store, 'clock>,
    descriptor: M1StartDescriptor,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CanonicalM1Start {
    descriptor: M1StartDescriptor,
    ingress: RunIngressArtifactRecord,
    batch_id: BatchId,
    acceptance_event_id: EventId,
    acceptance_message_id: MessageId,
    acceptance_message_digest: Sha256Digest,
    acceptance_event: PipelineEvent,
}

impl SqliteStore {
    #[allow(clippy::too_many_arguments)]
    pub fn start_m1_run<'store, 'clock>(
        &'store mut self,
        artifact_store: &ArtifactStore,
        descriptor: &M1StartDescriptor,
        ingress: M1RunIngress<'_>,
        owner_id: &OperationId,
        lease_duration_ms: u64,
        clock: &'clock dyn UnixMillisClock,
        ids: &mut dyn M1IdSource,
    ) -> Result<(M1StartOutcome, ResumableM1Run<'store, 'clock>), StoreError> {
        let descriptor_bytes = descriptor.canonical_bytes()?;
        let descriptor_digest = Sha256Digest::of_bytes(&descriptor_bytes);
        let stored_bytes = artifact_store
            .put(ingress.bytes)
            .map_err(StoreError::ArtifactStore)?;
        let size = u64::try_from(ingress.bytes.len())
            .map_err(|_| StoreError::ArtifactMetadataMismatch("ingress size exceeds platform"))?;
        if stored_bytes.size != size {
            return Err(StoreError::ArtifactMetadataMismatch(
                "ingress size mismatch",
            ));
        }
        let pipeline = descriptor.pipeline()?;
        let gate = descriptor.gate();
        descriptor.validate(&pipeline, &gate)?;

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        let existing: Option<(String, Vec<u8>, String)> = transaction
            .query_row(
                "SELECT start_digest, start_json, ingress_record_digest
                 FROM m1_run_starts WHERE run_id = ?1",
                params![descriptor.run_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(StoreError::Sqlite)?;
        if let Some((start_digest, start_json, ingress_record_digest)) = existing {
            let parsed_start_digest =
                Sha256Digest::from_str(&start_digest).map_err(StoreError::Digest)?;
            if parsed_start_digest != Sha256Digest::of_bytes(&start_json) {
                return Err(StoreError::M1StartConflict);
            }
            let committed = decode_start(&transaction, &descriptor.run_id, &start_json)?;
            if ingress_record_digest
                != Sha256Digest::of_bytes(&canonical_bytes(&committed.ingress)?)
                    .to_prefixed_string()
            {
                return Err(StoreError::RunIngressConflict);
            }
            verify_m1_start_event_ids(&transaction, &descriptor.run_id)?;
            if committed.descriptor != *descriptor
                || committed.ingress.sha256 != stored_bytes.sha256
                || committed.ingress.size != stored_bytes.size
                || committed.ingress.artifact_type != ingress.artifact_type
                || committed.ingress.schema_id != ingress.schema_id
                || committed.ingress.schema_version != ingress.schema_version
                || committed.ingress.media_type != ingress.media_type
                || committed.ingress.retention_class != ingress.retention_class
                || Sha256Digest::of_bytes(&descriptor_bytes) != descriptor_digest
            {
                return Err(StoreError::M1StartConflict);
            }
            transaction.commit().map_err(StoreError::Sqlite)?;
            verify_m1_recovery(self, artifact_store, descriptor)?;
            let lease = self.acquire_run(&descriptor.run_id, owner_id, lease_duration_ms, clock)?;
            return Ok((
                M1StartOutcome::Resumed,
                ResumableM1Run {
                    lease,
                    descriptor: descriptor.clone(),
                },
            ));
        }

        let run_exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runs WHERE run_id = ?1)",
                params![descriptor.run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        if run_exists {
            return Err(StoreError::M1StartConflict);
        }
        let now = clock.now_unix_ms().map_err(StoreError::Clock)?;
        let expires_at = lease_expiry(now, lease_duration_ms)?;
        let batch_id = ids.next_batch_id()?;
        let acceptance_event_id = ids.next_event_id()?;
        let acceptance_message_id = ids.next_message_id()?;
        let acceptance_sequence = 1_u64;
        let message_exists: bool = transaction
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM (
                        SELECT message_id FROM events WHERE message_id = ?1
                        UNION ALL SELECT message_id FROM pipeline_events WHERE message_id = ?1
                        UNION ALL SELECT message_id FROM publication_gate_events WHERE message_id = ?1
                        UNION ALL SELECT message_id FROM adversarial_review_events WHERE message_id = ?1
                    )
                 )",
                params![acceptance_message_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        let batch_exists: bool = transaction
            .query_row(
                "SELECT EXISTS(
                    SELECT batch_id FROM semantic_batches WHERE batch_id = ?1
                    UNION ALL SELECT batch_id FROM m1_run_starts WHERE batch_id = ?1
                 )",
                params![batch_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        if crate::event_id_exists(&transaction, &acceptance_event_id)?
            || message_exists
            || batch_exists
        {
            return Err(StoreError::M1StartConflict);
        }

        transaction
            .execute(
                "INSERT INTO runs(run_id, pipeline_definition_digest, next_event_sequence)
                 VALUES (?1, ?2, 2)",
                params![
                    descriptor.run_id.as_str(),
                    descriptor.pipeline_definition_digest.to_prefixed_string()
                ],
            )
            .map_err(StoreError::Sqlite)?;
        transaction
            .execute(
                "INSERT INTO run_leases(run_id, lease_epoch, owner_id, expires_at_unix_ms)
                 VALUES (?1, 1, ?2, ?3)",
                params![
                    descriptor.run_id.as_str(),
                    owner_id.as_str(),
                    to_sql_integer(expires_at)?
                ],
            )
            .map_err(StoreError::Sqlite)?;
        reserve_m1_artifact_ids(&transaction, descriptor)?;

        insert_m1_roots(&transaction, descriptor, &pipeline, &gate)?;
        let acceptance_event = pipeline
            .decide(PipelineCommand::AcceptArtifact {
                expected_revision: 0,
                artifact_id: descriptor.artifacts.implementation_input.clone(),
                sha256: stored_bytes.sha256,
                parent_artifact_ids: Vec::new(),
            })
            .map_err(StoreError::PipelineTransition)?;
        let next_pipeline = pipeline
            .apply(&acceptance_event)
            .map_err(StoreError::PipelineTransition)?;
        let acceptance_message_digest = canonical_digest(&acceptance_event)?;
        let ingress_record = RunIngressArtifactRecord {
            protocol: BASE_PROTOCOL.into(),
            artifact_id: descriptor.artifacts.implementation_input.clone(),
            artifact_type: ingress.artifact_type.into(),
            schema_id: ingress.schema_id.into(),
            schema_version: ingress.schema_version,
            sha256: stored_bytes.sha256,
            size: stored_bytes.size,
            media_type: ingress.media_type.into(),
            run_id: descriptor.run_id.clone(),
            producer_attempt: 0,
            producer_event_sequence: acceptance_sequence,
            pipeline_definition_digest: descriptor.pipeline_definition_digest,
            root_stage_instance_id: descriptor.stages.implementation.stage_instance_id.clone(),
            component_digest: descriptor.stages.implementation.component_digest,
            retention_class: ingress.retention_class.into(),
        };
        ingress_record
            .validate()
            .map_err(StoreError::ArtifactValidation)?;
        let ingress_json = canonical_bytes(&ingress_record)?;
        let ingress_digest = Sha256Digest::of_bytes(&ingress_json);
        let entry = SemanticPipelineEntry {
            event_id: &acceptance_event_id,
            message_id: &acceptance_message_id,
            message_digest: &acceptance_message_digest,
            event: &acceptance_event,
        };
        insert_pipeline_event(
            &transaction,
            &descriptor.run_id,
            acceptance_sequence,
            &entry,
        )?;
        transaction
            .execute(
                "INSERT INTO run_ingress_artifacts(
                    artifact_id, run_id, sha256, size, producer_event_sequence,
                    record_digest, record_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    ingress_record.artifact_id.as_str(),
                    descriptor.run_id.as_str(),
                    ingress_record.sha256.to_prefixed_string(),
                    to_sql_integer(ingress_record.size)?,
                    to_sql_integer(acceptance_sequence)?,
                    ingress_digest.to_prefixed_string(),
                    ingress_json
                ],
            )
            .map_err(StoreError::Sqlite)?;
        let pipeline_json =
            serde_json::to_vec(&next_pipeline).map_err(StoreError::Serialization)?;
        transaction
            .execute(
                "UPDATE pipeline_snapshots SET control_revision = 1, state_json = ?1
                 WHERE run_id = ?2 AND control_revision = 0",
                params![pipeline_json, descriptor.run_id.as_str()],
            )
            .map_err(StoreError::Sqlite)?;
        let commitment = CanonicalM1Start {
            descriptor: descriptor.clone(),
            ingress: ingress_record.clone(),
            batch_id,
            acceptance_event_id: acceptance_event_id.clone(),
            acceptance_message_id,
            acceptance_message_digest,
            acceptance_event,
        };
        let start_json = canonical_bytes(&commitment)?;
        let start_digest = Sha256Digest::of_bytes(&start_json);
        transaction
            .execute(
                "INSERT INTO m1_run_starts(
                    run_id, start_digest, start_json, batch_id,
                    acceptance_event_id, acceptance_sequence, ingress_artifact_id,
                    ingress_record_digest, ingress_record_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    descriptor.run_id.as_str(),
                    start_digest.to_prefixed_string(),
                    start_json,
                    commitment.batch_id.as_str(),
                    acceptance_event_id.as_str(),
                    to_sql_integer(acceptance_sequence)?,
                    ingress_record.artifact_id.as_str(),
                    ingress_digest.to_prefixed_string(),
                    canonical_bytes(&ingress_record)?
                ],
            )
            .map_err(StoreError::Sqlite)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        let fence = RunLeaseFence::new(descriptor.run_id.clone(), owner_id.clone(), 1, expires_at);
        Ok((
            M1StartOutcome::Started,
            ResumableM1Run {
                lease: LeasedRun {
                    store: self,
                    clock,
                    fence,
                    duration_ms: lease_duration_ms,
                },
                descriptor: descriptor.clone(),
            },
        ))
    }

    pub fn resume_m1_run<'store, 'clock>(
        &'store mut self,
        artifact_store: &ArtifactStore,
        run_id: &RunId,
        owner_id: &OperationId,
        lease_duration_ms: u64,
        clock: &'clock dyn UnixMillisClock,
    ) -> Result<ResumableM1Run<'store, 'clock>, StoreError> {
        let descriptor = load_start_descriptor(&self.connection, run_id)?;
        verify_m1_recovery(self, artifact_store, &descriptor)?;
        let lease = self.acquire_run(run_id, owner_id, lease_duration_ms, clock)?;
        Ok(ResumableM1Run { lease, descriptor })
    }
}

impl ResumableM1Run<'_, '_> {
    pub fn descriptor(&self) -> &M1StartDescriptor {
        &self.descriptor
    }

    pub fn fence(&self) -> &RunLeaseFence {
        self.lease.fence()
    }

    pub fn renew(&mut self) -> Result<(), StoreError> {
        self.lease.renew()
    }

    pub fn reconcile_once(
        &mut self,
        artifact_store: &ArtifactStore,
        ids: &mut dyn M1IdSource,
    ) -> Result<M1ReconcileOutcome, StoreError> {
        self.lease.ensure_active()?;
        let pipeline = self.lease.store.load_pipeline(&self.descriptor.run_id)?;
        let implementation = pipeline
            .stage(&self.descriptor.stages.implementation.stage_instance_id)
            .ok_or(StoreError::InvalidM1Start)?;
        let outcome = match implementation.phase {
            StagePhase::Pending => {
                let event = pipeline
                    .decide(PipelineCommand::ScheduleStage {
                        expected_revision: pipeline.control_revision,
                        stage_instance_id: implementation.stage_instance_id.clone(),
                    })
                    .map_err(StoreError::PipelineTransition)?;
                let PipelineEventKind::StageScheduled {
                    stage_event,
                    input_manifest,
                } = &event.kind
                else {
                    return Err(StoreError::InvalidM1Start);
                };
                let manifest_bytes = input_manifest
                    .canonical_bytes()
                    .map_err(StoreError::Canonicalization)?;
                let manifest_digest = Sha256Digest::of_bytes(&manifest_bytes);
                let sequence = self
                    .lease
                    .store
                    .next_event_sequence(&self.descriptor.run_id)?;
                let record = ArtifactRecord {
                    artifact_id: ids.next_artifact_id()?,
                    artifact_type: "stage-input-manifest/v1".into(),
                    schema_id: "stage-input-manifest".into(),
                    schema_version: 1,
                    sha256: manifest_digest,
                    size: u64::try_from(manifest_bytes.len()).map_err(|_| {
                        StoreError::ArtifactMetadataMismatch("manifest size exceeds platform")
                    })?,
                    media_type: "application/json".into(),
                    producer_stage_instance_id: implementation.stage_instance_id.clone(),
                    producer_attempt: 0,
                    producer_event_sequence: sequence,
                    pipeline_definition_digest: self.descriptor.pipeline_definition_digest,
                    component_digest: implementation.component_digest,
                    input_manifest_digest: manifest_digest,
                    retention_class: "run-record".into(),
                };
                let manifest_parents = input_manifest
                    .artifacts
                    .iter()
                    .map(|input| input.artifact_id.clone())
                    .collect::<Vec<_>>();
                let registration = [crate::ArtifactRegistration {
                    record: &record,
                    parent_artifact_ids: &manifest_parents,
                    bytes: &manifest_bytes,
                }];
                let stage_event_id = ids.next_event_id()?;
                let stage_message_id = ids.next_message_id()?;
                let stage_message_digest = canonical_digest(stage_event)?;
                let pipeline_event_id = ids.next_event_id()?;
                let pipeline_message_id = ids.next_message_id()?;
                let pipeline_message_digest = canonical_digest(&event)?;
                let stage_entries = [crate::SemanticStageEntry {
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
                    event: &event,
                }];
                let batch_id = ids.next_batch_id()?;
                self.lease.append_semantic_batch(
                    artifact_store,
                    crate::SemanticBatch {
                        batch_id: &batch_id,
                        run_id: &self.descriptor.run_id,
                        stage_entries: &stage_entries,
                        publication_gate_entries: &[],
                        pipeline_entries: &pipeline_entries,
                    },
                )?;
                M1ReconcileOutcome::ScheduledImplementation {
                    input_manifest_digest: manifest_digest,
                }
            }
            StagePhase::Ready | StagePhase::Provisioning => M1ReconcileOutcome::NeedsAgentTransport,
            StagePhase::Running | StagePhase::Blocked | StagePhase::Paused => {
                M1ReconcileOutcome::AwaitingReport
            }
            StagePhase::Completed | StagePhase::Invalidated | StagePhase::Failed => {
                let gate = self.lease.store.load_publication_gate(
                    &self.descriptor.run_id,
                    &self.descriptor.stages.publication_gate.stage_instance_id,
                )?;
                match gate.phase {
                    herdr_flow_core::PublicationGatePhase::AwaitingHuman => {
                        M1ReconcileOutcome::AwaitingHuman
                    }
                    herdr_flow_core::PublicationGatePhase::Authorized => {
                        M1ReconcileOutcome::PublicationPending
                    }
                    _ => M1ReconcileOutcome::AwaitingReport,
                }
            }
        };
        self.lease.ensure_active()?;
        Ok(outcome)
    }
}

fn verify_m1_recovery(
    store: &SqliteStore,
    artifact_store: &ArtifactStore,
    descriptor: &M1StartDescriptor,
) -> Result<(), StoreError> {
    let transaction = store
        .connection
        .unchecked_transaction()
        .map_err(StoreError::Sqlite)?;
    crate::verify_run_journal(&transaction, &descriptor.run_id)?;
    let expected_pipeline = descriptor.pipeline()?;
    let initial_pipeline_json: Vec<u8> = transaction
        .query_row(
            "SELECT initial_state_json FROM pipeline_snapshots WHERE run_id = ?1",
            params![descriptor.run_id.as_str()],
            |row| row.get(0),
        )
        .map_err(StoreError::Sqlite)?;
    let initial_pipeline: PipelineState =
        serde_json::from_slice(&initial_pipeline_json).map_err(StoreError::Serialization)?;
    let mut stored_stage_ids = transaction
        .prepare("SELECT stage_instance_id FROM stage_snapshots WHERE run_id = ?1 ORDER BY stage_instance_id")
        .map_err(StoreError::Sqlite)?
        .query_map(params![descriptor.run_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(StoreError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Sqlite)?;
    let mut expected_stage_ids = descriptor
        .pipeline()?
        .stage_states()
        .into_iter()
        .map(|stage| stage.stage_instance_id.to_string())
        .collect::<Vec<_>>();
    stored_stage_ids.sort();
    expected_stage_ids.sort();
    let (gate_count, registration_count): (i64, i64) = transaction
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM publication_gate_snapshots WHERE run_id = ?1),
                (SELECT COUNT(*) FROM adversarial_review_registrations WHERE run_id = ?1)",
            params![descriptor.run_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(StoreError::Sqlite)?;
    if initial_pipeline != expected_pipeline
        || stored_stage_ids != expected_stage_ids
        || gate_count != 1
        || registration_count != 1
    {
        return Err(StoreError::M1StartConflict);
    }
    crate::pipeline::verified_pipeline(&transaction, &descriptor.run_id)?;
    let expected_gate = descriptor.gate();
    let initial_gate_json: Vec<u8> = transaction
        .query_row(
            "SELECT initial_state_json FROM publication_gate_snapshots
             WHERE run_id = ?1 AND stage_instance_id = ?2",
            params![
                descriptor.run_id.as_str(),
                expected_gate.stage_instance_id.as_str()
            ],
            |row| row.get(0),
        )
        .map_err(StoreError::Sqlite)?;
    let initial_gate: PublicationGateState =
        serde_json::from_slice(&initial_gate_json).map_err(StoreError::Serialization)?;
    if initial_gate != expected_gate
        || crate::review::load_registration(
            &transaction,
            &descriptor.run_id,
            &descriptor.review.stage_instance_id,
        )?
        .as_ref()
            != Some(&descriptor.review)
    {
        return Err(StoreError::M1StartConflict);
    }
    crate::pipeline::verified_publication_gate(
        &transaction,
        &descriptor.run_id,
        &expected_gate.stage_instance_id,
    )?;
    let ingress_identity: Option<(String, String, Option<String>, Option<String>)> = transaction
        .query_row(
            "SELECT run_id, identity_kind, expected_producer_stage_instance_id,
                    expected_artifact_type
             FROM artifact_identities WHERE artifact_id = ?1",
            params![descriptor.artifacts.implementation_input.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    if ingress_identity
        != Some((
            descriptor.run_id.to_string(),
            "INGRESS".to_owned(),
            None,
            None,
        ))
    {
        return Err(StoreError::ArtifactIdConflict);
    }
    for (artifact_id, producer, artifact_type) in expected_output_reservations(descriptor) {
        ensure_output_identity(
            &transaction,
            &descriptor.run_id,
            artifact_id,
            producer,
            artifact_type,
            false,
        )?;
    }
    let records = crate::registry::load_and_verify_run_artifacts(&transaction, &descriptor.run_id)?;
    crate::review::verify_adversarial_review_candidate_bytes(
        &transaction,
        &descriptor.run_id,
        artifact_store,
    )?;
    transaction.commit().map_err(StoreError::Sqlite)?;
    crate::registry::verify_artifact_bytes(artifact_store, &records)
}

pub(crate) fn migrate_m1_artifact_reservations(
    transaction: &Transaction<'_>,
) -> Result<(), StoreError> {
    let mut statement = transaction
        .prepare("SELECT run_id, start_json FROM m1_run_starts ORDER BY run_id")
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(StoreError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Sqlite)?;
    drop(statement);
    for (run_id, start_json) in rows {
        let run_id = RunId::from_str(&run_id).map_err(StoreError::Identifier)?;
        let start = decode_start(transaction, &run_id, &start_json)?;
        for (artifact_id, producer, artifact_type) in
            expected_output_reservations(&start.descriptor)
        {
            ensure_output_identity(
                transaction,
                &run_id,
                artifact_id,
                producer,
                artifact_type,
                true,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn has_legacy_scheduled_m1_run(
    transaction: &Transaction<'_>,
) -> Result<bool, StoreError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM m1_run_starts AS starts
                JOIN pipeline_events AS events ON events.run_id = starts.run_id
                WHERE events.sequence > starts.acceptance_sequence
             )",
            [],
            |row| row.get(0),
        )
        .map_err(StoreError::Sqlite)
}

fn expected_output_reservations(
    descriptor: &M1StartDescriptor,
) -> [(&ArtifactId, &StageInstanceId, &'static str); 3] {
    [
        (
            &descriptor.artifacts.candidate,
            &descriptor.stages.implementation.stage_instance_id,
            "candidate-commit/v1",
        ),
        (
            &descriptor.artifacts.review_package,
            &descriptor.stages.adversarial_review.stage_instance_id,
            "review-package/v1",
        ),
        (
            &descriptor.artifacts.publication_authorization,
            &descriptor.stages.publication_gate.stage_instance_id,
            "publication-authorization/v1",
        ),
    ]
}

fn ensure_output_identity(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    artifact_id: &ArtifactId,
    producer: &StageInstanceId,
    artifact_type: &str,
    create_missing_reservation: bool,
) -> Result<(), StoreError> {
    type Identity = (String, String, Option<String>, Option<String>);
    let identity: Option<Identity> = transaction
        .query_row(
            "SELECT run_id, identity_kind, expected_producer_stage_instance_id,
                    expected_artifact_type
             FROM artifact_identities WHERE artifact_id = ?1",
            params![artifact_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    match identity {
        None if create_missing_reservation => {
            transaction
                .execute(
                    "INSERT INTO artifact_identities(
                        artifact_id, run_id, identity_kind,
                        expected_producer_stage_instance_id, expected_artifact_type
                     ) VALUES (?1, ?2, 'RESERVED', ?3, ?4)",
                    params![
                        artifact_id.as_str(),
                        run_id.as_str(),
                        producer.as_str(),
                        artifact_type
                    ],
                )
                .map_err(StoreError::Sqlite)?;
            Ok(())
        }
        Some((stored_run, kind, stored_producer, stored_type))
            if stored_run == run_id.as_str()
                && kind == "RESERVED"
                && stored_producer.as_deref() == Some(producer.as_str())
                && stored_type.as_deref() == Some(artifact_type) =>
        {
            Ok(())
        }
        Some((stored_run, kind, _, _)) if stored_run == run_id.as_str() && kind == "REGULAR" => {
            let stored = crate::registry::load_artifact_record(transaction, run_id, artifact_id)?
                .ok_or(StoreError::ArtifactNotFound)?;
            if stored.record.producer_stage_instance_id != *producer
                || stored.record.artifact_type != artifact_type
            {
                return Err(StoreError::ArtifactIdConflict);
            }
            Ok(())
        }
        _ => Err(StoreError::ArtifactIdConflict),
    }
}

fn reserve_m1_artifact_ids(
    transaction: &Transaction<'_>,
    descriptor: &M1StartDescriptor,
) -> Result<(), StoreError> {
    let reservations = [
        (
            &descriptor.artifacts.implementation_input,
            "INGRESS",
            None,
            None,
        ),
        (
            &descriptor.artifacts.candidate,
            "RESERVED",
            Some(&descriptor.stages.implementation.stage_instance_id),
            Some("candidate-commit/v1"),
        ),
        (
            &descriptor.artifacts.review_package,
            "RESERVED",
            Some(&descriptor.stages.adversarial_review.stage_instance_id),
            Some("review-package/v1"),
        ),
        (
            &descriptor.artifacts.publication_authorization,
            "RESERVED",
            Some(&descriptor.stages.publication_gate.stage_instance_id),
            Some("publication-authorization/v1"),
        ),
    ];
    for (artifact_id, kind, producer, artifact_type) in reservations {
        transaction
            .execute(
                "INSERT INTO artifact_identities(
                    artifact_id, run_id, identity_kind,
                    expected_producer_stage_instance_id, expected_artifact_type
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    artifact_id.as_str(),
                    descriptor.run_id.as_str(),
                    kind,
                    producer.map(StageInstanceId::as_str),
                    artifact_type,
                ],
            )
            .map_err(|error| {
                if matches!(error, rusqlite::Error::SqliteFailure(_, _)) {
                    StoreError::ArtifactIdConflict
                } else {
                    StoreError::Sqlite(error)
                }
            })?;
    }
    Ok(())
}

fn insert_m1_roots(
    transaction: &Transaction<'_>,
    descriptor: &M1StartDescriptor,
    pipeline: &PipelineState,
    gate: &PublicationGateState,
) -> Result<(), StoreError> {
    for stage in pipeline.stage_states() {
        let json = serde_json::to_vec(&stage).map_err(StoreError::Serialization)?;
        transaction
            .execute(
                "INSERT INTO stage_snapshots(
                    stage_instance_id, run_id, control_revision, initial_state_json, state_json
                 ) VALUES (?1, ?2, 0, ?3, ?3)",
                params![
                    stage.stage_instance_id.as_str(),
                    descriptor.run_id.as_str(),
                    json
                ],
            )
            .map_err(StoreError::Sqlite)?;
    }
    let pipeline_json = serde_json::to_vec(pipeline).map_err(StoreError::Serialization)?;
    transaction
        .execute(
            "INSERT INTO pipeline_snapshots(run_id, control_revision, initial_state_json, state_json)
             VALUES (?1, 0, ?2, ?2)",
            params![descriptor.run_id.as_str(), pipeline_json],
        )
        .map_err(StoreError::Sqlite)?;
    crate::review::insert_adversarial_review_registration(
        transaction,
        &descriptor.run_id,
        &descriptor.review,
    )?;
    let gate_json = serde_json::to_vec(gate).map_err(StoreError::Serialization)?;
    transaction
        .execute(
            "INSERT INTO publication_gate_snapshots(
                stage_instance_id, run_id, control_revision, initial_state_json, state_json
             ) VALUES (?1, ?2, 0, ?3, ?3)",
            params![
                gate.stage_instance_id.as_str(),
                descriptor.run_id.as_str(),
                gate_json
            ],
        )
        .map_err(StoreError::Sqlite)?;
    Ok(())
}

pub(crate) fn verify_m1_start_event_ids(
    connection: &rusqlite::Connection,
    run_id: &RunId,
) -> Result<Vec<EventId>, StoreError> {
    type StartIndex = (
        String,
        Vec<u8>,
        String,
        String,
        i64,
        String,
        String,
        Vec<u8>,
    );
    let row: Option<StartIndex> = connection
        .query_row(
            "SELECT start_digest, start_json, batch_id, acceptance_event_id,
                    acceptance_sequence, ingress_artifact_id,
                    ingress_record_digest, ingress_record_json
             FROM m1_run_starts WHERE run_id = ?1",
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
                    row.get(7)?,
                ))
            },
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let Some((
        digest,
        json,
        batch_id,
        acceptance_event_id,
        acceptance_sequence,
        ingress_artifact_id,
        ingress_record_digest,
        ingress_record_json,
    )) = row
    else {
        return Ok(Vec::new());
    };
    let digest = Sha256Digest::from_str(&digest).map_err(StoreError::Digest)?;
    if Sha256Digest::of_bytes(&json) != digest {
        return Err(StoreError::M1StartConflict);
    }
    let start = decode_start(connection, run_id, &json)?;
    let canonical_ingress = canonical_bytes(&start.ingress)?;
    if batch_id != start.batch_id.as_str()
        || acceptance_event_id != start.acceptance_event_id.as_str()
        || crate::from_sql_integer(acceptance_sequence)? != start.ingress.producer_event_sequence
        || ingress_artifact_id != start.ingress.artifact_id.as_str()
        || ingress_record_digest != Sha256Digest::of_bytes(&canonical_ingress).to_prefixed_string()
        || ingress_record_json != canonical_ingress
    {
        return Err(StoreError::M1StartConflict);
    }
    let events = crate::pipeline::load_pipeline_events(connection, run_id)?;
    let stored = events
        .iter()
        .find(|event| event.event_id == start.acceptance_event_id)
        .ok_or(StoreError::PartialSemanticBatch)?;
    if stored.sequence != start.ingress.producer_event_sequence
        || stored.message_id != start.acceptance_message_id
        || stored.message_digest != start.acceptance_message_digest
        || stored.event != start.acceptance_event
    {
        return Err(StoreError::M1StartConflict);
    }
    Ok(vec![start.acceptance_event_id])
}

pub(crate) fn load_run_ingress_records(
    connection: &rusqlite::Connection,
    run_id: &RunId,
) -> Result<Vec<StoredArtifactRecord>, StoreError> {
    let row: Option<Vec<u8>> = connection
        .query_row(
            "SELECT start_json FROM m1_run_starts WHERE run_id = ?1",
            params![run_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let Some(json) = row else {
        return Ok(Vec::new());
    };
    let start = decode_start(connection, run_id, &json)?;
    let record = ArtifactRecord {
        artifact_id: start.ingress.artifact_id.clone(),
        artifact_type: start.ingress.artifact_type.clone(),
        schema_id: start.ingress.schema_id.clone(),
        schema_version: start.ingress.schema_version,
        sha256: start.ingress.sha256,
        size: start.ingress.size,
        media_type: start.ingress.media_type.clone(),
        producer_stage_instance_id: start.ingress.root_stage_instance_id.clone(),
        producer_attempt: 0,
        producer_event_sequence: start.ingress.producer_event_sequence,
        pipeline_definition_digest: start.ingress.pipeline_definition_digest,
        component_digest: start.ingress.component_digest,
        input_manifest_digest: start.ingress.sha256,
        retention_class: start.ingress.retention_class.clone(),
    };
    let record_json = canonical_bytes(&record)?;
    Ok(vec![StoredArtifactRecord {
        run_id: run_id.clone(),
        record,
        parent_artifact_ids: Vec::new(),
        record_digest: Sha256Digest::of_bytes(&record_json),
    }])
}

pub(crate) fn verify_run_ingress_bytes(
    connection: &rusqlite::Connection,
    run_id: &RunId,
    artifact_store: &ArtifactStore,
) -> Result<(), StoreError> {
    let row: Option<Vec<u8>> = connection
        .query_row(
            "SELECT start_json FROM m1_run_starts WHERE run_id = ?1",
            params![run_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let Some(json) = row else {
        return Ok(());
    };
    let start = decode_start(connection, run_id, &json)?;
    let bytes = artifact_store
        .read_verified(start.ingress.sha256)
        .map_err(StoreError::ArtifactStore)?;
    if u64::try_from(bytes.len()).ok() != Some(start.ingress.size) {
        return Err(StoreError::RunIngressConflict);
    }
    Ok(())
}

fn load_start_descriptor(
    connection: &rusqlite::Connection,
    run_id: &RunId,
) -> Result<M1StartDescriptor, StoreError> {
    verify_m1_start_event_ids(connection, run_id)?;
    let (digest, json): (String, Vec<u8>) = connection
        .query_row(
            "SELECT start_digest, start_json FROM m1_run_starts WHERE run_id = ?1",
            params![run_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?
        .ok_or(StoreError::RunNotFound)?;
    if Sha256Digest::from_str(&digest).map_err(StoreError::Digest)? != Sha256Digest::of_bytes(&json)
    {
        return Err(StoreError::M1StartConflict);
    }
    decode_start(connection, run_id, &json).map(|start| start.descriptor)
}

fn decode_start(
    connection: &rusqlite::Connection,
    run_id: &RunId,
    start_json: &[u8],
) -> Result<CanonicalM1Start, StoreError> {
    let start: CanonicalM1Start =
        serde_json::from_slice(start_json).map_err(StoreError::Serialization)?;
    if start.descriptor.run_id != *run_id
        || canonical_bytes(&start)? != start_json
        || start.ingress.run_id != *run_id
        || start.ingress.artifact_id != start.descriptor.artifacts.implementation_input
        || start.ingress.pipeline_definition_digest != start.descriptor.pipeline_definition_digest
        || start.ingress.root_stage_instance_id
            != start.descriptor.stages.implementation.stage_instance_id
        || start.ingress.component_digest != start.descriptor.stages.implementation.component_digest
        || start.ingress.producer_attempt != 0
        || start.ingress.producer_event_sequence != 1
        || start.acceptance_event.prior_control_revision != 0
        || !matches!(
            &start.acceptance_event.kind,
            PipelineEventKind::ArtifactAccepted {
                artifact_id,
                sha256,
                parent_artifact_ids,
            } if artifact_id == &start.ingress.artifact_id
                && sha256 == &start.ingress.sha256
                && parent_artifact_ids.is_empty()
        )
    {
        return Err(StoreError::M1StartConflict);
    }
    start
        .ingress
        .validate()
        .map_err(StoreError::ArtifactValidation)?;
    let ingress_json = canonical_bytes(&start.ingress)?;
    let indexed: Option<(String, String, i64, i64, String, Vec<u8>)> = connection
        .query_row(
            "SELECT run_id, sha256, size, producer_event_sequence, record_digest, record_json
             FROM run_ingress_artifacts WHERE artifact_id = ?1",
            params![start.ingress.artifact_id.as_str()],
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
    let indexed = indexed.ok_or(StoreError::ArtifactNotFound)?;
    let identity: Option<(String, String)> = connection
        .query_row(
            "SELECT run_id, identity_kind FROM artifact_identities WHERE artifact_id = ?1",
            params![start.ingress.artifact_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    if identity
        .as_ref()
        .map(|(run, kind)| (run.as_str(), kind.as_str()))
        != Some((run_id.as_str(), "INGRESS"))
        || indexed.0 != run_id.as_str()
        || indexed.1 != start.ingress.sha256.to_prefixed_string()
        || crate::from_sql_integer(indexed.2)? != start.ingress.size
        || crate::from_sql_integer(indexed.3)? != start.ingress.producer_event_sequence
        || indexed.4 != Sha256Digest::of_bytes(&ingress_json).to_prefixed_string()
        || indexed.5 != ingress_json
    {
        return Err(StoreError::RunIngressConflict);
    }
    Ok(start)
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    let value = serde_json::to_value(value).map_err(StoreError::Serialization)?;
    canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)
}

fn canonical_digest<T: Serialize>(value: &T) -> Result<Sha256Digest, StoreError> {
    canonical_bytes(value).map(|bytes| Sha256Digest::of_bytes(&bytes))
}

fn lease_expiry(now: u64, duration_ms: u64) -> Result<u64, StoreError> {
    if duration_ms == 0 {
        return Err(StoreError::InvalidRunLeaseDuration);
    }
    now.checked_add(duration_ms)
        .filter(|value| *value <= i64::MAX as u64 && *value <= MAX_CONTROL_REVISION)
        .ok_or(StoreError::InvalidRunLeaseDuration)
}
