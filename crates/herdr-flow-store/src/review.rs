use std::str::FromStr;

use herdr_flow_core::{
    canonical_json, replay_adversarial_review, AdversarialReviewCommand, AdversarialReviewEvent,
    AdversarialReviewState, ArtifactId, EventId, ImplementationCandidateArtifact,
    InputManifestArtifact, MessageId, ParticipantPrincipalId, ReviewCandidateArtifact,
    ReviewCandidateCheckResult, ReviewCandidateObjectManifest, ReviewCandidateValidation, RunId,
    Sha256Digest, StageInputManifest, StageInstanceId, BASE_PROTOCOL, MAX_CONTROL_REVISION,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

type AdversarialReviewEventRow = (String, String, i64, String, String, String, Vec<u8>);

use crate::{
    event_id_exists, from_sql_integer,
    lease::{lease_now, LeasedRun, RunLeaseFence, UnixMillisClock},
    registry, to_sql_integer, verified_stage, verify_run_journal, ArtifactStore, GitRepository,
    SqliteStore, StoreError,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdversarialReviewRegistration {
    pub stage_instance_id: StageInstanceId,
    pub implementer: ParticipantPrincipalId,
    pub reviewers: Vec<ParticipantPrincipalId>,
    pub baseline: herdr_flow_core::GitObjectId,
    pub evidence_producer_stage_instance_id: StageInstanceId,
    pub evidence_component_digest: Sha256Digest,
    pub check_policy_digest: Sha256Digest,
}

impl AdversarialReviewRegistration {
    pub fn validate(&self) -> Result<(), StoreError> {
        AdversarialReviewState::new(
            self.stage_instance_id.clone(),
            self.implementer.clone(),
            self.reviewers.clone(),
            Sha256Digest::of_bytes(b"registration-validation-placeholder"),
            self.baseline.clone(),
        )
        .map(|_| ())
        .map_err(|_| StoreError::InvalidInitialAdversarialReview)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredAdversarialReviewEvent {
    pub event_id: EventId,
    pub run_id: RunId,
    pub sequence: u64,
    pub message_id: MessageId,
    pub message_digest: Sha256Digest,
    pub event_digest: Sha256Digest,
    pub event: AdversarialReviewEvent,
}

pub struct ReviewCandidateEvidence<'a> {
    pub object_manifest_artifact_id: &'a ArtifactId,
    pub validation_artifact_id: &'a ArtifactId,
    pub check_result_artifact_id: &'a ArtifactId,
}

pub struct AdversarialReviewSubmission<'a> {
    pub run_id: &'a RunId,
    pub stage_instance_id: &'a StageInstanceId,
    pub event_id: &'a EventId,
    pub message_id: &'a MessageId,
    pub authenticated_principal: &'a ParticipantPrincipalId,
    pub command: &'a AdversarialReviewCommand,
    pub candidate_bytes: Option<&'a [u8]>,
    pub candidate_evidence: Option<ReviewCandidateEvidence<'a>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendAdversarialReviewOutcome {
    Committed,
    Duplicate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CanonicalCommittedAdversarialReviewEvent {
    event_id: EventId,
    run_id: RunId,
    sequence: u64,
    message_id: MessageId,
    message_digest: Sha256Digest,
    stage_instance_id: StageInstanceId,
    event: AdversarialReviewEvent,
}

pub(crate) fn insert_adversarial_review_registration(
    transaction: &rusqlite::Transaction<'_>,
    run_id: &RunId,
    registration: &AdversarialReviewRegistration,
) -> Result<(), StoreError> {
    registration.validate()?;
    let value = serde_json::to_value(registration).map_err(StoreError::Serialization)?;
    let json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    let digest = Sha256Digest::of_bytes(&json);
    transaction
        .execute(
            "INSERT INTO adversarial_review_registrations(
                stage_instance_id, run_id, registration_digest, registration_json
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                registration.stage_instance_id.as_str(),
                run_id.as_str(),
                digest.to_prefixed_string(),
                json,
            ],
        )
        .map_err(StoreError::Sqlite)?;
    Ok(())
}

pub(crate) fn bind_scheduled_adversarial_review(
    transaction: &rusqlite::Transaction<'_>,
    run_id: &RunId,
    stage_instance_id: &StageInstanceId,
    input_manifest_digest: Sha256Digest,
) -> Result<(), StoreError> {
    let Some(registration) = load_registration(transaction, run_id, stage_instance_id)? else {
        return Ok(());
    };
    let state = AdversarialReviewState::new(
        stage_instance_id.clone(),
        registration.implementer,
        registration.reviewers,
        input_manifest_digest,
        registration.baseline,
    )
    .map_err(StoreError::AdversarialReviewTransition)?;
    let json = serde_json::to_vec(&state).map_err(StoreError::Serialization)?;
    let inserted = transaction
        .execute(
            "INSERT INTO adversarial_review_snapshots(
                stage_instance_id, run_id, control_revision, review_state_revision,
                initial_state_json, state_json
             ) VALUES (?1, ?2, 0, 0, ?3, ?3)",
            params![stage_instance_id.as_str(), run_id.as_str(), json],
        )
        .map_err(StoreError::Sqlite)?;
    if inserted != 1 {
        return Err(StoreError::AdversarialReviewAlreadyExists);
    }
    Ok(())
}

pub(crate) fn load_registration(
    connection: &Connection,
    run_id: &RunId,
    stage_instance_id: &StageInstanceId,
) -> Result<Option<AdversarialReviewRegistration>, StoreError> {
    let row: Option<(String, Vec<u8>)> = connection
        .query_row(
            "SELECT registration_digest, registration_json
             FROM adversarial_review_registrations
             WHERE run_id = ?1 AND stage_instance_id = ?2",
            params![run_id.as_str(), stage_instance_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    row.map(|(digest, json)| {
        let digest: Sha256Digest = digest.parse().map_err(StoreError::Digest)?;
        let registration: AdversarialReviewRegistration =
            serde_json::from_slice(&json).map_err(StoreError::Serialization)?;
        let value = serde_json::to_value(&registration).map_err(StoreError::Serialization)?;
        let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
        if canonical != json
            || Sha256Digest::of_bytes(&canonical) != digest
            || registration.stage_instance_id != *stage_instance_id
        {
            return Err(StoreError::CorruptData(
                "adversarial review registration failed integrity verification",
            ));
        }
        registration.validate()?;
        Ok(registration)
    })
    .transpose()
}

#[derive(Serialize)]
struct ReviewEvidenceCommitment<'a> {
    object_manifest_artifact_id: &'a ArtifactId,
    validation_artifact_id: &'a ArtifactId,
    check_result_artifact_id: &'a ArtifactId,
}

#[derive(Serialize)]
struct ReviewMessageCommitment<'a> {
    run_id: &'a RunId,
    stage_instance_id: &'a StageInstanceId,
    message_id: &'a MessageId,
    authenticated_principal: &'a ParticipantPrincipalId,
    command: &'a AdversarialReviewCommand,
    candidate_bytes_digest: Option<Sha256Digest>,
    evidence: Option<ReviewEvidenceCommitment<'a>>,
}

fn submitted_candidate_digest(
    command: &AdversarialReviewCommand,
) -> Result<Option<Sha256Digest>, StoreError> {
    let AdversarialReviewCommand::AcceptCandidate {
        object,
        evidence_commitment_digest,
        candidate_manifest_digest,
        validation_digest,
        check_result_digest,
        ..
    } = command
    else {
        return Ok(None);
    };
    ReviewCandidateArtifact {
        object: object.clone(),
        evidence_commitment_digest: *evidence_commitment_digest,
        candidate_manifest_digest: *candidate_manifest_digest,
        validation_digest: *validation_digest,
        check_result_digest: *check_result_digest,
    }
    .canonical_bytes()
    .map(|bytes| Some(Sha256Digest::of_bytes(&bytes)))
    .map_err(StoreError::AdversarialReviewTransition)
}

fn review_message_digest(
    run_id: &RunId,
    stage_instance_id: &StageInstanceId,
    message_id: &MessageId,
    authenticated_principal: &ParticipantPrincipalId,
    command: &AdversarialReviewCommand,
    candidate_bytes_digest: Option<Sha256Digest>,
    candidate_evidence: Option<&ReviewCandidateEvidence<'_>>,
) -> Result<Sha256Digest, StoreError> {
    let value = serde_json::to_value(ReviewMessageCommitment {
        run_id,
        stage_instance_id,
        message_id,
        authenticated_principal,
        command,
        candidate_bytes_digest,
        evidence: candidate_evidence.map(|evidence| ReviewEvidenceCommitment {
            object_manifest_artifact_id: evidence.object_manifest_artifact_id,
            validation_artifact_id: evidence.validation_artifact_id,
            check_result_artifact_id: evidence.check_result_artifact_id,
        }),
    })
    .map_err(StoreError::Serialization)?;
    let bytes = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    Ok(Sha256Digest::of_bytes(&bytes))
}

fn prepare_submitted_candidate(
    artifact_store: &ArtifactStore,
    submission: &AdversarialReviewSubmission<'_>,
) -> Result<Option<Sha256Digest>, StoreError> {
    let Some(bytes) = submission.candidate_bytes else {
        return Ok(None);
    };
    let AdversarialReviewCommand::AcceptCandidate {
        object,
        evidence_commitment_digest,
        candidate_manifest_digest,
        validation_digest,
        check_result_digest,
        ..
    } = submission.command
    else {
        return Err(StoreError::AdversarialReviewCandidateMismatch);
    };
    let candidate = ReviewCandidateArtifact {
        object: object.clone(),
        evidence_commitment_digest: *evidence_commitment_digest,
        candidate_manifest_digest: *candidate_manifest_digest,
        validation_digest: *validation_digest,
        check_result_digest: *check_result_digest,
    };
    if candidate
        .canonical_bytes()
        .map_err(StoreError::AdversarialReviewTransition)?
        != bytes
    {
        return Err(StoreError::AdversarialReviewCandidateMismatch);
    }
    artifact_store
        .put(bytes)
        .map(|stored| Some(stored.sha256))
        .map_err(StoreError::ArtifactStore)
}

impl SqliteStore {
    #[cfg(test)]
    pub fn register_adversarial_review(
        &mut self,
        run_id: &RunId,
        state: &AdversarialReviewState,
    ) -> Result<(), StoreError> {
        state
            .validate_pristine()
            .map_err(|_| StoreError::InvalidInitialAdversarialReview)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        crate::require_run(&transaction, run_id)?;
        verify_run_journal(&transaction, run_id)?;
        let stage_exists = transaction
            .query_row(
                "SELECT 1 FROM stage_snapshots
                 WHERE run_id = ?1 AND stage_instance_id = ?2",
                params![run_id.as_str(), state.stage_instance_id.as_str()],
                |_| Ok(true),
            )
            .optional()
            .map_err(StoreError::Sqlite)?
            .unwrap_or_default();
        if !stage_exists {
            return Err(StoreError::StageNotFound);
        }
        let json = serde_json::to_vec(state).map_err(StoreError::Serialization)?;
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO adversarial_review_snapshots(
                    stage_instance_id, run_id, control_revision, review_state_revision,
                    initial_state_json, state_json
                 ) VALUES (?1, ?2, 0, 0, ?3, ?3)",
                params![state.stage_instance_id.as_str(), run_id.as_str(), json],
            )
            .map_err(StoreError::Sqlite)?;
        if inserted != 1 {
            return Err(StoreError::AdversarialReviewAlreadyExists);
        }
        transaction.commit().map_err(StoreError::Sqlite)
    }

    #[cfg(test)]
    pub fn submit_adversarial_review_command(
        &mut self,
        artifact_store: &ArtifactStore,
        git_repository: &GitRepository,
        submission: AdversarialReviewSubmission<'_>,
    ) -> Result<AppendAdversarialReviewOutcome, StoreError> {
        let prepared_candidate = prepare_submitted_candidate(artifact_store, &submission)?;
        self.submit_adversarial_review_command_inner(
            artifact_store,
            Some(git_repository),
            submission,
            true,
            prepared_candidate,
            None,
        )
    }

    #[cfg(test)]
    fn submit_unbound_review_command_for_journal_test(
        &mut self,
        artifact_store: &ArtifactStore,
        submission: AdversarialReviewSubmission<'_>,
    ) -> Result<AppendAdversarialReviewOutcome, StoreError> {
        self.submit_adversarial_review_command_inner(
            artifact_store,
            None,
            submission,
            false,
            None,
            None,
        )
    }

    fn submit_adversarial_review_command_inner(
        &mut self,
        artifact_store: &ArtifactStore,
        git_repository: Option<&GitRepository>,
        submission: AdversarialReviewSubmission<'_>,
        validate_candidate: bool,
        prepared_candidate: Option<Sha256Digest>,
        lease: Option<(&RunLeaseFence, &dyn UnixMillisClock)>,
    ) -> Result<AppendAdversarialReviewOutcome, StoreError> {
        let AdversarialReviewSubmission {
            run_id,
            stage_instance_id,
            event_id,
            message_id,
            authenticated_principal,
            command,
            candidate_bytes: _,
            candidate_evidence,
        } = submission;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        if let Some((fence, clock)) = lease {
            lease_now(&transaction, fence, clock)?;
            if fence.run_id() != run_id {
                return Err(StoreError::RunLeaseRequired);
            }
        }
        verify_run_journal(&transaction, run_id)?;
        verify_adversarial_review_candidate_bytes(&transaction, run_id, artifact_store)?;
        let state = verified_adversarial_review(&transaction, run_id, stage_instance_id)?;
        let submitted_digest = submitted_candidate_digest(command)?;
        let retry_digest = review_message_digest(
            run_id,
            stage_instance_id,
            message_id,
            authenticated_principal,
            command,
            submitted_digest,
            candidate_evidence.as_ref(),
        )?;
        if let Some(existing) = review_event_by_message_id(&transaction, message_id)? {
            if existing.event_id == *event_id
                && existing.run_id == *run_id
                && existing.message_digest == retry_digest
                && existing.event.stage_instance_id == *stage_instance_id
            {
                transaction.commit().map_err(StoreError::Sqlite)?;
                return Ok(AppendAdversarialReviewOutcome::Duplicate);
            }
            return Err(StoreError::MessageIdConflict);
        }
        let mut candidate_bytes_digest = None;
        if validate_candidate {
            if let AdversarialReviewCommand::AcceptCandidate {
                object,
                evidence_commitment_digest,
                candidate_manifest_digest,
                validation_digest,
                check_result_digest,
                ..
            } = command
            {
                let submitted = ReviewCandidateArtifact {
                    object: object.clone(),
                    evidence_commitment_digest: *evidence_commitment_digest,
                    candidate_manifest_digest: *candidate_manifest_digest,
                    validation_digest: *validation_digest,
                    check_result_digest: *check_result_digest,
                };
                let git_repository =
                    git_repository.ok_or(StoreError::AdversarialReviewCandidateMismatch)?;
                let evidence = candidate_evidence
                    .as_ref()
                    .ok_or(StoreError::AdversarialReviewCandidateMismatch)?;
                git_repository
                    .verify_commit(object)
                    .map_err(StoreError::AdversarialReviewGit)?;
                let prior_object = state
                    .candidate
                    .as_ref()
                    .map_or(&state.baseline, |candidate| &candidate.object);
                if !git_repository
                    .is_ancestor(prior_object, object)
                    .map_err(StoreError::AdversarialReviewGit)?
                {
                    return Err(StoreError::AdversarialReviewCandidateMismatch);
                }
                validate_candidate_evidence(
                    &transaction,
                    artifact_store,
                    CandidateEvidenceValidation {
                        run_id,
                        review_stage_instance_id: stage_instance_id,
                        candidate: &submitted,
                        prior_object,
                        evidence,
                        require_current_validity: true,
                    },
                )?;
                if state.candidate.is_none() {
                    validate_scheduled_candidate(
                        &transaction,
                        artifact_store,
                        run_id,
                        &state,
                        &submitted,
                    )?;
                }
                let digest =
                    prepared_candidate.ok_or(StoreError::AdversarialReviewCandidateMismatch)?;
                let canonical = submitted
                    .canonical_bytes()
                    .map_err(StoreError::AdversarialReviewTransition)?;
                if Sha256Digest::of_bytes(&canonical) != digest {
                    return Err(StoreError::AdversarialReviewCandidateMismatch);
                }
                candidate_bytes_digest = Some(digest);
            }
        }
        #[cfg(test)]
        if !validate_candidate {
            if let AdversarialReviewCommand::AcceptCandidate {
                object,
                evidence_commitment_digest,
                candidate_manifest_digest,
                validation_digest,
                check_result_digest,
                ..
            } = command
            {
                let bytes = ReviewCandidateArtifact {
                    object: object.clone(),
                    evidence_commitment_digest: *evidence_commitment_digest,
                    candidate_manifest_digest: *candidate_manifest_digest,
                    validation_digest: *validation_digest,
                    check_result_digest: *check_result_digest,
                }
                .canonical_bytes()
                .map_err(StoreError::AdversarialReviewTransition)?;
                candidate_bytes_digest = Some(
                    artifact_store
                        .put(&bytes)
                        .map_err(StoreError::ArtifactStore)?
                        .sha256,
                );
            }
        }
        match command {
            AdversarialReviewCommand::AcceptCandidate { .. }
                if authenticated_principal != &state.implementer =>
            {
                return Err(StoreError::AdversarialReviewAuthorityMismatch);
            }
            AdversarialReviewCommand::SubmitReview { reviewer, .. }
                if authenticated_principal != reviewer =>
            {
                return Err(StoreError::AdversarialReviewAuthorityMismatch);
            }
            AdversarialReviewCommand::AuthorizeHumanOverride { .. } => {
                return Err(StoreError::AdversarialReviewAuthorityMismatch);
            }
            _ => {}
        }
        let message_digest = review_message_digest(
            run_id,
            stage_instance_id,
            message_id,
            authenticated_principal,
            command,
            candidate_bytes_digest,
            candidate_evidence.as_ref(),
        )?;
        if review_message_id_exists(&transaction, message_id)? {
            return Err(StoreError::MessageIdConflict);
        }
        if event_id_exists(&transaction, event_id)? {
            return Err(StoreError::EventIdConflict);
        }
        let event = state
            .decide(command.clone())
            .map_err(StoreError::AdversarialReviewTransition)?;
        let next = state
            .apply(&event)
            .map_err(StoreError::AdversarialReviewTransition)?;
        let sequence: i64 = transaction
            .query_row(
                "SELECT next_event_sequence FROM runs WHERE run_id = ?1",
                params![run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)?;
        let sequence = from_sql_integer(sequence)?;
        if sequence >= MAX_CONTROL_REVISION {
            return Err(StoreError::EventSequenceExhausted);
        }
        let record = CanonicalCommittedAdversarialReviewEvent {
            event_id: event_id.clone(),
            run_id: run_id.clone(),
            sequence,
            message_id: message_id.clone(),
            message_digest,
            stage_instance_id: event.stage_instance_id.clone(),
            event: event.clone(),
        };
        let value = serde_json::to_value(&record).map_err(StoreError::Serialization)?;
        let json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
        let digest = Sha256Digest::of_bytes(&json);
        transaction
            .execute(
                "INSERT INTO adversarial_review_events(
                    event_id, run_id, sequence, message_id, message_digest,
                    stage_instance_id, event_digest, event_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    event_id.as_str(),
                    run_id.as_str(),
                    to_sql_integer(sequence)?,
                    message_id.as_str(),
                    message_digest.to_prefixed_string(),
                    event.stage_instance_id.as_str(),
                    digest.to_prefixed_string(),
                    json,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if let Some(bytes_digest) = candidate_bytes_digest {
            let parent_bytes_digest: Option<String> = transaction
                .query_row(
                    "SELECT bytes_digest FROM adversarial_review_candidates
                     WHERE run_id = ?1 AND stage_instance_id = ?2
                     ORDER BY epoch DESC LIMIT 1",
                    params![run_id.as_str(), stage_instance_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(StoreError::Sqlite)?;
            let epoch = next
                .candidate
                .as_ref()
                .ok_or(StoreError::AdversarialReviewCandidateMismatch)?
                .epoch;
            transaction
                .execute(
                    "INSERT INTO adversarial_review_candidates(
                        message_id, run_id, stage_instance_id, epoch,
                        bytes_digest, parent_bytes_digest,
                        object_manifest_artifact_id, validation_artifact_id,
                        check_result_artifact_id
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        message_id.as_str(),
                        run_id.as_str(),
                        stage_instance_id.as_str(),
                        i64::from(epoch),
                        bytes_digest.to_prefixed_string(),
                        parent_bytes_digest,
                        candidate_evidence
                            .as_ref()
                            .map(|evidence| evidence.object_manifest_artifact_id.as_str()),
                        candidate_evidence
                            .as_ref()
                            .map(|evidence| evidence.validation_artifact_id.as_str()),
                        candidate_evidence
                            .as_ref()
                            .map(|evidence| evidence.check_result_artifact_id.as_str()),
                    ],
                )
                .map_err(StoreError::Sqlite)?;
        }
        #[cfg(test)]
        if self.fail_after_event_insert {
            return Err(StoreError::CorruptData(
                "injected adversarial review journal failure",
            ));
        }
        let next_json = serde_json::to_vec(&next).map_err(StoreError::Serialization)?;
        let updated = transaction
            .execute(
                "UPDATE adversarial_review_snapshots
                 SET control_revision = ?1, review_state_revision = ?2, state_json = ?3
                 WHERE run_id = ?4 AND stage_instance_id = ?5
                   AND control_revision = ?6 AND review_state_revision = ?7",
                params![
                    to_sql_integer(next.control_revision)?,
                    to_sql_integer(next.review_state_revision)?,
                    next_json,
                    run_id.as_str(),
                    next.stage_instance_id.as_str(),
                    to_sql_integer(state.control_revision)?,
                    to_sql_integer(state.review_state_revision)?,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if updated != 1 {
            return Err(StoreError::ConcurrentUpdate);
        }
        let advanced = transaction
            .execute(
                "UPDATE runs SET next_event_sequence = ?1
                 WHERE run_id = ?2 AND next_event_sequence = ?3",
                params![
                    to_sql_integer(sequence + 1)?,
                    run_id.as_str(),
                    to_sql_integer(sequence)?,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if advanced != 1 {
            return Err(StoreError::ConcurrentUpdate);
        }
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(AppendAdversarialReviewOutcome::Committed)
    }

    pub fn load_adversarial_review(
        &self,
        run_id: &RunId,
        stage_instance_id: &StageInstanceId,
    ) -> Result<AdversarialReviewState, StoreError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(StoreError::Sqlite)?;
        verify_run_journal(&transaction, run_id)?;
        let state = verified_adversarial_review(&transaction, run_id, stage_instance_id)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(state)
    }
}

impl LeasedRun<'_, '_> {
    pub fn submit_adversarial_review_command(
        &mut self,
        artifact_store: &ArtifactStore,
        git_repository: &GitRepository,
        submission: AdversarialReviewSubmission<'_>,
    ) -> Result<AppendAdversarialReviewOutcome, StoreError> {
        let prepared_candidate = prepare_submitted_candidate(artifact_store, &submission)?;
        self.store.submit_adversarial_review_command_inner(
            artifact_store,
            Some(git_repository),
            submission,
            true,
            prepared_candidate,
            Some((&self.fence, self.clock)),
        )
    }
}

struct CandidateEvidenceValidation<'a> {
    run_id: &'a RunId,
    review_stage_instance_id: &'a StageInstanceId,
    candidate: &'a ReviewCandidateArtifact,
    prior_object: &'a herdr_flow_core::GitObjectId,
    evidence: &'a ReviewCandidateEvidence<'a>,
    require_current_validity: bool,
}

fn validate_candidate_evidence(
    connection: &Connection,
    artifact_store: &ArtifactStore,
    validation: CandidateEvidenceValidation<'_>,
) -> Result<(), StoreError> {
    let CandidateEvidenceValidation {
        run_id,
        review_stage_instance_id,
        candidate,
        prior_object,
        evidence,
        require_current_validity,
    } = validation;
    let pipeline = crate::pipeline::verified_pipeline(connection, run_id)?;
    let registration = load_registration(connection, run_id, review_stage_instance_id)?
        .ok_or(StoreError::AdversarialReviewCandidateMismatch)?;
    let review_state = verified_adversarial_review(connection, run_id, review_stage_instance_id)?;
    let object_record =
        registry::load_artifact_record(connection, run_id, evidence.object_manifest_artifact_id)?
            .ok_or(StoreError::AdversarialReviewCandidateMismatch)?;
    let validation_record =
        registry::load_artifact_record(connection, run_id, evidence.validation_artifact_id)?
            .ok_or(StoreError::AdversarialReviewCandidateMismatch)?;
    let check_record =
        registry::load_artifact_record(connection, run_id, evidence.check_result_artifact_id)?
            .ok_or(StoreError::AdversarialReviewCandidateMismatch)?;
    if (require_current_validity
        && (!pipeline.artifact_is_valid(evidence.object_manifest_artifact_id)
            || !pipeline.artifact_is_valid(evidence.validation_artifact_id)
            || !pipeline.artifact_is_valid(evidence.check_result_artifact_id)))
        || object_record.record.artifact_type != "candidate-object-manifest/v1"
        || validation_record.record.artifact_type != "candidate-validation/v1"
        || check_record.record.artifact_type != "candidate-check-result/v1"
        || object_record.record.sha256 != candidate.candidate_manifest_digest
        || validation_record.record.sha256 != candidate.validation_digest
        || check_record.record.sha256 != candidate.check_result_digest
        || [&object_record, &validation_record, &check_record]
            .iter()
            .any(|record| {
                record.record.producer_stage_instance_id
                    != registration.evidence_producer_stage_instance_id
                    || record.record.component_digest != registration.evidence_component_digest
                    || record.record.input_manifest_digest != review_state.input_manifest_digest
                    || record.record.producer_attempt == 0
                    || record.record.producer_attempt != check_record.record.producer_attempt
                    || record.record.producer_event_sequence
                        != check_record.record.producer_event_sequence
            })
        || !artifact_edge_exists(
            connection,
            run_id,
            evidence.object_manifest_artifact_id,
            evidence.validation_artifact_id,
        )?
        || !artifact_edge_exists(
            connection,
            run_id,
            evidence.object_manifest_artifact_id,
            evidence.check_result_artifact_id,
        )?
    {
        return Err(StoreError::AdversarialReviewCandidateMismatch);
    }
    #[derive(Serialize)]
    struct CandidateEvidenceSubject<'a> {
        object_manifest_artifact_id: &'a ArtifactId,
        object_manifest_record_digest: Sha256Digest,
        validation_artifact_id: &'a ArtifactId,
        validation_record_digest: Sha256Digest,
        check_result_artifact_id: &'a ArtifactId,
        check_result_record_digest: Sha256Digest,
        producer_attempt: u32,
        input_manifest_digest: Sha256Digest,
        check_policy_digest: Sha256Digest,
    }
    let evidence_subject = serde_json::to_value(CandidateEvidenceSubject {
        object_manifest_artifact_id: evidence.object_manifest_artifact_id,
        object_manifest_record_digest: object_record.record_digest,
        validation_artifact_id: evidence.validation_artifact_id,
        validation_record_digest: validation_record.record_digest,
        check_result_artifact_id: evidence.check_result_artifact_id,
        check_result_record_digest: check_record.record_digest,
        producer_attempt: check_record.record.producer_attempt,
        input_manifest_digest: review_state.input_manifest_digest,
        check_policy_digest: registration.check_policy_digest,
    })
    .map_err(StoreError::Serialization)?;
    let evidence_subject =
        canonical_json::to_vec(&evidence_subject).map_err(StoreError::Canonicalization)?;
    if Sha256Digest::of_bytes(&evidence_subject) != candidate.evidence_commitment_digest {
        return Err(StoreError::AdversarialReviewCandidateMismatch);
    }
    let expected_object = ReviewCandidateObjectManifest {
        object: candidate.object.clone(),
    };
    let expected_validation = ReviewCandidateValidation {
        object: candidate.object.clone(),
        prior_object: prior_object.clone(),
        candidate_manifest_digest: candidate.candidate_manifest_digest,
        valid: true,
    };
    let expected_check = ReviewCandidateCheckResult {
        object: candidate.object.clone(),
        candidate_manifest_digest: candidate.candidate_manifest_digest,
        check_policy_digest: registration.check_policy_digest,
        passed: true,
    };
    for (digest, expected) in [
        (
            object_record.record.sha256,
            serde_json::to_value(expected_object).map_err(StoreError::Serialization)?,
        ),
        (
            validation_record.record.sha256,
            serde_json::to_value(expected_validation).map_err(StoreError::Serialization)?,
        ),
        (
            check_record.record.sha256,
            serde_json::to_value(expected_check).map_err(StoreError::Serialization)?,
        ),
    ] {
        let expected = canonical_json::to_vec(&expected).map_err(StoreError::Canonicalization)?;
        if artifact_store
            .read_verified(digest)
            .map_err(StoreError::ArtifactStore)?
            != expected
            || Sha256Digest::of_bytes(&expected) != digest
        {
            return Err(StoreError::AdversarialReviewCandidateMismatch);
        }
    }
    Ok(())
}

fn artifact_edge_exists(
    connection: &Connection,
    run_id: &RunId,
    parent: &ArtifactId,
    child: &ArtifactId,
) -> Result<bool, StoreError> {
    connection
        .query_row(
            "SELECT 1 FROM artifact_edges
             WHERE run_id = ?1 AND parent_artifact_id = ?2 AND child_artifact_id = ?3",
            params![run_id.as_str(), parent.as_str(), child.as_str()],
            |_| Ok(true),
        )
        .optional()
        .map(Option::unwrap_or_default)
        .map_err(StoreError::Sqlite)
}

fn validate_scheduled_candidate(
    connection: &Connection,
    artifact_store: &ArtifactStore,
    run_id: &RunId,
    review: &AdversarialReviewState,
    submitted: &ReviewCandidateArtifact,
) -> Result<(), StoreError> {
    let pipeline = crate::pipeline::verified_pipeline(connection, run_id)?;
    let node = pipeline
        .node_definition(&review.stage_instance_id)
        .ok_or(StoreError::AdversarialReviewCandidateMismatch)?;
    if node.needs.len() != 1 || node.required_input_artifact_ids.len() != 1 {
        return Err(StoreError::AdversarialReviewCandidateMismatch);
    }
    let artifact_id = &node.required_input_artifact_ids[0];
    if !pipeline.artifact_is_valid(artifact_id) {
        return Err(StoreError::AdversarialReviewCandidateMismatch);
    }
    let stored = registry::load_artifact_record(connection, run_id, artifact_id)?
        .ok_or(StoreError::AdversarialReviewCandidateMismatch)?;
    if stored.record.artifact_type != "candidate-commit/v1"
        || stored.record.schema_id != "candidate-commit"
        || stored.record.schema_version != 1
        || stored.record.producer_stage_instance_id != node.needs[0]
    {
        return Err(StoreError::AdversarialReviewCandidateMismatch);
    }
    let input_manifest = StageInputManifest {
        protocol: BASE_PROTOCOL.to_owned(),
        stage_instance_id: review.stage_instance_id.clone(),
        artifacts: vec![InputManifestArtifact {
            artifact_id: artifact_id.clone(),
            sha256: stored.record.sha256,
        }],
    };
    let stage = verified_stage(connection, run_id, &review.stage_instance_id)?;
    if stage.input_manifest_digest != Some(review.input_manifest_digest)
        || input_manifest
            .digest()
            .map_err(StoreError::Canonicalization)?
            != review.input_manifest_digest
    {
        return Err(StoreError::AdversarialReviewCandidateMismatch);
    }
    let bytes = artifact_store
        .read_verified(stored.record.sha256)
        .map_err(StoreError::ArtifactStore)?;
    let decoded: ImplementationCandidateArtifact =
        serde_json::from_slice(&bytes).map_err(StoreError::Serialization)?;
    let canonical = decoded
        .canonical_bytes()
        .map_err(StoreError::AdversarialReviewTransition)?;
    if canonical != bytes
        || decoded.object != submitted.object
        || decoded.candidate_manifest_digest != submitted.candidate_manifest_digest
        || Sha256Digest::of_bytes(&bytes) != stored.record.sha256
    {
        return Err(StoreError::AdversarialReviewCandidateMismatch);
    }
    Ok(())
}

pub(crate) fn verify_adversarial_review_candidate_bytes(
    connection: &Connection,
    run_id: &RunId,
    artifact_store: &ArtifactStore,
) -> Result<(), StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT message_id, stage_instance_id, epoch, bytes_digest, parent_bytes_digest,
                    object_manifest_artifact_id, validation_artifact_id,
                    check_result_artifact_id
             FROM adversarial_review_candidates WHERE run_id = ?1
             ORDER BY stage_instance_id, epoch",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map(params![run_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })
        .map_err(StoreError::Sqlite)?;
    let mut previous: Option<(
        StageInstanceId,
        u64,
        Sha256Digest,
        herdr_flow_core::GitObjectId,
    )> = None;
    for row in rows {
        let (
            message_id,
            stage_id,
            epoch,
            bytes_digest,
            parent_digest,
            object_manifest_id,
            validation_id,
            check_id,
        ) = row.map_err(StoreError::Sqlite)?;
        let message_id = MessageId::from_str(&message_id).map_err(StoreError::Identifier)?;
        let stage_id = StageInstanceId::from_str(&stage_id).map_err(StoreError::Identifier)?;
        let bytes_digest = Sha256Digest::from_str(&bytes_digest).map_err(StoreError::Digest)?;
        let parent_digest = parent_digest
            .map(|value| Sha256Digest::from_str(&value).map_err(StoreError::Digest))
            .transpose()?;
        let event = review_event_by_message_id(connection, &message_id)?
            .ok_or(StoreError::CorruptData("missing review candidate event"))?;
        let herdr_flow_core::AdversarialReviewEventKind::CandidateAccepted {
            prior_control_revision,
            object,
            evidence_commitment_digest,
            candidate_manifest_digest,
            validation_digest,
            check_result_digest,
            dispositions,
        } = event.event.kind.clone()
        else {
            return Err(StoreError::CorruptData(
                "review candidate row references a non-candidate event",
            ));
        };
        let candidate = ReviewCandidateArtifact {
            object: object.clone(),
            evidence_commitment_digest,
            candidate_manifest_digest,
            validation_digest,
            check_result_digest,
        };
        let canonical = candidate
            .canonical_bytes()
            .map_err(StoreError::AdversarialReviewTransition)?;
        let registration = load_registration(connection, run_id, &stage_id)?;
        let (expected_epoch, expected_parent, prior_object) = match &previous {
            Some((previous_stage, previous_epoch, digest, previous_object))
                if previous_stage == &stage_id =>
            {
                (previous_epoch + 1, Some(*digest), previous_object.clone())
            }
            _ => (
                1,
                None,
                registration
                    .as_ref()
                    .map(|registration| registration.baseline.clone())
                    .unwrap_or_else(|| object.clone()),
            ),
        };
        if event.run_id != *run_id
            || event.event.stage_instance_id != stage_id
            || u64::try_from(epoch).ok() != Some(expected_epoch)
            || Sha256Digest::of_bytes(&canonical) != bytes_digest
            || parent_digest != expected_parent
            || artifact_store
                .read_verified(bytes_digest)
                .map_err(StoreError::ArtifactStore)?
                != canonical
        {
            return Err(StoreError::CorruptData(
                "review candidate artifact failed integrity verification",
            ));
        }
        match (object_manifest_id, validation_id, check_id, registration) {
            (Some(object_manifest_id), Some(validation_id), Some(check_id), Some(registration)) => {
                let object_manifest_id =
                    ArtifactId::from_str(&object_manifest_id).map_err(StoreError::Identifier)?;
                let validation_id =
                    ArtifactId::from_str(&validation_id).map_err(StoreError::Identifier)?;
                let check_id = ArtifactId::from_str(&check_id).map_err(StoreError::Identifier)?;
                let evidence = ReviewCandidateEvidence {
                    object_manifest_artifact_id: &object_manifest_id,
                    validation_artifact_id: &validation_id,
                    check_result_artifact_id: &check_id,
                };
                validate_candidate_evidence(
                    connection,
                    artifact_store,
                    CandidateEvidenceValidation {
                        run_id,
                        review_stage_instance_id: &stage_id,
                        candidate: &candidate,
                        prior_object: &prior_object,
                        evidence: &evidence,
                        require_current_validity: false,
                    },
                )?;
                let command = AdversarialReviewCommand::AcceptCandidate {
                    expected_control_revision: prior_control_revision,
                    object: candidate.object.clone(),
                    evidence_commitment_digest: candidate.evidence_commitment_digest,
                    candidate_manifest_digest: candidate.candidate_manifest_digest,
                    validation_digest: candidate.validation_digest,
                    check_result_digest: candidate.check_result_digest,
                    dispositions,
                };
                let expected_message_digest = review_message_digest(
                    run_id,
                    &stage_id,
                    &message_id,
                    &registration.implementer,
                    &command,
                    Some(bytes_digest),
                    Some(&evidence),
                )?;
                if event.message_digest != expected_message_digest {
                    return Err(StoreError::CorruptData(
                        "review candidate message commitment failed recovery verification",
                    ));
                }
            }
            #[cfg(test)]
            (None, None, None, None) => {}
            _ => {
                return Err(StoreError::CorruptData(
                    "review candidate evidence commitment is incomplete",
                ));
            }
        }
        previous = Some((stage_id, expected_epoch, bytes_digest, object));
    }
    Ok(())
}

pub(crate) fn verify_adversarial_review_journal(
    connection: &Connection,
    run_id: &RunId,
) -> Result<Vec<u64>, StoreError> {
    let mut registrations = connection
        .prepare(
            "SELECT stage_instance_id FROM adversarial_review_registrations
             WHERE run_id = ?1 ORDER BY stage_instance_id",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = registrations
        .query_map(params![run_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(StoreError::Sqlite)?;
    for row in rows {
        let stage_id = StageInstanceId::from_str(&row.map_err(StoreError::Sqlite)?)
            .map_err(StoreError::Identifier)?;
        load_registration(connection, run_id, &stage_id)?
            .ok_or(StoreError::InvalidInitialAdversarialReview)?;
    }
    drop(registrations);
    let mut snapshots = connection
        .prepare(
            "SELECT stage_instance_id FROM adversarial_review_snapshots
             WHERE run_id = ?1 ORDER BY stage_instance_id",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = snapshots
        .query_map(params![run_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(StoreError::Sqlite)?;
    for row in rows {
        let stage_id = StageInstanceId::from_str(&row.map_err(StoreError::Sqlite)?)
            .map_err(StoreError::Identifier)?;
        verified_adversarial_review(connection, run_id, &stage_id)?;
    }
    drop(snapshots);
    let mut statement = connection
        .prepare(
            "SELECT message_id FROM adversarial_review_events
             WHERE run_id = ?1 ORDER BY sequence",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map(params![run_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(StoreError::Sqlite)?;
    let mut sequences = Vec::new();
    for row in rows {
        let message_id = MessageId::from_str(&row.map_err(StoreError::Sqlite)?)
            .map_err(StoreError::Identifier)?;
        let event = review_event_by_message_id(connection, &message_id)?
            .ok_or(StoreError::CorruptData("missing adversarial review event"))?;
        if matches!(
            event.event.kind,
            herdr_flow_core::AdversarialReviewEventKind::CandidateAccepted { .. }
        ) {
            let candidate_exists = connection
                .query_row(
                    "SELECT 1 FROM adversarial_review_candidates WHERE message_id = ?1",
                    params![message_id.as_str()],
                    |_| Ok(true),
                )
                .optional()
                .map_err(StoreError::Sqlite)?
                .unwrap_or_default();
            if !candidate_exists {
                return Err(StoreError::CorruptData(
                    "candidate event is missing its immutable candidate artifact",
                ));
            }
        }
        sequences.push(event.sequence);
    }
    Ok(sequences)
}

pub(crate) fn verified_adversarial_review(
    connection: &Connection,
    run_id: &RunId,
    stage_instance_id: &StageInstanceId,
) -> Result<AdversarialReviewState, StoreError> {
    let row: Option<(i64, i64, Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT control_revision, review_state_revision, initial_state_json, state_json
             FROM adversarial_review_snapshots
             WHERE run_id = ?1 AND stage_instance_id = ?2",
            params![run_id.as_str(), stage_instance_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let (control_revision, review_state_revision, initial_json, state_json) =
        row.ok_or(StoreError::AdversarialReviewNotFound)?;
    let initial: AdversarialReviewState =
        serde_json::from_slice(&initial_json).map_err(StoreError::Serialization)?;
    let stored: AdversarialReviewState =
        serde_json::from_slice(&state_json).map_err(StoreError::Serialization)?;
    if initial.stage_instance_id != *stage_instance_id
        || stored.stage_instance_id != *stage_instance_id
        || stored.control_revision != from_sql_integer(control_revision)?
        || stored.review_state_revision != from_sql_integer(review_state_revision)?
    {
        return Err(StoreError::CorruptData(
            "adversarial review snapshot identity mismatch",
        ));
    }
    let mut statement = connection
        .prepare(
            "SELECT message_id FROM adversarial_review_events
             WHERE run_id = ?1 AND stage_instance_id = ?2 ORDER BY sequence",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map(
            params![run_id.as_str(), stage_instance_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .map_err(StoreError::Sqlite)?;
    let mut events = Vec::new();
    for row in rows {
        let message_id = MessageId::from_str(&row.map_err(StoreError::Sqlite)?)
            .map_err(StoreError::Identifier)?;
        events.push(
            review_event_by_message_id(connection, &message_id)?
                .ok_or(StoreError::CorruptData("missing adversarial review event"))?
                .event,
        );
    }
    let replayed = replay_adversarial_review(initial, &events)
        .map_err(StoreError::AdversarialReviewTransition)?;
    if replayed != stored {
        return Err(StoreError::CorruptData(
            "adversarial review snapshot does not match replay",
        ));
    }
    Ok(stored)
}

fn review_event_by_message_id(
    connection: &Connection,
    message_id: &MessageId,
) -> Result<Option<StoredAdversarialReviewEvent>, StoreError> {
    let row: Option<AdversarialReviewEventRow> = connection
        .query_row(
            "SELECT event_id, run_id, sequence, message_digest, event_digest,
                    stage_instance_id, event_json
             FROM adversarial_review_events WHERE message_id = ?1",
            params![message_id.as_str()],
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
    row.map(
        |(event_id, run_id, sequence, message_digest, event_digest, stage_id, json)| {
            let event_id = EventId::from_str(&event_id).map_err(StoreError::Identifier)?;
            let run_id = RunId::from_str(&run_id).map_err(StoreError::Identifier)?;
            let message_digest =
                Sha256Digest::from_str(&message_digest).map_err(StoreError::Digest)?;
            let event_digest = Sha256Digest::from_str(&event_digest).map_err(StoreError::Digest)?;
            let stage_id = StageInstanceId::from_str(&stage_id).map_err(StoreError::Identifier)?;
            let record: CanonicalCommittedAdversarialReviewEvent =
                serde_json::from_slice(&json).map_err(StoreError::Serialization)?;
            let value = serde_json::to_value(&record).map_err(StoreError::Serialization)?;
            let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
            if canonical != json
                || Sha256Digest::of_bytes(&canonical) != event_digest
                || record.event_id != event_id
                || record.run_id != run_id
                || record.sequence != from_sql_integer(sequence)?
                || record.message_id != *message_id
                || record.message_digest != message_digest
                || record.stage_instance_id != stage_id
                || record.event.stage_instance_id != stage_id
            {
                return Err(StoreError::CorruptData(
                    "adversarial review event failed integrity verification",
                ));
            }
            Ok(StoredAdversarialReviewEvent {
                event_id,
                run_id,
                sequence: record.sequence,
                message_id: message_id.clone(),
                message_digest,
                event_digest,
                event: record.event,
            })
        },
    )
    .transpose()
}

pub(crate) fn review_message_id_exists(
    connection: &Connection,
    message_id: &MessageId,
) -> Result<bool, StoreError> {
    connection
        .query_row(
            "SELECT 1 FROM (
                SELECT message_id FROM events WHERE message_id = ?1
                UNION ALL SELECT message_id FROM pipeline_events WHERE message_id = ?1
                UNION ALL SELECT message_id FROM publication_gate_events WHERE message_id = ?1
                UNION ALL SELECT message_id FROM adversarial_review_events WHERE message_id = ?1
             ) LIMIT 1",
            params![message_id.as_str()],
            |_| Ok(true),
        )
        .optional()
        .map(Option::unwrap_or_default)
        .map_err(StoreError::Sqlite)
}

#[cfg(test)]
mod tests {
    use herdr_flow_core::{
        AdversarialReviewCommand, GitObjectFormat, GitObjectId, ParticipantPrincipalId,
        PipelineNodeDefinition, PipelineState, ReviewDecision, StageState,
    };

    use super::*;

    fn digest(value: &[u8]) -> Sha256Digest {
        Sha256Digest::of_bytes(value)
    }

    fn oid(value: char) -> GitObjectId {
        GitObjectId::from_hex(GitObjectFormat::Sha1, &value.to_string().repeat(40)).unwrap()
    }

    fn principal(value: &str) -> ParticipantPrincipalId {
        ParticipantPrincipalId::parse(format!("principal_{value}")).unwrap()
    }

    fn submission<'a>(
        run_id: &'a RunId,
        stage_instance_id: &'a StageInstanceId,
        event_id: &'a EventId,
        message_id: &'a MessageId,
        authenticated_principal: &'a ParticipantPrincipalId,
        command: &'a AdversarialReviewCommand,
    ) -> AdversarialReviewSubmission<'a> {
        AdversarialReviewSubmission {
            run_id,
            stage_instance_id,
            event_id,
            message_id,
            authenticated_principal,
            command,
            candidate_bytes: None,
            candidate_evidence: None,
        }
    }

    #[test]
    fn review_journal_replays_exact_slots_across_restart_and_rejects_tampering() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("review.sqlite3");
        let artifact_store = ArtifactStore::open(directory.path().join("artifacts")).unwrap();
        let run_id = RunId::parse("flow_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let stage_id = StageInstanceId::parse("stage_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap();
        let stage = StageState::new(stage_id.clone(), digest(b"component"), digest(b"predicate"));
        let pipeline = PipelineState::new(
            digest(b"pipeline"),
            vec![PipelineNodeDefinition {
                stage,
                needs: vec![],
                required_input_artifact_ids: vec![],
            }],
        )
        .unwrap();
        let reviewer_a = principal("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let reviewer_b = principal("01ARZ3NDEKTSV4RRFFQ69G5FAY");
        let implementer = principal("01ARZ3NDEKTSV4RRFFQ69G5FAZ");
        let initial = AdversarialReviewState::new(
            stage_id.clone(),
            implementer.clone(),
            vec![reviewer_a.clone(), reviewer_b.clone()],
            digest(b"input"),
            oid('1'),
        )
        .unwrap();
        let mut store = SqliteStore::open(&database).unwrap();
        store
            .create_run(&run_id, &pipeline.definition_digest)
            .unwrap();
        store.register_pipeline(&run_id, &pipeline).unwrap();
        store
            .register_adversarial_review(&run_id, &initial)
            .unwrap();

        let accept_command = AdversarialReviewCommand::AcceptCandidate {
            expected_control_revision: 0,
            object: oid('2'),
            evidence_commitment_digest: digest(b"evidence"),
            candidate_manifest_digest: digest(b"candidate"),
            validation_digest: digest(b"validation"),
            check_result_digest: digest(b"checks"),
            dispositions: vec![],
        };
        let event_1 = EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB0").unwrap();
        let message_1 = MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FB1").unwrap();
        store.fail_after_event_insert = true;
        assert!(matches!(
            store.submit_unbound_review_command_for_journal_test(
                &artifact_store,
                submission(
                    &run_id,
                    &stage_id,
                    &event_1,
                    &message_1,
                    &implementer,
                    &accept_command
                ),
            ),
            Err(StoreError::CorruptData(
                "injected adversarial review journal failure"
            ))
        ));
        store.fail_after_event_insert = false;
        assert_eq!(
            store.load_adversarial_review(&run_id, &stage_id).unwrap(),
            initial
        );
        assert_eq!(
            store
                .submit_unbound_review_command_for_journal_test(
                    &artifact_store,
                    submission(
                        &run_id,
                        &stage_id,
                        &event_1,
                        &message_1,
                        &implementer,
                        &accept_command
                    ),
                )
                .unwrap(),
            AppendAdversarialReviewOutcome::Committed
        );
        assert_eq!(
            store
                .submit_unbound_review_command_for_journal_test(
                    &artifact_store,
                    submission(
                        &run_id,
                        &stage_id,
                        &event_1,
                        &message_1,
                        &implementer,
                        &accept_command
                    ),
                )
                .unwrap(),
            AppendAdversarialReviewOutcome::Duplicate
        );
        let approval_a = AdversarialReviewCommand::SubmitReview {
            reviewer: reviewer_a.clone(),
            expected_slot_version: 0,
            object: oid('2'),
            epoch: 1,
            decision: ReviewDecision::Approved,
            finding_actions: vec![],
            review_package_digest: digest(b"review-a"),
        };
        store
            .submit_unbound_review_command_for_journal_test(
                &artifact_store,
                submission(
                    &run_id,
                    &stage_id,
                    &EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB2").unwrap(),
                    &MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FB3").unwrap(),
                    &reviewer_a,
                    &approval_a,
                ),
            )
            .unwrap();
        let approval_b = AdversarialReviewCommand::SubmitReview {
            reviewer: reviewer_b.clone(),
            expected_slot_version: 0,
            object: oid('2'),
            epoch: 1,
            decision: ReviewDecision::Approved,
            finding_actions: vec![],
            review_package_digest: digest(b"review-b"),
        };
        store
            .submit_unbound_review_command_for_journal_test(
                &artifact_store,
                submission(
                    &run_id,
                    &stage_id,
                    &EventId::parse("evt_01ARZ3NDEKTSV4RRFFQ69G5FB4").unwrap(),
                    &MessageId::parse("msg_01ARZ3NDEKTSV4RRFFQ69G5FB5").unwrap(),
                    &reviewer_b,
                    &approval_b,
                ),
            )
            .unwrap();
        let aligned = store.load_adversarial_review(&run_id, &stage_id).unwrap();
        assert_eq!(aligned.phase, herdr_flow_core::ReviewPhase::Aligned);
        drop(store);

        let reopened = SqliteStore::open(&database).unwrap();
        assert_eq!(
            reopened
                .load_adversarial_review(&run_id, &stage_id)
                .unwrap(),
            aligned
        );
        reopened
            .connection
            .execute(
                "UPDATE adversarial_review_events SET event_digest = ?1 WHERE message_id = ?2",
                params![digest(b"tampered").to_prefixed_string(), message_1.as_str()],
            )
            .unwrap();
        assert!(matches!(
            reopened.load_adversarial_review(&run_id, &stage_id),
            Err(StoreError::CorruptData(_))
        ));
    }
}
