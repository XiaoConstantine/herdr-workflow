use alloc::{boxed::Box, string::String, vec::Vec};
use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    canonical_json, ArtifactId, GitObjectId, RunId, Sha256Digest, StageInstanceId,
    MAX_CONTROL_REVISION,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TargetDriftPolicy {
    FailClosed,
    HumanAuthorizedAlternative { policy_digest: Sha256Digest },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationManifest {
    pub run_id: RunId,
    pub publication_stage_instance_id: StageInstanceId,
    pub publisher_component_digest: Sha256Digest,
    pub provider: String,
    pub project_identity_digest: Sha256Digest,
    pub canonical_remote_url_digest: Sha256Digest,
    pub final_object: GitObjectId,
    pub deterministic_head_ref: String,
    pub target_ref: String,
    pub observed_target_object: GitObjectId,
    pub reviewed_merge_base: GitObjectId,
    pub target_drift_policy: TargetDriftPolicy,
    pub expected_head_object: Option<GitObjectId>,
    pub title_digest: Sha256Digest,
    pub body_digest: Sha256Digest,
    pub metadata_digest: Sha256Digest,
    pub pipeline_definition_digest: Sha256Digest,
    pub requirements_digest: Sha256Digest,
    pub review_package_digest: Sha256Digest,
    pub gate_input_manifest_digest: Sha256Digest,
    pub check_policy_digest: Sha256Digest,
    pub check_result_digest: Sha256Digest,
    pub artifact_lineage_digest: Sha256Digest,
    pub frozen_review_state_revision: u64,
}

impl PublicationManifest {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, PublicationGateError> {
        if self.frozen_review_state_revision > MAX_CONTROL_REVISION {
            return Err(PublicationGateError::RevisionOutOfRange);
        }
        let value = serde_json::to_value(self).map_err(|_| PublicationGateError::Serialization)?;
        canonical_json::to_vec(&value).map_err(|_| PublicationGateError::Serialization)
    }

    pub fn digest(&self) -> Result<Sha256Digest, PublicationGateError> {
        self.canonical_bytes()
            .map(|bytes| Sha256Digest::of_bytes(&bytes))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicationGatePhase {
    AwaitingReview,
    AwaitingManifest,
    AwaitingHuman,
    Authorized,
    ChangesRequested,
    Cancelled,
    Invalidated,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicationFeedbackTarget {
    PublicationMetadata,
    IntegrationResult,
    RiskWaiver,
    Cancellation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicationSideEffectKind {
    PushRef,
    CreateChangeRequest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationObservation {
    pub target_object: GitObjectId,
    pub merge_base: GitObjectId,
    pub head_object: Option<GitObjectId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationAuthorization {
    pub run_id: RunId,
    pub gate_stage_instance_id: StageInstanceId,
    pub publication_stage_instance_id: StageInstanceId,
    pub publisher_component_digest: Sha256Digest,
    pub manifest_digest: Sha256Digest,
    pub final_object: GitObjectId,
    pub observed_target_object: GitObjectId,
    pub reviewed_merge_base: GitObjectId,
    pub authorization_digest: Sha256Digest,
    pub gate_control_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationGateRegistration {
    pub run_id: RunId,
    pub stage_instance_id: StageInstanceId,
    pub expected_publication_stage_instance_id: StageInstanceId,
    pub expected_publication_component_digest: Sha256Digest,
    pub expected_review_stage_instance_id: StageInstanceId,
    pub expected_review_component_digest: Sha256Digest,
    pub expected_implementation_stage_instance_id: StageInstanceId,
    pub expected_implementation_component_digest: Sha256Digest,
    pub expected_review_package_artifact_id: ArtifactId,
    pub expected_authorization_artifact_id: ArtifactId,
    pub pipeline_definition_digest: Sha256Digest,
    pub gate_component_digest: Sha256Digest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationGateState {
    pub run_id: RunId,
    pub stage_instance_id: StageInstanceId,
    pub expected_publication_stage_instance_id: StageInstanceId,
    pub expected_publication_component_digest: Sha256Digest,
    pub expected_review_stage_instance_id: StageInstanceId,
    pub expected_review_component_digest: Sha256Digest,
    pub expected_implementation_stage_instance_id: StageInstanceId,
    pub expected_implementation_component_digest: Sha256Digest,
    pub expected_input_manifest_digest: Option<Sha256Digest>,
    pub expected_review_package_digest: Option<Sha256Digest>,
    pub expected_review_package_artifact_id: ArtifactId,
    pub expected_authorization_artifact_id: ArtifactId,
    pub pipeline_definition_digest: Sha256Digest,
    pub gate_component_digest: Sha256Digest,
    pub phase: PublicationGatePhase,
    pub control_revision: u64,
    pub manifest: Option<PublicationManifest>,
    pub manifest_digest: Option<Sha256Digest>,
    pub authorization: Option<PublicationAuthorization>,
    pub feedback_target: Option<PublicationFeedbackTarget>,
    pub feedback_digest: Option<Sha256Digest>,
    pub invalidation_digest: Option<Sha256Digest>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicationGateCommand {
    BindReviewInput {
        expected_control_revision: u64,
        review_package_artifact_id: ArtifactId,
        review_package_digest: Sha256Digest,
        input_manifest_digest: Sha256Digest,
    },
    PresentManifest {
        expected_control_revision: u64,
        manifest: Box<PublicationManifest>,
    },
    HumanApprove {
        expected_control_revision: u64,
        manifest_digest: Sha256Digest,
        authorization_digest: Sha256Digest,
    },
    HumanRequestChanges {
        expected_control_revision: u64,
        manifest_digest: Sha256Digest,
        target: PublicationFeedbackTarget,
        feedback_digest: Sha256Digest,
    },
    HumanCancel {
        expected_control_revision: u64,
        manifest_digest: Sha256Digest,
        cancellation_digest: Sha256Digest,
    },
    Invalidate {
        expected_control_revision: u64,
        manifest_digest: Sha256Digest,
        invalidation_digest: Sha256Digest,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationGateEvent {
    pub run_id: RunId,
    pub stage_instance_id: StageInstanceId,
    pub prior_control_revision: u64,
    pub kind: PublicationGateEventKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PublicationGateEventKind {
    ReviewInputBound {
        review_package_artifact_id: ArtifactId,
        review_package_digest: Sha256Digest,
        input_manifest_digest: Sha256Digest,
    },
    ManifestPresented {
        manifest: Box<PublicationManifest>,
        manifest_digest: Sha256Digest,
    },
    HumanApproved {
        manifest_digest: Sha256Digest,
        authorization_digest: Sha256Digest,
    },
    HumanRequestedChanges {
        manifest_digest: Sha256Digest,
        target: PublicationFeedbackTarget,
        feedback_digest: Sha256Digest,
    },
    HumanCancelled {
        manifest_digest: Sha256Digest,
        cancellation_digest: Sha256Digest,
    },
    PublicationInvalidated {
        manifest_digest: Sha256Digest,
        invalidation_digest: Sha256Digest,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicationGateError {
    WrongRun,
    WrongStageInstance,
    StaleRevision { expected: u64, actual: u64 },
    RevisionOverflow,
    RevisionOutOfRange,
    InvalidPhase,
    WrongPublicationStage,
    ManifestDigestMismatch,
    TargetDrift,
    Serialization,
}

impl fmt::Display for PublicationGateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl PublicationGateState {
    pub fn new(registration: PublicationGateRegistration) -> Self {
        Self {
            run_id: registration.run_id,
            stage_instance_id: registration.stage_instance_id,
            expected_publication_stage_instance_id: registration
                .expected_publication_stage_instance_id,
            expected_publication_component_digest: registration
                .expected_publication_component_digest,
            expected_review_stage_instance_id: registration.expected_review_stage_instance_id,
            expected_review_component_digest: registration.expected_review_component_digest,
            expected_implementation_stage_instance_id: registration
                .expected_implementation_stage_instance_id,
            expected_implementation_component_digest: registration
                .expected_implementation_component_digest,
            expected_input_manifest_digest: None,
            expected_review_package_digest: None,
            expected_review_package_artifact_id: registration.expected_review_package_artifact_id,
            expected_authorization_artifact_id: registration.expected_authorization_artifact_id,
            pipeline_definition_digest: registration.pipeline_definition_digest,
            gate_component_digest: registration.gate_component_digest,
            phase: PublicationGatePhase::AwaitingReview,
            control_revision: 0,
            manifest: None,
            manifest_digest: None,
            authorization: None,
            feedback_target: None,
            feedback_digest: None,
            invalidation_digest: None,
        }
    }

    pub fn decide(
        &self,
        command: PublicationGateCommand,
    ) -> Result<PublicationGateEvent, PublicationGateError> {
        let (prior_control_revision, kind) = match command {
            PublicationGateCommand::BindReviewInput {
                expected_control_revision,
                review_package_artifact_id,
                review_package_digest,
                input_manifest_digest,
            } => (
                expected_control_revision,
                PublicationGateEventKind::ReviewInputBound {
                    review_package_artifact_id,
                    review_package_digest,
                    input_manifest_digest,
                },
            ),
            PublicationGateCommand::PresentManifest {
                expected_control_revision,
                manifest,
            } => {
                let manifest_digest = manifest.digest()?;
                (
                    expected_control_revision,
                    PublicationGateEventKind::ManifestPresented {
                        manifest,
                        manifest_digest,
                    },
                )
            }
            PublicationGateCommand::HumanApprove {
                expected_control_revision,
                manifest_digest,
                authorization_digest,
            } => (
                expected_control_revision,
                PublicationGateEventKind::HumanApproved {
                    manifest_digest,
                    authorization_digest,
                },
            ),
            PublicationGateCommand::HumanRequestChanges {
                expected_control_revision,
                manifest_digest,
                target,
                feedback_digest,
            } => (
                expected_control_revision,
                PublicationGateEventKind::HumanRequestedChanges {
                    manifest_digest,
                    target,
                    feedback_digest,
                },
            ),
            PublicationGateCommand::HumanCancel {
                expected_control_revision,
                manifest_digest,
                cancellation_digest,
            } => (
                expected_control_revision,
                PublicationGateEventKind::HumanCancelled {
                    manifest_digest,
                    cancellation_digest,
                },
            ),
            PublicationGateCommand::Invalidate {
                expected_control_revision,
                manifest_digest,
                invalidation_digest,
            } => (
                expected_control_revision,
                PublicationGateEventKind::PublicationInvalidated {
                    manifest_digest,
                    invalidation_digest,
                },
            ),
        };
        let event = PublicationGateEvent {
            run_id: self.run_id.clone(),
            stage_instance_id: self.stage_instance_id.clone(),
            prior_control_revision,
            kind,
        };
        self.apply(&event)?;
        Ok(event)
    }

    pub fn apply(&self, event: &PublicationGateEvent) -> Result<Self, PublicationGateError> {
        if event.run_id != self.run_id {
            return Err(PublicationGateError::WrongRun);
        }
        if event.stage_instance_id != self.stage_instance_id {
            return Err(PublicationGateError::WrongStageInstance);
        }
        if event.prior_control_revision != self.control_revision {
            return Err(PublicationGateError::StaleRevision {
                expected: event.prior_control_revision,
                actual: self.control_revision,
            });
        }
        let mut next = self.clone();
        match &event.kind {
            PublicationGateEventKind::ReviewInputBound {
                review_package_artifact_id,
                review_package_digest,
                input_manifest_digest,
            } => {
                if self.phase != PublicationGatePhase::AwaitingReview
                    || *review_package_artifact_id != self.expected_review_package_artifact_id
                {
                    return Err(PublicationGateError::InvalidPhase);
                }
                next.expected_review_package_digest = Some(*review_package_digest);
                next.expected_input_manifest_digest = Some(*input_manifest_digest);
                next.phase = PublicationGatePhase::AwaitingManifest;
            }
            PublicationGateEventKind::ManifestPresented {
                manifest,
                manifest_digest,
            } => {
                if self.phase != PublicationGatePhase::AwaitingManifest {
                    return Err(PublicationGateError::InvalidPhase);
                }
                if manifest.run_id != self.run_id || manifest.digest()? != *manifest_digest {
                    return Err(PublicationGateError::ManifestDigestMismatch);
                }
                if manifest.publication_stage_instance_id
                    != self.expected_publication_stage_instance_id
                    || manifest.publisher_component_digest
                        != self.expected_publication_component_digest
                {
                    return Err(PublicationGateError::WrongPublicationStage);
                }
                if Some(manifest.gate_input_manifest_digest) != self.expected_input_manifest_digest
                    || Some(manifest.review_package_digest) != self.expected_review_package_digest
                    || manifest.pipeline_definition_digest != self.pipeline_definition_digest
                {
                    return Err(PublicationGateError::ManifestDigestMismatch);
                }
                next.manifest = Some((**manifest).clone());
                next.manifest_digest = Some(*manifest_digest);
                next.phase = PublicationGatePhase::AwaitingHuman;
            }
            PublicationGateEventKind::HumanApproved {
                manifest_digest,
                authorization_digest,
            } => {
                if self.phase != PublicationGatePhase::AwaitingHuman {
                    return Err(PublicationGateError::InvalidPhase);
                }
                let manifest = self.require_manifest(*manifest_digest)?;
                next.authorization = Some(PublicationAuthorization {
                    run_id: self.run_id.clone(),
                    gate_stage_instance_id: self.stage_instance_id.clone(),
                    publication_stage_instance_id: manifest.publication_stage_instance_id.clone(),
                    publisher_component_digest: manifest.publisher_component_digest,
                    manifest_digest: *manifest_digest,
                    final_object: manifest.final_object.clone(),
                    observed_target_object: manifest.observed_target_object.clone(),
                    reviewed_merge_base: manifest.reviewed_merge_base.clone(),
                    authorization_digest: *authorization_digest,
                    gate_control_revision: self.control_revision + 1,
                });
                next.phase = PublicationGatePhase::Authorized;
            }
            PublicationGateEventKind::HumanRequestedChanges {
                manifest_digest,
                target,
                feedback_digest,
            } => {
                if self.phase != PublicationGatePhase::AwaitingHuman {
                    return Err(PublicationGateError::InvalidPhase);
                }
                self.require_manifest(*manifest_digest)?;
                next.phase = PublicationGatePhase::ChangesRequested;
                next.feedback_target = Some(*target);
                next.feedback_digest = Some(*feedback_digest);
            }
            PublicationGateEventKind::HumanCancelled {
                manifest_digest,
                cancellation_digest,
            } => {
                if self.phase != PublicationGatePhase::AwaitingHuman {
                    return Err(PublicationGateError::InvalidPhase);
                }
                self.require_manifest(*manifest_digest)?;
                next.phase = PublicationGatePhase::Cancelled;
                next.feedback_target = Some(PublicationFeedbackTarget::Cancellation);
                next.feedback_digest = Some(*cancellation_digest);
            }
            PublicationGateEventKind::PublicationInvalidated {
                manifest_digest,
                invalidation_digest,
            } => {
                if !matches!(
                    self.phase,
                    PublicationGatePhase::AwaitingHuman | PublicationGatePhase::Authorized
                ) {
                    return Err(PublicationGateError::InvalidPhase);
                }
                self.require_manifest(*manifest_digest)?;
                next.phase = PublicationGatePhase::Invalidated;
                next.authorization = None;
                next.invalidation_digest = Some(*invalidation_digest);
            }
        }
        next.control_revision = self
            .control_revision
            .checked_add(1)
            .filter(|revision| *revision <= MAX_CONTROL_REVISION)
            .ok_or(PublicationGateError::RevisionOverflow)?;
        Ok(next)
    }

    pub fn authorization(&self) -> Result<&PublicationAuthorization, PublicationGateError> {
        if self.phase != PublicationGatePhase::Authorized {
            return Err(PublicationGateError::InvalidPhase);
        }
        self.authorization
            .as_ref()
            .ok_or(PublicationGateError::InvalidPhase)
    }

    /// Must be called immediately before each publication side effect.
    pub fn validate_current_authorization(
        &self,
        authorization: &PublicationAuthorization,
    ) -> Result<(), PublicationGateError> {
        if self.authorization()? != authorization
            || authorization.gate_control_revision != self.control_revision
        {
            return Err(PublicationGateError::InvalidPhase);
        }
        Ok(())
    }

    pub fn validate_pre_side_effect(
        &self,
        authorization: &PublicationAuthorization,
        observation: &PublicationObservation,
        kind: PublicationSideEffectKind,
    ) -> Result<(), PublicationGateError> {
        self.validate_current_authorization(authorization)?;
        let manifest = self
            .manifest
            .as_ref()
            .ok_or(PublicationGateError::ManifestDigestMismatch)?;
        let head_matches = match kind {
            PublicationSideEffectKind::PushRef => {
                observation.head_object == manifest.expected_head_object
                    || observation.head_object.as_ref() == Some(&manifest.final_object)
            }
            PublicationSideEffectKind::CreateChangeRequest => {
                observation.head_object.as_ref() == Some(&manifest.final_object)
            }
        };
        if observation.target_object != manifest.observed_target_object
            || observation.merge_base != manifest.reviewed_merge_base
            || !head_matches
        {
            return Err(PublicationGateError::TargetDrift);
        }
        Ok(())
    }

    fn require_manifest(
        &self,
        manifest_digest: Sha256Digest,
    ) -> Result<&PublicationManifest, PublicationGateError> {
        if self.manifest_digest != Some(manifest_digest) {
            return Err(PublicationGateError::ManifestDigestMismatch);
        }
        self.manifest
            .as_ref()
            .ok_or(PublicationGateError::ManifestDigestMismatch)
    }
}

pub fn replay_publication_gate(
    initial: PublicationGateState,
    events: &[PublicationGateEvent],
) -> Result<PublicationGateState, PublicationGateError> {
    events
        .iter()
        .try_fold(initial, |state, event| state.apply(event))
}

#[cfg(test)]
mod tests {
    use alloc::format;

    use super::*;
    use crate::GitObjectFormat;

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    fn digest(byte: u8) -> Sha256Digest {
        Sha256Digest::of_bytes(&[byte])
    }

    fn stage(suffix: &str) -> StageInstanceId {
        StageInstanceId::parse(format!("stage_{suffix}")).unwrap()
    }

    fn artifact(suffix: &str) -> ArtifactId {
        ArtifactId::parse(format!("art_{suffix}")).unwrap()
    }

    fn oid(byte: char) -> GitObjectId {
        GitObjectId::from_hex(
            GitObjectFormat::Sha1,
            &alloc::string::ToString::to_string(&byte).repeat(40),
        )
        .unwrap()
    }

    fn manifest() -> PublicationManifest {
        PublicationManifest {
            run_id: RunId::parse(format!("flow_{ULID}")).unwrap(),
            publication_stage_instance_id: stage("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
            publisher_component_digest: digest(19),
            provider: "fake".into(),
            project_identity_digest: digest(1),
            canonical_remote_url_digest: digest(2),
            final_object: oid('2'),
            deterministic_head_ref: "refs/heads/herdr/run".into(),
            target_ref: "refs/heads/main".into(),
            observed_target_object: oid('1'),
            reviewed_merge_base: oid('1'),
            target_drift_policy: TargetDriftPolicy::FailClosed,
            expected_head_object: None,
            title_digest: digest(3),
            body_digest: digest(4),
            metadata_digest: digest(5),
            pipeline_definition_digest: digest(6),
            requirements_digest: digest(7),
            review_package_digest: digest(8),
            gate_input_manifest_digest: digest(15),
            check_policy_digest: digest(9),
            check_result_digest: digest(10),
            artifact_lineage_digest: digest(11),
            frozen_review_state_revision: 4,
        }
    }

    fn gate_state() -> PublicationGateState {
        let initial = PublicationGateState::new(PublicationGateRegistration {
            run_id: manifest().run_id.clone(),
            stage_instance_id: stage(ULID),
            expected_publication_stage_instance_id: manifest()
                .publication_stage_instance_id
                .clone(),
            expected_publication_component_digest: digest(19),
            expected_review_stage_instance_id: stage("01ARZ3NDEKTSV4RRFFQ69G5FAZ"),
            expected_review_component_digest: digest(17),
            expected_implementation_stage_instance_id: stage("01ARZ3NDEKTSV4RRFFQ69G5FB0"),
            expected_implementation_component_digest: digest(18),
            expected_review_package_artifact_id: artifact("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
            expected_authorization_artifact_id: artifact("01ARZ3NDEKTSV4RRFFQ69G5FAY"),
            pipeline_definition_digest: manifest().pipeline_definition_digest,
            gate_component_digest: digest(16),
        });
        let bound = initial
            .decide(PublicationGateCommand::BindReviewInput {
                expected_control_revision: 0,
                review_package_artifact_id: artifact("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
                review_package_digest: manifest().review_package_digest,
                input_manifest_digest: manifest().gate_input_manifest_digest,
            })
            .unwrap();
        initial.apply(&bound).unwrap()
    }

    #[test]
    fn approval_is_exact_manifest_bound_and_replays() {
        let initial = gate_state();
        let presented = initial
            .decide(PublicationGateCommand::PresentManifest {
                expected_control_revision: 1,
                manifest: Box::new(manifest()),
            })
            .unwrap();
        let awaiting = initial.apply(&presented).unwrap();
        let wrong = awaiting.decide(PublicationGateCommand::HumanApprove {
            expected_control_revision: 2,
            manifest_digest: digest(99),
            authorization_digest: digest(12),
        });
        assert_eq!(wrong, Err(PublicationGateError::ManifestDigestMismatch));
        let approved = awaiting
            .decide(PublicationGateCommand::HumanApprove {
                expected_control_revision: 2,
                manifest_digest: manifest().digest().unwrap(),
                authorization_digest: digest(12),
            })
            .unwrap();
        let authorized = awaiting.apply(&approved).unwrap();
        assert_eq!(authorized.phase, PublicationGatePhase::Authorized);
        assert_eq!(authorized.authorization().unwrap().final_object, oid('2'));
        assert_eq!(
            replay_publication_gate(initial, &[presented, approved]).unwrap(),
            authorized
        );
    }

    #[test]
    fn invalidation_revokes_authorization_and_human_actions_do_not_reopen() {
        let initial = gate_state();
        let awaiting = initial
            .apply(
                &initial
                    .decide(PublicationGateCommand::PresentManifest {
                        expected_control_revision: 1,
                        manifest: Box::new(manifest()),
                    })
                    .unwrap(),
            )
            .unwrap();
        let authorized = awaiting
            .apply(
                &awaiting
                    .decide(PublicationGateCommand::HumanApprove {
                        expected_control_revision: 2,
                        manifest_digest: manifest().digest().unwrap(),
                        authorization_digest: digest(12),
                    })
                    .unwrap(),
            )
            .unwrap();
        let issued = authorized.authorization().unwrap().clone();
        authorized.validate_current_authorization(&issued).unwrap();
        authorized
            .validate_pre_side_effect(
                &issued,
                &PublicationObservation {
                    target_object: manifest().observed_target_object,
                    merge_base: manifest().reviewed_merge_base,
                    head_object: None,
                },
                PublicationSideEffectKind::PushRef,
            )
            .unwrap();
        assert_eq!(
            authorized.validate_pre_side_effect(
                &issued,
                &PublicationObservation {
                    target_object: oid('3'),
                    merge_base: manifest().reviewed_merge_base,
                    head_object: None,
                },
                PublicationSideEffectKind::PushRef,
            ),
            Err(PublicationGateError::TargetDrift)
        );
        let invalidated = authorized
            .apply(
                &authorized
                    .decide(PublicationGateCommand::Invalidate {
                        expected_control_revision: 3,
                        manifest_digest: manifest().digest().unwrap(),
                        invalidation_digest: digest(13),
                    })
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(invalidated.phase, PublicationGatePhase::Invalidated);
        assert!(invalidated.authorization.is_none());
        assert!(matches!(
            invalidated.validate_current_authorization(&issued),
            Err(PublicationGateError::InvalidPhase)
        ));
        assert!(matches!(
            invalidated.decide(PublicationGateCommand::HumanApprove {
                expected_control_revision: 4,
                manifest_digest: manifest().digest().unwrap(),
                authorization_digest: digest(14),
            }),
            Err(PublicationGateError::InvalidPhase)
        ));
    }
}
