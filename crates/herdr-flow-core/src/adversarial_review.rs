use alloc::{collections::BTreeMap, vec::Vec};
use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    canonical_json, FindingId, GitObjectId, ParticipantPrincipalId, Sha256Digest, StageInstanceId,
    MAX_CONTROL_REVISION,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewPhase {
    AwaitingCandidate,
    Reviewing,
    CorrectionRequired,
    Aligned,
    HumanOverride,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FindingSeverity {
    Blocking,
    NonBlocking,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FindingStatus {
    Open,
    FixedPendingReview { candidate: GitObjectId },
    DisputedPendingReview { candidate: GitObjectId },
    Closed { candidate: GitObjectId },
    Superseded { candidate: GitObjectId },
    ConcernRecorded { candidate: GitObjectId },
}

impl FindingStatus {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Closed { .. } | Self::Superseded { .. } | Self::ConcernRecorded { .. }
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReviewFinding {
    pub finding_id: FindingId,
    pub owner: ParticipantPrincipalId,
    pub version: u64,
    pub severity: FindingSeverity,
    pub category_digest: Sha256Digest,
    pub evidence_digest: Sha256Digest,
    pub impact_digest: Sha256Digest,
    pub remediation_digest: Sha256Digest,
    pub opened_candidate: GitObjectId,
    pub status: FindingStatus,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewDecision {
    Approved,
    ChangesRequested,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReviewerSlot {
    pub principal: ParticipantPrincipalId,
    pub version: u64,
    pub decision: Option<ReviewDecision>,
    pub decided_candidate: Option<GitObjectId>,
    pub decided_epoch: Option<u32>,
    pub report_digest: Option<Sha256Digest>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReviewCandidate {
    pub object: GitObjectId,
    pub candidate_manifest_digest: Sha256Digest,
    pub validation_digest: Sha256Digest,
    pub check_result_digest: Sha256Digest,
    pub epoch: u32,
}

#[derive(Serialize)]
struct ReviewSubjectCommitment<'a> {
    stage_instance_id: &'a StageInstanceId,
    input_manifest_digest: Sha256Digest,
    baseline: &'a GitObjectId,
    review_state_revision: u64,
    candidate: &'a ReviewCandidate,
    reviewers: &'a [ReviewerSlot],
    findings: &'a BTreeMap<FindingId, ReviewFinding>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdversarialReviewState {
    pub stage_instance_id: StageInstanceId,
    pub implementer: ParticipantPrincipalId,
    pub input_manifest_digest: Sha256Digest,
    pub baseline: GitObjectId,
    pub phase: ReviewPhase,
    pub control_revision: u64,
    pub review_state_revision: u64,
    pub candidate: Option<ReviewCandidate>,
    pub reviewers: Vec<ReviewerSlot>,
    pub findings: BTreeMap<FindingId, ReviewFinding>,
    pub aligned_package_digest: Option<Sha256Digest>,
    pub override_package_digest: Option<Sha256Digest>,
    pub override_authorization_digest: Option<Sha256Digest>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FindingDisposition {
    Fixed {
        finding_id: FindingId,
        expected_version: u64,
    },
    Disputed {
        finding_id: FindingId,
        expected_version: u64,
    },
}

impl FindingDisposition {
    fn finding_id(&self) -> &FindingId {
        match self {
            Self::Fixed { finding_id, .. } | Self::Disputed { finding_id, .. } => finding_id,
        }
    }

    fn expected_version(&self) -> u64 {
        match self {
            Self::Fixed {
                expected_version, ..
            }
            | Self::Disputed {
                expected_version, ..
            } => *expected_version,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReviewerFindingAction {
    Open {
        finding_id: FindingId,
        severity: FindingSeverity,
        category_digest: Sha256Digest,
        evidence_digest: Sha256Digest,
        impact_digest: Sha256Digest,
        remediation_digest: Sha256Digest,
    },
    Close {
        finding_id: FindingId,
        expected_version: u64,
    },
    Supersede {
        finding_id: FindingId,
        expected_version: u64,
    },
    ConfirmOpen {
        finding_id: FindingId,
        expected_version: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AdversarialReviewCommand {
    AcceptCandidate {
        expected_control_revision: u64,
        object: GitObjectId,
        candidate_manifest_digest: Sha256Digest,
        validation_digest: Sha256Digest,
        check_result_digest: Sha256Digest,
        dispositions: Vec<FindingDisposition>,
    },
    SubmitReview {
        reviewer: ParticipantPrincipalId,
        expected_slot_version: u64,
        object: GitObjectId,
        epoch: u32,
        decision: ReviewDecision,
        finding_actions: Vec<ReviewerFindingAction>,
        review_package_digest: Sha256Digest,
    },
    AuthorizeHumanOverride {
        expected_control_revision: u64,
        object: GitObjectId,
        epoch: u32,
        expected_review_state_revision: u64,
        subject_manifest_digest: Sha256Digest,
        authorization_digest: Sha256Digest,
        override_package_digest: Sha256Digest,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdversarialReviewEvent {
    pub stage_instance_id: StageInstanceId,
    pub kind: AdversarialReviewEventKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AdversarialReviewEventKind {
    CandidateAccepted {
        prior_control_revision: u64,
        object: GitObjectId,
        candidate_manifest_digest: Sha256Digest,
        validation_digest: Sha256Digest,
        check_result_digest: Sha256Digest,
        dispositions: Vec<FindingDisposition>,
    },
    ReviewSubmitted {
        reviewer: ParticipantPrincipalId,
        expected_slot_version: u64,
        object: GitObjectId,
        epoch: u32,
        decision: ReviewDecision,
        finding_actions: Vec<ReviewerFindingAction>,
        review_package_digest: Sha256Digest,
    },
    HumanOverrideAuthorized {
        prior_control_revision: u64,
        object: GitObjectId,
        epoch: u32,
        expected_review_state_revision: u64,
        subject_manifest_digest: Sha256Digest,
        authorization_digest: Sha256Digest,
        override_package_digest: Sha256Digest,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdversarialReviewError {
    EmptyReviewerSet,
    DuplicateReviewer,
    ImplementerIsReviewer,
    WrongStageInstance,
    StaleControlRevision { expected: u64, actual: u64 },
    StaleReviewStateRevision { expected: u64, actual: u64 },
    StaleSlotVersion { expected: u64, actual: u64 },
    RevisionOverflow,
    EpochOverflow,
    InvalidPhase,
    CandidateMustDescendFromBaseline,
    CandidateUnchanged,
    WrongCandidate,
    StaleEpoch,
    UnknownReviewer,
    DuplicateFindingAction,
    UnknownFinding,
    DuplicateFinding,
    WrongFindingOwner,
    StaleFindingVersion { expected: u64, actual: u64 },
    MissingFindingDisposition,
    UnexpectedFindingDisposition,
    ApprovalLeavesBlockingFinding,
    ChangesRequestedWithoutBlockingFinding,
    InvalidFindingTransition,
    ManifestMismatch,
    CommitmentSerialization,
}

impl fmt::Display for AdversarialReviewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl AdversarialReviewState {
    pub fn new(
        stage_instance_id: StageInstanceId,
        implementer: ParticipantPrincipalId,
        reviewers: Vec<ParticipantPrincipalId>,
        input_manifest_digest: Sha256Digest,
        baseline: GitObjectId,
    ) -> Result<Self, AdversarialReviewError> {
        if reviewers.is_empty() {
            return Err(AdversarialReviewError::EmptyReviewerSet);
        }
        let mut slots = Vec::with_capacity(reviewers.len());
        for principal in reviewers {
            if principal == implementer {
                return Err(AdversarialReviewError::ImplementerIsReviewer);
            }
            if slots
                .iter()
                .any(|slot: &ReviewerSlot| slot.principal == principal)
            {
                return Err(AdversarialReviewError::DuplicateReviewer);
            }
            slots.push(ReviewerSlot {
                principal,
                version: 0,
                decision: None,
                decided_candidate: None,
                decided_epoch: None,
                report_digest: None,
            });
        }
        Ok(Self {
            stage_instance_id,
            implementer,
            input_manifest_digest,
            baseline,
            phase: ReviewPhase::AwaitingCandidate,
            control_revision: 0,
            review_state_revision: 0,
            candidate: None,
            reviewers: slots,
            findings: BTreeMap::new(),
            aligned_package_digest: None,
            override_package_digest: None,
            override_authorization_digest: None,
        })
    }

    pub fn decide(
        &self,
        command: AdversarialReviewCommand,
    ) -> Result<AdversarialReviewEvent, AdversarialReviewError> {
        let kind = match command {
            AdversarialReviewCommand::AcceptCandidate {
                expected_control_revision,
                object,
                candidate_manifest_digest,
                validation_digest,
                check_result_digest,
                dispositions,
            } => AdversarialReviewEventKind::CandidateAccepted {
                prior_control_revision: expected_control_revision,
                object,
                candidate_manifest_digest,
                validation_digest,
                check_result_digest,
                dispositions,
            },
            AdversarialReviewCommand::SubmitReview {
                reviewer,
                expected_slot_version,
                object,
                epoch,
                decision,
                finding_actions,
                review_package_digest,
            } => AdversarialReviewEventKind::ReviewSubmitted {
                reviewer,
                expected_slot_version,
                object,
                epoch,
                decision,
                finding_actions,
                review_package_digest,
            },
            AdversarialReviewCommand::AuthorizeHumanOverride {
                expected_control_revision,
                object,
                epoch,
                expected_review_state_revision,
                subject_manifest_digest,
                authorization_digest,
                override_package_digest,
            } => AdversarialReviewEventKind::HumanOverrideAuthorized {
                prior_control_revision: expected_control_revision,
                object,
                epoch,
                expected_review_state_revision,
                subject_manifest_digest,
                authorization_digest,
                override_package_digest,
            },
        };
        let event = AdversarialReviewEvent {
            stage_instance_id: self.stage_instance_id.clone(),
            kind,
        };
        self.apply(&event)?;
        Ok(event)
    }

    pub fn apply(&self, event: &AdversarialReviewEvent) -> Result<Self, AdversarialReviewError> {
        if event.stage_instance_id != self.stage_instance_id {
            return Err(AdversarialReviewError::WrongStageInstance);
        }
        let mut next = self.clone();
        match &event.kind {
            AdversarialReviewEventKind::CandidateAccepted {
                prior_control_revision,
                object,
                candidate_manifest_digest,
                validation_digest,
                check_result_digest,
                dispositions,
            } => {
                if *prior_control_revision != self.control_revision {
                    return Err(AdversarialReviewError::StaleControlRevision {
                        expected: *prior_control_revision,
                        actual: self.control_revision,
                    });
                }
                if !matches!(
                    self.phase,
                    ReviewPhase::AwaitingCandidate | ReviewPhase::CorrectionRequired
                ) {
                    return Err(AdversarialReviewError::InvalidPhase);
                }
                if object.format() != self.baseline.format() {
                    return Err(AdversarialReviewError::CandidateMustDescendFromBaseline);
                }
                if self
                    .candidate
                    .as_ref()
                    .is_some_and(|current| current.object == *object)
                {
                    return Err(AdversarialReviewError::CandidateUnchanged);
                }
                next.apply_dispositions(dispositions, object)?;
                let epoch = match &self.candidate {
                    Some(candidate) => candidate
                        .epoch
                        .checked_add(1)
                        .ok_or(AdversarialReviewError::EpochOverflow)?,
                    None => 1,
                };
                next.candidate = Some(ReviewCandidate {
                    object: object.clone(),
                    candidate_manifest_digest: *candidate_manifest_digest,
                    validation_digest: *validation_digest,
                    check_result_digest: *check_result_digest,
                    epoch,
                });
                for slot in &mut next.reviewers {
                    slot.decision = None;
                    slot.decided_candidate = None;
                    slot.decided_epoch = None;
                    slot.report_digest = None;
                }
                next.phase = ReviewPhase::Reviewing;
                next.aligned_package_digest = None;
                next.override_package_digest = None;
                next.override_authorization_digest = None;
                next.bump_control_and_review()?;
            }
            AdversarialReviewEventKind::ReviewSubmitted {
                reviewer,
                expected_slot_version,
                object,
                epoch,
                decision,
                finding_actions,
                review_package_digest,
            } => {
                if self.phase != ReviewPhase::Reviewing {
                    return Err(AdversarialReviewError::InvalidPhase);
                }
                self.require_candidate(object, *epoch)?;
                let slot_index = self
                    .reviewers
                    .iter()
                    .position(|slot| slot.principal == *reviewer)
                    .ok_or(AdversarialReviewError::UnknownReviewer)?;
                let slot = &self.reviewers[slot_index];
                if slot.version != *expected_slot_version {
                    return Err(AdversarialReviewError::StaleSlotVersion {
                        expected: *expected_slot_version,
                        actual: slot.version,
                    });
                }
                next.apply_finding_actions(reviewer, object, finding_actions)?;
                next.validate_decision(reviewer, *decision)?;
                let slot = &mut next.reviewers[slot_index];
                slot.version = checked_revision(slot.version)?;
                slot.decision = Some(*decision);
                slot.decided_candidate = Some(object.clone());
                slot.decided_epoch = Some(*epoch);
                slot.report_digest = Some(*review_package_digest);
                next.review_state_revision = next
                    .review_state_revision
                    .checked_add(1)
                    .filter(|revision| *revision <= MAX_CONTROL_REVISION)
                    .ok_or(AdversarialReviewError::RevisionOverflow)?;
                if next.is_aligned() {
                    next.phase = ReviewPhase::Aligned;
                    next.aligned_package_digest = Some(next.subject_manifest_digest()?);
                } else if next.all_reviewers_decided() {
                    next.phase = ReviewPhase::CorrectionRequired;
                }
            }
            AdversarialReviewEventKind::HumanOverrideAuthorized {
                prior_control_revision,
                object,
                epoch,
                expected_review_state_revision,
                subject_manifest_digest,
                authorization_digest,
                override_package_digest,
            } => {
                if *prior_control_revision != self.control_revision {
                    return Err(AdversarialReviewError::StaleControlRevision {
                        expected: *prior_control_revision,
                        actual: self.control_revision,
                    });
                }
                if *expected_review_state_revision != self.review_state_revision {
                    return Err(AdversarialReviewError::StaleReviewStateRevision {
                        expected: *expected_review_state_revision,
                        actual: self.review_state_revision,
                    });
                }
                if self.phase != ReviewPhase::CorrectionRequired {
                    return Err(AdversarialReviewError::InvalidPhase);
                }
                self.require_candidate(object, *epoch)?;
                if *subject_manifest_digest != self.subject_manifest_digest()? {
                    return Err(AdversarialReviewError::ManifestMismatch);
                }
                next.phase = ReviewPhase::HumanOverride;
                next.override_package_digest = Some(*override_package_digest);
                next.override_authorization_digest = Some(*authorization_digest);
                next.bump_control_and_review()?;
            }
        }
        Ok(next)
    }

    fn apply_dispositions(
        &mut self,
        dispositions: &[FindingDisposition],
        candidate: &GitObjectId,
    ) -> Result<(), AdversarialReviewError> {
        let required: Vec<FindingId> = self
            .findings
            .values()
            .filter(|finding| {
                finding.severity == FindingSeverity::Blocking && !finding.status.is_terminal()
            })
            .map(|finding| finding.finding_id.clone())
            .collect();
        if dispositions.len() != required.len() {
            return Err(AdversarialReviewError::MissingFindingDisposition);
        }
        let mut seen = Vec::new();
        for disposition in dispositions {
            if seen.iter().any(|id| id == disposition.finding_id()) {
                return Err(AdversarialReviewError::UnexpectedFindingDisposition);
            }
            seen.push(disposition.finding_id().clone());
            let finding = self
                .findings
                .get_mut(disposition.finding_id())
                .ok_or(AdversarialReviewError::UnexpectedFindingDisposition)?;
            if finding.severity != FindingSeverity::Blocking || finding.status.is_terminal() {
                return Err(AdversarialReviewError::UnexpectedFindingDisposition);
            }
            if finding.version != disposition.expected_version() {
                return Err(AdversarialReviewError::StaleFindingVersion {
                    expected: disposition.expected_version(),
                    actual: finding.version,
                });
            }
            finding.version = checked_revision(finding.version)?;
            finding.status = match disposition {
                FindingDisposition::Fixed { .. } => FindingStatus::FixedPendingReview {
                    candidate: candidate.clone(),
                },
                FindingDisposition::Disputed { .. } => FindingStatus::DisputedPendingReview {
                    candidate: candidate.clone(),
                },
            };
        }
        if required.iter().any(|id| !seen.contains(id)) {
            return Err(AdversarialReviewError::MissingFindingDisposition);
        }
        Ok(())
    }

    fn apply_finding_actions(
        &mut self,
        reviewer: &ParticipantPrincipalId,
        candidate: &GitObjectId,
        actions: &[ReviewerFindingAction],
    ) -> Result<(), AdversarialReviewError> {
        let mut seen = Vec::new();
        for action in actions {
            let id = match action {
                ReviewerFindingAction::Open { finding_id, .. }
                | ReviewerFindingAction::Close { finding_id, .. }
                | ReviewerFindingAction::Supersede { finding_id, .. }
                | ReviewerFindingAction::ConfirmOpen { finding_id, .. } => finding_id,
            };
            if seen.contains(id) {
                return Err(AdversarialReviewError::DuplicateFindingAction);
            }
            seen.push(id.clone());
            match action {
                ReviewerFindingAction::Open {
                    finding_id,
                    severity,
                    category_digest,
                    evidence_digest,
                    impact_digest,
                    remediation_digest,
                } => {
                    if self.findings.contains_key(finding_id) {
                        return Err(AdversarialReviewError::DuplicateFinding);
                    }
                    self.findings.insert(
                        finding_id.clone(),
                        ReviewFinding {
                            finding_id: finding_id.clone(),
                            owner: reviewer.clone(),
                            version: 0,
                            severity: *severity,
                            category_digest: *category_digest,
                            evidence_digest: *evidence_digest,
                            impact_digest: *impact_digest,
                            remediation_digest: *remediation_digest,
                            opened_candidate: candidate.clone(),
                            status: match severity {
                                FindingSeverity::Blocking => FindingStatus::Open,
                                FindingSeverity::NonBlocking => FindingStatus::ConcernRecorded {
                                    candidate: candidate.clone(),
                                },
                            },
                        },
                    );
                }
                ReviewerFindingAction::Close {
                    finding_id,
                    expected_version,
                }
                | ReviewerFindingAction::Supersede {
                    finding_id,
                    expected_version,
                }
                | ReviewerFindingAction::ConfirmOpen {
                    finding_id,
                    expected_version,
                } => {
                    let finding = self
                        .findings
                        .get_mut(finding_id)
                        .ok_or(AdversarialReviewError::UnknownFinding)?;
                    if finding.owner != *reviewer {
                        return Err(AdversarialReviewError::WrongFindingOwner);
                    }
                    if finding.version != *expected_version {
                        return Err(AdversarialReviewError::StaleFindingVersion {
                            expected: *expected_version,
                            actual: finding.version,
                        });
                    }
                    if !matches!(
                        finding.status,
                        FindingStatus::FixedPendingReview {
                            candidate: ref bound,
                        } | FindingStatus::DisputedPendingReview {
                            candidate: ref bound,
                        }
                            if bound == candidate
                    ) {
                        return Err(AdversarialReviewError::InvalidFindingTransition);
                    }
                    finding.version = checked_revision(finding.version)?;
                    finding.status = match action {
                        ReviewerFindingAction::Close { .. } => FindingStatus::Closed {
                            candidate: candidate.clone(),
                        },
                        ReviewerFindingAction::Supersede { .. } => FindingStatus::Superseded {
                            candidate: candidate.clone(),
                        },
                        ReviewerFindingAction::ConfirmOpen { .. } => FindingStatus::Open,
                        ReviewerFindingAction::Open { .. } => unreachable!(),
                    };
                }
            }
        }
        Ok(())
    }

    fn validate_decision(
        &self,
        reviewer: &ParticipantPrincipalId,
        decision: ReviewDecision,
    ) -> Result<(), AdversarialReviewError> {
        let has_owned_blocking = self.findings.values().any(|finding| {
            finding.owner == *reviewer
                && finding.severity == FindingSeverity::Blocking
                && !finding.status.is_terminal()
        });
        match decision {
            ReviewDecision::Approved if has_owned_blocking => {
                Err(AdversarialReviewError::ApprovalLeavesBlockingFinding)
            }
            ReviewDecision::ChangesRequested if !has_owned_blocking => {
                Err(AdversarialReviewError::ChangesRequestedWithoutBlockingFinding)
            }
            _ => Ok(()),
        }
    }

    fn require_candidate(
        &self,
        object: &GitObjectId,
        epoch: u32,
    ) -> Result<&ReviewCandidate, AdversarialReviewError> {
        let candidate = self
            .candidate
            .as_ref()
            .ok_or(AdversarialReviewError::WrongCandidate)?;
        if candidate.object != *object {
            return Err(AdversarialReviewError::WrongCandidate);
        }
        if candidate.epoch != epoch {
            return Err(AdversarialReviewError::StaleEpoch);
        }
        Ok(candidate)
    }

    pub fn subject_manifest_digest(&self) -> Result<Sha256Digest, AdversarialReviewError> {
        let candidate = self
            .candidate
            .as_ref()
            .ok_or(AdversarialReviewError::WrongCandidate)?;
        let value = serde_json::to_value(ReviewSubjectCommitment {
            stage_instance_id: &self.stage_instance_id,
            input_manifest_digest: self.input_manifest_digest,
            baseline: &self.baseline,
            review_state_revision: self.review_state_revision,
            candidate,
            reviewers: &self.reviewers,
            findings: &self.findings,
        })
        .map_err(|_| AdversarialReviewError::CommitmentSerialization)?;
        let bytes = canonical_json::to_vec(&value)
            .map_err(|_| AdversarialReviewError::CommitmentSerialization)?;
        Ok(Sha256Digest::of_bytes(&bytes))
    }

    fn all_reviewers_decided(&self) -> bool {
        let Some(candidate) = &self.candidate else {
            return false;
        };
        self.reviewers.iter().all(|slot| {
            slot.decision.is_some()
                && slot.decided_candidate.as_ref() == Some(&candidate.object)
                && slot.decided_epoch == Some(candidate.epoch)
        })
    }

    fn is_aligned(&self) -> bool {
        !self.findings.values().any(|finding| {
            finding.severity == FindingSeverity::Blocking && !finding.status.is_terminal()
        }) && self.all_reviewers_decided()
            && self
                .reviewers
                .iter()
                .all(|slot| slot.decision == Some(ReviewDecision::Approved))
    }

    fn bump_control_and_review(&mut self) -> Result<(), AdversarialReviewError> {
        self.control_revision = self
            .control_revision
            .checked_add(1)
            .filter(|revision| *revision <= MAX_CONTROL_REVISION)
            .ok_or(AdversarialReviewError::RevisionOverflow)?;
        self.review_state_revision = self
            .review_state_revision
            .checked_add(1)
            .filter(|revision| *revision <= MAX_CONTROL_REVISION)
            .ok_or(AdversarialReviewError::RevisionOverflow)?;
        Ok(())
    }
}

fn checked_revision(current: u64) -> Result<u64, AdversarialReviewError> {
    current
        .checked_add(1)
        .filter(|revision| *revision <= MAX_CONTROL_REVISION)
        .ok_or(AdversarialReviewError::RevisionOverflow)
}

pub fn replay_adversarial_review(
    initial: AdversarialReviewState,
    events: &[AdversarialReviewEvent],
) -> Result<AdversarialReviewState, AdversarialReviewError> {
    events
        .iter()
        .try_fold(initial, |state, event| state.apply(event))
}

#[cfg(test)]
mod tests {
    use alloc::{format, string::ToString, vec};

    use super::*;
    use crate::GitObjectFormat;

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    fn digest(byte: u8) -> Sha256Digest {
        Sha256Digest::of_bytes(&[byte])
    }

    fn oid(byte: char) -> GitObjectId {
        GitObjectId::from_hex(GitObjectFormat::Sha1, &byte.to_string().repeat(40)).unwrap()
    }

    fn principal(suffix: &str) -> ParticipantPrincipalId {
        ParticipantPrincipalId::parse(format!("principal_{suffix}")).unwrap()
    }

    fn state() -> AdversarialReviewState {
        AdversarialReviewState::new(
            StageInstanceId::parse(format!("stage_{ULID}")).unwrap(),
            principal(ULID),
            vec![
                principal("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
                principal("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
            ],
            digest(1),
            oid('1'),
        )
        .unwrap()
    }

    fn accept(
        state: &AdversarialReviewState,
        object: GitObjectId,
        dispositions: Vec<FindingDisposition>,
    ) -> AdversarialReviewEvent {
        state
            .decide(AdversarialReviewCommand::AcceptCandidate {
                expected_control_revision: state.control_revision,
                object,
                candidate_manifest_digest: digest(2),
                validation_digest: digest(3),
                check_result_digest: digest(4),
                dispositions,
            })
            .unwrap()
    }

    fn finding() -> FindingId {
        FindingId::parse("finding_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap()
    }

    #[test]
    fn independent_slots_align_only_on_one_exact_candidate_and_epoch() {
        let initial = state();
        let accepted = accept(&initial, oid('2'), vec![]);
        let reviewing = initial.apply(&accepted).unwrap();
        let first = reviewing
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
                expected_slot_version: 0,
                object: oid('2'),
                epoch: 1,
                decision: ReviewDecision::Approved,
                finding_actions: vec![],
                review_package_digest: digest(5),
            })
            .unwrap();
        let after_first = reviewing.apply(&first).unwrap();
        assert_eq!(after_first.phase, ReviewPhase::Reviewing);
        let second = after_first
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
                expected_slot_version: 0,
                object: oid('2'),
                epoch: 1,
                decision: ReviewDecision::Approved,
                finding_actions: vec![],
                review_package_digest: digest(6),
            })
            .unwrap();
        let aligned = after_first.apply(&second).unwrap();
        assert_eq!(aligned.phase, ReviewPhase::Aligned);
        assert_eq!(
            aligned.aligned_package_digest,
            Some(aligned.subject_manifest_digest().unwrap())
        );
        assert_eq!(
            replay_adversarial_review(initial.clone(), &[accepted, first, second]).unwrap(),
            aligned
        );

        let reverse_accepted = accept(&initial, oid('2'), vec![]);
        let reverse_start = initial.apply(&reverse_accepted).unwrap();
        let reviewer_b = reverse_start
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
                expected_slot_version: 0,
                object: oid('2'),
                epoch: 1,
                decision: ReviewDecision::Approved,
                finding_actions: vec![],
                review_package_digest: digest(6),
            })
            .unwrap();
        let after_b = reverse_start.apply(&reviewer_b).unwrap();
        let reviewer_a = after_b
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
                expected_slot_version: 0,
                object: oid('2'),
                epoch: 1,
                decision: ReviewDecision::Approved,
                finding_actions: vec![],
                review_package_digest: digest(5),
            })
            .unwrap();
        let reversed = after_b.apply(&reviewer_a).unwrap();
        assert_eq!(
            reversed.aligned_package_digest,
            aligned.aligned_package_digest
        );
    }

    #[test]
    fn non_blocking_concerns_are_terminal_and_do_not_fake_changes_requested() {
        let initial = state();
        let reviewing = initial.apply(&accept(&initial, oid('2'), vec![])).unwrap();
        let report = reviewing
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
                expected_slot_version: 0,
                object: oid('2'),
                epoch: 1,
                decision: ReviewDecision::Approved,
                finding_actions: vec![ReviewerFindingAction::Open {
                    finding_id: finding(),
                    severity: FindingSeverity::NonBlocking,
                    category_digest: digest(7),
                    evidence_digest: digest(8),
                    impact_digest: digest(9),
                    remediation_digest: digest(10),
                }],
                review_package_digest: digest(11),
            })
            .unwrap();
        let recorded = reviewing.apply(&report).unwrap();
        assert!(matches!(
            recorded.findings[&finding()].status,
            FindingStatus::ConcernRecorded { .. }
        ));
        let invalid = reviewing.decide(AdversarialReviewCommand::SubmitReview {
            reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
            expected_slot_version: 0,
            object: oid('2'),
            epoch: 1,
            decision: ReviewDecision::ChangesRequested,
            finding_actions: vec![ReviewerFindingAction::Open {
                finding_id: finding(),
                severity: FindingSeverity::NonBlocking,
                category_digest: digest(7),
                evidence_digest: digest(8),
                impact_digest: digest(9),
                remediation_digest: digest(10),
            }],
            review_package_digest: digest(11),
        });
        assert_eq!(
            invalid,
            Err(AdversarialReviewError::ChangesRequestedWithoutBlockingFinding)
        );
    }

    #[test]
    fn finding_closes_only_by_originating_reviewer_on_next_exact_candidate() {
        let initial = state();
        let reviewing = initial.apply(&accept(&initial, oid('2'), vec![])).unwrap();
        let request = reviewing
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
                expected_slot_version: 0,
                object: oid('2'),
                epoch: 1,
                decision: ReviewDecision::ChangesRequested,
                finding_actions: vec![ReviewerFindingAction::Open {
                    finding_id: finding(),
                    severity: FindingSeverity::Blocking,
                    category_digest: digest(7),
                    evidence_digest: digest(8),
                    impact_digest: digest(9),
                    remediation_digest: digest(10),
                }],
                review_package_digest: digest(11),
            })
            .unwrap();
        let after_request = reviewing.apply(&request).unwrap();
        assert_eq!(after_request.phase, ReviewPhase::Reviewing);
        let other_review = after_request
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
                expected_slot_version: 0,
                object: oid('2'),
                epoch: 1,
                decision: ReviewDecision::Approved,
                finding_actions: vec![],
                review_package_digest: digest(12),
            })
            .unwrap();
        let correction = after_request.apply(&other_review).unwrap();
        assert_eq!(correction.phase, ReviewPhase::CorrectionRequired);
        let candidate = correction
            .apply(&accept(
                &correction,
                oid('3'),
                vec![FindingDisposition::Fixed {
                    finding_id: finding(),
                    expected_version: 0,
                }],
            ))
            .unwrap();
        assert!(matches!(
            candidate.findings[&finding()].status,
            FindingStatus::FixedPendingReview { .. }
        ));
        let wrong_owner = candidate.decide(AdversarialReviewCommand::SubmitReview {
            reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
            expected_slot_version: 1,
            object: oid('3'),
            epoch: 2,
            decision: ReviewDecision::Approved,
            finding_actions: vec![ReviewerFindingAction::Close {
                finding_id: finding(),
                expected_version: 1,
            }],
            review_package_digest: digest(14),
        });
        assert_eq!(wrong_owner, Err(AdversarialReviewError::WrongFindingOwner));
        let close = candidate
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
                expected_slot_version: 1,
                object: oid('3'),
                epoch: 2,
                decision: ReviewDecision::Approved,
                finding_actions: vec![ReviewerFindingAction::Close {
                    finding_id: finding(),
                    expected_version: 1,
                }],
                review_package_digest: digest(13),
            })
            .unwrap();
        assert!(matches!(
            candidate.apply(&close).unwrap().findings[&finding()].status,
            FindingStatus::Closed { .. }
        ));
    }

    #[test]
    fn stale_wrong_object_and_partial_corrections_have_no_effect() {
        let initial = state();
        let reviewing = initial.apply(&accept(&initial, oid('2'), vec![])).unwrap();
        let wrong = reviewing.decide(AdversarialReviewCommand::SubmitReview {
            reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
            expected_slot_version: 0,
            object: oid('3'),
            epoch: 1,
            decision: ReviewDecision::Approved,
            finding_actions: vec![],
            review_package_digest: digest(9),
        });
        assert_eq!(wrong, Err(AdversarialReviewError::WrongCandidate));
        assert_eq!(reviewing.review_state_revision, 1);
    }

    #[test]
    fn human_override_is_distinct_from_agent_alignment_and_exactly_bound() {
        let initial = state();
        let reviewing = initial.apply(&accept(&initial, oid('2'), vec![])).unwrap();
        let early = reviewing.decide(AdversarialReviewCommand::AuthorizeHumanOverride {
            expected_control_revision: reviewing.control_revision,
            object: oid('2'),
            epoch: 1,
            expected_review_state_revision: reviewing.review_state_revision,
            subject_manifest_digest: digest(99),
            authorization_digest: digest(20),
            override_package_digest: digest(21),
        });
        assert_eq!(early, Err(AdversarialReviewError::InvalidPhase));
        let first = reviewing
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
                expected_slot_version: 0,
                object: oid('2'),
                epoch: 1,
                decision: ReviewDecision::ChangesRequested,
                finding_actions: vec![ReviewerFindingAction::Open {
                    finding_id: finding(),
                    severity: FindingSeverity::Blocking,
                    category_digest: digest(7),
                    evidence_digest: digest(8),
                    impact_digest: digest(9),
                    remediation_digest: digest(10),
                }],
                review_package_digest: digest(11),
            })
            .unwrap();
        let after_first = reviewing.apply(&first).unwrap();
        let second = after_first
            .decide(AdversarialReviewCommand::SubmitReview {
                reviewer: principal("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
                expected_slot_version: 0,
                object: oid('2'),
                epoch: 1,
                decision: ReviewDecision::Approved,
                finding_actions: vec![],
                review_package_digest: digest(12),
            })
            .unwrap();
        let correction = after_first.apply(&second).unwrap();
        assert_eq!(correction.phase, ReviewPhase::CorrectionRequired);
        let stale = correction.decide(AdversarialReviewCommand::AuthorizeHumanOverride {
            expected_control_revision: correction.control_revision,
            object: oid('2'),
            epoch: 1,
            expected_review_state_revision: correction.review_state_revision,
            subject_manifest_digest: digest(99),
            authorization_digest: digest(20),
            override_package_digest: digest(21),
        });
        assert_eq!(stale, Err(AdversarialReviewError::ManifestMismatch));
        let event = correction
            .decide(AdversarialReviewCommand::AuthorizeHumanOverride {
                expected_control_revision: correction.control_revision,
                object: oid('2'),
                epoch: 1,
                expected_review_state_revision: correction.review_state_revision,
                subject_manifest_digest: correction.subject_manifest_digest().unwrap(),
                authorization_digest: digest(20),
                override_package_digest: digest(21),
            })
            .unwrap();
        let overridden = correction.apply(&event).unwrap();
        assert_eq!(overridden.phase, ReviewPhase::HumanOverride);
        assert_eq!(overridden.aligned_package_digest, None);
        assert_eq!(overridden.override_package_digest, Some(digest(21)));
        assert_eq!(overridden.override_authorization_digest, Some(digest(20)));
    }
}
