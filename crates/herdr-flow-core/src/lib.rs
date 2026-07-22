#![no_std]
#![forbid(unsafe_code)]

//! Pure domain types and deterministic workflow logic for Herdr Flow.
//!
//! This crate is `no_std` and CI compiles it for a bare-metal target with no
//! standard library, preventing reducer code from accessing standard filesystem,
//! network, terminal, Git, clock, process, and random-number APIs. CI also
//! allowlists its direct dependencies.

extern crate alloc;

mod adversarial_review;
mod artifact;
pub mod canonical_json;
mod digest;
mod envelope;
mod git;
mod id;
mod m1;
mod pipeline;
mod publication;
mod stage;

pub use adversarial_review::{
    replay_adversarial_review, AdversarialReviewCommand, AdversarialReviewError,
    AdversarialReviewEvent, AdversarialReviewEventKind, AdversarialReviewState, FindingDisposition,
    FindingSeverity, FindingStatus, ImplementationCandidateArtifact, ReviewCandidate,
    ReviewCandidateArtifact, ReviewCandidateCheckResult, ReviewCandidateObjectManifest,
    ReviewCandidateValidation, ReviewDecision, ReviewFinding, ReviewPhase, ReviewerFindingAction,
    ReviewerSlot,
};
pub use artifact::{
    ArtifactCatalog, ArtifactCatalogError, ArtifactRecord, ArtifactRecordValidationError,
};
pub use digest::{DigestParseError, Sha256Digest};
pub use envelope::{
    ArtifactReference, AuthenticatedAgentContext, Envelope, EnvelopeParseError,
    EnvelopeValidationError, MessageKind, SubmissionAuthority,
};
pub use git::{GitObjectFormat, GitObjectId, GitObjectIdError};
pub use id::{
    ArtifactId, BatchId, EventId, FindingId, IdentifierError, MessageId, OperationId,
    ParticipantPrincipalId, RoleBindingId, RunId, StageInstanceId,
};
pub use m1::{
    decide_m1_publication_invalidation, m1_adversarial_pipeline,
    validate_m1_publication_authorization, M1DefinitionError, M1PipelineArtifacts,
    M1PipelineStages, M1PublicationInvalidationDecision, M1PublicationPredicateError,
    M1StageIdentity,
};
pub use pipeline::{
    replay_pipeline, InputManifestArtifact, PipelineCommand, PipelineDefinitionError,
    PipelineEvent, PipelineEventKind, PipelineNodeDefinition, PipelineState,
    PipelineTransitionError, StageInputManifest,
};
pub use publication::{
    replay_publication_gate, PublicationAuthorization, PublicationFeedbackTarget,
    PublicationGateCommand, PublicationGateError, PublicationGateEvent, PublicationGateEventKind,
    PublicationGatePhase, PublicationGateRegistration, PublicationGateState, PublicationManifest,
    PublicationObservation, PublicationReviewOutcome, PublicationSideEffectKind, TargetDriftPolicy,
};
pub use stage::{
    replay_stage, StageCommand, StageEvent, StageEventKind, StagePhase, StageState,
    StageTransitionError, MAX_CONTROL_REVISION,
};

/// The base protocol implemented by this runtime.
pub const BASE_PROTOCOL: &str = "herdr.flow/v1";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_the_versioned_base_protocol() {
        assert_eq!(BASE_PROTOCOL, "herdr.flow/v1");
    }
}
