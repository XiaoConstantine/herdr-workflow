#![no_std]
#![forbid(unsafe_code)]

//! Pure domain types and deterministic workflow logic for Herdr Flow.
//!
//! This crate is `no_std` and CI compiles it for a bare-metal target with no
//! standard library, preventing reducer code from accessing standard filesystem,
//! network, terminal, Git, clock, process, and random-number APIs. CI also
//! allowlists its direct dependencies.

extern crate alloc;

mod artifact;
pub mod canonical_json;
mod digest;
mod envelope;
mod id;
mod pipeline;
mod stage;

pub use artifact::{
    ArtifactCatalog, ArtifactCatalogError, ArtifactRecord, ArtifactRecordValidationError,
};
pub use digest::{DigestParseError, Sha256Digest};
pub use envelope::{
    ArtifactReference, AuthenticatedAgentContext, Envelope, EnvelopeParseError,
    EnvelopeValidationError, MessageKind, SubmissionAuthority,
};
pub use id::{
    ArtifactId, EventId, IdentifierError, MessageId, ParticipantPrincipalId, RoleBindingId, RunId,
    StageInstanceId,
};
pub use pipeline::{
    replay_pipeline, InputManifestArtifact, PipelineCommand, PipelineDefinitionError,
    PipelineEvent, PipelineEventKind, PipelineNodeDefinition, PipelineState,
    PipelineTransitionError, StageInputManifest,
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
