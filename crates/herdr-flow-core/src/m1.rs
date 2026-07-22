use alloc::{collections::BTreeSet, vec};

use serde::{Deserialize, Serialize};

use crate::{
    canonical_json, ArtifactId, ArtifactRecord, PipelineCommand, PipelineDefinitionError,
    PipelineNodeDefinition, PipelineState, PublicationGateCommand, PublicationGateError,
    PublicationGateEvent, PublicationGateState, Sha256Digest, StageInstanceId, StageState,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct M1StageIdentity {
    pub stage_instance_id: StageInstanceId,
    pub component_digest: Sha256Digest,
    pub completion_predicate_digest: Sha256Digest,
}

impl M1StageIdentity {
    fn initial_state(&self) -> StageState {
        StageState::new(
            self.stage_instance_id.clone(),
            self.component_digest,
            self.completion_predicate_digest,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct M1PipelineStages {
    pub implementation: M1StageIdentity,
    pub adversarial_review: M1StageIdentity,
    pub publication_gate: M1StageIdentity,
    pub publisher: M1StageIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct M1PipelineArtifacts {
    pub implementation_input: ArtifactId,
    pub candidate: ArtifactId,
    pub review_package: ArtifactId,
    pub publication_authorization: ArtifactId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum M1DefinitionError {
    Pipeline(PipelineDefinitionError),
    AliasedArtifact,
}

/// Constructs the fixed M1 graph exclusively from generic registered-stage
/// dependencies. The scheduler does not know review findings, reviewer decisions,
/// human actions, or publisher operations.
pub fn m1_adversarial_pipeline(
    definition_digest: Sha256Digest,
    stages: M1PipelineStages,
    artifacts: M1PipelineArtifacts,
) -> Result<PipelineState, M1DefinitionError> {
    let artifact_ids = [
        &artifacts.implementation_input,
        &artifacts.candidate,
        &artifacts.review_package,
        &artifacts.publication_authorization,
    ];
    if artifact_ids.iter().collect::<BTreeSet<_>>().len() != artifact_ids.len() {
        return Err(M1DefinitionError::AliasedArtifact);
    }
    PipelineState::new(
        definition_digest,
        vec![
            PipelineNodeDefinition {
                stage: stages.implementation.initial_state(),
                needs: vec![],
                required_input_artifact_ids: vec![artifacts.implementation_input],
            },
            PipelineNodeDefinition {
                stage: stages.adversarial_review.initial_state(),
                needs: vec![stages.implementation.stage_instance_id.clone()],
                required_input_artifact_ids: vec![artifacts.candidate],
            },
            PipelineNodeDefinition {
                stage: stages.publication_gate.initial_state(),
                needs: vec![stages.adversarial_review.stage_instance_id.clone()],
                required_input_artifact_ids: vec![artifacts.review_package],
            },
            PipelineNodeDefinition {
                stage: stages.publisher.initial_state(),
                needs: vec![stages.publication_gate.stage_instance_id],
                required_input_artifact_ids: vec![artifacts.publication_authorization],
            },
        ],
    )
    .map_err(M1DefinitionError::Pipeline)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct M1PublicationInvalidationDecision {
    pub gate_event: PublicationGateEvent,
    pub pipeline_command: PipelineCommand,
}

pub fn decide_m1_publication_invalidation(
    gate: &PublicationGateState,
    expected_pipeline_revision: u64,
    invalidation_digest: Sha256Digest,
) -> Result<M1PublicationInvalidationDecision, PublicationGateError> {
    let manifest_digest = gate
        .manifest_digest
        .ok_or(PublicationGateError::ManifestDigestMismatch)?;
    let gate_event = gate.decide(PublicationGateCommand::Invalidate {
        expected_control_revision: gate.control_revision,
        manifest_digest,
        invalidation_digest,
    })?;
    Ok(M1PublicationInvalidationDecision {
        gate_event,
        pipeline_command: PipelineCommand::InvalidateArtifact {
            expected_revision: expected_pipeline_revision,
            artifact_id: gate.expected_authorization_artifact_id.clone(),
            cause_digest: invalidation_digest,
        },
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum M1PublicationPredicateError {
    GateNotAuthorized,
    WrongArtifact,
    WrongArtifactType,
    WrongProducer,
    WrongPipeline,
    WrongComponent,
    WrongInputManifest,
    WrongParents,
    WrongBytesDigest,
    Serialization,
}

/// Registered completion predicate for the mandatory M1 publication gate.
/// The returned digest is safe to use as gate completion evidence only when the
/// exact typed artifact is the canonical serialization of the gate's current
/// human authorization and has the required provenance.
pub fn validate_m1_publication_authorization(
    gate: &PublicationGateState,
    record: &ArtifactRecord,
    parent_artifact_ids: &[ArtifactId],
) -> Result<Sha256Digest, M1PublicationPredicateError> {
    let authorization = gate
        .authorization()
        .map_err(|_| M1PublicationPredicateError::GateNotAuthorized)?;
    gate.validate_current_authorization(authorization)
        .map_err(|_| M1PublicationPredicateError::GateNotAuthorized)?;
    if record.artifact_id != gate.expected_authorization_artifact_id {
        return Err(M1PublicationPredicateError::WrongArtifact);
    }
    if record.artifact_type != "publication-authorization/v1"
        || record.schema_id != "publication-authorization"
        || record.schema_version != 1
    {
        return Err(M1PublicationPredicateError::WrongArtifactType);
    }
    if record.producer_stage_instance_id != gate.stage_instance_id {
        return Err(M1PublicationPredicateError::WrongProducer);
    }
    if record.pipeline_definition_digest != gate.pipeline_definition_digest {
        return Err(M1PublicationPredicateError::WrongPipeline);
    }
    if record.component_digest != gate.gate_component_digest {
        return Err(M1PublicationPredicateError::WrongComponent);
    }
    if Some(record.input_manifest_digest) != gate.expected_input_manifest_digest {
        return Err(M1PublicationPredicateError::WrongInputManifest);
    }
    if parent_artifact_ids != [gate.expected_review_package_artifact_id.clone()] {
        return Err(M1PublicationPredicateError::WrongParents);
    }
    let value = serde_json::to_value(authorization)
        .map_err(|_| M1PublicationPredicateError::Serialization)?;
    let bytes =
        canonical_json::to_vec(&value).map_err(|_| M1PublicationPredicateError::Serialization)?;
    if record.sha256 != Sha256Digest::of_bytes(&bytes) {
        return Err(M1PublicationPredicateError::WrongBytesDigest);
    }
    Ok(record.sha256)
}

#[cfg(test)]
mod tests {
    use alloc::format;

    use super::*;
    use crate::{PipelineCommand, PipelineTransitionError};

    const ULIDS: [&str; 8] = [
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        "01ARZ3NDEKTSV4RRFFQ69G5FAX",
        "01ARZ3NDEKTSV4RRFFQ69G5FAY",
        "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        "01ARZ3NDEKTSV4RRFFQ69G5FB1",
        "01ARZ3NDEKTSV4RRFFQ69G5FB2",
    ];

    fn digest(byte: u8) -> Sha256Digest {
        Sha256Digest::of_bytes(&[byte])
    }

    fn stage(index: usize) -> M1StageIdentity {
        M1StageIdentity {
            stage_instance_id: StageInstanceId::parse(format!("stage_{}", ULIDS[index])).unwrap(),
            component_digest: digest(index as u8 + 1),
            completion_predicate_digest: digest(index as u8 + 11),
        }
    }

    fn artifact(index: usize) -> ArtifactId {
        ArtifactId::parse(format!("art_{}", ULIDS[index + 4])).unwrap()
    }

    #[test]
    fn rejects_artifact_aliases_that_could_bypass_the_gate() {
        let shared = artifact(0);
        let result = m1_adversarial_pipeline(
            digest(99),
            M1PipelineStages {
                implementation: stage(0),
                adversarial_review: stage(1),
                publication_gate: stage(2),
                publisher: stage(3),
            },
            M1PipelineArtifacts {
                implementation_input: shared.clone(),
                candidate: artifact(1),
                review_package: artifact(2),
                publication_authorization: shared,
            },
        );
        assert_eq!(result, Err(M1DefinitionError::AliasedArtifact));
    }

    #[test]
    fn publisher_cannot_schedule_without_review_and_human_authorization() {
        let stages = M1PipelineStages {
            implementation: stage(0),
            adversarial_review: stage(1),
            publication_gate: stage(2),
            publisher: stage(3),
        };
        let publisher_id = stages.publisher.stage_instance_id.clone();
        let pipeline = m1_adversarial_pipeline(
            digest(99),
            stages,
            M1PipelineArtifacts {
                implementation_input: artifact(0),
                candidate: artifact(1),
                review_package: artifact(2),
                publication_authorization: artifact(3),
            },
        )
        .unwrap();
        assert!(matches!(
            pipeline.decide(PipelineCommand::ScheduleStage {
                expected_revision: 0,
                stage_instance_id: publisher_id,
            }),
            Err(PipelineTransitionError::DependencyNotCompleted(_))
        ));
    }
}
