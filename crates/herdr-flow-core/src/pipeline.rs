use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};
use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    canonical_json, ArtifactId, Sha256Digest, StageEvent, StageEventKind, StageInstanceId,
    StagePhase, StageState, StageTransitionError, BASE_PROTOCOL, MAX_CONTROL_REVISION,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PipelineNodeDefinition {
    pub stage: StageState,
    pub needs: Vec<StageInstanceId>,
    pub required_input_artifact_ids: Vec<ArtifactId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PipelineDefinitionError {
    Empty,
    DuplicateStage(StageInstanceId),
    DuplicateDependency(StageInstanceId),
    DuplicateInput(ArtifactId),
    UnknownDependency(StageInstanceId),
    SelfDependency(StageInstanceId),
    Cycle,
    NonPristineStage(StageInstanceId),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InputManifestArtifact {
    pub artifact_id: ArtifactId,
    pub sha256: Sha256Digest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StageInputManifest {
    pub protocol: String,
    pub stage_instance_id: StageInstanceId,
    pub artifacts: Vec<InputManifestArtifact>,
}

impl StageInputManifest {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, canonical_json::CanonicalJsonError> {
        let mut root = serde_json::Map::new();
        root.insert(
            "protocol".to_string(),
            serde_json::Value::String(self.protocol.clone()),
        );
        root.insert(
            "stage_instance_id".to_string(),
            serde_json::Value::String(self.stage_instance_id.to_string()),
        );
        root.insert(
            "artifacts".to_string(),
            serde_json::Value::Array(
                self.artifacts
                    .iter()
                    .map(|artifact| {
                        let mut value = serde_json::Map::new();
                        value.insert(
                            "artifact_id".to_string(),
                            serde_json::Value::String(artifact.artifact_id.to_string()),
                        );
                        value.insert(
                            "sha256".to_string(),
                            serde_json::Value::String(artifact.sha256.to_prefixed_string()),
                        );
                        serde_json::Value::Object(value)
                    })
                    .collect(),
            ),
        );
        canonical_json::to_vec(&serde_json::Value::Object(root))
    }

    pub fn digest(&self) -> Result<Sha256Digest, canonical_json::CanonicalJsonError> {
        self.canonical_bytes()
            .map(|bytes| Sha256Digest::of_bytes(&bytes))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PipelineArtifact {
    sha256: Sha256Digest,
    parents: Vec<ArtifactId>,
    valid: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PendingInvalidation {
    cause_digest: Sha256Digest,
    reconciliation_stage_ids: Vec<StageInstanceId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PipelineState {
    pub definition_digest: Sha256Digest,
    pub control_revision: u64,
    nodes: BTreeMap<StageInstanceId, PipelineNodeDefinition>,
    artifacts: BTreeMap<ArtifactId, PipelineArtifact>,
    frozen_stages: BTreeSet<StageInstanceId>,
    invalidated_stages: BTreeSet<StageInstanceId>,
    pending_invalidations: BTreeMap<ArtifactId, PendingInvalidation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PipelineCommand {
    AcceptArtifact {
        expected_revision: u64,
        artifact_id: ArtifactId,
        sha256: Sha256Digest,
        parent_artifact_ids: Vec<ArtifactId>,
    },
    ScheduleStage {
        expected_revision: u64,
        stage_instance_id: StageInstanceId,
    },
    ObserveStageEvent {
        expected_revision: u64,
        event: StageEvent,
    },
    InvalidateArtifact {
        expected_revision: u64,
        artifact_id: ArtifactId,
        cause_digest: Sha256Digest,
    },
    FinalizeInvalidation {
        expected_revision: u64,
        root_artifact_id: ArtifactId,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PipelineEvent {
    pub prior_control_revision: u64,
    pub kind: PipelineEventKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event_type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PipelineEventKind {
    ArtifactAccepted {
        artifact_id: ArtifactId,
        sha256: Sha256Digest,
        parent_artifact_ids: Vec<ArtifactId>,
    },
    StageScheduled {
        stage_event: StageEvent,
        input_manifest: StageInputManifest,
    },
    StageEventObserved {
        stage_event: StageEvent,
    },
    ArtifactInvalidated {
        root_artifact_id: ArtifactId,
        invalidated_artifact_ids: Vec<ArtifactId>,
        frozen_stage_ids: Vec<StageInstanceId>,
        invalidated_stage_ids: Vec<StageInstanceId>,
        reconciliation_stage_ids: Vec<StageInstanceId>,
        invalidated_stage_events: Vec<StageEvent>,
        cause_digest: Sha256Digest,
    },
    ArtifactInvalidationFinalized {
        root_artifact_id: ArtifactId,
        invalidated_stage_ids: Vec<StageInstanceId>,
        cause_digest: Sha256Digest,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PipelineTransitionError {
    Definition(PipelineDefinitionError),
    StaleRevision { expected: u64, actual: u64 },
    RevisionOverflow,
    UnknownStage(StageInstanceId),
    UnknownArtifact(ArtifactId),
    ArtifactAlreadyExists(ArtifactId),
    ArtifactAlreadyInvalidated(ArtifactId),
    DuplicateParent(ArtifactId),
    InvalidParent(ArtifactId),
    DependencyNotCompleted(StageInstanceId),
    InputArtifactUnavailable(ArtifactId),
    StageNotPending,
    StageInvalidated(StageInstanceId),
    SchedulerOwnedStageEvent,
    StageTransition(StageTransitionError),
    StageFrozen(StageInstanceId),
    InvalidationNotPending(ArtifactId),
    InvalidationReconciliationRequired(StageInstanceId),
    EventMismatch,
    Canonicalization(canonical_json::CanonicalJsonError),
}

impl PipelineState {
    pub fn new(
        definition_digest: Sha256Digest,
        definitions: Vec<PipelineNodeDefinition>,
    ) -> Result<Self, PipelineDefinitionError> {
        if definitions.is_empty() {
            return Err(PipelineDefinitionError::Empty);
        }
        let mut nodes = BTreeMap::new();
        for mut definition in definitions {
            let stage_id = definition.stage.stage_instance_id.clone();
            if !definition.stage.is_pristine() {
                return Err(PipelineDefinitionError::NonPristineStage(stage_id));
            }
            sort_unique_dependencies(&stage_id, &mut definition.needs)?;
            sort_unique_inputs(&mut definition.required_input_artifact_ids)?;
            if nodes.insert(stage_id.clone(), definition).is_some() {
                return Err(PipelineDefinitionError::DuplicateStage(stage_id));
            }
        }
        for definition in nodes.values() {
            for dependency in &definition.needs {
                if !nodes.contains_key(dependency) {
                    return Err(PipelineDefinitionError::UnknownDependency(
                        dependency.clone(),
                    ));
                }
            }
        }
        verify_acyclic(&nodes)?;
        Ok(Self {
            definition_digest,
            control_revision: 0,
            nodes,
            artifacts: BTreeMap::new(),
            frozen_stages: BTreeSet::new(),
            invalidated_stages: BTreeSet::new(),
            pending_invalidations: BTreeMap::new(),
        })
    }

    pub fn is_pristine(&self) -> bool {
        if self.control_revision != 0
            || !self.artifacts.is_empty()
            || !self.frozen_stages.is_empty()
            || !self.invalidated_stages.is_empty()
            || !self.pending_invalidations.is_empty()
        {
            return false;
        }
        let definitions = self.nodes.values().cloned().collect();
        Self::new(self.definition_digest, definitions).is_ok_and(|initial| initial == *self)
    }

    pub fn stage(&self, stage_instance_id: &StageInstanceId) -> Option<&StageState> {
        self.nodes.get(stage_instance_id).map(|node| &node.stage)
    }

    pub fn stage_is_frozen(&self, stage_instance_id: &StageInstanceId) -> bool {
        self.frozen_stages.contains(stage_instance_id)
    }

    pub fn stage_is_invalidated(&self, stage_instance_id: &StageInstanceId) -> bool {
        self.invalidated_stages.contains(stage_instance_id)
    }

    pub fn artifact_is_valid(&self, artifact_id: &ArtifactId) -> bool {
        self.artifacts
            .get(artifact_id)
            .is_some_and(|artifact| artifact.valid)
    }

    pub fn decide(
        &self,
        command: PipelineCommand,
    ) -> Result<PipelineEvent, PipelineTransitionError> {
        let expected_revision = command.expected_revision();
        if expected_revision != self.control_revision {
            return Err(PipelineTransitionError::StaleRevision {
                expected: expected_revision,
                actual: self.control_revision,
            });
        }
        let kind = match command {
            PipelineCommand::AcceptArtifact {
                artifact_id,
                sha256,
                mut parent_artifact_ids,
                ..
            } => {
                self.validate_artifact(&artifact_id, &mut parent_artifact_ids)?;
                PipelineEventKind::ArtifactAccepted {
                    artifact_id,
                    sha256,
                    parent_artifact_ids,
                }
            }
            PipelineCommand::ScheduleStage {
                stage_instance_id, ..
            } => {
                let (stage_event, input_manifest) = self.schedule(&stage_instance_id)?;
                PipelineEventKind::StageScheduled {
                    stage_event,
                    input_manifest,
                }
            }
            PipelineCommand::ObserveStageEvent { event, .. } => {
                if matches!(
                    event.kind,
                    StageEventKind::NodeReady { .. } | StageEventKind::NodeInvalidated { .. }
                ) {
                    return Err(PipelineTransitionError::SchedulerOwnedStageEvent);
                }
                self.apply_observed_stage_event(&event)?;
                PipelineEventKind::StageEventObserved { stage_event: event }
            }
            PipelineCommand::InvalidateArtifact {
                artifact_id,
                cause_digest,
                ..
            } => self.invalidation(&artifact_id, cause_digest)?,
            PipelineCommand::FinalizeInvalidation {
                root_artifact_id, ..
            } => self.finalize_invalidation(&root_artifact_id)?,
        };
        let event = PipelineEvent {
            prior_control_revision: self.control_revision,
            kind,
        };
        self.apply(&event)?;
        Ok(event)
    }

    pub fn apply(&self, event: &PipelineEvent) -> Result<Self, PipelineTransitionError> {
        if event.prior_control_revision != self.control_revision {
            return Err(PipelineTransitionError::StaleRevision {
                expected: event.prior_control_revision,
                actual: self.control_revision,
            });
        }
        if self.control_revision >= MAX_CONTROL_REVISION {
            return Err(PipelineTransitionError::RevisionOverflow);
        }
        let mut next = self.clone();
        match &event.kind {
            PipelineEventKind::ArtifactAccepted {
                artifact_id,
                sha256,
                parent_artifact_ids,
            } => {
                let mut parents = parent_artifact_ids.clone();
                self.validate_artifact(artifact_id, &mut parents)?;
                if &parents != parent_artifact_ids {
                    return Err(PipelineTransitionError::EventMismatch);
                }
                next.artifacts.insert(
                    artifact_id.clone(),
                    PipelineArtifact {
                        sha256: *sha256,
                        parents,
                        valid: true,
                    },
                );
            }
            PipelineEventKind::StageScheduled {
                stage_event,
                input_manifest,
            } => {
                let (expected_event, expected_manifest) =
                    self.schedule(&stage_event.stage_instance_id)?;
                if stage_event != &expected_event || input_manifest != &expected_manifest {
                    return Err(PipelineTransitionError::EventMismatch);
                }
                let node = next
                    .nodes
                    .get_mut(&stage_event.stage_instance_id)
                    .ok_or_else(|| {
                        PipelineTransitionError::UnknownStage(stage_event.stage_instance_id.clone())
                    })?;
                node.stage = node
                    .stage
                    .apply(stage_event)
                    .map_err(PipelineTransitionError::StageTransition)?;
            }
            PipelineEventKind::StageEventObserved { stage_event } => {
                if matches!(
                    stage_event.kind,
                    StageEventKind::NodeReady { .. } | StageEventKind::NodeInvalidated { .. }
                ) {
                    return Err(PipelineTransitionError::SchedulerOwnedStageEvent);
                }
                self.apply_observed_stage_event(stage_event)?;
                let node = next
                    .nodes
                    .get_mut(&stage_event.stage_instance_id)
                    .ok_or_else(|| {
                        PipelineTransitionError::UnknownStage(stage_event.stage_instance_id.clone())
                    })?;
                node.stage = node
                    .stage
                    .apply(stage_event)
                    .map_err(PipelineTransitionError::StageTransition)?;
            }
            PipelineEventKind::ArtifactInvalidated {
                root_artifact_id,
                invalidated_artifact_ids,
                frozen_stage_ids,
                invalidated_stage_ids,
                reconciliation_stage_ids,
                invalidated_stage_events,
                cause_digest,
            } => {
                let expected = self.invalidation(root_artifact_id, *cause_digest)?;
                if expected.ne(&event.kind) {
                    return Err(PipelineTransitionError::EventMismatch);
                }
                for artifact_id in invalidated_artifact_ids {
                    next.artifacts
                        .get_mut(artifact_id)
                        .ok_or_else(|| {
                            PipelineTransitionError::UnknownArtifact(artifact_id.clone())
                        })?
                        .valid = false;
                }
                for stage_id in frozen_stage_ids {
                    next.frozen_stages.insert(stage_id.clone());
                }
                for stage_id in invalidated_stage_ids {
                    next.invalidated_stages.insert(stage_id.clone());
                }
                if !reconciliation_stage_ids.is_empty() {
                    next.pending_invalidations.insert(
                        root_artifact_id.clone(),
                        PendingInvalidation {
                            cause_digest: *cause_digest,
                            reconciliation_stage_ids: reconciliation_stage_ids.clone(),
                        },
                    );
                }
                for stage_event in invalidated_stage_events {
                    let node = next
                        .nodes
                        .get_mut(&stage_event.stage_instance_id)
                        .ok_or_else(|| {
                            PipelineTransitionError::UnknownStage(
                                stage_event.stage_instance_id.clone(),
                            )
                        })?;
                    node.stage = node
                        .stage
                        .apply(stage_event)
                        .map_err(PipelineTransitionError::StageTransition)?;
                }
            }
            PipelineEventKind::ArtifactInvalidationFinalized {
                root_artifact_id,
                invalidated_stage_ids,
                cause_digest,
            } => {
                let expected = self.finalize_invalidation(root_artifact_id)?;
                if expected.ne(&event.kind) {
                    return Err(PipelineTransitionError::EventMismatch);
                }
                let pending = next
                    .pending_invalidations
                    .remove(root_artifact_id)
                    .ok_or_else(|| {
                        PipelineTransitionError::InvalidationNotPending(root_artifact_id.clone())
                    })?;
                if pending.cause_digest != *cause_digest {
                    return Err(PipelineTransitionError::EventMismatch);
                }
                for stage_id in invalidated_stage_ids {
                    next.invalidated_stages.insert(stage_id.clone());
                }
            }
        }
        next.control_revision += 1;
        Ok(next)
    }

    fn validate_artifact(
        &self,
        artifact_id: &ArtifactId,
        parent_artifact_ids: &mut [ArtifactId],
    ) -> Result<(), PipelineTransitionError> {
        if self.artifacts.contains_key(artifact_id) {
            return Err(PipelineTransitionError::ArtifactAlreadyExists(
                artifact_id.clone(),
            ));
        }
        parent_artifact_ids.sort();
        for pair in parent_artifact_ids.windows(2) {
            if pair[0] == pair[1] {
                return Err(PipelineTransitionError::DuplicateParent(pair[0].clone()));
            }
        }
        for parent in parent_artifact_ids.iter() {
            if parent == artifact_id
                || !self
                    .artifacts
                    .get(parent)
                    .is_some_and(|artifact| artifact.valid)
            {
                return Err(PipelineTransitionError::InvalidParent(parent.clone()));
            }
        }
        Ok(())
    }

    fn schedule(
        &self,
        stage_instance_id: &StageInstanceId,
    ) -> Result<(StageEvent, StageInputManifest), PipelineTransitionError> {
        if self.frozen_stages.contains(stage_instance_id) {
            return Err(PipelineTransitionError::StageFrozen(
                stage_instance_id.clone(),
            ));
        }
        let node = self
            .nodes
            .get(stage_instance_id)
            .ok_or_else(|| PipelineTransitionError::UnknownStage(stage_instance_id.clone()))?;
        if node.stage.phase != StagePhase::Pending {
            return Err(PipelineTransitionError::StageNotPending);
        }
        for dependency in &node.needs {
            if self.nodes.get(dependency).map(|node| node.stage.phase)
                != Some(StagePhase::Completed)
            {
                return Err(PipelineTransitionError::DependencyNotCompleted(
                    dependency.clone(),
                ));
            }
        }
        let mut artifacts = Vec::with_capacity(node.required_input_artifact_ids.len());
        for artifact_id in &node.required_input_artifact_ids {
            let artifact = self
                .artifacts
                .get(artifact_id)
                .filter(|artifact| artifact.valid)
                .ok_or_else(|| {
                    PipelineTransitionError::InputArtifactUnavailable(artifact_id.clone())
                })?;
            artifacts.push(InputManifestArtifact {
                artifact_id: artifact_id.clone(),
                sha256: artifact.sha256,
            });
        }
        let input_manifest = StageInputManifest {
            protocol: BASE_PROTOCOL.to_string(),
            stage_instance_id: stage_instance_id.clone(),
            artifacts,
        };
        let input_manifest_digest = input_manifest
            .digest()
            .map_err(PipelineTransitionError::Canonicalization)?;
        let stage_event = node
            .stage
            .decide(crate::StageCommand::AcceptInputs {
                expected_revision: node.stage.control_revision,
                input_manifest_digest,
            })
            .map_err(PipelineTransitionError::StageTransition)?;
        Ok((stage_event, input_manifest))
    }

    fn apply_observed_stage_event(
        &self,
        event: &StageEvent,
    ) -> Result<(), PipelineTransitionError> {
        if self.invalidated_stages.contains(&event.stage_instance_id) {
            return Err(PipelineTransitionError::StageInvalidated(
                event.stage_instance_id.clone(),
            ));
        }
        if self.frozen_stages.contains(&event.stage_instance_id)
            && !matches!(event.kind, StageEventKind::NodePaused { .. })
        {
            return Err(PipelineTransitionError::StageFrozen(
                event.stage_instance_id.clone(),
            ));
        }
        self.nodes
            .get(&event.stage_instance_id)
            .ok_or_else(|| PipelineTransitionError::UnknownStage(event.stage_instance_id.clone()))?
            .stage
            .apply(event)
            .map(|_| ())
            .map_err(PipelineTransitionError::StageTransition)
    }

    fn invalidation(
        &self,
        artifact_id: &ArtifactId,
        cause_digest: Sha256Digest,
    ) -> Result<PipelineEventKind, PipelineTransitionError> {
        let root = self
            .artifacts
            .get(artifact_id)
            .ok_or_else(|| PipelineTransitionError::UnknownArtifact(artifact_id.clone()))?;
        if !root.valid {
            return Err(PipelineTransitionError::ArtifactAlreadyInvalidated(
                artifact_id.clone(),
            ));
        }
        let mut affected_artifacts = BTreeSet::from([artifact_id.clone()]);
        loop {
            let before = affected_artifacts.len();
            for (candidate_id, candidate) in &self.artifacts {
                if candidate.valid
                    && candidate
                        .parents
                        .iter()
                        .any(|parent| affected_artifacts.contains(parent))
                {
                    affected_artifacts.insert(candidate_id.clone());
                }
            }
            if affected_artifacts.len() == before {
                break;
            }
        }

        let mut affected_stages = BTreeSet::new();
        loop {
            let before = affected_stages.len();
            for (stage_id, node) in &self.nodes {
                if node
                    .required_input_artifact_ids
                    .iter()
                    .any(|input| affected_artifacts.contains(input))
                    || node.needs.iter().any(|need| affected_stages.contains(need))
                {
                    affected_stages.insert(stage_id.clone());
                }
            }
            if affected_stages.len() == before {
                break;
            }
        }

        let frozen_stage_ids = affected_stages.iter().cloned().collect::<Vec<_>>();
        let mut invalidated_stage_ids = Vec::new();
        let mut reconciliation_stage_ids = Vec::new();
        let mut invalidated_stage_events = Vec::new();
        for stage_id in affected_stages {
            let stage = &self.nodes[&stage_id].stage;
            match stage.phase {
                StagePhase::Completed => {
                    invalidated_stage_ids.push(stage_id);
                    invalidated_stage_events.push(
                        stage
                            .decide(crate::StageCommand::Invalidate {
                                expected_revision: stage.control_revision,
                                cause_digest,
                            })
                            .map_err(PipelineTransitionError::StageTransition)?,
                    );
                }
                StagePhase::Pending
                | StagePhase::Ready
                | StagePhase::Failed
                | StagePhase::Paused
                | StagePhase::Invalidated => invalidated_stage_ids.push(stage_id),
                StagePhase::Provisioning | StagePhase::Running | StagePhase::Blocked => {
                    reconciliation_stage_ids.push(stage_id);
                }
            }
        }
        Ok(PipelineEventKind::ArtifactInvalidated {
            root_artifact_id: artifact_id.clone(),
            invalidated_artifact_ids: affected_artifacts.into_iter().collect(),
            frozen_stage_ids,
            invalidated_stage_ids,
            reconciliation_stage_ids,
            invalidated_stage_events,
            cause_digest,
        })
    }

    fn finalize_invalidation(
        &self,
        root_artifact_id: &ArtifactId,
    ) -> Result<PipelineEventKind, PipelineTransitionError> {
        let pending = self
            .pending_invalidations
            .get(root_artifact_id)
            .ok_or_else(|| {
                PipelineTransitionError::InvalidationNotPending(root_artifact_id.clone())
            })?;
        for stage_id in &pending.reconciliation_stage_ids {
            if self.nodes[stage_id].stage.phase != StagePhase::Paused {
                return Err(PipelineTransitionError::InvalidationReconciliationRequired(
                    stage_id.clone(),
                ));
            }
        }
        Ok(PipelineEventKind::ArtifactInvalidationFinalized {
            root_artifact_id: root_artifact_id.clone(),
            invalidated_stage_ids: pending.reconciliation_stage_ids.clone(),
            cause_digest: pending.cause_digest,
        })
    }
}

impl PipelineCommand {
    fn expected_revision(&self) -> u64 {
        match self {
            Self::AcceptArtifact {
                expected_revision, ..
            }
            | Self::ScheduleStage {
                expected_revision, ..
            }
            | Self::ObserveStageEvent {
                expected_revision, ..
            }
            | Self::InvalidateArtifact {
                expected_revision, ..
            }
            | Self::FinalizeInvalidation {
                expected_revision, ..
            } => *expected_revision,
        }
    }
}

fn sort_unique_dependencies(
    stage_id: &StageInstanceId,
    dependencies: &mut [StageInstanceId],
) -> Result<(), PipelineDefinitionError> {
    dependencies.sort();
    for dependency in dependencies.iter() {
        if dependency == stage_id {
            return Err(PipelineDefinitionError::SelfDependency(stage_id.clone()));
        }
    }
    for pair in dependencies.windows(2) {
        if pair[0] == pair[1] {
            return Err(PipelineDefinitionError::DuplicateDependency(
                pair[0].clone(),
            ));
        }
    }
    Ok(())
}

fn sort_unique_inputs(inputs: &mut [ArtifactId]) -> Result<(), PipelineDefinitionError> {
    inputs.sort();
    for pair in inputs.windows(2) {
        if pair[0] == pair[1] {
            return Err(PipelineDefinitionError::DuplicateInput(pair[0].clone()));
        }
    }
    Ok(())
}

fn verify_acyclic(
    nodes: &BTreeMap<StageInstanceId, PipelineNodeDefinition>,
) -> Result<(), PipelineDefinitionError> {
    let mut resolved = BTreeSet::new();
    while resolved.len() < nodes.len() {
        let before = resolved.len();
        for (stage_id, node) in nodes {
            if !resolved.contains(stage_id) && node.needs.iter().all(|need| resolved.contains(need))
            {
                resolved.insert(stage_id.clone());
            }
        }
        if resolved.len() == before {
            return Err(PipelineDefinitionError::Cycle);
        }
    }
    Ok(())
}

impl fmt::Display for PipelineTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Definition(error) => write!(formatter, "invalid pipeline definition: {error:?}"),
            Self::StaleRevision { expected, actual } => write!(
                formatter,
                "stale pipeline revision {expected}; current revision is {actual}"
            ),
            Self::RevisionOverflow => formatter.write_str("pipeline control revision overflow"),
            Self::UnknownStage(stage) => write!(formatter, "unknown stage {stage}"),
            Self::UnknownArtifact(artifact) => write!(formatter, "unknown artifact {artifact}"),
            Self::ArtifactAlreadyExists(artifact) => {
                write!(formatter, "artifact already exists: {artifact}")
            }
            Self::ArtifactAlreadyInvalidated(artifact) => {
                write!(formatter, "artifact is already invalidated: {artifact}")
            }
            Self::DuplicateParent(parent) => {
                write!(formatter, "duplicate artifact parent {parent}")
            }
            Self::InvalidParent(parent) => write!(formatter, "invalid artifact parent {parent}"),
            Self::DependencyNotCompleted(stage) => {
                write!(formatter, "dependency is not completed: {stage}")
            }
            Self::InputArtifactUnavailable(artifact) => {
                write!(formatter, "input artifact is unavailable: {artifact}")
            }
            Self::StageNotPending => formatter.write_str("only a pending stage can be scheduled"),
            Self::StageInvalidated(stage) => write!(formatter, "stage is invalidated: {stage}"),
            Self::SchedulerOwnedStageEvent => {
                formatter.write_str("stage event is owned by the pipeline scheduler")
            }
            Self::StageTransition(error) => error.fmt(formatter),
            Self::StageFrozen(stage) => write!(formatter, "stage is frozen: {stage}"),
            Self::InvalidationNotPending(artifact) => {
                write!(
                    formatter,
                    "artifact has no pending invalidation: {artifact}"
                )
            }
            Self::InvalidationReconciliationRequired(stage) => write!(
                formatter,
                "stage must be paused before invalidation can finalize: {stage}"
            ),
            Self::EventMismatch => {
                formatter.write_str("pipeline event does not match deterministic decision")
            }
            Self::Canonicalization(error) => error.fmt(formatter),
        }
    }
}

pub fn replay_pipeline<'a>(
    initial: &PipelineState,
    events: impl IntoIterator<Item = &'a PipelineEvent>,
) -> Result<PipelineState, PipelineTransitionError> {
    events
        .into_iter()
        .try_fold(initial.clone(), |state, event| state.apply(event))
}

#[cfg(test)]
mod tests {
    use alloc::{format, vec};

    use super::*;
    use crate::StageCommand;

    const A: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const B: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
    const C: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";

    fn stage(value: &str) -> StageState {
        StageState::new(
            StageInstanceId::parse(format!("stage_{value}")).unwrap(),
            Sha256Digest::of_bytes(format!("component-{value}").as_bytes()),
            Sha256Digest::of_bytes(format!("predicate-{value}").as_bytes()),
        )
    }

    fn artifact(value: &str) -> ArtifactId {
        ArtifactId::parse(format!("art_{value}")).unwrap()
    }

    fn definition(
        value: &str,
        needs: Vec<StageInstanceId>,
        inputs: Vec<ArtifactId>,
    ) -> PipelineNodeDefinition {
        PipelineNodeDefinition {
            stage: stage(value),
            needs,
            required_input_artifact_ids: inputs,
        }
    }

    fn apply_command(
        state: &mut PipelineState,
        events: &mut Vec<PipelineEvent>,
        command: PipelineCommand,
    ) -> Result<(), PipelineTransitionError> {
        let event = state.decide(command)?;
        *state = state.apply(&event)?;
        events.push(event);
        Ok(())
    }

    fn observe(
        state: &mut PipelineState,
        events: &mut Vec<PipelineEvent>,
        stage_event: StageEvent,
    ) {
        apply_command(
            state,
            events,
            PipelineCommand::ObserveStageEvent {
                expected_revision: state.control_revision,
                event: stage_event,
            },
        )
        .unwrap();
    }

    fn complete_stage(
        state: &mut PipelineState,
        events: &mut Vec<PipelineEvent>,
        stage_id: &StageInstanceId,
        output: Sha256Digest,
    ) {
        let ready = state.stage(stage_id).unwrap();
        let provisioning = ready
            .decide(StageCommand::BeginProvisioning {
                expected_revision: ready.control_revision,
            })
            .unwrap();
        observe(state, events, provisioning);
        let provisioning = state.stage(stage_id).unwrap();
        let started = provisioning
            .decide(StageCommand::StartAttempt {
                expected_revision: provisioning.control_revision,
            })
            .unwrap();
        observe(state, events, started);
        let running = state.stage(stage_id).unwrap();
        let completed = running
            .decide(StageCommand::Complete {
                expected_revision: running.control_revision,
                output_manifest_digest: output,
                completion_predicate_digest: running.completion_predicate_digest,
                completion_evidence_digest: Sha256Digest::of_bytes(b"evidence"),
            })
            .unwrap();
        observe(state, events, completed);
    }

    #[test]
    fn definition_rejects_unknown_dependencies_cycles_and_duplicates() {
        let a = stage(A).stage_instance_id;
        let b = stage(B).stage_instance_id;
        assert!(matches!(
            PipelineState::new(
                Sha256Digest::of_bytes(b"pipeline"),
                vec![definition(A, vec![b.clone()], vec![])]
            ),
            Err(PipelineDefinitionError::UnknownDependency(id)) if id == b
        ));
        assert!(matches!(
            PipelineState::new(
                Sha256Digest::of_bytes(b"pipeline"),
                vec![
                    definition(A, vec![b.clone()], vec![]),
                    definition(B, vec![a.clone()], vec![]),
                ]
            ),
            Err(PipelineDefinitionError::Cycle)
        ));
        let input = artifact(C);
        assert!(matches!(
            PipelineState::new(
                Sha256Digest::of_bytes(b"pipeline"),
                vec![definition(A, vec![], vec![input.clone(), input.clone()])]
            ),
            Err(PipelineDefinitionError::DuplicateInput(id)) if id == input
        ));
    }

    #[test]
    fn scheduling_requires_completed_dependencies_and_verified_exact_inputs() {
        let root_id = stage(A).stage_instance_id;
        let child_id = stage(B).stage_instance_id;
        let input_id = artifact(C);
        let initial = PipelineState::new(
            Sha256Digest::of_bytes(b"pipeline"),
            vec![
                definition(A, vec![], vec![]),
                definition(B, vec![root_id.clone()], vec![input_id.clone()]),
            ],
        )
        .unwrap();
        let mut state = initial.clone();
        let mut events = Vec::new();

        assert!(matches!(
            state.decide(PipelineCommand::ScheduleStage {
                expected_revision: 0,
                stage_instance_id: child_id.clone(),
            }),
            Err(PipelineTransitionError::DependencyNotCompleted(id)) if id == root_id
        ));
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::ScheduleStage {
                expected_revision: 0,
                stage_instance_id: root_id.clone(),
            },
        )
        .unwrap();
        let PipelineEventKind::StageScheduled { input_manifest, .. } = &events[0].kind else {
            panic!("expected scheduling event");
        };
        assert!(input_manifest.artifacts.is_empty());
        complete_stage(
            &mut state,
            &mut events,
            &root_id,
            Sha256Digest::of_bytes(b"root-output"),
        );
        assert!(matches!(
            state.decide(PipelineCommand::ScheduleStage {
                expected_revision: state.control_revision,
                stage_instance_id: child_id.clone(),
            }),
            Err(PipelineTransitionError::InputArtifactUnavailable(id)) if id == input_id
        ));
        let revision = state.control_revision;
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::AcceptArtifact {
                expected_revision: revision,
                artifact_id: input_id.clone(),
                sha256: Sha256Digest::of_bytes(b"root-output"),
                parent_artifact_ids: vec![],
            },
        )
        .unwrap();
        let revision = state.control_revision;
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::ScheduleStage {
                expected_revision: revision,
                stage_instance_id: child_id.clone(),
            },
        )
        .unwrap();

        let PipelineEventKind::StageScheduled {
            input_manifest,
            stage_event,
        } = &events.last().unwrap().kind
        else {
            panic!("expected scheduling event");
        };
        assert_eq!(input_manifest.artifacts[0].artifact_id, input_id);
        assert_eq!(
            stage_event.kind,
            StageEventKind::NodeReady {
                input_manifest_digest: input_manifest.digest().unwrap()
            }
        );
        assert_eq!(replay_pipeline(&initial, &events).unwrap(), state);
    }

    #[test]
    fn invalidation_propagates_through_artifacts_and_stage_dependencies() {
        let root_id = stage(A).stage_instance_id;
        let child_id = stage(B).stage_instance_id;
        let later_id = stage(C).stage_instance_id;
        let input_id = artifact(A);
        let child_output_id = artifact(B);
        let initial = PipelineState::new(
            Sha256Digest::of_bytes(b"pipeline"),
            vec![
                definition(A, vec![], vec![]),
                definition(B, vec![root_id.clone()], vec![input_id.clone()]),
                definition(C, vec![child_id.clone()], vec![child_output_id.clone()]),
            ],
        )
        .unwrap();
        let mut state = initial.clone();
        let mut events = Vec::new();
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::ScheduleStage {
                expected_revision: 0,
                stage_instance_id: root_id.clone(),
            },
        )
        .unwrap();
        complete_stage(
            &mut state,
            &mut events,
            &root_id,
            Sha256Digest::of_bytes(b"a"),
        );
        let revision = state.control_revision;
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::AcceptArtifact {
                expected_revision: revision,
                artifact_id: input_id.clone(),
                sha256: Sha256Digest::of_bytes(b"a"),
                parent_artifact_ids: vec![],
            },
        )
        .unwrap();
        let revision = state.control_revision;
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::ScheduleStage {
                expected_revision: revision,
                stage_instance_id: child_id.clone(),
            },
        )
        .unwrap();
        complete_stage(
            &mut state,
            &mut events,
            &child_id,
            Sha256Digest::of_bytes(b"b"),
        );
        let revision = state.control_revision;
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::AcceptArtifact {
                expected_revision: revision,
                artifact_id: child_output_id.clone(),
                sha256: Sha256Digest::of_bytes(b"b"),
                parent_artifact_ids: vec![input_id.clone()],
            },
        )
        .unwrap();
        let revision = state.control_revision;
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::InvalidateArtifact {
                expected_revision: revision,
                artifact_id: input_id.clone(),
                cause_digest: Sha256Digest::of_bytes(b"upstream changed"),
            },
        )
        .unwrap();

        assert!(!state.artifact_is_valid(&input_id));
        assert!(!state.artifact_is_valid(&child_output_id));
        assert!(matches!(
            state.decide(PipelineCommand::InvalidateArtifact {
                expected_revision: state.control_revision,
                artifact_id: input_id.clone(),
                cause_digest: Sha256Digest::of_bytes(b"second change"),
            }),
            Err(PipelineTransitionError::ArtifactAlreadyInvalidated(id)) if id == input_id
        ));
        assert_eq!(
            state.stage(&child_id).unwrap().phase,
            StagePhase::Invalidated
        );
        assert_eq!(state.stage(&later_id).unwrap().phase, StagePhase::Pending);
        assert!(state.stage_is_invalidated(&later_id));
        assert!(matches!(
            state.decide(PipelineCommand::ScheduleStage {
                expected_revision: state.control_revision,
                stage_instance_id: later_id.clone(),
            }),
            Err(PipelineTransitionError::StageFrozen(id)) if id == later_id
        ));
        assert_eq!(replay_pipeline(&initial, &events).unwrap(), state);
    }

    #[test]
    fn invalidation_freezes_pending_consumers_while_active_consumers_reconcile() {
        let active_id = stage(A).stage_instance_id;
        let pending_id = stage(C).stage_instance_id;
        let input_id = artifact(B);
        let initial = PipelineState::new(
            Sha256Digest::of_bytes(b"pipeline"),
            vec![
                definition(A, vec![], vec![input_id.clone()]),
                definition(C, vec![], vec![input_id.clone()]),
            ],
        )
        .unwrap();
        let mut state = initial.clone();
        let mut events = Vec::new();
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::AcceptArtifact {
                expected_revision: 0,
                artifact_id: input_id.clone(),
                sha256: Sha256Digest::of_bytes(b"input"),
                parent_artifact_ids: vec![],
            },
        )
        .unwrap();
        let revision = state.control_revision;
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::ScheduleStage {
                expected_revision: revision,
                stage_instance_id: active_id.clone(),
            },
        )
        .unwrap();
        let ready = state.stage(&active_id).unwrap();
        let provisioning = ready
            .decide(StageCommand::BeginProvisioning {
                expected_revision: ready.control_revision,
            })
            .unwrap();
        observe(&mut state, &mut events, provisioning);
        let revision = state.control_revision;
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::InvalidateArtifact {
                expected_revision: revision,
                artifact_id: input_id.clone(),
                cause_digest: Sha256Digest::of_bytes(b"change"),
            },
        )
        .unwrap();

        assert!(!state.artifact_is_valid(&input_id));
        assert!(state.stage_is_frozen(&active_id));
        assert!(state.stage_is_frozen(&pending_id));
        assert!(state.stage_is_invalidated(&pending_id));
        assert!(matches!(
            state.decide(PipelineCommand::ScheduleStage {
                expected_revision: state.control_revision,
                stage_instance_id: pending_id.clone(),
            }),
            Err(PipelineTransitionError::StageFrozen(id)) if id == pending_id
        ));
        assert!(matches!(
            state.decide(PipelineCommand::FinalizeInvalidation {
                expected_revision: state.control_revision,
                root_artifact_id: input_id.clone(),
            }),
            Err(PipelineTransitionError::InvalidationReconciliationRequired(id)) if id == active_id
        ));

        let provisioning = state.stage(&active_id).unwrap();
        let paused = provisioning
            .decide(StageCommand::Pause {
                expected_revision: provisioning.control_revision,
                reason_digest: Sha256Digest::of_bytes(b"upstream changed"),
            })
            .unwrap();
        observe(&mut state, &mut events, paused);
        let revision = state.control_revision;
        apply_command(
            &mut state,
            &mut events,
            PipelineCommand::FinalizeInvalidation {
                expected_revision: revision,
                root_artifact_id: input_id,
            },
        )
        .unwrap();

        assert!(state.stage_is_invalidated(&active_id));
        assert_eq!(state.stage(&active_id).unwrap().phase, StagePhase::Paused);
        assert_eq!(replay_pipeline(&initial, &events).unwrap(), state);
    }

    #[test]
    fn tampered_scheduling_event_and_stale_commands_are_rejected() {
        let stage_id = stage(A).stage_instance_id;
        let state = PipelineState::new(
            Sha256Digest::of_bytes(b"pipeline"),
            vec![definition(A, vec![], vec![])],
        )
        .unwrap();
        let mut event = state
            .decide(PipelineCommand::ScheduleStage {
                expected_revision: 0,
                stage_instance_id: stage_id,
            })
            .unwrap();
        let PipelineEventKind::StageScheduled { input_manifest, .. } = &mut event.kind else {
            panic!("expected scheduling event");
        };
        input_manifest.protocol = "other/v1".into();
        assert_eq!(
            state.apply(&event),
            Err(PipelineTransitionError::EventMismatch)
        );
        assert!(matches!(
            state.decide(PipelineCommand::AcceptArtifact {
                expected_revision: 1,
                artifact_id: artifact(B),
                sha256: Sha256Digest::of_bytes(b"artifact"),
                parent_artifact_ids: vec![],
            }),
            Err(PipelineTransitionError::StaleRevision {
                expected: 1,
                actual: 0
            })
        ));
    }
}
