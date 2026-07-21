use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{Sha256Digest, StageInstanceId};

/// Largest revision that can be represented exactly in RFC 8785's IEEE-754
/// JSON number domain.
pub const MAX_CONTROL_REVISION: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StagePhase {
    Pending,
    Ready,
    Provisioning,
    Running,
    Blocked,
    Completed,
    Failed,
    Paused,
    Invalidated,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StageState {
    pub stage_instance_id: StageInstanceId,
    pub component_digest: Sha256Digest,
    pub completion_predicate_digest: Sha256Digest,
    pub phase: StagePhase,
    pub control_revision: u64,
    pub attempt: u32,
    pub input_manifest_digest: Option<Sha256Digest>,
    pub output_manifest_digest: Option<Sha256Digest>,
    pub completion_evidence_digest: Option<Sha256Digest>,
    pub status_reason_digest: Option<Sha256Digest>,
    pub paused_from_phase: Option<StagePhase>,
    pub paused_from_status_reason_digest: Option<Sha256Digest>,
    pub invalidation_cause_digest: Option<Sha256Digest>,
}

impl StageState {
    pub fn new(
        stage_instance_id: StageInstanceId,
        component_digest: Sha256Digest,
        completion_predicate_digest: Sha256Digest,
    ) -> Self {
        Self {
            stage_instance_id,
            component_digest,
            completion_predicate_digest,
            phase: StagePhase::Pending,
            control_revision: 0,
            attempt: 0,
            input_manifest_digest: None,
            output_manifest_digest: None,
            completion_evidence_digest: None,
            status_reason_digest: None,
            paused_from_phase: None,
            paused_from_status_reason_digest: None,
            invalidation_cause_digest: None,
        }
    }

    /// Returns whether this is exactly the deterministic replay root produced by
    /// [`StageState::new`].
    pub fn is_pristine(&self) -> bool {
        self == &Self::new(
            self.stage_instance_id.clone(),
            self.component_digest,
            self.completion_predicate_digest,
        )
    }

    /// Decides one lifecycle event without mutating state.
    pub fn decide(&self, command: StageCommand) -> Result<StageEvent, StageTransitionError> {
        let expected_revision = command.expected_revision();
        if expected_revision != self.control_revision {
            return Err(StageTransitionError::StaleRevision {
                expected: expected_revision,
                actual: self.control_revision,
            });
        }

        let kind = match command {
            StageCommand::AcceptInputs {
                input_manifest_digest,
                ..
            } => StageEventKind::NodeReady {
                input_manifest_digest,
            },
            StageCommand::BeginProvisioning { .. } => StageEventKind::NodeProvisioning,
            StageCommand::StartAttempt { .. } => {
                let attempt = self
                    .attempt
                    .checked_add(1)
                    .ok_or(StageTransitionError::AttemptOverflow)?;
                StageEventKind::NodeStarted { attempt }
            }
            StageCommand::Block { reason_digest, .. } => {
                StageEventKind::NodeBlocked { reason_digest }
            }
            StageCommand::Resume { .. } => StageEventKind::NodeResumed,
            StageCommand::Complete {
                output_manifest_digest,
                completion_predicate_digest,
                completion_evidence_digest,
                ..
            } => {
                if completion_predicate_digest != self.completion_predicate_digest {
                    return Err(StageTransitionError::CompletionPredicateMismatch);
                }
                StageEventKind::NodeCompleted {
                    output_manifest_digest,
                    completion_predicate_digest,
                    completion_evidence_digest,
                }
            }
            StageCommand::Fail { reason_digest, .. } => {
                StageEventKind::NodeFailed { reason_digest }
            }
            StageCommand::Pause { reason_digest, .. } => {
                StageEventKind::NodePaused { reason_digest }
            }
            StageCommand::ReconcilePause { .. } => {
                let restored_phase = self
                    .paused_from_phase
                    .ok_or(StageTransitionError::MissingPausedState)?;
                StageEventKind::NodeReconciled { restored_phase }
            }
            StageCommand::Invalidate { cause_digest, .. } => {
                StageEventKind::NodeInvalidated { cause_digest }
            }
        };

        let event = StageEvent {
            stage_instance_id: self.stage_instance_id.clone(),
            prior_control_revision: self.control_revision,
            kind,
        };
        self.apply(&event)?;
        Ok(event)
    }

    /// Applies one committed event and returns the next immutable state.
    pub fn apply(&self, event: &StageEvent) -> Result<Self, StageTransitionError> {
        if event.stage_instance_id != self.stage_instance_id {
            return Err(StageTransitionError::WrongStageInstance);
        }
        if event.prior_control_revision != self.control_revision {
            return Err(StageTransitionError::StaleRevision {
                expected: event.prior_control_revision,
                actual: self.control_revision,
            });
        }

        let mut next = self.clone();
        match &event.kind {
            StageEventKind::NodeReady {
                input_manifest_digest,
            } if self.phase == StagePhase::Pending => {
                next.phase = StagePhase::Ready;
                next.input_manifest_digest = Some(*input_manifest_digest);
            }
            StageEventKind::NodeProvisioning if self.phase == StagePhase::Ready => {
                next.phase = StagePhase::Provisioning;
            }
            StageEventKind::NodeStarted { attempt }
                if self.phase == StagePhase::Provisioning
                    && self.attempt.checked_add(1) == Some(*attempt) =>
            {
                next.phase = StagePhase::Running;
                next.attempt = *attempt;
                next.status_reason_digest = None;
            }
            StageEventKind::NodeBlocked { reason_digest } if self.phase == StagePhase::Running => {
                next.phase = StagePhase::Blocked;
                next.status_reason_digest = Some(*reason_digest);
            }
            StageEventKind::NodeResumed if self.phase == StagePhase::Blocked => {
                next.phase = StagePhase::Running;
                next.status_reason_digest = None;
            }
            StageEventKind::NodeCompleted {
                output_manifest_digest,
                completion_predicate_digest,
                completion_evidence_digest,
            } if self.phase == StagePhase::Running
                && *completion_predicate_digest == self.completion_predicate_digest =>
            {
                next.phase = StagePhase::Completed;
                next.output_manifest_digest = Some(*output_manifest_digest);
                next.completion_evidence_digest = Some(*completion_evidence_digest);
                next.status_reason_digest = None;
            }
            StageEventKind::NodeFailed { reason_digest }
                if matches!(self.phase, StagePhase::Running | StagePhase::Blocked) =>
            {
                next.phase = StagePhase::Failed;
                next.status_reason_digest = Some(*reason_digest);
            }
            StageEventKind::NodePaused { reason_digest }
                if matches!(
                    self.phase,
                    StagePhase::Provisioning | StagePhase::Running | StagePhase::Blocked
                ) =>
            {
                next.phase = StagePhase::Paused;
                next.paused_from_phase = Some(self.phase);
                next.paused_from_status_reason_digest = self.status_reason_digest;
                next.status_reason_digest = Some(*reason_digest);
            }
            StageEventKind::NodeReconciled { restored_phase }
                if self.phase == StagePhase::Paused
                    && self.paused_from_phase == Some(*restored_phase)
                    && matches!(
                        restored_phase,
                        StagePhase::Provisioning | StagePhase::Running | StagePhase::Blocked
                    ) =>
            {
                next.phase = *restored_phase;
                next.status_reason_digest = self.paused_from_status_reason_digest;
                next.paused_from_phase = None;
                next.paused_from_status_reason_digest = None;
            }
            StageEventKind::NodeInvalidated { cause_digest }
                if self.phase == StagePhase::Completed =>
            {
                next.phase = StagePhase::Invalidated;
                next.invalidation_cause_digest = Some(*cause_digest);
            }
            _ => {
                return Err(StageTransitionError::InvalidTransition {
                    phase: self.phase,
                    event: event.kind.event_name(),
                });
            }
        }

        if next.control_revision >= MAX_CONTROL_REVISION {
            return Err(StageTransitionError::RevisionOverflow);
        }
        next.control_revision += 1;
        Ok(next)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageCommand {
    AcceptInputs {
        expected_revision: u64,
        input_manifest_digest: Sha256Digest,
    },
    BeginProvisioning {
        expected_revision: u64,
    },
    StartAttempt {
        expected_revision: u64,
    },
    Block {
        expected_revision: u64,
        reason_digest: Sha256Digest,
    },
    Resume {
        expected_revision: u64,
    },
    Complete {
        expected_revision: u64,
        output_manifest_digest: Sha256Digest,
        completion_predicate_digest: Sha256Digest,
        completion_evidence_digest: Sha256Digest,
    },
    Fail {
        expected_revision: u64,
        reason_digest: Sha256Digest,
    },
    Pause {
        expected_revision: u64,
        reason_digest: Sha256Digest,
    },
    ReconcilePause {
        expected_revision: u64,
    },
    Invalidate {
        expected_revision: u64,
        cause_digest: Sha256Digest,
    },
}

impl StageCommand {
    const fn expected_revision(self) -> u64 {
        match self {
            Self::AcceptInputs {
                expected_revision, ..
            }
            | Self::BeginProvisioning { expected_revision }
            | Self::StartAttempt { expected_revision }
            | Self::Block {
                expected_revision, ..
            }
            | Self::Resume { expected_revision }
            | Self::Complete {
                expected_revision, ..
            }
            | Self::Fail {
                expected_revision, ..
            }
            | Self::Pause {
                expected_revision, ..
            }
            | Self::ReconcilePause { expected_revision }
            | Self::Invalidate {
                expected_revision, ..
            } => expected_revision,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StageEvent {
    pub stage_instance_id: StageInstanceId,
    pub prior_control_revision: u64,
    pub kind: StageEventKind,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event_type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StageEventKind {
    NodeReady {
        input_manifest_digest: Sha256Digest,
    },
    NodeProvisioning,
    NodeStarted {
        attempt: u32,
    },
    NodeBlocked {
        reason_digest: Sha256Digest,
    },
    NodeResumed,
    NodeCompleted {
        output_manifest_digest: Sha256Digest,
        completion_predicate_digest: Sha256Digest,
        completion_evidence_digest: Sha256Digest,
    },
    NodeFailed {
        reason_digest: Sha256Digest,
    },
    NodePaused {
        reason_digest: Sha256Digest,
    },
    NodeReconciled {
        restored_phase: StagePhase,
    },
    NodeInvalidated {
        cause_digest: Sha256Digest,
    },
}

impl StageEventKind {
    const fn event_name(self) -> &'static str {
        match self {
            Self::NodeReady { .. } => "NODE_READY",
            Self::NodeProvisioning => "NODE_PROVISIONING",
            Self::NodeStarted { .. } => "NODE_STARTED",
            Self::NodeBlocked { .. } => "NODE_BLOCKED",
            Self::NodeResumed => "NODE_RESUMED",
            Self::NodeCompleted { .. } => "NODE_COMPLETED",
            Self::NodeFailed { .. } => "NODE_FAILED",
            Self::NodePaused { .. } => "NODE_PAUSED",
            Self::NodeReconciled { .. } => "NODE_RECONCILED",
            Self::NodeInvalidated { .. } => "NODE_INVALIDATED",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageTransitionError {
    StaleRevision {
        expected: u64,
        actual: u64,
    },
    WrongStageInstance,
    InvalidTransition {
        phase: StagePhase,
        event: &'static str,
    },
    CompletionPredicateMismatch,
    AttemptOverflow,
    RevisionOverflow,
    MissingPausedState,
}

impl fmt::Display for StageTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleRevision { expected, actual } => {
                write!(
                    formatter,
                    "stale revision {expected}; current revision is {actual}"
                )
            }
            Self::WrongStageInstance => formatter.write_str("event belongs to another stage"),
            Self::InvalidTransition { phase, event } => {
                write!(formatter, "{event} is invalid while stage is {phase:?}")
            }
            Self::CompletionPredicateMismatch => {
                formatter.write_str("completion proof uses an unregistered predicate")
            }
            Self::AttemptOverflow => formatter.write_str("stage attempt overflow"),
            Self::RevisionOverflow => formatter.write_str("stage control revision overflow"),
            Self::MissingPausedState => {
                formatter.write_str("stage has no recorded pre-pause state to reconcile")
            }
        }
    }
}

pub fn replay_stage<'a>(
    initial: &StageState,
    events: impl IntoIterator<Item = &'a StageEvent>,
) -> Result<StageState, StageTransitionError> {
    events
        .into_iter()
        .try_fold(initial.clone(), |state, event| state.apply(event))
}

#[cfg(test)]
mod tests {
    use alloc::{format, vec, vec::Vec};

    use super::*;

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const OTHER_ULID: &str = "01BX5ZZKBKACTAV9WEVGEMMVRZ";

    fn digest(value: &[u8]) -> Sha256Digest {
        Sha256Digest::of_bytes(value)
    }

    fn initial() -> StageState {
        StageState::new(
            format!("stage_{ULID}").parse().unwrap(),
            digest(b"component"),
            digest(b"predicate"),
        )
    }

    fn advance(state: StageState, command: StageCommand) -> (StageState, StageEvent) {
        let event = state.decide(command).unwrap();
        let next = state.apply(&event).unwrap();
        (next, event)
    }

    fn running() -> (StageState, Vec<StageEvent>) {
        let state = initial();
        let (state, ready) = advance(
            state,
            StageCommand::AcceptInputs {
                expected_revision: 0,
                input_manifest_digest: digest(b"input"),
            },
        );
        let (state, provisioning) = advance(
            state,
            StageCommand::BeginProvisioning {
                expected_revision: 1,
            },
        );
        let (state, started) = advance(
            state,
            StageCommand::StartAttempt {
                expected_revision: 2,
            },
        );
        (state, vec![ready, provisioning, started])
    }

    #[test]
    fn advances_through_the_happy_path_and_replays_identically() {
        let initial = initial();
        let (running, mut events) = running();
        let (completed, event) = advance(
            running,
            StageCommand::Complete {
                expected_revision: 3,
                output_manifest_digest: digest(b"output"),
                completion_predicate_digest: digest(b"predicate"),
                completion_evidence_digest: digest(b"evidence"),
            },
        );
        events.push(event);

        assert_eq!(completed.phase, StagePhase::Completed);
        assert_eq!(completed.attempt, 1);
        assert_eq!(completed.output_manifest_digest, Some(digest(b"output")));
        assert_eq!(replay_stage(&initial, &events).unwrap(), completed);
    }

    #[test]
    fn rejects_stale_commands_without_changing_state() {
        let (running, _) = running();
        let before = running.clone();

        assert_eq!(
            running.decide(StageCommand::Block {
                expected_revision: 2,
                reason_digest: digest(b"waiting"),
            }),
            Err(StageTransitionError::StaleRevision {
                expected: 2,
                actual: 3,
            })
        );
        assert_eq!(running, before);
    }

    #[test]
    fn completion_requires_the_registered_predicate() {
        let (running, _) = running();

        assert_eq!(
            running.decide(StageCommand::Complete {
                expected_revision: 3,
                output_manifest_digest: digest(b"output"),
                completion_predicate_digest: digest(b"different-predicate"),
                completion_evidence_digest: digest(b"evidence"),
            }),
            Err(StageTransitionError::CompletionPredicateMismatch)
        );
    }

    #[test]
    fn rejects_invalid_lifecycle_transitions() {
        let state = initial();

        assert!(matches!(
            state.decide(StageCommand::StartAttempt {
                expected_revision: 0
            }),
            Err(StageTransitionError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn blocks_and_resumes_without_starting_another_attempt() {
        let (running, _) = running();
        let (blocked, _) = advance(
            running,
            StageCommand::Block {
                expected_revision: 3,
                reason_digest: digest(b"human-input"),
            },
        );
        let (resumed, _) = advance(
            blocked,
            StageCommand::Resume {
                expected_revision: 4,
            },
        );

        assert_eq!(resumed.phase, StagePhase::Running);
        assert_eq!(resumed.attempt, 1);
        assert_eq!(resumed.status_reason_digest, None);
    }

    #[test]
    fn reconciliation_restores_each_recorded_pre_pause_state() {
        let initial = initial();
        let (running, running_events) = running();
        let provisioning = replay_stage(&initial, &running_events[..2]).unwrap();
        let provisioning_events = running_events[..2].to_vec();
        let (blocked, blocked_event) = advance(
            running.clone(),
            StageCommand::Block {
                expected_revision: running.control_revision,
                reason_digest: digest(b"blocked"),
            },
        );
        let mut blocked_events = running_events.clone();
        blocked_events.push(blocked_event);

        for (source, mut events) in [
            (provisioning, provisioning_events),
            (running, running_events),
            (blocked, blocked_events),
        ] {
            let source_phase = source.phase;
            let source_reason = source.status_reason_digest;
            let source_revision = source.control_revision;
            let (paused, pause_event) = advance(
                source,
                StageCommand::Pause {
                    expected_revision: source_revision,
                    reason_digest: digest(b"reconcile-required"),
                },
            );
            assert_eq!(paused.paused_from_phase, Some(source_phase));
            assert_eq!(
                paused.decide(StageCommand::ReconcilePause {
                    expected_revision: source_revision,
                }),
                Err(StageTransitionError::StaleRevision {
                    expected: source_revision,
                    actual: source_revision + 1,
                })
            );

            let paused_revision = paused.control_revision;
            let (reconciled, reconcile_event) = advance(
                paused,
                StageCommand::ReconcilePause {
                    expected_revision: paused_revision,
                },
            );
            events.push(pause_event);
            events.push(reconcile_event);

            assert_eq!(reconciled.phase, source_phase);
            assert_eq!(reconciled.status_reason_digest, source_reason);
            assert_eq!(reconciled.paused_from_phase, None);
            assert_eq!(reconciled.paused_from_status_reason_digest, None);
            assert_eq!(replay_stage(&initial, &events).unwrap(), reconciled);
        }
    }

    #[test]
    fn completed_outputs_are_immutable_and_only_invalidation_can_follow() {
        let (running, _) = running();
        let (completed, _) = advance(
            running,
            StageCommand::Complete {
                expected_revision: 3,
                output_manifest_digest: digest(b"output"),
                completion_predicate_digest: digest(b"predicate"),
                completion_evidence_digest: digest(b"evidence"),
            },
        );
        assert!(matches!(
            completed.decide(StageCommand::Complete {
                expected_revision: 4,
                output_manifest_digest: digest(b"other-output"),
                completion_predicate_digest: digest(b"predicate"),
                completion_evidence_digest: digest(b"other-evidence"),
            }),
            Err(StageTransitionError::InvalidTransition { .. })
        ));

        let (invalidated, _) = advance(
            completed,
            StageCommand::Invalidate {
                expected_revision: 4,
                cause_digest: digest(b"upstream-change"),
            },
        );
        assert_eq!(invalidated.phase, StagePhase::Invalidated);
        assert_eq!(invalidated.output_manifest_digest, Some(digest(b"output")));
    }

    #[test]
    fn refuses_revisions_outside_the_exact_json_integer_domain() {
        let mut state = initial();
        state.phase = StagePhase::Running;
        state.control_revision = MAX_CONTROL_REVISION;
        let event = StageEvent {
            stage_instance_id: state.stage_instance_id.clone(),
            prior_control_revision: MAX_CONTROL_REVISION,
            kind: StageEventKind::NodeBlocked {
                reason_digest: digest(b"reason"),
            },
        };

        assert_eq!(
            state.apply(&event),
            Err(StageTransitionError::RevisionOverflow)
        );
    }

    #[test]
    fn replay_rejects_duplicate_and_cross_stage_events() {
        let (running, events) = running();
        assert_eq!(
            running.apply(&events[2]),
            Err(StageTransitionError::StaleRevision {
                expected: 2,
                actual: 3,
            })
        );

        let mut wrong_stage = events[0].clone();
        wrong_stage.stage_instance_id = format!("stage_{OTHER_ULID}").parse().unwrap();
        assert_eq!(
            initial().apply(&wrong_stage),
            Err(StageTransitionError::WrongStageInstance)
        );
    }
}
