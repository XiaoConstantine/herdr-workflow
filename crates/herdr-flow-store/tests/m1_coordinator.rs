use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use herdr_flow_core::{
    ArtifactId, BatchId, EventId, GitObjectFormat, GitObjectId, M1PipelineArtifacts,
    M1PipelineStages, M1StageIdentity, MessageId, OperationId, ParticipantPrincipalId, RunId,
    Sha256Digest, StageInstanceId, StagePhase, BASE_PROTOCOL,
};
use herdr_flow_store::{
    AdversarialReviewRegistration, ArtifactStore, ClockError, M1IdSource, M1ReconcileOutcome,
    M1RunIngress, M1StartDescriptor, M1StartOutcome, SqliteStore, StoreError, UnixMillisClock,
};

const IDS: [&str; 32] = [
    "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    "01ARZ3NDEKTSV4RRFFQ69G5FAW",
    "01ARZ3NDEKTSV4RRFFQ69G5FAX",
    "01ARZ3NDEKTSV4RRFFQ69G5FAY",
    "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
    "01ARZ3NDEKTSV4RRFFQ69G5FB0",
    "01ARZ3NDEKTSV4RRFFQ69G5FB1",
    "01ARZ3NDEKTSV4RRFFQ69G5FB2",
    "01ARZ3NDEKTSV4RRFFQ69G5FB3",
    "01ARZ3NDEKTSV4RRFFQ69G5FB4",
    "01ARZ3NDEKTSV4RRFFQ69G5FB5",
    "01ARZ3NDEKTSV4RRFFQ69G5FB6",
    "01ARZ3NDEKTSV4RRFFQ69G5FB7",
    "01ARZ3NDEKTSV4RRFFQ69G5FB8",
    "01ARZ3NDEKTSV4RRFFQ69G5FB9",
    "01ARZ3NDEKTSV4RRFFQ69G5FBA",
    "01ARZ3NDEKTSV4RRFFQ69G5FBB",
    "01ARZ3NDEKTSV4RRFFQ69G5FBC",
    "01ARZ3NDEKTSV4RRFFQ69G5FBD",
    "01ARZ3NDEKTSV4RRFFQ69G5FBE",
    "01ARZ3NDEKTSV4RRFFQ69G5FBF",
    "01ARZ3NDEKTSV4RRFFQ69G5FBG",
    "01ARZ3NDEKTSV4RRFFQ69G5FBH",
    "01ARZ3NDEKTSV4RRFFQ69G5FBJ",
    "01ARZ3NDEKTSV4RRFFQ69G5FBK",
    "01ARZ3NDEKTSV4RRFFQ69G5FBM",
    "01ARZ3NDEKTSV4RRFFQ69G5FBN",
    "01ARZ3NDEKTSV4RRFFQ69G5FBP",
    "01ARZ3NDEKTSV4RRFFQ69G5FBQ",
    "01ARZ3NDEKTSV4RRFFQ69G5FBR",
    "01ARZ3NDEKTSV4RRFFQ69G5FBS",
    "01ARZ3NDEKTSV4RRFFQ69G5FBT",
];

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
    fn now_unix_ms(&self) -> Result<u64, ClockError> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

struct DeterministicIds {
    next: usize,
    fail: bool,
}

impl DeterministicIds {
    fn new(start: usize) -> Self {
        Self {
            next: start,
            fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            next: 0,
            fail: true,
        }
    }

    fn take(&mut self, prefix: &str) -> Result<String, StoreError> {
        if self.fail || self.next >= IDS.len() {
            return Err(StoreError::IdSourceExhausted);
        }
        let value = format!("{prefix}{}", IDS[self.next]);
        self.next += 1;
        Ok(value)
    }
}

impl M1IdSource for DeterministicIds {
    fn next_batch_id(&mut self) -> Result<BatchId, StoreError> {
        BatchId::parse(self.take("batch_")?).map_err(StoreError::Identifier)
    }

    fn next_event_id(&mut self) -> Result<EventId, StoreError> {
        EventId::parse(self.take("evt_")?).map_err(StoreError::Identifier)
    }

    fn next_message_id(&mut self) -> Result<MessageId, StoreError> {
        MessageId::parse(self.take("msg_")?).map_err(StoreError::Identifier)
    }

    fn next_artifact_id(&mut self) -> Result<ArtifactId, StoreError> {
        ArtifactId::parse(self.take("art_")?).map_err(StoreError::Identifier)
    }
}

fn digest(value: &[u8]) -> Sha256Digest {
    Sha256Digest::of_bytes(value)
}

fn stage(index: usize, name: &[u8]) -> M1StageIdentity {
    M1StageIdentity {
        stage_instance_id: StageInstanceId::parse(format!("stage_{}", IDS[index])).unwrap(),
        component_digest: digest(name),
        completion_predicate_digest: digest(&[name, b"-predicate"].concat()),
    }
}

fn descriptor() -> M1StartDescriptor {
    let stages = M1PipelineStages {
        implementation: stage(1, b"implementation"),
        adversarial_review: stage(2, b"review"),
        publication_gate: stage(3, b"gate"),
        publisher: stage(4, b"publisher"),
    };
    M1StartDescriptor {
        protocol: BASE_PROTOCOL.into(),
        run_id: RunId::parse(format!("flow_{}", IDS[0])).unwrap(),
        pipeline_definition_digest: digest(b"m1-definition"),
        artifacts: M1PipelineArtifacts {
            implementation_input: ArtifactId::parse(format!("art_{}", IDS[5])).unwrap(),
            candidate: ArtifactId::parse(format!("art_{}", IDS[6])).unwrap(),
            review_package: ArtifactId::parse(format!("art_{}", IDS[7])).unwrap(),
            publication_authorization: ArtifactId::parse(format!("art_{}", IDS[8])).unwrap(),
        },
        review: AdversarialReviewRegistration {
            stage_instance_id: stages.adversarial_review.stage_instance_id.clone(),
            implementer: ParticipantPrincipalId::parse(format!("principal_{}", IDS[9])).unwrap(),
            reviewers: vec![
                ParticipantPrincipalId::parse(format!("principal_{}", IDS[10])).unwrap(),
            ],
            baseline: GitObjectId::from_hex(GitObjectFormat::Sha1, &"1".repeat(40)).unwrap(),
            evidence_producer_stage_instance_id: stages
                .adversarial_review
                .stage_instance_id
                .clone(),
            evidence_component_digest: stages.adversarial_review.component_digest,
            check_policy_digest: digest(b"checks"),
        },
        stages,
    }
}

fn ingress(bytes: &[u8]) -> M1RunIngress<'_> {
    M1RunIngress {
        artifact_type: "implementation-request/v1",
        schema_id: "implementation-request",
        schema_version: 1,
        media_type: "application/json",
        retention_class: "run-record",
        bytes,
    }
}

fn operation(index: usize) -> OperationId {
    OperationId::parse(format!("op_{}", IDS[index])).unwrap()
}

#[test]
fn exact_start_retry_and_bounded_reconcile_bind_immutable_ingress() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("flow.sqlite3");
    let artifact_store = ArtifactStore::open(directory.path().join("objects")).unwrap();
    let clock = ManualClock::new(1_000);
    let descriptor = descriptor();
    let owner = operation(11);
    let original = br#"{"task":"implement exactly this"}"#.to_vec();
    let original_digest = digest(&original);
    let mut source = original.clone();
    let mut store = SqliteStore::open(&database).unwrap();
    let mut ids = DeterministicIds::new(12);
    let (outcome, mut run) = store
        .start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(&source),
            &owner,
            100,
            &clock,
            &mut ids,
        )
        .unwrap();
    assert_eq!(outcome, M1StartOutcome::Started);
    source.fill(b'x');
    let expected_manifest_digest = herdr_flow_core::StageInputManifest {
        protocol: BASE_PROTOCOL.into(),
        stage_instance_id: descriptor.stages.implementation.stage_instance_id.clone(),
        artifacts: vec![herdr_flow_core::InputManifestArtifact {
            artifact_id: descriptor.artifacts.implementation_input.clone(),
            sha256: original_digest,
        }],
    }
    .digest()
    .unwrap();
    assert_eq!(
        run.reconcile_once(&artifact_store, &mut ids).unwrap(),
        M1ReconcileOutcome::ScheduledImplementation {
            input_manifest_digest: expected_manifest_digest,
        }
    );
    assert_eq!(
        run.reconcile_once(&artifact_store, &mut ids).unwrap(),
        M1ReconcileOutcome::NeedsAgentTransport
    );
    drop(run);
    let stored_ingress = store
        .load_artifact(
            &descriptor.run_id,
            &descriptor.artifacts.implementation_input,
            &artifact_store,
        )
        .unwrap();
    assert_eq!(stored_ingress.record.sha256, original_digest);
    assert_eq!(stored_ingress.record.producer_attempt, 0);
    assert!(stored_ingress.parent_artifact_ids.is_empty());
    let descendants = store
        .artifact_descendants(
            &descriptor.run_id,
            &descriptor.artifacts.implementation_input,
            &artifact_store,
        )
        .unwrap();
    assert_eq!(descendants.len(), 1);
    assert_eq!(
        store
            .load_artifact(&descriptor.run_id, &descendants[0], &artifact_store)
            .unwrap()
            .record
            .artifact_type,
        "stage-input-manifest/v1"
    );

    let mut retry_ids = DeterministicIds::new(20);
    let (outcome, run) = store
        .start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(&original),
            &owner,
            100,
            &clock,
            &mut retry_ids,
        )
        .unwrap();
    assert_eq!(outcome, M1StartOutcome::Resumed);
    drop(run);
    assert!(matches!(
        store.start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(b"different"),
            &owner,
            100,
            &clock,
            &mut retry_ids,
        ),
        Err(StoreError::M1StartConflict)
    ));
    let mut conflicting_descriptor = descriptor.clone();
    conflicting_descriptor.pipeline_definition_digest = digest(b"different-definition");
    assert!(matches!(
        store.start_m1_run(
            &artifact_store,
            &conflicting_descriptor,
            ingress(&original),
            &owner,
            100,
            &clock,
            &mut retry_ids,
        ),
        Err(StoreError::M1StartConflict)
    ));

    drop(store);
    let reopened = SqliteStore::open(&database).unwrap();
    let pipeline = reopened.load_pipeline(&descriptor.run_id).unwrap();
    assert_eq!(
        pipeline
            .stage(&descriptor.stages.implementation.stage_instance_id)
            .unwrap()
            .phase,
        StagePhase::Ready
    );
}

#[test]
fn failed_bootstrap_rolls_back_and_retry_reopens_deterministically() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("flow.sqlite3");
    let artifact_store = ArtifactStore::open(directory.path().join("objects")).unwrap();
    let clock = ManualClock::new(2_000);
    let descriptor = descriptor();
    let owner = operation(11);
    let mut store = SqliteStore::open(&database).unwrap();
    assert!(matches!(
        store.start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(b"input"),
            &owner,
            100,
            &clock,
            &mut DeterministicIds::failing(),
        ),
        Err(StoreError::IdSourceExhausted)
    ));
    drop(store);

    let mut reopened = SqliteStore::open(&database).unwrap();
    assert!(matches!(
        reopened.resume_m1_run(&artifact_store, &descriptor.run_id, &owner, 100, &clock),
        Err(StoreError::RunNotFound)
    ));
    let (outcome, run) = reopened
        .start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(b"input"),
            &owner,
            100,
            &clock,
            &mut DeterministicIds::new(12),
        )
        .unwrap();
    assert_eq!(outcome, M1StartOutcome::Started);
    drop(run);
    drop(reopened);
    let replayed = SqliteStore::open(&database)
        .unwrap()
        .load_pipeline(&descriptor.run_id)
        .unwrap();
    assert_eq!(replayed.control_revision, 1);
}

#[test]
fn reserved_output_id_cannot_be_consumed_by_the_generated_input_manifest() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("flow.sqlite3");
    let artifact_store = ArtifactStore::open(directory.path().join("objects")).unwrap();
    let clock = ManualClock::new(2_500);
    let descriptor = descriptor();
    let mut store = SqliteStore::open(&database).unwrap();
    let (_, mut run) = store
        .start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(b"input"),
            &operation(11),
            100,
            &clock,
            &mut DeterministicIds::new(12),
        )
        .unwrap();
    assert!(matches!(
        run.reconcile_once(&artifact_store, &mut DeterministicIds::new(6)),
        Err(StoreError::ArtifactIdConflict)
    ));
    drop(run);
    assert_eq!(
        store
            .load_pipeline(&descriptor.run_id)
            .unwrap()
            .stage(&descriptor.stages.implementation.stage_instance_id)
            .unwrap()
            .phase,
        StagePhase::Pending
    );
}

#[test]
fn resume_rejects_missing_committed_ingress_bytes_before_returning_a_handle() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("flow.sqlite3");
    let objects = directory.path().join("objects");
    let artifact_store = ArtifactStore::open(&objects).unwrap();
    let clock = ManualClock::new(2_750);
    let descriptor = descriptor();
    let input = b"input";
    let input_digest = digest(input).to_prefixed_string();
    let mut store = SqliteStore::open(&database).unwrap();
    let (_, run) = store
        .start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(input),
            &operation(11),
            100,
            &clock,
            &mut DeterministicIds::new(12),
        )
        .unwrap();
    drop(run);
    std::fs::remove_file(
        objects
            .join("sha256")
            .join(&input_digest["sha256:".len()..]),
    )
    .unwrap();
    assert!(matches!(
        store.resume_m1_run(
            &artifact_store,
            &descriptor.run_id,
            &operation(11),
            100,
            &clock,
        ),
        Err(StoreError::ArtifactStore(_))
    ));
}

#[test]
fn legacy_unscheduled_start_migrates_missing_output_reservations_once() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("flow.sqlite3");
    let artifact_store = ArtifactStore::open(directory.path().join("objects")).unwrap();
    let clock = ManualClock::new(2_850);
    let descriptor = descriptor();
    let owner = operation(11);
    let mut store = SqliteStore::open(&database).unwrap();
    let (_, run) = store
        .start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(b"input"),
            &owner,
            100,
            &clock,
            &mut DeterministicIds::new(12),
        )
        .unwrap();
    drop(run);
    drop(store);
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "DELETE FROM schema_migrations
               WHERE migration_name = 'm1-coordinator-bootstrap-v2';
             DELETE FROM artifact_identities WHERE identity_kind = 'RESERVED';",
        )
        .unwrap();
    drop(connection);

    let mut reopened = SqliteStore::open(&database).unwrap();
    let resumed = reopened
        .resume_m1_run(&artifact_store, &descriptor.run_id, &owner, 100, &clock)
        .unwrap();
    drop(resumed);
}

#[test]
fn legacy_scheduled_start_is_rejected_without_rewriting_its_batch_commitment() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("flow.sqlite3");
    let artifact_store = ArtifactStore::open(directory.path().join("objects")).unwrap();
    let clock = ManualClock::new(2_900);
    let descriptor = descriptor();
    let owner = operation(11);
    let mut store = SqliteStore::open(&database).unwrap();
    let (_, mut run) = store
        .start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(b"input"),
            &owner,
            100,
            &clock,
            &mut DeterministicIds::new(12),
        )
        .unwrap();
    run.reconcile_once(&artifact_store, &mut DeterministicIds::new(17))
        .unwrap();
    drop(run);
    drop(store);
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "DELETE FROM schema_migrations
               WHERE migration_name = 'm1-coordinator-bootstrap-v2';
             DELETE FROM artifact_identities WHERE identity_kind = 'RESERVED';
             DELETE FROM artifact_ingress_edges;",
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        SqliteStore::open(&database),
        Err(StoreError::IncompatibleSchema)
    ));
}

#[test]
fn active_owner_conflicts_takeover_succeeds_and_stale_reconcile_is_fenced() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("flow.sqlite3");
    let artifact_store = ArtifactStore::open(directory.path().join("objects")).unwrap();
    let clock = ManualClock::new(3_000);
    let descriptor = descriptor();
    let owner_a = operation(11);
    let owner_b = operation(12);
    let mut first = SqliteStore::open(&database).unwrap();
    let (_, mut stale) = first
        .start_m1_run(
            &artifact_store,
            &descriptor,
            ingress(b"input"),
            &owner_a,
            100,
            &clock,
            &mut DeterministicIds::new(13),
        )
        .unwrap();
    assert!(matches!(
        stale
            .reconcile_once(&artifact_store, &mut DeterministicIds::new(17))
            .unwrap(),
        M1ReconcileOutcome::ScheduledImplementation { .. }
    ));
    let mut second = SqliteStore::open(&database).unwrap();
    assert!(matches!(
        second.resume_m1_run(&artifact_store, &descriptor.run_id, &owner_b, 100, &clock),
        Err(StoreError::RunLeaseConflict)
    ));
    clock.set(3_100);
    let mut takeover = second
        .resume_m1_run(&artifact_store, &descriptor.run_id, &owner_b, 100, &clock)
        .unwrap();
    assert!(matches!(
        stale.reconcile_once(&artifact_store, &mut DeterministicIds::new(21)),
        Err(StoreError::RunLeaseExpired)
    ));
    assert_eq!(
        takeover
            .reconcile_once(&artifact_store, &mut DeterministicIds::new(25))
            .unwrap(),
        M1ReconcileOutcome::NeedsAgentTransport
    );
}
