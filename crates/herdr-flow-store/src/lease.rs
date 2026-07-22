use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use herdr_flow_core::{OperationId, RunId, MAX_CONTROL_REVISION};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::{from_sql_integer, require_run, to_sql_integer, SqliteStore, StoreError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClockError(String);

impl ClockError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub trait UnixMillisClock: Send + Sync {
    fn now_unix_ms(&self) -> Result<u64, ClockError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl UnixMillisClock for SystemClock {
    fn now_unix_ms(&self) -> Result<u64, ClockError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| ClockError::new(error.to_string()))?;
        u64::try_from(duration.as_millis())
            .map_err(|_| ClockError::new("system time exceeds supported milliseconds"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunLeaseFence {
    run_id: RunId,
    owner_id: OperationId,
    lease_epoch: u64,
    expires_at_unix_ms: u64,
}

impl RunLeaseFence {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn owner_id(&self) -> &OperationId {
        &self.owner_id
    }

    pub fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    pub fn expires_at_unix_ms(&self) -> u64 {
        self.expires_at_unix_ms
    }
}

pub struct LeasedRun<'store, 'clock> {
    pub(crate) store: &'store mut SqliteStore,
    pub(crate) clock: &'clock dyn UnixMillisClock,
    pub(crate) fence: RunLeaseFence,
    duration_ms: u64,
}

impl SqliteStore {
    pub fn acquire_run<'store, 'clock>(
        &'store mut self,
        run_id: &RunId,
        owner_id: &OperationId,
        duration_ms: u64,
        clock: &'clock dyn UnixMillisClock,
    ) -> Result<LeasedRun<'store, 'clock>, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        let now = clock.now_unix_ms().map_err(StoreError::Clock)?;
        let expiry = lease_expiry(now, duration_ms)?;
        require_run(&transaction, run_id)?;
        let row: Option<(i64, Option<String>, Option<i64>)> = transaction
            .query_row(
                "SELECT lease_epoch, owner_id, expires_at_unix_ms
                 FROM run_leases WHERE run_id = ?1",
                params![run_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(StoreError::Sqlite)?;
        let (prior_epoch, prior_owner, prior_expiry) =
            row.ok_or(StoreError::CorruptData("run lease row missing"))?;
        let prior_epoch = from_sql_integer(prior_epoch)?;
        if prior_owner.as_deref() == Some(owner_id.as_str())
            && prior_expiry
                .map(from_sql_integer)
                .transpose()?
                .is_some_and(|value| value > now)
        {
            transaction.commit().map_err(StoreError::Sqlite)?;
            return Ok(LeasedRun {
                store: self,
                clock,
                fence: RunLeaseFence {
                    run_id: run_id.clone(),
                    owner_id: owner_id.clone(),
                    lease_epoch: prior_epoch,
                    expires_at_unix_ms: prior_expiry
                        .map(from_sql_integer)
                        .transpose()?
                        .ok_or(StoreError::CorruptData("active run lease has no expiry"))?,
                },
                duration_ms,
            });
        }
        if prior_expiry
            .map(from_sql_integer)
            .transpose()?
            .is_some_and(|value| value > now)
        {
            return Err(StoreError::RunLeaseConflict);
        }
        let epoch = prior_epoch
            .checked_add(1)
            .filter(|value| *value <= MAX_CONTROL_REVISION)
            .ok_or(StoreError::EventSequenceExhausted)?;
        transaction
            .execute(
                "UPDATE run_leases
                 SET lease_epoch = ?1, owner_id = ?2, expires_at_unix_ms = ?3
                 WHERE run_id = ?4 AND lease_epoch = ?5",
                params![
                    to_sql_integer(epoch)?,
                    owner_id.as_str(),
                    to_sql_integer(expiry)?,
                    run_id.as_str(),
                    to_sql_integer(prior_epoch)?
                ],
            )
            .map_err(StoreError::Sqlite)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(LeasedRun {
            store: self,
            clock,
            fence: RunLeaseFence {
                run_id: run_id.clone(),
                owner_id: owner_id.clone(),
                lease_epoch: epoch,
                expires_at_unix_ms: expiry,
            },
            duration_ms,
        })
    }
}

impl LeasedRun<'_, '_> {
    pub fn fence(&self) -> &RunLeaseFence {
        &self.fence
    }

    pub fn renew(&mut self) -> Result<(), StoreError> {
        let transaction = self
            .store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        let now = self.clock.now_unix_ms().map_err(StoreError::Clock)?;
        let proposed_expiry = lease_expiry(now, self.duration_ms)?;
        let expiry: Option<i64> = transaction
            .query_row(
                "UPDATE run_leases
                 SET expires_at_unix_ms = MAX(expires_at_unix_ms, ?1)
                 WHERE run_id = ?2 AND owner_id = ?3 AND lease_epoch = ?4
                   AND expires_at_unix_ms > ?5
                 RETURNING expires_at_unix_ms",
                params![
                    to_sql_integer(proposed_expiry)?,
                    self.fence.run_id.as_str(),
                    self.fence.owner_id.as_str(),
                    to_sql_integer(self.fence.lease_epoch)?,
                    to_sql_integer(now)?
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Sqlite)?;
        let expiry = expiry
            .map(from_sql_integer)
            .transpose()?
            .ok_or(StoreError::RunLeaseExpired)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        self.fence.expires_at_unix_ms = expiry;
        Ok(())
    }

    pub fn release(self) -> Result<(), StoreError> {
        let transaction = self
            .store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        let now = self.clock.now_unix_ms().map_err(StoreError::Clock)?;
        require_active_fence(&transaction, &self.fence, now)?;
        let updated = transaction
            .execute(
                "UPDATE run_leases SET owner_id = NULL, expires_at_unix_ms = NULL
                 WHERE run_id = ?1 AND owner_id = ?2 AND lease_epoch = ?3
                   AND expires_at_unix_ms > ?4",
                params![
                    self.fence.run_id.as_str(),
                    self.fence.owner_id.as_str(),
                    to_sql_integer(self.fence.lease_epoch)?,
                    to_sql_integer(now)?
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if updated != 1 {
            return Err(StoreError::RunLeaseExpired);
        }
        transaction.commit().map_err(StoreError::Sqlite)
    }
}

pub(crate) fn require_active_fence(
    transaction: &Transaction<'_>,
    fence: &RunLeaseFence,
    now_unix_ms: u64,
) -> Result<(), StoreError> {
    let active = transaction
        .query_row(
            "SELECT 1 FROM run_leases
             WHERE run_id = ?1 AND owner_id = ?2 AND lease_epoch = ?3
               AND expires_at_unix_ms > ?4",
            params![
                fence.run_id.as_str(),
                fence.owner_id.as_str(),
                to_sql_integer(fence.lease_epoch)?,
                to_sql_integer(now_unix_ms)?
            ],
            |_| Ok(()),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    active.ok_or(StoreError::RunLeaseExpired)
}

pub(crate) fn lease_now(
    transaction: &Transaction<'_>,
    fence: &RunLeaseFence,
    clock: &dyn UnixMillisClock,
) -> Result<(), StoreError> {
    let now = clock.now_unix_ms().map_err(StoreError::Clock)?;
    require_active_fence(transaction, fence, now)
}

fn lease_expiry(now: u64, duration_ms: u64) -> Result<u64, StoreError> {
    if duration_ms == 0 {
        return Err(StoreError::InvalidRunLeaseDuration);
    }
    now.checked_add(duration_ms)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or(StoreError::InvalidRunLeaseDuration)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    use herdr_flow_core::Sha256Digest;

    use super::*;

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

    fn operation(value: &str) -> OperationId {
        OperationId::parse(format!("op_{value}")).unwrap()
    }

    #[test]
    fn durable_fence_serializes_owners_and_never_reuses_an_epoch() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("lease.sqlite3");
        let run_id = RunId::parse("flow_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let owner_a = operation("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let owner_b = operation("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let clock = ManualClock::new(1_000);
        let mut first = SqliteStore::open(&database).unwrap();
        first
            .create_run(&run_id, &Sha256Digest::of_bytes(b"pipeline"))
            .unwrap();
        let lease_a = first.acquire_run(&run_id, &owner_a, 100, &clock).unwrap();
        assert_eq!(lease_a.fence().lease_epoch(), 1);
        let stale_fence = lease_a.fence().clone();
        drop(lease_a);

        let mut second = SqliteStore::open(&database).unwrap();
        assert!(matches!(
            second.acquire_run(&run_id, &owner_b, 100, &clock),
            Err(StoreError::RunLeaseConflict)
        ));
        clock.set(1_100);
        let lease_b = second.acquire_run(&run_id, &owner_b, 100, &clock).unwrap();
        assert_eq!(lease_b.fence().lease_epoch(), 2);
        let stale_transaction = first.connection.unchecked_transaction().unwrap();
        assert!(matches!(
            require_active_fence(&stale_transaction, &stale_fence, 1_100),
            Err(StoreError::RunLeaseExpired)
        ));
        stale_transaction.commit().unwrap();
        lease_b.release().unwrap();
        let lease_a = second.acquire_run(&run_id, &owner_a, 100, &clock).unwrap();
        assert_eq!(lease_a.fence().lease_epoch(), 3);
    }

    #[test]
    fn converged_handles_cannot_shorten_the_durable_expiry() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("lease.sqlite3");
        let run_id = RunId::parse("flow_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let owner = operation("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let clock = ManualClock::new(1_000);
        let mut first = SqliteStore::open(&database).unwrap();
        first
            .create_run(&run_id, &Sha256Digest::of_bytes(b"pipeline"))
            .unwrap();
        let initial = first.acquire_run(&run_id, &owner, 100, &clock).unwrap();
        drop(initial);
        let mut second = SqliteStore::open(&database).unwrap();
        let mut long = first.acquire_run(&run_id, &owner, 200, &clock).unwrap();
        let mut short = second.acquire_run(&run_id, &owner, 10, &clock).unwrap();
        clock.set(1_050);
        long.renew().unwrap();
        assert_eq!(long.fence().expires_at_unix_ms(), 1_250);
        short.renew().unwrap();
        assert_eq!(short.fence().expires_at_unix_ms(), 1_250);
    }

    #[test]
    fn expiry_boundary_fences_renewal_and_release() {
        let directory = tempfile::tempdir().unwrap();
        let run_id = RunId::parse("flow_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let clock = ManualClock::new(5_000);
        let mut store = SqliteStore::open(directory.path().join("lease.sqlite3")).unwrap();
        store
            .create_run(&run_id, &Sha256Digest::of_bytes(b"pipeline"))
            .unwrap();
        let mut lease = store
            .acquire_run(
                &run_id,
                &operation("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
                50,
                &clock,
            )
            .unwrap();
        clock.set(5_050);
        assert!(matches!(lease.renew(), Err(StoreError::RunLeaseExpired)));
        assert!(matches!(lease.release(), Err(StoreError::RunLeaseExpired)));
    }
}
