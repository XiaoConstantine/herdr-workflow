# ADR 0002: Use SQLite as the authoritative event and outbox journal

- Status: Accepted
- Date: 2026-07-21

## Context

A workflow transition may need to commit accepted events, derived snapshots, lease revisions, and side-effect intents atomically. Separate authoritative JSONL files cannot provide one transaction across those records and make uniqueness constraints and concurrent compare-and-swap checks difficult.

## Decision

Use SQLite in WAL mode as the authoritative local journal. Every connection enables foreign-key enforcement and `synchronous=FULL`. The store uses one transaction for accepted events, compare-and-swap revisions, snapshots, leases, and outbox intents. External effects never run inside that transaction.

Immutable artifact bytes remain in a content-addressed filesystem store. Before a transaction may reference an artifact, the store writes and verifies a temporary file, fsyncs it, atomically renames it to the digest path, and fsyncs the parent directory. Recovery treats a missing or invalid artifact file as corruption.

JSONL remains an audit and interchange export, not the authoritative mutable storage format.

The initial schema will include runs, events, stage instances, participant principals, role bindings, artifacts, artifact provenance edges, leases, outbox operations, and snapshots.

## Consequences

- Event acceptance and side-effect intent recording can be atomic.
- Message idempotency, event ordering, and compare-and-swap constraints can be enforced by the database.
- Recovery and status queries do not require replaying text files for every read.
- The draft specification and implementation share one persistence contract.
