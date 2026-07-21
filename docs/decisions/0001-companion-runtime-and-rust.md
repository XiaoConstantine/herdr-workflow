# ADR 0001: Build a separate Rust companion runtime

- Status: Accepted
- Date: 2026-07-21

## Context

Herdr 0.7.4 provides terminal workspace, agent, pane, and worktree primitives but does not provide typed workflow persistence, reducers, artifact provenance, human gates, or reconciled publication.

The framework needs deterministic state transitions, explicit trust boundaries, crash recovery, and a distributable local coordinator. Implementing unproven workflow abstractions directly in Herdr would couple the experiment to Herdr's native API prematurely.

## Decision

Build `herdr-flow` as a separate Rust companion in this repository.

Begin with three crates:

- `herdr-flow-core`: pure protocol and reducer logic
- `herdr-flow-store`: persistence adapters
- `herdr-flow-cli`: executable and composition root

Extract Git, agent-adapter, and publisher crates only when those capabilities are implemented and their boundaries are proven.

`herdr-flow-core` must not perform database, filesystem, network, terminal, Git, clock, or random-number-generator operations. External effects are requested through typed intents and reconciled by adapters outside the core.

## Consequences

- The initial runtime can evolve independently of Herdr releases.
- Pure reducers can be tested exhaustively and replayed deterministically.
- The project introduces a Rust toolchain and a companion process.
- Proven APIs may later be proposed for native Herdr integration.
