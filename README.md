# Herdr Workflow

Herdr is a composable, event-sourced workflow framework for coordinating independent AI agents through adversarially reviewed software delivery.

The framework keeps authority in a deterministic runtime: agents propose authenticated reports about exact immutable artifacts, while committed events and reducers advance workflow state. Publication always requires explicit human authorization.

> **Status:** Draft v0.7 — design and architecture phase.

![Herdr workflow architecture](docs/architecture.png)

## Start here

- [Workflow specification](docs/workflow-spec.md)
- [Editable tldraw architecture](docs/architecture.tldraw)
- [Architecture preview](docs/architecture.png)

The editable canvas has two pages:

1. **Architecture · Start Here** — a concise layered system view.
2. **Detailed Workflow** — the complete delivery and review graph.

## Core ideas

- Independent design proposals and sealed adversarial critique
- Reviewed TDD and deterministic plan-DAG expansion
- Sequential exact-object implementation and review
- Event-sourced state transitions with replayable reducers
- Stable principals, role separation, budgets, and recovery
- Artifact provenance with deterministic downstream invalidation
- Whole-change integration review
- Mandatory human publication gate
- A single recoverable publisher using explicit object refspecs

## Repository layout

```text
docs/
  workflow-spec.md      Normative Draft v0.7 specification
  architecture.tldraw  Editable architecture and detailed workflow
  architecture.png     Rendered architecture preview
examples/
  README.md             Planned workflow examples
```

## Editing the architecture

Open `docs/architecture.tldraw` in [tldraw](https://www.tldraw.com/) or tldraw offline. Use **Architecture · Start Here** for the overview and **Detailed Workflow** for the full graph.

## Scope

This repository currently defines the framework and architecture. Runtime code, provider adapters, schemas, conformance fixtures, and reference workflows will be added as the design progresses.

## License

No license has been selected yet.
