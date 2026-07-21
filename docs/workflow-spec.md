# Herdr Composable Agent Workflow Framework

Status: Draft v0.7; expanded from the adversarial-review workflow into a
composable workflow framework

Target: Herdr 0.7.4+

Base protocol: `herdr.flow/v1`

Initial stage protocols:

- `herdr.design-council/v1`
- `herdr.implementation-plan/v1`
- `herdr.implementation/v1`
- `herdr.adversarial-review/v1`
- `herdr.integration-review/v1`
- `herdr.artifact-authorization/v1`
- `herdr.human-gate/v1`
- `herdr.publication/v1`

This file retains its original path so existing review links remain valid. The
specification now covers the reusable runtime, the original adversarial-review
stage, a design-council stage, and their composition into a complete software
change pipeline.

## 1. Summary

This specification defines a Herdr-backed workflow framework that can be started
from any supported coding agent and can compose multiple deterministic stages.
The motivating pipeline is:

```text
Design council
  -> typed design authorization
  -> implementation planning
  -> dependency-ordered work items
       -> implementation
       -> adversarial exact-object review and correction loop
  -> whole-change integration review
  -> human publication approval
  -> push and PR/MR creation
```

The design council first asks multiple agents to work independently from the same
frozen brief. Only after all independent proposals are sealed does the coordinator
release them for cross-critique, synthesis, deliberation, and exact-digest council
approval. The resulting technical design document, or TDD, becomes an immutable
input to planning and implementation.

Implementation proceeds one work item at a time by default. Each item starts from
the exact approved object produced by its predecessor. Exactly one implementer
writes a candidate; one or more distinct reviewers challenge that exact object;
blocking findings return to the implementer until reviewers approve. The baseline
advances only after exact-object approval. A final integration review evaluates
the complete base-to-head change because individually approved items can still
interact incorrectly.

Herdr 0.7.4 supplies agent, pane, worktree, wait, and notification APIs, but it
has no workflow namespace, extension registry, typed mailbox, or protected human
action API. The MVP is therefore a standalone `herdr-flow` companion with its own
protocol socket, reducers, artifact store, scheduler, human UI, outbox, and
publisher. A native Herdr implementation is a later target.

The central rules are:

> The runtime coordinates; stage reducers enforce transitions; agents produce
> typed artifacts and decisions; only designated writers mutate canonical
> outputs; and only a human authorizes publication.

> Workflow state is never inferred by matching natural-language terminal output.

## 2. Goals

The framework must:

1. Start a pipeline from any supported Herdr-managed agent or a human shell.
2. Compose reusable, versioned stages through typed immutable artifacts.
3. Support fan-out/fan-in councils, sequential iteration, human gates, and nested
   or repeated stages.
4. Keep safety-critical transition logic in deterministic code, not prompts or
   arbitrary configuration expressions.
5. Let agents work independently before collective deliberation when required.
6. Maintain exactly one canonical writer for each mutable artifact or candidate
   lineage at a time.
7. Bind design approval to an exact document digest and code review to an exact
   full Git commit object ID.
8. Implement dependency-ordered work items one by one and advance the baseline
   only after adversarial approval.
9. Run a whole-change integration review before publication.
10. Propagate upstream changes by invalidating or explicitly revalidating affected
    downstream artifacts and stages.
11. Persist enough state for deterministic restart, takeover, and side-effect
    reconciliation.
12. Keep human design, override, and publication decisions explicit and bound to
    canonical artifact manifests.
13. Work initially with concrete Codex and Pi adapters and admit future agents
    without changing workflow semantics.

## 3. Non-goals and threat model

### 3.1 Non-goals

The MVP will not:

- Let an LLM choose arbitrary reducer transitions or execute workflow definitions
  as code.
- Infer agreement, task completion, or user intent from terminal prose.
- Allow concurrent agents to edit the same canonical TDD or candidate branch.
- Automatically choose a winner in unresolved design or code disagreements.
- Intentionally give agents publication or human-approval capabilities.
- Auto-merge a PR/MR.
- Treat separate worktrees, prompts, or same-user tokens as containment against a
  malicious local process.
- Promise safe parallel code merging in the MVP. The initial work-item scheduler
  is dependency-ordered and sequential.

### 3.2 Threat model

The MVP targets honest-but-fallible agents in a trusted local developer account.
Agents may misunderstand instructions, hallucinate reports, or accidentally touch
incorrect files. The coordinator detects these failures with schemas, artifact
digests, object validation, role bindings, reducer guards, and side-effect checks.

Herdr 0.7.4 normally runs agents as the same OS user. A malicious or
prompt-injected process can inspect files and processes, mutate shared Git refs,
invoke Herdr, imitate a human CLI action, or read same-user credentials. MVP role
separation and its human gate are therefore policy controls, not hostile-process
containment.

A hardened profile requires separate credentialless clones, containers or OS
users, a capability-filtered Herdr proxy, coordinator state under a protected
identity, sandboxed checks, and a clean publisher identity. This specification
marks security claims that require that profile.

## 4. User experience

### 4.1 Start from any agent

A human may tell the current agent:

> Draft a TDD for moving conversation state to Postgres. Use Codex, Pi, and
> Claude as a council. After I approve the design, plan the work and implement
> each item one by one with adversarial review, then ask before publishing.

The current agent creates a frozen brief and invokes the companion:

```bash
herdr-flow start software-change/v1 \
  --brief-file ./postgres-design-brief.md \
  --council-member codex \
  --council-member pi \
  --council-member claude \
  --implementation-strategy dependency-ordered-sequential
```

Before `start` returns, the companion atomically reads the brief, stores its exact
bytes as `design-brief/v1`, and records the digest. Later deletion or mutation of
the source path cannot change the run.

“Start from any agent” means initiation portability. The initiator is reused as a
stage participant only when its adapter, cwd, session identity, permissions, and
report channel satisfy the stage role contract; otherwise the runtime starts a
new role session.

### 4.2 Inspect and control

```bash
herdr-flow status <run-id>
herdr-flow show <run-id> --artifacts
herdr-flow review <run-id>
herdr-flow resume <run-id>
herdr-flow cancel <run-id>
```

`review` opens a dedicated interactive coordinator TTY/UI rather than injecting
an approval command into an agent pane. Piped and non-interactive approval input
is rejected. This records human intent by policy in the MVP; protected provenance
requires a future Herdr client-originated action.

The human is notified only when clarification, escalation, design approval,
publication approval, or failure recovery is required.

## 5. Runtime architecture

```text
herdr-flow
  runtime/
    pipeline reducer and scheduler
    stage registry and stage-instance reducers
    agent/session lifecycle
    typed delivery and report socket
    content-addressed artifact registry
    Git object/worktree manager
    human gates
    event log, leases, and outbox
    check runner and publisher
  stages/
    design-council/v1
    implementation-plan/v1
    implementation/v1
    adversarial-review/v1
    integration-review/v1
    artifact-authorization/v1
    human-gate/v1
    publication/v1
  pipelines/
    software-change/v1
  adapters/
    codex
    pi
  skills/
    herdr-flow
```

The runtime supplies orchestration mechanics. A stage module supplies typed
inputs and outputs, role contracts, a deterministic reducer, completion and
invalidation predicates, and recovery behavior. Pipeline definitions compose
registered stage modules; they cannot introduce new executable reducer logic.

## 6. Core model

### 6.1 Entities

- **Run**: one execution of a versioned pipeline definition.
- **Pipeline definition**: a human-approved DAG or structured control flow of
  registered stage types.
- **Stage instance**: one execution of one registered stage version.
- **Participant principal**: a stable run-scoped identity for one logical agent
  participant that survives process, pane, terminal, and agent-session replacement.
- **Role binding**: a stage role bound to a participant principal plus an exact
  terminal, pane, process, agent session, adapter, and capability set.
- **Artifact**: immutable typed bytes with a digest and provenance.
- **Artifact manifest**: the exact set of artifacts authorized as stage inputs or
  human review material.
- **Gate**: a deterministic or human transition that must approve a manifest.
- **Side effect**: an idempotently reconciled external operation such as push or
  PR creation.

### 6.2 Stage contract

Every registered stage declares:

```yaml
stage_type: herdr.adversarial-review/v1
component_version: 1.0.0
component_digest: sha256:...
input_schema: adversarial-review-input/v1
output_schema: adversarial-review-output/v1
roles:
  implementer: {count: 1, capabilities: [write_candidate]}
  reviewer: {min: 1, distinct_from: [implementer]}
permissions:
  filesystem: workflow_checkouts_only
  network: denied
  git_mutation: candidate_writer_only
  external_side_effects: denied
reducer: compiled-code-identifier
reducer_version: 1
completion_predicate: exact-object-review-aligned/v1
invalidation_rules:
  - candidate_changed
  - requirements_changed
  - reviewer_set_changed
retry_policy: stage-specific-idempotent/v1
cache_policy: never
cleanup_policy: owned-resources-only/v1
recovery_policy: adversarial-review-recovery/v1
```

Reducers and predicates are compiled, registered code with tests. Declarative
configuration may choose parameters and compose stages but cannot contain shell,
regex-based intent logic, or arbitrary transition expressions. Only trusted,
installed component implementations may execute. Naming a component in a
pipeline cannot grant human approval, publication credentials, unrestricted Git
mutation, network access, or a new side-effect class; coordinator policy must
authorize those capabilities independently.

### 6.3 Artifacts

Every artifact record contains:

```json
{
  "artifact_id": "art_01J...",
  "artifact_type": "approved-tdd/v1",
  "schema_id": "approved-tdd",
  "schema_version": 1,
  "sha256": "...",
  "size": 1234,
  "media_type": "text/markdown",
  "producer_stage_instance_id": "stage_01J...",
  "producer_attempt": 1,
  "producer_event_sequence": 42,
  "pipeline_definition_digest": "sha256:...",
  "component_digest": "sha256:...",
  "input_manifest_digest": "sha256:...",
  "retention_class": "design-record"
}
```

Paths are never artifact identity. The coordinator serves or materializes a
verified copy by ID and digest. Artifacts are immutable; revisions create new
artifact IDs and explicit lineage edges.

Initial artifact types include:

- `design-brief/v1`
- `clarified-requirements/v1`
- `design-proposal/v1`
- `design-critique/v1`
- `decision-register/v1`
- `council-approved-tdd/v1`
- `design-authorized-tdd/v1`
- `implementation-plan/v1`
- `plan-authorized-plan/v1`
- `work-item/v1`
- `candidate-commit/v1`
- `review-package/v1`
- `integration-review-package/v1`
- `publication-manifest/v1`

A completed stage also emits an output manifest binding the ordered input artifact
digests, component and reducer digests, role/session bindings, relevant policy
digests, output artifact digests, exact subject identity, and accepted check and
decision results. This manifest is the unit consumed by downstream stages.

The initial software-change pipeline forms an explicit provenance chain:

```text
effective requirements
  -> council input manifest
  -> ordered sealed proposal set
  -> council-approved TDD
  -> authorized TDD
  -> authorized plan
  -> expanded work graph
  -> ordered item input/output manifests
  -> integration candidate manifest
  -> integration review package
  -> publication manifest
```

Each arrow binds exact upstream digests. Upstream invalidation follows declared
artifact edges rather than natural-language interpretation.

### 6.4 Stage lifecycle

The generic lifecycle is:

```text
PENDING -> READY -> PROVISIONING -> RUNNING
RUNNING -> BLOCKED | COMPLETED | FAILED | PAUSED
COMPLETED -> INVALIDATED
PAUSED -> prior safe state after reconciliation
```

A stage becomes `READY` only when all required input artifacts exist, their
schemas and digests validate, dependencies completed, and its input-manifest
digest is frozen. `COMPLETED` requires its stage-specific predicate. A completed
stage never mutates its outputs; rerun creates a new stage instance.

The pipeline reducer alone schedules stage instances and accepts their outputs.
A stage reducer cannot directly start an unrelated stage or publish.

## 7. Composition model

A pipeline is declarative composition of registered stages. The initial software
change pipeline is conceptually:

```yaml
api_version: herdr.flow/v1
kind: Pipeline
metadata:
  name: software-change
  version: 1
spec:
  stages:
    - id: design
      uses: herdr.design-council/v1

    - id: design_authorization
      uses: herdr.artifact-authorization/v1
      needs: [design]
      inputs:
        subject: design.outputs.council_approved_tdd
      with:
        allowed_modes: [human_gate, repository_policy_waiver]

    - id: plan
      uses: herdr.implementation-plan/v1
      needs: [design_authorization]

    - id: plan_authorization
      uses: herdr.artifact-authorization/v1
      needs: [plan]
      inputs:
        subject: plan.outputs.plan_and_work_item_manifest
      with:
        allowed_modes: [human_gate, repository_policy_waiver]

    - id: work_items
      needs: [plan_authorization]
      foreach:
        from: plan_authorization.outputs.authorized_plan.work_items
        strategy: dependency-ordered-sequential
        baseline: previous_iteration.outputs.approved_candidate_oid
      stages:
        - id: implement
          uses: herdr.implementation/v1
        - id: review
          uses: herdr.adversarial-review/v1
          needs: [implement]

    - id: integration_review
      uses: herdr.integration-review/v1
      needs: [work_items]

    - id: publication_gate
      uses: herdr.human-gate/v1
      needs: [integration_review]

    - id: publish
      uses: herdr.publication/v1
      needs: [publication_gate]
```

Selectors are schema-checked references, not general expressions. The pipeline
definition is canonicalized and hashed before the run begins. Any definition or
parameter change is a protected human event and invalidates affected scheduling
and authorization state.

The artifact-authorization stage emits a typed authorization artifact rather than
only a boolean. `design-authorized-tdd/v1` and `plan-authorized-plan/v1` contain
`authorization_mode: human_gate | repository_policy_waiver`, the exact subject
digest, repository-policy digest, protected decision provenance, and recorded gate
omission. The plan authorization also binds ordered work-item IDs and digests,
dependency edges, sequence keys, checks, role policies, and scheduler parameters.
Planning and foreach input schemas require the corresponding authorization type,
so authorization dominance is enforced by data and capability types rather than
declaration order. A waiver can be produced only from a pre-existing protected,
human-approved repository policy; an agent or pipeline definition cannot mint it.

The runtime supports these initial composition operators:

- `needs`: DAG dependency.
- `foreach`: data-driven stage expansion from a typed artifact collection.
- `strategy: dependency-ordered-sequential`: topological order with one active
  writer iteration.
- `gate`: exact-manifest human or deterministic approval.
- `on_failure` and `on_escalation`: transitions to registered recovery stages or
  human gates.

Arbitrary loops are not accepted. Repetition is owned by a registered composite
stage reducer or a bounded `foreach` expansion.

Dynamic expansion is deterministic and durable. After validating a plan, the
scheduler persists `GRAPH_EXPANDED` with the plan digest, expansion
algorithm/version, generated stage-instance IDs, item-to-stage mapping, dependency
edges, stable topological schedule, and expanded-graph digest. Sequential
scheduling selects only dependency-ready items, breaks ties by explicit
`sequence_key` then stable work-item ID, and persists `ITEM_SCHEDULED` before
agent dispatch. A changed plan creates a new expansion; replay never regenerates a
possibly different graph from mutable prose.

## 8. Base protocol

### 8.1 Envelope

All companion messages use `herdr.flow/v1`:

```json
{
  "protocol": "herdr.flow/v1",
  "pipeline_definition_id": "software-change",
  "pipeline_definition_version": 1,
  "pipeline_definition_digest": "sha256:...",
  "run_id": "flow_01J...",
  "node_path": "work_items/W003/review",
  "stage_instance_id": "stage_01J...",
  "parent_stage_instance_id": "stage_parent_01J...",
  "stage_protocol": "herdr.design-council/v1",
  "component_version": "1.0.0",
  "component_digest": "sha256:...",
  "role_binding_id": "role_01J...",
  "attempt": 1,
  "iteration": 0,
  "message_kind": "AGENT_REPORT",
  "message_id": "msg_01J...",
  "causation_id": "msg_01J...",
  "report_type": "PROPOSAL_SUBMITTED",
  "subject_kind": "design-proposal/v1",
  "subject_id": "art_01J...",
  "expected_scheduler_revision": 8,
  "expected_stage_revision": 17,
  "expected_slot_version": 0,
  "input_manifest_digest": "sha256:...",
  "payload_digest": "sha256:...",
  "payload": {},
  "artifacts": []
}
```

Rules:

- IDs are globally unique; safe retries reuse the same ID and canonical digest.
- Reusing an ID with different content is rejected.
- RFC 8785 canonical JSON is used before hashing JSON artifacts and envelopes.
- Agent reports never assign authoritative event IDs, event sequences, lifecycle
  events, human actions, scheduler commands, or side-effect events.
- Slot-local reports CAS only their expected slot/finding versions plus frozen
  run, stage, role, subject, and input identities. They do not CAS scheduler or
  stage control revisions.
- Human and internal non-commutative commands CAS the applicable stage or
  scheduler control revision. The coordinator assigns authoritative event IDs and
  sequences only after authentication, schema validation, authorization, and
  reducer acceptance.
- The coordinator assigns a monotonic per-run event sequence that does not itself
  invalidate semantic state.
- Every stage payload is validated against its registered stage protocol schema.
- Role credentials authenticate report types but are not same-user containment.
- Natural-language text may be artifact content but cannot directly select a
  transition.

The transport defines four authenticated message kinds: `AGENT_REPORT`, permitted
only for stage-report schemas allowed by the binding; `HUMAN_COMMAND`, accepted
only from the protected interactive human channel; `INTERNAL_COMMAND`, produced
only by trusted coordinator components; and `COMMITTED_EVENT`, emitted only by
the reducer after acceptance. Role credentials cannot submit another message
kind.

The runtime core understands committed lifecycle events such as `NODE_READY`,
`NODE_STARTED`, `NODE_BLOCKED`, `NODE_COMPLETED`, `NODE_FAILED`, `NODE_PAUSED`,
`NODE_CANCELLED`, `ARTIFACT_PRODUCED`, `ARTIFACT_ACCEPTED`,
`ARTIFACT_REJECTED`, `FANOUT_CREATED`, `BARRIER_SATISFIED`, `GRAPH_EXPANDED`,
`NODE_INVALIDATED`, `LOOP_CONTINUE`, `LOOP_EXIT`, `HUMAN_ACTION`,
`SIDE_EFFECT_INTENT`, and `SIDE_EFFECT_RESULT`. Domain reports such as council
proposal submission or exact-object review decisions belong to registered stage
protocols. The scheduler does not contain hardcoded knowledge of
`IMPLEMENTATION_READY` or `REVIEW_DECISION`.

### 8.2 Herdr 0.7.4 transport

Coordinator-to-agent messages use tested adapter-specific Herdr pane/agent input.
Agent-to-coordinator reports use the companion socket:

```bash
herdr-flow report <run-id> --stage <stage-instance-id> --file report.json
```

Delivery records intent, recipient terminal/pane/process/session identity,
attempt, receipt, and semantic acceptance. The adapter revalidates identity before
and after input. Changed identity or ambiguous submission pauses the stage; the
coordinator never retries uncertain PTY injection automatically.

Terminal output is diagnostics only. It never proves completion. A stage may emit
an authenticated, rate-limited `PROGRESS_REPORTED` observability report that does
not advance reducer state, reset hard budgets, or prove liveness by itself.

Every delivery policy defines a bounded acknowledgment timeout, at most one
idempotent nudge when recipient identity remains exact, and then `PAUSED`; it never
nudges indefinitely or retries uncertain PTY input. Agents can validate a report
without submitting it:

```bash
herdr-flow report <run-id> --stage <stage-instance-id> --file report.json --dry-run
```

Dry-run performs local schema, digest, role, subject, and visible-version checks
without allocating an event ID or changing state.

## 9. Design council stage

### 9.1 Purpose and roles

`herdr.design-council/v1` produces a council-approved exact-digest TDD from a
frozen design brief.

Roles:

- **Council member**: independently proposes, critiques, deliberates, and reviews.
- **Editor**: the only role allowed to produce a canonical TDD revision. By
  default the editor is a non-voting facilitator and cannot ratify its own
  revision. If its principal also submitted a proposal, that member slot is
  excluded from the ratification set for revisions it edits. Allowing an editor
  vote requires an explicit human-approved policy override and is labelled as
  reduced-independence approval.
- **Coordinator**: seals and releases artifacts, manages slots, and applies the
  reducer; it does not make technical decisions.

At least two voting council-member principals distinct from the active editor are
required for normal council approval. Three heterogeneous members plus a separate
editor are recommended, although one non-editor member may become editor in a
later synthesis epoch if at least two other eligible voters remain. Default
consensus is unanimity. Distinctness is checked on stable participant principal,
not replaceable session identity.

### 9.2 Reducer

```text
INTAKE
  -> CLARIFICATION_GATE
  -> INDEPENDENT_PROPOSALS
  -> PROPOSALS_SEALED
  -> CROSS_CRITIQUE
  -> SYNTHESIS
  -> COUNCIL_REVIEW
       -> SYNTHESIS when blocking changes are requested
       -> DELIBERATION when positions remain disputed
       -> COMPLETED when all required slots approve one digest
DELIBERATION
  -> SYNTHESIS when a decision is resolved
  -> HUMAN_DECISION when the round bound is reached
HUMAN_DECISION
  -> SYNTHESIS after explicit direction
  -> CANCELLED
```

### 9.3 Independent-first guarantee

Before fan-out, the reducer freezes a council epoch containing the roster of
participant principals, required slot set, minimum membership, consensus policy,
input manifest, and anonymization policy. Every member receives that exact epoch
and the same brief, requirement digest, evidence manifest, and proposal schema.
Proposal artifacts remain sealed from other council members until every required
proposal slot submits.

Replacement, waiver, or timeout never mutates an active epoch. A protected human
action creates a new epoch, invalidates that epoch's sealed proposal/critique and
approval artifacts, and recollects every required slot under the new roster. The
new roster cannot fall below the stage minimum. Proceeding below the minimum uses
a separately labelled human override artifact and can never produce
`council-approved-tdd/v1`. The coordinator does not reveal summaries, authors, or
partial proposal content early.

Direct agent-to-agent terminal communication is disabled by policy for this phase.
In hardened mode a capability-filtered proxy enforces it. The MVP records the
policy limitation under the same-user threat model.

The first critique round may anonymize proposal authors. Anonymization changes
only presentation; artifact provenance remains available to the coordinator and
human audit package.

### 9.4 Clarification

Members may submit structured questions and assumptions before proposal work.
The coordinator presents the collection to the human without semantically
choosing or silently deduplicating questions. Human answers create a new
`clarified-requirements/v1` artifact and effective-requirements digest shared by
all proposal slots.

### 9.5 Proposal and critique artifacts

Each proposal follows a registered template covering:

- Problem interpretation and assumptions.
- Goals and non-goals.
- Architecture and data flows.
- APIs, protocols, and data model.
- Failure, recovery, security, privacy, and compatibility.
- Migration, rollout, observability, and testing.
- Alternatives, trade-offs, risks, and open questions.
- Repository evidence when the design concerns an existing codebase.

After all proposals are sealed, each member independently critiques every
proposal against the same rubric. A critique records strengths, blocking design
concerns, questions, reusable elements, and evidence. Critiques are sealed until
the critique fan-in completes, preventing the first critic from anchoring others.

### 9.6 Decision register and synthesis

The coordinator creates structural slots for proposal and critique artifacts but
does not semantically merge them. The editor receives all released artifacts and
produces:

- A canonical TDD revision.
- A typed decision register containing options, member positions, chosen outcome,
  rationale, conditions, evidence, and dissent.
- A traceability map showing how blocking critiques were addressed or escalated.

Exactly one editor binding may write the canonical revision. Concurrent edits are
rejected. Each revision has a new artifact ID and digest.

### 9.7 Council review and deliberation

Every member reviews the same TDD digest and decision-register digest. Each slot
submits an atomic decision:

```json
{
  "verdict": "approved",
  "tdd_artifact_id": "art_...",
  "tdd_digest": "sha256:...",
  "decision_register_digest": "sha256:...",
  "blocking_findings": [],
  "non_blocking_concerns": []
}
```

`changes_requested` requires at least one blocking finding. `approved` requires no
blocking findings but may disclose terminal non-blocking concerns. Independent
slot versions allow concurrent decisions against the same frozen digest.

A disagreement is deliberated by decision ID. Each member records a position,
evidence, and conditions for acceptance after seeing the released positions.
Agents may maintain, change, or escalate their position; the coordinator never
selects the winning technical option. An unresolved decision after the configured
bound goes to the human.

Council alignment requires all configured slots to approve the exact same TDD and
decision-register digests, no open blocking design finding, and no unresolved
required decision. Quorum policies may be added later; unanimity is the MVP.

The stage output is `council-approved-tdd/v1`. A separate artifact-authorization
stage produces `design-authorized-tdd/v1` using either a human gate or a recorded
repository-policy waiver. Council approval alone never satisfies the planning
input contract.

## 10. Implementation planning stage

`herdr.implementation-plan/v1` transforms the exact
`design-authorized-tdd/v1` subject into a reviewed dependency graph of bounded
work items. A designated planner is the
single writer; required council members review one exact plan digest.

Each work item contains:

```json
{
  "work_item_id": "W003",
  "title": "Migrate conversation repository reads",
  "requirements_digest": "sha256:...",
  "tdd_digest": "sha256:...",
  "dependencies": ["W001", "W002"],
  "scope": ["src/example.py"],
  "acceptance_criteria": [],
  "required_checks": [],
  "migration_constraints": [],
  "rollback_requirements": [],
  "evidence_refs": []
}
```

The plan reducer requires:

- Stable, unique work-item IDs.
- An acyclic dependency graph.
- Every item traceable to TDD requirements or an explicit enabling task.
- Acceptance criteria and check policy for every item.
- Migration and rollback ordering where applicable.
- A configured granularity policy, including maximum scope size, files or
  components touched, acceptance-criterion count, estimated review load, and
  explicit human approval for an oversized item.
- Atomic reviewer decisions against one plan digest.
- No unresolved blocking planning finding before completion.

The software-change pipeline places an artifact-authorization stage after planning
and defaults it to human-gate mode. Its `plan-authorized-plan/v1` digest and
ordered work-item collection become immutable scheduler inputs. A configured
repository-policy waiver remains truthfully labelled and carries its policy
provenance.

## 11. Sequential implementation and review

### 11.1 Baseline chain

The MVP scheduler expands work items in topological order and permits one active
candidate writer at a time:

```text
base O0
W1: O0 -> candidate O1 -> review -> approved O1
W2: O1 -> candidate O2 -> review -> approved O2
W3: O2 -> candidate O3 -> review -> approved O3
integration review: O0..O3
```

The sequential operator is a durable fold over a promotion chain. It records the
initial object `O0`; for each step, the exact step-input integration head,
approved step-output object, review package, output manifest, and promotion edge;
and the resulting current integration head.

A dependent item cannot start until every dependency completed and the scheduler
froze its exact baseline object. Among dependency-ready items the scheduler uses
the persisted plan's `sequence_key` and then stable item ID; it never silently
reorders an already scheduled item. The baseline advances only to an output
object approved by the item's adversarial-review stage. Promotion is one reducer
transaction recording the previous integration head, approved candidate object,
item review-package digest, item output-manifest digest, and new integration head.
Mutable branch heads are never baseline authority.

Because later objects descend from every earlier promotion, changing any promoted
object invalidates every later promotion-chain successor even when plan items are
logically dependency-independent. Historical remediation either appends a new
reviewed remediation item at the current head or creates a new schedule generation
that replays/rebases and re-reviews the affected suffix.

With only the Phase 1 Codex and Pi principals, the default satisfiable policy uses
one as item implementer and the other as item plus integration reviewer. Rotating
implementers while requiring integration-reviewer separation from every item
implementer requires at least three eligible principals and is rejected during
role-graph admission when unavailable. Each item has one implementer and at least
one distinct reviewer principal.

Code-review role policy defaults to different agent products for implementer and
reviewer. Same-product review requires an explicit per-item
`allow_same_product_review` human-approved policy and remains distinct by
participant principal. Session restart never satisfies either distinction.

### 11.2 Implementation stage

`herdr.implementation/v1` accepts the exact baseline object, work-item artifact,
TDD and plan digests, inherited constraints, and check policy. Exactly one
implementer may report a candidate. The coordinator rejects a candidate when it
is not a commit, violates lineage, leaves the implementation checkout dirty,
changes forbidden paths, or does not match current input manifests.

The first item candidate must equal or descend from the pipeline base; every later
candidate must equal or descend from its frozen item baseline. A protected human
`LINEAGE_RESET` event may replace the lineage baseline only after displaying the
old/new objects and refs. It invalidates affected candidates, checks, reviews,
packages, scheduler outputs, and publication authorization, increments control
and review-state revisions, and returns affected execution to implementation.
`LINEAGE_RESET` is forbidden during unreconciled publication.

Accepted candidates receive coordinator snapshot refs. Checks run against a
detached exact-object checkout with scrubbed environment, no report or publication
credentials, and policy-controlled network. Mutating checks invalidate the
candidate rather than committing changes.

### 11.3 Adversarial-review stage

`herdr.adversarial-review/v1` accepts a candidate object plus its exact work-item,
requirements, plan, baseline, and check-result manifests. Reviewers work from
detached exact-object checkouts and submit atomic `approved` or
`changes_requested` decisions.

A blocking finding has a coordinator-assigned ID, owner reviewer, version,
severity, category, evidence, impact, and requested remediation. The implementer
may disposition it as `fixed` or `disputed` in a new candidate report. Both become
pending exact-object review and can close or be superseded only in the originating
reviewer's later atomic decision. Implementer evidence alone never closes a
finding.

All required reviewers approve the same current object and review epoch before
alignment. Independent reviewer slots use local versions so one current-epoch
report does not stale another. New candidates invalidate all prior approvals.
Non-blocking concerns are terminal and appear in downstream review packages.
Review policy may require an additional reviewer or human confirmation when the
first review epoch returns approval with zero findings. This is an explicit
policy field and structured decision path, never a prose heuristic.

Risk escalation is a separate request bound to the unchanged candidate that has
already completed validation, checks, and the cited review epoch. It cannot
nominate a new object or be combined with a candidate report. Human waiver creates
a clearly labelled override package bound to exact object, checks, decisions, and
finding versions; it never reports agent alignment.

The stage owns the bounded correction loop:

```text
REVIEWING -> FINDINGS -> IMPLEMENTER_CORRECTION -> VALIDATING -> REVIEWING
```

It outputs only an approved candidate object and review package, or an explicit
human override package. The foreach scheduler advances its baseline only after
that output passes the configured gate.

## 12. Whole-change integration review

After all work items complete, `herdr.integration-review/v1` freezes the observed
target-ref object and merge-base object, then reviews the final aggregate object
against:

- The original pipeline base, observed target and merge base, and complete
  reviewed comparison diff.
- The authorized TDD, its authorization mode and policy provenance, and decision
  register.
- The approved implementation plan and every work item.
- Per-item review packages and residual concerns.
- Cross-item behavior, architecture, migrations, compatibility, security,
  observability, rollback, and end-to-end checks.

Per-item approvals cannot satisfy this stage. Every required integration reviewer
must approve the same final object and integration-review epoch.

A blocking integration decision must choose a typed remediation route: reopen an
existing item and invalidate its dependents; propose a reviewed plan amendment
with new remediation items; request a TDD or requirements amendment; escalate an
exact risk; or approve the exact integration candidate. The coordinator never
infers this routing from finding prose. Remediation items return through
implementation plus adversarial review, after which the whole-change integration
review restarts against the new final object. Integration reviewers never write
fixes directly.

The completed stage freezes the final candidate object and integration review
package used by the publication gate.

## 13. Upstream change and invalidation semantics

Every artifact records its input-manifest digest and provenance edges. When an
upstream artifact changes, the pipeline reducer computes affected descendants by
identity, not by interpreting prose.

Default invalidation rules:

- Design brief or clarified requirements change: invalidate TDD council approval,
  design authorization, plan authorization, work items, implementation, reviews,
  and publication.
- TDD amendment: invalidate the plan and all downstream artifacts unless a
  protected human revalidation explicitly identifies unaffected artifacts under a
  registered policy.
- Plan or work-item change: invalidate that item and all dependency descendants.
- Approved candidate change: invalidate its review and every later successor in
  the promotion chain, regardless of logical plan dependencies.
- Check policy/result or reviewer-set change: invalidate the relevant approval.
- Integration finding remediation: invalidate final integration approval and
  publication authorization.
- Publication manifest field change: require a new human publication action.

Human feedback and agent change requests select a typed target before carrying
free-form explanatory text: effective requirements, TDD, plan or dependencies,
specific work item, promotion-chain suffix, integration result, publication
metadata, risk waiver, or cancellation. The selected target determines invalidation; natural-language
matching never chooses the target.

Implementation may submit `DESIGN_CHANGE_REQUEST` with evidence against the
current TDD digest. It pauses affected work and starts a registered design-amendment
subpipeline:

```text
Design amendment council
  -> human approval
  -> plan amendment
  -> dependency impact calculation
  -> rerun affected items
```

The coordinator does not semantically decide impact. A planner proposes the typed
impact set; required reviewers and the human approve its exact digest. Automatic
amendment nesting is bounded to depth one per originating work-item attempt. A
second nested amendment pauses for human restructuring. An amendment defaults to
the original council principals, consumes the remaining run and stage budgets,
and requires human approval to replace the roster or increase budget.

## 14. Human gates

A human gate displays and binds an exact subject manifest. Design approval binds
the TDD, decision register, requirements, and council approvals. Plan approval
binds the TDD and plan digests. Publication approval binds the final object,
integration review, checks, destination, and PR/MR metadata.

Artifact authorization always emits a truthful typed artifact. Human mode displays
and records the exact subject manifest. Repository-policy-waiver mode validates a
pre-existing protected policy that names the artifact type and permitted omission,
then records that policy digest and the absence of a run-time human gate.
`plan-authorized-plan/v1` binds the plan, ordered work-item definitions and digests,
graph edges, sequence keys, checks, role policies, and scheduler configuration.
Work-item expansion accepts only that artifact type.

Human actions are:

- Approve the displayed manifest.
- Request changes with typed feedback, invalidating affected downstream state.
- Resolve or waive a specifically displayed dispute or risk.
- Cancel.

Approve-only records acceptance without publication. Human approval events may
advance control revision and event sequence but do not self-invalidate their
frozen subject manifest. Only a change to a bound semantic input does.

The publication gate is mandatory and cannot be disabled by pipeline or repository
policy. Design and plan authorization default to `human_gate` but may use
`repository_policy_waiver` under an explicit protected, human-approved repository
policy. Downstream stages consume the truthful authorized artifact rather than a
falsely human-labelled output. Risk and reduced-independence overrides always
require a human gate.

## 15. Publication

The canonical publication manifest contains:

- Provider and immutable project/repository identity.
- Canonical remote URL.
- Git object format and exact final object ID.
- Deterministic head ref, target/base ref, observed target-ref object, reviewed
  merge-base object, target-drift policy, and expected head object or absence.
- PR/MR title, body digest, draft state, labels, and metadata.
- Pipeline, requirements, TDD, decision-register, plan, work-item, per-item review,
  integration-review, check-policy, check-result, finding, and override digests.
- Run ID, stage instances, artifact lineage, and frozen review-state revision.

Every external operation uses a durable intent/result outbox. The publisher pushes
an explicit `<authorized-object>:<remote-ref>` refspec. New refs use atomic
absent-ref compare-and-create; updates require the manifest's exact expected object
and force-with-lease. Plain force is forbidden. Recovery reconciles unresolved
intents and searches for an existing PR/MR by immutable project, deterministic
head ref, and run marker before creation.

Immediately before any publication side effect, the coordinator re-reads the
target ref and merge base. Under the default safe policy, either changing from
the reviewed values invalidates integration approval and publication authorization
and requires a new whole-change integration review. Any alternative drift policy
must itself be present in the integration package and human-authorized manifest.

Publication executes in a narrow process with only provider credentials, from a
clean coordinator-owned Git context with hooks disabled. Repository checks never
run in the credentialed publisher. Agents do not receive publication capabilities.

## 16. Persistence, recovery, and concurrency

State lives outside checkouts:

```text
~/.local/state/herdr-flow/runs/<run-id>/
  run.sqlite3
  artifacts/
  checks/
  exports/
    events.jsonl
    outbox.jsonl
```

The SQLite event journal plus reconciled outbox is authoritative under the
selected threat model. JSONL files are derived audit/interchange exports and are
never recovery inputs. One coordinator holds a renewable run lease and reducer
lock.

The store enables WAL mode, foreign-key enforcement, and `synchronous=FULL` on
every connection. Event acceptance, compare-and-swap revisions, snapshot updates,
lease changes, and side-effect intents commit in one SQLite transaction. The
outbox follows intent-before-effect/result-after-effect ordering; no external
effect runs while a database transaction is open.

Artifact bytes become durable before any committed event may reference them. The
store writes a temporary file on the artifact filesystem, verifies its size and
digest, fsyncs it, atomically renames it to the digest path, and fsyncs the parent
directory before inserting the artifact record and accepting its event. Recovery
rejects a database artifact record whose bytes are absent or fail verification.

Version domains are separated:

- Event sequence for ordering only.
- Run control revision for non-commutative pipeline transitions.
- Stage control revision for non-commutative stage transitions.
- Frozen review-state revision for human review packages.
- Participant/delivery slots for concurrent independent reports.
- Finding, check-attempt, and outbox-operation versions.

A run persists an engine compatibility manifest covering the canonical pipeline
definition, run reducer, scheduler, canonicalizer, graph expander, Git validator,
artifact store, outbox reconciler, schema registry, policy implementations, and
all safety-relevant runtime dependency versions and digests. These run-level
components remain pinned for the run.

Each stage instance separately pins its component, reducer, adapter, and schema
digests while in flight. A completed stage is consumed through its verified
accepted output manifest; forward scheduling need not load its old implementation,
although full event-log replay still requires an archived reducer or a registered
state migration. Resume refuses changed in-flight or run-level compatibility.
Migration is a versioned deterministic transformation with source/target digests,
invariant tests, and protected human authorization; it need not claim general
semantic equivalence. Side-effect stages are reconciled from the outbox and are
never replayed as cacheable computation.

Recovery replays reducers, verifies artifacts and Git objects, reconciles outbox
intents, revalidates terminal/pane/process/agent-session bindings, and resumes only
from a safe recorded state. Missing or replaced participants require a recorded
human action; their approvals are invalidated. Reviewer processes are quiesced or
terminated before resetting owned checkouts.

## 17. Herdr capability mapping and adapters

| Need | Herdr 0.7.4 capability | Companion responsibility |
| --- | --- | --- |
| Discover/start agents | `herdr agent list/get/start` | Role and adapter validation |
| Deliver prompts | Agent/pane input primitives | Session-bound safe adapter delivery |
| Observe status | Agent/wait APIs | UX and timeout hints only |
| Read output | Agent/pane read | Diagnostics only |
| Workspaces/worktrees | Workspace and worktree APIs | Native Git detached/snapshot operations |
| Notify human | Notification API | Durable dedicated human UI |
| Typed reports | None | Companion socket and schemas |
| Workflow persistence | None | Reducers, event log, artifacts, outbox |

The MVP includes concrete Codex and Pi adapters in both role orderings. Each
adapter defines integration preflight, launch argv and environment, session
identity, readiness, exact target submission, receipt/report instructions, and
uncertain-delivery behavior. Missing or outdated integrations are rejected rather
than falling back to generic PTY guessing.

Roles are bound per stage instance to stable run-scoped participant principals.
One principal may fill multiple roles sequentially when policy permits, but
council editor/ratifier separation, implementer/reviewer separation for each item,
and integration-reviewer separation from item implementers are historical
conflict rules that session restart cannot erase. The runtime admission-checks the static
role graph before provisioning and the entire expanded role graph after
`GRAPH_EXPANDED` but before item dispatch; an unsatisfiable roster pauses for human
reconfiguration. Replacing a binding preserves principal history and invalidates
its affected stage output and artifact descendants. Adapters always target
explicit opaque terminal and pane IDs, never current focus or non-unique labels.

Claude, Amp, and additional integrations follow after the two adapters establish
the contract.

## 18. Security and data handling

- Treat repository content, proposals, critiques, diffs, and tool output as
  untrusted data.
- Validate every report against base and stage schemas plus role bindings.
- Never interpolate protocol payloads into shell commands.
- Keep check and publication processes credential-separated.
- Reject traversal, hard-link, and symlink escapes during artifact materialization.
- Store role credentials only in bound process environments and never in logs.
- Record remote artifact transfer and data classes before a run begins.
- Support retention policy, redaction, encryption where required, and per-run
  purge.
- Treat tasks, TDDs, prompts, logs, and diffs as potentially containing secrets,
  personal data, or protected health information.
- Hardened hostile-agent guarantees require OS-level separation described in
  Section 3.

## 19. Budgets and observability

Every run freezes a budget manifest before provisioning. It may bound wall-clock
time, agent calls, input/output tokens, estimated or provider-reported cost,
implementation rounds, review epochs, deliberation rounds, amendment depth,
work-item count, and per-stage allocations. Pipeline expansion reserves or checks
budget for generated stages before dispatch.

Budget accounting is coordinator-owned. Agent progress cannot reset it. Before
each paid or long-running dispatch, the scheduler atomically reserves the expected
allocation; completion reconciles actual usage. Exhaustion pauses or escalates and
never converts incomplete work into approval. Only a protected human action may
increase a budget, and the old/new manifest appears in audit history. Amendment
subpipelines inherit the remaining run budget unless explicitly increased.

`herdr-flow status` and the coordinator UI show:

- Pipeline graph, expansion generation, promotion chain, and invalidated suffixes.
- Every stage phase, attempt, next actor, role principal/session, and input/output
  manifest.
- Council epoch, sealed-slot/barrier state, current TDD and plan digests.
- Work-item schedule, baseline and candidate objects, findings, and review epochs.
- Human gates, authorized manifests, outbox operations, and target-drift state.
- Elapsed/remaining wall time, calls, tokens, cost, rounds, and per-stage/run budget.
- Delivery acknowledgments, bounded nudges, progress timestamps, pauses, and
  recovery actions.

Notifications remain limited to clarification, human action, budget warning or
exhaustion, pause/failure, target drift, and publication completion.

## 20. Skills and extensibility

The skill is an entry point, not the runtime. A general `herdr-flow` skill teaches
agents how to:

- Turn an explicit human request into a frozen brief.
- Start or inspect a pipeline.
- Accept a role assignment.
- Submit typed reports through the companion.
- Escalate rather than invent transitions.

Stage role instructions are delivered with each assignment, so an agent does not
need every workflow permanently embedded in its prompt. Optional authoring skills
may help developers create a new registered stage or pipeline, but they cannot
bypass reducer registration and tests.

Adding a stage requires:

1. Versioned input/output and event schemas.
2. Deterministic reducer and invariants.
3. Role and capability contract.
4. Artifact and invalidation rules.
5. Recovery and timeout policy.
6. Unit, property, component, and composition tests.
7. Registration in the companion.

## 21. Test-driven implementation plan

### 21.1 Runtime tests first

Implement in this order:

1. Base envelope, canonicalization, schema and role validation.
2. Pure run and stage reducers.
3. Artifact registry and provenance graph.
4. Stage scheduling, `needs`, gates, and bounded foreach expansion.
5. Persistence, replay, leases, and outbox.
6. Native Git exact-object operations and check isolation.
7. Human UI and publication reconciliation.
8. Codex/Pi adapter contracts.
9. Skills and live smoke tests.

Property tests must establish:

- A stage cannot complete without its registered predicate.
- A downstream stage cannot start with missing or invalidated inputs.
- Replaying events reconstructs identical state.
- Rejected events have no semantic effect.
- No publication occurs without a matching human-authorized manifest.
- Human waiver never becomes agent alignment.

### 21.2 Design council tests

- Proposals remain sealed until all required proposal slots complete.
- Members receive identical input manifests.
- Critiques remain sealed until critique fan-in completes.
- Only the editor can publish a canonical TDD revision.
- Concurrent council decisions use independent slot versions.
- Different TDD digests cannot satisfy council alignment.
- An unresolved required decision blocks completion.
- Human feedback produces a new digest and invalidates prior council approval.

### 21.3 Sequential pipeline tests

- Work items execute in topological order.
- Only one candidate writer is active.
- Item N receives exactly the approved object from its predecessors.
- A failed or unapproved item cannot advance the baseline.
- A changed earlier object invalidates dependent later items.
- Per-item approval cannot substitute for whole-change integration approval.
- Integration remediation re-enters implementation/review and invalidates final
  approval.

### 21.4 Adversarial-review safety tests

- Wrong-object, stale-epoch, late-session, or partial-reviewer approval cannot
  align.
- Fixed/disputed findings close only in originating reviewer decisions against
  the next exact object.
- New candidate adoption invalidates approvals.
- Risk requests bind only the unchanged validated/reviewed object.
- Mixed candidate and risk reports are rejected.
- Concurrent reviewer slots do not stale one another.
- Lineage reset invalidates all dependent state and is forbidden during active
  publication.

### 21.5 Crash and publication tests

Crash injection occurs before and after every push and PR/MR side effect. Recovery
must produce one remote ref at the authorized object and one PR/MR URL. Tests also
verify absent-ref leases, manifest mismatch handling, disabled repository hooks,
and credential isolation.

Core CI uses scripted deterministic agents and fake providers. Isolated Herdr
contract tests exercise real Herdr 0.7.4 with scripted terminal agents. Real Codex
and Pi runs are optional smoke tests and never replace deterministic safety tests.

## 22. MVP phases

### Phase 1: Framework vertical slice

Phase 1 has two falsifiable milestones.

#### M1: Re-derive the proven adversarial workflow

Express the v0.4 behavior only through registered `herdr.flow/v1` stages:

```text
implementation -> adversarial review/fix loop -> mandatory human publication gate
-> one reconciled publisher
```

M1 includes the runtime kernel, typed artifacts, one static pipeline, persistence,
exact-object Git semantics, human UI, outbox, Codex/Pi adapters, and one provider
selected by the test environment. It is complete only when v0.4 safety invariants
and crash tests pass without scheduler knowledge of review-domain events. Failure
to express those semantics blocks further abstraction work.

#### M2: Council-planned sequential delivery

Add the independent design council, typed design authorization, reviewed plan DAG
and typed plan authorization, deterministic graph expansion, dependency-ordered
sequential work items, per-item exact-object review, whole-change integration
review, run budgets, and the second GitHub/GitLab provider. M2 must compose existing
M1 stages rather than fork their safety logic.

Both milestones retain the honest-but-fallible local threat model.

### Phase 2: Hardening and breadth

- Claude, Amp, and additional adapters.
- Sandboxed checks and credentialless role clones.
- Capability-filtered Herdr proxy and protected coordinator identity.
- Optional quorum policy, richer design amendment flows, remote artifact relay,
  and additional composition operators proven by real workflows.

### Phase 3: Native Herdr

Promote proven runtime concepts into Herdr with registered native stage/workflow
APIs, atomic session-bound mailboxes, event subscriptions, protected state, and
client-originated human actions unavailable to agent capabilities.

## 23. Acceptance criteria

The initial framework is complete when automated tests demonstrate that:

1. A pipeline can be started from either the Codex or Pi adapter.
2. A design council seals independent proposals and critiques before release.
3. The council approves one exact TDD and decision-register digest.
4. Planning accepts only `design-authorized-tdd/v1`, truthfully identifying human
   or repository-policy-waiver authorization and exact provenance.
5. Planning emits an acyclic, reviewed, exact-digest work-item graph.
6. Work items run dependency-ordered and sequentially from exact approved
   baselines.
7. Every item has one implementer and a distinct adversarial reviewer.
8. Blocking findings cause correction and another exact-object review.
9. The baseline advances only after exact-object alignment or a clearly labelled
   human override.
10. The final aggregate object receives a separate whole-change integration
    review.
11. Upstream artifact changes invalidate every affected downstream stage.
12. Design change requests enter an amendment pipeline rather than silently
    mutating the TDD.
13. Stale, duplicate, wrong-role, wrong-object, and wrong-digest reports cannot
    satisfy a stage.
14. Concurrent independent slots do not invalidate one another.
15. Restart and crash recovery reproduce reducer state and reconcile side effects
    without duplication.
16. No push or PR/MR creation occurs without an exact human-authorized publication
    manifest.
17. Existing user worktrees and unrelated changes remain untouched.
18. The MVP does not depend on imaginary Herdr 0.7.4 workflow APIs.
19. Dynamic graph expansion and topological scheduling replay identically from
    persisted events.
20. Workflow, component, reducer, schema, adapter, and policy digests prevent
    unsafe resume under changed implementations.
21. A pipeline definition cannot grant itself human, publisher, network, or Git
    capabilities outside coordinator policy.
22. Typed feedback and integration-remediation targets drive invalidation without
    natural-language routing.
23. Work-item expansion cannot bypass `plan-authorized-plan/v1`, and waiver mode
    cannot be represented as human approval.
24. Agent credentials cannot submit human, scheduler, lifecycle, committed-event,
    or side-effect message kinds.
25. Council roster changes create a new epoch and never reuse sealed outputs.
26. Changing a promoted object invalidates and re-reviews every later chain
    successor.
27. Resume rejects a changed engine compatibility manifest without a registered,
    invariant-validated, protected-human-authorized migration.
28. Target-ref or merge-base drift invalidates integration/publication under the
    default policy.
29. M1 reproduces v0.4 safety through generic stages before council or dynamic
    planning is implemented.
30. Unsatisfiable principal/product conflict policies fail admission before agent
    work begins.
31. The active editor cannot count toward normal ratification of its own TDD.
32. Per-run and per-stage budget exhaustion pauses without approval, and status
    exposes spend and remaining budget.
33. The mandatory publication gate cannot be disabled by repository configuration.
34. Amendment depth, nudge count, and zero-finding confirmation policies are
    enforced by typed reducer state.

## 24. Open decisions

1. Should the companion ship from the Herdr repository or a separate package?
2. Should the default design council have two or three members?
3. Which planning-review roles may overlap with design-council roles?
4. What registered composition operator should be added after sequential foreach
   is proven?
5. What retention defaults apply to proposals, critiques, TDDs, code, and logs?
6. Which protected native Herdr UI should eventually authenticate human actions?

## 25. Review history

- Drafts v0.1-v0.4 specified and hardened the adversarial implementation/review
  loop through four Codex/Pi reconciliation rounds. Those reviews established the
  companion boundary for Herdr 0.7.4, exact-object semantics, finding lifecycle,
  independent slot versions, human and publication manifests, risk escalation,
  lineage reset, persistence, outbox, check isolation, and Codex/Pi adapters.
- Draft v0.5 generalized those mechanisms into a reusable runtime and added the
  independent-first design council, reviewed planning, dependency-ordered
  sequential implementation, per-item adversarial review, whole-change integration
  review, artifact provenance, and cross-stage invalidation.
- Codex's framework review found seven blockers in v0.5: plan-gate dominance,
  report/event authority separation, stable participant principals, council epoch
  semantics, promotion-chain invalidation, engine replay compatibility, and target
  branch drift. Draft v0.6 resolved all seven. Codex's bounded reconciliation
  review returned `ALIGNED` with no unresolved implementation-safety blockers.
- Claude's subsequent review identified four delivery blockers: lack of an early
  framework milestone, an unsatisfiable two-principal rotation example, editor
  self-ratification, and missing run economics. Draft v0.7 adds M1/M2 checkpoints,
  principal/product admission rules, non-voting editors, budgets and observability,
  bounded amendment/nudge behavior, explicit gate policy, and the accompanying
  editorial corrections. Codex's reconciliation then found one contradiction
  between optional design/plan gates and required human-labelled artifact types;
  v0.7 resolves it with truthful typed authorization modes and provenance.
