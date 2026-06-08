# CaseHub Workers

## Project Type

type: java

## Repository Role

Integration-tier collection of CaseHub worker implementations. Each module provides `ReactiveWorkerProvisioner` and `WorkerExecutionManager` SPI implementations (from `casehub-engine-api` and `casehub-engine-common`) that allow CaseHub cases to dispatch work to different execution runtimes — HTTP endpoints, Apache Camel routes, shell scripts, Kubernetes Jobs, and more.

**Tier:** Integration (alongside `claudony` and `casehub-openclaw` in the build order)

**Design philosophy:** Thin wrappers — each worker module translates a CaseHub case step dispatch into the target runtime's protocol and fires `WorkflowExecutionCompleted` on `WORKER_EXECUTION_FINISHED` when done. No domain logic here.

**Spec:** `docs/superpowers/specs/2026-06-08-casehub-workers-camel-design.md` — fully approved, 7 review cycles.

## Build Commands

```bash
# Build all modules
mvn --batch-mode install

# Publish to GitHub Packages (CI only — requires GITHUB_TOKEN)
mvn --batch-mode deploy -DskipTests
```

## Module Structure

| Module | Artifact | Root package | Purpose |
|--------|----------|-------------|---------|
| `workers-common` | `casehub-workers-common` | `io.casehub.workers.common` | General async worker infrastructure — shared by all worker types |
| `workers-http` | `casehub-workers-http` | `io.casehub.workers.http` | HTTP/webhook worker (skeleton — spec not yet written) |
| `workers-camel` | `casehub-workers-camel` | `io.casehub.workers.camel` | Apache Camel worker — 300+ connectors |
| `workers-testing` | `casehub-workers-testing` | `io.casehub.workers.testing` | Shared test fixtures — **test scope only, never compile/runtime** |

Sub-packages follow function: `.registry`, `.callback`, `.fault`, `.route`, `.component` as needed within each root package.

**Build order:** `workers-common` must be first in parent POM `<modules>` — all others depend on it.

## Engine Integration — Two SPIs, Two Call Sites

Workers implement two engine SPIs — these are called at different times:

| SPI | Call site | Purpose for Camel |
|-----|-----------|-------------------|
| `ReactiveWorkerProvisioner` | `CaseContextChangedEventHandler.tryProvision()` | Capability probe — validates route exists, returns `ProvisionResult.empty()` |
| `WorkerExecutionManager` | `WorkerScheduleEventHandler` | Actual dispatch — sends exchange, manages completion |

Both are `@ApplicationScoped` (no `@DefaultBean`). CDI displaces `NoOpReactiveWorkerProvisioner` and `NoOpWorkerExecutionManager` when Camel beans are present.

**`NoOpWorkerExecutionManager @DefaultBean`** does not yet exist in `casehub-engine` — tracked as engine#447. Must land before a deployment without `scheduler-quartz` AND without `workers-camel` can start.

## workers-common Key Types

| Type | Purpose |
|------|---------|
| `PendingCompletion` | Registry entry per async dispatch — carries `dispatchId`, `workerType`, `callbackToken`, `capability`, `eventLogId` |
| `WorkerCorrelationContext` | Per-dispatch context — `CaseInstance`, `Worker`, `idempotency`, `tenancyId` |
| `AsyncWorkerCompletionRegistry` | In-memory pending completion store; `expireStale()` fires `CompletionExpiredEvent` CDI async |
| `WorkflowCompletionPublisher` | Fires `WorkflowExecutionCompleted` on `WORKER_EXECUTION_FINISHED` via `eventBus.publish()` |
| `WorkerCallbackResource` | `POST /workers/complete/{dispatchId}` — REST callback for external systems |
| `FaultCallbackEvent` | CDI async event fired by `WorkerCallbackResource` on faulted REST callback |
| `CompletionExpiredEvent` | CDI async event fired by `AsyncWorkerCompletionRegistry.expireStale()` |
| `CasehubWorkerHeaders` | Header name constants shared across all worker types |

## workers-camel Key Types

| Type | Purpose |
|------|---------|
| `CamelWorkerConstants.WORKER_TYPE = "camel"` | workerType discriminator — passed to `register()`, used by CDI observers to filter events |
| `CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT` | Separate fault address from Quartz's `WORKFLOW_EXECUTION_FAILED` |
| `CamelWorkerFaultPublisher` | Fires `WorkflowExecutionFailed` on `CAMEL_WORKER_FAULT` |
| `CamelWorkerFaultEventHandler` | `@ConsumeEvent(CAMEL_WORKER_FAULT, blocking=true)` — persists failure, counts retries, re-dispatches or exhausts |
| `CamelCompletionExpiryObserver` | `@ObservesAsync CompletionExpiredEvent` — filters on `WORKER_TYPE`, routes to fault publisher |
| `CamelFaultCallbackObserver` | `@ObservesAsync FaultCallbackEvent` — filters on `WORKER_TYPE`, routes to fault publisher |

## Key Rules

- `workers-testing` is never a compile or runtime dependency — test scope only.
- Each worker module activates by classpath presence (`@ApplicationScoped`, no config required to enable).
- Workers are stateless — all state in the case instance or external system, never in provisioner beans.
- `tenancyId` propagated through all calls — bind in Repository layer only (PP-20260520-e6a5f0).
- Completion fires `eventBus.publish()` on `WORKER_EXECUTION_FINISHED` — never `request()`. Two consumers exist (`WorkflowExecutionCompletedHandler` + `PlanItemCompletionHandler`); `publish()` delivers to both.
- Camel faults fire on `CAMEL_WORKER_FAULT`, NOT `WORKFLOW_EXECUTION_FAILED` — Quartz listens on the latter and would double-process Camel faults.
- Every CDI event observer (`CamelCompletionExpiryObserver`, `CamelFaultCallbackObserver`) MUST filter by `pending.workerType()` — required when two worker modules are co-deployed.
- Retry uses `emitOn(Infrastructure.getDefaultWorkerPool())` after Vert.x timer — not `runSubscriptionOn`. Timer fires on event loop; `emitOn` re-dispatches to worker pool before `submit()`.
- Retry logic mirrors `QuartzWorkerExecutionJobListener` exactly: `failureCount < retryPolicy.maxAttempts()` (strict `<`); null policy defaults to `new RetryPolicy()` (3 attempts, 10s FIXED).

## Co-deployment Constraints

- `workers-camel` + `scheduler-quartz` on same classpath → CDI ambiguity on `WorkerExecutionManager` → startup failure. Unsupported until a composite manager is built in engine.
- `workers-camel` + `workers-http` → `workerType` discriminator in `PendingCompletion` prevents double CDI event handling. `WorkerExecutionManager` CDI ambiguity still applies — same composite manager needed.

## Cross-Repo Dependencies

| Dependency | Why |
|---|---|
| `casehub-engine-api` | `ReactiveWorkerProvisioner`, `WorkerExecutionManager`, `Worker`, `Capability`, `ExecutionPolicy`, `RetryPolicy`, `BackoffStrategy` |
| `casehub-engine-common` | `WorkflowExecutionCompleted`, `WorkflowExecutionFailed`, `CaseInstance`, `EventLog`, `EventBusAddresses`, `WorkerExecutionKeys`, `EventLogRepository` |
| engine#447 | `NoOpWorkerExecutionManager @DefaultBean` — must land before full deployment works |
| platform#73 | `casehub-endpoints` — `EndpointRegistry` SPI for named endpoint resolution (workers designed to work without it until it ships) |

## Cross-Repo Conventions

Protocols live in `casehub/garden`. Do not write protocol files in this repo.

## Artifact Locations

| Skill | Writes to |
|-------|-----------|
| brainstorming (specs) | `docs/superpowers/specs/` |
| writing-plans (plans) | workspace `plans/` |
| handover | workspace `HANDOFF.md` |
| idea-log | workspace `IDEAS.md` |
| design-snapshot | workspace `snapshots/` |
| adr | `docs/adr/` |
| write-blog | workspace `blog/` |

## Routing

| Artifact   | Destination | Notes |
|------------|-------------|-------|
| adr        | project     | lands in `docs/adr/` |
| blog       | workspace   | staged here; published via publish-blog |
| specs      | project     | lands in `docs/superpowers/specs/` |
| plans      | workspace   | |
| handover   | workspace   | |

## Work Tracking

**Issue tracking:** enabled
**GitHub repo:** casehubio/casehub-workers

## Workspace

**Project repo:** `/Users/mdproctor/claude/casehub/workers`
**Workspace:** `/Users/mdproctor/claude/public/casehub-workers`
**Workspace type:** public

Git discipline — always use explicit paths:
```bash
git -C /Users/mdproctor/claude/public/casehub-workers ...   # workspace artifacts
git -C /Users/mdproctor/claude/casehub/workers ...          # project artifacts
```
