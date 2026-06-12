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
| `workers-http` | `casehub-workers-http` | `io.casehub.workers.http` | HTTP/webhook worker — 3-tier endpoint resolution, sync/async dispatch |
| `workers-camel` | `casehub-workers-camel` | `io.casehub.workers.camel` | Apache Camel worker — 300+ connectors |
| `workers-github-actions` | `casehub-workers-github-actions` | `io.casehub.workers.githubactions` | GitHub Actions worker — workflow_dispatch + repository_dispatch |
| `workers-mcp` | `casehub-workers-mcp` | `io.casehub.workers.mcp` | MCP worker — dispatch case steps to MCP server tools via Streamable HTTP |
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

## workers-common Key Types

| Type | Purpose |
|------|---------|
| `PendingCompletion` | Registry entry per async dispatch — carries `dispatchId`, `workerType`, `callbackToken`, `capability`, `eventLogId` |
| `WorkerCorrelationContext` | Per-dispatch context — `CaseInstance`, `Worker`, `idempotency`, `tenancyId` |
| `AsyncWorkerCompletionRegistry` | In-memory pending completion store; `expireStale()` fires `CompletionExpiredEvent` CDI async |
| `WorkflowCompletionPublisher` | Fires `WorkflowExecutionCompleted` on `WORKER_EXECUTION_FINISHED` via `eventBus.publish()` |
| `WorkerCallbackResource` | `POST /workers/complete/{dispatchId}` — REST callback for external systems |
| `WorkerRetrySupport` | Shared retry building blocks — `persistFailureLog`, `countFailedAttempts`, `publishRetriesExhausted`, `resolveRetryPolicy`, `computeBackoffDelayMs`, `parseRetryAfter` |
| `PermanentFaultException` | Worker-agnostic "don't retry" signal — extracted from workers-http |
| `RetryAfterException` | Worker-agnostic "retry after delay" signal — extracted from workers-http |
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

## workers-http Key Types

| Type | Purpose |
|------|---------|
| `HttpWorkerConstants.WORKER_TYPE = "http"` | workerType discriminator |
| `HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT` | Separate fault address from Camel and Quartz |
| `HttpWorkerRoute` | SPI interface for Tier 1 endpoint registration |
| `HttpEndpointResolver` | 3-tier capability tag → `ResolvedEndpoint` resolution (SPI bean > config > EndpointRegistry) |
| `HttpWorkerExecutionManager` | Sync/async dispatch via Vert.x WebClient — reactive-native, no `emitOn` needed |
| `HttpWorkerFaultPublisher` | Fires `WorkflowExecutionFailed` on `HTTP_WORKER_FAULT` |
| `HttpWorkerFaultEventHandler` | `@ConsumeEvent(HTTP_WORKER_FAULT, blocking=true)` — uses `WorkerRetrySupport`, `PermanentFaultException` (4xx) and `RetryAfterException` (429) from workers-common |
| `ExchangeMode` | `SYNC` (default) or `ASYNC` |

## workers-github-actions Key Types

| Type | Purpose |
|------|---------|
| `GitHubActionsWorkerConstants.WORKER_TYPE = "github-actions"` | workerType discriminator |
| `GitHubActionsWorkerEventBusAddresses.GITHUB_ACTIONS_WORKER_FAULT` | Separate fault address from HTTP and Camel |
| `GitHubActionsTokenResolver` | Per-org + global PAT resolution from config properties |
| `GitHubActionsWorkerExecutionManager` | Dispatches via Vert.x WebClient — fire-and-forget on 204 |
| `GitHubActionsWorkerFaultPublisher` | Fires `WorkflowExecutionFailed` on `GITHUB_ACTIONS_WORKER_FAULT` |
| `GitHubActionsWorkerFaultEventHandler` | `@ConsumeEvent(GITHUB_ACTIONS_WORKER_FAULT, blocking=true)` — 422 on workflow-dispatch retryable (60s), 422 on repository-dispatch permanent |
| `GitHubActionsReactiveWorkerProvisioner` | Capability probe — validates tags and token availability |

## workers-mcp Key Types

| Type | Purpose |
|------|---------|
| `McpWorkerConstants.WORKER_TYPE = "mcp"` | workerType discriminator |
| `McpWorkerEventBusAddresses.MCP_WORKER_FAULT` | Separate fault address from HTTP, Camel, and GitHub Actions |
| `McpServerResolver` | Config-based server registry — N:1 capability tag mapping (`mcp:<server>:<tool>` → `ResolvedMcpServer`) |
| `McpSessionManager` | `@ApplicationScoped` — MCP session lifecycle: lazy init with concurrent dedup via memoized Uni, session caching, `@PreDestroy` cleanup |
| `McpSession` | Per-server runtime state — `sessionId`, `protocolVersion`, `AtomicLong requestIdCounter` |
| `McpWorkerExecutionManager` | Dispatches `tools/call` via Vert.x WebClient — dual response parsing (JSON + SSE), `structuredContent` preferred |
| `McpWorkerFaultPublisher` | Fires `WorkflowExecutionFailed` on `MCP_WORKER_FAULT` |
| `McpWorkerFaultEventHandler` | `@ConsumeEvent(MCP_WORKER_FAULT, blocking=true)` — `isError: true` retryable, malformed retryable, 404-with-session retryable (re-initializes) |
| `McpReactiveWorkerProvisioner` | Capability probe — validates tag in resolved set, server URL non-blank |

## Key Rules

- `workers-testing` is never a compile or runtime dependency — test scope only.
- Each worker module activates by classpath presence (`@ApplicationScoped`, no config required to enable).
- Workers are stateless — all state in the case instance or external system, never in provisioner beans.
- `tenancyId` propagated through all calls — bind in Repository layer only (PP-20260520-e6a5f0).
- Completion fires `eventBus.publish()` on `WORKER_EXECUTION_FINISHED` — never `request()`. Two consumers exist (`WorkflowExecutionCompletedHandler` + `PlanItemCompletionHandler`); `publish()` delivers to both.
- Worker faults fire on worker-specific addresses (`CAMEL_WORKER_FAULT`, `HTTP_WORKER_FAULT`, `GITHUB_ACTIONS_WORKER_FAULT`), NOT `WORKFLOW_EXECUTION_FAILED` — Quartz listens on the latter and would double-process.
- Every CDI event observer MUST filter by `pending.workerType()` — required when two worker modules are co-deployed.
- Camel retry uses `emitOn(Infrastructure.getDefaultWorkerPool())` after Vert.x timer — `ProducerTemplate` is blocking.
- HTTP retry does NOT use `emitOn` — `WebClient` is event-loop native, no thread hop needed.
- Retry logic via `WorkerRetrySupport`: `failureCount < retryPolicy.maxAttempts()` (strict `<`); null policy defaults to `new RetryPolicy()` (3 attempts, 10s FIXED).
- HTTP 4xx (except 429) throws `PermanentFaultException` — skips retry immediately.
- HTTP 429 with `Retry-After` header throws `RetryAfterException` — overrides configured backoff delay.
- GitHub Actions retry does NOT use `emitOn` — `WebClient` is event-loop native (same as HTTP).
- GitHub Actions 422 on `workflow-dispatch` throws `RetryAfterException(60_000)` — workflow_dispatch trigger caching (GE-20260426-805acb). 422 on `repository-dispatch` throws `PermanentFaultException` — malformed request.
- GitHub Actions `ref` is required for `workflow-dispatch` — GitHub API rejects requests without it.
- MCP retry does NOT use `emitOn` — `WebClient` is event-loop native (same as HTTP and GitHub Actions).
- MCP `isError: true` is retryable (not permanent) — MCP spec's own example is "API rate limit exceeded."
- MCP malformed responses are retryable — load balancer HTML pages, proxy timeouts are transient.
- MCP 404 with active `Mcp-Session-Id` → session expired, retryable (re-initializes). 404 without session → `PermanentFaultException` (endpoint not found).
- MCP session initialization uses `ConcurrentHashMap.computeIfAbsent` + `Uni.memoize().indefinitely()` — `onFailure().invoke(remove)` BEFORE `memoize()` (GE-20260609-78dc3a).
- MCP protocol version: `2025-06-18` only. No backwards compatibility with `2024-11-05` HTTP+SSE transport.
- MCP required headers: `Accept: application/json, text/event-stream`, `MCP-Protocol-Version`, `Mcp-Session-Id` (when assigned).

## Co-deployment Constraints

- `workers-camel` + `scheduler-quartz` on same classpath → CDI ambiguity on `WorkerExecutionManager` → startup failure. Unsupported until a composite manager is built in engine.
- `workers-camel` + `workers-http` → `workerType` discriminator in `PendingCompletion` prevents double CDI event handling. `WorkerExecutionManager` CDI ambiguity still applies — same composite manager needed.
- `workers-github-actions` + any other worker → same `WorkerExecutionManager` CDI ambiguity. `workerType` discriminator prevents event cross-talk.
- `workers-mcp` + any other worker → same `WorkerExecutionManager` CDI ambiguity. `workerType = "mcp"` discriminator prevents event cross-talk.

## Cross-Repo Dependencies

| Dependency | Why |
|---|---|
| `casehub-engine-api` | `ReactiveWorkerProvisioner`, `WorkerExecutionManager`, `Worker`, `Capability`, `ExecutionPolicy`, `RetryPolicy`, `BackoffStrategy` |
| `casehub-engine-common` | `WorkflowExecutionCompleted`, `WorkflowExecutionFailed`, `CaseInstance`, `EventLog`, `EventBusAddresses`, `WorkerExecutionKeys`, `EventLogRepository` |
| platform#73 | `casehub-endpoints` — `EndpointRegistry` SPI for named endpoint resolution (workers designed to work without it until it ships) |
| engine#461 | Composite `WorkerExecutionManager` — required for co-deploying HTTP + Camel + Quartz on same classpath |

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
