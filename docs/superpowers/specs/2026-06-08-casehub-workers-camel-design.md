# CaseHub Workers — Camel Worker Design (Revised)

**Date:** 2026-06-08
**Status:** Approved — pending implementation plan
**Revision:** v2 — post code review; C1/C2/C3 architectural corrections + D1–D13 + M1/M2 fixes

---

## 1. Overview

`casehub-workers` is a multi-module Maven repo at the Integration tier. It provides `ReactiveWorkerProvisioner` and `WorkerExecutionManager` SPI implementations that allow CaseHub cases to dispatch work to different execution runtimes. Apache Camel (300+ connectors) is the first non-trivial implementation.

This spec covers:
- **`workers-common`** — general worker infrastructure shared by all worker implementations. Future migration target: `casehub-engine`, alongside Drools and Flow.
- **`workers-camel`** — Apache Camel adapter implementing both `ReactiveWorkerProvisioner` and `WorkerExecutionManager`.

### Endpoint registry

`casehub-endpoints` is being designed separately in `casehubio/platform` (platform#73). Workers reference named endpoints rather than hardcoded URIs once that ships. Until then, `casehub.workers.camel.capabilities.<tag>` accepts any Camel URI (e.g. `kafka:my-topic?brokers=localhost:9092`) — this is the inline config path. When `EndpointRegistry` ships, `CamelWorkerRoute` beans can inject it to resolve endpoint names at route-build time.

---

## 2. Engine Integration — Corrected Architecture

Two distinct engine-side call sites exist. The spec must satisfy both.

### 2.1 ReactiveWorkerProvisioner — capability probe and compute spin-up

Called from `CaseContextChangedEventHandler.tryProvision()` when no pre-defined worker in the case definition matches a required capability. The engine:

1. Calls `reactiveWorkerProvisioner.getCapabilities()` — checks if this provisioner advertises the needed capability.
2. If yes, calls `reactiveWorkerProvisioner.provision(caps, provisionContext)`.
3. After provision returns, fires `CaseLifecycleEvent("WorkerStarted")` — notification only.
4. Does NOT fire `WorkflowExecutionCompleted` here — no work has been dispatched yet.

For Camel workers, `provision()` is lightweight: validate that a Camel route exists for the capability and return `ProvisionResult`. No exchange is dispatched. Camel routes are always-running; there is no long-lived process to spin up.

### 2.2 WorkerExecutionManager — work dispatch and completion

Called from `WorkerScheduleEventHandler` for every work dispatch. The engine:

1. Computes `inputDataHash = WorkerExecutionKeys.inputDataHash(caseId, workerName, capabilityName, inputData)` — the idempotency key.
2. Calls `workerExecutionManager.submit(eventLogId, instance, worker, capability, inputData)`.
3. The execution manager is responsible for eventually firing `WorkflowExecutionCompleted` on `WORKER_EXECUTION_FINISHED` event bus.
4. `WorkflowExecutionCompletedHandler` then applies output to the case context, calls `caseResumptionService.resumeIfWaiting()`, fires `CaseLifecycleEvent`, and calls `workerStatusListener.onWorkerCompleted()` as a post-resumption notification.

For Camel workers, `submit()` is where all work happens: dispatch the exchange (sync or async), manage the completion callback chain, and fire `WorkflowExecutionCompleted` when done.

### 2.3 CDI resolution

`NoOpReactiveWorkerProvisioner` and `NoOpWorkerExecutionManager` are `@DefaultBean @ApplicationScoped`. The Camel implementations must be plain `@ApplicationScoped` (no `@DefaultBean`, no `@Priority`) — CDI displaces defaults when a non-default bean is present. Same pattern as claudony.

### 2.4 Module dependencies

`workers-common` depends on:
- `casehub-engine-api` — `ReactiveWorkerProvisioner`, `ReactiveWorkerStatusListener`, `ProvisionResult`, `ProvisionContext`, `WorkResult`, `Worker`
- `casehub-engine-common` — `WorkerExecutionManager`, `WorkflowExecutionCompleted`, `CaseInstance`, `EventBusAddresses.WORKER_EXECUTION_FINISHED`, `WorkerExecutionKeys`

This is the same dependency pattern as claudony. `casehub-engine-common` is an internal module but is explicitly consumed by integration-tier repos.

---

## 3. Module Structure

```
casehub-workers/
  workers-common/         General worker infrastructure — sync and async
  workers-http/           HTTP/webhook adapter (separate spec)
  workers-camel/          Apache Camel adapter
  workers-testing/        Shared test fixtures — never compile/runtime dep
```

**workers-common is the first module listed** in the parent POM `<modules>` — it must be built before the others.

---

## 4. `workers-common`

General infrastructure shared by all worker types. Depends on `casehub-engine-api` and `casehub-engine-common`.

### 4.1 Core types

**`WorkerCorrelationContext`** — carries all data needed to fire `WorkflowExecutionCompleted` at async completion time. Stored in the registry for the lifetime of an async dispatch:

```java
record WorkerCorrelationContext(
    CaseInstance caseInstance,   // full instance — needed for WorkflowExecutionCompleted
    Worker worker,               // worker definition — needed for WorkflowExecutionCompleted
    String idempotency,          // WorkerExecutionKeys.inputDataHash(...) — matches waitingForWorkId
    String tenancyId,            // tenant scope for auth validation
    String callbackToken         // one-time UUID token — required in REST callback header
) {}
```

`idempotency` is the value that `CaseResumptionService.resumeIfWaiting()` checks against `caseInstance.getWaitingForWorkId()`. It must be the `inputDataHash` computed by `WorkerExecutionKeys` — not a UUID or sequential ID. The execution manager computes it via `WorkerExecutionKeys.inputDataHash(instance.getUuid(), worker.getName(), capability.getName(), inputData)` from the parameters already available in `submit()`.

`callbackToken` — generated as `UUID.randomUUID().toString()` at registration time. Required as `X-Casehub-Callback-Token` header on `POST /workers/complete/{workerId}`. Compared constant-time (`MessageDigest.isEqual`) against stored token. Prevents any process that knows a `workerId` from hijacking completion.

**`PendingCompletion`** — one entry per registered async dispatch:

```java
record PendingCompletion(
    WorkerCorrelationContext correlationContext,
    Instant registeredAt,
    Instant expiresAt,
    Map<String, String> provisionerMeta   // provisioner-specific; opaque to registry
) {}
```

**`WorkerCompletionPayload`** — JSON body for `POST /workers/complete/{workerId}`:

```java
record WorkerCompletionPayload(
    Map<String, Object> output,   // result data; empty map for faulted
    boolean faulted               // true = faulted; false (default) = completed
) {}
```

**`CasehubWorkerHeaders`** — header/key name constants. Shared across all worker types — each worker maps these to its native format (Camel exchange headers, HTTP request headers, process environment variables):

```java
public final class CasehubWorkerHeaders {
    public static final String WORKER_ID        = "casehub-worker-id";
    public static final String IDEMPOTENCY      = "casehub-idempotency";
    public static final String CASE_ID          = "casehub-case-id";
    public static final String TENANCY_ID       = "casehub-tenancy-id";
    public static final String TASK_TYPE        = "casehub-task-type";
    public static final String CALLBACK_TOKEN   = "casehub-callback-token";
    public static final String WORK_STATUS      = "casehub-work-status";   // FAULTED override
}
```

**`WorkerProvisioningException`** — standard exception for all workers. Factory methods:

```java
public class WorkerProvisioningException extends RuntimeException {
    private final String capability;
    public static WorkerProvisioningException noRouteFound(String capability) { ... }
    public static WorkerProvisioningException startupFailed(String capability, Throwable cause) { ... }
}
```

**`WorkerCapabilityResolver<T>`** — plain interface (not `@FunctionalInterface`) documenting the three-tier resolution pattern that all worker modules must follow:

```java
/**
 * Resolves the worker-specific dispatch target (Camel URI, HTTP URL, script path, …)
 * for a capability tag.
 *
 * Implementations MUST attempt resolution in this order:
 *   1. SPI  — CDI-discovered worker-specific route/endpoint beans (highest priority)
 *   2. Config — explicit property overrides
 *   3. Convention — capability tag → target by naming rule (lowest priority)
 *
 * Throw WorkerProvisioningException if no mapping is found after all three steps.
 */
public interface WorkerCapabilityResolver<T> {
    T resolve(String capabilityTag);
    Set<String> capabilities();   // union of all three tiers, computed at startup
}
```

### 4.2 Services

**`WorkflowCompletionPublisher`** — fires `WorkflowExecutionCompleted` on the engine event bus. Used by all workers at async completion time, and by the sync path in the execution manager:

```java
@ApplicationScoped
public class WorkflowCompletionPublisher {
    @Inject EventBus eventBus;

    /** Normal completion — output applied to case context, case resumed if WAITING. */
    public Uni<Void> complete(WorkerCorrelationContext ctx, Map<String, Object> output) {
        return eventBus.request(EventBusAddresses.WORKER_EXECUTION_FINISHED,
            WorkflowExecutionCompleted.approved(
                ctx.caseInstance(), ctx.worker(), ctx.idempotency(), output));
    }

    /**
     * Faulted completion — fires WorkflowExecutionCompleted with empty output.
     * Engine retry machinery handles the failure path from there.
     */
    public Uni<Void> fault(WorkerCorrelationContext ctx) {
        return complete(ctx, Map.of());
    }
}
```

`WorkflowExecutionCompletedHandler` handles output application, `resumeIfWaiting()`, ledger events, and the post-resumption `workerStatusListener.onWorkerCompleted()` call automatically. Workers do not call `workerStatusListener` directly for completion — only for started/stalled lifecycle notifications.

**`WorkerStatusPublisher`** — CDI helper for lifecycle notifications only. Method names mirror `ReactiveWorkerStatusListener` exactly:

```java
@ApplicationScoped
public class WorkerStatusPublisher {
    @Inject ReactiveWorkerStatusListener reactiveWorkerStatusListener;

    public Uni<Void> onWorkerStarted(String workerId, Map<String, String> sessionMeta) { ... }
    public Uni<Void> onWorkerCompleted(String workerId, WorkResult result) { ... }
    public Uni<Void> onWorkerStalled(String workerId) { ... }
}
```

Note: `onWorkerCompleted` here is for observability — the engine calls this automatically via `WorkflowExecutionCompletedHandler` after resumption. Workers should not call it directly as a completion trigger.

**`WorkerProvisionerSupport`** — common provisioner boilerplate:

- `validateCapabilities(Set<String> requested, Set<String> supported)` — throws `WorkerProvisioningException` if any requested capability is absent from supported. Void return — it is a validation guard, not a filter. If every requested capability is present, it passes silently.
- `tenancyId(ProvisionContext ctx)` — extracts tenancyId from context for stamping.
- `wrap(Throwable t, String capability)` — converts any exception to `WorkerProvisioningException.startupFailed()`.

**`AsyncWorkerCompletionRegistry`** — in-memory pending completion store:

```java
@ApplicationScoped
public class AsyncWorkerCompletionRegistry {
    /** Register a pending completion. Generates callbackToken — returned in PendingCompletion. */
    PendingCompletion register(String workerId, WorkerCorrelationContext ctx,
                               Duration ttl, Map<String, String> provisionerMeta);

    /** Remove and return the pending completion; empty if unknown or already completed. */
    Optional<PendingCompletion> complete(String workerId);

    /** Scheduled: fires WorkerStatusPublisher.onWorkerStalled for expired entries. */
    void expireStale();
}
```

TTL configured via `casehub.workers.async.timeout-minutes` (default: 60, in `workers-common`).

**Deployment constraint — sticky routing required:** The registry is JVM-local. `POST /workers/complete/{workerId}` must reach the same JVM instance that registered the completion. Multi-node deployments must configure sticky load balancing keyed on `workerId`, or replace the in-memory registry with a distributed alternative (Infinispan, Redis). This is a deployment constraint, not a bug — documented here and in operator docs.

**`WorkerCallbackResource`** — REST endpoint. Present in `workers-common`; all worker types share it:

```
POST /workers/complete/{workerId}
Headers: X-Casehub-Callback-Token: <token>
Body: WorkerCompletionPayload
```

Validates `X-Casehub-Callback-Token` against stored `callbackToken` (constant-time comparison). Calls `AsyncWorkerCompletionRegistry.complete(workerId)` — if present, calls `WorkflowCompletionPublisher.complete/fault()`. Returns 200 (idempotent — returns 200 even if the entry was already resolved). Returns 404 only if `workerId` was never registered. Returns 401 if token is missing or mismatched.

No JWT/OIDC required — the callback token is the auth mechanism. External systems that need to call back must be given the token (via the `CasehubWorkerHeaders.CALLBACK_TOKEN` exchange header or provisioner metadata).

---

## 5. `workers-camel`

Depends on `workers-common`. Implements `ReactiveWorkerProvisioner` and `WorkerExecutionManager`.

### 5.1 Capability-to-route resolution — `CamelCapabilityResolver`

Implements `WorkerCapabilityResolver<String>` (target type = Camel entry URI). Three-tier resolution:

**Priority 1 — SPI (highest):** CDI-discovered `CamelWorkerRoute @ApplicationScoped` beans:

```java
public interface CamelWorkerRoute {
    Set<String> getCapabilities();
    String getEntryUri();              // e.g. "direct:lead-enrichment"
    ExchangePattern exchangePattern(); // InOut (sync) or InOnly (async) — AUTHORITATIVE
}
```

`exchangePattern()` is the authoritative declaration. The Camel route's `from:` URI exchange pattern MUST match. A mismatch is detected at startup (see Section 5.4) and throws `IllegalStateException` — it is a route-author error, not a runtime condition.

**Priority 2 — Config:** `casehub.workers.camel.capabilities.<tag> = <camelUri>`. The value is a full Camel URI (e.g. `direct:my-route`, `kafka:my-topic?brokers=localhost:9092`). Exchange pattern is inferred from the registered route's observed pattern. When `EndpointRegistry` ships, `CamelWorkerRoute` beans can inject it to resolve endpoint names at route-build time — the config path remains as a simpler fallback.

**Priority 3 — Convention (lowest):** Both conditions must hold:
1. A route with ID exactly equal to the capability tag is registered in `CamelContext`.
2. The route's first `from:` URI is `direct:{capabilityTag}`.

Neither condition alone is sufficient. A route with a matching ID but a different entry URI does NOT satisfy convention — it falls through to `WorkerProvisioningException`.

**`capabilities()`** returns the union of all three tiers. Computed once at startup (see Section 5.4).

**Multi-capability dispatch:** When `provision()` or `submit()` is called with a `Set<String>`, `CamelCapabilityResolver` resolves the first capability in the set that has a registered route. Only one route is dispatched per call — Camel workers are single-capability per execution. Route authors who need multi-capability work should implement separate routes per capability and let the engine schedule them independently.

### 5.2 Route definition

Both YAML DSL and programmatic `RouteBuilder` CDI beans are supported. Quarkus Camel loads both into the same `CamelContext`. The provisioner and execution manager do not care which form was used.

YAML DSL (convention-based — route ID = capability tag, entry URI = `direct:{capabilityTag}`):

```yaml
- route:
    id: lead-enrichment
    from:
      uri: direct:lead-enrichment
    steps:
      - to:
          uri: salesforce:Lead?...
      - to:
          uri: casehub:complete
```

Programmatic (SPI-based — can inject `EndpointRegistry` once platform#73 ships):

```java
@ApplicationScoped
public class LeadEnrichmentRoute extends RouteBuilder implements CamelWorkerRoute {
    @Override
    public void configure() {
        from("direct:lead-enrichment")
            .setHeader(CasehubWorkerHeaders.WORK_STATUS, constant(""))
            .to("salesforce:Lead?...")
            .to("casehub:complete");
    }

    @Override public Set<String> getCapabilities() { return Set.of("lead-enrichment"); }
    @Override public String getEntryUri()          { return "direct:lead-enrichment"; }
    @Override public ExchangePattern exchangePattern() { return ExchangePattern.InOnly; }
}
```

### 5.3 `CamelReactiveWorkerProvisioner`

Implements `ReactiveWorkerProvisioner`. Lightweight — validates route existence, does not dispatch work.

```java
@ApplicationScoped
public class CamelReactiveWorkerProvisioner implements ReactiveWorkerProvisioner {

    @Override
    public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
        String capability = camelCapabilityResolver.firstMatch(capabilities)
            .orElseThrow(() -> WorkerProvisioningException.noRouteFound(capabilities.toString()));
        // Validate route is registered — throws WorkerProvisioningException if not
        camelCapabilityResolver.resolve(capability);
        return Uni.createFrom().item(ProvisionResult.empty());
    }

    @Override
    public Uni<Void> terminate(String workerId) {
        return Uni.createFrom().voidItem(); // Camel routes are always-running; no teardown
    }

    @Override
    public Uni<Set<String>> getCapabilities() {
        return Uni.createFrom().item(camelCapabilityResolver.capabilities());
    }
}
```

### 5.4 `CamelWorkerExecutionManager`

Implements `WorkerExecutionManager`. This is where all work dispatch and completion handling occurs.

**Startup initialization** — capabilities must be computed after the Camel context is fully started. Use Quarkus `StartupEvent` observed at `@Priority(jakarta.interceptor.Interceptor.Priority.APPLICATION)` (fires after all extensions, including Camel route registration). `@PostConstruct` alone is insufficient — Camel routes may not be registered yet at bean construction time. The `capabilities()` set is computed once in the event observer and cached.

```java
@ApplicationScoped
public class CamelWorkerExecutionManager implements WorkerExecutionManager {

    @Observes @Priority(APPLICATION)
    void onStartup(StartupEvent event) {
        camelCapabilityResolver.initialize(); // compute and cache capability set
        validateRoutePatternConsistency();    // detect exchangePattern() vs route mismatch
    }

    @Override
    public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                            Capability capability, Map<String, Object> inputData) {

        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.getName(), capability.getName(), inputData);

        String entryUri = camelCapabilityResolver.resolve(capability.getName());
        ExchangePattern pattern = camelCapabilityResolver.exchangePattern(capability.getName());

        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            instance, worker, idempotency, instance.tenancyId, null /* set by registry */);

        if (pattern == ExchangePattern.InOut) {
            return submitSync(ctx, entryUri, capability, inputData);
        } else {
            return submitAsync(ctx, entryUri, capability, inputData);
        }
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return asyncWorkerCompletionRegistry.countByWorkerId(workerId);
    }
}
```

**Startup validation — exchange pattern consistency:** After Camel context is started, for each `CamelWorkerRoute` SPI bean, compare `route.exchangePattern()` against the actual `ExchangePattern` of the registered Camel route. Mismatch → `IllegalStateException` at startup with the route ID, declared pattern, and observed pattern. Fail fast — do not allow inconsistent routes to serve production traffic.

### 5.5 Sync path (ExchangePattern.InOut)

```java
private Uni<Void> submitSync(WorkerCorrelationContext ctx, String entryUri,
                              Capability capability, Map<String, Object> inputData) {
    return Uni.createFrom()
        .item(() -> buildExchange(ctx, capability, inputData))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())  // blocking → worker thread
        .map(exchange -> producerTemplate.request(entryUri, exchange))
        .flatMap(response -> {
            if (response.getException() != null
                || "FAULTED".equals(response.getIn().getHeader(CasehubWorkerHeaders.WORK_STATUS))) {
                return completionPublisher.fault(ctx);
            }
            Map<String, Object> output = extractOutput(response);
            return completionPublisher.complete(ctx, output);
        })
        .onFailure()
        .call(t -> completionPublisher.fault(ctx));
}
```

Route exception or `casehub-work-status: FAULTED` header → `WorkflowCompletionPublisher.fault()` fires `WorkflowExecutionCompleted` with empty output. The engine's `WorkflowExecutionCompletedHandler` applies the empty output and the retry/fault machinery runs from there (same as any other failed worker execution).

### 5.6 Async path (ExchangePattern.InOnly)

```java
private Uni<Void> submitAsync(WorkerCorrelationContext ctx, String entryUri,
                               Capability capability, Map<String, Object> inputData) {
    PendingCompletion pending = asyncWorkerCompletionRegistry.register(
        ctx.worker().getName(), ctx,
        Duration.ofMinutes(asyncTimeoutMinutes), Map.of());

    // Update ctx with the callback token generated by the registry
    WorkerCorrelationContext ctxWithToken = ctx.withCallbackToken(pending.correlationContext().callbackToken());

    Exchange exchange = buildExchange(ctxWithToken, capability, inputData);
    // casehub-callback-token header lets external systems include it in their callback
    exchange.getIn().setHeader(CasehubWorkerHeaders.CALLBACK_TOKEN, ctxWithToken.callbackToken());

    return Uni.createFrom()
        .voidItem()
        .invoke(() -> producerTemplate.send(entryUri, exchange));
    // Returns immediately. Completion arrives via casehub:complete or POST /workers/complete/{workerId}
}
```

### 5.7 Exchange input mapping

`CamelExchangeMapper` builds the Camel exchange from `WorkerCorrelationContext` and `inputData`:

| Header (from `CasehubWorkerHeaders`) | Value |
|---|---|
| `casehub-worker-id` | `worker.getName()` |
| `casehub-idempotency` | `ctx.idempotency()` (inputDataHash) |
| `casehub-case-id` | `instance.getUuid().toString()` |
| `casehub-tenancy-id` | `instance.tenancyId` |
| `casehub-task-type` | `capability.getName()` |
| `casehub-callback-token` | `ctx.callbackToken()` (async only) |

Body: `inputData` serialised as JSON (the evaluated input schema — already a `Map<String, Object>`).

### 5.8 `CasehubCamelComponent` — `casehub:complete`

Camel component registered as `casehub:`. Endpoint path `complete` is the only path in scope.

On `process(Exchange exchange)`:
1. Read `casehub-worker-id` header — absent → `IllegalStateException` (route-author error).
2. Check `casehub-work-status: FAULTED` header OR exchange exception present.
3. If faulted: `asyncWorkerCompletionRegistry.complete(workerId)` → `completionPublisher.fault(pending.correlationContext())`.
4. Otherwise: extract body as `Map<String, Object>` (null or wrong type → `Map.of()`). Call `asyncWorkerCompletionRegistry.complete(workerId)` → `completionPublisher.complete(ctx, output)`.

Body contract for route authors: set the exchange body to `Map<String, Object>` representing the worker's output before routing to `casehub:complete`. The component reads body with `exchange.getIn().getBody(Map.class)`. Non-Map body (String, POJO, null) → treated as empty output `Map.of()` without error.

Camel component registration: `META-INF/services/org/apache/camel/component/casehub` pointing to `CasehubComponent`. The component creates `CasehubEndpoint` which creates `CasehubProducer` — the `process()` method lives in `CasehubProducer`.

---

## 6. `workers-testing`

All fixtures usable by `workers-http`, `workers-camel`, `workers-script`, and any future worker module. Test scope only — never compile or runtime.

| Fixture | Purpose |
|---|---|
| `MockAsyncWorkerCompletionRegistry` | Captures registrations; exposes `triggerCompletion(workerId, output)` and `triggerFault(workerId)` for test control |
| `CapturingWorkerStatusPublisher` | Records all `onWorkerStarted/Completed/Stalled` calls |
| `TestCamelWorkerRoute` | Sample `CamelWorkerRoute` SPI impl; exchange pattern configurable |
| `WorkflowCompletionCaptor` | Captures `WorkflowExecutionCompleted` events fired on event bus — asserts on them in tests |
| `WorkerTestSupport` | Static helpers: `correlationContext(instance, worker)`, `completedPayload(output)`, `faultedPayload()` |

---

## 7. Test Coverage

### `workers-common`

- `AsyncWorkerCompletionRegistry`: register → complete (success); register → expire (stalled); second `complete()` on same workerId (no-op — returns empty); wrong callbackToken (rejected before registry lookup)
- `WorkerCallbackResource`: 200 on valid token + completion; 401 on wrong token; 404 on unknown workerId; 200 idempotent on second call after completion
- TTL: `expireStale()` fires `WorkerStatusPublisher.onWorkerStalled()` for expired entries
- `WorkflowCompletionPublisher`: verify event fired on `WORKER_EXECUTION_FINISHED` with correct `idempotency`, `caseInstance`, `worker`

### `workers-camel`

- Sync path: route invoked with correct headers; response body mapped to `WorkflowExecutionCompleted.output`; `WorkflowCompletionCaptor` asserts event fired within `submit()`
- Sync fault: route throws → `WorkflowExecutionCompleted` fired with empty output
- Async Path A: `PendingCompletion` registered; `submit()` returns immediately; `to("casehub:complete")` fires `WorkflowExecutionCompleted`
- Async Path B: `PendingCompletion` registered; `POST /workers/complete/{workerId}` with valid token fires `WorkflowExecutionCompleted`; wrong token returns 401
- Async timeout: TTL expiry fires `onWorkerStalled()`
- Resolver — convention: both route ID = capability AND `from: direct:{capability}` required; partial match → `WorkerProvisioningException`
- Resolver — config: full Camel URI in property overrides convention
- Resolver — SPI: `CamelWorkerRoute` bean overrides config
- Multi-capability: first matched capability wins; unmatched → `WorkerProvisioningException`
- Startup validation: `exchangePattern()` vs actual route pattern mismatch → `IllegalStateException` at startup
- Header propagation: `casehub-idempotency`, `casehub-tenancy-id`, `casehub-callback-token` appear on exchange
- Tenant isolation: `callbackToken` is per-registration; a token for workerId A cannot complete workerId B

---

## 8. Open questions (out of scope for this spec)

- `workers-common` migration to `casehub-engine` alongside Drools and Flow
- `EndpointRegistry` integration (platform#73)
- Distributed `AsyncWorkerCompletionRegistry` for multi-node (Infinispan/Redis)
- Faulted path: should `WorkflowExecutionCompleted` with empty output trigger the engine's retry policy, or should a separate `WorkflowExecutionFailed`/`WORKER_RETRIES_EXHAUSTED` event be used? The current spec uses empty-output completion; confirm with engine team before implementing.
- `workers-http`, `workers-script`, `workers-k8s-job` specs
