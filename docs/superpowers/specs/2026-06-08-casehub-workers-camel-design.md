# CaseHub Workers — Camel Worker Design (Revised v4)

**Date:** 2026-06-08
**Status:** Approved — pending implementation plan
**Revision:** v4 — third review cycle; NC1–NC4 + ND1–ND5 + NM1–NM2 addressed

---

## 1. Overview

`casehub-workers` is a multi-module Maven repo at the Integration tier. It provides `ReactiveWorkerProvisioner` and `WorkerExecutionManager` SPI implementations allowing CaseHub cases to dispatch work to different execution runtimes. Apache Camel (300+ connectors) is the first non-trivial implementation.

This spec covers:
- **`workers-common`** — general worker infrastructure shared by all worker implementations. Future migration target: `casehub-engine`, alongside Drools and Flow.
- **`workers-camel`** — Apache Camel adapter implementing both `ReactiveWorkerProvisioner` and `WorkerExecutionManager`.

### Endpoint registry

`casehub-endpoints` is being designed separately in `casehubio/platform` (platform#73). Until then, `casehub.workers.camel.capabilities.<tag>` accepts any full Camel URI (e.g. `kafka:my-topic?brokers=localhost:9092`) as inline connection config. When `EndpointRegistry` ships, `CamelWorkerRoute` SPI beans can inject it to resolve endpoint names at route-build time.

---

## 2. Engine Integration

### 2.1 ReactiveWorkerProvisioner — capability probe

Called from `CaseContextChangedEventHandler.tryProvision()` when no pre-defined case-definition worker matches a required capability:

1. Engine calls `getCapabilities()` — checks provisioner's supported set.
2. If the needed capability is present, calls `provision(caps, provisionContext)`.
3. After provision returns, fires `CaseLifecycleEvent("WorkerStarted")` — notification only.
4. Does **not** fire `WorkflowExecutionCompleted`. No work dispatched yet.

For Camel: `provision()` validates the route exists and returns `ProvisionResult.empty()`. Routes are always-running; nothing to spin up.

### 2.2 WorkerExecutionManager — work dispatch and completion

Called from `WorkerScheduleEventHandler` for every work dispatch:

1. Computes `inputDataHash = WorkerExecutionKeys.inputDataHash(caseId, workerName, capabilityName, inputData)` — the idempotency key matching `caseInstance.getWaitingForWorkId()`.
2. Calls `workerExecutionManager.submit(eventLogId, instance, worker, capability, inputData)`.
3. On success: execution manager fires `WorkflowExecutionCompleted` on `WORKER_EXECUTION_FINISHED`.
4. On fault: execution manager fires on `CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT` (NOT `WORKFLOW_EXECUTION_FAILED` — see Section 2.4).
5. `WorkflowExecutionCompletedHandler` and `PlanItemCompletionHandler` both consume `WORKER_EXECUTION_FINISHED` via `eventBus.publish()`.

### 2.3 CDI resolution and co-deployment constraint

`NoOpReactiveWorkerProvisioner` and `NoOpWorkerExecutionManager` are `@DefaultBean @ApplicationScoped`. `CamelReactiveWorkerProvisioner` and `CamelWorkerExecutionManager` are plain `@ApplicationScoped` (no `@DefaultBean`) — CDI displaces the no-op defaults when Camel beans are present.

**Co-deployment constraint:** `QuartzWorkerExecutionManager` is also `@ApplicationScoped` (not `@DefaultBean`). If `scheduler-quartz` and `workers-camel` are both on the classpath, CDI has two non-default `WorkerExecutionManager` beans and fails with an ambiguous dependency at startup. `workers-camel` is not designed for co-deployment with `scheduler-quartz`. Applications requiring both must implement a composite `WorkerExecutionManager` that routes by worker type — this is a separate concern tracked in a follow-on engine issue (see Section 8).

### 2.4 Fault event bus address isolation

`QuartzWorkerExecutionJobListener` handles `WORKFLOW_EXECUTION_FAILED` with no worker-type filter — it processes all events on that address. If Camel fires on `WORKFLOW_EXECUTION_FAILED`, two things happen: (1) Quartz schedules a Quartz job for a Camel worker (wrong execution type), and (2) the failure count is incremented twice (one write per handler), causing the retry comparison to be off by one.

**The fix: Camel faults use a separate event bus address**, defined in `workers-camel`:

```java
// workers-camel
public final class CamelWorkerEventBusAddresses {
    public static final String CAMEL_WORKER_FAULT = "casehub.workers.camel.fault";
}
```

`CamelWorkerFaultPublisher` fires on `CAMEL_WORKER_FAULT`. `CamelWorkerFaultEventHandler` handles `CAMEL_WORKER_FAULT`. `QuartzWorkerExecutionJobListener` never sees Camel faults; `CamelWorkerFaultEventHandler` never sees Quartz faults.

`WorkflowCompletionPublisher` in `workers-common` has no `fail()` method. Fault publication is worker-type-specific and lives in `workers-camel`.

### 2.5 Module dependencies

`workers-common` depends on:
- `casehub-engine-api` — `ReactiveWorkerProvisioner`, `ReactiveWorkerStatusListener`, `ProvisionResult`, `ProvisionContext`, `WorkResult`, `Worker`, `Capability`, `ExecutionPolicy`, `RetryPolicy`, `BackoffStrategy`
- `casehub-engine-common` — `WorkerExecutionManager`, `WorkflowExecutionCompleted`, `CaseInstance`, `EventBusAddresses`, `WorkerExecutionKeys`, `EventLogRepository`

Same dependency pattern as claudony.

---

## 3. Module Structure

```
casehub-workers/
  workers-common/         ← must be first in parent POM <modules>
  workers-http/
  workers-camel/
  workers-testing/
```

---

## 4. `workers-common`

### 4.1 Core types

**`WorkerCorrelationContext`**:

```java
record WorkerCorrelationContext(
    CaseInstance caseInstance,   // mutable — see note below
    Worker worker,
    String idempotency,          // WorkerExecutionKeys.inputDataHash — matches waitingForWorkId
    String tenancyId
) {}
```

**Mutable CaseInstance note:** `CaseInstance` is mutable — the engine calls `setState()` and `setWaitingForWorkId()` during normal operation. The registry holds this reference for up to `casehub.workers.async.timeout-minutes` (default: 60 minutes). `resumeIfWaiting()` inside `WorkflowExecutionCompletedHandler` checks `caseInstance.getState() == WAITING` before resuming — it no-ops if the case is already terminal. Stale-reference risk is a known constraint shared with claudony; more significant at 60-minute TTLs. Mitigation (deferred): load a fresh `CaseInstance` from `CrossTenantCaseInstanceRepository` at completion time.

**`PendingCompletion`** — registry generates `dispatchId` and `callbackToken`:

```java
record PendingCompletion(
    String dispatchId,              // UUID generated by registry — registry key AND casehub-worker-id header value
    WorkerCorrelationContext correlationContext,
    String callbackToken,           // UUID for REST callback auth; generated by registry
    Capability capability,          // needed to construct fault events on timeout
    Long eventLogId,                // needed for WORKER_EXECUTION_FAILED event log entry
    Instant registeredAt,
    Instant expiresAt,
    Map<String, String> provisionerMeta
) {}
```

`dispatchId` is the stable per-dispatch unique key — prevents the collision that `worker.getName()` would cause when two concurrent dispatches of the same capability for different cases are registered simultaneously.

**`CompletionExpiredEvent`** — CDI event fired by `AsyncWorkerCompletionRegistry.expireStale()`:

```java
record CompletionExpiredEvent(PendingCompletion pending) {}
```

**`WorkerCompletionPayload`** — REST callback body:

```java
record WorkerCompletionPayload(
    Map<String, Object> output,
    boolean faulted,
    String errorMessage   // nullable; informational when faulted=true
) {}
```

**`CasehubWorkerHeaders`**:

```java
public final class CasehubWorkerHeaders {
    public static final String WORKER_ID        = "casehub-worker-id";   // value = dispatchId UUID
    public static final String IDEMPOTENCY      = "casehub-idempotency";
    public static final String CASE_ID          = "casehub-case-id";
    public static final String TENANCY_ID       = "casehub-tenancy-id";
    public static final String TASK_TYPE        = "casehub-task-type";
    public static final String CALLBACK_TOKEN   = "casehub-callback-token";
    public static final String WORK_STATUS      = "casehub-work-status";  // FAULTED override
}
```

**`WorkerProvisioningException`**, **`WorkerCapabilityResolver<T>`** (plain interface, not `@FunctionalInterface`): unchanged from v3.

### 4.2 Services

**`WorkflowCompletionPublisher`** — fires `WORKER_EXECUTION_FINISHED` using `eventBus.publish()`. No `fail()` method — fault publication is worker-type-specific:

```java
@ApplicationScoped
public class WorkflowCompletionPublisher {
    @Inject EventBus eventBus;

    /** Fires to all consumers of WORKER_EXECUTION_FINISHED (publish, not request). */
    public void complete(WorkerCorrelationContext ctx, Map<String, Object> output) {
        eventBus.publish(EventBusAddresses.WORKER_EXECUTION_FINISHED,
            WorkflowExecutionCompleted.approved(
                ctx.caseInstance(), ctx.worker(), ctx.idempotency(), output));
    }
}
```

`eventBus.publish()` delivers to all consumers — `WorkflowExecutionCompletedHandler` (applies output, resumes case) and `PlanItemCompletionHandler` (marks PlanItems COMPLETED). Using `request()` instead would be point-to-point and silently break one of the two handlers. This is the same bug documented in the engine's 2026-04-22 diary entry (`CaseStartedEventHandler` used `request()` and caused rotating delivery).

**`WorkerStatusPublisher`** — lifecycle notifications. Names mirror `ReactiveWorkerStatusListener` exactly:

```java
@ApplicationScoped
public class WorkerStatusPublisher {
    @Inject ReactiveWorkerStatusListener reactiveWorkerStatusListener;
    public Uni<Void> onWorkerStarted(String dispatchId, Map<String, String> sessionMeta) { ... }
    public Uni<Void> onWorkerCompleted(String dispatchId, WorkResult result) { ... }
    public Uni<Void> onWorkerStalled(String dispatchId) { ... }
}
```

**`WorkerProvisionerSupport`**:
- `validateCapabilities(Set<String> requested, Set<String> supported)` — throws `WorkerProvisioningException` if **any** requested capability is absent. Void return — strict all-or-nothing guard. **Not appropriate for `provision()`** where the engine passes all capabilities and the provisioner handles a subset.
- `tenancyId(ProvisionContext ctx)`, `wrap(Throwable t, String capability)`.

**`AsyncWorkerCompletionRegistry`**:

```java
@ApplicationScoped
public class AsyncWorkerCompletionRegistry {
    @Inject Event<CompletionExpiredEvent> expiryEvents;

    /** Registry generates dispatchId (UUID) and callbackToken (UUID). */
    PendingCompletion register(WorkerCorrelationContext ctx, Capability capability,
                               Long eventLogId, Duration ttl, Map<String, String> provisionerMeta);

    /** Remove and return; empty if dispatchId unknown or already completed (idempotent). */
    Optional<PendingCompletion> complete(String dispatchId);

    /** Count active dispatches for a given worker name (for getActiveWorkCount). */
    int countByWorkerName(String workerName);

    /**
     * Scheduled: removes expired entries and fires CompletionExpiredEvent per entry.
     * Fires CDI async — workers-common does not know about engine event bus addresses.
     */
    void expireStale();
}
```

TTL: `casehub.workers.async.timeout-minutes` (default: 60).

**Deployment constraint:** Registry is JVM-local. `POST /workers/complete/{dispatchId}` must reach the originating JVM. Multi-node deployments require sticky load balancing or a distributed registry.

**`WorkerCallbackResource`**:

```
POST /workers/complete/{dispatchId}
Headers: X-Casehub-Callback-Token: <token>
Body: WorkerCompletionPayload
```

Validates token constant-time. Calls `AsyncWorkerCompletionRegistry.complete(dispatchId)`. On success: calls `WorkflowCompletionPublisher.complete()` (faulted=false) or notifies via fault path (faulted=true — how the fault is signalled is worker-specific; the resource calls an injected `WorkerFaultNotifier` SPI, described in Section 5.9). Returns 200 idempotently; 404 if `dispatchId` never registered; 401 on bad token.

**`WorkerFaultNotifier` SPI** — `workers-common` needs to notify on fault from `WorkerCallbackResource` without knowing the Camel event bus address. This SPI decouples the resource from the address:

```java
public interface WorkerFaultNotifier {
    void notifyFault(PendingCompletion pending, String errorMessage);
}
```

`CamelWorkerFaultNotifier` implements it in `workers-camel` and fires on `CAMEL_WORKER_FAULT`. `NoOpWorkerFaultNotifier @DefaultBean` is the fallback.

---

## 5. `workers-camel`

Depends on `workers-common`. Implements `ReactiveWorkerProvisioner`, `WorkerExecutionManager`, `CamelWorkerFaultEventHandler`, `CamelCompletionExpiryObserver`.

### 5.1 `CamelWorkerEventBusAddresses`

```java
public final class CamelWorkerEventBusAddresses {
    /** Published by CamelWorkerFaultPublisher for all Camel worker faults (sync and async). */
    public static final String CAMEL_WORKER_FAULT = "casehub.workers.camel.fault";
}
```

This address is separate from `EventBusAddresses.WORKFLOW_EXECUTION_FAILED` (Quartz/Flow). `QuartzWorkerExecutionJobListener` never sees events on `CAMEL_WORKER_FAULT`. `CamelWorkerFaultEventHandler` never sees events on `WORKFLOW_EXECUTION_FAILED`.

### 5.2 `CamelWorkerFaultPublisher`

Fires Camel faults. Reuses `WorkflowExecutionFailed` type (carries all needed retry context) but on the Camel-specific address:

```java
@ApplicationScoped
public class CamelWorkerFaultPublisher {
    @Inject EventBus eventBus;

    public void fault(PendingCompletion pending, Throwable cause) {
        eventBus.publish(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT,
            new WorkflowExecutionFailed(
                pending.correlationContext().caseInstance(),
                pending.correlationContext().worker(),
                pending.capability(),
                pending.correlationContext().idempotency(),   // = inputDataHash
                pending.eventLogId().toString(),
                cause));
    }

    /** For faults detected without a PendingCompletion (e.g. sync path). */
    public void fault(WorkerCorrelationContext ctx, Capability capability,
                      Long eventLogId, Throwable cause) {
        eventBus.publish(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT,
            new WorkflowExecutionFailed(
                ctx.caseInstance(), ctx.worker(), capability,
                ctx.idempotency(), eventLogId.toString(), cause));
    }
}
```

Also implements `WorkerFaultNotifier` (Section 4.2) for REST callback path:
```java
@Override
public void notifyFault(PendingCompletion pending, String errorMessage) {
    fault(pending, errorMessage != null ? new RuntimeException(errorMessage) : null);
}
```

### 5.3 Capability-to-route resolution — `CamelCapabilityResolver`

Implements `WorkerCapabilityResolver<String>`. Three-tier resolution (SPI → Config → Convention — unchanged from v3). `CamelCapabilityResolver` itself observes `StartupEvent @Priority(APPLICATION)` to initialize the capability cache. This ensures both `CamelReactiveWorkerProvisioner.getCapabilities()` and `CamelWorkerExecutionManager.submit()` read from a populated cache regardless of which bean is activated first:

```java
@ApplicationScoped
public class CamelCapabilityResolver implements WorkerCapabilityResolver<String> {

    @Observes @Priority(APPLICATION)
    void onStartup(StartupEvent ev) {
        this.initialize();  // idempotent; safe if called multiple times
    }
    // ...
}
```

Exchange pattern validation for SPI-registered `CamelWorkerRoute` beans runs in the same `onStartup()` — `exchangePattern()` vs actual Camel route pattern. Mismatch → `IllegalStateException`. Convention and config routes are trusted to declare correct patterns in their Camel DSL — no external validation possible.

**Multi-capability dispatch:** `firstMatch(Set<String>)` returns the first resolvable capability. One route per dispatch. The engine schedules capabilities independently.

**Convention:** Both conditions must hold — route ID = capability tag AND `from: direct:{capabilityTag}`. Either alone is insufficient.

### 5.4 `CamelReactiveWorkerProvisioner`

```java
@ApplicationScoped
public class CamelReactiveWorkerProvisioner implements ReactiveWorkerProvisioner {

    @Override
    public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
        // Engine passes ALL case capabilities; use firstMatch — validateCapabilities() is wrong
        // here because it rejects any unsupported capability, but the engine expects partial match.
        String capability = camelCapabilityResolver.firstMatch(capabilities)
            .orElseThrow(() -> WorkerProvisioningException.noRouteFound(capabilities.toString()));
        camelCapabilityResolver.resolve(capability); // validates route registered; throws if not
        return Uni.createFrom().item(ProvisionResult.empty());
    }

    @Override
    public Uni<Void> terminate(String workerId) {
        return Uni.createFrom().voidItem(); // routes are always-running
    }

    @Override
    public Uni<Set<String>> getCapabilities() {
        // CamelCapabilityResolver.onStartup() guarantees cache is populated before any engine call
        return Uni.createFrom().item(camelCapabilityResolver.capabilities());
    }
}
```

### 5.5 `CamelWorkerExecutionManager`

```java
@ApplicationScoped
public class CamelWorkerExecutionManager implements WorkerExecutionManager {

    @Override
    public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                            Capability capability, Map<String, Object> inputData) {
        String entryUri;
        try {
            entryUri = camelCapabilityResolver.resolve(capability.getName());
        } catch (WorkerProvisioningException e) {
            // Route removed after startup — operator error. Fire fault so case eventually fails
            // rather than hanging WAITING indefinitely.
            LOG.errorf("Camel route for capability %s missing at dispatch time", capability.getName());
            camelWorkerFaultPublisher.fault(
                new WorkerCorrelationContext(instance, worker,
                    WorkerExecutionKeys.inputDataHash(instance.getUuid(), worker.getName(),
                        capability.getName(), inputData), instance.tenancyId),
                capability, eventLogId, e);
            return Uni.createFrom().voidItem();
        }

        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.getName(), capability.getName(), inputData);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            instance, worker, idempotency, instance.tenancyId);
        ExchangePattern pattern = camelCapabilityResolver.exchangePattern(capability.getName());

        return pattern == ExchangePattern.InOut
            ? submitSync(ctx, entryUri, capability, inputData, eventLogId)
            : submitAsync(ctx, entryUri, capability, eventLogId, inputData);
    }

    @Override
    public Uni<Void> schedulePersistedEvent(EventLog scheduledEventLog) {
        return Uni.createFrom().voidItem(); // Camel workers have no Quartz persisted events
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return asyncWorkerCompletionRegistry.countByWorkerName(workerId);
    }
}
```

Missing route at `submit()` time fires on `CAMEL_WORKER_FAULT` — `CamelWorkerFaultEventHandler` handles retry/exhaustion. If the route is permanently gone, retries also fail quickly, exhausting the retry count and firing `WORKER_RETRIES_EXHAUSTED`. The case does not hang.

### 5.6 Sync path (ExchangePattern.InOut)

```java
private Uni<Void> submitSync(WorkerCorrelationContext ctx, String entryUri,
                              Capability capability, Map<String, Object> inputData,
                              Long eventLogId) {
    return Uni.createFrom()
        .item(() -> producerTemplate.request(entryUri, buildExchange(ctx, capability, inputData)))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .flatMap(response -> {
            boolean faulted = response.getException() != null
                || "FAULTED".equals(response.getIn().getHeader(CasehubWorkerHeaders.WORK_STATUS));
            if (faulted) {
                camelWorkerFaultPublisher.fault(ctx, capability, eventLogId, response.getException());
            } else {
                completionPublisher.complete(ctx, extractOutput(response));
            }
            return Uni.createFrom().voidItem();
        })
        .onFailure().call(t -> {
            camelWorkerFaultPublisher.fault(ctx, capability, eventLogId, t);
            return Uni.createFrom().voidItem();
        });
}
```

Faults fire on `CAMEL_WORKER_FAULT`. `submit()` always returns `Uni.voidItem()` — fault handling is asynchronous via the event bus.

### 5.7 Async path (ExchangePattern.InOnly)

```java
private Uni<Void> submitAsync(WorkerCorrelationContext ctx, String entryUri,
                               Capability capability, Long eventLogId,
                               Map<String, Object> inputData) {
    PendingCompletion pending = asyncWorkerCompletionRegistry.register(
        ctx, capability, eventLogId, Duration.ofMinutes(asyncTimeoutMinutes), Map.of());

    Exchange exchange = buildExchange(ctx, capability, inputData);
    exchange.getIn().setHeader(CasehubWorkerHeaders.WORKER_ID, pending.dispatchId());
    exchange.getIn().setHeader(CasehubWorkerHeaders.CALLBACK_TOKEN, pending.callbackToken());

    return Uni.createFrom().voidItem()
        .invoke(() -> producerTemplate.send(entryUri, exchange));
}
```

### 5.8 Exchange input mapping

| Header (`CasehubWorkerHeaders`) | Value |
|---|---|
| `casehub-worker-id` | `pending.dispatchId()` — per-dispatch UUID |
| `casehub-idempotency` | `ctx.idempotency()` — `WorkerExecutionKeys.inputDataHash(...)` |
| `casehub-case-id` | `instance.getUuid().toString()` |
| `casehub-tenancy-id` | `instance.tenancyId` |
| `casehub-task-type` | `capability.getName()` |
| `casehub-callback-token` | `pending.callbackToken()` (async only) |

Body: `inputData` serialised as JSON.

### 5.9 `CasehubCamelComponent` — `casehub:complete`

Camel `Processor.process(Exchange)` is synchronous. Camel threads are standard Java threads (not Vert.x IO threads). `eventBus.publish()` is non-blocking — it enqueues and returns immediately. The processor blocks briefly on the publish call and then returns. Engine consumers process the event asynchronously.

```java
@Override
public void process(Exchange exchange) throws Exception {
    String dispatchId = exchange.getIn().getHeader(CasehubWorkerHeaders.WORKER_ID, String.class);
    if (dispatchId == null) {
        throw new IllegalStateException("casehub-worker-id header missing on casehub:complete");
    }

    Optional<PendingCompletion> pending = asyncWorkerCompletionRegistry.complete(dispatchId);
    if (pending.isEmpty()) {
        LOG.warnf("casehub:complete — dispatchId %s not found (already resolved or expired)", dispatchId);
        return;
    }

    boolean faulted = exchange.getException() != null
        || "FAULTED".equals(exchange.getIn().getHeader(CasehubWorkerHeaders.WORK_STATUS));

    if (faulted) {
        camelWorkerFaultPublisher.fault(pending.get(), exchange.getException());
    } else {
        Map<String, Object> output = exchange.getIn().getBody(Map.class);
        completionPublisher.complete(pending.get().correlationContext(),
                                     output != null ? output : Map.of());
    }
}
```

Body contract: set exchange body to `Map<String, Object>` before routing to `casehub:complete`. Non-Map body → treated as `Map.of()`.

### 5.10 Fault handling — two separate beans

**`CamelWorkerFaultEventHandler`** — handles Vert.x event bus events on `CAMEL_WORKER_FAULT`:

```java
@ApplicationScoped
public class CamelWorkerFaultEventHandler {

    @ConsumeEvent(value = CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT, blocking = true)
    public void onFault(WorkflowExecutionFailed event) {
        CaseInstance instance = event.caseInstance();
        Worker worker = event.worker();
        String inputDataHash = event.inputDataHash();
        String tenancyId = instance.tenancyId;

        // 1. Persist WORKER_EXECUTION_FAILED — countFailedAttempts queries this count
        EventLog failureLog = new EventLog();
        failureLog.setCaseId(instance.getUuid());
        failureLog.setWorkerId(worker.getName());
        failureLog.setEventType(CaseHubEventType.WORKER_EXECUTION_FAILED);
        failureLog.setStreamType(EventStreamType.CASE);
        failureLog.setTimestamp(Instant.now());
        failureLog.setMetadata(OBJECT_MAPPER.createObjectNode()
            .put("inputDataHash", inputDataHash)
            .put("errorMessage", event.cause() != null ? event.cause().getMessage() : "unknown"));

        eventLogRepository.append(failureLog, tenancyId)
            .flatMap(ignored -> countFailedAttempts(instance.getUuid(), worker.getName(),
                                                    inputDataHash, tenancyId))
            .flatMap(failureCount -> {
                RetryPolicy retryPolicy = resolveRetryPolicy(instance, worker);
                if (retryPolicy != null && failureCount < retryPolicy.maxAttempts()) {
                    long delayMs = computeBackoffDelayMs(retryPolicy, failureCount + 1);
                    return reloadAndResubmit(event, delayMs);
                } else {
                    // Exhausted — WorkerRetriesExhaustedEvent.idempotency maps to inputDataHash
                    eventBus.publish(EventBusAddresses.WORKER_RETRIES_EXHAUSTED,
                        new WorkerRetriesExhaustedEvent(
                            instance.getUuid(), worker.getName(), inputDataHash));
                    return Uni.createFrom().voidItem();
                }
            })
            .subscribe().with(ignored -> {}, ex ->
                LOG.errorf(ex, "Fault handling failed for worker %s case %s",
                           worker.getName(), instance.getUuid()));
    }

    private Uni<Long> countFailedAttempts(UUID caseId, String workerId,
                                           String inputDataHash, String tenancyId) {
        // Mirror of QuartzWorkerExecutionJobListener.countFailedAttempts()
        return eventLogRepository
            .findByCaseAndWorkerAndType(caseId, workerId, CaseHubEventType.WORKER_EXECUTION_FAILED, tenancyId)
            .map(logs -> logs.stream()
                .filter(log -> {
                    JsonNode meta = log.getMetadata();
                    JsonNode node = meta == null ? null : meta.get("inputDataHash");
                    return node != null && inputDataHash.equals(node.asText());
                })
                .count());
    }

    private RetryPolicy resolveRetryPolicy(CaseInstance instance, Worker worker) {
        // Mirror of QuartzWorkerExecutionJobListener.resolveRetryPolicy()
        ExecutionPolicy policy = worker.getExecutionPolicy();
        if (policy == null || policy.retries() == null) {
            return null;  // no retries configured
        }
        return policy.retries();
    }

    /** Same computation as QuartzWorkerExecutionJobListener.computeBackoffDelayMs(). */
    private static long computeBackoffDelayMs(RetryPolicy policy, long attemptNumber) {
        long baseDelayMs = policy.delayMs() != null ? policy.delayMs() : 0L;
        BackoffStrategy strategy = policy.backoffStrategy() != null
            ? policy.backoffStrategy() : BackoffStrategy.FIXED;
        return switch (strategy) {
            case FIXED -> baseDelayMs;
            case EXPONENTIAL -> {
                long shift = Math.min(attemptNumber - 1, 30);
                yield Math.min(baseDelayMs * (1L << shift), 30_000L);
            }
            case EXPONENTIAL_WITH_JITTER -> {
                long shift = Math.min(attemptNumber - 1, 30);
                long cap = Math.min(baseDelayMs * (1L << shift), 30_000L);
                yield cap == 0 ? 0 : ThreadLocalRandom.current().nextLong(cap + 1);
            }
        };
    }

    private Uni<Void> reloadAndResubmit(WorkflowExecutionFailed event, long delayMs) {
        return eventLogRepository
            .findById(Long.parseLong(event.eventLogId()), event.caseInstance().tenancyId)
            .flatMap(eventLog -> {
                Map<String, Object> inputData =
                    OBJECT_MAPPER.convertValue(eventLog.getPayload(), MAP_TYPE);
                return Uni.createFrom().completionStage(() ->
                    CompletableFuture.runAsync(
                        () -> {}, CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS)))
                    .flatMap(ignored -> camelWorkerExecutionManager.submit(
                        Long.parseLong(event.eventLogId()),
                        event.caseInstance(), event.worker(), event.capability(), inputData));
            });
    }
}
```

`reloadAndResubmit()` calls `camelWorkerExecutionManager.submit()` directly, bypassing `WorkerScheduleEventHandler.scheduleUnderLock()`. **Known gap:** concurrent expiry events (e.g., two `expireStale()` scheduler ticks before the first is processed) could produce two retry dispatches. Since `AsyncWorkerCompletionRegistry.complete(dispatchId)` uses `ConcurrentHashMap.remove()` (atomic), the first caller wins and the second gets `Optional.empty()`. The gap is narrow and bounded: it can only occur when `expireStale()` is scheduled faster than it completes, which is prevented by keeping TTL much larger than the scheduler interval. Document as known; add a scheduled-interval guard in configuration.

**`CamelCompletionExpiryObserver`** — separate CDI bean observing async CDI events. Splits event dispatch mechanisms cleanly:

```java
@ApplicationScoped
public class CamelCompletionExpiryObserver {

    @Inject CamelWorkerFaultPublisher faultPublisher;

    void onExpiry(@ObservesAsync CompletionExpiredEvent event) {
        PendingCompletion pending = event.pending();
        faultPublisher.fault(pending, new RuntimeException("Async timeout — no completion received"));
    }
}
```

`CamelWorkerFaultEventHandler` then handles the resulting `CAMEL_WORKER_FAULT` event via its normal path.

---

## 6. `workers-testing`

```
MockAsyncWorkerCompletionRegistry    — captures registrations; exposes triggerCompletion(dispatchId, output) and triggerFault(dispatchId, cause)
CapturingWorkerStatusPublisher       — records all onWorkerStarted/Completed/Stalled calls
TestCamelWorkerRoute                 — sample CamelWorkerRoute SPI impl; exchange pattern configurable
WorkflowCompletionCaptor             — captures WorkflowExecutionCompleted events on event bus
CamelWorkerFaultCaptor               — captures WorkflowExecutionFailed events on CAMEL_WORKER_FAULT
WorkerTestSupport                    — static helpers: correlationContext(instance, worker), completedPayload(output), faultedPayload(msg)
```

Never compile or runtime dependency — test scope only.

---

## 7. Test Coverage

### `workers-common`

- `AsyncWorkerCompletionRegistry`: two concurrent `register()` calls for same `worker.getName()` → distinct `dispatchId` keys; both resolvable independently
- `complete(dispatchId)` → success; second call → empty (idempotent)
- Wrong `callbackToken` → 401 before registry lookup
- `expireStale()` fires `CompletionExpiredEvent` CDI event per expired entry

### `workers-camel`

- **NC1 isolation:** `CamelWorkerFaultEventHandler` handles `CAMEL_WORKER_FAULT`; `QuartzWorkerExecutionJobListener` does NOT fire for Camel faults (separate address — verify in integration test that `findByCaseAndWorkerAndType(WORKER_EXECUTION_FAILED)` returns exactly 1 entry after a Camel fault, not 2)
- Sync fault: `CamelWorkerFaultCaptor` asserts `WorkflowExecutionFailed` on `CAMEL_WORKER_FAULT`; exactly one `WORKER_EXECUTION_FAILED` event log entry written
- Retry: `failureCount < retryPolicy.maxAttempts()` — strict `<`; verify backoff delay applied before re-dispatch
- Retry exhaustion: when `failureCount >= maxAttempts()`, `WORKER_RETRIES_EXHAUSTED` published with correct `(caseId, workerId, inputDataHash)` — note `inputDataHash` maps to `WorkerRetriesExhaustedEvent.idempotency()` record component
- `countFailedAttempts()`: filters by `metadata.get("inputDataHash")` — verify entries with different `inputDataHash` are not counted
- Async timeout: `CompletionExpiredEvent` → `CamelCompletionExpiryObserver` → `CAMEL_WORKER_FAULT` → `CamelWorkerFaultEventHandler`
- Missing route at `submit()`: fault fired, retry path runs, quickly exhausts if route stays missing
- Startup: `CamelCapabilityResolver.initialize()` called via `StartupEvent` — `getCapabilities()` returns non-empty before any engine call

---

## 8. Remaining open questions (deferred)

- **Composite `WorkerExecutionManager` for Quartz + Camel co-deployment**: engine must support `@Any Instance<WorkerExecutionManager>` and route by worker type. Separate engine issue — Quartz + Camel single-classpath deployment is unsupported until resolved.
- **`workers-common` migration to `casehub-engine`** alongside Drools and Flow
- **`EndpointRegistry` integration** (platform#73)
- **Distributed `AsyncWorkerCompletionRegistry`** for multi-node
- **Retry count deduplication** for concurrent expiry events — add scheduler-interval guard configuration (`casehub.workers.camel.async.expiry-check-interval-minutes` must be `<< casehub.workers.async.timeout-minutes`)
- **Stale `CaseInstance` reference**: reload fresh instance at completion time once multi-node registry ships
- **`WorkerExecutionFailed` vs empty-output `WorkflowExecutionCompleted`**: Camel uses `WorkflowExecutionFailed` + retry path; confirm with engine team that empty-output `WorkflowExecutionCompleted` (previously proposed) would bypass retry machinery (confirmed it does — fault path is correct choice)
