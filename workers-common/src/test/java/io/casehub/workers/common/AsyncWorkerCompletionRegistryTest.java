package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import jakarta.enterprise.event.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AsyncWorkerCompletionRegistryTest {

    private AsyncWorkerCompletionRegistry registry;
    private List<CompletionExpiredEvent> firedEvents;

    @BeforeEach
    void setUp() {
        firedEvents = new CopyOnWriteArrayList<>();
        Event<CompletionExpiredEvent> capturingEvent = new CapturingEvent<>(firedEvents);
        registry = new AsyncWorkerCompletionRegistry();
        registry.expiryEvents = capturingEvent;
    }

    @Test
    void register_generatesUniqueDispatchIdAndCallbackToken() {
        WorkerCorrelationContext ctx = testContext();
        PendingCompletion p1 = registry.register("camel", ctx, testCapability(), 1L, Duration.ofMinutes(60), Map.of());
        PendingCompletion p2 = registry.register("camel", ctx, testCapability(), 2L, Duration.ofMinutes(60), Map.of());
        assertThat(p1.dispatchId()).isNotEqualTo(p2.dispatchId());
        assertThat(p1.callbackToken()).isNotEqualTo(p2.callbackToken());
        assertThat(p1.workerType()).isEqualTo("camel");
    }

    @Test
    void complete_returnsAndRemoves() {
        PendingCompletion pending = registry.register("camel", testContext(), testCapability(), 1L, Duration.ofMinutes(60), Map.of());
        Optional<PendingCompletion> completed = registry.complete(pending.dispatchId());
        assertThat(completed).isPresent().contains(pending);
        Optional<PendingCompletion> second = registry.complete(pending.dispatchId());
        assertThat(second).isEmpty();
    }

    @Test
    void complete_unknownDispatchId_returnsEmpty() {
        assertThat(registry.complete("nonexistent")).isEmpty();
    }

    @Test
    void countByWorkerName_countsActiveDispatches() {
        WorkerCorrelationContext ctx = testContext();
        registry.register("camel", ctx, testCapability(), 1L, Duration.ofMinutes(60), Map.of());
        registry.register("camel", ctx, testCapability(), 2L, Duration.ofMinutes(60), Map.of());
        assertThat(registry.countByWorkerName(ctx.worker().getName())).isEqualTo(2);
    }

    @Test
    void expireStale_firesEventForExpiredEntries() {
        WorkerCorrelationContext ctx = testContext();
        registry.register("camel", ctx, testCapability(), 1L, Duration.ofSeconds(-1), Map.of());
        PendingCompletion live = registry.register("camel", ctx, testCapability(), 2L, Duration.ofMinutes(60), Map.of());
        registry.expireStale();
        assertThat(firedEvents).hasSize(1);
        assertThat(firedEvents.get(0).pending().eventLogId()).isEqualTo(1L);
        assertThat(registry.complete(live.dispatchId())).isPresent();
    }

    private WorkerCorrelationContext testContext() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = new Worker("test-worker", List.of(new Capability("cap", "", "")), (ctx) -> null);
        return new WorkerCorrelationContext(instance, worker, "hash", "t1");
    }

    private Capability testCapability() {
        return new Capability("send-email", "", "");
    }

    @SuppressWarnings("unchecked")
    static class CapturingEvent<T> implements Event<T> {
        private final List<T> captured;
        CapturingEvent(List<T> captured) { this.captured = captured; }
        @Override public void fire(T event) { captured.add(event); }
        @Override public <U extends T> java.util.concurrent.CompletionStage<U> fireAsync(U event) {
            captured.add(event);
            return java.util.concurrent.CompletableFuture.completedFuture(event);
        }
        @Override public <U extends T> java.util.concurrent.CompletionStage<U> fireAsync(U event, jakarta.enterprise.event.NotificationOptions options) {
            return fireAsync(event);
        }
        @Override public Event<T> select(java.lang.annotation.Annotation... qualifiers) { return this; }
        @Override public <U extends T> Event<U> select(Class<U> subtype, java.lang.annotation.Annotation... qualifiers) { return (Event<U>) this; }
        @Override public <U extends T> Event<U> select(jakarta.enterprise.util.TypeLiteral<U> subtype, java.lang.annotation.Annotation... qualifiers) { return (Event<U>) this; }
    }
}
