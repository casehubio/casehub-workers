package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkerCallbackResourceTest {

    private WorkerCallbackResource resource;
    private AsyncWorkerCompletionRegistry registry;
    private WorkflowCompletionPublisher completionPublisher;
    private List<FaultCallbackEvent> faultEvents;

    @BeforeEach
    void setUp() {
        registry = new AsyncWorkerCompletionRegistry();
        registry.expiryEvents = new AsyncWorkerCompletionRegistryTest.CapturingEvent<>(new java.util.ArrayList<>());

        completionPublisher = mock(WorkflowCompletionPublisher.class);
        faultEvents = new CopyOnWriteArrayList<>();

        resource = new WorkerCallbackResource();
        resource.registry = registry;
        resource.completionPublisher = completionPublisher;
        resource.faultCallbackEvents = new AsyncWorkerCompletionRegistryTest.CapturingEvent<>(faultEvents);
    }

    @Test
    void successfulCompletion_callsPublisher() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(Map.of("key", "value"), false, null);
        Response response = resource.complete(pending.dispatchId(), pending.callbackToken(), payload);
        assertThat(response.getStatus()).isEqualTo(200);
        verify(completionPublisher).complete(eq(pending.correlationContext()), eq(Map.of("key", "value")));
    }

    @Test
    void faultedCompletion_firesFaultCallbackEvent() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(null, true, "route failed");
        Response response = resource.complete(pending.dispatchId(), pending.callbackToken(), payload);
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(faultEvents).hasSize(1);
        assertThat(faultEvents.get(0).pending()).isEqualTo(pending);
        assertThat(faultEvents.get(0).cause().getMessage()).isEqualTo("route failed");
        verifyNoInteractions(completionPublisher);
    }

    @Test
    void unknownDispatchId_returns404() {
        WorkerCompletionPayload payload = new WorkerCompletionPayload(null, false, null);
        Response response = resource.complete("unknown", "token", payload);
        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    void wrongCallbackToken_returns401() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(null, false, null);
        Response response = resource.complete(pending.dispatchId(), "wrong-token", payload);
        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void duplicateCompletion_returns404() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(Map.of(), false, null);
        resource.complete(pending.dispatchId(), pending.callbackToken(), payload);
        Response second = resource.complete(pending.dispatchId(), pending.callbackToken(), payload);
        assertThat(second.getStatus()).isEqualTo(404);
    }

    private PendingCompletion registerTestPending() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash", "t1");
        return registry.register("camel", ctx, new Capability("cap", "", ""), 1L, Duration.ofMinutes(60), Map.of());
    }
}
