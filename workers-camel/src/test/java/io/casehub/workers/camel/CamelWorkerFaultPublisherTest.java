package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.vertx.mutiny.core.eventbus.EventBus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CamelWorkerFaultPublisherTest {

    @Test
    void fault_fromPendingCompletion_publishesToCamelWorkerFault() {
        EventBus eventBus = mock(EventBus.class);
        CamelWorkerFaultPublisher publisher = new CamelWorkerFaultPublisher();
        publisher.eventBus = eventBus;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        Capability capability = new Capability("send-email", "", "");
        PendingCompletion pending = new PendingCompletion(
            "dispatch-1", "camel", ctx, "token", capability, 42L,
            Instant.now(), Instant.now().plusSeconds(3600), Map.of());
        Throwable cause = new RuntimeException("route failed");

        publisher.fault(pending, cause);

        ArgumentCaptor<WorkflowExecutionFailed> captor = ArgumentCaptor.forClass(WorkflowExecutionFailed.class);
        verify(eventBus).publish(eq(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT), captor.capture());

        WorkflowExecutionFailed event = captor.getValue();
        assertThat(event.caseInstance()).isSameAs(instance);
        assertThat(event.worker()).isSameAs(worker);
        assertThat(event.capability()).isSameAs(capability);
        assertThat(event.inputDataHash()).isEqualTo("hash-1");
        assertThat(event.eventLogId()).isEqualTo("42");
        assertThat(event.cause()).isSameAs(cause);
    }

    @Test
    void fault_fromContext_publishesToCamelWorkerFault() {
        EventBus eventBus = mock(EventBus.class);
        CamelWorkerFaultPublisher publisher = new CamelWorkerFaultPublisher();
        publisher.eventBus = eventBus;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        Capability capability = new Capability("cap", "", "");

        publisher.fault(ctx, capability, 99L, new RuntimeException("boom"));

        verify(eventBus).publish(eq(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT), any(WorkflowExecutionFailed.class));
    }
}
