package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.EventBusAddresses;
import io.casehub.engine.common.internal.event.WorkflowExecutionCompleted;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.vertx.mutiny.core.eventbus.EventBus;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class WorkflowCompletionPublisherTest {

    @Test
    void complete_publishesToWorkerExecutionFinished() {
        EventBus eventBus = mock(EventBus.class);
        WorkflowCompletionPublisher publisher = new WorkflowCompletionPublisher();
        publisher.eventBus = eventBus;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        Map<String, Object> output = Map.of("result", "ok");

        publisher.complete(ctx, output);

        ArgumentCaptor<WorkflowExecutionCompleted> captor =
            ArgumentCaptor.forClass(WorkflowExecutionCompleted.class);
        verify(eventBus).publish(eq(EventBusAddresses.WORKER_EXECUTION_FINISHED), captor.capture());

        WorkflowExecutionCompleted event = captor.getValue();
        assertThat(event.caseInstance()).isSameAs(instance);
        assertThat(event.worker()).isSameAs(worker);
        assertThat(event.idempotency()).isEqualTo("hash-1");
        assertThat(event.output()).isEqualTo(output);
        assertThat(event.plannedAction()).isNull();
    }
}
