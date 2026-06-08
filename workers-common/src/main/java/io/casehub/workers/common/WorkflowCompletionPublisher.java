package io.casehub.workers.common;

import io.casehub.engine.common.internal.event.EventBusAddresses;
import io.casehub.engine.common.internal.event.WorkflowExecutionCompleted;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;

@ApplicationScoped
public class WorkflowCompletionPublisher {

    @Inject
    EventBus eventBus;

    public void complete(WorkerCorrelationContext ctx, Map<String, Object> output) {
        eventBus.publish(EventBusAddresses.WORKER_EXECUTION_FINISHED,
            WorkflowExecutionCompleted.approved(
                ctx.caseInstance(), ctx.worker(), ctx.idempotency(), output));
    }
}
