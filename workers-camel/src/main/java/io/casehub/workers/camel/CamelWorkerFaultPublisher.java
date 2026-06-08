package io.casehub.workers.camel;

import io.casehub.api.model.Capability;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CamelWorkerFaultPublisher {

    @Inject
    EventBus eventBus;

    public void fault(PendingCompletion pending, Throwable cause) {
        eventBus.publish(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT,
            new WorkflowExecutionFailed(
                pending.correlationContext().caseInstance(),
                pending.correlationContext().worker(),
                pending.capability(),
                pending.correlationContext().idempotency(),
                pending.eventLogId().toString(),
                cause));
    }

    public void fault(WorkerCorrelationContext ctx, Capability capability,
                      Long eventLogId, Throwable cause) {
        eventBus.publish(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT,
            new WorkflowExecutionFailed(
                ctx.caseInstance(), ctx.worker(), capability,
                ctx.idempotency(), eventLogId.toString(), cause));
    }
}
