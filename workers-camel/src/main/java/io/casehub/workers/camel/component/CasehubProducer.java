package io.casehub.workers.camel.component;

import io.casehub.workers.camel.CamelWorkerFaultPublisher;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.jboss.logging.Logger;
import java.util.Map;
import java.util.Optional;

public class CasehubProducer extends DefaultProducer {

    private static final Logger LOG = Logger.getLogger(CasehubProducer.class);

    private final AsyncWorkerCompletionRegistry registry;
    private final WorkflowCompletionPublisher completionPublisher;
    private final CamelWorkerFaultPublisher faultPublisher;

    public CasehubProducer(CasehubEndpoint endpoint,
                           AsyncWorkerCompletionRegistry registry,
                           WorkflowCompletionPublisher completionPublisher,
                           CamelWorkerFaultPublisher faultPublisher) {
        super(endpoint);
        this.registry = registry;
        this.completionPublisher = completionPublisher;
        this.faultPublisher = faultPublisher;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String dispatchId = exchange.getIn().getHeader(CasehubWorkerHeaders.WORKER_ID, String.class);
        if (dispatchId == null) {
            throw new IllegalStateException("casehub-worker-id header missing on casehub:complete");
        }

        Optional<PendingCompletion> maybePending = registry.complete(dispatchId);
        if (maybePending.isEmpty()) {
            LOG.warnf("casehub:complete — dispatchId %s not found (already resolved or expired)", dispatchId);
            return;
        }

        PendingCompletion pending = maybePending.get();
        boolean faulted = exchange.getException() != null
            || "FAULTED".equals(exchange.getIn().getHeader(CasehubWorkerHeaders.WORK_STATUS));

        if (faulted) {
            faultPublisher.fault(pending, exchange.getException());
        } else {
            @SuppressWarnings("unchecked")
            Map<String, Object> output = exchange.getIn().getBody(Map.class);
            completionPublisher.complete(pending.correlationContext(),
                output != null ? output : Map.of());
        }
    }
}
