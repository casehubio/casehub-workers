package io.casehub.workers.http;

import io.casehub.workers.common.CompletionExpiredEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

@ApplicationScoped
public class HttpCompletionExpiryObserver {

    @Inject
    HttpWorkerFaultPublisher faultPublisher;

    void onExpiry(@ObservesAsync CompletionExpiredEvent event) {
        if (!HttpWorkerConstants.WORKER_TYPE.equals(event.pending().workerType())) return;
        faultPublisher.fault(event.pending(),
            new RuntimeException("Async timeout — no completion received within TTL"));
    }
}
