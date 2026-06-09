package io.casehub.workers.http;

import io.casehub.workers.common.FaultCallbackEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

@ApplicationScoped
public class HttpFaultCallbackObserver {

    @Inject
    HttpWorkerFaultPublisher faultPublisher;

    void onFaultCallback(@ObservesAsync FaultCallbackEvent event) {
        if (!HttpWorkerConstants.WORKER_TYPE.equals(event.pending().workerType())) return;
        faultPublisher.fault(event.pending(), event.cause());
    }
}
