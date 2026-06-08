package io.casehub.workers.camel;

import io.casehub.workers.common.FaultCallbackEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

@ApplicationScoped
public class CamelFaultCallbackObserver {

    @Inject
    CamelWorkerFaultPublisher faultPublisher;

    void onFaultCallback(@ObservesAsync FaultCallbackEvent event) {
        if (!CamelWorkerConstants.WORKER_TYPE.equals(event.pending().workerType())) return;
        faultPublisher.fault(event.pending(), event.cause());
    }
}
