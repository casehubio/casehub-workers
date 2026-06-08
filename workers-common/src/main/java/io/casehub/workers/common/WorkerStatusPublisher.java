package io.casehub.workers.common;

import io.casehub.api.model.WorkResult;
import io.casehub.api.spi.ReactiveWorkerStatusListener;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;

@ApplicationScoped
public class WorkerStatusPublisher {

    @Inject
    ReactiveWorkerStatusListener reactiveWorkerStatusListener;

    public Uni<Void> onWorkerStarted(String dispatchId, Map<String, String> sessionMeta) {
        return reactiveWorkerStatusListener.onWorkerStarted(dispatchId, sessionMeta);
    }

    public Uni<Void> onWorkerCompleted(String dispatchId, WorkResult result) {
        return reactiveWorkerStatusListener.onWorkerCompleted(dispatchId, result);
    }

    public Uni<Void> onWorkerStalled(String dispatchId) {
        return reactiveWorkerStatusListener.onWorkerStalled(dispatchId);
    }
}
