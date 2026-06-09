package io.casehub.workers.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.RetryPolicy;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.WorkerRetrySupport;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class HttpWorkerFaultEventHandler {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final Logger LOG = Logger.getLogger(HttpWorkerFaultEventHandler.class);

    @Inject WorkerRetrySupport retrySupport;
    @Inject WorkerExecutionManager workerExecutionManager;
    @Inject Vertx vertx;
    @Inject EventLogRepository eventLogRepository;

    @ConsumeEvent(value = HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT, blocking = true)
    public Uni<Void> onFault(WorkflowExecutionFailed event) {
        CaseInstance instance = event.caseInstance();
        Worker worker = event.worker();
        String inputDataHash = event.inputDataHash();
        String tenancyId = instance.tenancyId;
        String errorMsg = (event.cause() != null && event.cause().getMessage() != null)
            ? event.cause().getMessage() : "unknown";

        // 1. Always persist (for observability)
        return retrySupport.persistFailureLog(instance, worker, inputDataHash, errorMsg, tenancyId)
            .flatMap(ignored -> {
                // 2. PermanentFaultException → skip retry immediately
                if (event.cause() instanceof PermanentFaultException) {
                    retrySupport.publishRetriesExhausted(
                        instance.getUuid(), worker.getName(), inputDataHash);
                    return Uni.createFrom().voidItem();
                }

                // 3. Count and check retry eligibility
                return retrySupport.countFailedAttempts(
                        instance.getUuid(), worker.getName(), inputDataHash, tenancyId)
                    .flatMap(failureCount -> {
                        RetryPolicy retryPolicy = WorkerRetrySupport.resolveRetryPolicy(worker);
                        if (failureCount < retryPolicy.maxAttempts()) {
                            long delayMs;
                            if (event.cause() instanceof RetryAfterException ra) {
                                delayMs = ra.retryAfterMs();
                            } else {
                                delayMs = WorkerRetrySupport.computeBackoffDelayMs(
                                    retryPolicy, failureCount + 1);
                            }
                            return reloadAndResubmit(event, delayMs);
                        } else {
                            retrySupport.publishRetriesExhausted(
                                instance.getUuid(), worker.getName(), inputDataHash);
                            return Uni.createFrom().voidItem();
                        }
                    });
            })
            .onFailure().recoverWithUni(ex -> {
                LOG.errorf(ex, "Fault handling failed for worker %s case %s — case may stall",
                           worker.getName(), instance.getUuid());
                return Uni.createFrom().voidItem();
            });
    }

    private Uni<Void> reloadAndResubmit(WorkflowExecutionFailed event, long delayMs) {
        return eventLogRepository
            .findById(Long.parseLong(event.eventLogId()), event.caseInstance().tenancyId)
            .flatMap(eventLog -> {
                Map<String, Object> inputData =
                    OBJECT_MAPPER.convertValue(eventLog.getPayload(), MAP_TYPE);
                return Uni.createFrom().<Void>emitter(em -> {
                        long timerId = vertx.setTimer(delayMs, id -> em.complete(null));
                        em.onTermination(() -> vertx.cancelTimer(timerId));
                    })
                    // NO emitOn(workerPool) — WebClient is event-loop native
                    .flatMap(ignored -> workerExecutionManager.submit(
                        Long.parseLong(event.eventLogId()),
                        event.caseInstance(), event.worker(), event.capability(), inputData));
            });
    }
}
