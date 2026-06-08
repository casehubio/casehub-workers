package io.casehub.workers.camel;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CamelWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(CamelWorkerExecutionManager.class);

    @Inject CamelCapabilityResolver camelCapabilityResolver;
    @Inject CamelWorkerFaultPublisher camelWorkerFaultPublisher;
    @Inject AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry;
    @Inject WorkflowCompletionPublisher completionPublisher;
    @Inject ProducerTemplate producerTemplate;

    @ConfigProperty(name = "casehub.workers.async.timeout-minutes", defaultValue = "60")
    int asyncTimeoutMinutes;

    @Override
    public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                            Capability capability, Map<String, Object> inputData) {
        String entryUri;
        try {
            entryUri = camelCapabilityResolver.resolve(capability.getName());
        } catch (WorkerProvisioningException e) {
            LOG.errorf("Camel route for capability %s missing at dispatch time", capability.getName());
            camelWorkerFaultPublisher.fault(
                new WorkerCorrelationContext(instance, worker,
                    WorkerExecutionKeys.inputDataHash(instance.getUuid(), worker.getName(),
                        capability.getName(), inputData), instance.tenancyId),
                capability, eventLogId, e);
            return Uni.createFrom().voidItem();
        }

        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.getName(), capability.getName(), inputData);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            instance, worker, idempotency, instance.tenancyId);
        ExchangePattern pattern = camelCapabilityResolver.exchangePattern(capability.getName());

        return pattern == ExchangePattern.InOut
            ? submitSync(ctx, entryUri, capability, inputData, eventLogId)
            : submitAsync(ctx, entryUri, capability, eventLogId, inputData);
    }

    private Uni<Void> submitSync(WorkerCorrelationContext ctx, String entryUri,
                                  Capability capability, Map<String, Object> inputData,
                                  Long eventLogId) {
        return Uni.createFrom()
            .item(() -> producerTemplate.request(entryUri, exchange -> {
                exchange.getIn().setHeader(CasehubWorkerHeaders.IDEMPOTENCY, ctx.idempotency());
                exchange.getIn().setHeader(CasehubWorkerHeaders.CASE_ID,
                    ctx.caseInstance().getUuid().toString());
                exchange.getIn().setHeader(CasehubWorkerHeaders.TENANCY_ID, ctx.tenancyId());
                exchange.getIn().setHeader(CasehubWorkerHeaders.TASK_TYPE, capability.getName());
                exchange.getIn().setBody(inputData);
            }))
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .flatMap(response -> {
                boolean faulted = response.getException() != null
                    || "FAULTED".equals(response.getIn().getHeader(CasehubWorkerHeaders.WORK_STATUS));
                if (faulted) {
                    camelWorkerFaultPublisher.fault(ctx, capability, eventLogId, response.getException());
                } else {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> output = response.getIn().getBody(Map.class);
                    completionPublisher.complete(ctx, output != null ? output : Map.of());
                }
                return Uni.createFrom().voidItem();
            })
            .onFailure().recoverWithUni(t -> {
                camelWorkerFaultPublisher.fault(ctx, capability, eventLogId, t);
                return Uni.createFrom().voidItem();
            });
    }

    private Uni<Void> submitAsync(WorkerCorrelationContext ctx, String entryUri,
                                   Capability capability, Long eventLogId,
                                   Map<String, Object> inputData) {
        PendingCompletion pending = asyncWorkerCompletionRegistry.register(
            CamelWorkerConstants.WORKER_TYPE, ctx, capability, eventLogId,
            Duration.ofMinutes(asyncTimeoutMinutes), Map.of());

        return Uni.createFrom()
            .item(() -> {
                producerTemplate.send(entryUri, exchange -> {
                    exchange.getIn().setHeader(CasehubWorkerHeaders.IDEMPOTENCY, ctx.idempotency());
                    exchange.getIn().setHeader(CasehubWorkerHeaders.CASE_ID,
                        ctx.caseInstance().getUuid().toString());
                    exchange.getIn().setHeader(CasehubWorkerHeaders.TENANCY_ID, ctx.tenancyId());
                    exchange.getIn().setHeader(CasehubWorkerHeaders.TASK_TYPE, capability.getName());
                    exchange.getIn().setHeader(CasehubWorkerHeaders.WORKER_ID, pending.dispatchId());
                    exchange.getIn().setHeader(CasehubWorkerHeaders.CALLBACK_TOKEN, pending.callbackToken());
                    exchange.getIn().setBody(inputData);
                });
                return null;
            })
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .replaceWithVoid();
    }

    @Override
    public Uni<Void> schedulePersistedEvent(EventLog scheduledEventLog) {
        return Uni.createFrom().voidItem();
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return asyncWorkerCompletionRegistry.countByWorkerName(workerId);
    }
}
