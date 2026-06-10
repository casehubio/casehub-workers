package io.casehub.workers.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class HttpWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(HttpWorkerExecutionManager.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final Pattern URI_TEMPLATE_PATTERN = Pattern.compile("\\{(\\w+)\\}");

    @Inject HttpEndpointResolver httpEndpointResolver;
    @Inject HttpWorkerFaultPublisher httpWorkerFaultPublisher;
    @Inject AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry;
    @Inject WorkflowCompletionPublisher completionPublisher;
    @Inject io.vertx.mutiny.core.Vertx vertx;
    @Inject ObjectMapper objectMapper;

    @ConfigProperty(name = "casehub.workers.async.timeout-minutes", defaultValue = "60")
    int asyncTimeoutMinutes;

    WebClient webClient;

    @PostConstruct
    void init() {
        this.webClient = WebClient.create(vertx);
    }

    @Override
    public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                            Capability capability, Map<String, Object> inputData) {
        ResolvedEndpoint endpoint;
        try {
            endpoint = httpEndpointResolver.resolve(capability.getName());
        } catch (WorkerProvisioningException e) {
            LOG.errorf("HTTP endpoint for capability %s missing at dispatch time", capability.getName());
            httpWorkerFaultPublisher.fault(
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

        String resolvedUrl;
        try {
            resolvedUrl = interpolateUrl(endpoint.url(), inputData);
        } catch (PermanentFaultException e) {
            httpWorkerFaultPublisher.fault(ctx, capability, eventLogId, e);
            return Uni.createFrom().voidItem();
        }

        if (endpoint.mode() == ExchangeMode.SYNC) {
            return submitSync(ctx, endpoint, resolvedUrl, capability, inputData, eventLogId);
        }
        return submitAsync(ctx, endpoint, resolvedUrl, capability, inputData, eventLogId);
    }

    private Uni<Void> submitSync(WorkerCorrelationContext ctx, ResolvedEndpoint endpoint,
                                  String resolvedUrl, Capability capability,
                                  Map<String, Object> inputData, Long eventLogId) {
        HttpRequest<Buffer> request = webClient.requestAbs(
            HttpMethod.valueOf(endpoint.method()), resolvedUrl);
        request.timeout(endpoint.timeoutSeconds() * 1000L);

        // CaseHub headers first
        request.putHeader(CasehubWorkerHeaders.IDEMPOTENCY, ctx.idempotency());
        request.putHeader(CasehubWorkerHeaders.CASE_ID, ctx.caseInstance().getUuid().toString());
        request.putHeader(CasehubWorkerHeaders.TENANCY_ID, ctx.tenancyId());
        request.putHeader(CasehubWorkerHeaders.TASK_TYPE, capability.getName());

        // Endpoint headers AFTER — endpoint wins on collision
        endpoint.headers().forEach(request::putHeader);

        return request.sendJson(inputData)
            .flatMap(response -> handleResponse(ctx, response))
            .onFailure().recoverWithUni(t -> {
                httpWorkerFaultPublisher.fault(ctx, capability, eventLogId, t);
                return Uni.createFrom().voidItem();
            });
    }

    private Uni<Void> submitAsync(WorkerCorrelationContext ctx, ResolvedEndpoint endpoint,
                                   String resolvedUrl, Capability capability,
                                   Map<String, Object> inputData, Long eventLogId) {
        PendingCompletion pending = asyncWorkerCompletionRegistry.register(
            HttpWorkerConstants.WORKER_TYPE, ctx, capability, eventLogId,
            Duration.ofMinutes(asyncTimeoutMinutes), Map.of());

        HttpRequest<Buffer> request = webClient.requestAbs(
            HttpMethod.valueOf(endpoint.method()), resolvedUrl);

        // CaseHub headers first
        request.putHeader(CasehubWorkerHeaders.IDEMPOTENCY, ctx.idempotency());
        request.putHeader(CasehubWorkerHeaders.CASE_ID, ctx.caseInstance().getUuid().toString());
        request.putHeader(CasehubWorkerHeaders.TENANCY_ID, ctx.tenancyId());
        request.putHeader(CasehubWorkerHeaders.TASK_TYPE, capability.getName());
        // Async-specific headers
        request.putHeader(CasehubWorkerHeaders.WORKER_ID, pending.dispatchId());
        request.putHeader(CasehubWorkerHeaders.CALLBACK_TOKEN, pending.callbackToken());

        // Endpoint headers AFTER — endpoint wins on collision
        endpoint.headers().forEach(request::putHeader);

        return request.sendJson(inputData)
            .flatMap(response -> {
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                    return Uni.createFrom().<Void>voidItem();
                }
                if (status == 429) {
                    String retryAfter = response.getHeader("Retry-After");
                    RuntimeException ex = WorkerRetrySupport.parseRetryAfter(retryAfter, status, response.statusMessage());
                    if (ex instanceof RetryAfterException ra) {
                        long remainingMs = java.time.Duration.between(
                            java.time.Instant.now(), pending.expiresAt()).toMillis();
                        long capped = Math.min(ra.retryAfterMs(), Math.max(0, remainingMs));
                        throw new RetryAfterException(capped, ra.getMessage());
                    }
                    throw ex;
                }
                if (status >= 400 && status < 500) {
                    throw new PermanentFaultException(status, status + " " + response.statusMessage());
                }
                throw new RuntimeException(status + " " + response.statusMessage());
            })
            .onFailure().recoverWithUni(t -> {
                httpWorkerFaultPublisher.fault(ctx, capability, eventLogId, t);
                return Uni.createFrom().voidItem();
            });
    }

    private Uni<Void> handleResponse(WorkerCorrelationContext ctx, HttpResponse<Buffer> response) {
        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            Map<String, Object> output = deserializeBody(response);
            completionPublisher.complete(ctx, output);
            return Uni.createFrom().voidItem();
        }
        if (status == 429) {
            String retryAfter = response.getHeader("Retry-After");
            throw WorkerRetrySupport.parseRetryAfter(retryAfter, status, response.statusMessage());
        }
        if (status >= 400 && status < 500) {
            throw new PermanentFaultException(status, status + " " + response.statusMessage());
        }
        // 5xx
        throw new RuntimeException(status + " " + response.statusMessage());
    }

    private Map<String, Object> deserializeBody(HttpResponse<Buffer> response) {
        Buffer body = response.body();
        if (body == null || body.length() == 0) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(body.toString(), MAP_TYPE);
        } catch (Exception e) {
            return Map.of();
        }
    }

    static String interpolateUrl(String urlTemplate, Map<String, Object> inputData) {
        Matcher matcher = URI_TEMPLATE_PATTERN.matcher(urlTemplate);
        if (!matcher.find()) {
            return urlTemplate;
        }
        StringBuilder sb = new StringBuilder();
        matcher.reset();
        while (matcher.find()) {
            String key = matcher.group(1);
            Object value = inputData.get(key);
            if (value == null) {
                throw new PermanentFaultException(0,
                    "URI template variable {" + key + "} not found in inputData");
            }
            matcher.appendReplacement(sb,
                Matcher.quoteReplacement(
                    URLEncoder.encode(value.toString(), StandardCharsets.UTF_8)));
        }
        matcher.appendTail(sb);
        return sb.toString();
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
