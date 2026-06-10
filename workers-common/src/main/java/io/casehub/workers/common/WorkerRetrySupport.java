package io.casehub.workers.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.BackoffStrategy;
import io.casehub.api.model.ExecutionPolicy;
import io.casehub.api.model.RetryPolicy;
import io.casehub.api.model.Worker;
import io.casehub.api.model.event.CaseHubEventType;
import io.casehub.api.model.event.EventStreamType;
import io.casehub.engine.common.internal.event.EventBusAddresses;
import io.casehub.engine.common.internal.event.WorkerRetriesExhaustedEvent;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Shared retry building blocks for worker fault handlers. Extracted from
 * {@code CamelWorkerFaultEventHandler} so both Camel and HTTP (and future)
 * fault handlers can share retry infrastructure.
 *
 * <p>Static methods ({@link #resolveRetryPolicy}, {@link #computeBackoffDelayMs})
 * are pure logic. Instance methods ({@link #persistFailureLog},
 * {@link #countFailedAttempts}, {@link #publishRetriesExhausted}) require CDI injection.
 */
@ApplicationScoped
public class WorkerRetrySupport {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter HTTP_DATE =
        DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

    @Inject EventLogRepository eventLogRepository;
    @Inject EventBus eventBus;

    /**
     * Resolves the effective retry policy from a worker. Null-safe: falls back to
     * {@code new RetryPolicy()} (3 attempts, 10s FIXED) if the worker has no execution
     * policy or no retry policy.
     */
    public static RetryPolicy resolveRetryPolicy(Worker worker) {
        ExecutionPolicy executionPolicy = worker.getExecutionPolicy();
        if (executionPolicy == null || executionPolicy.retries() == null) {
            return new RetryPolicy();
        }
        return executionPolicy.retries();
    }

    /**
     * Computes the backoff delay in milliseconds for a given retry attempt.
     *
     * <ul>
     *   <li>FIXED — returns baseDelay regardless of attempt number</li>
     *   <li>EXPONENTIAL — {@code baseDelay * 2^(attempt-1)}, capped at 30s</li>
     *   <li>EXPONENTIAL_WITH_JITTER — random in {@code [0, exponentialCap]}, capped at 30s</li>
     * </ul>
     */
    public static long computeBackoffDelayMs(RetryPolicy policy, long attemptNumber) {
        long baseDelayMs = policy.delayMs() != null ? policy.delayMs() : 0L;
        BackoffStrategy strategy = policy.backoffStrategy() != null
            ? policy.backoffStrategy() : BackoffStrategy.FIXED;
        return switch (strategy) {
            case FIXED -> baseDelayMs;
            case EXPONENTIAL -> {
                long shift = Math.min(attemptNumber - 1, 30);
                yield Math.min(baseDelayMs * (1L << shift), 30_000L);
            }
            case EXPONENTIAL_WITH_JITTER -> {
                long shift = Math.min(attemptNumber - 1, 30);
                long cap = Math.min(baseDelayMs * (1L << shift), 30_000L);
                yield cap == 0 ? 0 : ThreadLocalRandom.current().nextLong(cap + 1);
            }
        };
    }

    /**
     * Parses a Retry-After header value (integer seconds or HTTP-date) and returns
     * {@link RetryAfterException} when parseable, or a generic {@link RuntimeException}
     * otherwise.
     *
     * <ul>
     *   <li>Integer seconds → {@code RetryAfterException} with {@code retryAfterMs = seconds * 1000}</li>
     *   <li>HTTP-date → {@code RetryAfterException} with {@code retryAfterMs = max(0, delta)}</li>
     *   <li>Null, blank, or unparseable → generic {@code RuntimeException}</li>
     * </ul>
     */
    public static RuntimeException parseRetryAfter(String retryAfter, int status, String statusMessage) {
        String message = status + " " + statusMessage;
        if (retryAfter == null || retryAfter.isBlank()) {
            return new RuntimeException(message);
        }
        try {
            long seconds = Long.parseLong(retryAfter.trim());
            return new RetryAfterException(seconds * 1000, message);
        } catch (NumberFormatException ignored) {
            // not an integer
        }
        try {
            ZonedDateTime retryDate = ZonedDateTime.parse(retryAfter.trim(), HTTP_DATE);
            long deltaMs = retryDate.toInstant().toEpochMilli() - System.currentTimeMillis();
            return new RetryAfterException(Math.max(0, deltaMs), message);
        } catch (DateTimeParseException ignored) {
            // unparseable
        }
        return new RuntimeException(message);
    }

    /**
     * Persists a WORKER_EXECUTION_FAILED event log entry with inputDataHash and
     * errorMessage metadata.
     */
    public Uni<Void> persistFailureLog(CaseInstance instance, Worker worker,
                                        String inputDataHash, String errorMsg,
                                        String tenancyId) {
        EventLog failureLog = new EventLog();
        failureLog.setCaseId(instance.getUuid());
        failureLog.setWorkerId(worker.getName());
        failureLog.setEventType(CaseHubEventType.WORKER_EXECUTION_FAILED);
        failureLog.setStreamType(EventStreamType.CASE);
        failureLog.setTimestamp(Instant.now());
        String msg = (errorMsg != null) ? errorMsg : "unknown";
        failureLog.setMetadata(OBJECT_MAPPER.createObjectNode()
            .put("inputDataHash", inputDataHash)
            .put("errorMessage", msg));

        return eventLogRepository.append(failureLog, tenancyId);
    }

    /**
     * Counts previous WORKER_EXECUTION_FAILED entries for a given case/worker/inputDataHash
     * combination. Used to determine whether retries are exhausted.
     */
    public Uni<Long> countFailedAttempts(UUID caseId, String workerId,
                                          String inputDataHash, String tenancyId) {
        return eventLogRepository
            .findByCaseAndWorkerAndType(caseId, workerId, CaseHubEventType.WORKER_EXECUTION_FAILED, tenancyId)
            .map(logs -> logs.stream()
                .filter(log -> {
                    JsonNode meta = log.getMetadata();
                    JsonNode node = meta == null ? null : meta.get("inputDataHash");
                    return node != null && inputDataHash.equals(node.asText());
                })
                .count());
    }

    /**
     * Publishes a {@link WorkerRetriesExhaustedEvent} on the
     * {@link EventBusAddresses#WORKER_RETRIES_EXHAUSTED} address.
     */
    public void publishRetriesExhausted(UUID caseId, String workerId, String inputDataHash) {
        eventBus.publish(EventBusAddresses.WORKER_RETRIES_EXHAUSTED,
            new WorkerRetriesExhaustedEvent(caseId, workerId, inputDataHash));
    }
}
