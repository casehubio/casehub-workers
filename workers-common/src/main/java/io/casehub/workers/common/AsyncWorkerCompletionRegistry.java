package io.casehub.workers.common;

import io.casehub.api.model.Capability;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import io.quarkus.scheduler.Scheduled;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class AsyncWorkerCompletionRegistry {

    @Inject
    Event<CompletionExpiredEvent> expiryEvents;

    private final ConcurrentHashMap<String, PendingCompletion> pending = new ConcurrentHashMap<>();

    public PendingCompletion register(String workerType, WorkerCorrelationContext ctx,
                                      Capability capability, Long eventLogId,
                                      Duration ttl, Map<String, String> provisionerMeta) {
        Instant now = Instant.now();
        PendingCompletion entry = new PendingCompletion(
            UUID.randomUUID().toString(),
            workerType,
            ctx,
            UUID.randomUUID().toString(),
            capability,
            eventLogId,
            now,
            now.plus(ttl),
            provisionerMeta);
        pending.put(entry.dispatchId(), entry);
        return entry;
    }

    public Optional<PendingCompletion> complete(String dispatchId) {
        return Optional.ofNullable(pending.remove(dispatchId));
    }

    public int countByWorkerName(String workerName) {
        return (int) pending.values().stream()
            .filter(p -> p.correlationContext().worker().getName().equals(workerName))
            .count();
    }

    @Scheduled(every = "${casehub.workers.async.expiry-check-interval:5m}")
    @Blocking
    void expireStale() {
        pending.forEach((key, value) ->
            pending.computeIfPresent(key, (k, p) -> {
                if (!p.expiresAt().isBefore(Instant.now())) return p;
                expiryEvents.fireAsync(new CompletionExpiredEvent(p));
                return null;
            })
        );
    }
}
