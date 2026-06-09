package io.casehub.workers.http;

import static org.mockito.Mockito.*;

import io.casehub.workers.common.CompletionExpiredEvent;
import io.casehub.workers.common.PendingCompletion;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HttpCompletionExpiryObserverTest {

    @Test
    void onExpiry_httpWorkerType_firesFault() {
        HttpWorkerFaultPublisher faultPublisher = mock(HttpWorkerFaultPublisher.class);
        HttpCompletionExpiryObserver observer = new HttpCompletionExpiryObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", HttpWorkerConstants.WORKER_TYPE, null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());

        observer.onExpiry(new CompletionExpiredEvent(pending));

        verify(faultPublisher).fault(eq(pending), any(RuntimeException.class));
    }

    @Test
    void onExpiry_camelWorkerType_doesNotFire() {
        HttpWorkerFaultPublisher faultPublisher = mock(HttpWorkerFaultPublisher.class);
        HttpCompletionExpiryObserver observer = new HttpCompletionExpiryObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", "camel", null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());

        observer.onExpiry(new CompletionExpiredEvent(pending));

        verifyNoInteractions(faultPublisher);
    }
}
