package io.casehub.workers.camel;

import static org.mockito.Mockito.*;

import io.casehub.workers.common.CompletionExpiredEvent;
import io.casehub.workers.common.PendingCompletion;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CamelCompletionExpiryObserverTest {

    @Test
    void onExpiry_camelType_firesFault() {
        CamelWorkerFaultPublisher faultPublisher = mock(CamelWorkerFaultPublisher.class);
        CamelCompletionExpiryObserver observer = new CamelCompletionExpiryObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", CamelWorkerConstants.WORKER_TYPE, null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());

        observer.onExpiry(new CompletionExpiredEvent(pending));

        verify(faultPublisher).fault(eq(pending), any(RuntimeException.class));
    }

    @Test
    void onExpiry_nonCamelType_doesNotFire() {
        CamelWorkerFaultPublisher faultPublisher = mock(CamelWorkerFaultPublisher.class);
        CamelCompletionExpiryObserver observer = new CamelCompletionExpiryObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", "http", null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());

        observer.onExpiry(new CompletionExpiredEvent(pending));

        verifyNoInteractions(faultPublisher);
    }
}
