package io.casehub.workers.http;

import static org.mockito.Mockito.*;

import io.casehub.workers.common.FaultCallbackEvent;
import io.casehub.workers.common.PendingCompletion;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HttpFaultCallbackObserverTest {

    @Test
    void onFaultCallback_httpWorkerType_firesFault() {
        HttpWorkerFaultPublisher faultPublisher = mock(HttpWorkerFaultPublisher.class);
        HttpFaultCallbackObserver observer = new HttpFaultCallbackObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", HttpWorkerConstants.WORKER_TYPE, null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());
        RuntimeException cause = new RuntimeException("boom");

        observer.onFaultCallback(new FaultCallbackEvent(pending, cause));

        verify(faultPublisher).fault(pending, cause);
    }

    @Test
    void onFaultCallback_camelWorkerType_doesNotFire() {
        HttpWorkerFaultPublisher faultPublisher = mock(HttpWorkerFaultPublisher.class);
        HttpFaultCallbackObserver observer = new HttpFaultCallbackObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", "camel", null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());

        observer.onFaultCallback(new FaultCallbackEvent(pending, null));

        verifyNoInteractions(faultPublisher);
    }
}
