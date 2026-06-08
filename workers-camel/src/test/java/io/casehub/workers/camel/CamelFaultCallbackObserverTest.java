package io.casehub.workers.camel;

import static org.mockito.Mockito.*;

import io.casehub.workers.common.FaultCallbackEvent;
import io.casehub.workers.common.PendingCompletion;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CamelFaultCallbackObserverTest {

    @Test
    void onFaultCallback_camelType_firesFault() {
        CamelWorkerFaultPublisher faultPublisher = mock(CamelWorkerFaultPublisher.class);
        CamelFaultCallbackObserver observer = new CamelFaultCallbackObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", CamelWorkerConstants.WORKER_TYPE, null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());
        RuntimeException cause = new RuntimeException("boom");

        observer.onFaultCallback(new FaultCallbackEvent(pending, cause));

        verify(faultPublisher).fault(pending, cause);
    }

    @Test
    void onFaultCallback_nonCamelType_doesNotFire() {
        CamelWorkerFaultPublisher faultPublisher = mock(CamelWorkerFaultPublisher.class);
        CamelFaultCallbackObserver observer = new CamelFaultCallbackObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", "http", null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());

        observer.onFaultCallback(new FaultCallbackEvent(pending, null));

        verifyNoInteractions(faultPublisher);
    }
}
