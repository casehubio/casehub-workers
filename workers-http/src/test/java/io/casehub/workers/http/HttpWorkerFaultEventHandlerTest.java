package io.casehub.workers.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.api.model.BackoffStrategy;
import io.casehub.api.model.Capability;
import io.casehub.api.model.ExecutionPolicy;
import io.casehub.api.model.RetryPolicy;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.testing.WorkerTestSupport;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class HttpWorkerFaultEventHandlerTest {

    private HttpWorkerFaultEventHandler handler;
    private WorkerRetrySupport retrySupport;
    private WorkerExecutionManager workerExecutionManager;
    private Vertx vertx;
    private EventLogRepository eventLogRepository;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeEach
    void setUp() {
        retrySupport = mock(WorkerRetrySupport.class);
        workerExecutionManager = mock(WorkerExecutionManager.class);
        vertx = mock(Vertx.class);
        eventLogRepository = mock(EventLogRepository.class);

        handler = new HttpWorkerFaultEventHandler();
        handler.retrySupport = retrySupport;
        handler.workerExecutionManager = workerExecutionManager;
        handler.vertx = vertx;
        handler.eventLogRepository = eventLogRepository;

        // Default: persistFailureLog succeeds
        when(retrySupport.persistFailureLog(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());
    }

    @Test
    void persistFailureLog_calledForEveryFault() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("boom");

        // Will exhaust retries so we don't need timer mocking
        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(3L));

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        verify(retrySupport).persistFailureLog(instance, worker, "hash-1", "boom", instance.tenancyId);
    }

    @Test
    void permanentFault_publishesExhaustedImmediately() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");
        PermanentFaultException cause = new PermanentFaultException(400, "Bad Request");

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        verify(retrySupport).publishRetriesExhausted(instance.getUuid(), "w1", "hash-1");
        verify(retrySupport, never()).countFailedAttempts(any(), any(), any(), any());
    }

    @Test
    void retryAfterException_usesRetryAfterDelay() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");
        RetryAfterException cause = new RetryAfterException(5000, "429 Too Many Requests");

        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(1L));

        EventLog eventLog = new EventLog();
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("key", "val");
        eventLog.setPayload(payload);
        when(eventLogRepository.findById(42L, instance.tenancyId))
            .thenReturn(Uni.createFrom().item(eventLog));

        // Capture setTimer to invoke handler immediately and verify delay
        when(vertx.setTimer(anyLong(), any(Handler.class))).thenAnswer(invocation -> {
            Handler<Long> timerHandler = invocation.getArgument(1);
            timerHandler.handle(1L);
            return 1L;
        });

        when(workerExecutionManager.submit(anyLong(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        // Verify timer was set with RetryAfter delay of 5000, not configured backoff
        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        verify(vertx).setTimer(delayCaptor.capture(), any(Handler.class));
        assertThat(delayCaptor.getValue()).isEqualTo(5000L);
    }

    @Test
    void normalFault_usesConfiguredBackoff() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        RetryPolicy retryPolicy = new RetryPolicy(3, 10000, BackoffStrategy.FIXED);
        ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        worker.setExecutionPolicy(ep);
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("500 Internal Server Error");

        // First failure (count=1), so attempt number for backoff = 2
        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(1L));

        EventLog eventLog = new EventLog();
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("key", "val");
        eventLog.setPayload(payload);
        when(eventLogRepository.findById(42L, instance.tenancyId))
            .thenReturn(Uni.createFrom().item(eventLog));

        when(vertx.setTimer(anyLong(), any(Handler.class))).thenAnswer(invocation -> {
            Handler<Long> timerHandler = invocation.getArgument(1);
            timerHandler.handle(1L);
            return 1L;
        });

        when(workerExecutionManager.submit(anyLong(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        // FIXED backoff = 10000ms regardless of attempt
        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        verify(vertx).setTimer(delayCaptor.capture(), any(Handler.class));
        assertThat(delayCaptor.getValue()).isEqualTo(10000L);
    }

    @Test
    void exhausted_afterMaxAttempts() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        RetryPolicy retryPolicy = new RetryPolicy(3, 10000, BackoffStrategy.FIXED);
        ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        worker.setExecutionPolicy(ep);
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("500 error");

        // failureCount == maxAttempts → strict < means exhausted
        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(3L));

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        verify(retrySupport).publishRetriesExhausted(instance.getUuid(), "w1", "hash-1");
        verify(vertx, never()).setTimer(anyLong(), any(Handler.class));
    }

    @Test
    void nullExecutionPolicy_defaultsToThreeAttempts() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        // No setExecutionPolicy → null policy → default RetryPolicy(3, 10000, FIXED)
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("error");

        // failureCount 2 < 3 maxAttempts → should retry
        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(2L));

        EventLog eventLog = new EventLog();
        eventLog.setPayload(OBJECT_MAPPER.createObjectNode());
        when(eventLogRepository.findById(42L, instance.tenancyId))
            .thenReturn(Uni.createFrom().item(eventLog));

        when(vertx.setTimer(anyLong(), any(Handler.class))).thenAnswer(invocation -> {
            Handler<Long> timerHandler = invocation.getArgument(1);
            timerHandler.handle(1L);
            return 1L;
        });

        when(workerExecutionManager.submit(anyLong(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        // Default backoff: FIXED 10000ms
        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        verify(vertx).setTimer(delayCaptor.capture(), any(Handler.class));
        assertThat(delayCaptor.getValue()).isEqualTo(10000L);

        verify(workerExecutionManager).submit(eq(42L), eq(instance), eq(worker), eq(cap), any());
    }

    @Test
    void retryCallsSubmitDirectly() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("error");

        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(0L));

        EventLog eventLog = new EventLog();
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("data", "value");
        eventLog.setPayload(payload);
        when(eventLogRepository.findById(42L, instance.tenancyId))
            .thenReturn(Uni.createFrom().item(eventLog));

        when(vertx.setTimer(anyLong(), any(Handler.class))).thenAnswer(invocation -> {
            Handler<Long> timerHandler = invocation.getArgument(1);
            timerHandler.handle(1L);
            return 1L;
        });

        when(workerExecutionManager.submit(anyLong(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        // Verify timer fires then submit is called with correct arguments
        verify(vertx).setTimer(anyLong(), any(Handler.class));

        ArgumentCaptor<Map<String, Object>> inputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(workerExecutionManager).submit(
            eq(42L), eq(instance), eq(worker), eq(cap), inputCaptor.capture());
        assertThat(inputCaptor.getValue()).containsEntry("data", "value");
    }

    @Test
    void faultHandlingFailure_logsAndRecovers() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("error");

        // persistFailureLog fails
        when(retrySupport.persistFailureLog(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("DB down")));

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        // Should NOT throw — handler recovers from internal failures
        handler.onFault(event).await().indefinitely();

        // No retry or exhausted publishing since persist failed before we got there
        verify(retrySupport, never()).publishRetriesExhausted(any(), any(), any());
        verify(workerExecutionManager, never()).submit(anyLong(), any(), any(), any(), any());
    }
}
