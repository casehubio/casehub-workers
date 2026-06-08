package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CamelWorkerExecutionManagerTest {

    private CamelWorkerExecutionManager manager;
    private CamelCapabilityResolver resolver;
    private CamelWorkerFaultPublisher faultPublisher;
    private AsyncWorkerCompletionRegistry registry;

    @BeforeEach
    void setUp() {
        resolver = mock(CamelCapabilityResolver.class);
        faultPublisher = mock(CamelWorkerFaultPublisher.class);
        registry = mock(AsyncWorkerCompletionRegistry.class);

        manager = new CamelWorkerExecutionManager();
        manager.camelCapabilityResolver = resolver;
        manager.camelWorkerFaultPublisher = faultPublisher;
        manager.asyncWorkerCompletionRegistry = registry;
        manager.completionPublisher = mock(WorkflowCompletionPublisher.class);
        manager.producerTemplate = mock(ProducerTemplate.class);
        manager.asyncTimeoutMinutes = 60;
    }

    @Test
    void submit_missingRoute_firesFault() {
        CaseInstance instance = testInstance();
        Worker worker = testWorker();
        Capability cap = new Capability("missing", "", "");
        when(resolver.resolve("missing")).thenThrow(WorkerProvisioningException.noRouteFound("missing"));

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), any(WorkerProvisioningException.class));
    }

    @Test
    void schedulePersistedEvent_returnsVoid() {
        assertThat(manager.schedulePersistedEvent(new EventLog()).await().indefinitely()).isNull();
    }

    @Test
    void getActiveWorkCount_delegatesToRegistry() {
        when(registry.countByWorkerName("w1")).thenReturn(3);
        assertThat(manager.getActiveWorkCount("w1")).isEqualTo(3);
    }

    private CaseInstance testInstance() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        return instance;
    }

    private Worker testWorker() {
        return new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
    }
}
