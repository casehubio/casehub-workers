package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.api.model.Worker;
import io.casehub.api.model.Capability;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WorkerCorrelationContextTest {
    @Test
    void recordComponents() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "tenant-1";
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-123", "tenant-1");
        assertThat(ctx.caseInstance()).isSameAs(instance);
        assertThat(ctx.worker()).isSameAs(worker);
        assertThat(ctx.idempotency()).isEqualTo("hash-123");
        assertThat(ctx.tenancyId()).isEqualTo("tenant-1");
    }
}
