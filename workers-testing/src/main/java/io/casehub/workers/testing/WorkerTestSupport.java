package io.casehub.workers.testing;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import java.util.List;
import java.util.UUID;

public final class WorkerTestSupport {

    private WorkerTestSupport() {}

    public static CaseInstance testCaseInstance() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "test-tenant";
        return instance;
    }

    public static CaseInstance testCaseInstance(String tenancyId) {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = tenancyId;
        return instance;
    }

    public static Worker testWorker(String name, String... capabilityTags) {
        List<Capability> caps = java.util.Arrays.stream(capabilityTags)
            .map(tag -> new Capability(tag, "", ""))
            .toList();
        return new Worker(name, caps, (ctx) -> null);
    }

    public static Capability testCapability(String tag) {
        return new Capability(tag, "", "");
    }
}
