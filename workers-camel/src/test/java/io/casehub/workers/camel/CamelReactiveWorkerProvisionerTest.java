package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.spi.ProvisionResult;
import io.casehub.workers.common.WorkerProvisioningException;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CamelReactiveWorkerProvisionerTest {

    private CamelReactiveWorkerProvisioner provisioner;
    private CamelCapabilityResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = mock(CamelCapabilityResolver.class);
        provisioner = new CamelReactiveWorkerProvisioner();
        provisioner.camelCapabilityResolver = resolver;
    }

    @Test
    void provision_matchingCapability_returnsEmpty() {
        when(resolver.firstMatch(Set.of("send-email", "unknown")))
            .thenReturn(Optional.of("send-email"));
        when(resolver.resolve("send-email")).thenReturn("direct:send-email");

        ProvisionResult result = provisioner.provision(Set.of("send-email", "unknown"), null)
            .await().indefinitely();

        assertThat(result.causedByEntryId()).isNull();
    }

    @Test
    void provision_noMatch_fails() {
        when(resolver.firstMatch(any())).thenReturn(Optional.empty());

        try {
            provisioner.provision(Set.of("nonexistent"), null)
                .await().indefinitely();
            org.junit.jupiter.api.Assertions.fail("Expected WorkerProvisioningException");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(WorkerProvisioningException.class);
        }
    }

    @Test
    void getCapabilities_delegatesToResolver() {
        when(resolver.capabilities()).thenReturn(Set.of("a", "b"));
        Set<String> caps = provisioner.getCapabilities().await().indefinitely();
        assertThat(caps).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void terminate_returnsVoid() {
        assertThat(provisioner.terminate("any").await().indefinitely()).isNull();
    }
}
