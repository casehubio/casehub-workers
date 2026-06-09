package io.casehub.workers.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.spi.ProvisionResult;
import io.casehub.workers.common.WorkerProvisioningException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpReactiveWorkerProvisionerTest {

    private HttpReactiveWorkerProvisioner provisioner;
    private HttpEndpointResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = mock(HttpEndpointResolver.class);
        provisioner = new HttpReactiveWorkerProvisioner();
        provisioner.httpEndpointResolver = resolver;
    }

    @Test
    void provision_matchingCapability_returnsEmpty() {
        ResolvedEndpoint endpoint = new ResolvedEndpoint(
            "https://api.example.com/pay",
            "POST",
            ExchangeMode.SYNC,
            Map.of(),
            30
        );
        when(resolver.firstMatch(Set.of("payment-api", "unknown")))
            .thenReturn(Optional.of("payment-api"));
        when(resolver.resolve("payment-api")).thenReturn(endpoint);

        ProvisionResult result = provisioner.provision(Set.of("payment-api", "unknown"), null)
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
        when(resolver.capabilities()).thenReturn(Set.of("payment-api", "notification-api"));
        Set<String> caps = provisioner.getCapabilities().await().indefinitely();
        assertThat(caps).containsExactlyInAnyOrder("payment-api", "notification-api");
    }

    @Test
    void terminate_returnsVoid() {
        assertThat(provisioner.terminate("any").await().indefinitely()).isNull();
    }
}
