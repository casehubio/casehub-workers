package io.casehub.workers.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.workers.common.WorkerProvisioningException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpEndpointResolverTest {

    private HttpEndpointResolver resolver;

    // SPI route for "send-email" — Tier 1
    private static final HttpWorkerRoute SEND_EMAIL_SPI = new HttpWorkerRoute() {
        @Override public String capabilityTag() { return "send-email"; }
        @Override public String url() { return "https://spi.example.com/send"; }
        @Override public String method() { return "PUT"; }
        @Override public ExchangeMode exchangeMode() { return ExchangeMode.ASYNC; }
        @Override public Map<String, String> headers() { return Map.of("X-SPI", "true"); }
        @Override public int timeoutSeconds() { return 60; }
    };

    // SPI route with timeout -1 — should inherit global default
    private static final HttpWorkerRoute VALIDATE_SPI = new HttpWorkerRoute() {
        @Override public String capabilityTag() { return "validate-address"; }
        @Override public String url() { return "https://spi.example.com/validate"; }
        @Override public int timeoutSeconds() { return -1; }
    };

    // Tier 2 config for "process-order"
    private static final Map<String, Map<String, String>> CONFIG_ENDPOINTS = Map.of(
        "process-order", Map.of(
            "url", "https://orders.example.com/process",
            "method", "POST",
            "mode", "ASYNC",
            "timeout-seconds", "45",
            "headers.X-Api-Key", "secret-key"
        ),
        "send-email", Map.of(
            "url", "https://config.example.com/email",
            "method", "POST"
        ),
        "generate-report", Map.of(
            "url", "https://reports.example.com/generate"
        )
    );

    @BeforeEach
    void setUp() {
        resolver = new HttpEndpointResolver();
        resolver.initialize(
            List.of(SEND_EMAIL_SPI, VALIDATE_SPI),
            CONFIG_ENDPOINTS,
            30
        );
    }

    @Test
    void tier1WinsOverTier2ForSameTag() {
        ResolvedEndpoint endpoint = resolver.resolve("send-email");
        // SPI values, not config values
        assertThat(endpoint.url()).isEqualTo("https://spi.example.com/send");
        assertThat(endpoint.method()).isEqualTo("PUT");
        assertThat(endpoint.mode()).isEqualTo(ExchangeMode.ASYNC);
        assertThat(endpoint.headers()).containsEntry("X-SPI", "true");
        assertThat(endpoint.timeoutSeconds()).isEqualTo(60);
    }

    @Test
    void tier2ResolvesFromConfig() {
        ResolvedEndpoint endpoint = resolver.resolve("process-order");
        assertThat(endpoint.url()).isEqualTo("https://orders.example.com/process");
        assertThat(endpoint.method()).isEqualTo("POST");
        assertThat(endpoint.mode()).isEqualTo(ExchangeMode.ASYNC);
        assertThat(endpoint.timeoutSeconds()).isEqualTo(45);
        assertThat(endpoint.headers()).containsEntry("X-Api-Key", "secret-key");
    }

    @Test
    void allTiersMerged() {
        Set<String> caps = resolver.capabilities();
        // Tier 1: send-email, validate-address
        // Tier 2: process-order, generate-report (send-email already from Tier 1)
        assertThat(caps).containsExactlyInAnyOrder(
            "send-email", "validate-address", "process-order", "generate-report"
        );
    }

    @Test
    void resolveUnknownTagThrows() {
        assertThatThrownBy(() -> resolver.resolve("nonexistent"))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("nonexistent");
    }

    @Test
    void firstMatchReturnsFirstKnown() {
        assertThat(resolver.firstMatch(Set.of("unknown", "send-email")))
            .isPresent()
            .hasValue("send-email");
    }

    @Test
    void firstMatchNoneKnownReturnsEmpty() {
        assertThat(resolver.firstMatch(Set.of("unknown-a", "unknown-b"))).isEmpty();
    }

    @Test
    void capabilitiesReturnsAllResolvedTags() {
        assertThat(resolver.capabilities())
            .contains("send-email", "validate-address", "process-order", "generate-report");
    }

    @Test
    void configDefaultsToPostSyncWhenNotSpecified() {
        ResolvedEndpoint endpoint = resolver.resolve("generate-report");
        assertThat(endpoint.method()).isEqualTo("POST");
        assertThat(endpoint.mode()).isEqualTo(ExchangeMode.SYNC);
        assertThat(endpoint.timeoutSeconds()).isEqualTo(30); // global default
    }

    @Test
    void spiTimeoutMinusOneInheritsGlobalDefault() {
        ResolvedEndpoint endpoint = resolver.resolve("validate-address");
        assertThat(endpoint.timeoutSeconds()).isEqualTo(30);
    }

    @Test
    void spiWithExplicitTimeoutUsesItsOwnValue() {
        ResolvedEndpoint endpoint = resolver.resolve("send-email");
        assertThat(endpoint.timeoutSeconds()).isEqualTo(60);
    }

    @Test
    void configHeadersParsedFromDottedKeys() {
        ResolvedEndpoint endpoint = resolver.resolve("process-order");
        assertThat(endpoint.headers()).containsEntry("X-Api-Key", "secret-key");
    }

    @Test
    void spiRouteEmptyHeadersPreserved() {
        // validate-address SPI uses default empty headers
        ResolvedEndpoint endpoint = resolver.resolve("validate-address");
        assertThat(endpoint.headers()).isEmpty();
    }

    @Test
    void initializeWithEmptySpiAndConfig() {
        HttpEndpointResolver empty = new HttpEndpointResolver();
        empty.initialize(List.of(), Map.of(), 30);
        assertThat(empty.capabilities()).isEmpty();
    }

    @Test
    void initializeWithNullConfig() {
        HttpEndpointResolver spiOnly = new HttpEndpointResolver();
        spiOnly.initialize(List.of(SEND_EMAIL_SPI), null, 30);
        assertThat(spiOnly.capabilities()).containsExactly("send-email");
    }

    @Test
    void firstMatchEmptyCapabilities() {
        assertThat(resolver.firstMatch(Set.of())).isEmpty();
    }

    @Test
    void capabilitiesReturnsUnmodifiableSet() {
        Set<String> caps = resolver.capabilities();
        assertThatThrownBy(() -> caps.add("should-fail"))
            .isInstanceOf(UnsupportedOperationException.class);
    }
}
