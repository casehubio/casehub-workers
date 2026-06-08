package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.workers.common.WorkerProvisioningException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.camel.ExchangePattern;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CamelCapabilityResolverTest {

    private CamelCapabilityResolver resolver;

    @BeforeEach
    void setUp() throws Exception {
        DefaultCamelContext camelContext = new DefaultCamelContext();
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:send-email").id("send-email").to("mock:result");
            }
        });
        camelContext.start();

        resolver = new CamelCapabilityResolver();
        resolver.camelContext = camelContext;
        resolver.configCapabilities = Map.of("enrich-lead", "kafka:leads?brokers=localhost:9092");

        // Empty SPI routes — use an Instance that iterates over nothing
        resolver.spiRoutes = new EmptyInstance();
        resolver.initialize();
    }

    @Test
    void resolve_conventionRoute_returnsDirectUri() {
        assertThat(resolver.resolve("send-email")).isEqualTo("direct:send-email");
    }

    @Test
    void resolve_configRoute_returnsConfiguredUri() {
        assertThat(resolver.resolve("enrich-lead")).isEqualTo("kafka:leads?brokers=localhost:9092");
    }

    @Test
    void resolve_unknownCapability_throws() {
        assertThatThrownBy(() -> resolver.resolve("nonexistent"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void firstMatch_returnsFirstKnown() {
        Optional<String> match = resolver.firstMatch(Set.of("unknown", "send-email", "enrich-lead"));
        assertThat(match).isPresent();
        assertThat(Set.of("send-email", "enrich-lead")).contains(match.get());
    }

    @Test
    void firstMatch_noneKnown_returnsEmpty() {
        assertThat(resolver.firstMatch(Set.of("a", "b"))).isEmpty();
    }

    @Test
    void capabilities_returnsAll() {
        assertThat(resolver.capabilities()).containsExactlyInAnyOrder("send-email", "enrich-lead");
    }

    @Test
    void exchangePattern_defaultsToInOnly() {
        assertThat(resolver.exchangePattern("send-email")).isEqualTo(ExchangePattern.InOnly);
    }

    /**
     * Minimal Instance implementation that iterates over nothing.
     * Avoids full CDI mock complexity for unit tests.
     */
    @SuppressWarnings("unchecked")
    static class EmptyInstance implements jakarta.enterprise.inject.Instance<CamelWorkerRoute> {
        @Override public CamelWorkerRoute get() { throw new UnsupportedOperationException(); }
        @Override public java.util.Iterator<CamelWorkerRoute> iterator() { return Collections.emptyIterator(); }
        @Override public boolean isUnsatisfied() { return true; }
        @Override public boolean isAmbiguous() { return false; }
        @Override public boolean isResolvable() { return false; }
        @Override public void destroy(CamelWorkerRoute instance) {}
        @Override public Handle<CamelWorkerRoute> getHandle() { return null; }
        @Override public Iterable<? extends Handle<CamelWorkerRoute>> handles() { return java.util.List.of(); }
        @Override public jakarta.enterprise.inject.Instance<CamelWorkerRoute> select(java.lang.annotation.Annotation... qualifiers) { return this; }
        @Override public <U extends CamelWorkerRoute> jakarta.enterprise.inject.Instance<U> select(Class<U> subtype, java.lang.annotation.Annotation... qualifiers) { return (jakarta.enterprise.inject.Instance<U>) this; }
        @Override public <U extends CamelWorkerRoute> jakarta.enterprise.inject.Instance<U> select(jakarta.enterprise.util.TypeLiteral<U> subtype, java.lang.annotation.Annotation... qualifiers) { return (jakarta.enterprise.inject.Instance<U>) this; }
        @Override public Stream<CamelWorkerRoute> stream() { return Stream.empty(); }
    }
}
