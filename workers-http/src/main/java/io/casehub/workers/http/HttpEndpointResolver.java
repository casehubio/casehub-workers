package io.casehub.workers.http;

import io.casehub.workers.common.WorkerCapabilityResolver;
import io.casehub.workers.common.WorkerProvisioningException;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static jakarta.interceptor.Interceptor.Priority.APPLICATION;

@ApplicationScoped
public class HttpEndpointResolver implements WorkerCapabilityResolver<ResolvedEndpoint> {

    @Inject @Any
    Instance<HttpWorkerRoute> spiRoutes;

    @Inject
    Config config;

    @ConfigProperty(name = "casehub.workers.http.default-timeout-seconds", defaultValue = "30")
    int defaultTimeoutSeconds;

    private final Map<String, ResolvedEndpoint> resolvedEndpoints = new HashMap<>();

    void onStartup(@Observes @Priority(APPLICATION) StartupEvent ev) {
        initialize();
    }

    /**
     * CDI startup entry point — gathers injected fields and delegates.
     */
    void initialize() {
        Map<String, Map<String, String>> configEndpoints = loadConfigEndpoints();
        List<HttpWorkerRoute> routes = List.of();
        if (spiRoutes != null && !spiRoutes.isUnsatisfied()) {
            routes = spiRoutes.stream().toList();
        }
        initialize(routes, configEndpoints, defaultTimeoutSeconds);
    }

    /**
     * Test-friendly initializer. Accepts all inputs explicitly so tests
     * can call without CDI.
     */
    void initialize(List<HttpWorkerRoute> spiRouteList,
                    Map<String, Map<String, String>> configEndpoints,
                    int defaultTimeout) {
        resolvedEndpoints.clear();

        // Tier 1: SPI-registered HttpWorkerRoute beans (highest priority)
        for (HttpWorkerRoute route : spiRouteList) {
            int timeout = route.timeoutSeconds() == -1 ? defaultTimeout : route.timeoutSeconds();
            resolvedEndpoints.put(route.capabilityTag(), new ResolvedEndpoint(
                route.url(),
                route.method(),
                route.exchangeMode(),
                route.headers(),
                timeout
            ));
        }

        // Tier 2: Configuration properties (putIfAbsent — Tier 1 wins)
        if (configEndpoints != null) {
            configEndpoints.forEach((tag, props) -> {
                resolvedEndpoints.putIfAbsent(tag, buildFromConfig(props, defaultTimeout));
            });
        }

        // Tier 3: EndpointRegistry (platform#73) — structural placeholder.
        // When EndpointRegistry ships, add a tier here that queries named
        // endpoints and calls resolvedEndpoints.putIfAbsent() for each.
        // No code yet — the type doesn't exist.
    }

    @Override
    public ResolvedEndpoint resolve(String capabilityTag) {
        ResolvedEndpoint endpoint = resolvedEndpoints.get(capabilityTag);
        if (endpoint == null) {
            throw WorkerProvisioningException.noRouteFound(capabilityTag);
        }
        return endpoint;
    }

    @Override
    public Optional<String> firstMatch(Set<String> capabilities) {
        return capabilities.stream()
            .filter(resolvedEndpoints::containsKey)
            .findFirst();
    }

    @Override
    public Set<String> capabilities() {
        return Set.copyOf(resolvedEndpoints.keySet());
    }

    private ResolvedEndpoint buildFromConfig(Map<String, String> props, int defaultTimeout) {
        String url = props.get("url");
        String method = props.getOrDefault("method", "POST");
        ExchangeMode mode = parseMode(props.getOrDefault("mode", "SYNC"));
        int timeout = parseTimeout(props.get("timeout-seconds"), defaultTimeout);
        Map<String, String> headers = extractHeaders(props);
        return new ResolvedEndpoint(url, method, mode, headers, timeout);
    }

    private ExchangeMode parseMode(String value) {
        try {
            return ExchangeMode.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ExchangeMode.SYNC;
        }
    }

    private int parseTimeout(String value, int defaultTimeout) {
        if (value == null || value.isBlank()) {
            return defaultTimeout;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultTimeout;
        }
    }

    private Map<String, String> extractHeaders(Map<String, String> props) {
        Map<String, String> headers = new LinkedHashMap<>();
        String headerPrefix = "headers.";
        props.forEach((key, value) -> {
            if (key.startsWith(headerPrefix)) {
                headers.put(key.substring(headerPrefix.length()), value);
            }
        });
        return headers.isEmpty() ? Map.of() : Map.copyOf(headers);
    }

    /**
     * Loads config endpoints from MicroProfile Config by iterating keys
     * with prefix {@code casehub.workers.http.endpoints.} and grouping by tag.
     *
     * <p>Expected format:
     * <pre>
     * casehub.workers.http.endpoints.send-email.url=https://...
     * casehub.workers.http.endpoints.send-email.method=POST
     * casehub.workers.http.endpoints.send-email.mode=SYNC
     * casehub.workers.http.endpoints.send-email.timeout-seconds=30
     * casehub.workers.http.endpoints.send-email.headers.Authorization=Bearer xxx
     * </pre>
     */
    private Map<String, Map<String, String>> loadConfigEndpoints() {
        if (config == null) {
            return Map.of();
        }
        String prefix = "casehub.workers.http.endpoints.";
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        for (String key : config.getPropertyNames()) {
            if (key.startsWith(prefix)) {
                String remainder = key.substring(prefix.length());
                int dot = remainder.indexOf('.');
                if (dot > 0) {
                    String tag = remainder.substring(0, dot);
                    String prop = remainder.substring(dot + 1);
                    config.getOptionalValue(key, String.class).ifPresent(value ->
                        result.computeIfAbsent(tag, k -> new LinkedHashMap<>()).put(prop, value)
                    );
                }
            }
        }
        return result;
    }
}
