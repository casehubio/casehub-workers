package io.casehub.workers.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.testing.WorkerTestSupport;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class HttpWorkerExecutionManagerTest {

    private HttpWorkerExecutionManager manager;
    private HttpEndpointResolver httpEndpointResolver;
    private HttpWorkerFaultPublisher httpWorkerFaultPublisher;
    private AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry;
    private WorkflowCompletionPublisher completionPublisher;
    private WebClient webClient;
    private HttpRequest<Buffer> request;

    private static final ResolvedEndpoint SYNC_ENDPOINT = new ResolvedEndpoint(
        "https://api.example.com/process", "POST", ExchangeMode.SYNC, Map.of(), 30);
    private static final ResolvedEndpoint ASYNC_ENDPOINT = new ResolvedEndpoint(
        "https://api.example.com/process", "POST", ExchangeMode.ASYNC, Map.of(), 30);

    @BeforeEach
    void setUp() {
        httpEndpointResolver = mock(HttpEndpointResolver.class);
        httpWorkerFaultPublisher = mock(HttpWorkerFaultPublisher.class);
        asyncWorkerCompletionRegistry = mock(AsyncWorkerCompletionRegistry.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        webClient = mock(WebClient.class);
        request = mock(HttpRequest.class);

        manager = new HttpWorkerExecutionManager();
        manager.httpEndpointResolver = httpEndpointResolver;
        manager.httpWorkerFaultPublisher = httpWorkerFaultPublisher;
        manager.asyncWorkerCompletionRegistry = asyncWorkerCompletionRegistry;
        manager.completionPublisher = completionPublisher;
        manager.objectMapper = new ObjectMapper();
        manager.webClient = webClient;
        manager.asyncTimeoutMinutes = 60;

        when(webClient.requestAbs(any(HttpMethod.class), anyString())).thenReturn(request);
        when(request.timeout(anyLong())).thenReturn(request);
        when(request.putHeader(anyString(), anyString())).thenReturn(request);
    }

    // --- Sync 2xx tests ---

    @Test
    void sync_2xx_completesWithResponseBody() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(200, "OK", "{\"result\":\"ok\"}");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of("key", "val")).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).containsEntry("result", "ok");
        verify(httpWorkerFaultPublisher, never()).fault(
            any(WorkerCorrelationContext.class), any(), anyLong(), any());
    }

    @Test
    void sync_2xx_emptyBody_completesWithEmptyMap() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(200, "OK", "");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).isEmpty();
    }

    @Test
    void sync_2xx_nonJsonBody_completesWithEmptyMap() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(200, "OK", "not json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).isEmpty();
    }

    // --- 4xx tests ---

    @Test
    void sync_400_throwsPermanentFault() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(400, "Bad Request", "");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
        assertThat(((PermanentFaultException) causeCaptor.getValue()).statusCode()).isEqualTo(400);
    }

    @Test
    void sync_404_throwsPermanentFault() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(404, "Not Found", "");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
        assertThat(((PermanentFaultException) causeCaptor.getValue()).statusCode()).isEqualTo(404);
    }

    // --- 429 tests ---

    @Test
    void sync_429_withRetryAfterSeconds() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(429, "Too Many Requests", "");
        when(response.getHeader("Retry-After")).thenReturn("30");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) causeCaptor.getValue()).retryAfterMs()).isEqualTo(30000);
    }

    @Test
    void sync_429_withoutRetryAfter() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(429, "Too Many Requests", "");
        when(response.getHeader("Retry-After")).thenReturn(null);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isInstanceOf(RuntimeException.class)
            .isNotInstanceOf(RetryAfterException.class);
    }

    // --- 5xx tests ---

    @Test
    void sync_500_throwsTransientFault() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(500, "Internal Server Error", "");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isInstanceOf(RuntimeException.class)
            .isNotInstanceOf(PermanentFaultException.class)
            .isNotInstanceOf(RetryAfterException.class)
            .hasMessageContaining("500");
    }

    // --- Header tests ---

    @Test
    void sync_casehubHeaders_set() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(200, "OK", "{}");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        verify(request).putHeader(eq("casehub-idempotency"), anyString());
        verify(request).putHeader(eq("casehub-case-id"), eq(instance.getUuid().toString()));
        verify(request).putHeader(eq("casehub-tenancy-id"), eq(instance.tenancyId));
        verify(request).putHeader(eq("casehub-task-type"), eq("cap"));
    }

    @Test
    void sync_endpointHeaders_overrideCasehubHeaders() {
        // Endpoint has a header that collides with a CaseHub header
        ResolvedEndpoint endpointWithOverride = new ResolvedEndpoint(
            "https://api.example.com/process", "POST", ExchangeMode.SYNC,
            Map.of("casehub-tenancy-id", "override-tenant", "X-Custom", "custom-value"), 30);
        when(httpEndpointResolver.resolve("cap")).thenReturn(endpointWithOverride);
        HttpResponse<Buffer> response = mockResponse(200, "OK", "{}");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        // CaseHub header set first, then endpoint header overrides
        var inOrder = inOrder(request);
        inOrder.verify(request).putHeader("casehub-tenancy-id", instance.tenancyId);
        inOrder.verify(request).putHeader("casehub-tenancy-id", "override-tenant");
    }

    // --- Async tests ---

    @Test
    void async_2xx_registersAndFiresForget() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(ASYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(200, "OK", "{}");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        PendingCompletion pending = stubPendingCompletion(instance, worker, cap);
        when(asyncWorkerCompletionRegistry.register(
            eq(HttpWorkerConstants.WORKER_TYPE), any(WorkerCorrelationContext.class),
            eq(cap), eq(1L), eq(Duration.ofMinutes(60)), eq(Map.of())))
            .thenReturn(pending);

        manager.submit(1L, instance, worker, cap, Map.of("key", "val")).await().indefinitely();

        verify(asyncWorkerCompletionRegistry).register(
            eq(HttpWorkerConstants.WORKER_TYPE), any(WorkerCorrelationContext.class),
            eq(cap), eq(1L), any(Duration.class), eq(Map.of()));
        verify(completionPublisher, never()).complete(any(), any());
        verify(httpWorkerFaultPublisher, never()).fault(
            any(WorkerCorrelationContext.class), any(), anyLong(), any());
    }

    @Test
    void async_nonOk_firesFault() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(ASYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(500, "Internal Server Error", "");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        PendingCompletion pending = stubPendingCompletion(instance, worker, cap);
        when(asyncWorkerCompletionRegistry.register(
            eq(HttpWorkerConstants.WORKER_TYPE), any(WorkerCorrelationContext.class),
            eq(cap), eq(1L), eq(Duration.ofMinutes(60)), eq(Map.of())))
            .thenReturn(pending);

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("500");
    }

    @Test
    void async_headersIncludeWorkerIdAndCallbackToken() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(ASYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(200, "OK", "{}");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        PendingCompletion pending = stubPendingCompletion(instance, worker, cap);
        when(asyncWorkerCompletionRegistry.register(
            eq(HttpWorkerConstants.WORKER_TYPE), any(WorkerCorrelationContext.class),
            eq(cap), eq(1L), eq(Duration.ofMinutes(60)), eq(Map.of())))
            .thenReturn(pending);

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        verify(request).putHeader(CasehubWorkerHeaders.WORKER_ID, pending.dispatchId());
        verify(request).putHeader(CasehubWorkerHeaders.CALLBACK_TOKEN, pending.callbackToken());
    }

    @Test
    void sync_headersDoNotIncludeWorkerIdOrCallbackToken() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        HttpResponse<Buffer> response = mockResponse(200, "OK", "{}");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        verify(request, never()).putHeader(eq(CasehubWorkerHeaders.WORKER_ID), anyString());
        verify(request, never()).putHeader(eq(CasehubWorkerHeaders.CALLBACK_TOKEN), anyString());
    }

    // --- URI template tests ---

    @Test
    void uriTemplate_interpolated() {
        String result = HttpWorkerExecutionManager.interpolateUrl(
            "https://api.example.com/orders/{orderId}/ship",
            Map.of("orderId", "123"));
        assertThat(result).isEqualTo("https://api.example.com/orders/123/ship");
    }

    @Test
    void uriTemplate_missingKey_throwsPermanentFault() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(new ResolvedEndpoint(
            "https://api.example.com/{missing}", "POST", ExchangeMode.SYNC, Map.of(), 30));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isInstanceOf(PermanentFaultException.class)
            .hasMessageContaining("{missing}");
    }

    @Test
    void uriTemplate_noPlaceholders_passthrough() {
        String url = "https://example.com/api";
        String result = HttpWorkerExecutionManager.interpolateUrl(url, Map.of());
        assertThat(result).isEqualTo(url);
    }

    @Test
    void uriTemplate_urlEncoded() {
        String result = HttpWorkerExecutionManager.interpolateUrl(
            "https://api.example.com/search/{query}",
            Map.of("query", "hello world&foo=bar"));
        assertThat(result).isEqualTo("https://api.example.com/search/hello+world%26foo%3Dbar");
    }

    // --- Delegation tests ---

    @Test
    void schedulePersistedEvent_returnsVoid() {
        assertThat(manager.schedulePersistedEvent(new EventLog()).await().indefinitely()).isNull();
    }

    @Test
    void getActiveWorkCount_delegatesToRegistry() {
        when(asyncWorkerCompletionRegistry.countByWorkerName("w1")).thenReturn(3);
        assertThat(manager.getActiveWorkCount("w1")).isEqualTo(3);
    }

    // --- Connection failure tests ---

    @Test
    void sync_connectionRefused_firesFault() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        when(request.sendJson(any())).thenReturn(
            Uni.createFrom().failure(new java.net.ConnectException("Connection refused")));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(java.net.ConnectException.class);
        verify(completionPublisher, never()).complete(any(), any());
    }

    @Test
    void sync_connectionTimeout_firesFault() {
        when(httpEndpointResolver.resolve("cap")).thenReturn(SYNC_ENDPOINT);
        when(request.sendJson(any())).thenReturn(
            Uni.createFrom().failure(new java.util.concurrent.TimeoutException("Connection timed out")));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(java.util.concurrent.TimeoutException.class);
        verify(completionPublisher, never()).complete(any(), any());
    }

    // --- Async 429 TTL capping test ---

    @Test
    void async_429_retryAfterCappedToRemainingTtl() {
        ResolvedEndpoint asyncEndpoint = new ResolvedEndpoint(
            "https://api.example.com/process", "POST", ExchangeMode.ASYNC, Map.of(), 30);
        when(httpEndpointResolver.resolve("cap")).thenReturn(asyncEndpoint);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        PendingCompletion pending = new PendingCompletion(
            "dispatch-123", HttpWorkerConstants.WORKER_TYPE,
            new WorkerCorrelationContext(instance, worker, "idem", instance.tenancyId),
            "token", cap, 1L, Instant.now(),
            Instant.now().plusSeconds(60), Map.of());
        when(asyncWorkerCompletionRegistry.register(anyString(), any(), any(), any(), any(), any()))
            .thenReturn(pending);

        HttpResponse<Buffer> response = mockResponse(429, "Too Many Requests", "");
        when(response.getHeader("Retry-After")).thenReturn("3600");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        RetryAfterException ra = (RetryAfterException) causeCaptor.getValue();
        assertThat(ra.retryAfterMs()).isLessThanOrEqualTo(60_000L);
    }

    // --- Missing route test ---

    @Test
    void submit_missingRoute_firesFault() {
        when(httpEndpointResolver.resolve("missing"))
            .thenThrow(WorkerProvisioningException.noRouteFound("missing"));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "missing");
        Capability cap = WorkerTestSupport.testCapability("missing");

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        verify(httpWorkerFaultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L),
            any(WorkerProvisioningException.class));
        verify(completionPublisher, never()).complete(any(), any());
    }

    // --- Helpers ---

    private PendingCompletion stubPendingCompletion(CaseInstance instance, Worker worker, Capability cap) {
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            instance, worker, "test-idempotency", instance.tenancyId);
        return new PendingCompletion(
            "dispatch-123", HttpWorkerConstants.WORKER_TYPE, ctx,
            "callback-token-abc", cap, 1L,
            Instant.now(), Instant.now().plusSeconds(3600), Map.of());
    }

    private HttpResponse<Buffer> mockResponse(int statusCode, String statusMessage, String body) {
        HttpResponse<Buffer> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(statusCode);
        when(response.statusMessage()).thenReturn(statusMessage);
        if (body == null || body.isEmpty()) {
            when(response.body()).thenReturn(null);
        } else {
            when(response.body()).thenReturn(Buffer.buffer(body));
        }
        return response;
    }
}
