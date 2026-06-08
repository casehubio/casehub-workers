package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.api.model.BackoffStrategy;
import io.casehub.api.model.ExecutionPolicy;
import io.casehub.api.model.RetryPolicy;
import org.junit.jupiter.api.Test;

class CamelWorkerFaultEventHandlerTest {

    @Test
    void computeBackoffDelayMs_fixed() {
        RetryPolicy policy = new RetryPolicy(3, 10000, BackoffStrategy.FIXED);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1)).isEqualTo(10000L);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 2)).isEqualTo(10000L);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 3)).isEqualTo(10000L);
    }

    @Test
    void computeBackoffDelayMs_exponential() {
        RetryPolicy policy = new RetryPolicy(5, 1000, BackoffStrategy.EXPONENTIAL);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1)).isEqualTo(1000L);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 2)).isEqualTo(2000L);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 3)).isEqualTo(4000L);
    }

    @Test
    void computeBackoffDelayMs_exponential_cappedAt30Seconds() {
        RetryPolicy policy = new RetryPolicy(10, 5000, BackoffStrategy.EXPONENTIAL);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 10)).isEqualTo(30_000L);
    }

    @Test
    void computeBackoffDelayMs_jitter_inRange() {
        RetryPolicy policy = new RetryPolicy(3, 10000, BackoffStrategy.EXPONENTIAL_WITH_JITTER);
        for (int i = 0; i < 100; i++) {
            long delay = CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1);
            assertThat(delay).isBetween(0L, 10000L);
        }
    }

    @Test
    void computeBackoffDelayMs_nullDelayMs_returnsZero() {
        RetryPolicy policy = new RetryPolicy(3, null, BackoffStrategy.FIXED);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1)).isEqualTo(0L);
    }

    @Test
    void computeBackoffDelayMs_nullStrategy_defaultsToFixed() {
        RetryPolicy policy = new RetryPolicy(3, 5000, null);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1)).isEqualTo(5000L);
    }

    @Test
    void resolveRetryPolicy_nullExecutionPolicy_returnsDefault() {
        RetryPolicy result = CamelWorkerFaultEventHandler.resolveRetryPolicy(null, null);
        assertThat(result.maxAttempts()).isEqualTo(3);
        assertThat(result.delayMs()).isEqualTo(10000);
        assertThat(result.backoffStrategy()).isEqualTo(BackoffStrategy.FIXED);
    }

    @Test
    void resolveRetryPolicy_nullRetryPolicy_returnsDefault() {
        ExecutionPolicy ep = new ExecutionPolicy(5000, null);
        RetryPolicy result = CamelWorkerFaultEventHandler.resolveRetryPolicy(ep, null);
        assertThat(result.maxAttempts()).isEqualTo(3);
    }

    @Test
    void resolveRetryPolicy_withPolicy_returnsIt() {
        RetryPolicy custom = new RetryPolicy(5, 2000, BackoffStrategy.EXPONENTIAL);
        ExecutionPolicy ep = new ExecutionPolicy(5000, custom);
        RetryPolicy result = CamelWorkerFaultEventHandler.resolveRetryPolicy(ep, custom);
        assertThat(result).isSameAs(custom);
    }
}
