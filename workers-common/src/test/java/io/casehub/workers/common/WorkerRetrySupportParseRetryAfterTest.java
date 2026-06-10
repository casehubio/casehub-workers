package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class WorkerRetrySupportParseRetryAfterTest {

    @Test
    void integerSeconds() {
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter("60", 429, "Too Many Requests");
        assertThat(ex).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) ex).retryAfterMs()).isEqualTo(60000);
    }

    @Test
    void httpDate_inFuture() {
        ZonedDateTime future = ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(120);
        String httpDate = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US)
            .format(future);
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter(httpDate, 429, "Too Many Requests");
        assertThat(ex).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) ex).retryAfterMs()).isBetween(115_000L, 125_000L);
    }

    @Test
    void httpDate_inPast() {
        ZonedDateTime past = ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(60);
        String httpDate = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US)
            .format(past);
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter(httpDate, 429, "Too Many Requests");
        assertThat(ex).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) ex).retryAfterMs()).isEqualTo(0);
    }

    @Test
    void unparseable() {
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter("garbage", 429, "Too Many Requests");
        assertThat(ex).isNotInstanceOf(RetryAfterException.class);
        assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    void nullValue() {
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter(null, 429, "Too Many Requests");
        assertThat(ex).isNotInstanceOf(RetryAfterException.class);
    }

    @Test
    void blankValue() {
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter("   ", 429, "Too Many Requests");
        assertThat(ex).isNotInstanceOf(RetryAfterException.class);
    }
}
