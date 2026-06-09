package io.casehub.workers.http;

public class RetryAfterException extends RuntimeException {
    private final long retryAfterMs;
    public RetryAfterException(long retryAfterMs, String message) {
        super(message);
        this.retryAfterMs = retryAfterMs;
    }
    public long retryAfterMs() { return retryAfterMs; }
}
