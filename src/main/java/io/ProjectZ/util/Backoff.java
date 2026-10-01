package io.ProjectZ.util;

/** Tiny exponential backoff helper - avoids pulling in a resilience library for one calculation. */
public final class Backoff {

    private Backoff() {
    }

    public static long delayForAttempt(long initialMs, int attempt) {
        // attempt is 1-based
        long delay = initialMs * (1L << Math.max(0, attempt - 1));
        return Math.min(delay, 30_000L);
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
