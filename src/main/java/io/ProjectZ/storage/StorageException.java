package io.ProjectZ.storage;

/**
 * Signals a failure to persist a message.
 * <p>
 * {@link #isTransient()} distinguishes:
 * <ul>
 *     <li><b>transient = true</b> - infrastructure-level failure (connection refused,
 *         timeout, broker/server temporarily unavailable). The consumer will retry with
 *         backoff and will NOT advance the Kafka offset until the write succeeds, so no
 *         message is ever skipped because storage was briefly down.</li>
 *     <li><b>transient = false</b> - a failure that is not expected to heal by retrying
 *         (e.g. a data/serialization issue on the storage side). Treated the same as a
 *         codec failure: routed to the DLT so the pipeline keeps moving.</li>
 * </ul>
 */
public class StorageException extends Exception {

    private final boolean isTransient;

    public StorageException(String message, Throwable cause, boolean isTransient) {
        super(message, cause);
        this.isTransient = isTransient;
    }

    public boolean isTransient() {
        return isTransient;
    }
}
