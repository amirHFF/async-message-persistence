package io.projectZ.storage;

import io.projectZ.model.ChatMessage;
import io.projectZ.codec.MessageCodec;
import io.projectZ.consumer.MessagePersistenceConsumer;

import java.io.Closeable;

/**
 * Persistence seam. This is the single interface the consumer pipeline depends on.
 * <p>
 * To add a new backing database (Postgres, Mongo, S3, ...) later, implement this
 * interface and register it in {@link MessageRepositoryFactory} - no change is required
 * to {@link MessagePersistenceConsumer},
 * {@link MessageCodec} or the DLT publisher
 * (Open/Closed principle).
 */
public interface MessageRepository extends Closeable {

    /**
     * Persists a single message durably. Implementations MUST make the write durable
     * before returning (e.g. Redis: rely on AOF/RDB persistence being enabled server-side;
     * a synchronous WAL-backed DB write for SQL backends) since these records are the
     * archive of record.
     *
     * @param message    the decoded message (used for building keys / indices)
     * @param rawPayload the exact original bytes polled from Kafka. Storing the original
     *                   bytes (rather than re-serializing to e.g. JSON) avoids pulling in
     *                   a serialization library purely for storage and keeps the archive
     *                   byte-for-byte reproducible.
     * @throws StorageException on any failure to write. Callers decide (based on
     *                          {@link StorageException#isTransient()}) whether to retry.
     */
    void save(ChatMessage message, byte[] rawPayload) throws StorageException;

    /** Cheap liveness probe used before/around retries. Should not throw. */
    boolean isHealthy();
}
