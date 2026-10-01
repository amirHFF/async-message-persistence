package io.ProjectZ.dlt;

import java.io.Closeable;

/**
 * Publishes an unprocessable record to the Dead Letter Topic.
 */
public interface DeadLetterPublisher extends Closeable {

    /**
     * @param originalTopic     topic the poison-pill record was originally read from
     * @param originalPartition partition of the original record
     * @param originalOffset    offset of the original record
     * @param originalKey       original Kafka record key bytes (may be {@code null})
     * @param originalValue     original Kafka record value bytes (never re-encoded/altered)
     * @param reason            short machine-readable failure reason, e.g. {@code DESERIALIZATION_ERROR}
     * @param errorDetail       human-readable detail (exception message) for debugging
     * @return {@code true} if the record was durably accepted by the DLT, {@code false} otherwise.
     *         A {@code false} result means the caller MUST NOT advance the source offset,
     *         since the original message would otherwise be lost.
     */
    boolean publish(String originalTopic, int originalPartition, long originalOffset,
                     String originalKey, byte[] originalValue, String reason, String errorDetail);
}
